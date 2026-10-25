"""The fidelity knob and its deterministic, RECORDED subsampling (#2029; ADR-0102 D2).

A run on a subsample is only worth having if two such runs are **comparable**:
two runs whose subsamples differ are two different experiments wearing one name,
and comparing them would rank the data rather than the configuration.

So a subsample here is three things, and the third is the one usually missed:

1. **deterministic** — the kept set is a pure function of (corpus, seed, n),
   with no dependence on row ORDER, file layout, engine or process. Each row is
   content-hashed and the kept set is the ``n`` rows with the smallest
   ``H(seed ‖ row_hash)``. That is the whole algorithm: streaming, stable under
   shuffling and re-serialisation, and identical in Python, DuckDB or anywhere
   else it is ever reimplemented, which a reservoir sample seeded by row arrival
   order is not;
2. **recorded** — ``seed``, ``n`` and a **row-hash digest** ride the stage's
   stats and the run's ``parameters``, so "was this the same data?" is answered
   from the record rather than by re-reading a corpus that may have moved;
3. **order-independent in the digest** — the digest is over the SORTED row
   hashes, so a stage that streams rows in a different order still proves it
   kept the same rows.

Row hashing is over the row's *canonical* form: JSON objects are re-serialised
with sorted keys, so a re-export that changed key order is not a different
corpus. A row that is not JSON hashes as its stripped bytes.

Nothing here reads a file or knows what a dataset is — the pure half. A training
pack's ``sample.fidelity`` stage and a dataset pack's ``sample.rows`` stage share
this one algorithm, and since an operator pack never imports another, the module
both may import is the core's.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

#: The stat/parameter keys a fidelity-aware stage records. Named here so the
#: stage, the operator and the readers cannot spell them differently.
STAT_KEYS = ("fidelity_rows", "fidelity_fraction", "fidelity_seed", "fidelity_n",
             "fidelity_total", "fidelity_digest", "fidelity_sampled")

_DIGEST_ALGO = "sha256"


def row_hash(row: Any) -> str:
    """The content hash of one row, canonicalised.

    A JSON object is re-serialised with sorted keys and compact separators
    before hashing, so ``{"a":1,"b":2}`` and ``{"b":2,"a":1}`` are the same row
    — an export that changed key order must not read as a different corpus.
    Anything that does not parse as JSON hashes as its stripped UTF-8 bytes.
    """
    if isinstance(row, (bytes, bytearray)):
        raw = bytes(row).decode("utf-8", "replace")
    elif isinstance(row, str):
        raw = row
    else:
        raw = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
    text = raw.strip()
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
    canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def digest_rows(rows: Iterable[Any], *, hashed: bool = False) -> str:
    """The ORDER-INDEPENDENT digest of a set of rows.

    ``sha256`` over the sorted row hashes: two stages that kept the same rows in
    different orders produce the same digest, and one that kept a different row
    cannot. ``hashed=True`` when the caller already has the per-row hexdigests
    (the streaming path, which never holds the corpus in memory).
    """
    hexes = sorted(str(r) if hashed else row_hash(r) for r in rows)
    h = hashlib.sha256()
    for x in hexes:
        h.update(x.encode("ascii"))
        h.update(b"\n")
    return f"{_DIGEST_ALGO}:{h.hexdigest()}"


@dataclass(frozen=True)
class Fidelity:
    """How much of the corpus a run may see.

    ``rows`` wins over ``fraction`` when both are set — an absolute cap is what
    an operator types when they mean "this many, whatever the corpus does", and
    silently multiplying it by a fraction would be a surprise. Both unset (0) is
    FULL fidelity: the knob is off and the pipeline behaves exactly as before,
    which is what keeps an existing launch byte-identical.
    """

    rows: int = 0
    fraction: float = 0.0
    seed: int = 13

    @classmethod
    def from_config(cls, config: dict[str, Any], *, prefix: str = "fidelity_") -> "Fidelity":
        def _get(name: str, default: Any) -> Any:
            v = config.get(f"{prefix}{name}", config.get(name))
            return default if v in (None, "") else v

        return cls(rows=int(_get("rows", 0) or 0), fraction=float(_get("fraction", 0.0) or 0.0),
                   seed=int(_get("seed", 13) or 13))

    def is_set(self) -> bool:
        return self.rows > 0 or self.fraction > 0

    def n_for(self, total: int) -> int:
        """How many rows this fidelity keeps out of ``total`` (``total`` = all).

        A fraction always keeps at least one row: a 1 % fidelity on a 20-row
        corpus means "as small as this gets", never "train on nothing".
        """
        if not self.is_set() or total <= 0:
            return max(0, int(total))
        if self.rows > 0:
            return min(int(self.rows), int(total))
        return min(int(total), max(1, int(total * float(self.fraction))))

    def as_dict(self) -> dict[str, Any]:
        return {"fidelity_rows": self.rows, "fidelity_fraction": self.fraction,
                "fidelity_seed": self.seed}


@dataclass
class SamplePlan:
    """What a fidelity did to a corpus — the RECORD half."""

    n: int
    total: int
    seed: int
    digest: str
    sampled: bool
    kept: list[Any] = field(default_factory=list)

    def as_stats(self, fidelity: Optional[Fidelity] = None) -> dict[str, Any]:
        """The flat scalars a stage publishes (the boundary rule: no nesting)."""
        f = fidelity or Fidelity(seed=self.seed)
        return {"fidelity_rows": f.rows, "fidelity_fraction": f.fraction,
                "fidelity_seed": self.seed, "fidelity_n": self.n,
                "fidelity_total": self.total, "fidelity_digest": self.digest,
                "fidelity_sampled": self.sampled}


def keeps(row_hex: str, seed: int) -> str:
    """The sort key one row gets under ``seed`` — ``H(seed ‖ row_hash)``.

    Exposed because a streaming implementation (DuckDB, a 40 GB JSONL) needs the
    same key without importing the rest of this module's row handling.
    """
    return hashlib.sha256(f"{int(seed)}:{row_hex}".encode("ascii")).hexdigest()


def plan(rows: Iterable[Any], fidelity: Fidelity) -> SamplePlan:
    """Apply ``fidelity`` to ``rows`` and record what happened.

    Keeps the ``n`` rows with the smallest ``H(seed ‖ row_hash)`` — a
    deterministic, order-independent choice (see the module docstring). A corpus
    at or under ``n``, or a fidelity that is not set, passes through untouched
    and is reported as ``sampled=False`` with the digest of the WHOLE corpus, so
    a full-fidelity run is still comparable to itself.
    """
    materialised = list(rows)
    total = len(materialised)
    hexes = [row_hash(r) for r in materialised]
    n = fidelity.n_for(total)
    if not fidelity.is_set() or n >= total:
        return SamplePlan(n=total, total=total, seed=fidelity.seed,
                          digest=digest_rows(hexes, hashed=True), sampled=False,
                          kept=materialised)
    order = sorted(range(total), key=lambda i: (keeps(hexes[i], fidelity.seed), hexes[i]))
    picked = sorted(order[:n])
    return SamplePlan(n=n, total=total, seed=fidelity.seed,
                      digest=digest_rows([hexes[i] for i in picked], hashed=True),
                      sampled=True, kept=[materialised[i] for i in picked])


__all__ = ["Fidelity", "STAT_KEYS", "SamplePlan", "digest_rows", "keeps", "plan", "row_hash"]
