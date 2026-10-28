"""The holdout door (ADR-0102 D3, #2020) — refuse a training version that
intersects a holdout, before any compute is spent.

Research C found that no fine-tuning platform pins an eval-set version into a
run or checks contamination; cairn has the pieces (``dataset/decontam``,
``dedup_minhash``, a hash-pinned registry) but they were a pipeline a user had
to know to run. This module is the DOOR: at launch, every ``dataset:`` ref a
run carries is resolved, the ones registered ``role='training'`` are compared
against every ``golden`` and ``test`` version **in the same workspace**, and an
intersection refuses the run naming the offending row ids.

Two passes, in order, over the definitions ``operator_dataset_checks`` owns:

1. **exact** — the canonical row hash (sorted-key JSON, whitespace-collapsed,
   sha256). Always runs; hermetic; no optional dependency.
2. **near-duplicate** — MinHash + LSH over word shingles, which catches the
   paraphrased leak an exact hash cannot. Needs datasketch (the pack's
   ``[minhash]`` extra). When it is absent the verdict says
   ``near_dup_checked = False`` rather than pretending the pass ran — a door
   that silently downgrades its own guarantee is worse than no door.

Both definitions are RE-STATED here rather than imported, because **core may
not import an operator pack** (ADR-0084 D1, pinned by
``tests/core/test_engine_registry.py::test_core_never_imports_an_operator_pack``)
and because the control plane has to be able to close its own door on a
deployment where no pack is installed. They are held to the pack's definitions
by tests that compare the two implementations directly — the hash unconditionally,
the shingling wherever the pack is importable.

**Workspace scoping is the whole safety property in a multi-tenant deployment**
and is enforced at the store: :meth:`ArtifactStore.list_dataset_versions_by_role`
filters on ``workspace_id``, so tenant B's training set is never compared with
tenant A's golden rows — no refusal leaks that A's golden set exists, and no
row of A's is ever read on B's behalf.

The verdict is also the RECORD: :meth:`HoldoutVerdict.as_record` is what the
dispatcher stamps on the run (``Run.holdout``), so a finished model can be
asked which eval sets, at which version hashes, were held out from it.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from cairn.core.dataset_roles import (
    DEFAULT_ROLE,
    HOLDOUT_ROLES,
    read_rows,
    row_id,
)
from cairn.core.eval_cutoff import (
    cutoff_in,
    describe_exclusion,
    filter_rows,
    normalize_cutoff,
    version_after_cutoff,
)

logger = logging.getLogger("cairn.holdout")

#: MinHash Jaccard threshold above which two rows count as the same example.
#: Matches ``dataset/decontam``'s default so the door and the remediation
#: operator agree on what a near-duplicate is.
DEFAULT_NEAR_DUP_THRESHOLD = 0.8
#: Word-shingle size for the near-dup pass — ``dataset/decontam``'s default.
DEFAULT_NGRAM = 5
#: Cap on rows read per version. A door is a launch-time check, not a scan;
#: past this the pass is reported as truncated rather than silently partial.
MAX_ROWS = 200_000


#: MinHash permutations and seed — ``operator_dataset_checks._minhash``'s
#: ``NUM_PERM`` / ``SEED``, so the door's near-dup verdict is the one
#: ``dataset/decontam`` would give over the same rows.
MINHASH_PERM = 128
MINHASH_SEED = 1


def canonical_row_hash(row: dict) -> str:
    """Deterministic content hash of one dataset row.

    Byte-for-byte the definition in ``operator_dataset_checks._canon`` — sorted-key
    JSON, whitespace-collapsed, sha256 — because "the same example" must mean the
    same thing at the door as it does in ``dataset/dedup`` and ``dataset/decontam``.
    It is re-stated here rather than imported so the CONTROL PLANE never needs an
    operator pack installed to close its own door;
    ``tests/core/test_holdout_door.py::test_the_door_hashes_a_row_exactly_as_the_pack_does``
    pins the two together and fails the moment they drift.
    """
    text = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(" ".join(text.split()).encode("utf-8")).hexdigest()


def row_text(row: dict) -> str:
    """Every string leaf of a row, joined — the text the near-dup pass compares.

    Schema-agnostic on purpose (chat, alpaca and completion rows all flatten the
    same way), and identical to ``operator_dataset_checks._ngram.row_text``.
    """
    parts: list[str] = []

    def collect(obj: Any) -> None:
        if isinstance(obj, str):
            parts.append(obj)
        elif isinstance(obj, dict):
            for value in obj.values():
                collect(value)
        elif isinstance(obj, list):
            for value in obj:
                collect(value)

    collect(row)
    return " ".join(parts)


def row_shingles(row: dict, n: int) -> set[tuple[str, ...]]:
    """Overlapping ``n``-word windows of :func:`row_text`, case-folded.

    Identical to ``operator_dataset_checks._ngram.shingles`` — including the
    short-text rule (a text shorter than ``n`` words yields ONE shingle of
    everything it has, so short rows stay comparable instead of contributing an
    empty set). Pinned against the pack by
    ``tests/core/test_holdout_door.py::test_the_door_shingles_a_row_exactly_as_the_pack_does``.
    """
    words = row_text(row).lower().split()
    if not words:
        return set()
    if len(words) < n:
        return {tuple(words)}
    return {tuple(words[i:i + n]) for i in range(len(words) - n + 1)}


@dataclass
class VersionRef:
    """One registry version the door looked at."""

    name: str
    version: int
    role: str
    content_hash: str
    uri: str = ""
    rows: int = 0
    #: False when the rows could not be read — the door reports what it could
    #: not check instead of scoring it as clean.
    readable: bool = True
    note: str = ""
    #: #2033 — when this version's material is from. Empty for a version
    #: registered before the field: undated is never excluded by a cutoff.
    dated_at: str = ""
    #: #2033 — rows of THIS version the run's eval cutoff removed.
    rows_excluded_by_cutoff: int = 0

    @property
    def ref(self) -> str:
        return f"dataset:{self.name}@{self.version}"

    def as_record(self) -> dict[str, Any]:
        return {"ref": self.ref, "role": self.role, "content_hash": self.content_hash,
                "rows": self.rows, "checked": self.readable,
                **({"dated_at": self.dated_at} if self.dated_at else {}),
                **({"rows_excluded_by_cutoff": self.rows_excluded_by_cutoff}
                   if self.rows_excluded_by_cutoff else {}),
                **({"note": self.note} if self.note else {})}


@dataclass
class Overlap:
    """Rows of a training version that are also in a holdout version."""

    training: str          # "dataset:<name>@<v>"
    holdout: str           # "dataset:<name>@<v>"
    holdout_role: str
    holdout_hash: str
    method: str            # "exact" | "near-dup"
    row_ids: list[str]

    def as_record(self) -> dict[str, Any]:
        return {"training": self.training, "holdout": self.holdout,
                "holdout_role": self.holdout_role, "holdout_hash": self.holdout_hash,
                "method": self.method, "row_ids": self.row_ids[:50],
                "row_count": len(self.row_ids)}

    def reason(self) -> str:
        shown = ", ".join(self.row_ids[:10])
        more = f" (+{len(self.row_ids) - 10} more)" if len(self.row_ids) > 10 else ""
        return (f"{self.training} shares {len(self.row_ids)} row(s) with the "
                f"{self.holdout_role} set {self.holdout} ({self.method}): {shown}{more}")


@dataclass
class HoldoutVerdict:
    """What the door found. ``ok`` is the answer; the rest is the record."""

    ok: bool = True
    #: False when nothing was checkable at all (no registry, no dataset refs) —
    #: distinct from ``ok=True``, which means "checked, and clean".
    checked: bool = False
    training: list[VersionRef] = field(default_factory=list)
    holdouts: list[VersionRef] = field(default_factory=list)
    overlaps: list[Overlap] = field(default_factory=list)
    near_dup_checked: bool = False
    warnings: list[str] = field(default_factory=list)
    #: #2033 (ADR-0102 D1) — the eval cutoff the run declared, what it removed,
    #: and which whole versions it removed. On the RECORD, because a door that
    #: compared against a filtered holdout set without saying so would report
    #: "clean" over material it never looked at.
    eval_cutoff: str = ""
    rows_excluded_by_cutoff: int = 0
    versions_excluded_by_cutoff: list[str] = field(default_factory=list)

    def reasons(self) -> list[str]:
        return [o.reason() for o in self.overlaps]

    def cutoff_note(self) -> str:
        """What the cutoff removed from this check, in words, or ``""``."""
        if not self.eval_cutoff:
            return ""
        parts = [p for p in (
            describe_exclusion(self.rows_excluded_by_cutoff, self.eval_cutoff),
            (f"{len(self.versions_excluded_by_cutoff)} whole version(s) excluded: "
             + ", ".join(self.versions_excluded_by_cutoff))
            if self.versions_excluded_by_cutoff else "",
        ) if p]
        return "; ".join(parts)

    def as_record(self) -> dict[str, Any]:
        """The run record's holdout stamp (ADR-0102 D3: "the run record carries
        every eval-set version hash")."""
        return {
            "checked": self.checked,
            "ok": self.ok,
            "near_dup_checked": self.near_dup_checked,
            "training_versions": [v.as_record() for v in self.training],
            "eval_versions": [v.as_record() for v in self.holdouts],
            "overlaps": [o.as_record() for o in self.overlaps],
            "warnings": self.warnings,
            "eval_cutoff": self.eval_cutoff,
            "rows_excluded_by_cutoff": self.rows_excluded_by_cutoff,
            "versions_excluded_by_cutoff": self.versions_excluded_by_cutoff,
        }


# ─── collecting the dataset refs a launch carries ────────────────────────────


def dataset_refs_in(values: Any) -> list[str]:
    """Every ``dataset:`` ref in a launch's input values, de-duplicated,
    first-seen order.

    Mirrors :func:`cairn.core.storage.artifacts.resolve_dataset_refs_in_config`
    exactly — top-level strings and top-level lists of strings — because the
    door must see precisely the refs the launch will resolve. Seeing MORE would
    refuse on data the run never reads; seeing fewer would be the false negative
    this door exists to prevent (#1625 is the same lesson on the resolver side).
    """
    out: list[str] = []

    def take(value: Any) -> None:
        if isinstance(value, str) and value.startswith("dataset:") and value not in out:
            out.append(value)

    if isinstance(values, dict):
        for value in values.values():
            take(value)
            if isinstance(value, list):
                for item in value:
                    take(item)
    return out


# ─── the door ────────────────────────────────────────────────────────────────


def _rows_and_hashes(version: VersionRef) -> tuple[list[dict], list[str]]:
    rows = read_rows(version.uri, limit=MAX_ROWS)
    return rows, [canonical_row_hash(r) for r in rows]


def _near_dup_matches(train_rows: list[dict], holdout_rows: list[dict],
                      *, threshold: float, ngram: int) -> Optional[list[int]]:
    """Training-row indices near-duplicating a holdout row, or ``None`` when
    datasketch (the ``[minhash]`` extra) is not installed.

    The same algorithm as ``dataset/decontam``'s ``minhash`` method — index the
    HOLDOUT set in an LSH, query once per training row — over the same shingles
    (:func:`row_shingles`), at the same defaults. It is written here rather than
    imported because **core may not import an operator pack**
    (``tests/core/test_engine_registry.py::test_core_never_imports_an_operator_pack``,
    ADR-0084 D1): the door has to close on a control plane where no pack is
    installed at all. The shared definition is pinned by a test that compares
    this module's shingling against the pack's.
    """
    try:
        from datasketch import MinHash, MinHashLSH  # noqa: PLC0415 — optional dependency (the [minhash] extra)
    except ImportError:
        return None

    def signature(row: dict):
        sig = MinHash(num_perm=MINHASH_PERM, seed=MINHASH_SEED)
        for shingle in row_shingles(row, ngram):
            sig.update(" ".join(shingle).encode("utf-8"))
        return sig

    lsh = MinHashLSH(threshold=threshold, num_perm=MINHASH_PERM)
    for j, row in enumerate(holdout_rows):
        lsh.insert(f"holdout-{j}", signature(row))
    return [i for i, row in enumerate(train_rows) if lsh.query(signature(row))]


async def check_holdout(
    refs: Sequence[str],
    *,
    store: Any,
    workspace_id: str,
    near_dup: bool = True,
    threshold: float = DEFAULT_NEAR_DUP_THRESHOLD,
    ngram: int = DEFAULT_NGRAM,
    cutoff: Optional[str] = None,
) -> HoldoutVerdict:
    """Judge ``refs`` (``dataset:<name>[@<v>]`` strings) against every ``golden``
    and ``test`` version registered in ``workspace_id``.

    Never raises: a registry that cannot answer produces a verdict with
    ``checked=False`` and a warning, so the door degrades to "not checked"
    rather than to a 500 on the launch path — the same posture the config-scope
    preflight uses. Refusal is only ever on a PROVEN intersection.

    ``cutoff`` (#2033) is the run's declared eval cutoff. Holdout material dated
    after it is not this run's holdout — a version stamped later is dropped
    whole, and later rows inside an earlier version are dropped individually.
    Both counts land on the verdict and therefore on the run record: a door that
    quietly compared against less than the workspace holds would report "clean"
    over material it never read.
    """
    verdict = HoldoutVerdict(eval_cutoff=normalize_cutoff(cutoff) or "")
    if not refs:
        return verdict

    from cairn.core.storage.artifacts import parse_dataset_ref  # noqa: PLC0415 — cycle: artifacts imports core settings

    # 1. resolve the launch's refs, keep the ones registered as training data.
    candidates: list[VersionRef] = []
    for ref in refs:
        try:
            parsed = parse_dataset_ref(ref)
        except ValueError as exc:
            verdict.warnings.append(f"{ref}: {exc}")
            continue
        if parsed is None:
            continue
        name, version = parsed
        try:
            row = await store.get_dataset_version_meta(name, version, workspace_id=workspace_id)
        except Exception as exc:  # noqa: BLE001 — a registry hiccup is "not checked", never a 500
            verdict.warnings.append(f"{ref}: registry lookup failed ({type(exc).__name__}: {exc})")
            continue
        if row is None:
            verdict.warnings.append(f"{ref}: not registered — nothing to check")
            continue
        if (row.get("workspace_id") or "") not in ("", workspace_id):
            # Not this workspace's data; the launch will fail to resolve it
            # anyway. Never read another tenant's rows on this run's behalf.
            continue
        if (row.get("role") or DEFAULT_ROLE) != DEFAULT_ROLE:
            continue
        candidates.append(VersionRef(
            name=name, version=int(row["version"]), role=DEFAULT_ROLE,
            content_hash=str(row.get("artifact_id") or ""), uri=str(row.get("uri") or "")))
    if not candidates:
        return verdict

    # 2. every holdout in THIS workspace — the set the door promises about.
    try:
        holdout_rows = await store.list_dataset_versions_by_role(
            HOLDOUT_ROLES, workspace_id=workspace_id)
    except Exception as exc:  # noqa: BLE001
        verdict.warnings.append(
            f"could not list holdout versions ({type(exc).__name__}: {exc}) — holdout NOT checked")
        return verdict
    holdouts = []
    for r in holdout_rows:
        ref = VersionRef(
            name=str(r["name"]), version=int(r["version"]), role=str(r["role"]),
            content_hash=str(r.get("artifact_id") or ""), uri=str(r.get("uri") or ""),
            dated_at=str(r.get("dated_at") or ""))
        # #2033 — a holdout version dated after the run's cutoff is not this
        # run's holdout. Named on the record so the exclusion is auditable.
        if version_after_cutoff(r, verdict.eval_cutoff or None):
            verdict.versions_excluded_by_cutoff.append(ref.ref)
            continue
        holdouts.append(ref)
    verdict.training = candidates
    verdict.holdouts = holdouts
    verdict.checked = True
    if not holdouts:
        return verdict

    # 3. read both sides once.
    loaded_rows: dict[str, list[dict]] = {}
    loaded_hashes: dict[str, list[str]] = {}
    for version in candidates + holdouts:
        try:
            rows, hashes = _rows_and_hashes(version)
        except Exception as exc:  # noqa: BLE001 — an unreadable version is reported, not guessed at
            version.readable = False
            version.note = f"{type(exc).__name__}: {exc}"
            verdict.warnings.append(
                f"{version.ref} ({version.role}) could not be read for the holdout check: "
                f"{version.note}")
            continue
        if verdict.eval_cutoff and version.role in HOLDOUT_ROLES:
            # Row-level cutoff on the HOLDOUT side only. Filtering the training
            # side would change what the run trains on, which is not a door's
            # job — the door decides what it is COMPARED against.
            paired = [(row, h) for row, h in zip(rows, hashes)]
            kept, excluded, _ = filter_rows(
                [{"__row": row, "__hash": h, "dated_at": row.get("dated_at")}
                 for row, h in paired], verdict.eval_cutoff)
            rows = [k["__row"] for k in kept]
            hashes = [k["__hash"] for k in kept]
            version.rows_excluded_by_cutoff = excluded
            verdict.rows_excluded_by_cutoff += excluded
        version.rows = len(rows)
        loaded_rows[version.ref] = rows
        loaded_hashes[version.ref] = hashes

    # 4. exact, then near-duplicate.
    near_dup_ran = False
    near_dup_unavailable = False
    for train in candidates:
        train_rows = loaded_rows.get(train.ref)
        if train_rows is None:
            continue
        train_hashes = loaded_hashes[train.ref]
        for holdout in holdouts:
            hold_rows = loaded_rows.get(holdout.ref)
            if hold_rows is None:
                continue
            hold_hashes = set(loaded_hashes[holdout.ref])
            exact = [i for i, h in enumerate(train_hashes) if h in hold_hashes]
            if exact:
                verdict.overlaps.append(Overlap(
                    training=train.ref, holdout=holdout.ref, holdout_role=holdout.role,
                    holdout_hash=holdout.content_hash, method="exact",
                    row_ids=[row_id(train_rows[i], i) for i in exact]))
            if not near_dup:
                continue
            matches = _near_dup_matches(train_rows, hold_rows, threshold=threshold, ngram=ngram)
            if matches is None:
                near_dup_unavailable = True
                continue
            near_dup_ran = True
            extra = [i for i in matches if i not in set(exact)]
            if extra:
                verdict.overlaps.append(Overlap(
                    training=train.ref, holdout=holdout.ref, holdout_role=holdout.role,
                    holdout_hash=holdout.content_hash, method="near-dup",
                    row_ids=[row_id(train_rows[i], i) for i in extra]))

    verdict.near_dup_checked = near_dup_ran
    if near_dup and near_dup_unavailable:
        verdict.warnings.append(
            "near-duplicate pass skipped: install the operator pack's [minhash] extra "
            "(pip install 'cairn-operator-dataset-checks[minhash]') for paraphrase-level "
            "contamination checking — only exact row hashes were compared")
    verdict.ok = not verdict.overlaps
    return verdict


async def check_holdout_for_inputs(
    inputs: dict[str, Any],
    *,
    workspace_id: str,
    store: Any = None,
    near_dup: bool = True,
    cutoff: Optional[str] = None,
) -> HoldoutVerdict:
    """The launch-path entry point: pull the ``dataset:`` refs out of a run's
    inputs and run :func:`check_holdout` over them.

    The eval cutoff comes from the inputs too (#2033,
    :func:`cairn.core.eval_cutoff.cutoff_in`) unless the caller passes one, so a
    pipeline declares ``eval_cutoff`` wherever its schema puts it and the door
    finds it — the same treatment ``dataset:`` refs already get.

    Builds the artifact store from ``CAIRN_STORAGE`` when one isn't supplied.
    Returns a ``checked=False`` verdict — never an exception — when the registry
    is unconfigured, so an OSS single-node install with no storage still
    launches runs.
    """
    refs = dataset_refs_in(inputs)
    try:
        cutoff = normalize_cutoff(cutoff) if cutoff is not None else cutoff_in(inputs)
    except ValueError as exc:
        # A malformed cutoff must not read as "no cutoff" — that would silently
        # measure against everything, which is the failure the field prevents.
        return HoldoutVerdict(warnings=[f"eval_cutoff is unusable ({exc}) — holdout NOT checked"])
    if not refs:
        return HoldoutVerdict(eval_cutoff=cutoff or "")
    if store is None:
        from cairn.core.storage.artifacts import build_artifact_store  # noqa: PLC0415 — off the hot import path

        store = build_artifact_store()
    if not hasattr(store, "list_dataset_versions_by_role"):
        return HoldoutVerdict(warnings=["registry does not support dataset roles — holdout NOT checked"])
    try:
        return await check_holdout(
            refs, store=store, workspace_id=workspace_id, near_dup=near_dup, cutoff=cutoff)
    except Exception as exc:  # noqa: BLE001 — the door must never 500 a launch
        logger.warning("holdout check failed", exc_info=True)
        return HoldoutVerdict(warnings=[f"holdout check failed ({type(exc).__name__}: {exc})"])


__all__ = [
    "DEFAULT_NEAR_DUP_THRESHOLD",
    "DEFAULT_NGRAM",
    "HoldoutVerdict",
    "Overlap",
    "VersionRef",
    "MINHASH_PERM",
    "MINHASH_SEED",
    "canonical_row_hash",
    "check_holdout",
    "check_holdout_for_inputs",
    "dataset_refs_in",
    "row_shingles",
    "row_text",
]
