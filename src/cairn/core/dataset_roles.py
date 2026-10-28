"""Dataset ROLES, declared schemas and column mappings (ADR-0102 D1, #2020).

The registry (#1466) has named, hash-pinned, append-only versions, but until
now no notion of what a version is **for**. Eval sets were pipeline-specific
splits on disk, there was no golden-set object anywhere, and nothing could tell
a training corpus from a holdout — so nothing could refuse a training run that
overlapped one (the holdout door lives in :mod:`cairn.core.holdout`).

This module is the vocabulary, and the one place that validates it:

``role``
    ``training`` (the default — every version that predates this field is a
    training version, which is what they all were) · ``validation`` · ``test``
    · ``golden``. ``golden`` is the benchmark a JUDGE is measured against
    (ADR-0102 D3), so it is **human-rated only**: registering a golden version
    whose rows carry no human rating is refused, here, at the write.

``schema``
    the declared shape of a row — ``chat``, ``prompt-completion``,
    ``preference-pairs``, ``kto``, ``text``. Nullable: an unlabelled version is
    honest about being unlabelled, and a wrong declaration is worse than none.

``column_mapping``
    ``{declared field -> the column in THIS dataset}``, e.g.
    ``{"prompt": "question", "completion": "gold"}``. JSON, nullable, flat
    string→string: a mapping is a rename table, not a transform.

The role is metadata about a version's PURPOSE, not about its bytes — so it is
settable on an existing version (:meth:`ArtifactStore.set_dataset_version_meta`)
without minting a new one. The bytes stay frozen; what you call them does not.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

logger = logging.getLogger("cairn.dataset_roles")

#: Every role a registry version may carry. Order is the training lifecycle.
ROLES: tuple[str, ...] = ("training", "validation", "test", "golden")

#: The role a version has when nobody declared one — including every row
#: written before #2020. Existing datasets keep working, as training data.
DEFAULT_ROLE = "training"

#: Roles that make a version a HOLDOUT: a training version may not intersect
#: one (ADR-0102 D3, the holdout door). ``validation`` is deliberately absent —
#: it is carved from the same corpus by a split and is monitored, not held out.
HOLDOUT_ROLES: tuple[str, ...] = ("golden", "test")

#: The role that requires human ratings on its rows.
GOLDEN_ROLE = "golden"

#: Declared row shapes (ADR-0102 D1). Nullable — ``None`` means "not declared".
SCHEMAS: tuple[str, ...] = ("chat", "prompt-completion", "preference-pairs", "kto", "text")

#: Where a version's rows CAME FROM (#2033, ADR-0102 D1). Nullable — a version
#: registered before this field, or by a producer that did not say, has no
#: origin and is unconstrained. Closed, because two of the values carry rules:
#: ``synthetic`` may never be trained on, and ``mined`` rows reference a capture
#: trace rather than copying it.
ORIGINS: tuple[str, ...] = ("import", "split", "mined", "synthetic")

#: Rows a model GENERATED. Fit for measuring a model, never for teaching one:
#: training on them is training on the generator's own beliefs, so the loop
#: closes on itself and the eval set that was supposed to be independent becomes
#: the training set. Refused at the role-assignment seam below.
SYNTHETIC_ORIGIN = "synthetic"

#: Rows derived from real production signal — a thumbs-down, a rejection, an RCA
#: failure label — that REFERENCE the capture trace they came from.
MINED_ORIGIN = "mined"

#: Roles an origin may never carry, and WHY — the reason travels with the rule
#: because the refusal has to name it (a caller told only "no" re-registers the
#: same rows under a different name). Both entries forbid ``training`` for
#: different reasons, which is exactly why this is a table and not an ``if``.
ROLES_FORBIDDEN_BY_ORIGIN: dict[str, tuple[tuple[str, ...], str]] = {
    SYNTHETIC_ORIGIN: (
        ("training",),
        "synthetic rows are a model's own output, so training on them trains the model "
        "on its own beliefs and the eval set stops being independent",
    ),
    MINED_ORIGIN: (
        ("training",),
        "mined rows are a regression suite of known failures, so training on them teaches "
        "the model this exact list rather than the behaviour behind it — and the set that "
        "was measuring the fix becomes part of the fix",
    ),
}


class DatasetRoleError(ValueError):
    """A role / schema / column-mapping declaration the registry refuses.

    A ``ValueError`` so every existing caller of ``register_dataset_version``
    (the leg executor's ``register:`` hook, the API, the CLI) turns it into the
    same 4xx it already turns a bad ref into — never a 500.
    """


def normalize_role(role: Optional[str], *, default: Optional[str] = DEFAULT_ROLE) -> Optional[str]:
    """Validate a role, or return ``default`` when none was declared.

    Case- and whitespace-insensitive; an unknown value raises rather than
    silently becoming ``training`` — a typo'd ``"golden "`` that quietly
    registered as training data is exactly the failure this field exists to
    prevent.
    """
    if role is None:
        return default
    value = str(role).strip().lower()
    if not value:
        return default
    if value not in ROLES:
        raise DatasetRoleError(
            f"unknown dataset role {role!r} — must be one of {', '.join(ROLES)}")
    return value


def normalize_schema(schema: Optional[str]) -> Optional[str]:
    """Validate a declared row schema, or ``None`` when none was declared."""
    if schema is None:
        return None
    value = str(schema).strip().lower()
    if not value:
        return None
    if value not in SCHEMAS:
        raise DatasetRoleError(
            f"unknown dataset schema {schema!r} — must be one of {', '.join(SCHEMAS)}")
    return value


def normalize_origin(origin: Optional[str]) -> Optional[str]:
    """Validate a version's declared ORIGIN, or ``None`` when none was declared.

    Same discipline as :func:`normalize_role`: an unknown value raises rather
    than becoming ``None``, because a typo'd ``"synthetc"`` would slip past the
    never-trainable rule silently — which is the one thing this field exists for.
    """
    if origin is None:
        return None
    value = str(origin).strip().lower()
    if not value:
        return None
    if value not in ORIGINS:
        raise DatasetRoleError(
            f"unknown dataset origin {origin!r} — must be one of {', '.join(ORIGINS)}")
    return value


def assert_role_allowed_for_origin(role: Optional[str], origin: Optional[str]) -> None:
    """THE hard rule (#2033, ADR-0102 D1): an EVAL version is never trainable.

    Two origins are covered, for two different reasons
    (:data:`ROLES_FORBIDDEN_BY_ORIGIN`): a ``synthetic`` version is a model's own
    output, and a ``mined`` one is a regression suite of known failures. Both
    stop being able to measure anything the moment they are trained on. An
    ``import`` or ``split`` version — the common case — is untouched, as is a
    version with no declared origin.

    Called from every role-assignment seam in core — registration and
    re-declaration, on every store backend — so there is one function to call
    and no second place to forget (the #1376 lesson: a chokepoint is a FUNCTION
    others call, never a rule others re-implement).

    Deliberately not a warning and not a policy knob. The refusal names both the
    reason and the alternative, because a caller told only "no" registers the
    same rows again under a name this rule does not catch.
    """
    forbidden = ROLES_FORBIDDEN_BY_ORIGIN.get(origin or "")
    if forbidden is None:
        return None
    roles, why = forbidden
    if (role or DEFAULT_ROLE) in roles:
        raise DatasetRoleError(
            f"a {origin} dataset version cannot carry role {role!r}: {why}. "
            "Register it as role='test' (or 'golden' once a human has rated every row) "
            "instead.")
    return None


def normalize_dated_at(value: Optional[Any]) -> Optional[str]:
    """Validate a version's or a row's ``dated_at``, or ``None``.

    Delegates to :func:`cairn.core.eval_cutoff.normalize_dated_at` so the
    registry and the cutoff filter cannot disagree about what a date IS; the
    re-export is here because ``dataset_roles`` is what the stores import.
    """
    from cairn.core.eval_cutoff import CutoffError, normalize_dated_at as _norm  # noqa: PLC0415 — cycle: eval_cutoff imports nothing from here, but the stores import only this module

    try:
        return _norm(value)
    except CutoffError as exc:
        raise DatasetRoleError(str(exc)) from None


def normalize_origin_detail(detail: Optional[Any]) -> Optional[dict]:
    """Validate the free-form provenance blob that rides with an origin.

    An object or nothing. Unlike ``column_mapping`` its VALUES are open (a mined
    version carries lists of run and trace ids; a synthetic one carries its
    generator and seed), so only the envelope is checked.
    """
    if detail is None:
        return None
    if isinstance(detail, str):
        text = detail.strip()
        if not text:
            return None
        try:
            detail = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DatasetRoleError(f"origin_detail is not valid JSON: {exc}") from None
    if not isinstance(detail, dict):
        raise DatasetRoleError("origin_detail must be an object")
    return detail or None


def normalize_column_mapping(mapping: Optional[Any]) -> Optional[dict[str, str]]:
    """Validate a ``{declared field -> dataset column}`` map, or ``None``.

    Flat and string→string by construction: a mapping renames columns, it does
    not compute them. A nested value would be a transform nobody declared and
    no consumer would run.
    """
    if mapping is None:
        return None
    if isinstance(mapping, str):
        text = mapping.strip()
        if not text:
            return None
        try:
            mapping = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DatasetRoleError(f"column_mapping is not valid JSON: {exc}") from None
    if not isinstance(mapping, dict):
        raise DatasetRoleError("column_mapping must be an object of {field: column}")
    out: dict[str, str] = {}
    for key, value in mapping.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise DatasetRoleError(
                "column_mapping must map strings to strings "
                f"(got {type(key).__name__} -> {type(value).__name__})")
        k, v = key.strip(), value.strip()
        if not k or not v:
            raise DatasetRoleError("column_mapping keys and values must be non-empty")
        out[k] = v
    return out or None


# ─── row identity ────────────────────────────────────────────────────────────


def row_id(row: dict, index: int) -> str:
    """A stable, human-quotable id for one dataset row.

    The first of ``id`` / ``row_id`` / ``_id`` / ``uid`` the row actually
    carries; otherwise its 0-based position as ``row-<n>``. The holdout door
    quotes these back ("this row is in your golden set"), so they must mean
    something to whoever reads the refusal — a bare hash does not.
    """
    for key in ("id", "row_id", "_id", "uid"):
        value = row.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return f"row-{index}"


# ─── reading a version's rows, control-side ──────────────────────────────────


def _local_path(uri: str) -> Optional[str]:
    """The filesystem path behind ``uri``, or ``None`` when it isn't local."""
    if not uri:
        return None
    if "://" not in uri:
        return uri
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return parsed.path or None
    return None


def _iter_jsonl(path: str) -> Iterable[dict]:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj


def read_rows(uri: str, *, limit: Optional[int] = None) -> list[dict]:
    """Read a dataset version's JSONL rows CONTROL-SIDE, by URI.

    Supports a local path / ``file://`` (a file, or a directory of ``.jsonl``
    shards read in sorted order so a sharded export is deterministic) and
    ``s3://`` through boto3 (the ``kernel`` extra; without it the read raises
    ``ImportError``), which is already how the control plane fetches an export
    back (``cairn.core.dataflow.capture``).

    Raises on anything it cannot read. The holdout door needs the rows to make
    its promise, so "could not read" has to be an outcome the caller decides
    about — never an empty list that reads as "no overlap".
    """
    path = _local_path(uri)
    if path is not None:
        if os.path.isdir(path):
            rows: list[dict] = []
            for name in sorted(os.listdir(path)):
                if name.endswith(".jsonl"):
                    rows.extend(_iter_jsonl(os.path.join(path, name)))
                    if limit is not None and len(rows) >= limit:
                        return rows[:limit]
            return rows
        rows = list(_iter_jsonl(path))
        return rows[:limit] if limit is not None else rows
    if uri.startswith("s3://"):
        return _read_s3_rows(uri, limit=limit)
    raise ValueError(f"cannot read dataset rows from {uri!r} — unsupported scheme")


def _read_s3_rows(uri: str, *, limit: Optional[int] = None) -> list[dict]:
    import boto3  # noqa: PLC0415 — the kernel extra, deferred so a local-only door never builds a client

    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 uri {uri!r}")
    client = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION") or None)
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    rows: list[dict] = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            rows.append(obj)
        if limit is not None and len(rows) >= limit:
            break
    return rows


# ─── the golden guard ────────────────────────────────────────────────────────


async def assert_golden_rows_rated(name: str, uri: str, *, workspace_id: str) -> int:
    """Refuse a ``golden`` registration whose rows carry no human rating.

    A golden set exists to measure a JUDGE (ADR-0102 D3) — its authority comes
    entirely from a human having ruled on each row. A golden set assembled from
    judge output would measure the judge against itself, so this is a hard
    write-time refusal, not a warning.

    Ground truth is :mod:`cairn.core.storage.judge_labels`, whose labels are
    keyed by ``(dataset_ref, row_id)`` with ``dataset_ref = "<name>@<version>"``.
    The version being registered does not exist yet at check time, so coverage
    is matched on the dataset NAME across every version of it — a human ruling
    on ``facts-eval@2`` row 7 is a ruling on that row, not on that version
    number (the same reasoning ``JudgeLabel.key`` gives for not keying labels
    by judge version).

    Fails closed: unreadable rows, an unconfigured label store, or a single
    unrated row all refuse. Returns the number of rated rows on success.
    """
    from cairn.core.storage.judge_labels import build_judge_label_store  # noqa: PLC0415 — cycle: judge_labels imports storage helpers

    try:
        rows = read_rows(uri)
    except Exception as exc:  # noqa: BLE001 — every read failure is the same refusal
        raise DatasetRoleError(
            f"cannot register {name!r} as a golden set: its rows are unreadable at {uri!r} "
            f"({type(exc).__name__}: {exc}). A golden set is human-rated by definition, so "
            "cairn will not take the rating on trust."
        ) from None
    if not rows:
        raise DatasetRoleError(
            f"cannot register {name!r} as a golden set: {uri!r} has no rows to be rated.")

    store = build_judge_label_store()
    try:
        labels = await store.list_labels(workspace_id=workspace_id, limit=100_000)
    finally:
        closer = getattr(store, "close", None)
        if callable(closer):
            try:
                await closer()
            except Exception:  # noqa: BLE001 — best-effort cleanup, never fatal
                logger.debug("judge-label store close failed", exc_info=True)

    prefix = f"{name}@"
    rated = {
        label.row_id for label in labels
        if str(label.human_label or "").strip()
        and str(label.dataset_ref or "").startswith(prefix)
    }
    missing = [row_id(row, i) for i, row in enumerate(rows) if row_id(row, i) not in rated]
    if missing:
        shown = ", ".join(missing[:10])
        more = f" (+{len(missing) - 10} more)" if len(missing) > 10 else ""
        raise DatasetRoleError(
            f"cannot register {name!r} as a golden set: {len(missing)} of {len(rows)} rows "
            f"carry no human rating — {shown}{more}. Label them in Decisions (they land in "
            "judge_labels keyed by this dataset's rows), or register this version as "
            "role='test' instead."
        )
    return len(rows)


__all__ = [
    "DEFAULT_ROLE",
    "GOLDEN_ROLE",
    "HOLDOUT_ROLES",
    "MINED_ORIGIN",
    "ORIGINS",
    "ROLES",
    "ROLES_FORBIDDEN_BY_ORIGIN",
    "SCHEMAS",
    "SYNTHETIC_ORIGIN",
    "DatasetRoleError",
    "assert_golden_rows_rated",
    "assert_role_allowed_for_origin",
    "normalize_column_mapping",
    "normalize_dated_at",
    "normalize_origin",
    "normalize_origin_detail",
    "normalize_role",
    "normalize_schema",
    "read_rows",
    "row_id",
]
