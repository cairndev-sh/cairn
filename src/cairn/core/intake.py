"""The intake verdict (ADR-0102 D1, #2021) — what admission decided about a
dataset version, and the door that reads it.

T1 (#2020) gave a registry version a ``role``; this module gives it a
**verdict**. The data-readiness checks already existed as pipeline stages a
user had to know to run, and nothing wrote a piece of state a launch door could
read. Together's two-phase intake is the documented pattern (research A §4.1):
a status enum plus a structured ``validation_report``, both on the record.

Four values, and the door treats them differently:

``admitted``
    every check that ran came back clean.
``degraded``
    something was found and remediated, or found and tolerated. **The run
    still starts**, with the reasons attached: on an agentic run's ``Run.intake``
    (``dispatcher.run_template``), and on a pipeline's ``PipelineRecord.admission``
    — the per-launch decision the admission door writes (#2190) and the first
    human gate shows. A warning a human can see next to the result, not a refusal.
``blocked``
    a declared ``block``-severity expectation failed, or over-length rows were
    present under the ``refuse`` policy. The launch door refuses, naming the
    reasons.
``invalid_format``
    the bytes are not what they claim to be — nothing survived the read, or
    lines had to be quarantined. Refused too, with the report.

Two halves live here because they are one rule:

* :func:`verdict_from_flow_stats` — the AGGREGATION. It reads the flat,
  stage-prefixed stats a ``clean``-flow leg returns (``dedup.exact.dropped``,
  ``expectations.check.verdict``, ``profile.tokens.over_length_rows``, …) and
  folds them into one verdict plus a structured report. It runs control-side,
  in :func:`cairn.core.dispatch.leg_executor._register_dataset_versions`, for
  the same reason registration does: the box has no database (ADR-0081 D3).
* :func:`check_intake_for_inputs` — the DOOR. The launch-path entry point,
  shaped exactly like :func:`cairn.core.holdout.check_holdout_for_inputs` so
  the two read the same at both call sites (``preflight`` and the dispatcher).

**A version registered before this change has no verdict, and is admitted with
a note.** Retroactively blocking data that was legal when it was written would
break every deployment on upgrade, and the verdict is evidence of a check that
ran — its absence is not evidence of a check that failed.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

logger = logging.getLogger("cairn.intake")

#: Every verdict a version may carry, best to worst.
VERDICTS: tuple[str, ...] = ("admitted", "degraded", "blocked", "invalid_format")

ADMITTED = "admitted"
DEGRADED = "degraded"
BLOCKED = "blocked"
INVALID_FORMAT = "invalid_format"

#: The two the launch door refuses (ADR-0102 D1). ``degraded`` runs.
REFUSING_VERDICTS: frozenset[str] = frozenset({BLOCKED, INVALID_FORMAT})

#: What a version with no verdict reads as, and why.
NO_VERDICT_NOTE = (
    "registered before the intake verdict existed (#2021) — admitted with no "
    "report; re-run the dataset-readiness clean flow to give it one"
)

#: Stage names the aggregation reads, so the key layout is stated once.
STAGE_INGEST = "ingest.normalize"
STAGE_DEDUP_EXACT = "dedup.exact"
STAGE_DEDUP_NEAR = "dedup.near"
STAGE_DECONTAM = "decontam.apply"
STAGE_PII = "pii.redact"
STAGE_EXPECTATIONS = "expectations.check"
STAGE_PROFILE = "profile.tokens"
STAGE_EXPORT = "export.jsonl"


#: #2188 (T03, C2) — the check-evidence vocabulary: the checks whose evidence
#: this module already reads, keyed by the name a report and a policy use.
#: Core RECORDS evidence for these; which of them a version must have run is
#: owned by policy / pack data (#2190), never hardcoded here.
CHECKS: dict[str, str] = {
    "ingest": STAGE_INGEST,
    "dedup_exact": STAGE_DEDUP_EXACT,
    "dedup_near": STAGE_DEDUP_NEAR,
    "decontam": STAGE_DECONTAM,
    "pii": STAGE_PII,
    "expectations": STAGE_EXPECTATIONS,
    "profile_tokens": STAGE_PROFILE,
    "export": STAGE_EXPORT,
}
#: Check states. A stage that is not in the flow is ``skipped`` — a FACT, never
#: a failure; a flow may declare a check ``not_applicable`` (its `checks:`
#: block); a stage that ran but could not do its job (no extra, no eval set,
#: no tokenizer) is ``degraded``; a kernel error is ``failed``.
CHECK_RAN = "ran"
CHECK_SKIPPED = "skipped"
CHECK_NOT_APPLICABLE = "not_applicable"
CHECK_DEGRADED = "degraded"
CHECK_FAILED = "failed"
CHECK_STATES = (CHECK_RAN, CHECK_SKIPPED, CHECK_NOT_APPLICABLE, CHECK_DEGRADED, CHECK_FAILED)
#: The flat stat-key prefix the map travels under on a leg's stats
#: (``checks.<name>.state``), so the record's findings show it and a flow's
#: ``not_applicable`` declaration can ride the same dict into the verdict.
CHECKS_PREFIX = "checks."


def normalize_verdict(value: Optional[str]) -> Optional[str]:
    """A stored verdict string, validated; ``None`` when there is none.

    Unknown values raise rather than degrading to ``admitted``: a verdict the
    door cannot read must not silently become the permissive one.
    """
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text not in VERDICTS:
        raise ValueError(f"unknown intake verdict {value!r} — must be one of {', '.join(VERDICTS)}")
    return text


# ─── the aggregation ─────────────────────────────────────────────────────────


@dataclass
class IntakeVerdict:
    """One version's admission decision plus the report behind it."""

    verdict: str = ADMITTED
    reasons: list[str] = field(default_factory=list)
    report: dict[str, Any] = field(default_factory=dict)

    @property
    def refused(self) -> bool:
        return self.verdict in REFUSING_VERDICTS

    def as_record(self) -> dict[str, Any]:
        return {"verdict": self.verdict, "reasons": list(self.reasons), "report": dict(self.report)}


def _stage(stats: dict[str, Any], stage: str) -> dict[str, Any]:
    """The ``<stage>.<key>`` slice of a flow's flat stats, unprefixed.

    The dataflow runner flattens every stage's return into one dict keyed
    ``"<stage>.<key>"`` (``cairn.core.dataflow.runner``). Reading it back
    here rather than asking each kernel to agree on a shared blob keeps the
    boundary rule intact: only scalars ever crossed it.
    """
    prefix = f"{stage}."
    return {k[len(prefix):]: v for k, v in (stats or {}).items() if k.startswith(prefix)}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _histogram(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """The ``bucket_<lo>_<hi> = count`` scalars back as ordered buckets.

    Flat scalars are what may cross the dataflow boundary (ADR-0080 R5), so the
    kernel emits the histogram spread across keys and it is reassembled here —
    the same shape ``dataset/curate``'s score distribution already uses.
    """
    out: list[dict[str, Any]] = []
    for key, value in profile.items():
        if not key.startswith("bucket_"):
            continue
        lo, _, hi = key[len("bucket_"):].partition("_")
        try:
            low = int(lo)
        except ValueError:
            continue
        high: Optional[int] = None
        if hi and hi != "plus":
            try:
                high = int(hi)
            except ValueError:
                high = None
        out.append({"from": low, "to": high, "count": _int(value)})
    return sorted(out, key=lambda b: b["from"])


def _failures(raw: Any) -> list[dict[str, Any]]:
    """The expectation stage's failures, carried across the boundary as a JSON
    STRING (a scalar) and parsed back here."""
    if not raw:
        return []
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    return [f for f in parsed if isinstance(f, dict)] if isinstance(parsed, list) else []


def _declared_not_applicable(stats: dict[str, Any]) -> set[str]:
    """Checks the flow declared ``not_applicable`` — carried on the stats as
    ``checks.<name>.state`` by the leg executor (from ``FlowDef.checks``)."""
    out: set[str] = set()
    for k, v in stats.items():
        if k.startswith(CHECKS_PREFIX) and k.endswith(".state") and v == CHECK_NOT_APPLICABLE:
            out.add(k[len(CHECKS_PREFIX):-len(".state")])
    return out


def _check_entry(name: str, stage: dict[str, Any]) -> dict[str, Any]:
    """One check's evidence from its stage stats: state, backend, counts,
    kernel version. The rule is the same one the notes above apply."""
    entry: dict[str, Any] = {"state": CHECK_RAN}
    if stage.get("error"):
        entry["state"] = CHECK_FAILED
        entry["error"] = str(stage.get("error"))
    elif "checked" in stage and not bool(stage.get("checked")):
        entry["state"] = CHECK_DEGRADED       # ran, could not do its job (no extra / no eval set)
        if stage.get("note"):
            entry["note"] = str(stage.get("note"))
    elif name == "profile_tokens" and str(stage.get("token_backend") or "") == "whitespace":
        entry["state"] = CHECK_DEGRADED
        entry["note"] = str(stage.get("token_backend_note") or "whitespace word counts, not subword tokens")
    elif name == "pii" and str(stage.get("pii_backend") or "") == "none":
        entry["state"] = CHECK_SKIPPED        # declined by the flow — a fact, recorded as such
        entry["note"] = "redaction declined (backend: none)"
    backend = stage.get("backend") or stage.get("pii_backend") or stage.get("token_backend") or stage.get("method")
    if backend:
        entry["backend"] = str(backend)
    for key in ("rows", "dropped", "dropped_exact", "dropped_near", "redacted_rows",
                "over_length_rows", "over_length_policy", "total", "passed", "failed", "verdict",
                "eval_sets", "tokens_total"):
        if key in stage:
            entry[key] = stage[key]
    if stage.get("kernel_version"):
        entry["kernel_version"] = str(stage.get("kernel_version"))
    return entry


def check_map_from_flow_stats(stats: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """#2188 (T03, C2) — the check-evidence map: one entry per known check.

    ``ran`` when the stage reported; ``degraded`` when it reported it could
    not do its job; ``failed`` on a kernel error; ``not_applicable`` when the
    flow declared it so and the stage did not run; ``skipped`` otherwise — a
    kernel absent from the flow is now a visible fact, not an empty report.
    Which of these a version MUST have run is a policy question, not this
    function's (the special note on #2188).
    """
    declared = _declared_not_applicable(stats)
    out: dict[str, dict[str, Any]] = {}
    for name, stage_id in CHECKS.items():
        stage = _stage(stats, stage_id)
        if stage:
            out[name] = _check_entry(name, stage)
        elif name in declared:
            out[name] = {"state": CHECK_NOT_APPLICABLE}
        else:
            out[name] = {"state": CHECK_SKIPPED}
    for name in sorted(declared - set(CHECKS)):
        # A declaration this vocabulary does not know is recorded, not dropped:
        # a typo in a flow's `checks:` block must be visible on the record.
        out[name] = {"state": CHECK_NOT_APPLICABLE, "note": "declared for a check this vocabulary does not know"}
    return out


def flatten_check_map(checks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The map as flat ``checks.<name>.<field>`` stats — the shape a leg's
    findings carry (``findings.checks_dedup_near_state``)."""
    flat: dict[str, Any] = {}
    for name, entry in checks.items():
        for field_name, value in entry.items():
            flat[f"{CHECKS_PREFIX}{name}.{field_name}"] = value
    return flat


def verdict_from_flow_stats(stats: dict[str, Any]) -> IntakeVerdict:
    """Fold a ``clean``-flow leg's stats into the version's intake verdict.

    The rule, in order — the first tier that fires decides, and every tier
    contributes its reasons:

    ``invalid_format``
        no rows survived the read, or the reader had to quarantine malformed
        lines. Neither is a data-quality opinion; the bytes are not what they
        claimed to be.
    ``blocked``
        a ``block``-severity expectation failed, or over-length rows exist and
        the flow declared ``over_length: refuse``.
    ``degraded``
        a ``warn``-severity expectation failed · contamination against a
        holdout was found and removed · over-length rows were counted or
        dropped. Each is a finding a human should see beside the run; none of
        them is a reason to refuse compute.
    ``admitted``
        otherwise.

    Facts about checks that did NOT run (no eval set, no ``[minhash]`` extra,
    no tokenizer) land in ``report['notes']`` and never in ``reasons``: a
    missing optional dependency is the deployment's business, not the dataset's
    fault, and degrading every version on a lean install would make the state
    meaningless.
    """
    ingest = _stage(stats, STAGE_INGEST)
    exact = _stage(stats, STAGE_DEDUP_EXACT)
    near = _stage(stats, STAGE_DEDUP_NEAR)
    decontam = _stage(stats, STAGE_DECONTAM)
    pii = _stage(stats, STAGE_PII)
    expect = _stage(stats, STAGE_EXPECTATIONS)
    profile = _stage(stats, STAGE_PROFILE)
    export = _stage(stats, STAGE_EXPORT)

    notes: list[str] = []
    rows_in = _int(ingest.get("rows"), -1)
    rows_out = _int(export.get("rows"), _int(profile.get("rows"), -1))

    near_checked = bool(near.get("checked", False)) if near else False
    if near and not near_checked:
        notes.append(str(near.get("note") or "near-duplicate dedup did not run"))
    decontam_checked = bool(decontam.get("checked", False)) if decontam else False
    if decontam and not decontam_checked:
        notes.append("no golden or test version to decontaminate against — contamination NOT checked")
    token_backend = str(profile.get("token_backend") or "")
    if profile and token_backend == "whitespace":
        notes.append(str(profile.get("token_backend_note")
                         or "no tokenizer available — token counts are whitespace WORD counts, "
                            "not the base model's subword tokens"))
    if str(pii.get("pii_backend") or "") == "none":
        notes.append("PII redaction was explicitly declined for this corpus (backend: none)")

    over_rows = _int(profile.get("over_length_rows"))
    over_policy = str(profile.get("over_length_policy") or "count")
    tokens_total = _int(profile.get("tokens_total"))

    report: dict[str, Any] = {
        "rows_in": rows_in if rows_in >= 0 else None,
        "rows_out": rows_out if rows_out >= 0 else None,
        "dedup": {
            "exact": _int(exact.get("dropped_exact", exact.get("dropped"))),
            "near": _int(near.get("dropped_near", near.get("dropped"))),
            "near_checked": near_checked,
        },
        "contamination": {
            "checked": decontam_checked,
            "dropped": _int(decontam.get("dropped")),
            "method": str(decontam.get("method") or ""),
            "eval_sets": [s for s in str(decontam.get("eval_sets") or "").split(",") if s],
        },
        "pii": {
            "redacted_rows": _int(pii.get("redacted_rows")),
            "backend": str(pii.get("pii_backend") or ""),
        },
        "expectations": {
            "suite": str(expect.get("suite") or ""),
            "verdict": str(expect.get("verdict") or ""),
            "total": _int(expect.get("total")),
            "passed": _int(expect.get("passed")),
            "failed": _int(expect.get("failed")),
            "failures": _failures(expect.get("failures_json")),
        },
        "profile": {
            "tokens_total": tokens_total,
            "p50": _int(profile.get("tokens_p50")),
            "p95": _int(profile.get("tokens_p95")),
            "max": _int(profile.get("tokens_max")),
            "backend": token_backend,
            "tokenizer": str(profile.get("tokenizer") or ""),
            "max_seq_len": _int(profile.get("max_seq_len")),
            "over_length_rows": over_rows,
            "over_length_policy": over_policy,
            "histogram": _histogram(profile),
        },
        # ADR-0102 D1 "fold the token total into the verdict card" — hoisted to
        # the top level because it is the one number the cost estimate reads.
        "tokens_total": tokens_total,
        "quarantined_rows": _int(ingest.get("quarantined_rows"))
        or _int(profile.get("quarantined_rows")),
        "notes": notes,
        # #2188 (T03, C2) — what was checked, what was not, and by which kernel.
        "checks": check_map_from_flow_stats(stats),
    }

    invalid: list[str] = []
    if rows_out == 0 or (rows_out < 0 and rows_in == 0):
        invalid.append("no rows survived intake — the source parsed to an empty dataset")
    if report["quarantined_rows"]:
        invalid.append(
            f"{report['quarantined_rows']} line(s) were not valid JSON objects and had to be "
            "quarantined — the file is not the format it was read as")
    if invalid:
        return IntakeVerdict(verdict=INVALID_FORMAT, reasons=invalid, report=report)

    blocking: list[str] = []
    if str(expect.get("verdict") or "") == "block":
        for f in report["expectations"]["failures"]:
            if str(f.get("severity") or "block") == "block":
                blocking.append(
                    f"expectation {f.get('type')}"
                    + (f" on '{f.get('column')}'" if f.get("column") else "")
                    + f" failed: {f.get('detail') or f.get('observed') or 'no detail'}")
        if not blocking:  # a block verdict with no itemised failure is still a block
            blocking.append(
                f"the '{report['expectations']['suite'] or 'data-quality'}' expectation suite "
                "returned block")
    if over_rows and over_policy == "refuse":
        blocking.append(
            f"{over_rows} row(s) exceed max_seq_len={report['profile']['max_seq_len']} and this "
            "flow declared over_length: refuse — nothing was truncated")
    if blocking:
        return IntakeVerdict(verdict=BLOCKED, reasons=blocking, report=report)

    degrading: list[str] = []
    if str(expect.get("verdict") or "") == "degrade":
        for f in report["expectations"]["failures"]:
            degrading.append(
                f"expectation {f.get('type')}"
                + (f" on '{f.get('column')}'" if f.get("column") else "")
                + f" failed (warn): {f.get('detail') or f.get('observed') or 'no detail'}")
        if not degrading:
            degrading.append(
                f"the '{report['expectations']['suite'] or 'data-quality'}' expectation suite "
                "returned degrade")
    if decontam_checked and report["contamination"]["dropped"]:
        degrading.append(
            f"{report['contamination']['dropped']} row(s) overlapped a golden or test version "
            "and were removed by decontamination")
    if over_rows:
        how = "dropped" if over_policy == "drop" else "kept and counted"
        degrading.append(
            f"{over_rows} row(s) exceed max_seq_len={report['profile']['max_seq_len']} "
            f"({how}; never truncated)")
    if degrading:
        return IntakeVerdict(verdict=DEGRADED, reasons=degrading, report=report)
    return IntakeVerdict(verdict=ADMITTED, reasons=[], report=report)


# ─── defaulting the decontamination probe to the workspace's holdouts ────────


async def holdout_eval_uris(store: Any, *, workspace_id: str) -> list[str]:
    """Every ``golden`` and ``test`` version's URI in ``workspace_id``.

    ADR-0102 D1: ``decontam.apply``'s ``eval_path`` defaults to "every golden
    and test version in the workspace" rather than the one path a pipeline
    author happened to pass. Reads through the SAME store method the holdout
    door uses (:meth:`ArtifactStore.list_dataset_versions_by_role`), which is
    workspace-scoped at the store — a tenant's probe never sees another
    tenant's rows.

    Never raises: an unconfigured or unhappy registry yields no probes, and the
    stage reports ``checked: false`` honestly rather than failing the flow.
    """
    from cairn.core.dataset_roles import HOLDOUT_ROLES  # noqa: PLC0415 — off the hot import path

    lister = getattr(store, "list_dataset_versions_by_role", None)
    if lister is None:
        return []
    try:
        rows = await lister(HOLDOUT_ROLES, workspace_id=workspace_id)
    except Exception:  # noqa: BLE001 — a registry hiccup is "no probe", never a failed leg
        logger.debug("could not list holdout versions for the decontamination probe", exc_info=True)
        return []
    seen: list[str] = []
    for row in rows or []:
        uri = str((row or {}).get("uri") or "")
        if uri and uri not in seen:
            seen.append(uri)
    return seen


# ─── the launch door ─────────────────────────────────────────────────────────


@dataclass
class VersionVerdict:
    """One dataset version the door looked at."""

    ref: str
    verdict: str = ADMITTED
    reasons: list[str] = field(default_factory=list)
    note: str = ""

    def as_record(self) -> dict[str, Any]:
        return {"ref": self.ref, "verdict": self.verdict, "reasons": list(self.reasons),
                **({"note": self.note} if self.note else {})}

    def reason_line(self) -> str:
        detail = "; ".join(self.reasons) if self.reasons else "no reason recorded"
        return f"{self.ref} is {self.verdict}: {detail}"


@dataclass
class IntakeGateVerdict:
    """What the door found across every ``dataset:`` ref a launch carries."""

    ok: bool = True
    #: False when nothing was checkable at all (no refs, no registry).
    checked: bool = False
    versions: list[VersionVerdict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def refused(self) -> list[VersionVerdict]:
        return [v for v in self.versions if v.verdict in REFUSING_VERDICTS]

    @property
    def degraded(self) -> list[VersionVerdict]:
        return [v for v in self.versions if v.verdict == DEGRADED]

    def reasons(self) -> list[str]:
        """Why the launch is refused — empty when it is not."""
        return [v.reason_line() for v in self.refused]

    def degraded_reasons(self) -> list[str]:
        """Findings that ride along on the run record without refusing it."""
        return [v.reason_line() for v in self.degraded]

    def as_record(self) -> dict[str, Any]:
        return {"checked": self.checked, "ok": self.ok,
                "versions": [v.as_record() for v in self.versions],
                "warnings": self.warnings}


async def check_intake(
    refs: Sequence[str], *, store: Any, workspace_id: str,
) -> IntakeGateVerdict:
    """Read the stored verdict of every ``dataset:<name>[@<v>]`` ref in ``refs``.

    Never raises — a registry that cannot answer yields ``checked=False`` and a
    warning, the same posture the holdout door and the config preflight take on
    the launch path. Refusal is only ever on a verdict that was actually
    written.
    """
    gate = IntakeGateVerdict()
    if not refs:
        return gate

    from cairn.core.storage.artifacts import parse_dataset_ref  # noqa: PLC0415 — cycle: artifacts imports core settings

    for ref in refs:
        try:
            parsed = parse_dataset_ref(ref)
        except ValueError as exc:
            gate.warnings.append(f"{ref}: {exc}")
            continue
        if parsed is None:
            continue
        name, version = parsed
        try:
            row = await store.get_dataset_version_meta(name, version, workspace_id=workspace_id)
        except Exception as exc:  # noqa: BLE001 — a registry hiccup is "not checked", never a 500
            gate.warnings.append(f"{ref}: registry lookup failed ({type(exc).__name__}: {exc})")
            continue
        if row is None:
            gate.warnings.append(f"{ref}: not registered — no intake verdict to read")
            continue
        if (row.get("workspace_id") or "") not in ("", workspace_id):
            # Not this workspace's data; the launch will fail to resolve it
            # anyway, and reading it here would be a cross-tenant read.
            continue
        gate.checked = True
        pinned = f"dataset:{name}@{int(row.get('version') or 0)}"
        try:
            verdict = normalize_verdict(row.get("verdict"))
        except ValueError as exc:
            # An unreadable verdict is a warning, not a refusal and not an
            # admission: say so and leave the version unjudged.
            gate.warnings.append(f"{pinned}: {exc}")
            continue
        if verdict is None:
            gate.versions.append(VersionVerdict(ref=pinned, verdict=ADMITTED, note=NO_VERDICT_NOTE))
            continue
        reasons = row.get("verdict_reasons") or []
        if isinstance(reasons, str):
            try:
                reasons = json.loads(reasons)
            except ValueError:
                reasons = [reasons]
        gate.versions.append(VersionVerdict(
            ref=pinned, verdict=verdict,
            reasons=[str(r) for r in reasons] if isinstance(reasons, list) else []))
    gate.ok = not gate.refused
    return gate


async def check_intake_for_inputs(
    inputs: dict[str, Any], *, workspace_id: str, store: Any = None,
) -> IntakeGateVerdict:
    """The launch-path entry point: pull the ``dataset:`` refs out of a run's
    inputs and read each one's intake verdict.

    Shares :func:`cairn.core.holdout.dataset_refs_in` on purpose — the two
    doors must see exactly the same refs, or one of them is judging data the
    run does not read (or missing data it does).
    """
    from cairn.core.holdout import dataset_refs_in  # noqa: PLC0415 — off the hot import path

    refs = dataset_refs_in(inputs)
    if not refs:
        return IntakeGateVerdict()
    if store is None:
        from cairn.core.storage.artifacts import build_artifact_store  # noqa: PLC0415 — off the hot import path

        store = build_artifact_store()
    if not hasattr(store, "get_dataset_version_meta"):
        return IntakeGateVerdict(
            warnings=["registry does not support dataset version meta — intake NOT checked"])
    try:
        return await check_intake(refs, store=store, workspace_id=workspace_id)
    except Exception as exc:  # noqa: BLE001 — the door must never 500 a launch
        logger.warning("intake check failed", exc_info=True)
        return IntakeGateVerdict(warnings=[f"intake check failed ({type(exc).__name__}: {exc})"])


__all__ = [
    "ADMITTED",
    "BLOCKED",
    "DEGRADED",
    "INVALID_FORMAT",
    "NO_VERDICT_NOTE",
    "REFUSING_VERDICTS",
    "VERDICTS",
    "IntakeGateVerdict",
    "IntakeVerdict",
    "VersionVerdict",
    "check_intake",
    "check_intake_for_inputs",
    "holdout_eval_uris",
    "normalize_verdict",
    "verdict_from_flow_stats",
]
