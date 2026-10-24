"""The admission door for PIPELINES (#2190, T05 — C3, I6, I15).

Decision, not facts (16 §4): a dataset version's facts are measured once and
live on the version (verdict, report, the check map). Whether THIS launch may
train on it is decided here, once, at the launch door, before any record
exists — from the facts, the checks the applicable policies REQUIRE, and the
context (environment, workspace). The result is written on the
:class:`~cairn.core.storage.pipeline_store.PipelineRecord` as ``admission`` and
shown to the first human gate, so the decision does not evaporate at launch.

Enforcement point per the maintainer's decision on #2190: the launch door,
once, over the pipeline's declared inputs. The three run-admission doors the
agentic path already uses (`intake`, `holdout`, `licence`) are reused, never
copied; this module adds only the policy's required-check rule and the record.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from cairn.core.intake import CHECK_RAN, CHECKS, DEGRADED

ADMITTED = "admitted"
DEGRADED_ADMISSION = "degraded"
REFUSED = "refused"


class AdmissionRefusedError(ValueError):
    """The launch is refused; ``decision`` carries every reason."""

    def __init__(self, decision: "AdmissionDecision") -> None:
        self.decision = decision
        super().__init__("admission door: " + "; ".join(decision.refusing_reasons))


@dataclass
class AdmissionDecision:
    verdict: str = ADMITTED
    refusing_reasons: list[str] = field(default_factory=list)
    degrading_reasons: list[str] = field(default_factory=list)
    versions: list[dict[str, Any]] = field(default_factory=list)
    holdout: dict[str, Any] = field(default_factory=dict)
    licence: dict[str, Any] = field(default_factory=dict)
    #: whether the intake door could check anything, and what it warned about —
    #: "admitted, not checked" must be readable on the record, not silent
    intake: dict[str, Any] = field(default_factory=dict)
    #: ADR-0146 — what each input's declaration required of its version, and what it found
    expectations: list[dict[str, Any]] = field(default_factory=list)
    policy: dict[str, Any] = field(default_factory=dict)
    environment: str = ""
    workspace_id: str = ""
    decided_at: str = ""
    #: ADR-0151 D2 — the launch door's estimate for the legs on a priced box:
    #: ``{estimate_usd, legs: [{step, priced, connection, hourly_usd, hours,
    #: estimate_usd, basis}]}``; recorded once, read by the legs, never re-derived.
    #: Empty (and absent from the record) when no leg has a compute target.
    cost_estimate: dict[str, Any] = field(default_factory=dict)

    @property
    def refused(self) -> bool:
        return self.verdict == REFUSED

    def as_record(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "refusing_reasons": list(self.refusing_reasons),
            "degrading_reasons": list(self.degrading_reasons),
            "versions": [dict(v) for v in self.versions],
            "holdout": dict(self.holdout),
            "licence": dict(self.licence),
            "intake": dict(self.intake),
            "expectations": [dict(e) for e in self.expectations],
            "policy": dict(self.policy),
            "environment": self.environment,
            "workspace_id": self.workspace_id,
            "decided_at": self.decided_at,
            **({"cost_estimate": dict(self.cost_estimate)} if self.cost_estimate else {}),
        }


def applicable_admission_policies(policies: list[Any], environment: str) -> list[Any]:
    """The policies whose ``admission`` block applies to this environment —
    the same name-or-tier match the per-operation engine uses (#1766)."""
    from cairn.core.policy.engine import policy_tier  # noqa: PLC0415 — off the hot import path

    out = []
    for p in policies or []:
        adm = getattr(p, "admission", None)
        if adm is None or not getattr(adm, "required_checks", None):
            continue
        envs = list(getattr(getattr(p, "applies_to", None), "environments", None) or [])
        if envs and not (environment in envs or policy_tier(environment) in envs):
            continue
        out.append(p)
    return out


def policy_identity(policies: list[Any]) -> dict[str, Any]:
    """``policy@<digest>`` — policy versioning does not exist yet, so the
    identity is a digest of the applicable admission requirements (names +
    required checks). Two launches under the same requirements share it."""
    names = sorted(str(getattr(p, "name", "")) for p in policies)
    required = sorted({c for p in policies for c in getattr(p.admission, "required_checks", [])})
    digest = hashlib.sha256(json.dumps({"names": names, "required_checks": required}, sort_keys=True).encode()).hexdigest()[:16]
    return {"ref": f"policy@{digest}", "names": names, "required_checks": required}


def _runtime_policies() -> list[Any]:
    from cairn.core.approval.policy_source import runtime_policy_engine  # noqa: PLC0415 — off the hot import path

    engine = runtime_policy_engine()
    if engine is None:
        return []
    active = getattr(engine, "_is_active", None)
    # the same activation rule the per-operation gate applies (adopted pack
    # policies, attached store policies): admission and dispatch never diverge
    return [p for p in (getattr(engine, "policies", []) or []) if active is None or active(p)]


async def _check_expectations(inputs: dict[str, Any], expectations: dict[str, dict[str, str]],
                              store: Any, decision: AdmissionDecision, *, workspace_id: str) -> None:
    """ADR-0146 D2 — each input that declares ``role:`` / ``verdict: required`` is checked against
    the version it was given: a value that is not a ``dataset:`` ref, a ref the registry does not
    hold, a different role, or no verdict when one is required refuses the launch. Every check
    is recorded, passed or not.

    Workspace-scoped like the intake and holdout doors: another workspace's version is "not
    registered" here — the same answer as a name nobody holds, so a refusal never tells one
    tenant what another has (ADR-0051 §4) — and its role and verdict are never read."""
    from cairn.core.dataset_roles import DEFAULT_ROLE  # noqa: PLC0415 — off the hot import path
    from cairn.core.intake import normalize_verdict  # noqa: PLC0415
    from cairn.core.storage.artifacts import parse_dataset_ref  # noqa: PLC0415

    for name, want in sorted(expectations.items()):
        value = inputs.get(name)
        if value in (None, ""):
            continue  # a missing input is the input layer's refusal, not this door's
        entry: dict[str, Any] = {"input": name, "ref": str(value), **{f"expected_{k}": v for k, v in want.items()}}
        problem = ""
        try:
            parsed = parse_dataset_ref(str(value))
        except ValueError as exc:
            parsed, problem = None, f"{exc}"
        if parsed is None and not problem:
            problem = "is not a dataset: ref, so its role and verdict cannot be checked"
        meta = None
        if parsed is not None:
            try:
                meta = await store.get_dataset_version_meta(parsed[0], parsed[1], workspace_id=workspace_id)
            except Exception as exc:  # noqa: BLE001 — a registry that cannot answer refuses, never admits unchecked
                problem = f"the registry could not be read ({type(exc).__name__})"
            else:
                if meta is not None and (meta.get("workspace_id") or "") not in ("", workspace_id):
                    meta = None  # another tenant's version: never read, reported as absent
                if meta is None:
                    problem = "is not registered"
        if meta is not None:
            entry["role"] = str(meta.get("role") or DEFAULT_ROLE)
            try:
                # an unreadable verdict is no verdict, as the intake door treats it
                entry["verdict"] = normalize_verdict(meta.get("verdict")) if meta.get("verdict") else None
            except ValueError:
                entry["verdict"] = None
            if "role" in want and entry["role"] != want["role"]:
                problem = f"is a {entry['role']} version; this input takes a {want['role']} one"
            elif want.get("verdict") and not entry["verdict"]:
                problem = "has no intake verdict; this input requires one"
        entry["ok"] = not problem
        decision.expectations.append(entry)
        if problem:
            decision.refusing_reasons.append(f"input {name!r} ({value}) {problem}")


async def decide_admission(
    inputs: dict[str, Any], *, workspace_id: str, environment: str = "",
    store: Any = None, policies: Optional[list[Any]] = None,
    expectations: Optional[dict[str, dict[str, str]]] = None,
) -> AdmissionDecision:
    """Decide once, for this launch, from facts + required checks + policy + context.
    ``expectations`` (ADR-0146) are the pipeline's declared per-input role and verdict
    requirements (``engine.pipeline.dataset_expectations``)."""
    from cairn.core.holdout import check_holdout_for_inputs  # noqa: PLC0415 — off the hot import path
    from cairn.core.intake import check_intake_for_inputs  # noqa: PLC0415
    from cairn.core.model_license_gate import check_license_for_inputs  # noqa: PLC0415
    from cairn.core.storage.artifacts import build_artifact_store, parse_dataset_ref  # noqa: PLC0415

    store = store if store is not None else build_artifact_store()
    applicable = applicable_admission_policies(
        policies if policies is not None else _runtime_policies(), environment or "")
    identity = policy_identity(applicable)
    required = list(identity["required_checks"])
    unknown = [c for c in required if c not in CHECKS]

    decision = AdmissionDecision(environment=environment or "", workspace_id=workspace_id,
                                 policy=identity, decided_at=datetime.now(timezone.utc).isoformat())
    if unknown:
        decision.refusing_reasons.append(
            f"policy {identity['ref']} requires unknown check(s) {unknown} — known: {sorted(CHECKS)}")

    holdout = await check_holdout_for_inputs(inputs, workspace_id=workspace_id)
    decision.holdout = holdout.as_record() if hasattr(holdout, "as_record") else {"ok": bool(getattr(holdout, "ok", True))}
    if not getattr(holdout, "ok", True):
        decision.refusing_reasons.extend(holdout.reasons())

    intake = await check_intake_for_inputs(inputs, workspace_id=workspace_id, store=store)
    for v in intake.versions:
        entry: dict[str, Any] = {"ref": v.ref, "verdict": v.verdict, "reasons": list(v.reasons)}
        if v.note:
            entry["note"] = v.note
        parsed = parse_dataset_ref(v.ref)
        meta = None
        if parsed is not None:
            try:
                meta = await store.get_dataset_version_meta(
                    parsed[0], parsed[1], workspace_id=workspace_id)
            except Exception:  # noqa: BLE001 — a lookup failure is reported, never hidden
                meta = None
        checks = dict(((meta or {}).get("verdict_report") or {}).get("checks") or {})
        entry["artifact_id"] = str((meta or {}).get("artifact_id") or "")
        entry["checks"] = {name: {**dict(c), "required": name in required} for name, c in checks.items()}
        missing = [c for c in required if c not in unknown and str((checks.get(c) or {}).get("state") or "") != CHECK_RAN]
        entry["missing_required"] = missing
        for c in missing:
            state = str((checks.get(c) or {}).get("state") or "no evidence")
            decision.refusing_reasons.append(
                f"{v.ref}: policy {identity['ref']} requires check '{c}' to have run — it is {state}")
        decision.versions.append(entry)
    decision.refusing_reasons.extend(intake.reasons())
    decision.degrading_reasons.extend(intake.degraded_reasons())
    decision.intake = {"checked": bool(intake.checked), "warnings": list(intake.warnings)}

    if expectations:
        await _check_expectations(inputs, expectations, store, decision, workspace_id=workspace_id)

    licence = await check_license_for_inputs(inputs)
    decision.licence = licence.as_record() if hasattr(licence, "as_record") else {"ok": bool(getattr(licence, "ok", True))}
    if not getattr(licence, "ok", True):
        decision.refusing_reasons.extend(list(getattr(licence, "reasons", lambda: [])()) if callable(getattr(licence, "reasons", None)) else list(getattr(licence, "reasons", []) or []))

    # door order mirrors run_template: holdout → intake → licence (dispatcher.py:837-877)
    if decision.refusing_reasons:
        decision.verdict = REFUSED
    elif decision.degrading_reasons or any(v.verdict == DEGRADED for v in intake.versions):
        decision.verdict = DEGRADED_ADMISSION
    return decision


__all__ = ["ADMITTED", "DEGRADED_ADMISSION", "REFUSED", "AdmissionDecision", "AdmissionRefusedError",
           "applicable_admission_policies", "decide_admission", "policy_identity"]
