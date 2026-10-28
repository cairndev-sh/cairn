"""cairn preflight (ADR-0072) — the side-effect-free "can this run?" verdict.

`dry-run` today is a static render of the plan (#783): it shows the *shape* of a
run but calls nothing and returns no answer. This module turns it into a live
**preflight verdict** — the Terraform-`plan` / `kubectl --dry-run=server` analog —
by composing the checks that already exist and have **no side effects**:

  - inputs        → :func:`cairn.core.ui_render.coerce_and_validate` (would it 422?)
  - operators     → the plan's ``missing_bindings`` (build_plan / #304 doctor shape)
  - gates         → the plan's ``approvals_required`` (what pauses for a human)
  - cost          → the plan's ``estimated_cost`` (honest: runtime-tracked, capped)
  - config/policy → :func:`cairn.core.operator.config.preflight_operator_configs`
                    (#744, fail-closed — the SAME gate the dispatcher enforces)
  - connections   → stored :class:`~cairn.core.connections.HealthState` (advisory)

No run row, no spend, no external writes. The same verdict is meant to power the
console Run tab (#798), ``cairn run --dry-run`` / ``cairn preflight`` (CLI parity,
ADR-0072 §3), and the ``cairn ask`` plan step.

Honest scope (MVP): the credential leg is **advisory** — it reports the health of
already-probed connections but cannot yet fail-closed on "provider X required by
this template is not connected", because operators don't declare ``requires_secrets``
(the contract's :class:`~cairn.core.ui_contract.Remediation` notes the same gap). That
upgrade is the tracked follow-up; until then gates and connection health are
surfaced but never block ``ready``.
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from cairn.core.ui_contract import build_ui_contract
from cairn.core.ui_render import coerce_and_validate


class Blocker(BaseModel):
    """A reason the run cannot proceed, plus the one thing that fixes it."""

    #: input-missing | unresolved-operator | policy-config | template-not-found
    kind: str
    what: str
    fix: str = ""


class GateInfo(BaseModel):
    """A step that will pause for a human decision. NOT a blocker — a heads-up:
    the run can start, it just won't finish unattended."""

    step: str
    needs_approval: bool = True


class Preflight(BaseModel):
    """The dry-run verdict (ADR-0072). ``ready`` answers "will this run"; the rest
    answers "what will it cost, what pauses for me, what's blocking"."""

    ref: str
    found: bool = True
    ready: bool = False
    blockers: list[Blocker] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    cost: dict[str, Any] = Field(default_factory=dict)
    gates: list[GateInfo] = Field(default_factory=list)
    #: the config that WOULD apply (ADR-0071 preset once built; today the saved
    #: per-workflow operational values, or ``{}``).
    resolved_config: dict[str, Any] = Field(default_factory=dict)
    inputs_echoed: dict[str, Any] = Field(default_factory=dict)
    command: str = ""


async def _connection_warnings() -> list[str]:
    """Advisory connection health (MVP): surface any already-probed connection
    that last came back unhealthy. Reads the STORED ``HealthState`` — no live
    probe, no network — so the verdict stays fast and strictly side-effect-free.

    Best-effort: a store hiccup yields no warnings rather than failing the
    verdict (connection health is advisory here, never a hard blocker)."""
    from cairn.core.connections import (  # noqa: PLC0415 — off the hot import path
        lookup_account, default_connection_store, run_account,
    )

    try:
        account = lookup_account(await run_account())  # only the launching account's own
        store = default_connection_store()
        try:
            conns = await store.list(account)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()
    except Exception:  # noqa: BLE001 — advisory only; never break the verdict on it
        return []

    out: list[str] = []
    for c in conns:
        h = getattr(c, "health", None)
        if getattr(c, "enabled", True) and h is not None and h.ok is False:
            detail = (h.detail or "unhealthy").strip()
            out.append(f"connection '{c.name}' last probed unhealthy: {detail}")
    return out


async def _holdout_verdict(typed: dict[str, Any], raw: dict[str, Any]):
    """The holdout door's verdict for a dry run (#2020).

    Looks at BOTH the coerced values and the raw ones: a ``dataset:`` ref that
    failed input validation is still a ref the user meant, and a dry run that
    stayed silent about the overlap because a different field was blank would be
    the least useful possible answer.
    """
    from cairn.core.holdout import check_holdout_for_inputs  # noqa: PLC0415 — off the hot import path
    from cairn.core.workspace import current_workspace  # noqa: PLC0415 — off the hot import path

    # Key-prefixed union rather than a dict merge: only the VALUES matter to
    # `dataset_refs_in` (which de-duplicates), and a plain merge would let a
    # coerced default shadow the ref the user actually typed.
    merged: dict[str, Any] = {f"typed.{k}": v for k, v in (typed or {}).items()}
    merged.update({f"raw.{k}": v for k, v in (raw or {}).items()})
    return await check_holdout_for_inputs(merged, workspace_id=current_workspace())


async def _intake_gate(typed: dict[str, Any], raw: dict[str, Any]):
    """The intake door's verdict for a dry run (#2021).

    Same both-values reasoning as :func:`_holdout_verdict`: a ``dataset:`` ref
    the user typed is a ref they meant, whether or not a sibling field was
    blank, and a dry run that stayed quiet about a ``blocked`` version because
    of an unrelated validation error would be the least useful answer.
    """
    from cairn.core.intake import check_intake_for_inputs  # noqa: PLC0415 — off the hot import path
    from cairn.core.workspace import current_workspace  # noqa: PLC0415 — off the hot import path

    merged: dict[str, Any] = {f"typed.{k}": v for k, v in (typed or {}).items()}
    merged.update({f"raw.{k}": v for k, v in (raw or {}).items()})
    return await check_intake_for_inputs(merged, workspace_id=current_workspace())


async def _license_gate(typed: dict[str, Any], raw: dict[str, Any]):
    """The LICENSE door's verdict for a dry run (#2027, ADR-0102 D5).

    Same fill-in-the-blanks reasoning as the holdout/intake doors above: the
    typed values win where coercion succeeded, the raw ones fill in where a
    sibling field's validation error would otherwise have hidden a gated
    ``base_model`` the user did type. Reads the catalog + the stored
    connection health; writes nothing."""
    from cairn.core.model_license_gate import check_license_for_inputs  # noqa: PLC0415

    merged: dict[str, Any] = dict(raw or {})
    merged.update(typed or {})
    return await check_license_for_inputs(merged)


async def build_preflight(
    ref: str,
    values: dict[str, Any] | None = None,
    *,
    profile: Optional[str] = None,
    resolved_config: dict[str, Any] | None = None,
    check_connections: bool = True,
) -> Preflight:
    """Compose the side-effect-free preflight verdict for ``ref`` = ``pack/template``.

    ``values`` are the submitted (raw) form inputs; ``profile`` selects the
    execution environment the plan resolves under. ``resolved_config`` is the
    config that would apply (passed by the route from the workflow-config store);
    it's echoed into the verdict, not re-fetched here.
    """
    contract = build_ui_contract(ref, profile=profile)
    if not contract.plan.get("template_found"):
        return Preflight(
            ref=ref,
            found=False,
            ready=False,
            blockers=[Blocker(kind="template-not-found", what=f"no template {ref!r}",
                              fix="check the workflow ref, or install its pack")],
        )

    plan = contract.plan or {}
    blockers: list[Blocker] = []
    warnings: list[str] = []

    # 1. inputs — the SAME coerce+validate every surface runs at its edge.
    typed, errors = coerce_and_validate(contract, values or {})
    for e in errors:
        blockers.append(Blocker(kind="input-missing", what=e,
                                fix="fill the field or fix its type"))

    # 2. operators / capabilities — unresolved bindings block the run.
    for m in plan.get("missing_bindings", []) or []:
        blockers.append(Blocker(kind="unresolved-operator", what=str(m),
                                fix="install the operator pack or bind the capability in this environment"))

    # 3. config / policy — the #744 fail-closed preflight, exactly as the
    #    dispatcher enforces it at run-submit. A malformed deployment knob blocks.
    try:
        from cairn.core.operator.config import preflight_operator_configs  # noqa: PLC0415
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415

        try:
            profile_cfg = load_profile_from_env().config
        except Exception:  # noqa: BLE001 — a broken profile is reported by doctor, not here
            profile_cfg = {}
        preflight_operator_configs(profile_cfg)
    except Exception as exc:  # noqa: BLE001 — OperatorConfigError (or any) → a blocker, not a 500
        for msg in (getattr(exc, "errors", None) or [str(exc)]):
            blockers.append(Blocker(kind="policy-config", what=str(msg),
                                    fix="fix the [profile.config] knob in this environment"))

    # 3b. holdout (#2020, ADR-0102 D3) — the SAME door the dispatcher enforces at
    #     run-submit, so the console's dry-run says "refused, and here is the row"
    #     instead of the user finding out at launch. Reads the registry and the
    #     dataset files; writes nothing.
    holdout = await _holdout_verdict(typed, values or {})
    for reason in holdout.reasons():
        blockers.append(Blocker(
            kind="holdout-overlap", what=reason,
            fix="remove the overlapping rows (the clean flow's decontamination step "
                "against that eval set) and register a new version, or re-check the "
                "roles on these versions"))
    warnings.extend(holdout.warnings)

    # 3c. intake verdict (#2021, ADR-0102 D1) — the SAME door the dispatcher
    #     enforces at run-submit. `blocked` and `invalid_format` block; a
    #     `degraded` version is a WARNING here (it runs, with its reasons on the
    #     run record), and a version registered before verdicts existed is
    #     admitted with a note rather than refused retroactively.
    intake = await _intake_gate(typed, values or {})
    for reason in intake.reasons():
        blockers.append(Blocker(
            kind="intake-blocked", what=reason,
            fix="re-run the dataset-readiness clean flow over this data (it redacts, "
                "decontaminates and re-checks the declared expectations) and launch on "
                "the version it registers"))
    warnings.extend(intake.degraded_reasons())
    warnings.extend(intake.warnings)

    # 3d. licence gate (#2027, ADR-0102 D5) — the SAME door the dispatcher
    #     enforces at run-submit. A gated catalog `base_model` refuses here
    #     naming the model and its licence unless `accept_license: true` is
    #     set AND a Hugging Face token connection is present. A free-text
    #     base_model (not a catalog id) or an ungated catalog entry is
    #     unaffected — the escape hatch stays free.
    license_gate = await _license_gate(typed, values or {})
    for reason in license_gate.reasons():
        blockers.append(Blocker(
            kind="license-not-accepted", what=reason,
            fix="set accept_license: true and connect a Hugging Face token "
                "under Connections, or pick a different base model",
        ))

    # 3e. job-channel door (#2025, ADR-0099 §1): a remote-job leg on a NATS
    #     channel needs that NATS reachable, or its live events go nowhere.
    #     Derived from configuration, never declared; refused, never started.
    from cairn.core.job_channel_door import check_job_channel  # noqa: PLC0415 — the door probes sockets; keep it off the import path

    op_names = [str(o.get("operator") or "") for o in (plan.get("operators", []) or []) if isinstance(o, dict)]
    svc = check_job_channel(op_names)
    for reason in svc.reasons:
        blockers.append(Blocker(kind="service-unreachable", what=reason,
                                fix="start the nats service (COMPOSE_PROFILES=nats) or set CAIRN_JOB_CHANNEL=http"))

    # 3f. flow-engine door (ADR-0134 D3c; was the SkyPilot door, #2073 / ADR-0104 D5;
    #     ADR-0099 §1): when the template reaches a flow engine, the selected engine must
    #     be installed and, if it offers check_ready(), ready (a provisioning engine: its
    #     target resolves and its cloud is enabled). Only the kinds this template's own
    #     operators reach are asked. Derived from configuration; refused, never installed
    #     or provisioned.
    from cairn.core.dataflow.engine_door import check_flow_engines, kinds_reached  # noqa: PLC0415 — an engine's check may reach a cloud; keep it off the import path

    _kinds = kinds_reached(op_names)
    for reason in ((await check_flow_engines(kinds=_kinds)).reasons if _kinds else []):
        blockers.append(Blocker(kind="engine-not-ready", what=reason,
                                fix="install the selected engine's extra (pip install 'cairndev[<engine>]'), "
                                    "or select an installed engine; then fix what the engine's own check "
                                    "names (for a provisioning engine: its ml_target/dataflow_target "
                                    "connection and enabled cloud)"))

    # 3g. pull-worker door (#565, ADR-0104 D2, ADR-0099 §1): a remote leg on a
    #     `worker:` ML target needs a worker that has actually checked in.
    from cairn.core.pull_worker_door import check_pull_worker  # noqa: PLC0415 — the door reads the job store; keep it off the import path

    for reason in (await check_pull_worker(op_names)).reasons:
        blockers.append(Blocker(kind="service-unreachable", what=reason,
                                fix="start a worker on the machine that should run it: "
                                    "cairn worker --pull --engine-url <url> --token <token>"))

    # 4. gates — approvals that will pause the run (heads-up, not a blocker).
    gates = [GateInfo(step=str(a)) for a in (plan.get("approvals_required", []) or [])]

    # 5. connections — advisory health of already-probed connections (MVP).
    if check_connections:
        warnings.extend(await _connection_warnings())

    return Preflight(
        ref=ref,
        found=True,
        ready=not blockers,
        blockers=blockers,
        warnings=warnings,
        cost=plan.get("estimated_cost", {}) or {},
        gates=gates,
        resolved_config=resolved_config or {},
        inputs_echoed=typed,
        command=contract.recommended_command or plan.get("recommended_command", ""),
    )
