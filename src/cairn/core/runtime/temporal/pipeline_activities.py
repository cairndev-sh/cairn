"""Activities for :class:`~cairn.core.runtime.temporal.pipeline_workflow.PipelineWorkflow`.

The workflow is pure orchestration; every side effect + piece of cairn logic lives here so
the workflow stays inside the Temporal sandbox (ADR-0069 Phase 4). Mirrors the split in the
run-level ``activities.py`` (``run_node``). The prod PipelineWorkflow runs these; the
hermetic Slice-① test registers mocks with the same names.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable

from cairn.core.runtime.temporal import require

logger = logging.getLogger("cairn.runtime.temporal.pipeline_activities")

require()
from temporalio import activity  # noqa: E402
from cairn.core.dispatch.coordination import CriticalSections  # noqa: E402

_DISPATCHER: Any = None  # worker-process dispatcher, built once (holds the store pool)
_SECTIONS = CriticalSections(namespace="pipeline")

_RUN_TERMINAL = ("done", "succeeded", "failed", "cancelled")
# #2223 — the failure types of the flow-leg re-adoption checks live beside NEEDS_APPROVAL
# (the workflow reads them without importing engine code).
from cairn.core.runtime.temporal.pipeline_workflow import (  # noqa: E402
    RECORD_CORRUPT,
    RECORD_MISSING,
    RUN_MISSING,
    STORE_UNAVAILABLE,
)
_STEP_TERMINAL = ("done", "skipped", "failed", "cancelled")
#: #1288 parity with the bridge: a flow leg polls under the declared leg
#: timeout, else this generous default — never execute_leg's 3600s (whose
#: expiry CANCELS the engine job mid-train).
_FLOW_LEG_DEFAULT_BUDGET_S = 7 * 86400.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _pin_step_run(
    pstore: Any,
    pipeline_id: str,
    step_id: str,
    run_id: str,
    *,
    section: Callable[[str], AbstractAsyncContextManager[None]],
) -> None:
    """Record the leg's run_id on the pipeline's step the moment it exists —
    the retry/adopt anchor (#1429). Best-effort: the workflow mirrors it too."""
    try:
        async with section(pipeline_id):
            rec = await pstore.get(pipeline_id)
            st = rec.step_state.get(step_id) if rec else None
            if rec is None or st is None:
                return
            st.run_id = run_id
            st.status = "running"
            st.started_at = st.started_at or _now()
            rec.step_state[step_id] = st
            await pstore.put(rec)
    except Exception:  # noqa: BLE001 — read-model, never fail the leg on it
        logger.debug("_pin_step_run failed for %s/%s", pipeline_id, step_id, exc_info=True)


def _dispatcher() -> Any:
    global _DISPATCHER
    if _DISPATCHER is None:
        from cairn.core.dispatch.dispatcher import RunDispatcher  # noqa: PLC0415
        from cairn.core.engine.runner import Runner  # noqa: PLC0415
        from cairn.core.storage import build_storage  # noqa: PLC0415

        _DISPATCHER = RunDispatcher(Runner(), backend=build_storage())
    return _DISPATCHER


@activity.defn(name="cairn.pipeline_prepare_step")
async def pipeline_prepare_step(step: dict, inputs: dict, outputs: dict) -> dict:
    """Evaluate the step's gate over persisted output + resolve its inputs — the same
    governance re-check + data contract the bridge does, in an activity so the workflow
    doesn't import engine code."""
    from cairn.core.engine.bundle import eval_gate_predicate, resolve_step_inputs  # noqa: PLC0415

    gate = step.get("gate")
    passed = True if not gate else eval_gate_predicate(gate, bundle_input=inputs, steps=outputs)
    # #1251 (ADR-0087 D2) — `when` ROUTES: false means skip this leg and carry
    # on, which is a different answer from a blocked gate. Reported separately so
    # the workflow can tell a route from a stop (the bridge makes the same split).
    when = step.get("when")
    routed_on = True if not when else eval_gate_predicate(when, bundle_input=inputs, steps=outputs)
    resolved = resolve_step_inputs(
        SimpleNamespace(inputs=step.get("inputs", {}),
                        optional_inputs=step.get("optional_inputs") or []),
        bundle_input=inputs, steps=outputs,
    )
    return {"gate_passed": bool(passed), "routed_on": bool(routed_on), "resolved": resolved}


@activity.defn(name="cairn.pipeline_submit_step")
async def pipeline_submit_step(
    ref: str, resolved: dict, pipeline_id: str, step_id: str,
    environment: str | None, step: dict | None = None,
) -> str:
    """Submit a step as its own governed run (stamped with the pipeline lineage). Returns
    the run_id the workflow then awaits.

    Routes by LEG KIND (#1127/ADR-0084 D1), mirroring ``PipelineBridge._submit_step``:
    a ``flow:`` leg executes on the environment's engine — dispatching it through
    ``run_template`` parsed ``flow:<pack>`` as a pack name and failed every modern
    pipeline under the temporal owner (the in-process bridge had this fix from #1181;
    this owner predates leg kinds and was never driven with a flow leg until the
    first prod E2E). ``gate:`` legs never reach SUBMIT — the workflow completes them
    on approval, the same #1180 semantics as the bridge.
    """
    from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 — engine code stays out of workflow imports

    if leg_kind_of(ref) == "flow":
        return await _run_flow_leg(ref, resolved, pipeline_id, step_id, environment, step or {})
    from cairn.core.storage.pipeline_store import default_pipeline_store  # noqa: PLC0415 — cycle
    from cairn.core.workspace import workspace_scope  # noqa: PLC0415 — engine code stays out of workflow imports

    # Bridge parity (`_submit_step` runs in its record's workspace): the worker has no
    # request context, so the pipeline record names the leg's workspace — the one the
    # leg's run is stamped with and its connections and secrets resolve in.
    rec = await _leg_pipeline_record(default_pipeline_store(), pipeline_id, step_id)
    with workspace_scope(getattr(rec, "workspace_id", "") or ""):
        run = await _dispatcher().run_template(
            ref, resolved, environment=environment or None,
            bundle_pipeline_id=pipeline_id, step_id=step_id,
        )
    return run.id


async def _leg_pipeline_record(pstore: Any, pipeline_id: str, step_id: str, *,
                               flow_ref: str = "") -> Any:
    """The pipeline record a leg runs under (governance, workspace). #2223 — an
    UNREADABLE store is not an ABSENT record: a read failure retries (Temporal's
    backoff) with nothing created or submitted, and a missing or undecodable record
    refuses. Neither ever runs the leg ungoverned or outside its workspace.

    A flow leg (``flow_ref``) refused because its record names no workspace also
    cancels the engine job an earlier attempt recorded on the step, if any: the
    refusal ends the leg, so nothing would watch that job any more."""
    from pydantic import ValidationError  # noqa: PLC0415
    from temporalio.exceptions import ApplicationError  # noqa: PLC0415 — engine code stays out of workflow imports

    try:
        rec = await pstore.get(pipeline_id)
    except ValidationError as e:   # read fine, cannot be decoded: retrying will not fix it
        raise ApplicationError(f"pipeline {pipeline_id}: record cannot be decoded; refusing to run leg "
                               f"{step_id} on corrupt state", type=RECORD_CORRUPT, non_retryable=True) from e
    except Exception as e:  # noqa: BLE001 — any read failure is "unknown", never "absent"
        raise ApplicationError(f"pipeline {pipeline_id}: record unreadable ({type(e).__name__}); "
                               f"leg {step_id} not submitted, retrying", type=STORE_UNAVAILABLE) from e
    if rec is None:
        raise ApplicationError(f"pipeline {pipeline_id}: no pipeline record; refusing to run leg "
                               f"{step_id} without its governance", type=RECORD_MISSING, non_retryable=True)
    from cairn.core.auth_tokens import auth_enabled  # noqa: PLC0415 — engine code stays out of workflow imports

    if not str(getattr(rec, "workspace_id", "") or "").strip() and auth_enabled():
        # bridge parity: with auth on a record that names no workspace runs nothing —
        # refused before any run record exists, and retrying cannot fix it
        if flow_ref:
            await _cancel_recorded_flow_job(rec, step_id, flow_ref)
        raise ApplicationError(f"pipeline {pipeline_id}: record names no workspace; refusing to run "
                               f"leg {step_id}", type=RECORD_CORRUPT, non_retryable=True)
    return rec


async def _cancel_recorded_flow_job(rec: Any, step_id: str, flow_ref: str) -> None:
    """Best-effort cancel of the engine job recorded on ``step_id`` (bridge parity)."""
    from cairn.core.dataflow.engine_registry import resolve_engine  # noqa: PLC0415 — cycle
    from cairn.core.dispatch.leg_executor import cancel_recorded_job, flow_kind_of  # noqa: PLC0415 — cycle

    st = (getattr(rec, "step_state", None) or {}).get(step_id)
    handle = dict(st.handle) if st is not None and st.handle else None
    if not handle:
        return
    try:
        engine = resolve_engine(None, kind=flow_kind_of(flow_ref))
    except Exception:  # noqa: BLE001 — no engine to reach: the refusal still lands
        logger.exception("leg %s: no engine to cancel recorded job %s on", step_id, handle.get("job_id"))
        return
    await cancel_recorded_job(engine, handle)


async def _gate_flow_leg(
    ref: str, pipeline_id: str, step_id: str, environment: str, step: dict,
    approver: str | None, rec: Any,
) -> None:
    """#2299 — the Temporal owner's call into THE flow-leg enforcement point. Same inputs
    as ``PipelineBridge._enforce_flow_leg_gate``: the flow's aggregate risk, the step's own
    ``approval:`` mandate, and the approval a human recorded on the step (``advance_step``).
    The authority merge stays in ``approval.enforce``. A required gate announces the pause
    (the bridge's audit event + gate notification) and raises a non-retryable
    ``NEEDS_APPROVAL`` error the workflow turns into a durable wait — no run, no engine job."""
    from temporalio.exceptions import ApplicationError  # noqa: PLC0415 — engine code stays out of workflow imports

    from cairn.core.approval.enforce import NeedsApprovalError, approved_state, enforce_flow_gate  # noqa: PLC0415 — cycle
    from cairn.core.dataflow.defs import flow_leg_risk  # noqa: PLC0415 — cycle
    from cairn.core.runtime.temporal.pipeline_workflow import NEEDS_APPROVAL  # noqa: PLC0415 — the shared name

    risk = None
    try:
        risk = flow_leg_risk(ref)   # the bridge's lookup; unreadable resolves as unknown, never safe
    except Exception:  # noqa: BLE001
        logger.exception("flow leg %s: could not resolve aggregate risk", ref)
    try:
        enforce_flow_gate(ref, risk, (step or {}).get("approval") or None,
                          state=approved_state(environment, step_id, approver),
                          node_name=step_id, run_id=pipeline_id)
    except NeedsApprovalError as e:
        req = e.request
        detail = {"run_id": "", "operator": getattr(req, "operator", "") or ref,
                  "risk": getattr(req, "risk", "") or "",
                  "sources": list(getattr(req, "sources", ()) or ()),
                  "reason": getattr(req, "reason", "") or ""}
        await _announce_flow_pause(pipeline_id, step_id, detail, rec)
        raise ApplicationError(f"flow leg {ref} ({step_id}) awaits approval", detail,
                               type=NEEDS_APPROVAL, non_retryable=True) from None


async def _announce_flow_pause(pipeline_id: str, step_id: str, detail: dict, rec: Any) -> None:
    """The bridge's pause announcement (``_pause_leg_for_approval``): the
    ``pipeline.leg_awaiting_approval`` audit event and the gate notification. Best-effort —
    an announcement never decides the gate."""
    from cairn.core.bus import get_bus  # noqa: PLC0415
    from cairn.core.gate_notify import emit_gate_event, pause_dedupe_key, release_commands  # noqa: PLC0415
    from cairn.core.notifier import NotifyEvent  # noqa: PLC0415

    try:
        await get_bus().publish("pipeline.leg_awaiting_approval",
                                {"pipeline_id": pipeline_id, "step_id": step_id, **detail},
                                correlation_id=pipeline_id)
    except Exception:  # noqa: BLE001 — audit must never break the transition
        logger.debug("leg_awaiting_approval publish failed for %s/%s", pipeline_id, step_id)
    try:
        await emit_gate_event(NotifyEvent(
            kind="awaiting_approval", pipeline_id=pipeline_id, step_id=step_id,
            pipeline_name=(getattr(rec, "pipeline_name", "") or getattr(rec, "bundle_ref", "") or ""),
            operator=str(detail["operator"]), risk=str(detail["risk"]),
            authority=tuple(str(x) for x in detail["sources"]), reason=str(detail["reason"]),
            commands=release_commands(pipeline_id, step_id),
            dedupe_key=pause_dedupe_key(pipeline_id, step_id, _now())))
    except Exception:  # noqa: BLE001 — a notification never decides the gate
        logger.exception("gate notification failed for %s/%s", pipeline_id, step_id)


async def _run_flow_leg(
    ref: str, resolved: dict, pipeline_id: str, step_id: str,
    environment: str | None, step: dict,
) -> str:
    """Run one ``flow:`` leg to completion inside this activity — the temporal-owner
    counterpart of ``PipelineBridge._submit_flow_leg`` (same run-record shape, same
    ``_flow_leg_output`` produces contract, same terminal emit). Heartbeats while the
    engine works so a long leg doesn't look stalled to Temporal.

    **Idempotent across activity retries (#1429).** Temporal re-runs this activity
    when the worker that was running it dies (heartbeat timeout → retry on a live
    worker). Before this, a retry created a NEW run record and RE-SUBMITTED the
    engine job — a duplicate of paid, possibly still-running compute, the exact
    failure C3 (#1412 G1) forbids. Now every retry first reads what the previous
    attempt persisted on the pipeline's step: an already-terminal run is returned
    as is; a persisted engine handle is ADOPTED (watched to its end, never
    resubmitted); a step whose earlier attempt never reached the engine submits
    fresh — and even that resumes from the newest durable checkpoint (H6c).

    **Unknown is never absent (#2223).** A store that cannot be read raises a
    retryable ``STORE_UNAVAILABLE`` with nothing created or submitted. A missing or
    undecodable pipeline record, and an engine job recorded without a live run, are
    PERMANENT refusals the workflow turns into a failed step: no fresh run over a
    possibly live job, and never a leg run without its governance.
    """
    from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 — cycle: defs → pack registry
    from cairn.core.dispatch.dispatcher import new_run_id  # noqa: PLC0415 — cycle
    from cairn.core.dispatch.leg_executor import execute_leg, parse_flow_ref, recorded_stage_contracts  # noqa: PLC0415 — cycle
    from cairn.core.dispatch.leg_hooks import FlowLegHooks, flow_leg_record, leg_price_kwargs, pipeline_spent_usd  # noqa: PLC0415 — cycle
    from cairn.core.dispatch.pipeline_bridge import (  # noqa: PLC0415 — cycle
        _flow_leg_output,
        _leg_retry_policy,
        _leg_run_status,
    )
    from cairn.core.engine.schema import TriggerEvent  # noqa: PLC0415 — cycle
    from cairn.core.storage.pipeline_store import default_pipeline_store  # noqa: PLC0415 — cycle
    from cairn.core.storage.protocols import Run  # noqa: PLC0415 — cycle
    from cairn.core.dataflow.engine_registry import resolve_engine  # noqa: PLC0415 — cycle

    from temporalio.exceptions import ApplicationError  # noqa: PLC0415 — engine code stays out of workflow imports

    d = _dispatcher()
    sections = _SECTIONS
    pstore = default_pipeline_store()
    # Governance + workspace come from the pipeline record (the workflow doesn't
    # carry them). #2223 — an UNREADABLE store is not an ABSENT record: a read
    # failure retries (Temporal's backoff) with nothing created or submitted, and a
    # missing record refuses. Neither ever runs the leg ungoverned.
    gov: dict[str, Any] = {}
    workspace_id = ""
    prior_run_id = ""
    prior_handle: dict | None = None
    approver: str | None = None
    rec = await _leg_pipeline_record(pstore, pipeline_id, step_id, flow_ref=ref)
    gov = dict(getattr(rec, "governance", None) or {})
    workspace_id = getattr(rec, "workspace_id", "") or ""
    # #1441 S4 — parity with the bridge: the reservation is checked against what
    # the earlier legs ACTUALLY spent. Read before anything is created; an
    # unreadable run store is "unknown", so the activity retries (#2223).
    series_id = str(gov.get("series", "") or "")
    budget_usd = float((gov.get("budget") or {}).get("usd", 0.0) or 0.0)
    spent_usd = 0.0
    if series_id and budget_usd:
        try:
            spent_usd = await pipeline_spent_usd(rec, d.store, exclude_step=step_id)
        except Exception as e:  # noqa: BLE001 — any read failure is "unknown", never "nothing spent"
            raise ApplicationError(f"pipeline {pipeline_id}: spend of earlier legs unreadable "
                                   f"({type(e).__name__}); leg {step_id} not submitted, retrying",
                                   type=STORE_UNAVAILABLE) from e
    st = rec.step_state.get(step_id)
    if st is not None:
        prior_run_id = str(st.run_id or "")
        prior_handle = dict(st.handle) if st.handle else None
        approver = getattr(st, "approver", None)

    if prior_handle and not prior_run_id:
        # #2223 — a recorded engine job with no run to watch it: the same live-job hazard
        # as a vanished run (on_submit writes both together, so only legacy/corrupt rows).
        raise ApplicationError(
            f"leg {step_id} recorded engine job {prior_handle.get('job_id', '?')} but no run; "
            "refusing to resubmit over a possibly live job", type=RUN_MISSING, non_retryable=True)
    run = None
    if prior_run_id:
        try:
            run = await d.store.get(prior_run_id)
        except Exception as e:  # noqa: BLE001 — #2223: unreadable is not absent
            raise ApplicationError(f"run {prior_run_id}: unreadable ({type(e).__name__}); leg {step_id} "
                                   "not resubmitted, retrying", type=STORE_UNAVAILABLE) from e
        if run is None and prior_handle:
            # #2223 (maintainer 1a) — the step recorded an engine job whose run record is
            # gone: the job may still be running, so a fresh submit risks duplicate paid
            # compute. Refuse; a human resolves the job. (No handle = the earlier attempt
            # never reached the engine, so a fresh submit stays safe.)
            raise ApplicationError(
                f"run {prior_run_id} is gone but leg {step_id} recorded engine job "
                f"{prior_handle.get('job_id', '?')}; refusing to resubmit over a possibly live job",
                type=RUN_MISSING, non_retryable=True)
        if run is not None and str(getattr(run, "status", "")) in _RUN_TERMINAL:
            # The previous attempt finished the leg and died before returning:
            # the work is done, the record is authoritative — hand it back.
            logger.info("flow leg %s/%s: prior run %s already %s — not re-running",
                        pipeline_id, step_id, run.id, run.status)
            return run.id
    if run is None:
        prior_handle = None   # no run_id recorded: the earlier attempt never reached the engine
    if prior_handle is None:
        # #2299 — the approval plane, BEFORE any run record or engine work, exactly as
        # the bridge's `_run_flow_leg` (#1760). Skipped when adopting a handle a previous
        # attempt already submitted: that leg cleared its gate and is mid-flight.
        await _gate_flow_leg(ref, pipeline_id, step_id,
                             environment or getattr(rec, "environment", "") or "", step, approver, rec)
    if run is None:
        run = Run(
            id=new_run_id(ref.replace("/", "--").replace(":", "-")),
            trigger=TriggerEvent(source="manual", payload={"leg": ref}),
            status="running",
            started_at=_now(),   # #1434 — wall-clock per leg from the API
            fingerprint=ref,
            template_ref=ref,  # ADR-0084 D0
            inputs=dict(resolved or {}),
            environment=environment or "",
            bundle_pipeline_id=pipeline_id,
            step_id=step_id,
            workspace_id=workspace_id,
        )
        await d.store.create(run)
        async def heartbeat_until_pin() -> None:
            while True:
                activity.heartbeat()
                await asyncio.sleep(5.0)

        heartbeat_task = asyncio.create_task(heartbeat_until_pin())
        try:
            await _pin_step_run(pstore, pipeline_id, step_id, run.id, section=sections.hold)
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    # The flow's OWN kind picks the engine (#1180 bugs 5-6: kind from the def,
    # with the pack dir/manifest name alias fallback). #2126 — through
    # `flow_def_for`, the ONE lookup the in-process leg executor and `pipeline
    # explain` use, rather than a fourth private copy of the pack-spelling
    # fallback: a durable leg picking a different ENGINE from the in-process one
    # for the same flow is the worst possible place for the two to disagree.
    kind = "data"
    if parse_flow_ref(ref) is not None:
        kind = getattr(flow_def_for(ref), "kind", None) or "data"
    engine = resolve_engine(None, kind=kind)

    # Parity with PipelineBridge (#1316, #1429): the SAME side-channel assembly
    # the in-process owner uses — control-side artifact registration (this
    # owner never passed a store: 0 artifacts under Temporal, found live
    # 2026-08-18), persisted handle, heartbeat + checkpoint relay, retry and
    # resume visibility, adopt-by-handle.
    from cairn.core.storage.artifacts import build_artifact_store  # noqa: PLC0415 — engine/storage stays out of workflow imports

    hooks = FlowLegHooks(
        pipeline_id=pipeline_id, step_id=step_id, leg_ref=ref, run_id=run.id,
        pipeline_store=pstore, run_store=d.store, emit=d._emit,
        artifact_store=build_artifact_store(), engine=engine,
        section=sections.hold,
    )
    resolved = dict(resolved or {})
    retry_policy = _leg_retry_policy(resolved.pop("retry", None))
    if prior_handle:
        logger.info("flow leg %s/%s: adopting engine job %s (attempt %s) — not resubmitting",
                    pipeline_id, step_id, prior_handle.get("job_id"), activity.info().attempt)

    from cairn.core.workspace import workspace_scope  # noqa: PLC0415 — engine code stays out of workflow imports

    # The worker has no request context: the record's workspace is the leg's, and
    # with it the account its connections and secrets resolve in (bridge parity).
    with workspace_scope(workspace_id):  # the task copies this context at creation
        task = asyncio.ensure_future(execute_leg(
            ref, inputs=resolved, engine=engine,
            run_id=run.id,
            series_id=series_id,
            # ADR-0151 D2/D5 — the bridge's call: the recorded estimate and box rate.
            **leg_price_kwargs(rec, step_id, run),
            spent_usd=spent_usd,
            budget_usd=budget_usd,
            poll_budget_s=float(step.get("timeout_s") or _FLOW_LEG_DEFAULT_BUDGET_S),
            # ADR-0118 D8 — stage entries reach the leg exactly as through the bridge.
            stage_contracts=recorded_stage_contracts(getattr(rec, "plan", None), step_id),
            **hooks.execute_kwargs(retry=retry_policy, adopt=prior_handle),
        ))
    while not task.done():
        activity.heartbeat()
        await asyncio.wait({task}, timeout=5.0)
    outcome = task.result()
    await hooks.relay_checkpoints()   # H6b final relay, same as the bridge

    step_output = _flow_leg_output(
        SimpleNamespace(produces=list(step.get("produces") or [])), outcome)
    await d.store.update(
        run.id,
        status=_leg_run_status(outcome),
        ended_at=_now(),   # #1434
        error=outcome.error or "",
        **flow_leg_record(step_output, outcome),   # #1441 — same payloads + cost as the bridge
    )
    # Same terminal-emit parity as the bridge (#1180 bug 1): run-event append +
    # SSE + firehose. Chaining here is workflow-driven, so this is observability,
    # not the advance mechanism.
    await d._emit(
        run.id,
        "done" if outcome.ok else "failed",
        {"leg": ref, "step_id": step_id, "error": outcome.error or ""},
    )
    return run.id


@activity.defn(name="cairn.pipeline_await_run")
async def pipeline_await_run(run_id: str) -> dict:
    """Poll the run store until the step's run is terminal; return its status + verdict +
    reconstructed output (the data contract for downstream steps). Heartbeats so a long
    step run doesn't look stalled to Temporal."""
    from cairn.core.dispatch.pipeline_bridge import _verdict_of, reconstruct_step_output  # noqa: PLC0415

    store = _dispatcher().store
    while True:
        run = await store.get(run_id)
        # `succeeded` is the flow-leg terminal (leg_executor outcome state);
        # templates end `done`. Normalized to `done` for the workflow's check.
        if run is not None and getattr(run, "status", "") in ("done", "succeeded", "failed", "cancelled"):
            output = reconstruct_step_output(run)
            status = "done" if run.status == "succeeded" else run.status
            return {"status": status, "verdict": _verdict_of(run, output), "output": output}
        activity.heartbeat()
        await asyncio.sleep(1.0)


@activity.defn(name="cairn.pipeline_next_tick")
async def pipeline_next_tick(cron: str, from_iso: str) -> float:
    """Seconds from ``from_iso`` (the workflow's deterministic ``workflow.now()``) to the
    next matching tick of ``cron`` — so the durable ``PipelineWorkflow`` can ``workflow.sleep``
    exactly to the next occurrence (ADR-0069 Phase 4 Slice ②), replacing Slice ①'s 1s
    placeholder. Computed in an activity (not the workflow) so apscheduler stays out of the
    Temporal sandbox and the recorded result makes the sleep replay-deterministic.

    A pure function of its inputs (cron + base time, both treated as UTC): the same base
    time always yields the same delay, so replay reproduces the identical timer. Returns
    ``0.0`` for a cron that never fires again (submit now, don't block forever) and for a
    malformed cron (the alternative — wedging the pipeline — is worse than firing once)."""
    from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415 — scheduler-tier dep

    base = datetime.fromisoformat(from_iso)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    try:
        trig = CronTrigger.from_crontab(cron.strip(), timezone=timezone.utc)
    except Exception as e:  # noqa: BLE001 — a bad cron must not wedge the workflow
        logger.warning("pipeline_next_tick: bad cron %r: %s", cron, e)
        return 0.0
    nxt = trig.get_next_fire_time(None, base)
    if nxt is None:
        return 0.0
    return max(0.0, (nxt - base).total_seconds())


@activity.defn(name="cairn.pipeline_emit_event")
async def pipeline_emit_event(event: str, output: dict, pipeline_id: str, step_id: str) -> None:
    """Publish a step's declared ``emits`` domain event on completion (ADR-0069). Downstream
    ``on:<event>`` steps fire off it: the engine-process ``PipelineBridge`` subscribes to the
    (durable) bus and forwards the event to the waiting workflow as a ``deliver_event`` signal.
    The workflow can't publish from inside the sandbox, so this is an activity."""
    from cairn.core.bus import get_bus  # noqa: PLC0415 — engine bus, not a workflow dep

    try:
        await get_bus().publish(
            event, dict(output or {}),
            publisher=f"pipeline:{pipeline_id}", correlation_id=pipeline_id,
        )
    except Exception as e:  # noqa: BLE001 — a broadcast failure must not fail the completed step
        logger.warning("pipeline_emit_event failed for %r (step %s): %s", event, step_id, e)


@activity.defn(name="cairn.pipeline_mirror_state")
async def pipeline_mirror_state(pipeline_id: str, step_state: dict, status: str) -> None:
    """Mirror the workflow's authoritative state into the ``pipelines`` read-model so the
    console (which reads the store, not Temporal) shows live progress. Best-effort — the
    workflow's own history is the source of truth.

    MERGES, never replaces (#1429/#1434): the workflow's dict knows status/run_id/
    verdict; the ACTIVITY side persists what the workflow can't see — the engine
    handle (the re-attach token) and the run_id pinned before SUBMIT returns — and
    a wholesale overwrite here clobbered both. Timestamps are stamped on the
    transitions this mirror observes (step started/ended, pipeline ended), which
    is what the API had null for every owner-run pipeline (#1434).
    """
    if not pipeline_id:
        return
    try:
        from cairn.core.storage.pipeline_store import (  # noqa: PLC0415
            PipelineRecord,
            StepState,
            default_pipeline_store,
        )

        store = default_pipeline_store()
        # The mirror is a whole-record read-modify-write. It must use the same
        # section as pin/handle/approver writers or a stale workflow snapshot can
        # put those fields back after a sibling leg updates them.
        async with _SECTIONS.hold(pipeline_id):
            rec = await store.get(pipeline_id)
            if rec is None:
                # #2223 — never INVENT a record: a synthesized one (empty governance, the
                # "default" workspace) would let the next leg run ungoverned. The bridge
                # writes the record before the workflow starts; absent means deleted.
                logger.warning("pipeline_mirror_state: no record for %s; not recreating it", pipeline_id)
                return
            now = _now()
            merged: dict[str, StepState] = {}
            for sid, st in (step_state or {}).items():
                incoming = {k: v for k, v in st.items() if k in StepState.model_fields}
                prev = rec.step_state.get(sid)
                if prev is not None:
                    keep = prev.model_dump()
                    # activity-owned fields win when the workflow has nothing newer
                    for k in ("handle", "run_id", "started_at", "ended_at", "approver", "awaiting_since"):
                        if incoming.get(k) is None and keep.get(k) is not None:
                            incoming[k] = keep[k]
                new = StepState(**incoming)
                was = str(getattr(prev, "status", "") or "pending")
                if new.status == "running" and not new.started_at:
                    new.started_at = now
                if new.status in _STEP_TERMINAL and not new.ended_at and (was != new.status or prev is None):
                    new.ended_at = now
                if new.status.startswith("awaiting_") and not new.awaiting_since:
                    new.awaiting_since = now
                merged[sid] = new
            rec.step_state = merged
            rec.status = status
            rec.updated_at = now
            if status in ("done", "failed", "cancelled") and not rec.ended_at:
                rec.ended_at = now
            await store.put(rec)
    except Exception as e:  # noqa: BLE001 — read-model, never fail the workflow on it
        logger.debug("pipeline_mirror_state failed for %s: %s", pipeline_id, e)


ACTIVITIES = [
    pipeline_prepare_step,
    pipeline_submit_step,
    pipeline_await_run,
    pipeline_next_tick,
    pipeline_emit_event,
    pipeline_mirror_state,
]
