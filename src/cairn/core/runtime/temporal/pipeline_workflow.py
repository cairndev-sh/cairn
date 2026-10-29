"""PipelineWorkflow — the durable, crash-safe owner for a bundle pipeline (ADR-0069
Phase 4, Temporal-prod variant of the in-process ``PipelineBridge``).

Where the ``PipelineBridge`` advances a pipeline from in-memory state + a best-effort
restart-adopt, this workflow makes the *orchestration itself* durable: Temporal's event
history is the owner. On a worker crash it replays deterministically and resumes exactly
where it was — no lost transition, no double-submit.

Determinism discipline (why this file imports no cairn engine code): the workflow body is
**pure orchestration** — dict bookkeeping + `execute_activity` + signals. Every side effect
and every piece of cairn logic (gate eval, input resolution, submitting a run, awaiting it,
mirroring the read-model) lives in an ACTIVITY (``pipeline_activities.py``), so the workflow
stays inside the Temporal sandbox. Same shape as ``TemplateWorkflow`` (one run) but a level
up (the whole pipeline; each step's run may itself be a ``TemplateWorkflow``).

The gate is re-evaluated (in an activity, over persisted output) before every submit — the
same governance guarantee the bridge gives: an advance/approval can never bypass it.

#1361 — independent branches OVERLAP: one task per step, each parked on
``workflow.wait_condition`` until its dependencies are terminal, so two legs with disjoint
``depends_on`` submit and run concurrently (the in-process bridge has always done this —
its `_advance` submits every ready step per pass; this owner serialized them). Guarded by
``workflow.patched("1361-parallel-ready-steps")``: histories recorded before the patch
replay the original sequential pass verbatim, new executions take the DAG path.
``asyncio`` here is Temporal's deterministic workflow event loop, not real concurrency —
commands still replay in one recorded order.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, Callable

from cairn.core.runtime.temporal import require

require()
from temporalio import workflow  # noqa: E402
from temporalio.exceptions import ActivityError, ApplicationError  # noqa: E402

# Activity names (string-dispatched so the workflow never imports the impls).
PREPARE = "cairn.pipeline_prepare_step"   # (step, inputs, outputs) → {gate_passed, routed_on, resolved}
SUBMIT = "cairn.pipeline_submit_step"     # (ref, resolved, pipeline_id, step_id, environment) → run_id
AWAIT_RUN = "cairn.pipeline_await_run"    # (run_id) → {status, verdict, output}
NEXT_TICK = "cairn.pipeline_next_tick"    # (cron, from_iso) → delay seconds to the next tick
EMIT = "cairn.pipeline_emit_event"        # (event, output, pipeline_id, step_id) → None (bus publish)
MIRROR = "cairn.pipeline_mirror_state"    # (pipeline_id, step_state, status) → None
#: #2299 — the ApplicationError type SUBMIT raises when a flow leg's approval gate is required
#: and not yet granted: the workflow parks the step instead of failing it.
NEEDS_APPROVAL = "cairn.NeedsApproval"
#: #2223 — flow-leg re-adoption checks: a read failure retries (never "absent"); the
#: three PERMANENT refusals fail the step visibly instead of running it ungoverned.
STORE_UNAVAILABLE = "cairn.StoreUnavailable"     # retryable: the store could not be read
RECORD_MISSING = "cairn.PipelineRecordMissing"   # no pipeline record to govern the leg
RECORD_CORRUPT = "cairn.PipelineRecordCorrupt"   # a record that cannot be decoded
RUN_MISSING = "cairn.RunMissingWithHandle"       # an engine job recorded, its run gone
_PERMANENT_REFUSALS = (RECORD_MISSING, RECORD_CORRUPT, RUN_MISSING)

_SHORT = timedelta(seconds=30)
_SUBMIT_TO = timedelta(minutes=10)
_AWAIT_TO = timedelta(hours=12)
#: #1429 — a flow leg RUNS inside its SUBMIT activity (and AWAIT_RUN polls),
#: both heartbeating every ≤5s. Without a heartbeat timeout Temporal cannot
#: tell a dead worker from a slow leg until start_to_close (12h) expires: the
#: leg would sit for half a day before the retry that re-adopts it. With one,
#: a worker death is detected in minutes and the activity is retried on a live
#: worker — where `_run_flow_leg` adopts the persisted handle, not resubmits.
_LEG_HEARTBEAT_TO = timedelta(minutes=2)

#: A step in one of these states no longer blocks its dependents' wait_condition.
_TERMINAL = ("done", "skipped", "failed")


@workflow.defn(name="cairn.PipelineWorkflow")
class PipelineWorkflow:
    """Durable execution of one bundle pipeline. Input is the serialized plan +
    pipeline-level inputs; returns the final ``{status, step_state}``."""

    def __init__(self) -> None:
        self._approved: set[str] = set()   # step_ids advanced/approved via signal
        self._rejected: set[str] = set()   # step_ids rejected at a gate via signal (ADR-0156)
        self._events: set[str] = set()     # domain events delivered via signal
        self._failed = False               # any gate_blocked / failed leg
        self._abort = False                # stop LAUNCHING pending steps (#1361)

    @workflow.signal
    def approve_step(self, step_id: str) -> None:
        """Advance a `manual`/`approval` step (HITL) — the durable counterpart of
        ``PipelineBridge.advance_step``. The gate is still re-checked before submit."""
        self._approved.add(step_id)

    @workflow.signal
    def reject_step(self, step_id: str) -> None:
        """Reject an `approval` gate (ADR-0156 D5) — the twin of ``approve_step``. The step
        ends ``skipped`` with the verdict ``rejected`` and the workflow ends failed,
        scheduling nothing more. The bridge has already recorded the decider."""
        self._rejected.add(step_id)

    @workflow.signal
    def deliver_event(self, event: str) -> None:
        """Deliver an `on:<domain-event>` a step is waiting on (a bus→signal bridge
        forwards the cairn bus event to this signal)."""
        self._events.add(event)

    @workflow.run
    async def run(self, plan: dict[str, Any], inputs: dict[str, Any], pipeline_id: str = "") -> dict[str, Any]:
        steps = plan.get("steps", [])
        state: dict[str, dict] = {s["id"]: {"status": "pending", "run_id": None, "verdict": None} for s in steps}
        outputs: dict[str, dict] = {}   # step_id → persisted output (for gate + input resolution)

        async def act(name: str, *args: Any, to: timedelta = _SHORT,
                      heartbeat: timedelta | None = None) -> Any:
            return await workflow.execute_activity(
                name, args=list(args), start_to_close_timeout=to,
                heartbeat_timeout=heartbeat)

        if workflow.patched("1361-parallel-ready-steps"):
            return await self._run_dag(steps, inputs, pipeline_id, state, outputs, act)
        return await self._run_sequential(steps, inputs, pipeline_id, state, outputs, act)

    # ── #1361 DAG path: one task per step, deps via wait_condition ──────────

    async def _run_dag(self, steps: list, inputs: dict, pipeline_id: str,
                       state: dict, outputs: dict, act: Callable) -> dict[str, Any]:
        """Independent branches overlap; everything else keeps the sequential
        path's exact per-step semantics. Two deliberate deltas, both toward the
        in-process bridge's behavior: (1) a failed/gate-blocked pipeline marks
        never-launched steps ``skipped`` instead of leaving them ``pending``;
        (2) legs already RUNNING when a sibling fails finish and record
        honestly — they were governed and submitted, so their outcome is real."""

        async def run_step(s: dict) -> None:
            sid = s["id"]
            deps = s.get("depends_on", [])
            await workflow.wait_condition(
                lambda: self._abort or all(state[d]["status"] in _TERMINAL for d in deps))
            if state[sid]["status"] != "pending":
                return
            if self._abort:
                state[sid]["status"] = "skipped"
                return
            # #1251 — a dep ROUTED OFF by a false `when` is a normal outcome, so
            # its dependents still run (their wires onto it are optional by the
            # plan-time check). Any other skip/failure still propagates.
            if any(state[d]["status"] in ("failed", "skipped")
                   and state[d].get("verdict") != "routed_off" for d in deps):
                state[sid]["status"] = "skipped"
                return

            # WHEN (ROUTE) + GATE (STOP), both over persisted output, + input resolution.
            prep = await act(PREPARE, s, inputs, outputs)
            if not prep.get("routed_on", True):
                state[sid].update(status="skipped", verdict="routed_off")
                await act(MIRROR, pipeline_id, state, "running")
                return
            if not prep.get("gate_passed", False):
                state[sid].update(status="skipped", verdict="gate_blocked")
                self._failed = True
                self._abort = True   # the #1251-pinned STOP semantic: nothing new launches
                await act(MIRROR, pipeline_id, state, "failed")
                return

            # A `gate:` leg has NO executable (#1180): a PREDICATE gate is complete
            # the moment PREPARE says gate_passed; only an approval gate goes on
            # to park below. Without this, a passed predicate gate fell through
            # to SUBMIT → run_template("gate:<name>") → FileNotFoundError. The
            # in-process bridge never submits gate legs (_submit_step); this
            # owner only short-circuited them INSIDE the approval branch — found
            # live 2026-08-18 on the first predicate-gated pipeline under Temporal.
            if str(s.get("ref", "")).startswith("gate:") and not s.get("approval"):
                state[sid].update(status="done", verdict="passed")
                await act(MIRROR, pipeline_id, state, "running")
                return

            # Trigger gate before submit (durable): approval/manual → signal; on:event → signal.
            # Every wait also releases on abort, so a failed sibling can never
            # leave this workflow parked on a gate that will never matter.
            trigger = str(s.get("trigger", "auto"))
            if s.get("approval") or trigger == "manual":
                state[sid]["status"] = "awaiting_approval" if s.get("approval") else "awaiting_advance"
                await act(MIRROR, pipeline_id, state, "paused")
                await workflow.wait_condition(
                    lambda sid=sid: sid in self._approved or sid in self._rejected or self._abort)
                if await self._rejected_at_gate(sid, state, pipeline_id, act):
                    return
                if self._abort and sid not in self._approved:
                    state[sid]["status"] = "skipped"
                    return
                # #1180 — a `gate:` leg has no executable: the approval IS the step.
                if str(s.get("ref", "")).startswith("gate:"):
                    state[sid].update(status="done", verdict="approved")
                    await act(MIRROR, pipeline_id, state, "running")
                    return
            elif trigger.startswith("schedule:"):
                cron = trigger[len("schedule:"):]
                state[sid]["status"] = "awaiting_schedule"
                await act(MIRROR, pipeline_id, state, "running")
                delay = await act(NEXT_TICK, cron, workflow.now().isoformat())
                await workflow.sleep(timedelta(seconds=max(0.0, float(delay))))
                if self._abort:
                    state[sid]["status"] = "skipped"
                    return
            elif trigger.startswith("on:"):
                ev = trigger[len("on:"):]
                state[sid]["status"] = "awaiting_event"
                await act(MIRROR, pipeline_id, state, "running")
                await workflow.wait_condition(lambda ev=ev: ev in self._events or self._abort)
                if self._abort and ev not in self._events:
                    state[sid]["status"] = "skipped"
                    return

            # SUBMIT the step as its own governed run + AWAIT its terminal output.
            state[sid]["status"] = "running"
            await act(MIRROR, pipeline_id, state, "running")
            is_flow = str(s.get("ref", "")).startswith("flow:")
            submit_to = _AWAIT_TO if is_flow else _SUBMIT_TO
            # #1429 — heartbeat-timed so a dead worker is retried in minutes,
            # not at the 12h ceiling. Patched: an execution recorded before this
            # replays its original activity options.
            hb = (_LEG_HEARTBEAT_TO
                  if workflow.patched("1429-leg-heartbeat-timeout") else None)
            while True:
                try:
                    run_id = await act(SUBMIT, s["ref"], prep.get("resolved", {}), pipeline_id,
                                       sid, s.get("environment"), s, to=submit_to,
                                       heartbeat=(hb if is_flow else None))
                    break
                except ActivityError as e:
                    # #2299 — a flow leg whose approval gate is required (risk / policy /
                    # profile) PARKS like a declared `approval:` step, never fails. Replay-
                    # safe without a patch marker: this branch depends only on the
                    # activity's outcome, which recorded histories never contain.
                    cause = e.cause
                    if isinstance(cause, ApplicationError) and cause.type in _PERMANENT_REFUSALS:
                        # #2223 — fail the step where a human looks (the console), with the
                        # refusal as its verdict, instead of crashing the workflow and leaving
                        # the read-model "running". Replay-safe like the park below.
                        state[sid].update(status="failed", verdict=cause.type)
                        self._failed = True
                        self._abort = True
                        await act(MIRROR, pipeline_id, state, "failed")
                        return
                    if not (isinstance(cause, ApplicationError) and cause.type == NEEDS_APPROVAL):
                        raise
                    # a FRESH approval is needed: an earlier release of this step (its
                    # declared `approval:`) must not satisfy this wait twice, or a gate
                    # that still refuses would spin SUBMIT in a loop
                    self._approved.discard(sid)
                    state[sid]["status"] = "awaiting_approval"
                    await act(MIRROR, pipeline_id, state, "paused")
                    await workflow.wait_condition(
                        lambda sid=sid: sid in self._approved or sid in self._rejected or self._abort)
                    if await self._rejected_at_gate(sid, state, pipeline_id, act):
                        return
                    if self._abort and sid not in self._approved:
                        state[sid]["status"] = "skipped"
                        return
                    state[sid]["status"] = "running"
                    await act(MIRROR, pipeline_id, state, "running")
            state[sid]["run_id"] = run_id
            res = await act(AWAIT_RUN, run_id, to=_AWAIT_TO, heartbeat=hb)
            if res.get("status") != "done":
                state[sid].update(status="failed", verdict=res.get("verdict"))
                self._failed = True
                self._abort = True
                await act(MIRROR, pipeline_id, state, "failed")
                return
            state[sid].update(status="done", verdict=res.get("verdict"))
            outputs[sid] = res.get("output", {}) or {}
            if s.get("emits"):
                await act(EMIT, s["emits"], outputs[sid], pipeline_id, sid)

        # gather() creates the tasks in plan order — Temporal's workflow event
        # loop schedules them deterministically, so this replays.
        await asyncio.gather(*(run_step(s) for s in steps))
        status = "failed" if self._failed else "done"
        await act(MIRROR, pipeline_id, state, status)
        return {"status": status, "step_state": state}

    async def _rejected_at_gate(self, sid: str, state: dict, pipeline_id: str,
                                act: Callable) -> bool:
        """ADR-0156 D5 — if a person rejected this gate, end the step ``skipped`` /
        ``rejected``, stop launching, mirror ``failed``, and tell the caller to return.
        A reject wins over an approve that raced it. Only the DAG path takes the signal:
        no history recorded before it contains one, so replay is unchanged."""
        if sid not in self._rejected:
            return False
        state[sid].update(status="skipped", verdict="rejected")
        self._failed = True
        self._abort = True
        await act(MIRROR, pipeline_id, state, "failed")
        return True

    # ── pre-#1361 sequential pass — KEPT VERBATIM for replay of histories ───
    # recorded before the patch marker. Do not "improve" this body: an old
    # execution replaying here must re-issue the exact command sequence.

    async def _run_sequential(self, steps: list, inputs: dict, pipeline_id: str,
                              state: dict, outputs: dict, act: Callable) -> dict[str, Any]:
        status = "running"
        # plan.steps are topologically ordered (plan_bundle guarantees deps precede a step),
        # so a single forward pass is a valid execution order for the linear + DAG cases.
        for s in steps:
            sid = s["id"]
            deps = s.get("depends_on", [])
            if any(state[d]["status"] in ("failed", "skipped")
                   and state[d].get("verdict") != "routed_off" for d in deps):
                state[sid]["status"] = "skipped"
                continue

            prep = await act(PREPARE, s, inputs, outputs)
            if not prep.get("routed_on", True):
                state[sid].update(status="skipped", verdict="routed_off")
                await act(MIRROR, pipeline_id, state, "running")
                continue
            if not prep.get("gate_passed", False):
                state[sid].update(status="skipped", verdict="gate_blocked")
                status = "failed"
                await act(MIRROR, pipeline_id, state, status)
                break

            trigger = str(s.get("trigger", "auto"))
            if s.get("approval") or trigger == "manual":
                state[sid]["status"] = "awaiting_approval" if s.get("approval") else "awaiting_advance"
                await act(MIRROR, pipeline_id, state, "paused")
                await workflow.wait_condition(lambda sid=sid: sid in self._approved)
                if str(s.get("ref", "")).startswith("gate:"):
                    state[sid].update(status="done", verdict="approved")
                    await act(MIRROR, pipeline_id, state, "running")
                    continue
            elif trigger.startswith("schedule:"):
                cron = trigger[len("schedule:"):]
                state[sid]["status"] = "awaiting_schedule"
                await act(MIRROR, pipeline_id, state, "running")
                delay = await act(NEXT_TICK, cron, workflow.now().isoformat())
                await workflow.sleep(timedelta(seconds=max(0.0, float(delay))))
            elif trigger.startswith("on:"):
                ev = trigger[len("on:"):]
                state[sid]["status"] = "awaiting_event"
                await act(MIRROR, pipeline_id, state, "running")
                await workflow.wait_condition(lambda ev=ev: ev in self._events)

            state[sid]["status"] = "running"
            await act(MIRROR, pipeline_id, state, "running")
            submit_to = _AWAIT_TO if str(s.get("ref", "")).startswith("flow:") else _SUBMIT_TO
            run_id = await act(SUBMIT, s["ref"], prep.get("resolved", {}), pipeline_id, sid, s.get("environment"), s, to=submit_to)
            state[sid]["run_id"] = run_id
            res = await act(AWAIT_RUN, run_id, to=_AWAIT_TO)
            if res.get("status") != "done":
                state[sid].update(status="failed", verdict=res.get("verdict"))
                status = "failed"
                await act(MIRROR, pipeline_id, state, status)
                break
            state[sid].update(status="done", verdict=res.get("verdict"))
            outputs[sid] = res.get("output", {}) or {}
            if s.get("emits"):
                await act(EMIT, s["emits"], outputs[sid], pipeline_id, sid)
        else:
            status = "done"

        await act(MIRROR, pipeline_id, state, status)
        return {"status": status, "step_state": state}


WORKFLOWS = [PipelineWorkflow]
