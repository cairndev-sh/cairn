"""The core PipelineBridge — bundle-pipeline enforcement (ADR-0069 §2, #746 Slice 2).

This is the platform-owned enforcer for bundle pipelines. It is a **sibling of the
dispatcher, NOT of `BusSkillBridge`**: `BusSkillBridge` activates *userspace* pack
skills; a pipeline step transition is a **governance boundary** and so is owned +
enforced by core here. The bundle config is declaration-only (the `PipelinePlan`);
this class reads it and enforces the gated handoff:

    a step's run completes
      → PipelineBridge (subscribed to the core `run.*` terminal firehose) sees it
      → marks the step done + records its verdict
      → for every downstream step whose deps are all done:
          eval `when` (ROUTE) then `gate` (STOP) on the upstream runs' PERSISTED
          output (result_payloads)
            `when` FALSE → the leg is `skipped`/`routed_off` and the pipeline
                   CONTINUES (#1251, ADR-0087 D2 — an optional leg, e.g. a
                   benchmark that only runs when the input asks for it)
            PASS → resolve inputs from persisted output → dispatcher.run_template(...)
                   stamped with bundle_pipeline_id/step_id (its OWN governed run)
            `gate` FAIL → the step is not submitted; the pipeline terminates with a
                   cited reason
      → when every step is done/skipped, the pipeline is done.

A pack can never inject code into the transition — `gate` is a declarative predicate
core evaluates (`eval_gate_predicate`), and inputs are resolved by `resolve_step_inputs`
from the durable output, never an in-graph channel. Reuses the bus as *transport* only.

Durability: the :class:`PipelineRecord` (plan snapshot + step_state + cursor + status)
is the durable owner; on restart the bridge resumes from it (adopt in-flight children by
`run_id`). This slice wires the in-process path; the Temporal-owner variant (ADR-0036)
is Phase 4.
"""
from __future__ import annotations

import asyncio
import functools
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from cairn.core.engine.pipeline_loop import iteration_view
from cairn.core.engine.bundle import (
    WireResolutionError,
    eval_gate_predicate,
    resolve_step_inputs,
)
from cairn.core.engine.expr import step_field as _step_field  # ONE lookup (#1118)
from cairn.core.dataflow.engine_registry import resolve_engine
from cairn.core.dispatch.coordination import CriticalSections
from cairn.core.dispatch.leg_executor import (
    LegOutcome,
    cancel_recorded_job,
    execute_leg,
    leg_kind_of,
    recorded_stage_contracts,
)
from cairn.core.dispatch.leg_hooks import (
    FlowLegHooks, audit_bound, audit_flow_leg_finished, audit_leg_finished, audit_leg_submitted,
    audit_pipeline_event, budget_cap_usd, compute_target_for_kind, estimate_evidence, flow_leg_record,
    leg_price_kwargs, pipeline_spent_usd,
)
from cairn.core.dispatch.pipeline_resume import plan_resume
from cairn.core.dispatch.target_concurrency import in_flight_on_target, target_cap
from cairn.core.workspace import WorkspaceScopeRequired, workspace_scope
from cairn.core.engine.pipeline_gate import GATE_STATUS, gate_context_for
# #1791 — the approval-notification subscriber. Core→core; `gate_notify` pulls
# the profile and the notifier registry lazily, so this adds no import weight.
from cairn.core.gate_notify import (
    emit_gate_event,
    failure_dedupe_key,
    pause_dedupe_key,
    release_commands,
)
from cairn.core.notifier import NotifyEvent
# #222 — one pipeline, one trace: the root span's W3C context is stored on the record.
from cairn.core.obs.telemetry import (
    current_trace_id, span, trace_carrier, tracing_active, use_parent,
)
# ADR-0125 — the owner lock (D2) and the owner verdict (D3). Core → core; neither
# imports dispatch, so this is not a cycle.
from cairn.core import liveness
from cairn.core.runtime.reconciler import ALIVE, UNDETERMINED, owner_verdict
from cairn.core.runtime.retry import RetryPolicy
from cairn.core.storage.pipeline_store import (
    PipelineRecord,
    StepState,
    default_pipeline_store,
    new_pipeline_id,
)
from cairn.core.storage.protocols import ACTIVE_STATUSES, PIPELINE_ID_KEY, SCAN_COUNT_LIMIT
# The gate signal both leg kinds raise (#1759/#1760). Top-level: `approval`
# imports nothing from `dispatch`, so this is not a cycle — only the two
# *enforcers* below are lazy (they pull the operator/dataflow stacks).
from cairn.core.approval.enforce import NeedsApprovalError
from cairn.core.storage.transitions import PIPELINE_TERMINAL, RUN_TERMINAL

logger = logging.getLogger("cairn.pipeline_bridge")

_TERMINAL_TOPICS = ["run.done", "run.failed", "run.cancelled"]

#: ADR-0086 H5 (#1247) — how long a leg queued behind a target cap waits before
#: re-counting when no terminal event woke it. The bus event is the fast path
#: (sub-millisecond); this poll is the CORRECTNESS floor, because the leg that
#: frees the slot may terminalize in another control-plane process, or be
#: terminalized by H1's orphan reconciler rather than by its own task — neither
#: reaches this bridge's `run.*` subscription. Backs off so a long wait is cheap.
_SLOT_POLL_MIN_S = 1.0

#: #1288 — the poll budget for a flow leg that declares NO `timeout:`. 7 days,
#: deliberately generous: pipelines are long-running by nature, liveness is
#: H1's heartbeat/reconciler and spend is the budget tier — the poll budget
#: exists to stop a leg nothing will ever terminalize, not to police duration.
#: (execute_leg's own 3600s default remains for direct callers/conformance.)
_FLOW_LEG_DEFAULT_BUDGET_S = 7 * 86400.0
_SLOT_POLL_MAX_S = 15.0

#: The verdict on a step skipped by a false ``when`` — a first-class NON-failure
#: (#1251, ADR-0087 D2). Deliberately distinct from ``gate_blocked``, which is
#: also a `skipped` status but means the pipeline is over: one is a route, the
#: other is a stop, and conflating them is the bug this issue fixes.
ROUTED_OFF = "routed_off"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


#: ADR-0125 D5 — the two waits a clock or a topic releases, as opposed to a person.
_TRIGGERED_WAITS = ("awaiting_schedule", "awaiting_event")

#: ADR-0125 D5 — how far back :func:`cron_fire_time` looks for the tick a fire
#: belongs to. APScheduler delivers a fire within its misfire grace of the tick,
#: so the first probe (the fire's own minute) answers; the bound only caps a fire
#: that was somehow delivered a day late.
_CRON_LOOKBACK_MINUTES = 24 * 60


def cron_fire_time(cron: str, now: datetime) -> datetime:
    """ADR-0125 D5 — the scheduled tick a ``schedule:<cron>`` fire at ``now``
    belongs to: the latest tick at or before ``now``, in UTC, to the second.

    APScheduler does not hand a job its scheduled run time, so the bridge
    derives it from the same trigger :meth:`PipelineBridge._schedule_step`
    registers — same parser, same zone — by asking it for the first tick at or
    after the fire's own minute and stepping back a minute while that is not
    the minute itself. A cron that does not parse keys on the fire's own second,
    so the write-ahead still happens."""
    try:
        from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415 — scheduler tier dep

        trigger = CronTrigger.from_crontab(cron.strip())
        probe = now.astimezone(trigger.timezone).replace(second=0, microsecond=0)
        for _ in range(_CRON_LOOKBACK_MINUTES):
            if trigger.get_next_fire_time(None, probe) == probe:
                return probe.astimezone(timezone.utc)
            probe -= timedelta(minutes=1)
    except Exception:  # noqa: BLE001 — an unparseable cron never blocks the fire
        logger.debug("cron %r: could not derive the fire time", cron, exc_info=True)
    return now.astimezone(timezone.utc).replace(microsecond=0)


def step_fire_key(trigger: str, fired_at: datetime) -> str:
    """ADR-0125 D5 — ``"<trigger>@<fire time, UTC, second precision>"``: the
    cron tick for a ``schedule:`` trigger, the receipt second for an ``on:``
    event. The key records only that a fire began, so no event id is needed and
    the ``Message`` contract is unchanged."""
    if trigger.startswith("schedule:"):
        at = cron_fire_time(trigger[len("schedule:"):], fired_at)
    else:
        at = fired_at.astimezone(timezone.utc).replace(microsecond=0)
    return f"{trigger}@{at.isoformat()}"


def _dep_satisfied(st: Any) -> bool:
    """Has this dependency cleared the way for its dependents?

    ``done`` — it ran. ``skipped``/``routed_off`` — it was routed off by a false
    ``when``, which is a normal outcome, so its dependents proceed (their wires
    onto it are guaranteed optional by the plan-time check). Every OTHER skip
    cause (``gate_blocked``) has already failed the pipeline, and is not
    accepted here so a route can never be mistaken for a stop.
    """
    status = getattr(st, "status", "")
    return status == "done" or (
        status == "skipped" and getattr(st, "verdict", None) == ROUTED_OFF
    )


#: #1791 — a `show:` field whose NAME says it is money. The gate author already
#: chose what an approver should look at; when one of those is a cost, it is the
#: cost context the notification quotes. Nothing is computed here — a pipeline
#: with no cost field simply carries no cost line, which is the honest outcome.
_COST_NAME_HINTS = ("cost", "usd", "spend", "price", "budget")


def _cost_fields(fields: dict) -> dict:
    out = {}
    for k, v in (fields or {}).items():
        if isinstance(v, (int, float)) and any(h in str(k).lower() for h in _COST_NAME_HINTS):
            out[str(k)] = v
    return out


def _audience_of(gate_ctx: Optional[dict]) -> str:
    """The audience line on the notification, when the gate names one (#1128).
    Rendering only — the audience is ENFORCED at the approve door
    (``routers/pipelines.py``, #2393), not here."""
    who = str((gate_ctx or {}).get("approvers") or "")
    return f"approvers: {who}" if who else ""


def _flow_leg_output(s: Any, outcome: Any) -> dict:
    """Map a flow leg's engine stats onto its DECLARED ``produces`` names (#1180).

    v1 contract — deliberate and loud:
    - a produce name with an exact stat-key match takes that value;
    - otherwise, if exactly ONE ``<stage>.output`` stat exists (the flow's
      final product), every unmatched produce maps to it;
    - ambiguity (several ``.output`` stats, no exact match) leaves the name
      unmapped — the downstream leg then fails on a missing input naming what
      it wanted, instead of receiving a silent None.
    """
    produces = list(getattr(s, "produces", None) or [])
    stats = dict(getattr(outcome, "stats", None) or {})
    if not getattr(outcome, "ok", False):
        return {}
    product_keys = [k for k in stats if k.endswith(".output")]
    sole_product = stats[product_keys[0]] if len(product_keys) == 1 else None
    out: dict[str, Any] = {}
    for name in produces:
        if name in stats:
            out[name] = stats[name]
        elif sole_product is not None:
            out[name] = sole_product
    # A flow leg's stats ARE its findings (the ADR-0066 convention): stage
    # prefixes are dotted ("scan.all.pii_rows"), which the `$steps` path
    # walker can never reach, so expose them dot→underscore under `findings`
    # — the scope `_step_lookup` already falls back to. This is what lets a
    # data gate (#1275) ride `$steps.prep.scan_all_pii_rows == 0`, a wire
    # thread `prep.dedup_exact_dropped` into a report leg, and `show:` cite
    # real flow numbers to an approver.
    if stats:
        out["findings"] = {k.replace(".", "_"): v for k, v in stats.items()}
    return out


def _leg_retry_policy(raw: Any) -> Optional["RetryPolicy"]:
    """A leg's ``with: retry: {...}`` block → RetryPolicy (#1244, ADR-0086 H2).

    Accepts the small author-facing keys (``max_attempts``, ``initial_s``,
    ``backoff``, ``max_interval_s``); anything else is ignored rather than
    failing the leg — the policy shape is a tuning knob, not a schema surface.
    ``None``/non-dict → None (execute_leg applies its kind-aware default)."""
    if not isinstance(raw, dict):
        return None
    return RetryPolicy(
        maximum_attempts=int(raw.get("max_attempts", 2) or 2),
        initial_interval_seconds=float(raw.get("initial_s", 1.0) or 1.0),
        backoff_coefficient=float(raw.get("backoff", 2.0) or 2.0),
        maximum_interval_seconds=float(raw.get("max_interval_s", 60.0) or 60.0),
    )


def _leg_run_status(outcome: Any) -> str:
    """Map an engine LegOutcome state onto the Run status enum (#1180 bug 9):
    ``succeeded`` → ``done``; ``cancelled`` passes through; anything else is
    ``failed``. Never returns a value outside the model's literal set."""
    if getattr(outcome, "ok", False):
        return "done"
    return "cancelled" if getattr(outcome, "state", "") == "cancelled" else "failed"


def reconstruct_step_output(run: Any) -> dict:
    """Normalize a completed run's ``result_payloads`` into the single output dict the
    bundle resolvers read (`_step_field`: top-level field, else ``findings.<field>``).

    Two persisted shapes exist and both are handled (later payloads win): the
    initial-completion path dumps ``RunResult`` objects (structured output under
    ``raw_output``); the #738 resume path writes ``{template_name, output, …}`` (the
    final state, which carries ``findings``). Missing/None → ``{}`` (a gate over an
    empty output fails closed)."""
    out: dict[str, Any] = {}
    for p in getattr(run, "result_payloads", None) or []:
        if not isinstance(p, dict):
            continue
        if isinstance(p.get("output"), dict):
            out.update(p["output"])
        elif isinstance(p.get("raw_output"), dict):
            out.update(p["raw_output"])
    return out


def findings_of(output: dict) -> dict:
    """The findings a HUMAN should be shown for one leg's normalized output (#1804).

    ``step_field``'s rule is "top-level field, else ``findings.<field>``" — BOTH
    shapes are first-class, because a flow leg publishes its stats under
    ``findings`` while a TASK leg's operator output lands at the top level. The
    findings doors implemented only the first half, so every task leg read as
    "(no findings)": ``plan-cost`` computed ``usd``/``gpu_hours``/``vram_gb`` and
    the human approving compute spend at the very next gate was shown nothing.

    So: the ``findings`` sub-dict when there is one, else the leg's own top-level
    output — which for a task leg IS what it found. Non-dict/missing → ``{}``,
    never None: a caller that gets None cannot tell "nothing found" from
    "the door is broken".
    """
    if not isinstance(output, dict):
        return {}
    found = output.get("findings")
    if isinstance(found, dict) and found:
        return found
    # a task leg: its operator output is the finding. `findings` itself is
    # dropped if present-but-empty so it never shadows the real fields.
    return {k: v for k, v in output.items() if k != "findings"}


def _verdict_of(run: Any, output: dict) -> Optional[str]:
    """The step's verdict, for step_state — the run's eval verdict if set, else a
    ``verdict`` field in its output (top-level or under findings)."""
    fv = getattr(run, "final_verdict", "") or ""
    if fv:
        return fv
    v = _step_field(output, "verdict")
    return v if isinstance(v, str) else None



#: The `run_template` keyword arguments a one-leg pipeline carries for its leg
#: (ADR-0105 D1) — written by the launch door under `trigger.run_options`.
#: ``lane`` joined them in #2115: a cron fire has always run on the background
#: lane, and a one-leg pipeline that put it back on `realtime` would change what
#: a schedule competes with for concurrency.
_RUN_OPTION_KEYS = ("schema_hash", "resolved_operators", "config_preset", "sandbox",
                    "parent_run", "series_id", "estimated_usd", "lane")


def _run_options(rec: PipelineRecord) -> dict[str, Any]:
    raw = (getattr(rec, "trigger", None) or {}).get("run_options") or {}
    return {k: raw[k] for k in _RUN_OPTION_KEYS if k in raw}

def _in_record_workspace(method):
    """Run a bridge method whose first argument is the pipeline record in that
    record's workspace (``current_workspace()``). A leg is advanced off a bus
    event, re-adopted at boot or resumed from a gate — never reliably inside the
    request that launched it — so the record is what names the workspace, and
    with it the account the leg's connections and secrets resolve in (#1639's
    "from the RECORD" rule, applied to execution)."""

    @functools.wraps(method)
    async def wrapper(self: Any, rec: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            scope = workspace_scope(getattr(rec, "workspace_id", "") or "")
        except WorkspaceScopeRequired:
            # auth on and the record names no workspace: fail closed AND visibly —
            # the leg or the pipeline ends failed with the reason, nothing runs.
            # `_REFUSED` tells a `_submit_step` caller to stop, as a blocked gate does.
            await self._refuse_unscoped(method.__name__, rec, *args, **kwargs)
            return _REFUSED
        with scope:
            return await method(self, rec, *args, **kwargs)

    return wrapper


def _launch_root(plan: Any, *, bundle_ref: str, workspace_id: str,
                 environment: Optional[str]) -> tuple[str, dict[str, str]]:
    """#222 — the pipeline's id and the W3C context of its root span.

    The root is a SHORT ``cairn.pipeline`` span opened and closed at launch: a gate
    wait can outlive the request, the process and the server, so no span stays open
    for it. Its context is what the record stores; every later span (legs, gates,
    flow stages, the terminal ``cairn.pipeline.settle``) starts as its child.
    ``{}`` when tracing is off."""
    pid = new_pipeline_id()
    with span("cairn.pipeline", {
        "pipeline": getattr(plan, "name", ""), "pipeline_ref": bundle_ref,
        "content_hash": getattr(plan, "content_hash", ""), "run_id": pid,
        "workspace_id": workspace_id, "environment": environment,
        "legs": len(getattr(plan, "steps", None) or []),
    }):
        return pid, trace_carrier()


def _in_pipeline_trace(method):
    """Run a bridge method whose first argument is the pipeline record with the
    record's stored trace context as the ambient parent, so the legs it starts (and
    the tasks they spawn, the dispatcher's ``cairn.run`` among them) join the
    pipeline's trace even when this is a later request or another process."""

    @functools.wraps(method)
    async def wrapper(self: Any, rec: Any, *args: Any, **kwargs: Any) -> Any:
        with use_parent(getattr(rec, "trace_context", None)):
            return await method(self, rec, *args, **kwargs)

    return wrapper


def _flow_leg_traced(method):
    """``cairn.flow_leg`` around ``_run_flow_leg``: a child of the pipeline root, the
    parent of the leg's ``cairn.flow_stage`` spans. Zero work when tracing is off."""

    @functools.wraps(method)
    async def wrapper(self: Any, rec: Any, s: Any, run: Any, *args: Any, **kwargs: Any) -> Any:
        if not tracing_active():
            return await method(self, rec, s, run, *args, **kwargs)
        try:
            engine = getattr(self._leg_engine(s), "name", "") or ""
        except Exception:  # noqa: BLE001 — a label, never a reason to fail the leg
            engine = ""
        budget = ((getattr(rec, "governance", None) or {}).get("budget") or {}).get("usd")
        with span("cairn.flow_leg", {
            "flow": s.ref, "step": s.id, "leg_run_id": getattr(run, "id", ""),
            "run_id": rec.pipeline_id, "engine": engine, "budget_usd": budget,
            "adopted": bool(kwargs.get("adopt")),
        }, parent=getattr(rec, "trace_context", None)) as sp:
            try:
                return await method(self, rec, s, run, *args, **kwargs)
            finally:
                try:
                    done = await self.dispatcher.store.get(run.id)
                    if sp is not None and done is not None:
                        sp.set_attribute("outcome", str(done.status))
                except Exception:  # noqa: BLE001
                    logger.debug("flow-leg span outcome unreadable", exc_info=True)

    return wrapper


def _settle_span(rec: Any) -> None:
    """``cairn.pipeline.settle`` (#222): the closing child of the root, emitted once, at
    the transition to a terminal status (done / failed / cancelled)."""
    if tracing_active():
        with span("cairn.pipeline.settle", {"run_id": rec.pipeline_id, "status": rec.status},
                  parent=getattr(rec, "trace_context", None)):
            pass


def _gate_span(rec: Any, step_id: str, decision: str, actor: str = "") -> None:
    """``cairn.gate`` for one gate decision (#222): started when the pause began
    (``awaiting_since``) and ended now, at the decision, so its duration is the wait
    — emitted by whichever request or process decides, as a child of the stored
    root. A data gate (a predicate) has no wait: it is an instant span."""
    if not tracing_active():
        return
    st = (getattr(rec, "step_state", None) or {}).get(step_id)
    waiting = getattr(st, "awaiting_since", None)
    status = str(getattr(st, "status", "") or "")
    kind = status.removeprefix("awaiting_") if status.startswith("awaiting_") else "data"
    with span("cairn.gate", {
        "step": step_id, "kind": kind, "decision": decision, "actor": actor or None,
        "run_id": rec.pipeline_id,
    }, parent=getattr(rec, "trace_context", None), start_time=waiting):
        pass


_NO_WORKSPACE_REASON = "record names no workspace"
_NO_WORKSPACE_VERDICT = "workspace_unresolved"
#: What a refused ``_submit_step`` returns (it otherwise returns ``None``): the
#: pipeline is already failed and persisted, so the caller stops right there.
#: Both refusals return it: a record that names no workspace, and a wire that
#: cannot resolve.
_REFUSED = False


async def _close_task_reservation(reservation: Any, cost: dict) -> None:
    """Close a task leg's hold the way ``execute_leg`` closes a flow attempt's:
    settle at the measured spend, or release when nothing was spent."""
    if reservation is None:
        return
    from cairn.core.budget_reservations import release, settle  # noqa: PLC0415 - as execute_leg

    actual = float((cost or {}).get("total_usd") or 0.0)
    if actual > 0:
        await settle(reservation, actual)
    else:
        await release(reservation)


class PipelineBridge:
    """Core-owned enforcer that advances bundle pipelines off run completions."""

    def __init__(self, dispatcher: Any, *, store: Any = None, bus: Any = None,
                 artifact_store: Any = None) -> None:
        self.dispatcher = dispatcher
        self._store = store  # resolved lazily so merely constructing a bridge opens no DB
        self._bus = bus
        # H6b/c (#1248) — the checkpoint registry. None → resolved lazily from
        # CAIRN_STORAGE (a Null store when unset: relay and resume both no-op).
        self._artifact_store = artifact_store
        self._sub = None
        # #1532 part 2 — the per-pipeline critical section, fleet-wide under
        # Postgres (CAIRN_STORAGE selector), process-local otherwise. The owner
        # decision (2026-08-24): coordination lives on Postgres so scaling the
        # control plane never requires Temporal.
        self._sections = CriticalSections(namespace="pipeline")
        # run_id → pipeline_id for the child runs WE launched. The firehose carries
        # every run.* terminal; we filter on this map so a non-pipeline run never
        # touches the run store (which would race concurrent store ops — asyncpg
        # allows one operation per connection). In-memory only; a restart re-adopts
        # in-flight children from the store (adopt-by-id, ADR-0069 resume / Phase 4).
        self._run_to_pipeline: dict[str, str] = {}
        # #1218 — in-flight flow-leg tasks (run_id → task). A flow leg executes in a
        # BACKGROUND task so launch/advance return at submit, not at leg completion
        # (a cold readiness leg held POST /pipelines/launch — and the per-pipeline
        # lock — for 20-30s+). Held here so the tasks aren't garbage-collected
        # mid-flight; stop() cancels them (a restart re-adopts from the store).
        self._leg_tasks: dict[str, asyncio.Task] = {}
        #: #233 — pipelines whose terminal row (`pipeline.finished`) this process wrote
        self._audited_terminal: set[str] = set()
        # ADR-0086 H5 (#1247) + #1532p2 — per-target count→admit critical
        # section. Was a single process-local lock, honestly documented as "a
        # brake, not a distributed mutex"; with the #1531 store-side count
        # inside a per-TARGET fleet-wide section, the cap IS the mutex now:
        # two replicas racing the same last slot serialize on Postgres and
        # count the same store. (Per-target rather than global on purpose —
        # different targets never contend for the same slots.)
        # `_target_free` stays exactly what its wait-loop always documented:
        # a LOCAL fast wake; the polling floor in _await_target_slot is what
        # is correct cross-process, and already was before #1532.
        self._admit = CriticalSections(namespace="target-admit")
        self._target_free: dict[str, asyncio.Event] = {}
        # ADR-0069 §3c event-driven triggers.
        self._scheduler: Any = None  # lazy AsyncIOScheduler for `schedule:<cron>` steps
        self._event_subs: dict[str, Any] = {}  # domain-event topic → bus Subscription
        self._event_waiters: dict[str, set[tuple[str, str]]] = {}  # topic → {(pipeline_id, step_id)}
        # #763: detached tasks that replay a durable bus's missed events on (re)subscribe;
        # + a one-shot flag so the "in-process bus isn't durable" warning fires once.
        self._replay_tasks: set[asyncio.Task] = set()
        self._warned_inprocess_event = False
        # #1791 — the notifier set gate events fan out to. None → the process
        # default (`default_gate_registry`), which is the same stdout + inbox +
        # bound `notify.sink` set the runner already uses. Injectable for tests.
        self._notify_registry: Any = None
        # ADR-0125 D2 — set when start() found this process without its owner
        # lock: adoption waits for the lock (`_on_owner_reacquired`).
        self._adopt_deferred = False
        # ADR-0125 D4 hand-off: the dead-owner legs this process keeps re-mapped
        # (nothing here can finish them) and has said so once (log, not audit).
        self._kept_remap_logged: set[str] = set()
        self._owner_cb_remove: Any = None

    @property
    def store(self) -> Any:
        """The durable pipelines store — resolved on first use (default selects sqlite/
        postgres via CAIRN_STORAGE), so constructing a bridge is side-effect-free."""
        if self._store is None:
            self._store = default_pipeline_store()
        return self._store

    @store.setter
    def store(self, value: Any) -> None:
        self._store = value

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Subscribe to the core run-terminal firehose. Unlike `BusSkillBridge`, the
        bridge is core and MAY consume `run.*` (the reserved guard is a userspace
        loop-guard); it filters to runs stamped with a `bundle_pipeline_id`."""
        from cairn.core.bus import get_bus  # noqa: PLC0415 — lazy like BusSkillBridge

        bus = self._bus or get_bus()
        self._sub = await bus.subscribe(_TERMINAL_TOPICS, self._on_run_terminal)
        lv = liveness.current()
        if lv is not None and self._owner_cb_remove is None:
            self._owner_cb_remove = lv.on_reacquired(self._on_owner_reacquired)
        if liveness.may_start_legs():
            await self._adopt_in_flight()  # ADR-0069 Phase 4 — resume pipelines after a restart
        else:
            # ADR-0125 D2 — without its owner lock this process adopts nothing:
            # a leg it took over would name an owner nobody can prove alive.
            self._adopt_deferred = True
            logger.warning("pipeline bridge: owner lock not held — adoption waits for it")
        logger.info("pipeline bridge subscribed to %s", _TERMINAL_TOPICS)

    async def _on_owner_reacquired(self) -> None:
        """ADR-0125 D2 — this process holds its owner lock again: run the
        adoption start-up skipped, then one advance pass over the pipelines whose
        legs were held back meanwhile."""
        if self._adopt_deferred:
            self._adopt_deferred = False
            await self._adopt_in_flight()
        lv = liveness.current()
        for pid, released in (lv.take_held_back() if lv is not None else {}).items():
            try:
                await self._advance_held_back(pid, released)
            except Exception:  # noqa: BLE001 — one pipeline must not cost the others
                logger.exception("pipeline %s: advance after re-acquiring the owner lock "
                                 "failed", pid)

    async def _advance_held_back(self, pipeline_id: str, released: set[str]) -> None:
        """Submit what was held back on ``pipeline_id`` (:meth:`_hold_back_leg`):
        a step a person had already released is submitted as that door would
        have, without asking again; every other ready step by an advance pass."""
        async with self._lock(pipeline_id):
            rec = await self.store.get(pipeline_id)
            if rec is None or rec.status not in ("running", "paused") or rec.plan is None:
                return
            raw = await self._collected_outputs(rec)
            for sid in sorted(released):
                s, st = rec.plan.step(sid), rec.step_state.get(sid)
                if s is not None and st is not None and st.status == "pending":
                    if await self._submit_step(
                            rec, s, iteration_view(raw, sid, rec.plan.loops or {})) is _REFUSED:
                        return  # the pipeline is failed and persisted
            rec.status = "running"  # `_advance`'s `_settle` recomputes it
            await self._advance(pipeline_id, _rec=rec)

    async def _hold_back_leg(self, rec: PipelineRecord, s: Any) -> None:
        """ADR-0125 D2 — this process does not hold its owner lock, so it starts
        no in-process leg: the step stays ``pending`` (no launch is refused, no
        API errors) and the pipeline is recorded for the advance pass that
        re-acquiring the lock runs. A step a person released (approve/advance,
        a fired schedule or event) is remembered as released."""
        st = rec.step_state.get(s.id) or StepState()
        released = st.status.startswith("awaiting_")
        st.status = "pending"
        rec.step_state[s.id] = st
        lv = liveness.current()
        if lv is not None:
            lv.hold_back(rec.pipeline_id, s.id, released=released)
        await self._audit(rec, s.id, "leg_held_back", {
            "reason": "this process does not hold its owner lock (ADR-0125)"})

    async def _take_leg_ownership(self, run_id: str) -> None:
        """ADR-0125 D1 — this process now runs the leg (re-attached, or its slot
        wait re-armed), so its run names this process. Called inside the
        pipeline's critical section, after a fresh read of the run."""
        me = liveness.self_owner()
        if not me:
            return
        try:
            await self.dispatcher.store.update(run_id, owner=me)
        except Exception:  # noqa: BLE001 — the leg still runs; the verdict reads stale
            logger.warning("could not record this process as the owner of %s", run_id,
                           exc_info=True)

    async def _owned_elsewhere(self, run_id: str, probed: dict) -> bool:
        """ADR-0125 D4 — is this leg's run owned by a process that is ALIVE, or
        cannot be judged in one probe (UNDETERMINED)? Such a leg is only
        re-mapped: never #1289-failed, parked, re-attached or re-run. Adopt is
        one-shot, so it never sees DEAD; the reconciler's sweep does, and hands
        the leg back through :meth:`adopt_dead_leg`. A finished run, or one with
        no owner (LEGACY), keeps the #2540 rules."""
        run = await self._leg_run(run_id)
        if run is None or str(getattr(run, "status", "") or "") in RUN_TERMINAL:
            return False
        verdict = await owner_verdict(run, liveness.lock_free,
                                      self_owner=liveness.self_owner(), cache=probed)
        return verdict in (ALIVE, UNDETERMINED)

    async def adopt_dead_leg(self, run: Any) -> bool:
        """ADR-0125 D4 — the reconciler found this leg's owner process DEAD and its
        pipeline running or paused: apply the adopt rules to this one leg, as a
        restart would have — a task leg goes to #1289, a template leg or a flow
        leg without a handle is failed and parked (#2555), a flow leg with a
        handle is re-attached, a finalizer is reset for re-run. A step that never
        recorded the run is claimed first (#2540). Returns True when it acted.

        Under the pipeline's critical section, from a fresh read: nothing is done
        when the run finished or another process took it over since the sweep (its
        owner changed), when the owner holds its lock again (the sweep that judged
        it ran before this section was taken: one more probe here closes that
        window, and a probe that cannot tell is never a reason to act), when
        nothing in this process can finish the leg but it must keep its re-map
        (:meth:`_would_only_remap` — False, no audit, said once per run), or when
        this process does not hold its own owner lock."""
        pid = str(getattr(run, "bundle_pipeline_id", "") or "")
        sid = str(getattr(run, "step_id", "") or "")
        dead_owner = str(getattr(run, "owner", "") or "")
        if not (pid and sid and dead_owner) or not liveness.may_start_legs():
            return False
        rec = await self.store.get(pid)
        if rec is None or (rec.trigger or {}).get("owner") == "temporal":
            return False
        st = rec.step_state.get(sid)
        if st is not None and self._may_own_unrecorded_run(st):
            rec = await self._claim_unrecorded_leg_runs(rec, [run])
        ended: list[Any] = []
        async with self._lock(pid):
            rec = await self.store.get(pid)
            if rec is None or rec.status not in ("running", "paused"):
                return False
            cur = await self._leg_run(str(run.id))
            st = rec.step_state.get(sid)
            if (cur is None or str(getattr(cur, "status", "")) not in ("queued", "running")
                    or str(getattr(cur, "owner", "") or "") != dead_owner
                    or st is None or st.run_id != str(run.id)
                    or st.status not in ("queued", "running")):
                return False
            try:
                free = await asyncio.wait_for(liveness.lock_free(dead_owner),
                                              timeout=liveness.PROBE_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 — undetermined: never a destructive action
                free = None
            if free is not True:
                logger.info("pipeline %s: leg %s's owner %s %s since the sweep that judged "
                            "it dead — leaving the leg alone", pid, sid, dead_owner,
                            "holds its lock again" if free is False else "cannot be probed")
                return False
            if await self._would_only_remap(rec, sid, st, cur):
                # Hand-off churn: the sweep would hand this leg over every
                # interval, and each time the answer is the same re-map.
                self._run_to_pipeline[str(run.id)] = pid
                if str(run.id) not in self._kept_remap_logged:
                    self._kept_remap_logged.add(str(run.id))
                    logger.info("pipeline %s: leg %s's owner %s is dead but the leg keeps "
                                "its re-map — nothing in this process can finish run %s "
                                "(a template run with an active remote job is resumed by "
                                "the remote-job control loop)", pid, sid, dead_owner, run.id)
                return False
            await self._audit(rec, sid, "leg_owner_dead", {
                "run_id": str(run.id), "owner": dead_owner})
            ended = await self._adopt_steps(rec, probed={}, only=sid)
            if getattr(rec, "terminal_intent", ""):
                await self._settle(rec)
            else:
                self._recompute_status(rec)
                await self.store.put(rec)
        for done in ended:  # finished meanwhile: applied like adopt does (#2555)
            await self._apply_terminal(f"run.{done.status}", str(done.id), pid, done,
                                       after_restart=True)
        return True

    async def _adopt_in_flight(self) -> None:
        """Re-adopt in-flight pipelines from the durable store after a (re)start: re-map
        each running step's ``run_id`` so its completion still advances the pipeline
        (adopt-by-id), and re-arm any ``schedule``/``on-event`` step that was waiting —
        unless its fire was already recorded (ADR-0125 D5): then it takes the run the
        fire created, or is parked for a person. Best-effort — a store hiccup must not
        stop the bridge from serving new pipelines.

        ADR-0125 D4 — adopt is not leader-gated, so a replica booting beside a live
        one sees that replica's legs: each in-flight leg's owner is asked first
        (:meth:`_adopt_steps`), and one whose process is alive, or cannot be judged
        in one probe, is only re-mapped."""
        try:
            recs = list(await self.store.list(status="running")) + list(await self.store.list(status="paused"))
        except Exception:  # noqa: BLE001 — degrade to serving only new pipelines
            logger.debug("pipeline bridge: could not list in-flight pipelines to adopt", exc_info=True)
            return
        # #2540 — non-terminal child runs by pipeline, read once and only when
        # some in-process record has a step that may own one it never recorded.
        child_runs: Optional[dict[str, list[Any]]] = None
        probed: dict[str, Optional[bool]] = {}  # ADR-0125 D3 — one probe per owner
        for rec in recs:
            # Temporal-owned pipelines: the durable workflow re-adopts its own in-flight run
            # and owns its schedule timers on replay — the bridge only re-arms the bus→signal
            # forwarding for their on:<event> steps (Slice ③).
            if (rec.trigger or {}).get("owner") == "temporal":
                await self._register_temporal_event_waiters(rec)
                continue
            if child_runs is None and self._has_unrecorded_steps(rec):
                child_runs = await self._active_child_runs()
            if child_runs and child_runs.get(rec.pipeline_id):
                rec = await self._claim_unrecorded_leg_runs(
                    rec, child_runs[rec.pipeline_id])
            # ADR-0125 D1 — inside the pipeline's critical section, from a fresh
            # read: a leg this pass takes over names this process as its owner,
            # and a replica mid-transition on the same pipeline finishes first.
            async with self._lock(rec.pipeline_id):
                rec = await self.store.get(rec.pipeline_id) or rec
                if rec.status in PIPELINE_TERMINAL:
                    continue
                ended = await self._adopt_steps(rec, probed=probed)
            if ended:
                # #2555 (4) — after the loop, so no in-memory put above can undo
                # what the terminal path stores. `after_restart` keeps the
                # adopt-time rule for whatever the advance then makes ready.
                for run in ended:
                    await self._apply_terminal(f"run.{run.status}", str(run.id),
                                               rec.pipeline_id, run, after_restart=True)
                rec = await self.store.get(rec.pipeline_id) or rec
                if rec.status in PIPELINE_TERMINAL:
                    continue
            # #1787 — this pipeline had already decided how it ends when the
            # control plane died, and it still owes its `finally:`. Settling
            # re-submits every pending finalizer and applies the terminal status
            # once they land. No new query and no terminal-table scan: the
            # record is still `running` precisely so this listing finds it.
            if getattr(rec, "terminal_intent", ""):
                async with self._lock(rec.pipeline_id):
                    await self._settle(rec)
            elif any(st.status == "pending" for st in rec.step_state.values()):
                # #2540 — `plan_resume` buckets a `pending` step as RESUBMIT, and
                # until this nothing acted on that bucket: a record whose ready
                # step was still `pending` when the process died sat `running`
                # forever. But "pending" is only what the record SAYS: the leg
                # may have been submitted, and even finished, before the record
                # was put — and its run is not always findable (a terminal run is
                # not scanned; the scan is capped) — and so may a step that only
                # BECOMES ready during this pass (a `when` routed off, a gate
                # leg passing). So this one pass runs with `after_restart`: every
                # step it would submit goes through `_rerun_is_safe`, and a leg
                # that is not safe to repeat is parked for a person instead.
                # From a fresh read under the pipeline's critical section, so a
                # replica that already advanced the step is seen, not repeated.
                async with self._lock(rec.pipeline_id):
                    cur = await self.store.get(rec.pipeline_id)
                    if (cur is not None and cur.status == "running"
                            and not getattr(cur, "terminal_intent", "")):
                        await self._advance(cur.pipeline_id, _rec=cur, after_restart=True)

    async def _adopt_steps(self, rec: PipelineRecord, *, probed: dict,
                           only: Optional[str] = None) -> list[Any]:
        """Adopt one record's in-flight steps (or, with ``only``, one step — the
        reconciler's DEAD-leg handoff, :meth:`adopt_dead_leg`). Returns the runs
        that already finished, for the caller to apply outside the critical
        section (#2555 (4)).

        ADR-0125 D4 — a ``running``/``queued`` leg whose owner process is ALIVE or
        UNDETERMINED is only re-mapped (:meth:`_owned_elsewhere`); every rule
        below applies to a LEGACY leg (no owner) and, through the handoff, to a
        DEAD one."""
        plan_steps = {s.id: s for s in (rec.plan.steps if rec.plan else [])}
        # #1429 — the resume PLAN (#1130) decides what each in-flight step
        # needs; before this it existed and nothing on the run path called
        # it, so a running flow leg was only re-MAPPED here — nobody polled
        # its persisted handle again and the H1 reconciler failed it as an
        # orphan 300s later. `reattach` = running WITH a handle → adopt.
        resume = plan_resume(rec)
        finalizer_ids = {s.id for s in self._finalizer_steps(rec)}
        ended: list[Any] = []   # #2555 (4) — running steps whose run already finished
        for sid, st in rec.step_state.items():
            if only is not None and sid != only:
                continue
            if (only is None and st.status in ("running", "queued") and st.run_id
                    and await self._owned_elsewhere(st.run_id, probed)):
                self._run_to_pipeline[st.run_id] = rec.pipeline_id
                continue
            if sid in finalizer_ids and st.status in ("running", "queued"):
                # #1787 — a finalizer caught mid-flight by a restart is
                # RE-RUN, not parked. `_resume_task_leg`'s fail-closed rule
                # (a non-read_only op is not re-fired without a human) is
                # right for the pipeline's own work and exactly wrong here:
                # parking the teardown for a decision is a box billing until
                # somebody notices, which is the failure this feature exists
                # to end. A finalizer is idempotent by contract (ADR-0097) —
                # `compute/release` on a stopped instance is a no-op success.
                await self._reset_finalizer_for_rerun(rec, sid, st)
                continue
            if st.status == "running" and st.run_id:
                # #1289 — a task leg is IN-PROCESS: its op died with the
                # previous process and its run can never terminalize (no
                # engine to poll, no heartbeat stamped). Re-mapping the
                # run_id would stall the pipeline forever; resume by the
                # op's RISK instead.
                s = plan_steps.get(sid)
                kind = leg_kind_of(getattr(s, "ref", "") or "") if s is not None else ""
                run = None
                # #2555 (4) — but first, what the run itself says. A leg that
                # FINISHED before the crash (its terminal written, its event
                # never applied) is settled through the normal terminal path
                # below, once this record's other steps are handled: its
                # output is used, nothing is re-run and nothing is parked.
                # A flow leg WITH a handle too: re-adopting a finished run
                # watches nothing (`_readopt_flow_leg` needs `running`).
                if kind in ("task", "flow", "template"):
                    run = await self._leg_run(st.run_id)
                    if str(getattr(run, "status", "") or "") in RUN_TERMINAL:
                        ended.append(run)
                        continue
                if kind == "task":
                    await self._resume_task_leg(rec, s, st)
                else:
                    self._run_to_pipeline[st.run_id] = rec.pipeline_id
                    if s is not None and sid in resume.reattach and kind == "flow":
                        await self._readopt_flow_leg(rec, s, st)
                    elif s is not None and await self._unadoptable(kind, st, run):
                        await self._park_unadoptable_leg(rec, s, st, kind)
            elif st.status == "queued" and st.run_id:
                # ADR-0086 H5 (#1247) — a leg that was WAITING for a slot on
                # its compute target never reached an engine, so there is
                # nothing to re-attach to and nothing paid to protect: re-arm
                # the wait in this process. Without this the leg would sit
                # `queued` forever and strand its pipeline — the exact class
                # of stall ADR-0086 exists to close.
                self._run_to_pipeline[st.run_id] = rec.pipeline_id
                await self._resume_queued_leg(rec, plan_steps.get(sid), st)
            elif st.status in _TRIGGERED_WAITS:
                s = plan_steps.get(sid)
                if s is None:
                    continue
                if st.fired_key and not st.run_id:
                    # ADR-0125 D5 — a fire began (its key was put) and the claim
                    # found no run for it: the process died between the
                    # write-ahead and the submit, or the leg finished before the
                    # crash. Never re-armed silently: parked for a person.
                    await self._park_fired_step(rec, s, st)
                elif st.status == "awaiting_schedule" and s.trigger.startswith("schedule:"):
                    self._schedule_step(rec.pipeline_id, sid, s.trigger[len("schedule:"):])
                elif st.status == "awaiting_event" and s.trigger.startswith("on:"):
                    await self._subscribe_event(rec.pipeline_id, sid, s.trigger[len("on:"):])
        return ended

    def _rerun_is_safe(self, s: Any) -> bool:
        """May this leg run again without a person deciding? (#2540)

        A gate leg has no executable, and a task leg whose op is ``read_only``
        is the one case #1289 already re-fires unattended — by the same risk
        lookup (:meth:`_task_op_risk`). Everything else (a write or unknown-risk
        op, a flow leg, a template leg) may have already had its effect."""
        kind = leg_kind_of(getattr(s, "ref", "") or "")
        if kind == "gate":
            return True
        return kind == "task" and self._task_op_risk(s) == "read_only"

    async def _park_after_restart(
        self, rec: PipelineRecord, s: Any,
        reason: str = "the control plane restarted before this step's state "
                      "was recorded; it may already have run — advance to run it",
        detail: Optional[dict] = None,
    ) -> None:
        """#2540 — park a step the adopt-time ``_advance`` pass would submit but
        that is not safe to repeat (:meth:`_rerun_is_safe`).

        The record says ``pending`` (no run id), yet the process may have died
        after the leg ran: a leg's run is written, and may finish, before the
        record that would show it is put. The step waits ``awaiting_advance`` for
        the EXISTING advance door, as #1289 parks a write op. Mutates ``rec`` in
        memory only — ``_advance``'s ``_settle`` puts it. #2555 (5): the person
        is told, through the gate notification every other human wait sends."""
        st = rec.step_state.get(s.id) or StepState()
        st.status = "awaiting_advance"
        st.awaiting_since = _now()
        rec.step_state[s.id] = st
        await self._audit(rec, s.id, "leg_parked_after_restart", {
            "leg_kind": leg_kind_of(getattr(s, "ref", "") or ""), "reason": reason,
            **(detail or {}),
        })
        await self._notify_parked(rec, s, reason)

    async def _park_fired_step(self, rec: PipelineRecord, s: Any, st: Any) -> None:
        """ADR-0125 D5 — a ``schedule:``/``on:`` step whose fire was recorded
        (``fired_key``, the write-ahead) but whose run the #2540 claim did not
        find. The process died between the write-ahead and the submit, or the
        leg ran and finished before the crash (a terminal run is not scanned):
        the same "may already have run" trade #2540 makes, so the step is parked
        ``awaiting_advance`` for the EXISTING advance door, never re-armed and
        never left waiting forever. Called under the pipeline's critical section
        from :meth:`_adopt_steps`; puts the record itself, as
        :meth:`_park_unadoptable_leg` does, because no advance pass follows a
        step that is not ``pending``."""
        await self._park_after_restart(rec, s, reason=(
            f"the control plane restarted after this step's fire ({st.fired_key}) was "
            "recorded and before its leg was recorded as submitted; the leg may "
            "already have run — advance to run it"),
            detail={"fired_key": st.fired_key, "wait": st.status})
        self._recompute_status(rec)
        await self.store.put(rec)

    async def _notify_parked(self, rec: PipelineRecord, s: Any, reason: str,
                             risk: str = "") -> None:
        """#2555 (5) — a step a restart parked is a human wait like any manual
        step: the same ``awaiting_advance`` notification, with the reason."""
        await self._notify_gate(rec, s.id, "awaiting_advance",
                                operator=str(getattr(s, "ref", "") or ""), risk=risk,
                                authority=("control plane restart (fail-closed resume)",),
                                reason=reason)

    async def _leg_run(self, run_id: str) -> Any:
        """The leg's run as the store holds it now; ``None`` when unreadable."""
        try:
            return await self.dispatcher.store.get(run_id)
        except Exception:  # noqa: BLE001 — unreadable: the existing branches decide
            logger.debug("pipeline bridge: could not read leg run %s", run_id, exc_info=True)
            return None

    async def _would_only_remap(self, rec: PipelineRecord, sid: str, st: Any, run: Any) -> bool:
        """Would :meth:`_adopt_steps` leave this ``running`` leg re-mapped and
        otherwise untouched? Its running-step branch, read without acting: not
        a finalizer (reset for re-run), not a task leg (#1289), not a flow leg
        the resume plan re-attaches, and not :meth:`_unadoptable` (parked) —
        i.e. a template run with an active remote job, or a flow leg with a
        handle the plan does not re-attach. :meth:`adopt_dead_leg` asks this
        before it audits and puts, so a leg kept re-mapped costs one log line,
        not an audit event and a record write per sweep."""
        if st.status != "running" or not st.run_id:
            return False
        if sid in {f.id for f in self._finalizer_steps(rec)}:
            return False
        s = rec.plan.step(sid) if rec.plan else None
        kind = leg_kind_of(getattr(s, "ref", "") or "") if s is not None else ""
        if kind == "task":
            return False
        if s is not None and kind == "flow" and sid in plan_resume(rec).reattach:
            return False
        return s is None or not await self._unadoptable(kind, st, run)

    async def _unadoptable(self, kind: str, st: Any, run: Any) -> bool:
        """#2555 (1) — a ``running`` flow/template leg nothing in this runtime can
        finish. A flow leg without an engine handle has nothing to re-attach to
        (``plan_resume`` calls it orphaned). An in-process template run died with
        its process: ``InProcessRuntime`` has no boot recovery, and the one thing
        that resumes a ``running`` template run — the remote-job control loop
        (``operator/job_control.py`` ``build_default_resume`` →
        ``RunDispatcher.resume_run``), for a run paused on a remote job, which
        the dispatcher keeps ``running`` — needs a live remote job for that run.
        So a template run with an ACTIVE remote job keeps the re-map, and so does
        one whose jobs cannot be read (unknown is not dead). Under DBOS/Temporal
        the runtime recovers the template run and the reconciler reports its
        terminal, so those are left as they were. Only a run that still reads
        ``running`` (or is gone) qualifies: a paused or queued run has its own
        path, and a finished one is applied as a terminal."""
        if st.handle or kind not in ("flow", "template"):
            return False
        if run is not None and str(getattr(run, "status", "") or "") != "running":
            return False
        if kind == "flow":
            return True
        from cairn.core import settings  # noqa: PLC0415 — cycle: settings → storage → dispatch
        if settings.runtime() in ("dbos", "temporal"):
            return False
        try:
            from cairn.core.operator.job_control import active_remote_jobs_for_run  # noqa: PLC0415 — off the dispatch import path

            return not await active_remote_jobs_for_run(st.run_id)
        except Exception:  # noqa: BLE001 — unreadable jobs: keep the re-map
            logger.warning("could not read remote jobs of run %s; keeping its re-map",
                           st.run_id, exc_info=True)
            return False

    async def _park_unadoptable_leg(self, rec: PipelineRecord, s: Any, st: Any,
                                    kind: str) -> None:
        """#2555 (1) — #1289's rule for a non-read_only leg, applied to a flow or
        template leg :meth:`_unadoptable` says nothing can finish: the dead run
        is settled ``failed`` (left ``running`` it is a lie no reconciler
        corrects) and the step is parked ``awaiting_advance`` for a person —
        it may already have had its effect, so it is never re-run unasked."""
        try:
            await self.dispatcher.store.update(
                st.run_id, status="failed", ended_at=_now(),
                error=f"in-process {kind} leg died with the control plane restart")
        except Exception:  # noqa: BLE001 — a store hiccup must not stop adoption
            logger.debug("%s leg %s: could not terminalize dead run %s",
                         kind, s.id, st.run_id, exc_info=True)
        self._run_to_pipeline.pop(st.run_id, None)
        reason = (f"restart mid-leg; this {kind} leg cannot be re-attached and may "
                  "already have had its effect — advance to run it again")
        if kind == "flow":
            reason = ("restart mid-leg; this flow leg's engine handle was never "
                      "recorded, so an engine job it submitted may still be running "
                      "unwatched — check the engine for an orphaned job (and stop "
                      "it) before you advance to run the leg again")
        await self._park_after_restart(rec, s, reason=reason)
        self._recompute_status(rec)
        await self.store.put(rec)

    @staticmethod
    def _may_own_unrecorded_run(st: Any) -> bool:
        """Could this step own a run it never recorded? A ``pending``/``queued``
        step with no run id (#2540), or — ADR-0125 D5 — a scheduled/on-event
        step still waiting whose fire was written ahead of its submit."""
        return not st.run_id and (st.status in ("pending", "queued")
                                  or (st.status in _TRIGGERED_WAITS and bool(st.fired_key)))

    @classmethod
    def _has_unrecorded_steps(cls, rec: PipelineRecord) -> bool:
        """Does this record have a step that could own a run it never recorded?"""
        return any(cls._may_own_unrecorded_run(st) for st in rec.step_state.values())

    async def _active_child_runs(self, *, workspace_id: Optional[str] = None,
                                 statuses: Any = ACTIVE_STATUSES,
                                 ) -> dict[str, list[Any]]:
        """Non-terminal pipeline child runs, grouped by ``bundle_pipeline_id`` (#2540).

        The run carries its own lineage (``bundle_pipeline_id`` + ``step_id``,
        stamped at creation), so the run store is the one place that knows about
        a leg whose step was never persisted past ``pending``. Scanned through
        :meth:`RunStore.list` per non-terminal status — no new store method —
        capped at :data:`SCAN_COUNT_LIMIT` rows per status, which is a restart /
        cancel path, not a hot one. Best-effort: an unreadable store returns
        ``{}`` and adoption carries on as before."""
        out: dict[str, list[Any]] = {}
        seen: set[str] = set()
        try:
            for status in sorted(statuses):
                for run in await self.dispatcher.store.list(
                        status=status, workspace_id=workspace_id,
                        limit=SCAN_COUNT_LIMIT, include_payloads=False):
                    pid = str(getattr(run, "bundle_pipeline_id", "") or "")
                    rid = str(getattr(run, "id", "") or "")
                    if pid and rid and rid not in seen:
                        seen.add(rid)
                        out.setdefault(pid, []).append(run)
        except Exception:  # noqa: BLE001 — never stop adoption / cancel on a scan
            logger.debug("pipeline bridge: could not scan child runs", exc_info=True)
        return out

    async def _claim_unrecorded_leg_runs(self, rec: PipelineRecord,
                                         runs: list[Any]) -> PipelineRecord:
        """#2540 — give a step back the run it owns but never recorded.

        A leg's run is created (and its op started, in a background task) before
        the record that says the step is ``running`` is persisted: ``_submit_step``
        stamps the step in memory and ``_advance`` puts the record only at its end.
        A process that dies in between — or whose event loop the leg itself
        blocks, as a sync read inside an async op does — leaves the step
        ``pending`` beside a ``running`` run. Resubmitting that step would repeat
        a leg that may already have had its effect.

        So the step takes its run's status and id here, and the EXISTING adopt
        branches below decide what happens next, unchanged: a ``running`` task
        leg goes through :meth:`_resume_task_leg` (the dead run is terminalized
        ``failed``, then #1289's risk rule re-fires a ``read_only`` op or parks
        any other for a human), a ``queued`` leg has its slot wait re-armed, a
        flow/template leg is re-mapped by run id, and a run that a gate had
        paused leaves its step ``awaiting_approval`` for the approve door.

        Done under the pipeline's critical section with a fresh read, so a live
        replica that is mid-``_advance`` on the same pipeline finishes its put
        first and its step is seen as recorded."""
        async with self._lock(rec.pipeline_id):
            cur = await self.store.get(rec.pipeline_id)
            if cur is None or cur.status in PIPELINE_TERMINAL:
                return cur or rec
            changed = False
            # newest first: if a step somehow owns two, the latest attempt is its leg
            for run in sorted(runs, key=lambda r: str(getattr(r, "started_at", "") or ""),
                              reverse=True):
                sid = str(getattr(run, "step_id", "") or "")
                st = cur.step_state.get(sid)
                if st is None or not self._may_own_unrecorded_run(st):
                    continue
                status = str(getattr(run, "status", "") or "")
                was = st.status  # ADR-0125 D5: a fired wait takes its run the same way
                st.status = status if status in ("queued", "awaiting_approval") else "running"
                st.run_id = run.id
                st.started_at = getattr(run, "started_at", None) or _now()
                if st.status == "awaiting_approval":
                    st.awaiting_since = _now()
                cur.step_state[sid] = st
                cur.cursor = sorted(set(cur.cursor) | {sid})
                changed = True
                await self._audit(cur, sid, "leg_run_claimed_after_restart", {
                    "run_id": run.id, "run_status": status,
                    "reason": f"the step was persisted {was} but its run existed",
                    **({"fired_key": st.fired_key} if st.fired_key else {}),
                })
            if changed:
                self._recompute_status(cur)
                await self.store.put(cur)
            return cur

    def stop(self) -> None:
        if self._sub is not None:
            self._sub.unsubscribe()
            self._sub = None
        if self._owner_cb_remove is not None:
            self._owner_cb_remove()
            self._owner_cb_remove = None
        for sub in self._event_subs.values():
            try:
                sub.unsubscribe()
            except Exception:  # noqa: BLE001
                pass
        self._event_subs.clear()
        self._event_waiters.clear()
        for task in list(self._replay_tasks):
            task.cancel()
        self._replay_tasks.clear()
        # #1218 — in-flight leg tasks die with the bridge; a restart re-adopts
        # in-flight pipelines from the store (_adopt_in_flight), same as before.
        for task in list(self._leg_tasks.values()):
            task.cancel()
        self._leg_tasks.clear()
        self._target_free.clear()  # H5 (#1247) — no waiters survive the bridge
        # #1532p2 — release the advisory-lock pools. stop() is sync, so this is
        # best-effort: if no loop is running the process is exiting, and session
        # death frees every advisory lock anyway (that is the design).
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._sections.aclose())
            loop.create_task(self._admit.aclose())
        except RuntimeError:
            pass
        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:  # noqa: BLE001
                pass
            self._scheduler = None

    def _lock(self, pipeline_id: str):
        """The per-pipeline critical section (#1532p2). Same call shape as the
        old per-pipeline ``asyncio.Lock`` — ``async with self._lock(pid):`` —
        but under a Postgres-backed deployment the section is FLEET-wide:
        two replicas advancing one pipeline serialize on a session-scoped
        advisory lock, and a replica that dies mid-section releases it by
        losing its DB session (no lease, no reaper)."""
        return self._sections.hold(pipeline_id)

    async def _start_temporal_pipeline(
        self, plan: Any, *, inputs: dict, bundle_ref: str, environment: Optional[str],
        workspace_id: str, trigger: Optional[dict], admission: Optional[dict] = None,
    ) -> PipelineRecord:
        """Own the pipeline with a durable ``PipelineWorkflow`` (ADR-0069 Phase 4). Creates
        the queued ``PipelineRecord`` read-model up front (so the console shows it
        immediately), then starts the workflow keyed by the pipeline id; the workflow's
        ``pipeline_mirror_state`` activity drives the record forward."""
        from temporalio.client import Client  # noqa: PLC0415 — optional dep (temporal extra)

        from cairn.core import settings  # noqa: PLC0415

        pid, trace_context = _launch_root(
            plan, bundle_ref=bundle_ref, workspace_id=workspace_id, environment=environment)
        rec = PipelineRecord(
            pipeline_id=pid, trace_context=trace_context, bundle_ref=bundle_ref, pipeline_name=getattr(plan, "name", ""),
            status="queued", plan=plan, inputs=dict(inputs or {}), environment=environment,
            workspace_id=workspace_id, trigger={**(trigger or {}), "owner": "temporal"},
            governance=dict(getattr(plan, "governance", None) or {}),  # #1688
            admission=dict(admission or {}),  # #2190 — same decision, both twins
            step_state={s.id: StepState(status="pending") for s in plan.steps},
        )
        rec = await self.store.put(rec)
        client = await Client.connect(settings.temporal_address(), namespace=settings.temporal_namespace())
        await client.start_workflow(
            "cairn.PipelineWorkflow",
            args=[plan.model_dump(), dict(inputs or {}), pid],
            id=pid, task_queue=settings.temporal_task_queue(),
        )
        await self._audit(rec, "", "pipeline_started", {"owner": "temporal", "workflow_id": pid})
        # Slice ③: the workflow owns its schedule timers, but an on:<event> step waits on
        # the cairn bus — forward those events into the durable workflow as signals.
        await self._register_temporal_event_waiters(rec)
        logger.info("started Temporal pipeline %s (%s)", pid, bundle_ref)
        return rec

    # ── start a pipeline ───────────────────────────────────────────────────

    async def start_pipeline(
        self,
        plan: Any,
        *,
        inputs: dict,
        bundle_ref: str = "",
        environment: Optional[str] = None,
        workspace_id: str = "default",
        trigger: Optional[dict] = None,
        admission: Optional[dict] = None,
    ) -> PipelineRecord:
        """Create the durable :class:`PipelineRecord` and submit the ready entrypoint
        step(s). Returns the persisted record.

        Under ``CAIRN_RUNTIME=temporal`` the pipeline is owned by a durable
        PipelineWorkflow instead. The check lives HERE because this is the
        method the ONE launch door (`launch_pipeline`: CLI, console, cron) calls;
        a check anywhere else would let a launch run in-process and non-durable
        under the temporal runtime.
        """
        from cairn.core import settings  # noqa: PLC0415 — cycle: settings → storage → dispatch

        if settings.runtime() == "temporal" and (trigger or {}).get("owner") != "temporal":
            return await self._start_temporal_pipeline(
                plan, inputs=inputs, bundle_ref=bundle_ref,
                environment=environment, workspace_id=workspace_id,
                trigger=trigger, admission=admission,
            )
        pid, trace_context = _launch_root(
            plan, bundle_ref=bundle_ref, workspace_id=workspace_id, environment=environment)
        rec = PipelineRecord(
            pipeline_id=pid, trace_context=trace_context,
            bundle_ref=bundle_ref, pipeline_name=getattr(plan, "name", ""),
            status="running", plan=plan, inputs=dict(inputs or {}),
            environment=environment, workspace_id=workspace_id, trigger=trigger or {},
            # #1688 — the declared terms become part of the durable record, so a
            # resumed or re-adopted pipeline is still bound by the cap it started
            # under rather than by whatever the def says today.
            governance=dict(getattr(plan, "governance", None) or {}),
            admission=dict(admission or {}),
            step_state={s.id: StepState(status="pending") for s in plan.steps},
        )
        rec = await self.store.put(rec)
        async with self._lock(rec.pipeline_id):
            await self._advance(rec.pipeline_id)
        return await self.store.get(rec.pipeline_id)  # type: ignore[return-value]

    # ── react to a step completing ─────────────────────────────────────────

    async def _on_run_terminal(self, msg: Any) -> None:
        run_id = getattr(msg, "correlation_id", None)
        if not run_id:
            return
        # Which pipeline does this terminal belong to? Read it off the MESSAGE
        # first (#1532): the publisher stamps `bundle_pipeline_id` from the run
        # it already holds, so ANY replica can answer — including one that never
        # launched the run. The process-local map stays as the fallback for
        # events published before this key existed (and for the in-process
        # paths that emit their own terminals), which is also why neither branch
        # touches the run store: the firehose carries every run.* terminal, and a
        # store.get per event would race concurrent store ops on a shared
        # asyncpg pool ("another operation is in progress").
        # #1775 — the bus Message carries `payload`; this read `msg.data`, which
        # does not exist, so the #1532 message-borne pipeline id was dead code
        # and correlation depended ENTIRELY on the process-local map. `data`
        # stays as a fallback for any foreign message shape.
        data = getattr(msg, "payload", None) or getattr(msg, "data", None)
        pid = ""
        if isinstance(data, dict):
            pid = str(data.get(PIPELINE_ID_KEY) or "")
        pid = pid or self._run_to_pipeline.get(run_id) or ""
        if not pid:
            return  # not one of our pipeline child runs — ignore
        run = await self.dispatcher.store.get(run_id)
        if run is None:
            return
        await self._apply_terminal(msg.topic, run_id, pid, run)

    async def _apply_terminal(self, topic: str, run_id: str, pid: str, run: Any, *,
                              after_restart: bool = False) -> None:
        """Apply one leg run's terminal to its pipeline — from the bus, or (#2555)
        from adopt for a run that finished before the restart."""
        try:
            await self._handle_terminal(topic, run_id, pid, run, after_restart=after_restart)
        except Exception as e:  # noqa: BLE001 — see #1825
            # The bus contract is at-most-once: it logs and DROPS a handler that
            # raises (§24.10.6). For a generic subscriber that is right; for THIS
            # handler it converted a crash into a pipeline stuck `running`
            # forever with its leg already done — a silent stall that took a
            # class-level spy inside a live engine to diagnose. An exception here
            # is a pipeline outcome, so it fails the pipeline WITH the reason
            # rather than escaping into the drop.
            logger.exception("pipeline %s: terminal handler failed for run %s", pid, run_id)
            try:
                rec = await self.store.get(pid)
                if rec is not None and rec.status not in ("done", "failed", "cancelled"):
                    await self._fail_pipeline(rec, f"terminal handler failed: {type(e).__name__}: {e}")
            except Exception:  # noqa: BLE001 — nothing left to do but log
                logger.exception("pipeline %s: could not record the handler failure", pid)

    async def _handle_terminal(self, topic: str, run_id: str, pid: str, run: Any, *,
                               after_restart: bool = False) -> None:
        """The body of :meth:`_on_run_terminal`, split out so the caller can turn
        any exception into a FAILED pipeline instead of a dropped message.

        #2555 — a terminal applies only while the step still names this run and
        is ``running``/``queued``/``awaiting_approval`` (a claimed run resumed
        through the run door), or was just settled ``skipped``/``cancelled`` by
        ``_settle_cancelled_while_queued`` or ``_settle_human_waits_on_cancel``
        (see :meth:`_terminal_applies`). Anything else is
        late — the step was re-run, parked for a person, or a refused store update
        was announced anyway — and is ignored with a ``late_terminal_ignored``
        audit. ``after_restart``: adopt is applying a terminal it read from the
        run store; the advance that follows keeps the adopt-time re-run check
        (#2540)."""
        # ADR-0086 H5 (#1247) — this run just released whatever slot it held on
        # its compute target. Wake anything queued behind that target BEFORE
        # taking the pipeline lock: the waiter belongs to a different pipeline as
        # often as not, and it re-counts from the store anyway (the event is a
        # wake, never the decision).
        self._release_target(str(getattr(run, "compute_target", "") or ""))
        # ADR-0120 D4 (#2421): one leg settled (its own run record is already terminal).
        # #2555 — fired after the pipeline lock is released (`finally`), so a
        # slow hook still never holds it, and NOT for a terminal ignored as late:
        # a duplicate delivery (adopt racing the bus) is not announced twice.
        announce = True
        try:
            async with self._lock(pid):
                self._run_to_pipeline.pop(run_id, None)  # consumed
                rec = await self.store.get(pid)
                step_id = getattr(run, "step_id", "") or ""
                term = topic.split(".", 1)[1]  # done | failed | cancelled
                prior = rec.step_state.get(step_id) if rec is not None else None
                if rec is not None and not self._terminal_applies(prior, run_id):
                    announce = False
                    if rec.status not in PIPELINE_TERMINAL:
                        await self._audit(rec, step_id, "late_terminal_ignored", {
                            "run_id": run_id, "run_status": term,
                            "step_status": getattr(prior, "status", ""),
                            "step_run_id": getattr(prior, "run_id", "") or ""})
                    return
                if rec is None:
                    announce = False  # no pipeline to settle the leg against
                    return
                if rec.status in PIPELINE_TERMINAL:
                    await self._audit_run_terminal(pid, step_id, term, run)   # #233 — still an outcome
                    return  # a leg finishing after its pipeline ended: announced, no-op
                await self._apply_leg_terminal(rec, pid, step_id, term, run_id, run,
                                               after_restart)
        finally:
            if announce:
                from cairn.core.pack import run_hooks  # noqa: PLC0415 — off the hot path

                await run_hooks.fire(run_hooks.LEG_FINISHED, pipeline_id=pid, run_id=run_id,
                                     step_id=str(getattr(run, "step_id", "") or ""),
                                     status=topic.split(".", 1)[1])

    @staticmethod
    def _terminal_applies(prior: Any, run_id: str) -> bool:
        """#2555 — may this run's terminal settle the step? Only while the step
        still names the run and waits on it: ``running``/``queued``, or
        ``awaiting_approval`` — a paused run the step names (a claimed template
        run, #2540) can be resumed by the run-level door and finish, and that
        ``done`` is the step's; releasing it later would repeat the work.
        ``skipped``/``cancelled`` is the one settled state accepted:
        ``_settle_cancelled_while_queued`` and ``_settle_human_waits_on_cancel``
        write it before the run's ``cancelled`` is emitted, which still decides
        the step."""
        if prior is None or prior.run_id != run_id:
            return False
        return prior.status in ("running", "queued", "awaiting_approval") or (
            prior.status == "skipped" and prior.verdict == "cancelled")

    async def _apply_leg_terminal(self, rec: PipelineRecord, pid: str, step_id: str,
                                  term: str, run_id: str, run: Any,
                                  after_restart: bool) -> None:
        """The step-and-pipeline half of :meth:`_handle_terminal`, run under the
        pipeline lock once the terminal is known to apply."""
        output = reconstruct_step_output(run)
        await self._audit_run_terminal(pid, step_id, term, run)
        st = rec.step_state.get(step_id) or StepState()
        st.run_id = run_id
        st.status = "done" if term == "done" else "failed"
        st.verdict = _verdict_of(run, output)
        st.ended_at = _now()
        rec.step_state[step_id] = st
        rec.cursor = [c for c in rec.cursor if c != step_id]
        if term != "done":
            await self._fail_pipeline(rec, f"step '{step_id}' {term}")
            return
        # #1155 — a loop's LAST body step just finished: decide before advancing,
        # so the next iteration is already in the plan when _advance runs.
        # #1787 — but not while a `finally:` is running: the record still
        # reads `running`, and appending another loop iteration on a
        # cancelled pipeline would restart the very spending the teardown is
        # in the middle of ending.
        if not rec.terminal_intent and await self._decide_loop(rec, step_id):
            return
        if rec.status == "paused":
            # #2555 — the step was `awaiting_approval` on this run, so the record
            # reads paused; `_advance` skips a paused record, and its `_settle`
            # recomputes the status (paused again if another person-wait remains).
            rec.status = "running"
        # Advance FIRST — this arms any downstream on:<event> subscription — THEN publish
        # the step's declared `emits` event, so a same-process subscriber is already
        # listening (a durable bus also replays it, #763; the ordering matters for the
        # in-process bus, which has no replay).
        await self._advance(pid, _rec=rec, after_restart=after_restart)
        step = rec.plan.step(step_id) if rec.plan else None
        if step is not None:
            await self._emit_step_event(rec, step, output)

    async def _audit_run_terminal(self, pid: str, step_id: str, term: str, run: Any) -> None:
        """#233 — ``leg.finished`` for a leg run's terminal. A FLOW leg wrote its own row
        from the engine outcome (:func:`audit_flow_leg_finished`: attempts, engine, cost),
        so only the other kinds are written here."""
        ref = str(getattr(run, "template_ref", "") or "")
        if not audit_bound() or ref.startswith("flow:"):
            return
        await audit_leg_finished(pid, step_id, ref=ref, state=term, run=run,
                                 reason=str(getattr(run, "error", "") or ""))

    async def _decide_loop(self, rec: PipelineRecord, step_id: str) -> bool:
        """Act on a bounded loop whose current iteration just completed (#1155).

        Returns True when the pipeline must NOT advance normally — i.e. the loop
        escalated to a gate or failed. `continue` appends the next iteration and
        returns False so the ordinary ready-step scan submits it.

        The loop runtime (`pipeline_loop`, ADR-0084 D7) decides; this only
        applies the decision. Keeping the policy there and the effects here is
        why the bound stays testable without a bridge.
        """
        plan = rec.plan
        if not plan or not getattr(plan, "loops", None):
            return False
        # which loop (if any) does this step close out?
        name = next((n for n, tail in (plan.loop_tail or {}).items()
                     if tail and tail[-1] == step_id), "")
        if not name:
            return False

        from cairn.core.engine.pipeline import PipelineStep, lower_step  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_loop import next_loop_action  # noqa: PLC0415 - cycle

        loop_step = PipelineStep.model_validate(plan.loops[name])
        iteration = int((rec.loop_iterations or {}).get(name, 1))
        outputs = await self._collected_outputs(rec)
        action = next_loop_action(loop_step, iteration=iteration, outputs=outputs)
        await self._audit(rec, step_id, f"loop_{action.kind}",
                          {"loop": name, "iteration": iteration,
                           "reason": getattr(action, "reason", "") or ""})

        if action.kind == "exit":
            return False                       # converged — downstream proceeds

        if action.kind == "fail":
            await self._fail_pipeline(
                rec, getattr(action, "reason", "") or
                f"loop '{name}' exhausted {loop_step.max_iterations} iterations")
            return True

        if action.kind == "gate":
            # THE safety property: cannot converge → a human decides, in the SAME
            # approval queue as every other gate (#1128's one-queue invariant).
            st = rec.step_state.get(step_id) or StepState()
            st.status = GATE_STATUS
            st.verdict = "loop_exhausted"
            rec.step_state[step_id] = st
            rec.cursor = sorted(set(rec.cursor) | {step_id})
            self._recompute_status(rec)
            await self.store.put(rec)
            return True

        # continue — append the next iteration's steps and re-point the tail
        nxt = action.steps or []
        if not nxt:
            return False
        nxt[0].depends_on = [step_id]          # chain onto the iteration that just ran
        prev_tail = (plan.loop_tail or {}).get(name, [])
        new_ids = [s.name for s in nxt]
        # #1303 stamped the REF here from a hand-copy of plan_pipeline's
        # lowering; #1627 found four more fields the copy had never carried —
        # `produces` (so a flow leg's stats mapped onto no name and the next
        # leg's wire resolved to nothing), the data-gate `predicate`, the HITL
        # `approval` flag, and `optional_inputs` — plus the `<step>.<field>` →
        # `$steps.…` wire lowering. All of it silently applied from iteration 2
        # on. `lower_step` is now the ONE lowering both planners call. Wires are
        # authored against UNSUFFIXED body names (iteration_view aliases them at
        # read time), so that is what `known` holds.
        known = {str(b.get("id") or "")
                 for b in (plan.loops[name].get("steps") or [])}
        known |= {ps.id for ps in plan.steps}
        for s in nxt:
            plan.steps.append(
                lower_step(s, depends_on=list(s.depends_on), known=known))
            rec.step_state[s.name] = StepState(status="pending")
        # anything that waited on the old tail now waits on the new one
        for ps in plan.steps:
            if ps.id not in new_ids and prev_tail and prev_tail[-1] in ps.depends_on:
                ps.depends_on = [new_ids[-1] if d == prev_tail[-1] else d
                                 for d in ps.depends_on]
        plan.loop_tail[name] = new_ids
        rec.loop_iterations[name] = action.iteration
        await self.store.put(rec)
        return False

    # ── the enforcement: submit every newly-ready step (gated) ─────────────

    async def _advance(self, pipeline_id: str, *, _rec: Optional[PipelineRecord] = None,
                       after_restart: bool = False) -> None:
        """Submit every newly-ready step. ``after_restart`` (#2540) is set only
        on the adopt-time pass: there a ``pending`` step may already have run
        before the record was put, so every step this pass (and a gate leg's
        recursion inside it) would submit is checked by :meth:`_rerun_is_safe`
        and parked for a person when it is not safe. Passes driven by run
        events or the advance door never set it."""
        rec = _rec or await self.store.get(pipeline_id)
        if rec is not None and rec.status in PIPELINE_TERMINAL:
            await self._audit_pipeline_finished(rec)   # #233 — ended on a path that skipped `_settle`
        if rec is None or rec.status != "running":
            return
        plan = rec.plan
        raw_outputs = await self._collected_outputs(rec)
        for s in plan.steps:
            # #1787 — a terminal INTENT is already decided: no more of the
            # pipeline's own work may start. Only the finalizers below run, and
            # `_settle` is what runs them. (Without this a deferred failure would
            # keep submitting downstream legs while the teardown was in flight.)
            if rec.terminal_intent:
                break
            # A pass a passed gate leg ran from inside `_submit_step` may have
            # failed and persisted the record already: start nothing more on it.
            if rec.status in PIPELINE_TERMINAL:
                return
            # A finalizer is never dependency-driven — `_run_finalizers` owns it.
            if getattr(s, "finalizer", False):
                continue
            st = rec.step_state.get(s.id) or StepState()
            if st.status != "pending":
                continue
            if not all(_dep_satisfied(rec.step_state.get(d, StepState())) for d in s.depends_on):
                continue  # a dependency (or join arm) hasn't completed yet
            # #1671 — wires, predicates and `show:` all read step outputs BY
            # AUTHORED NAME; inside a loop the name must mean "the latest
            # completed iteration", not "iteration 1 forever". One view per
            # consuming step, shared by every read below.
            outputs = iteration_view(raw_outputs, s.id, plan.loops or {})
            # WHEN — ROUTE (#1251, ADR-0087 D2). A false predicate skips THIS leg
            # and the pipeline carries on; downstream wires onto it were forced
            # optional at plan time, so they resolve to an omitted key. This is
            # checked BEFORE the gate: an optional leg that is routed off never
            # has its governance evaluated, because it never runs.
            if getattr(s, "when", None) and not eval_gate_predicate(
                s.when, bundle_input=rec.inputs, steps=outputs
            ):
                st.status = "skipped"
                st.verdict = ROUTED_OFF
                st.ended_at = _now()
                rec.step_state[s.id] = st
                await self._audit(rec, s.id, "routed_off", {"when": s.when})
                await audit_leg_finished(rec.pipeline_id, s.id, ref=s.ref, state="skipped",
                                         reason=f"routed off: when {s.when}")
                continue  # NOT a failure — the next steps see the skip as satisfied
            # GATE — governance as a trigger condition, evaluated by core over
            # persisted output. Fail → the step never submits; the pipeline
            # terminates with a cited reason (ADR-0069 acceptance).
            if s.gate and not eval_gate_predicate(s.gate, bundle_input=rec.inputs, steps=outputs):
                _gate_span(rec, s.id, "blocked")  # #222
                st.status = "skipped"
                st.verdict = "gate_blocked"
                rec.step_state[s.id] = st
                await self._audit(rec, s.id, "gate_blocked", {"gate": s.gate})
                await audit_leg_finished(rec.pipeline_id, s.id, ref=s.ref, state="skipped",
                                         reason=f"blocked by gate: {s.gate}")
                await self._fail_pipeline(rec, f"step '{s.id}' blocked by gate: {s.gate}")
                return
            # The step is READY (deps done + gate passed). What gates the actual submit?
            #  - approval=true    → wait for HITL approval (strongest gate, any trigger);
            #  - trigger=manual   → wait for an explicit advance;
            #  - schedule:<cron>  → wait for the next cron tick (registered with the scheduler);
            #  - on:<event>       → wait for a domain event on the bus;
            #  - else (auto)      → submit now.
            if s.approval:
                # #1128 — carry the approver's context (gate name, `show:` fields
                # resolved from prior legs, audience, timeout) into the EXISTING
                # queue. No new status, endpoint or store: one approval surface.
                gate_ctx = gate_context_for(
                    s, outputs=outputs, governance=getattr(rec, "governance", None) or {},
                ).as_dict()
                # #2190 (T05) — the approver sees the ADMISSION DECISION beside the
                # gate's own `show:` fields: the same record, no second surface.
                if getattr(rec, "admission", None):
                    gate_ctx["admission"] = dict(rec.admission)
                # #2358 — and what the pipeline has spent against its budget, so
                # the Approvals panel can show cost vs cap on a pipeline gate.
                # ADR-0151 D4 — and, before the first priced leg, what the run is
                # estimated to cost on its box (recorded at launch, never re-derived).
                gate_ctx["evidence"] = {**(gate_ctx.get("evidence") or {}),
                                        **await self._spend_evidence(rec),
                                        **estimate_evidence(rec, s.id)}
                await self._mark_awaiting(rec, s, GATE_STATUS, gate_ctx=gate_ctx)
                # #1468 — the gate firing IS a decision being asked: open its
                # review record (advisory — the queue stays the source of truth).
                await self._open_review(rec, s, gate_ctx)
            elif s.trigger == "manual":
                await self._mark_awaiting(rec, s, "awaiting_advance")
            elif s.trigger.startswith("schedule:"):
                await self._mark_awaiting(rec, s, "awaiting_schedule")
                self._schedule_step(rec.pipeline_id, s.id, s.trigger[len("schedule:"):])
            elif s.trigger.startswith("on:"):
                await self._mark_awaiting(rec, s, "awaiting_event")
                await self._subscribe_event(rec.pipeline_id, s.id, s.trigger[len("on:"):])
            elif after_restart and not self._rerun_is_safe(s):
                await self._park_after_restart(rec, s)
            elif await self._submit_step(rec, s, outputs, after_restart=after_restart) is _REFUSED:
                return  # failed and persisted, like a blocked gate
        await self._settle(rec)

    async def _refuse_unscoped(self, method: str, rec: PipelineRecord, s: Any, *rest: Any,
                               adopt: Optional[dict] = None, **_kw: Any) -> None:
        """The visible half of :func:`_in_record_workspace`'s refusal. A step about
        to be submitted fails its pipeline (audited, as a wire that cannot resolve
        does); a leg already created marks its run failed and emits the terminal
        event its pipeline advances on. A re-adopted flow leg (``adopt``, the
        engine handle a previous owner recorded) also cancels that engine job:
        nothing watches it once the leg is refused."""
        if method == "_submit_step":
            st = rec.step_state.get(s.id) or StepState()
            st.status = "failed"
            st.verdict = _NO_WORKSPACE_VERDICT
            st.ended_at = _now()
            rec.step_state[s.id] = st
            await self._audit(rec, s.id, _NO_WORKSPACE_VERDICT, {"error": _NO_WORKSPACE_REASON})
            # Once: a finalizer reached here because the pipeline is already ending
            # (`_run_finalizers` runs only under a terminal intent); failing it again
            # would re-enter the finalizers. The step is terminal, so it is not retried.
            if not rec.terminal_intent and rec.status not in PIPELINE_TERMINAL:
                await self._fail_pipeline(rec, _NO_WORKSPACE_REASON)
            return
        run = rest[0]
        if adopt:
            try:
                engine = self._leg_engine(s)
            except Exception:  # noqa: BLE001 — no engine to reach: the run still fails below
                logger.exception("leg %s (%s): no engine to cancel the recorded job on", s.ref, run.id)
                engine = None
            await cancel_recorded_job(engine, adopt)
        try:
            await self.dispatcher.store.update(run.id, status="failed", ended_at=_now(),
                                               error=_NO_WORKSPACE_REASON)
            await self.dispatcher._emit(
                run.id, "failed", {"leg": s.ref, "step_id": s.id, "error": _NO_WORKSPACE_REASON},
                pipeline_id=rec.pipeline_id)
        except Exception:  # noqa: BLE001 — nothing left to do but log
            logger.exception("leg %s (%s): failed to terminalize", s.ref, run.id)

    @_in_record_workspace
    @_in_pipeline_trace
    async def _submit_step(self, rec: PipelineRecord, s: Any, outputs: dict, *,
                           after_restart: bool = False) -> Optional[bool]:
        """Resolve a ready step's inputs from persisted output + submit it as its OWN
        governed run (stamped with the pipeline lineage). The data contract never reads
        an in-graph channel.

        Optional wires (`consumes: {x: Report?}`, carried as ``optional_inputs``)
        whose producer was routed off resolve to an OMITTED key, not to ``None``
        — the leg then applies its own default (#1251).

        A non-optional wire that resolves to NOTHING (#1540 — the producer ran
        but has no such field) fails the PIPELINE with the two-sided message as
        the cited reason; letting it propagate would skip the record `put` in
        `_advance` and wedge the run mid-transition. It returns ``_REFUSED``, so
        the caller stops: no further leg starts on the failed record."""
        try:
            resolved = resolve_step_inputs(s, bundle_input=rec.inputs, steps=outputs)
        except WireResolutionError as e:
            await self._audit(rec, s.id, "wire_unresolved", {"error": str(e)})
            await self._fail_pipeline(rec, str(e))
            return _REFUSED
        # #1180 — a GATE leg has no executable: the approval that routed us here
        # IS the step. Mark done and advance dependents; dispatching its name
        # through run_template was the fourth bug the first full E2E found.
        if leg_kind_of(s.ref) == "gate":
            st = rec.step_state.get(s.id) or StepState()
            st.status = "done"
            # #1275 — a DATA gate (plan `gate` predicate, approval=False)
            # reaches here only after _advance evaluated it TRUE: it resolves
            # instantly, verdict `data`, audited with the predicate as the
            # answerer. A FALSE predicate never gets here (gate_blocked →
            # _fail_pipeline in _advance — the #1251-pinned STOP semantic).
            data_gate = bool(getattr(s, "gate", None)) and not s.approval
            st.verdict = "data" if data_gate else "approved"
            st.ended_at = _now()
            rec.step_state[s.id] = st
            rec.cursor = [c for c in rec.cursor if c != s.id]
            await self._audit(rec, s.id, "gate_passed",
                              {"predicate": s.gate} if data_gate else {})
            if data_gate:
                _gate_span(rec, s.id, "data")  # #222 — a human gate's span is the decision's
            await audit_leg_finished(rec.pipeline_id, s.id, ref=s.ref, state="done",
                                     reason="data gate predicate true" if data_gate else "gate approved")
            # #1627 — a loop body may END on a gate: the last word of a
            # retrain cycle is a human deciding whether to promote.
            # A gate leg has no RUN, so its completion never reaches
            # _on_run_terminal — where the loop decision lives — and the loop
            # silently ran exactly one iteration before the pipeline reported
            # `done`. Same call, same ordering as line ~554 (decide BEFORE
            # advancing, so the next iteration is already in the plan when
            # _advance scans), on the path a gate actually takes. Reachability,
            # not capability: the decision function was correct and simply
            # never invoked from here.
            if await self._decide_loop(rec, s.id):
                return
            # The pipeline is still `paused` from the gate's awaiting_approval —
            # recompute BEFORE recursing, or _advance's `status != "running"`
            # guard silently drops every step downstream of a HITL gate.
            self._recompute_status(rec)
            await self.store.put(rec)
            # #2540 — a gate passed on the adopt-time pass keeps that pass's check
            await self._advance(rec.pipeline_id, _rec=rec, after_restart=after_restart)
            return
        # ADR-0125 D2 — a process without its owner lock starts no leg: the leg
        # would name an owner nobody can prove alive. It stays `pending`.
        if not liveness.may_start_legs():
            await self._hold_back_leg(rec, s)
            return
        # #1127 (ADR-0084 D1) — route by LEG KIND. A `flow:` leg goes straight to
        # the environment's engine; only an agentic leg touches a graph runtime.
        # Before this branch every step went through run_template, so a pure data
        # pipeline still loaded LangGraph to hold a door node.
        if leg_kind_of(s.ref) == "flow":
            run = await self._submit_flow_leg(rec, s, resolved)
            # ADR-0086 H5 (#1247) — a flow leg admitted at its target's cap comes
            # back `queued`: it has identity, lineage and a console row, but no
            # engine work yet. The step mirrors that (plan_resume already buckets
            # a `queued` step as RESUBMIT — correct, since nothing was submitted).
            leg_status = "queued" if str(getattr(run, "status", "")) == "queued" else "running"
        elif leg_kind_of(s.ref) == "task":
            # #1278 — a task leg names an OPERATOR; it runs in-process as its
            # own governed run, never through run_template.
            run = await self._submit_task_leg(rec, s, resolved)
            leg_status = "running"
        else:
            run = await self.dispatcher.run_template(
                s.ref, resolved,
                environment=s.environment or rec.environment,
                bundle_pipeline_id=rec.pipeline_id, step_id=s.id,
                # ADR-0105 D1 (#2113) — a flow started on its own is a one-leg
                # pipeline; the sandbox switch, preset, lineage, series and cost
                # cap it was started with ride on the record and reach the leg
                # here, so the leg is the same governed run a lone start made.
                **_run_options(rec),
            )
            leg_status = "running"
        # record run_id → pipeline so _on_run_terminal recognizes this run WITHOUT a
        # store lookup (set synchronously before the run can complete + emit).
        self._run_to_pipeline[run.id] = rec.pipeline_id
        st = rec.step_state.get(s.id) or StepState()
        st.status = leg_status
        st.run_id = run.id
        st.started_at = _now()
        rec.step_state[s.id] = st
        rec.cursor = sorted(set(rec.cursor) | {s.id})
        await self._audit(rec, s.id, "step_submitted", {"run_id": run.id, "ref": s.ref})
        await audit_leg_submitted(rec, s.id, run, s.ref)   # #233 — hash-chained trail

    async def _submit_flow_leg(self, rec: PipelineRecord, s: Any, resolved: dict) -> Any:
        """Submit a `flow:` leg as its OWN governed run — no template wrapper (#1127).

        The run record is created FIRST so the leg has identity, lineage and a
        console row before any engine work starts (R2: identity at admission,
        never re-derived). `template_ref` is the D0 `flow:<pack>/<name>` form,
        so grouping and audit need no special case.

        The engine comes from the ENVIRONMENT (`resolve_engine`), never from the
        pipeline — packs say what, environments say where (ADR-0070/D4).

        ADR-0086 H5 (#1247): when the environment binds this leg's kind to a
        compute target whose connection sets ``max_in_flight``, the run is created
        `queued` instead of `running` once that target is at its cap. The leg then
        WAITS for a slot (`_await_target_slot`) — it is never refused, and it
        holds no engine resource while it waits. With no target, or no cap on the
        target's connection, not one line of this changes: no connection lookup,
        no count, no lock.
        """
        from cairn.core.dispatch.dispatcher import new_run_id  # noqa: PLC0415 - cycle
        from cairn.core.storage.protocols import Run  # noqa: PLC0415 - cycle
        from cairn.core.engine.schema import TriggerEvent  # noqa: PLC0415 - cycle

        target = self._leg_target(s)
        cap = await target_cap(target) if target else None

        def _leg_run(status: str) -> Any:
            return Run(
                id=new_run_id(s.ref.replace("/", "--").replace(":", "-")),
                trigger=TriggerEvent(source="manual", payload={"leg": s.ref}),
                status=status,
                # #1233 — created running, so the leg starts NOW. A capped leg
                # created `queued` has not started: its clock begins at admission.
                started_at=_now() if status == "running" else None,
                fingerprint=s.ref,
                template_ref=s.ref,          # ADR-0084 D0
                inputs=dict(resolved or {}),
                environment=s.environment or rec.environment or "",
                compute_target=target,       # H5 — what the cap is counted by
                bundle_pipeline_id=rec.pipeline_id,
                step_id=s.id,
                workspace_id=rec.workspace_id,
                trace_id=current_trace_id(),  # #222 — the pipeline's trace
                owner=liveness.self_owner(),  # ADR-0125 D1 — the poll loop runs here
            )

        if cap is None:
            run = _leg_run("running")
            await self.dispatcher.store.create(run)
        else:
            # count → create is ONE critical section, or two legs submitted in the
            # same tick both see the last slot free and both take it.
            async with self._admit.hold(target or "(none)"):
                at_cap = await in_flight_on_target(self.dispatcher.store, target) >= cap
                run = _leg_run("queued" if at_cap else "running")
                await self.dispatcher.store.create(run)
            if at_cap:
                await self._audit(rec, s.id, "leg_queued", {
                    "run_id": run.id, "target": target, "max_in_flight": cap})

        # #1235 — reproducibility pins at submit time, SAME contract as the
        # direct train/* path (#1072 via record_train_pins): dataset ref +
        # content hash + the resolved leg config stamp the run record even if
        # the leg later fails. Best-effort by the helper's contract (never
        # raises; returns None when the inputs carry no dataset reference —
        # e.g. a gate leg or a flow with nothing to reproduce).
        from cairn.core.operator.pins import record_train_pins  # noqa: PLC0415 — keep httpx off the dispatch import path
        await record_train_pins(
            run.id, s.id, operator=s.ref, resolved_input=dict(resolved or {}),
        )

        # #1218 — run the leg in a BACKGROUND task and return at submit. The leg's
        # terminal emit drives _on_run_terminal off the bus (exactly how template
        # legs chain), so awaiting completion here only served to hold the launch
        # HTTP response — and the per-pipeline lock — for the leg's whole duration.
        task = asyncio.create_task(
            self._run_flow_leg(rec, s, run, resolved),
            name=f"flow-leg:{rec.pipeline_id}:{s.id}",
        )
        self._leg_tasks[run.id] = task
        task.add_done_callback(lambda _t, rid=run.id: self._leg_tasks.pop(rid, None))
        return run

    async def _pause_leg_for_approval(
        self, rec: PipelineRecord, s: Any, run: Any, req: Any,
    ) -> None:
        """A required gate with no recorded approval PAUSES the leg (#1759).

        The contract everywhere else in the product: a gate that fires waits for
        a human. The template path does it with LangGraph's ``interrupt()``; the
        bridge's legs cannot (they are not graphs), and until this existed they
        did the one thing a gate must never do — they FAILED the leg, taking the
        whole pipeline with it and wasting every leg already paid for. Seen live
        on 2026-08-28: `dataset/replay` died on a gate the operator was supposed
        to WAIT on, after the champion measurement had already been paid for.

        Nothing new is invented here: ``awaiting_approval`` is already in the
        StepState vocabulary, ``_recompute_status`` already turns it into a
        paused pipeline, and :meth:`advance_step` (already wired to
        ``POST /pipelines/{id}/steps/{step}/approve``) is already the release
        path — it re-evaluates the gate and re-submits the step, so releasing
        cannot bypass governance. This just routes the two leg kinds into it.

        The run is marked ``awaiting_approval`` rather than failed so the run
        record says what actually happened. On release a TASK leg resumes this
        same run (#2630, one Run per leg: ``_paused_task_run``); a flow leg's
        release still submits a fresh run.
        """
        st = rec.step_state.get(s.id) or StepState()
        st.status = "awaiting_approval"
        st.awaiting_since = _now()
        rec.step_state[s.id] = st
        detail = {
            "run_id": getattr(run, "id", ""),
            "operator": getattr(req, "operator", "") or s.ref,
            "risk": getattr(req, "risk", "") or "",
            # WHICH authority demanded the gate — operator / profile / policy /
            # binding. Without it "needs approval" is unactionable (#55).
            "sources": list(getattr(req, "sources", ()) or ()),
            "reason": getattr(req, "reason", "") or "",
        }
        await self._audit(rec, s.id, "leg_awaiting_approval", detail)
        try:
            await self.dispatcher.store.update(
                getattr(run, "id", ""), status="awaiting_approval")
        except Exception:  # noqa: BLE001 — the STEP state is the pipeline's truth
            logger.exception("leg %s: failed to mark run awaiting_approval", s.ref)
        # #1791 — the same detail the audit entry carries, delivered to a human.
        # `sources` is the merged authority from the approval plane (policy /
        # profile / operator / binding), which is the one thing that makes
        # "needs approval" actionable.
        await self._notify_gate(
            rec, s.id, "awaiting_approval",
            operator=str(detail["operator"]),
            risk=str(detail["risk"]),
            authority=tuple(str(x) for x in detail["sources"]),
            reason=str(detail["reason"]),
        )
        self._recompute_status(rec)
        await self.store.put(rec)

    def _enforce_flow_leg_gate(self, rec: PipelineRecord, s: Any) -> None:
        """Run the approval plane over a FLOW leg before it reaches the engine
        (#1760). Raises ``NeedsApprovalError``, which the caller turns into a
        pause — the same contract the ``task:`` path has.

        The authority merge lives in ``approval.enforce``, not here: a second
        call path that reproduces it is exactly how #1402 happened. This only
        supplies the two things the bridge knows — the flow's own
        ``aggregate_risk`` (the admission plane's number) and the step's
        ``approval:`` declaration.
        """
        from cairn.core.approval.enforce import (  # noqa: PLC0415 - cycle: approval → operator
            approved_state, enforce_flow_gate,
        )
        from cairn.core.dataflow.defs import flow_leg_risk  # noqa: PLC0415 - cycle

        risk: Optional[str] = None
        try:
            # #1792 — through the SHARED lookup. The copy that lived here
            # partitioned `flow:training/sft-train` without stripping the D0
            # prefix, so the pack read as `flow:training`, every alias missed,
            # and this gate keyed on `risk=None` for EVERY flow leg since
            # #1760 shipped. `pipeline explain` renders the same number from
            # the same function, so the two cannot drift again.
            risk = flow_leg_risk(str(s.ref))
        except Exception:  # noqa: BLE001 — an unreadable def resolves as unknown risk, not as safe
            logger.exception("flow leg %s: could not resolve aggregate risk", s.ref)

        st = rec.step_state.get(s.id)
        enforce_flow_gate(
            str(s.ref),
            risk,
            getattr(s, "approval", None) or None,   # the step's own strictness-only mandate
            state=approved_state(
                s.environment or rec.environment or "",
                s.id,
                getattr(st, "approver", None),
            ),
            node_name=s.id,
            run_id=rec.pipeline_id,
        )

    @_in_record_workspace
    @_flow_leg_traced
    async def _run_flow_leg(self, rec: PipelineRecord, s: Any, run: Any, resolved: dict,
                            *, adopt: Optional[dict] = None) -> None:
        """The background half of a flow leg: execute → persist outcome → emit the
        terminal event. execute_leg never raises for execution failures; the belt
        here terminalizes the run on anything unexpected so a leg can never strand
        its pipeline in 'running' with no terminal event. ``adopt`` (#1429) is a
        persisted engine handle from a previous owner process: watch, don't submit."""
        try:
            # #1760 — the approval plane, BEFORE any engine work. Skipped when
            # adopting a handle from a previous owner: that leg already cleared
            # its gate and is mid-flight; re-gating it would strand a running job.
            if adopt is None:
                self._enforce_flow_leg_gate(rec, s)
            if adopt is None and str(getattr(run, "status", "")) == "queued":
                # ADR-0086 H5 (#1247) — the leg's target is at its cap. Wait for a
                # slot BEFORE any engine work; a False return means the pipeline
                # was cancelled while we waited (the run is already terminal and
                # `engine.submit` was never called).
                if not await self._await_target_slot(rec, s, run):
                    return
            await self._execute_and_settle_flow_leg(rec, s, run, resolved, adopt=adopt)
        except NeedsApprovalError as e:
            # #1759/#1760 — same contract as the task leg: a gate WAITS.
            logger.info("flow leg %s (%s) awaits approval", s.ref, run.id)
            async with self._lock(rec.pipeline_id):
                cur = await self.store.get(rec.pipeline_id) or rec
                await self._pause_leg_for_approval(cur, s, run, e.request)
        except Exception as e:  # noqa: BLE001 — a stranded 'running' run is worse than any error
            logger.exception("flow leg %s (%s) crashed outside execute_leg", s.ref, run.id)
            try:
                await self.dispatcher.store.update(
                    run.id, status="failed", ended_at=_now(),
                    error=f"{type(e).__name__}: {e}")
                await self.dispatcher._emit(
                    run.id, "failed",
                    {"leg": s.ref, "step_id": s.id, "error": f"{type(e).__name__}: {e}"},
                    pipeline_id=rec.pipeline_id,
                )
            except Exception:  # noqa: BLE001 — nothing left to do but log
                logger.exception("flow leg %s (%s): failed to terminalize", s.ref, run.id)

    # ── H6b/c (#1248): checkpoint relay + resume selection ─────────────────
    # The relay/resume closures moved to ``FlowLegHooks`` (#1429) so the
    # Temporal owner runs the identical side-channels; only the store stays.

    def _checkpoint_store(self) -> Any:
        if self._artifact_store is None:
            from cairn.core.storage.artifacts import build_artifact_store  # noqa: PLC0415 - cycle off dispatch import
            self._artifact_store = build_artifact_store()
        return self._artifact_store

    # ── #1278: operator (task) legs ────────────────────────────────────────

    async def _submit_task_leg(self, rec: PipelineRecord, s: Any, resolved: dict) -> Any:
        """Submit a ``task:`` leg — one OPERATOR call as its OWN governed run.

        Mirrors ``_submit_flow_leg``'s shape (run record FIRST — identity at
        admission; execution in a background task; terminal via the bus so
        ``_on_run_terminal`` chains) with the engine swapped for the operator
        registry: the op runs in-process on this control plane, exactly like an
        operation node in a template. Inputs arrive already resolved by
        ``resolve_step_inputs`` — there is no binding grammar to evaluate.
        No H5 target cap (nothing dispatches to a compute target) and no H2
        retry (v1: an op that fails failed; a retry: block can extend this the
        way it did flow legs).
        """
        from cairn.core.dispatch.dispatcher import new_run_id  # noqa: PLC0415 - cycle
        from cairn.core.storage.protocols import Run  # noqa: PLC0415 - cycle
        from cairn.core.engine.schema import TriggerEvent  # noqa: PLC0415 - cycle

        # #2630 (invariant 4: one Run per leg) — an operator-declared gate paused this
        # leg's run at `awaiting_approval` (`_pause_leg_for_approval`), and the person's
        # release re-submits the step. The leg resumes THAT run instead of creating a
        # second one and leaving the first at `awaiting_approval` for good. Its identity
        # stays the one recorded at admission (#2641): it runs with the inputs recorded
        # on the run (the re-resolved ones only when the run carries none) and keeps its
        # original `started_at`; the resume time goes on the audit entry and on the
        # run's own `resumed` event (the type `RunDispatcher.resume_run` emits).
        paused = await self._paused_task_run(rec, s)
        if paused is not None:
            resumed_at = _now()
            recorded = getattr(paused, "inputs", None)
            inputs = dict(recorded) if recorded else dict(resolved or {})
            await self.dispatcher.store.update(paused.id, status="running")
            await self._take_leg_ownership(paused.id)
            await self._audit(rec, s.id, "leg_resumed_after_approval",
                              {"run_id": paused.id, "resumed_at": resumed_at})
            approver = getattr(rec.step_state.get(s.id), "approver", None)
            await self.dispatcher._emit(
                paused.id, "resumed",
                {"leg": s.ref, "step_id": s.id, "resumed_at": resumed_at,
                 **({"approver": approver} if approver else {})},
                pipeline_id=rec.pipeline_id)
            task = asyncio.create_task(
                self._run_task_leg(rec, s, paused, inputs),
                name=f"task-leg:{rec.pipeline_id}:{s.id}",
            )
            self._leg_tasks[paused.id] = task
            task.add_done_callback(lambda _t, rid=paused.id: self._leg_tasks.pop(rid, None))
            return paused

        run = Run(
            id=new_run_id(s.ref.replace("/", "--").replace(":", "-")),
            trigger=TriggerEvent(source="manual", payload={"leg": s.ref}),
            status="running",
            started_at=_now(),
            fingerprint=s.ref,
            template_ref=s.ref,          # D0: the `task:<op>` form, like flow:/gate:
            inputs=dict(resolved or {}),
            environment=s.environment or rec.environment or "",
            bundle_pipeline_id=rec.pipeline_id,
            step_id=s.id,
            workspace_id=rec.workspace_id,
            trace_id=current_trace_id(),  # #222 — the pipeline's trace
            owner=liveness.self_owner(),  # ADR-0125 D1 — the op runs in this process
        )
        await self.dispatcher.store.create(run)
        task = asyncio.create_task(
            self._run_task_leg(rec, s, run, resolved),
            name=f"task-leg:{rec.pipeline_id}:{s.id}",
        )
        self._leg_tasks[run.id] = task
        task.add_done_callback(lambda _t, rid=run.id: self._leg_tasks.pop(rid, None))
        return run

    async def _paused_task_run(self, rec: PipelineRecord, s: Any) -> Any:
        """The run this task step paused at an operator gate, if it still waits: the
        step's recorded run, of this pipeline and step, at ``awaiting_approval``."""
        st = rec.step_state.get(s.id)
        run_id = getattr(st, "run_id", None) if st is not None else None
        if not run_id:
            return None
        try:
            run = await self.dispatcher.store.get(run_id)
        except Exception:  # noqa: BLE001 — unreadable ⇒ a fresh run, as before
            return None
        if (run is None or str(getattr(run, "status", "")) != "awaiting_approval"
                or getattr(run, "bundle_pipeline_id", None) != rec.pipeline_id
                or getattr(run, "step_id", None) != s.id):
            return None
        return run

    @_in_record_workspace
    async def _run_task_leg(self, rec: PipelineRecord, s: Any, run: Any, resolved: dict) -> None:
        """The background half: resolve the op → validate → run → persist →
        emit terminal. An op failure is an OUTCOME (run `failed`, pipeline
        decides), never an unhandled crash — the belt here terminalizes on
        anything unexpected so a leg can never strand its pipeline."""
        op_name = s.ref[len("task:"):] if s.ref.startswith("task:") else s.ref
        # #129 / #63 — a task leg is accounted like the other legs: its LLM spend is
        # measured in a cost scope and recorded on its run (so gate cost lines and
        # later legs' admission see it), and under a capped series it reserves before
        # it spends and settles after, as a flow leg does in `execute_leg`.
        from cairn.core import obs  # noqa: PLC0415 - keep obs off the bridge's import path
        from cairn.core.dispatch.dispatcher import _aggregate_cost  # noqa: PLC0415 - the template path's rollup
        cost_ctx: Any = None
        reservation: Any = None
        try:
            # #1687 — `dataset:<name>[@N]` refs resolve HERE for task legs, with
            # the SAME functions the flow path uses in `execute_leg` (#1466/
            # #1625) — the gate is the resolver's own predicate, so a form the
            # rewriter handles can never be one the gate skips, and there is no
            # second copy to drift (#1402's chokepoint lesson). Flow legs needed
            # this control-plane-side because remote boxes have no DB (D4);
            # task legs run in-process where the store IS reachable, which is
            # why nobody wired it — until `dataset/replay` was designed around
            # governed version refs and a bare ref reached `load_rows` as a
            # literal filename. An unknown ref fails the LEG here, with the
            # resolver's cited reason, before the operator reads anything.
            from cairn.core.storage.artifacts import (  # noqa: PLC0415 - cycle: storage → dispatch on some paths
                build_artifact_store,
                config_has_dataset_ref,
                resolve_dataset_refs_in_config,
            )
            if config_has_dataset_ref(resolved):
                resolved = await resolve_dataset_refs_in_config(
                    build_artifact_store(), resolved)
            # #1288 — a declared `timeout:` bounds the op call; expiry is a
            # FAILED leg naming the limit (outcome, not crash). No timeout →
            # unbounded, today's behavior.
            reservation = await self._reserve_task_leg(rec, s, run)
            limit = getattr(s, "timeout_s", None)
            with obs.cost_scope(f"leg-{rec.pipeline_id}-{s.id}") as cost_ctx:
                if limit:
                    try:
                        output = await asyncio.wait_for(
                            self._invoke_task_op(op_name, resolved, rec, s),
                            timeout=float(limit))
                    except asyncio.TimeoutError:
                        raise TimeoutError(
                            f"task leg exceeded its declared timeout of {limit}s"
                        ) from None
                else:
                    output = await self._invoke_task_op(op_name, resolved, rec, s)
            # the run's `cost` always carries the rollup; the payload names the LLM spend
            # only when there was some, so a leg that spent nothing keeps its shape
            llm_usd = round(cost_ctx.cost_usd, 6)
            payloads = [{"output": output, **({"llm_cost_usd": llm_usd} if llm_usd else {})}]
            cost = _aggregate_cost(payloads)
            await self.dispatcher.store.update(
                run.id, status="done", ended_at=_now(),
                result_payloads=payloads, cost=cost,
            )
            await _close_task_reservation(reservation, cost)
            reservation = None
            await self.dispatcher._emit(
                run.id, "done", {"leg": s.ref, "step_id": s.id, "error": ""},
                pipeline_id=rec.pipeline_id)
        except NeedsApprovalError as e:
            # #1759 — a required gate is a WAIT, not a failure. Before this, the
            # generic handler below turned it into `failed` and killed the
            # pipeline mid-flight, after earlier legs had already been paid for.
            logger.info("task leg %s (%s) awaits approval", s.ref, run.id)
            # the gate fired before the operator ran: nothing was spent, the hold goes
            await _close_task_reservation(reservation, {})
            reservation = None
            async with self._lock(rec.pipeline_id):
                cur = await self.store.get(rec.pipeline_id) or rec
                await self._pause_leg_for_approval(cur, s, run, e.request)
        except asyncio.CancelledError:
            # a cancelled leg still settles what it spent, as a cancelled flow attempt does
            spent = round(float(getattr(cost_ctx, "cost_usd", 0.0) or 0.0), 6)
            try:
                await _close_task_reservation(reservation, _aggregate_cost([{"llm_cost_usd": spent}]))
            except Exception:  # noqa: BLE001 — cancellation proceeds either way
                logger.exception("task leg %s (%s): failed to close its reservation", s.ref, run.id)
            raise
        except Exception as e:  # noqa: BLE001 — outcome, not crash (incl. CapabilityNotDeclared)
            logger.exception("task leg %s (%s) failed", s.ref, run.id)
            err = f"{type(e).__name__}: {e}"
            # what a failed attempt already spent is still spend (#1441's rule)
            spent = round(float(getattr(cost_ctx, "cost_usd", 0.0) or 0.0), 6)
            cost = _aggregate_cost([{"llm_cost_usd": spent}])
            try:
                await _close_task_reservation(reservation, cost)
            except Exception:  # noqa: BLE001 — the leg's outcome is recorded either way
                logger.exception("task leg %s (%s): failed to close its reservation", s.ref, run.id)
            try:
                await self.dispatcher.store.update(
                    run.id, status="failed", ended_at=_now(), error=err,
                    **({"cost": cost} if spent else {}))
                await self.dispatcher._emit(
                    run.id, "failed", {"leg": s.ref, "step_id": s.id, "error": err},
                    pipeline_id=rec.pipeline_id)
            except Exception:  # noqa: BLE001 — nothing left to do but log
                logger.exception("task leg %s (%s): failed to terminalize", s.ref, run.id)

    @staticmethod
    def _recorded_contract(rec: Any, path: str) -> "Any | None":
        """#2241 B — the leg's ``InvocationEntry`` from the record (never re-resolved),
        turned into the envelope the seam validates against. ``None`` for a legacy
        record or a leg whose operator is not opted in."""
        from cairn.core.engine.pipeline_loop import declared_step_id  # noqa: PLC0415 — engine helper, lazy like its siblings

        plan = getattr(rec, "plan", None)
        resolution = getattr(plan, "contract_resolution", None)
        # #2344 — loop iteration ≥2 runs as `<id>#<n>`; the door recorded `<id>`. Repeats
        # of one declared step share its entry, never silently run without it.
        path = declared_step_id(path, getattr(plan, "loops", None) or {})
        # ADR-0118 D4 (N7) — a flow leg's stage entries share its path; only an operation
        # entry is the leg's own operator contract.
        entry = next((e for e in (resolution.invocations if resolution else ())
                      if e.surface == "operation" and e.path == path), None)
        if entry is None:
            return None
        from cairn.core.operator.effective import ContractEnvelope  # noqa: PLC0415 - operator stack off the hot import path
        from cairn.core.pack.registry import invocation_contract  # noqa: PLC0415 - cycle: dispatch → pack, lazy like its siblings
        data = invocation_contract([(f"contract://{d.id}/{d.revision}", d.digest) for d in entry.definitions],
                                   [(f"contract://{d.id}/{d.revision}", d.digest) for d in resolution.definitions])
        return ContractEnvelope.from_dict({**data, "operator_ref": entry.operator_ref,
                                           "operator_version": entry.operator_version})

    async def _reserve_task_leg(self, rec: PipelineRecord, s: Any, run: Any) -> Any:
        """#63 — a task leg's budget claim, made the way ``execute_leg`` makes a flow
        leg's: only under a capped series, against what the pipeline's other legs
        actually spent, for the leg's estimate (``leg_price_kwargs``). A refusal is a
        failed leg with the reason, on the audit trail; ``None`` when uncapped."""
        from cairn.core.budget_reservations import BudgetExceeded, reserve  # noqa: PLC0415 - as execute_leg

        gov = getattr(rec, "governance", None) or {}
        series_id = str(gov.get("series", "") or "")
        budget_usd = budget_cap_usd(rec)
        if not (series_id and budget_usd):
            return None
        spent_usd, spend_error = await self._admission_spend(rec, s, capped=True, adopt=None)
        if spend_error:
            raise RuntimeError(spend_error)
        est = float(leg_price_kwargs(rec, s.id, run).get("estimated_usd") or 0.0)
        try:
            return await reserve(series_id, est_usd=est, spent_usd=float(spent_usd or 0.0),
                                 budget_usd=float(budget_usd), run_id=str(run.id))
        except BudgetExceeded as e:
            await self._audit(rec, s.id, "budget_refused", {
                "series": series_id, "estimated_usd": est, "spent_usd": spent_usd,
                "budget_usd": budget_usd, "reason": str(e)[:300]})
            raise

    async def _invoke_task_op(self, op_name: str, resolved: dict,
                              rec: PipelineRecord, s: Any) -> dict:
        """One operator call with pre-resolved inputs.

        The binding layer (`invoke_operator`) speaks the template `$state`
        grammar; pipeline wires were already resolved by ``resolve_step_inputs``,
        so this performs the same steps minus binding evaluation: validate
        against ``input_schema``, build an :class:`OperationContext` (Null
        stores — the same default a template operation node gets when nothing
        is bound), honor mock mode, run — sync ops in a worker thread so a
        long op never blocks the bridge's event loop. Lifted @step ops (#1254)
        arrive with their ctx guard baked into the generated class.

        **The enforcement point is shared, not reproduced** (#1402): this calls
        the same :func:`~cairn.core.approval.enforce.enforce_gate` the template
        path calls. Skipping binding evaluation must never mean skipping the
        gate — an operator that declares ``requires_approval`` is gated here as
        surely as in a template, and the *step's* ``approval: true`` can only
        add strictness, never relax the operator's own declaration.
        """
        # lazy: cairn.core.operator pulls the executor/binding stack — a cycle
        # off the dispatch import path
        from cairn.core.approval.enforce import (  # noqa: PLC0415 - cycle, as above
            ApprovalIntent, approved_state, enforce_gate,
        )
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415
        from cairn.core.operator.runtime import (  # noqa: PLC0415 - cycle, as above
            invoke_operator_core, resolve_is_mocked,
        )

        op_cls = default_registry.get(op_name)  # UnknownOperatorError names the known ops
        # #2182 — the launch door's rule again: a resumed, restarted or looped leg
        # reaches here without the door, and this path never picks an executor.
        from cairn.core.dispatch.pipeline_doors import task_leg_executor_problem  # noqa: PLC0415 — operator stack off the import path
        if (problem := task_leg_executor_problem(op_cls)):
            raise RuntimeError(f"leg {s.id!r}: {problem}")
        # Per-operator, like the template path: full mock stubs everything, a
        # sandbox stubs only write-class ops (#836) — not the global flag.
        is_mocked = resolve_is_mocked(op_cls)
        # A step released through the bridge's HITL queue carries its approver;
        # that human decision IS the approval record the gate looks for.
        st = rec.step_state.get(s.id)
        enforce_gate(
            op_cls,
            ApprovalIntent(approval=getattr(s, "approval", None) or None),
            state=approved_state(
                s.environment or rec.environment or "",
                s.id,
                getattr(st, "approver", None),
            ),
            node_name=s.id,
            run_id=rec.pipeline_id,
            is_mocked=is_mocked,
        )
        # #1542 — the profile's per-operator config layer, sourced exactly the
        # way the template compiler seeds state["operator_config"]: best-effort,
        # config is additive, a broken profile must never fail the leg.
        try:
            from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415 - cycle
            profile_config: dict | None = dict(load_profile_from_env().config)
        except Exception:  # noqa: BLE001 — config is additive; never fail a run on it
            profile_config = None
        op_label = getattr(op_cls, "name", None) or op_cls.__name__
        contract = self._recorded_contract(rec, s.id)
        with span("cairn.operator", {
            "operator": op_label, "node": s.id, "executor": "in_process",
            "mocked": bool(is_mocked), "run_id": str(rec.pipeline_id),
        }, parent=rec.trace_context):  # #222 — under the pipeline root, in any process
            # ONE invoke path with the template operation node (#1542): config
            # layering + governance-strip + secret scoping + sharding all
            # happen inside the core, not re-implemented (and drifted) here.
            # Sync ops run in a worker thread so a long op never blocks the
            # bridge's event loop. Task legs carry no secrets declaration yet
            # (a step-grammar gap, #1543 territory) → secret_keys=None keeps
            # today's NullSecretStore behavior through the shared code.
            # ADR-0120 D7 (#2455) — the leg's operator may start other governed work
            # only through this handle (the launch door; never its own dispatcher).
            _handle_fn = getattr(self.dispatcher, "launch_handle", None)
            return await invoke_operator_core(
                op_cls,
                dict(resolved or {}),
                pipeline_id=rec.pipeline_id,
                node_name=s.id,
                profile_config=profile_config,
                is_mocked=is_mocked,
                contract=contract,
                run_sync_in_thread=True,
                ctx_overrides={"launch": _handle_fn()} if callable(_handle_fn) else None,
            )

    async def _reset_finalizer_for_rerun(self, rec: PipelineRecord, step_id: str,
                                         st: Any) -> None:
        """#1787 — a finalizer that was in flight when the process died goes back
        to ``pending`` so ``_settle`` re-submits it on adoption.

        The dead run is terminalized first, for the same reason
        :meth:`_resume_task_leg` does it: a ``running`` row nothing can ever
        finish is a lie. Reached only for a finalizer whose run has no owner
        (LEGACY) or whose owner process is DEAD — the reconciler's handoff,
        ADR-0125 D4; a live owner's finalizer is only re-mapped.
        """
        if st.run_id:
            try:
                await self.dispatcher.store.update(
                    st.run_id, status="failed", ended_at=_now(),
                    error="finalizer leg died with the control plane restart")
            except Exception:  # noqa: BLE001 — a store hiccup must not stop adoption
                logger.debug("finalizer %s: could not terminalize dead run %s",
                             step_id, st.run_id, exc_info=True)
        st.status = "pending"
        st.run_id = None
        st.ended_at = None
        rec.step_state[step_id] = st
        rec.cursor = [c for c in rec.cursor if c != step_id]
        await self._audit(rec, step_id, "finalizer_rerun_after_restart", {})

    @staticmethod
    def _task_op_risk(s: Any) -> str:
        """The manifest RISK of a task leg's operator; ``""`` when unknown —
        which every caller treats fail-closed (#1289)."""
        op_name = s.ref[len("task:"):] if s.ref.startswith("task:") else s.ref
        try:
            # lazy: cairn.core.operator pulls the executor/binding stack — a
            # cycle off the dispatch import path (same as _invoke_task_op)
            from cairn.core.operator.registry import default_registry  # noqa: PLC0415
            return str(getattr(default_registry.get_metadata(op_name), "risk", "") or "")
        except Exception:  # noqa: BLE001 — unknown risk = fail-closed
            return ""

    async def _resume_task_leg(self, rec: PipelineRecord, s: Any, st: Any) -> None:
        """#1289 — resume policy for a task leg caught mid-op by a restart.

        The old run is terminalized first (its op is gone; leaving it
        `running` is a lie — task legs stamp no heartbeat). Reached only for a
        run with no owner (LEGACY) or whose owner process is DEAD (the
        reconciler's handoff, ADR-0125 D4): a leg a live process still runs is
        only re-mapped. Then the op's manifest RISK decides:

        - ``read_only`` → auto-resubmit: re-running a read is free and
          correct, so the pipeline continues unattended;
        - anything else — including NO metadata (#1293 makes that common for
          installed packs) → the step parks ``awaiting_advance``: an
          ``external_write`` op that may or may not have fired must not be
          silently re-fired. A human resumes it through the EXISTING advance
          door (``advance_step``); no new surface.
        """
        try:
            await self.dispatcher.store.update(
                st.run_id, status="failed", ended_at=_now(),
                error="in-process task leg died with the control plane restart")
        except Exception:  # noqa: BLE001 — a store hiccup must not stop adoption
            logger.debug("task leg %s: could not terminalize dead run %s",
                         s.id, st.run_id, exc_info=True)
        risk = self._task_op_risk(s)

        if risk == "read_only":
            outputs = iteration_view(
                await self._collected_outputs(rec), s.id,
                (rec.plan.loops if rec.plan else None) or {})  # #1671
            try:
                resolved = resolve_step_inputs(s, bundle_input=rec.inputs, steps=outputs)
            except WireResolutionError as e:  # #1540 — cited failure, not a wedged restart
                await self._audit(rec, s.id, "wire_unresolved", {"error": str(e)})
                await self._fail_pipeline(rec, str(e))
                return
            run = await self._submit_task_leg(rec, s, resolved)
            self._run_to_pipeline[run.id] = rec.pipeline_id
            st.status = "running"
            st.run_id = run.id
            st.started_at = _now()
            rec.step_state[s.id] = st
            await self._audit(rec, s.id, "task_leg_resubmitted",
                              {"run_id": run.id, "risk": risk})
        else:
            reason = ("restart mid-op; a non-read_only op is not re-fired "
                      "without a decision (fail-closed)")
            st.status = "awaiting_advance"
            st.awaiting_since = _now()   # #2555 (5) — the notification's dedupe key
            rec.step_state[s.id] = st
            await self._audit(rec, s.id, "task_leg_awaiting_decision", {
                "risk": risk or "unknown", "reason": reason,
            })
            await self._notify_parked(rec, s, reason, risk=risk or "unknown")
        self._recompute_status(rec)
        await self.store.put(rec)

    # ── ADR-0086 H5 (#1247): per-target concurrency ────────────────────────

    def _target_event(self, target: str) -> asyncio.Event:
        """The wake signal for legs queued behind ``target``'s cap."""
        ev = self._target_free.get(target)
        if ev is None:
            ev = self._target_free[target] = asyncio.Event()
        return ev

    def _release_target(self, target: str) -> None:
        """A leg on ``target`` terminalized — wake everyone waiting on it. The
        event is only a WAKE: each waiter re-counts from the run store, so a
        spurious set costs one query and a missed set costs one poll interval."""
        if target:
            ev = self._target_free.get(target)
            if ev is not None:
                ev.set()

    def _leg_target(self, s: Any) -> str:
        """The compute TARGET this leg's engine will dispatch to.

        The same selection ``resolve_engine`` reads — ``dataflow_target`` for a
        ``kind: data`` flow, ``ml_target`` for ``kind: ml`` — through the ONE
        config plane (#1093: profile → ``CAIRN_*`` env → schema default). Read from
        CORE rather than by asking the engine object, so the cap does not depend
        on any engine class exposing its target, and a control plane with no
        dataflow operator pack installed still imports. ``""`` = no target (the
        local builtin engine): nothing to cap, nothing to look up.
        """
        try:
            # ADR-0151 D1 — the one selection, shared with the launch door's pricing.
            return compute_target_for_kind(self._leg_flow_kind(s))
        except Exception:  # noqa: BLE001 — an unreadable selection means "no target"
            logger.debug("could not resolve the compute target for leg %s",
                         getattr(s, "ref", ""), exc_info=True)
            return ""

    async def _await_target_slot(self, rec: PipelineRecord, s: Any, run: Any) -> bool:
        """Hold a `queued` leg until its target has a free slot.

        Returns True once the leg is admitted (its run is now `running`), False
        when it must not run at all — the pipeline (or the run) was cancelled
        while it waited, in which case the leg is terminalized here and the
        engine is never touched.

        The wait is event-driven with a polling floor. The event (`run.*`
        terminal → :meth:`_release_target`) makes the common case immediate; the
        poll is what makes it CORRECT, because the leg that frees the slot may
        terminalize somewhere this bridge's subscription cannot see it — another
        control-plane process, or H1's orphan reconciler (#1239) writing the
        terminal status for a worker that died. Subscribing harder cannot fix
        that; re-counting the store can.
        """
        target = str(getattr(run, "compute_target", "") or "")
        cap = await target_cap(target)
        if cap is None:  # the cap was removed while we were being scheduled
            return await self._admit_leg(rec, s, run, target=target, cap=0)
        delay = _SLOT_POLL_MIN_S
        while True:
            if await self._settle_cancelled_while_queued(rec, s, run):
                return False
            # clear BEFORE counting: a release that lands during the count then
            # leaves the event set, so the wait below returns at once (no lost wakeup).
            ev = self._target_event(target)
            ev.clear()
            free = False
            # ADR-0125 D2 — while this process does not hold its owner lock the
            # leg stays `queued`: it takes no slot and reaches no engine.
            if liveness.may_start_legs():
                async with self._admit.hold(target or "(none)"):
                    free = await in_flight_on_target(
                        self.dispatcher.store, target, exclude_run_id=run.id) < cap
                    if free:
                        # flip INSIDE the lock — the run becoming `running` is
                        # what claims the slot for every later count.
                        await self.dispatcher.store.update(
                            run.id, status="running", started_at=_now())
                        run.status = "running"
            if free:
                return await self._admit_leg(rec, s, run, target=target, cap=cap)
            try:
                await asyncio.wait_for(ev.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 1.5, _SLOT_POLL_MAX_S)

    async def _admit_leg(self, rec: PipelineRecord, s: Any, run: Any, *,
                         target: str, cap: int) -> bool:
        """Mark a queued leg admitted: the run is `running` (already written by
        the caller when a cap applied), the step follows, the decision is audited."""
        if str(getattr(run, "status", "")) == "queued":
            await self.dispatcher.store.update(run.id, status="running", started_at=_now())
        try:
            run.status = "running"
        except Exception:  # noqa: BLE001 — a frozen/stub run object is fine
            pass
        # #2635 — a read-modify-write of the whole record: inside the pipeline's
        # section, so a concurrent advance cannot put back a copy read before it.
        # This runs in the leg's own task (`_run_flow_leg`), never under the section.
        async with self._lock(rec.pipeline_id):
            cur = await self.store.get(rec.pipeline_id)
            if cur is not None:
                st = cur.step_state.get(s.id)
                if st is not None and st.status == "queued":
                    st.status = "running"
                    st.started_at = _now()
                    cur.step_state[s.id] = st
                    await self.store.put(cur)
        await self._audit(rec, s.id, "leg_admitted",
                          {"run_id": run.id, "target": target, "max_in_flight": cap})
        return True

    async def _settle_cancelled_while_queued(self, rec: PipelineRecord, s: Any,
                                             run: Any) -> bool:
        """Did this queued leg lose its right to run? True = terminalized here.

        Cancelling a pipeline must be a CLEAN skip for anything still waiting on a
        slot: the leg never reached an engine, so there is nothing to kill and
        nothing paid to account for — it just stops being a leg.
        """
        cur = await self.store.get(rec.pipeline_id)
        cur_run = await self.dispatcher.store.get(run.id)
        pipeline_dead = cur is None or str(cur.status) in PIPELINE_TERMINAL
        run_dead = cur_run is None or str(cur_run.status) in RUN_TERMINAL
        if not (pipeline_dead or run_dead):
            return False
        reason = (f"pipeline cancelled while leg '{s.id}' waited for a slot on "
                  f"compute target {str(getattr(run, 'compute_target', '') or '')!r}")
        if not run_dead:
            await self.dispatcher.store.update(
                run.id, status="cancelled", ended_at=_now(), error=reason)
        if cur is not None:
            # #2635 — re-read and write inside the pipeline's section (as in
            # `_admit_leg`); the emit below stays OUTSIDE it, because the bridge's
            # own `run.cancelled` handler takes the same section.
            async with self._lock(rec.pipeline_id):
                cur = await self.store.get(rec.pipeline_id) or cur
                st = cur.step_state.get(s.id)
                if st is not None and st.status == "queued":
                    st.status = "skipped"
                    st.verdict = "cancelled"
                    st.ended_at = _now()
                    cur.step_state[s.id] = st
                    await self.store.put(cur)
        await audit_leg_finished(rec.pipeline_id, s.id, ref=s.ref, state="cancelled", run=run, reason=reason)
        await self.dispatcher._emit(run.id, "cancelled",
                                    {"leg": s.ref, "step_id": s.id, "error": reason},
                                    pipeline_id=rec.pipeline_id)
        return True

    async def _resume_queued_leg(self, rec: PipelineRecord, s: Any, st: StepState) -> None:
        """Re-arm a leg that was waiting for a target slot when we restarted.

        Safe to re-arm (unlike a `running` leg, which needs its handle): a queued
        leg never submitted, so there is no engine job to duplicate and no paid
        compute to double-charge. Its resolved inputs are on the run record."""
        if s is None or not st.run_id:
            return
        try:
            run = await self.dispatcher.store.get(st.run_id)
            if run is None or str(run.status) != "queued":
                return
            await self._take_leg_ownership(run.id)  # ADR-0125 D1 — the wait runs here now
            task = asyncio.create_task(
                self._run_flow_leg(rec, s, run, dict(getattr(run, "inputs", None) or {})),
                name=f"flow-leg:{rec.pipeline_id}:{s.id}",
            )
            self._leg_tasks[run.id] = task
            task.add_done_callback(lambda _t, rid=run.id: self._leg_tasks.pop(rid, None))
        except Exception:  # noqa: BLE001 — one un-resumable leg must not stop adoption
            logger.debug("could not re-arm queued leg %s of %s",
                         getattr(s, "id", ""), rec.pipeline_id, exc_info=True)

    async def _readopt_flow_leg(self, rec: PipelineRecord, s: Any, st: StepState) -> None:
        """Re-attach to a flow leg that was RUNNING when this process started
        (#1429, ADR-0086 H1 adopt-by-handle): the engine kept working through
        the restart, so the leg is watched to its terminal state through the
        same settle path a fresh leg takes — never resubmitted."""
        try:
            run = await self.dispatcher.store.get(st.run_id)
            if run is None or str(run.status) != "running":
                return
            await self._take_leg_ownership(run.id)  # ADR-0125 D1 — the poll loop runs here now
            task = asyncio.create_task(
                self._run_flow_leg(rec, s, run, dict(getattr(run, "inputs", None) or {}),
                                   adopt=dict(st.handle or {})),
                name=f"flow-leg:{rec.pipeline_id}:{s.id}",
            )
            self._leg_tasks[run.id] = task
            task.add_done_callback(lambda _t, rid=run.id: self._leg_tasks.pop(rid, None))
        except Exception:  # noqa: BLE001 — one un-adoptable leg must not stop adoption
            logger.debug("could not re-adopt flow leg %s of %s",
                         getattr(s, "id", ""), rec.pipeline_id, exc_info=True)

    async def _admission_spend(self, rec: PipelineRecord, s: Any, *, capped: bool,
                               adopt: Optional[dict]) -> tuple[Optional[float], str]:
        """#1441 S4 — ``(spent_usd, refusal)`` for a flow leg's budget reservation.

        Read only under a capped series. For a FRESH leg an unreadable spend is a
        refusal (unknown is never "nothing spent"). For an ADOPTED leg (#1429) it is
        never a refusal — the adopted attempt was admitted before and never
        reserves, and refusing it would strand the live engine job — so the spend
        is read best-effort and, if unreadable, handed on as ``None`` (unknown):
        ``execute_leg`` then refuses any FRESH attempt that follows (a retry), the
        same outcome the Temporal owner reaches by reading before it adopts.
        """
        if not capped:
            return 0.0, ""
        try:
            return await pipeline_spent_usd(rec, self.dispatcher.store, exclude_step=s.id), ""
        except Exception as e:  # noqa: BLE001 — a refusal for a fresh leg, unknown for an adopted one
            logger.exception("spend of pipeline %s unreadable", rec.pipeline_id)
            if adopt is not None:
                return None, ""
            return 0.0, (f"budget: the spend of earlier legs is unreadable "
                         f"({type(e).__name__}); leg not admitted")

    async def _execute_and_settle_flow_leg(
        self, rec: PipelineRecord, s: Any, run: Any, resolved: dict,
        *, adopt: Optional[dict] = None,
    ) -> None:
        gov = getattr(rec, "governance", None) or {}

        # #1429 — the leg's durable side-channels (persist handle, heartbeat +
        # checkpoint relay, retry/resume visibility) come from ONE assembly
        # shared with the Temporal owner: two owners, one durability story.
        engine = self._leg_engine(s)
        hooks = FlowLegHooks(
            pipeline_id=rec.pipeline_id, step_id=s.id, leg_ref=s.ref, run_id=run.id,
            pipeline_store=self.store, run_store=self.dispatcher.store,
            emit=self.dispatcher._emit,
            # #1316 — leg outputs register in the SAME store the relay uses.
            artifact_store=self._checkpoint_store(), engine=engine, bus=self._bus,
            audit=lambda ev, det: self._audit(rec, s.id, ev, det),
            section=self._lock,
        )

        # H2 (#1244) — an explicit `retry:` block on the leg's `with:` opts into
        # (or tunes) retries; popped so it never reaches the flow config. An ml
        # leg without one never retries (paid compute is not re-spent silently).
        retry_policy = _leg_retry_policy(resolved.pop("retry", None)) if isinstance(resolved, dict) else None

        # #1441 S4 — the reservation is checked against what this pipeline's
        # earlier legs ACTUALLY spent, so an overspent paid leg refuses the next
        # one. Read only under a capped series; see `_admission_spend` for the
        # fresh (unreadable → refused) vs adopted (unreadable → unknown, never
        # refused; a fresh retry after it is refused by execute_leg) rule.
        series_id = str(gov.get("series", "") or "")
        budget_usd = budget_cap_usd(rec)   # ADR-0155 D4: the recorded effective cap
        spent_usd, spend_error = await self._admission_spend(
            rec, s, capped=bool(series_id and budget_usd), adopt=adopt)

        outcome = LegOutcome(state="failed", error=spend_error) if spend_error else await execute_leg(
            s.ref, inputs=resolved, engine=engine,
            run_id=run.id,
            series_id=series_id,
            # ADR-0151 D2/D5 — a priced leg reserves its RECORDED estimate and settles
            # its box time; any other leg reserves `governance.estimated_usd` as before.
            **leg_price_kwargs(rec, s.id, run),
            spent_usd=spent_usd,
            budget_usd=budget_usd,
            # #1288 — the declared leg timeout IS the poll budget; without one,
            # a generous pipeline default. Before this line every flow leg ran
            # under execute_leg's 3600s default, whose expiry CANCELS the
            # engine job — any >1h pipeline train leg was killed at the hour.
            poll_budget_s=float(getattr(s, "timeout_s", None)
                                or _FLOW_LEG_DEFAULT_BUDGET_S),
            # ADR-0118 D4 — the stage contracts the leg was admitted with, from the record.
            stage_contracts=recorded_stage_contracts(getattr(rec, "plan", None), s.id),
            # H1 persist-handle · H1 heartbeat + H6b relay · H2 announce ·
            # H6c resume selection · #1316 registry · #1429 adopt-by-handle.
            **hooks.execute_kwargs(retry=retry_policy, adopt=adopt),
        )
        # H6b — final relay: catch a checkpoint the last beat didn't see
        # (e.g. the terminal stage checkpointed and the leg finished fast).
        await hooks.relay_checkpoints()
        # #1180 — expose the leg's DECLARED produces as its step output, so a
        # downstream wire (`$steps.readiness.export`) resolves to a VALUE. The
        # engine reports stage-scoped stats ("export.jsonl.output"); the
        # pipeline promised names ("export"). v1 contract, loud on ambiguity:
        #   exact stat-key match → it; exactly ONE final `<stage>.output` stat
        #   → every declared produce maps to it; else the leg FAILS naming the
        #   available keys — never a silent None into the next leg's spec.
        # (The full typed mapping is #1137's runtime half; this run found that
        # only the compile-time check existed.)
        step_output = _flow_leg_output(s, outcome)
        # The engine outcome speaks `succeeded`; the Run model's status enum
        # speaks `done`. sqlite persisted the raw string unchecked, but postgres
        # re-validates on EVERY hydrate — one out-of-enum status poisoned the
        # row so `store.get()` threw forever (found live, first prod E2E).
        # Same for `results`: the model wants list[RunResultSummary], so the
        # engine stats/metrics ride in the result_payloads blob instead.
        await self.dispatcher.store.update(
            run.id,
            status=_leg_run_status(outcome),
            ended_at=_now(),  # #1233 — the console duration column needs a terminal stamp
            error=outcome.error or "",
            # #1441 — the payloads plus the leg's REPORTED cost (Run.cost), one
            # shape with the Temporal owner (leg_hooks.flow_leg_record).
            **flow_leg_record(step_output, outcome),
        )
        # #1180 — publish the terminal event through the SAME path template runs
        # use. Leg chaining is driven entirely by run.done/run.failed on the bus
        # (_on_run_terminal); without this a multi-leg pipeline ran leg 1 and
        # then sat 'running' forever — the bridge had even recorded
        # _run_to_pipeline[run.id] for an event that could never arrive. Going
        # through dispatcher._emit (not a bare bus.publish) also gives flow legs
        # the run-event append + SSE + continuous-eval firehose parity.
        await self.dispatcher._emit(
            run.id,
            "done" if outcome.ok else "failed",
            {"leg": s.ref, "step_id": s.id, "error": outcome.error or ""},
            pipeline_id=rec.pipeline_id,
        )
        await audit_flow_leg_finished(
            rec.pipeline_id, s.id, ref=s.ref, run=run, outcome=outcome, status=_leg_run_status(outcome),
            engine=str((getattr(hooks, "leg_handle", None) or {}).get("engine", "") or ""))

    def _leg_engine(self, s: Any) -> Any:
        """The environment's engine for this leg's FLOW's kind.

        #1512 — resolved through the CORE registry
        (`cairn.core.dataflow.engine_registry`), not by importing an operator
        pack: core picks the engine NAME from the environment and the pack that
        registered it builds it. This used to `from operator_dataflow.engines
        import resolve_engine`, which made one specific pack a hard requirement
        for dispatching any flow leg (ADR-0084 D1).

        #1180 — the kind comes from the flow DEF, not from the plan step:
        `s.flow_kind` never existed on PipelinePlanStep, so this read "data"
        for every leg and an ml leg was handed the data engine."""
        return resolve_engine(None, kind=self._leg_flow_kind(s))

    @staticmethod
    def _leg_flow_kind(s: Any) -> str:
        """The declared kind of a `flow:` leg's flow def; "data" when unknown.

        #2101 — the lookup itself now lives beside `leg_kind_of` as
        `leg_executor.flow_kind_of`, because the launch door has to ask the same
        question before this bridge is ever reached."""
        from cairn.core.dispatch.leg_executor import flow_kind_of  # noqa: PLC0415 - cycle

        return flow_kind_of(getattr(s, "ref", "") or "")

    # ── #1787 (ADR-0097): `finalizer:` — the `finally:` block of a pipeline ──
    #
    # A finalizer step is NOT dependency-driven. It runs when the pipeline
    # reaches a terminal INTENT (done / failed / cancelled) — exactly once, even
    # if its dependency chain never fired — and the record only takes on that
    # terminal status once every finalizer has settled. The two-move terminal is
    # what buys restart safety for free: while a finalizer is owed, the record is
    # still `running`, so `_adopt_in_flight` (which already lists running +
    # paused) finds it with no new sweep and no new query.
    #
    # Every branch below is guarded on the plan actually declaring a finalizer,
    # so a pipeline without one takes byte-identical paths to before (#1759:
    # nothing new where existing machinery serves).

    #: A finalizer step-state that owes nothing more. `skipped` is settled: a
    #: finalizer whose wires never resolved (its producer never ran) has nothing
    #: to tear down, and saying so is not a failure.
    _FINALIZER_SETTLED = ("done", "failed", "skipped")

    @staticmethod
    def _finalizer_awaiting_approval(rec: PipelineRecord, step_id: str) -> bool:
        """#218 — the one step a person may still decide once the terminal intent is
        set: a ``finalizer:`` whose operator a profile or policy gated, parked at
        ``awaiting_approval``. The pipeline cannot settle until it does (its status
        waits on every finalizer), so refusing the decision stranded the pipeline
        in ``running`` and left whatever the finalizer releases (a paid box) held.
        Every other step stays refused (#1787): no new work starts after the
        teardown began."""
        step = rec.plan.step(step_id) if rec.plan else None
        st = rec.step_state.get(step_id)
        return bool(getattr(step, "finalizer", False)) and st is not None \
            and st.status == "awaiting_approval"

    @staticmethod
    def _finalizer_steps(rec: PipelineRecord) -> list[Any]:
        """The plan's finalizer steps, in plan order (empty for every pipeline
        authored before #1787 — the whole feature keys off this being empty)."""
        plan = getattr(rec, "plan", None)
        return [s for s in (getattr(plan, "steps", None) or [])
                if getattr(s, "finalizer", False)]

    def _unsettled_finalizers(self, rec: PipelineRecord) -> list[str]:
        """Finalizer step ids that have not reached a settled state yet."""
        return [
            s.id for s in self._finalizer_steps(rec)
            if (rec.step_state.get(s.id) or StepState()).status
            not in self._FINALIZER_SETTLED
        ]

    async def _run_finalizers(self, rec: PipelineRecord) -> None:
        """Submit every finalizer the pipeline still owes. Exactly-once.

        The once-ness guard is the step's OWN durable status, not a flag: only a
        ``pending`` finalizer is submitted, and ``_submit_step`` moves it to
        ``running`` synchronously inside the per-pipeline critical section — the
        same section every other transition takes, so two replicas cannot both
        submit it.

        A finalizer whose wires cannot resolve is ``skipped`` with the cited
        reason rather than failing the pipeline: a release whose acquire never
        ran has nothing to release, and turning "nothing to do" into a failure
        would make the teardown the loudest part of every early exit. (The
        managed pipelines wire release off ``$input``, not off acquire, exactly
        so this stays a belt rather than the normal path.)
        """
        if not rec.terminal_intent:
            return
        plan = rec.plan
        raw_outputs = await self._collected_outputs(rec)
        for s in self._finalizer_steps(rec):
            st = rec.step_state.get(s.id) or StepState()
            if st.status != "pending":
                continue
            outputs = iteration_view(raw_outputs, s.id, (plan.loops if plan else None) or {})
            try:
                resolve_step_inputs(s, bundle_input=rec.inputs, steps=outputs)
            except WireResolutionError as e:
                st.status = "skipped"
                st.verdict = "finalizer_unresolved"
                st.ended_at = _now()
                rec.step_state[s.id] = st
                await self._audit(rec, s.id, "finalizer_skipped", {"error": str(e)})
                await audit_leg_finished(rec.pipeline_id, s.id, ref=s.ref, state="skipped",
                                         reason=f"finalizer skipped: {e}")
                continue
            try:
                if await self._submit_step(rec, s, outputs) is _REFUSED:
                    continue  # the step is failed with its reason; nothing started
                await self._audit(rec, s.id, "finalizer_started",
                                  {"terminal_intent": rec.terminal_intent})
            except Exception as e:  # noqa: BLE001 — a finalizer that cannot even
                # be submitted must not strand the pipeline in `running` forever;
                # it settles as failed and the terminal status still lands.
                logger.exception("finalizer %s could not be submitted", s.id)
                st = rec.step_state.get(s.id) or StepState()
                st.status = "failed"
                st.ended_at = _now()
                rec.step_state[s.id] = st
                await self._audit(rec, s.id, "finalizer_failed",
                                  {"error": f"{type(e).__name__}: {e}"})

    async def _settle(self, rec: PipelineRecord, *, failure_reason: str = "") -> None:
        """Recompute the status, run any owed finalizer, persist — and announce a
        NEWLY terminal pipeline exactly once.

        Replaces the ``_recompute_status(rec); await store.put(rec)`` pair. With
        no finalizer on the plan it IS that pair, call for call.
        """
        was_terminal = rec.status in PIPELINE_TERMINAL
        self._recompute_status(rec)
        if self._finalizer_steps(rec) and rec.status not in PIPELINE_TERMINAL:
            await self._run_finalizers(rec)
            # a finalizer that resolved to `skipped` settled synchronously, so
            # the terminal status can land in this same pass.
            self._recompute_status(rec)
        await self.store.put(rec)
        if not was_terminal and rec.status in PIPELINE_TERMINAL:
            await self._announce_terminal(rec, failure_reason)

    async def _announce_terminal(self, rec: PipelineRecord, failure_reason: str = "") -> None:
        """The side effects a terminal transition owes — fired on the transition,
        so a deferred failure announces when it is APPLIED, not when it was
        decided (and never twice)."""
        await self._audit_pipeline_finished(rec, failure_reason)
        if rec.status != "failed":
            return
        reason = failure_reason or str((rec.trigger or {}).get("failure_reason") or "")
        await self._audit(rec, "", "pipeline_failed", {"reason": reason})
        await self._notify_pipeline_failed(rec, reason)
        logger.info("pipeline %s failed: %s", rec.pipeline_id, reason)

    async def _audit_pipeline_finished(self, rec: PipelineRecord, failure_reason: str = "") -> None:
        """#233 — ``pipeline.finished``: the terminal state with the pipeline's total settled
        spend, once per pipeline per process. Read from the legs' run records, never estimated."""
        # setdefault: a bridge built without __init__ (tests use __new__) has no set yet
        audited = self.__dict__.setdefault("_audited_terminal", set())
        if rec.pipeline_id in audited or not audit_bound():
            return
        audited.add(rec.pipeline_id)
        try:
            spent = await pipeline_spent_usd(rec, self.dispatcher.store)
        except Exception:  # noqa: BLE001 — unknown spend is omitted, never reported as 0
            spent = None
        reason = failure_reason or str((rec.trigger or {}).get("failure_reason") or "")
        await audit_pipeline_event(
            "pipeline.finished", pipeline_id=rec.pipeline_id,
            outcome={"done": "ok", "cancelled": "ok"}.get(rec.status, "error"),
            state=rec.status, reason=reason, spent_usd=spent,
            budget_usd=((getattr(rec, "governance", None) or {}).get("budget") or {}).get("usd"))

    def _recompute_status(self, rec: PipelineRecord) -> None:
        """`done` when every step is done/skipped; `paused` when something waits on a
        human and nothing is in flight; else `running`.

        A leg skipped by a false `when` (verdict `routed_off`, #1251) is `skipped`
        like any other, so a pipeline whose optional legs did not run still
        reaches `done` — the whole point of routing rather than gating.

        #1787: with a ``finalizer:`` on the plan the terminal decision is made in
        two moves. The *intent* is decided over the non-finalizer steps (they are
        the pipeline's actual work); the *status* only becomes terminal once
        every finalizer has settled. Until then the record honestly reads
        `running`, because it is: the `finally:` block is executing."""
        finals = {s.id for s in self._finalizer_steps(rec)}
        was_terminal = rec.status in PIPELINE_TERMINAL
        if not finals:
            statuses = [v.status for v in rec.step_state.values()]
            if all(x in ("done", "skipped") for x in statuses):
                rec.status = "done"
                rec.ended_at = _now()
                if not was_terminal:
                    _settle_span(rec)
                self._cleanup_pipeline_triggers(rec.pipeline_id)
            elif (any(x in ("awaiting_advance", "awaiting_approval") for x in statuses)
                  and not any(x in ("running", "queued") for x in statuses)):
                rec.status = "paused"
            else:
                rec.status = "running"
            return

        if not rec.terminal_intent:
            statuses = [v.status for sid, v in rec.step_state.items() if sid not in finals]
            if all(x in ("done", "skipped") for x in statuses):
                rec.terminal_intent = "done"
            elif (any(x in ("awaiting_advance", "awaiting_approval") for x in statuses)
                  and not any(x in ("running", "queued") for x in statuses)):
                rec.status = "paused"
                return
            else:
                rec.status = "running"
                return
        if self._unsettled_finalizers(rec):
            rec.status = "running"  # the `finally:` block is still executing
            return
        rec.status = rec.terminal_intent
        rec.ended_at = _now()
        if not was_terminal:
            _settle_span(rec)
        self._cleanup_pipeline_triggers(rec.pipeline_id)

    # ── human-in-the-loop: advance a manual / approval step ─────────────────

    async def advance_step(
        self, pipeline_id: str, step_id: str, *, approver: Optional[str] = None,
        rationale: str = "", fired_at: Optional[datetime] = None,
    ) -> PipelineRecord:
        """Advance a step that is waiting on a human (a ``manual`` trigger or
        ``approval = true``). Re-evaluates the gate first — advancing can NOT bypass
        governance — then submits the step as its own governed run. ``approver`` records
        who approved (governance trail).

        ADR-0125 D5 — a step waiting on a clock or a topic (``awaiting_schedule``,
        ``awaiting_event``) names its fire before it submits: the key
        ``"<trigger>@<fire time, UTC, to the second>"`` (:func:`step_fire_key`,
        derived from ``fired_at`` — the fire's own time, now when a person fires
        it) is written to the step and the record is put BEFORE ``_submit_step``.
        A crash between the two then leaves a step that adopt parks for a person
        instead of re-arming (:meth:`_park_fired_step`); a fire whose key is
        already stored is refused as the same fire. The in-process path only: a
        Temporal-owned pipeline's workflow owns its own timers and signals."""
        async with self._lock(pipeline_id):
            rec = await self.store.get(pipeline_id)
            if rec is None:
                raise ValueError(f"no pipeline {pipeline_id!r}")
            if rec.status in ("done", "failed", "cancelled"):
                raise ValueError(f"pipeline {pipeline_id!r} is {rec.status}")
            # #1787 — the record still reads `running` while a `finalizer:` runs,
            # so the status check above is not enough: this door submits its step
            # DIRECTLY, bypassing `_advance`'s terminal-intent guard. Without
            # this, approving a gate on a cancelled pipeline would start a leg
            # after the teardown had already begun.
            if getattr(rec, "terminal_intent", "") and not self._finalizer_awaiting_approval(rec, step_id):
                raise ValueError(
                    f"pipeline {pipeline_id!r} is finalizing "
                    f"({rec.terminal_intent}) — no further step can advance")
            st = rec.step_state.get(step_id)
            if st is None or not st.status.startswith("awaiting_"):
                raise ValueError(
                    f"step {step_id!r} is not awaiting (status="
                    f"{st.status if st else 'unknown'})"
                )
            awaiting = st.status
            # Temporal-owned pipeline (Phase 4): the durable workflow holds the wait —
            # deliver an `approve_step` signal; the workflow re-checks the gate + advances.
            if (rec.trigger or {}).get("owner") == "temporal":
                _gate_span(rec, step_id, "approved" if approver else "advanced", approver or "")  # #222
                # #2299 — record the approver BEFORE the signal: the released step's
                # SUBMIT re-runs the flow-leg gate against the stored approval, and a
                # signal that wins the race would re-gate it as unapproved.
                if approver:
                    st.approver = approver
                    rec.step_state[step_id] = st
                    await self.store.put(rec)
                await self._audit_gate_decision(rec, step_id, approver, awaiting)   # #233 — before the leg it releases
                await self._signal_temporal(pipeline_id, "approve_step", step_id)
                await self._audit(rec, step_id, "approved" if approver else "advanced",
                                  {"approver": approver, "owner": "temporal"})
                await self._decide_review(pipeline_id, step_id, "approved",
                                          reviewer=approver or "", rationale=rationale)
                return rec
            step = rec.plan.step(step_id)
            fire_key: Optional[str] = None
            if st.status in _TRIGGERED_WAITS:
                fire_key = step_fire_key(step.trigger, fired_at or datetime.now(timezone.utc))
                if st.fired_key == fire_key:
                    # the same tick (or receipt second) twice: a fire already began
                    # for it, and its leg may exist — this one is the same fire.
                    await self._audit(rec, step_id, "fire_refused", {
                        "fired_key": fire_key, "reason": "a fire with this key already began"})
                    raise ValueError(f"step {step_id!r} already fired for {fire_key!r}")
            outputs = iteration_view(
                await self._collected_outputs(rec), step_id, rec.plan.loops or {})  # #1671
            if step.gate and not eval_gate_predicate(step.gate, bundle_input=rec.inputs, steps=outputs):
                st.status = "skipped"
                st.verdict = "gate_blocked"
                rec.step_state[step_id] = st
                self._cleanup_step_triggers(pipeline_id, step_id)
                await self._fail_pipeline(rec, f"step '{step_id}' blocked by gate: {step.gate}")
                await self._decide_review(pipeline_id, step_id, "rejected",
                                          reviewer=approver or "",
                                          rationale=f"blocked by gate: {step.gate}")
                return await self.store.get(pipeline_id)  # type: ignore[return-value]
            _gate_span(rec, step_id, "approved" if approver else "advanced", approver or "")  # #222
            if approver:
                st.approver = approver
            if fire_key is not None:
                # ADR-0125 D5 — write-ahead: the record says "this fire began"
                # before any leg exists, inside the critical section.
                st.fired_key = fire_key
                rec.step_state[step_id] = st
                await self.store.put(rec)
                await self._audit(rec, step_id, "fire_recorded", {"fired_key": fire_key})
            self._cleanup_step_triggers(pipeline_id, step_id)  # a schedule/event step is now firing
            await self._audit_gate_decision(rec, step_id, approver, awaiting)   # #233 — before the leg it releases
            if await self._submit_step(rec, step, outputs) is _REFUSED:
                await self._decide_review(
                    pipeline_id, step_id, "rejected", reviewer=approver or "",
                    rationale=(rec.trigger or {}).get("failure_reason") or _NO_WORKSPACE_REASON)
                return await self.store.get(pipeline_id)  # type: ignore[return-value]
            await self._audit(rec, step_id, "approved" if approver else "advanced",
                              {"approver": approver})
            if approver:
                # #1468 — the approval that released the gate decides its review
                # (same path, never a parallel approval surface).
                await self._decide_review(pipeline_id, step_id, "approved",
                                          reviewer=approver, rationale=rationale)
            self._recompute_status(rec)
            await self.store.put(rec)
            return rec

    async def _audit_gate_decision(self, rec: PipelineRecord, step_id: str, approver: Optional[str],
                                   awaiting: str) -> None:
        """#233 — ``pipeline.gate_approved`` (a person, the token's identity) or
        ``pipeline.step_advanced`` (a manual/schedule/event wait released with no named
        person), in the hash-chained trail. Written by the control plane for BOTH owners."""
        await audit_pipeline_event(
            "pipeline.gate_approved" if approver else "pipeline.step_advanced",
            pipeline_id=rec.pipeline_id, step_id=step_id, actor=approver, wait=awaiting,
            owner=(rec.trigger or {}).get("owner") or "inprocess")

    async def reject_step(
        self, pipeline_id: str, step_id: str, *, decider: str, reason: str,
    ) -> PipelineRecord:
        """Reject a gate parked at ``awaiting_approval`` (ADR-0156 D5) — the attributable
        "no" beside :meth:`advance_step`'s "yes". The step becomes ``skipped`` with the
        verdict ``rejected`` and ``approver=decider``; the pipeline fails through
        :meth:`_fail_pipeline` (so finalizers run and in-flight legs are handled as on any
        failure), with ``step '<step>' rejected by <decider>: <reason>`` as the failure
        reason; the gate's review is decided ``rejected`` (reviewer + rationale); and a
        ``pipeline.rejected`` event carries ``{step, decider, reason}``. The pipeline's
        status vocabulary is unchanged: the status is ``failed``, the verdict tells a
        rejection from any other failure. Any other wait is refused (``ValueError``):
        a manual, schedule or event wait is released or cancelled, not rejected.

        Under a Temporal owner the verdict and decider are recorded first, then the
        ``reject_step`` signal (the twin of ``approve_step``) ends the durable workflow as
        failed; it schedules nothing more."""
        async with self._lock(pipeline_id):
            rec = await self.store.get(pipeline_id)
            if rec is None:
                raise ValueError(f"no pipeline {pipeline_id!r}")
            if rec.status in ("done", "failed", "cancelled"):
                raise ValueError(f"pipeline {pipeline_id!r} is {rec.status}")
            if getattr(rec, "terminal_intent", "") and not self._finalizer_awaiting_approval(rec, step_id):
                raise ValueError(
                    f"pipeline {pipeline_id!r} is finalizing "
                    f"({rec.terminal_intent}) — no step can be rejected")
            st = rec.step_state.get(step_id)
            if st is None or st.status != "awaiting_approval":
                raise ValueError(
                    f"step {step_id!r} is not awaiting approval (status="
                    f"{st.status if st else 'unknown'}) — only a gate parked at "
                    f"awaiting_approval can be rejected")
            reason_line = f"step '{step_id}' rejected by {decider}: {reason}"
            _gate_span(rec, step_id, "rejected", decider)  # #222
            st.verdict = "rejected"
            st.approver = decider
            if (rec.trigger or {}).get("owner") == "temporal":
                rec.step_state[step_id] = st
                rec.trigger = {**(rec.trigger or {}), "failure_reason": reason_line}
                await self.store.put(rec)
                await self._signal_temporal(pipeline_id, "reject_step", step_id)
            else:
                st.status = "skipped"
                st.ended_at = st.ended_at or _now()
                rec.step_state[step_id] = st
                self._cleanup_step_triggers(pipeline_id, step_id)
                await self._fail_pipeline(rec, reason_line)
            await self._decide_review(pipeline_id, step_id, "rejected",
                                      reviewer=decider, rationale=reason)
            await self._audit(rec, step_id, "rejected",
                              {"step": step_id, "decider": decider, "reason": reason})
            await audit_pipeline_event(
                "pipeline.gate_rejected", pipeline_id=pipeline_id, step_id=step_id, actor=decider,
                outcome="deny", reason=reason, owner=(rec.trigger or {}).get("owner") or "inprocess")
            return await self.store.get(pipeline_id)  # type: ignore[return-value]

    async def cancel_pipeline(self, pipeline_id: str, *, actor: Optional[str] = None) -> PipelineRecord:
        """Cancel a pipeline as a unit (no new steps submit).

        #2540 — a child run a gate paused at ``awaiting_approval`` is settled
        ``cancelled`` (:meth:`_cancel_stranded_child_runs`): nothing executes
        it and nobody can approve a leg of a cancelled pipeline. ``queued`` and
        ``running`` child runs are left to their own paths — a leg executing in
        some process finishes and its completion no-ops once the pipeline is
        terminal; a queued leg settles itself (`_settle_cancelled_while_queued`).
        One exception (ADR-0151 D9): a ``running`` FLOW leg on a box the
        pipeline releases — its recorded compute target equals the connection a
        ``finalizer:`` ``compute.release`` step resolves to — has its recorded
        engine job cancelled through ``cancel_recorded_job`` BEFORE the
        finalizers are submitted (:meth:`_cancel_legs_on_released_boxes`), so the
        box stop is never the only kill. The run itself still settles through
        its own path; a cancel that fails is logged and audited and never holds
        the release back. Every other running leg is unchanged (#207).
        Cancel is one-shot, so it never sees an owner DEAD (that takes two probes,
        ADR-0125 D3) and leaves ``queued``/``running`` legs alone. A leg still
        ``running`` when its process dies AFTER this cancel is settled
        ``cancelled`` by the reconciler's sweep once its owner is DEAD (ADR-0125
        D4), with its remote jobs and budget holds closed; a leg with no owner
        (written before ADR-0125) is left to the one-off cleanup script
        (``scripts/ops/settle_dangling_runs.py``, D6).
        A finalizer's run is never touched: the teardown is what a cancel still
        owes (#1787). A Temporal-owned pipeline has its workflow cancelled
        first, which is how its executing legs stop; #233: ``actor`` (the caller's token
        identity) goes on the audit trail with the cancel; #2555 (8): its
        ``awaiting_approval`` child runs are then settled the same way, because
        nothing in the workflow or its activities does (cancelling the workflow
        cancels only the activity awaiting the run). Calling cancel again on a
        cancelled pipeline settles any ``awaiting_approval`` run an earlier
        cancel left."""
        stranded_owner = False
        async with self._lock(pipeline_id):
            rec = await self.store.get(pipeline_id)
            if rec is None:
                raise ValueError(f"no pipeline {pipeline_id!r}")
            # A repeated cancel also settles runs an earlier cancel left behind.
            stranded_owner = rec.status not in ("done", "failed")
            if rec.status not in ("done", "failed", "cancelled"):
                if (rec.trigger or {}).get("owner") == "temporal":
                    await self._cancel_temporal(pipeline_id)  # cancel the durable workflow
                self._cleanup_pipeline_triggers(pipeline_id)
                await self._audit(rec, "", "pipeline_cancelled", {})
                await audit_pipeline_event(
                    "pipeline.cancelled", pipeline_id=pipeline_id, actor=actor, prior_status=rec.status,
                    owner=(rec.trigger or {}).get("owner") or "inprocess")
                for sid in self._settle_human_waits_on_cancel(rec):
                    await audit_leg_finished(pipeline_id, sid, ref=getattr(rec.plan.step(sid), "ref", ""),
                                             state="cancelled", reason="pipeline cancelled while waiting")
                if self._finalizer_steps(rec):
                    # #1787 — cancel is the third terminal state a `finally:`
                    # must survive, and the one that used to leave a box billing
                    # most often (someone stops a run mid-train). The cancel is
                    # recorded as the INTENT immediately — no further work of the
                    # pipeline's own can start once it is set — and the record
                    # becomes `cancelled` when the teardown has settled.
                    rec.terminal_intent = "cancelled"
                    # ADR-0151 D9 — the job on a box this pipeline releases is
                    # cancelled BEFORE the release stops the box, so a box stop
                    # is never the only kill. Best-effort: it never blocks it.
                    await self._cancel_legs_on_released_boxes(rec)
                    await self._settle(rec)
                else:
                    rec.status = "cancelled"
                    rec.ended_at = _now()
                    _settle_span(rec)
                    await self.store.put(rec)
                    await self._audit_pipeline_finished(rec)
                # The human queue closes at REQUEST time either way: a cancelled
                # pipeline must not still be collecting approvals while its
                # teardown runs.
                await self._close_open_reviews(pipeline_id, "pipeline cancelled",
                                               workspace_id=rec.workspace_id)
        # Outside the critical section: the `run.cancelled` emits below reach
        # `_on_run_terminal`, which takes this pipeline's section itself.
        if stranded_owner:
            await self._cancel_stranded_child_runs(rec)
        return rec

    #: The capability a box-stopping finalizer provides (`compute/release`, ADR-0151 D6).
    _RELEASE_CAPABILITY = "compute.release"

    def _is_release_step(self, s: Any) -> bool:
        """A task step whose operator provides ``compute.release``: from its manifest,
        else from the class's declared capabilities, so a registry without the
        manifest metadata does not fail open (the box would stop under its job)."""
        ref = str(getattr(s, "ref", "") or "")
        if leg_kind_of(ref) != "task":
            return False
        name = ref[len("task:"):]
        try:
            from cairn.core.operator.registry import default_registry  # noqa: PLC0415 — cycle, as _task_op_risk

            md = default_registry.get_metadata(name)
            if md is not None:
                caps = getattr(md, "provides_capabilities", None) or []
            else:
                caps = getattr(default_registry.get(name), "capabilities", None) or []
        except Exception:  # noqa: BLE001 — unknown operator: not a release
            return False
        return self._RELEASE_CAPABILITY in list(caps)

    async def _released_boxes(self, rec: PipelineRecord) -> set[str]:
        """The connections this pipeline's ``finalizer:`` release steps will stop,
        resolved the way the finalizer itself will resolve its inputs."""
        boxes: set[str] = set()
        steps = [s for s in self._finalizer_steps(rec) if self._is_release_step(s)]
        if not steps:
            return boxes
        raw_outputs = await self._collected_outputs(rec)
        loops = (rec.plan.loops if rec.plan else None) or {}
        for s in steps:
            try:
                resolved = resolve_step_inputs(s, bundle_input=rec.inputs,
                                               steps=iteration_view(raw_outputs, s.id, loops))
            except WireResolutionError:
                continue  # the finalizer will be skipped too: it stops nothing
            box = str((resolved or {}).get("connection") or "").strip()
            if box:
                boxes.add(box)
        return boxes

    async def _cancel_legs_on_released_boxes(self, rec: PipelineRecord) -> None:
        """ADR-0151 D9 — on a pipeline cancel, cancel the engine job of every
        ``running`` flow leg on a box this pipeline's ``compute.release`` finalizer
        is about to stop, BEFORE the finalizers are submitted.

        The leg's box is its RECORDED compute target (``Run.compute_target``, set
        at admission), falling back to :meth:`_leg_target` for a run that carries
        none. The cancel goes through :func:`cancel_recorded_job` with the handle
        the leg persisted (``StepState.handle``); it never raises. A cancel that
        was not sent is logged there and audited here (``leg_job_cancel``,
        ``sent: false``), and the release still runs: the box must stop either
        way. The run is not settled here — its poll loop sees the cancelled job
        and settles it through its own path. Any other running leg is left
        alone, as before (#207)."""
        boxes = await self._released_boxes(rec)
        if not boxes:
            return
        for s in rec.plan.steps if rec.plan else []:
            st = rec.step_state.get(s.id)
            if (st is None or st.status != "running" or not st.handle
                    or leg_kind_of(getattr(s, "ref", "")) != "flow"):
                continue
            target = ""
            if st.run_id:
                try:
                    run = await self.dispatcher.store.get(st.run_id)
                    target = str(getattr(run, "compute_target", "") or "")
                except Exception:  # noqa: BLE001 — fall back to the selection below
                    logger.debug("leg %s: run %s unreadable", s.id, st.run_id, exc_info=True)
            target = target or self._leg_target(s)
            if target not in boxes:
                continue
            try:
                engine = self._leg_engine(s)
            except Exception:  # noqa: BLE001 — no engine to reach: report it, release anyway
                logger.exception("leg %s (%s): no engine to cancel the job on before release",
                                 s.id, st.run_id)
                engine = None
            sent = await cancel_recorded_job(engine, st.handle)
            if not sent:
                logger.warning("leg %s (%s): job %s on %s was not cancelled before release",
                               s.id, st.run_id, (st.handle or {}).get("job_id"), target)
            await self._audit(rec, s.id, "leg_job_cancel",
                              {"run_id": st.run_id, "job_id": (st.handle or {}).get("job_id"),
                               "target": target, "sent": sent, "before": "release"})

    async def _cancel_stranded_child_runs(self, rec: PipelineRecord) -> None:
        """#2540 — settle the cancelled pipeline's ``awaiting_approval`` child runs.

        Candidates are the runs its steps name plus any ``awaiting_approval`` run
        carrying its lineage that no step recorded. The dispatcher's own
        :meth:`cancel` runs first for each — it closes budget reservations and
        stops a waiting dispatcher task — then the run is marked ``cancelled``
        with the reason, and ``run.cancelled`` is emitted only when that update
        actually landed (the terminal-immutability guard may refuse it, and a
        run already cancelled is not announced twice). Best-effort per run: one
        store error must not leave the others."""
        pid = rec.pipeline_id
        finals = {s.id for s in self._finalizer_steps(rec)}
        candidates: dict[str, Any] = {}
        for sid, st in rec.step_state.items():
            if sid in finals or not st.run_id:
                continue
            try:
                run = await self.dispatcher.store.get(st.run_id)
            except Exception:  # noqa: BLE001
                run = None
            if run is not None and str(getattr(run, "status", "") or "") == "awaiting_approval":
                candidates[run.id] = run
        scanned = await self._active_child_runs(workspace_id=rec.workspace_id,
                                                statuses=("awaiting_approval",))
        for run in scanned.get(pid, []):
            if str(getattr(run, "step_id", "") or "") not in finals:
                candidates.setdefault(run.id, run)
        for run in candidates.values():
            if str(getattr(run, "status", "") or "") != "awaiting_approval":
                continue
            step_id = str(getattr(run, "step_id", "") or "")
            reason = (f"pipeline cancelled while leg '{step_id}' was awaiting "
                      "approval; no one can approve it any more")
            try:
                cancel = getattr(self.dispatcher, "cancel", None)
                if callable(cancel):
                    await cancel(run.id)
                cur = await self.dispatcher.store.get(run.id)
                if cur is None or str(getattr(cur, "status", "") or "") != "awaiting_approval":
                    continue  # the dispatcher's cancel already settled (and announced) it
                updated = await self.dispatcher.store.update(
                    run.id, status="cancelled", ended_at=_now(), error=reason)
                if str(getattr(updated, "status", "") or "") != "cancelled":
                    continue  # refused (terminal-immutability guard) — nothing to announce
                await self.dispatcher._emit(
                    run.id, "cancelled",
                    {"leg": str(getattr(run, "template_ref", "") or ""),
                     "step_id": step_id, "error": reason},
                    pipeline_id=pid)
            except Exception:  # noqa: BLE001 — settle the rest regardless
                logger.exception("pipeline %s: could not cancel child run %s", pid, run.id)

    def _settle_human_waits_on_cancel(self, rec: PipelineRecord) -> list[str]:
        """#2541 — a step parked on a person will never run once its pipeline is
        cancelled, so it stops reading as waiting: `skipped` with verdict
        `cancelled`, the same settlement `_settle_cancelled_while_queued` gives a
        leg that lost its slot. Without it the approvals inbox kept listing a gate
        no one can act on (`advance_step` refuses a cancelled pipeline).

        Finalizer steps are left alone (#1787): their settlement is the teardown's
        own business, and marking one `skipped` here would count it as settled."""
        finals = {s.id for s in self._finalizer_steps(rec)}
        settled: list[str] = []
        for sid, st in rec.step_state.items():
            if sid in finals or st.status not in ("awaiting_approval", "awaiting_advance"):
                continue
            st.status = "skipped"
            st.verdict = "cancelled"
            st.ended_at = _now()
            settled.append(sid)
        return settled

    # ── Temporal-owned pipelines (Phase 4): signal / cancel the workflow ────

    async def _signal_temporal(self, pipeline_id: str, signal: str, *args: Any) -> None:
        from temporalio.client import Client  # noqa: PLC0415 — temporal extra

        from cairn.core import settings  # noqa: PLC0415

        client = await Client.connect(settings.temporal_address(), namespace=settings.temporal_namespace())
        await client.get_workflow_handle(pipeline_id).signal(signal, *args)

    async def _cancel_temporal(self, pipeline_id: str) -> None:
        from temporalio.client import Client  # noqa: PLC0415

        from cairn.core import settings  # noqa: PLC0415

        try:
            client = await Client.connect(settings.temporal_address(), namespace=settings.temporal_namespace())
            await client.get_workflow_handle(pipeline_id).cancel()
        except Exception as e:  # noqa: BLE001 — the record is marked cancelled regardless
            logger.debug("cancel Temporal pipeline %s failed: %s", pipeline_id, e)

    # ── ADR-0069 §3c: event-driven triggers (schedule:<cron> / on:<event>) ──

    # ── Review records (#1468) — the decision object attached to HITL gates.
    # All three helpers are ADVISORY: a review-store failure logs and never
    # blocks the gate flow (the queue itself stays the source of truth).

    def _review_store(self) -> Any:
        from cairn.core.storage.reviews import build_review_store  # noqa: PLC0415 - lazy: keep storage off the bridge import path
        return build_review_store()

    async def _open_review(self, rec: PipelineRecord, s: Any, gate_ctx: dict) -> None:
        try:
            from cairn.core.storage.reviews import ReviewRecord, pipeline_review_id  # noqa: PLC0415 - lazy, see _review_store
            lineage = {sid: st.run_id for sid, st in rec.step_state.items() if st.run_id}
            await self._review_store().open_review(ReviewRecord(
                review_id=pipeline_review_id(rec.pipeline_id, s.id),
                subject_ref=f"{rec.pipeline_id}/{s.id}",
                title=f"Approve {s.id}",
                pipeline_id=rec.pipeline_id, step_id=s.id,
                gate_snapshot=gate_ctx or {},
                lineage=lineage,
                # #1639 — from the RECORD, not current_workspace(): this also
                # runs in a Temporal worker where the ContextVar is unset.
                workspace_id=rec.workspace_id,
            ))
        except Exception:  # noqa: BLE001 — advisory record, never gate-blocking
            logger.exception("review open failed for %s/%s", rec.pipeline_id, s.id)

    async def _decide_review(self, pipeline_id: str, step_id: str, verdict: str,
                             *, reviewer: str = "", rationale: str = "") -> None:
        try:
            from cairn.core.storage.reviews import pipeline_review_id  # noqa: PLC0415 - lazy, see _review_store
            await self._review_store().decide_review(
                pipeline_review_id(pipeline_id, step_id), verdict,
                reviewer=reviewer, rationale=rationale)
        except Exception:  # noqa: BLE001 — advisory record, never gate-blocking
            logger.exception("review decide failed for %s/%s", pipeline_id, step_id)

    async def _close_open_reviews(self, pipeline_id: str, rationale: str,
                                  workspace_id: Optional[str] = None) -> None:
        """A terminal pipeline leaves no dangling open decisions in the inbox."""
        try:
            store = self._review_store()
            for r in await store.list_reviews(status="open", limit=500,
                                              workspace_id=workspace_id):
                if r.pipeline_id == pipeline_id:
                    await store.decide_review(r.review_id, "rejected", rationale=rationale)
        except Exception:  # noqa: BLE001 — advisory record, never gate-blocking
            logger.exception("review close-out failed for %s", pipeline_id)

    async def _mark_awaiting(self, rec: PipelineRecord, s: Any, status: str,
                             *, gate_ctx: Optional[dict] = None) -> None:
        st = rec.step_state.get(s.id) or StepState()
        st.status = status
        st.awaiting_since = _now()  # replay cursor for on:<event> catch-up (#763)
        # ADR-0125 D5 — the fire key belongs to the wait that is ending: a loop
        # iteration's earlier fire must never park a step that has not fired.
        st.fired_key = None
        rec.step_state[s.id] = st
        detail = {"trigger": s.trigger, "status": status}
        if gate_ctx:
            detail["gate_context"] = gate_ctx  # what the approver sees (#1128)
        await self._audit(rec, s.id, "awaiting", detail)
        # #1791 — the pause is the silent state; tell the human. Only the two
        # HUMAN waits notify: `awaiting_schedule` / `awaiting_event` are waiting
        # on a clock and a topic, not on a person, and paging for them would
        # train the operator to ignore the channel.
        if status in ("awaiting_approval", "awaiting_advance"):
            # The demanding authority is the PLAN itself here — this is the
            # declared gate, not one a policy raised mid-leg (that path is
            # `_pause_leg_for_approval`, which carries the merged sources).
            authority = ("pipeline plan: approval: true",) if status == GATE_STATUS \
                else ("pipeline plan: trigger: manual",)
            await self._notify_gate(
                rec, s.id, status,
                operator=str(getattr(s, "ref", "") or ""),
                authority=authority,
                reason=_audience_of(gate_ctx),
                extra_cost=_cost_fields((gate_ctx or {}).get("fields") or {}),
            )

    def _ensure_scheduler(self) -> Any:
        if self._scheduler is None:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler  # noqa: PLC0415 — scheduler tier dep
            self._scheduler = AsyncIOScheduler()
            self._scheduler.start()
        return self._scheduler

    def _schedule_step(self, pipeline_id: str, step_id: str, cron: str) -> None:
        """Register a ``schedule:<cron>`` step — the next matching tick advances it."""
        try:
            from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415

            self._ensure_scheduler().add_job(
                self._auto_advance, CronTrigger.from_crontab(cron.strip()),
                args=[pipeline_id, step_id], id=f"pl:{pipeline_id}:{step_id}", replace_existing=True,
            )
        except Exception:  # noqa: BLE001 — a bad cron holds the step (awaiting_schedule), never crashes
            logger.exception("pipeline %s step %s: bad schedule %r", pipeline_id, step_id, cron)

    async def _subscribe_event(self, pipeline_id: str, step_id: str, event: str) -> None:
        """Register an ``on:<event>`` step — a domain event on that topic fires it.

        On a durable bus (PostgresBus) also REPLAY the topic since the step began awaiting,
        so an event published cross-process, during a restart window, or just before this
        subscription existed still fires the step (#763). Delivery is at-least-once and
        idempotent: ``advance_step`` re-checks the gate and tolerates an already-advanced
        step; ``deliver_event`` (Temporal) is a set-add. On the in-process bus replay is
        unavailable — warn once that on:<event> is neither durable nor cross-process."""
        from cairn.core.bus import get_bus  # noqa: PLC0415 — lazy like BusSkillBridge

        bus = self._bus or get_bus()
        self._event_waiters.setdefault(event, set()).add((pipeline_id, step_id))
        if event not in self._event_subs:
            self._event_subs[event] = await bus.subscribe([event], self._on_domain_event)
        if not self._bus_supports_replay(bus):
            self._warn_inprocess_event(event)
            return
        # replay cursor: when the step started awaiting (tight) else the pipeline's start.
        rec = await self.store.get(pipeline_id)
        since = None
        if rec is not None:
            st = rec.step_state.get(step_id)
            since = (st.awaiting_since if st and st.awaiting_since else None) or rec.created_at
        self._spawn_replay(bus, event, since)

    @staticmethod
    def _bus_supports_replay(bus: Any) -> bool:
        """True for a durable bus (PostgresBus). The in-process bus has a ``replay`` method
        that only raises, so gate on type rather than ``hasattr``."""
        from cairn.core.bus import InProcessBus  # noqa: PLC0415

        return not isinstance(bus, InProcessBus)

    def _warn_inprocess_event(self, event: str) -> None:
        if self._warned_inprocess_event:
            return
        self._warned_inprocess_event = True
        logger.warning(
            "pipeline on:<event> trigger (%r) is on the in-process bus — events are NOT "
            "durable or cross-process (an event during a restart window or from another "
            "engine node is lost). Set CAIRN_BUS=postgres://… for durable on:<event> "
            "chaining (#763).", event,
        )

    def _spawn_replay(self, bus: Any, event: str, since: Optional[str]) -> None:
        """Fire already-published events on ``event`` since ``since`` into the SAME handler
        the live path uses, as a detached task — so it never blocks (or re-enters, under the
        pipeline lock) the arming caller. No-op without a cursor."""
        if not since:
            return
        task = asyncio.create_task(self._replay_events(bus, event, since))
        self._replay_tasks.add(task)
        task.add_done_callback(self._replay_tasks.discard)

    async def _replay_events(self, bus: Any, event: str, since_iso: str) -> None:
        try:
            since = datetime.fromisoformat(since_iso)
        except ValueError:
            return
        try:
            async for msg in bus.replay(event, since):
                await self._on_domain_event(msg)
        except NotImplementedError:
            pass  # a bus without durable replay — the in-process warn already covers it
        except Exception:  # noqa: BLE001 — replay is best-effort catch-up, never fatal
            logger.debug("pipeline on:<event> replay failed for %r", event, exc_info=True)

    async def _emit_step_event(self, rec: PipelineRecord, s: Any, output: dict) -> None:
        """Publish a step's declared ``emits`` domain event on completion, so a downstream
        ``on:<event>`` step — in this pipeline or another bundle — fires (ADR-0069). The
        payload is the step's reconstructed output; the durable bus (#763) persists + replays
        it. Without this, ``emits`` is inert and intra-pipeline event chaining never triggers."""
        event = getattr(s, "emits", None)
        if not event:
            return
        try:
            from cairn.core.bus import get_bus  # noqa: PLC0415 — lazy like the rest of the bus use

            bus = self._bus or get_bus()
            await bus.publish(
                event, dict(output or {}),
                publisher=f"pipeline:{rec.pipeline_id}", correlation_id=rec.pipeline_id,
            )
            await self._audit(rec, s.id, "emitted", {"event": event})
        except Exception:  # noqa: BLE001 — a broadcast failure must not fail the completed step
            logger.debug("pipeline %s step %s: emit %r failed", rec.pipeline_id, s.id, event, exc_info=True)

    async def _register_temporal_event_waiters(self, rec: PipelineRecord) -> None:
        """Subscribe to each ``on:<event>`` step's topic for a Temporal-owned pipeline
        (Slice ③, the bus→signal bridge). The wait itself lives in the durable workflow;
        the bridge just forwards a matching bus event to it as a ``deliver_event`` signal.
        ``schedule:<cron>`` steps are NOT registered here — the workflow owns those via its
        own durable timer (``workflow.sleep``, Slice ②). Idempotent: a step already resolved
        (done/skipped/failed) is skipped, so re-arming on restart never re-subscribes a
        finished step."""
        plan = rec.plan
        if plan is None:
            return
        terminal = {"done", "skipped", "failed"}
        for s in plan.steps:
            if not s.trigger.startswith("on:"):
                continue
            st = rec.step_state.get(s.id)
            if st is not None and st.status in terminal:
                continue
            await self._subscribe_event(rec.pipeline_id, s.id, s.trigger[len("on:"):])

    async def _on_domain_event(self, msg: Any) -> None:
        """A domain event arrived — fire every ``on:<event>`` step waiting on it. An
        in-process pipeline advances directly; a Temporal-owned pipeline's wait lives in
        the durable workflow, so the event is forwarded as a ``deliver_event`` signal
        (the bus→signal bridge, ADR-0069 Phase 4 Slice ③)."""
        topic = getattr(msg, "topic", "")
        for pid, step_id in list(self._event_waiters.get(topic, set())):
            rec = await self.store.get(pid)
            if rec is not None and (rec.trigger or {}).get("owner") == "temporal":
                try:
                    await self._signal_temporal(pid, "deliver_event", topic)
                except Exception:  # noqa: BLE001 — workflow gone/terminal → stop watching for it
                    self._cleanup_step_triggers(pid, step_id)
            else:
                await self._auto_advance(pid, step_id)

    async def _auto_advance(self, pipeline_id: str, step_id: str, *,
                            fired_at: Optional[datetime] = None) -> None:
        """Advance a schedule/event step automatically (no approver). Tolerant — the
        pipeline may be cancelled or the step already moved on. ``fired_at`` is
        the fire's own time (ADR-0125 D5), by default now: the scheduler calls
        this within its misfire grace of the tick, and an event's handler runs on
        receipt. APScheduler passes no scheduled run time, so the tick is derived."""
        try:
            await self.advance_step(pipeline_id, step_id,
                                    fired_at=fired_at or datetime.now(timezone.utc))
        except ValueError:
            self._cleanup_step_triggers(pipeline_id, step_id)  # stale job/waiter → drop it

    def _cleanup_step_triggers(self, pipeline_id: str, step_id: str) -> None:
        if self._scheduler is not None:
            try:
                self._scheduler.remove_job(f"pl:{pipeline_id}:{step_id}")
            except Exception:  # noqa: BLE001 — no such job
                pass
        for topic, waiters in list(self._event_waiters.items()):
            waiters.discard((pipeline_id, step_id))
            if not waiters:
                self._event_waiters.pop(topic, None)
                sub = self._event_subs.pop(topic, None)
                if sub is not None:
                    sub.unsubscribe()

    def _cleanup_pipeline_triggers(self, pipeline_id: str) -> None:
        """Drop every pending schedule/event trigger for a pipeline (terminal / cancel)."""
        if self._scheduler is not None:
            for job in list(self._scheduler.get_jobs()):
                if job.id.startswith(f"pl:{pipeline_id}:"):
                    try:
                        job.remove()
                    except Exception:  # noqa: BLE001
                        pass
        for topic, waiters in list(self._event_waiters.items()):
            for pid, sid in list(waiters):
                if pid == pipeline_id:
                    waiters.discard((pid, sid))
            if not waiters:
                self._event_waiters.pop(topic, None)
                sub = self._event_subs.pop(topic, None)
                if sub is not None:
                    sub.unsubscribe()

    async def _collected_outputs(self, rec: PipelineRecord) -> dict[str, dict]:
        """`step_id → output dict` for every DONE step — the `steps` map the gate +
        input resolvers read, reconstructed from each child run's persisted output."""
        out: dict[str, dict] = {}
        for sid, st in rec.step_state.items():
            if st.status == "done" and st.run_id:
                run = await self.dispatcher.store.get(st.run_id)
                if run is not None:
                    out[sid] = reconstruct_step_output(run)
        return out

    async def _fail_pipeline(self, rec: PipelineRecord, reason: str) -> None:
        rec.trigger = {**(rec.trigger or {}), "failure_reason": reason}
        if not self._finalizer_steps(rec):
            rec.status = "failed"
            rec.ended_at = _now()
            _settle_span(rec)
            self._cleanup_pipeline_triggers(rec.pipeline_id)
            await self._audit(rec, "", "pipeline_failed", {"reason": reason})
            await self.store.put(rec)
            await self._audit_pipeline_finished(rec, reason)   # #233
            # #1791 — opt-in per [profile.notify]; the pause events are the default.
            await self._notify_pipeline_failed(rec, reason)
            logger.info("pipeline %s failed: %s", rec.pipeline_id, reason)
            return
        # #1787 — the plan declares a `finally:`. Record the INTENT and let the
        # finalizers run; `_settle` applies `failed` (and announces it) once they
        # have settled. A `cancelled` intent is never downgraded: the human's
        # cancel is the stronger word, and a leg failing *because* it was
        # cancelled must not rewrite what the record says happened.
        if rec.terminal_intent != "cancelled":
            rec.terminal_intent = "failed"
        await self._settle(rec, failure_reason=reason)

    async def _audit(self, rec: PipelineRecord, step_id: str, event: str, detail: dict) -> None:
        """Best-effort gate-as-trigger / lifecycle audit — a `pipeline.<event>` domain
        event on the bus (the console audit view + other bundles chain off it). The
        durable trail is the PipelineRecord itself; this is the live signal."""
        try:
            from cairn.core.bus import get_bus  # noqa: PLC0415

            bus = self._bus or get_bus()
            await bus.publish(
                f"pipeline.{event}",
                {"pipeline_id": rec.pipeline_id, "step_id": step_id, **detail},
                correlation_id=rec.pipeline_id,
            )
        except Exception:  # noqa: BLE001 — audit must never break the transition
            logger.debug("pipeline audit publish failed for %s/%s", rec.pipeline_id, event)

    # ── #1791: the approval NOTIFICATION (Slack + console + CLI) ────────────

    def _budget_context(self, rec: PipelineRecord) -> dict[str, Any]:
        """Cost context WHEN PRESENT — never invented. The only cost number the
        bridge actually holds at a pause is the budget this pipeline was
        admitted under (``governance.budget``, #1688, bounded by the deployment: ADR-0155); a gate author who wants a
        measured number surfaces it with ``show:``, and that arrives separately
        as the gate context's fields."""
        usd = budget_cap_usd(rec)   # ADR-0155 D4: the effective cap the launch door recorded
        return {"budget_usd": usd} if usd else {}

    async def _spend_evidence(self, rec: PipelineRecord) -> dict[str, Any]:
        """#2358 — a gate's cost line: the pipeline's spend so far, summed from
        each leg run's own ``cost.total_usd`` (the figure ``GET /pipelines/{id}``
        already shows per step), and the declared ``governance.budget.usd`` when
        there is one. Read from records, never estimated; best-effort, because
        evidence that cannot be read must never hold up the gate it describes."""
        try:
            spent = await pipeline_spent_usd(rec, self.dispatcher.store)
        except Exception:  # noqa: BLE001 — see docstring
            logger.exception("gate spend evidence failed for %s", rec.pipeline_id)
            return {}
        cap = self._budget_context(rec).get("budget_usd")
        return {"cost_usd": round(spent, 6), **({"cost_cap_usd": float(cap)} if cap else {})}

    async def _notify_gate(
        self, rec: PipelineRecord, step_id: str, kind: str, *,
        operator: str = "", risk: str = "", authority: tuple[str, ...] = (),
        reason: str = "", extra_cost: Optional[dict] = None,
    ) -> None:
        """Announce a pause. Dedupe is keyed on the step's ``awaiting_since`` and
        the sent-marker is persisted ON the step, so one pause is announced once
        even across a bridge restart. Best-effort throughout — a notification
        that cannot be delivered must never hold up the gate it describes."""
        st = rec.step_state.get(step_id)
        if st is None:
            return
        key = pause_dedupe_key(rec.pipeline_id, step_id, st.awaiting_since or "")
        if st.notified_key == key:
            return  # already announced — the restart case
        cost = self._budget_context(rec)
        cost.update(extra_cost or {})
        try:
            sent = await emit_gate_event(
                NotifyEvent(
                    kind=kind,
                    pipeline_id=rec.pipeline_id,
                    step_id=step_id,
                    pipeline_name=rec.pipeline_name or rec.bundle_ref,
                    operator=operator,
                    risk=risk,
                    authority=authority,
                    reason=reason,
                    cost=cost,
                    commands=release_commands(rec.pipeline_id, step_id),
                    dedupe_key=key,
                ),
                registry=getattr(self, "_notify_registry", None),
            )
        except Exception:  # noqa: BLE001 — belt; emit_gate_event does not raise
            logger.exception("gate notification failed for %s/%s", rec.pipeline_id, step_id)
            return
        if sent:
            st.notified_key = key  # persisted by the caller's `store.put(rec)`

    async def _notify_pipeline_failed(self, rec: PipelineRecord, reason: str) -> None:
        """The failure half (opt-in per ``[profile.notify]``). Once-by-construction:
        ``guard_pipeline_put`` refuses a second transition into ``failed``, so the
        in-process key is all the dedupe this needs."""
        try:
            await emit_gate_event(
                NotifyEvent(
                    kind="pipeline_failed",
                    pipeline_id=rec.pipeline_id,
                    pipeline_name=rec.pipeline_name or rec.bundle_ref,
                    reason=reason,
                    cost=self._budget_context(rec),
                    commands=(f"cairn remote findings {rec.pipeline_id}",),
                    dedupe_key=failure_dedupe_key(rec.pipeline_id, rec.ended_at or ""),
                ),
                registry=getattr(self, "_notify_registry", None),
            )
        except Exception:  # noqa: BLE001 — a notification never changes the outcome
            logger.exception("failure notification failed for %s", rec.pipeline_id)
