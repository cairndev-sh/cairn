"""
RunDispatcher — concurrency, lifecycle, and dedup around the Runner.

The Runner does *one* investigation well (``run(event) -> [RunResult]``).
It knows nothing about who else is running, whether this is a duplicate alert,
or how much LLM budget is in flight. The dispatcher wraps it to add exactly
that, without changing the Runner:

  - **Two lanes.** ``realtime`` (alerts/webhooks — bounded, preferred) and
    ``background`` (scheduled sweeps, retrospective — lower concurrency). Each
    lane has its own ``asyncio.Semaphore`` so a background sweep can't starve a
    live incident.
  - **Dedup / coalescing.** A burst of identical alerts (same fingerprint)
    folds onto the one active investigation instead of spawning N agent loops.
  - **Lifecycle.** Every submission gets an ``Run`` record that moves
    queued → running → done/failed/cancelled, queryable via the store.
  - **Isolation.** A failing investigation never takes down the dispatcher.

``submit(..., wait=False)`` returns the handle immediately (fire-and-forget,
for a 202-style API); ``wait=True`` awaits the result (for a synchronous
response or for tests). The HTTP contract choice lives in trigger_server, not
here.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json
import logging
import os
import sys
from contextlib import nullcontext
from datetime import datetime
from typing import Any, Optional

from cairn.core import liveness  # ADR-0125 — the in-process leg owner (D1)
from cairn.core import settings as _settings
from cairn.core.interrupts import KIND_REMOTE_JOB, interrupt_kind
from cairn.core.storage import (
    Lane,
    Run,
    RunResultSummary,
    RunStore,
    StorageBackend,
    build_storage,
    default_memory_store,
    default_priority,
    default_rag_catalog,
    default_rag_store,
    fingerprint,
    new_run_id,
)
from cairn.core.storage.protocols import PIPELINE_ID_KEY, _now
from cairn.core.engine.runner import Runner
from cairn.core.engine.schema import TriggerEvent
from cairn.core.obs.metrics import record_run
from cairn.core.obs.telemetry import current_trace_id, span
from cairn.core.runtime.node_events import (
    bound_node_output as _bound_node_output,  # re-exported for tests + back-compat
    llm_cost_from_node_events,
    node_event_data,
)
from cairn.core.workspace import current_workspace

logger = logging.getLogger("cairn.dispatcher")


def _is_isolated_playground_env(environment: str | None) -> bool:
    """True when a sandbox run's environment is the ISOLATED playground env —
    scratch backends + TTL — where `sandbox_safe` cheap writes may run live
    (real report to the throwaway bucket, never prod). Kept deliberately strict:
    match the reserved playground/sandbox env name (overridable via
    ``CAIRN_PLAYGROUND_ENV``). Any unrecognised env → False → cheap writes stay
    stubbed, so a sandbox can never write to prod infra (#844)."""
    names = {n.strip().lower() for n in getenv("CAIRN_PLAYGROUND_ENV", "playground,sandbox").split(",") if n.strip()}
    return bool(environment) and environment.strip().lower() in names


#: Public name for the predicate above. `cairn run --mode sandbox` (ADR-0101 D3,
#: #1975) has to answer the SAME question this dispatcher answers before it opens
#: `sandbox_mode`, and a CLI reaching for a leading-underscore name is how two
#: definitions of "the isolated playground env" start. The private name stays
#: bound for the existing callers and tests.
is_isolated_playground_env = _is_isolated_playground_env


def _duration_ms(start: str | None, end: str | None) -> Optional[float]:
    """Wall-clock ms between two ISO-8601 stamps (``_now()`` format); None if
    either is missing or unparseable — metrics duration must never break a run."""
    if not start or not end:
        return None
    try:
        return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() * 1000.0
    except (ValueError, TypeError):
        return None


# Fields of an ApprovalRecord (cairn.core.engine.schema) worth surfacing on the
# `resumed` event so a run's governance timeline (#283) can show WHO decided,
# WHEN, and WHY — not just that a resume happened. Callers (tests, older
# clients) may pass a bare `{"approved": True}`; missing fields are omitted
# rather than fabricated.
_DECISION_EVENT_FIELDS = ("approved", "approver", "note", "timestamp_iso")


def _decision_fields(decision: dict) -> dict[str, Any]:
    return {k: decision[k] for k in _DECISION_EVENT_FIELDS if decision.get(k) is not None}


class OrphanedGateError(RuntimeError):
    """#365 — an awaiting_approval run whose gate checkpoint no longer exists in
    this process/backing store (an engine restart orphaned the in-process
    waiter). Approving it would lie — the graph cannot actually resume; the
    server maps this to 409 so the operator rejects to close it out instead."""


class NotResumableError(ValueError):
    """The run has no template interrupt to resume: a pipeline STEP gate (released
    by advancing the step) or an in-process run with no template on record. The
    caller used the wrong verb, so the server answers 409 with the door that works,
    not a 502 that reads as an engine fault."""


# Keys (substring match, case-insensitive) that bucket a result's *_usd cost
# fields into the run's structured rollup (#242). `llm` is summed reliably from
# RunResult.llm_cost_usd; gpu/storage are best-effort from whatever a compute /
# storage operator surfaced (e.g. the lineage operator's `actual_usd`).
_GPU_COST_KEYS = ("gpu_usd", "gpu_cost_usd", "actual_usd")
_STORAGE_COST_KEYS = ("storage_usd", "storage_cost_usd")


def _scan_usd(obj: Any, keys: tuple[str, ...]) -> float:
    """Sum numeric values under any of ``keys`` found recursively in ``obj``."""
    total = 0.0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool) and k in keys:
                total += float(v)
            else:
                total += _scan_usd(v, keys)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            total += _scan_usd(v, keys)
    return total


def _aggregate_cost(payloads: list[dict]) -> dict[str, float]:
    """Roll a run's structured cost (#242) from its result payloads.

    Recursive scan by key so it works for the skill path (top-level
    ``llm_cost_usd`` on each ``RunResult``) and any nested compute/storage cost a
    payload surfaced; ``gpu``/``storage`` are best-effort (0 when unknown). Note:
    the template-run path only captures cost the workflow output exposes — full
    per-LLM-call rollup for that path is a follow-up. Returns the 4-key breakdown."""
    llm = _scan_usd(payloads, ("llm_cost_usd",))
    gpu = _scan_usd(payloads, _GPU_COST_KEYS)
    storage = _scan_usd(payloads, _STORAGE_COST_KEYS)
    return {
        "llm_usd": round(llm, 6),
        "gpu_usd": round(gpu, 6),
        "storage_usd": round(storage, 6),
        "total_usd": round(llm + gpu + storage, 6),
    }


def _llm_cost_from_node_events(events: list) -> float:
    """Sum the latest measured LLM spend for each node."""
    return llm_cost_from_node_events(events)


def _cost_baseline_best_effort(cost: dict[str, float]) -> Optional[float]:
    """Best-effort ``cost_baseline_usd`` for a just-finished run (ADR-0059 §D3,
    #481 Slice F). Per-call token data isn't available at this point (it only
    ever reaches OTel spans/fleet metrics — see ``core/eval/cost_baseline.py``'s
    module docstring for the full gap writeup), so this scales the run's
    already-aggregated ``llm_usd`` by the strong-vs-fast rate ratio rather than
    reconstructing per-call baselines. Returns ``None`` (never a fabricated 0)
    when the run had no LLM cost to scale. Never raises — a rate-lookup hiccup
    (e.g. a malformed model_roles override) must not break run completion."""
    try:
        from cairn.core.eval.cost_baseline import estimate_cost_baseline_from_actual
        return estimate_cost_baseline_from_actual(cost.get("llm_usd", 0.0))
    except Exception:  # noqa: BLE001 — best-effort, never breaks run completion
        logger.warning("cost_baseline_usd estimation failed", exc_info=True)
        return None


def _json_safe(obj: Any) -> Any:
    """Best-effort JSON-serializable coercion for a run's output before it's
    persisted in the run record (#220). Serializable leaves pass through; the
    rest are stringified, so a stray non-serializable value never breaks the
    store's ``model_dump_json``."""
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        if isinstance(obj, dict):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_json_safe(v) for v in obj]
        return str(obj)


def _lane_concurrency(lane: str) -> int:
    """Thin pass-through to the centralized settings (kept for module-local use)."""
    return _settings.lane_concurrency(lane)


#: #2128 item A (ADR-0105 D1) — the NO-ORPHAN-RUNS invariant's one switch.
#:
#: ``False`` (today): a :meth:`RunDispatcher.run_template` call that arrives
#: with no ``bundle_pipeline_id`` is WRAPPED in a synthesized one-leg pipeline
#: and logged ``orphan run wrapped: <producer>``.
#: ``True``: the same call is REFUSED, naming the producer.
#:
#: The strict flip is this one line and nothing else — every producer already
#: calls ``run_template``, so there is no second place to change (the governance
#: thesis's "a chokepoint must be a FUNCTION others call", applied to run
#: identity).
#:
#: **ADR-0106 D7 asks for the flip; #2134 S1 measured what it would cost and
#: left it OFF.** Two blockers, and the second is the one #2133 did not know
#: about:
#:
#: 1. *The documented bridge-less hole.* A dispatcher wired without a
#:    ``pipeline_bridge`` (the CLI's in-process paths, test harnesses) has
#:    nothing to execute a pipeline with. Closing it by constructing a bridge
#:    here would mint a ``PipelineRecord`` that nobody advances — no
#:    ``bridge.start()``, no bus subscription — i.e. a governed record that is a
#:    lie about what happened. That is exactly the argument #2133 used to
#:    REFUSE the same shortcut for the dev loop.
#: 2. *Strict refuses ahead of the bridge check*, so it does not only close the
#:    hole — it refuses EVERY pipeline-less producer, including the legitimate
#:    one #2133 deliberately left unchanged: the ``monitor_threshold`` dispatch
#:    operator (``operators/builtins/monitor_threshold/…/dispatch.py``), whose
#:    run #2133 pins as WRAPPED. Its remedy under ADR-0106 D1 is to dispatch a
#:    declared pipeline ref instead of a bare flow — a PACK change (#2134 S2),
#:    not an engine one.
#:
#: So the flip waits on S2. ``test_strict_mode_refuses_instead_of_wrapping``
#: already pins what it does, so it stays a constant change with a green test
#: behind it.
STRICT_NO_ORPHAN_RUNS = False


def _producer_of_run_template(depth: int = 2) -> str:
    """``module.function:line`` of whoever called ``run_template``.

    The orphan warning is only useful if it names the hole. ``sys._getframe``
    rather than :func:`inspect.stack` on purpose: one frame object, no source
    lookup, no list of every frame — and it is walked ONLY on the orphan path,
    so the hot path pays nothing.
    """
    try:
        f = sys._getframe(depth)  # noqa: SLF001 — the cheap half of inspect.stack()
        return f"{f.f_globals.get('__name__', '?')}.{f.f_code.co_name}:{f.f_lineno}"
    except Exception:  # noqa: BLE001 — a diagnostic must never break a dispatch
        return "<unknown producer>"


def _run_template_ref(inv: Run) -> str:
    """The template a Run executed — delegates to the canonical resolver in
    ``cairn.core.eval.scoring`` (kept in sync with
    ``cairn.core.triggers.server._run_template_ref``; the eval module is import-
    safe from dispatch — no cycle, unlike triggers)."""
    from cairn.core.eval.scoring import run_template_ref

    return run_template_ref(inv)


def _inprocess_owner() -> str:
    """ADR-0125 D1 — the owner to stamp on a template LEG: this process's owner
    id on the in-process runtime; ``""`` under Temporal/DBOS (their own liveness
    keeps precedence) or when this process holds no owner lock. The callers
    stamp it only on a run with a ``bundle_pipeline_id``: a standalone run is
    not a leg and names no owner."""
    if _settings.runtime() in ("temporal", "dbos"):
        return ""
    return liveness.self_owner()


class RunDispatcher:
    """Owns the lanes, the store, and the in-flight task table."""

    def __init__(
        self,
        runner: Runner,
        store: Optional[RunStore] = None,
        *,
        backend: Optional[StorageBackend] = None,
        realtime_concurrency: Optional[int] = None,
        background_concurrency: Optional[int] = None,
    ) -> None:
        """Construct a dispatcher.

        Three ways to supply storage, in priority order:

          1. ``backend=<StorageBackend>`` — the §24.4 model. The dispatcher
             pulls every store it needs off the backend, and exposes the
             whole thing as ``self.backend`` for downstream consumers.
          2. ``store=<RunStore>`` — legacy + tests. The dispatcher wraps it
             in a fresh ``StorageBackend(runs=store, memory=…, rag=…)`` so
             ``self.backend`` is still populated; memory/rag fall back to
             their default factories.
          3. neither — calls :func:`build_storage` to construct a default
             backend from env.

        Passing both ``store=`` and ``backend=`` is a usage error.
        """
        self.runner = runner
        if store is not None and backend is not None:
            raise ValueError("pass either `store` or `backend`, not both")
        if backend is None:
            if store is None:
                backend = build_storage()
            else:
                # Legacy path: caller supplied a bare RunStore. Fill in the
                # rest from defaults so self.backend is always usable.
                backend = StorageBackend(
                    runs=store,
                    memory=default_memory_store(),
                    rag=default_rag_store(),
                    rag_catalog=default_rag_catalog(),
                )
        self.backend = backend
        self.store = backend.runs
        self._sems: dict[str, asyncio.Semaphore] = {
            "realtime": asyncio.Semaphore(
                realtime_concurrency or _lane_concurrency("realtime")
            ),
            "background": asyncio.Semaphore(
                background_concurrency or _lane_concurrency("background")
            ),
        }
        # id → running asyncio.Task, so we can cancel and drain.
        self._tasks: dict[str, asyncio.Task] = {}
        # #618: set during graceful shutdown (aclose) so the per-run
        # CancelledError handlers can tell a SHUTDOWN cancel (durable workflow
        # survives → don't mark cancelled) from a user cancel.
        self._shutting_down: bool = False

    # ── Submit ────────────────────────────────────────────────────────

    async def submit(
        self,
        event: TriggerEvent,
        *,
        lane: Lane = "realtime",
        environment: Optional[str] = None,
        wait: bool = False,
    ) -> Run:
        """Accept an event for investigation.

        Coalesces onto an active duplicate when one exists (non-cascade only).
        Returns the handle immediately unless ``wait=True``, in which case it
        awaits completion and returns the final record.
        """
        fp = fingerprint(event)

        # ── Dedup: fold identical, still-active alerts onto the original ──
        if event.source != "cascade":
            existing = await self.store.find_active_by_fingerprint(fp)
            if existing is not None:
                await self.store.update(
                    existing.id, attached_count=existing.attached_count + 1
                )
                logger.info(
                    "dispatcher coalesced duplicate onto %s (fp=%s, attached=%d)",
                    existing.id, fp, existing.attached_count + 1,
                )
                if wait:
                    await self._await_task(existing.id)
                return await self.store.get(existing.id)  # type: ignore[return-value]

        inv = Run(
            id=new_run_id(fp),
            trigger=event,
            lane=lane,
            priority=default_priority(lane),
            status="queued",
            fingerprint=fp,
            workspace_id=current_workspace(),  # tenancy scope (#223); "default" single-node
        )
        await self.store.create(inv)
        await self._emit(inv.id, "queued", {"lane": lane, "fingerprint": fp})
        task = asyncio.create_task(self._run(inv, environment), name=inv.id)
        self._tasks[inv.id] = task
        logger.info("dispatcher queued %s (lane=%s, fp=%s)", inv.id, lane, fp)

        if wait:
            await self._await_task(inv.id)
            return await self.store.get(inv.id)  # type: ignore[return-value]
        return inv

    # ── Event emission ─────────────────────────────────────────────────

    async def _emit(self, run_id: str, type: str, data: dict,
                    *, pipeline_id: str = "") -> None:
        """Record one run event AND publish it to the Bus (ADR-0010 P3.b).

        The append → ``_EventHub`` → SSE path is unchanged; we *also* publish to
        the Bus on a ``run.<type>`` topic so topic/wildcard subscribers
        (cross-pack, future UI) see run events. Both legs are best-effort —
        neither logging nor the bus may ever break an investigation.

        ``pipeline_id`` (#1532) travels IN the payload when this run is a
        pipeline leg. The pipeline bridge used to answer "is this terminal one
        of mine?" from a process-local dict, so an event delivered to a replica
        that did not launch the run was dropped and its pipeline simply stopped
        advancing. Carrying the link on the message makes the answer derivable
        by ANY replica with no extra store read — the publisher already holds
        the run, and the value is `Run.bundle_pipeline_id`, stamped at submit.
        """
        payload = dict(data)
        if pipeline_id:
            payload[PIPELINE_ID_KEY] = pipeline_id
        try:
            await self.store.append_event(run_id, type, payload)
        except Exception:  # streaming is best-effort, never fatal
            logger.debug("event append failed for %s/%s", run_id, type)
        try:
            from cairn.core.bus import get_bus
            await get_bus().publish(f"run.{type}", dict(payload), correlation_id=run_id)
        except Exception:  # the bus is optional — never fatal
            logger.debug("bus publish failed for %s/%s", run_id, type)

    def _node_sink(self, run_id: str):
        """A node sink (ADR-0044) that emits one ``node`` event per completed graph
        node, so the console canvas progresses mid-run. The node's output delta is
        captured (bounded) so the per-run trace can show what each step produced
        (#172) — never the full payload (kept off the event log + SSE)."""
        async def _sink(node_name: str, output: Any = None) -> None:
            await self._emit(run_id, "node", node_event_data(node_name, output))
        return _sink

    # ── Worker ────────────────────────────────────────────────────────

    async def _run(self, inv: Run, environment: Optional[str]) -> None:
        sem = self._sems[inv.lane]

        # Event sink the Runner uses to stream evidence/decisions into THIS
        # run's append-only log as they happen (and onto the Bus via _emit).
        async def _sink(type: str, data: dict) -> None:
            await self._emit(inv.id, type, data)

        started_at = _now()
        try:
            async with sem:  # blocks here while > lane budget is in flight
                await self.store.update(inv.id, status="running", started_at=started_at)
                await self._emit(inv.id, "running", {})
                # Root run span (#242) — operator spans created inside runner.run
                # nest under it (OTel current-span context), forming the trace tree.
                with span("cairn.run", {
                    "run_id": inv.id, "workspace_id": inv.workspace_id,
                    "lane": inv.lane, "trigger.source": inv.trigger.source,
                }):
                    # Persist the root span's trace_id so the console can deep-link
                    # this run's trace in Langfuse (#248). No-op ("") when tracing
                    # is off; only write when we actually captured one.
                    tid = current_trace_id()
                    if tid:
                        await self.store.update(inv.id, trace_id=tid)
                    results = await self.runner.run(
                        inv.trigger,
                        environment=environment,  # type: ignore[arg-type]
                        event_sink=_sink,
                        run_id=inv.id,  # template thread_id / workflow_id == Run.id (#38)
                    )
                summaries = [
                    RunResultSummary(
                        skill_name=r.skill_name,
                        template_name=getattr(r, "template_name", "") or "",
                        duration_seconds=getattr(r, "duration_seconds", 0.0) or 0.0,
                        action_count=len(getattr(r, "actions", []) or []),
                    )
                    for r in results
                ]
                # #1096: a SKILL-matched run doesn't know its workflow at admission
                # (the skill engine picks the template while running) — record it the
                # moment it IS known, so the workflow is never re-derived at read
                # time. First non-empty wins; a run that matched no template keeps "".
                if not inv.template_ref:
                    matched = next((s.template_name for s in summaries if s.template_name), "")
                    if matched:
                        await self.store.update(inv.id, template_ref=matched)
                # Operation-gate HITL (ADR-0034, #38): if a result paused at an
                # interrupt, the run is awaiting approval — not done. Resume via
                # POST /runs/{id}/resume. in_process surfaces __interrupt__ in
                # raw_output; temporal/dbos block inside runner.run and never
                # reach here while paused (status stays "running").
                # Also capture the pending interrupt payload (the value passed to
                # interrupt() = the ApprovalRequest dict: operator/risk/sources/
                # reason/...), so /runs/{id}/pending-approval + the CLI can show
                # WHAT is being approved (#55), not just "awaiting_approval".
                paused = False
                pending_approval = None
                for r in results:
                    ro = getattr(r, "raw_output", None)
                    if isinstance(ro, dict) and "__interrupt__" in ro:
                        paused = True
                        intr = ro["__interrupt__"]
                        items = list(intr) if isinstance(intr, (list, tuple)) else [intr]
                        if items and pending_approval is None:
                            pending_approval = getattr(items[0], "value", None)
                # ADR-0065: a remote-job pause is RUNNING (the control loop drives it),
                # not awaiting_approval — never mislabel it or surface a bogus request.
                remote_pause = paused and interrupt_kind(pending_approval) == KIND_REMOTE_JOB
                if remote_pause:
                    pending_approval = None
                pause_status = "running" if remote_pause else "awaiting_approval"
                payloads = [
                    r.model_dump(mode="json")
                    for r in results
                    if hasattr(r, "model_dump")
                ]
                ended_at = None if paused else _now()
                cost = _aggregate_cost(payloads)  # structured rollup (#242)
                # counterfactual (#481 Slice F) — only on terminal completion,
                # same guard as the terminal hooks below.
                cost_baseline = _cost_baseline_best_effort(cost) if not paused else None
                await self.store.update(
                    inv.id,
                    status=pause_status if paused else "done",
                    ended_at=ended_at,
                    results=summaries,
                    # Full structured result(s) → the DB result backend (#76).
                    # Additive to the on-disk .cairn/runs/<id>/ JSONL trace;
                    # excluded from the /runs list projection so listing stays
                    # cheap, read back via GET /runs/{id}.
                    result_payloads=payloads,
                    cost=cost,
                    cost_baseline_usd=cost_baseline,
                    matched_skills=len(summaries),
                )
                if not paused:  # fleet metric on terminal completion (#242 P2)
                    record_run("done", workspace=inv.workspace_id,
                               duration_ms=_duration_ms(started_at, ended_at), cost=cost)
                await self._emit(
                    inv.id,
                    (pause_status if paused else "done"),
                    {"matched_skills": len(summaries),
                     **({"approval": pending_approval} if pending_approval is not None else {})},
                    pipeline_id=inv.bundle_pipeline_id,
                )
                logger.info(
                    "dispatcher %s %s — %d skill(s)",
                    "awaiting approval" if paused else "finished",
                    inv.id, len(summaries),
                )
                if not paused:  # scoring (#474/#480, ADR-0060 D1) — terminal "done" only
                    await self._terminal_hooks(inv.id)
        except asyncio.CancelledError:
            if self._preserve_on_shutdown(inv.id):
                raise
            ended = _now()
            await self.store.update(inv.id, status="cancelled", ended_at=ended)
            await self._emit(inv.id, "cancelled", {},
                             pipeline_id=inv.bundle_pipeline_id)
            record_run("cancelled", workspace=inv.workspace_id, duration_ms=_duration_ms(started_at, ended))
            logger.info("dispatcher cancelled %s", inv.id)
            raise
        except Exception as exc:  # isolation: a bad investigation never kills the loop
            ended = _now()
            await self.store.update(
                inv.id, status="failed", ended_at=ended, error=str(exc)
            )
            await self._emit(inv.id, "failed", {"error": str(exc)},
                             pipeline_id=inv.bundle_pipeline_id)
            record_run("failed", workspace=inv.workspace_id, duration_ms=_duration_ms(started_at, ended))
            logger.exception("dispatcher investigation %s failed", inv.id)
            await self._terminal_hooks(inv.id, status="failed")  # scoring (#474/#480, ADR-0060 D1) — terminal "failed"
        finally:
            self._tasks.pop(inv.id, None)

    async def _await_task(self, inv_id: str) -> None:
        task = self._tasks.get(inv_id)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    async def _terminal_hooks(self, run_id: str, *, status: str = "done") -> None:
        """Completion hook: a run that just reached a terminal state (``done`` or
        ``failed`` — not ``awaiting_approval``/``cancelled``) fires ``run.finished`` /
        ``run.failed`` with THIS dispatcher's own run store (the one that owns the
        record; it diverges from the process singleton in the in-memory tier).

        ADR-0120 D4 (#2421): labelling + scoring (#474/#480, ADR-0060 D1), the
        Playground report upload (ADR-0073) and the like are product features the
        dispatcher no longer names — whoever is installed under ``cairn.run_hooks``
        listens (the platform's ``scoring`` and ``playground`` providers). With none
        installed a finished run is recorded and nothing else happens. Fired AFTER
        the record is terminal; :func:`~cairn.core.pack.run_hooks.fire` never raises.
        """
        from cairn.core.pack import run_hooks  # noqa: PLC0415 — off the hot path

        await run_hooks.fire(run_hooks.RUN_FAILED if status == "failed" else run_hooks.RUN_FINISHED,
                             run_id=run_id, status=status, store=self.store)

    # ── the no-orphan-runs invariant (#2128 item A) ─────────────────────

    async def _wrap_orphan_run(
        self,
        template_ref: str,
        inputs: dict,
        *,
        environment: Optional[str],
        run_options: dict,
        producer: str,
    ) -> Optional[Run]:
        """Give a pipeline-less run the one-leg pipeline it belongs to.

        **The invariant (#2128, ADR-0105 D1):** every run is created under a
        pipeline id and is trackable by its pipeline. There are no orphan runs.

        S1 (#2113) and S3 (#2115) put the one-leg synthesis at the DOORS
        (``launch_workflow`` / ``launch_ref``), so every new producer of runs was
        a new hole and the orphan list kept growing (#2128 §4). It lives HERE
        instead: the one function every producer already calls. A caller that
        supplies no ``bundle_pipeline_id`` gets the pipeline synthesized for it,
        whether or not it knows pipelines exist.

        **No double wrap.** The doors still synthesize, and every pipeline leg —
        :meth:`PipelineBridge._submit_step`, the Temporal ``pipeline_activities``
        — arrives WITH a pipeline id, so this method is never entered for them
        and no second record is written.

        **No recursion.** Wrapping calls ``launch_pipeline_def`` →
        ``bridge.start_pipeline`` → ``_submit_step`` → ``run_template`` again —
        but `_submit_step` hard-codes ``bundle_pipeline_id=rec.pipeline_id``,
        which a persisted :class:`PipelineRecord` always has, so the re-entry
        takes the pass-through branch. Exactly one ``PipelineRecord`` and one
        ``Run`` per wrapped call; ``test_no_orphan_runs`` asserts both counts.

        Returns the leg's ``Run``, or ``None`` when the caller should fall back
        to today's direct dispatch — **the one remaining documented hole**: a
        dispatcher wired without a ``pipeline_bridge`` (the CLI's in-process
        paths, test harnesses) has nothing to execute a pipeline with, so
        refusing would mean it could not dispatch at all. It is logged as loudly
        as a wrap, with the producer named.
        """
        if STRICT_NO_ORPHAN_RUNS:
            raise ValueError(
                f"no-orphan-runs (#2128): {producer} called run_template({template_ref!r}) "
                f"with no bundle_pipeline_id. Every run is a leg of a pipeline "
                f"(ADR-0105 D1) — start it through cairn.core.dispatch.start.launch_ref, "
                f"which synthesizes the one-leg pipeline."
            )

        # `dispatcher.pipeline_bridge` is attached in `build_app`
        # (triggers/server.py) — the same lookup `dispatch/start.py::_bridge_for`
        # makes, so the door and the chokepoint agree about what "a bridge is in
        # reach" means.
        bridge = getattr(self, "pipeline_bridge", None)
        if bridge is None:
            logger.warning(
                "orphan run NOT wrapped: %s started %s with no pipeline and this dispatcher "
                "has no pipeline_bridge — the ONE documented hole (#2128 A). The run is "
                "governed, but belongs to no pipeline.", producer, template_ref)
            return None

        # lazy, as the neighbouring code is: `run_template` is deep in dispatch
        # and both of these import the engine (and each other's package), so a
        # module-level import would be a cycle and would put the compiler on
        # `cairn --help`'s import path.
        from cairn.core.dispatch.pipeline_launch import launch_pipeline_def  # noqa: PLC0415
        from cairn.core.dispatch.start import synthesize_flow_pipeline  # noqa: PLC0415

        try:
            d = synthesize_flow_pipeline(template_ref, typed=dict(inputs or {}))
        except Exception as e:  # noqa: BLE001 — a ref that is not <pack>/<name>
            # cannot BE a pipeline leg (the leg ref is the flow's D0 address).
            # Same hole, same loudness: dispatch as it did before rather than
            # refuse a producer this invariant cannot yet express.
            logger.warning(
                "orphan run NOT wrapped: %s started %s, which cannot be a one-leg pipeline "
                "(%s) — the ONE documented hole (#2128 A).", producer, template_ref, e)
            return None

        logger.warning(
            "orphan run wrapped: %s started %s with no pipeline — synthesizing its one-leg "
            "pipeline (ADR-0105 D1, #2128 A)", producer, template_ref)
        rec = await launch_pipeline_def(
            d, inputs=dict(inputs or {}), bridge=bridge,
            environment=environment or None,
            workspace_id=current_workspace(),
            # ADR-0084 D0 — a synthesized definition is not an installed
            # pipeline, so it is stamped with the FLOW's address.
            bundle_ref=f"flow:{template_ref}",
            trigger={
                "source": "dispatch", "flow": template_ref, "one_leg": True,
                # the console + the backfill (#2128 C) can tell a pipeline the
                # producer asked for from one this chokepoint gave it.
                "orphan_wrapped": True, "producer": producer,
                "mode": "sandbox" if run_options.get("sandbox") else "live",
                # every option this call carried rides the record and reaches the
                # leg through `pipeline_bridge._run_options`, so the leg is the
                # same governed run the producer would have got directly.
                "run_options": run_options,
            },
        )
        st = (getattr(rec, "step_state", None) or {}).get(d.steps[0].id)
        run_id = str(getattr(st, "run_id", "") or "")
        if not run_id:
            # Same refusal the launch door makes (S1, #2113): a policy-injected
            # gate ahead of the leg, or a durable owner that has not submitted
            # yet, means there is no run to hand back. Loud, never a silent
            # second run outside the pipeline.
            raise ValueError(
                f"pipeline {rec.pipeline_id} for {template_ref} started without submitting "
                f"its leg (status {getattr(rec, 'status', '?')}) — see its audit trail")
        run = await self.store.get(run_id)
        if run is None:
            raise ValueError(
                f"pipeline {rec.pipeline_id} submitted leg {run_id} but the run store has "
                f"no such run")
        return run

    # ── Direct template run (web/UI execution, #113) ────────────────────

    async def run_template(
        self,
        template_ref: str,
        inputs: dict,
        *,
        lane: Lane = "realtime",
        environment: Optional[str] = None,
        schema_hash: str = "",
        resolved_operators: Optional[list[dict]] = None,
        bundle_pipeline_id: str = "",
        step_id: str = "",
        config_preset: str = "",
        sandbox: bool = False,
        parent_run: str = "",
        series_id: str = "",
        # #1094 — the run's cost ESTIMATE, reserved atomically against the series
        # budget at admission. Defaults to 0.0, which reproduces today's
        # behaviour exactly; supplying it turns the gate from reactive
        # ("already overspent?") into predictive ("would this push us over?").
        estimated_usd: float = 0.0,
    ) -> Run:
        """Run a template directly by ref + inputs — NOT via skill matching (#113).

        ``bundle_pipeline_id``/``step_id`` (ADR-0069, #746) stamp this run as a step
        of a bundle pipeline so the core ``PipelineBridge`` can advance the pipeline
        off its completion and the console groups it under the pipeline.

        Compiles + runs in-process with ``thread_id == Run.id`` (``compiled.run``
        reads ``input_data['run_id']``), streams lifecycle events, and records
        ``template_name`` so a paused operation-gate is resumable by the existing
        :meth:`resume_run`. Returns the handle immediately (stream it via
        ``GET /runs/{id}/stream``).

        #2128 item A — **the no-orphan-runs invariant is enforced here**, first,
        before every other door: a call with no ``bundle_pipeline_id`` is wrapped
        in its one-leg pipeline (:meth:`_wrap_orphan_run`) and comes back as that
        pipeline's leg. First on purpose — the wrap re-enters this method through
        the bridge, and the doors below then run exactly once, on the leg."""
        # ── the NO-ORPHAN-RUNS invariant (#2128 A, ADR-0105 D1) ─────────
        # Every run is created under a pipeline id and is trackable by its
        # pipeline. A producer that knows nothing about pipelines gets one
        # anyway; a pipeline leg (which arrives WITH an id) passes straight
        # through, so nothing is wrapped twice. See _wrap_orphan_run.
        if not bundle_pipeline_id:
            wrapped = await self._wrap_orphan_run(
                template_ref, inputs, environment=environment,
                run_options={
                    "schema_hash": schema_hash,
                    "resolved_operators": list(resolved_operators or []),
                    "config_preset": config_preset,
                    "sandbox": bool(sandbox),
                    "parent_run": parent_run,
                    "series_id": series_id,
                    "estimated_usd": float(estimated_usd or 0.0),
                    "lane": lane,
                },
                producer=_producer_of_run_template(),
            )
            if wrapped is not None:
                return wrapped

        # Fail closed when the pack's stateful requires aren't provisioned by
        # the environment (#304, ADR-0052 §4) — the runtime half of doctor's
        # check_provisioned_backends. Raised BEFORE a run record exists so the
        # caller gets the remediation (HTTP → 409; trigger routes → 400),
        # never a run that silently skips memory/RAG.
        from cairn.core.pack.provisioning import (  # lazy: pack/profile stack off the hot import path
            provisioning_remediation,
            unprovisioned_requires,
        )

        pack_name = template_ref.split("/", 1)[0]
        missing = unprovisioned_requires(pack_name)
        if missing:
            raise ValueError(provisioning_remediation(pack_name, missing))

        # #744 (ADR-0068 enforcement): CONFIG PREFLIGHT — validate the active
        # profile's [profile.config] against each operator's config_schema BEFORE
        # the run record exists, so a malformed deployment knob aborts fail-closed
        # (caller → 409/400) rather than blowing up mid-graph after side effects
        # (Dagster validates the whole run config at launch; this is the analog).
        # Validates the SAME source seed_initial_state seeds operator_config from.
        from cairn.core.operator.config import preflight_operator_configs  # noqa: PLC0415
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415

        try:
            _profile_cfg = load_profile_from_env().config
        except Exception:  # noqa: BLE001 — a broken profile is reported by check_profile / provisioning
            _profile_cfg = {}
        preflight_operator_configs(_profile_cfg)  # raises OperatorConfigError on any invalid knob

        # #2020 (ADR-0102 D3): the HOLDOUT DOOR. A training dataset version that
        # intersects a golden or test version in this workspace is refused HERE —
        # before the run record exists, so the caller gets the overlapping row ids
        # (HTTP → 409/400) and no GPU is ever bought for a contaminated fine-tune.
        # Same fail-closed shape as the config preflight above; the verdict is
        # stamped on the run below either way, so a clean run records WHICH eval
        # sets, at which hashes, it was held out from.
        from cairn.core.holdout import check_holdout_for_inputs  # noqa: PLC0415 — off the hot import path

        _holdout = await check_holdout_for_inputs(inputs, workspace_id=current_workspace())
        if not _holdout.ok:
            raise ValueError(
                "holdout door: this run's training data overlaps a held-out set — "
                + "; ".join(_holdout.reasons())
                + ". Re-run the clean flow's decontamination against that set, or "
                  "register a version with the overlapping rows removed."
            )

        # #2021 (ADR-0102 D1): the INTAKE DOOR. Admission already judged every
        # registered version — `admitted | degraded | blocked | invalid_format`
        # with a structured report (cairn.core.intake). Here that state becomes
        # enforcement: `blocked` and `invalid_format` refuse the launch naming
        # the reasons; `degraded` RUNS, with its reasons stamped on the run
        # record below so the finding is visible next to the result. A version
        # registered before verdicts existed carries none and is admitted with
        # a note — nothing is blocked retroactively.
        from cairn.core.intake import check_intake_for_inputs  # noqa: PLC0415 — off the hot import path

        _intake = await check_intake_for_inputs(inputs, workspace_id=current_workspace())
        if not _intake.ok:
            raise ValueError(
                "intake door: this run's data did not pass admission — "
                + "; ".join(_intake.reasons())
                + ". Re-run the dataset-readiness clean flow over it and launch on the "
                  "version that flow registers."
            )

        # #2027 (ADR-0102 D5): the LICENSE DOOR. A `base_model` that names a
        # gated catalog entry (cairn_platform.models_catalog) is refused here
        # unless the run set `accept_license: true` AND a Hugging Face token
        # connection is present — same fail-closed shape as the doors above,
        # before the run record exists and before any GPU is bought. A
        # free-text base_model (the catalog's "Any Hugging Face id" escape
        # hatch) or an ungated catalog entry passes through unaffected.
        from cairn.core.model_license_gate import (  # noqa: PLC0415 — off the hot import path
            check_license_for_inputs,
            record_license_acceptance,
        )

        _license = await check_license_for_inputs(inputs)
        if not _license.ok:
            raise ValueError(
                "license door: " + "; ".join(_license.reasons())
            )

        # #2025 (ADR-0099 §1): the JOB-CHANNEL door. A leg that hands work to a
        # remote box streams its live events over CAIRN_JOB_CHANNEL; when that is
        # NATS and no broker answers, the run would train blind — refuse here,
        # naming the service, the same shape as the doors above.
        from cairn.core.engine.workflow_config import collect_template_operators  # noqa: PLC0415 — off the hot import path
        from cairn.core.job_channel_door import check_job_channel  # noqa: PLC0415 — same

        try:
            _svc = check_job_channel(collect_template_operators(template_ref))
        except Exception:  # noqa: BLE001 — an unresolvable template is refused elsewhere, never here
            _svc = None
        if _svc is not None and not _svc.ok:
            raise ValueError("job-channel door: " + "; ".join(_svc.reasons))

        # ADR-0134 D3c (was the SkyPilot door, #2073 / ADR-0104 D5; ADR-0099 §1): the
        # FLOW-ENGINE door. When the template reaches a flow engine, the selected engine
        # must be installed and, if it offers check_ready(), ready — refuse here, before
        # the run record and before anything starts billing. Only the kinds the
        # template's own operators reach are asked: an agentic template is never judged
        # by ml_engine.
        from cairn.core.dataflow.engine_door import check_flow_engines, kinds_reached  # noqa: PLC0415 — same

        # A capability-bound node resolves its operator through the registry, and an
        # unloaded registry drops it — which would let a template that reaches an engine
        # skip this door (the pipeline door loads it for the same reason).
        try:
            from cairn.core.operator.loader import ensure_loaded  # noqa: PLC0415 — same

            ensure_loaded()
        except Exception:  # noqa: BLE001 — a partial registry is the doors' own fail-open case
            pass
        try:
            _kinds = kinds_reached(collect_template_operators(template_ref))
        except Exception:  # noqa: BLE001 — an unresolvable template is refused elsewhere, never here
            _kinds = []
        if _kinds:
            _eng = await check_flow_engines(kinds=_kinds)
            if not _eng.ok:
                raise ValueError("flow-engine door: " + "; ".join(_eng.reasons))

        # #565 (ADR-0104 D2, ADR-0099 §1): the PULL-WORKER door. When the
        # environment's ML target is a `worker:` pool, a remote leg has nowhere
        # to run until some machine has claimed or heartbeated — refuse here
        # rather than admit a run that would sit in `submitted` forever.
        from cairn.core.pull_worker_door import check_pull_worker  # noqa: PLC0415 — same

        try:
            _pw = await check_pull_worker(collect_template_operators(template_ref))
        except Exception:  # noqa: BLE001 — an unresolvable template is refused elsewhere, never here
            _pw = None
        if _pw is not None and not _pw.ok:
            raise ValueError("pull-worker door: " + "; ".join(_pw.reasons))

        # #993 (ADR-0076 D5) / #1094 / #1655 — the SERIES budget is checked and the
        # run's estimate reserved BEFORE the run exists. The series is a platform
        # feature (#2421 decision 6): the dispatcher fires the QUERY-shaped
        # `run.admitting` event and whoever is installed may refuse; the platform's
        # `series` provider reads the series and reserves through
        # cairn.core.budget_reservations. Absent = no series cap is checked.
        # #1325 — a reservation is STAMPED WITH THE RUN ID, so the id is minted
        # before the claim, and every terminal closes it BY RUN ID
        # (_close_reservations) — the dispatcher never holds the claim itself.
        run_id = new_run_id(template_ref.replace("/", "--"))
        from cairn.core.pack import run_hooks  # noqa: PLC0415 — off the hot path

        try:
            await run_hooks.fire(run_hooks.RUN_ADMITTING, run_id=run_id, template_ref=template_ref,
                                 workspace_id=current_workspace(), series_id=series_id or "",
                                 estimated_usd=float(estimated_usd or 0.0), inputs=inputs)
        except run_hooks.AdmissionRefused as e:
            await self._close_reservations(run_id, settled=False)  # a provider before the refuser may have reserved
            raise ValueError(e.reason) from None

        ev = TriggerEvent(source="manual", payload={"template": template_ref})
        inv = Run(
            # slugged ref (no "/") so the id stays URL-safe for /runs/{id}
            id=run_id,
            trigger=ev,
            lane=lane,
            priority=default_priority(lane),
            status="queued",
            fingerprint=template_ref,
            # #1096: the workflow this run executes, recorded AT ADMISSION — not
            # re-derived at read time from results/trigger/fingerprint.
            template_ref=template_ref,
            workspace_id=current_workspace(),  # tenancy scope (#223); "default" single-node
            # persist the launch inputs + environment + schema/operator provenance
            # in the run record, right before dispatch — so the run is reproducible
            # and drift-safe (#220, ADR-0047).
            inputs=inputs,
            environment=environment or "",
            schema_hash=schema_hash,
            resolved_operators=resolved_operators or [],
            bundle_pipeline_id=bundle_pipeline_id,  # ADR-0069 (#746): pipeline lineage
            step_id=step_id,
            parent_run=parent_run,  # #989 (ADR-0076): ad-hoc run lineage (`--from`)
            sandbox=sandbox,  # ADR-0073 (#826): Playground run → operator mock mode
            # #2020 — the eval-set version hashes this run was held out from.
            holdout=_holdout.as_record() if _holdout.checked else {},
            # #2021 — the intake verdict of every dataset version this run
            # reads. A `degraded` version runs; its reasons live HERE, so the
            # finding is attached to the result rather than lost at the door.
            intake=_intake.as_record() if _intake.checked else {},
            # ADR-0125 D1 — on the in-process runtime a LEG's graph runs in THIS
            # process, which the run names; a durable runtime owns its own runs.
            # A standalone run (no pipeline — the ADR-0106 D7 unwrapped orphan)
            # is not a leg: it names no owner, so no reconciler sweep can judge
            # it DEAD and cancel it out from under the remote-job control loop
            # that resumes it (`resume_run`) while it is paused on a remote job.
            owner=_inprocess_owner() if bundle_pipeline_id else "",
        )
        try:
            await self.store.create(inv)
            # #2027 (ADR-0102 D5) — the licence-acceptance audit row, written
            # ONLY once the run id exists (preflight's check above is
            # side-effect-free; this is the one place it becomes a record).
            # No-op for a free-text / ungated base_model (record_license_acceptance
            # checks `_license.gated` itself).
            await record_license_acceptance(_license, run_id=inv.id)
            await self._emit(inv.id, "queued", {"template": template_ref, "lane": lane})
            # ADR-0120 D4 (#2421): the run is admitted and queued — listeners learn of it
            # here, never by the dispatcher naming them: the platform's `pilot` provider
            # attaches a run to its series (#993, ADR-0076 D5) on this event.
            await run_hooks.fire(run_hooks.RUN_LAUNCHED, run_id=inv.id, template_ref=template_ref,
                                 workspace_id=inv.workspace_id, series_id=series_id or "", lane=lane)
            task = asyncio.create_task(
                self._run_template_task(
                    inv, template_ref, inputs, environment, config_preset,
                ),
                name=inv.id,
            )
        except BaseException:
            # #1325 — the claim is taken; if admission itself fails from here on,
            # NOTHING will ever run to settle it. Release before re-raising, or a
            # store hiccup permanently consumes series budget.
            await self._close_reservations(run_id, settled=False)
            raise
        self._tasks[inv.id] = task
        logger.info("dispatcher queued template run %s (%s)", inv.id, template_ref)
        return inv

    @staticmethod
    async def _load_workflow_config(workspace_id: str, template_ref: str) -> dict:
        """The saved per-workflow OPERATIONAL config (ADR-0070, #748), best-effort.

        Returns the ``values`` (config-overrides shape) the console saved for this
        workflow + workspace, or ``{}``. A store hiccup must never break a run —
        config is additive — so any error degrades to ``{}``."""
        try:
            from cairn.core.workflow_config_store import default_workflow_config_store  # noqa: PLC0415

            rec = await default_workflow_config_store().get(workspace_id, template_ref)
            return dict(rec.values) if rec else {}
        except Exception:  # noqa: BLE001 — additive config, never fail a run on it
            return {}

    @staticmethod
    async def _resolve_workflow_config(
        workspace_id: str, template_ref: str, preset_override: str = ""
    ) -> tuple[dict, dict]:
        """The workflow-config layer for a run + its preset provenance (ADR-0071,
        #788/#790). A preset is layered OVER the legacy saved per-workflow config,
        so precedence is *run-preset-override > default-preset > workflow-config*
        (all below explicit run inputs, above env + operator defaults).

        ``preset_override`` is a per-run pick (the Run form's Config dropdown, #790)
        — it wins for THIS run only. With no override, the marked **default preset**
        applies, which is why every run — manual, webhook, schedule, event — picks
        it up (triggers have no run-form).

        Returns ``(values, preset_provenance)`` where ``preset_provenance`` is
        ``{name, version, source}`` (``source`` = ``run`` | ``default``) for the
        applied preset, or ``{}``. Best-effort: any preset-resolution error degrades
        to just the legacy config (never fails a run — config is additive)."""
        legacy = await RunDispatcher._load_workflow_config(workspace_id, template_ref)
        try:
            from cairn.core.operator.config import deep_merge  # noqa: PLC0415
            from cairn.core.workflow_config_store import (  # noqa: PLC0415
                default_preset,
                default_workflow_config_store,
            )

            preset = None
            source = ""
            if preset_override:
                preset = await default_workflow_config_store().get_preset(workspace_id, template_ref, preset_override)
                source = "run"
            if preset is None:
                preset = await default_preset(workspace_id, template_ref)
                source = "default"
            if preset:
                return (
                    deep_merge(legacy, preset.values),
                    {"name": preset.name, "version": preset.version, "source": source},
                )
        except Exception:  # noqa: BLE001 — additive; never fail a run on preset resolution
            pass
        return legacy, {}

    async def _contract_invocations(self, inv: Any) -> dict[str, Any]:
        """#2241 C (D2/D3) — the leg's recorded ``InvocationEntry`` data for every opted-in
        operation node this template reaches, keyed by node path and stamped with the
        leg id, read from the pipeline record (never re-resolved) and seeded into graph
        state. ``{}`` for a run that is not a pipeline leg or a record without a
        resolution — the dev loop and legacy rows stay legacy."""
        pid, step = getattr(inv, "bundle_pipeline_id", "") or "", getattr(inv, "step_id", "") or ""
        if not pid or not step:
            return {}
        from cairn.core.pack.registry import invocation_contract  # noqa: PLC0415 — cycle: dispatch → pack, lazy like its siblings
        from cairn.core.storage.pipeline_store import default_pipeline_store  # noqa: PLC0415 — cycle

        rec = await default_pipeline_store().get(pid)
        resolution = getattr(getattr(rec, "plan", None), "contract_resolution", None)
        if resolution is None:
            return {}
        from cairn.core.engine.pipeline_loop import declared_step_id  # noqa: PLC0415 — engine helper

        closure = [(f"contract://{d.id}/{d.revision}", d.digest) for d in resolution.definitions]
        out: dict[str, Any] = {"__leg__": step}
        declared = declared_step_id(step, getattr(rec.plan, "loops", None) or {})   # #2344 — `check#2` → `check`
        for entry in resolution.invocations:
            # ADR-0118 D4 — stage entries never enter the operation lookup (a stage entry at a
            # dataflow/run node's path would be read as that node's own operator contract)
            if entry.surface == "operation" and entry.path.startswith(f"{declared}/"):
                data = invocation_contract([(f"contract://{d.id}/{d.revision}", d.digest) for d in entry.definitions], closure)
                out[entry.path.split("/", 1)[1]] = {**data, "operator_ref": entry.operator_ref,
                                                     "operator_version": entry.operator_version}
        from cairn.core.dispatch.leg_executor import recorded_stage_contracts  # noqa: PLC0415 — the one reader of a leg's stage entries

        stages = recorded_stage_contracts(rec.plan, step)
        if stages is not None and (stages["nodes"] or stages.get("error")):
            out["__stages__"] = stages   # per-node dataflow_contracts, keyed by node path and stage index
        return out

    async def _run_template_task(
        self, inv: Run, template_ref: str, inputs: dict, environment: Optional[str],
        config_preset: str = "",
    ) -> None:
        """Execute one admitted template run to its terminal.

        #1325 — a series-budget claim taken at admission (by a ``run.admitting``
        provider, stamped with the run id; none for most runs) is closed here BY
        RUN ID. This method owns its terminal: **settle** on completion (the spend is
        real in the ledger now, so holding the estimate too would double-count
        it), **release** on failure or cancel. A run that PAUSES at a gate is not
        terminal and keeps its hold — it is still in flight and its estimate
        still has to count against the cap; ``resume_run`` closes it out.
        """
        from cairn.core.engine.compiler import compile_template  # noqa: PLC0415
        from cairn.core.engine.runner import run_within_budget  # noqa: PLC0415
        from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415
        from cairn.core.operator import load_all_operators  # noqa: PLC0415
        from cairn.core.pack.loader import load_all_packs  # noqa: PLC0415
        from cairn.core import obs  # noqa: PLC0415 — #530 cost scope for the template-run path
        # lazy: matches the runtime lazy-import pattern below (keep the runtime
        # package off the dispatcher's import-time path).
        from cairn.core.runtime.node_stream import node_sink_scope  # noqa: PLC0415

        sem = self._sems[inv.lane]
        started_at = _now()
        try:
            async with sem:
                await self.store.update(inv.id, status="running", started_at=started_at)
                await self._emit(inv.id, "running", {"template": template_ref})
                # idempotent prime (never clear — the server shares this registry)
                load_all_operators(strict=False)
                load_all_packs(strict=False)
                compiled = compile_template(resolve_template_path(template_ref))
                # run_id in the input becomes the graph thread_id, so a HITL pause
                # is resumable by Run.id via resume_run (#38 / #113).
                # Per-node events stream onto the run log as each node completes
                # (ADR-0044) — the canvas lights up mid-run instead of all-at-once.
                with node_sink_scope(self._node_sink(inv.id)), span("cairn.run", {
                    "run_id": inv.id, "workspace_id": inv.workspace_id,
                    "lane": inv.lane, "template": template_ref,
                }):
                    # Persist the root span's trace_id for the Langfuse deep-link
                    # (#248); no-op ("") when tracing is off.
                    tid = current_trace_id()
                    if tid:
                        await self.store.update(inv.id, trace_id=tid)
                    # #530 — establish a cost scope so record_llm accumulates LLM
                    # spend on the template-run path. The skill path (Runner.run)
                    # already does this; without it record_llm no-ops here (no
                    # ambient RequestCtx) → cost.total_usd was ALWAYS $0.00 for a
                    # real-LLM template run (e.g. incident-rca). Mirrors the skill
                    # path's graceful BudgetExceeded handling.
                    # ADR-0070/0071 (#748/#788) — thread the workflow config layer:
                    # the marked default PRESET over the saved per-workflow config
                    # (run > preset > workflow > env). Resolved for EVERY run incl.
                    # triggers (no run-form), and the applied preset is stamped on the
                    # run for audit/pilot provenance (ADR-0047).
                    wf_cfg, preset_prov = await self._resolve_workflow_config(inv.workspace_id, template_ref, config_preset)
                    if preset_prov:
                        await self.store.update(inv.id, config_preset=preset_prov)
                    # ADR-0073 (#826/#836/#844) — a sandbox (Playground) run uses
                    # the read/write split: write-class ops (notify, registry,
                    # serve, train) take their `.mock()` path — no side effects,
                    # ~0 spend — while readers/analysis (`side_effect_free`) run
                    # LIVE on the capped sample so the metrics are REAL. When the
                    # env is the ISOLATED playground env, `sandbox_safe` cheap
                    # writes (artifact/report → scratch bucket) also run live so
                    # the report is a real downloadable file (#844). Enforced by
                    # the `sandbox_mode` ContextVars, consulted per invoke.
                    # nullcontext() for a normal run — no behavior change.
                    from cairn.core.operator import default_registry, sandbox_mode  # noqa: PLC0415
                    isolated = _is_isolated_playground_env(inv.environment)
                    sandbox_ctx = sandbox_mode(True, isolated=isolated) if inv.sandbox else nullcontext()
                    run_inputs = inputs
                    # ADR-0073 item 1 — in the isolated playground env, route the
                    # LLM-judge to the cheap Playground eval model (DeepSeek via the
                    # gateway) so eval/judge scores are REAL at ~$0. Injected as a
                    # run-layer config override for the `eval.judge` operators; `setdefault`
                    # so an explicit workflow/preset model still wins. No-op for
                    # workflows that don't use eval (e.g. dataset-readiness).
                    if inv.sandbox and isolated:
                        eval_model = os.environ.get("PLAYGROUND_EVAL_MODEL", "playground-eval")
                        co = {k: dict(v) for k, v in (inputs.get("config_overrides") or {}).items()}
                        for _op, _cls in default_registry.all().items():  # by capability, not by name (#2415)
                            if "eval.judge" in (getattr(_cls, "capabilities", None) or ()):
                                co.setdefault(_op, {}).setdefault("model", eval_model)
                        run_inputs = {**inputs, "config_overrides": co}
                    from cairn.core.operator.runtime import launch_scope  # noqa: PLC0415 — ADR-0120 D7 (#2455)
                    with obs.cost_scope(f"run-{template_ref}") as cost_ctx, sandbox_ctx, \
                            launch_scope(self.launch_handle()):
                        try:
                            output = await run_within_budget(
                                compiled, {**run_inputs, "run_id": inv.id, "__workflow_config__": wf_cfg,
                                           "__contract_invocations__": await self._contract_invocations(inv)}
                            )
                        except obs.BudgetExceeded as be:
                            output = {"type": "budget_exceeded", "budget_exceeded": True,
                                      "reason": str(be), "output": ""}
                    run_llm_usd = round(cost_ctx.cost_usd, 6)

                paused = isinstance(output, dict) and "__interrupt__" in output
                pending = None
                if paused:
                    intr = output["__interrupt__"]
                    items = list(intr) if isinstance(intr, (list, tuple)) else [intr]
                    pending = getattr(items[0], "value", None) if items else None
                # ADR-0065: a remote-job pause is RUNNING (control loop drives it), not
                # awaiting_approval — don't mislabel it or surface a bogus request.
                remote_pause = paused and interrupt_kind(pending) == KIND_REMOTE_JOB
                if remote_pause:
                    pending = None
                pause_status = "running" if remote_pause else "awaiting_approval"
                # record template_name so resume_run can recompile + continue
                summary = RunResultSummary(
                    skill_name="", template_name=template_ref,
                    duration_seconds=0.0, action_count=0,
                )
                ended_at = None if paused else _now()
                update_fields: dict[str, Any] = dict(
                    status=pause_status if paused else "done",
                    ended_at=ended_at,
                    results=[summary],
                    matched_skills=1,
                )
                # persist the workflow output as the run result (#220) — the
                # template-run path previously computed `output` then discarded it.
                # Only on completion; a paused run has no final output yet.
                cost = {}
                if not paused:
                    # #530 — surface the captured LLM spend under the canonical
                    # `llm_cost_usd` key that _aggregate_cost scans, so the run's
                    # cost.total_usd reflects real spend instead of $0.00.
                    # run_llm_usd = server-side cost scope (works for in_process).
                    # Under Temporal the LLM calls run in the WORKER, so the spend
                    # rides on the run's `node` events instead — sum that and take
                    # the larger, so BOTH runtimes report real cost.
                    worker_llm_usd = _llm_cost_from_node_events(await self.store.get_events(inv.id))
                    llm_usd = max(run_llm_usd, worker_llm_usd)
                    payloads = [{"template_name": template_ref,
                                 "output": _json_safe(output),
                                 "llm_cost_usd": llm_usd}]
                    # ADR-0073 item 1 — a sandbox run's report upload to the
                    # Playground bucket is the platform's `playground` run hook
                    # now (ADR-0120 D4, #2421): it patches `artifact_uri` into
                    # these payloads a moment after the terminal event.
                    update_fields["result_payloads"] = payloads
                    cost = _aggregate_cost(payloads)  # structured rollup (#242)
                    update_fields["cost"] = cost
                    # counterfactual (#481 Slice F) — best-effort, see _cost_baseline_best_effort.
                    update_fields["cost_baseline_usd"] = _cost_baseline_best_effort(cost)
                await self.store.update(inv.id, **update_fields)
                await self._emit(
                    inv.id,
                    pause_status if paused else "done",
                    {"template": template_ref,
                     **({"approval": pending} if pending is not None else {})},
                    pipeline_id=inv.bundle_pipeline_id,
                )
                if not paused:  # fleet metric on terminal completion (#242 P2)
                    record_run("done", workspace=inv.workspace_id,
                               duration_ms=_duration_ms(started_at, ended_at), cost=cost)
                logger.info(
                    "dispatcher template run %s %s", inv.id,
                    "awaiting approval" if paused else "finished",
                )
                if not paused:
                    # #1325 — the run finished: the estimate's hold gives way to
                    # the ledger's real spend. A PAUSED run keeps its hold (see
                    # this method's docstring); resume_run owns that terminal.
                    await self._close_reservations(inv.id, settled=True,
                                                   actual_usd=float((cost or {}).get("total_usd") or 0.0))
                    # scoring (#474/#480, ADR-0060 D1) — terminal "done" only
                    await self._terminal_hooks(inv.id)
        except asyncio.CancelledError:
            if self._preserve_on_shutdown(inv.id):
                # The run SURVIVES in Temporal/DBOS and will finish under a later
                # process — so its budget claim must survive too (#1325/#1530):
                # releasing here would un-count spend that is still coming.
                raise
            ended = _now()
            await self.store.update(inv.id, status="cancelled", ended_at=ended)
            await self._emit(inv.id, "cancelled", {},
                             pipeline_id=inv.bundle_pipeline_id)
            record_run("cancelled", workspace=inv.workspace_id, duration_ms=_duration_ms(started_at, ended))
            await self._close_reservations(inv.id, settled=False)  # #1325 — cancelled: no spend to account for
            raise
        except Exception as exc:  # isolation — a bad run never kills the loop
            ended = _now()
            await self.store.update(inv.id, status="failed", ended_at=ended, error=str(exc))
            await self._emit(inv.id, "failed", {"error": str(exc)},
                             pipeline_id=inv.bundle_pipeline_id)
            record_run("failed", workspace=inv.workspace_id, duration_ms=_duration_ms(started_at, ended))
            logger.exception("dispatcher template run %s failed", inv.id)
            # #1325 — the leak this issue names: a failed run that keeps its
            # reservation holds series budget forever, until the series can admit
            # nothing at all. Released BEFORE scoring, which is best-effort.
            await self._close_reservations(inv.id, settled=False)
            await self._terminal_hooks(inv.id, status="failed")  # scoring (#474/#480, ADR-0060 D1) — terminal "failed"
        finally:
            self._tasks.pop(inv.id, None)

    # ── HITL resume (operation-gate, ADR-0034; an internal issue) ──

    async def resume_run(self, run_id: str, decision: dict) -> dict:
        """Resume a run paused at an operation-gate ``interrupt()``, routing to
        the deployment's runtime (``CAIRN_RUNTIME`` is fixed per process, so it is
        the runtime the run used).

        - **temporal / dbos**: the paused workflow loops internally on
          signal/recv; deliver the decision **by id** (the in-flight ``_run``
          task — blocked in ``runner.run`` — then completes and records). After a
          server restart the recovered workflow completes but the store isn't
          re-attached (separate follow-up, #38).
        - **in_process**: ``invoke`` returned at the interrupt, so recompile the
          template and resume from the durable checkpoint (keyed
          ``thread_id=run_id``).
        """
        from cairn.core.runtime import select_runtime  # noqa: PLC0415

        run = await self.store.get(run_id)
        if run is None:
            raise KeyError(f"run not found: {run_id}")
        rt_name = _settings.runtime()

        if rt_name in ("temporal", "dbos"):
            await select_runtime(rt_name).resume(run_id=run_id, decision=decision)
            await self._emit(run_id, "resumed", {"runtime": rt_name, **_decision_fields(decision)})
            return {"run_id": run_id, "runtime": rt_name, "resumed": True}

        # in_process — recompile + resume from the checkpoint.
        from cairn.core.runtime.inprocess import InProcessRuntime  # noqa: PLC0415
        from cairn.core.runtime.node_stream import node_sink_scope  # noqa: PLC0415

        template_name = next(
            (s.template_name for s in run.results if s.template_name), ""
        )
        if not template_name:
            # #1791 — a PIPELINE LEG paused at a gate is not a template run with
            # a LangGraph interrupt to resume; it is released by advancing the
            # STEP (`advance_step`, which re-checks the gate). Reaching here with
            # a pipeline stamp on the record means the operator used the
            # run-level verb on a step gate, so name the door that works rather
            # than reporting a missing field they cannot act on.
            pid = str(getattr(run, PIPELINE_ID_KEY, "") or "")
            sid = str(getattr(run, "step_id", "") or "")
            if pid and sid:
                raise NotResumableError(
                    f"run {run_id} is a pipeline STEP gate, not a resumable template run. "
                    f"Release it with: cairn remote advance {pid} {sid} --approver <you> "
                    f"(or POST /pipelines/{pid}/steps/{sid}/approve)"
                )
            raise NotResumableError(
                f"cannot resume in_process run {run_id}: no template_name on record"
            )
        graph = self._recompile_graph(template_name)
        runtime = InProcessRuntime()
        # #365 — a gate whose checkpoint died with a previous process (or the
        # volatile memory saver) cannot actually resume: invoking would restart
        # the graph from scratch and re-pause, silently 200ing forever. Fail
        # loud on approve; on reject close the run out directly (that IS the
        # cleanup the operator wants) with the decision in the audit trail.
        if run.status == "awaiting_approval" and not await runtime.has_pending_interrupt(graph, run_id):
            if decision.get("approved"):
                raise OrphanedGateError(
                    f"run {run_id}'s gate was orphaned by an engine restart — its "
                    "checkpoint is gone, so approving cannot resume it. Reject to "
                    "close it out, or re-run the workflow."
                )
            await self.store.update(run_id, status="cancelled", ended_at=_now())
            await self._emit(run_id, "cancelled", {
                "reason": "orphaned gate closed by reject (#365)",
                **_decision_fields(decision),
            })
            await self._close_reservations(run_id, settled=False)  # #1325
            return {"run_id": run_id, "runtime": rt_name, "resumed": False,
                    "status": "cancelled", "orphaned": True}
        # ADR-0126 D1 — a resumed LEG's graph runs in THIS process from here on,
        # so its run names this process; a standalone run is not a leg (ADR-0125
        # D1/D3) and stays unowned across its resume.
        me = _inprocess_owner()
        if me and run.bundle_pipeline_id and run.owner != me:
            await self.store.update(run_id, owner=me)
        # keep streaming per-node events after the resume (ADR-0044).
        from cairn.core.operator.runtime import launch_scope  # noqa: PLC0415 — ADR-0120 D7 (#2455)
        with node_sink_scope(self._node_sink(run_id)), launch_scope(self.launch_handle()):
            result = await runtime.resume(graph, decision, run_id=run_id)
        still_paused = isinstance(result, dict) and "__interrupt__" in result
        # ADR-0065: if the resume re-paused on a remote job, that's RUNNING, not
        # awaiting_approval.
        repause_status = "awaiting_approval"
        if still_paused:
            intr = result["__interrupt__"]
            items = list(intr) if isinstance(intr, (list, tuple)) else [intr]
            val = getattr(items[0], "value", None) if items else None
            if interrupt_kind(val) == KIND_REMOTE_JOB:
                repause_status = "running"
        resume_update: dict[str, Any] = dict(
            status=repause_status if still_paused else "done",
            ended_at=None if still_paused else _now(),
        )
        # #738 — on resume-to-completion, PERSIST the final output as result_payloads
        # (+ results summary + cost), exactly like the initial template-run completion
        # (see the `if not paused` block above). Previously the resume path updated
        # only status/ended_at, so a HITL-approved run finished `done` with EMPTY
        # result_payloads → /runs/{id}/report returned "no structured result" and
        # the scoring hook had nothing to verdict (no report, no auto-eval/insight for
        # exactly the governed runs that paused for approval).
        if not still_paused:
            llm_usd = _llm_cost_from_node_events(await self.store.get_events(run_id))
            payloads = [{"template_name": template_name,
                         "output": _json_safe(result),
                         "llm_cost_usd": llm_usd}]
            cost = _aggregate_cost(payloads)
            resume_update["result_payloads"] = payloads
            resume_update["results"] = [RunResultSummary(
                skill_name="", template_name=template_name,
                duration_seconds=0.0, action_count=0,
            )]
            resume_update["cost"] = cost
            resume_update["cost_baseline_usd"] = _cost_baseline_best_effort(cost)
        await self.store.update(run_id, **resume_update)
        await self._emit(
            run_id, "resumed",
            {"runtime": rt_name, "still_paused": still_paused, **_decision_fields(decision)},
        )
        # SSE subscribers close on a terminal EVENT type (done/failed/cancelled),
        # not on status (protocols.subscribe). So a resume-to-completion MUST
        # publish `done` — otherwise `remote logs -f` / `approve --watch` streams
        # hang after the resume (an internal issue). A re-pause emits the new gate.
        await self._emit(
            run_id,
            repause_status if still_paused else "done",
            {"resumed": True},
        )
        if not still_paused:
            # #1325 — the claim taken at admission outlived the pause (and, under
            # the shared backend, possibly the process that took it), so it is
            # looked up by run id rather than carried. A re-pause keeps holding.
            await self._close_reservations(run_id, settled=True)
            # scoring (#474/#480, ADR-0060 D1) — resume-to-completion is terminal too
            _rec = await self.store.get(run_id)
            await self._terminal_hooks(run_id, status="failed" if getattr(_rec, "status", "") == "failed" else "done")
        return {"run_id": run_id, "runtime": rt_name, "resumed": True,
                "still_paused": still_paused}

    @staticmethod
    async def _close_reservations(run_id: str, *, settled: bool,
                                  actual_usd: float = 0.0) -> None:
        """Close every series-budget claim still held for ``run_id`` (#1325).

        Every terminal closes its claim this way — the claim is taken by a
        ``run.admitting`` provider (#2421 decision 6), so no handler holds the
        :class:`Reservation` object: a completed, failed or cancelled run task, a
        HITL resume (the claim was taken before the pause), an orphaned-gate
        close-out, a cancel of a run whose task is already gone. Under the Postgres backend (#1530) the lookup is shared,
        so this also closes a claim admitted by a *different* replica.

        Best-effort: a reservation hiccup must never fail a terminal that has
        already been persisted — the run is done either way, and an unreleased
        hold is a budget-accounting problem, not a correctness one for THIS run.
        """
        from cairn.core.budget_reservations import held_for_run, release, settle  # noqa: PLC0415 — lazy, matches run_template

        try:
            for r in await held_for_run(run_id):
                if settled:
                    await settle(r, actual_usd=actual_usd)
                else:
                    await release(r)
        except Exception:  # noqa: BLE001
            logger.warning("failed to close budget reservations for run %s",
                           run_id, exc_info=True)

    @staticmethod
    def _recompile_graph(template_name: str):
        """Recompile a template by name → its plain compiled graph (for
        in_process resume; the durable checkpointer holds the paused state)."""
        from cairn.core.engine.compiler import compile_template  # noqa: PLC0415
        from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415

        path = resolve_template_path(template_name)
        compiled = compile_template(path)
        graph = getattr(compiled.runnable, "_compiled_graph", None)
        if graph is None:
            raise ValueError(f"template {template_name!r} has no compiled graph")
        return graph

    def launch_handle(self):
        """The ``OperationContext.launch`` closure (ADR-0120 D7, #2455): how an operator
        starts other governed work without importing the dispatcher. ``pipeline:<ref>``
        goes through the launch door — admission, policy gates (a ``deny`` raises before
        any record exists) and budget apply exactly as for any launch, and only a
        declared pipeline runs (ADR-0106 D1). Any other ref is a template run through
        :meth:`run_template`, the no-orphan chokepoint (ADR-0106 D7), on the background
        lane. Returns the pipeline or run id."""
        async def _launch(ref: str, inputs: dict, trigger: dict) -> str:
            if ref.startswith("pipeline:"):
                from cairn.core.dispatch.pipeline_bridge import PipelineBridge  # noqa: PLC0415 — cycle
                from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: PLC0415 — cycle

                bridge = getattr(self, "_pipeline_bridge", None) or PipelineBridge(self)
                rec = await launch_pipeline(ref, inputs=dict(inputs or {}), bridge=bridge,
                                            trigger=dict(trigger or {}))
                return str(rec.pipeline_id)
            run = await self.run_template(ref, dict(inputs or {}), lane="background")
            return str(run.id)
        return _launch

    # ── Query / control ───────────────────────────────────────────────

    async def get(self, inv_id: str) -> Optional[Run]:
        return await self.store.get(inv_id)

    async def list(
        self,
        *,
        lane: Optional[Lane] = None,
        status: Optional[str] = None,
        workspace_id: Optional[str] = None,
        limit: int = 100,
        include_payloads: bool = False,
    ) -> list[Run]:
        """List runs. Payloads are PROJECTED OUT by default (#1508) — pass
        ``include_payloads=True`` only if you read ``run.result_payloads``."""
        return await self.store.list(  # type: ignore[arg-type]
            lane=lane, status=status, workspace_id=workspace_id, limit=limit,
            include_payloads=include_payloads,
        )

    async def events(self, inv_id: str, *, after_seq: int = 0):
        """Return the persisted event backlog for a run (non-streaming)."""
        return await self.store.get_events(inv_id, after_seq=after_seq)

    async def pending_approval(self, inv_id: str) -> Optional[dict]:
        """The pending HITL approval context for a paused run (#55): the
        ApprovalRequest payload (operator/risk/sources/reason/allowed_decisions)
        the gate raised via ``interrupt()``, captured on the ``awaiting_approval``
        event. Returns ``None`` if the run is unknown.

        ``{"pending": False, "status": ...}`` when not paused. For temporal/dbos
        the pause is internal to the workflow (status stays ``running``), so the
        payload isn't captured here yet — ``approval`` is ``None`` in that case.
        """
        inv = await self.store.get(inv_id)
        if inv is None:
            return None
        if inv.status != "awaiting_approval":
            return {"run_id": inv_id, "pending": False, "status": inv.status}
        approval = None
        for e in await self.store.get_events(inv_id):
            if e.type == "awaiting_approval" and isinstance(e.data, dict) and e.data.get("approval"):
                approval = e.data["approval"]  # latest wins (multi-gate runs)
        return {"run_id": inv_id, "pending": True, "approval": approval}

    def subscribe(self, inv_id: str, *, after_seq: int = 0):
        """Async-iterate a run's events live (backlog then tail). For SSE."""
        return self.store.subscribe(inv_id, after_seq=after_seq)

    async def cancel(self, inv_id: str) -> Optional[Run]:
        """Best-effort cancel. Terminal investigations are returned unchanged."""
        inv = await self.store.get(inv_id)
        if inv is None:
            return None
        if inv.status in ("done", "failed", "cancelled"):
            return inv
        # #601: kill any ACTIVE remote job for this run so a cancelled run does not
        # orphan the remote (GPU) process — a leaked pod / SSH training keeps
        # billing. Best-effort (a cancel failure must not block the run cancel).
        try:
            from cairn.core.operator.job_control import cancel_remote_jobs_for_run  # noqa: PLC0415

            n = await cancel_remote_jobs_for_run(inv_id)
            if n:
                logger.info("cancelled %d remote job(s) for run %s", n, inv_id)
        except Exception:  # noqa: BLE001
            logger.exception("remote-job cancel failed for run %s", inv_id)
        task = self._tasks.get(inv_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        # #1325 — a run cancelled while PAUSED has no live task, so nothing fired
        # _run_template_task's release; and a run whose task did release lands
        # here idempotently (closing a terminal reservation is a no-op).
        await self._close_reservations(inv_id, settled=False)
        return await self.store.get(inv_id)

    def _preserve_on_shutdown(self, run_id: str) -> bool:
        """#618 — True when a CancelledError is from a GRACEFUL SHUTDOWN under a
        DURABLE runtime: the workflow survives in Temporal/DBOS, so the run must
        NOT be marked `cancelled` (that falsely terminates a recoverable run and,
        on the next resume, orphans the durable workflow). Its status is left as-is
        (running / awaiting_approval) for re-attach after restart (#38). A genuine
        user cancel goes through `cancel()`, which sets `cancelled` explicitly and
        happens while `_shutting_down` is False."""
        if self._shutting_down and _settings.runtime() in ("temporal", "dbos"):
            logger.info(
                "shutdown: preserving durable run %s (status kept for re-attach, #618)",
                run_id,
            )
            return True
        return False

    async def aclose(self) -> None:
        """Cancel and drain all in-flight tasks (for graceful shutdown)."""
        self._shutting_down = True  # #618: signal the CancelledError handlers
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
