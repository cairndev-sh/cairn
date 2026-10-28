"""
Skill scheduler — fires scheduled-trigger skills on their cron expressions.

This is the **Tier 1** scheduler: in-process apscheduler. Single-instance,
volatile state (jobs don't survive restart). Good enough for development,
demos, and single-node deployments.

The **Tier 2** replacement is a Temporal-backed durable scheduled workflow
(ADR-0024: Temporal is the durable engine) that survives restarts and
provides observability — same interface, swap the implementation.

Usage::

    runner = Runner(...)
    scheduler = SkillScheduler(runner)
    scheduler.start()
    # ... server runs ...
    scheduler.shutdown()

Each skill with ``triggers.schedule: "<cron>"`` becomes one apscheduler job.
When the job fires, the scheduler builds a synthetic ``TriggerEvent`` and
calls ``runner.run(event)``.

Because Tier-1 state is volatile, fires that fall inside downtime would vanish
silently. ADR-0086 H4 (#1246) closes that: stored schedules carry a watermark
and a ``misfire`` policy, and :meth:`SkillScheduler.catch_up_stored` reconciles
them against the wall clock on every start — see
:mod:`cairn.core.dispatch.schedule_catchup`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from cairn.core._time import utc_now_iso
from cairn.core.dispatch.schedule_catchup import CatchupPlan, apply_catchup, plan_catchup
from cairn.core.engine.runner import Runner
from cairn.core.engine.schema import TriggerEvent

if TYPE_CHECKING:
    from cairn.core.dispatch.dispatcher import RunDispatcher

logger = logging.getLogger("cairn.scheduler")


async def fire_scheduled_workflow(dispatcher: "RunDispatcher", row: Any):
    """Dispatch one stored schedule's fire — through the ONE launch door.

    The **background** lane, as it always was; what changed in #2115 is who does
    it. The fire path goes through :func:`cairn.core.dispatch.start.launch_ref`,
    the same door ``POST /start`` resolves through, so a schedule cannot start
    something the interactive doors would refuse.

    ADR-0106 D3 (#2134) is what it refuses now: triggers belong to a PIPELINE,
    never to a flow. A stored schedule whose target is a bare flow ref — which
    ADR-0101 D1 allowed and #2122 fired as a one-leg pipeline — raises
    :class:`~cairn.core.dispatch.start.FlowNotRunnableError` here, naming the
    wrapper to declare and re-point the schedule at. Loud rather than silent:
    quietly starting a component on a timer is how a pipeline-less run gets
    into the store at 3am.

    Raises on an unknown or non-runnable target (callers log; the create
    endpoint validates up front, so this only trips when a pack changed under a
    stored schedule). The refusals keep their schedule-shaped wording: the
    operator reading the log needs to know it was a SCHEDULE that stopped
    firing."""
    # lazy: the start door pulls the pack/contract stack — keep it off the
    # scheduler's import path (this module is imported at server boot).
    from cairn.core.dispatch.start import (  # noqa: PLC0415
        FlowNotRunnableError,
        UnknownStartRefError,
        launch_ref,
    )

    ref = row.workflow
    inputs = dict(row.inputs or {})
    # ADR-0131 D3 — a CORE dataset watch rides this row under a reserved key: it
    # never reaches the launch, and it makes this fire a probe first.
    from cairn.core.engine.triggers_grammar import CORE_WATCH_KEY  # noqa: PLC0415 - engine grammar off the boot import path
    watch = inputs.pop(CORE_WATCH_KEY, None)
    marker: Optional[dict] = None
    if isinstance(watch, dict):
        marker = await _probe_core_watch(row, watch)
        if marker is None:
            return None  # unchanged: nothing to start (checked_at stamped)
        if watch.get("on_change") == "none":
            await _advance_core_watch(row, marker)
            return None
    # #1159 — a schedule may target a PIPELINE (ADR-0084 D9 makes it the primary
    # runnable noun). It has no template contract, so it takes the pipeline door
    # instead of run_template; `launch_ref` keeps that branch and adds the flow
    # one, which is now the one-leg pipeline door rather than a bare dispatch.
    is_pipeline = str(ref).startswith("pipeline:")
    try:
        launched = await launch_ref(
            ref, inputs, dispatcher=dispatcher,
            environment=row.environment or "",
            # ADR-0071 (#792) — a schedule may pin a preset; empty falls through to the
            # workflow's default preset in the dispatcher (like any trigger).
            config_preset=getattr(row, "config_preset", "") or "",
            lane="" if is_pipeline else "background",
            workspace_id=getattr(row, "workspace_id", "default") or "default",
            trigger={"source": "watch" if marker is not None else "schedule",
                     "schedule_id": row.id, "cron": row.cron},
        )
        if marker is not None:
            # only once the door accepted the start: a refused launch keeps the old
            # marker, so the next tick sees the same change and tries again.
            await _advance_core_watch(row, marker)
        return launched
    except FlowNotRunnableError as e:
        raise ValueError(
            f"schedule {getattr(row, 'id', '?')} targets {ref}, which is a flow "
            f"and no longer starts on its own (ADR-0106 D1/D3). {e}") from e
    except UnknownStartRefError as e:
        raise ValueError(f"schedule target not found: {ref} ({e})") from e


async def _probe_core_watch(row: Any, watch: dict) -> Optional[dict]:
    """ADR-0131 D3 — did the watched registered dataset gain a version since this
    row's marker? Returns the marker to store once the change is acted on, or
    ``None`` when nothing changed (then ``checked_at`` is stamped). The probe is
    core's one copy (``operator/watch_probe.py``), shared with ``ingest/watch``."""
    from cairn.core.operator import build_artifact_store  # noqa: PLC0415 - the storage stack, lazily
    from cairn.core.operator.watch_probe import probe_dataset  # noqa: PLC0415
    from cairn.core.workspace import workspace_scope  # noqa: PLC0415

    name = str(watch.get("dataset") or "")
    state = dict(getattr(row, "watch_state", None) or {})
    store = build_artifact_store()
    try:
        # The watched name is the schedule's workspace's dataset, the same
        # workspace its launch starts in.
        with workspace_scope(getattr(row, "workspace_id", "default") or "default"):
            versions = await store.list_dataset_versions(name)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            await close()
    probe = probe_dataset(name, versions, state.get("marker") or {})
    if not probe.changed:
        await _write_watch_state(row, {"checked_at": utc_now_iso()})
        logger.info("watch %s: %s", getattr(row, "id", "?"), probe.summary)
        return None
    logger.info("watch %s: %s", getattr(row, "id", "?"), probe.summary)
    return dict(probe.marker)


async def _advance_core_watch(row: Any, marker: dict) -> None:
    now = utc_now_iso()
    await _write_watch_state(row, {"marker": marker, "changed_at": now, "checked_at": now})


async def _write_watch_state(row: Any, updates: dict) -> None:
    """Merge into the row's ``watch_state`` blob — the marker's only home (no store)."""
    from cairn.core.storage.schedule_store import default_schedule_store  # noqa: PLC0415

    merged = {**(getattr(row, "watch_state", None) or {}), **updates}
    store = default_schedule_store()
    try:
        await store.update_fields(row.id, {"watch_state": merged},
                                  getattr(row, "workspace_id", None) or None)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()


class SkillScheduler:
    """Fires scheduled-trigger skills via apscheduler.

    Built around the SkillEngine — at start() we walk every loaded skill,
    look at its ``triggers.schedule`` field, and register a cron job for
    each non-empty expression.
    """

    def __init__(self, runner: Runner, dispatcher: "RunDispatcher | None" = None) -> None:
        self.runner = runner
        # When a dispatcher is supplied, scheduled fires go through the
        # **background lane** (shared concurrency budget + registry records).
        # Without one we fall back to calling the runner directly (back-compat).
        self.dispatcher = dispatcher
        self._scheduler = AsyncIOScheduler()
        self._jobs: dict[str, str] = {}  # skill_name → apscheduler job_id
        # #1246 — the start-time catch-up pass; held so the task isn't GC'd mid-flight.
        self._catchup_task: Optional[asyncio.Task] = None

    def register_all(self) -> int:
        """Walk skills, register a job for each one that has a schedule.

        Returns the number of jobs registered.
        """
        registered = 0
        for skill_name in self.runner.skill_engine.list_skills():
            spec = self._spec_for(skill_name)
            if spec is None:
                continue
            cron = getattr(spec.triggers, "schedule", None)
            if not cron:
                continue
            try:
                job = self._scheduler.add_job(
                    self._fire,
                    CronTrigger.from_crontab(cron),
                    args=[skill_name, spec.name, cron],
                    id=f"skill:{skill_name}",
                    name=f"skill {skill_name} @ {cron}",
                    misfire_grace_time=60,
                    coalesce=True,        # if multiple fires queue, run once
                    max_instances=1,      # never run two of the same in parallel
                    replace_existing=True,
                )
                self._jobs[skill_name] = job.id
                logger.info(
                    "scheduler registered: skill=%s cron=%s next_run=%s",
                    skill_name, cron, job.next_run_time,
                )
                registered += 1
            except Exception as exc:
                logger.error(
                    "scheduler skipped skill=%s cron=%s reason=%s",
                    skill_name, cron, exc,
                )
        return registered

    # ── Console-authored schedules (#354) ────────────────────────────
    #
    # Layered like triggers: pack skills carry read-only cron defaults
    # (register_all above); the schedule store carries what the console
    # writes. One apscheduler job per ENABLED row, re-synced on every
    # console mutation (create/toggle/delete → resync_stored()).

    def register_stored(self) -> int:
        """Register one job per enabled StoredSchedule. Returns the count."""
        from cairn.core.storage.schedule_store import default_schedule_store

        registered = 0
        store = default_schedule_store()
        try:
            rows = store.list_sync()
        finally:
            store.close()
        for row in rows:
            if not row.enabled:
                continue
            try:
                self._scheduler.add_job(
                    self._fire_stored,
                    CronTrigger.from_crontab(row.cron),
                    args=[row.id],
                    id=f"sched:{row.id}",
                    name=f"schedule {row.name} → {row.workflow} @ {row.cron}",
                    misfire_grace_time=60,
                    coalesce=True,
                    max_instances=1,
                    replace_existing=True,
                )
                registered += 1
            except Exception as exc:  # bad cron in the store must not kill startup
                logger.error("scheduler skipped stored schedule=%s cron=%s reason=%s", row.id, row.cron, exc)
        return registered

    def resync_stored(self) -> int:
        """Drop every ``sched:*`` job and re-register from the store — called by
        the console CRUD endpoints so mutations take effect live (no restart)."""
        for job in list(self._scheduler.get_jobs()):
            if str(job.id).startswith("sched:"):
                self._scheduler.remove_job(job.id)
        n = self.register_stored()
        logger.info("scheduler re-synced stored schedules: %d job(s)", n)
        return n

    def list_stored_jobs(self) -> list[dict[str, Any]]:
        """Stored-schedule rows + their live next-fire time, for the console."""
        from cairn.core.storage.schedule_store import default_schedule_store

        store = default_schedule_store()
        try:
            rows = store.list_sync()
        finally:
            store.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            job = self._scheduler.get_job(f"sched:{row.id}")
            # pending jobs (scheduler not started yet) have no next_run_time attr
            nxt = getattr(job, "next_run_time", None) if job else None
            out.append({
                "id": row.id,
                "name": row.name,
                "workspace_id": row.workspace_id,
                "workflow": row.workflow,
                "schedule": row.cron,
                "environment": row.environment,
                "enabled": row.enabled,
                "next_run_time": str(nxt) if nxt else None,
                "origin": "store",
                # #1246 — what happens to fires missed during downtime, and when
                # this schedule last actually ran.
                "misfire": getattr(row, "misfire", "") or "skip",
                "last_fired_at": getattr(row, "last_fired_at", "") or None,
            })
        return out

    async def _fire_stored(self, schedule_id: str) -> None:
        """Fire one stored schedule: re-read the row (fresh inputs/enabled),
        enter its workspace, and dispatch the template run through the SAME
        path as ``POST /packs/{ref}/run`` (validate → provenance → dispatch).
        Errors log, never propagate — a broken schedule must not kill the
        scheduler."""
        from cairn.core.storage.schedule_store import default_schedule_store
        from cairn.core.workspace import reset_current_workspace, set_current_workspace

        store = default_schedule_store()
        try:
            row = await store.get(schedule_id)
        finally:
            store.close()
        if row is None or not row.enabled:
            logger.info("stored schedule %s gone/disabled — skipping fire", schedule_id)
            return
        if self.dispatcher is None:
            logger.error("stored schedule %s cannot fire — no dispatcher", schedule_id)
            return
        logger.info("scheduler firing stored schedule=%s workflow=%s cron=%s", row.id, row.workflow, row.cron)
        token = set_current_workspace(row.workspace_id)
        try:
            run = await fire_scheduled_workflow(self.dispatcher, row)
            # a pipeline schedule returns its PipelineRecord (`pipeline_id`), a template one its Run (`id`)
            launched = getattr(run, "pipeline_id", None) or getattr(run, "id", None)
            logger.info("stored schedule %s dispatched %s", row.id, launched)
            # #1246 — the fire watermark. Stamped only after a SUCCESSFUL dispatch,
            # so a fire that raised stays "missed" for the next catch-up pass.
            await self._stamp(row, {"last_fired_at": utc_now_iso()})
        except Exception as exc:
            logger.exception("stored schedule fire failed schedule=%s: %s", row.id, exc)
        finally:
            reset_current_workspace(token)

    # ── Catch-up after downtime (ADR-0086 H4, #1246) ─────────────────

    async def _stamp(self, row: Any, updates: dict[str, Any]) -> None:
        """Persist watermark fields on one stored schedule. Best-effort: losing a
        stamp costs a re-audit on the next start, never a broken fire."""
        from cairn.core.storage.schedule_store import default_schedule_store

        store = default_schedule_store()
        try:
            await store.update_fields(row.id, updates, getattr(row, "workspace_id", None))
        except Exception as exc:  # noqa: BLE001 — bookkeeping must not break the fire
            logger.warning("could not stamp schedule %s %s: %s", row.id, sorted(updates), exc)
        finally:
            store.close()

    async def catch_up_stored(self, now: Optional[datetime] = None) -> list[CatchupPlan]:
        """Reconcile every enabled stored schedule against the wall clock.

        For each row, cron occurrences between its watermark and now are the
        fires that server downtime swallowed; the row's ``misfire`` policy
        decides what happens (``skip`` audits only — the default — ``fire_once``
        coalesces, ``fire_all`` replays up to a cap). Returns the non-empty plans
        so callers/tests can see what was decided.

        Called once from :meth:`start`, NOT from :meth:`resync_stored` — a
        console mutation must never re-fire history."""
        from cairn.core.storage.schedule_store import default_schedule_store
        from cairn.core.workspace import reset_current_workspace, set_current_workspace

        store = default_schedule_store()
        try:
            rows = store.list_sync()
        finally:
            store.close()

        applied: list[CatchupPlan] = []
        for row in rows:
            if not row.enabled:
                continue
            try:
                plan = plan_catchup(row, now=now)
            except Exception as exc:  # noqa: BLE001 — a bad cron must not stop the pass
                logger.error("schedule %s catch-up planning failed: %s", row.id, exc)
                continue
            if not plan:
                continue
            token = set_current_workspace(row.workspace_id)
            try:
                await apply_catchup(row, plan, self._fire_stored)
                # Advance the watermark past everything this plan accounted for —
                # audited-and-skipped windows included — so the next restart
                # doesn't replay the same report.
                if plan.watermark is not None:
                    await self._stamp(row, {"last_missed_at": plan.watermark.astimezone(timezone.utc).isoformat()})
                applied.append(plan)
            except Exception as exc:  # noqa: BLE001 — one schedule must not kill the pass
                logger.exception("schedule %s catch-up failed: %s", row.id, exc)
            finally:
                reset_current_workspace(token)
        if applied:
            logger.info("schedule catch-up: %d schedule(s) had missed windows", len(applied))
        return applied

    def _start_catchup(self) -> None:
        """Kick the catch-up pass onto the running loop (fire-and-forget: startup
        must not block on N dispatches). Without a running loop — a synchronous
        embedder — the pass is skipped and said so."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.info("schedule catch-up skipped — scheduler started without a running loop")
            return

        async def _guarded() -> None:
            try:
                await self.catch_up_stored()
            except Exception as exc:  # noqa: BLE001 — catch-up must never kill startup
                logger.exception("schedule catch-up pass failed: %s", exc)

        self._catchup_task = loop.create_task(_guarded())

    def start(self) -> None:
        """Register jobs and start the scheduler. Idempotent."""
        if self._scheduler.running:
            return
        self.register_all()
        self.register_stored()
        self._scheduler.start()
        logger.info("scheduler started with %d job(s)", len(self._scheduler.get_jobs()))
        self._start_catchup()

    def shutdown(self) -> None:
        if self._catchup_task is not None and not self._catchup_task.done():
            self._catchup_task.cancel()  # a shutdown mid-catch-up leaves the watermark for next start
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("scheduler stopped")

    def list_jobs(self) -> list[dict[str, Any]]:
        """For debug endpoints / `cairn scheduler list` / the console Scheduled view."""
        out: list[dict[str, Any]] = []
        for skill_name, job_id in self._jobs.items():
            job = self._scheduler.get_job(job_id)
            # the cron string is the 3rd positional arg passed at registration
            # (args=[skill_name, spec_name, cron]); surface it so the schedule is
            # visible, not just the next fire time.
            cron = job.args[2] if job and len(getattr(job, "args", []) or []) >= 3 else None
            out.append({
                "skill_name": skill_name,
                "job_id": job_id,
                "schedule": cron,
                "next_run_time": str(job.next_run_time) if job else None,
            })
        return out

    # ── Internals ────────────────────────────────────────────────────

    def _spec_for(self, skill_name: str):
        """Reach into SkillEngine for a SkillSpec by name."""
        for spec in getattr(self.runner.skill_engine, "skills", []):
            if spec.name == skill_name:
                return spec
        return None

    async def _fire(self, skill_name: str, spec_name: str, cron: str) -> None:
        """Build a synthetic schedule TriggerEvent and run the pipeline.

        Errors are logged but never propagated — a misbehaving skill must not
        kill the scheduler.
        """
        event = TriggerEvent(
            source="schedule",
            matched_pattern=spec_name,
            service=None,
            payload={"cron": cron, "schedule_source": "in-process"},
        )
        logger.info("scheduler firing skill=%s cron=%s", skill_name, cron)
        try:
            if self.dispatcher is not None:
                inv = await self.dispatcher.submit(event, lane="background")
                logger.info(
                    "scheduler enqueued skill=%s on background lane — inv=%s",
                    skill_name, inv.id,
                )
            else:
                results = await self.runner.run(event)
                logger.info(
                    "scheduler fired skill=%s — produced %d investigation(s)",
                    skill_name, len(results),
                )
        except Exception as exc:
            logger.exception(
                "scheduler fire failed skill=%s: %s", skill_name, exc,
            )
