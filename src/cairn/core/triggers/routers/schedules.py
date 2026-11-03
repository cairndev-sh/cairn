"""Skills + scheduled (cron) triggers — list skills, CRUD console-authored
schedules (#354), workspace-scoped visibility (#271).

Extracted verbatim from server.py (#1191).
"""
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import _oauth2_scheme, require_auth
from cairn.core.workspace import current_workspace


def build_schedules_router(runner, scheduler, rag_scheduler) -> APIRouter:
    """The /triggers/skills + /triggers/scheduled surface, closed over the
    runner (skill engine) and the two schedulers."""
    r = APIRouter()

    @r.get("/triggers/skills")
    async def list_skills() -> dict[str, Any]:
        # Detailed listing surfaces the Pack → Module → Template grouping;
        # `names` kept for back-compat with existing callers.
        detail = runner.skill_engine.list_skills_detail()
        return {"skills": detail, "names": [d["name"] for d in detail]}

    @r.get("/triggers/scheduled")
    async def list_scheduled() -> dict[str, Any]:
        """Scheduled (cron) skill jobs, scoped to the workspace (#271). The Tier-1
        scheduler is engine-global, so a skill's cron job surfaces in a workspace only
        when the workflow it fires is enabled there — mirroring /ui/api/triggers, so a
        fresh workspace doesn't inherit every pack's schedules. Workspace comes from
        the path (`/workspaces/{id}/triggers/scheduled`)."""
        jobs = scheduler.list_jobs() if scheduler else []
        if jobs and scheduler is not None:
            from cairn.core.workflow_registry import active_workflow_refs
            enabled = await active_workflow_refs(current_workspace())
            # skill_name → the template it fires (SkillSpec.template.template)
            tmpl_for: dict[str, str] = {}
            for spec in getattr(getattr(scheduler, "runner", None) and scheduler.runner.skill_engine, "skills", []):
                t = getattr(getattr(spec, "template", None), "template", None)
                if t:
                    tmpl_for[spec.name] = t

            def _vis(j: dict) -> bool:
                t = tmpl_for.get(j.get("skill_name") or "") or ""
                return bool(t) and (t in enabled or t.rsplit("/", 1)[-1] in enabled)

            jobs = [j for j in jobs if _vis(j)]
        # console-authored schedules (#354): store rows are the truth (they
        # exist with or without a live scheduler); next-fire time joins in
        # from the scheduler when it's running. Workspace-scoped like jobs.
        from cairn.core.storage.schedule_store import default_schedule_store
        sstore = default_schedule_store()
        try:
            stored_rows = await sstore.list(current_workspace())
        finally:
            sstore.close()
        by_id = {j["id"]: j for j in (scheduler.list_stored_jobs() if scheduler else [])}
        stored = []
        for row in stored_rows:
            live = by_id.get(row.id) or {}
            stored.append({
                "id": row.id, "name": row.name, "workflow": row.workflow,
                "schedule": row.cron, "environment": row.environment,
                "enabled": row.enabled, "origin": "store",
                "config_preset": getattr(row, "config_preset", "") or "",  # #792 pinned preset
                "next_run_time": live.get("next_run_time"),
                # #1246 — downtime policy + when it last actually ran, so
                # "why didn't the nightly run?" is answerable in the console.
                "misfire": getattr(row, "misfire", "") or "skip",
                "last_fired_at": getattr(row, "last_fired_at", "") or None,
                # ADR-0101 D1 (#1973) — the workflow whose front matter declares
                # this row, empty when a user made it here. The console shows a
                # declared row read-only ("edit the pack") and links it back to
                # its workflow's Triggers tab.
                "declared_by": getattr(row, "declared_by", "") or "",
                # ADR-0101 D4 (#1976) — a WATCH's memory. Empty for every other
                # schedule, so the console can tell a watch row from a cron row
                # without a second read: `checked_at` is the last look,
                # `changed_at` the last time something was actually admitted.
                "watch_state": dict(getattr(row, "watch_state", None) or {}),
            })
        return {
            "jobs": jobs,
            "stored": stored,
            "rag_jobs": rag_scheduler.list_jobs() if rag_scheduler else [],
        }

    def _validated_inputs(workflow: str, inputs: dict[str, Any], environment: str) -> dict[str, Any]:
        """Check a new schedule's inputs at the door the FIRE will actually meet.

        A stored schedule is fail-closed by design (#354): it is validated at
        create so that a row in the store is always runnable. There are two fire
        doors, though, and they validate differently — ``fire_scheduled_workflow``
        sends a ``pipeline:`` row to ``launch_pipeline`` (``missing_inputs`` →
        ``coerce_declared_inputs``) and everything else to ``run_template``
        (``coerce_and_validate`` over the UI contract). Validating a pipeline
        row against the template contract is how a schedule passes here and
        fails at 3am, so each ref is checked against its own door (ADR-0101 S7,
        #1987; the seeded-watch lesson from #1985).
        """
        # lazy, like every other import in this router: the contract stack and
        # the pipeline resolver both reach back into the server (routers ↔
        # server cycle at import time), and only one of the two is ever needed.
        from cairn.core.engine.triggers_grammar import PIPELINE_REF_PREFIX

        if not workflow.startswith(PIPELINE_REF_PREFIX):
            from cairn.core.ui_contract import build_ui_contract
            from cairn.core.ui_render import coerce_and_validate

            contract = build_ui_contract(workflow, profile=environment or None)
            if not contract.plan.get("template_found"):
                raise HTTPException(status_code=404, detail=f"template not found: {workflow}")
            typed, errors = coerce_and_validate(contract, inputs)
            if errors:
                raise HTTPException(status_code=422, detail={"errors": errors})
            return typed

        from cairn.core.dispatch.pipeline_launch import (
            InputCoercionError, PipelineNotFoundError, coerce_declared_inputs,
            missing_inputs, resolve_or_raise,
        )
        from cairn.core.engine.pipeline_resolve import AmbiguousRefError

        try:
            d = resolve_or_raise(workflow)
        except PipelineNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except AmbiguousRefError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        try:
            typed = coerce_declared_inputs(d, inputs)
        except InputCoercionError as e:
            raise HTTPException(status_code=422, detail={"errors": [str(e)]}) from e
        if (missing := missing_inputs(d, typed)):
            raise HTTPException(
                status_code=422,
                detail={"errors": [f"pipeline {d.pack}/{d.name} requires input(s) "
                                   f"{missing!r} — supply them or give the pipeline "
                                   f"a default"]},
            )
        return typed

    @r.post("/triggers/scheduled", dependencies=[Depends(support.writable_workspace)])
    async def create_scheduled(body: dict[str, Any]) -> dict[str, Any]:
        """Create a console-authored cron schedule (#354): validate the cron,
        the ref AND the inputs (same validator the fire path uses) before
        anything is stored — fail-closed, a stored schedule is always runnable.
        The scheduler re-syncs immediately (no restart).

        ``workflow`` names a template ref or, since ADR-0101 S7 (#1987), a
        ``pipeline:<pack>/<name>`` ref — the same value the fire path has
        accepted since #1159, so the pipeline's own Triggers tab can create a
        hand-made schedule beside its declared ones.
        """
        from apscheduler.triggers.cron import CronTrigger
        from cairn.core.dispatch.schedule_catchup import DEFAULT_MISFIRE, MISFIRE_POLICIES
        from cairn.core.storage.schedule_store import StoredSchedule, default_schedule_store

        name = str(body.get("name", "")).strip()
        cron = str(body.get("cron", "")).strip()
        workflow = str(body.get("workflow", "")).strip()
        if not name or not cron or not workflow:
            raise HTTPException(status_code=400, detail="name, cron and workflow are required")
        try:
            CronTrigger.from_crontab(cron)
        except Exception as e:  # noqa: BLE001 — apscheduler raises plain ValueError subclasses
            raise HTTPException(status_code=400, detail=f"invalid cron expression: {e}") from e
        # #1246 — misfire policy is fail-closed like the cron: an unknown value is
        # rejected at create rather than silently degrading to the default later.
        misfire = str(body.get("misfire", "") or DEFAULT_MISFIRE).strip().lower()
        if misfire not in MISFIRE_POLICIES:
            raise HTTPException(
                status_code=400,
                detail=f"invalid misfire policy {misfire!r} — one of {list(MISFIRE_POLICIES)}",
            )
        environment = str(body.get("environment", "") or "")
        typed = _validated_inputs(workflow, dict(body.get("inputs") or {}), environment)
        row = StoredSchedule(
            name=name, cron=cron, workflow=workflow, inputs=typed,
            environment=environment, enabled=bool(body.get("enabled", True)),
            description=str(body.get("description", "") or ""),
            # ADR-0071 (#792) — pin a preset to this schedule (best-effort; if the
            # preset is later deleted the dispatcher falls back to the default).
            config_preset=str(body.get("config_preset", "") or ""),
            misfire=misfire,
            workspace_id=current_workspace(),
        )
        sstore = default_schedule_store()
        try:
            await sstore.create(row)
        finally:
            sstore.close()
        if scheduler:
            scheduler.resync_stored()
        return {"id": row.id, "name": row.name, "cron": row.cron, "workflow": row.workflow,
                "enabled": row.enabled, "misfire": row.misfire}

    @r.post("/triggers/scheduled/preview")
    async def preview_scheduled(body: dict[str, Any]) -> dict[str, Any]:
        """When would this cron actually fire? (#1568)

        Computed with the SAME ``CronTrigger`` class that schedules the job, so
        the preview cannot disagree with reality. The alternative — parsing cron
        in the console — would make the preview a copy of a fact that lives in
        apscheduler, which is the drift shape that produced #1640 and #1597.

        Invalid cron returns 400 with the parser's own message, so the form can
        show the reason while someone is still typing rather than at save.
        """
        from apscheduler.triggers.cron import CronTrigger

        cron = str(body.get("cron", "")).strip()
        count = max(1, min(int(body.get("count") or 3), 10))
        if not cron:
            raise HTTPException(status_code=400, detail="cron is required")
        try:
            trigger = CronTrigger.from_crontab(cron)
        except Exception as e:  # noqa: BLE001 — apscheduler raises plain ValueError subclasses
            raise HTTPException(status_code=400, detail=f"invalid cron expression: {e}") from e

        # Walk the trigger forward the way the scheduler does: each fire time is
        # the previous one's successor, so a preview of N reflects the real
        # cadence rather than N copies of "the next one".
        from datetime import datetime, timezone
        fires: list[str] = []
        previous = None
        now = datetime.now(timezone.utc)
        for _ in range(count):
            nxt = trigger.get_next_fire_time(previous, now if previous is None else previous)
            if nxt is None:
                break
            fires.append(nxt.isoformat())
            previous = nxt
        return {"cron": cron, "next": fires}

    @r.patch("/triggers/scheduled/{schedule_id}", dependencies=[Depends(support.writable_workspace)])
    async def toggle_scheduled(schedule_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Pause/resume a stored schedule — the off switch unattended crons need —
        and (#1246) change its downtime ``misfire`` policy without recreating it."""
        from cairn.core.dispatch.schedule_catchup import MISFIRE_POLICIES
        from cairn.core.storage.schedule_store import default_schedule_store

        updates: dict[str, Any] = {}
        if "enabled" in body:
            updates["enabled"] = bool(body["enabled"])
        if "misfire" in body:
            misfire = str(body.get("misfire") or "").strip().lower()
            if misfire not in MISFIRE_POLICIES:
                raise HTTPException(
                    status_code=400,
                    detail=f"invalid misfire policy {misfire!r} — one of {list(MISFIRE_POLICIES)}",
                )
            updates["misfire"] = misfire
        if not updates:
            raise HTTPException(status_code=400, detail="body must carry 'enabled' and/or 'misfire'")
        sstore = default_schedule_store()
        try:
            row = await sstore.update_fields(schedule_id, updates, current_workspace())
        finally:
            sstore.close()
        if row is None:
            raise HTTPException(status_code=404, detail="schedule not found")
        if scheduler:
            scheduler.resync_stored()
        return {"id": row.id, "enabled": row.enabled, "misfire": row.misfire}

    # ── ADR-0160 (#290): the switch for trigger-store rows (event, webhook) ──

    @r.get("/triggers/stored")
    async def list_stored_triggers() -> dict[str, Any]:
        """This workspace's trigger-store rows: the ones a pipeline declares
        (``declared_by``) and the ones made by hand, with their switch."""
        from cairn.core.storage.trigger_store import default_trigger_store

        store = default_trigger_store()
        try:
            rows = await store.list(current_workspace())
        finally:
            store.close()
        return {"triggers": [_stored_view(t) for t in rows]}

    @r.patch("/triggers/stored/{trigger_id}",
             dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def toggle_stored_trigger(
        trigger_id: str, body: dict[str, Any],
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """Enable or pause one trigger-store row of this workspace (ADR-0131 D4's
        "paused / resumed per workspace"). Another workspace's row is a 404. An
        event row takes effect on the next message; a webhook row is mounted when
        the server starts, so it takes effect then."""
        from cairn.core._time import utc_now_iso
        from cairn.core.obs.audit import audit_emit
        from cairn.core.storage.trigger_store import default_trigger_store
        from cairn.core.triggers.routers.reviews import decision_actor

        if not isinstance(body.get("enabled"), bool):
            raise HTTPException(status_code=400, detail="body must carry 'enabled': true or false")
        enabled = body["enabled"]
        store = default_trigger_store()
        try:
            row = await store.get(trigger_id, current_workspace())
            if row is None:
                raise HTTPException(status_code=404, detail="trigger not found")
            if row.enabled != enabled:
                # the store's create is an upsert (the declared-trigger converge path
                # updates rows the same way); only the switch and the clock change
                row = await store.create(row.model_copy(update={"enabled": enabled,
                                                                "updated_at": utc_now_iso()}))
        finally:
            store.close()
        actor = await decision_actor(token, None, fallback="anonymous")
        await audit_emit("trigger.enabled" if enabled else "trigger.paused", actor=actor,
                         target=row.id, source=row.source, declared_by=row.declared_by or None)
        return {**_stored_view(row),
                "applies": "now" if row.source == "event" else "at the next server start"}

    @r.delete("/triggers/scheduled/{schedule_id}", dependencies=[Depends(support.writable_workspace)])
    async def delete_scheduled(schedule_id: str) -> dict[str, Any]:
        from cairn.core.storage.schedule_store import default_schedule_store

        sstore = default_schedule_store()
        try:
            ok = await sstore.delete(schedule_id, current_workspace())
        finally:
            sstore.close()
        if not ok:
            raise HTTPException(status_code=404, detail="schedule not found")
        if scheduler:
            scheduler.resync_stored()
        return {"deleted": schedule_id}

    return r


def _stored_view(t: Any) -> dict[str, Any]:
    return {"id": t.id, "name": t.name, "source": t.source, "enabled": t.enabled,
            "declared_by": t.declared_by, "description": t.description,
            "updated_at": t.updated_at}
