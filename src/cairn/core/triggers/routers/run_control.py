"""Run control — HITL resume (ADR-0034), event log/SSE stream, cancel, and
the approval-expiry lifecycle (#896).

Extracted verbatim from server.py (#1191). The runs-surface workspace guard
lives here now (the read-side twin is in routers/runs.py).
"""
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from starlette.responses import StreamingResponse

from cairn.core._time import utc_now_iso
from cairn.core.dispatch.dispatcher import NotResumableError, OrphanedGateError
from cairn.core.engine.schema import ApprovalRecord
from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import _oauth2_scheme, require_auth
from cairn.core.triggers.routers.reviews import decision_actor
from cairn.core.workspace import current_workspace

logger = logging.getLogger("cairn.triggers.run_control")


def build_run_control_router(dispatcher, *, pipeline_bridge=None) -> APIRouter:
    """The run-action surface (/runs/{id} resume/events/pending-approval/
    stream/cancel/expire + /approvals/dismiss), closed over the dispatcher."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # request models + module helpers the inline block shared.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        DismissApprovals,
        ResumeRequest,
        logger,
    )

    r = APIRouter()

    async def _run_in_ws_or_404(run_id: str):
        """Fetch a run, 404 if absent OR owned by another workspace (#228 B3/B6).
        Cross-workspace access is indistinguishable from not-found, so run
        existence never leaks across tenants."""
        inv = await dispatcher.get(run_id)
        if inv is None or inv.workspace_id != current_workspace():
            raise HTTPException(status_code=404, detail="run not found")
        return inv

    # ── Operation-gate HITL resume (ADR-0034; an internal issue) ──
    #
    # Distinct from /triggers/approve (the action-saga HITL, ADR-0024): this
    # resumes a TEMPLATE run paused at an operation interrupt, routing to the
    # deployment's runtime (in_process recompile / temporal signal / dbos send).
    @r.post("/runs/{run_id}/resume", dependencies=[Depends(support.writable_workspace)])
    async def resume_run_endpoint(
        run_id: str,
        body: ResumeRequest | None = None,
        # Legacy query-param form — the CLI, TUI and web console all POST
        # ``?decision=…`` with no body, so we keep accepting it. Defaults are
        # None (NOT "approve") so an omitted decision is caught below.
        decision: str | None = None,
        approver: str | None = None,
        note: str | None = None,
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        # #628 — resolve the decision from the JSON body first (a UI/SDK sends
        # one), then the legacy query param. NO fail-open default: an absent
        # decision is a 422, never a silent approve. Previously the body was
        # ignored and a missing query param defaulted to "approve", so a client
        # that sent {"decision": "reject"} had its rejection turned into an
        # approval — the governance footgun this fixes. Request-shape validation
        # (422) runs before the existence guard (404), the conventional order.
        decision_val = (body.decision if body else None) or decision
        if not decision_val or not decision_val.strip():
            raise HTTPException(
                status_code=422,
                detail="resume requires an explicit 'decision' (approve|reject) — "
                       "refusing to default to approve",
            )
        inv = await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        # #618 — resume must reject a run that is NOT awaiting approval. Resuming a
        # terminal run (cancelled/done/failed) previously returned 200 + delivered
        # the approve signal to the durable runtime, ORPHANING the workflow (running
        # in Temporal, terminal in cairn). This is the terminal-state half of the
        # fail-closed guarantee (#628 added the missing-decision half).
        if inv.status != "awaiting_approval":
            raise HTTPException(
                status_code=409,
                detail=f"run is '{inv.status}', not awaiting approval — cannot resume",
            )
        # #2310 — recorded as the caller's identity; a claimed approver is ignored under auth
        approver_val = await decision_actor(token, (body.approver if body else None) or approver, fallback="api-user")
        note_val = (body.note if body else None) or note
        approved = decision_val.strip().lower() in ("approve", "approved", "yes", "y")
        record = ApprovalRecord(
            approved=approved,
            approver=approver_val,
            channel="api",
            timestamp_iso=utc_now_iso(),
            # caller-supplied note is recorded verbatim into the audit chain;
            # otherwise fall back to an auto-generated provenance string.
            note=note_val or f"resolved via /runs/{run_id}/resume (decision={decision_val})",
        )
        try:
            return await dispatcher.resume_run(run_id, record.model_dump(mode="json"))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
        except OrphanedGateError as exc:
            # #365 — approving a gate whose checkpoint died with a previous
            # engine process would lie (nothing can resume). 409 + remediation.
            raise HTTPException(status_code=409, detail=str(exc))
        except NotResumableError as exc:
            # A pipeline step gate (or a run with no template): the wrong verb for
            # this run, not an engine fault. 409 + the door that works.
            raise HTTPException(status_code=409, detail=str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.warning("resume failed for %s: %s", run_id, exc)
            raise HTTPException(status_code=502, detail=f"resume failed: {exc}")

    @r.get("/runs/{run_id}/events")
    async def get_run_events(run_id: str, after_seq: int = 0) -> dict[str, Any]:
        """Non-streaming snapshot of a run's append-only event log."""
        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        evs = await dispatcher.events(run_id, after_seq=after_seq)
        return {"run_id": run_id, "count": len(evs),
                "events": [e.model_dump(mode="json") for e in evs]}

    # ── #2025 (ADR-0102 D4): a remote worker's events UP, control DOWN, over the
    #    control plane's own API — the `http` job channel. The store is the log:
    #    ingest is idempotent on the worker's (node, seq), control is a run event.

    @r.post("/runs/{run_id}/events/ingest",
            dependencies=[Depends(require_auth), Depends(support.writable_workspace_unless_worker)])
    async def ingest_run_events(run_id: str, body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        """Append a batch of a worker's report events — once each, whatever the
        worker replayed after a reconnect. Body: ``{"events": [{"type", "data":
        {node, seq, …}}, …]}``. Returns the per-node ``last_seq`` the worker
        advances its spool by, and names anything refused."""
        from cairn.core.operator.report_ingest import ingest_events  # noqa: PLC0415 — routers ↔ core cycle at import time

        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        events = body.get("events")
        if not isinstance(events, list):
            raise HTTPException(status_code=422, detail="body.events must be a list")
        if len(events) > 5000:
            raise HTTPException(status_code=413, detail="at most 5000 events per batch")
        res = await ingest_events(dispatcher.store, run_id, events)
        if res.accepted:
            # ADR-0120 D4 (#2421): whoever listens reacts (the platform's mid-training
            # divergence handling); the public core only records.
            from cairn.core.pack import run_hooks  # noqa: PLC0415 — off the hot path

            await run_hooks.fire(run_hooks.RUN_EVENTS_INGESTED, run_id=run_id, events=list(events),
                                 store=dispatcher.store, pipeline_store=getattr(pipeline_bridge, "store", None))
        return {"run_id": run_id, **res.as_dict()}

    # ── #2026 (ADR-0103 D1): what every operator DECLARES it reports ──

    @r.get("/operators/reports", dependencies=[Depends(require_auth)])
    async def operator_reports() -> dict[str, Any]:
        """What every installed operator DECLARES it reports (ADR-0103 D1).

        The console's Activity page and Metrics tab render from this and
        nothing else (ADR-0102 D7): the stages a step moves through, the
        time-series it charts with their units, and the final scalars. Read
        off the CLASS, which is where the declaration lives (the manifest row
        is records-only, the same split as ``risk``, #1539).

        Deliberately its own route rather than a field on ``/builder/catalog``:
        a run page needs the declaration for three operators, and the catalog
        pays for live MCP discovery across the caller's connections. An
        operator declaring ``ReportSpec.NONE`` is present with empty lists, so
        the console can tell "reports nothing" from "not installed".

        Lives here, NOT on the builder router: that router is mounted behind
        ``require_feature("builder")`` and a workspace with the builder off
        (prod, 2026-09-06) answered 404 — the run and pipeline pages are core
        surfaces and must never depend on an authoring feature flag.
        """
        from cairn.core.operator.loader import ensure_loaded  # noqa: PLC0415 - cycle
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415 - cycle
        from cairn.core.reporting import ReportSpec  # noqa: PLC0415 - kept beside the registry import

        ensure_loaded(strict=False)
        out: dict[str, Any] = {}
        for name, cls in sorted(default_registry.all().items()):
            spec = getattr(cls, "reports", None)
            if not isinstance(spec, ReportSpec):
                # an operator predating the contract (the shrinking backfill
                # set) reports nothing rather than breaking the whole page.
                spec = ReportSpec.NONE
            out[name] = {
                "stages": list(spec.stages),
                "series": [{"name": s.name, "unit": s.unit} for s in spec.series],
                "scalars": list(spec.scalars),
            }
        return {"count": len(out), "operators": out}

    # ── #2030 (ADR-0102 D2 tier three): proposals — the clone a human approves ──

    @r.get("/runs/{run_id}/control", dependencies=[Depends(require_auth)])
    async def get_run_control(run_id: str, after_seq: int = 0, wait_s: float = 0.0) -> dict[str, Any]:
        """The newest control request (``abort`` / ``checkpoint-and-stop``) with
        store ``seq > after_seq``; ``wait_s`` (≤ 25) long-polls for one. A
        worker asks with the seq it last saw and obeys what comes back."""
        from cairn.core.operator.report_ingest import wait_control  # noqa: PLC0415 — same

        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        found = await wait_control(dispatcher.store, run_id, after_seq=max(0, int(after_seq)),
                                   timeout_s=max(0.0, min(25.0, float(wait_s))))
        return {"run_id": run_id, "control": found}

    @r.post("/runs/{run_id}/control", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def request_run_control(run_id: str, body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        """Ask the run's live worker to ``abort`` or ``checkpoint-and-stop``
        (save a checkpoint at the current step, then stop). Recorded as a
        ``control`` run event — durable, readable over either job channel."""
        from cairn.core.operator.report_ingest import CONTROL_ACTIONS, record_control  # noqa: PLC0415 — same

        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        action = str(body.get("action") or "")
        if action not in CONTROL_ACTIONS:
            raise HTTPException(status_code=422, detail=f"action must be one of {', '.join(CONTROL_ACTIONS)}")
        rec = await record_control(dispatcher.store, run_id, action, node=str(body.get("node") or ""),
                                   reason=str(body.get("reason") or ""), by=str(body.get("by") or "api"))
        # a NATS-channel worker reads control off the channel, not the store
        try:
            from cairn.core.operator.job_channel import StoreJobChannel, get_job_channel  # noqa: PLC0415 — same
            chan = get_job_channel()
            if not isinstance(chan, StoreJobChannel):
                await chan.publish_control(run_id, {"action": action, "node": rec.get("node", "")})
        except Exception as exc:  # noqa: BLE001 — the store carries the request; the channel is best-effort
            logger.debug("control publish on the job channel failed for %s: %s", run_id, exc)
        return {"run_id": run_id, **rec}

    @r.get("/runs/{run_id}/pending-approval")
    async def get_pending_approval(run_id: str) -> dict[str, Any]:
        """What a run paused at an operation gate is asking a human to approve
        (ADR-0034, an internal issue): the ApprovalRequest payload (operator,
        risk, sources, reason, allowed_decisions). Lets `remote approve --watch`
        and a future UI show WHAT is being approved, not just `awaiting_approval`.
        """
        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        result = await dispatcher.pending_approval(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="run not found")
        return result

    @r.get("/runs/{run_id}/stream")
    async def stream_run_events(
        run_id: str,
        # Optional, NOT `= 0`: an explicit `?after_seq=0` means "replay
        # everything", and with an int default that is indistinguishable from
        # the parameter being absent — so the header would silently win over a
        # caller that asked for the full backlog.
        after_seq: Optional[int] = None,
        last_event_id: Annotated[Optional[str], Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        """Server-Sent Events stream of a run's partial outputs.

        Replays the backlog (so late subscribers and finished runs still get
        the full evidence trail), then tails live events until the run reaches
        a terminal state. Resume a dropped stream with ``?after_seq=N``.

        #1574 — a dropped stream ALSO resumes automatically. Each frame carries
        ``id: <seq>``, so a browser's EventSource replays that id back in the
        ``Last-Event-ID`` header when it reconnects on its own. Honouring it is
        the resume mechanism SSE already defines; ignoring it meant every
        reconnect replayed the whole backlog and the console's event trail — the
        run's evidence — grew a duplicate copy of itself each time.

        An explicit ``?after_seq=`` still wins: a caller that names a cursor
        means it, and EventSource cannot set the query string on its own retry.
        """
        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        cursor = after_seq
        if cursor is None and last_event_id:
            # A malformed header is not worth a 400 — it costs a backlog replay,
            # which is exactly the pre-#1574 behaviour, so fall back rather than
            # break a reconnect.
            try:
                cursor = max(0, int(last_event_id))
            except (TypeError, ValueError):
                cursor = 0

        async def _gen():
            async for ev in dispatcher.subscribe(run_id, after_seq=cursor or 0):
                # SSE frame: id + event-type + JSON data line.
                yield (
                    f"id: {ev.seq}\n"
                    f"event: {ev.type}\n"
                    f"data: {ev.model_dump_json()}\n\n"
                )

        return StreamingResponse(_gen(), media_type="text/event-stream")

    @r.post("/runs/{run_id}/cancel", dependencies=[Depends(support.writable_workspace)])
    async def cancel_run(run_id: str) -> dict[str, Any]:
        await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        inv = await dispatcher.cancel(run_id)
        if inv is None:
            raise HTTPException(status_code=404, detail="investigation not found")
        return inv.model_dump(mode="json")

    # ── #896 HITL approval lifecycle: expire one / bulk-dismiss stale gates ──
    @r.post("/runs/{run_id}/expire", dependencies=[Depends(support.writable_workspace)])
    async def expire_run_endpoint(run_id: str) -> dict[str, Any]:
        """Dismiss ONE stale HITL gate: transition an ``awaiting_approval`` run →
        ``expired`` (audited). 409 if the run isn't awaiting approval — you can't
        expire a decided/terminal run, mirroring the resume terminal-state guard
        (#618)."""
        from cairn.core.approval.reaper import expire_run

        inv = await _run_in_ws_or_404(run_id)  # workspace guard (#228 B6)
        if inv.status != "awaiting_approval":
            raise HTTPException(
                status_code=409,
                detail=f"run is '{inv.status}', not awaiting approval — cannot expire",
            )
        await expire_run(
            dispatcher.store, inv, now=datetime.now(timezone.utc),
            reason=f"approval dismissed via /runs/{run_id}/expire (#896)",
        )
        updated = await dispatcher.get(run_id)
        return (updated or inv).model_dump(mode="json")

    @r.post("/approvals/dismiss", dependencies=[Depends(support.writable_workspace)])
    async def dismiss_approvals_endpoint(body: DismissApprovals) -> dict[str, Any]:
        """Bulk-dismiss stale HITL gates (the console action). ``all_stale`` sweeps
        every awaiting_approval run past the governance TTL now; otherwise the named
        ``run_ids`` are expired. Workspace-scoped + fail-closed: only awaiting_approval
        runs in the caller's workspace are touched, each audited."""
        from cairn.core.approval.reaper import dismiss_runs, expire_stale_approvals, ttl_hours_from_env

        ws = current_workspace()
        if body.all_stale:
            expired = await expire_stale_approvals(
                dispatcher.store, ttl_hours=ttl_hours_from_env(), workspace_id=ws,
            )
            return {"expired": expired, "skipped": [], "count": len(expired)}
        result = await dismiss_runs(
            dispatcher.store, body.run_ids, actor=body.actor, workspace_id=ws,
        )
        return {**result, "count": len(result["expired"])}

    return r
