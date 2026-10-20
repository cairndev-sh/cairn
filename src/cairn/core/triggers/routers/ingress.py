"""Legacy webhook ingress (/triggers/alert|pattern|manual), the action-saga
HITL approval callback (ADR-0024 RM3), and the durable trigger inbox.

Extracted verbatim from server.py (#1191). Two factories because the
spec-declared trigger routes (§24.2) mount BETWEEN these surfaces in
server.py — route order is part of the committed OpenAPI contract.
"""
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.engine.schema import ApprovalRecord, TriggerEvent
from cairn.core.triggers.auth import _oauth2_scheme
from cairn.core.triggers.routers.reviews import decision_actor

_APPROVE = ("approve", "approved", "yes", "y")
_REJECT = ("reject", "rejected", "no", "n")


class SagaApprovalRequest(BaseModel):
    """Body of ``POST /triggers/approve/{workflow_id}`` (#63). ``decision`` is
    REQUIRED and must say approve or reject: no default, so a request that omits
    it is a 422, never a silent approve (the #628 rule ``ResumeRequest`` keeps)."""
    decision: str = Field(..., description="'approve' | 'reject' (or approved/yes/y, rejected/no/n)")
    approver: str = Field("slack-user", description="accepted for old clients; under auth the caller is recorded")
    note: Optional[str] = Field(None, description="free-text note recorded on the approval")


def build_ingress_router(dispatcher, environment, runner) -> APIRouter:
    """The pre-shaped webhook doors + the saga approval callback."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # request models + module helpers the inline block shared.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        AlertWebhook,
        ManualWebhook,
        PatternWebhook,
        _dispatch,
        _req_id,
        _summarize_investigation,
        logger,
    )

    r = APIRouter()

    @r.post("/triggers/alert")
    async def alert_endpoint(body: AlertWebhook, request: Request) -> dict[str, Any]:
        event = TriggerEvent(
            source=body.source,
            matched_pattern=body.alert_name,
            service=body.service,
            payload={
                **body.payload,
                "alert_name": body.alert_name,
                "severity": body.severity,
                "message": body.message,
            },
        )
        return await _dispatch(dispatcher, event, environment, request_id=_req_id(request))

    @r.post("/triggers/pattern")
    async def pattern_endpoint(body: PatternWebhook, request: Request) -> dict[str, Any]:
        event = TriggerEvent(
            source="pattern",
            matched_pattern=body.matched_pattern,
            service=body.service,
            payload={
                **body.payload,
                "message": body.message,
                "count": body.count,
                "first_seen": body.first_seen,
            },
        )
        return await _dispatch(dispatcher, event, environment, request_id=_req_id(request))

    @r.post("/triggers/manual")
    async def manual_endpoint(body: ManualWebhook, request: Request) -> dict[str, Any]:
        event = TriggerEvent(
            source="manual",
            matched_pattern=body.skill_name,
            service=body.service,
            payload={
                **body.payload,
                "note": body.note,
            },
        )
        if body.wait:
            return await _dispatch(dispatcher, event, environment, request_id=_req_id(request))
        # Async submit (issue #2): accept on the background lane and return the
        # handle immediately so the caller can follow /runs/{id}/stream. Long
        # jobs (e.g. training) must not hold the POST open for their duration.
        inv = await dispatcher.submit(
            event, lane="background", environment=environment, wait=False,
        )
        return _summarize_investigation(inv, request_id=_req_id(request))

    # ── HITL approval callback (ADR-0024 RM3) ────────────────────────
    #
    # Slack/Email/etc. approval channels post URLs that point back here.
    # The human's click resolves a saga that paused at the durable
    # ``approval`` node: we build an ApprovalRecord and resume the run via
    # the durable handler's ``Command(resume=record)`` path. Requires the
    # Runner to be wired with a DurableActionHandler (one that exposes
    # ``resume``); otherwise the run had nothing durable to pause on.
    #
    # #2358 — WHO decided follows the one rule every gate door uses (#2310,
    # ``decision_actor``): under auth the bearer guard has already demanded a
    # token, and the recorded approver is that caller; the ``approver`` query
    # parameter is accepted for old links but ignored (a mismatch is logged).
    # A Slack link click carries no Slack user id, so none is recorded; the
    # record's ``channel="slack"`` is the "via Slack" marker.
    #
    # #63 — the decision is a POST with an explicit ``decision`` in the body (no
    # default, the #628 rule). The GET used to read it from the query string and
    # default to approve, so a logged, prefetched or replayed URL approved; it now
    # answers how to decide and never resumes anything.
    @r.get("/triggers/approve/{workflow_id}")
    async def approve_confirm(workflow_id: str) -> dict[str, Any]:
        """Never decides (#63): says how to approve or reject this saga."""
        return {
            "workflow_id": workflow_id,
            "decided": False,
            "detail": "a GET never decides an approval: POST /triggers/approve/{workflow_id} "
                      'with {"decision": "approve"} or {"decision": "reject"}',
            "decide": {"method": "POST", "path": f"/triggers/approve/{workflow_id}",
                       "body": {"decision": "approve | reject"}},
        }

    @r.post("/triggers/approve/{workflow_id}")
    async def approve_endpoint(
        workflow_id: str,
        body: SagaApprovalRequest,
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        decision = body.decision.strip().lower()
        if decision in _APPROVE:
            approved = True
        elif decision in _REJECT:
            approved = False
        else:
            raise HTTPException(status_code=422, detail="decision must be 'approve' or 'reject'")
        note = f"resolved via POST /triggers/approve (decision={decision})"
        if body.note:
            note = f"{note}: {body.note}"
        record = ApprovalRecord(
            approved=approved,
            approver=await decision_actor(token, body.approver, fallback="slack-user"),
            channel="slack",
            timestamp_iso=utc_now_iso(),
            note=note,
        )
        handler = getattr(runner, "action_handler", None)
        resume = getattr(handler, "resume", None)
        if resume is None:
            raise HTTPException(
                status_code=501,
                detail=(
                    "no durable action handler is wired — set CAIRN_ACTION_HANDLER "
                    "to a durable handler so approvals can pause/resume (ADR-0024 RM3)"
                ),
            )
        try:
            outcome = await resume(workflow_id, record)
        except Exception as exc:  # noqa: BLE001
            logger.warning("approval resume failed for %s: %s", workflow_id, exc)
            raise HTTPException(status_code=502, detail=f"resume failed: {exc}")
        return {
            "workflow_id": workflow_id,
            "decision": decision,
            "approved": approved,
            "status": outcome.status,
            "note": "decision resumed the paused saga",
        }

    return r


def build_inbox_router(trigger_router) -> APIRouter:
    """Durable trigger inbox (opt-in CAIRN_TRIGGER_DURABLE): inspect the queue +
    replay past inbound triggers from a point in time. 409 when not enabled.
    ``trigger_router`` is the TriggerRouter built at app construction (or None
    when spec routes are disabled / failed to mount)."""
    r = APIRouter()

    @r.get("/triggers/inbox")
    async def list_inbox(status: Optional[str] = None, limit: int = 100) -> dict[str, Any]:
        inbox = getattr(trigger_router, "inbox", None) if trigger_router is not None else None
        if inbox is None:
            return {"durable": False, "records": []}
        recs = await inbox.list(status=status, limit=limit)
        return {"durable": True, "records": [r.model_dump(mode="json") for r in recs]}

    @r.post("/triggers/inbox/replay")
    async def replay_inbox(since: str) -> dict[str, Any]:
        inbox = getattr(trigger_router, "inbox", None) if trigger_router is not None else None
        if inbox is None:
            raise HTTPException(
                status_code=409,
                detail="durable trigger inbox is not enabled (set CAIRN_TRIGGER_DURABLE=1)",
            )
        from cairn.core.storage.trigger_inbox import replay_since
        n = await replay_since(inbox, since=since)
        return {"replayed": n, "since": since}

    return r
