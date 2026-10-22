"""The pull lane's two routes (#565, ADR-0104 D2).

``POST /jobs/claim`` and ``POST /jobs/{id}/heartbeat`` are the whole inbound
surface a remote machine needs. Everything else it does — streaming its curve,
obeying an abort — goes over the routes #2025 already added
(``/runs/{id}/events/ingest`` up, ``/runs/{id}/control`` down), which is why
this router is small: D2 adds a claim, not a protocol.

Auth is the same bearer as the rest of the control plane, with ONE scope rule
on top: the **per-run worker token** ``live_channel_env_for`` mints for a
container (``sub = worker:<run_id>``, ``scope = run-ingest``) exists to report
that run. It may heartbeat that run's job, and nothing else — it cannot claim,
because a claim hands out ANY queued job and a token scoped to run X must never
be able to take run Y's work.

cairn-platform#224 — and a claim is a WORKER's act, not a user's. A claim hands
out any account's queued job with its input and a run-scoped token for its run,
so only a worker credential claims, or heartbeats a job that is not a run-scoped
token's own: the deployment's admin (``CAIRN_API_USER``) or a worker-pool token
(``scope=worker-pool``, ``cairn remote worker-token``; the auth guard limits it
to these two routes). A user's token, of any account and any role, gets 403.
Auth off (keyless dev) is unchanged: there is no tenant to protect.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from cairn.core.operator.job_store import DEFAULT_LEASE_S, RemoteJobRecord, get_remote_job_store
from cairn.core.operator.jobs import JobStatus, live_channel_env_for
from cairn.core.triggers.auth import (
    auth_enabled,
    authenticated_claims,
    is_env_admin,
    is_worker_pool_token,
    require_auth,
    token_run_scope,
)

logger = logging.getLogger("cairn.triggers.jobs")

#: Bounds on what a worker may ask for, so one cannot park a job forever.
MIN_LEASE_S, MAX_LEASE_S = 30, 3600


class ClaimRequest(BaseModel):
    """Who is asking, and what it can run."""

    worker_id: str = Field(..., min_length=1, max_length=200)
    capabilities: dict[str, str] = Field(default_factory=dict)
    lease_seconds: int = int(DEFAULT_LEASE_S)


class ClaimedJobResponse(BaseModel):
    """One job with a lease: what to run, on what input, reporting where."""

    id: str
    run_id: str
    node_name: str
    provider: str
    input: dict[str, Any] = Field(default_factory=dict)
    image: str = ""
    #: The live-channel env (``CAIRN_RUN_ID``/``CAIRN_NODE`` + one transport) the
    #: worker passes into the container — the same variables a pushed job gets.
    env: dict[str, str] = Field(default_factory=dict)
    lease_until: str = ""
    attempts: int = 0


class HeartbeatRequest(BaseModel):
    """I still hold this job, and here is where it is."""

    worker_id: str = Field(..., min_length=1, max_length=200)
    status: str = "running"
    error: str = ""
    metrics: dict[str, Any] = Field(default_factory=dict)
    output: Optional[dict[str, Any]] = None
    lease_seconds: int = int(DEFAULT_LEASE_S)


class HeartbeatResponse(BaseModel):
    job_id: str
    accepted: bool
    status: str = ""
    lease_until: str = ""


_STATES = ("queued", "running", "succeeded", "failed")


def _clamp_lease(seconds: int) -> int:
    return max(MIN_LEASE_S, min(MAX_LEASE_S, int(seconds or DEFAULT_LEASE_S)))


def _scoped_run(request: Request, subject: Optional[str]) -> Optional[str]:
    """The run this caller's token is scoped to, or ``None`` for an operator
    credential (or keyless dev, where ``subject`` is None).

    #208 — the guard's own rule (:func:`token_run_scope`) on the claims the guard
    authenticated, so a token the guard treats as run-scoped (``scope=run-ingest``
    or a ``worker:<run>`` subject) is run-scoped here too. ``""`` (a token whose
    scope and subject disagree) matches no run. Without guard claims and with auth
    off (keyless dev) the subject alone decides, as before; with auth ON a caller the
    guard did not vet is run-scoped to no run (fail closed)."""
    claims = authenticated_claims(request.scope)
    if claims is None:
        if auth_enabled() and subject:
            return ""
        claims = {"sub": subject} if subject else {}
    return token_run_scope(claims)


_NOT_A_WORKER = ("claiming or reporting pull-lane jobs needs a worker credential: the "
                 "deployment admin's token or a worker-pool token (cairn remote worker-token)")


def _is_worker_credential(request: Request, subject: Optional[str]) -> bool:
    """Whether this caller may claim, and heartbeat any job (cairn-platform#224):
    a worker-pool token or the deployment's admin. Auth off (keyless dev) is
    always a worker. With auth on, only claims the guard verified count."""
    if not auth_enabled():
        return True
    claims = authenticated_claims(request.scope)
    if claims is None:
        return False
    return is_worker_pool_token(claims) or is_env_admin(subject)


def _as_response(rec: RemoteJobRecord) -> ClaimedJobResponse:
    return ClaimedJobResponse(
        id=rec.id, run_id=rec.run_id, node_name=rec.node_name, provider=rec.provider,
        input=dict(rec.input), image=rec.image or str(rec.job_meta.get("image") or ""),
        env=live_channel_env_for(rec.run_id, rec.node_name),
        lease_until=rec.lease_until or "", attempts=rec.attempts,
    )


def build_jobs_router(store_factory=None) -> APIRouter:
    """The claim surface. ``store_factory`` defaults to the process store,
    resolved per request (never bound at build time) so a test — and the
    server's own reconfiguration — can point it somewhere else."""
    r = APIRouter()

    def _store():
        return (store_factory or get_remote_job_store)()

    @r.post("/jobs/claim", response_model=Optional[ClaimedJobResponse], responses={204: {"description": "nothing to claim"}})
    async def claim_job(body: ClaimRequest, request: Request,
                        subject: Annotated[Optional[str], Depends(require_auth)]) -> Any:
        """Hand this worker at most ONE queued job, with a lease. 204 when there
        is nothing it can run — an empty queue is not an error, and a worker
        polls this route forever."""
        # cairn-platform#224: only a worker credential claims (403 otherwise). A
        # comment, not the docstring: the docstring is the OpenAPI text.
        if _scoped_run(request, subject) is not None:
            raise HTTPException(
                status_code=403,
                detail="a per-run worker token ingests that run's events; claiming work "
                       "needs a worker credential (cairn worker --pull --token <worker-pool token>)")
        if not _is_worker_credential(request, subject):
            raise HTTPException(status_code=403, detail=_NOT_A_WORKER)
        store = _store()
        rec = await store.claim_next(body.worker_id, capabilities=body.capabilities,
                                     lease_s=_clamp_lease(body.lease_seconds))
        if rec is None:
            return Response(status_code=204)
        logger.info("job %s claimed by %s (lease until %s)", rec.id, body.worker_id, rec.lease_until)
        return _as_response(rec)

    @r.post("/jobs/{job_id}/heartbeat", response_model=HeartbeatResponse)
    async def heartbeat_job(job_id: str, body: HeartbeatRequest, request: Request,
                            subject: Annotated[Optional[str], Depends(require_auth)]) -> HeartbeatResponse:
        """Extend the lease and record the worker's ``JobStatus``. Refused (200
        with ``accepted: false``) when this worker no longer holds the job — the
        honest answer to a worker whose lease expired while it was working, and
        the signal for it to stop. A per-run token may only heartbeat its own
        run's job (403 otherwise)."""
        # cairn-platform#224: any other caller needs a worker credential (403).
        if body.status not in _STATES:
            raise HTTPException(status_code=422, detail=f"status must be one of {', '.join(_STATES)}")
        store = _store()
        scoped = _scoped_run(request, subject)
        if scoped is not None:
            owner = await store.get(job_id)
            if not scoped or owner is None or owner.run_id != scoped:
                raise HTTPException(status_code=403,
                                    detail=f"this token is scoped to run {scoped}; it cannot heartbeat {job_id}")
        elif not _is_worker_credential(request, subject):
            raise HTTPException(status_code=403, detail=_NOT_A_WORKER)
        ok = await store.heartbeat(
            job_id, body.worker_id,
            JobStatus(state=body.status, output=body.output, error=body.error, metrics=body.metrics),
            lease_s=_clamp_lease(body.lease_seconds),
        )
        rec = await store.get(job_id)
        return HeartbeatResponse(job_id=job_id, accepted=ok,
                                 status=(rec.status if rec else ""),
                                 lease_until=(rec.lease_until or "") if rec else "")

    return r


__all__ = ["ClaimRequest", "ClaimedJobResponse", "HeartbeatRequest", "HeartbeatResponse",
           "build_jobs_router"]
