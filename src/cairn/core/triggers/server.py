"""
Webhook trigger server — turns external HTTP events into ``TriggerEvent``s
and fires the Runner.

Routes:
    POST /triggers/alert    — generic alert webhook (PagerDuty, Alertmanager, ...)
    POST /triggers/pattern  — pattern-matched signal (caller has already done the matching)
    POST /triggers/manual   — manual invocation (CLI / UI / Slack slash command)
    GET  /healthz           — liveness
    GET  /triggers/skills   — list currently loaded skills (debug)

This module exposes ``build_app(runner)`` so callers can mount the FastAPI
app into an existing process or run it standalone via
``scripts/trigger_server.py``.

JWT auth (an internal issue) is mounted via ``auth.install_auth`` —
enforced when ``CAIRN_JWT_SECRET`` is set, open (with a warning) otherwise.
Signature verification and rate limiting remain out of scope here.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json
import logging
import os
import re
from typing import Any, Optional

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.storage import build_storage, get_storage
from cairn.core.engine.runner import Runner
from cairn.core.engine.schema import TriggerEvent
from cairn.core.dispatch.event_triggers import EventTriggerBridge
from cairn.core.dispatch.scheduler import SkillScheduler
from cairn.core.leadership import Leadership  # #1533 — fleet-singleton election
from cairn.core import liveness  # ADR-0125 — the in-process leg owner lock
from cairn.core.triggers.auth import (
    _is_public_path,
    _oauth2_scheme,
    authenticated_claims,
    caller_identity,
    install_auth,
    register_public_trigger_path,
    require_admin,
    require_auth,
    password_hash_problem,
    auth_enabled,
)
from cairn.core.triggers.router import TriggerRouter
from cairn.core.workspace import (
    WorkspaceForbidden,
    assert_workspace_allowed,
    current_workspace,
    reset_current_workspace,
    set_current_workspace,
)


logger = logging.getLogger("cairn.trigger_server")


# ─── Run-status vocabulary normalization (#651) ──────────────────────────
# cairn's canonical Run statuses (storage.protocols.Status) are queued / running
# / awaiting_approval / done / failed / cancelled. Callers — especially the
# `cairn ask` agent — routinely guess common synonyms ("completed", "success",
# "error", "in_progress"). Unmapped, a `status=completed` filter matched 0 rows
# even when runs were "done", so the agent wrongly answered "no runs". Map the
# usual synonyms to the canonical value; pass anything else (incl. a real
# canonical value or an unknown one) through unchanged.
_STATUS_SYNONYMS: dict[str, str] = {
    "completed": "done", "complete": "done", "success": "done",
    "succeeded": "done", "finished": "done", "ok": "done", "passed": "done",
    "failure": "failed", "errored": "failed", "error": "failed", "faulted": "failed",
    "canceled": "cancelled", "aborted": "cancelled", "stopped": "cancelled",
    "killed": "cancelled",
    "in_progress": "running", "in-progress": "running", "active": "running",
    "executing": "running", "started": "running",
    "pending": "queued", "waiting": "queued", "scheduled": "queued",
    "enqueued": "queued",
    "awaiting-approval": "awaiting_approval", "blocked": "awaiting_approval",
    "paused": "awaiting_approval",
}


def _normalize_run_status(status: Optional[str]) -> Optional[str]:
    """Map a caller-supplied status synonym to cairn's canonical vocabulary
    (#651). ``None``/canonical/unknown values pass through unchanged."""
    if not status:
        return status
    return _STATUS_SYNONYMS.get(status.strip().lower(), status)


# ─── Run results / artifacts (#179 console "Results" surface) ────────
# A finished run's outputs live in two places: its structured result
# (`Run.result_payloads`, the downloadable "report") and the artifact URIs that
# result references (model adapters, datasets, files). These helpers extract
# both for GET /runs/{id}/artifacts; nothing new is stored.

#: URI schemes we recognize as a produced artifact reference.
_ARTIFACT_SCHEMES = ("file://", "s3://", "gs://", "hf://", "https://", "http://", "artifact://")
#: result keys whose string values are artifact references worth surfacing.
_ARTIFACT_KEYS = ("uri", "adapter_uri", "model_uri", "artifact_uri", "path", "url", "output_uri")


def _run_template_ref(run) -> str:
    """The workflow a Run executed — for the Results "workflow" column.

    Delegates to the ONE canonical resolver (#1096): the ``Run.template_ref``
    recorded by the dispatcher, with the pre-#1096 inference chain as the legacy
    fallback. This module used to carry its own copy of that chain.
    """
    from cairn.core.eval.scoring import run_template_ref  # noqa: PLC0415 - cycle: triggers → eval

    return run_template_ref(run)


def _langfuse_trace_url(trace_id: str) -> Optional[str]:
    """``{langfuse_base}/project/{id}/traces/{trace_id}`` deep-link (§D5,
    #248), mirroring ``core/console/api.py``'s ``ConsoleConfig`` build.
    ``None`` when tracing is off or Langfuse isn't configured — the console
    hides the "Open trace in Langfuse" link in that case."""
    if not trace_id:
        return None
    from cairn.core.provider_settings import get_settings

    s = get_settings()
    base = (s.langfuse_base or "").rstrip("/")
    if not base:
        return None
    project = (s.langfuse_project_id or "").strip()
    if project:
        return f"{base}/project/{project}/traces/{trace_id}"
    return f"{base}/trace/{trace_id}"


async def _run_failure_types(ledger: Any, run: Any) -> list[str]:
    """The failure-type tags for a run — the eval ledger's ``failure_labels``
    (#474) is the source of truth, since the diagnosis write path
    (``label_run_failures``) persists there and ``Run.failure_type`` is not
    populated by the current dispatcher path. Falls back to ``Run.failure_type``
    if it IS set (forward-compat: a future writer / a migrated row), unioned
    so neither source is lost. Deduped, order-stable."""
    labels = await ledger.get_failure_labels(run.id)
    types_ = [l.failure_type for l in labels]
    for t in (run.failure_type or []):
        if t not in types_:
            types_.append(t)
    return types_


def _artifact_kind(key: str, uri: str) -> str:
    """Best-effort classification of an artifact reference for the UI icon —
    by the result key it sat under and by the URI's file extension."""
    k = (key or "").lower()
    u = uri.lower()
    if "adapter" in k or "model" in k or u.endswith((".safetensors", ".bin", ".gguf", ".pt", ".ckpt")):
        return "model"
    if "dataset" in k or "data" in k or u.endswith((".jsonl", ".csv", ".parquet", ".arrow")):
        return "dataset"
    if "report" in k or u.endswith((".md", ".html", ".pdf", ".json", ".txt")):
        return "report"
    return "file"


def _harvest_artifacts(payloads: list[dict]) -> list[dict[str, Any]]:
    """Walk a run's structured result for artifact URIs (dedup, order-preserving).

    Surfaces any string that is an artifact-scheme URI, or any value under a
    known artifact key. Each entry: {uri, kind, label, downloadable}. Only local
    ``file://`` paths under the artifact root are marked downloadable; remote
    refs (hf/s3/https) are external links, never proxied through the engine."""
    seen: dict[str, dict[str, Any]] = {}

    def add(uri: str, key: str) -> None:
        if uri and uri not in seen:
            seen[uri] = {
                "uri": uri,
                "kind": _artifact_kind(key, uri),
                "label": key or "output",
                # downloadable only via the artifact-store proxy (local files);
                # decided by the route, not here (kept False to be safe).
                "downloadable": False,
            }

    def walk(obj: Any, keyhint: str = "") -> None:
        if isinstance(obj, str):
            if obj.startswith(_ARTIFACT_SCHEMES):
                add(obj, keyhint)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, str) and k in _ARTIFACT_KEYS and not v.startswith(_ARTIFACT_SCHEMES):
                    add(v, k)  # a bare path/uri under a known key
                walk(v, k)
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                walk(v, keyhint)

    for p in payloads or []:
        walk(p)
    return list(seen.values())


def _extract_metric_value(payloads: list[dict], metric: str) -> Optional[float]:
    """Best-effort: pull a numeric ``metric`` out of a run's structured result
    (``result_payloads[*].output``) for the Playground gate what-if history
    (#837). Matches the key case-insensitively and parses ``"0.4%"`` → 0.004.
    Returns None when the metric isn't present as a number."""
    target = metric.strip().lower()

    def _num(v: Any) -> Optional[float]:
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            m = re.search(r"-?\d+(\.\d+)?", v)
            if m:
                return float(m.group(0)) / (100.0 if "%" in v else 1.0)
        return None

    found: list[float] = []

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if str(k).strip().lower() == target:
                    n = _num(v)
                    if n is not None:
                        found.append(n)
                walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    for p in payloads or []:
        walk(p)
    return found[0] if found else None


def _s3_presigned_url(uri: str) -> str:
    """Presign a short-lived (~5 min) GET for an ``s3://bucket/key`` artifact ref
    (#353 download endpoint). The engine never proxies the bytes — it hands the
    browser a redirect. Raises 501 when the optional boto3 dependency or AWS
    credentials are absent, so the console can surface an honest message."""
    _NEEDS = "s3 download needs boto3 + AWS credentials on the engine"
    try:
        import boto3  # optional dependency — only needed to presign s3:// artifact downloads
    except ImportError:
        raise HTTPException(status_code=501, detail=_NEEDS)
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise HTTPException(status_code=404, detail="malformed s3 artifact reference")
    try:
        session = boto3.session.Session()
        if session.get_credentials() is None:  # presigning is offline; creds must exist up front
            raise HTTPException(status_code=501, detail=_NEEDS)
        return session.client("s3").generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
        )
    except HTTPException:
        raise
    except Exception as exc:  # region/config errors — same honest 501, with the cause
        raise HTTPException(status_code=501, detail=f"{_NEEDS} ({exc})")


# ─── Request bodies ──────────────────────────────────────────────────


class AlertWebhook(BaseModel):
    """Payload for /triggers/alert.

    Maps loosely from PagerDuty / Alertmanager / GitHub webhook shapes —
    callers normalize on their side, we accept the lowest common denominator.
    """
    source: str = Field(..., description="e.g. 'pagerduty', 'alertmanager', 'github'")
    service: Optional[str] = None
    alert_name: Optional[str] = None
    severity: Optional[str] = None
    message: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class PatternWebhook(BaseModel):
    """Payload for /triggers/pattern — used when an external signal-extractor
    (HyperDX, Datadog monitors, Drain3 service) has already classified a
    log/event as matching a known pattern and wants cairn to investigate.
    """
    matched_pattern: str
    service: str
    message: Optional[str] = None
    count: int = 1
    first_seen: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ManualWebhook(BaseModel):
    """Payload for /triggers/manual — a human or CLI fires the skill by name."""
    skill_name: Optional[str] = None
    service: Optional[str] = None
    note: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    #: Block until the run finishes (realtime lane, default) vs. accept it on
    #: the background lane and return a run_id immediately so the caller can
    #: stream ``/runs/{id}/stream``. The CLI's ``remote submit`` sends
    #: ``wait=False`` so long jobs (e.g. training) don't hold the HTTP
    #: connection open for hours (an internal issue).
    wait: bool = True


class InstallRequest(BaseModel):
    """Payload for POST /registry/install — a registry pack/bundle ref to install."""
    ref: str = Field(..., description="catalog ref or install name, e.g. 'obs-operator-s3'")


class WorkspaceCreate(BaseModel):
    """Payload for POST /workspaces (#228). ``id`` is optional — derived as a
    slug from ``name`` when omitted. The id assigned is at most
    ``MAX_WORKSPACE_ID_LEN`` (40) characters: a longer slug is cut."""
    name: str = Field(..., min_length=1)
    id: Optional[str] = None




class PipelineApproveRequest(BaseModel):
    """Payload for POST /pipelines/{id}/steps/{step}/approve — the approver identity."""
    approver: Optional[str] = Field(None, description="Who approved (governance trail).")
    rationale: Optional[str] = Field(None, description="Why — recorded on the decision's review record (#1468).")


class PipelineRejectRequest(BaseModel):
    """Payload for POST /pipelines/{id}/steps/{step}/reject (ADR-0156 D4)."""
    reason: str = Field(..., min_length=1, description="Why the gate is rejected — required; recorded on the step, the review and the lifecycle event.")
    approver: Optional[str] = Field(None, description="Who rejected (recorded when auth is off; under auth the token's identity is recorded and this is ignored).")


class WorkspaceUpdate(BaseModel):
    """Payload for PATCH /workspaces/{id} (#258) — lifecycle status change."""
    status: str = Field(..., description="'active' (restore) or 'archived'")


class WorkspaceVariableWrite(BaseModel):
    """Payload for PUT /workspaces/{id}/variables/{key} (ADR-0101 D2, #1974).

    ``value`` is arbitrary JSON and may legitimately be ``null``, ``0``, ``false``
    or ``""`` — so it is REQUIRED rather than defaulted: an omitted value would
    otherwise silently write a null over whatever the key holds."""
    value: Any = Field(..., description="the value, any JSON shape")
    description: str = Field("", description="what this variable is for (shown in Settings)")


class UserCreate(BaseModel):
    """Payload for POST /admin/users (ADR-0054 §3 — admin-invite only). Creates a
    user with a password and assigns them to an account/role. ``account_id``
    defaults to ``default`` (OSS single-tenant); ``role`` ∈ {owner, admin, member}."""
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)
    account_id: str = Field("default", description="account to assign the user to")
    role: str = Field("member", description="'owner' | 'admin' | 'member'")


class PasswordReset(BaseModel):
    """Payload for POST /admin/users/{id}/password — admin resets a user's password."""
    password: str = Field(..., min_length=1)


class DismissApprovals(BaseModel):
    """Payload for POST /approvals/dismiss (#896) — bulk-dismiss stale HITL gates.

    Either name the ``run_ids`` to expire, or set ``all_stale`` to sweep every
    awaiting_approval run past the governance TTL now. A dismiss can only expire a
    run that IS awaiting approval (a resolved run is skipped) and only within the
    caller's workspace — it never touches a decided gate or another tenant's run."""
    run_ids: list[str] = Field(default_factory=list, description="Specific gates to dismiss.")
    all_stale: bool = Field(False, description="Sweep every gate past the TTL now (ignores run_ids).")
    actor: str = Field("console", description="Who dismissed (recorded in the audit event).")


class PilotFinding(BaseModel):
    """One reviewer finding on a run (severity + free text)."""
    severity: str = Field("low", description="'low' | 'high' (advisory — no enum enforced)")
    text: str = Field(..., min_length=1)


class PilotReviewRequest(BaseModel):
    """Payload for POST /runs/{id}/pilot/review (#281) — a single reviewer's
    ratings for the 2 human scorecard dimensions, plus optional notes/findings.
    The range check (1..5) is enforced in ``apply_review`` so it surfaces as a
    422 with a precise message. Reviewer identity comes from the authenticated
    actor, never the body."""
    output_usefulness: int = Field(..., description="1..5 — how usable the output was")
    setup_experience: int = Field(..., description="1..5 — how smooth setup/onboarding was")
    notes: str = Field("", description="optional overall review note")
    findings: list[PilotFinding] = Field(default_factory=list)


class ResumeRequest(BaseModel):
    """JSON body for POST /runs/{id}/resume (#628). ``decision`` is REQUIRED —
    there is no fail-open default, so a request that omits it is a 422, never a
    silent approve. A client that PUTs ``{"decision": "reject"}`` is honored
    (the old query-only signature ignored the body and fell through to approve,
    turning a human's rejection into an approval — the governance footgun #628)."""
    decision: str = Field(..., description="'approve' | 'reject' (or approve/approved/yes/y)")
    approver: str = Field("api-user", description="who resolved the gate (audit)")
    note: str | None = Field(None, description="free-text note recorded into the audit chain")


#: #220 — where an authenticated request acts when it names no workspace and its
#: account has no active one: an id no workspace can have (``~`` never survives
#: :func:`_slug_workspace_id`) and so no store row. Only the account-level routes
#: (:func:`_account_level_route`) are served there; every other route answers 404,
#: so no record is ever written under it.
_NO_WORKSPACE = "~no-workspace"


def _account_level_route(method: str, path: str) -> bool:
    """The routes an account with no active workspace may still call (#220): who am I
    (``/auth/me``; the other ``/auth/*`` routes are public and never get here), listing
    and creating its workspaces (``/workspaces``),
    archiving or restoring one (``/workspaces/{id}``, which checks ownership itself,
    #157), and the operator report list. None of them writes a workspace-scoped
    record; everything else needs a workspace and answers 404 without one."""
    if path in ("/auth/me", "/workspaces"):
        return True
    if path == "/operators/reports":
        return method == "GET"
    rest = path[len("/workspaces/"):] if path.startswith("/workspaces/") else None
    return bool(rest) and "/" not in rest.strip("/") and method in ("PATCH", "DELETE", "GET")


def _slug_workspace_id(raw: str) -> str:
    """Slug-safe workspace id: lowercase, ``[a-z0-9-]``, collapsed dashes."""
    s = "".join(c if (c.isalnum() or c == "-") else "-" for c in (raw or "").strip().lower())
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")


# ─── Request → workspace resolution (#228 · ADR-0048 · ADR-0051) ──────
# A *pure ASGI* middleware (NOT BaseHTTPMiddleware / @app.middleware, which runs
# the route in a separate task where a ContextVar set here would NOT propagate).
# Resolves the request workspace and sets the request-scoped ContextVar so
# `current_workspace()` — and every record stamped through it — is per-request.
#
# ADR-0051: the URL **path** is the source of truth. A workspace-scoped resource
# is addressed `/workspaces/{id}/<rest>`; the middleware lifts `{id}`, sets the
# workspace, and **rewrites the scope path to `/<rest>`** so the existing flat
# handlers serve it unchanged (no per-route duplication). The `X-Cairn-Workspace`
# header stays as a transitional fallback for the flat routes until the console
# (#262) and other clients move to the path form.
class _WorkspaceMiddleware:
    def __init__(self, app: Any) -> None:
        self.app = app

    @staticmethod
    def _workspace_from_path(path: str) -> tuple[Optional[str], str]:
        """`/workspaces/{id}/<rest>` → (`id`, `/<rest>`); anything else →
        (None, path). The bare collection routes (`/workspaces`, `POST
        /workspaces`) and an id with no sub-resource are left untouched."""
        prefix = "/workspaces/"
        if not path.startswith(prefix):
            return None, path
        remainder = path[len(prefix):]            # "{id}/<rest>" | "{id}" | ""
        slash = remainder.find("/")
        if slash <= 0:                            # "" | "{id}" (no sub-resource)
            return None, path
        ws_id, rest = remainder[:slash], remainder[slash:]
        if not rest.strip("/"):                   # "/workspaces/{id}/" → no resource
            return None, path
        return ws_id, rest

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        ws, rewritten = self._workspace_from_path(scope.get("path", ""))
        if ws is not None:
            # path is authoritative: rewrite to the flat route the handlers declare
            scope = {**scope, "path": rewritten, "raw_path": rewritten.encode("latin-1")}
        elif not (auth_enabled() and _is_public_path(scope.get("path", ""), scope.get("method", ""))):
            # #209 — NOT on a public route: the guard authenticated nobody there, so
            # nothing below could check the header's workspace against a caller. A
            # signed webhook binds to the workspace that declared its receiver.
            hdrs = dict(scope.get("headers") or [])
            ws = hdrs.get(b"x-cairn-workspace", b"").decode("latin-1").strip() or None
        # #1610 (ADR-0094 D3) — AUTHORIZE the asserted workspace before acting
        # in it. Until now this line simply trusted whatever the caller named,
        # which is what made a workspace a filing convention rather than a
        # boundary (#1609).
        #
        # Best-effort account resolution ON PURPOSE: this middleware runs before
        # routing, so it also sees `/healthz`, `/token` and the login page. Its
        # job is "may this caller use this workspace", NOT "is this caller
        # authenticated" — that stays with each route's `require_auth`. No token,
        # a bad token, or auth switched off all resolve to None, which
        # `assert_workspace_allowed` treats as "no tenant boundary to enforce".
        if ws:
            # #205 — the identity the bearer guard (outermost) authenticated:
            # it may have come from `?access_token=` on the run stream, and
            # the tenant check must judge that same credential. #209 — the
            # guard's decoded claims, not its token, so no second decode can
            # fail in between and skip the check. The header parse remains
            # for requests the guard did not authenticate.
            claims = authenticated_claims(scope)
            try:
                from cairn.core.triggers.console_support import account_from_claims, caller_account
                if claims is not None:
                    acct = await account_from_claims(claims)
                else:
                    hdrs = dict(scope.get("headers") or [])
                    raw = hdrs.get(b"authorization", b"").decode("latin-1")
                    bearer = raw.split(" ", 1)[1].strip() if raw.lower().startswith("bearer ") else None
                    acct = await caller_account(bearer) if bearer else None
            except Exception as e:  # noqa: BLE001 — unauthenticated is not unauthorized
                if claims is not None:
                    # The guard authenticated this caller, so failing to resolve
                    # their account fails CLOSED (503), as the implicit branch
                    # does, never "act in the named workspace unchecked".
                    logger.error("workspace middleware: could not resolve the caller's account (%s)",
                                 type(e).__name__)
                    await self._unavailable(send)
                    return
                acct = None
            if acct:
                try:
                    await assert_workspace_allowed(ws, acct)
                except WorkspaceForbidden:
                    # 404, never 403: cross-tenant EXISTENCE must not leak
                    # (ADR-0051 §4, same choice as `_run_in_ws_or_404`).
                    await self._not_found(send)
                    return
        elif auth_enabled() and (claims := authenticated_claims(scope)) is not None:
            # #220 — a request that names no workspace still acts in one: the
            # implicit `CAIRN_WORKSPACE`, else `default`. Nothing checked that the
            # caller's account owns it, so a member of ANY account read `default`
            # (the default account's) by sending no header. The implicit workspace
            # is authorized like a named one; a caller whose account does not own
            # it acts in their account's home workspace (the one sign-in lands in);
            # an account with no active workspace acts in none (``_NO_WORKSPACE``).
            # Only the guard's claims count, so public paths are untouched.
            #
            # Fails CLOSED: the guard authenticated this caller, so a failure to
            # decide where they act is a 503, never "act in the implicit workspace
            # unchecked".
            try:
                ws = await self._implicit_workspace_for(claims)
            except Exception as e:  # noqa: BLE001 — the class is logged, never the token
                logger.error("workspace middleware: could not resolve the caller's workspace (%s)",
                             type(e).__name__)
                await self._unavailable(send)
                return
            if ws == _NO_WORKSPACE and not _account_level_route(scope.get("method", ""), scope.get("path", "")):
                await self._not_found(send)
                return

        token = set_current_workspace(ws)
        try:
            await self.app(scope, receive, send)
        finally:
            reset_current_workspace(token)

    @staticmethod
    async def _implicit_workspace_for(claims: dict[str, Any]) -> str:
        """Where an authenticated request that names no workspace acts (#220).

        The implicit workspace (``CAIRN_WORKSPACE``, else ``default``) when the
        caller's account owns it; otherwise the account's home workspace;
        ``_NO_WORKSPACE`` when the account has no active one, where only the
        account-level routes answer (the caller 404s everything else). With auth on,
        ``assert_workspace_allowed`` refuses a workspace with no row (#156), so
        a ``CAIRN_WORKSPACE`` that names none (a misconfiguration, warned about
        at startup) sends every caller to their home workspace."""
        from cairn.core.triggers.console_support import account_from_claims, home_workspace  # noqa: PLC0415

        acct = await account_from_claims(claims)
        implicit = current_workspace()
        try:
            await assert_workspace_allowed(implicit, acct)
        except WorkspaceForbidden:
            return await home_workspace(acct) or _NO_WORKSPACE
        return implicit

    @staticmethod
    async def _unavailable(send: Any) -> None:
        body = b'{"detail":"could not resolve the workspace for this request; try again"}'
        await send({"type": "http.response.start", "status": 503,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    async def _not_found(send: Any) -> None:
        body = b'{"detail":"workspace not found"}'
        await send({"type": "http.response.start", "status": 404,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


# ─── App builder ─────────────────────────────────────────────────────


def build_app(
    runner: Runner,
    environment: str | None = None,
    *,
    enable_scheduler: bool = True,
    enable_spec_routes: bool = True,
) -> FastAPI:
    """Build the FastAPI app bound to a Runner instance.

    Every trigger goes through a single, shared ``RunDispatcher``
    rather than calling ``runner.run`` directly. This gives the webhook path
    dedup/coalescing (alert storms fold onto one investigation), a bounded
    per-lane concurrency budget, and a queryable lifecycle record — without
    changing what the Runner does. Webhooks submit on the ``realtime`` lane
    with ``wait=True`` so the HTTP response still carries the result; the
    scheduler shares the same dispatcher and submits on the ``background``
    lane, so scheduled sweeps show up in ``GET /runs`` too.

    If ``enable_scheduler`` is True (default), a ``SkillScheduler`` is started
    on app startup so skills with ``triggers.schedule`` fire automatically.
    """
    # One dispatcher shared by the webhook path and the scheduler, so both
    # lanes record into the same StorageBackend and are visible on the
    # status surface. The backend's run store is restart-safe SQLite when
    # CAIRN_RUN_DB is set, else in-memory; memory/RAG come from their own
    # env-driven defaults (see :func:`cairn.core.storage.build_storage`).
    dispatcher = RunDispatcher(runner, backend=build_storage())
    scheduler = SkillScheduler(runner, dispatcher) if enable_scheduler else None
    # #896 — HITL approval TTL reaper: a periodic sweep that auto-expires abandoned
    # awaiting_approval runs (governance-state hygiene). Off unless the governance
    # TTL (CAIRN_APPROVAL_TTL_HOURS, default 7d) is > 0; started/stopped with the
    # scheduler in the lifespan below.
    from cairn.core.approval.reaper import ApprovalReaper
    reaper = ApprovalReaper(dispatcher.store)

    # #1510 — bus_messages retention: the durable bus is a TRANSPORT buffer
    # duplicated in run_events, and nothing ever pruned it. Postgres-only and
    # off unless CAIRN_BUS is a postgres DSN; run_events is never touched (it is
    # the evidence, and has no indexed timestamp to prune by).
    from cairn.core.storage.retention import RetentionSweeper
    _bus_dsn = (getenv("CAIRN_BUS") or "").strip()
    retention = RetentionSweeper(_bus_dsn if _bus_dsn.startswith("postgres") else None)

    # #976 — run reconciler: heals "zombie" runs stuck at a non-terminal status.
    # Two verdict sources (ADR-0086 H1, #1239): the Temporal workflow-status
    # query when that runtime is on (the original #976 case), and — for
    # engine-owned flow legs on ANY runtime — the run's own heartbeat stamp
    # (leg poll loops beat every ≥10s; a `running` run silent past the TTL has
    # no poll loop left, so nothing else will ever terminalize it). The emit
    # hook publishes the terminal on the bus exactly like a live completion, so
    # a reconciled leg ADVANCES/FAILS its pipeline instead of hanging it.
    from cairn.core import settings as _reconciler_settings  # _settings is imported later in this fn
    from cairn.core.runtime.reconciler import RunReconciler

    _owner_alive_fn = None
    _reconcile_result_fn = None  # only DBOS can hand back a finished workflow's result today (#1869)
    if _reconciler_settings.runtime() == "temporal":
        from cairn.core.runtime.temporal.runtime import TemporalRuntime
        _reconcile_rt = TemporalRuntime()
        _reconcile_status_fn = _reconcile_rt.workflow_terminal_status

        async def _owner_alive_fn(run) -> bool:
            # #1429 — a flow leg of a Temporal-OWNED pipeline whose workflow is
            # still RUNNING has a durable owner that will retry + re-adopt it;
            # the heartbeat verdict must not race that (found live: worker
            # down 10 min → reconciler failed the leg at 5 min → the retry
            # honored the failure). Bridge-owned pipelines keep H1 as is.
            pid = str(getattr(run, "bundle_pipeline_id", "") or "")
            if not pid:
                return False
            from cairn.core.storage.pipeline_store import default_pipeline_store  # noqa: PLC0415
            rec = await default_pipeline_store().get(pid)
            if rec is None or (rec.trigger or {}).get("owner") != "temporal":
                return False
            return await _reconcile_rt.workflow_is_running(pid)
    elif _reconciler_settings.runtime() == "dbos":
        # #1847 — DBOS needs this MORE than Temporal does, not less. Temporal's
        # worker survives a control-plane restart, so a run whose workflow
        # finished is a narrow race. Under DBOS the workflow runs IN this
        # process: a crash kills the `invoke` await that would have written the
        # terminal status, `launch()` then recovers the workflow and completes
        # it with nobody listening, and the record is stranded at whatever it
        # said when the process died. Without this the reconciler cannot see it,
        # and the approval reaper eventually EXPIRES a run that succeeded.
        from cairn.core.runtime.dbos.runtime import DBOSRuntime  # noqa: PLC0415
        _reconcile_status_fn = DBOSRuntime().workflow_terminal_status
        # #1869 — and the RESULT, not just the status: a workflow recovered after
        # a restart finishes with nobody awaiting it; the record got `done` and
        # empty result_payloads. DBOS keeps the output; hand it to the reconciler.
        _reconcile_result_fn = DBOSRuntime().workflow_result
    else:
        async def _reconcile_status_fn(_run_id: str):  # no runtime to ask — heartbeat only
            return None
        _reconcile_result_fn = None

    async def _reconcile_emit(run_id: str, type_: str, data: dict) -> None:
        # bus-only on purpose: reconcile_stale_runs already appended the run
        # event with the full note; going through dispatcher._emit would append
        # it twice. The bridge chains on the `run.<type>` topic.
        from cairn.core.bus import get_bus  # noqa: PLC0415 — lazy like dispatcher._emit
        await get_bus().publish(f"run.{type_}", dict(data), correlation_id=run_id)

    # ADR-0125 D3/D4 — the owner verdict for in-process legs, on every runtime.
    # A DEAD leg of a running/paused pipeline goes to the bridge (built below;
    # the closures resolve it at call time), whose adopt rules decide; one of a
    # terminal pipeline is settled `cancelled` through the dispatcher's cancel,
    # which closes its remote jobs and budget holds.
    async def _reconcile_dead_leg(run) -> bool:
        return await pipeline_bridge.adopt_dead_leg(run)

    async def _reconcile_pipeline_status(pid: str):
        rec = await pipeline_bridge.store.get(pid)
        return rec.status if rec is not None else None

    reconciler = RunReconciler(
        dispatcher.store, _reconcile_status_fn, emit_fn=_reconcile_emit,
        owner_alive_fn=_owner_alive_fn, result_fn=_reconcile_result_fn,
        owner_probe=liveness.lock_free, self_owner_fn=liveness.self_owner,
        owner_reacquire_seconds=liveness.REACQUIRE_SECONDS,
        dead_leg_fn=_reconcile_dead_leg, pipeline_status_fn=_reconcile_pipeline_status,
        cancel_fn=dispatcher.cancel,
    )

    # Bus → skill bridge (ADR-0010 P3.c): activates skills from cross-pack bus
    # events on their `subscribes_to:` topics. Inert unless a skill declares one.
    from cairn.core.dispatch.bus_bridge import BusSkillBridge
    bus_bridge = BusSkillBridge(runner, dispatcher)

    # Pipeline bridge (ADR-0069 §2, #746): the CORE-owned enforcer that advances
    # bundle pipelines — subscribes to the run-terminal firehose and, on a step's
    # completion, evaluates its gate over persisted output and submits the next
    # governed run. NOT a skill (governance boundary); reuses the bus as transport.
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    pipeline_bridge = PipelineBridge(dispatcher)
    # ADR-0105 D1 (#2113) — every door that only holds the dispatcher (the
    # legacy /packs/{p}/{t}/run route, triggers) still starts a flow as a
    # every start goes through the bridge: the doors find it here.
    dispatcher.pipeline_bridge = pipeline_bridge

    # Bus → SSE hub bridge (#967, ADR-0010 §24.10): feed the cross-process `run.*`
    # firehose into the store's in-process `_hub` so `GET /runs/{id}/stream` shows
    # WORKER-appended events (Temporal node progress) live. One per-process bus
    # LISTEN, not one per SSE client.
    from cairn.core.dispatch.sse_bridge import SseEventBridge
    sse_bridge = SseEventBridge(dispatcher.store)

    # RAG indexing lifecycle (ADR-0023 slice 2). Opt-in via CAIRN_RAG_AUTOINDEX —
    # off by default so keyless local-dev pays nothing. When on: a startup pass
    # indexes declared corpora (install/update), then a sibling scheduler fires
    # periodic re-index per [requires.rag.<name>].freshness.
    from cairn.services.rag.lifecycle import autoindex_enabled
    rag_scheduler = None
    if autoindex_enabled():
        from cairn.services.rag.schedule import RagIndexScheduler
        rag_scheduler = RagIndexScheduler(catalog=dispatcher.backend.rag_catalog)

    # Durable trigger ingress (opt-in CAIRN_TRIGGER_DURABLE): the spec-route
    # router is built below; declared here so the lifespan closure can start a
    # drain loop over its inbox. Holder survives until shutdown to cancel it.
    router: "Optional[TriggerRouter]" = None
    _inbox_drain: dict[str, Any] = {"task": None}
    _jobctl: dict[str, Any] = {"task": None}  # remote-job control worker (#127)
    _continuous_eval: dict[str, Any] = {"consumer": None}  # #477 bus-driven scorer
    _leadership: dict[str, Any] = {"leader": None}  # #1533 fleet-singleton election
    # sub-app lifespans a router provider registered (the ops-MCP, ADR-0067 #642) — #2422
    _router_lifespans: list = []

    # Lifespan (replaces the deprecated @app.on_event startup/shutdown pair).
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        # A router provider's sub-app lifespans (the ops-MCP's StreamableHTTP
        # session manager, ADR-0067 #642): registered at the end of build_app by
        # mount_router_providers; unwound LAST via the stack in `finally`.
        # #1853 — refuse to boot with a password hash that can never verify.
        # A control plane whose every login fails as "incorrect password" while
        # reporting itself healthy is worse than one that does not start: the
        # cause is named here, at startup, instead of being discovered from
        # the outside as an unfalsifiable credential mismatch. Only when auth
        # is ON — keyless dev never sets the hash.
        if auth_enabled():
            _hash_problem = password_hash_problem()
            if _hash_problem:
                raise RuntimeError(_hash_problem)
            # #220/#156 — with auth on, `CAIRN_WORKSPACE` must name a workspace that
            # exists: one that does not is refused, so every header-less request
            # acts in its caller's home workspace instead. Said once, at boot.
            _implicit_ws = getenv("CAIRN_WORKSPACE", "").strip()
            if _implicit_ws:
                try:
                    from cairn.core.workspace import get_workspace_store  # noqa: PLC0415
                    if await get_workspace_store().get(_implicit_ws) is None:
                        logger.warning(
                            "CAIRN_WORKSPACE=%r names no workspace: with auth on it is refused, "
                            "and requests that name no workspace act in each caller's home "
                            "workspace instead. Create it (POST /workspaces) or unset "
                            "CAIRN_WORKSPACE.", _implicit_ws)
                except Exception as e:  # noqa: BLE001 — a store hiccup must not stop the boot
                    logger.warning("could not check CAIRN_WORKSPACE at startup (%s)", type(e).__name__)
        from contextlib import AsyncExitStack
        _stack = AsyncExitStack()
        for _sub_lifespan in _router_lifespans:
            await _stack.enter_async_context(_sub_lifespan(_app))
        # OpenTelemetry (#242): initialize tracing + metrics so run/operator/LLM
        # spans and the fleet metrics actually emit. No-op unless
        # OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_CONSOLE is set, so keyless local-dev
        # pays nothing.
        from cairn.core.obs.metrics import init_metrics
        from cairn.core.obs.telemetry import init_tracing
        init_tracing()
        init_metrics()
        # DBOS runtime (ADR-0035): rebuild the template registry + launch DBOS,
        # which recovers any PENDING runs left by a previous process. Runs here
        # (inside uvicorn's loop) so recovered async workflows have a loop. No-op
        # unless CAIRN_RUNTIME=dbos.
        from cairn.core import settings as _settings
        if _settings.runtime() == "dbos":
            from cairn.core.runtime.dbos.registry import set_run_store  # noqa: PLC0415
            from cairn.core.runtime.dbos.runtime import boot as _dbos_boot
            try:
                # #1865 — the workflow's approval surfacing + node events write to
                # THIS server's store, not a fresh default_store() of their own.
                set_run_store(dispatcher.store)
                n = _dbos_boot()
                logging.getLogger(__name__).info(
                    "[dbos] registry rebuilt (%d template(s)) + launched; "
                    "PENDING runs recovered", n,
                )
            except Exception as e:  # noqa: BLE001 — a DBOS boot failure must not kill the server
                # The cause goes in the ERROR line itself (e.g. the SQLite-version
                # refusal), not only in the traceback a log shipper may drop.
                logging.getLogger(__name__).exception("[dbos] boot failed: %s", e)
        # ADR-0101 D1 (#1973) — arm the `triggers:` declared in the front matter
        # of every ALREADY-enabled workflow. Enabling a workflow materialises its
        # triggers, but a workflow enabled by an earlier process (or a pack
        # update that changed a cron) would otherwise stay armed as it was.
        # Runs BEFORE the scheduler starts below, so a newly materialised row is
        # picked up by this boot's registration pass rather than the next one.
        # Convergent + id-deterministic, so N replicas racing here agree.
        try:
            from cairn.core.engine.triggers_grammar import sync_declared_triggers
            _armed = await sync_declared_triggers()
            if _armed:
                logging.getLogger(__name__).info(
                    "[triggers] %d declared trigger row(s) converged at start", _armed)
        except Exception:  # noqa: BLE001 — arming must never block the boot
            logging.getLogger(__name__).exception("[triggers] declared-trigger sync failed")
        # #2631 — a PIPELINE arms its declared `triggers:` when it is installed
        # (`enable_pipeline` → `materialise_pipeline_triggers`). Nothing on the public
        # core installed discovered pipelines, so every declared schedule, watch,
        # webhook and `kind: event` row stayed dead. Install them into the DEFAULT
        # workspace here, as discovered workflows are (#258); other workspaces stay
        # explicit. Each row keeps its declared `enabled:`. Convergent and
        # id-deterministic, so N replicas racing agree; before the scheduler starts so
        # this boot registers the rows it wrote.
        try:
            from cairn.core.workflow_registry import sync_discovered_pipelines  # noqa: PLC0415 — registry off the boot import path
            _installed = await sync_discovered_pipelines(workspace_id="default")
            logging.getLogger(__name__).info(
                "[triggers] %d discovered pipeline(s) installed; their declared triggers armed", _installed)
        except Exception:  # noqa: BLE001 — arming must never block the boot
            logging.getLogger(__name__).exception("[triggers] pipeline trigger sync failed")
        # The webhook routes were mounted when the app was built, before the pass
        # above: mount the ingress paths it just armed (only paths not yet mounted).
        if router is not None:
            try:
                from cairn.core.dispatch.start import unattended_starter  # noqa: PLC0415 — dispatch ↔ server cycle
                _armed_router = TriggerRouter.from_installed(
                    run_template=unattended_starter(dispatcher, source="webhook"),
                    run_environment=environment,
                )
                _mounted = {getattr(r, "path", "") for r in _app.routes}
                _mount_trigger_routes(_app, _armed_router, skip_paths=_mounted)
            except Exception:  # noqa: BLE001 — a route that fails to mount must not stop the boot
                logging.getLogger(__name__).exception("[triggers] armed webhook routes not mounted")
        # #1533 — these five are FLEET singletons, not per-replica loops. With N
        # replicas each running the cron scheduler, one `schedule:` step fires N
        # runs per tick: N× the spend and N× the audit trail, from a governance
        # product. The reaper/retention/reconciler/RAG sweeps are the same shape
        # (idempotent, but N× the churn). One replica wins a Postgres advisory
        # lock and owns them; if it dies its session dies, the lock frees, and a
        # follower picks them up within a poll interval. Without Postgres every
        # process is trivially leader, so single-node dev is unchanged.
        # NOT elected: the durable trigger inbox drain — a work queue WANTS many
        # consumers, and electing one would discard the throughput it exists for.
        _leader = Leadership(role="periodic")
        if scheduler is not None:
            _leader.manage("skill-scheduler", start=scheduler.start,
                           stop=scheduler.shutdown)
        _leader.manage("approval-reaper", start=reaper.start,  # #896
                       stop=reaper.shutdown)
        _leader.manage("retention-sweeper", start=retention.start,  # #1510
                       stop=retention.shutdown)
        if reconciler is not None:
            _leader.manage("run-reconciler", start=reconciler.start,  # #976 + H1
                           stop=reconciler.shutdown)
        # ADR-0131 D5 — declared `kind: event` triggers start their pipeline once
        # per message: on the leader only, like the scheduler.
        event_triggers = EventTriggerBridge(dispatcher)
        _leader.manage("event-triggers", start=event_triggers.start, stop=event_triggers.stop)
        _leadership["leader"] = _leader
        await _leader.start()
        await bus_bridge.start()
        # ADR-0125 D2 — this process's owner lock, held for its life, BEFORE the
        # bridge adopts: a leg it starts or takes over names this process, and
        # the lock is how every other process can tell it is alive.
        await liveness.start_process_owner(dispatcher.store)
        await pipeline_bridge.start()  # ADR-0069 (#746) — live bundle-pipeline advancement
        await sse_bridge.start()  # #967 — cross-process live SSE (worker node events)
        # Continuous-eval consumer (#477): opt-in bus-driven scoring for durable/
        # out-of-process topologies — it re-fires the run hooks (#2421), whose
        # listeners skip a run the dispatcher's inline terminal path already handled.
        from cairn.core.bus import get_bus
        from cairn.core.eval.continuous import maybe_start_continuous_eval
        _continuous_eval["consumer"] = await maybe_start_continuous_eval(get_bus(), dispatcher.store)
        if rag_scheduler is not None:
            from cairn.services.rag.lifecycle import (
                check_backend_alignment, check_embedder_alignment, index_all,
            )
            check_backend_alignment()
            try:
                await index_all(catalog=dispatcher.backend.rag_catalog)
            except Exception:  # noqa: BLE001 — indexing must not block startup
                logging.getLogger(__name__).exception("[index] startup index pass failed")
            # #512 — after the index pass, warn about any corpus still built under a
            # different embedder (index_all disabled/failed) → search won't find it.
            try:
                await check_embedder_alignment(catalog=dispatcher.backend.rag_catalog)
            except Exception:  # noqa: BLE001 — a doctor check must never block startup
                pass
            # #1533 — elected like the other sweeps (see above).
            _leader.manage("rag-index-scheduler", start=rag_scheduler.start,
                           stop=rag_scheduler.shutdown)
            if _leader.is_leader:
                rag_scheduler.start()
        # Durable trigger ingress: drain the inbox with retry/backoff. Only when
        # CAIRN_TRIGGER_DURABLE made the router build an inbox (else inline as before).
        if router is not None and getattr(router, "inbox", None) is not None:
            from cairn.core.storage.trigger_inbox import run_drain_loop

            async def _dispatch_record(rec):
                # #212 — the run starts in the workspace that declared its receiver,
                # as the inline delivery does (``None`` → the process default).
                # #217 — through the launch door the inline delivery takes (the
                # router's starter), never a bare compile-and-run: plan-time policy
                # gates, admission and the run records apply to a queued delivery too.
                ws_token = set_current_workspace(getattr(rec, "workspace", None))
                try:
                    await router.start_queued(rec.template, rec.input)
                finally:
                    reset_current_workspace(ws_token)

            _inbox_drain["task"] = asyncio.create_task(run_drain_loop(router.inbox, _dispatch_record))
            logging.getLogger(__name__).info("[inbox] durable trigger drain started")
        # Remote-job control worker (ADR-0038, #127): drives registered remote
        # jobs (poll → resume the paused run). Opt-in CAIRN_REMOTE_JOB_DURABLE.
        if getenv("CAIRN_REMOTE_JOB_DURABLE", "").strip().lower() in ("1", "true", "yes", "on"):
            from cairn.core.operator.job_control import (
                build_default_cancel,
                build_default_poll,
                build_default_relaunch,
                build_default_resume,
                run_control_loop,
            )
            from cairn.core.operator.job_store import get_remote_job_store

            # Mid-train early-abort (ADR-0039 Gate 2) is opt-in on top of the worker:
            # CAIRN_LEARNING_ABORT enables "is it learning?" → abort to stop spend.
            _learn = None
            _cancel = None
            if getenv("CAIRN_LEARNING_ABORT", "").strip().lower() in ("1", "true", "yes", "on"):
                from cairn.core.operator.learning import LearningConfig

                _learn = LearningConfig()
                _cancel = build_default_cancel()
            # Resume-from-checkpoint on a resumable death (#552): only fires when the
            # job is retriable AND reported a durable checkpoint_uri, bounded by
            # CAIRN_REMOTE_JOB_MAX_ATTEMPTS (default 2; 0 disables).
            _max_attempts = int(getenv("CAIRN_REMOTE_JOB_MAX_ATTEMPTS", "2") or 2)
            _jobctl["task"] = asyncio.create_task(
                run_control_loop(
                    get_remote_job_store(), build_default_poll(), build_default_resume(dispatcher),
                    cancel=_cancel, learning=_learn,
                    relaunch=build_default_relaunch(), max_attempts=_max_attempts,
                )
            )
            logging.getLogger(__name__).info(
                "[jobs] remote-job control worker started (learning-abort=%s, max-attempts=%d)",
                _learn is not None, _max_attempts,
            )
        try:
            yield
        finally:
            # #1533 — stand down: releases the advisory lock (so a follower is
            # promoted immediately rather than after a poll) and shuts down every
            # loop this replica was running as leader.
            _lead = _leadership.get("leader")
            if _lead is not None:
                await _lead.stop()
            bus_bridge.stop()
            pipeline_bridge.stop()
            await liveness.stop_process_owner()  # ADR-0125 — releases the owner lock
            sse_bridge.stop()  # #967
            if _continuous_eval["consumer"] is not None:
                _continuous_eval["consumer"].stop()
            if rag_scheduler is not None:
                rag_scheduler.shutdown()
            if _inbox_drain["task"] is not None:
                _inbox_drain["task"].cancel()
            if _jobctl["task"] is not None:
                _jobctl["task"].cancel()
            await dispatcher.aclose()
            await _stack.aclose()  # tear down the ops-MCP session manager (#642)

    app = FastAPI(
        title="cairn trigger server",
        version="0.1.0",
        docs_url="/docs",
        lifespan=_lifespan,
    )

    # request → workspace resolution (#228): every request's records stamp the
    # workspace from the X-Cairn-Workspace header (else env/default).
    app.add_middleware(_WorkspaceMiddleware)

    # JWT auth (an internal issue): mounts POST /token + a bearer-token
    # guard. Enforced iff CAIRN_JWT_SECRET is set; open (warned) otherwise, so
    # keyless local-dev is unchanged. /healthz and /token stay reachable.
    install_auth(app)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        # #229 — a fingerprint of the connection store this engine reads (never its
        # path or DSN), so a CLI about to write a connection can tell it is writing
        # another one.
        from cairn.core.settings import connection_store_identity  # noqa: PLC0415 — healthz only

        ident = connection_store_identity()
        return {
            "ok": True,
            "store_fingerprint": ident[1] if ident else None,
            "skills_loaded": len(runner.skill_engine.list_skills()),
            "environment": environment or runner.default_environment,
            "scheduler_enabled": scheduler is not None,
            "scheduled_jobs": scheduler.list_jobs() if scheduler else [],
        }

    # ── Skills + scheduled triggers (#354/#271) — extracted to
    # routers/schedules.py (#1191). Included at exactly this position so route
    # order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.schedules import build_schedules_router  # noqa: PLC0415 — routers import server helpers back
    app.include_router(build_schedules_router(runner, scheduler, rag_scheduler))

    # ── Run status surface ─────────────────────────────────
    #
    # The dispatcher records every submission (both lanes) as an
    # Run that moves queued → running → done/failed/cancelled.
    # These endpoints make that lifecycle queryable.
    #
    # These control-plane reads opt into ``require_auth`` (issue #2): when
    # CAIRN_JWT_SECRET is set they require a valid bearer token (401 otherwise);
    # when it's unset the dependency is a no-op, so keyless dev is unchanged.
    # This composes with — does not replace — the global ``install_auth``
    # middleware guard; it's the demonstration of the reusable dependency.

    # ── Bundle pipelines + flows (ADR-0069/#746, #1136) — extracted to
    # routers/pipelines.py (#1191). Included at exactly this position so route
    # order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.pipelines import build_pipelines_router  # noqa: PLC0415 — routers import server helpers back
    # #1878 — the feature gate: a flagged router answers 404 for a caller whose
    # flag is off (workspace rule → global rule → env seed). Imported here, lazily
    # like every router: feature_gate imports auth, which imports server helpers.
    from cairn.core.triggers.feature_gate import require_feature  # noqa: PLC0415
    app.include_router(build_pipelines_router(dispatcher, pipeline_bridge), dependencies=[Depends(require_feature('pipelines'))])

    # ── Runs + series (results/report/pilot/evaluate/diagnosis) — extracted
    # to routers/runs.py (#1191). Included at exactly this position so route
    # order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.runs import build_runs_router  # noqa: PLC0415 — routers import server helpers back
    # #1880 — the runs router also serves /runs/pending-approvals, which counts open
    # reviews; it gets the SAME review store the reviews router is built with.
    from cairn.core.storage.reviews import build_review_store  # noqa: PLC0415 — routers import server helpers back
    review_store = build_review_store()
    app.include_router(build_runs_router(dispatcher, pipeline_bridge, review_store=review_store))

    # ── Review records (#1468) — read surface over gated decisions; opened by
    # HITL gate fire, decided by the pipeline approve path (never a parallel
    # approval surface).
    from cairn.core.triggers.routers.reviews import build_reviews_router  # noqa: PLC0415 — routers import server helpers back
    app.include_router(build_reviews_router(review_store))

    # ── Pack UI contract + registry (#109, ADR-0045/0047/0049) — extracted to
    # routers/registry.py (#1191). Included at exactly this position so route
    # order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.registry import build_registry_router  # noqa: PLC0415 — routers import server helpers back
    app.include_router(build_registry_router())

    # ── Pack templates (#109-#113, ADR-0072) + console-pack uninstall (ADR-0045)
    # — extracted to routers/templates.py (#1191). Included at exactly this
    # position so route order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.templates import build_templates_router  # noqa: PLC0415 — routers import server helpers back
    app.include_router(build_templates_router(dispatcher))

    # ── One start call (ADR-0101 D3, #1975): POST /start resolves a ref to a
    # workflow or a pipeline and delegates to the doors above — no feature
    # dependency at include time, because the façade must stay usable for
    # workflows where the pipelines feature is off; the handler re-asks the
    # pipelines gate itself once a ref resolves to one.
    from cairn.core.triggers.routers.start import build_start_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_start_router(dispatcher, pipeline_bridge))

    # ── Run control: HITL resume/expire, events, SSE stream, cancel (ADR-0034,
    # #896) — extracted to routers/run_control.py (#1191). Included at exactly
    # this position so route order (and the committed OpenAPI contract) is
    # unchanged.
    from cairn.core.triggers.routers.run_control import build_run_control_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_run_control_router(dispatcher, pipeline_bridge=pipeline_bridge))

    # ── Legacy webhook ingress + saga approval callback (ADR-0024 RM3) —
    # extracted to routers/ingress.py (#1191). Included at exactly this
    # position so route order (and the committed OpenAPI contract) is unchanged.
    from cairn.core.triggers.routers.ingress import build_ingress_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_ingress_router(dispatcher, environment, runner))


    # ── Spec-declared trigger ingress (§24.2) ────────────────────────
    #
    # Domain packs declare ``triggers/<source>.yaml``; the router verifies
    # the signature, normalizes the payload (generic YAML-driven provider
    # by default), applies the ``match`` filter, and starts the ``run``
    # template. One HTTP route per declared ``ingress.path``, mounted here
    # at build time. Distinct from the legacy ``/triggers/alert`` path,
    # which stays for callers that pre-shape their own payload.
    if enable_spec_routes:
        try:
            # #687: hand the router a start entry so a matched webhook event is
            # RECORDED in the run store (first-class in /runs) instead of a
            # fire-and-forget uuid + bare run_within_budget.
            #
            # #2115 (ADR-0105 D4): that entry is the ONE door, not
            # `dispatcher.run_template`. A flow-authored `triggers:` block stays
            # valid and fires the flow's ONE-LEG PIPELINE, which is only true if
            # the ingress starts it the way `/start` does; the run-only door
            # produced a bare run and the same flow two records.
            from cairn.core.dispatch.start import unattended_starter  # noqa: PLC0415 — dispatch ↔ server cycle
            router = TriggerRouter.from_installed(
                run_template=unattended_starter(dispatcher, source="webhook"),
                run_environment=environment,
            )
            # Mounted LAST (below, after every router) — #206: a receiver is
            # refused when an app route already answers its path, so every app
            # route must be there to be collided with.
        except Exception as exc:  # never let trigger discovery break boot
            logger.warning("spec-declared trigger routes not mounted: %s", exc)

    # ── Durable trigger inbox — extracted to routers/ingress.py (#1191).
    # Included at exactly this position so route order (and the committed
    # OpenAPI contract) is unchanged. `router` is settled by now (built above
    # or None), so the value is passed, not the closure.
    from cairn.core.triggers.routers.ingress import build_inbox_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_inbox_router(router))

    # ── Accounts: workspaces, workspace variables, sign-in/sign-up (`/auth/*`) —
    # the public half of the old `admin` provider (ADR-0120 D2, #2427): the CLI logs
    # in and scopes by workspace. Mounted where that provider mounted (first after
    # the public routes), so route order is unchanged.
    from cairn.core.triggers.routers.accounts import build_accounts_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_accounts_router())

    # ── The pull lane: `POST /jobs/claim` + `/jobs/{id}/heartbeat` (#565, ADR-0104 D2) —
    # D2 by the owner's call on #2427: the public CLI's `cairn worker --pull` calls them.
    # The spec is written sorted, so mounting here instead of as a provider changes nothing.
    from cairn.core.triggers.routers.jobs import build_jobs_router  # noqa: PLC0415 — routers ↔ server cycle
    app.include_router(build_jobs_router())

    # ── The OAuth redirect target, `GET /oauth/callback` (#116/#310) — D2 by the owner's
    # call on #2427: the public CLI's `cairn connect --via oauth` redirects here by default.
    # Mounted where the `ask` provider that carried it mounted (first after the public
    # routes); the spec is written sorted either way.
    from cairn.core.triggers.routers.oauth import build_oauth_router  # noqa: PLC0415 — local, like its neighbours
    app.include_router(build_oauth_router())

    # ── Everything beyond the public API (ADR-0120 D2/D4, #2422) — the console's
    # surfaces and the ops-MCP — mounts through the `cairn.routers` entry-point
    # group, after every public route. None registered → the server is D2 only.
    from cairn.core.triggers.router_providers import RouterDeps, mount_router_providers  # noqa: PLC0415
    mount_router_providers(app, RouterDeps(
        app=app, runner=runner, environment=environment, dispatcher=dispatcher,
        pipeline_bridge=pipeline_bridge, scheduler=scheduler, review_store=review_store,
        lifespan=_router_lifespans,
    ))

    # ── The spec-declared receivers, discovered above (#206: after every other
    # route, so a receiver that collides with one is refused, not mounted).
    if router is not None:
        try:
            _mount_trigger_routes(app, router)
        except Exception as exc:  # never let trigger mounting break boot
            logger.warning("spec-declared trigger routes not mounted: %s", exc)

    return app


def _colliding_route(app: FastAPI, method: str, path: str) -> Optional[str]:
    """The app route that already answers ``path`` (any method), else ``None``.

    #206 — a receiver may not sit on a path an app route serves: its public pass
    would reach that route (the receiver mounted after it never sees the
    request), or it would shadow the route (mounted before it). Matched the way
    Starlette routes a request, so ``/triggers/approve/{workflow_id}`` collides
    with a receiver at ``/triggers/approve/x``."""
    from starlette.routing import Match  # noqa: PLC0415 — only the mount needs it

    scope = {"type": "http", "path": path, "root_path": "", "method": method,
             "path_params": {}, "headers": []}
    for route in app.routes:
        matches = getattr(route, "matches", None)
        if matches is None:
            continue
        try:
            match, _child = matches(scope)
        except Exception:  # noqa: BLE001 — an exotic route that cannot answer is no collision
            continue
        if match != Match.NONE:
            return str(getattr(route, "path", "") or getattr(route, "name", "") or route)
    return None


def _mount_trigger_routes(app: FastAPI, router: "TriggerRouter", *,
                          skip_paths: "frozenset[str] | set[str]" = frozenset()) -> None:
    """Add one HTTP route per installed trigger spec's ``ingress.path`` (except
    ``skip_paths``, the ones already mounted).

    #206 — a receiver is refused (logged, not mounted, never public) when its path
    is not a literal path under ``/triggers/`` (a spec built without the loader
    reaches here too) or an app route already answers it."""
    from cairn.core.engine.triggers_spec import ingress_path_problem  # noqa: PLC0415 — engine off the import path
    from cairn.core.triggers.router import resolve_secret  # noqa: PLC0415 — local, like its neighbours

    for spec in router.specs:
        if spec.ingress.path in skip_paths:
            continue
        problem = ingress_path_problem(spec.ingress.path)
        clash = None if problem else _colliding_route(app, spec.ingress.method, spec.ingress.path)
        if problem or clash:
            logger.error(
                "trigger receiver %r NOT mounted: %s", spec.source,
                problem or (f"{spec.ingress.method} {spec.ingress.path} collides with the "
                            f"app route {clash!r}; give the receiver its own path under "
                            f"/triggers/"),
            )
            continue

        async def handler(request: Request, _spec=spec) -> dict[str, Any]:
            body = await request.body()
            headers = dict(request.headers)
            # #209 — the run lands in the workspace that declared this receiver,
            # never one the sender names (a public route ignores X-Cairn-Workspace
            # in the middleware; this binds it whatever reached the handler). A
            # caller the guard authenticated must be allowed in that workspace —
            # the same check, and the same 404, as the path form.
            ws_token = set_current_workspace(router.workspace_for(_spec))
            try:
                claims = authenticated_claims(request.scope)
                if claims is not None:
                    from cairn.core.triggers.console_support import account_from_claims  # noqa: PLC0415 — cycle
                    try:
                        await assert_workspace_allowed(current_workspace(),
                                                       await account_from_claims(claims))
                    except WorkspaceForbidden:
                        raise HTTPException(status_code=404, detail="workspace not found") from None
                try:
                    results = await router.route(_spec, headers, body)
                except PermissionError as e:
                    raise HTTPException(status_code=401, detail=str(e))
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=str(e))
            finally:
                reset_current_workspace(ws_token)
            return _summarize_routes(_spec.source, results)

        app.add_api_route(
            spec.ingress.path,
            handler,
            methods=[spec.ingress.method],
            name=f"trigger_{spec.source}",
        )
        # #686: a signed webhook receiver authenticates by signature, not JWT —
        # a real Alertmanager/PagerDuty sender has no bearer token. Exempt it
        # from the JWT guard so provider.verify() is the gate. #59: only when
        # the signature is REQUIRED — an optional one authenticates nothing (an
        # unsigned payload passes verify), so that receiver, like one with no
        # signature block, keeps the JWT guard when auth is on.
        sig = spec.ingress.signature
        signature_authed = sig is not None and sig.required
        if signature_authed and resolve_secret(sig.secret) is None:
            # #206 — a required signature with no usable secret (unset, or set but
            # empty) verifies nothing: the receiver refuses every request (the
            # check fails closed) and is never public.
            signature_authed = False
            logger.error(
                "trigger receiver %s %s requires a signature but its secret %s is "
                "unset or empty: it refuses every request and is NOT public. Set the "
                "secret to the sender's signing secret",
                spec.ingress.method, spec.ingress.path, sig.secret,
            )
        elif signature_authed:
            register_public_trigger_path(spec.ingress.path, spec.ingress.method)
        elif sig is not None:
            logger.warning(
                "trigger ingress %s has signature.required=false, so it is NOT public: "
                "with auth on it needs a bearer token. Set a webhook secret + "
                "required: true for senders that sign but carry no token",
                spec.ingress.path,
            )
        logger.info(
            "mounted trigger route %s %s → %s%s",
            spec.ingress.method, spec.ingress.path,
            spec.run.template if spec.run else "(no run target)",
            " [public: signature-authed]" if signature_authed else " [jwt-gated]",
        )


def _summarize_routes(source: str, results: list) -> dict[str, Any]:
    """Compact response: how many events arrived, how many matched, and the
    per-run handles for polling ``GET /runs/{id}`` (when tracked)."""
    return {
        "source": source,
        "events": len(results),
        "matched": sum(1 for r in results if r.matched),
        "runs": [
            {
                "run_id": r.run_id,
                # #2115 — the pipeline that owns the leg (ADR-0105 D4): a
                # flow-authored webhook fires the flow's one-leg pipeline, and
                # this is the id the console opens.
                "pipeline_id": r.pipeline_id,
                "template": r.template,
                "matched": r.matched,
                "status": r.status,
                "error": r.error,
            }
            for r in results
        ],
    }


# ─── Dispatch ───────────────────────────────────────────────────────


async def _dispatch(
    dispatcher: RunDispatcher,
    event: TriggerEvent,
    environment: str | None,
    request_id: str,
) -> dict[str, Any]:
    logger.info(
        "trigger received (req=%s): source=%s pattern=%s service=%s",
        request_id, event.source, event.matched_pattern, event.service,
    )
    try:
        # realtime lane + wait=True: bounded concurrency and dedup, but the
        # HTTP caller still gets the finished record back synchronously.
        inv = await dispatcher.submit(
            event,
            lane="realtime",
            environment=environment,
            wait=True,
        )
    except Exception as e:
        # The dispatcher isolates investigation failures (records them as
        # status=failed); reaching here means an unexpected dispatcher error.
        logger.exception("dispatch failed for req=%s", request_id)
        raise HTTPException(status_code=500, detail=f"dispatch failed: {e}")

    if inv.matched_skills == 0 and inv.status == "done":
        logger.info("trigger req=%s — no skills matched", request_id)

    return _summarize_investigation(inv, request_id)


def _summarize_investigation(inv, request_id: str) -> dict[str, Any]:
    """Compact, investigation-centric response.

    Carries the lifecycle handle (id/status/lane) plus per-skill summaries.
    The full RunResult still flows to memory/notifiers; the caller
    can poll ``GET /runs/{id}`` for the tracked record.
    """
    return {
        "request_id": request_id,
        "run_id": inv.id,
        "status": inv.status,
        "lane": inv.lane,
        "matched_skills": inv.matched_skills,
        "coalesced": inv.attached_count > 0,
        "error": inv.error,
        "results": [
            {
                "skill_name": s.skill_name,
                "template_name": s.template_name,
                "duration_seconds": round(s.duration_seconds, 2),
                "actions_proposed": s.action_count,
            }
            for s in inv.results
        ],
    }


def _req_id(request: Request) -> str:
    return request.headers.get("x-request-id") or request.headers.get("x-correlation-id") or "anon"
