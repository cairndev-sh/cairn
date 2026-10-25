"""Accounts: workspaces, workspace variables, sign-in and sign-up (#228/ADR-0048, #258,
ADR-0054 §3, ADR-0094) — extracted from server.py in #1191.

The half of the old ``admin`` router the public CLI needs (it logs in and scopes by
workspace), mounted by the public server itself (ADR-0120 D2, #2427): ``/workspaces``,
``/variables`` and ``/auth/*``. User and invite management and the demo workspace
(``/admin/users``, ``/admin/invites``, ``/admin/account/domain``, ``/demo/*``) are the
platform's ``admin`` router provider. A user record NEVER exposes its password hash.
"""
# NO `from __future__ import annotations` here: the request models
# (WorkspaceCreate, …) are lazily imported inside the factory (cycle rule), so
# PEP 563 string annotations would be unresolvable in this module's globals
# when FastAPI builds the OpenAPI schema. Without it, handler annotations bind
# the factory-local classes at def time.

from typing import Annotated, Any, Optional
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from cairn.core._time import utc_now_iso
from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import (
    _oauth2_scheme,
    caller_identity,
    identity_claims,
    is_env_admin,
    issue_refresh_cookie,
    require_admin,
    require_auth,
)
# Cycle-free (it imports only settings + pydantic), so module-top per CLAUDE.md
# rule 4 — unlike the `cairn.core.workspace` / `cairn.core.account` imports in
# the handlers below, which this module keeps lazy for the routers ↔ server cycle.
from cairn.core.workspace_variables import (
    RESERVED_KEYS,
    WorkspaceVariable,
    default_workspace_variable_store,
    valid_key,
)

#: The workspace-write guard (#1610), as the route dependency every write
#: route shares (``console_support.writable_workspace``, #221) — one guard, or
#: the doors drift.
_writable_workspace = support.writable_workspace


async def _workspace_created(workspace_id: str, *, actor: str, source: str) -> None:
    """Announce a new workspace on ``cairn.run_hooks`` (``workspace.created``, #2421
    decision 7). Fire-and-forget: :func:`~cairn.core.pack.run_hooks.fire` never
    raises here, so a listener can never break the creation that already happened.
    The platform's listener gives the workspace its starter budget (#1571)."""
    from cairn.core.pack import run_hooks  # noqa: PLC0415 — keep the router import light

    await run_hooks.fire(run_hooks.WORKSPACE_CREATED, workspace_id=workspace_id, actor=actor, source=source)


def _token_actor(token: Optional[str]) -> str:
    """Who is calling, for ``workspace.created``: the token's subject, ``"dev"``
    with auth off (as :func:`caller_identity` names it), ``""`` when unreadable."""
    from cairn.core.triggers.auth import auth_enabled, decode_claims  # noqa: PLC0415

    if not auth_enabled():
        return "dev"
    claims = decode_claims(token) if token else None
    return str((claims or {}).get("sub") or "")


async def _provision_first_workspace(acct: Any, company: str, *, actor: str, source: str) -> str:
    """A new account's first workspace, stamped with that account (#1610).

    The id is the first free slug: ``put`` is an upsert, so a second "Acme" —
    or a second sign-up from the same email domain with no company name, the
    norm for Google/GitHub (#2336) — used to overwrite the first company's
    workspace and re-stamp it with the new account."""
    from cairn.core.triggers.server import _slug_workspace_id  # noqa: PLC0415 — routers ↔ server cycle
    from cairn.core.workspace import Workspace, cap_workspace_id, get_workspace_store  # noqa: PLC0415

    store = get_workspace_store()
    base = cap_workspace_id(_slug_workspace_id(company or acct.name) or acct.id)
    ws_id, n = base, 1
    while await store.get(ws_id) is not None:
        n += 1
        ws_id = cap_workspace_id(base, f"-{n}")
    await store.put(Workspace(id=ws_id, name=company or acct.name, account_id=acct.id))
    # #1571 — whatever a new workspace starts with (the platform: a starter
    # budget) arrives the same way on the signup path as on POST /workspaces.
    await _workspace_created(ws_id, actor=actor, source=source)
    return ws_id


async def _owned_workspace_or_404(workspace_id: str, admin: Optional[str],
                                  token: Optional[str]) -> Any:
    """The workspace row an archive/restore acts on, once the caller may manage
    it: an owner/admin (``require_admin``) of the account that owns it, or the
    instance admin (the env admin, or anyone with auth off), as for replay
    (#218). Another account's workspace answers the same 404 as a missing one."""
    from cairn.core.workspace import WorkspaceForbidden, assert_workspace_allowed, get_workspace_store
    ws = await get_workspace_store().get(workspace_id)
    if ws is not None and not (admin is None or is_env_admin(admin)):
        try:
            await assert_workspace_allowed(workspace_id, await support.caller_account(token))
        except WorkspaceForbidden:
            ws = None
    if ws is None:
        raise HTTPException(status_code=404, detail=f"workspace {workspace_id!r} not found")
    return ws


def _oauth_refused(reason: str) -> RedirectResponse:
    """Back to the console's sign-in page with the reason it explains."""
    return RedirectResponse(f"/console/login?error={quote(reason)}", status_code=302)


def build_accounts_router() -> APIRouter:
    """The /workspaces, /variables and /auth/* routes. Captures no
    build_app state — everything resolves through module-level stores."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # these are the module-level request models + helper the inline block used.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        WorkspaceCreate,
        WorkspaceUpdate,
        WorkspaceVariableWrite,
        _slug_workspace_id,
    )

    r = APIRouter()

    # ── Workspaces (#228, ADR-0048) — list/create + report the active one ──
    @r.get("/workspaces", dependencies=[Depends(require_auth)])
    async def list_workspaces(
        include_archived: bool = False,
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """The **active** workspaces + the one resolved for this request. The
        console's switcher reads this; archived workspaces (#258) are hidden unless
        ``?include_archived=true`` — the Settings management view (#325) passes it to
        list all workspaces so archived ones can be restored."""
        from cairn.core.workspace import DEFAULT_ACCOUNT, current_workspace, get_workspace_store
        store = get_workspace_store()
        all_ws = await store.list()
        # #1610 — scope to the caller's account. This route used to return EVERY
        # workspace in the deployment, unauthenticated: with self-serve signup
        # that is one company reading another's workspace list.
        #
        # `caller_account` never returns empty — auth-off (keyless dev) resolves
        # to the implicit `default` account, which is also what an unstamped
        # workspace belongs to, so single-node OSS is unchanged.
        #
        # The comparison falls back to DEFAULT_ACCOUNT, NOT to `acct`. The
        # earlier `(w.account_id or acct) == acct` read as "an unstamped
        # workspace belongs to whoever is asking", which made one visible to
        # EVERY account — a tenancy filter that fails OPEN. Verified by probe:
        # two signed-up accounts each correctly saw only their own workspaces,
        # and both saw the unstamped one.
        acct = await support.caller_account(token)
        if acct:
            all_ws = [w for w in all_ws if (w.account_id or DEFAULT_ACCOUNT) == acct]
        shown = all_ws if include_archived else [w for w in all_ws if w.status != "archived"]
        return {
            "workspaces": [w.model_dump(mode="json") for w in shown],
            "current": current_workspace(),
        }

    @r.post("/workspaces", dependencies=[Depends(require_auth)])
    async def create_workspace(
        body: WorkspaceCreate,
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """Create a workspace (auth-gated — it provisions a tenant boundary).
        ``id`` is slug-derived from ``name`` when omitted. A fresh workspace
        starts EMPTY (#258) — no auto-inherited workflows.

        Create-only, never an upsert: every store's ``put`` replaces the row, so
        a create naming an existing id re-stamped that workspace with the
        caller's account and handed it the owner's runs, variables and starts.
        An id another account holds (``default`` included) answers 404, as a
        cross-account read does; an id the caller's own account holds, 409. With no
        explicit ``id`` the id is the first free slug of the name, as for a new
        account's first workspace: two accounts may both call a workspace "Ops"."""
        from cairn.core.workspace import DEFAULT_ACCOUNT, Workspace, cap_workspace_id, get_workspace_store
        ws_id = cap_workspace_id(_slug_workspace_id(body.id or body.name))
        if not ws_id:
            raise HTTPException(status_code=422, detail="workspace id/name must yield a non-empty slug")
        store = get_workspace_store()
        acct = await support.caller_account(token)
        if not body.id:
            base, n = ws_id, 1
            while await store.get(ws_id) is not None:
                n += 1
                ws_id = cap_workspace_id(base, f"-{n}")
        existing = await store.get(ws_id)
        if existing is not None:
            if (existing.account_id or DEFAULT_ACCOUNT) != acct:
                raise HTTPException(status_code=404, detail=f"workspace {ws_id!r} not found")
            raise HTTPException(status_code=409, detail=f"workspace {ws_id!r} already exists")
        # #1610 — stamp the creating caller's account, so the workspace belongs
        # to a tenant from the moment it exists rather than being adopted later.
        ws = await store.put(Workspace(id=ws_id, name=body.name.strip(), account_id=acct))
        # #1571 — listeners provision what a workspace starts with (the
        # platform: a budget it can spend, so the run form's series picker
        # exists from the first visit).
        await _workspace_created(ws.id, actor=_token_actor(token), source="api")
        return ws.model_dump(mode="json")

    @r.delete("/workspaces/{workspace_id}", dependencies=[Depends(require_auth)])
    async def archive_workspace_endpoint(
        workspace_id: str,
        admin: Annotated[Optional[str], Depends(require_admin)],
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
        confirm: str = "",
    ) -> dict[str, Any]:
        """Soft-delete a workspace (#258): mark it ``archived`` + flip its installed-
        workflow records inactive. **Never** deletes runs / events / audit / pilot
        scores / artifacts — they stay queryable. ``default`` can't be archived.
        Reversible via ``PATCH /workspaces/{id}`` ``{status:"active"}``.

        Backend-enforced typed delete (#323): requires ``?confirm=delete`` so the
        console's typed confirmation can't be bypassed by calling the API directly."""
        from cairn.core.workspace import DEFAULT_WORKSPACE, get_workspace_store
        from cairn.core.workflow_registry import archive_workspace
        if confirm != "delete":
            raise HTTPException(status_code=400, detail="destructive action requires confirm=delete")
        if workspace_id == DEFAULT_WORKSPACE:
            raise HTTPException(status_code=400, detail="the default workspace cannot be archived")
        store = get_workspace_store()
        ws = await _owned_workspace_or_404(workspace_id, admin, token)
        ws.status, ws.archived_at = "archived", utc_now_iso()
        await store.put(ws)
        n = await archive_workspace(workspace_id)
        return {"id": workspace_id, "status": "archived", "workflows_deactivated": n}

    @r.patch("/workspaces/{workspace_id}", dependencies=[Depends(require_auth)])
    async def update_workspace_status(
        workspace_id: str,
        body: WorkspaceUpdate,
        admin: Annotated[Optional[str], Depends(require_admin)],
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """Update a workspace's lifecycle status (#258). ``{status:"active"}``
        restores an archived workspace (reactivating its workflow records);
        ``{status:"archived"}`` is the archive path (same as DELETE)."""
        from cairn.core.workspace import get_workspace_store
        from cairn.core.workflow_registry import archive_workspace, restore_workspace
        if body.status not in ("active", "archived"):
            raise HTTPException(status_code=422, detail="status must be 'active' or 'archived'")
        store = get_workspace_store()
        ws = await _owned_workspace_or_404(workspace_id, admin, token)
        ws.status = body.status
        ws.archived_at = utc_now_iso() if body.status == "archived" else None
        await store.put(ws)
        n = await (archive_workspace if body.status == "archived" else restore_workspace)(workspace_id)
        return {"id": workspace_id, "status": body.status, "workflows_changed": n}

    # ── Workspace variables (ADR-0101 D2, #1974) ───────────────────────────
    #
    # A small KV per workspace — Kestra's namespace variables. The run dialog's
    # Advanced fold defaults from four well-known keys (`environment`,
    # `config_preset`, `cost_cap_usd`, `series`), so a normal run asks only for
    # the workflow's declared inputs; everything else is free-form and shown in
    # Settings › Variables.
    #
    # **The paths are declared FLAT on purpose.** These are addressed
    # `/workspaces/{id}/variables[/{key}]`, but `_WorkspaceMiddleware` lifts the
    # `{id}` and REWRITES the scope path to `/variables[/{key}]` before routing
    # (ADR-0051 §1) — a route declared at the long spelling would never be
    # reached. Same shape as `/triggers/scheduled` and
    # `/registry/installed-workflows`, which are workspace-scoped the same way.
    #
    # Values are NOT secrets: they are stored and echoed in the clear, like
    # every other console store. Credentials belong in connections.
    @r.get("/variables", dependencies=[Depends(require_auth)])
    async def list_workspace_variables() -> dict[str, Any]:
        """Every variable for this workspace, plus the well-known keys the run
        dialog reads — so the console can label them without hard-coding the
        list twice."""
        from cairn.core.workspace import current_workspace
        ws = current_workspace()
        rows = await default_workspace_variable_store().list(ws)
        return {
            "workspace_id": ws,
            "variables": [v.model_dump(mode="json") for v in rows],
            "reserved_keys": list(RESERVED_KEYS),
        }

    @r.put("/variables/{key}", dependencies=[Depends(require_auth)])
    async def put_workspace_variable(
        key: str,
        body: WorkspaceVariableWrite,
        ws: Annotated[str, Depends(_writable_workspace)],
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """Create or replace one variable. Idempotent; stamps who wrote it."""
        if not valid_key(key):
            raise HTTPException(
                status_code=422,
                detail="a variable key is 1–128 chars of letters, digits, '_', '-' or '.'",
            )
        ident = await caller_identity(token)
        saved = await default_workspace_variable_store().put(WorkspaceVariable(
            workspace_id=ws, key=key, value=body.value,
            description=(body.description or "").strip(),
            updated_by=str(ident.get("username") or ""),
        ))
        return saved.model_dump(mode="json")

    @r.delete("/variables/{key}", dependencies=[Depends(require_auth)])
    async def delete_workspace_variable(
        key: str,
        ws: Annotated[str, Depends(_writable_workspace)],
    ) -> dict[str, Any]:
        """Remove one variable. 404 when it isn't set — deleting a key that was
        never there is a mistake worth reporting, not a silent success."""
        if not await default_workspace_variable_store().delete(ws, key):
            raise HTTPException(status_code=404, detail=f"no variable {key!r} in workspace {ws!r}")
        return {"workspace_id": ws, "key": key, "deleted": True}

    # ── Accepting an invite — UNAUTHENTICATED by necessity ─────────────────
    #
    # The invitee has no account yet; the token in the link IS the credential.
    # Both routes are therefore public, and both are careful to say only what
    # the holder of a valid token already knows.
    @r.get("/auth/invite/{raw_token}")
    async def peek_invite_ep(raw_token: str) -> dict[str, Any]:
        """What this link is for, without consuming it — so the accept page can
        show which account and role BEFORE asking for a password. A person
        should know what they are joining."""
        from cairn.core.account import InviteError, get_account_store, peek_invite
        try:
            inv = await peek_invite(raw_token)
        except InviteError as e:
            # 404 + the REASON: "invalid link" for unknown/consumed/expired/
            # revoked alike leaves the recipient unable to tell whether to ask
            # for a new one or check they opened the right thing.
            raise HTTPException(status_code=404, detail=e.reason) from None
        acct = await get_account_store().get_account(inv.account_id)
        return {"email": inv.email, "role": inv.role, "expires_at": inv.expires_at,
                "account_name": (acct.name if acct else inv.account_id)}

    @r.post("/auth/accept-invite")
    async def accept_invite_ep(body: dict) -> dict[str, Any]:
        """Redeem an invite: create the user with THEIR password, join the
        account, consume the link."""
        from cairn.core.account import InviteError, accept_invite
        from cairn.core.triggers.auth import hash_password
        raw = str(body.get("token") or "").strip()
        username = str(body.get("username") or "").strip()
        password = str(body.get("password") or "")
        if not raw or not username or not password:
            raise HTTPException(status_code=422, detail="token, username and password are required")
        try:
            user, inv = await accept_invite(raw, username, hash_password(password))
        except InviteError as e:
            raise HTTPException(status_code=400, detail=e.reason) from None
        return {"username": user.username, "account_id": inv.account_id, "role": inv.role}


    # ── Signup (#1613, ADR-0094 D5/D6) — public by necessity ───────────────
    @r.get("/auth/signup")
    async def signup_status_ep() -> dict[str, Any]:
        """Whether self-serve signup is available, so the login page can offer a
        link ONLY when it would actually work. Offering a door that refuses is
        worse than not offering one."""
        from cairn.core.oauth_login import configured_providers
        from cairn.core.signup import is_first_run, signup_mode
        from cairn.core.triggers.auth import auth_enabled
        mode = signup_mode()
        first = await is_first_run()
        return {"mode": mode, "first_run": first,
                # D6: the first person always gets in, whatever the mode says
                "enabled": first or mode in ("open", "domain"),
                # #2336 — the sign-in providers that would work, so the console
                # shows only live buttons. None without auth: there is no
                # session to sign in to.
                "providers": configured_providers() if auth_enabled() else []}

    @r.post("/auth/signup")
    async def signup_ep(body: dict, request: Request, response: Response) -> dict[str, Any]:
        """Create an account + its first user (or join by domain).

        Also creates the account's first WORKSPACE, so a new company lands
        somewhere it can actually run something rather than on an empty
        switcher — and that workspace is stamped with the new account, which is
        what makes #1610's boundary meaningful for it.
        """
        from cairn.core.signup import SignupError, signup
        from cairn.core.triggers.auth import hash_password

        company = str(body.get("company") or "").strip()
        email = str(body.get("email") or "").strip()
        password = str(body.get("password") or "")
        if not password:
            raise HTTPException(status_code=422, detail="password_required")
        try:
            acct, user, created = await signup(company, email, hash_password(password))
        except SignupError as e:
            # 400 + the REASON so the console can explain WHICH refusal this is
            # (closed / unknown domain / already registered) instead of "failed".
            raise HTTPException(status_code=400, detail=e.reason) from None

        ws_id = (await _provision_first_workspace(acct, company, actor=user.id, source="signup")
                 if created else "")
        # #2323 — sign-up IS a login: the caller just set this password, so the
        # token is the one `POST /token` would issue for these credentials (same
        # claims) and the console lands in setup instead of on /login. Issued on
        # the join-by-domain path too, where `/token` would equally succeed.
        # `None` when auth is off (keyless dev needs no token). ADR-0142: a
        # remembered sign-in, so the refresh cookie is set too (none with auth off).
        from cairn.core.triggers.auth import auth_enabled, create_access_token
        token = (create_access_token(identity_claims(user.id, acct.id))
                 if auth_enabled() else None)
        await issue_refresh_cookie(response, request, user.id, acct.id, remember=True)
        return {"account_id": acct.id, "account_name": acct.name,
                "username": user.username, "created_account": created,
                "workspace_id": ws_id, "access_token": token, "token_type": "bearer",
                "setup_url": f"/w/{ws_id}/setup" if ws_id else None}

    # ── Sign in / sign up with Google or GitHub (#2336, ADR-0119) ──────────
    # Public by necessity (the caller has no session yet; `/auth/oauth/` is a
    # public prefix in auth.py). An identity method, not a door: a new person
    # still goes through `signup()`, so CAIRN_SIGNUP decides as for a password.
    @r.get("/auth/oauth/{provider}/start", response_class=RedirectResponse, status_code=302)
    async def oauth_start_ep(provider: str, request: Request) -> RedirectResponse:
        """Send the browser to the provider, with a signed state and the
        cookie that binds the callback to this browser."""
        from cairn.core import oauth_login as ol
        from cairn.core.triggers.auth import _secret, auth_enabled
        if not auth_enabled():
            return _oauth_refused("oauth_unavailable")
        try:
            started = await ol.begin(provider, secret=_secret(),
                                     redirect_uri=ol.redirect_uri(provider, str(request.base_url)))
        except ol.OAuthLoginError as e:
            return _oauth_refused(e.reason)
        resp = RedirectResponse(started.authorize_url, status_code=302)
        resp.set_cookie(ol.COOKIE_NAME, started.cookie, max_age=ol.STATE_TTL_SECONDS,
                        path=ol.COOKIE_PATH, httponly=True, samesite="lax",
                        secure=started.redirect_uri.startswith("https://"))
        return resp

    @r.get("/auth/oauth/{provider}/callback", response_class=RedirectResponse, status_code=302)
    async def oauth_callback_ep(
        provider: str, request: Request, code: str = "", state: str = "", error: str = "",
    ) -> RedirectResponse:
        """Finish the sign-in. Success hands the console the same JWT
        ``POST /token`` issues, in the URL FRAGMENT of a same-origin redirect:
        a fragment never reaches a server, a proxy log or a Referer, and the
        console strips it from history on arrival (ADR-0119 D3)."""
        from cairn.core import oauth_login as ol
        from cairn.core.triggers.auth import _secret, auth_enabled, create_access_token
        try:
            if not auth_enabled():
                raise ol.OAuthLoginError("oauth_unavailable")
            if error:  # the person cancelled at the provider, or it refused
                raise ol.OAuthLoginError("oauth_denied")
            pid = await ol.complete(
                provider, code=code, state=state, secret=_secret(),
                cookie=request.cookies.get(ol.COOKIE_NAME, ""),
                redirect_uri=ol.redirect_uri(provider, str(request.base_url)))
            out = await ol.sign_in(pid)
        except ol.OAuthLoginError as e:
            resp = _oauth_refused(e.reason)
        else:
            from cairn.core.account import get_account_store
            if out.created_account:
                acct = await get_account_store().get_account(out.account_id)
                ws_id = await _provision_first_workspace(acct, "", actor=out.user.id, source="oauth")
            else:
                ws_id = await support.home_workspace(out.account_id)
            claims = identity_claims(out.user.id, out.account_id)
            frag = urlencode({"access_token": create_access_token(claims), "token_type": "bearer",
                              "workspace": ws_id, "new_account": "1" if out.created_account else "0"})
            resp = RedirectResponse(f"/console/oauth/callback#{frag}", status_code=302)
            # ADR-0142 — a remembered sign-in: the refresh cookie rides the redirect.
            await issue_refresh_cookie(resp, request, out.user.id, out.account_id, remember=True)
        resp.delete_cookie(ol.COOKIE_NAME, path=ol.COOKIE_PATH)
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    @r.get("/auth/me")
    async def auth_me(token: Annotated[Optional[str], Depends(_oauth2_scheme)], workspace: Optional[str] = None) -> dict[str, Any]:
        """The signed-in caller's identity + admin flag — the console reads this to
        show/hide admin-only surfaces (the `/admin/*` API is the real gate). Any
        authenticated user may call it; auth-off dev → a full-access identity.

        #1879 — also carries ``features``: the resolved feature map (env-seeded
        today; the store + overrides + rollout are #1878) the console uses to
        decide which nav items, tabs and Settings sections it renders."""
        from cairn.core.features import features_for  # noqa: PLC0415 — keep the router import light
        from cairn.core.workspace import current_workspace  # noqa: PLC0415

        ident = await caller_identity(token)
        # #1878 — resolved from the store for THIS caller in THIS workspace
        # (workspace rule → global rule → env seed; a % rule buckets the user).
        feats = await features_for(workspace_id=workspace or current_workspace(), user_id=str(ident.get("username") or ""))
        return {**ident, "features": feats}

    return r
