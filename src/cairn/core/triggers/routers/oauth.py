"""The OAuth redirect target for the built-in AuthlibBroker (#116/#310) — D2 (#2427).

Public by the owner's decision on #2427 (ADR-0123 D1): the public CLI's
``cairn connect --via oauth`` runs the auth-code flow whose default redirect is this
route (``cairn/core/oauth/broker.py``, ``CAIRN_OAUTH_REDIRECT_URI``). Split out of the
ask router, which is the platform's; the handler is unchanged.
"""
from html import escape

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse


def build_oauth_router() -> APIRouter:
    """The OAuth redirect target for the built-in AuthlibBroker (#116/#310)."""
    r = APIRouter()

    @r.get("/oauth/callback", response_class=HTMLResponse)
    async def oauth_callback(state: str = "", code: str = "", error: str = "") -> str:
        if error:
            # Every value echoed into this page is escaped: the route is reachable without a token
            # (the provider's redirect carries none), so an unescaped `error` would be reflected script.
            return f"<h3>OAuth error: {escape(error)}</h3>"
        if not state or not code:
            raise HTTPException(status_code=400, detail="missing state or code")
        from cairn.core.oauth.broker import AuthlibBroker  # lazy: [oauth] extra
        try:
            cid = await AuthlibBroker().complete_authorization(state=state, code=code)
        except Exception as e:  # noqa: BLE001 — show the failure in the browser tab
            return f"<h3>Authorization failed: {escape(str(e))}</h3>"
        return f"<h3>✓ Connected: {escape(str(cid))}. You can close this tab.</h3>"

    return r
