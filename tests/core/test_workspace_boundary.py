"""A workspace is a security boundary, not just a filing label (#1610, ADR-0094 D3).

Found by probe (#1609): `_WorkspaceMiddleware` took the workspace from the URL
or the `X-Cairn-Workspace` header and set it active with NO membership check,
no account check, nothing. Data was correctly *filtered* by workspace and not
*protected* by it — fine under ADR-0054 (one account, admin-invite, everyone is
one company's staff), fatal the moment self-serve signup puts two companies on
one deployment.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.workspace import (
    DEFAULT_WORKSPACE,
    InMemoryWorkspaceStore,
    Workspace,
    WorkspaceForbidden,
    assert_workspace_allowed,
)


async def _store():
    s = InMemoryWorkspaceStore()
    await s.put(Workspace(id="acme-ws", name="Acme", account_id="acme"))
    await s.put(Workspace(id="globex-ws", name="Globex", account_id="globex"))
    return s


# ── the boundary ─────────────────────────────────────────────────────────────

async def test_a_member_may_use_their_own_accounts_workspace():
    await assert_workspace_allowed("acme-ws", "acme", store=await _store())


async def test_another_account_may_not():
    """THE bug. Before #1610 this was allowed — one header and you were in."""
    with pytest.raises(WorkspaceForbidden):
        await assert_workspace_allowed("acme-ws", "globex", store=await _store())


# ── the three deliberate permissive paths ────────────────────────────────────

async def test_auth_off_is_permissive():
    """Keyless dev / single-node OSS has no tenant boundary to enforce, and
    inventing one here would break every existing install."""
    await assert_workspace_allowed("acme-ws", None, store=await _store())
    await assert_workspace_allowed("acme-ws", "", store=await _store())


async def test_no_workspace_asserted_is_permissive():
    await assert_workspace_allowed(None, "acme", store=await _store())
    await assert_workspace_allowed("", "acme", store=await _store())


async def test_with_auth_off_a_missing_workspace_is_permitted():
    """Auth off (no JWT secret in this module's environment): no tenant boundary, so
    a missing workspace is not refused here. With auth on it is refused exactly like
    another account's (tests/core/test_unknown_workspace_404.py), so the two fail
    the same way and existence never leaks."""
    await assert_workspace_allowed("no-such-ws", "acme", store=await _store())


async def test_a_legacy_workspace_with_no_account_belongs_to_default():
    """Rows written before #1610 have no stamped account. They must keep
    working for the deployment they already belong to."""
    s = InMemoryWorkspaceStore()
    await s.put(Workspace(id="old"))                      # account_id defaults
    await assert_workspace_allowed("old", "default", store=s)
    with pytest.raises(WorkspaceForbidden):
        await assert_workspace_allowed("old", "someone-else", store=s)


# ── the model ────────────────────────────────────────────────────────────────

def test_a_workspace_is_bound_to_an_account():
    assert Workspace(id="x").account_id == "default"
    assert Workspace(id="x", account_id="acme").account_id == "acme"


def test_a_row_serialized_before_the_field_existed_still_hydrates():
    """Adding a required field would make every existing workspace unreadable —
    the #1601 failure shape. This one defaults instead."""
    legacy = '{"id":"old","name":"Legacy"}'
    assert Workspace.model_validate_json(legacy).account_id == "default"


def test_the_default_workspace_is_in_the_default_account():
    assert InMemoryWorkspaceStore()._ws[DEFAULT_WORKSPACE].account_id == "default"


# ── both spellings must agree ────────────────────────────────────────────────

def test_the_middleware_lifts_the_workspace_from_either_spelling():
    """The middleware accepts BOTH the `/workspaces/{id}/…` path form and the
    `X-Cairn-Workspace` header. Authorization has to cover both or the weaker
    one becomes the way in — so this pins that the path parser and the header
    fallback resolve the same id.
    """
    from cairn.core.triggers.server import _WorkspaceMiddleware as M

    ws, rewritten = M._workspace_from_path("/workspaces/acme-ws/runs")
    assert ws == "acme-ws"
    assert rewritten == "/runs", "the flat handlers serve it unchanged"

    # the bare collection routes are NOT workspace-scoped and must pass through
    assert M._workspace_from_path("/workspaces")[0] is None
    assert M._workspace_from_path("/workspaces/acme-ws")[0] is None
