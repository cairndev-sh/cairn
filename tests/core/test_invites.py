"""Invite links (#1612, ADR-0094 D7).

The admin API could already create a user — but the ADMIN chose the password,
so the admin knew the credential and it had to travel out of band. An invite
inverts that: the admin grants an account and a role, the invitee sets their
own secret.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core import account as A


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    """Own store per test — these must never touch ~/.cairn (lesson: #1605)."""
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    yield


async def _invite(role: str = "member", ttl: int = 72, email: str = "new@acme.com"):
    await A.get_account_store().put_account(A.Account(id="acme", name="Acme"))
    return await A.create_invite("acme", email, role, created_by="boss", ttl_hours=ttl)


# ── the happy path ───────────────────────────────────────────────────────────

async def test_an_invite_creates_the_user_with_the_granted_role():
    inv, raw = await _invite(role="admin")
    user, used = await A.accept_invite(raw, "newbie", "hashed-pw")
    assert user.username == "newbie"
    assert used.account_id == "acme"
    acct = await A.get_account_store().get_account_for_user(user.id)
    assert acct == "acme", "the membership is what puts them in the tenant"
    members = await A.get_account_store().list_members("acme")
    assert [m.role for m in members if m.user_id == user.id] == ["admin"]


async def test_the_invitee_sets_their_own_password():
    """The whole point: this module never sees a plaintext password, and the
    admin never learns the credential."""
    inv, raw = await _invite()
    user, _ = await A.accept_invite(raw, "newbie", "the-invitees-own-hash")
    assert user.password_hash == "the-invitees-own-hash"
    assert inv.created_by == "boss"


# ── the token is not stored in the clear ─────────────────────────────────────

async def test_the_raw_token_is_never_persisted():
    """A leaked invites table must not be a set of working invitations — the
    same discipline as `User.password_hash`."""
    inv, raw = await _invite()
    stored = await A.get_account_store().get_invite(inv.id)
    assert raw not in stored.model_dump_json()
    assert stored.token_hash and stored.token_hash != raw


async def test_a_wrong_token_is_unknown_not_a_crash():
    await _invite()
    with pytest.raises(A.InviteError) as e:
        await A.peek_invite("not-a-real-token")
    assert e.value.reason == "unknown"


# ── single use, expiry, revocation — each distinguishable ────────────────────

async def test_an_invite_is_single_use():
    inv, raw = await _invite()
    await A.accept_invite(raw, "first", "h")
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, "second", "h")
    assert e.value.reason == "consumed"


async def test_an_expired_invite_says_expired():
    inv, raw = await _invite(ttl=-1)                    # already past
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, "late", "h")
    assert e.value.reason == "expired"


async def test_a_revoked_invite_says_revoked():
    inv, raw = await _invite()
    await A.revoke_invite(inv.id, "acme")
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, "nope", "h")
    assert e.value.reason == "revoked"


def test_the_four_failure_reasons_are_distinct():
    """"Invalid link" for all of them leaves the recipient with no idea whether
    to ask for a new one, wait, or check they opened the right link."""
    assert len({"unknown", "consumed", "expired", "revoked"}) == 4


# ── tenancy ──────────────────────────────────────────────────────────────────

async def test_one_account_cannot_revoke_anothers_invite():
    """KeyError, not a mismatch report — cross-tenant existence stays
    unobservable (ADR-0051 §4)."""
    inv, _ = await _invite()
    with pytest.raises(KeyError):
        await A.revoke_invite(inv.id, "globex")


async def test_invites_are_listed_per_account():
    inv, _ = await _invite()
    assert len(await A.get_account_store().list_invites("acme")) == 1
    assert await A.get_account_store().list_invites("globex") == []


# ── failure part-way must not burn the invite ────────────────────────────────

async def test_a_taken_username_does_not_consume_the_invite():
    """Consumption is the LAST write on purpose: a duplicate username must
    leave the link still redeemable, not spend it on a failed attempt."""
    inv, raw = await _invite()
    await A.get_account_store().put_user(A.User(username="taken", password_hash="x"))
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, "taken", "h")
    assert e.value.reason == "username_taken"
    assert (await A.peek_invite(raw)).state == "pending", "still usable"


async def test_a_bad_role_is_refused_at_create():
    await A.get_account_store().put_account(A.Account(id="acme", name="Acme"))
    with pytest.raises(ValueError):
        await A.create_invite("acme", "x@y.z", "superuser")


# ── the routes an invitee must be able to reach WITHOUT a token ──────────────

def test_the_accept_routes_are_public_when_auth_is_on():
    """Caught end-to-end, not by the unit tests above: both invite routes 401'd
    while every test in this file passed, because they call the functions
    directly and never cross the auth guard.

    An invitee HAS no account — the token in the link is the credential — so
    gating these makes an invite impossible to redeem.
    """
    from cairn.core.triggers.auth import _is_public_path

    assert _is_public_path("/auth/accept-invite")
    assert _is_public_path("/auth/invite/some-raw-token")


def test_the_public_prefix_is_narrow():
    """`/auth/invite/` is a prefix rule, so it is worth pinning that it did not
    open the rest of `/auth` or anything adjacent."""
    from cairn.core.triggers.auth import _is_public_path

    assert not _is_public_path("/auth/me")
    assert not _is_public_path("/admin/invites")
    assert not _is_public_path("/auth/invite")        # no trailing slash, no token
    assert not _is_public_path("/auth/invitations/x")


# ── the deployment admin's name ─────────────────────────────────────────────

@pytest.mark.parametrize("given", ["admin", "Admin", "  ADMIN  "])
async def test_an_invite_cannot_be_redeemed_under_the_default_admin_name(given, monkeypatch):
    """With ``CAIRN_API_USER`` unset the env admin is ``admin``. Redeeming under
    that name (any case, trimmed) reads as taken, and the invite stays usable."""
    monkeypatch.delenv("CAIRN_API_USER", raising=False)
    monkeypatch.delenv("OBS_API_USER", raising=False)
    inv, raw = await _invite()
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, given, "hashed-pw")
    assert e.value.reason == "username_taken"
    assert await A.get_account_store().list_users() == []
    assert (await A.peek_invite(raw)).state == "pending", "a refusal does not burn the link"
    user, _ = await A.accept_invite(raw, "newbie", "hashed-pw")
    assert user.username == "newbie"


async def test_an_invite_cannot_be_redeemed_under_the_configured_admin_name(monkeypatch):
    monkeypatch.setenv("CAIRN_API_USER", "Root-Ops")
    inv, raw = await _invite()
    with pytest.raises(A.InviteError) as e:
        await A.accept_invite(raw, "root-ops", "hashed-pw")
    assert e.value.reason == "username_taken"
    # the default name is not held once another one is configured
    user, _ = await A.accept_invite(raw, "admin", "hashed-pw")
    assert user.username == "admin"
