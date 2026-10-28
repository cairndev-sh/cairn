"""Self-serve signup (#1613, ADR-0094 D5/D6).

ADR-0054 chose admin-invite and no public signup — right for a self-hosted box
holding an audit trail, wrong as a permanent answer to "how does a company get
started". This opens the door behind a knob that is CLOSED by default, so an
existing deployment that upgrades gains no new attack surface.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core import account as A
from cairn.core import signup as S


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.delenv("OBS_SIGNUP", raising=False)
    yield


# ── the knob ─────────────────────────────────────────────────────────────────

def test_the_default_is_closed():
    """The whole point of the default. An operator who upgrades must not
    silently gain a public registration page."""
    assert S.signup_mode() == "invite"
    assert S.DEFAULT_SIGNUP_MODE == "invite"


@pytest.mark.parametrize("mode", ["invite", "domain", "open"])
def test_each_mode_is_selectable(mode, monkeypatch):
    monkeypatch.setenv("OBS_SIGNUP", mode)
    assert S.signup_mode() == mode


def test_a_typo_fails_CLOSED(monkeypatch):
    """The failure mode of a mistyped env var must be "the door stays shut" —
    never "the door is open", and never "the server will not boot"."""
    monkeypatch.setenv("OBS_SIGNUP", "opne")
    assert S.signup_mode() == "invite"


# ── first-run bootstrap (D6) ─────────────────────────────────────────────────

async def test_the_first_person_gets_in_even_when_closed():
    """Otherwise an invite-only deployment has nobody to send the first
    invite — the bootstrapping paradox."""
    assert await S.is_first_run()
    acct, user, created = await S.signup("Acme Inc", "founder@acme.com", "h")
    assert created and acct.name == "Acme Inc"
    members = await A.get_account_store().list_members(acct.id)
    assert [m.role for m in members] == ["owner"], "the founder owns the account"


async def test_the_seeded_default_account_does_not_count_as_used():
    """Every store seeds `default` on open, so its presence proves nothing."""
    assert await S.is_first_run()


async def test_the_second_signup_is_refused_when_closed():
    await S.signup("Acme", "founder@acme.com", "h")
    with pytest.raises(S.SignupError) as e:
        await S.signup("Globex", "someone@globex.com", "h")
    assert e.value.reason == "signup_closed"


# ── open ─────────────────────────────────────────────────────────────────────

async def test_open_lets_a_second_company_in(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "open")
    acct, _, created = await S.signup("Globex", "boss@globex.com", "h")
    assert created and acct.id != "acme"


# ── domain ───────────────────────────────────────────────────────────────────

async def test_domain_joins_the_matching_account(monkeypatch):
    acme, _, _ = await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    acct, user, created = await S.signup("", "colleague@acme.com", "h")
    assert not created and acct.id == acme.id, "joined, not a new account"
    assert await A.get_account_store().get_account_for_user(user.id) == acme.id


async def test_domain_refuses_an_unrecognised_domain(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    with pytest.raises(S.SignupError) as e:
        await S.signup("", "stranger@globex.com", "h")
    assert e.value.reason == "domain_not_registered"


async def test_a_domain_joiner_is_a_member_not_an_owner(monkeypatch):
    """Joining a company must not make you its owner."""
    acme, _, _ = await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    _, user, _ = await S.signup("", "colleague@acme.com", "h")
    roles = {m.user_id: m.role for m in await A.get_account_store().list_members(acme.id)}
    assert roles[user.id] == "member"


# ── public mail domains (#2351) ──────────────────────────────────────────────

async def test_a_second_gmail_signup_does_not_join_the_first(monkeypatch):
    """The first Gmail founder must not own a tenant every Gmail user joins."""
    first, _, _ = await S.signup("Solo", "founder@gmail.com", "h")
    assert first.domain == "", "a public domain is never registered"
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    with pytest.raises(S.SignupError) as e:
        await S.signup("", "stranger@gmail.com", "h")
    assert e.value.reason == "public_domain"
    assert await A.get_account_store().get_user_by_username("stranger@gmail.com") is None
    assert [m.role for m in await A.get_account_store().list_members(first.id)] == ["owner"]


async def test_an_account_that_claimed_a_public_domain_before_the_fix_is_no_door(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    await A.get_account_store().put_account(A.Account(id="legacy", domain="gmail.com"))
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    with pytest.raises(S.SignupError) as e:
        await S.signup("", "stranger@gmail.com", "h")
    assert e.value.reason == "public_domain"


async def test_open_mode_creates_a_public_domain_account_claiming_no_domain(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "open")
    acct, _, created = await S.signup("Side Project", "me@outlook.com", "h")
    assert created and acct.domain == ""


async def test_a_company_domain_still_self_serves(monkeypatch):
    acme, _, _ = await S.signup("Acme", "founder@acme.com", "h")
    assert acme.domain == "acme.com"
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    acct, _, created = await S.signup("", "colleague@acme.com", "h")
    assert not created and acct.id == acme.id


# ── input handling ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["", "not-an-email", "@nodomain", "no-at-sign.com"])
async def test_a_bad_email_is_refused(bad):
    with pytest.raises(S.SignupError) as e:
        await S.signup("Acme", bad, "h")
    assert e.value.reason == "bad_email"


async def test_signing_up_twice_with_the_same_email_is_refused(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    monkeypatch.setenv("OBS_SIGNUP", "open")
    with pytest.raises(S.SignupError) as e:
        await S.signup("Acme Again", "founder@acme.com", "h")
    assert e.value.reason == "already_registered"


def test_email_domain_extraction():
    assert S.email_domain("A.User@Acme.COM") == "acme.com"
    assert S.email_domain("nope") == ""


async def test_the_default_account_id_is_reserved_even_when_its_row_is_absent():
    """`default` is the deployment's own account: sign-up never hands it out,
    whether or not the store has seeded its row yet."""
    class _Empty:
        async def get_account(self, _id):
            return None

    assert await S._free_account_id(A.DEFAULT_ACCOUNT, store=_Empty()) == f"{A.DEFAULT_ACCOUNT}-2"
    assert await S._free_account_id("acme", store=_Empty()) == "acme"


async def test_a_company_named_default_does_not_get_the_default_account():
    acct, _, created = await S.signup("Default", "founder@acme.com", "h")
    assert created and acct.id != A.DEFAULT_ACCOUNT


async def test_a_missing_company_falls_back_to_the_domain():
    acct, _, _ = await S.signup("", "founder@acme.com", "h")
    assert acct.name == "acme.com"


# ── an admin's explicit claim (#2367) ────────────────────────────────────────

async def test_an_admin_claim_of_a_public_domain_is_a_door(monkeypatch):
    """A company that really runs its mail on gmail.com can be registered — by
    an admin, explicitly; the claim names who made it."""
    acme, _, _ = await S.signup("Acme", "founder@acme.com", "h")
    claimed = await S.set_account_domain(acme.id, "Gmail.com", claimed_by="founder@acme.com")
    assert (claimed.domain, claimed.domain_claimed_by) == ("gmail.com", "founder@acme.com")
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    acct, user, created = await S.signup("", "colleague@gmail.com", "h")
    assert not created and acct.id == acme.id
    roles = {m.user_id: m.role for m in await A.get_account_store().list_members(acme.id)}
    assert roles[user.id] == "member"


async def test_an_admin_claim_wins_over_an_automatic_one_and_clearing_closes_it(monkeypatch):
    await S.signup("Acme", "founder@acme.com", "h")
    await A.get_account_store().put_account(A.Account(id="legacy", domain="gmail.com"))  # automatic, pre-#2351
    await A.get_account_store().put_account(A.Account(id="gco", name="G Co"))
    await S.set_account_domain("gco", "gmail.com", claimed_by="boss")
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    acct, _, _ = await S.signup("", "a@gmail.com", "h")
    assert acct.id == "gco"
    cleared = await S.set_account_domain("gco", "", claimed_by="boss")
    assert (cleared.domain, cleared.domain_claimed_by) == ("", "")
    with pytest.raises(S.SignupError) as e:
        await S.signup("", "b@gmail.com", "h")
    assert e.value.reason == "public_domain", "the automatic claim is still no door"


async def test_a_domain_another_account_holds_is_refused():
    acme, _, _ = await S.signup("Acme", "founder@acme.com", "h")
    await A.get_account_store().put_account(A.Account(id="rival", name="Rival"))
    with pytest.raises(S.SignupError) as e:
        await S.set_account_domain("rival", "acme.com", claimed_by="mallory")
    assert e.value.reason == "domain_taken"
    # an automatic claim of a PUBLIC domain is not honoured, so it holds nothing
    await A.get_account_store().put_account(A.Account(id="legacy", domain="gmail.com"))
    assert (await S.set_account_domain("rival", "gmail.com", claimed_by="r")).domain == "gmail.com"


@pytest.mark.parametrize("bad", ["not a domain", "acme", "-acme.com", "acme..com", "a@b.com"])
async def test_a_malformed_domain_is_refused(bad):
    await A.get_account_store().put_account(A.Account(id="acme", name="Acme"))
    with pytest.raises(S.SignupError) as e:
        await S.set_account_domain("acme", bad, claimed_by="boss")
    assert e.value.reason == "bad_domain"


async def test_the_claim_survives_the_store_round_trip():
    """The marker rides the JSON blob every account store persists."""
    await A.get_account_store().put_account(A.Account(id="acme", name="Acme"))
    await S.set_account_domain("acme", "@acme.io", claimed_by="boss")
    got = await A.get_account_store().get_account("acme")
    assert (got.domain, got.domain_claimed_by) == ("acme.io", "boss")


# ── the deployment admin's name ─────────────────────────────────────────────

@pytest.mark.parametrize("given", ["ops@acme.com", "  OPS@Acme.com ", "Ops@ACME.COM"])
async def test_signup_never_creates_a_user_under_the_env_admin_name(given, monkeypatch):
    """Login tries the store before the env admin, so a self-served user under
    the admin's name would stand in front of the deployment admin's sign-in.
    The name reads as registered, whatever the case or surrounding space, and
    even on an empty deployment (the first-run bootstrap does not apply)."""
    monkeypatch.setenv("CAIRN_API_USER", "ops@acme.com")
    monkeypatch.setenv("OBS_SIGNUP", "open")
    with pytest.raises(S.SignupError) as e:
        await S.signup("Acme", given, "h")
    assert e.value.reason == "already_registered"
    assert await A.get_account_store().list_users() == []
    assert await S.is_first_run(), "nothing was created"


async def test_signup_admin_name_check_reads_the_configured_name(monkeypatch):
    """Only the configured name is held: another address still signs up."""
    monkeypatch.setenv("CAIRN_API_USER", "ops@acme.com")
    acct, user, created = await S.signup("Acme", "founder@acme.com", "h")
    assert created and user.username == "founder@acme.com"


def test_signup_endpoint_refuses_the_env_admin_name(tmp_path, monkeypatch):
    """The HTTP door: 400 with the same reason, and no account appears."""
    import types

    from fastapi.testclient import TestClient

    from cairn.core import workspace as _ws
    from cairn.core.triggers.server import build_app

    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    monkeypatch.setenv("CAIRN_API_USER", "ops@acme.com")

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    c = TestClient(build_app(FakeRunner(), enable_scheduler=False))
    r = c.post("/auth/signup", json={"company": "Acme", "email": " OPS@acme.com", "password": "pw12345"})
    assert (r.status_code, r.json().get("detail")) == (400, "already_registered")
    assert c.get("/auth/signup").json()["first_run"] is True
