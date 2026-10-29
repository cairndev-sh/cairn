"""Self-serve signup (#1613, ADR-0094 D5/D6).

ADR-0054 chose admin-invite and **no public signup**, and the console's login
page said so: *"No account? Ask your instance admin."* That is the right posture
for a self-hosted box holding somebody's audit trail and credentials — a
deployment must never accumulate strangers' accounts because someone found the
URL.

But it leaves two problems unsolved, and they are different problems:

* how does a NEW COMPANY get an account (bootstrapping), and
* how does a COLLEAGUE join an existing one — which is #1612's invite links.

This module answers the first, behind a knob that is **closed by default**, so
an existing deployment that upgrades gains no new attack surface until an
operator opts in.

Modes (``CAIRN_SIGNUP``):

``invite``  the default. Nobody self-serves; joining is by invite link only.
``domain``  a work email matching an account's verified domain self-serves
            into THAT account. The expected steady state for real teams: it
            takes the admin out of the loop without opening the door. A public
            mail domain (gmail.com …) is never a company's (#2351).
``open``    anyone creates an account. Hosted demo/cloud only.

**First-run bootstrap** (D6) cuts across all three: on a deployment with no
real account yet, the first person to sign up becomes ``owner`` of a new
account regardless of mode. Without it an invite-only system has nobody to send
the first invite — the bootstrapping paradox.

**An identity method is not a door** (#2336, ADR-0119). Signing up with Google
or GitHub comes through this same function with an :class:`Identity` instead
of a password hash, so ``CAIRN_SIGNUP`` and the bootstrap decide exactly as they
do for a password.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
import re
from typing import Any, Optional

from cairn.core.account import (
    DEFAULT_ACCOUNT,
    Account,
    Identity,
    User,
    add_member,
    get_account_store,
    is_env_admin_username,
)

#: The three doors. See the module docstring for what each is for.
SIGNUP_MODES = ("invite", "domain", "open")

#: Closed by default — an upgrade must not silently publish a registration page.
DEFAULT_SIGNUP_MODE = "invite"


def signup_mode() -> str:
    """The configured mode, defaulting to ``invite``.

    An unrecognised value falls back to the DEFAULT rather than raising: the
    failure mode of a typo in a deployment env var should be "the door stays
    shut", never "the door is open" or "the server will not boot".
    """
    raw = (getenv("CAIRN_SIGNUP") or "").strip().lower()
    return raw if raw in SIGNUP_MODES else DEFAULT_SIGNUP_MODE


class SignupError(Exception):
    """Signup refused. ``reason`` is machine-readable so the console can say
    WHICH refusal this is instead of a generic failure."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


_EMAIL = re.compile(r"^[^@\s]+@([^@\s]+\.[^@\s]+)$")

#: #2351 — consumer mail providers. A domain here is shared by strangers, so it
#: never identifies a company: it is never registered as an account's domain,
#: and under ``domain`` mode a sign-up from one is refused (``public_domain``)
#: exactly as ``invite`` would refuse it. Otherwise the first Gmail founder owns
#: a tenant every later Gmail user self-serves into. A constant, not a knob: the
#: failure mode of a missing entry is "domain join refused", never "joined".
PUBLIC_MAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "msn.com", "yahoo.com", "ymail.com", "icloud.com", "me.com", "mac.com",
    "aol.com", "proton.me", "protonmail.com", "gmx.com", "gmx.de", "web.de",
    "mail.com", "yandex.com", "zoho.com", "fastmail.com", "hey.com", "qq.com",
    "163.com",
})


def email_domain(email: str) -> str:
    """The domain part of a work email, lowercased. ``""`` if not an email."""
    m = _EMAIL.match((email or "").strip().lower())
    return m.group(1) if m else ""


async def is_first_run(*, store: Any = None) -> bool:
    """True when no real account exists yet.

    The implicit ``default`` account is seeded by every store on open, so its
    presence proves nothing — it is excluded deliberately. What makes a
    deployment "used" is a real account, or any user at all.
    """
    store = store or get_account_store()
    accounts = [a for a in await store.list_accounts() if a.id != DEFAULT_ACCOUNT]
    if accounts:
        return False
    return not await store.list_users()


async def signup(
    company: str, email: str, password_hash: str, *, store: Any = None,
    identity: Optional[Identity] = None,
) -> tuple[Account, User, bool]:
    """Create an account + its first user, or join an existing one by domain.

    Returns ``(account, user, created_account)``. ``password_hash`` arrives
    already hashed — this module never sees a plaintext password.

    ``identity`` (#2336) replaces the password for a Google/GitHub sign-up: the
    user is created with that identity linked and NO password. The caller must
    have checked that the provider verified ``email`` — this function decides
    only whether the door is open, never whether the email is real.

    Raises :class:`SignupError` with a reason the console can explain.
    """
    store = store or get_account_store()
    addr = (email or "").strip().lower()
    domain = email_domain(addr)
    if not domain:
        raise SignupError("bad_email")
    if not password_hash and identity is None:
        raise SignupError("password_required")
    # The deployment admin's name is held by the env admin, so it reads as
    # registered (an OAuth landing arrives here too).
    if is_env_admin_username(addr) or await store.get_user_by_username(addr) is not None:
        raise SignupError("already_registered")

    mode = signup_mode()
    first_run = await is_first_run(store=store)

    # D6 — the first person on an empty deployment always gets in, whatever the
    # mode says, or an invite-only install can never issue its first invite.
    if not first_run:
        if mode == "invite":
            raise SignupError("signup_closed")
        if mode == "domain":
            # Only an HONOURED claim is a door: an account that registered a
            # public domain automatically (before #2351) is not one, an admin's
            # explicit claim of it is (#2367).
            existing = await _account_for_domain(domain, store=store)
            if existing is None:
                raise SignupError("public_domain" if domain in PUBLIC_MAIL_DOMAINS
                                  else "domain_not_registered")
            user = await store.put_user(_new_user(addr, password_hash, identity))
            await add_member(existing.id, user.id, "member", store=store)
            return existing, user, False

    name = (company or "").strip() or domain
    # A public domain names no company, so the account claims none (#2351).
    claimed = "" if domain in PUBLIC_MAIL_DOMAINS else domain
    acct = await store.put_account(
        Account(id=await _free_account_id(_slug(name), store=store), name=name, domain=claimed))
    user = await store.put_user(_new_user(addr, password_hash, identity))
    # The founder is `owner`, not `admin`: an account must always have someone
    # who can hand over ownership, and that cannot be a role you can be demoted
    # out of by a peer.
    await add_member(acct.id, user.id, "owner", store=store)
    return acct, user, True


def _new_user(addr: str, password_hash: str, identity: Optional[Identity]) -> User:
    return User(username=addr, password_hash=password_hash or "",
                identities=[identity] if identity is not None else [])


async def _free_account_id(base: str, *, store: Any) -> str:
    """``base``, or ``base-2``, ``base-3``… — the first id no account holds.

    ``put_account`` is an upsert: two companies whose names slug alike (two
    "Acme"s, or two sign-ups with no company name from the same email domain —
    the norm for a Google sign-up, #2336) used to land on ONE account id, and
    the second founder became an owner of the first company's tenant."""
    candidate, n = base, 1
    while await store.get_account(candidate) is not None:
        n += 1
        candidate = f"{base}-{n}"
    return candidate


def claim_is_honoured(acct: Account) -> bool:
    """Whether ``acct``'s domain is a door under ``domain`` mode: an admin's
    explicit claim always (#2367); an automatic one unless the domain is public
    mail (#2351) or the account is the implicit ``default``."""
    if not acct.domain:
        return False
    if acct.domain_claimed_by:
        return True
    return acct.id != DEFAULT_ACCOUNT and acct.domain.strip().lower() not in PUBLIC_MAIL_DOMAINS


async def _account_for_domain(domain: str, *, store: Any = None,
                              exclude: str = "") -> Optional[Account]:
    """The account whose HONOURED claim is ``domain``, if any. An explicit claim
    wins over an automatic one; otherwise first match — a domain is expected to
    belong to one company."""
    store = store or get_account_store()
    hits = [a for a in await store.list_accounts()
            if a.id != exclude and (a.domain or "").strip().lower() == domain and claim_is_honoured(a)]
    return next((a for a in hits if a.domain_claimed_by), hits[0] if hits else None)


_DOMAIN = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$")


async def set_account_domain(account_id: str, domain: str, *, claimed_by: str,
                             store: Any = None) -> Account:
    """An admin's explicit claim of ``domain`` for ``account_id`` (#2367), or
    ``""`` to clear it. Explicit, so it is honoured even for a public mail
    domain. Raises ``KeyError`` for an unknown account and :class:`SignupError`
    ``bad_domain`` / ``domain_taken`` (another account's honoured claim — an
    admin of one tenant must not redirect another company's sign-ups)."""
    store = store or get_account_store()
    acct = await store.get_account(account_id)
    if acct is None:
        raise KeyError(account_id)
    d = (domain or "").strip().lower().lstrip("@")
    if d and not _DOMAIN.match(d):
        raise SignupError("bad_domain")
    if d and await _account_for_domain(d, store=store, exclude=acct.id) is not None:
        raise SignupError("domain_taken")
    return await store.put_account(acct.model_copy(
        update={"domain": d, "domain_claimed_by": (claimed_by or "admin") if d else ""}))


def _slug(raw: str) -> str:
    """Account id from a company name. Mirrors the workspace slug rule."""
    s = re.sub(r"[^a-z0-9]+", "-", (raw or "").strip().lower()).strip("-")
    return re.sub(r"-{2,}", "-", s) or "account"
