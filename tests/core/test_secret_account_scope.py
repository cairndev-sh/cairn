"""Secret references are scoped to the account that owns the connection.

A tenant account's connection resolves only the references minted for that
account: never a server environment variable (a bare name or ``${env:NAME}``),
never the deployment's own ``cairn://`` secrets and never another account's.
The deployment's callers (no account, or the ``default`` account a single-node
or auth-off deployment runs in) are unchanged. Hermetic: a temp SQLite vault
with a fixed Fernet key; no network, no live provider.
"""
from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from cairn.core import secrets_store
from cairn.core.connections import Connection, connection_from_provider, resolve_connection_obj
from cairn.core.secrets_store import mint_ref, put_secret, resolve_secret


@pytest.fixture(autouse=True)
def _vault(tmp_path, monkeypatch):
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    monkeypatch.delenv("CAIRN_SECRET_BACKEND", raising=False)
    monkeypatch.setenv("CAIRN_SECRETS_DB", str(tmp_path / "secrets.db"))
    monkeypatch.setenv("CAIRN_SECRET_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("SERVER_ONLY_SECRET", "server-value")
    # no bound secrets.store provider unless a test binds one
    monkeypatch.setattr("cairn.core.operator.secrets.get_secret_provider", lambda: None)


# ── mint_ref ──────────────────────────────────────────────────────────


def test_mint_ref_namespaces_a_tenant_account():
    assert mint_ref("my-conn", "API_KEY", account_id="acct-b") == "cairn://secrets#acct-b/my-conn-API_KEY"


def test_mint_ref_for_the_deployment_is_unchanged():
    assert mint_ref("my-conn", "API_KEY") == "cairn://secrets#my-conn-API_KEY"
    assert mint_ref("my-conn", "API_KEY", account_id="default") == "cairn://secrets#my-conn-API_KEY"


def test_mint_ref_infisical_namespaces_a_tenant_account(monkeypatch):
    monkeypatch.setenv("INFISICAL_PROJECT_ID", "proj")
    assert mint_ref("c", "K", scheme="infisical", account_id="acct-b") == "infisical://proj/cairn/acct-b#c-K"
    assert mint_ref("c", "K", scheme="infisical") == "infisical://proj/cairn#c-K"


# ── resolve_secret(account_id=…) ────────────────────────────────────


def test_tenant_cannot_resolve_a_server_env_var():
    assert resolve_secret("SERVER_ONLY_SECRET", account_id="acct-b") is None
    assert resolve_secret("${env:SERVER_ONLY_SECRET}", account_id="acct-b") is None


def test_deployment_still_resolves_env_refs():
    assert resolve_secret("SERVER_ONLY_SECRET") == "server-value"
    assert resolve_secret("${env:SERVER_ONLY_SECRET}") == "server-value"
    assert resolve_secret("SERVER_ONLY_SECRET", account_id="default") == "server-value"


def test_tenant_reads_its_own_secret_but_not_another_accounts():
    ref_a = put_secret(mint_ref("c", "API_KEY", account_id="acct-a"), "value-a", account_id="acct-a")
    ref_b = put_secret(mint_ref("c2", "API_KEY", account_id="acct-b"), "value-b", account_id="acct-b")
    assert resolve_secret(ref_b, account_id="acct-b") == "value-b"  # its own
    assert resolve_secret(ref_a, account_id="acct-b") is None  # another account's
    assert resolve_secret(ref_a, account_id="acct-a") == "value-a"


def test_tenant_cannot_resolve_a_deployment_secret():
    legacy = put_secret(mint_ref("ops", "API_KEY"), "deployment-value")
    assert legacy == "cairn://secrets#ops-API_KEY"
    assert resolve_secret(legacy, account_id="acct-b") is None
    # the deployment keeps reading its own (and pre-existing) refs
    assert resolve_secret(legacy) == "deployment-value"
    assert resolve_secret(legacy, account_id="default") == "deployment-value"


def test_tenant_prefix_of_another_account_does_not_match():
    ref = put_secret(mint_ref("c", "K", account_id="acct-ab"), "v", account_id="acct-ab")
    assert resolve_secret(ref, account_id="acct-a") is None


@pytest.mark.parametrize("ref", [
    "cairn://secrets#acct-b",            # the account name alone, no "/<conn>-<KEY>"
    "cairn://secrets#acct-b/",           # an empty key after the account
    "cairn://other#acct-b/c-K",          # a different locator
    "cairn://#acct-b/c-K",               # no locator
    "CAIRN://secrets#acct-b/c-K",        # upper-case scheme is not the cairn scheme
    "Cairn://secrets#acct-b/c-K",
])
def test_tenant_malformed_cairn_refs_are_refused(ref):
    # the deployment holds a value under every key these could be read as
    put_secret("cairn://secrets#acct-b", "deployment-acct-b")
    put_secret("cairn://secrets#acct-b/c-K", "tenant-value", account_id="acct-b")
    assert resolve_secret(ref, account_id="acct-b") is None
    assert secrets_store.ref_in_account(ref, "acct-b") is False
    with pytest.raises(ValueError):
        put_secret(ref, "x", account_id="acct-b")
    # the well-formed ref still resolves for its own account
    assert resolve_secret("cairn://secrets#acct-b/c-K", account_id="acct-b") == "tenant-value"


def test_tenant_infisical_ref_outside_its_namespace_never_reaches_the_backend(monkeypatch):
    monkeypatch.setenv("INFISICAL_PROJECT_ID", "proj")

    reads: list[str] = []

    def _get(self, locator, key):
        reads.append(f"{locator}#{key}")
        return "value"

    monkeypatch.setattr(secrets_store.InfisicalSecretBackend, "get", _get)
    assert resolve_secret("infisical://other/cairn/acct-b#c-K", account_id="acct-b") is None
    assert resolve_secret("infisical://proj/cairn#c-K", account_id="acct-b") is None
    assert resolve_secret("infisical://proj/cairn/acct-a#c-K", account_id="acct-b") is None
    assert reads == []  # refused before any backend read
    assert resolve_secret("infisical://proj/cairn/acct-b#c-K", account_id="acct-b") == "value"
    assert reads == ["proj/cairn/acct-b#c-K"]


def test_put_secret_refuses_a_ref_outside_the_tenant():
    with pytest.raises(ValueError):
        put_secret(mint_ref("c", "K", account_id="acct-a"), "x", account_id="acct-b")
    with pytest.raises(ValueError):
        put_secret(mint_ref("c", "K"), "x", account_id="acct-b")  # the deployment's namespace
    # the deployment writes anywhere, as before
    assert put_secret(mint_ref("c", "K", account_id="acct-a"), "x") == "cairn://secrets#acct-a/c-K"


# ── connection_from_provider(account_id=…) ─────────────────────────


def test_tenant_connection_drops_the_catalog_env_defaults():
    conn = connection_from_provider("openai", account_id="acct-b")
    assert conn.account_id == "acct-b"
    assert conn.secrets == {}  # no OPENAI_API_KEY pointer to the server env


def test_tenant_connection_refuses_a_caller_supplied_env_ref():
    with pytest.raises(ValueError):
        connection_from_provider("openai", secrets={"API_KEY": "SERVER_ONLY_SECRET"}, account_id="acct-b")
    with pytest.raises(ValueError):
        connection_from_provider(
            "openai", secrets={"API_KEY": mint_ref("x", "API_KEY", account_id="acct-a")}, account_id="acct-b"
        )


def test_tenant_connection_keeps_its_own_minted_ref():
    ref = mint_ref("openai", "API_KEY", account_id="acct-b")
    conn = connection_from_provider("openai", secrets={"API_KEY": ref}, account_id="acct-b")
    assert conn.secrets == {"API_KEY": ref}


def test_deployment_connection_from_provider_is_unchanged():
    assert connection_from_provider("openai").secrets == {"API_KEY": "OPENAI_API_KEY"}
    conn = connection_from_provider("openai", secrets={"API_KEY": "MY_KEY"}, account_id="default")
    assert conn.secrets == {"API_KEY": "MY_KEY"} and conn.account_id == "default"


# ── resolve_connection_obj: the row's account scopes resolution ────


@pytest.mark.asyncio
async def test_tenant_connection_resolves_only_its_own_refs(monkeypatch):
    other = put_secret(mint_ref("x", "K", account_id="acct-a"), "value-a", account_id="acct-a")
    own = put_secret(mint_ref("mine", "K", account_id="acct-b"), "value-b", account_id="acct-b")
    deployment = put_secret(mint_ref("ops", "K"), "deployment-value")

    class _EnvProvider:  # the default secrets.store binding reads os.environ
        def get(self, name):
            return {"SERVER_ONLY_SECRET": "server-value"}[name]

        def available_keys(self):
            return ["SERVER_ONLY_SECRET"]

    monkeypatch.setattr("cairn.core.operator.secrets.get_secret_provider", lambda: _EnvProvider())
    conn = Connection(
        name="mine", kind="api_key", account_id="acct-b",
        secrets={"BARE": "SERVER_ONLY_SECRET", "ENV": "${env:SERVER_ONLY_SECRET}",
                 "OTHER": other, "DEPLOY": deployment, "OWN": own},
    )
    resolved = await resolve_connection_obj(conn)
    assert resolved.secrets == {"BARE": "", "ENV": "", "OTHER": "", "DEPLOY": "", "OWN": "value-b"}


@pytest.mark.asyncio
async def test_deployment_connection_resolution_is_unchanged():
    deployment = put_secret(mint_ref("ops", "K"), "deployment-value")
    conn = Connection(
        name="ops", kind="api_key",
        secrets={"BARE": "SERVER_ONLY_SECRET", "ENV": "${env:SERVER_ONLY_SECRET}", "DEPLOY": deployment},
    )
    resolved = await resolve_connection_obj(conn)
    assert resolved.secrets == {"BARE": "server-value", "ENV": "server-value", "DEPLOY": "deployment-value"}


@pytest.mark.asyncio
async def test_tenant_oauth_connection_reads_only_its_own_token(monkeypatch):
    seen: list[str] = []

    class _Broker:
        async def get_access_token(self, *, provider, connection_id):
            seen.append(connection_id)
            return "tok"

    monkeypatch.setattr("cairn.core.oauth.get_oauth_broker", lambda name=None: _Broker())
    tenant = Connection(name="mine", kind="oauth", account_id="acct-b",
                        config={"provider": "github", "connection_id": "someone-elses"})
    deployment = Connection(name="ops", kind="oauth",
                            config={"provider": "github", "connection_id": "shared-github"})
    await resolve_connection_obj(tenant)
    await resolve_connection_obj(deployment)
    assert seen == ["mine", "shared-github"]
