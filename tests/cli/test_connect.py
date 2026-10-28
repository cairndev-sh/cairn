"""`cairn connect` CLI (#68/#98).

End-to-end coverage of the verb against a tmp connections DB: add (defaults +
overrides), ls, the duplicate guard, unknown-provider, secret-resolution via
`test` (incl. the guarantee that a resolved secret value is never printed), and
rm. The store location is isolated per test via ``OBS_CONNECTIONS_DB`` so the
real ``~/.cairn`` is never touched.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.testing import CliRunner

from cairn.cli.commands.connect import app as connect_app
from cairn.core.oauth import AuthSession, register_broker

cli = CliRunner()


class _FakeBroker:
    """A no-network broker for the `--via oauth` CLI path (no sidecar)."""

    name = "fake-cli"

    async def start_authorization(self, *, provider, connection_id, scopes=None):
        return AuthSession(
            redirect_url=f"https://auth.example/go?cid={connection_id}",
            connection_id=connection_id,
        )

    async def get_access_token(self, *, provider, connection_id):
        return "fake-token"

    async def revoke(self, *, provider, connection_id):  # pragma: no cover
        ...


register_broker("fake-cli", _FakeBroker)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("OBS_CONNECTIONS_DB", str(tmp_path / "conns.db"))
    monkeypatch.delenv("OBS_PROFILE", raising=False)  # no SecretStore binding → env fallback
    # Hermetic: the add/test verbs now run an authenticated probe — stub it so
    # the CLI tests never touch the network. Individual probe tests override this.
    from cairn.core.connection_probe import ProbeResult
    import cairn.cli.commands.connect as connect_mod

    async def _fake_probe(name):
        return None, ProbeResult(ok=True, status=200, detail="authenticated (stub)")

    monkeypatch.setattr(connect_mod, "_probe_by_name", _fake_probe)
    # #229 — the store check asks the configured engine; these tests have none.
    import cairn.cli.commands.remote as remote_mod
    monkeypatch.setattr(remote_mod, "engine_store_fingerprint", lambda *a, **k: None)
    yield


def test_providers_lists_catalog():
    r = cli.invoke(connect_app, ["providers"])
    assert r.exit_code == 0
    assert "openai" in r.stdout
    assert "compute.docker" in r.stdout


def test_add_then_ls_roundtrip():
    r = cli.invoke(connect_app, ["add", "openai"])
    assert r.exit_code == 0
    assert "openai" in r.stdout

    r = cli.invoke(connect_app, ["ls"])
    assert r.exit_code == 0
    assert "openai" in r.stdout
    assert "api_key" in r.stdout


def test_add_with_name_and_config_override():
    r = cli.invoke(
        connect_app,
        ["add", "docker", "--name", "edge", "--set", "host=tcp://h:2375"],
    )
    assert r.exit_code == 0
    r = cli.invoke(connect_app, ["ls"])
    assert "edge" in r.stdout
    assert "compute.docker" in r.stdout


def test_duplicate_is_rejected_without_force():
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0
    dup = cli.invoke(connect_app, ["add", "openai"])
    assert dup.exit_code == 1
    assert "--force" in dup.stdout
    # --force overwrites
    assert cli.invoke(connect_app, ["add", "openai", "--force"]).exit_code == 0


def test_unknown_provider_exit_2():
    r = cli.invoke(connect_app, ["add", "not-a-provider"])
    assert r.exit_code == 2
    assert "no such provider" in r.stdout.lower()


def test_test_resolves_present_secret_without_printing_it(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-SUPER-SECRET-value")
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0
    r = cli.invoke(connect_app, ["test", "openai"])
    assert r.exit_code == 0                       # secret resolves
    assert "sk-SUPER-SECRET-value" not in r.stdout  # but is NEVER printed


def test_test_flags_missing_secret(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert cli.invoke(connect_app, ["add", "anthropic", "--name", "ant"]).exit_code == 0
    r = cli.invoke(connect_app, ["test", "ant"])
    assert r.exit_code == 1                       # ref unset → unusable


def test_test_missing_connection_exit_1():
    assert cli.invoke(connect_app, ["test", "ghost"]).exit_code == 1


def test_test_runs_authenticated_probe_and_reports_connected(monkeypatch):
    """`connect test` runs the authed probe and renders its verdict; ok → exit 0."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-present")
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0
    r = cli.invoke(connect_app, ["test", "openai"])
    assert r.exit_code == 0
    assert "connected" in r.stdout.lower()          # probe verdict rendered


def test_test_probe_failure_exit_1(monkeypatch):
    """A rejected credential (probe ok=False) → exit 1 even if the secret is set."""
    from cairn.core.connection_probe import ProbeResult
    import cairn.cli.commands.connect as connect_mod

    monkeypatch.setenv("OPENAI_API_KEY", "sk-bad")
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0

    async def _reject(name):
        return None, ProbeResult(ok=False, status=401, detail="credential rejected (HTTP 401)")

    monkeypatch.setattr(connect_mod, "_probe_by_name", _reject)
    r = cli.invoke(connect_app, ["test", "openai"])
    assert r.exit_code == 1
    assert "failed" in r.stdout.lower()


def test_test_degraded_provider_exit_0(monkeypatch):
    """A structural/SDK-only provider (probe ok=None) is reachable-or-unsupported
    → exit 0 (degraded, not a failure)."""
    from cairn.core.connection_probe import ProbeResult
    import cairn.cli.commands.connect as connect_mod

    monkeypatch.setenv("MODAL_TOKEN_ID", "id")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "sec")
    assert cli.invoke(connect_app, ["add", "modal"]).exit_code == 0

    async def _degraded(name):
        return None, ProbeResult(ok=None, status=0, detail="unsupported: SDK-only")

    monkeypatch.setattr(connect_mod, "_probe_by_name", _degraded)
    r = cli.invoke(connect_app, ["test", "modal"])
    assert r.exit_code == 0
    assert "degraded" in r.stdout.lower() or "unsupported" in r.stdout.lower()


def test_rm_roundtrip():
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0
    assert cli.invoke(connect_app, ["rm", "openai"]).exit_code == 0
    assert cli.invoke(connect_app, ["rm", "openai"]).exit_code == 1   # already gone


# ── --via oauth + authorize (broker seam, #100) ────────────────────────


def test_add_via_oauth_persists_and_prints_redirect():
    r = cli.invoke(connect_app, ["add", "github", "--via", "oauth", "--broker", "fake-cli"])
    assert r.exit_code == 0
    assert "auth.example" in r.stdout            # broker redirect URL printed
    ls = cli.invoke(connect_app, ["ls"])
    assert "github" in ls.stdout and "oauth" in ls.stdout


def test_add_via_oauth_with_name_connection_id_and_scope():
    r = cli.invoke(connect_app, [
        "add", "github", "--via", "oauth", "--broker", "fake-cli",
        "--name", "gh-work", "--connection-id", "cid-123",
        "--scope", "repo", "--scope", "read:org",
    ])
    assert r.exit_code == 0
    assert "cid-123" in r.stdout                 # custom connection_id flows to the broker


def test_add_via_oauth_dup_guard():
    assert cli.invoke(connect_app, ["add", "github", "--via", "oauth", "--broker", "fake-cli"]).exit_code == 0
    dup = cli.invoke(connect_app, ["add", "github", "--via", "oauth", "--broker", "fake-cli"])
    assert dup.exit_code == 1


def test_add_via_unknown_value_exit_2():
    assert cli.invoke(connect_app, ["add", "x", "--via", "bogus"]).exit_code == 2


def test_add_via_oauth_unknown_broker_exit_2_and_not_persisted():
    r = cli.invoke(connect_app, ["add", "slack", "--via", "oauth", "--broker", "does-not-exist"])
    assert r.exit_code == 2
    assert "slack" not in cli.invoke(connect_app, ["ls"]).stdout   # validated before persist


def test_authorize_reinitiates_existing_oauth_connection():
    assert cli.invoke(connect_app, ["add", "github", "--via", "oauth", "--broker", "fake-cli"]).exit_code == 0
    r = cli.invoke(connect_app, ["authorize", "github"])
    assert r.exit_code == 0
    assert "auth.example" in r.stdout


def test_authorize_missing_exit_1():
    assert cli.invoke(connect_app, ["authorize", "ghost"]).exit_code == 1


def test_authorize_rejects_non_oauth_connection():
    assert cli.invoke(connect_app, ["add", "openai"]).exit_code == 0
    assert cli.invoke(connect_app, ["authorize", "openai"]).exit_code == 1   # kind=api_key


# ── --preset: the shipped MCP connector presets (#2479) ────────────────


def _stored(name):
    import asyncio

    from cairn.core.connections import default_connection_store

    store = default_connection_store()
    try:
        return asyncio.run(store.get_by_name(name))
    finally:
        store.close()


def test_presets_lists_the_three_shipped_presets():
    r = cli.invoke(connect_app, ["presets"])
    assert r.exit_code == 0
    for name in ("github", "slack", "notion"):
        assert name in r.stdout


def test_add_from_preset_creates_the_mcp_connection(monkeypatch):
    monkeypatch.setenv("GITHUB_PERSONAL_ACCESS_TOKEN", "ghp-SUPER-SECRET-value")
    r = cli.invoke(connect_app, ["add", "--preset", "github"])
    assert r.exit_code == 0, r.stdout
    assert "ghp-SUPER-SECRET-value" not in r.stdout          # refs only, never values
    assert "GITHUB_PERSONAL_ACCESS_TOKEN" in r.stdout          # says which env var supplies it
    conn = _stored("github")
    assert conn.kind == "mcp"
    assert conn.config["transport"] == "stdio"
    assert conn.config["command"] == "npx"
    assert conn.config["args"] == "-y @modelcontextprotocol/server-github"
    assert conn.secrets == {"GITHUB_PERSONAL_ACCESS_TOKEN": "GITHUB_PERSONAL_ACCESS_TOKEN"}
    assert conn.description.startswith("GitHub")


def test_add_from_preset_flags_override_preset_values():
    r = cli.invoke(connect_app, [
        "add", "--preset", "slack", "--name", "team-slack",
        "--set", "command=bunx", "--secret", "SLACK_BOT_TOKEN=MY_SLACK_REF",
    ])
    assert r.exit_code == 0, r.stdout
    conn = _stored("team-slack")
    assert conn.config["command"] == "bunx"
    assert conn.secrets == {"SLACK_BOT_TOKEN": "MY_SLACK_REF", "SLACK_TEAM_ID": "SLACK_TEAM_ID"}


def test_add_unknown_preset_exit_1_and_not_persisted():
    r = cli.invoke(connect_app, ["add", "--preset", "jira"])
    assert r.exit_code == 1
    assert "no such preset" in r.stdout.lower()
    assert "github" in r.stdout                                # lists the known presets
    assert "jira" not in cli.invoke(connect_app, ["ls"]).stdout


def test_add_preset_with_provider_is_rejected_and_neither_is_exit_2():
    assert cli.invoke(connect_app, ["add", "openai", "--preset", "github"]).exit_code == 2
    assert cli.invoke(connect_app, ["add"]).exit_code == 2
