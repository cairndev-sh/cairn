"""`cairn dev` (#1188, ADR-0085 D5) — the pinned dev environment.

The contract under test is the PINNING: whatever a repo .env or a stale shell
export says, the dev engine must come up on sqlite/inprocess with dev/dev auth.
"""
from __future__ import annotations

import os
from pathlib import Path

from cairn.cli.commands import dev as dev_cli
from cairn.cli.commands.dev import _PINNED_EMPTY, dev_env


def test_dev_env_pins_the_storage_and_runtime_family(monkeypatch):
    # a poisoned environment — exactly the .env foot-guns ADR-0085 D5 retires
    monkeypatch.setenv("CAIRN_STORAGE", "postgres://prod:oops@db:5432/cairn")
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    monkeypatch.setenv("RAG_VECTOR_STORE", "pgvector")
    monkeypatch.setenv("CAIRN_BUS", "postgres://prod:oops@db:5432/cairn")

    env = dev_env(port=8123, db_path="/tmp/x/dev.db")

    assert env["CAIRN_STORAGE"] == "sqlite:////tmp/x/dev.db"
    assert env["CAIRN_RUNTIME"] == "inprocess"
    assert env["CAIRN_TRIGGER_PORT"] == "8123"
    for k in _PINNED_EMPTY:
        assert env[k] == ""  # set-but-empty: .env can never refill (override=False)


def test_dev_env_seeds_dev_auth_never_disables_it():
    env = dev_env(port=8000)
    assert env["CAIRN_JWT_SECRET"]  # auth is ON
    assert env["CAIRN_API_USER"] == "dev"
    assert env["CAIRN_API_PASSWORD"] == "dev"


def test_dev_env_passes_real_keys_through(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-real-key")
    env = dev_env(port=8000)
    assert env["OPENAI_API_KEY"] == "sk-real-key"  # keyless-by-default means
    # mock BACKEND unless a key exists — the key itself always passes through


def test_dev_env_defaults_db_under_home():
    env = dev_env(port=8000)
    assert env["CAIRN_STORAGE"].startswith("sqlite:///")
    assert env["CAIRN_STORAGE"].endswith("/.cairn/dev.db")
    assert os.path.isabs(env["CAIRN_STORAGE"].removeprefix("sqlite:///"))


def test_shared_sqlite_follows_the_pinned_storage_url(monkeypatch):
    """#1195 slice 2 finding: only the RUN store parsed CAIRN_STORAGE's sqlite
    URL — the ~15 config-family stores (pipelines included) fell back to
    ~/.cairn/cairn.db, leaking prior sessions into a pinned dev view."""
    from cairn.core import settings

    monkeypatch.delenv("CAIRN_SQLITE_DB", raising=False)
    monkeypatch.setenv("CAIRN_STORAGE", "sqlite:////tmp/pinned/dev.db")
    assert settings.default_sqlite_db() == "/tmp/pinned/dev.db"
    # explicit CAIRN_SQLITE_DB still wins
    monkeypatch.setenv("CAIRN_SQLITE_DB", "/tmp/explicit.db")
    assert settings.default_sqlite_db() == "/tmp/explicit.db"
    # postgres URL → untouched home default
    monkeypatch.delenv("CAIRN_SQLITE_DB", raising=False)
    monkeypatch.setenv("CAIRN_STORAGE", "postgres://x@y/z")
    assert settings.default_sqlite_db().endswith("/.cairn/cairn.db")


def test_seed_runs_the_demo_seeder_under_the_pinned_env(monkeypatch):
    """#1189 — --seed delegates to `cairn demo` WITH the pinned env, so the
    demo data lands in dev.db, never in whatever the shell .env points at."""
    from cairn.cli.commands import dev as dev_mod

    seen = {}

    def fake_run(cmd, env=None, **kw):
        seen["cmd"], seen["env"] = cmd, env
        return type("R", (), {"returncode": 0})()

    monkeypatch.setattr(dev_mod.subprocess, "run", fake_run)
    env = dev_env(port=8000, db_path="/tmp/dev-seed.db")
    dev_mod._seed(env)
    assert seen["cmd"][-1] == "demo"
    assert seen["env"]["CAIRN_STORAGE"] == "sqlite:////tmp/dev-seed.db"


def test_dev_env_puts_the_checkouts_example_pack_on_the_dev_pack_path(monkeypatch):
    """#2418 — from a checkout, `cairn dev` (engine and `--seed`) sees the example
    pack through CAIRN_PACK_PATH, after what the shell already lists, once."""
    from pathlib import Path

    example = str(Path(__file__).resolve().parents[2] / "examples" / "hello-cairn")
    monkeypatch.setenv("CAIRN_PACK_PATH", "/tmp/my-packs")
    env = dev_env(port=8000)
    assert env["CAIRN_PACK_PATH"].split(os.pathsep) == ["/tmp/my-packs", example]
    monkeypatch.setenv("CAIRN_PACK_PATH", example)
    assert dev_env(port=8000)["CAIRN_PACK_PATH"] == example


def _example() -> Path:
    return Path(__file__).resolve().parents[2] / "examples" / "hello-cairn"


def test_the_example_pack_runs_under_its_own_profile(monkeypatch) -> None:
    """The example requires an MCP server local-dev doesn't name: `cairn setup --seed` failed its checks."""
    from cairn.core.pack.profile import load_profile

    monkeypatch.delenv("CAIRN_PROFILE", raising=False)
    monkeypatch.delenv("OBS_PROFILE", raising=False)
    monkeypatch.setattr(dev_cli, "_example_pack_dir", _example)
    env = dev_cli.dev_env(8000, example_pack=True)
    assert Path(env["CAIRN_PROFILE"]) == _example() / "profiles" / "hello.toml"
    assert "hello-calc" in load_profile(Path(env["CAIRN_PROFILE"])).mcp_servers


def test_without_the_example_pack_the_profile_is_local_dev(monkeypatch) -> None:
    monkeypatch.delenv("CAIRN_PROFILE", raising=False)
    monkeypatch.delenv("OBS_PROFILE", raising=False)
    monkeypatch.setattr(dev_cli, "_example_pack_dir", _example)
    assert Path(dev_cli.dev_env(8000, example_pack=False)["CAIRN_PROFILE"]).name == "local-dev.toml"
    monkeypatch.setattr(dev_cli, "_example_pack_dir", lambda: None)  # installed: no checkout to take it from
    assert Path(dev_cli.dev_env(8000, example_pack=True)["CAIRN_PROFILE"]).name == "local-dev.toml"


def test_a_profile_you_set_is_kept_with_the_example_pack(monkeypatch) -> None:
    monkeypatch.setenv("CAIRN_PROFILE", "/mine.toml")
    monkeypatch.setattr(dev_cli, "_example_pack_dir", _example)
    assert dev_cli.dev_env(8000, example_pack=True)["CAIRN_PROFILE"] == "/mine.toml"
