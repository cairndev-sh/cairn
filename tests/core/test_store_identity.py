"""#229 — the store a process reads and writes, named without credentials.

`settings.store_identity()` is what the engine advertises on `/healthz` (as a
fingerprint) and what the CLI compares before it writes a connection. It must name the
same store the same way from both sides (SQLite: the resolved file; Postgres: host, port
and database) and never carry a password.
"""
from __future__ import annotations

import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402

from cairn.core import settings  # noqa: E402


@pytest.fixture(autouse=True)
def _no_sqlite_override(monkeypatch):
    for name in ("SQLITE_DB", "CONNECTIONS_DB"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)


def _ident(monkeypatch, storage: str, **env: str):
    monkeypatch.setenv("CAIRN_STORAGE", storage)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return settings.store_identity()


def test_sqlite_identity_is_the_resolved_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    label, fp = _ident(monkeypatch, f"sqlite:///{tmp_path}/train.db")
    assert label == f"sqlite {os.path.realpath(tmp_path / 'train.db')}"
    assert len(fp) == 16
    # the same file spelled another way is the same store
    assert _ident(monkeypatch, f"sqlite:////{tmp_path}/train.db") == (label, fp)
    assert _ident(monkeypatch, f"sqlite:///{tmp_path}/sub/../train.db") == (label, fp)
    assert _ident(monkeypatch, "", CAIRN_SQLITE_DB="train.db") == (label, fp)
    # another file is another store
    monkeypatch.delenv("CAIRN_SQLITE_DB")
    assert _ident(monkeypatch, f"sqlite:///{tmp_path}/cairn.db")[1] != fp


def test_unset_storage_is_the_default_shared_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    label, _ = _ident(monkeypatch, "")
    assert label == f"sqlite {os.path.realpath(tmp_path / '.cairn' / 'cairn.db')}"


def test_an_explicit_sqlite_path_names_that_file(tmp_path, monkeypatch):
    label, _ = _ident(monkeypatch, "")
    assert settings.store_identity(str(tmp_path / "conns.db"))[0] == (
        f"sqlite {os.path.realpath(tmp_path / 'conns.db')}")
    assert label != settings.store_identity(str(tmp_path / "conns.db"))[0]


def test_postgres_identity_carries_no_credentials(monkeypatch):
    label, fp = _ident(monkeypatch, "postgresql://cairn:s3cret@DB.example:5433/cairn?sslmode=require")
    assert label == "postgres db.example:5433/cairn"
    assert "s3cret" not in label and "cairn:" not in label
    # another password, or the postgres:// spelling, is the same store
    assert _ident(monkeypatch, "postgres://other:pw@db.example:5433/cairn")[1] == fp
    # another database or host is another store
    assert _ident(monkeypatch, "postgresql://cairn:s3cret@db.example:5433/train")[1] != fp
    assert _ident(monkeypatch, "postgresql://cairn:s3cret@db2.example:5433/cairn")[1] != fp
    # the default port is spelled out, so `host` and `host:5432` agree
    assert _ident(monkeypatch, "postgresql://u@db.example/cairn")[0] == "postgres db.example:5432/cairn"


def test_postgres_and_sqlite_never_share_a_fingerprint(tmp_path, monkeypatch):
    pg = _ident(monkeypatch, "postgresql://u:p@localhost:5432/cairn")[1]
    lite = _ident(monkeypatch, f"sqlite:///{tmp_path}/cairn.db")[1]
    assert pg != lite


def _healthz(tmp_path, monkeypatch) -> dict:
    from fastapi.testclient import TestClient

    from cairn.core.triggers.server import build_app

    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    return TestClient(build_app(FakeRunner(), enable_scheduler=False)).get("/healthz").json()


def test_healthz_advertises_the_store_fingerprint(tmp_path, monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path}/engine.db")
    body = _healthz(tmp_path, monkeypatch)
    assert body["store_fingerprint"] == settings.store_identity()[1]
    assert str(tmp_path) not in str(body)  # a fingerprint, never the path


def test_healthz_names_the_connections_db_the_engine_reads(tmp_path, monkeypatch):
    """An engine started with `CAIRN_CONNECTIONS_DB` reads connections from that file,
    not the shared one: `/healthz` names it, as the CLI's check does with the same env,
    so the two sides compare the same store (#229 review, B3)."""
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path}/engine.db")
    monkeypatch.setenv("CAIRN_CONNECTIONS_DB", str(tmp_path / "conns.db"))
    body = _healthz(tmp_path, monkeypatch)
    assert body["store_fingerprint"] == settings.connection_store_identity()[1]
    assert body["store_fingerprint"] == settings.store_identity(str(tmp_path / "conns.db"))[1]
    assert body["store_fingerprint"] != settings.store_identity()[1]


def test_postgres_storage_wins_over_connections_db(monkeypatch):
    """As `default_connection_store` chooses: a Postgres `CAIRN_STORAGE` is the
    connection store whatever `CAIRN_CONNECTIONS_DB` says."""
    monkeypatch.setenv("CAIRN_STORAGE", "postgresql://u:pw@db:5432/cairn")
    monkeypatch.setenv("CAIRN_CONNECTIONS_DB", "/tmp/ignored.db")
    assert settings.connection_store_identity() == settings.store_identity()
