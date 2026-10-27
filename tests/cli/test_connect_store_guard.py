"""#229 — `cairn connect add`/`rm` refuse to write a store the running engine does not read.

The qualification-run case: the engine ran on `cairn dev --engine-only --db ~/.cairn/train.db`,
the CLI had no `CAIRN_STORAGE`, and `cairn connect add ssh --name gpu` wrote to
`~/.cairn/cairn.db`. The engine never saw the connection and the command said it succeeded.

The engine advertises a fingerprint of its store on `/healthz`; the CLI compares it with
its own before writing. A mismatch exits 3 and writes nothing, unless `--force`. Matching
stores behave as before. `cairn doctor` reports the same comparison. The engine is faked at
`remote.engine_store_fingerprint` (the one HTTP seam); everything else is real.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

from cairn.cli.commands import connect, remote  # noqa: E402
from cairn.core import settings  # noqa: E402
from cairn.core.connections import SqliteConnectionStore  # noqa: E402

cli = CliRunner()
ENGINE = "http://localhost:8000"


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    """The CLI's default store is `<tmp>/.cairn/cairn.db`; no probe, no real config."""
    from cairn.core.connection_probe import ProbeResult

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path / "cfg"))
    monkeypatch.setenv("CAIRN_STORAGE", "")
    monkeypatch.setenv("CAIRN_CONNECTIONS_DB", "")
    monkeypatch.delenv("CAIRN_SQLITE_DB", raising=False)
    monkeypatch.delenv("OBS_SQLITE_DB", raising=False)

    async def _fake_probe(name):
        return None, ProbeResult(ok=True, status=200, detail="authenticated (stub)")

    monkeypatch.setattr(connect, "_probe_by_name", _fake_probe)


def _engine_on(monkeypatch, storage: str | None):
    """An engine that answers at ENGINE with the store `storage` names (None: no engine)."""
    if storage is None:
        monkeypatch.setattr(remote, "engine_store_fingerprint", lambda *a, **k: None)
        return
    saved = os.environ.get("CAIRN_STORAGE", "")
    monkeypatch.setenv("CAIRN_STORAGE", storage)
    fp = settings.store_identity()[1]
    monkeypatch.setenv("CAIRN_STORAGE", saved)
    monkeypatch.setattr(remote, "engine_store_fingerprint", lambda *a, **k: (ENGINE, fp))


def _saved(path) -> list[str]:
    if not os.path.exists(path):
        return []
    store = SqliteConnectionStore(path=str(path))
    try:
        return [c.name for c in asyncio.run(store.list())]
    finally:
        store.close()


def test_the_qualification_run_case_is_refused_and_writes_nothing(tmp_path, monkeypatch):
    _engine_on(monkeypatch, f"sqlite:///{tmp_path}/.cairn/train.db")
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu"])
    assert res.exit_code == 3, res.output
    out = " ".join(res.output.split())
    assert "cairn.db" in out and ENGINE in out and "--force" in out
    assert _saved(tmp_path / ".cairn" / "cairn.db") == []


def test_force_writes_to_the_other_store_with_a_warning(tmp_path, monkeypatch):
    _engine_on(monkeypatch, f"sqlite:///{tmp_path}/.cairn/train.db")
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu", "--force"])
    assert res.exit_code == 0, res.output
    assert "will not see" in " ".join(res.output.split())
    assert _saved(tmp_path / ".cairn" / "cairn.db") == ["gpu"]


def test_matching_stores_behave_as_before(tmp_path, monkeypatch):
    train = f"sqlite:///{tmp_path}/.cairn/train.db"
    _engine_on(monkeypatch, train)
    monkeypatch.setenv("CAIRN_STORAGE", train)
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu"])
    assert res.exit_code == 0, res.output
    assert "different store" not in res.output and "will not see" not in res.output
    assert _saved(tmp_path / ".cairn" / "train.db") == ["gpu"]


def test_no_engine_to_compare_writes_and_names_the_store(tmp_path, monkeypatch):
    _engine_on(monkeypatch, None)
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu"])
    assert res.exit_code == 0, res.output
    assert "cairn.db" in " ".join(res.output.split())
    assert _saved(tmp_path / ".cairn" / "cairn.db") == ["gpu"]


def test_postgres_cli_against_a_sqlite_engine_is_refused_without_the_password(tmp_path, monkeypatch):
    _engine_on(monkeypatch, f"sqlite:///{tmp_path}/.cairn/train.db")
    monkeypatch.setenv("CAIRN_STORAGE", "postgresql://cairn:s3cret@db.example:5432/cairn")
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu"])
    assert res.exit_code == 3, res.output
    assert "db.example:5432/cairn" in res.output
    assert "s3cret" not in res.output


def test_rm_is_guarded_too(tmp_path, monkeypatch):
    _engine_on(monkeypatch, None)
    assert cli.invoke(connect.app, ["add", "openai", "--name", "gpu"]).exit_code == 0
    _engine_on(monkeypatch, f"sqlite:///{tmp_path}/.cairn/train.db")
    res = cli.invoke(connect.app, ["rm", "gpu"])
    assert res.exit_code == 3, res.output
    assert _saved(tmp_path / ".cairn" / "cairn.db") == ["gpu"]
    assert cli.invoke(connect.app, ["rm", "gpu", "--force"]).exit_code == 0
    assert _saved(tmp_path / ".cairn" / "cairn.db") == []


def test_engine_store_fingerprint_reads_healthz(monkeypatch):
    class _Client:
        def __init__(self, ep):
            self.ep = ep

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def healthz(self):
            return {"ok": True, "store_fingerprint": "abc123"}

    monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: _Client(ep))
    monkeypatch.setenv("CAIRN_REMOTE_ENDPOINT", ENGINE)
    assert remote.engine_store_fingerprint() == (ENGINE, "abc123")

    def _down(ep, tok, timeout=30.0):
        raise OSError("connection refused")

    monkeypatch.setattr(remote, "_api", _down)
    assert remote.engine_store_fingerprint() is None


def test_doctor_reports_the_mismatch(tmp_path, monkeypatch):
    from cairn.cli.commands import doctor

    _engine_on(monkeypatch, f"sqlite:///{tmp_path}/.cairn/train.db")
    r = doctor.check_engine_store()
    assert r.status == "warn" and ENGINE in r.detail and "cairn.db" in r.detail

    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path}/.cairn/train.db")
    assert doctor.check_engine_store().status == "ok"

    _engine_on(monkeypatch, None)
    assert doctor.check_engine_store() is None


def test_the_cli_compares_the_connections_db_it_writes(tmp_path, monkeypatch):
    """`CAIRN_CONNECTIONS_DB` on the CLI side is the store it writes, so that file is
    what it compares with the engine's fingerprint (#238 review, B3)."""
    conns = tmp_path / "conns.db"
    monkeypatch.setenv("CAIRN_CONNECTIONS_DB", str(conns))
    fp = settings.store_identity(str(conns))[1]
    monkeypatch.setattr(remote, "engine_store_fingerprint", lambda *a, **k: (ENGINE, fp))
    res = cli.invoke(connect.app, ["add", "openai", "--name", "gpu"])
    assert res.exit_code == 0, res.output
    assert _saved(conns) == ["gpu"]

    _engine_on(monkeypatch, None)  # an engine on the shared file, not conns.db
    monkeypatch.setattr(remote, "engine_store_fingerprint",
                        lambda *a, **k: (ENGINE, settings.store_identity()[1]))
    res = cli.invoke(connect.app, ["add", "openai", "--name", "other"])
    assert res.exit_code == 3, res.output
    assert _saved(conns) == ["gpu"]
