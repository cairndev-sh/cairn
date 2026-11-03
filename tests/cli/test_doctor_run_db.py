"""#61 — `cairn doctor`'s run_db row resolves the run store as
``storage.default_store`` does: ``CAIRN_STORAGE`` first, then ``CAIRN_RUN_DB``,
else in-memory. It warns only when the store really is in-memory.
"""
from __future__ import annotations

import pytest

from cairn.cli.commands import doctor as doctor_mod


@pytest.fixture(autouse=True)
def _no_ambient_store(monkeypatch):
    for var in ("CAIRN_STORAGE", "OBS_STORAGE", "CAIRN_RUN_DB", "OBS_RUN_DB"):
        monkeypatch.delenv(var, raising=False)


def test_sqlite_storage_is_ok_and_names_the_file(monkeypatch, tmp_path):
    db = tmp_path / "cairn.db"
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite://{db}")
    r = doctor_mod.check_run_db()
    assert r.status == "ok", r.detail
    assert str(db) in r.detail
    assert "CAIRN_STORAGE" in r.detail


def test_postgres_storage_is_ok_without_credentials(monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", "postgresql://u:secretpw@db.example:5433/cairn")
    r = doctor_mod.check_run_db()
    assert r.status == "ok", r.detail
    assert "postgres db.example:5433/cairn" in r.detail
    assert "secretpw" not in r.detail


def test_neither_set_still_warns_in_memory():
    r = doctor_mod.check_run_db()
    assert r.status == "warn"
    assert "in-memory" in r.detail
    assert "CAIRN_STORAGE" in r.detail


def test_run_db_alone_is_ok_and_names_the_file(monkeypatch, tmp_path):
    db = tmp_path / "runs.db"
    monkeypatch.setenv("CAIRN_RUN_DB", str(db))
    r = doctor_mod.check_run_db()
    assert r.status == "ok", r.detail
    assert str(db) in r.detail


def test_storage_wins_over_run_db(monkeypatch, tmp_path):
    db = tmp_path / "cairn.db"
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite://{db}")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "runs.db"))
    r = doctor_mod.check_run_db()
    assert r.status == "ok", r.detail
    assert str(db) in r.detail
    assert "runs.db" not in r.detail


def test_sqlite_memory_storage_is_explicitly_in_memory(monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", "sqlite://:memory:")
    r = doctor_mod.check_run_db()
    assert r.status == "ok", r.detail
    assert ":memory:" in r.detail


def test_unrecognized_storage_fails_as_the_store_refuses_it(monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", "sqlite")
    r = doctor_mod.check_run_db()
    assert r.status == "fail", r.detail
    assert "CAIRN_STORAGE" in r.detail


@pytest.mark.parametrize("value", [
    "postgres+asyncpg://user:secret@db.example:5432/cairn",
    "mysql://user:secret@db.example/cairn",
    "user:secret@db.example:5432/cairn",
    "//user:secret@[::1/cairn",
])
def test_unrecognized_storage_never_echoes_credentials(monkeypatch, value):
    monkeypatch.setenv("CAIRN_STORAGE", value)
    r = doctor_mod.check_run_db()
    assert r.status == "fail", r.detail
    assert "secret" not in r.detail
    assert "user" not in r.detail
    assert "db.example" not in r.detail
