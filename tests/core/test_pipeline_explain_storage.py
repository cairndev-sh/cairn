"""#274 — ``pipeline explain``'s deployment row names the run store as
``storage.default_store`` chooses it, and never prints the value.

An unrecognised ``CAIRN_STORAGE`` was reported as falling back to ``CAIRN_RUN_DB`` /
in-memory; since #1855 the store refuses it. With ``CAIRN_STORAGE`` unset and
``CAIRN_RUN_DB`` set, the row said in-memory while the store used that SQLite file.
"""
from __future__ import annotations

import pytest

from cairn.core.engine.pipeline_explain import _deployment
from cairn.core.storage import InMemoryRunStore, SqliteRunStore, default_store


@pytest.fixture(autouse=True)
def _no_ambient(monkeypatch):
    for var in ("CAIRN_STORAGE", "OBS_STORAGE", "CAIRN_RUN_DB", "OBS_RUN_DB"):
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(("value", "named"), [
    ("foo://alice:s3cret@db.invalid/x", "is a foo:// URL"),
    ("alice:s3cret@db.invalid:5432/cairn", "is not a storage URL"),
    ("sqlite", "is not a storage URL"),
])
def test_an_unrecognised_value_is_refused_as_the_store_refuses_it(monkeypatch, value, named):
    monkeypatch.setenv("CAIRN_STORAGE", value)
    storage = _deployment()["storage"]
    assert storage.startswith("refused: CAIRN_STORAGE ") and named in storage, storage
    assert "falls back" not in storage
    assert not any(s in storage for s in ("alice", "s3cret", "db.invalid")), storage
    with pytest.raises(ValueError):
        default_store()


def test_run_db_alone_is_sqlite_like_the_store(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "runs.db"))
    assert _deployment()["storage"] == "sqlite (CAIRN_RUN_DB)"
    assert isinstance(default_store(), SqliteRunStore)


def test_neither_set_is_in_memory_like_the_store():
    assert _deployment()["storage"].startswith("in-memory (CAIRN_STORAGE and CAIRN_RUN_DB unset")
    assert isinstance(default_store(), InMemoryRunStore)


@pytest.mark.parametrize(("value", "family"), [
    ("postgresql://alice:s3cret@db.invalid/cairn", "postgres"),
    ("sqlite:///tmp/cairn.db", "sqlite"),
])
def test_a_storage_url_is_named_by_family(monkeypatch, value, family):
    monkeypatch.setenv("CAIRN_STORAGE", value)
    assert _deployment()["storage"] == family
