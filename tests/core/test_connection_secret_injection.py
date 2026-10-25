"""Dispatch-time connector→env credential bridge (#532).

A stored connection carries logical→env-target refs (`kaggle: USERNAME→
KAGGLE_USERNAME`). `inject_declared_secrets` resolves, for each secret an
operator's binding DECLARES, the connection that provides it and sets
`os.environ[target]` — so an operator reading `os.environ` (train/kaggle) or the
env-backed SecretStore actually sees a credential the user connected. Hermetic:
in-memory store + a stubbed `resolve_connection_obj` (the resolver internals are
covered elsewhere)."""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

import cairn.core.connections as conn
from cairn.core.connections import (
    Connection,
    InMemoryConnectionStore,
    ResolvedConnection,
    inject_declared_secrets,
)


@pytest.fixture
async def kaggle_store(monkeypatch):
    store = InMemoryConnectionStore()
    # A kaggle connection: logical keys map to their conventional env targets.
    await store.create(Connection(
        name="my-kaggle", kind="api_key",
        secrets={"USERNAME": "KAGGLE_USERNAME", "KEY": "KAGGLE_KEY"},
    ))
    monkeypatch.setattr(conn, "default_connection_store", lambda: store)

    async def _resolve(c):  # the row is already in hand (and in the run's account)
        if c.name == "my-kaggle":
            return ResolvedConnection(
                name=c.name, kind="api_key",
                secrets={"USERNAME": "alice", "KEY": "sk-kaggle-123"},
            )
        return None

    monkeypatch.setattr(conn, "resolve_connection_obj", _resolve)
    for var in ("KAGGLE_USERNAME", "KAGGLE_KEY", "HF_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    return store


async def test_declared_secrets_are_injected_from_connection(kaggle_store):
    injected = await inject_declared_secrets({"KAGGLE_USERNAME", "KAGGLE_KEY"})
    assert set(injected) == {"KAGGLE_USERNAME", "KAGGLE_KEY"}
    assert os.environ["KAGGLE_USERNAME"] == "alice"
    assert os.environ["KAGGLE_KEY"] == "sk-kaggle-123"


async def test_undeclared_targets_are_not_injected(kaggle_store):
    # Operator only declares USERNAME → KEY must NOT be injected (declared-scoped).
    injected = await inject_declared_secrets({"KAGGLE_USERNAME"})
    assert injected == ["KAGGLE_USERNAME"]
    assert "KAGGLE_KEY" not in os.environ


async def test_missing_connection_leaves_env_untouched(kaggle_store):
    # HF_TOKEN is declared but no connection provides it → no-op, no crash.
    injected = await inject_declared_secrets({"HF_TOKEN"})
    assert injected == []
    assert "HF_TOKEN" not in os.environ


async def test_existing_env_is_not_overridden(kaggle_store, monkeypatch):
    monkeypatch.setenv("KAGGLE_USERNAME", "explicit-box-value")
    injected = await inject_declared_secrets({"KAGGLE_USERNAME", "KAGGLE_KEY"})
    # Only the absent one is filled; the explicit env wins.
    assert injected == ["KAGGLE_KEY"]
    assert os.environ["KAGGLE_USERNAME"] == "explicit-box-value"


async def test_no_declared_secrets_is_a_noop(kaggle_store):
    assert await inject_declared_secrets([]) == []
    assert await inject_declared_secrets({""}) == []
