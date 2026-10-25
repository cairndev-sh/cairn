"""Workspace ids and corpus names are bounded where they are created.

A workspace id is the prefix of its vector-collection names, and Postgres keeps
only the first 63 bytes of an identifier. A create assigns at most
``MAX_WORKSPACE_ID_LEN`` (40) characters, keeping the ``-2`` dedupe; a slug id
already longer than that (created before the cap) takes the hashed collection
prefix. A BYO corpus name is at most ``MAX_CORPUS_NAME_LEN`` (48) characters.
"""
from __future__ import annotations

import asyncio
import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core import account as A
from cairn.core import workspace as W
from cairn.core.triggers import auth

LONG = "Acme International Holdings Research And Development Division North"  # slug: 68 chars


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE", "CAIRN_JWT_SECRET",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed():
        await accounts.put_account(A.Account(id="globex", name="Globex"))
        u = await A.create_user("gina", "gina-password", store=accounts)
        await A.add_member("globex", u.id, "admin", store=accounts)
        return u.id
    uid = asyncio.run(_seed())

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        headers={"Authorization": f"Bearer {auth.create_access_token({'sub': uid, 'account_id': 'globex'})}"},
        workspaces=workspaces)


def test_a_long_name_gets_a_40_character_id_and_the_dedupe_stays_within_it(env):
    with env.client() as c:
        first = c.post("/workspaces", json={"name": LONG}, headers=env.headers)
        second = c.post("/workspaces", json={"name": LONG}, headers=env.headers)
    assert first.status_code == second.status_code == 200, (first.text, second.text)
    a, b = first.json()["id"], second.json()["id"]
    assert a == "acme-international-holdings-research-and"  # cut at 40, trailing dash dropped
    assert len(a) <= W.MAX_WORKSPACE_ID_LEN and len(b) <= W.MAX_WORKSPACE_ID_LEN
    assert b.endswith("-2") and b != a


def test_an_explicit_long_id_is_cut_to_40_characters(env):
    with env.client() as c:
        r = c.post("/workspaces", json={"name": "x", "id": "z" * 60}, headers=env.headers)
    assert r.status_code == 200, r.text
    assert r.json()["id"] == "z" * 40


def test_a_new_accounts_first_workspace_id_is_capped_too(env, monkeypatch):
    from cairn.core.triggers.routers import accounts as routes

    async def _quiet(*_a, **_k):
        return None

    monkeypatch.setattr(routes, "_workspace_created", _quiet)
    acct = types.SimpleNamespace(id="acct-1", name="ignored")
    first = asyncio.run(routes._provision_first_workspace(acct, LONG, actor="u", source="signup"))
    second = asyncio.run(routes._provision_first_workspace(acct, LONG, actor="u", source="signup"))
    assert len(first) <= W.MAX_WORKSPACE_ID_LEN and len(second) <= W.MAX_WORKSPACE_ID_LEN
    assert second.endswith("-2") and second != first


def test_collection_prefix_is_readable_up_to_40_characters_and_hashed_beyond():
    at_cap, over = "a" * 40, "a" * 41
    assert W.workspace_collection("rag_x", at_cap) == f"ws_{at_cap}__rag_x"
    longer = W.workspace_collection("rag_x", over)
    assert longer.startswith("ws__") and len(longer.split("__")[1]) == 16
    # every prefix a create can assign ends well inside Postgres' 63-byte identifier
    assert len(W.workspace_collection("", "a" * W.MAX_WORKSPACE_ID_LEN)) < 63


@pytest.mark.parametrize("make", ["memory", "sqlite"])
def test_a_corpus_name_longer_than_48_characters_is_refused(make, tmp_path):
    from cairn.core.storage import knowledge_corpora as kc

    store = kc.InMemoryWorkspaceCorpusStore() if make == "memory" else kc.SqliteWorkspaceCorpusStore(
        path=str(tmp_path / "kc.db"))
    ok = kc.WorkspaceCorpus(workspace_id="w", name="n" * kc.MAX_CORPUS_NAME_LEN, source_uri="git+https://x/y.git")
    asyncio.run(store.add(ok))
    with pytest.raises(ValueError, match="at most 48"):
        asyncio.run(store.add(kc.WorkspaceCorpus(workspace_id="w", name="n" * 49, source_uri="git+https://x/y.git")))
    assert [c.name for c in asyncio.run(store.list("w"))] == [ok.name]
    assert kc.corpus_name_problem("n" * 49) and kc.corpus_name_problem("ok") is None
