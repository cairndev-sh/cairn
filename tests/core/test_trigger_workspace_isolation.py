"""A webhook receiver belongs to one workspace, queued or inline.

Two ways a receiver's runs could land in a workspace that did not declare it:

* **The durable inbox (#212).** With ``CAIRN_TRIGGER_DURABLE`` on, a matched event is
  queued and a drain loop starts it later. The queued record stored no workspace,
  so the drain started every run in the process default, wherever the receiver was
  declared. The record now carries the receiver's workspace and the drain binds the
  run to it (a replayed record keeps it too).
* **Two workspaces, one path (#213).** Receivers are keyed by ingress path, so two
  workspaces declaring a receiver at the same path collapsed to one, the last row
  loaded, and one workspace's sender started runs in the other's. Worse, trigger ids
  and names are global and every store's ``create`` is an upsert, so enabling the
  SAME pipeline in a second workspace replaced the first workspace's row and moved
  it. The write path now refuses a row whose id, name or ingress path belongs to
  another workspace (the store raises, materialisation skips and logs), and a path
  that still reaches the router from two workspaces is refused at load.

Driven through the real app (``build_app``) with auth on.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.engine.runner import Runner
from cairn.core.storage import trigger_inbox as inbox_mod
from cairn.core.storage.trigger_inbox import SqliteTriggerInbox, drain_once
from cairn.core.engine.triggers_grammar import WebhookTrigger, materialise_declared_triggers
from cairn.core.storage.schedule_store import InMemoryScheduleStore
from cairn.core.storage import trigger_store as trigger_store_mod
from cairn.core.storage.trigger_store import InMemoryTriggerStore, SqliteTriggerStore, StoredTrigger
from cairn.core.triggers import auth
from cairn.core.triggers import router as router_mod
from cairn.core.triggers.router import TriggerRouter
from cairn.core.triggers.server import build_app
from cairn.core.workspace import current_workspace

SECRET = "hook-secret"
BODY = json.dumps({"type": "push"}).encode()


def _spec_text(path: str) -> str:
    return (
        "source: custom\n"
        f"ingress: {{transport: http, path: '{path}', method: POST, signature: "
        "{header: X-Signature, secret: '${env:HOOK_SECRET}', required: true}}\n"
        "normalize: {event_type: type}\n"
        "run: {template: pipeline:p/x}\n"
    )


def _row(rid: str, ws: str, path: str) -> StoredTrigger:
    return StoredTrigger(id=rid, workspace_id=ws, name=rid, source="custom",
                         yaml=_spec_text(path), enabled=True)


def _sig(secret: str = SECRET, body: bytes = BODY) -> dict[str, str]:
    return {"X-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
            "content-type": "application/json"}


class _Started:
    """The launch-door stub (the inline path): records each start and its workspace."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, ref, inputs, *, environment=None):
        self.calls.append((ref, current_workspace()))
        return type("Run", (), {"id": f"run-{len(self.calls)}", "status": "queued"})()


@pytest.fixture
def started() -> _Started:
    return _Started()


@pytest.fixture
def stored(monkeypatch):
    """The trigger store the router loads receivers from, and no overlay files."""
    rows: list[StoredTrigger] = []
    store = types.SimpleNamespace(list_sync=lambda *a, **k: list(rows), close=lambda: None)
    import cairn.core.storage.trigger_store as ts

    monkeypatch.setattr(ts, "default_trigger_store", lambda: store)
    monkeypatch.setattr(router_mod, "load_trigger_specs", lambda *_a, **_k: [])
    return rows


@pytest.fixture
def env(monkeypatch, started):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_TRIGGER_DURABLE", "CAIRN_TRIGGER_DURABLE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("HOOK_SECRET", SECRET)
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())  # module state: start clean
    import cairn.core.dispatch.start as start_mod

    # the ONE launch door build_app hands the router; the stub records where it ran
    monkeypatch.setattr(start_mod, "unattended_starter", lambda *a, **k: started)


# ═══ #212 — a queued delivery keeps its receiver's workspace ═══════════════


@pytest.fixture
def durable(monkeypatch, env, started):
    """Durable ingress on an in-memory inbox. The lifespan's drain loop is captured
    (inbox + the server's own dispatch handler) so the test drains it once. The drain
    starts a record through the launch door the inline path uses (#217), so ``ran``
    is that door's record of each start and the workspace it ran in; the bare
    in-process ``start_template_run`` is a tripwire."""
    monkeypatch.setenv("OBS_TRIGGER_DURABLE", "1")
    monkeypatch.setenv("OBS_TRIGGER_INBOX", "memory")
    captured: dict = {}

    async def _capture(inbox, handler, **_kw):
        captured["inbox"], captured["handler"] = inbox, handler

    monkeypatch.setattr(inbox_mod, "run_drain_loop", _capture)
    async def _bare(ref, inputs):
        raise AssertionError(f"the drain took the bare in-process path for {ref!r}")

    monkeypatch.setattr(router_mod, "start_template_run", _bare)

    async def _no_sync(**_kw):
        return 0

    import cairn.core.workflow_registry as wr

    monkeypatch.setattr(wr, "sync_discovered_pipelines", _no_sync)
    return captured, started.calls


def _drain(captured) -> dict:
    return asyncio.run(drain_once(captured["inbox"], captured["handler"]))


def test_a_queued_run_lands_in_its_receivers_workspace(durable, stored, started):
    captured, ran = durable
    stored.append(_row("acme-hook", "acme-ops", "/triggers/acme"))
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        r = c.post("/triggers/acme", content=BODY, headers=_sig())
        assert r.status_code == 200, (r.status_code, r.text)
        assert ran == []  # queued, not started inline
        assert _drain(captured)["dispatched"] == 1
    assert ran == [("pipeline:p/x", "acme-ops")]


def test_a_replayed_record_keeps_its_workspace(durable, stored, monkeypatch):
    captured, ran = durable
    # replay is admin-only (#218): the caller is the deployment's env admin
    monkeypatch.setenv("CAIRN_API_USER", "admin")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "admin-password-for-tests")
    stored.append(_row("acme-hook", "acme-ops", "/triggers/acme"))
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        assert c.post("/triggers/acme", content=BODY, headers=_sig()).status_code == 200
        _drain(captured)
        tok = auth.create_access_token({"sub": "admin"})
        r = c.post("/triggers/inbox/replay", params={"since": "1970-01-01T00:00:00"},
                   headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 200, (r.status_code, r.text)
        assert _drain(captured)["dispatched"] == 1
    assert [ws for *_x, ws in ran] == ["acme-ops", "acme-ops"]


def test_a_queued_run_for_a_default_workspace_receiver_still_replays_there(durable, stored):
    captured, ran = durable
    stored.append(_row("dflt-hook", "default", "/triggers/dflt"))
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        assert c.post("/triggers/dflt", content=BODY, headers=_sig()).status_code == 200
        assert _drain(captured)["dispatched"] == 1
    assert [ws for *_x, ws in ran] == ["default"]


def test_a_record_queued_before_the_field_existed_replays_in_the_default(tmp_path):
    """A row written by an older server has no ``workspace``: it reads back as None,
    which binds nothing, so it runs where every queued run ran before."""
    db = str(tmp_path / "inbox.db")
    inbox = SqliteTriggerInbox(db)
    rec = asyncio.run(inbox.enqueue("custom", "pipeline:p/x", {}, workspace="acme-ops"))
    old = rec.model_dump(mode="json")
    old.pop("workspace")
    inbox._conn.execute("UPDATE inbox SET data=? WHERE id=?", (json.dumps(old), rec.id))
    inbox._conn.commit()
    (row,) = asyncio.run(inbox.list())
    assert row.workspace is None
    rec2 = asyncio.run(inbox.enqueue("custom", "pipeline:p/x", {}, workspace="acme-ops"))
    assert [r.workspace for r in asyncio.run(inbox.list()) if r.id == rec2.id] == ["acme-ops"]
    inbox.close()


# ═══ #213 — one ingress path, one workspace ════════════════════════════════


def test_two_workspaces_at_one_path_are_refused_at_load(env, stored, started, caplog):
    stored.extend([_row("a-hook", "acme-ops", "/triggers/shared"),
                   _row("g-hook", "globex", "/triggers/shared")])
    with caplog.at_level(logging.ERROR):
        c = TestClient(build_app(Runner(), enable_scheduler=False))
    assert not auth._is_public_path("/triggers/shared", "POST")
    r = c.post("/triggers/shared", content=BODY, headers=_sig())
    assert r.status_code == 401, (r.status_code, r.text)
    tok = auth.create_access_token({"sub": "admin"})
    r = c.post("/triggers/shared", content=BODY,
               headers={**_sig(), "Authorization": f"Bearer {tok}"})
    assert r.status_code in (404, 405), (r.status_code, r.text)  # no receiver mounted
    assert started.calls == []
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("/triggers/shared" in m and "acme-ops" in m and "globex" in m for m in msgs), msgs


def test_the_router_has_no_receiver_at_a_contested_path(env, stored):
    stored.extend([_row("a-hook", "acme-ops", "/triggers/shared"),
                   _row("g-hook", "globex", "/triggers/shared"),
                   _row("a-own", "acme-ops", "/triggers/acme")])
    r = TriggerRouter.from_installed()
    assert r.spec_for_path("/triggers/shared") is None
    assert "/triggers/shared" not in r.paths()
    assert r.workspace_for(r.spec_for_path("/triggers/acme")) == "acme-ops"


def test_one_workspaces_receiver_still_works(env, stored, started):
    stored.extend([_row("a-hook", "acme-ops", "/triggers/shared"),
                   _row("g-hook", "globex", "/triggers/shared"),
                   _row("a-own", "acme-ops", "/triggers/acme")])
    c = TestClient(build_app(Runner(), enable_scheduler=False))
    r = c.post("/triggers/acme", content=BODY, headers=_sig())
    assert r.status_code == 200, (r.status_code, r.text)
    assert started.calls == [("pipeline:p/x", "acme-ops")]


def test_the_same_workspace_declaring_a_path_twice_is_unchanged(env, stored, started):
    """Not a cross-workspace collision: the last row wins, as before."""
    stored.extend([_row("a-1", "acme-ops", "/triggers/acme"),
                   _row("a-2", "acme-ops", "/triggers/acme")])
    c = TestClient(build_app(Runner(), enable_scheduler=False))
    r = c.post("/triggers/acme", content=BODY, headers=_sig())
    assert r.status_code == 200, (r.status_code, r.text)
    assert started.calls == [("pipeline:p/x", "acme-ops")]


# ─── #213 at the write path: the same pipeline enabled in two workspaces ────

REF = "pipeline:shared/p"


def _materialise(store, ws: str):
    return asyncio.run(materialise_declared_triggers(
        REF, [WebhookTrigger(kind="webhook", source="github")], workspace_id=ws,
        trigger_store=store, schedule_store=InMemoryScheduleStore()))


def test_enabling_the_same_webhook_in_a_second_workspace_does_not_take_the_first_ones(
        env, monkeypatch, tmp_path, started, caplog):
    """The real sqlite store and the real materialise/converge path: ids and names are
    global, so B's write used to REPLACE A's row and move it to B — one row, no
    collision left for the router to see, and A's sender started runs in B."""
    db = str(tmp_path / "triggers.db")
    monkeypatch.setenv("CAIRN_TRIGGERS_DB", db)
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", SECRET)
    monkeypatch.setattr(router_mod, "load_trigger_specs", lambda *_a, **_k: [])
    store = SqliteTriggerStore(db)
    a = _materialise(store, "acme-ops")
    assert len(a.created) == 1, a
    with caplog.at_level(logging.ERROR):
        b = _materialise(store, "globex")
    assert b.created == [] and b.updated == [] and len(b.unarmed) == 1, b
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("/triggers/github" in m and "acme-ops" in m and "globex" in m for m in msgs), msgs
    rows = store.list_sync()
    assert [(r.id, r.workspace_id) for r in rows] == [(a.created[0], "acme-ops")]
    store.close()

    router = TriggerRouter.from_installed()
    assert router.workspace_for(router.spec_for_path("/triggers/github")) == "acme-ops"
    c = TestClient(build_app(Runner(), enable_scheduler=False))
    sig = "sha256=" + hmac.new(SECRET.encode(), BODY, hashlib.sha256).hexdigest()
    r = c.post("/triggers/github", content=BODY,
               headers={"X-Hub-Signature-256": sig, "content-type": "application/json",
                        "X-Cairn-Workspace": "globex"})
    assert r.status_code == 200, (r.status_code, r.text)
    assert [ws for _ref, ws in started.calls] == ["acme-ops"]


def test_re_enabling_in_the_owning_workspace_is_unchanged(tmp_path):
    store = SqliteTriggerStore(str(tmp_path / "triggers.db"))
    first = _materialise(store, "acme-ops")
    again = _materialise(store, "acme-ops")
    assert len(first.created) == 1 and again.unchanged == first.created and again.unarmed == []
    store.close()


def test_another_pipeline_at_a_path_another_workspace_holds_is_not_armed(tmp_path):
    """A different id and name, the same ingress path: refused at the write too."""
    store = SqliteTriggerStore(str(tmp_path / "triggers.db"))
    _materialise(store, "acme-ops")
    other = asyncio.run(materialise_declared_triggers(
        "pipeline:other/q", [WebhookTrigger(kind="webhook", source="github")],
        workspace_id="globex", trigger_store=store, schedule_store=InMemoryScheduleStore()))
    assert other.created == [] and len(other.unarmed) == 1, other
    assert "/triggers/github" in other.unarmed[0] and "acme-ops" in other.unarmed[0]
    assert {r.workspace_id for r in store.list_sync()} == {"acme-ops"}
    store.close()


@pytest.mark.parametrize("make", [lambda p: SqliteTriggerStore(str(p / "t.db")),
                                  lambda p: InMemoryTriggerStore()])
def test_the_store_refuses_a_row_another_workspace_owns(tmp_path, make):
    """Every writer (the console's create route included) goes through ``create``:
    a cross-workspace id or name raises instead of replacing the row."""
    store = make(tmp_path)
    mine = StoredTrigger(id="hook", workspace_id="acme-ops", name="hook", source="custom",
                         yaml=_spec_text("/triggers/hook"))
    asyncio.run(store.create(mine))
    for clash in (mine.model_copy(update={"workspace_id": "globex"}),
                  mine.model_copy(update={"workspace_id": "globex", "id": "other-id"})):
        with pytest.raises(getattr(trigger_store_mod, "TriggerOwnedByAnotherWorkspace", ValueError),
                           match="acme-ops"):
            asyncio.run(store.create(clash))
    # the owner may still update its own row (create is the upsert _converge uses)
    asyncio.run(store.create(mine.model_copy(update={"description": "edited"})))
    rows = store.list_sync()
    assert [(r.id, r.workspace_id, r.description) for r in rows] == [("hook", "acme-ops", "edited")]
    store.close()
