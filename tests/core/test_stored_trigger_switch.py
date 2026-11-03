"""ADR-0160 (#290) — a stored trigger (event, webhook) can be enabled or paused.

A trigger a pipeline declares is written to the trigger store paused (ADR-0131), and
before this nothing could turn it on: the store had no update, no route set
``enabled``, and the console's Pause/Resume only reached cron schedules. Driven through
the real app (``build_app``) and the real ``EventTriggerBridge``.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.bus import InProcessBus
from cairn.core.dispatch.event_triggers import EventTriggerBridge
from cairn.core.engine.runner import Runner
from cairn.core.engine.triggers_grammar import event_spec_yaml, parse_declared_triggers
from cairn.core.storage.trigger_store import InMemoryTriggerStore, StoredTrigger
from cairn.core.triggers.server import build_app


def _row(rid: str, *, ws: str = "default", enabled: bool = False) -> StoredTrigger:
    (d,) = parse_declared_triggers([{"kind": "event", "topic": "dataset.prepared",
                                     "input": {"key": "$event.key"}}], where="t")
    return StoredTrigger(id=rid, workspace_id=ws, name=rid, source="event",
                         yaml=event_spec_yaml(d, workflow_ref="pipeline:finetune/sft-local"),
                         enabled=enabled, declared_by="pipeline:finetune/sft-local")


@pytest.fixture
def store(monkeypatch):
    import cairn.core.storage.trigger_store as ts

    s = InMemoryTriggerStore()
    monkeypatch.setattr(ts, "default_trigger_store", lambda: s)
    return s


@pytest.fixture
def audits(monkeypatch):
    import cairn.core.obs.audit as audit

    seen: list[tuple[str, dict]] = []

    async def _emit(action, **kw):
        seen.append((action, kw))

    monkeypatch.setattr(audit, "audit_emit", _emit)
    return seen


@pytest.fixture
def client(monkeypatch):
    for k in ("OBS_JWT_SECRET", "OBS_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    return TestClient(build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))


def test_a_declared_trigger_is_listed_paused(client, store):
    asyncio.run(store.create(_row("sft-on-prepared")))
    rows = client.get("/triggers/stored").json()["triggers"]
    assert [(r["id"], r["enabled"], r["declared_by"]) for r in rows] == [
        ("sft-on-prepared", False, "pipeline:finetune/sft-local")]


def test_enable_then_pause_flips_the_row_and_is_audited(client, store, audits):
    asyncio.run(store.create(_row("sft-on-prepared")))

    on = client.patch("/triggers/stored/sft-on-prepared", json={"enabled": True})
    assert on.status_code == 200 and on.json()["enabled"] is True and on.json()["applies"] == "now"
    assert asyncio.run(store.get("sft-on-prepared")).enabled is True

    off = client.patch("/triggers/stored/sft-on-prepared", json={"enabled": False})
    assert off.status_code == 200 and off.json()["enabled"] is False
    assert asyncio.run(store.get("sft-on-prepared")).enabled is False
    assert [a for a, _ in audits] == ["trigger.enabled", "trigger.paused"]
    assert audits[0][1]["target"] == "sft-on-prepared" and audits[0][1]["actor"]


def test_another_workspaces_trigger_is_not_found_and_stays_paused(client, store):
    asyncio.run(store.create(_row("theirs", ws="team")))
    r = client.patch("/triggers/stored/theirs", json={"enabled": True})
    assert r.status_code == 404
    assert asyncio.run(store.get("theirs")).enabled is False


@pytest.mark.parametrize("body", [{}, {"enabled": "yes"}, {"enabled": 1}])
def test_the_switch_must_be_a_boolean(client, store, body):
    asyncio.run(store.create(_row("sft-on-prepared")))
    assert client.patch("/triggers/stored/sft-on-prepared", json=body).status_code == 400


def test_an_enabled_event_trigger_starts_its_pipeline_and_a_paused_one_does_not(client, store, monkeypatch):
    """The switch is what the bus bridge reads: paused starts nothing, enabled starts it."""
    import cairn.core.dispatch.start as start_mod

    started: list[str] = []

    async def fake_launch(ref, inputs, **kw):
        started.append(ref)
        return type("Rec", (), {"pipeline_id": "pl_1"})()

    monkeypatch.setattr(start_mod, "launch_ref", fake_launch)
    asyncio.run(store.create(_row("sft-on-prepared")))

    async def _publish_once() -> None:
        bus = InProcessBus()
        b = EventTriggerBridge(object(), bus=bus, trigger_store=lambda: store)
        await b.start()
        try:
            await bus.publish("dataset.prepared", {"key": "k"}, publisher="pipeline:pl_prep")
            for _ in range(50):
                await asyncio.sleep(0)
        finally:
            b.stop()

    asyncio.run(_publish_once())
    assert started == []
    client.patch("/triggers/stored/sft-on-prepared", json={"enabled": True})
    asyncio.run(_publish_once())
    assert started == ["pipeline:finetune/sft-local"]
