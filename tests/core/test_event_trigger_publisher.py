"""#289 — a pipeline started by a `kind: event` trigger names who published the event.

A leg's `emits:` publishes with `publisher="pipeline:<id>"` and the pipeline id as
`correlation_id` (`PipelineBridge._emit_step_event`). `EventTriggerBridge` puts both on
the started pipeline's `trigger`, so the record links back to the pipeline that fed it.
"""
from __future__ import annotations

import asyncio

import pytest

from cairn.core.bus import InProcessBus
from cairn.core.dispatch.event_triggers import EventTriggerBridge
from cairn.core.engine.triggers_grammar import event_spec_yaml, parse_declared_triggers
from cairn.core.storage.trigger_store import InMemoryTriggerStore, StoredTrigger


def _row() -> StoredTrigger:
    (d,) = parse_declared_triggers([{"kind": "event", "topic": "dataset.prepared",
                                     "input": {"key": "$event.key"}}], where="t")
    return StoredTrigger(id="t1", workspace_id="default", name="t1", source="event",
                         yaml=event_spec_yaml(d, workflow_ref="pipeline:demo/x"),
                         enabled=True, declared_by="pipeline:demo/x")


@pytest.fixture
def bridge(monkeypatch):
    import cairn.core.dispatch.start as start_mod

    store = InMemoryTriggerStore()
    launches: list[dict] = []

    async def fake_launch(ref, inputs, *, dispatcher, workspace_id="default", trigger=None, **kw):
        launches.append(dict(trigger or {}))
        return type("Rec", (), {"pipeline_id": f"pl_{len(launches)}"})()

    monkeypatch.setattr(start_mod, "launch_ref", fake_launch)
    bus = InProcessBus()
    return EventTriggerBridge(object(), bus=bus, trigger_store=lambda: store), bus, store, launches


async def _fire(bridge, **publish_kw) -> dict:
    b, bus, store, launches = bridge
    await store.create(_row())
    await b.start()
    try:
        await bus.publish("dataset.prepared", {"key": "k1"}, **publish_kw)
        for _ in range(50):
            await asyncio.sleep(0)
    finally:
        b.stop()
    (trigger,) = launches
    return trigger


async def test_a_started_pipeline_names_the_pipeline_that_published_the_event(bridge):
    trigger = await _fire(bridge, publisher="pipeline:pl_prep", correlation_id="pl_prep")

    assert trigger == {"source": "event", "topic": "dataset.prepared", "trigger_id": "t1",
                       "publisher": "pipeline:pl_prep", "correlation_id": "pl_prep"}


async def test_an_event_with_no_publisher_adds_nothing_to_the_trigger(bridge):
    trigger = await _fire(bridge)

    assert trigger == {"source": "event", "topic": "dataset.prepared", "trigger_id": "t1"}
