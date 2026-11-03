"""ADR-0161 (#293) — a pipeline resolves its ``outputs:`` when it ends ``done`` and may
``emits:`` them as one event.

``outputs:`` was parsed and documented but never resolved, stored or shown, and only a
task or flow step could emit, so no event could leave after a pipeline's last gate.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

from cairn.core.bus import get_bus
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.leg_hooks import resolve_pipeline_outputs
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import (
    PipelineDefError,
    identity_dump,
    parse_pipeline_def,
    plan_pipeline,
)
from cairn.core.operator import Operation, register_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

VERSIONS = ["demo-train@1", "demo-val@1", "demo-test@1"]
TRAIN = "join('', ['dataset:', steps.split.versions[?contains(@, '-train@')] | [0]])"


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t293/split", version="0.1.0")
class _Split(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        if getattr(inp, "fail", False):
            raise RuntimeError("the split failed")
        return _P(versions=list(VERSIONS))


@pytest.fixture(autouse=True)
def _registered():
    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Split, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass
    default_registry.attach_metadata(OperatorEntry(name="_t293/split", risk="read_only"))


def _yaml(*, emits: str = "  dataset.prepared", fail: bool = False, outputs: str = "") -> str:
    outputs = outputs or (
        "outputs:\n"
        f"  train_version: \"{TRAIN}\"\n"
        "  versions: split.versions\n")
    return (
        "name: prep\nkind: pipeline\n"
        f"emits:{emits}\n"
        f"{outputs}"
        "steps:\n"
        f"  - id: split\n    task: _t293/split\n    with: {{fail: {str(fail).lower()}}}\n"
        "  - gate: data-ok\n    depends_on: [split]\n    predicate: length(steps.split.versions) == `3`\n")


# ── grammar ───────────────────────────────────────────────────────────────────


def test_a_pipeline_level_emits_must_be_a_domain_topic():
    for bad in (" run.done", " not a topic"):
        with pytest.raises(PipelineDefError, match="emits"):
            parse_pipeline_def(_yaml(emits=bad), pack="t293", source="<test>")


def test_an_outputs_expression_that_does_not_compile_fails_at_load():
    with pytest.raises(PipelineDefError, match="outputs.bad"):
        parse_pipeline_def(_yaml(outputs="outputs:\n  bad: \"steps.split.[[\"\n"), pack="t293", source="<test>")


def test_a_pipeline_cannot_start_on_the_topic_it_emits():
    text = _yaml().replace("kind: pipeline\n", "kind: pipeline\ntriggers:\n  - {kind: event, topic: dataset.prepared}\n")
    with pytest.raises(PipelineDefError, match="every run would start the next"):
        parse_pipeline_def(text, pack="t293", source="<test>")


def test_an_unset_emits_stays_out_of_the_identity():
    """#1255 — no installed pipeline's content_hash moves because the field exists."""
    plain = parse_pipeline_def(_yaml().replace("emits:  dataset.prepared\n", ""), pack="t293", source="<test>")
    assert "emits" not in identity_dump(plain)
    assert identity_dump(parse_pipeline_def(_yaml(), pack="t293", source="<test>"))["emits"] == "dataset.prepared"


# ── resolution ────────────────────────────────────────────────────────────────


def test_shorthand_and_expressions_resolve_and_a_miss_is_reported_not_raised():
    steps = {"split": {"findings": {"versions": VERSIONS}}}
    values, errors = resolve_pipeline_outputs(
        {"versions": "split.versions",                 # shorthand: the findings fallback
         "n": "length(steps.split.findings.versions)",
         "where": "input.register",
         "missing": "split.nothing"},
        inputs={"register": "demo"}, steps=steps)
    assert values == {"versions": VERSIONS, "n": 3, "where": "demo", "missing": None}
    assert set(errors) == {"missing"}


# ── the bridge: resolved once on `done`, emitted once, never on failure ──────


async def _run(yaml_text: str):
    seen: list = []

    async def _seen(msg) -> None:
        seen.append(msg)

    sub = await get_bus().subscribe("dataset.prepared", _seen)  # the bus the bridge publishes on
    disp = RunDispatcher(FakeRunner(), backend=build_storage())
    pstore = InMemoryPipelineStore()
    bridge = PipelineBridge(disp, store=pstore)
    await bridge.start()
    try:
        rec = await bridge.start_pipeline(
            plan_pipeline(parse_pipeline_def(yaml_text, pack="t293", source="<test>")), inputs={})
        deadline = asyncio.get_event_loop().time() + 8
        while True:
            rec = await pstore.get(rec.pipeline_id)
            if rec.status in ("done", "failed", "cancelled"):
                break
            assert asyncio.get_event_loop().time() < deadline, rec.status
            await asyncio.sleep(0.05)
        for _ in range(50):
            await asyncio.sleep(0)
        return rec, seen
    finally:
        sub.unsubscribe()
        bridge.stop()


async def test_a_done_pipeline_stores_its_outputs_and_emits_them_once_after_its_last_gate():
    rec, seen = await _run(_yaml())

    assert rec.status == "done"
    assert rec.outputs == {"train_version": "dataset:demo-train@1", "versions": VERSIONS}
    assert rec.outputs_errors == {}
    (msg,) = seen
    assert msg.payload == rec.outputs
    assert msg.publisher == f"pipeline:{rec.pipeline_id}" and msg.correlation_id == rec.pipeline_id


async def test_a_failed_pipeline_resolves_and_emits_nothing():
    rec, seen = await _run(_yaml(fail=True))

    assert rec.status == "failed"
    assert rec.outputs is None and seen == []
