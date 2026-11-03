"""#222 — a pipeline run is ONE trace.

The root ``cairn.pipeline`` span is short (a gate wait can outlive the request, the
process and the server), so its W3C context is stored on the ``PipelineRecord`` and every
later span — legs, gates, flow stages, the terminal ``cairn.pipeline.settle`` — starts as
its child. The decisive case is a gate approved in a SECOND request on a FRESH bridge: the
legs it releases must land in the launch trace. In-memory exporter, real dispatcher,
bridge, bus and operator registry; the flow engine is the only fake.
"""
from __future__ import annotations

import asyncio
import os
import sys

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, ConfigDict

import cairn.core.dispatch.pipeline_bridge as bridge_mod
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.leg_executor import LegOutcome
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.obs import telemetry
from cairn.core.operator import Operation, register_operator
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t222/echo", version="0.1.0")
class _Echo(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        return _P(n_out=int(getattr(inp, "n", 0)) + 1)


@pytest.fixture(autouse=True)
def _ops_registered():
    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Echo, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass


@pytest.fixture
def exporter(monkeypatch):
    """Tracing ON, into memory. A private provider (the global one can be set once)."""
    exp = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    monkeypatch.setattr(telemetry, "_tracer", provider.get_tracer("test"))
    return exp


@pytest.fixture
def fake_flow_engine(monkeypatch):
    """The flow engine is the one fake: ``execute_leg`` reports two stages the way the
    dataflow runner does (``StageSpans`` is what the real one feeds from each poll)."""
    from types import SimpleNamespace

    async def _fake_execute_leg(ref, **kw):
        stages = telemetry.StageSpans()
        for i, name in enumerate(("ingest", "export"), start=1):
            await asyncio.sleep(0.01)
            stages.observe(SimpleNamespace(metrics={"stage": name, "stages_done": i, "stages_total": 2}))
        return LegOutcome(state="succeeded", stats={}, metrics={})

    monkeypatch.setattr(bridge_mod, "execute_leg", _fake_execute_leg)
    monkeypatch.setattr(PipelineBridge, "_leg_engine", lambda self, s: type("E", (), {"name": "fake"})())


def _def(yaml_text: str):
    return parse_pipeline_def(yaml_text, pack="t222", source="<test>")


_PIPELINE = """
name: traced
kind: pipeline
steps:
  - id: first
    task: _t222/echo
    with: {n: 1}
  - id: crunch
    flow: t222/clean
    depends_on: [first]
  - gate: approve
    depends_on: [crunch]
  - id: second
    task: _t222/echo
    with: {n: first.n_out}
    depends_on: [approve]
"""


async def _bridge(store, backend):
    d = RunDispatcher(FakeRunner(), backend=backend)
    bridge = PipelineBridge(d, store=store)
    await bridge.start()
    return d, bridge


async def _wait_for(bridge, pid, states, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await bridge.store.get(pid)
        if rec.status in states:
            return rec
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"pipeline stuck in {rec.status!r}; steps="
                + str({k: v.status for k, v in rec.step_state.items()}))
        await asyncio.sleep(0.02)


def _by_name(exporter):
    out: dict[str, list] = {}
    for s in exporter.get_finished_spans():
        out.setdefault(s.name, []).append(s)
    return out


async def _launch_to_gate(store, backend):
    """Request 1: launch, run to the gate, then the request (and its bridge) is gone."""
    d, bridge = await _bridge(store, backend)
    try:
        rec = await bridge.start_pipeline(plan_pipeline(_def(_PIPELINE)), inputs={})
        rec = await _wait_for(bridge, rec.pipeline_id, ("paused",))
    finally:
        bridge.stop()
    return rec


# ── one trace across launch, a gate approved in a second request, and the legs after ──


async def test_gate_approved_in_a_second_request_joins_the_launch_trace(exporter, fake_flow_engine):
    store, backend = InMemoryPipelineStore(), build_storage()
    rec = await _launch_to_gate(store, backend)
    pid = rec.pipeline_id
    assert rec.step_state["crunch"].status == "done"

    # Request 2: no active span, a FRESH bridge and dispatcher over the same stores.
    from opentelemetry import trace
    assert not trace.get_current_span().get_span_context().is_valid
    d2, bridge2 = await _bridge(store, backend)
    try:
        await bridge2.advance_step(pid, "approve", approver="dev")
        done = await _wait_for(bridge2, pid, ("done", "failed"))
        assert done.status == "done"
        second = await d2.store.get(done.step_state["second"].run_id)
    finally:
        bridge2.stop()

    spans = _by_name(exporter)
    (root,) = spans["cairn.pipeline"]
    trace_id = root.context.trace_id

    # ONE trace id, everywhere.
    assert {s.context.trace_id for s in exporter.get_finished_spans()} == {trace_id}
    # The record carries the root's context (trace id, span id, flags) as a W3C carrier.
    assert done.trace_context["traceparent"] == f"00-{trace_id:032x}-{root.context.span_id:016x}-01"
    # Each leg's Run records the pipeline's trace.
    assert second.trace_id == f"{trace_id:032x}"
    first = await d2.store.get(done.step_state["first"].run_id)
    assert first.trace_id == f"{trace_id:032x}"

    # Parent links: launch-time and post-gate legs, the gate and the settle are all children of the root.
    root_id = root.context.span_id
    (flow_leg,) = spans["cairn.flow_leg"]
    (gate,) = spans["cairn.gate"]
    (settle,) = spans["cairn.pipeline.settle"]
    operators = spans["cairn.operator"]
    assert len(operators) == 2                      # one before the gate, one after it
    for s in (flow_leg, gate, settle, *operators):
        assert s.parent is not None and s.parent.span_id == root_id, s.name
    # Flow stages hang off the flow leg.
    stages = spans["cairn.flow_stage"]
    assert [s.attributes["stage"] for s in stages] == ["ingest", "export"]
    assert all(s.parent.span_id == flow_leg.context.span_id for s in stages)

    # The gate span: pause -> decision, with who decided what.
    assert gate.attributes["decision"] == "approved"
    assert gate.attributes["actor"] == "dev"
    assert gate.attributes["kind"] == "approval"
    assert gate.start_time < gate.end_time
    # The flow leg says what ran and how it ended.
    assert flow_leg.attributes["flow"] == "flow:t222/clean"
    assert flow_leg.attributes["engine"] == "fake"
    assert flow_leg.attributes["outcome"] == "done"
    assert settle.attributes["status"] == "done"
    # The post-gate operator ran in the approving request, after the launch trace's root ended.
    assert max(o.start_time for o in operators) > root.end_time


async def test_a_rejected_gate_in_a_second_request_settles_the_same_trace(exporter, fake_flow_engine):
    store, backend = InMemoryPipelineStore(), build_storage()
    rec = await _launch_to_gate(store, backend)
    _, bridge2 = await _bridge(store, backend)
    try:
        await bridge2.reject_step(rec.pipeline_id, "approve", decider="dev", reason="no")
    finally:
        bridge2.stop()
    spans = _by_name(exporter)
    (root,) = spans["cairn.pipeline"]
    (gate,) = spans["cairn.gate"]
    (settle,) = spans["cairn.pipeline.settle"]
    assert gate.attributes["decision"] == "rejected" and gate.attributes["actor"] == "dev"
    assert settle.attributes["status"] == "failed"
    assert {s.context.trace_id for s in exporter.get_finished_spans()} == {root.context.trace_id}


# ── tracing off, or the telemetry extra missing: nothing to pay, nothing to break ──


async def test_no_exporter_configured_no_overhead_no_errors(monkeypatch, fake_flow_engine):
    monkeypatch.setattr(telemetry, "_tracer", None)
    store, backend = InMemoryPipelineStore(), build_storage()
    rec = await _launch_to_gate(store, backend)
    assert rec.trace_context == {}
    d2, bridge2 = await _bridge(store, backend)
    try:
        await bridge2.advance_step(rec.pipeline_id, "approve", approver="dev")
        done = await _wait_for(bridge2, rec.pipeline_id, ("done", "failed"))
    finally:
        bridge2.stop()
    assert done.status == "done" and done.trace_context == {}
    run = await d2.store.get(done.step_state["second"].run_id)
    assert run.trace_id == ""


async def test_without_the_telemetry_extra_a_pipeline_still_runs(monkeypatch, fake_flow_engine):
    """Tracing stays off when `opentelemetry` is not importable (init_tracing's guard, #248),
    and no helper in the pipeline path imports it while off."""
    monkeypatch.setattr(telemetry, "_tracer", None)
    for mod in [m for m in sys.modules if m == "opentelemetry" or m.startswith("opentelemetry.")]:
        monkeypatch.setitem(sys.modules, mod, None)   # `import opentelemetry...` -> ImportError
    store, backend = InMemoryPipelineStore(), build_storage()
    rec = await _launch_to_gate(store, backend)
    _, bridge2 = await _bridge(store, backend)
    try:
        await bridge2.advance_step(rec.pipeline_id, "approve", approver="dev")
        done = await _wait_for(bridge2, rec.pipeline_id, ("done", "failed"))
    finally:
        bridge2.stop()
    assert done.status == "done"


def test_helpers_are_inert_when_tracing_is_off(monkeypatch):
    monkeypatch.setattr(telemetry, "_tracer", None)
    for mod in [m for m in sys.modules if m == "opentelemetry" or m.startswith("opentelemetry.")]:
        monkeypatch.setitem(sys.modules, mod, None)
    carrier = {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
    with telemetry.span("x", {"a": 1}, parent=carrier, start_time="2026-01-01T00:00:00+00:00") as s:
        assert s is None
    with telemetry.use_parent(carrier):
        pass
    assert telemetry.trace_carrier() == {} and telemetry.current_traceparent() == ""
    telemetry.StageSpans().observe(type("S", (), {"metrics": {"stages_done": 1}})())


# ── the span() / carrier contract ──────────────────────────────────────────────


def test_span_parent_param_starts_a_child_of_the_stored_context(exporter):
    with telemetry.span("root"):
        carrier = telemetry.trace_carrier()
    with telemetry.span("elsewhere"):                  # something else is current
        with telemetry.span("child", parent=carrier):
            assert telemetry.current_traceparent().split("-")[1] == carrier["traceparent"].split("-")[1]
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["child"].parent.span_id == spans["root"].context.span_id
    assert spans["child"].context.trace_id == spans["root"].context.trace_id
    assert spans["elsewhere"].context.trace_id != spans["root"].context.trace_id
    # a bare traceparent string works too, and a malformed one degrades to ambient nesting
    with telemetry.span("by_string", parent=carrier["traceparent"]):
        pass
    with telemetry.span("garbage", parent={"traceparent": "nonsense"}):
        pass
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["by_string"].parent.span_id == spans["root"].context.span_id
    assert spans["garbage"].parent is None


def test_use_parent_makes_the_stored_context_ambient(exporter):
    with telemetry.span("root"):
        carrier = telemetry.trace_carrier()
    with telemetry.use_parent(carrier):
        with telemetry.span("inner"):
            pass
    with telemetry.span("after"):
        pass
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["inner"].parent.span_id == spans["root"].context.span_id
    assert spans["after"].parent is None               # detached again


def test_remote_jobs_get_the_traceparent_next_to_the_run_identity(exporter):
    from cairn.core.operator.jobs import live_channel_env_for

    assert "TRACEPARENT" not in live_channel_env_for("run-1", "flow")        # no active span
    with telemetry.span("leg"):
        env = live_channel_env_for("run-1", "flow")
        assert env["TRACEPARENT"] == telemetry.current_traceparent()
        assert env["CAIRN_RUN_ID"] == "run-1"


def test_stage_spans_nest_under_the_current_span_and_collapse_between_polls(exporter):
    from types import SimpleNamespace

    with telemetry.span("leg"):
        stages = telemetry.StageSpans()
        stages.observe(SimpleNamespace(metrics={}))                                  # nothing reported yet
        stages.observe(SimpleNamespace(metrics={"stage": "a", "stages_done": 1, "stages_total": 3}))
        stages.observe(SimpleNamespace(metrics={"stage": "a", "stages_done": 1, "stages_total": 3}))  # same poll
        stages.observe(SimpleNamespace(metrics={"stage": "c", "stages_done": 3, "stages_total": 3}))  # b+c between polls
    spans = {s.name: s for s in exporter.get_finished_spans()}
    got = [s for s in exporter.get_finished_spans() if s.name == "cairn.flow_stage"]
    assert [s.attributes["stage"] for s in got] == ["a", "c"]
    assert all(s.parent.span_id == spans["leg"].context.span_id for s in got)
    assert got[0].end_time <= got[1].start_time


async def test_a_legs_cairn_run_span_joins_the_stored_context_and_run_trace_id_records_it(exporter):
    """The dispatcher's per-leg ``cairn.run`` span (one asyncio task per run) inherits the
    ambient context the bridge sets from the record, so it is a child of the pipeline
    root instead of a root of its own, and ``Run.trace_id`` is the pipeline's."""
    from cairn.core.engine.schema import TriggerEvent

    with telemetry.span("cairn.pipeline"):
        carrier = telemetry.trace_carrier()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    with telemetry.use_parent(carrier):
        run = await d.submit(TriggerEvent(source="manual", payload={"service": "db-pool"}), wait=True)
    run = await d.store.get(run.id)
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["cairn.run"].parent.span_id == spans["cairn.pipeline"].context.span_id
    assert run.trace_id == f"{spans['cairn.pipeline'].context.trace_id:032x}"


# ── the Temporal owner: the workflow carries no trace state; the activities read the record ──


async def test_temporal_activities_start_spans_under_the_stored_context(exporter, monkeypatch):
    pytest.importorskip("temporalio")
    from types import SimpleNamespace

    import cairn.core.storage.pipeline_store as store_mod
    from cairn.core.runtime.temporal import pipeline_activities as PA

    store = InMemoryPipelineStore()
    with telemetry.span("cairn.pipeline"):
        carrier = telemetry.trace_carrier()
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState
    rec = await store.put(PipelineRecord(
        trace_context=carrier, status="running", step_state={"leg": StepState(status="running")}))
    pid = rec.pipeline_id
    monkeypatch.setattr(store_mod, "default_pipeline_store", lambda: store)

    async def _flow(*a, **kw):
        with telemetry.span("inside-the-flow-leg"):
            return "run_flow"

    async def _run_template(r, *a, **kw):
        with telemetry.span("cairn.run"):
            return SimpleNamespace(id="run_tpl")

    async def _record(*a, **kw):
        return SimpleNamespace(workspace_id="default")

    monkeypatch.setattr(PA, "_run_flow_leg", _flow)
    monkeypatch.setattr(PA, "_leg_pipeline_record", _record)
    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(run_template=_run_template))

    assert await PA.pipeline_submit_step("flow:p/f", {}, pid, "leg", None, {}) == "run_flow"
    assert await PA.pipeline_submit_step("p/template", {}, pid, "leg", None, {}) == "run_tpl"
    await PA.pipeline_mirror_state(pid, {"leg": {"status": "done"}}, "done")

    spans = {s.name: s for s in exporter.get_finished_spans()}
    root = spans["cairn.pipeline"]
    assert spans["cairn.flow_leg"].parent.span_id == root.context.span_id
    assert spans["inside-the-flow-leg"].parent.span_id == spans["cairn.flow_leg"].context.span_id
    assert spans["cairn.run"].parent.span_id == root.context.span_id
    assert spans["cairn.pipeline.settle"].parent.span_id == root.context.span_id
    assert spans["cairn.pipeline.settle"].attributes["status"] == "done"
