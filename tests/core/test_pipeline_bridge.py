"""ADR-0069 (#746) Slice 2 — the core PipelineBridge: advance a bundle pipeline off
run completions, enforcing the gate as a trigger condition over persisted output.

Uses fakes for the dispatcher + runs store so the enforcement logic is tested without
a live engine/bus. The bridge only needs `dispatcher.store.get(run_id)` and
`dispatcher.run_template(...)`.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cairn.core.dispatch.pipeline_bridge import PipelineBridge, reconstruct_step_output
from cairn.core.engine.bundle import BundlePipeline, BundlePipelineStep
from cairn.core.engine.bundle_pipeline import plan_bundle
from cairn.core.storage.pipeline_store import InMemoryPipelineStore


# ── fakes ──────────────────────────────────────────────────────────────────


class FakeRunStore:
    def __init__(self):
        self.runs: dict[str, SimpleNamespace] = {}

    async def get(self, run_id):
        return self.runs.get(run_id)


class FakeDispatcher:
    """Records run_template calls; each becomes a fake run in `.store`."""

    def __init__(self):
        self.store = FakeRunStore()
        self.calls: list[dict] = []
        self._n = 0

    async def run_template(self, template_ref, inputs, *, environment=None,
                           bundle_pipeline_id="", step_id="", **kw):
        self._n += 1
        run_id = f"run_{self._n}"
        self.calls.append({
            "run_id": run_id, "ref": template_ref, "inputs": inputs,
            "environment": environment, "bundle_pipeline_id": bundle_pipeline_id,
            "step_id": step_id,
        })
        run = SimpleNamespace(
            id=run_id, bundle_pipeline_id=bundle_pipeline_id, step_id=step_id,
            result_payloads=[], final_verdict="",
        )
        self.store.runs[run_id] = run
        return run


def _msg(topic, run_id):
    return SimpleNamespace(topic=topic, correlation_id=run_id)


async def _complete(bridge, disp, run_id, *, output=None, verdict="", topic="run.done"):
    """Simulate a step run finishing: stamp its persisted output, fire the terminal event."""
    run = disp.store.runs[run_id]
    run.result_payloads = [{"output": output or {}}]
    run.final_verdict = verdict
    await bridge._on_run_terminal(_msg(topic, run_id))


def _prep_train_plan(gate="$steps.prep.verdict == 'pass'"):
    return plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="dataset_readiness", template="prep"),
        BundlePipelineStep(
            id="train", pack="training", template="finetune-remote",
            depends_on=["prep"], gate=gate,
            inputs={"dataset": "$steps.prep.staged_dataset_uri"},
        ),
    ]))


def _bridge():
    disp = FakeDispatcher()
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=_NullBus())
    return bridge, disp


class _NullBus:
    async def publish(self, *a, **k):  # audit sink — swallow
        return None

    async def subscribe(self, _topics, _handler):  # on:<event> steps subscribe here
        return SimpleNamespace(unsubscribe=lambda: None)

    async def replay(self, _topic, _since):  # durable-bus shape, empty log (no catch-up)
        return
        yield  # pragma: no cover — makes this an async generator


class _ReplayBus(_NullBus):
    """A durable bus fake with a canned log — serves past events via replay() (#763)."""

    def __init__(self, log=None):
        self._log = list(log or [])

    async def replay(self, topic, _since):
        for m in self._log:
            if m.topic == topic:
                yield m


# ── the happy path: gate passes, train submits, pipeline completes ─────────


async def test_linear_gate_pass_submits_next_and_completes():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={"path": "s3://ds"})
    # prep submitted first (entrypoint), train not yet
    assert [c["step_id"] for c in disp.calls] == ["prep"]
    assert rec.status == "running"

    # prep completes PASS with a staged dataset → gate passes → train submits
    await _complete(bridge, disp, "run_1",
                    output={"verdict": "pass", "staged_dataset_uri": "s3://staged"})
    assert [c["step_id"] for c in disp.calls] == ["prep", "train"]
    train_call = disp.calls[1]
    assert train_call["inputs"] == {"dataset": "s3://staged"}          # data contract from persisted output
    assert train_call["bundle_pipeline_id"] == rec.pipeline_id          # lineage stamp
    assert train_call["step_id"] == "train"

    # train completes → pipeline done
    await _complete(bridge, disp, "run_2", output={"verdict": "pass"})
    final = await bridge.store.get(rec.pipeline_id)
    assert final.status == "done"
    assert final.step_state["prep"].status == "done"
    assert final.step_state["train"].status == "done"


# ── the governance guarantee: gate fails → next never submits ──────────────


async def test_gate_fail_terminates_pipeline_and_blocks_next():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={"path": "s3://ds"})

    await _complete(bridge, disp, "run_1", output={"verdict": "fail"})   # prep says FAIL
    # train was NEVER submitted
    assert [c["step_id"] for c in disp.calls] == ["prep"]
    final = await bridge.store.get(rec.pipeline_id)
    assert final.status == "failed"
    assert final.step_state["train"].status == "skipped"
    assert final.step_state["train"].verdict == "gate_blocked"
    assert "blocked by gate" in final.trigger.get("failure_reason", "")


async def test_step_run_failure_terminates_pipeline():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={"path": "s3://ds"})
    await _complete(bridge, disp, "run_1", topic="run.failed")           # prep run itself crashed
    final = await bridge.store.get(rec.pipeline_id)
    assert final.status == "failed"
    assert final.step_state["prep"].status == "failed"
    assert len(disp.calls) == 1                                          # train never submitted


# ── DAG join: c waits for BOTH a and b ─────────────────────────────────────


async def test_join_waits_for_all_deps():
    # diamond: root fans out to a + b (both depend on root), c joins on [a, b].
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="root", pack="p", template="root"),
        BundlePipelineStep(id="a", pack="p", template="a", depends_on=["root"]),
        BundlePipelineStep(id="b", pack="p", template="b", depends_on=["root"]),
        BundlePipelineStep(id="c", pack="p", template="c", depends_on=["a", "b"]),
    ]))
    bridge, disp = _bridge()
    await bridge.start_pipeline(plan, inputs={})
    submitted = lambda: [c["step_id"] for c in disp.calls]
    assert submitted() == ["root"]                                       # only the entrypoint

    # root completes → BOTH a and b fan out
    root_run = next(c["run_id"] for c in disp.calls if c["step_id"] == "root")
    await _complete(bridge, disp, root_run, output={"ok": True})
    assert set(submitted()) == {"root", "a", "b"}

    # complete a only → c must NOT fire yet (join arm b outstanding)
    a_run = next(c["run_id"] for c in disp.calls if c["step_id"] == "a")
    await _complete(bridge, disp, a_run, output={"ok": True})
    assert "c" not in submitted()

    # complete b → join satisfied → c fires
    b_run = next(c["run_id"] for c in disp.calls if c["step_id"] == "b")
    await _complete(bridge, disp, b_run, output={"ok": True})
    assert "c" in submitted()


# ── non-pipeline runs on the firehose are ignored ──────────────────────────


async def test_ignores_runs_without_pipeline_id():
    bridge, disp = _bridge()
    disp.store.runs["stray"] = SimpleNamespace(
        id="stray", bundle_pipeline_id="", step_id="", result_payloads=[], final_verdict="")
    await bridge._on_run_terminal(_msg("run.done", "stray"))  # must not raise
    await bridge._on_run_terminal(_msg("run.done", "missing"))  # unknown run → no-op


# ── the output normalizer handles both persisted shapes ────────────────────


def _prep_then(step2, **step2_kw):
    return plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep"),
        BundlePipelineStep(id=step2, pack="p", template=step2, depends_on=["prep"],
                           inputs={"dataset": "$steps.prep.staged_dataset_uri"}, **step2_kw),
    ]))


# ── manual trigger: ready step waits for an explicit advance ───────────────


async def test_manual_step_awaits_then_advances():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", trigger="manual"), inputs={})
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://staged"})

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.status == "paused"                                     # waiting on a human
    assert rec.step_state["train"].status == "awaiting_advance"
    assert [c["step_id"] for c in disp.calls] == ["prep"]            # train NOT submitted

    await bridge.advance_step(rec.pipeline_id, "train")
    assert [c["step_id"] for c in disp.calls] == ["prep", "train"]
    assert disp.calls[1]["inputs"] == {"dataset": "s3://staged"}     # resolved at advance time
    await _complete(bridge, disp, "run_2", output={})
    assert (await bridge.store.get(rec.pipeline_id)).status == "done"


# ── approval: ready step waits for HITL, records the approver ───────────────


async def test_approval_step_records_approver():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.step_state["train"].status == "awaiting_approval"
    assert len(disp.calls) == 1

    await bridge.advance_step(rec.pipeline_id, "train", approver="alice")
    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.step_state["train"].approver == "alice"
    assert [c["step_id"] for c in disp.calls] == ["prep", "train"]


async def test_advance_non_awaiting_step_raises():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={"path": "x"})
    with pytest.raises(ValueError, match="not awaiting"):         # prep is running (auto)
        await bridge.advance_step(rec.pipeline_id, "prep")


async def test_cancel_pipeline_blocks_further_advance():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={"path": "x"})
    cancelled = await bridge.cancel_pipeline(rec.pipeline_id)
    assert cancelled.status == "cancelled"
    # a late completion of the in-flight prep run no-ops (pipeline is terminal)
    await _complete(bridge, disp, "run_1", output={"verdict": "pass", "staged_dataset_uri": "s3://s"})
    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.status == "cancelled"
    assert [c["step_id"] for c in disp.calls] == ["prep"]           # train never submitted


async def test_cancel_settles_a_gate_waiting_on_a_person():
    """#2541 — a pipeline cancelled while paused at a person gate must not leave
    that gate `awaiting_approval`: nobody can act on it any more (`advance_step`
    refuses a cancelled pipeline), yet the approvals inbox kept listing it."""
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
    assert (await bridge.store.get(rec.pipeline_id)).step_state["train"].status == "awaiting_approval"

    await bridge.cancel_pipeline(rec.pipeline_id)

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.status == "cancelled"
    assert not [sid for sid, st in rec.step_state.items()
                if st.status in ("awaiting_approval", "awaiting_advance")], rec.step_state
    train = rec.step_state["train"]
    assert (train.status, train.verdict) == ("skipped", "cancelled")
    assert train.ended_at
    assert rec.step_state["prep"].status == "done"                 # finished work is untouched


def test_reconstruct_step_output_shapes():
    # #738 resume shape
    r1 = SimpleNamespace(result_payloads=[{"output": {"verdict": "pass", "findings": {"x": 1}}}])
    assert reconstruct_step_output(r1) == {"verdict": "pass", "findings": {"x": 1}}
    # initial-completion RunResult shape (structured output under raw_output)
    r2 = SimpleNamespace(result_payloads=[{"raw_output": {"dataset_uri": "s3://d"}}])
    assert reconstruct_step_output(r2) == {"dataset_uri": "s3://d"}
    # empty / missing → {}
    assert reconstruct_step_output(SimpleNamespace(result_payloads=[])) == {}
    assert reconstruct_step_output(SimpleNamespace(result_payloads=None)) == {}


# ── ADR-0069 §3c: event-driven triggers (on:<event> / schedule:<cron>) ──────


async def test_on_event_step_awaits_then_fires_on_event():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("notify", trigger="on:model.ready"), inputs={})
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.step_state["notify"].status == "awaiting_event"
    assert len(disp.calls) == 1                                          # not submitted yet

    # a matching domain event fires it
    await bridge._on_domain_event(SimpleNamespace(topic="model.ready"))
    assert [c["step_id"] for c in disp.calls] == ["prep", "notify"]
    assert disp.calls[1]["inputs"] == {"dataset": "s3://s"}             # data contract still resolved


# ── #763: durable bus replays a missed on:<event> on (re)subscribe ──────────


async def test_on_event_replays_missed_event_on_durable_bus():
    """An event already in the durable log when the step arms its subscription still fires
    the step — no live publish needed (closes the cross-process / restart-window gap)."""
    disp = FakeDispatcher()
    past = SimpleNamespace(topic="model.ready", payload={}, correlation_id=None)
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=_ReplayBus(log=[past]))
    rec = await bridge.start_pipeline(_prep_then("notify", trigger="on:model.ready"), inputs={})
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.step_state["notify"].status == "awaiting_event"
    assert rec.step_state["notify"].awaiting_since is not None          # replay cursor recorded

    # drain the detached replay task — it fires the already-logged event WITHOUT a live publish
    await asyncio.gather(*list(bridge._replay_tasks))
    assert [c["step_id"] for c in disp.calls] == ["prep", "notify"]
    assert disp.calls[1]["inputs"] == {"dataset": "s3://s"}


async def test_inprocess_bus_warns_on_event_trigger(caplog):
    """on:<event> on the in-process bus (no replay) → a one-shot #763 warning."""
    from cairn.core.bus import InProcessBus

    disp = FakeDispatcher()
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=InProcessBus())
    try:
        with caplog.at_level("WARNING", logger="cairn.pipeline_bridge"):
            await bridge.start_pipeline(_prep_then("notify", trigger="on:model.ready"), inputs={})
            await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
        assert "in-process bus" in caplog.text and "#763" in caplog.text
    finally:
        bridge.stop()


# ── a step's emits event fires a downstream on:<event> step (ADR-0069) ──────


async def test_step_emits_event_fires_downstream_on_event():
    """A step's declared `emits` is published on completion and drives a downstream
    on:<event> step — the intra-pipeline chaining that was inert before the emit wiring."""
    from cairn.core.bus import InProcessBus

    disp = FakeDispatcher()
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=InProcessBus())
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="train", pack="p", template="train", emits="model.trained"),
        BundlePipelineStep(id="serve", pack="p", template="serve",
                           depends_on=["train"], trigger="on:model.trained"),
    ]))
    try:
        await bridge.start_pipeline(plan, inputs={})
        await _complete(bridge, disp, "run_1", output={"endpoint": "https://x"})
        # train done → advance arms serve's on:model.trained sub → emit publishes it →
        # the bus consumer task delivers → serve submits. Drain the loop until it lands.
        for _ in range(50):
            if len(disp.calls) >= 2:
                break
            await asyncio.sleep(0)
        assert [c["step_id"] for c in disp.calls] == ["train", "serve"]
    finally:
        bridge.stop()


async def test_schedule_step_awaits_and_auto_advances():
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("nightly", trigger="schedule:0 0 * * *"), inputs={})
    try:
        await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
        rec = await bridge.store.get(rec.pipeline_id)
        assert rec.step_state["nightly"].status == "awaiting_schedule"
        assert bridge._scheduler is not None
        assert bridge._scheduler.get_job(f"pl:{rec.pipeline_id}:nightly") is not None
        assert len(disp.calls) == 1                                      # cron hasn't ticked

        # simulate the cron firing
        await bridge._auto_advance(rec.pipeline_id, "nightly")
        assert [c["step_id"] for c in disp.calls] == ["prep", "nightly"]
    finally:
        bridge.stop()


# ── ADR-0069 Phase 4: restart re-adopts in-flight pipelines ────────────────


async def test_restart_adopts_in_flight_pipeline(monkeypatch):
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    # #2555 — a durable runtime can still finish a running template run, so it is
    # re-mapped; under the in-process runtime nothing can, and it is parked
    # (tests/core/test_pipeline_task_resume.py).
    monkeypatch.setattr("cairn.core.settings.runtime", lambda: "dbos")

    store = InMemoryPipelineStore()
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="a", pack="p", template="a"),
        BundlePipelineStep(id="b", pack="p", template="b", depends_on=["a"], trigger="on:evt"),
    ]))
    rec = PipelineRecord(plan=plan, status="running", step_state={
        "a": StepState(status="running", run_id="run_9"),   # a governed run still in flight
        "b": StepState(status="awaiting_event"),            # an event trigger to re-arm
    })
    await store.put(rec)

    bridge = PipelineBridge(FakeDispatcher(), store=store, bus=_NullBus())
    await bridge.start()  # Phase-4 adopt
    try:
        assert bridge._run_to_pipeline.get("run_9") == rec.pipeline_id          # completion will advance it
        assert (rec.pipeline_id, "b") in bridge._event_waiters.get("evt", set())  # event re-subscribed
    finally:
        bridge.stop()


# ── ADR-0069 Phase 4 Slice ④: start_pipeline dispatches to Temporal ─────────


async def test_start_pipeline_under_temporal_dispatches_to_workflow(monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep

    monkeypatch.setattr("cairn.core.settings.runtime", lambda: "temporal")
    calls: dict = {}

    class _FakeClient:
        async def start_workflow(self, wf, *, args, id, task_queue):  # noqa: A002
            calls.update(wf=wf, args=args, id=id, task_queue=task_queue)

    async def _fake_connect(address, **kw):
        return _FakeClient()

    monkeypatch.setattr("temporalio.client.Client.connect", _fake_connect)

    bridge, disp = _bridge()
    # #1135 — the bundle FORMAT is gone; the bridge that dispatches plans to
    # Temporal is not, so build the execution IR directly (ADR-0084 D5).
    plan = PipelinePlan(name="pl", steps=[
        PipelinePlanStep(id="prep", ref="p/prep"),
        PipelinePlanStep(id="train", ref="p/train", depends_on=["prep"],
                         gate="$steps.prep.verdict == 'pass'"),
    ])
    rec = await bridge.start_pipeline(plan, inputs={"path": "s3://ds"},
                                      bundle_ref="demo@0.1.0")

    assert rec.status == "queued"                          # record created up front
    assert rec.trigger.get("owner") == "temporal"
    assert calls["wf"] == "cairn.PipelineWorkflow"
    assert calls["id"] == rec.pipeline_id                  # workflow keyed by pipeline id
    assert calls["args"][2] == rec.pipeline_id             # (plan_dict, inputs, pipeline_id)
    assert calls["args"][1] == {"path": "s3://ds"}
    assert [c["step_id"] for c in disp.calls] == []        # NO in-process run_template


# ── ADR-0069 Phase 4 Slice ③: bus→signal bridge for on:<event> ──────────────


async def test_temporal_pipeline_forwards_event_as_signal(monkeypatch):
    """A Temporal-owned pipeline's on:<event> wait lives in the durable workflow — a
    matching bus event is forwarded as a `deliver_event` signal, not advanced in-process."""
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep

    monkeypatch.setattr("cairn.core.settings.runtime", lambda: "temporal")

    class _FakeClient:
        async def start_workflow(self, wf, *, args, id, task_queue):  # noqa: A002
            return None

    async def _fake_connect(address, **kw):
        return _FakeClient()

    monkeypatch.setattr("temporalio.client.Client.connect", _fake_connect)

    bridge, disp = _bridge()
    signals: list = []

    async def _rec_signal(pid, signal, *args):
        signals.append((pid, signal, args))

    monkeypatch.setattr(bridge, "_signal_temporal", _rec_signal)

    plan = PipelinePlan(name="pl", steps=[
        PipelinePlanStep(id="train", ref="p/train"),
        PipelinePlanStep(id="serve", ref="p/serve", depends_on=["train"],
                         trigger="on:model.trained"),
    ])
    rec = await bridge.start_pipeline(plan, inputs={}, bundle_ref="demo@0.1.0")

    # the on:<event> topic is subscribed at start — the bus→signal bridge is armed
    assert (rec.pipeline_id, "serve") in bridge._event_waiters.get("model.trained", set())

    # a matching domain event → deliver_event signal to the durable workflow (not run_template)
    await bridge._on_domain_event(SimpleNamespace(topic="model.trained"))
    assert signals == [(rec.pipeline_id, "deliver_event", ("model.trained",))]
    assert disp.calls == []


# ── #1532: a terminal routes on a replica that never launched the run ────────
#
# `_run_to_pipeline` is a PROCESS-LOCAL dict. With N control-plane replicas the
# bus delivers a run's terminal to whichever replica is subscribed, not to the
# one that launched it — the map missed, the event was dropped, and the pipeline
# silently stopped advancing. The publisher now stamps the owning pipeline onto
# the message (it holds the run, and `Run.bundle_pipeline_id` is set at submit),
# so the link travels with the event instead of living in one process's memory.


def _msg_with_pipeline(topic, run_id, pipeline_id):
    from cairn.core.storage.protocols import PIPELINE_ID_KEY

    return SimpleNamespace(topic=topic, correlation_id=run_id,
                           data={PIPELINE_ID_KEY: pipeline_id})


async def test_a_terminal_routes_from_the_message_not_the_local_map():
    """THE cross-replica case: this bridge never launched the run, so its map is
    empty — the event must still advance the pipeline."""
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={})
    pid = rec.pipeline_id
    run_id = disp.calls[0]["run_id"]

    # simulate "a different replica": drop everything this process learned at
    # submit time, leaving only what the message carries.
    bridge._run_to_pipeline.clear()

    disp.store.runs[run_id].result_payloads = [{"output": {"verdict": "pass"}}]
    await bridge._on_run_terminal(_msg_with_pipeline("run.done", run_id, pid))

    rec = await bridge.store.get(pid)
    assert rec.step_state[disp.calls[0]["step_id"]].status == "done", (
        "a terminal delivered to a replica that did not launch the run must "
        "still advance its pipeline (#1532)")


async def test_the_local_map_still_works_when_the_message_lacks_the_key():
    """Back-compat: events published before #1532 (and any path that emits its
    own terminal without the stamp) keep routing through the map."""
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_train_plan(), inputs={})
    run_id = disp.calls[0]["run_id"]
    assert bridge._run_to_pipeline.get(run_id) == rec.pipeline_id

    disp.store.runs[run_id].result_payloads = [{"output": {"verdict": "pass"}}]
    await bridge._on_run_terminal(_msg("run.done", run_id))   # no data at all

    rec = await bridge.store.get(rec.pipeline_id)
    assert rec.step_state[disp.calls[0]["step_id"]].status == "done"


async def test_a_foreign_pipelines_terminal_is_still_ignored():
    """The stamp must not turn the bridge into a firehose consumer: an event for
    a pipeline this store does not hold is a no-op, not a crash."""
    bridge, _ = _bridge()
    await bridge._on_run_terminal(
        _msg_with_pipeline("run.done", "run_x", "pipeline-that-does-not-exist"))
