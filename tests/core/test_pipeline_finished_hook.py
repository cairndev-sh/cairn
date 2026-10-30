"""#306 — the ``pipeline.finished`` run hook: a pipeline that reaches a terminal state is
announced to every installed ``cairn.run_hooks`` provider, once per pipeline per process.

A provider that materialises per-pipeline results needs a terminal signal: legs finish
before the pipeline's ``outputs:`` are resolved (ADR-0161), and a pipeline can end on a
gate, a cancel or a deferred failure without a leg of its own. The event is notify-only:
it fires after the record is persisted with its final status (and, on ``done``, its
outputs), it cannot refuse, and a failing provider never breaks the terminal transition.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

import pytest  # noqa: E402

from cairn.core.dispatch.pipeline_bridge import PipelineBridge  # noqa: E402
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline  # noqa: E402
from cairn.core.obs import audit as audit_mod  # noqa: E402
from cairn.core.pack import run_hooks as RH  # noqa: E402
from cairn.core.storage.pipeline_store import InMemoryPipelineStore  # noqa: E402
from tests.core.test_pipeline_bridge import FakeDispatcher, _complete, _NullBus, _prep_then  # noqa: E402

REF = "pipeline:p/pl"


class _Recorder:
    """One installed provider: records every ``pipeline.finished`` with the record as the
    pipeline store held it AT the moment the hook ran."""

    def __init__(self, store=None, *, raises: BaseException | None = None):
        self.store = store
        self.raises = raises
        self.seen: list[dict] = []
        self.stored: list = []

    async def __call__(self, event, payload):
        if event != RH.PIPELINE_FINISHED:
            return
        self.seen.append(dict(payload))
        if self.store is not None:
            self.stored.append(await self.store.get(payload["pipeline_id"]))
        if self.raises is not None:
            raise self.raises


@pytest.fixture
def no_audit_sink(monkeypatch):
    """No audit sink bound: the `pipeline.finished` AUDIT row is skipped, the hook is not."""
    audit_mod.reset_audit_sink()
    monkeypatch.setattr(audit_mod, "_CACHE", audit_mod.NullAuditSink())
    yield
    audit_mod.reset_audit_sink()


def _install(monkeypatch, *providers):
    hooks = {f"p{i}": p for i, p in enumerate(providers)}
    monkeypatch.setattr(RH, "load_run_hooks", lambda: dict(hooks))


def _bridge():
    disp = FakeDispatcher()
    return PipelineBridge(disp, store=InMemoryPipelineStore(), bus=_NullBus()), disp


async def _start(bridge, plan):
    return await bridge.start_pipeline(plan, inputs={}, bundle_ref=REF, workspace_id="ws-1")


# ── the contract ────────────────────────────────────────────────────────────


def test_pipeline_finished_is_a_notify_only_event():
    assert RH.PIPELINE_FINISHED == "pipeline.finished"
    assert RH.PIPELINE_FINISHED in RH.EVENTS and "PIPELINE_FINISHED" in RH.__all__
    assert RH.PIPELINE_FINISHED not in RH.REFUSABLE_EVENTS
    assert RH.PIPELINE_FINISHED not in RH.FAIL_CLOSED_EVENTS


async def test_fire_accepts_it_and_a_refusal_on_it_is_ignored(monkeypatch):
    async def _refuses(event, payload):
        raise RH.AdmissionRefused("no")

    ok = _Recorder()
    _install(monkeypatch, _refuses, ok)
    ran = await RH.fire(RH.PIPELINE_FINISHED, pipeline_id="pl_x", ref=REF, status="done",
                        workspace_id="ws-1")
    assert ran == ["p1"] and ok.seen[0]["event"] == "pipeline.finished"


# ── once per terminal pipeline: done, failed, cancelled ─────────────────────


async def test_done_fires_once_with_its_payload(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    rec_hook = _Recorder(bridge.store)
    _install(monkeypatch, rec_hook)
    rec = await _start(bridge, _prep_then("train"))
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
    assert rec_hook.seen == []                       # a leg finishing is not the pipeline
    await _complete(bridge, disp, "run_2")

    assert rec_hook.seen == [{"event": "pipeline.finished", "pipeline_id": rec.pipeline_id,
                              "ref": REF, "status": "done", "workspace_id": "ws-1"}]
    (at_fire,) = rec_hook.stored
    assert at_fire.status == "done"                  # persisted before the hook ran
    # a later pass over the terminal record (`_advance`'s #233 path) does not repeat it
    await bridge._advance(rec.pipeline_id)
    assert len(rec_hook.seen) == 1


async def test_failed_fires_once(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    rec_hook = _Recorder(bridge.store)
    _install(monkeypatch, rec_hook)
    rec = await _start(bridge, _prep_then("train"))
    await _complete(bridge, disp, "run_1", topic="run.failed")
    await bridge._advance(rec.pipeline_id)

    assert [(s["pipeline_id"], s["status"]) for s in rec_hook.seen] == [(rec.pipeline_id, "failed")]
    assert rec_hook.stored[0].status == "failed"


async def test_a_rejected_gate_ends_the_pipeline_failed_and_fires_once(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    rec_hook = _Recorder(bridge.store)
    _install(monkeypatch, rec_hook)
    rec = await _start(bridge, _prep_then("train", approval=True))
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
    await bridge.reject_step(rec.pipeline_id, "train", decider="bob", reason="no")

    assert [s["status"] for s in rec_hook.seen] == ["failed"]
    assert rec_hook.stored[0].status == "failed"


async def test_cancelled_fires_once(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    rec_hook = _Recorder(bridge.store)
    _install(monkeypatch, rec_hook)
    rec = await _start(bridge, _prep_then("train", approval=True))
    await bridge.cancel_pipeline(rec.pipeline_id, actor="carol")
    await bridge._advance(rec.pipeline_id)

    assert [(s["status"], s["ref"], s["workspace_id"]) for s in rec_hook.seen] == [
        ("cancelled", REF, "ws-1")]
    assert rec_hook.stored[0].status == "cancelled"


# ── after the outputs are stored (ADR-0161) ─────────────────────────────────


_OUTPUTS_YAML = (
    "name: pl\nkind: pipeline\n"
    "outputs:\n  uri: prep.staged_dataset_uri\n"
    "steps:\n"
    "  - id: prep\n    template: p/prep\n"
    "  - gate: ok\n    depends_on: [prep]\n    predicate: steps.prep.staged_dataset_uri == 's3://s'\n")


async def test_done_fires_after_the_outputs_are_stored(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    rec_hook = _Recorder(bridge.store)
    _install(monkeypatch, rec_hook)
    plan = plan_pipeline(parse_pipeline_def(_OUTPUTS_YAML, pack="p", source="<test>"))
    rec = await _start(bridge, plan)
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})

    (at_fire,) = rec_hook.stored
    assert at_fire.status == "done" and at_fire.outputs == {"uri": "s3://s"}
    assert rec_hook.seen[0]["pipeline_id"] == rec.pipeline_id


# ── a provider never breaks the terminal transition ─────────────────────────


async def test_a_raising_provider_does_not_break_the_terminal_transition(monkeypatch, no_audit_sink):
    bridge, disp = _bridge()
    boom = _Recorder(raises=RuntimeError("provider exploded"))
    after = _Recorder(bridge.store)
    _install(monkeypatch, boom, after)               # name order: the raiser runs first
    plan = plan_pipeline(parse_pipeline_def(_OUTPUTS_YAML, pack="p", source="<test>"))
    rec = await _start(bridge, plan)
    await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})

    final = await bridge.store.get(rec.pipeline_id)
    assert final.status == "done" and final.outputs == {"uri": "s3://s"}
    assert len(boom.seen) == 1 and len(after.seen) == 1   # the next provider still ran


async def test_a_failure_escaping_fire_is_contained_by_the_bridge(monkeypatch, no_audit_sink):
    async def _broken_fire(event, **payload):
        if event == RH.PIPELINE_FINISHED:
            raise RuntimeError("discovery blew up")

    monkeypatch.setattr(RH, "fire", _broken_fire)
    bridge, disp = _bridge()
    rec = await _start(bridge, _prep_then("train", approval=True))
    await bridge.cancel_pipeline(rec.pipeline_id, actor="carol")
    assert (await bridge.store.get(rec.pipeline_id)).status == "cancelled"

    bridge2, disp2 = _bridge()
    rec2 = await _start(bridge2, _prep_then("train"))
    await _complete(bridge2, disp2, "run_1", topic="run.failed")
    assert (await bridge2.store.get(rec2.pipeline_id)).status == "failed"


async def test_with_an_audit_sink_bound_the_hook_still_fires_once(monkeypatch, tmp_path):
    """The audit row's own once-guard and the hook's are independent: with a sink bound
    both are written, each once."""
    from operator_audit_store.operators.audit_store import AuditStoreOp

    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    audit_mod.reset_audit_sink()
    monkeypatch.setattr(audit_mod, "_CACHE", AuditStoreOp())
    try:
        bridge, disp = _bridge()
        rec_hook = _Recorder(bridge.store)
        _install(monkeypatch, rec_hook)
        rec = await _start(bridge, _prep_then("train"))
        await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
        await _complete(bridge, disp, "run_2")
        await bridge._advance(rec.pipeline_id)

        assert [s["status"] for s in rec_hook.seen] == ["done"]
        rows = [r for r in log.read_text().splitlines() if '"pipeline.finished"' in r]
        assert len(rows) == 1
    finally:
        audit_mod.reset_audit_sink()
