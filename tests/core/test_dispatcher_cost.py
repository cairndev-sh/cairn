from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("fcntl", reason="dispatcher liveness uses POSIX flock")

from cairn.core import obs
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.engine.runner import Runner
from cairn.core.runtime.node_events import node_event_data
from cairn.core.storage import InMemoryRunStore
from cairn.core.storage.protocols import Run, TriggerEvent


class _Compiled:
    spec = SimpleNamespace(budget=None)

    def __init__(self, runtime: str, store: InMemoryRunStore, run_id: str):
        self.runtime = runtime
        self.store = store
        self.run_id = run_id

    async def run(self, _inputs: dict) -> dict:
        if self.runtime == "dbos":
            # DBOS node wrappers fold this child meter into the dispatcher's
            # run scope; this simulates the same observed LLM call at the seam.
            obs.record_llm("test-model", 10, 5, 0.0, usd=0.25)
        else:
            await self.store.append_event(
                self.run_id,
                "node",
                node_event_data("answer", {"output": "ok"}, llm_cost_usd=0.25),
            )
        return {"final_output": "ok"}


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime", ["dbos", "temporal"])
async def test_dispatcher_persists_worker_runtime_llm_cost(monkeypatch, runtime):
    store = InMemoryRunStore()
    run = Run(
        id=f"cost-{runtime}",
        trigger=TriggerEvent(source="manual", payload={}),
        status="queued",
        template_ref="demo/cost",
    )
    await store.create(run)
    compiled = _Compiled(runtime, store, run.id)
    dispatcher = RunDispatcher(runner=Runner(), store=store)

    monkeypatch.setattr("cairn.core.dispatch.dispatcher._settings.runtime", lambda: runtime)
    monkeypatch.setattr(
        "cairn.core.engine.compiler.compile_template",
        lambda _path: compiled,
    )
    monkeypatch.setattr(
        "cairn.core.engine.skill_engine.resolve_template_path",
        lambda _ref: "demo/cost.md",
    )
    monkeypatch.setattr("cairn.core.operator.load_all_operators", lambda **_: [])
    monkeypatch.setattr("cairn.core.pack.loader.load_all_packs", lambda **_: None)

    async def resolve_config(*_args, **_kwargs):
        return {}, {}

    async def contracts(*_args, **_kwargs):
        return {}

    async def terminal_hooks(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dispatcher, "_resolve_workflow_config", resolve_config)
    monkeypatch.setattr(dispatcher, "_contract_invocations", contracts)
    monkeypatch.setattr(dispatcher, "_terminal_hooks", terminal_hooks)

    await dispatcher._run_template_task(run, "demo/cost", {}, None)

    record = await store.get(run.id)
    assert record is not None
    assert record.status == "done"
    assert record.cost["total_usd"] == pytest.approx(0.25)
