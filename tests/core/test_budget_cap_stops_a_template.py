"""#395 — a template's model calls stop at the cap, and the run fails.

LangChain logs and swallows a callback's exception unless the handler sets ``raise_error``, so
a test that calls ``BudgetCallback.on_llm_end`` directly can't see whether a cap stops anything.
These tests go through real LangChain calls to a keyless stub model, and through the real
compiler and dispatcher. The real engine, with its pipeline, is in ``tests/e2e/hello/test_budget_cap.py``.
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

import pytest  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from cairn.core import obs  # noqa: E402
from cairn.core.dispatch import dispatcher as dispatcher_module  # noqa: E402
from cairn.core.dispatch.dispatcher import RunDispatcher  # noqa: E402
from cairn.core.engine import compiler as compiler_module  # noqa: E402
from cairn.core.engine import runner as runner_module  # noqa: E402
from cairn.core.engine import skill_engine  # noqa: E402
from cairn.core.engine.compiler import compile_template  # noqa: E402
from cairn.core.engine.python_function_registry import python_function  # noqa: E402
from cairn.core.engine.runner import run_within_budget  # noqa: E402
from cairn.core.engine.schema import TriggerEvent  # noqa: E402
from cairn.core.obs import audit as audit_module  # noqa: E402
from cairn.core.obs import cost as obs_cost  # noqa: E402
from cairn.core.operator import Operation, OperationContext  # noqa: E402
from cairn.core.operator import default_registry as op_registry  # noqa: E402
from cairn.core.runtime.inprocess import InProcessRuntime  # noqa: E402
from cairn.core.storage import InMemoryRunStore  # noqa: E402
from cairn.core.storage.protocols import Run  # noqa: E402
from cairn.services.llm import factory as llm_factory  # noqa: E402
from cairn.services.llm.factory import build_chat_model  # noqa: E402
from tests.e2e.model_stub import ModelStub  # noqa: E402

CHAT = "/chat/completions"
MODEL = "gpt-4o-mini"


@pytest.fixture
def stub(monkeypatch):
    model = ModelStub().start()
    monkeypatch.setenv("OPENAI_BASE_URL", model.base_url)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-stub")
    yield model
    model.stop()


@pytest.fixture
def one_call_usd(stub) -> float:
    """What one call to the stub costs, measured, and the calls that took don't count."""
    with obs.cost_scope("measure") as ctx:
        build_chat_model(MODEL).invoke("hi")
    stub.calls.clear()
    assert ctx.cost_usd > 0
    return ctx.cost_usd


async def _ainvoke(llm) -> None:
    await llm.ainvoke("hi")


async def _invoke(llm) -> None:
    llm.invoke("hi")


async def _astream(llm) -> None:
    async for _ in llm.astream("hi"):
        pass


# ── the model call ───────────────────────────────────────────────────


@pytest.mark.parametrize("call", [_ainvoke, _invoke, _astream], ids=["ainvoke", "invoke", "astream"])
async def test_a_cap_stops_real_model_calls_on_the_call_that_crosses_it(stub, one_call_usd, call):
    llm = build_chat_model(MODEL, max_cost_usd=one_call_usd * 2.5)
    with obs.cost_scope("run") as ctx, pytest.raises(obs.BudgetExceeded):
        for _ in range(6):
            await call(llm)
    assert stub.calls[CHAT] == 3
    assert ctx.cost_usd == pytest.approx(one_call_usd * 3)


async def test_the_deployment_wide_backstop_stops_real_model_calls(stub, one_call_usd, monkeypatch):
    monkeypatch.setattr(obs_cost, "get_settings", lambda: SimpleNamespace(max_cost_usd=one_call_usd * 1.5))
    llm = build_chat_model(MODEL)
    with obs.cost_scope("run"), pytest.raises(obs.BudgetExceeded):
        for _ in range(6):
            await llm.ainvoke("hi")
    assert stub.calls[CHAT] == 2


async def test_a_bookkeeping_error_does_not_fail_a_call_already_paid_for(stub, monkeypatch):
    def broken(*_a, **_kw):
        raise ValueError("bad usage")

    monkeypatch.setattr(llm_factory, "_extract_usage", broken)
    with obs.cost_scope("run"):
        assert (await build_chat_model(MODEL).ainvoke("hi")).content


# ── nodes that turn other errors into findings ───────────────────────


@python_function("_over_the_cap")
def _over_the_cap(state, **_config):
    raise obs.BudgetExceeded("over the cap")


def _template(tmp_path: Path, root: str, extra: str = "", budget_usd: float | None = None) -> Path:
    budget = f"budget:\n  max_cost_usd: {budget_usd}\n" if budget_usd else ""
    path = tmp_path / "budget-test.md"
    path.write_text(
        "---\nname: budget-test\nversion: 0.0.1\ndescription: a template under a cap\n"
        f"models:\n  fast: {MODEL}\n{budget}flow:\n  type: state_machine\n  source: behavior_tree\n"
        f"  root:\n{root}{extra}---\n# Budget test\n", encoding="utf-8")
    return path


async def test_a_python_node_lets_the_cap_through(tmp_path):
    compiled = compile_template(_template(
        tmp_path, "    type: python_node\n    function: _over_the_cap\n", "  nodes: {}\n"))
    with pytest.raises(obs.BudgetExceeded):
        await run_within_budget(compiled, {"run_id": "r1"})


async def test_a_map_lets_the_cap_through_instead_of_recording_one_failed_item(tmp_path):
    compiled = compile_template(_template(tmp_path, (
        "    type: map\n    over: symptom\n    as: item\n"
        "    body:\n      type: python_node\n      function: _over_the_cap\n"), "  nodes: {}\n"))
    with pytest.raises(obs.BudgetExceeded):
        await run_within_budget(compiled, {"run_id": "r1", "symptom": ["a", "b"]})


class _In(BaseModel):
    message: str


class _Out(BaseModel):
    echoed: str


class _OverTheCapOp(Operation[_In, _Out]):
    name = "test/over-the-cap"
    version = "0.1.0"
    input_schema = _In
    output_schema = _Out

    def run(self, inp, ctx: OperationContext):
        raise obs.BudgetExceeded("over the cap")


async def test_an_operator_node_lets_the_cap_through(tmp_path):
    snapshot = op_registry.all()
    op_registry.register(_OverTheCapOp)
    try:
        (tmp_path / "operations").mkdir()
        (tmp_path / "operations" / "over.yaml").write_text(
            "operator: test/over-the-cap\ninput:\n  message: $state.symptom\n"
            "output:\n  echoed: $state.final_output\n", encoding="utf-8")
        templates = tmp_path / "templates"
        templates.mkdir()
        compiled = compile_template(_template(
            templates, "    type: operation\n    ref: over\n", "  nodes: {}\n"))
        with pytest.raises(obs.BudgetExceeded):
            await run_within_budget(compiled, {"run_id": "r1", "symptom": "s"})
    finally:
        op_registry.clear()
        for cls in snapshot.values():
            op_registry.register(cls)


# ── the run ──────────────────────────────────────────────────────────


@pytest.fixture
def trail(monkeypatch):
    """What the run wrote to the audit log, and how it closed its reservation."""
    seen = SimpleNamespace(audit=[], reservations=[])

    async def _audit(action, **fields):
        seen.audit.append({"action": action, **fields})

    async def _close(run_id, *, settled, actual_usd=0.0):
        seen.reservations.append({"settled": settled, "actual_usd": actual_usd})

    monkeypatch.setattr(audit_module, "audit_emit", _audit)
    monkeypatch.setattr(RunDispatcher, "_close_reservations", staticmethod(_close))
    return seen


@pytest.fixture
def harness(monkeypatch):
    """A real dispatcher and store. Only finding the template and the pack scan are stubbed."""
    from cairn.core import operator as operator_module
    from cairn.core.pack import loader

    monkeypatch.setattr(operator_module, "load_all_operators", lambda **kw: None)
    monkeypatch.setattr(loader, "load_all_packs", lambda **kw: None)
    monkeypatch.setattr(dispatcher_module, "load_all_operators", lambda **kw: None, raising=False)
    monkeypatch.setattr(dispatcher_module, "load_all_packs", lambda **kw: None, raising=False)
    dispatcher = RunDispatcher(runner=object(), store=InMemoryRunStore())

    async def _cfg(*_a, **_kw):
        return {}, None

    async def _contracts(*_a, **_kw):
        return {}

    monkeypatch.setattr(RunDispatcher, "_resolve_workflow_config", _cfg)
    monkeypatch.setattr(RunDispatcher, "_contract_invocations", _contracts)
    return dispatcher


def _use_template(monkeypatch, path) -> None:
    monkeypatch.setattr(skill_engine, "resolve_template_path", lambda ref: path)


def _stub_compile(monkeypatch, cap: float | None = None) -> None:
    spec = SimpleNamespace(budget=SimpleNamespace(max_cost_usd=cap))
    monkeypatch.setattr(skill_engine, "resolve_template_path", lambda ref: "/nonexistent.md")
    monkeypatch.setattr(compiler_module, "compile_template", lambda path: SimpleNamespace(spec=spec))


async def _run(dispatcher, run_id: str = "r1", inputs: dict | None = None, **fields) -> Run:
    run = Run(id=run_id, trigger=TriggerEvent(source="manual"), workspace_id="default", **fields)
    await dispatcher.store.create(run)
    await dispatcher._run_template_task(run, "demo/capped", inputs or {}, None)
    return await dispatcher.store.get(run_id)


def _six_calls(tmp_path: Path, cap: float) -> Path:
    refs = [f"c{i}" for i in range(6)]
    children = "".join(f"      - type: action\n        ref: {r}\n" for r in refs)
    nodes = "".join(f"    {r}:\n      type: prompt\n      model: {MODEL}\n      capabilities: []\n" for r in refs)
    return _template(tmp_path, f"    type: sequence\n    children:\n{children}", f"  nodes:\n{nodes}", budget_usd=cap)


async def test_a_capped_template_fails_its_run_and_keeps_what_it_spent(
        harness, stub, one_call_usd, trail, tmp_path, monkeypatch):
    _use_template(monkeypatch, _six_calls(tmp_path, round(one_call_usd * 2.5, 6)))
    run = await _run(harness, inputs={"question": "q"}, bundle_pipeline_id="pl_1", step_id="calls")

    assert stub.calls[CHAT] == 3
    assert run.status == "failed" and run.ended_at
    assert "over budget" in run.error
    assert run.cost["llm_usd"] == pytest.approx(one_call_usd * 3)
    output = run.result_payloads[0]["output"]
    assert output["budget_exceeded"] is True and output["reason"] == run.error

    # what was spent counts against the series: settled, not released
    assert trail.reservations == [{"settled": True, "actual_usd": pytest.approx(one_call_usd * 3)}]
    (row,) = trail.audit
    assert row["action"] == "leg.budget_exceeded" and row["outcome"] == "deny"
    assert row["run_id"] == "r1" and row["pipeline_id"] == "pl_1" and row["step_id"] == "calls"
    assert row["spent_usd"] == pytest.approx(one_call_usd * 3)
    terminal = [e for e in await harness.store.get_events("r1") if e.type in ("done", "failed")]
    assert [e.type for e in terminal] == ["failed"] and "over budget" in terminal[0].data["error"]


async def test_a_template_under_its_cap_is_untouched(harness, stub, one_call_usd, trail, tmp_path, monkeypatch):
    _use_template(monkeypatch, _six_calls(tmp_path, round(one_call_usd * 7, 6)))
    run = await _run(harness, inputs={"question": "q"})

    assert stub.calls[CHAT] == 6
    assert run.status == "done" and not run.error
    assert trail.audit == []
    assert trail.reservations == [{"settled": True, "actual_usd": pytest.approx(one_call_usd * 6)}]


async def test_a_run_outside_a_pipeline_has_its_own_audit_action(harness, trail, monkeypatch):
    _stub_compile(monkeypatch)

    async def _over(compiled, inputs):
        raise obs.BudgetExceeded("over the cap")

    monkeypatch.setattr(runner_module, "run_within_budget", _over)
    await _run(harness)
    assert [r["action"] for r in trail.audit] == ["run.budget_exceeded"]


async def test_spend_a_handler_swallowed_still_fails_the_run(harness, trail, monkeypatch):
    _stub_compile(monkeypatch, cap=0.01)

    async def _swallowing(compiled, inputs):
        try:
            obs.record_llm(MODEL, 1_000_000, 1_000_000, 0.0)
        except obs.BudgetExceeded:
            pass
        return {"output": "looks fine"}

    monkeypatch.setattr(runner_module, "run_within_budget", _swallowing)
    run = await _run(harness)
    assert run.status == "failed" and "over budget" in run.error
    assert [r["action"] for r in trail.audit] == ["run.budget_exceeded"]


async def test_spend_a_worker_only_metered_still_fails_the_run(harness, trail, monkeypatch):
    """Temporal: the worker's scopes don't enforce, and the spend arrives on the node events."""
    _stub_compile(monkeypatch, cap=0.01)

    async def _ok(compiled, inputs):
        return {"output": "looks fine"}

    monkeypatch.setattr(runner_module, "run_within_budget", _ok)
    monkeypatch.setattr(dispatcher_module, "_llm_cost_from_node_events", lambda events: 0.02)
    run = await _run(harness)
    assert run.status == "failed" and "over budget" in run.error
    assert run.cost["llm_usd"] == pytest.approx(0.02)


# ── a run that paused for a person ───────────────────────────────────


async def _paused_run(harness, monkeypatch, *, spend_tokens: int = 1000) -> tuple[Run, float]:
    _stub_compile(monkeypatch)

    async def _pausing(compiled, inputs):
        obs.record_llm(MODEL, spend_tokens, spend_tokens, 0.0)
        return {"__interrupt__": [SimpleNamespace(value={"kind": "approval"})]}

    monkeypatch.setattr(runner_module, "run_within_budget", _pausing)
    run = await _run(harness)
    assert run.status == "awaiting_approval"
    spent = run.cost["llm_usd"]
    assert spent > 0
    return run, spent


def _resume_with(monkeypatch, resume) -> None:
    async def _pending(self, graph, run_id):
        return True

    monkeypatch.setattr(InProcessRuntime, "has_pending_interrupt", _pending)
    monkeypatch.setattr(InProcessRuntime, "resume", resume)
    monkeypatch.setattr(RunDispatcher, "_recompile_graph", staticmethod(lambda name: object()))


async def test_a_resumed_run_meters_from_what_it_spent_before_its_pause(harness, trail, monkeypatch):
    _, spent = await _paused_run(harness, monkeypatch)
    seen = {}

    async def _resume(self, graph, decision, *, run_id):
        seen["start"] = obs.get().cost_usd
        obs.record_llm(MODEL, 1000, 1000, 0.0)
        return {"output": "done"}

    _resume_with(monkeypatch, _resume)
    await harness.resume_run("r1", {"approved": True})

    run = await harness.store.get("r1")
    assert seen["start"] == spent
    assert run.status == "done"
    assert run.cost["llm_usd"] == pytest.approx(spent * 2)
    assert trail.reservations == [{"settled": True, "actual_usd": pytest.approx(spent * 2)}]


async def test_a_template_cap_counts_the_spend_before_the_pause(harness, stub, one_call_usd, trail, monkeypatch):
    """A real call that would fit the cap alone, but not on top of what came before the gate."""
    _, spent = await _paused_run(harness, monkeypatch)
    llm = build_chat_model(MODEL, max_cost_usd=spent + one_call_usd / 2)

    async def _resume(self, graph, decision, *, run_id):
        await llm.ainvoke("hi")
        return {"output": "should not get here"}

    _resume_with(monkeypatch, _resume)
    await harness.resume_run("r1", {"approved": True})

    run = await harness.store.get("r1")
    assert run.status == "failed" and "over budget" in run.error
    assert run.cost["llm_usd"] == pytest.approx(spent + one_call_usd, abs=2e-6)
    assert run.result_payloads[0]["output"]["budget_exceeded"] is True
    assert [r["action"] for r in trail.audit] == ["run.budget_exceeded"]
    assert trail.reservations == [{"settled": True, "actual_usd": pytest.approx(spent + one_call_usd, abs=2e-6)}]
    assert (await harness.store.get_events("r1"))[-1].type == "failed"


async def test_a_second_pause_keeps_the_running_total(harness, trail, monkeypatch):
    _, spent = await _paused_run(harness, monkeypatch)

    async def _pauses_again(self, graph, decision, *, run_id):
        obs.record_llm(MODEL, 1000, 1000, 0.0)
        return {"__interrupt__": [SimpleNamespace(value=None)]}

    _resume_with(monkeypatch, _pauses_again)
    await harness.resume_run("r1", {"approved": True})

    run = await harness.store.get("r1")
    assert run.status == "awaiting_approval"
    assert run.cost["llm_usd"] == pytest.approx(spent * 2)
    assert trail.reservations == []
