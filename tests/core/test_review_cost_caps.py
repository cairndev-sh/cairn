from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from types import SimpleNamespace

import pytest

from cairn.core import obs
from cairn.core.obs import cost
from cairn.core.run_progress import _trace_from_events


@pytest.mark.parametrize("scope_cap", [None, 0, 50])
def test_scope_cannot_raise_deployment_cap(monkeypatch, scope_cap):
    monkeypatch.setattr(cost, "get_settings", lambda: SimpleNamespace(max_cost_usd=1.0))
    with obs.cost_scope("run", max_cost_usd=scope_cap) as run:
        with pytest.raises(obs.BudgetExceeded):
            obs.record_llm("model", 0, 0, 0, usd=1.01)
    assert run.cost_usd == pytest.approx(1.01)


def test_child_propagates_each_call_and_enforces_parent(monkeypatch):
    monkeypatch.setattr(cost, "get_settings", lambda: SimpleNamespace(max_cost_usd=0.1))
    with obs.cost_scope("run") as run:
        with obs.cost_scope("node", enforce_cap=False, propagate_cost=True) as node:
            obs.record_llm("model", 0, 0, 0, usd=0.06)
            with pytest.raises(obs.BudgetExceeded):
                obs.record_llm("model", 0, 0, 0, usd=0.06)
    assert run.cost_usd == node.cost_usd == pytest.approx(0.12)
    assert len(run.llm_calls) == len(node.llm_calls) == 2
    assert "cost_parent" not in asdict(run)


def test_failed_node_progress_is_not_ok():
    events = [{"type": "node", "seq": 1, "data": {"node": "n", "state": "failed"}}]
    assert _trace_from_events(events)[0]["status"] == "failed"


def test_parallel_child_meters_do_not_lose_parent_calls(monkeypatch):
    monkeypatch.setattr(cost, "get_settings", lambda: SimpleNamespace(max_cost_usd=0))

    def node():
        with obs.cost_scope("node", enforce_cap=False, propagate_cost=True):
            for _ in range(100):
                obs.record_llm("model", 0, 0, 0, usd=0.001)

    with obs.cost_scope("run") as run, ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(copy_context().run, node) for _ in range(8)]
        for future in futures:
            future.result()
    assert len(run.llm_calls) == 800
    assert run.cost_usd == pytest.approx(0.8)
