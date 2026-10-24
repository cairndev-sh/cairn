from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_temporal_node_event_captures_llm_cost(monkeypatch):
    pytest.importorskip("temporalio")

    from cairn.core import obs
    from cairn.core.runtime.temporal import activities, worker

    recorded: list[tuple[str, dict, float | None]] = []

    async def record_node_event(
        node_name: str,
        delta: dict,
        llm_cost_usd: float | None = None,
    ) -> None:
        recorded.append((node_name, delta, llm_cost_usd))

    async def node(state: dict) -> dict:
        obs.record_llm("test-model", 10, 5, 0.0, usd=0.123456)
        return {"answer": "ok"}

    monkeypatch.setattr(activities, "_record_node_event", record_node_event)
    runnable = SimpleNamespace(afunc=node)
    worker._wrap_node_for_events(runnable, "answer")

    result = await runnable.afunc({})

    assert result == {"answer": "ok"}
    assert recorded == [("answer", {"answer": "ok"}, 0.123456)]
    assert obs.get() is None
