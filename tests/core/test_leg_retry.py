"""ADR-0086 H2 (#1244) — leg-level retry, driven by the engine's `retriable` flag.

The flag existed on every engine failure status (`engines_ssh.py` poll) and was
consumed by nobody: a transient SSH drop failed the leg, and with it the
pipeline. execute_leg now runs a bounded, kind-aware attempt loop over the
EXISTING RetryPolicy shape — every attempt budgeted (a retry is new spend,
never free) and announced (`leg_retrying` run event via the on_retry hook).
"""
from __future__ import annotations

import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from cairn.core.dataflow.contract import StageRef
from cairn.core.dispatch.leg_executor import execute_leg
from cairn.core.runtime.retry import RetryPolicy


def _stub_resolution(monkeypatch, *, kind="data"):
    stage = StageRef(stage="clean.run")
    monkeypatch.setattr("cairn.core.dataflow.defs.load_flow_defs", lambda: {
        "demo/clean": types.SimpleNamespace(kind=kind),
    })
    monkeypatch.setattr(
        "cairn.core.dataflow.defs.resolve_flow_ref",
        lambda ref, cfg, kind="data": [stage],
    )
    monkeypatch.setattr("cairn.core.dispatch.leg_executor._POLL_INTERVAL_S", 0.01)


class _FlakyEngine:
    """Fails `failures` times (retriable or not), then succeeds."""

    name = "flaky"

    def __init__(self, failures=1, *, retriable=True) -> None:
        self.failures = failures
        self.retriable = retriable
        self.submits = 0

    def validate(self, spec):
        return []

    async def submit(self, spec, ctx):
        self.submits += 1
        return types.SimpleNamespace(job_id=f"j{self.submits}", engine=self.name, meta={})

    async def poll(self, handle):
        if self.submits <= self.failures:
            return types.SimpleNamespace(
                state="failed", error="transient: connection reset",
                metrics={}, retriable=self.retriable,
            )
        return types.SimpleNamespace(state="succeeded", metrics={}, error="", retriable=False)

    async def cancel(self, handle):
        pass

    async def result(self, handle):
        return types.SimpleNamespace(stats={"clean.output": "s3://ok"})


async def _run(engine, *, kind="data", retry=None, monkeypatch=None):
    retries: list[tuple[int, str]] = []

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    _stub_resolution(monkeypatch, kind=kind)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1",
        retry=retry, on_retry=_on_retry,
    )
    return out, retries


async def test_retriable_failure_retries_once_and_succeeds(monkeypatch):
    engine = _FlakyEngine(failures=1, retriable=True)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert out.ok, out.error
    assert engine.submits == 2, "data-leg default is exactly one automatic retry"
    assert retries == [(1, "transient: connection reset")], "every retry must be announced"


async def test_non_retriable_failure_never_retries(monkeypatch):
    engine = _FlakyEngine(failures=5, retriable=False)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert not out.ok and engine.submits == 1 and retries == []


async def test_data_leg_default_is_two_attempts_total(monkeypatch):
    engine = _FlakyEngine(failures=5, retriable=True)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert not out.ok
    assert engine.submits == 2, "bounded: one retry, then the failure stands"
    assert len(retries) == 1


async def test_ml_leg_never_retries_without_opt_in(monkeypatch):
    """A failed train leg must not silently re-spend GPU dollars."""
    engine = _FlakyEngine(failures=5, retriable=True)
    out, retries = await _run(engine, kind="ml", monkeypatch=monkeypatch)
    assert not out.ok and engine.submits == 1 and retries == []


async def test_ml_leg_retries_with_explicit_policy(monkeypatch):
    engine = _FlakyEngine(failures=1, retriable=True)
    policy = RetryPolicy(maximum_attempts=3, initial_interval_seconds=0.0)
    out, retries = await _run(engine, kind="ml", retry=policy, monkeypatch=monkeypatch)
    assert out.ok and engine.submits == 2 and len(retries) == 1


async def test_budget_refusal_never_retries(monkeypatch):
    """Budget exhaustion is not transient — no retry, no reservation churn."""
    _stub_resolution(monkeypatch)
    calls = {"n": 0}

    async def _reserve(*a, **k):
        from cairn.core.budget_reservations import BudgetExceeded
        calls["n"] += 1
        raise BudgetExceeded("series budget exhausted")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    engine = _FlakyEngine(failures=0, retriable=True)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert not out.ok and "budget" in out.error.lower()
    assert calls["n"] == 1 and engine.submits == 0


async def test_each_attempt_reserves_fresh_budget(monkeypatch):
    """Governance invariant: a retry is NEW spend — one reservation per attempt."""
    _stub_resolution(monkeypatch)
    ledger = {"reserved": 0, "released": 0, "settled": 0}

    async def _reserve(*a, **k):
        ledger["reserved"] += 1
        return object()

    async def _release(res):
        ledger["released"] += 1

    async def _settle(res, **k):
        ledger["settled"] += 1

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    engine = _FlakyEngine(failures=1, retriable=True)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert out.ok
    assert ledger == {"reserved": 2, "released": 1, "settled": 1}, (
        "attempt 1: reserved+released; attempt 2: reserved+settled"
    )


async def test_submit_engine_error_fails_the_leg_releases_and_never_retries(monkeypatch):
    """A refusal raised by ``engine.submit`` (e.g. the SSH engine's image mode
    with no image named, ADR-0150 D1) is a failed leg: its reservation is
    released, nothing is settled, and the default data-leg retry does not
    submit again — a configuration refusal is not transient."""
    from cairn.core.dataflow import EngineError

    _stub_resolution(monkeypatch)
    ledger = {"reserved": 0, "released": 0, "settled": 0}

    async def _reserve(*a, **k):
        ledger["reserved"] += 1
        return object()

    async def _release(res):
        ledger["released"] += 1

    async def _settle(res, **k):
        ledger["settled"] += 1

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)

    class _RefusingEngine(_FlakyEngine):
        async def submit(self, spec, ctx):
            self.submits += 1
            raise EngineError("image mode but no image is named")

    engine = _RefusingEngine()
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert not out.ok and "no image is named" in out.error
    assert engine.submits == 1
    assert ledger == {"reserved": 1, "released": 1, "settled": 0}
