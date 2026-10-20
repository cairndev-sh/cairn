"""ADR-0086 H2 (#1244) — leg-level retry, driven by the engine's `retriable` flag.

The flag existed on every engine failure status (`engines_ssh.py` poll) and was
consumed by nobody: a transient SSH drop failed the leg, and with it the
pipeline. execute_leg now runs a bounded, kind-aware attempt loop over the
EXISTING RetryPolicy shape — every attempt budgeted (a retry is new spend,
never free) and announced (`leg_retrying` run event via the on_retry hook).
"""
from __future__ import annotations

import os
import sys
import types

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")


def _stub_unused_file_lock_import(monkeypatch):
    """Allow in-memory dispatch imports without emulating a POSIX file lock."""
    if sys.platform == "win32" and "fcntl" not in sys.modules:
        monkeypatch.setitem(sys.modules, "fcntl", types.ModuleType("fcntl"))

from cairn.core.dataflow.contract import StageRef
from cairn.core.dispatch.leg_executor import execute_leg
from cairn.core.dispatch.leg_hooks import flow_leg_record
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


async def test_failed_retry_accumulates_and_settles_known_partial_cost(monkeypatch):
    _stub_resolution(monkeypatch)
    _stub_unused_file_lock_import(monkeypatch)
    reservations: list[float] = []
    settlements: list[float] = []
    releases: list[object] = []

    async def _reserve(*args, **kwargs):
        reservations.append(kwargs["spent_usd"])
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        releases.append(reservation)

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _PaidFailures(_FlakyEngine):
        async def poll(self, handle):
            cost_usd = 0.2 if self.submits == 1 else 0.3
            return types.SimpleNamespace(
                state="failed", error="paid attempt failed", retriable=True,
                metrics={"cost_usd": cost_usd},
            )

    retries: list[tuple[int, str]] = []

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=_PaidFailures(failures=2, retriable=True),
        run_id="r1", series_id="s1", estimated_usd=0.5, spent_usd=0.0,
        budget_usd=2.0,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
        on_retry=_on_retry,
    )

    assert not out.ok and out.cost_usd == 0.5
    assert reservations == [0.0, 0.2]
    assert settlements == [0.2, 0.3]
    assert releases == []
    assert retries == [(1, "paid attempt failed")]
    record = flow_leg_record({"error": out.error}, out)
    assert record["cost"]["total_usd"] == 0.5
    assert record["result_payloads"][0]["llm_cost_usd"] == 0.5


async def test_cancelled_attempt_with_partial_cost_settles_without_retry(monkeypatch):
    _stub_resolution(monkeypatch)
    _stub_unused_file_lock_import(monkeypatch)
    settlements: list[float] = []
    releases: list[object] = []
    retries: list[tuple[int, str]] = []

    async def _reserve(*args, **kwargs):
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        releases.append(reservation)

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _CancelledPaid(_FlakyEngine):
        async def poll(self, handle):
            return types.SimpleNamespace(
                state="cancelled", error="cancelled after paid call",
                metrics={"cost_usd": 0.15}, retriable=True,
            )

    engine = _CancelledPaid(failures=0)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine,
        run_id="r1", series_id="s1", estimated_usd=0.5,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
        on_retry=_on_retry,
    )

    assert out.state == "cancelled" and out.cost_usd == 0.15
    assert engine.submits == 1 and retries == []
    assert settlements == [0.15] and releases == []


def test_failed_leg_without_reported_cost_keeps_run_cost_unknown():
    from cairn.core.dispatch.leg_executor import LegOutcome

    record = flow_leg_record(
        {"error": "worker disappeared before reporting metrics"},
        LegOutcome(state="failed", error="worker disappeared before reporting metrics"),
    )

    assert "cost" not in record
    assert record["result_payloads"][0]["llm_cost_usd"] == 0.0


async def test_successful_retry_run_includes_cost_of_replaced_attempt(monkeypatch):
    _stub_resolution(monkeypatch)
    _stub_unused_file_lock_import(monkeypatch)
    reservations: list[float] = []
    settlements: list[float] = []

    async def _reserve(*args, **kwargs):
        reservations.append(kwargs["spent_usd"])
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        pytest.fail("a paid attempt must be settled, not released")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _PaidThenSuccess(_FlakyEngine):
        async def poll(self, handle):
            if self.submits == 1:
                return types.SimpleNamespace(
                    state="failed", error="paid attempt failed", retriable=True,
                    metrics={"cost_usd": 0.2},
                )
            return types.SimpleNamespace(
                state="succeeded", error="", retriable=False,
                metrics={"cost_usd": 0.3},
            )

    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=_PaidThenSuccess(failures=1),
        run_id="r1", series_id="s1", estimated_usd=0.5, spent_usd=0.0,
        budget_usd=2.0,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
    )

    assert out.ok and out.cost_usd == 0.5
    assert reservations == [0.0, 0.2]
    assert settlements == [0.2, 0.3]
    record = flow_leg_record({"output": "s3://ok"}, out)
    assert record["cost"]["total_usd"] == 0.5


def test_unrepresentable_reported_cost_is_treated_as_unknown():
    from cairn.core.dispatch.leg_executor import _reported_cost, _reported_metrics_cost

    huge_cost = 10**400
    assert _reported_cost(types.SimpleNamespace(cost_usd=huge_cost)) == 0.0
    assert _reported_metrics_cost({"cost_usd": huge_cost}) == 0.0
