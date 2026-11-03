"""#976 — RunReconciler heals zombie runs from the runtime's terminal status."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

from cairn.core.engine.schema import TriggerEvent
from cairn.core.runtime.reconciler import reconcile_stale_runs
from cairn.core.storage.protocols import Run
from cairn.core.storage.sqlite.runs import SqliteRunStore

OLD = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


def _young() -> str:
    """NOW at call time — as a module constant this was stamped at COLLECTION,
    so in a full-suite run the test executed minutes later and "young" had
    aged past the 120s grace window (#1199 flake)."""
    return datetime.now(timezone.utc).isoformat()


def _store() -> SqliteRunStore:
    return SqliteRunStore(os.path.join(tempfile.mkdtemp(), "runs.db"))


def _run(rid: str, status: str, created_at: str) -> Run:
    return Run(
        id=rid, status=status, workspace_id="default",
        created_at=created_at, trigger=TriggerEvent(source="manual"),
    )


def _fixed_status(mapping):
    async def fn(run_id: str):
        return mapping.get(run_id)
    return fn


@pytest.mark.asyncio
async def test_reconciles_terminal_workflow_to_failed():
    store = _store()
    await store.create(_run("run-a", "running", OLD))
    done = await reconcile_stale_runs(store, _fixed_status({"run-a": "failed"}), grace_seconds=120)
    assert done == ["run-a"]
    got = await store.get("run-a")
    assert got.status == "failed"
    # audit event tagged as reconciled + prior status recorded
    events = await store.get_events("run-a")
    last = events[-1]
    assert last.type == "failed"
    assert last.data.get("reconciled_by") == "reconciler"
    assert last.data.get("prior_status") == "running"


@pytest.mark.asyncio
async def test_completed_workflow_reconciles_to_done():
    store = _store()
    await store.create(_run("run-c", "running", OLD))
    await reconcile_stale_runs(store, _fixed_status({"run-c": "done"}), grace_seconds=120)
    assert (await store.get("run-c")).status == "done"


@pytest.mark.asyncio
async def test_still_running_workflow_is_left_untouched():
    store = _store()
    await store.create(_run("run-b", "running", OLD))
    done = await reconcile_stale_runs(store, _fixed_status({"run-b": None}), grace_seconds=120)
    assert done == []
    assert (await store.get("run-b")).status == "running"


@pytest.mark.asyncio
async def test_young_run_inside_grace_window_is_skipped():
    store = _store()
    await store.create(_run("run-young", "running", _young()))
    # even though the runtime would say terminal, the grace window protects it
    done = await reconcile_stale_runs(store, _fixed_status({"run-young": "failed"}), grace_seconds=120)
    assert done == []
    assert (await store.get("run-young")).status == "running"


@pytest.mark.asyncio
async def test_terminal_runs_are_never_touched():
    store = _store()
    await store.create(_run("run-done", "done", OLD))
    # a status_fn that (wrongly) claims failed must not flip an already-terminal run,
    # because terminal statuses aren't in ACTIVE_STATUSES so they're never swept
    done = await reconcile_stale_runs(store, _fixed_status({"run-done": "failed"}), grace_seconds=120)
    assert done == []
    assert (await store.get("run-done")).status == "done"


# ── a run that reached its own terminal while the sweep was looking at it ──


def _listing_only(run):
    async def fn(*, status=None, **_kw):
        return [run] if status == run.status else []
    return fn


def _finishes_during(store, *, result_fn=False, **terminal):
    """A status_fn or result_fn where the run's own task records its end first, as a workflow
    that has just ended and the dispatcher that was waiting on it do."""
    async def fn(run_id: str):
        await store.update(run_id, **terminal)
        return {"output": "recovered"} if result_fn else terminal["status"]
    return fn


@pytest.mark.asyncio
async def test_a_run_that_ended_during_the_probe_keeps_its_own_reason():
    store = _store()
    reason = "session run-x over budget: $0.000084 > $0.000070"
    await store.create(_run("run-x", "running", OLD))
    await store.append_event("run-x", "running", {})

    done = await reconcile_stale_runs(
        store, _finishes_during(store, status="failed", error=reason), grace_seconds=120)

    assert done == []
    got = await store.get("run-x")
    assert got.status == "failed" and got.error == reason
    assert [e.type for e in await store.get_events("run-x")] == ["running"]


@pytest.mark.asyncio
async def test_a_run_that_ended_during_the_probe_publishes_no_second_terminal():
    store = _store()
    emitted = []

    async def emit(run_id, event_type, data):
        emitted.append(event_type)

    await store.create(_run("run-y", "running", OLD))
    await reconcile_stale_runs(store, _finishes_during(store, status="failed", error="its own"),
                               grace_seconds=120, emit_fn=emit)
    assert emitted == []


@pytest.mark.asyncio
async def test_a_run_that_finished_while_its_result_was_fetched_is_left_alone():
    store = _store()
    await store.create(_run("run-z", "running", OLD))
    payloads = [{"output": {"answer": "its own"}}]

    done = await reconcile_stale_runs(
        store, _fixed_status({"run-z": "done"}), grace_seconds=120,
        result_fn=_finishes_during(store, result_fn=True, status="done", result_payloads=payloads))

    assert done == []
    assert (await store.get("run-z")).result_payloads == payloads


@pytest.mark.asyncio
async def test_a_run_that_only_moved_between_active_states_is_still_reconciled():
    """A resume can flip awaiting_approval to running: the runtime's terminal verdict still stands."""
    store = _store()
    await store.create(_run("run-w", "awaiting_approval", OLD))

    async def status_fn(run_id):
        await store.update(run_id, status="running")
        return "failed"

    assert await reconcile_stale_runs(store, status_fn, grace_seconds=120) == ["run-w"]
    assert (await store.get("run-w")).status == "failed"


@pytest.mark.asyncio
async def test_a_run_the_store_no_longer_has_is_skipped(monkeypatch):
    store = _store()
    await store.create(_run("run-v", "running", OLD))
    with monkeypatch.context() as patch:
        async def gone(run_id):
            return None

        patch.setattr(store, "get", gone)
        assert await reconcile_stale_runs(store, _fixed_status({"run-v": "failed"}), grace_seconds=120) == []
    assert (await store.get("run-v")).status == "running"


@pytest.mark.asyncio
async def test_a_verdict_the_store_refuses_publishes_nothing(monkeypatch):
    """The run ends done between the re-read and the write: the guard refuses `failed`."""
    store = _store()
    emitted = []

    async def emit(run_id, event_type, data):
        emitted.append(event_type)

    await store.create(_run("run-u", "running", OLD))
    stale = await store.get("run-u")
    await store.update("run-u", status="done")

    async def stale_get(run_id):
        return stale

    monkeypatch.setattr(store, "get", stale_get)
    monkeypatch.setattr(store, "list", _listing_only(stale))
    done = await reconcile_stale_runs(store, _fixed_status({"run-u": "failed"}), grace_seconds=120, emit_fn=emit)
    monkeypatch.undo()

    assert done == [] and emitted == []
    assert (await store.get("run-u")).status == "done"
    assert not [e for e in await store.get_events("run-u") if e.type == "failed"]
