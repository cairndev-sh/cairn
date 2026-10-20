"""#1364 (Argo-readiness epic #1367) — poll a run from a process that is a
STRANGER to it.

The engine's poll had two sources of truth: this process's `_PROCS` entry and
the submitter's workdir files. A foreign process — another box, a fresh
container, a detached scheduler — has neither, so the re-attach branch
defaulted to "running" forever: a finished run that never finishes. With both
durability axes configured (#1349: s3 datastore + metadata service) the
Metaflow client is the truth instead, and the pathspec travels in the
PERSISTED handle's meta (#1239) — the only channel a stranger actually has.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import types
from pathlib import Path

import pytest

from cairn.core.dataflow import EngineJobHandle

from operator_dataflow_metaflow.engines_metaflow import MetaflowEngine, _status

CROSS_BOX = dict(datastore="s3", datastore_root="s3://b/c",
                 metadata_service="http://mf-svc:8080")


@pytest.fixture(autouse=True)
def _hermetic_env():
    """``_status_from_client`` applies ``durability_env()`` to ``os.environ``
    — the same deliberate pattern as ``_result_from_datastore`` (the client
    reads config from env). Fine in prod, where the values ARE the engine's
    config; fatal in this suite, where the fake service URL above would leak
    into the real-subprocess conformance tests that run later in the same
    session (caught: they point real metaflow at http://mf-svc:8080)."""
    snap = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(snap)


def _handle(workdir: Path, **meta) -> EngineJobHandle:
    return EngineJobHandle(engine="metaflow", job_id="mf-foreign",
                           meta={"workdir": str(workdir), **meta})


# ── the pathspec channel a foreign process actually has ──────────────────────

def test_pathspec_reads_the_persisted_handle_meta(tmp_path):
    """No _PROCS entry, no run_id.txt — meta is the only channel that travels."""
    eng = MetaflowEngine(**CROSS_BOX)
    h = _handle(tmp_path, pathspec="CairnFlow/1234")
    assert eng.pathspec(h) == "CairnFlow/1234"


def test_resolving_a_pathspec_stamps_the_handle(tmp_path):
    """The submitter's resolution must survive _PROCS dying (restart, #1239):
    once run_id.txt is read, the handle itself carries the pathspec."""
    (tmp_path / "run_id.txt").write_text("777", encoding="utf-8")
    eng = MetaflowEngine()
    h = _handle(tmp_path)
    assert eng.pathspec(h) == "CairnFlow/777"
    assert h.meta["pathspec"] == "CairnFlow/777"


# ── poll routing: when the client is (and is not) consulted ─────────────────

def test_foreign_poll_without_cross_box_stays_running(tmp_path, monkeypatch):
    """Single-box deployments keep the old behavior — no client, no surprise."""
    eng = MetaflowEngine()
    called = []
    monkeypatch.setattr(eng, "_status_from_client",
                        lambda h: called.append(1) or _status("failed", None, {}))
    st = asyncio.run(eng.poll(_handle(tmp_path)))
    assert st.state == "running"
    assert called == [], "a non-cross-box engine must not pay a client call"


def test_foreign_poll_cross_box_asks_the_client(tmp_path, monkeypatch):
    eng = MetaflowEngine(**CROSS_BOX)
    monkeypatch.setattr(eng, "_status_from_client",
                        lambda h: _status("succeeded", None, {}))
    st = asyncio.run(eng.poll(_handle(tmp_path, pathspec="CairnFlow/1")))
    assert st.state == "succeeded"


def test_workdir_files_still_beat_the_client(tmp_path, monkeypatch):
    """On the SUBMITTING box the files carry nuance the client lacks
    (cancelled, stage progress) — presence of progress.json wins."""
    (tmp_path / "progress.json").write_text(
        json.dumps({"state": "cancelled", "stages_done": 1,
                    "stages_total": 3, "cost_usd": 0.18}),
        encoding="utf-8")
    eng = MetaflowEngine(**CROSS_BOX)
    monkeypatch.setattr(eng, "_status_from_client",
                        lambda h: pytest.fail("client consulted despite local files"))
    st = asyncio.run(eng.poll(_handle(tmp_path)))
    assert st.state == "cancelled"
    assert st.metrics["cost_usd"] == 0.18


def test_client_cannot_say_keeps_running(tmp_path, monkeypatch):
    """No pathspec yet / run not registered → 'running' (the #1239 reconciler
    owns true orphans), never a fabricated terminal state."""
    eng = MetaflowEngine(**CROSS_BOX)
    monkeypatch.setattr(eng, "_status_from_client", lambda h: None)
    st = asyncio.run(eng.poll(_handle(tmp_path)))
    assert st.state == "running"


# ── the client mapping itself ────────────────────────────────────────────────

def _fake_metaflow(monkeypatch, *, finished: bool, successful: bool = False,
                   partial_cost_usd: float | None = None):
    mod = types.ModuleType("metaflow")

    class _Run:
        def __init__(self, spec):
            self.finished, self.successful = finished, successful
            self._data = types.SimpleNamespace(partial_cost_usd=partial_cost_usd)

        def __getitem__(self, step_name):
            assert step_name == "execute"
            return types.SimpleNamespace(task=types.SimpleNamespace(data=self._data))

    mod.Run = _Run
    mod.namespace = lambda ns: None
    monkeypatch.setitem(sys.modules, "metaflow", mod)


@pytest.mark.parametrize(
    ("finished", "successful", "expect"),
    [(False, False, "running"), (True, True, "succeeded"), (True, False, "failed")],
)
def test_client_status_mapping(tmp_path, monkeypatch, finished, successful, expect):
    _fake_metaflow(monkeypatch, finished=finished, successful=successful)
    eng = MetaflowEngine(**CROSS_BOX)
    st = eng._status_from_client(_handle(tmp_path, pathspec="CairnFlow/9"))
    assert st is not None and st.state == expect


def test_failed_client_status_reads_persisted_partial_cost(tmp_path, monkeypatch):
    _fake_metaflow(monkeypatch, finished=True, partial_cost_usd=0.37)
    eng = MetaflowEngine(**CROSS_BOX)
    st = eng._status_from_client(_handle(tmp_path, pathspec="CairnFlow/9"))
    assert st is not None and st.state == "failed"
    assert st.metrics == {"cost_usd": 0.37}


def test_bridge_persists_partial_cost_on_failed_metaflow_task(tmp_path, monkeypatch):
    mod = types.ModuleType("metaflow")

    class _FlowSpec:
        def next(self, step):
            self.next_step = step

    mod.FlowSpec = _FlowSpec
    mod.Parameter = lambda *args, **kwargs: None
    mod.step = lambda fn: fn
    monkeypatch.setitem(sys.modules, "metaflow", mod)
    module_name = "operator_dataflow_metaflow.metaflow_bridge"
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    try:
        bridge = importlib.import_module(module_name)

        def fail_after_paid_call(spec, workdir, *, on_progress):
            on_progress({"stage": "paid", "cost_usd": 0.37})
            (workdir / "metrics.json").write_text('{"cost_usd": 0.37}', encoding="utf-8")
            raise RuntimeError("stage failed after paid call")

        monkeypatch.setattr(bridge, "run_dataflow", fail_after_paid_call)
        flow = bridge.CairnFlow()
        flow.spec = {"stages": [{"stage": "paid"}]}
        flow.workdir = str(tmp_path)

        flow.execute()

        assert flow.partial_cost_usd == 0.37
        assert json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))[
            "cost_usd"] == 0.37
        assert json.loads((tmp_path / "result.json").read_text(encoding="utf-8")) == {
            "error": "RuntimeError: stage failed after paid call",
        }
        with pytest.raises(RuntimeError, match="stage failed after paid call"):
            flow.failed()
    finally:
        sys.modules.pop(module_name, None)


def test_client_status_is_deliberately_coarse(tmp_path, monkeypatch):
    """No stage, no metrics — the client cannot see mid-run stage progress and
    must not pretend to (#1364 scope honesty)."""
    _fake_metaflow(monkeypatch, finished=False)
    eng = MetaflowEngine(**CROSS_BOX)
    st = eng._status_from_client(_handle(tmp_path, pathspec="CairnFlow/9"))
    # stage rides inside metrics (EngineJobStatus has no top-level field) —
    # an empty metrics dict IS "no stage, no progress claimed"
    assert st.metrics == {}


def test_client_status_without_a_pathspec_is_none(tmp_path):
    assert MetaflowEngine(**CROSS_BOX)._status_from_client(_handle(tmp_path)) is None


def test_client_error_means_cannot_say_not_failed(tmp_path, monkeypatch):
    mod = types.ModuleType("metaflow")

    def _boom(spec):
        raise RuntimeError("service down")

    mod.Run = _boom
    mod.namespace = lambda ns: None
    monkeypatch.setitem(sys.modules, "metaflow", mod)
    eng = MetaflowEngine(**CROSS_BOX)
    assert eng._status_from_client(_handle(tmp_path, pathspec="CairnFlow/9")) is None


# ── result: the datastore read now covers cross-box readers too ──────────────

def test_foreign_result_reads_the_datastore_artifact(tmp_path, monkeypatch):
    """compute=local (not strict) but cross-box: the submitter's result.json
    is another machine's file — the artifact is the shared channel (#1358)."""
    eng = MetaflowEngine(**CROSS_BOX)   # compute defaults to local → _strict() False
    assert eng._strict() is False, "precondition: this is the NON-strict path"
    payload = {"stats": {"ingest.normalize.rows": 5}, "output_artifacts": [],
               "warnings": [], "summary": "ok"}
    monkeypatch.setattr(eng, "_result_from_datastore", lambda h: payload)
    res = asyncio.run(eng.result(_handle(tmp_path, pathspec="CairnFlow/9")))
    assert res.stats["ingest.normalize.rows"] == 5
