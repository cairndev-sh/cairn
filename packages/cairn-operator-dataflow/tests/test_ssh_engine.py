"""BuiltinSshEngine (#1050 slice 3): thin composition over the PROVEN
remote/exec machinery — these tests pin the seams (exec-input construction,
state mapping, fetched-result rewrite, backend selection), monkeypatching
exec.py's functions exactly like cairn-operator-remote's own suite does. The live
box acceptance is the slice's separate, deploy-gated step."""
from __future__ import annotations

import asyncio
import json

import pytest

import cairn.core.connections as connections
from cairn.core.dataflow import Budget, DataflowSpec, EngineJobHandle, StageRef
from cairn.core.operator.jobs import JobHandle, JobStatus

from operator_dataflow import engines as engines_mod
from operator_dataflow.engines import BuiltinLocalEngine, resolve_engine
from operator_dataflow.engines_ssh import BuiltinSshEngine

import operator_remote.operators.exec as remote_exec


def _spec(**kw) -> DataflowSpec:
    return DataflowSpec(stages=[
        StageRef(stage="noop"),
        StageRef(stage="noop.slow", config={"seconds": 1}),
    ], **kw)


@pytest.fixture(autouse=True)
def _fake_connection(monkeypatch):
    async def fake_resolve(name):
        return {"provider": "ssh", "host": "box", "user": "ubuntu",
                "private_key": "k", "known_hosts": "/tmp/kh"}
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)


def test_resolve_engine_ssh_only_when_target_set(monkeypatch):
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "builtin")
    monkeypatch.setenv("OBS_DATAFLOW_TARGET", "fixbox")
    eng = resolve_engine(None)
    assert isinstance(eng, BuiltinSshEngine) and eng.name == "builtin-ssh"
    monkeypatch.setenv("OBS_DATAFLOW_TARGET", "")
    assert isinstance(resolve_engine(None), BuiltinLocalEngine)


def test_fetch_list_derives_stage_outputs():
    spec = DataflowSpec(stages=[
        StageRef(stage="ingest.normalize", config={"path": "/d.jsonl"}),
        StageRef(stage="scan.all"),
        StageRef(stage="split"),
        StageRef(stage="export.jsonl", config={"name": "clean.jsonl"}),
    ])
    pats = BuiltinSshEngine._fetch_list(spec)
    assert pats[:2] == ["result.json", "metrics.json"]
    assert "scan_findings.json" in pats and "splits" in pats and "clean.jsonl" in pats


def test_submit_wraps_spec_in_exec_input(monkeypatch):
    captured = {}

    async def fake_submit(inp, ctx):
        captured["inp"], captured["ctx"] = inp, ctx
        return JobHandle(job_id="box:/jobs/x", backend="ssh",
                         meta={"host": "box", "user": "ubuntu", "port": 22,
                               "workdir": "/jobs/x", "known_hosts": "/tmp/kh"})
    monkeypatch.setattr(remote_exec, "_submit", fake_submit)

    eng = BuiltinSshEngine("fixbox")
    spec = _spec(parent_run="run-parent-9")
    handle = asyncio.run(eng.submit(spec, type("C", (), {"pipeline_id": "run-1", "node_name": "clean"})()))

    inp = captured["inp"]
    assert inp.command == "cairn-dataflow-run"
    assert json.loads(inp.env["CAIRN_DATAFLOW_SPEC"])["parent_run"] == "run-parent-9"
    assert inp.env["CAIRN_PARENT_RUN"] == "run-parent-9"
    assert "CAIRN_SUBMIT_CWD" not in inp.env  # on a box, relative = on-box
    assert captured["ctx"].pipeline_id == "run-1"  # stable workdir keying (#551)
    # the handle is self-contained: poll/cancel after a restart need no state
    assert handle.engine == "builtin-ssh"
    assert handle.meta["job_handle"]["meta"]["workdir"] == "/jobs/x"
    assert handle.meta["exec_input"]["command"] == "cairn-dataflow-run"


def _handle(workdir="/jobs/x") -> EngineJobHandle:
    return EngineJobHandle(job_id=f"box:{workdir}", engine="builtin-ssh", meta={
        "job_handle": {"job_id": f"box:{workdir}", "backend": "ssh",
                       "meta": {"host": "box", "user": "ubuntu", "port": 22,
                                "workdir": workdir, "known_hosts": "/tmp/kh"}},
        "exec_input": {"command": "cairn-dataflow-run", "env": {}, "fetch": ["result.json"]},
    })


@pytest.mark.parametrize("remote_state,expect_state,retriable", [
    ("running", "running", False),
    ("failed", "failed", True),
    ("succeeded", "succeeded", False),
])
def test_poll_maps_remote_states(monkeypatch, remote_state, expect_state, retriable):
    async def fake_poll(jh, inp, ctx):
        return JobStatus(state=remote_state,
                         metrics={"stage": "pii.redact", "cost_usd": 0.25},
                         error="boom" if remote_state == "failed" else None,
                         retriable=retriable)
    monkeypatch.setattr(remote_exec, "_poll", fake_poll)

    status = asyncio.run(BuiltinSshEngine("fixbox").poll(_handle()))
    assert status.state == expect_state
    assert status.metrics == {"stage": "pii.redact", "cost_usd": 0.25}
    if expect_state == "failed":
        assert status.error == "boom" and status.retriable is True


def test_cancel_delegates_to_exec(monkeypatch):
    seen = {}

    async def fake_cancel(jh):
        seen["workdir"] = jh.meta["workdir"]
    monkeypatch.setattr(remote_exec, "_cancel", fake_cancel)

    asyncio.run(BuiltinSshEngine("fixbox").cancel(_handle("/jobs/y")))
    assert seen["workdir"] == "/jobs/y"


def test_result_reads_fetched_copy_and_rewrites_stats(tmp_path, monkeypatch):
    workdir = "/home/ubuntu/.cairn-jobs/run-z-clean"
    fetch_dir = BuiltinSshEngine._fetch_dir(workdir)
    fetch_dir.mkdir(parents=True, exist_ok=True)
    (fetch_dir / "output.jsonl").write_text('{"ok":1}\n')
    (fetch_dir / "result.json").write_text(json.dumps({
        "output_artifacts": [{"uri": f"file://{workdir}/stage-03", "content_hash": "h3",
                              "parents": [], "stats": {}}],
        "stats": {"export.jsonl.output": f"{workdir}/output.jsonl",
                  "pii.redact.redacted_rows": 17,
                  "scan.all.findings_path": f"{workdir}/scan_findings.json"},
        "summary": "dataflow ok: 4 stage(s)",
    }))

    result = asyncio.run(BuiltinSshEngine("fixbox").result(_handle(workdir)))
    # fetched file → local path; unfetched file + non-path stats untouched
    assert result.stats["export.jsonl.output"] == str(fetch_dir / "output.jsonl")
    assert result.stats["scan.all.findings_path"] == f"{workdir}/scan_findings.json"
    assert result.stats["pii.redact.redacted_rows"] == 17
    # artifact URIs keep the on-box location (that's where the product lives)
    assert result.output_artifacts[0].uri == f"file://{workdir}/stage-03"


def test_result_missing_fetch_fails_loudly():
    with pytest.raises(Exception, match="result.json"):
        asyncio.run(BuiltinSshEngine("fixbox").result(_handle("/jobs/never-ran")))


# ─── kind: ml over SSH (ADR-0082 P1c, #1075) — all three cases below are
# REGRESSIONS caught by a live dispatch to the training box, not by review.


def test_fetch_list_includes_ml_stage_outputs():
    """#1075 bug 1: without these the box produced a candidate the control
    plane could never read — the ML flow 'succeeded' with nothing fetched."""
    spec = DataflowSpec(stages=[
        StageRef(stage="train.qlora", config={"stub": True}),
        StageRef(stage="eval.checkpoints"),
        StageRef(stage="package.adapter"),
    ])
    pats = BuiltinSshEngine._fetch_list(spec)
    assert "train_result.json" in pats   # the flow's chaining record
    assert "candidate.json" in pats      # the packaged candidate manifest


def test_remote_command_is_overridable(monkeypatch):
    """#1075 bug 2: a box whose venv bin isn't on the non-interactive SSH PATH
    fails `exit 127: cairn-dataflow-run: not found` — the deployment knob."""
    from operator_dataflow.engines_ssh import _remote_command

    monkeypatch.delenv("OBS_DATAFLOW_REMOTE_CMD", raising=False)
    assert _remote_command() == "cairn-dataflow-run"
    monkeypatch.setenv("OBS_DATAFLOW_REMOTE_CMD", "/home/ubuntu/train-venv/bin/cairn-dataflow-run")
    assert _remote_command() == "/home/ubuntu/train-venv/bin/cairn-dataflow-run"


def test_rewrite_handles_relative_workdir_vs_absolute_stat_path():
    """#1075 bug 3: the handle records the workdir RELATIVE (job_id is
    `host:.cairn-jobs/<run>`) while a kernel writes an ABSOLUTE on-box path, so
    a plain startswith silently no-oped and the control plane was handed a path
    that exists only on the box. NB the workdir's leading dot is part of the
    directory NAME (`.cairn-jobs`) — stripping it breaks the match."""
    workdir = ".cairn-jobs/ml-ssh-4-finetune-flow"
    fetch_dir = BuiltinSshEngine._fetch_dir(workdir)
    fetch_dir.mkdir(parents=True, exist_ok=True)
    (fetch_dir / "candidate.json").write_text('{"adapter_uri":"hf://a"}')

    absolute = f"/home/ubuntu/{workdir}/candidate.json"
    assert BuiltinSshEngine._rewrite(absolute, workdir, fetch_dir) == str(fetch_dir / "candidate.json")
    # a file that was NOT fetched stays remote (no phantom local path)
    assert BuiltinSshEngine._rewrite(f"/home/ubuntu/{workdir}/nope.json", workdir, fetch_dir) is None
    # unrelated paths + non-strings untouched
    assert BuiltinSshEngine._rewrite("/elsewhere/candidate.json", workdir, fetch_dir) is None
    assert BuiltinSshEngine._rewrite(17, workdir, fetch_dir) is None


def test_ml_kind_selects_ssh_engine_via_its_own_backend(monkeypatch):
    """kind: ml reads OBS_ML_ENGINE/OBS_ML_TARGET — engines stay kind-blind,
    the ENVIRONMENT maps kinds to engines (ADR-0082 D7)."""
    monkeypatch.setenv("OBS_ML_ENGINE", "builtin")
    monkeypatch.setenv("OBS_ML_TARGET", "fixbox")
    monkeypatch.setenv("OBS_DATAFLOW_TARGET", "")  # data kind stays local
    eng = resolve_engine(None, kind="ml")
    assert isinstance(eng, BuiltinSshEngine) and eng.name == "builtin-ssh"
    assert isinstance(resolve_engine(None, kind="data"), BuiltinLocalEngine)
