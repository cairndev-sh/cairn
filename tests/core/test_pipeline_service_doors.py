"""The three service doors on the PIPELINE surface (#2101, ADR-0099 §1).

`RunDispatcher.dispatch` and `build_preflight` have asked `check_job_channel`,
`check_flow_engines` (the SkyPilot door until ADR-0134 D3c) and `check_pull_worker` for a while, and all three doors have
their own green unit tests. A pipeline `flow:` leg goes through NEITHER — the
bridge calls `resolve_engine()` / `engine.submit()` directly — so on 2026-09-08
two launches that ADR-0099 §1 says must be refused *before any record exists*
were admitted with HTTP 200 and died late at submit, after the pipeline record,
the leg record and the run existed. Every fine-tune pipeline is a flow leg.

These tests pin the ask on the surface that was uncovered, and the property that
makes it worth having: **no PipelineRecord**. A door that refuses after
`start_pipeline` is the defect wearing a fix's clothes, so every refusal test
asserts the bridge was never called.
"""
from __future__ import annotations

import os
import textwrap

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

from cairn.core.dispatch.pipeline_doors import (  # noqa: E402
    FLOW_DOOR_OPERATOR,
    ServiceDoorError,
    engine_kinds,
    leg_operators,
    pipeline_operators,
)
from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: E402
from cairn.core.engine.pipeline import PipelineDef, plan_pipeline  # noqa: E402
from cairn.core.pack import registry as pack_registry  # noqa: E402
from cairn.core.pack.protocol import Pack  # noqa: E402


# ── the fixtures: one throwaway pack with a kind: ml and a kind: data flow ───


@pytest.fixture()
def flow_pack(tmp_path):
    """A pack exposing `flows/tune.yaml` (kind: ml) and `flows/prep.yaml`
    (kind: data). `noop` is a real registered kernel, so both defs LOAD — a def
    that fails to load is refused by `assert_runnable` long before a door."""
    fdir = tmp_path / "flowpack" / "flows"
    fdir.mkdir(parents=True)
    (fdir / "tune.yaml").write_text("name: tune\nkind: ml\nstages:\n  - stage: noop\n")
    (fdir / "prep.yaml").write_text("name: prep\nkind: data\nstages:\n  - stage: noop\n")
    pack = Pack(name="doorpack", version="0.0.1", description="test", flows_dir=str(fdir))
    pack_registry.register_pack(pack)
    yield fdir
    pack_registry._PACKS.pop("doorpack", None)


_ML_LEG = """
name: mltrain
kind: pipeline
steps:
  - {id: train, flow: doorpack/tune}
"""

_DATA_LEG = """
name: dataprep
kind: pipeline
steps:
  - {id: prep, flow: doorpack/prep}
"""

_NO_ENGINE_LEG = """
name: justagate
kind: pipeline
steps:
  - {id: approve, gate: approve}
"""


def _install(tmp_path, monkeypatch, *, name, body):
    import cairn.core.engine.pipeline_resolve as R

    d = tmp_path / "doorpack" / "pipelines"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.yaml").write_text(textwrap.dedent(body))
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(d)])
    return d


class _Bridge:
    """Records every start_pipeline — the launch path's only side effect. A
    refusal that reaches here has already created the record it was meant to
    prevent."""

    def __init__(self):
        self.calls: list[dict] = []

    async def start_pipeline(self, plan, **kw):
        self.calls.append({"plan": plan, **kw})
        return type("Rec", (), {"pipeline_id": "pl_test", "status": "running",
                                "step_state": {}})()


def _no_worker(monkeypatch):
    """No pull worker has ever checked in — asserted, not left to the store."""
    from cairn.core import pull_worker_door as pw

    async def _never():
        return None

    monkeypatch.setattr(pw, "last_worker_checkin", _never)


def _skypilot_extra_absent(monkeypatch):
    """The `skypilot` extra is not importable here: nothing registers the
    engine. Injected rather than uninstalled — a live `sky launch` provisions a
    paid machine, so the door is never proved against one."""
    from cairn.core.dataflow import engine_door

    monkeypatch.setattr(engine_door, "load_engine_factory", lambda name: None)


# ── what a leg ASKS: derived per leg kind, never declared ───────────────────


def test_a_flow_leg_asks_as_its_kind_matched_door_operator(flow_pack):
    """ADR-0082 D7: `ml/run` runs a kind: ml flow, `dataflow/run` a kind: data
    one. That operator IS the leg's submission, so the doors recognise a flow
    leg as a remote job with no new predicate."""
    plan = plan_pipeline(PipelineDef.model_validate(
        {"name": "p", "pack": "doorpack",
         "steps": [{"id": "train", "flow": "doorpack/tune"},
                   {"id": "prep", "flow": "doorpack/prep"}]}))
    train, prep = plan.steps[0], plan.steps[1]
    assert leg_operators(train) == [FLOW_DOOR_OPERATOR["ml"]]
    assert leg_operators(prep) == [FLOW_DOOR_OPERATOR["data"]]
    assert engine_kinds(pipeline_operators(plan)) == ["data", "ml"]


def test_a_gate_leg_asks_nothing(flow_pack):
    plan = plan_pipeline(PipelineDef.model_validate(
        {"name": "p", "pack": "doorpack", "steps": [{"id": "approve", "gate": "approve"}]}))
    assert leg_operators(plan.steps[0]) == []
    assert engine_kinds(pipeline_operators(plan)) == []


# ── (a) the flow-engine door (ADR-0134 D3c; was the SkyPilot door, ADR-0104 D5) ─────────────────────────────────────


async def test_skypilot_door_refuses_a_flow_leg_pipeline_and_creates_no_record(
        tmp_path, monkeypatch, flow_pack):
    """The live #2101 observation: OBS_ML_ENGINE=skypilot with the extra absent
    was admitted HTTP 200 and died at submit, after the record existed."""
    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    _skypilot_extra_absent(monkeypatch)
    bridge = _Bridge()

    with pytest.raises(ServiceDoorError) as e:
        await launch_pipeline("doorpack/mltrain", bridge=bridge)

    assert "flow-engine door" in str(e.value)
    assert bridge.calls == []


async def test_the_skypilot_refusal_carries_the_doors_own_remediation(
        tmp_path, monkeypatch, flow_pack):
    """The message is the product: the door already writes the fix, and the
    pipeline surface SURFACES it rather than writing a second one."""
    from cairn.core.dataflow.engine_door import extra_for

    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    _skypilot_extra_absent(monkeypatch)

    with pytest.raises(ServiceDoorError) as e:
        await launch_pipeline("doorpack/mltrain", bridge=_Bridge())

    msg = str(e.value)
    assert f"pip install '{extra_for('skypilot')}'" in msg
    assert "doorpack/mltrain" in msg and "'train'" in msg  # the pipeline AND the leg


async def test_a_data_only_pipeline_is_not_refused_for_the_ML_engine(
        tmp_path, monkeypatch, flow_pack):
    """A pipeline knows its legs. `ml_engine=skypilot` governs kind: ml flows,
    and refusing a data-only pipeline for an engine none of its legs will ever
    construct is the over-refusal that makes doors get switched off."""
    _install(tmp_path, monkeypatch, name="dataprep", body=_DATA_LEG)
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "builtin")
    _skypilot_extra_absent(monkeypatch)
    _no_worker(monkeypatch)
    bridge = _Bridge()

    await launch_pipeline("doorpack/dataprep", bridge=bridge)
    assert len(bridge.calls) == 1


# ── (b) the pull-worker door (ADR-0104 D2) ──────────────────────────────────


async def test_pull_worker_door_refuses_a_flow_leg_pipeline_and_creates_no_record(
        tmp_path, monkeypatch, flow_pack):
    """The other live observation: OBS_ML_TARGET=worker:gpu with nothing checked
    in was admitted, and the leg went to the SSH engine and died at submit."""
    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_ML_TARGET", "worker:gpu")
    _no_worker(monkeypatch)
    bridge = _Bridge()

    with pytest.raises(ServiceDoorError) as e:
        await launch_pipeline("doorpack/mltrain", bridge=bridge)

    msg = str(e.value)
    assert "pull-worker door" in msg
    assert "cairn admin worker --pull" in msg  # the door's own remedy, not a new one
    assert bridge.calls == []


async def test_a_checked_in_worker_admits_the_same_pipeline(
        tmp_path, monkeypatch, flow_pack):
    """The door must be able to say yes, or the test above proves only that it
    always says no."""
    from datetime import datetime, timezone

    from cairn.core import pull_worker_door as pw

    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_ML_TARGET", "worker:gpu")

    async def _just_now():
        return datetime.now(timezone.utc).isoformat()

    monkeypatch.setattr(pw, "last_worker_checkin", _just_now)
    bridge = _Bridge()

    await launch_pipeline("doorpack/mltrain", bridge=bridge)
    assert len(bridge.calls) == 1


async def test_a_data_leg_is_judged_by_its_OWN_target(tmp_path, monkeypatch, flow_pack):
    """`dataflow_target` binds a kind: data leg; `ml_target` binds an ml one.
    Asking one door with the other's target refuses work that would have run."""
    _install(tmp_path, monkeypatch, name="dataprep", body=_DATA_LEG)
    monkeypatch.setenv("OBS_ML_TARGET", "worker:gpu")
    monkeypatch.setenv("OBS_DATAFLOW_TARGET", "")
    _no_worker(monkeypatch)
    bridge = _Bridge()

    await launch_pipeline("doorpack/dataprep", bridge=bridge)
    assert len(bridge.calls) == 1


# ── (c) the job-channel door (#2025) ────────────────────────────────────────


async def test_job_channel_door_refuses_a_flow_leg_pipeline_and_creates_no_record(
        tmp_path, monkeypatch, flow_pack):
    """A flow leg streams its live events over the job channel. NATS with no
    broker means the pipeline would train blind — refused before the record."""
    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    # port 1 on loopback: refused immediately, no network, no flake.
    monkeypatch.setenv("OBS_JOB_CHANNEL", "nats://127.0.0.1:1")
    _no_worker(monkeypatch)
    bridge = _Bridge()

    with pytest.raises(ServiceDoorError) as e:
        await launch_pipeline("doorpack/mltrain", bridge=bridge)

    msg = str(e.value)
    assert "job-channel door" in msg
    assert "COMPOSE_PROFILES=nats" in msg  # the door's own remedy
    assert bridge.calls == []


async def test_an_http_job_channel_asks_nothing(tmp_path, monkeypatch, flow_pack):
    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_JOB_CHANNEL", "http")
    _no_worker(monkeypatch)
    bridge = _Bridge()

    await launch_pipeline("doorpack/mltrain", bridge=bridge)
    assert len(bridge.calls) == 1


# ── the pipelines that must be unaffected ───────────────────────────────────


async def test_a_pipeline_with_no_engine_legs_launches_unaffected(
        tmp_path, monkeypatch, flow_pack):
    """A gate-only pipeline reaches no engine, so it asks nothing — even with
    every environment knob set to something that would refuse a flow leg."""
    _install(tmp_path, monkeypatch, name="justagate", body=_NO_ENGINE_LEG)
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_ML_TARGET", "worker:gpu")
    _skypilot_extra_absent(monkeypatch)
    _no_worker(monkeypatch)
    bridge = _Bridge()

    await launch_pipeline("doorpack/justagate", bridge=bridge)
    assert len(bridge.calls) == 1


async def test_every_failing_door_is_reported_at_once(tmp_path, monkeypatch, flow_pack):
    """Same discipline as `assert_runnable`: an operator on an under-provisioned
    box is told all of what is missing, not one thing per attempt."""
    _install(tmp_path, monkeypatch, name="mltrain", body=_ML_LEG)
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_ML_TARGET", "worker:gpu")
    monkeypatch.setenv("OBS_JOB_CHANNEL", "nats://127.0.0.1:1")
    _skypilot_extra_absent(monkeypatch)
    _no_worker(monkeypatch)

    with pytest.raises(ServiceDoorError) as e:
        await launch_pipeline("doorpack/mltrain", bridge=_Bridge())

    msg = str(e.value)
    assert "job-channel door" in msg and "flow-engine door" in msg and "pull-worker door" in msg


# ── the same door, every surface ────────────────────────────────────────────


async def test_the_door_asks_only_the_kinds_it_is_given(monkeypatch):
    """ADR-0134 D3c: every surface narrows to the kinds its launch reaches — the pipeline by
    its legs (above), the dispatcher and preflight by the template's operators
    (tests/core/test_engine_door.py). ``None`` still means both kinds."""
    from cairn.core.dataflow.engine_door import check_flow_engines

    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "skypilot")
    _skypilot_extra_absent(monkeypatch)

    assert len((await check_flow_engines()).reasons) == 2
    assert len((await check_flow_engines(kinds=["ml"])).reasons) == 1
    assert (await check_flow_engines(kinds=[])).ok  # asks about nothing
