"""compute/acquire + compute/release inside a governed pipeline (ADR-0151 D6, #201).

Two properties the operators exist for, proven on the real paths:

* ``compute/acquire`` reaches the approval chokepoint (``enforce_gate``) as
  ``paid_compute`` — the risk the manifest declares, rated high — so a profile that
  gates paid compute pauses it, and ``compute/release`` (``external_write``) is not
  gated by the same profile.
* a ``finalizer: true`` ``compute/release`` stops the box when the pipeline ends
  done, failed or cancelled, on the in-process bridge.

Hermetic: EC2 is a fake behind ``CAIRN_EC2_CLIENT_FACTORY`` (the dev seam the real
``ec2_client`` reads), the connection store is stubbed at ``resolve_connection``, and
the SSH readiness probe answers at once. No AWS call is made.
"""
from __future__ import annotations

import asyncio
import os
import threading
import time

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

import cairn.core.approval.enforce as enforce
from cairn.core.approval.enforce import NeedsApprovalError
from cairn.core.connections import ResolvedConnection
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, default_registry, load_all_operators, register_operator
from cairn.core.operator.binding import OperationBinding
from cairn.core.operator.runtime import invoke_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

BOX = {"host": "10.0.0.9", "port": "22", "instance_id": "i-abc123",
       "region": "eu-central-1", "hourly_usd": "0.66"}


class FakeEC2:
    def __init__(self, state: str = "stopped") -> None:
        self.state = state
        self.calls: list[str] = []
        self.tags: dict[str, str] = {}

    def describe_instances(self, **kw):
        self.calls.append("describe")
        return {"Reservations": [{"Instances": [
            {"InstanceId": "i-abc123", "State": {"Name": self.state}}]}]}

    def start_instances(self, **kw):
        self.calls.append("start")
        self.state = "running"
        return {"StartingInstances": [{"CurrentState": {"Name": "running"}}]}

    def stop_instances(self, **kw):
        self.calls.append("stop")
        self.state = "stopped"
        return {"StoppingInstances": [{"CurrentState": {"Name": "stopping"}}]}

    def create_tags(self, **kw):
        self.calls.append("tags")
        for t in kw.get("Tags") or []:
            self.tags[t["Key"]] = t["Value"]


_SEAM: dict[str, FakeEC2] = {}


def _seam_factory(region: str = "") -> FakeEC2:
    return _SEAM["ec2"]


@pytest.fixture()
def ec2(monkeypatch):
    fake = _SEAM["ec2"] = FakeEC2()
    monkeypatch.setenv("CAIRN_EC2_CLIENT_FACTORY", f"{__name__}:_seam_factory")

    async def _resolve(name, **kw):
        return ResolvedConnection(name=name, kind="compute.ssh", config=dict(BOX))

    monkeypatch.setattr("cairn.core.connections.resolve_connection", _resolve)

    async def _ready(host, port, *, timeout_s, interval_s=5.0):
        return True

    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _ready)
    yield fake
    _SEAM.clear()


@pytest.fixture(autouse=True)
def _installed_operators():
    """The suite may reset the default registry: re-load the installed operators
    (entry points + pack.toml metadata, which is where `risk` comes from)."""
    load_all_operators()
    for cls in (_Work,):
        try:
            default_registry.register(cls, strict=False)
        except Exception:  # noqa: BLE001 — already registered is fine
            pass
    default_registry.attach_metadata(OperatorEntry(name="_t201/work", risk="read_only"))


# ── enforce_gate reaches acquire as paid_compute ─────────────────────────────
def _profile_gates_paid_compute(monkeypatch):
    monkeypatch.setattr(enforce, "_profile_approval",
                        lambda: {"mode": "enforce", "default_on_risk": ["paid_compute"]})
    monkeypatch.setattr(enforce, "_runtime_policy_engine", lambda: None)


async def test_acquire_reaches_enforce_gate_as_paid_compute(ec2, monkeypatch):
    _profile_gates_paid_compute(monkeypatch)
    assert default_registry.get_metadata("compute/acquire").risk == "paid_compute"
    cls = default_registry.get("compute/acquire")
    binding = OperationBinding(operator="compute/acquire", input={"connection": "gpu"}, output={})

    with pytest.raises(NeedsApprovalError) as ei:
        await invoke_operator(binding, cls, state={}, pipeline_id="pl-1", node_name="acquire")
    assert ei.value.request.operator == "compute/acquire"
    assert ei.value.request.sources == ("profile",)
    assert ec2.calls == [], "nothing may start before the approval"


async def test_release_is_not_gated_by_the_paid_compute_profile(ec2, monkeypatch):
    _profile_gates_paid_compute(monkeypatch)
    ec2.state = "running"
    cls = default_registry.get("compute/release")
    binding = OperationBinding(operator="compute/release", input={"connection": "gpu"},
                               output={"stopped": "$state.stopped"})

    patch = await invoke_operator(binding, cls, state={}, pipeline_id="pl-1", node_name="release")
    assert patch == {"stopped": True}
    assert "stop" in ec2.calls


# ── a finalizer release runs on done, failed and cancelled ───────────────────
class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t201/work", version="0.1.0")
class _Work(Operation[_P, _P]):
    """The pipeline's own work: finishes, raises, or runs until cancelled."""

    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        mode = getattr(inp, "mode", "ok")
        if mode == "fail":
            raise RuntimeError("the work failed")
        time.sleep(float(getattr(inp, "sleep_s", 0.0)))
        return _P(worked=True)


def _def(mode: str, sleep_s: float = 0.0):
    return parse_pipeline_def(
        "name: box\nkind: pipeline\n"
        "inputs:\n  compute_connection: {type: str, default: gpu}\n"
        "steps:\n"
        "  - id: acquire\n    task: compute/acquire\n"
        "    with: {connection: $input.compute_connection, max_hours: 1}\n"
        f"  - id: work\n    task: _t201/work\n    depends_on: [acquire]\n"
        f"    with: {{mode: {mode}, sleep_s: {sleep_s}}}\n"
        "  - id: release\n    task: compute/release\n    finalizer: true\n"
        "    with: {connection: $input.compute_connection}\n",
        pack="t201", source="<test>")


async def _wait(store, pid, pred, timeout=10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await store.get(pid)
        if pred(rec):
            return rec
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"stuck: {rec.status!r} "
                                 + str({k: v.status for k, v in rec.step_state.items()}))
        await asyncio.sleep(0.05)


async def _run(mode: str, *, cancel: bool = False):
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b.start()
    try:
        rec = await b.start_pipeline(plan_pipeline(_def(mode, 3.0 if cancel else 0.0)),
                                     inputs={"compute_connection": "gpu"})
        if cancel:
            await _wait(b.store, rec.pipeline_id,
                        lambda r: getattr(r.step_state.get("work"), "status", "") == "running")
            await b.cancel_pipeline(rec.pipeline_id)
        return await _wait(b.store, rec.pipeline_id,
                           lambda r: r.status in ("done", "failed", "cancelled"))
    finally:
        b.stop()


@pytest.mark.parametrize("mode, cancel, terminal", [
    ("ok", False, "done"),
    ("fail", False, "failed"),
    ("ok", True, "cancelled"),
])
async def test_the_finalizer_release_stops_the_box_on_every_outcome(ec2, mode, cancel, terminal):
    rec = await _run(mode, cancel=cancel)

    assert rec.status == terminal
    assert rec.step_state["acquire"].status == "done"
    assert rec.step_state["release"].status == "done"
    assert ec2.calls.index("tags") < ec2.calls.index("start") < ec2.calls.index("stop")
    assert ec2.state == "stopped", "the box must not bill after the pipeline ended"


# ── ADR-0151 D9: on cancel, the job on the box is cancelled before the box stops ──
class _Engine:
    """The flow engine the bridge resolves for a leg; records its cancels on the
    SAME timeline the fake EC2 writes, so the order is observable."""

    def __init__(self, timeline: list[str], *, fail: bool = False) -> None:
        self.timeline = timeline
        self.fail = fail

    async def cancel(self, handle):
        self.timeline.append(f"cancel:{handle.job_id}")
        if self.fail:
            raise RuntimeError("ssh: the box did not answer the cancel")


async def _cancel_with_running_legs(ec2, monkeypatch, legs: dict[str, str], *, fail: bool = False,
                                    audits: list | None = None, selected_target: str = ""):
    """A pipeline whose flow legs `legs` (step id → recorded compute target) are
    running on an engine job, with a finalizer `compute/release` of the box
    `$input.compute_connection` = "gpu". Cancel it; return the settled record."""
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState
    from cairn.core.storage.protocols import Run

    ec2.state = "running"
    plan = PipelinePlan(name="gpu", steps=[
        *(PipelinePlanStep(id=sid, ref="flow:t201/train") for sid in legs),
        PipelinePlanStep(id="release", ref="task:compute/release", finalizer=True,
                         inputs={"connection": "$input.compute_connection"})])
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    engine = _Engine(ec2.calls, fail=fail)
    monkeypatch.setattr(b, "_leg_engine", lambda s: engine)
    # the environment's selection, read only when a run recorded no compute target
    monkeypatch.setattr(b, "_leg_target", lambda s: selected_target)
    if audits is not None:
        real_audit = b._audit

        async def _spy(rec, step_id, event, detail):
            audits.append((step_id, event, dict(detail)))
            return await real_audit(rec, step_id, event, detail)
        monkeypatch.setattr(b, "_audit", _spy)
    await b.start()
    try:
        rec = await b.store.put(PipelineRecord(
            pipeline_name="gpu", status="running", plan=plan,
            inputs={"compute_connection": "gpu"},
            step_state={sid: StepState(status="running", run_id=f"r-{sid}",
                                       handle={"job_id": f"j-{sid}", "engine": "ssh", "meta": {}})
                        for sid in legs}))
        for sid, target in legs.items():
            await d.store.create(Run(
                id=f"r-{sid}", trigger=TriggerEvent(source="manual", payload={}), status="running",
                template_ref="flow:t201/train", compute_target=target,
                bundle_pipeline_id=rec.pipeline_id, step_id=sid, workspace_id=rec.workspace_id))
        await b.cancel_pipeline(rec.pipeline_id)
        return await _wait(b.store, rec.pipeline_id, lambda r: r.status in ("done", "failed", "cancelled"))
    finally:
        b.stop()


async def test_cancel_stops_the_job_on_the_box_before_the_box(ec2, monkeypatch):
    rec = await _cancel_with_running_legs(ec2, monkeypatch, {"train": "gpu"})

    assert rec.status == "cancelled" and rec.step_state["release"].status == "done"
    assert "cancel:j-train" in ec2.calls and "stop" in ec2.calls
    assert ec2.calls.index("cancel:j-train") < ec2.calls.index("stop"), ec2.calls
    assert ec2.state == "stopped"


async def test_a_failed_job_cancel_still_stops_the_box(ec2, monkeypatch):
    audits: list = []
    rec = await _cancel_with_running_legs(ec2, monkeypatch, {"train": "gpu"}, fail=True, audits=audits)

    assert ec2.calls.index("cancel:j-train") < ec2.calls.index("stop"), ec2.calls
    assert rec.status == "cancelled" and rec.step_state["release"].status == "done"
    assert ec2.state == "stopped", "a cancel that failed must never keep the box billing"
    (cancel,) = [a for a in audits if a[1] == "leg_job_cancel"]
    assert cancel[0] == "train"
    assert cancel[2] == {"run_id": "r-train", "job_id": "j-train", "target": "gpu",
                         "sent": False, "before": "release"}


async def test_a_run_with_no_recorded_target_falls_back_to_the_selection(ec2, monkeypatch):
    """A run written before compute targets were recorded: the environment's
    selection (`_leg_target`) names its box."""
    await _cancel_with_running_legs(ec2, monkeypatch, {"train": ""}, selected_target="gpu")
    assert ec2.calls.index("cancel:j-train") < ec2.calls.index("stop"), ec2.calls


async def test_the_release_is_found_without_manifest_metadata(ec2, monkeypatch):
    """With no manifest metadata for compute/release, the class's declared
    capability still identifies it: D9 does not fail open."""
    monkeypatch.delitem(default_registry._metadata, "compute/release")
    assert default_registry.get_metadata("compute/release") is None
    await _cancel_with_running_legs(ec2, monkeypatch, {"train": "gpu"})
    assert ec2.calls.index("cancel:j-train") < ec2.calls.index("stop"), ec2.calls


async def test_a_running_leg_on_another_target_is_left_alone(ec2, monkeypatch):
    """Only the legs on the box the pipeline releases are cancelled; any other
    running leg keeps today's rule (left to its own path, #207)."""
    await _cancel_with_running_legs(ec2, monkeypatch, {"train": "gpu", "side": "cpu-box"})

    assert "cancel:j-train" in ec2.calls
    assert "cancel:j-side" not in ec2.calls, ec2.calls


# ── ADR-0159 (#219): a cancel at any point of acquire ends with the box stopped ──
class _SlowEC2(FakeEC2):
    """One EC2 call (``block``) waits until the test lets it go, the way a real call is
    still in flight when a cancel lands. ``pending`` describes after a start read
    ``pending`` (EC2 refuses to stop such an instance), then ``running``."""

    def __init__(self, block: str = "", pending: int = 0) -> None:
        super().__init__()
        self.block, self.pending = block, pending
        self.entered, self.go = threading.Event(), threading.Event()

    def _hold(self, name: str) -> None:
        if self.block == name:
            self.block = ""  # one call only: the release's own calls go straight through
            self.entered.set()
            self.go.wait(5)

    def describe_instances(self, **kw):
        self._hold("describe")
        if self.state == "pending":
            if self.pending > 0:
                self.pending -= 1
            else:
                self.state = "running"
        return super().describe_instances(**kw)

    def create_tags(self, **kw):
        self._hold("tags")
        return super().create_tags(**kw)

    def start_instances(self, **kw):
        self._hold("start")
        resp = super().start_instances(**kw)
        if self.pending:
            self.state = "pending"
        return resp

    def stop_instances(self, **kw):
        if self.state == "pending":
            self.calls.append("stop-refused")
            raise RuntimeError("IncorrectInstanceState: the instance is pending and cannot be stopped")
        return super().stop_instances(**kw)


async def _cancel_during_acquire(fake: _SlowEC2, landed):
    """Start the box pipeline, cancel it once ``landed()`` says acquire is at the
    point under test, let the held EC2 call go, and return the settled record."""
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b.start()
    try:
        rec = await b.start_pipeline(plan_pipeline(_def("ok")), inputs={"compute_connection": "gpu"})
        deadline = asyncio.get_event_loop().time() + 5
        while not landed():
            assert asyncio.get_event_loop().time() < deadline, fake.calls
            await asyncio.sleep(0.01)
        cancel = asyncio.create_task(b.cancel_pipeline(rec.pipeline_id))
        await asyncio.sleep(0.2)  # the cancel has reached acquire while the call is in flight
        fake.go.set()
        await cancel
        return await _wait(b.store, rec.pipeline_id, lambda r: r.status in ("done", "failed", "cancelled"))
    finally:
        fake.go.set()
        b.stop()


@pytest.mark.parametrize("point", ["describe", "tags"])
async def test_a_cancel_before_the_start_never_starts_the_box(ec2, point):
    fake = _SEAM["ec2"] = _SlowEC2(block=point)
    rec = await _cancel_during_acquire(fake, fake.entered.is_set)
    await asyncio.sleep(0.1)  # the held call's thread has returned; nothing may follow it

    assert rec.status == "cancelled" and rec.step_state["release"].status == "done"
    assert rec.step_state["acquire"].verdict == "cancelled"
    assert "start" not in fake.calls, fake.calls
    assert fake.state == "stopped"


async def test_a_cancel_while_the_start_is_in_flight_stops_what_it_started(ec2):
    fake = _SEAM["ec2"] = _SlowEC2(block="start")
    rec = await _cancel_during_acquire(fake, fake.entered.is_set)

    assert rec.status == "cancelled" and rec.step_state["release"].status == "done"
    assert fake.calls.index("start") < fake.calls.index("stop"), fake.calls
    assert fake.state == "stopped", "the start landed after the cancel: the box must not bill"


async def test_a_cancel_while_the_box_is_pending_stops_it_once_it_is_up(ec2, monkeypatch):
    monkeypatch.setattr("operator_remote.operators.release.PENDING_POLL_S", 0.01)

    async def _booting(host, port, *, timeout_s, interval_s=5.0):
        await asyncio.sleep(3600)  # SSH never answers before the cancel

    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _booting)
    fake = _SEAM["ec2"] = _SlowEC2(pending=3)
    rec = await _cancel_during_acquire(fake, lambda: "start" in fake.calls)

    assert rec.status == "cancelled" and rec.step_state["release"].status == "done"
    assert "stop-refused" not in fake.calls, fake.calls
    assert fake.state == "stopped"
