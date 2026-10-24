"""compute/acquire + compute/release — the governed compute lifecycle (#1787;
public since ADR-0151 D6, cairndev-sh/cairn#201).

Hermetic: a fake EC2 client stands in at the ONE named seam
(:func:`cairn.core.compute_lifecycle.ec2_client`), so no AWS call is made and no
instance is touched. That seam is the same one the CLI sweep and the live drive
use — a stub here proves the path production takes, not a parallel one. The
lease-ordering tests go through ``CAIRN_EC2_CLIENT_FACTORY`` itself, the dev seam
the real ``ec2_client`` reads, so nothing on the operator's path is patched.

Copied from the platform's ``cairn-operator-compute`` 0.1.0 tests with the import
root changed; the ADR-0151 D8 tests (lease before start) are new.
"""
from __future__ import annotations

import asyncio
import datetime as dt

import pytest

from cairn.core import compute_lifecycle as cl
from cairn.core.connections import ResolvedConnection
from cairn.core.operator import OperationContext
from operator_remote.operators.acquire import ComputeAcquireOp
from operator_remote.operators.release import ComputeReleaseOp
from operator_remote.schemas.lifecycle import AcquireInput, ReleaseInput

BOX = {"host": "10.0.0.9", "port": "22", "instance_id": "i-abc123",
       "region": "eu-central-1", "hourly_usd": "0.66"}


class FakeEC2:
    """The three EC2 calls the lifecycle makes, and a record of every one."""

    def __init__(self, state: str = "stopped") -> None:
        self.state = state
        self.calls: list[tuple] = []
        self.tags: dict[str, str] = {}

    def describe_instances(self, **kw):
        self.calls.append(("describe", kw))
        return {"Reservations": [{"Instances": [
            {"InstanceId": "i-abc123", "State": {"Name": self.state},
             "Tags": [{"Key": k, "Value": v} for k, v in self.tags.items()]}]}]}

    def start_instances(self, **kw):
        self.calls.append(("start", kw))
        self.state = "running"
        return {"StartingInstances": [{"CurrentState": {"Name": "running"}}]}

    def stop_instances(self, **kw):
        self.calls.append(("stop", kw))
        self.state = "stopping"
        return {"StoppingInstances": [{"CurrentState": {"Name": "stopping"}}]}

    def create_tags(self, **kw):
        self.calls.append(("tags", kw))
        for t in kw.get("Tags") or []:
            self.tags[t["Key"]] = t["Value"]


@pytest.fixture()
def ec2(monkeypatch):
    fake = FakeEC2()
    monkeypatch.setattr(cl, "ec2_client", lambda region="": fake)
    return fake


@pytest.fixture()
def box(monkeypatch):
    """`load_box` resolves the connection store; stub the store lookup, not the
    parsing — the ComputeBox mapping is part of what is under test."""
    async def _resolve(name, **kw):
        return ResolvedConnection(name=name, kind="compute.ssh", config=dict(BOX))

    monkeypatch.setattr("cairn.core.connections.resolve_connection", _resolve)


@pytest.fixture(autouse=True)
def _no_port_wait(monkeypatch):
    """The readiness probe opens a socket; the port is not what these tests are
    about, so it answers instantly. `test_acquire_reports_an_unreachable_box`
    overrides it back to a failure."""
    async def _ok(host, port, *, timeout_s, interval_s=5.0):
        return True

    monkeypatch.setattr(cl, "wait_port_open", _ok)
    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _ok)


def _ctx(node: str) -> OperationContext:
    return OperationContext(pipeline_id="pl_t", node_name=node)


# ── compute/acquire ─────────────────────────────────────────────────────────
def test_acquire_starts_the_instance_the_connection_names(ec2, box, monkeypatch):
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu", max_hours=2), _ctx("acquire")))

    assert ("start", {"InstanceIds": ["i-abc123"]}) in ec2.calls
    assert out.instance_id == "i-abc123"
    assert out.state == "running"
    # the target downstream legs bind is the CONNECTION NAME, not the host
    assert out.target == "gpu"
    assert out.hourly_usd == 0.66, "the gate shows this — it must come from the connection"
    assert out.started_at, "started_at is the lease's origin; it must be recorded"


def test_acquire_stamps_the_lease_the_sweep_enforces(ec2, box, monkeypatch):
    """max_hours is only a guard if the DEADLINE lands on the instance — that is
    what survives a crashed control plane (#1787 criterion 5)."""
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu", max_hours=3, acquired_by="pl_42"),
        _ctx("acquire")))

    assert ec2.tags[cl.TAG_MANAGED] == "cairn"
    assert ec2.tags[cl.TAG_ACQUIRED_BY] == "pl_42"
    deadline = cl.parse_iso(ec2.tags[cl.TAG_RELEASE_AFTER])
    assert deadline is not None
    delta = deadline - cl.parse_iso(out.started_at)
    assert abs(delta - dt.timedelta(hours=3)) < dt.timedelta(seconds=5)
    assert out.release_after == ec2.tags[cl.TAG_RELEASE_AFTER]


def test_acquire_without_max_hours_still_marks_the_box_as_ours(ec2, box, monkeypatch):
    """No lease is not the same as no ownership: the `cairn:managed` tag is what
    scopes the sweep, so a box cairn turned on always carries it."""
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu"), _ctx("acquire")))

    assert ec2.tags[cl.TAG_MANAGED] == "cairn"
    assert cl.TAG_RELEASE_AFTER not in ec2.tags
    assert out.release_after == ""


def test_acquire_on_a_running_box_is_a_no_op_it_reports(ec2, box, monkeypatch):
    ec2.state = "running"
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu"), _ctx("acquire")))

    assert out.already_running is True
    assert "no-op" in out.note


def test_acquire_reports_an_unreachable_box_rather_than_raising(ec2, box, monkeypatch):
    """A slow boot is a fact the next leg will hit with its own error; failing
    HERE would leave a started box with no release leg ever reached."""
    async def _never(host, port, *, timeout_s, interval_s=5.0):
        return False

    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _never)
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu", wait_ready_s=1), _ctx("acquire")))

    assert out.ssh_ready is False
    assert "did not accept" in out.note


def test_acquire_refuses_a_connection_with_no_instance_id(ec2, monkeypatch):
    """Cited refusal, not a guess: starting "some instance" is the one thing a
    paid_compute operator must never improvise."""
    async def _resolve(name, **kw):
        return ResolvedConnection(name=name, kind="compute.ssh",
                                  config={"host": "h", "port": "22"})

    monkeypatch.setattr("cairn.core.connections.resolve_connection", _resolve)
    with pytest.raises(cl.ComputeLifecycleError) as ei:
        asyncio.run(ComputeAcquireOp().run(
            AcquireInput(connection="gpu"), _ctx("acquire")))
    assert "instance_id" in str(ei.value)
    assert not ec2.calls, "nothing may be started when the target is unknown"


def test_acquire_is_paid_compute_on_the_class():
    """The bridge's restart policy reads the CLASS. A softer class here would
    mean a restarted control plane silently re-started a paid box (#1293/#1289)."""
    assert ComputeAcquireOp.risk == "paid_compute"


def test_acquire_mock_never_calls_ec2(ec2, box, monkeypatch):
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeAcquireOp().mock(
        AcquireInput(connection="gpu", max_hours=1), _ctx("acquire")))
    assert out.note.startswith("mocked")
    assert ec2.calls == []


# ── compute/release ─────────────────────────────────────────────────────────
def test_release_stops_a_running_box(ec2, box, monkeypatch):
    ec2.state = "running"
    monkeypatch.setattr("operator_remote.operators.release.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeReleaseOp().run(
        ReleaseInput(connection="gpu"), _ctx("release")))

    assert ("stop", {"InstanceIds": ["i-abc123"]}) in ec2.calls
    assert out.stopped is True
    assert out.already_stopped is False


def test_release_on_a_stopped_box_is_a_no_op_SUCCESS(ec2, box, monkeypatch):
    """THE property the finalizer machinery depends on. The bridge re-runs a
    finalizer that was in flight when the control plane died; if the second
    release raised, a successful teardown would come back as a failed pipeline
    (ADR-0097)."""
    ec2.state = "stopped"
    monkeypatch.setattr("operator_remote.operators.release.ec2_client",
                        lambda region="": ec2)
    out = asyncio.run(ComputeReleaseOp().run(
        ReleaseInput(connection="gpu"), _ctx("release")))

    assert out.already_stopped is True
    assert out.stopped is False
    assert not any(c[0] == "stop" for c in ec2.calls)


def test_release_twice_is_safe(ec2, box, monkeypatch):
    ec2.state = "running"
    monkeypatch.setattr("operator_remote.operators.release.ec2_client",
                        lambda region="": ec2)
    first = asyncio.run(ComputeReleaseOp().run(
        ReleaseInput(connection="gpu"), _ctx("release")))
    second = asyncio.run(ComputeReleaseOp().run(
        ReleaseInput(connection="gpu"), _ctx("release")))

    assert first.stopped is True
    assert second.already_stopped is True, "a re-run finalizer must succeed"


def test_release_is_never_approval_gated():
    """A teardown paused behind an unanswered gate is a box billing while it
    waits — the exact failure #1787 exists to end. The class says so."""
    assert ComputeReleaseOp.requires_approval is False
    assert ComputeReleaseOp.risk == "external_write"


# ── the connection surface (#1787, additive) ────────────────────────────────
def test_compute_box_reads_the_three_new_config_keys():
    b = cl.ComputeBox("gpu", dict(BOX))
    assert (b.instance_id, b.region, b.hourly_usd) == ("i-abc123", "eu-central-1", 0.66)


def test_compute_box_tolerates_the_keys_being_absent_or_junk():
    """Every lifecycle key is OPTIONAL on the connection — a compute.ssh
    connection authored before #1787 must keep working as pure SSH transport."""
    b = cl.ComputeBox("gpu", {"host": "h", "hourly_usd": "not-a-number"})
    assert (b.instance_id, b.region, b.hourly_usd) == ("", "", 0.0)
    assert b.port == 22


# ── ADR-0151 D8: the lease is stamped BEFORE the box starts ──────────────────
#: The fake the ``CAIRN_EC2_CLIENT_FACTORY`` seam hands out (set per test).
_SEAM: dict[str, FakeEC2] = {}


def _seam_factory(region: str = "") -> FakeEC2:
    return _SEAM["ec2"]


class _CrashAfterFirstWrite(FakeEC2):
    """A control plane that dies between acquire's two EC2 writes: the first
    mutating call lands, the second never does."""

    def __init__(self) -> None:
        super().__init__()
        self.writes = 0

    def _write(self) -> None:
        self.writes += 1
        if self.writes > 1:
            raise RuntimeError("control plane died between the two calls")

    def start_instances(self, **kw):
        self._write()
        return super().start_instances(**kw)

    def create_tags(self, **kw):
        self._write()
        return super().create_tags(**kw)


@pytest.fixture()
def seam(monkeypatch, box):
    """Route the REAL ``ec2_client`` to a fake through its env seam."""
    def _use(fake: FakeEC2) -> FakeEC2:
        _SEAM["ec2"] = fake
        monkeypatch.setenv("CAIRN_EC2_CLIENT_FACTORY", f"{__name__}:_seam_factory")
        return fake
    yield _use
    _SEAM.clear()


def test_acquire_stamps_the_lease_before_it_starts_the_box(seam):
    """ADR-0151 D8: tag first, then start — the platform's 0.1.0 order was the
    reverse."""
    ec2 = seam(FakeEC2())
    asyncio.run(ComputeAcquireOp().run(
        AcquireInput(connection="gpu", max_hours=2, acquired_by="pl_7"), _ctx("acquire")))

    kinds = [c[0] for c in ec2.calls]
    assert kinds.index("tags") < kinds.index("start"), kinds
    assert ec2.tags[cl.TAG_MANAGED] == "cairn"
    assert ec2.tags[cl.TAG_ACQUIRED_BY] == "pl_7"
    assert cl.TAG_RELEASE_AFTER in ec2.tags


def test_a_crash_between_the_two_calls_never_leaves_an_untagged_billing_box(seam):
    """The sweep filters on ``cairn:managed``: a running box without it is a box
    nothing will ever stop. Whatever single call lands before the crash, the
    instance must not be billing without the tag."""
    ec2 = seam(_CrashAfterFirstWrite())
    with pytest.raises(RuntimeError, match="control plane died"):
        asyncio.run(ComputeAcquireOp().run(
            AcquireInput(connection="gpu", max_hours=2), _ctx("acquire")))

    billing = ec2.state in cl.BILLING_STATES
    assert not (billing and cl.TAG_MANAGED not in ec2.tags), (ec2.state, ec2.tags)
    assert ec2.tags.get(cl.TAG_MANAGED) == "cairn"
    assert not billing, "the start never ran, so nothing bills"


def test_acquire_mock_never_reaches_the_ec2_seam(seam):
    """Mock is a hard stop: the env seam would hand out a client if asked, and
    the mock path must never ask."""
    ec2 = seam(FakeEC2())
    out = asyncio.run(ComputeAcquireOp().mock(
        AcquireInput(connection="gpu", max_hours=1), _ctx("acquire")))
    assert out.note.startswith("mocked") and out.instance_id == "i-mock"
    assert ec2.calls == []


def test_release_mock_never_reaches_the_ec2_seam(seam):
    ec2 = seam(FakeEC2(state="running"))
    out = asyncio.run(ComputeReleaseOp().mock(ReleaseInput(connection="gpu"), _ctx("release")))
    assert out.note.startswith("mocked")
    assert ec2.calls == []


# ── the class and the manifest agree (CLAUDE.md §4 invariant 6) ──────────────
def test_the_class_risk_is_the_manifest_risk():
    """The bridge's restart rule reads the class; policy reads the manifest. They
    must say the same thing (acquire paid_compute, release external_write)."""
    import tomllib
    from pathlib import Path

    manifest = tomllib.loads((Path(__file__).parents[1] / "pack.toml").read_text())["operator"]
    for ref, cls in (("compute/acquire", ComputeAcquireOp), ("compute/release", ComputeReleaseOp)):
        assert manifest[ref]["risk"] == cls.risk, ref
        assert manifest[ref]["supports_mock"] is cls.supports_mock is True, ref
        assert manifest[ref]["capabilities"]["provides"] == list(cls.capabilities), ref
        assert "implements" not in manifest[ref], f"{ref}: no contract URI yet (ADR-0151 D7)"
    assert manifest["compute/acquire"]["risk"] == "paid_compute"
    assert manifest["compute/release"]["risk"] == "external_write"
