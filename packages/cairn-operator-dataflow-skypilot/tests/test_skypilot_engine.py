"""SkyPilotEngine (ADR-0104 D5, #2073) — every ``sky`` call is behind ONE seam
and every test drives a fake through it.

A live ``sky launch`` provisions a paid machine, so nothing here may reach the
real SDK: the engine takes ``sky=`` (the seam) and the fake records what the
adapter WOULD have submitted. That is the same discipline
``cairn.core.compute_lifecycle`` uses for boto3 (``CAIRN_EC2_CLIENT_FACTORY``)
and for the same reason — the governance is provable without spending money.
"""
from __future__ import annotations

import asyncio
import json

import pytest

import cairn.core.connections as connections
from cairn.core.dataflow import Budget, DataflowSpec, EngineJobHandle, StageRef
from cairn.core.dataflow.contract import StageResources

from operator_dataflow_skypilot import engines_skypilot as sp
from operator_dataflow_skypilot.factory import skypilot_engine
from operator_dataflow_skypilot.engines_skypilot import SkyPilotEngine


# ── the fake SDK ────────────────────────────────────────────────────────────


class FakeSky:
    """Records calls; answers `status` from a scripted queue."""

    def __init__(self, statuses=None, enabled=("ssh/gpubox",)):
        self.launched: list[tuple[dict, str]] = []
        self.downed: list[str] = []
        self.autostop: list = []
        self._statuses = list(statuses or [])
        self._enabled = list(enabled)

    def launch(self, task: dict, cluster_name: str, idle_minutes=None) -> dict:
        self.launched.append((task, cluster_name))
        self.autostop.append(idle_minutes)
        return {"job_id": 1, "cluster_name": cluster_name}

    def status(self, cluster_name: str) -> dict:
        return self._statuses.pop(0) if self._statuses else {"status": "UP", "job_status": "RUNNING"}

    def down(self, cluster_name: str) -> None:
        self.downed.append(cluster_name)

    def enabled_infra(self) -> list[str]:
        return list(self._enabled)


# NOTE (#2073): `_spec` asks for a GPU, and since the accelerator fix a GPU
# request needs a declared TYPE — SkyPilot wants <type>:<count>, and a bare
# count is an accelerator NAMED that number. So the engines below are built
# with `accelerator="A100"`; that is the environment's knob, not the flow's.
def _spec(**kw) -> DataflowSpec:
    return DataflowSpec(
        stages=[
            StageRef(stage="noop", resources=StageResources(cpu=4, memory_mb=16384)),
            StageRef(stage="noop.slow", config={"seconds": 1},
                     resources=StageResources(gpu=1, cpu=8)),
        ],
        parent_run="r-42",
        **kw,
    )


@pytest.fixture(autouse=True)
def _ssh_connection(monkeypatch):
    """`ml_target` names a compute.ssh box; resolve_connection is the seam the
    SSH engine already resolves through."""

    class _Conn:
        name = "gpubox"
        kind = "compute.ssh"
        config = {"host": "10.0.0.9", "user": "ubuntu", "port": "22", "hourly_usd": "1.20"}
        secrets = {"PRIVATE_KEY": "-----BEGIN KEY-----\nx\n-----END KEY-----\n"}

    async def fake_resolve(name):
        return _Conn() if name == "gpubox" else None

    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)


_WRITTEN: list[tuple[str, str, dict]] = []


class _Runs:
    async def append_event(self, run_id, type, data=None):
        _WRITTEN.append((run_id, type, dict(data or {})))
        return None


class _Ctx:
    """The submitting OperationContext stand-in: the engine writes its teardown
    receipt through ``run_store()`` (#2453 B) and reads nothing else from it."""
    node_name = ""

    def run_store(self):
        return _Runs()


_CTX = _Ctx()


@pytest.fixture(autouse=True)
def _receipts():
    """What the engine wrote through the submitting context's run store."""
    _WRITTEN.clear()
    return _WRITTEN


# ── task construction ───────────────────────────────────────────────────────


def test_cluster_name_is_deterministic_from_run_and_node():
    a = sp.cluster_name("r-42", "train")
    assert a == sp.cluster_name("r-42", "train")
    assert a != sp.cluster_name("r-43", "train")
    assert a.startswith("cairn-") and a.islower() and len(a) <= 40
    # SkyPilot cluster names are DNS-ish: letters, digits, dashes only.
    assert all(c.isalnum() or c == "-" for c in a)


def test_build_task_maps_resources_envs_and_the_run_command():
    task = sp.build_task(
        _spec(), name="cairn-r42-flow", infra="ssh/gpubox", use_spot=True,
        accelerator="A100",
        envs={"CAIRN_DATAFLOW_SPEC": "{}", "CAIRN_RUN_ID": "r-42"},
    )
    res = task["resources"]
    # aggregated per-stage hints: max across stages, "+" = at least.
    # #2073: this used to assert "1" — the COUNT alone — which real SkyPilot
    # parses as an accelerator NAMED "1" ({'1': 1}). The test encoded the bug.
    assert res["accelerators"] == "A100:1"
    assert res["cpus"] == "8+" and res["memory"] == "16+"
    assert res["infra"] == "ssh/gpubox" and res["use_spot"] is True
    assert task["run"].strip() == "cairn-dataflow-run"
    assert task["envs"]["CAIRN_RUN_ID"] == "r-42"
    assert task["name"] == "cairn-r42-flow"
    assert task["num_nodes"] == 1


def test_build_task_uses_the_image_when_one_is_configured():
    task = sp.build_task(_spec(), name="c", accelerator="A100",
                         image="ghcr.io/example/cairn-train-gpu:1")
    assert task["resources"]["image_id"] == "docker:ghcr.io/example/cairn-train-gpu:1"
    assert "setup" not in task  # an image carries the install; a setup line would fight it


def test_build_task_falls_back_to_a_pip_setup_line_without_an_image():
    task = sp.build_task(_spec(), name="c", accelerator="A100")
    assert task["setup"] == "pip install 'cairndev[dataflow-engine]'"  # ADR-0137 D5: the engine, never a builtin dist


# ── submit: envs, the live channel, and the cost gate ───────────────────────


def test_submit_launches_once_with_the_spec_and_the_live_channel(monkeypatch):
    monkeypatch.setenv("OBS_JOB_CHANNEL", "nats://bus:4222")
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    handle = asyncio.run(eng.submit(_spec(), _CTX))

    assert len(fake.launched) == 1
    task, cname = fake.launched[0]
    assert cname == handle.job_id and handle.engine == "skypilot"
    envs = task["envs"]
    # the SAME spec-over-env contract the builtin and SSH engines run
    assert json.loads(envs["CAIRN_DATAFLOW_SPEC"])["stages"][0]["stage"] == "noop"
    # #2025 live channel — the curve streams back from the cluster
    assert envs["CAIRN_RUN_ID"] == "r-42" and envs["OBS_JOB_CHANNEL"] == "nats://bus:4222"
    # a compute.ssh target becomes an SSH node pool, never a cloud
    assert task["resources"]["infra"].startswith("ssh/")


def test_submit_forwards_only_essential_env(monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak-me")
    monkeypatch.setenv("LANG", "en_US.UTF-8")
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    asyncio.run(eng.submit(_spec(), _CTX))
    envs = fake.launched[0][0]["envs"]
    assert "AWS_SECRET_ACCESS_KEY" not in envs
    assert envs.get("LANG") == "en_US.UTF-8"


def test_over_budget_estimate_refuses_before_any_launch():
    fake = FakeSky()
    # $1.20/hr from the connection × 10h ceiling = $12 > the $2 budget
    spec = _spec(budget=Budget(max_cost_usd=2.0, max_seconds=36000))
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    with pytest.raises(Exception) as err:
        asyncio.run(eng.submit(spec, _CTX))
    assert "budget" in str(err.value).lower()
    assert fake.launched == [] and fake.downed == []


def test_within_budget_launches():
    fake = FakeSky()
    spec = _spec(budget=Budget(max_cost_usd=50.0, max_seconds=3600))
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    asyncio.run(eng.submit(spec, _CTX))
    assert len(fake.launched) == 1


def test_estimate_seam_is_injectable():
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100", estimate=lambda spec, res: 999.0)
    with pytest.raises(Exception):
        asyncio.run(eng.submit(_spec(budget=Budget(max_cost_usd=1.0)), _CTX))
    assert fake.launched == []


# ── poll: cluster + job state → the engine vocabulary ───────────────────────


@pytest.mark.parametrize("record,expected", [
    ({"status": "INIT", "job_status": None}, "queued"),
    ({"status": "UP", "job_status": "PENDING"}, "queued"),
    ({"status": "UP", "job_status": "RUNNING"}, "running"),
    ({"status": "UP", "job_status": "SUCCEEDED"}, "succeeded"),
    ({"status": "UP", "job_status": "FAILED"}, "failed"),
    ({"status": "UP", "job_status": "CANCELLED"}, "cancelled"),
    ({"status": "STOPPED", "job_status": "RUNNING"}, "failed"),
    ({}, "failed"),  # cluster gone entirely
])
def test_poll_maps_states(record, expected):
    fake = FakeSky(statuses=[record])
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot",
                             meta={"cluster": "cairn-x", "run_id": "r-42"})
    st = asyncio.run(eng.poll(handle))
    assert st.state == expected


def test_a_reclaimed_spot_cluster_is_retriable():
    fake = FakeSky(statuses=[{"status": "STOPPED", "job_status": "RUNNING"}])
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot",
                             meta={"cluster": "cairn-x", "run_id": "r-42", "use_spot": True})
    st = asyncio.run(eng.poll(handle))
    assert st.state == "failed" and st.retriable is True


# ── teardown + receipt ──────────────────────────────────────────────────────


def test_terminal_state_tears_down_once_and_writes_a_receipt(_receipts):
    fake = FakeSky(statuses=[{"status": "UP", "job_status": "SUCCEEDED"},
                             {"status": "UP", "job_status": "SUCCEEDED"}])
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    eng._run_stores["cairn-x"] = _CTX.run_store  # what submit() records for the cluster (#2453 B)
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot", meta={
        "cluster": "cairn-x", "run_id": "r-42", "resources": "1x gpu",
        "launched_at": 0.0, "estimate_usd": 1.5,
    })
    asyncio.run(eng.poll(handle))
    asyncio.run(eng.poll(handle))  # a second poll must NOT tear down twice
    assert fake.downed == ["cairn-x"]
    kinds = [t for _, t, _ in _receipts]
    assert kinds.count(sp.RECEIPT_EVENT_TYPE) == 1
    run_id, _, payload = _receipts[0]
    assert run_id == "r-42"
    assert payload["cluster"] == "cairn-x" and payload["resources"] == "1x gpu"
    assert payload["estimated_usd"] == 1.5 and payload["duration_s"] >= 0


def test_without_the_submitting_context_the_receipt_is_logged_not_written(_receipts, caplog):
    """The process that submitted the job is gone (no run store for the cluster):
    the cluster still comes down once, and the receipt goes to the log, not the
    store — the engine never reaches for storage itself (#2453 B)."""
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot",
                             meta={"cluster": "cairn-x", "run_id": "r-42"})
    with caplog.at_level("WARNING"):
        asyncio.run(eng.cancel(handle))
    assert fake.downed == ["cairn-x"]
    assert _receipts == []
    assert any("not persisted" in r.getMessage() for r in caplog.records)


def test_cancel_tears_down_once_and_writes_a_receipt(_receipts):
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    eng._run_stores["cairn-x"] = _CTX.run_store
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot",
                             meta={"cluster": "cairn-x", "run_id": "r-42"})
    asyncio.run(eng.cancel(handle))
    asyncio.run(eng.cancel(handle))
    assert fake.downed == ["cairn-x"]
    assert [t for _, t, _ in _receipts].count(sp.RECEIPT_EVENT_TYPE) == 1
    assert _receipts[0][2]["reason"] == "cancelled"


# ── target resolution: the compute.ssh box becomes a node pool ──────────────


def test_ssh_connection_becomes_a_node_pool_entry(tmp_path):
    target = asyncio.run(sp.resolve_target("gpubox", home=tmp_path))
    pools_file = tmp_path / ".sky" / "ssh_node_pools.yaml"
    assert pools_file.exists()
    text = pools_file.read_text()
    assert "gpubox:" in text and "10.0.0.9" in text and "ubuntu" in text
    assert target.infra == "ssh/gpubox"
    # the key is materialized because SkyPilot reads an identity_file PATH,
    # and only with owner-only permissions
    key = tmp_path / ".sky" / "cairn-keys" / "gpubox.pem"
    assert key.exists() and (key.stat().st_mode & 0o077) == 0
    assert str(key) in text


def test_node_pool_write_is_idempotent_and_keeps_other_pools(tmp_path):
    (tmp_path / ".sky").mkdir()
    (tmp_path / ".sky" / "ssh_node_pools.yaml").write_text("other:\n  hosts:\n  - 1.2.3.4\n")
    asyncio.run(sp.resolve_target("gpubox", home=tmp_path))
    asyncio.run(sp.resolve_target("gpubox", home=tmp_path))
    text = (tmp_path / ".sky" / "ssh_node_pools.yaml").read_text()
    assert text.count("gpubox:") == 1 and "other:" in text


def test_an_unresolvable_target_is_a_loud_refusal(tmp_path):
    with pytest.raises(Exception) as err:
        asyncio.run(sp.resolve_target("nope", home=tmp_path))
    assert "nope" in str(err.value)


def test_vast_connection_becomes_the_credential_file_skypilot_reads(tmp_path, monkeypatch):
    class _Vast:
        name = "vastai"
        kind = "compute.vast"
        config = {}
        secrets = {"API_KEY": "vk-123"}

    async def fake_resolve(name):
        return _Vast()

    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)
    target = asyncio.run(sp.resolve_target("vastai", home=tmp_path))
    assert target.infra == "vast"
    keyfile = tmp_path / ".config" / "vastai" / "vast_api_key"
    assert keyfile.read_text().strip() == "vk-123"
    assert (keyfile.stat().st_mode & 0o077) == 0


# ── the registry factory ────────────────────────────────────────────────────


def test_factory_builds_the_engine():
    eng = skypilot_engine(kind="ml", target="gpubox")
    assert isinstance(eng, SkyPilotEngine) and eng.name == "skypilot"


# ── the pack half of the door: check_ready ──────────────────────────────────


def test_check_ready_is_silent_when_target_and_cloud_hold(tmp_path):
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    assert asyncio.run(eng.check_ready()) == []


def test_check_ready_names_an_unresolvable_target(tmp_path):
    eng = SkyPilotEngine("ghost", sky=FakeSky(), home=tmp_path)
    reasons = asyncio.run(eng.check_ready())
    assert reasons and "ghost" in reasons[0]


def test_check_ready_names_a_cloud_sky_check_has_not_enabled(tmp_path):
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["aws"]), home=tmp_path)
    reasons = asyncio.run(eng.check_ready())
    assert reasons and "ssh/gpubox" in reasons[0] and "aws" in reasons[0]
    assert "sky check" in reasons[0]


def test_check_ready_refuses_an_empty_target(tmp_path):
    eng = SkyPilotEngine("", sky=FakeSky(), home=tmp_path)
    reasons = asyncio.run(eng.check_ready())
    assert reasons and "target" in reasons[0]


# ── autostop: the backstop for a control plane that dies mid-run ────────────


def test_autostop_is_derived_from_the_run_budget():
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    asyncio.run(eng.submit(_spec(budget=Budget(max_seconds=3600, max_cost_usd=500)), _CTX))
    # one hour of budget + the idle grace, in minutes
    assert fake.autostop[0] == 60 + sp.AUTOSTOP_GRACE_MIN


def test_autostop_falls_back_to_the_default_ceiling_without_a_budget():
    fake = FakeSky()
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100")
    asyncio.run(eng.submit(_spec(), _CTX))
    assert fake.autostop[0] == sp.AUTOSTOP_DEFAULT_MIN


def test_the_door_resolves_a_target_without_writing_any_credential(tmp_path):
    """A dry run must not materialize secrets. `check_ready` answers the door's
    question from the connection alone; only a real submit writes the node pool
    and the key."""
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    assert asyncio.run(eng.check_ready()) == []
    assert not (tmp_path / ".sky").exists()


def test_resolve_target_can_answer_without_materializing(tmp_path):
    target = asyncio.run(sp.resolve_target("gpubox", home=tmp_path, materialize=False))
    assert target.infra == "ssh/gpubox" and target.files == []
    assert not (tmp_path / ".sky").exists()


# ── conformance with the REAL SkyPilot 0.13 API (#2073) ──────────────────
# Every call in `_SkySdk` was written from documentation and never run against
# an install. Checking them against skypilot 0.13.0 in a throwaway venv found
# three defects, each of which would have failed a real launch:
#   1. `sky.enabled_clouds` does not exist at the top level — it is
#      `sky.client.sdk.enabled_clouds`, so the DOOR raised and refused every
#      launch even on a correct install;
#   2. `sky.status(refresh=...)` takes a StatusRefreshMode enum, not a bool;
#   3. `accelerators` needs a TYPE. A bare count renders as an accelerator
#      *named* "1" ({'1': 1}), which no cloud can satisfy.
# These tests pin all three without needing skypilot installed.


class _FakeSdkModule:
    """The shape skypilot 0.13 actually exposes."""

    class client:
        class sdk:
            @staticmethod
            def enabled_clouds(workspace=None, expand=False):
                return "REQ:enabled"

    class utils:
        class common:
            class StatusRefreshMode:
                NONE = "NONE"
                AUTO = "AUTO"
                FORCE = "FORCE"

    @staticmethod
    def get(request_id):
        return ["aws", "ssh"] if request_id == "REQ:enabled" else []


def test_enabled_infra_uses_the_sdk_path_that_exists(monkeypatch):
    """`sky.enabled_clouds` is not a thing; the door must not depend on it."""
    import sys

    from operator_dataflow_skypilot import engines_skypilot as sp

    monkeypatch.setitem(sys.modules, "sky", _FakeSdkModule)
    monkeypatch.setitem(sys.modules, "sky.client", _FakeSdkModule.client)
    monkeypatch.setitem(sys.modules, "sky.client.sdk", _FakeSdkModule.client.sdk)
    sdk = sp._SkySdk()
    assert sorted(sdk.enabled_infra()) == ["aws", "ssh"]


def test_a_gpu_request_emits_a_TYPED_accelerator():
    """`accelerators: "1"` is an accelerator NAMED "1" — SkyPilot parses it as
    {'1': 1} and no cloud has one. It must be `<type>:<count>`."""
    from cairn.core.dataflow import DataflowSpec
    from cairn.core.dataflow.contract import StageRef

    from operator_dataflow_skypilot.engines_skypilot import build_task

    spec = DataflowSpec(stages=[StageRef(stage="train.qlora", resources={"gpu": 2})])
    task = build_task(spec, name="t", accelerator="A100")
    assert task["resources"]["accelerators"] == "A100:2"


def test_a_gpu_request_without_a_type_is_refused():
    """Fail closed: emitting a nonsense accelerator would surface as a cloud
    error minutes later, or worse, as a machine with no GPU."""
    from cairn.core.dataflow import DataflowSpec
    from cairn.core.dataflow.contract import StageRef

    from operator_dataflow_skypilot.engines_skypilot import EngineError, build_task

    spec = DataflowSpec(stages=[StageRef(stage="train.qlora", resources={"gpu": 1})])
    with pytest.raises(EngineError) as e:
        build_task(spec, name="t", accelerator="")
    assert "accelerator" in str(e.value).lower()


# ── #2102: an EMPTY `sky check` is a refusal, not a pass ──────────────────
# Seen live inside the chart-installed control plane with real skypilot 0.13:
# `enabled_infra()` returned [] (no cloud, no pool provisioned) and the door
# said ok — because `if enabled and cloud not in enabled` skips the whole check
# when the list is empty. That is the one state that most clearly means "this
# install can launch on nothing", and a launch then reaches `sky.launch` and
# fails AFTER the run record exists.


class _NothingEnabledSdk:
    def enabled_infra(self):
        return []


async def test_no_enabled_infra_is_a_refusal_not_a_pass(monkeypatch):
    from operator_dataflow_skypilot import engines_skypilot as sp

    async def _resolve(name, *, home=None, materialize=True):
        return sp.SkyTarget(infra="ssh/gpubox", env={}, files=[])

    monkeypatch.setattr(sp, "resolve_target", _resolve)
    eng = sp.SkyPilotEngine("gpubox", sky=_NothingEnabledSdk(), accelerator="T4")
    reasons = await eng.check_ready()
    assert reasons, "an empty sky check must refuse — nothing is enabled to launch on"
    joined = " ".join(reasons).lower()
    assert "sky check" in joined and "ssh/gpubox" in joined
    assert "sky ssh up" in joined, "an SSH pool must be told how to become enabled"


# ── #2102 item 2: the two binaries SkyPilot shells out to ────────────────
# Found live in the chart-installed control plane on the GPU box: the pod had
# `cairndev[skypilot]` installed and NEITHER `ssh` nor `rsync`
# (`command -v ssh` → nothing). SkyPilot shells out to both for every node it
# provisions or adopts, `sky ssh up` needs them to register a pool, and the
# fetch-back below rides them too — so their absence is a door refusal that
# must NAME the missing program, not a launch that fails late on a cloud.


async def test_check_ready_refuses_when_ssh_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(sp.shutil, "which", lambda name: None if name == "ssh" else "/usr/bin/" + name)
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    reasons = await eng.check_ready()
    assert reasons, "a control plane with no ssh cannot launch anything"
    # the refusal NAMES the missing binary — "skypilot is not ready" would send
    # a reader to sky check, which would pass
    assert reasons[0].startswith("skypilot needs ssh on PATH")
    assert "openssh-client" in reasons[0]


async def test_check_ready_refuses_when_rsync_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(sp.shutil, "which", lambda name: None if name == "rsync" else "/usr/bin/" + name)
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    reasons = await eng.check_ready()
    assert reasons and reasons[0].startswith("skypilot needs rsync on PATH")
    assert "openssh-client rsync" in reasons[0]


async def test_check_ready_names_both_binaries_when_both_are_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(sp.shutil, "which", lambda name: None)
    eng = SkyPilotEngine("gpubox", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    reasons = await eng.check_ready()
    assert len(reasons) == 1, "the missing binaries are the whole answer — nothing below them can pass"
    assert "ssh" in reasons[0] and "rsync" in reasons[0]


def test_missing_binaries_is_a_plain_path_probe(monkeypatch):
    monkeypatch.setattr(sp.shutil, "which", lambda name: None if name == "rsync" else "/bin/ssh")
    assert sp.missing_binaries() == ["rsync"]


# ── #2102: results come home BEFORE the machine is destroyed ─────────────


class _RecordingFetch:
    """The fetch seam. Writes what a real rsync would have landed and records
    WHEN it ran relative to `sky down` — the ordering IS the property."""

    def __init__(self, timeline: list, payload: dict | None = None):
        self.timeline = timeline
        self.payload = payload
        self.calls: list[tuple] = []

    def __call__(self, cluster, workdir, names, dest, *, home=None, timeout=None):
        self.timeline.append(("fetch", cluster))
        self.calls.append((cluster, workdir, tuple(names), dest))
        if self.payload is None:
            return 0
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "result.json").write_text(json.dumps(self.payload), encoding="utf-8")
        return len(names)


class _OrderedSky(FakeSky):
    def __init__(self, timeline, **kw):
        super().__init__(**kw)
        self.timeline = timeline

    def down(self, cluster_name: str) -> None:
        self.timeline.append(("down", cluster_name))
        super().down(cluster_name)


def _submitted(home, timeline, payload=None, statuses=None):
    fake = _OrderedSky(timeline, statuses=statuses)
    fetch = _RecordingFetch(timeline, payload)
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100", fetch=fetch,
                         home=home)
    handle = asyncio.run(eng.submit(_spec(), _CTX))
    return eng, fake, fetch, handle


def test_submit_records_the_workdir_and_the_fetch_contract(tmp_path):
    eng, fake, _fetch, handle = _submitted(tmp_path, [])
    cluster = handle.job_id
    assert handle.meta["workdir"] == f"{sp.REMOTE_WORKDIR_ROOT}/{cluster}"
    # the SSH engine's OWN table — one result contract, two transports
    assert handle.meta["fetch"][:2] == ["result.json", "metrics.json"]
    # ...and the task actually runs there, or the artifacts land nowhere findable
    task = fake.launched[0][0]
    assert f'cd "$HOME"/{sp.REMOTE_WORKDIR_ROOT}/{cluster}' in task["run"]
    assert task["run"].rstrip().endswith(sp.REMOTE_COMMAND)


def test_a_succeeded_job_is_fetched_before_the_cluster_is_torn_down(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    timeline: list = []
    eng, fake, fetch, handle = _submitted(
        tmp_path, timeline, payload={"stats": {}},
        statuses=[{"status": "UP", "job_status": "SUCCEEDED"}])
    st = asyncio.run(eng.poll(handle))
    assert st.state == "succeeded"
    kinds = [k for k, _ in timeline]
    assert kinds == ["fetch", "down"], (
        "the product lives on the machine sky down destroys — fetching after it "
        "loses the result with the disk")
    assert fetch.calls[0][1] == handle.meta["workdir"]


def test_cancel_fetches_before_it_destroys_the_cluster(tmp_path, monkeypatch):
    """A cancel can arrive after the job already SUCCEEDED; tearing down first
    would throw away a result that was sitting on the disk."""
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    timeline: list = []
    eng, _fake, _fetch, handle = _submitted(tmp_path, timeline, payload={"stats": {}})
    asyncio.run(eng.cancel(handle))
    assert [k for k, _ in timeline] == ["fetch", "down"]


def test_the_receipt_says_how_much_came_home(tmp_path, monkeypatch, _receipts):
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    eng, _fake, _fetch, handle = _submitted(
        tmp_path, [], payload={"stats": {}},
        statuses=[{"status": "UP", "job_status": "SUCCEEDED"}])
    asyncio.run(eng.poll(handle))
    payload = [d for _, t, d in _receipts if t == sp.RECEIPT_EVENT_TYPE][-1]
    assert payload["fetched"] == len(handle.meta["fetch"])


def test_result_reads_the_fetched_json_and_rewrites_stats_to_local_copies(tmp_path, monkeypatch):
    """The SSH engine's contract, exactly: remote workdir paths in `stats`
    become the LOCAL fetched copies, so a pipeline hoist can open them."""
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    workdir = f"{sp.REMOTE_WORKDIR_ROOT}/cairn-x"
    landed = tmp_path / "cairn-x"
    landed.mkdir()
    (landed / "train_result.json").write_text("{}", encoding="utf-8")
    (landed / "result.json").write_text(json.dumps({
        "stats": {
            "train.qlora.result_path": f"/home/ubuntu/{workdir}/train_result.json",
            "rows": 7,
        },
        "lineage_parent_run": "r-42",
    }), encoding="utf-8")

    eng = SkyPilotEngine("gpubox", sky=FakeSky(), accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot", meta={
        "cluster": "cairn-x", "run_id": "r-42", "workdir": workdir,
        "image": "ghcr.io/example/cairn-train-gpu:v0.1.0",
    })
    res = asyncio.run(eng.result(handle))
    assert res.stats["train.qlora.result_path"] == str(landed / "train_result.json")
    assert res.stats["rows"] == 7
    # provenance, the same key the SSH engine stamps
    assert res.stats["engine.image"] == "ghcr.io/example/cairn-train-gpu:v0.1.0"


def test_result_refuses_loudly_when_nothing_was_fetched(tmp_path, monkeypatch):
    """A silently empty DataflowResult would read as 'the flow produced
    nothing' — the #568 discipline says refuse and say where to look."""
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    eng = SkyPilotEngine("gpubox", sky=FakeSky(), accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot", meta={
        "cluster": "cairn-x", "workdir": f"{sp.REMOTE_WORKDIR_ROOT}/cairn-x"})
    with pytest.raises(sp.EngineError) as err:
        asyncio.run(eng.result(handle))
    assert "result.json" in str(err.value) and "cairn-x" in str(err.value)


def test_result_surfaces_the_runners_own_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "fetch_dir", lambda wd: tmp_path / wd.rsplit("/", 1)[-1])
    landed = tmp_path / "cairn-x"
    landed.mkdir()
    (landed / "result.json").write_text(json.dumps({"error": "budget.max_seconds exceeded"}),
                                        encoding="utf-8")
    eng = SkyPilotEngine("gpubox", sky=FakeSky(), accelerator="A100")
    handle = EngineJobHandle(job_id="cairn-x", engine="skypilot", meta={
        "cluster": "cairn-x", "workdir": f"{sp.REMOTE_WORKDIR_ROOT}/cairn-x"})
    with pytest.raises(sp.EngineError) as err:
        asyncio.run(eng.result(handle))
    assert "budget.max_seconds" in str(err.value)


def test_a_failed_fetch_never_blocks_the_teardown(tmp_path, monkeypatch):
    """An unfetched result is bad; a cluster that keeps billing is worse."""
    timeline: list = []

    def _boom(*a, **kw):
        timeline.append(("fetch", "boom"))
        raise OSError("network is unreachable")

    fake = _OrderedSky(timeline, statuses=[{"status": "UP", "job_status": "SUCCEEDED"}])
    eng = SkyPilotEngine("gpubox", sky=fake, accelerator="A100", fetch=_boom)
    handle = asyncio.run(eng.submit(_spec(), _CTX))
    asyncio.run(eng.poll(handle))
    assert fake.downed == [handle.job_id]


# ── the fetch itself: SkyPilot's own generated ssh config ────────────────
# Verified against skypilot 0.13.0: SSHConfigHelper.ssh_cluster_path is
# `constants.SKY_USER_FILE_PATH + '/ssh/{}'` and SKY_USER_FILE_PATH is
# `~/.sky/generated`, so the stanza's Host IS the cluster name. There is no
# generic file-download call in `sky.client.sdk` (only `download_logs`, which
# returns LOGS), which is why the fetch goes over rsync at all.


def test_ssh_config_path_is_where_skypilot_writes_it(tmp_path):
    p = sp.ssh_config_path("cairn-x", home=tmp_path)
    assert p == tmp_path / ".sky" / "generated" / "ssh" / "cairn-x"


def test_fetch_is_a_no_op_without_a_generated_ssh_config(tmp_path):
    """No config means the cluster was never UP under this control plane —
    report nothing fetched rather than shelling out to a hostname rsync would
    try to resolve on the public internet."""
    assert sp.fetch_outputs("cairn-x", ".cairn-jobs/cairn-x", ["result.json"],
                            tmp_path / "dest", home=tmp_path) == 0


def test_fetch_runs_one_rsync_per_name_over_that_config(tmp_path, monkeypatch):
    cfg = tmp_path / ".sky" / "generated" / "ssh" / "cairn-x"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("Host cairn-x\n  HostName 10.0.0.9\n", encoding="utf-8")
    monkeypatch.setattr(sp.shutil, "which", lambda name: f"/usr/bin/{name}")
    ran: list[list[str]] = []

    class _Proc:
        returncode = 0
        stderr = b""

    monkeypatch.setattr(sp.subprocess, "run",
                        lambda cmd, **kw: (ran.append(cmd), _Proc())[1])
    got = sp.fetch_outputs("cairn-x", ".cairn-jobs/cairn-x",
                           ["result.json", "metrics.json"],
                           tmp_path / "dest", home=tmp_path)
    assert got == 2 and len(ran) == 2
    assert ran[0][0] == "/usr/bin/rsync"
    rsh = ran[0][ran[0].index("-e") + 1]
    assert rsh.startswith("/usr/bin/ssh -F ") and str(cfg) in rsh
    assert "BatchMode=yes" in rsh          # never hang the teardown on a prompt
    assert ran[0][-2] == "cairn-x:.cairn-jobs/cairn-x/result.json"


def test_a_name_the_stage_never_wrote_is_skipped_not_fatal(tmp_path, monkeypatch):
    cfg = tmp_path / ".sky" / "generated" / "ssh" / "cairn-x"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("Host cairn-x\n", encoding="utf-8")
    monkeypatch.setattr(sp.shutil, "which", lambda name: f"/usr/bin/{name}")

    class _Proc:
        def __init__(self, rc):
            self.returncode = rc
            self.stderr = b"No such file or directory"

    codes = iter([0, 23])
    monkeypatch.setattr(sp.subprocess, "run", lambda cmd, **kw: _Proc(next(codes)))
    assert sp.fetch_outputs("cairn-x", ".cairn-jobs/cairn-x",
                            ["result.json", "splits"],
                            tmp_path / "dest", home=tmp_path) == 1


def test_check_ready_refuses_an_empty_target_before_asking_anything_else(tmp_path):
    """ADR-0134 D3c: the empty-target rule moved here from core's SkyPilot door — the
    engine owns what it needs. No sky call is made to find this out."""
    eng = SkyPilotEngine("", sky=FakeSky(enabled=["ssh"]), home=tmp_path)
    reasons = asyncio.run(eng.check_ready())
    assert len(reasons) == 1 and "ml_target/dataflow_target is empty" in reasons[0]
