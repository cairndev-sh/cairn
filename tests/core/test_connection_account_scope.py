"""Connections and declared secrets resolve in the run's account.

A run's account is the owner of its workspace (``Workspace.account_id``). A
tenant account's run resolves only its own connections by name and reads only
its own secret values; the deployment's (``default``) run resolves only the
deployment's. The deployment's behaviour is otherwise unchanged. Hermetic:
in-memory connection and workspace stores, a temp SQLite vault, no network.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.fernet import Fernet
from pydantic import BaseModel

import cairn.core.connections as connections
from cairn.core import workspace
from cairn.core.connections import (
    Connection,
    HealthState,
    InMemoryConnectionStore,
    declared_secret_values,
    inject_declared_secrets,
    resolve_connection,
)
from cairn.core.operator.binding import ExecutionField, OperationBinding
from cairn.core.operator.runtime import BindingEvalError
from cairn.core.operator.secrets import build_scoped_secrets
from cairn.core.secrets_store import mint_ref, put_secret
from cairn.core.workspace import InMemoryWorkspaceStore, Workspace, current_workspace, workspace_scope


class _Provider:
    """A bound deployment SecretStore that records every read."""

    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.reads: list[str] = []

    def get(self, name: str) -> str:
        self.reads.append(name)
        if name not in self.values:
            raise KeyError(name)
        return self.values[name]

    def available_keys(self) -> list[str]:
        return sorted(self.values)


@pytest.fixture
async def world(tmp_path, monkeypatch):
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    monkeypatch.delenv("CAIRN_SECRET_BACKEND", raising=False)
    monkeypatch.setenv("CAIRN_SECRETS_DB", str(tmp_path / "secrets.db"))
    monkeypatch.setenv("CAIRN_SECRET_KEY", Fernet.generate_key().decode())
    for var in ("KAGGLE_USERNAME", "KAGGLE_KEY", "HF_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    wstore = InMemoryWorkspaceStore()
    await wstore.put(Workspace(id="ws-a", account_id="acct-a"))
    await wstore.put(Workspace(id="ws-b", account_id="acct-b"))
    monkeypatch.setattr(workspace, "_store", wstore)

    store = InMemoryConnectionStore()
    # the deployment's own rows
    await store.create(Connection(name="gpu", kind="compute.ssh", config={"host": "deploy-host"},
                                  max_in_flight=2,
                                  health=HealthState(ok=False, detail="deploy box down")))
    await store.create(Connection(name="dep-kaggle", kind="api_key",
                                  secrets={"USERNAME": "KAGGLE_USERNAME", "KEY": "KAGGLE_KEY"}))
    await store.create(Connection(name="huggingface", kind="api_key", secrets={"TOKEN": "HF_TOKEN"}))
    # tenant rows
    await store.create(Connection(name="a-box", kind="compute.ssh", account_id="acct-a",
                                  config={"host": "a-host"}))
    await store.create(Connection(name="b-box", kind="compute.ssh", account_id="acct-b",
                                  config={"host": "b-host"}))
    a_key = put_secret(mint_ref("kaggle", "KEY", account_id="acct-a"), "a-kaggle-key", account_id="acct-a")
    await store.create(Connection(name="kaggle", kind="api_key", account_id="acct-a",
                                  secrets={"KEY": a_key}))
    b_tok = put_secret(mint_ref("b-hf", "HF_TOKEN", account_id="acct-b"), "b-hf-token", account_id="acct-b")
    await store.create(Connection(name="b-hf", kind="api_key", account_id="acct-b",
                                  secrets={"HF_TOKEN": b_tok}))
    monkeypatch.setattr(connections, "default_connection_store", lambda: store)

    provider = _Provider({"KAGGLE_USERNAME": "dep-user", "KAGGLE_KEY": "dep-key", "HF_TOKEN": "dep-hf"})
    monkeypatch.setattr("cairn.core.operator.secrets.get_secret_provider", lambda: provider)
    return SimpleNamespace(store=store, provider=provider)


def _env_snapshot() -> dict[str, str]:
    return dict(os.environ)


# ── workspace → account ───────────────────────────────────────────────


async def test_workspace_account_maps_owner_and_defaults(world):
    assert await workspace.workspace_account("ws-a") == "acct-a"
    assert await workspace.workspace_account("default") == "default"
    # auth off: a workspace no store holds is the deployment's (keyless dev)
    assert await workspace.workspace_account("never-created") == "default"
    with workspace_scope("ws-b"):
        assert await workspace.workspace_account() == "acct-b"
    with workspace_scope(""):                            # auth off: no workspace = default
        assert await workspace.workspace_account() == "default"


async def test_auth_on_a_rowless_workspace_has_no_account(world, monkeypatch):
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    assert await workspace.workspace_account("ws-a") == "acct-a"
    with pytest.raises(workspace.WorkspaceForbidden):
        await workspace.workspace_account("never-created")
    with workspace_scope("never-created"), pytest.raises(workspace.WorkspaceForbidden):
        await resolve_connection("gpu")


async def test_auth_on_a_record_with_no_workspace_is_refused(world, monkeypatch):
    from cairn.core.dispatch.pipeline_bridge import _in_record_workspace

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    with pytest.raises(workspace.WorkspaceScopeRequired):
        with workspace_scope(""):
            pass

    class Probe:
        refused: list = []
        ran: list = []

        async def _refuse_unscoped(self, method, rec, *args):
            self.refused.append(method)

        @_in_record_workspace
        async def leg(self, rec):
            self.ran.append(await resolve_connection("gpu"))

    probe = Probe()
    assert await probe.leg(SimpleNamespace(workspace_id="")) is False   # the caller is told to stop
    assert probe.refused == ["leg"] and probe.ran == []      # refused visibly, nothing resolved


def test_workspace_scope_restores_the_outer_workspace():
    outer = current_workspace()
    with workspace_scope("ws-a"):
        assert current_workspace() == "ws-a"
    assert current_workspace() == outer


# ── resolve_connection ────────────────────────────────────────────────


async def test_auth_on_resolution_outside_any_workspace_scope_is_refused(world, monkeypatch):
    """A template's operation node on a durable runtime's worker runs with no
    request and no record-scoped run: with auth on it is refused, never resolved
    as the deployment."""
    from cairn.core.operator.runtime import invoke_operator_core, invoke_operator_inprocess

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    assert workspace._active.get() is None
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await resolve_connection("gpu")
    binding = OperationBinding(operator="probe/op", input={}, output={},
                               execution=ExecutionField(compute_target="gpu"))
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await invoke_operator_inprocess(binding, _op({}), state={}, pipeline_id="p", node_name="n")
    before = _env_snapshot()
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await invoke_operator_core(_op({}), {}, pipeline_id="p", node_name="n", secret_keys=["KAGGLE_KEY"])
    assert _env_snapshot() == before
    assert await inject_declared_secrets(["KAGGLE_KEY"]) == []          # never raises, injects nothing
    # an explicit account (the deployment's own tools) and a scoped run still resolve
    assert (await resolve_connection("gpu", account_id="default")).config["host"] == "deploy-host"
    with workspace_scope("ws-a"):
        assert (await resolve_connection("a-box")).config["host"] == "a-host"


async def test_auth_off_resolution_outside_a_scope_is_the_deployments(world):
    assert workspace._active.get() is None
    assert (await resolve_connection("gpu")).config["host"] == "deploy-host"




async def test_tenant_resolves_only_its_own_rows_by_name(world):
    assert (await resolve_connection("a-box", account_id="acct-a")).config["host"] == "a-host"
    assert await resolve_connection("b-box", account_id="acct-a") is None   # another account's
    assert await resolve_connection("gpu", account_id="acct-a") is None     # the deployment's


async def test_deployment_resolves_only_deployment_rows(world):
    assert (await resolve_connection("gpu", account_id="default")).config["host"] == "deploy-host"
    assert (await resolve_connection("gpu")).config["host"] == "deploy-host"  # no context: deployment
    assert await resolve_connection("a-box", account_id="default") is None
    assert await resolve_connection("a-box") is None


async def test_omitted_account_is_the_run_workspace_owner(world):
    with workspace_scope("ws-a"):
        assert (await resolve_connection("a-box")).config["host"] == "a-host"
        assert await resolve_connection("gpu") is None
        assert await resolve_connection("b-box") is None
    with workspace_scope("default"):
        assert (await resolve_connection("gpu")).config["host"] == "deploy-host"


# ── operator runtime: compute_target ──────────────────────────────────


class _Inp(BaseModel):
    x: int = 1


class _Out(BaseModel):
    host: str = ""
    key: str = ""
    hf: str = ""


def _op(seen: dict):
    class Probe:
        name = "probe/op"
        input_schema = _Inp
        output_schema = _Out
        supports_mock = False

        def run(self, inp, ctx):
            seen["compute"] = getattr(ctx, "compute", None)
            try:
                seen["key"] = ctx.secrets.get("KAGGLE_KEY")
            except KeyError as e:
                seen["key_error"] = str(e)
            try:
                seen["hf"] = ctx.secrets.get("HF_TOKEN")
            except KeyError as e:
                seen["hf_error"] = str(e)
            return _Out()

    return Probe


async def test_tenant_compute_target_cannot_name_the_deployment_box(world):
    from cairn.core.operator.runtime import invoke_operator_inprocess

    binding = OperationBinding(operator="probe/op", input={}, output={},
                               execution=ExecutionField(compute_target="gpu"))
    with workspace_scope("ws-a"), pytest.raises(BindingEvalError, match="did not resolve"):
        await invoke_operator_inprocess(binding, _op({}), state={}, pipeline_id="p", node_name="n")

    seen: dict = {}
    with workspace_scope("default"):
        await invoke_operator_inprocess(binding, _op(seen), state={}, pipeline_id="p", node_name="n")
    assert seen["compute"].config["host"] == "deploy-host"


async def test_tenant_compute_target_resolves_its_own_box(world):
    from cairn.core.operator.runtime import invoke_operator_inprocess

    seen: dict = {}
    binding = OperationBinding(operator="probe/op", input={}, output={},
                               execution=ExecutionField(compute_target="a-box"))
    with workspace_scope("ws-a"):
        await invoke_operator_inprocess(binding, _op(seen), state={}, pipeline_id="p", node_name="n")
    assert seen["compute"].config["host"] == "a-host"


# ── declared secrets ──────────────────────────────────────────────────


async def test_tenant_injection_never_writes_the_process_env(world):
    before = _env_snapshot()
    with workspace_scope("ws-a"):
        assert await inject_declared_secrets(["KAGGLE_USERNAME", "KAGGLE_KEY", "HF_TOKEN"]) == []
    assert await inject_declared_secrets(["KAGGLE_KEY"], account_id="acct-b") == []
    assert _env_snapshot() == before


async def test_deployment_injection_is_unchanged(world, monkeypatch):
    injected = await inject_declared_secrets(["KAGGLE_USERNAME", "KAGGLE_KEY"])
    try:
        assert sorted(injected) == ["KAGGLE_KEY", "KAGGLE_USERNAME"]
        assert os.environ["KAGGLE_KEY"] == "dep-key"
    finally:
        for var in injected:
            monkeypatch.delenv(var, raising=False)


async def test_declared_secret_values_come_from_the_account_only(world):
    assert await declared_secret_values(["KAGGLE_KEY", "HF_TOKEN"], account_id="acct-a") == {
        "KAGGLE_KEY": "a-kaggle-key"}                     # catalog: kaggle.KEY → KAGGLE_KEY
    assert await declared_secret_values(["KAGGLE_KEY", "HF_TOKEN"], account_id="acct-b") == {
        "HF_TOKEN": "b-hf-token"}                         # the logical key is the target


def test_build_scoped_secrets_for_a_tenant_never_reads_the_bound_provider(world):
    scoped = build_scoped_secrets(["KAGGLE_KEY", "HF_TOKEN"], account_id="acct-a",
                                  values={"KAGGLE_KEY": "a-kaggle-key", "OTHER": "x"})
    assert scoped.get("KAGGLE_KEY") == "a-kaggle-key"
    with pytest.raises(KeyError):
        scoped.get("HF_TOKEN")          # declared, but this account supplies none
    with pytest.raises(KeyError):
        scoped.get("OTHER")             # supplied, but not declared
    assert world.provider.reads == []


def test_build_scoped_secrets_for_the_deployment_is_unchanged(world):
    scoped = build_scoped_secrets(["HF_TOKEN"])
    assert scoped.get("HF_TOKEN") == "dep-hf"
    assert build_scoped_secrets(["HF_TOKEN"], account_id="default").get("HF_TOKEN") == "dep-hf"


async def test_tenant_operator_sees_its_own_secret_and_not_the_deployments(world):
    from cairn.core.operator.runtime import invoke_operator_core

    seen: dict = {}
    before = _env_snapshot()
    with workspace_scope("ws-a"):
        await invoke_operator_core(_op(seen), {}, pipeline_id="p", node_name="n",
                                   secret_keys=["KAGGLE_KEY", "HF_TOKEN"])
    assert seen["key"] == "a-kaggle-key"
    assert "hf" not in seen and "HF_TOKEN" in seen["hf_error"]   # the deployment's is not reachable
    assert world.provider.reads == []
    assert _env_snapshot() == before


async def test_deployment_operator_secrets_are_unchanged(world, monkeypatch):
    from cairn.core.operator.runtime import invoke_operator_core

    seen: dict = {}
    try:
        await invoke_operator_core(_op(seen), {}, pipeline_id="p", node_name="n",
                                   secret_keys=["KAGGLE_KEY", "HF_TOKEN"])
        assert seen["key"] == "dep-key"
        assert seen["hf"] == "dep-hf"
    finally:
        monkeypatch.delenv("KAGGLE_KEY", raising=False)


# ── remote-job control worker ─────────────────────────────────────────


async def test_job_control_resolves_compute_in_the_jobs_workspace(world):
    from cairn.core.operator.job_control import build_default_poll
    from cairn.core.operator.job_store import RemoteJobRecord
    from cairn.core.operator.jobs import JobStatus

    seen: dict = {}

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def poll(self, handle, inp, ctx):
            seen.setdefault("compute", []).append(ctx.compute)
            seen.setdefault("workspace", []).append(current_workspace())
            return JobStatus(state="running")

    registry = SimpleNamespace(get=lambda name: StubOp)
    poll = build_default_poll(registry)
    for target in ("gpu", "a-box"):
        await poll(RemoteJobRecord(run_id="r", workspace_id="ws-a", node_name="n", provider="stub/gpu",
                                   job_id="j", compute_target=target))
    assert seen["compute"][0] is None                      # the deployment's box: not this tenant's
    assert seen["compute"][1].config["host"] == "a-host"
    assert seen["workspace"] == ["ws-a", "ws-a"]           # the job's workspace, not the worker's


# ── admission-side lookups ────────────────────────────────────────────


async def test_target_cap_is_read_in_the_run_account(world):
    from cairn.core.dispatch.target_concurrency import target_cap

    assert await target_cap("gpu") == 2
    with workspace_scope("ws-a"):
        assert await target_cap("gpu") is None


async def test_license_gate_token_check_is_per_account(world, monkeypatch):
    from cairn.core.model_license_gate import _has_hf_token

    monkeypatch.setenv("HF_TOKEN", "dep-hf")        # the deployment's ref resolves from its env
    assert await _has_hf_token() is True
    with workspace_scope("ws-a"):
        assert await _has_hf_token() is False        # the deployment's huggingface row is not a's


async def test_preflight_connection_warnings_are_per_account(world):
    from cairn.core.preflight import _connection_warnings

    assert any("gpu" in w for w in await _connection_warnings())
    with workspace_scope("ws-a"):
        assert await _connection_warnings() == []


# ── dispatch: legs run in their record's workspace ────────────────────


class _RunStore:
    """Just enough of the run store for a leg or a template run to terminalize."""

    def __init__(self, run: Any = None) -> None:
        self.run = run
        self.updates: list[tuple[str, dict]] = []

    async def create(self, run):
        return run

    async def update(self, run_id=None, **kw):
        self.updates.append((run_id, kw))

    async def append_event(self, *a, **kw):
        return None

    async def get_events(self, *a, **kw):
        return []

    async def get(self, run_id, *a, **kw):
        return self.run


def _bridge(run_store: _RunStore):
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    async def _emit(*a, **kw):
        return None

    return PipelineBridge(SimpleNamespace(store=run_store, _emit=_emit), store=InMemoryPipelineStore())


async def test_a_task_leg_advanced_from_a_default_context_resolves_in_its_records_account(world, monkeypatch):
    """A leg the bridge advances off a bus event runs in whatever context the
    subscriber has (the deployment's). It still resolves in its record's account."""
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    async def _op(self, op_name, resolved, rec, s):
        conn = await resolve_connection("a-box")
        dep = await resolve_connection("gpu")
        return {"host": conn.config["host"] if conn else "", "deployment_box": dep is not None}

    monkeypatch.setattr(PipelineBridge, "_invoke_task_op", _op)
    runs = _RunStore()
    rec = SimpleNamespace(workspace_id="ws-a", pipeline_id="p1")
    with workspace_scope("default"):
        await _bridge(runs)._run_task_leg(rec, SimpleNamespace(ref="task:x", id="s1", timeout_s=None),
                                          SimpleNamespace(id="r1"), {})
        assert current_workspace() == "default"
    (run_id, done), = runs.updates
    assert done["status"] == "done"
    assert done["result_payloads"] == [{"output": {"host": "a-host", "deployment_box": False}}]


async def test_a_flow_leg_runs_in_its_records_workspace(world, monkeypatch):
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    seen: dict = {}

    async def _settle(self, rec, s, run, resolved, *, adopt=None):
        seen["workspace"] = current_workspace()
        seen["target"] = await resolve_connection("gpu")

    monkeypatch.setattr(PipelineBridge, "_execute_and_settle_flow_leg", _settle)
    monkeypatch.setattr(PipelineBridge, "_enforce_flow_leg_gate", lambda self, rec, s: None)
    rec = SimpleNamespace(workspace_id="ws-a", pipeline_id="p1")
    with workspace_scope("default"):
        await _bridge(_RunStore())._run_flow_leg(rec, SimpleNamespace(ref="flow:x", id="s1"),
                                                 SimpleNamespace(id="r1", status="running"), {})
    assert seen == {"workspace": "ws-a", "target": None}   # the deployment's target is not a's


# ── dispatcher: template runs and resumes ─────────────────────────────


def _stub_template(monkeypatch, seen: dict, *, result: Any = None):
    import cairn.core.engine.compiler as C
    import cairn.core.engine.runner as R
    import cairn.core.engine.skill_engine as SE
    import cairn.core.operator as OP
    import cairn.core.pack.loader as PL
    from cairn.core.dispatch.dispatcher import RunDispatcher

    monkeypatch.setattr(SE, "resolve_template_path", lambda ref: "/nonexistent.yaml")
    monkeypatch.setattr(C, "compile_template", lambda path: object())
    monkeypatch.setattr(OP, "load_all_operators", lambda **kw: None)
    monkeypatch.setattr(PL, "load_all_packs", lambda **kw: None)

    async def _cfg(*a, **kw):
        return {}, None

    async def _contracts(self, inv):
        return {}

    monkeypatch.setattr(RunDispatcher, "_resolve_workflow_config", staticmethod(_cfg))
    monkeypatch.setattr(RunDispatcher, "_contract_invocations", _contracts)

    async def _run(compiled, inputs):
        seen["workspace"] = current_workspace()
        seen["box"] = await resolve_connection("a-box")
        return result if result is not None else {"output": "ok"}

    monkeypatch.setattr(R, "run_within_budget", _run)


async def test_a_template_run_executes_in_its_records_workspace(world, monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.storage.protocols import Run

    seen: dict = {}
    _stub_template(monkeypatch, seen)
    inv = Run(id="r-a", trigger=TriggerEvent(source="manual"), workspace_id="ws-a")
    disp = RunDispatcher(runner=object(), store=_RunStore())
    with workspace_scope("default"):
        await disp._run_template_task(inv, "demo/tmpl", {}, None)
    assert seen["workspace"] == "ws-a"
    assert seen["box"].config["host"] == "a-host"


async def test_an_in_process_resume_runs_in_its_records_workspace(world, monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.runtime.inprocess import InProcessRuntime

    seen: dict = {}

    async def _pending(self, graph, run_id):
        return True

    async def _resume(self, graph, decision, *, run_id):
        seen["workspace"] = current_workspace()
        seen["box"] = await resolve_connection("a-box")
        return {"__interrupt__": [SimpleNamespace(value=None)]}   # paused again: least follow-on work

    monkeypatch.setattr(InProcessRuntime, "has_pending_interrupt", _pending)
    monkeypatch.setattr(InProcessRuntime, "resume", _resume)
    monkeypatch.setattr(RunDispatcher, "_recompile_graph", staticmethod(lambda name: object()))
    run = SimpleNamespace(id="r-a", status="awaiting_approval", workspace_id="ws-a",
                          results=[SimpleNamespace(template_name="demo/tmpl")],
                          bundle_pipeline_id="", owner="")
    disp = RunDispatcher(runner=object(), store=_RunStore(run))
    with workspace_scope("default"):
        await disp.resume_run("r-a", {"approved": True})
    assert seen["workspace"] == "ws-a"
    assert seen["box"].config["host"] == "a-host"


# ── Temporal owner: the activity twins of the bridge ──────────────────


async def test_temporal_template_leg_runs_in_the_pipeline_records_workspace(world, monkeypatch):
    pytest.importorskip("temporalio")
    from temporalio.testing import ActivityEnvironment

    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord

    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(pipeline_id="p1", workspace_id="ws-a"))
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)
    seen: dict = {}

    async def _run_template(ref, inputs, **kw):
        seen["workspace"] = current_workspace()
        seen["box"] = await resolve_connection("a-box")
        return SimpleNamespace(id="run-1")

    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(run_template=_run_template))
    with workspace_scope("default"):
        assert await ActivityEnvironment().run(PA.pipeline_submit_step, "demo/tmpl", {}, "p1", "s1", None) == "run-1"
    assert seen["workspace"] == "ws-a"
    assert seen["box"].config["host"] == "a-host"


async def test_temporal_template_leg_without_a_record_is_refused(world, monkeypatch):
    pytest.importorskip("temporalio")
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    pstore = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)
    called: list = []

    async def _run_template(ref, inputs, **kw):
        called.append(ref)
        return SimpleNamespace(id="run-1")

    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(run_template=_run_template))
    with pytest.raises(ApplicationError):
        await ActivityEnvironment().run(PA.pipeline_submit_step, "demo/tmpl", {}, "p-missing", "s1", None)
    assert called == []


async def test_temporal_flow_leg_runs_in_the_pipeline_records_workspace(world, monkeypatch):
    pytest.importorskip("temporalio")
    from temporalio.testing import ActivityEnvironment

    import cairn.core.dispatch.leg_executor as LE
    import cairn.core.dispatch.leg_hooks as LH
    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord

    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(pipeline_id="p1", workspace_id="ws-a"))
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)

    async def _emit(*a, **kw):
        return None

    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(store=_RunStore(), _emit=_emit))

    async def _no_gate(*a, **kw):
        return None

    monkeypatch.setattr(PA, "_gate_flow_leg", _no_gate)
    monkeypatch.setattr(LE, "parse_flow_ref", lambda ref: None)          # kind "data", no def lookup
    monkeypatch.setattr("cairn.core.dataflow.engine_registry.resolve_engine", lambda *a, **kw: object())

    class _Hooks:
        def __init__(self, **kw):
            pass

        def execute_kwargs(self, **kw):
            return {}

        async def relay_checkpoints(self):
            return None

    monkeypatch.setattr(LH, "FlowLegHooks", _Hooks)
    seen: dict = {}

    async def _execute_leg(ref, **kw):
        seen["workspace"] = current_workspace()
        seen["target"] = await resolve_connection("gpu")
        seen["own"] = await resolve_connection("a-box")
        return LE.LegOutcome(state="done")

    monkeypatch.setattr(LE, "execute_leg", _execute_leg)
    with workspace_scope("default"):
        await ActivityEnvironment().run(PA.pipeline_submit_step, "flow:demo", {}, "p1", "s1", None, {})
    assert seen["workspace"] == "ws-a"
    assert seen["target"] is None                         # the deployment's target is not a's
    assert seen["own"].config["host"] == "a-host"


# ── templates contract and profile-bound backends ────────────────────


async def test_templates_contract_lists_only_the_accounts_mcp_connections(world, monkeypatch):
    from cairn.core.triggers.routers.templates import build_templates_router

    await world.store.create(Connection(name="dep-mcp", kind="mcp"))
    await world.store.create(Connection(name="a-mcp", kind="mcp", account_id="acct-a"))
    seen: dict = {}

    def _contract(ref, *, connected_names, profile):
        seen["connected"] = set(connected_names)
        return SimpleNamespace(plan={"template_found": True}, model_dump=lambda **kw: {})

    monkeypatch.setattr("cairn.core.ui_contract.build_ui_contract", _contract)
    route = next(r for r in build_templates_router().routes if r.path == "/packs/{pack}/{template}/contract")
    with workspace_scope("ws-a"):
        await route.endpoint(pack="demo", template="tmpl", profile=None)
    assert seen["connected"] == {"a-mcp"}
    with workspace_scope("default"):
        await route.endpoint(pack="demo", template="tmpl", profile=None)
    assert seen["connected"] == {"dep-mcp"}


async def test_profile_bound_backend_connection_is_always_the_deployments(world, tmp_path, monkeypatch):
    import cairn.core.backend_connections as bc

    for var in ("QDRANT_URL", "QDRANT_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    await world.store.create(Connection(name="prod-qdrant", kind="api_key",
                                        config={"url": "https://deploy-qdrant:6333"}))
    await world.store.create(Connection(name="a-qdrant", kind="api_key", account_id="acct-a",
                                        config={"url": "https://a-qdrant:6333"}))
    profile = tmp_path / "env.toml"
    monkeypatch.setenv("OBS_PROFILE", str(profile))
    bc.reset_applied()
    try:
        profile.write_text('[profile.backends]\nrag_vector_store = "qdrant"\n'
                           'rag_vector_store_connection = "prod-qdrant"\n', encoding="utf-8")
        with workspace_scope("ws-a"):                    # even inside a tenant's run
            assert await bc.apply_backend_connection("rag_vector_store") == "prod-qdrant"
        assert os.environ["QDRANT_URL"] == "https://deploy-qdrant:6333"

        bc.reset_applied()
        monkeypatch.delenv("QDRANT_URL", raising=False)
        profile.write_text('[profile.backends]\nrag_vector_store = "qdrant"\n'
                           'rag_vector_store_connection = "a-qdrant"\n', encoding="utf-8")
        with workspace_scope("ws-a"):
            assert await bc.apply_backend_connection("rag_vector_store") is None   # a tenant row never binds
        assert "QDRANT_URL" not in os.environ
    finally:
        bc.reset_applied()


# ── launches from inside a run ────────────────────────────────────────


async def test_a_launch_from_a_tenant_run_lands_in_the_runs_workspace(world, monkeypatch):
    import cairn.core.dispatch.pipeline_launch as PLN
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.operator.runtime import _LAUNCH
    from cairn.core.storage.protocols import Run

    launched: dict = {}

    async def _launch_pipeline(ref, **kw):
        launched["pipeline_ws"] = kw.get("workspace_id")
        return SimpleNamespace(pipeline_id="p-new")

    monkeypatch.setattr(PLN, "launch_pipeline", _launch_pipeline)
    seen: dict = {}
    _stub_template(monkeypatch, seen)

    import cairn.core.engine.runner as R

    async def _run(compiled, inputs):        # the operator calling ctx.launch mid-run
        handle = _LAUNCH.get()
        launched["pid"] = await handle("pipeline:demo/p", {}, {})
        return {"output": "ok"}

    monkeypatch.setattr(R, "run_within_budget", _run)
    inv = Run(id="r-a", trigger=TriggerEvent(source="manual"), workspace_id="ws-a")
    disp = RunDispatcher(runner=object(), store=_RunStore())
    with workspace_scope("default"):
        await disp._run_template_task(inv, "demo/tmpl", {}, None)
    assert launched == {"pipeline_ws": "ws-a", "pid": "p-new"}


async def test_a_template_launched_from_a_run_is_stamped_with_its_workspace_or_refused(world, monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher

    disp = RunDispatcher(runner=object(), store=_RunStore())
    stamped: list = []

    async def _run_template(ref, inputs, **kw):
        stamped.append(current_workspace())
        return SimpleNamespace(id="r-new")

    monkeypatch.setattr(disp, "run_template", _run_template)
    with workspace_scope("ws-a"):
        assert await disp.launch_handle()("demo/other", {}, {}) == "r-new"
    assert stamped == ["ws-a"]
    # with auth on and no workspace in scope the template is not started in the default
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await disp.launch_handle()("demo/other", {}, {})
    assert stamped == ["ws-a"]


async def test_auth_on_a_launch_with_no_workspace_in_scope_is_refused(world, monkeypatch):
    import cairn.core.dispatch.pipeline_launch as PLN
    from cairn.core.dispatch.dispatcher import RunDispatcher

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    called: list = []

    async def _launch_pipeline(ref, **kw):
        called.append(kw)
        return SimpleNamespace(pipeline_id="p-new")

    monkeypatch.setattr(PLN, "launch_pipeline", _launch_pipeline)
    disp = RunDispatcher(runner=object(), store=_RunStore())
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await disp.launch_handle()("pipeline:demo/p", {}, {})
    assert called == []


# ── auth on: a record with no workspace fails visibly ─────────────────


async def test_auth_on_a_step_whose_record_names_no_workspace_fails_its_pipeline(world, monkeypatch):
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.storage.pipeline_store import PipelineRecord

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    audited: list = []

    async def _audit(self, rec, step_id, event, detail):
        audited.append((step_id, event, detail))

    monkeypatch.setattr(PipelineBridge, "_audit", _audit)
    runs = _RunStore()
    bridge = _bridge(runs)
    rec = PipelineRecord(pipeline_id="p1", workspace_id="")
    await bridge._submit_step(rec, SimpleNamespace(id="s1", ref="task:x"), {})
    assert rec.status == "failed"
    assert rec.trigger["failure_reason"] == "record names no workspace"
    assert ("s1", "workspace_unresolved", {"error": "record names no workspace"}) in audited
    assert runs.updates == []                               # nothing was submitted


async def test_auth_on_a_leg_whose_record_names_no_workspace_is_marked_failed(world, monkeypatch):
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    ran: list = []

    async def _op(self, *a, **kw):
        ran.append(a)
        return {}

    monkeypatch.setattr(PipelineBridge, "_invoke_task_op", _op)
    emitted: list = []

    async def _emit(run_id, type, data, **kw):
        emitted.append((run_id, type, data))

    from cairn.core.dispatch.pipeline_bridge import PipelineBridge as PB
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    runs = _RunStore()
    bridge = PB(SimpleNamespace(store=runs, _emit=_emit), store=InMemoryPipelineStore())
    rec = SimpleNamespace(workspace_id="", pipeline_id="p1")
    s = SimpleNamespace(ref="task:x", id="s1", timeout_s=None)
    await bridge._run_task_leg(rec, s, SimpleNamespace(id="r1"), {})
    await bridge._run_flow_leg(rec, s, SimpleNamespace(id="r2", status="running"), {})
    assert ran == []
    assert [(r, u["status"], u["error"]) for r, u in runs.updates] == [
        ("r1", "failed", "record names no workspace"), ("r2", "failed", "record names no workspace")]
    assert [(r, t) for r, t, _ in emitted] == [("r1", "failed"), ("r2", "failed")]


async def test_auth_on_temporal_legs_whose_record_names_no_workspace_are_refused(world, monkeypatch):
    pytest.importorskip("temporalio")
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(pipeline_id="p1", workspace_id=""))
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)
    runs = _RunStore()
    created: list = []

    async def _create(run):
        created.append(run)
        return run

    runs.create = _create
    called: list = []

    async def _run_template(ref, inputs, **kw):
        called.append(ref)
        return SimpleNamespace(id="run-1")

    async def _emit(*a, **kw):
        return None

    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(run_template=_run_template, store=runs,
                                                                   _emit=_emit))
    for ref in ("demo/tmpl", "flow:demo"):
        with pytest.raises(ApplicationError) as e:
            await ActivityEnvironment().run(PA.pipeline_submit_step, ref, {}, "p1", "s1", None, {})
        assert e.value.non_retryable and "names no workspace" in str(e.value)
    assert called == [] and created == []


async def test_auth_on_job_control_refuses_a_job_with_no_workspace(world, monkeypatch):
    from cairn.core.operator.job_control import build_default_poll
    from cairn.core.operator.job_store import RemoteJobRecord

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    polled: list = []

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def poll(self, handle, inp, ctx):
            polled.append(ctx)

    poll = build_default_poll(SimpleNamespace(get=lambda name: StubOp))
    with pytest.raises(workspace.WorkspaceScopeRequired):
        await poll(RemoteJobRecord(run_id="r", workspace_id="", node_name="n", provider="stub/gpu",
                                   job_id="j", compute_target="gpu"))
    assert polled == []


# ── the deployment's CLI ──────────────────────────────────────────────


async def test_compute_status_cli_reads_the_deployments_box_with_auth_on(world, monkeypatch):
    from cairn.cli.commands import compute

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    assert workspace._active.get() is None                  # a CLI process: no workspace in scope
    row = await compute._status("gpu")
    assert row["host"] == "deploy-host"
    with pytest.raises(Exception, match="did not resolve"):
        await compute._status("a-box")                      # a tenant's box is not the deployment's


# ── auth on: a refused step stops its callers (real bridge paths) ─────


class _Recorder:
    def __init__(self, monkeypatch):
        from cairn.core.dispatch.pipeline_bridge import PipelineBridge

        self.audits: list = []
        self.notified: list = []

        async def _audit(bridge, rec, step_id, event, detail):
            self.audits.append((step_id, event))

        async def _notify(bridge, rec, reason):
            self.notified.append(reason)

        monkeypatch.setattr(PipelineBridge, "_audit", _audit)
        monkeypatch.setattr(PipelineBridge, "_notify_pipeline_failed", _notify)


def _real_bridge():
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore

    calls: list = []

    async def _run_template(ref, inputs, **kw):
        calls.append(ref)
        return SimpleNamespace(id=f"run-{len(calls)}")

    async def _emit(*a, **kw):
        return None

    disp = SimpleNamespace(store=_RunStore(), _emit=_emit, run_template=_run_template)
    return PipelineBridge(disp, store=InMemoryPipelineStore()), calls


def _plan(*steps):
    from cairn.core.engine.bundle_pipeline import PipelinePlan

    return PipelinePlan(name="ws-less", steps=list(steps))


async def test_auth_on_a_pipeline_whose_record_names_no_workspace_ends_failed_once(world, monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlanStep

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    rec_ = _Recorder(monkeypatch)
    bridge, calls = _real_bridge()
    rec = await bridge.start_pipeline(_plan(PipelinePlanStep(id="work", ref="demo/work"),
                                            PipelinePlanStep(id="next", ref="demo/next", depends_on=["work"])),
                                      inputs={}, workspace_id="")
    stored = await bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert stored.step_state["work"].status == "failed"
    assert stored.step_state["work"].verdict == "workspace_unresolved"
    assert rec_.notified == ["record names no workspace"]
    assert calls == []


async def test_auth_on_a_refused_pipeline_with_a_finalizer_does_not_recurse(world, monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlanStep

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    rec_ = _Recorder(monkeypatch)
    bridge, calls = _real_bridge()
    rec = await bridge.start_pipeline(_plan(PipelinePlanStep(id="work", ref="demo/work"),
                                            PipelinePlanStep(id="release", ref="demo/release", finalizer=True)),
                                      inputs={}, workspace_id="")
    stored = await bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert stored.step_state["release"].status == "failed"            # refused once, not started
    assert ("release", "finalizer_started") not in rec_.audits
    assert len(rec_.notified) == 1
    assert calls == []


async def test_auth_on_advance_step_on_a_record_with_no_workspace_fails_without_approval(world, monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlanStep

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    rec_ = _Recorder(monkeypatch)
    bridge, calls = _real_bridge()
    rec = await bridge.start_pipeline(_plan(PipelinePlanStep(id="work", ref="demo/work", trigger="manual")),
                                      inputs={}, workspace_id="")
    assert (await bridge.store.get(rec.pipeline_id)).status != "failed"   # nothing submitted yet
    out = await bridge.advance_step(rec.pipeline_id, "work", approver="alice")
    assert out.status == "failed"
    assert (await bridge.store.get(rec.pipeline_id)).status == "failed"
    assert not any(e in ("approved", "advanced") for _, e in rec_.audits)
    assert len(rec_.notified) == 1 and calls == []


async def test_auth_on_held_back_steps_on_a_record_with_no_workspace_end_failed(world, monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlanStep
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    rec_ = _Recorder(monkeypatch)
    bridge, calls = _real_bridge()
    rec = await bridge.store.put(PipelineRecord(
        pipeline_name="ws-less", status="running", workspace_id="", inputs={},
        plan=_plan(PipelinePlanStep(id="work", ref="demo/work")),
        step_state={"work": StepState(status="pending")}))
    await bridge._advance_held_back(rec.pipeline_id, {"work"})
    stored = await bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert len(rec_.notified) == 1 and calls == []


# ── auth on: a refused re-adopted leg cancels its recorded engine job ─────


class _StubEngine:
    name = "stub"

    def __init__(self):
        self.cancelled: list[str] = []
        self.polled: list[str] = []

    async def cancel(self, handle):
        self.cancelled.append(handle.job_id)

    async def poll(self, handle):
        self.polled.append(handle.job_id)
        raise AssertionError("a refused leg is never watched")


async def test_auth_on_a_readopted_flow_leg_refused_for_its_workspace_cancels_its_job(world, monkeypatch):
    """Re-adoption at boot (`_readopt_flow_leg`) of a leg whose record names no
    workspace: the run fails, and the engine job the previous owner recorded is
    cancelled instead of running on unwatched."""
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.storage.pipeline_store import StepState

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    engine = _StubEngine()
    monkeypatch.setattr(PipelineBridge, "_leg_engine", lambda self, s: engine)
    runs = _RunStore(SimpleNamespace(id="r1", status="running", inputs={}))
    bridge = _bridge(runs)
    rec = SimpleNamespace(workspace_id="", pipeline_id="p1")
    s = SimpleNamespace(ref="flow:demo", id="s1", timeout_s=None)
    await bridge._readopt_flow_leg(rec, s, StepState(status="running", run_id="r1",
                                                     handle={"job_id": "job-7", "engine": "stub"}))
    await asyncio.gather(*list(bridge._leg_tasks.values()))
    assert engine.cancelled == ["job-7"] and engine.polled == []
    assert [(r, u["status"]) for r, u in runs.updates if "status" in u] == [("r1", "failed")]


async def test_auth_on_a_fresh_flow_leg_refusal_cancels_nothing(world, monkeypatch):
    """No recorded handle (the leg never reached the engine): nothing to cancel."""
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    engine = _StubEngine()
    monkeypatch.setattr(PipelineBridge, "_leg_engine", lambda self, s: engine)
    runs = _RunStore()
    await _bridge(runs)._run_flow_leg(SimpleNamespace(workspace_id="", pipeline_id="p1"),
                                      SimpleNamespace(ref="flow:demo", id="s1"),
                                      SimpleNamespace(id="r2", status="running"), {})
    assert engine.cancelled == []
    assert [(r, u["status"]) for r, u in runs.updates] == [("r2", "failed")]


async def test_auth_on_a_readopted_flow_leg_in_its_workspace_is_not_cancelled(world, monkeypatch):
    """The control: a record that names its workspace is adopted and watched."""
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    engine = _StubEngine()
    monkeypatch.setattr(PipelineBridge, "_leg_engine", lambda self, s: engine)
    seen: dict = {}

    async def _settle(self, rec, s, run, resolved, *, adopt=None):
        seen["adopt"] = adopt

    monkeypatch.setattr(PipelineBridge, "_execute_and_settle_flow_leg", _settle)
    await _bridge(_RunStore())._run_flow_leg(SimpleNamespace(workspace_id="ws-a", pipeline_id="p1"),
                                             SimpleNamespace(ref="flow:demo", id="s1"),
                                             SimpleNamespace(id="r3", status="running"), {},
                                             adopt={"job_id": "job-8"})
    assert engine.cancelled == [] and seen["adopt"] == {"job_id": "job-8"}


async def test_auth_on_temporal_flow_leg_refused_for_its_workspace_cancels_its_job(world, monkeypatch):
    pytest.importorskip("temporalio")
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    import cairn.core.runtime.temporal.pipeline_activities as PA
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(pipeline_id="p1", workspace_id="", step_state={
        "s1": StepState(status="running", run_id="r1", handle={"job_id": "job-9", "engine": "stub"})}))
    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: pstore)
    engine = _StubEngine()
    kinds: list = []

    def _resolve(name, *, kind="data"):
        kinds.append(kind)
        return engine

    monkeypatch.setattr("cairn.core.dataflow.engine_registry.resolve_engine", _resolve)
    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(store=_RunStore(), _emit=None))
    with pytest.raises(ApplicationError) as e:
        await ActivityEnvironment().run(PA.pipeline_submit_step, "flow:demo", {}, "p1", "s1", None, {})
    assert e.value.non_retryable and "names no workspace" in str(e.value)
    assert engine.cancelled == ["job-9"] and engine.polled == []


async def test_auth_on_job_control_cancels_a_refused_job(world, monkeypatch):
    """The remote-job control worker: a job whose record names no workspace is
    refused on poll and on relaunch, and the job is cancelled each time."""
    from cairn.core.operator.job_control import build_default_poll, build_default_relaunch
    from cairn.core.operator.job_store import RemoteJobRecord

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    seen: dict = {"cancelled": [], "polled": [], "submitted": []}

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def poll(self, handle, inp, ctx):
            seen["polled"].append(handle.job_id)

        async def submit(self, inp, ctx):
            seen["submitted"].append(inp)

        async def cancel(self, handle):
            seen["cancelled"].append(handle.job_id)

    reg = SimpleNamespace(get=lambda name: StubOp)
    job = RemoteJobRecord(run_id="r", workspace_id="", node_name="n", provider="stub/gpu",
                          job_id="j-1", compute_target="gpu")
    for fn in (build_default_poll(reg), build_default_relaunch(reg)):
        with pytest.raises(workspace.WorkspaceScopeRequired):
            await fn(job)
    assert seen == {"cancelled": ["j-1", "j-1"], "polled": [], "submitted": []}
    # the control: a job in its workspace is polled, never cancelled
    seen["cancelled"].clear()
    await build_default_poll(reg)(job.model_copy(update={"workspace_id": "ws-a"}))
    assert seen["polled"] == ["j-1"] and seen["cancelled"] == []
