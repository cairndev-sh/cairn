"""#121 — compute connections → remote-job executor seam.

A binding's ``execution.compute_target`` is resolved at dispatch and handed to a
remote-job operator via ``ctx.compute`` (config + secrets), so the operator can
submit to the chosen backend (RunPod, Vast.ai, …). resolve_connection is mocked
— no real connection store / provider needed.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")
os.environ.setdefault("OBS_CONNECTIONS_DB", ":memory:")

import pytest
from pydantic import BaseModel

from cairn.core.connections import ResolvedConnection
from cairn.core.operator.binding import ExecutionField, OperationBinding
from cairn.core.operator.jobs import JobHandle, JobStatus
from cairn.core.operator.runtime import invoke_operator_job


class _Inp(BaseModel):
    x: int = 1


class _Out(BaseModel):
    gpu: str = ""
    key: str = ""


def _stub_op(captured: dict):
    class StubGpuOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out
        supports_mock = False

        async def submit(self, inp, ctx):
            captured["compute"] = ctx.compute  # what the operator actually sees
            return JobHandle(job_id="j1", backend="remote_job")

        async def poll(self, handle, inp, ctx):
            c = ctx.compute
            return JobStatus(
                state="succeeded",
                output=_Out(gpu=(c.config.get("gpu", "") if c else ""), key=(c.secrets.get("API_KEY", "") if c else "")),
            )

        async def cancel(self, handle):  # pragma: no cover
            pass

    return StubGpuOp


def _patch_resolve(monkeypatch, seen: dict, *, returns="runpod"):
    async def fake_resolve(name, *, store=None, account_id=None):
        seen["name"] = name
        if returns is None:
            return None
        return ResolvedConnection(name=name, kind="compute.runpod", config={"gpu": "A100"}, secrets={"API_KEY": "rp-secret"})

    monkeypatch.setattr("cairn.core.connections.resolve_connection", fake_resolve)


async def test_compute_target_literal_name_threaded_to_ctx(monkeypatch):
    captured, seen = {}, {}
    _patch_resolve(monkeypatch, seen)
    binding = OperationBinding(
        operator="stub/gpu", input={}, output={}, execution=ExecutionField(compute_target="my-runpod")
    )
    out = await invoke_operator_job(
        binding, _stub_op(captured), driver_kind="remote_job",
        state={}, pipeline_id="p", node_name="n", poll_interval=0,
    )
    assert seen["name"] == "my-runpod"               # resolved the named connection
    assert captured["compute"].config["gpu"] == "A100"   # operator received config
    assert captured["compute"].secrets["API_KEY"] == "rp-secret"  # ...and secrets
    assert out == {}  # no output binding mapped


async def test_compute_target_binding_expression_resolves_from_input(monkeypatch):
    captured, seen = {}, {}
    _patch_resolve(monkeypatch, seen)
    binding = OperationBinding(
        operator="stub/gpu", input={}, output={}, execution=ExecutionField(compute_target="$input.gpu_provider")
    )
    await invoke_operator_job(
        binding, _stub_op(captured), driver_kind="remote_job",
        state={}, template_input={"gpu_provider": "my-vast"}, pipeline_id="p", node_name="n", poll_interval=0,
    )
    assert seen["name"] == "my-vast"  # $input.gpu_provider → connection name


async def test_unresolved_compute_target_raises(monkeypatch):
    captured, seen = {}, {}
    _patch_resolve(monkeypatch, seen, returns=None)  # connection not found/disabled
    binding = OperationBinding(
        operator="stub/gpu", input={}, output={}, execution=ExecutionField(compute_target="ghost")
    )
    with pytest.raises(Exception, match="did not resolve"):
        await invoke_operator_job(
            binding, _stub_op(captured), driver_kind="remote_job",
            state={}, pipeline_id="p", node_name="n", poll_interval=0,
        )


async def test_no_compute_target_leaves_ctx_compute_none(monkeypatch):
    captured, seen = {}, {}
    _patch_resolve(monkeypatch, seen)  # patched, but should NOT be called
    binding = OperationBinding(operator="stub/gpu", input={}, output={})  # no execution.compute_target
    await invoke_operator_job(
        binding, _stub_op(captured), driver_kind="remote_job",
        state={}, pipeline_id="p", node_name="n", poll_interval=0,
    )
    assert captured["compute"] is None
    assert "name" not in seen  # resolve_connection never called
