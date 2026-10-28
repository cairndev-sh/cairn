"""The DataflowEngine protocol (ADR-0081 D3, #1046).

The engine-agnostic boundary every execution backend conforms to — builtin
(one box / SSH box), the Metaflow bridge adapter, SkyPilot. Same
pattern as :class:`cairn.core.operator.job_channel.RemoteJobChannel` and the
storage backends: core defines the protocol; implementations live in the
``cairn-operator-dataflow`` wheel (never in core — ADR-0081 R3).

Prior-art shape: Flyte Agents' async create/get/delete for external engines +
Dagster Pipes' structured-events-back channel (ADR-0081 §3 prior art).

Hard rules:
- the protocol speaks manifests + scalars, never rows;
- ``validate`` is CAPABILITY DISCOVERY — it reports what THIS engine cannot
  run (unknown stage, unsupported resources), never shrinking the spec to the
  intersection of all engines (Beam's LCD mistake, inverted);
- ``poll`` streams progress metrics (stage, rows done, spend) — the input to
  the console and to mid-run budget/quality gates via the existing control
  loop (#552 machinery);
- the conformance suite (:mod:`cairn.core.dataflow.conformance`) is the
  executable definition of this protocol — an engine IS whatever passes it.
"""
from __future__ import annotations

from typing import Any, Literal, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cairn.core.dataflow.contract import DataflowResult, DataflowSpec, _assert_scalar_payload

EngineState = Literal["queued", "running", "succeeded", "failed", "cancelled"]

TERMINAL_STATES: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})


class EngineError(RuntimeError):
    """An engine-side failure the operator should surface loudly (a dataflow
    that could not run is a REAL failure — the #568 fail-loud discipline)."""


class EngineJobHandle(BaseModel):
    """Opaque reference to a submitted dataflow job. ``meta`` carries whatever
    the engine needs to poll/cancel later (run id, workdir, host) — the control
    plane persists it but never interprets it."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(..., min_length=1)
    engine: str = Field(..., min_length=1, description="Name of the engine that issued the handle.")
    meta: dict[str, Any] = Field(default_factory=dict)


class EngineJobStatus(BaseModel):
    """One poll observation. ``metrics`` is the scalar progress stream
    (stage, rows_done, spend_usd, …) — scalar-enforced so a status can never
    smuggle rows into the control plane."""

    model_config = ConfigDict(extra="forbid")

    state: EngineState = "running"
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: str = ""
    retriable: bool = Field(
        False,
        description="True for a RECOVERABLE death (machine reclaimed mid-run) — the "
        "control loop may relaunch (#552); a clean non-zero failure is not retriable.",
    )

    @field_validator("metrics")
    @classmethod
    def _metrics_scalar(cls, v: dict) -> dict:
        return _assert_scalar_payload(v, where="EngineJobStatus.metrics")


@runtime_checkable
class DataflowEngine(Protocol):
    """What every execution backend implements. Async because submit/poll are
    IO against remote systems; a local engine just doesn't await anything.

    ADR-0118 D4/D7 — an engine that runs kernels through ``run_dataflow`` (and so
    carries stage contracts to the interpreter) DECLARES ``carries_stage_contracts =
    True``. The attribute is optional and deliberately not a protocol member (a
    runtime-checkable member would make every existing engine non-conforming): read it
    through :func:`carries_stage_contracts`, which defaults to ``False``.

    ADR-0134 D3c — an engine whose launch depends on more than being installed (a
    provisioning engine: a target that resolves, an enabled cloud) offers
    ``async def check_ready(self) -> list[str]``: reasons this environment cannot launch,
    empty when it can. It is optional and, for the same reason, not a protocol member;
    :mod:`cairn.core.dataflow.engine_door` asks it before any record exists, and an
    engine without it is ready once installed. It must not install or provision.

    ADR-0149 D3 — an engine whose job runs on another machine, which cannot read a
    control-plane path, DECLARES ``needs_durable_inputs = True``; the leg executor then
    resolves the leg's ``dataset:`` refs to their durable copies. Optional and not a
    protocol member for the same reason: read it through :func:`needs_durable_inputs`,
    which defaults to ``False`` (co-located)."""

    name: str

    def validate(self, spec: DataflowSpec) -> list[str]:
        """Capability discovery: human-readable reasons this ENGINE cannot run
        this spec (unknown stage, unsupported resource hint). Empty ⇒ runnable.
        Doctor/preflight surfaces these before any submission."""
        ...

    async def submit(self, spec: DataflowSpec, ctx: Any) -> EngineJobHandle:
        """Dispatch the dataflow; return immediately with a pollable handle.
        ``ctx`` carries resolved environment handles (e.g. the compute
        connection) — engines never read ambient credentials."""
        ...

    async def poll(self, handle: EngineJobHandle) -> EngineJobStatus:
        ...

    async def cancel(self, handle: EngineJobHandle) -> None:
        """Best-effort abort that KILLS THE COMPUTE (an orphaned job is a cost
        leak — the #603 lesson). Must be safe to call on a terminal job."""
        ...

    async def result(self, handle: EngineJobHandle) -> DataflowResult:
        """Terminal payload of a SUCCEEDED job. Raises :class:`EngineError`
        if the job is not terminal-successful."""
        ...


def carries_stage_contracts(engine: Any) -> bool:
    """ADR-0118 D7 — True only for an engine that positively declares it carries stage
    contracts; the control plane refuses an opted-in spec on any other engine."""
    return getattr(engine, "carries_stage_contracts", False) is True


def needs_durable_inputs(engine: Any) -> bool:
    """ADR-0149 D3 — True only for an engine that positively declares its job runs
    where a control-plane path cannot be read."""
    return getattr(engine, "needs_durable_inputs", False) is True


def is_terminal(status: EngineJobStatus | str) -> bool:
    state = status if isinstance(status, str) else status.state
    return state in TERMINAL_STATES
