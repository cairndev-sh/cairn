"""dataflow/run — the ONE door to the data plane (ADR-0081 D1, #1048).

A remote-job operator (submit → poll → cancel, the train/runpod shape): builds
a ``DataflowSpec`` from its input, hands it to the environment's engine (via
:func:`operator_dataflow.engines.resolve_engine` — the operator NEVER
constructs an engine), streams the engine's scalar metrics up the existing
poll path, and on success REGISTERS the returned artifacts in the registry
(control-plane side — engines get no database credentials, D3) before
returning scalar stats into findings.

``external_write`` risk → a governed environment gates the submission with one
HITL approval for the whole dataflow (the fix-batch model).
"""
from __future__ import annotations

from typing import Optional

from cairn.core.dataflow import DataflowSpec, EngineJobHandle
from cairn.core.dataflow.conformance_check import (
    StageContractError,
    descriptor_problems,
    stamp,
    submission_problems,
)
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator
from cairn.core.operator.jobs import JobHandle, JobStatus
from cairn.core.operator import ArtifactRegistry, build_artifact_store

from operator_dataflow.engines import resolve_engine
from operator_dataflow.schemas.run import DataflowRunInput, DataflowRunOutput

_artifact_store_instance: Optional[ArtifactRegistry] = None


def _artifact_store() -> ArtifactRegistry:
    global _artifact_store_instance
    if _artifact_store_instance is None:
        _artifact_store_instance = build_artifact_store()
    return _artifact_store_instance


def _reset_artifact_store(store: Optional[ArtifactRegistry] = None) -> None:
    """Test seam."""
    global _artifact_store_instance
    _artifact_store_instance = store


def stamp_for_submit(spec: DataflowSpec, ctx: OperationContext, engine: object) -> tuple[DataflowSpec, dict]:
    """ADR-0118 D4/D5 at the flow doors (dataflow/run, ml/run): stamp the node's admitted
    stage contracts (``ctx.dataflow_contracts``) onto the spec, then refuse — before the
    engine sees anything — an undeclared engine, an oversized spec or a configuration
    violation. Inside an admitted leg an opted-in stage without its entry refuses (M2a).
    Returns the spec and the handle-meta that keeps the contracts for poll and relaunch."""
    from cairn.core.dataflow.defs import stage_implements  # noqa: PLC0415 - breaking a genuine import cycle (defs → pack registry → operators)

    carried = getattr(ctx, "dataflow_contracts", None)
    spec = stamp(spec, carried, stage_implements)
    refused = submission_problems(spec, engine)
    if refused:
        raise StageContractError("; ".join(refused))
    return spec, ({"dataflow_contracts": carried, "stages_total": len(spec.stages)} if spec.contracts else {})


def recheck_descriptors(handle: JobHandle, descriptors: list) -> None:
    """ADR-0118 D5 — re-check each opted-in stage's descriptor stats before registration."""
    problems = descriptor_problems(handle.meta.get("dataflow_contracts"),
                                   int(handle.meta.get("stages_total") or 0), descriptors)
    if problems:
        raise StageContractError("; ".join(problems))


@register_operator(
    name="dataflow/run",
    version="0.1.0",
    capabilities=["dataflow.run"],
    implements=["contract://dataflow.run/1.0.0"],  # #2242 V1-B — cairn-contracts-dataflow
)
class DataflowRunOp(Operation[DataflowRunInput, DataflowRunOutput]):
    """Submit a dataflow to the environment's engine; poll for artifacts + stats."""

    input_schema = DataflowRunInput
    output_schema = DataflowRunOutput
    # #1539 — governance declarations; the [operator.*] table is generated from these
    risk = 'external_write'
    executors = ['remote_job']
    supports_mock = True
    supports_cancel = True
    long_running = True
    # ADR-0103 (#2035) — a remote_job operator's progress already flows
    # through the poll path's JobStatus.metrics (stage_events.py is the
    # producer that turns those into progress events), and the stage/metric
    # NAMES here are the resolved dataflow's own stages — arbitrary per
    # pack, not a vocabulary this class can declare statically.
    reports = ReportSpec.NONE

    async def submit(self, inp: DataflowRunInput, ctx: OperationContext) -> JobHandle:
        spec = inp.to_spec()
        if spec.dataflow_ref and not spec.stages:
            # first-class pack dataflow (ADR-0081 D2, #1051): resolve the ref
            # CONTROL-side, so every engine receives concrete stages — engines
            # never read packs.
            from cairn.core.dataflow.defs import resolve_dataflow_ref  # noqa: PLC0415 - breaking a genuine import cycle (defs → pack registry → operators)
            spec = spec.model_copy(
                update={"stages": resolve_dataflow_ref(spec.dataflow_ref, spec.config)}
            )
        if not spec.parent_run and getattr(ctx, "pipeline_id", None):
            # default lineage: the submitting run is the parent unless --from set one
            spec = spec.model_copy(update={"parent_run": str(ctx.pipeline_id)})
        engine = resolve_engine(ctx)
        spec, carried = stamp_for_submit(spec, ctx, engine)
        eh = await engine.submit(spec, ctx)
        return JobHandle(
            job_id=eh.job_id, backend="dataflow",
            # run_id STASHED at submit (#1050 slice 3): the poll path's ctx is
            # not guaranteed to carry pipeline_id (the CLI in-process driver lost
            # it — seen live as EMPTY producer_run_id on registered artifacts),
            # while the submit ctx provably has it. The handle is the durable
            # carrier of run identity, same as remote/exec's workdir keying.
            meta={"engine": engine.name, "engine_handle": eh.model_dump(),
                  "run_id": str(getattr(ctx, "pipeline_id", "") or ""), **carried},
        )

    @staticmethod
    def _engine_handle(handle: JobHandle) -> EngineJobHandle:
        return EngineJobHandle.model_validate(handle.meta["engine_handle"])

    async def poll(self, handle: JobHandle, inp: DataflowRunInput, ctx: OperationContext) -> JobStatus:
        engine = resolve_engine(ctx)
        status = await engine.poll(self._engine_handle(handle))
        if status.state in ("queued", "running"):
            return JobStatus(state="running", metrics=status.metrics)
        if status.state == "cancelled":  # ADR-0151 D9 — terminal, never retriable
            return JobStatus(state="cancelled", error=status.error or "dataflow cancelled",
                             metrics=status.metrics)
        if status.state != "succeeded":
            return JobStatus(state="failed", error=status.error or f"dataflow {status.state}",
                             metrics=status.metrics, retriable=status.retriable)
        result = await engine.result(self._engine_handle(handle))
        recheck_descriptors(handle, list(result.output_artifacts))
        # control-plane registration (D3): the engine only DESCRIBED its outputs.
        store = _artifact_store()
        ids: list[str] = []
        run_id = str(handle.meta.get("run_id") or getattr(ctx, "pipeline_id", "") or "")
        for descriptor in result.output_artifacts:
            ids.append(await store.register(
                descriptor, producer_run_id=run_id, producer_job=handle.job_id,
            ))
        out = DataflowRunOutput(
            stats=result.stats, artifact_ids=ids,
            artifact_uris=[d.uri for d in result.output_artifacts],
            engine=engine.name, cost_usd=result.cost_usd,
            warnings=result.warnings, summary=result.summary,
        )
        return JobStatus(state="succeeded", output=out.model_dump(), metrics=status.metrics)

    async def cancel(self, handle: JobHandle) -> None:
        # cancel has no ctx (control-worker path, #127) — the seam resolves the
        # default engine; the P1c backend keeps handle.meta["engine"] authoritative.
        engine = resolve_engine(None)
        await engine.cancel(self._engine_handle(handle))

    def mock(self, inp: DataflowRunInput, ctx: OperationContext) -> DataflowRunOutput:
        return DataflowRunOutput(
            stats={"stages_total": len(inp.stages), "mock": True},
            engine="mock", summary="mock dataflow (nothing executed)",
            warnings=["mock run — no data was processed"],
        )
