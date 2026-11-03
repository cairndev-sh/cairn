"""ml/run — the ONE door to the ML plane (ADR-0082 D7, #1074).

Kind-symmetric with ``dataflow/run`` and just as thin: build the engine-neutral
spec, resolve the KIND-MATCHED flow ref control-side (``kind: ml`` only — a
data flow through this door is a compile/runtime error), hand the stage list to
the environment's ML engine (``resolve_engine(ctx, kind="ml")`` — the operator
NEVER constructs an engine, and nothing here names one: builtin today, a
Metaflow/Flyte/Dagster bridge tomorrow, zero changes at this door), and on
success register the returned artifacts control-side (D3 — engines get no
database credentials).

Governance attaches HERE (the chokepoint): ``external_write`` risk +
compile-time structural gating on the flow's stage risk classes; the flow only
ever PACKAGES a candidate URI — registration/promotion stay cairn's gated runs
(D4, #1052).
"""
from __future__ import annotations

from cairn.core.dataflow import EngineJobHandle
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator
from cairn.core.operator.jobs import JobHandle, JobStatus

from operator_dataflow.engines import resolve_engine
from operator_dataflow.operators.run import _artifact_store, recheck_descriptors, stamp_for_submit
from operator_dataflow.schemas.run_ml import MlRunInput, MlRunOutput


@register_operator(
    name="ml/run",
    version="0.1.0",
    capabilities=["ml.run"],
    implements=["contract://ml.run/1.0.0"],  # #2243 V2 — cairn-contracts-dataflow
)
class MlRunOp(Operation[MlRunInput, MlRunOutput]):
    """Submit a kind: ml flow to the environment's ML engine; poll for the
    packaged candidate's artifacts + scalar stats."""

    input_schema = MlRunInput
    output_schema = MlRunOutput
    # #1539 — governance declarations; the [operator.*] table is generated from these
    risk = 'external_write'
    executors = ['remote_job']
    supports_mock = True
    supports_cancel = True
    long_running = True
    # ADR-0103 (#2035) — same reasoning as dataflow/run: progress already
    # flows through JobStatus.metrics on the poll path, and the resolved
    # flow's stage names are per-pack, not a static vocabulary.
    reports = ReportSpec.NONE

    async def submit(self, inp: MlRunInput, ctx: OperationContext) -> JobHandle:
        resolved = None
        if inp.flow_ref and not inp.stages:
            # kind-matched resolution (ADR-0082 D7): control-side, so every
            # engine receives concrete stages — engines never read packs.
            from cairn.core.dataflow.defs import resolve_flow_ref  # noqa: PLC0415 - breaking a genuine import cycle (defs → pack registry → operators)
            resolved = resolve_flow_ref(inp.flow_ref, inp.config, kind="ml")
        spec = inp.to_spec(stages=resolved)
        if not spec.parent_run and getattr(ctx, "pipeline_id", None):
            spec = spec.model_copy(update={"parent_run": str(ctx.pipeline_id)})
        engine = resolve_engine(ctx, kind="ml")
        spec, carried = stamp_for_submit(spec, ctx, engine)   # ADR-0118 — the same door as dataflow/run
        eh = await engine.submit(spec, ctx)
        return JobHandle(
            job_id=eh.job_id, backend="mlflow-run",
            # run_id STASHED at submit (#1050 slice 3 pattern) — the handle is
            # the durable carrier of run identity through poll/cancel.
            meta={"engine": engine.name, "engine_handle": eh.model_dump(),
                  "run_id": str(getattr(ctx, "pipeline_id", "") or ""), **carried},
        )

    @staticmethod
    def _engine_handle(handle: JobHandle) -> EngineJobHandle:
        return EngineJobHandle.model_validate(handle.meta["engine_handle"])

    async def poll(self, handle: JobHandle, inp: MlRunInput, ctx: OperationContext) -> JobStatus:
        engine = resolve_engine(ctx, kind="ml")
        status = await engine.poll(self._engine_handle(handle))
        if status.state in ("queued", "running"):
            return JobStatus(state="running", metrics=status.metrics)
        if status.state == "cancelled":  # ADR-0151 D9 — terminal, never retriable
            return JobStatus(state="cancelled", error=status.error or "ml flow cancelled",
                             metrics=status.metrics)
        if status.state != "succeeded":
            return JobStatus(state="failed", error=status.error or f"ml flow {status.state}",
                             metrics=status.metrics, retriable=status.retriable)
        result = await engine.result(self._engine_handle(handle))
        recheck_descriptors(handle, list(result.output_artifacts))
        # control-plane registration (D3): the engine only DESCRIBED outputs.
        store = _artifact_store()
        ids: list[str] = []
        run_id = str(handle.meta.get("run_id") or getattr(ctx, "pipeline_id", "") or "")
        for descriptor in result.output_artifacts:
            ids.append(await store.register(
                descriptor, producer_run_id=run_id, producer_job=handle.job_id,
            ))
        out = MlRunOutput(
            stats=result.stats, artifact_ids=ids,
            artifact_uris=[d.uri for d in result.output_artifacts],
            engine=engine.name, cost_usd=result.cost_usd,
            warnings=result.warnings, summary=result.summary,
        )
        return JobStatus(state="succeeded", output=out.model_dump(), metrics=status.metrics)

    async def cancel(self, handle: JobHandle) -> None:
        # cancel has no ctx (control-worker path, #127) — the seam resolves the
        # environment's ML engine.
        engine = resolve_engine(None, kind="ml")
        await engine.cancel(self._engine_handle(handle))

    def mock(self, inp: MlRunInput, ctx: OperationContext) -> MlRunOutput:
        return MlRunOutput(
            stats={"stages_total": len(inp.stages), "mock": True},
            engine="mock", summary="mock ml flow (nothing executed)",
            warnings=["mock run — no training was performed"],
        )
