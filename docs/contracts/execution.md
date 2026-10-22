# Execution and dataflow-engine contract

## What describes what
- A **pipeline** orchestrates legs (ADR-0084 D4); a **flow** (`kind: data | ml`,
  `flows/*.yaml`, `src/cairn/core/dataflow/defs.py:69`) is a stage list a **kernel registry**
  interprets; an **agentic** flow (markdown) compiles to a LangGraph graph run by a
  **runtime** (`src/cairn/core/runtime/`: in-process, DBOS, Temporal; selected by
  `CAIRN_RUNTIME` / profile `durable_runtime`, `src/cairn/core/profile_backends.py:88-90`).
- A flow leg is lowered to a **`DataflowSpec`** (`src/cairn/core/dataflow/contract.py:228`):
  `stages[]` (`StageRef {stage, config, resources, checkpoint, gate, map, contract}`, `:133`),
  `input_artifacts`, `config`, `budget {max_cost_usd, max_rows, max_seconds}`,
  `resume_from`, `checkpoint_key`, `parent_run`, `contracts`. The spec is **engine-neutral**: nothing
  in it names an engine (ADR-0081 D4; `engine` is an optional hint, never pack data).
- **Stage contracts (ADR-0118).** An opted-in stage carries `StageRef.contract`
  (`StageContract {capability_ref, version, configuration, output}`); the spec carries the
  admission stamp `contracts` (`SpecContracts {format: 1, leg, schemas}`: `leg` is the declared
  step id, shared by loop iterations; self-contained, key-sorted schemas by URI). Both are optional and **omitted while unset**, so a legacy
  spec's JSON and `spec_key` are unchanged; an opted-in spec's `spec_key` includes the stamp.
  They are stamped at the two submission sites (`execute_leg`, `dataflow/run`/`ml/run`)
  from the record and checked by the ONE validator, `src/cairn/core/dataflow/conformance_check.py`
  (control plane: configuration, engine declaration, a 96 KiB bound on the serialized spec,
  descriptor stats before registration; interpreter: identity, configuration, raw output
  before the done marker, frozen stats on resume). Since #2242 V1-C the nine stages
  `dataset-readiness/readiness` runs are opted in (`operators-and-packs.md`). The door reads a
  kernel's `implements` on the control plane, so a kernel that does not import there would run
  unchecked: `cairn doctor` (`stage_kernels`) warns for every registered kernel that does not
  load (#2363).

## What an engine owns
`DataflowEngine` protocol (`src/cairn/core/dataflow/engine.py:76`): `validate(spec)` (what
this engine cannot run), `submit(spec, ctx) → EngineJobHandle`, `poll(handle)`,
`cancel(handle)` (must kill compute), `result(handle) → DataflowResult`. An engine **is**
whatever passes the conformance suite (`src/cairn/core/dataflow/conformance.py`; ADR-0081 D3).
An engine that runs kernels through `run_dataflow` (`src/cairn/core/dataflow/runner.py`,
core since #2583 / ADR-0129) declares the optional
`carries_stage_contracts = True` (read by `carries_stage_contracts()`, default `False`;
builtin and builtin-ssh, metaflow declare it, skypilot does not); the control plane
refuses an opted-in spec on any other engine (ADR-0118 D7).
It owns execution, all rows, the per-stage loop, its own retries/resources on its
substrate; it receives no registry or database credentials (ADR-0082 D3) and returns
descriptors + scalar stats only.

**Spend (#1441, #131).** A stage reports its own spend under the stat `cost_usd`; the runner
(`run_dataflow`) sums it across stages and mapped items for `budget.max_cost_usd` and returns
it as `DataflowResult.cost_usd` (`contract.py:332`, the field ADR-0081 D2 lists). The runner
also meters the governed LLM chokepoint itself: each kernel call and each mapped item runs in
an `obs.cost_scope` without the session backstop, and a stage that made at least one
`llm_complete` call gains the runner-owned scalar stats `llm_calls`, `llm_tokens_in`,
`llm_tokens_out`, `llm_usd` and `llm_unpriced_calls` (absent when it made none); its
`cost_usd` is the kernel's own value when the kernel reported one, else `llm_usd`. They are
added after the raw output check and ride the descriptor and findings like any stat
(`<stage>_llm_usd`). As each stage or mapped item progresses, the runner writes the cumulative
known positive cost to `metrics.json`, including metered calls made before a stage fails. The
builtin local/SSH and Metaflow file paths can expose that progress; SkyPilot reads the fetched
runner metrics at teardown. This is stage-reported cost, including metered LLM spend, not an
engine invoice: SkyPilot's cluster/GPU charge is not included in `DataflowResult.cost_usd` or
`Run.cost` (its launch estimate is recorded separately). Other remote status paths may be
unable to read partial metrics, so terminal cost can be absent. The control plane settles each
attempt's known positive spend, including failed or cancelled attempts, and adds earlier
attempt spend to the next reservation. For checkpoint retries it subtracts frozen spend already
settled on the earlier attempt before settling the resumed attempt. The final known total is
written on the leg's `Run` (`llm_cost_usd` → `Run.cost`, `leg_hooks.flow_leg_record`, both
owners). Unknown cost remains absent, never $0; a succeeded leg whose engine reports no cost
(a GPU train stage today) still records no `Run.cost`.

## What cairn owns
Admission and governance before submit, the record, the handle for re-attach
(`StepState.handle`), budget reservation/settlement, the poll loop with backoff
(`leg_executor.py:520-556`), leg-level retry policy (`_leg_retry_policy`), control-side
resolution of `dataset:`/`judge:` refs (`leg_executor.py:185-215`), registration of the
engine's descriptors, lineage, the `parameters` event.

## Backend / environment / profile selection (CURRENT)
`resolve_engine(kind)` (`src/cairn/core/dataflow/engine_registry.py:102`) → `selected_engine(kind)`
(`:83`) reads the profile plane: `dataflow_engine` / `dataflow_target` for `data` legs,
`ml_engine` / `ml_target` for `ml` legs (`profile_backends.py:95-130`; env
`CAIRN_DATAFLOW_ENGINE`, `CAIRN_ML_ENGINE`, `*_TARGET`). Default **builtin**. Engines are
discovered by the `cairn.dataflow_engines` entry-point group (`cairn-operator-dataflow`
registers `builtin`, `metaflow`, `skypilot`; Prefect was removed by ADR-0134 D4); selecting an uninstalled engine
fails loudly at submit. A `PipelineDef` or flow that names an engine is drift.

| Engine | Role | Where it runs | Selected by |
|---|---|---|---|
| builtin | the reference engine and the contract; CI, dev, stub execution | `cairn-dataflow-run` as a local subprocess (`operator_dataflow/engines.py:59-90`), or on a `compute.ssh` box when `*_target` names one (the ssh dispatch) | default |
| metaflow | production ML substrate for Kubernetes/Batch compute (ADR-0089 — substrate, not the DAG; ADR-0104 D3) | Metaflow runner/datastore per the `metaflow_*` profile keys (`profile_backends.py:152-164`) | `ml_engine=metaflow` |
| skypilot | provisioning engine: `sky launch/status/down` from the spec's resources (ADR-0104 D5) | a SkyPilot-managed cluster; `CAIRN_SKYPILOT_USE_SPOT` (`profile_backends.py:217`) | `ml_engine=skypilot` |
| provider operators (`train/runpod|vast|modal|kaggle`) | `task:` legs outside the engine abstraction (`managed_leg.py`) | the provider | the leg's `task:` |

Metaflow is **not mandatory**: no production profile pins it (`src/cairn/profiles/*.toml`);
prod today runs builtin with an ssh target. Every training capability must execute on
builtin with stubs in CI and through the configured engine in production; the integration
proof for Metaflow is `tests/core/test_training_core_golden_path.py::test_the_same_flow_executes_through_the_metaflow_engine`.

## Local vs remote boundaries
The control plane never reaches into a box for rows; a box never reaches the database.
Remote execution moves **the image and the spec** (ADR-0104): the GPU trainer image is built
by CI and pinned by tag (`RUNBOOK-gpu-finetune-ssh.md`), secrets reach the flow as a scoped
set (ADR-0089 D3), tracking credentials are forwarded by the `run-train-remote` binding.
Doors that refuse before a record exists: job-channel, SkyPilot, pull-worker
(`dispatcher.py:888-911`, `pipeline_launch.py:283`).

## Failure semantics of a template operation node (CURRENT)
An in-process operator that raises inside a template operation node is degraded to a finding
and the run continues (#568, `engine/compiler.py` operation node); an operator declaring
`remote_job`/`docker` re-raises and the run fails. ADR-0117 adds one exemption:
`ContractConformanceError` — a shared-contract refusal (identity, input, configuration, output,
or a recorded entry the node cannot find) — re-raises and fails the run on every executor. A
task leg fails on the same error through the bridge's own handler.

## Retry, preemption, checkpoints — who owns what
- **Leg retry**: cairn (`retry:` on the leg; effective policy computed, not yet persisted — #2192).
- **In-flow retry/resources**: the engine on its substrate (Metaflow `@retry`/`@resources`, ADR-0089 D2).
- **Preemption/spot**: no owner today (`CAIRN_SKYPILOT_USE_SPOT` is a toggle without a
  recovery path, GAP) — decided in #2192.
- **Checkpoints**: the trainer writes them; `checkpoint_sync.py` mirrors to
  `CAIRN_CHECKPOINT_BUCKET` and is a silent no-op when unset (`:53-55`, GAP — #2191);
  the control plane relays local checkpoints (`src/cairn/core/dataflow/checkpoints.py`) and
  seeds `resume_from` through `resume_provider` (`leg_executor.py:335-345`); a stage
  with `checkpoint: true` snapshots durably when claimed (`runner.py:396-408`, ADR-0088).
- **RL backend loop** (TARGET): internal to the backend behind one `training.grpo` leg;
  never a cairn engine (FREEZE-v1; #2204).
