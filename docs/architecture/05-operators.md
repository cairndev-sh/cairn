# Operators — `src/cairn/core/operator/` (substrate) and `cairn/operators/` (packs)

> **DOCUMENTED design update, 2026-10-05:** [ADR-0111 production design proposal](../decisions/ADR-0111-contracts-capabilities-and-pack-separation.md#production-design-proposal--2026-10-05-2230) specifies candidate discovery, exact revisions, offline references, authoring and provenance changes. Design preparation is authorized; the schema is still PROPOSED. #2230 acceptance and the explicit P7 review conditions precede production implementation. Gold fixture grammar is not the production format.

**DOCUMENTED maintainer scope / PROPOSED destination:** every operator, including
builtins and optional packages, is covered by the [contract migration](19-contracts-and-operator-migration.md)
and [operator ledger](../architecture-fix/operator-contract-migration-inventory.csv).
The proposed target is one shared capability identity per operator, with implementation
config/schemas/diagnostics kept local. Multiple operators may share one distribution.
Preserve existing invocation and legacy call compatibility; metadata alone is not conformance.
The current capability-list API below remains current until the scoped migration is approved.

## Purpose
Operators are **the execution boundary**: the only place where Cairn code does real work
(query a backend, train, serve, redact, write). The substrate defines the contract and the
one invocation chokepoint; operator packs implement it as independently installable wheels.

## Owns
### Substrate (`src/cairn/core/operator/`, OBSERVED)
| Module | Responsibility |
|---|---|
| `protocol.py` | `Operation[I, O]` (`:261`), `OperationContext` (`:187`), `ResourceSpec`, store protocols (`ArtifactStore`, `SecretStore`, `MetricsLogger` — deprecated ADR-0103, `CostLedger`) and Null impls |
| `registry.py` | `OperatorRegistry` + `@register_operator(name, version, capabilities)`; `default_registry` |
| `loader.py` | walks entry-point group `cairn.operators`; `discover_operator_pack_manifests` reads installed `pack.toml`s (risk class reaches governance only if the pack is *installed*) |
| `runtime.py` (1100) | `invoke_operator` (`:888`) = **enforce_gate → pick_executor → invoke_operator_{inprocess,mcp_tool,job}`; binding grammar resolution (`$state`, `$input`, `$nodes`, `$artifacts`, `$env`); `mock_mode`/`sandbox_mode` ContextVars; reporter attach; parameter recording |
| `executor.py` | `pick_executor` over the manifest's declared `executors`; implemented today: `in_process`, `mcp_tool`, `remote_job` (docker/kubernetes_job/temporal_activity/http_call raise `UnsupportedExecutorError`) |
| `binding.py`, `binding_validate.py`, `binding_inline` (engine) | `operations/*.yaml` schema (10 locked fields) |
| `jobs.py`, `job_store.py`, `job_channel.py`, `job_control.py` | long-running remote jobs: `JobHandle`/`JobStatus`, durable job store triad, bidirectional control channel (ADR-0038) |
| `config.py`, `effective.py` | config layering run > workflow > env > default (ADR-0068/0070); governance-scoped `config_schema` fields are environment-owned |
| `manifest_sync.py`, `reports.py`, `report_ingest.py`, `stage_events.py`, `pins.py`, `secrets.py`, `guard.py`, `sharded.py`, `batch.py`, `testing.py` | class → manifest derivation; ADR-0103 reporting contract; stage events; train pins; scoped secrets; capability guard; sharding/batch helpers; the operator testing harness |

### Packs (`cairn/operators/`, OBSERVED)
- 14 `builtins/` (substrate providers, the public set — `scripts/oss/assemble_public_tree.py` `BUILTINS`): `artifact_local`, `audit_store`, `data_profile`, `dataflow` (doors `dataflow/run`, `ml/run`, `artifact/promote` + the engines), `mcp`, `memory_local`, `monitor_threshold`, `rag_local`, `rag_rerank`, `redact_regex`, `remote` (#2428), `secrets_env`, `secrets_infisical` (hello-cairn WP2, owner decision 2026-10-14), `train_local` (`train/lora` implementing `train.finetune/1.1.0`, the same fine-tune as the ml stage `train.lora`, and `train/eval-loss` implementing `train.eval_loss/1.1.0`; torch behind its `[train]` extra — ADR-0127 D4).
- 23 `official/`: `blast_radius`, `clickstack`, `compress`, `compute`, `data_expectations`, `dataset_checks` (23 operators), `dataset_synth`, `eval`, `eval_judged`, `eval_lm`, `git`, `ingest`, `lineage`, `llm`, `logs_analytics`, `model_registry`, `notify_slack`, `postmortem`, `redteam`, `report`, `s3`, `serving`, `training`.
- Layout per pack: `pack.toml` (governance metadata per `[operator."pack/op"]`: `risk`, `executors`, `long_running`, `requires_gpu`, `supports_*`, `capabilities.provides`), `pyproject.toml` (entry points), `src/operator_<name>/{operators,schemas}/`, `tests/`, `DESIGN.md` (10 of 37 have it; the rest are in the `BACKFILL` set of `tests/core/test_pack_design_docs.py`).

## Does not own
Orchestration (which operator runs when) → pipelines/templates. Approval decisions →
`approval/`. Store implementations → `storage/`. The engine selection for flows →
`dataflow/engine_registry.py` (cairn-operator-dataflow only *provides* engines).

## Public interface (the extension contract)
```python
from cairn.core.operator import Operation, OperationContext, register_operator, ReportSpec

@register_operator(name="serve/vllm", version="0.1.0", capabilities=["model.serve"])
class VllmServeOp(Operation[ServeSpec, ServeResult]):
    input_schema = ServeSpec; output_schema = ServeResult
    risk = "paid_compute"; executors = ["remote_job", "in_process"]; long_running = True
    async def run(self, inp, ctx): ...
```
(`cairn/operators/official/serving/src/operator_serving/operators/vllm_serve.py:132`)

Plus, for data/ml flows, **stage kernels** registered under `cairn.flow_stages`
(`operator_training.flow_stages: train_qlora`, …) and **engines** under
`cairn.dataflow_engines` (`operator_dataflow.engines`).

## Dependencies
Operator packs must import only `cairn.core.operator` (+ `cairn.core.llm` for the governed
LLM client, `cairn.core.eval` for eval operators). OBSERVED deviations:
`monitor_threshold` → `core.dispatch`/`core.engine.runner`; `ingest` →
`core.storage.schedule_store`; `dataflow` →
`operator_remote` (pack→pack); several → `cairn.services` (memory, rag, mcp) and
`cairn.integrations` (clickstack, blast_radius, git, logs_analytics).

## Dependents
`engine/compiler.py` (operation nodes), `dispatch/pipeline_bridge.py` (task legs),
`dispatch/leg_executor.py` (via engines/stages), `cli` (19), `triggers` (16), `obs`
(redaction/audit providers resolve operator classes by binding).

## Side effects
Anything: SSH to GPU boxes (`serving`, `remote`), paid compute (`training/*`), object
storage (`s3`), Slack, DB reads (`ingest`), LLM calls through the gateway (`llm`, `eval_*`,
`dataset_synth`). The manifest `risk` class (`read_only | external_write | paid_compute |
destructive`) is what governance keys on.

## State
`default_registry` (process-global), `mock_mode`/`sandbox_mode` ContextVars, remote job
store (memory/sqlite/postgres), reporter buffers.

## Entry points
`invoke_operator` (`runtime.py`) from the compiler's operation node (`compiler.py`): it
consults `pick_executor` and dispatches `in_process`, `remote_job`, `docker` or `mcp_tool`. A
pipeline `task:` leg on the in-process owner does NOT take that path (the Temporal owner runs no
`task:` legs, #1378): `pipeline_bridge.py _invoke_task_op` → `enforce_gate` →
`invoke_operator_core` always runs the operator in-process, so the launch door, the launch
preview and the bridge refuse a task leg whose operator does not declare `in_process`
(`pipeline_doors.task_leg_executor_problem`, #2182). The managed training legs
(`train/runpod`, `train/vast`) rely on this: their in-process `run()` submits, writes the
durable job record, then polls. Stages are entered by the kernel
runner (`src/cairn/core/dataflow/runner.py`, core since #2583 / ADR-0129; every engine adapter
runs it), never by the control plane directly.

## Tests
- Substrate: `tests/core/test_invoke_operator_core.py`, `test_operator_*.py` (14 files),
  `test_approval_chokepoint.py`, `test_operator_manifest_sync.py`, `test_pack_manifest_drift.py`,
  `test_pack_design_docs.py`, `test_installed_pack_manifests.py`.
- Packs: inside each pack's `tests/`. **Discovery gap (OBSERVED):** 14 packs with tests are
  not listed in `pyproject.toml` `testpaths` (artifact_local, mcp, memory_local,
  monitor_threshold, rag_local, rag_rerank, clickstack, dataset_synth, git, ingest,
  logs_analytics, postmortem, redteam, report); 6 packs have no `tests/` dir at all
  (audit_store, redact_regex, secrets_env, blast_radius, notify_slack, s3); a legacy
  `tests/operators/` tree still exists despite the ruling in `cairn/operators/CLAUDE.md` §A (#134 tracks the migration).

## Invariants
| Invariant | Status |
|---|---|
| Every operator invocation passes `enforce_gate` | CONFIRMED for compiler nodes and task legs (`runtime.py:950`, `pipeline_bridge.py:1332`); stages inside a flow are governed at admission by aggregated stage risk, not per stage (ADR-0089 R2, `tests/core/test_inflow_policy_boundary.py`) |
| Manifest ↔ class in sync | CONFIRMED when installed; **vacuous** when not (`cairn/operators/CLAUDE.md` §B) |
| Operator packs own their impl; no cross-pack imports | PARTIALLY: `operator_dataflow → operator_remote` (lazy) |
| `guard_context` protects lifted SDK ops | reachable only from SDK-generated code (`sdk/src/cairn_sdk/compiler.py:474`); hand-written operators build unguarded contexts (DOCUMENTED in `docs/audits/chokepoint-reachability-2026-08-13.md`, still true) |
