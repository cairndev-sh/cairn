# Runtime — `src/cairn/core/runtime/` and the dataflow engine waist `src/cairn/core/dataflow/`

Two different things execute work in Cairn. Keeping them apart is the single most useful
distinction in this area (ADR-0084 D1, D6):

| | **Agentic runtime** (`src/cairn/core/runtime/`) | **Dataflow engine** (`src/cairn/core/dataflow/` + `cairn-operator-dataflow`) |
|---|---|---|
| Runs | a compiled LangGraph graph (template / agentic flow / `task:` legs indirectly) | a `DataflowSpec` (stage list of a `kind: data` or `kind: ml` flow) |
| Protocol | `Runtime.invoke(compiled_graph, state, run_id, template_name, template_sha)` (`runtime/base.py:123`) | `DataflowEngine.validate / submit / poll / cancel / result` (`dataflow/engine.py:76`) |
| Implementations | `InProcessRuntime` (`inprocess.py:26`), `DBOSRuntime` (`dbos/runtime.py:98`), `TemporalRuntime` (`temporal/runtime.py:32`) | `builtin`, `metaflow`, `skypilot` (entry points in `packages/cairn-operator-dataflow/pyproject.toml`), plus SSH variants (`engines_ssh.py`); all run kernels through core's `dataflow/runner.py` (#2583, ADR-0129) |
| Selected by | `CAIRN_RUNTIME` / profile `durable_runtime` → `compiler.py:792 _resolve_runtime`, `runtime/__init__.py:30 select_runtime` | `[profile.backends]` (`dataflow_engine`, `ml_engine`) → `dataflow/engine_registry.py:83 selected_engine`, `:102 resolve_engine` |
| Durability | LangGraph checkpointer tiers (`checkpointer.py`: memory/sqlite/postgres, `CAIRN_CHECKPOINTER`); Temporal event history; DBOS | engine-owned (`.done` markers, Metaflow checkpoints); control plane holds the run record + engine handle and re-attaches (`leg_executor.py` adopt path, `leg_hooks.py`) |
| Never | holds a data/ml leg (CONFIRMED: `tests/core/test_leg_executor.py::test_a_data_leg_never_imports_langgraph`) | runs an LLM prompt node |

## `src/cairn/core/runtime/` — owns
- `base.py`: `Runtime` protocol, `NodeKind` tags (`PURE/LLM/TOOL/IO`), retry/queue metadata helpers.
- `inprocess.py`: Pregel `ainvoke` in this process (default).
- `temporal/`: `runtime.py` (submit), `workflow.py` (`TemplateWorkflow`, one type for every template), `activities.py` (`run_node`), `worker.py` (`cairn-worker`: loads packs + operators, registers templates, adapts graphs), `pipeline_workflow.py` + `pipeline_activities.py` (durable owner of a pipeline — the Temporal twin of `PipelineBridge`), `registry.py`.
- `dbos/`: single-process durable workflows (`CAIRN_RUNTIME=dbos`), `recv/send` HITL.
- `checkpointer.py`: `get_checkpointer()` tiers, per-loop pools, `reset_checkpointer`.
- `reconciler.py`: `RunReconciler` heals zombie runs (heartbeat TTL, Temporal status sweep).
- `node_events.py`, `node_stream.py`, `node_record.py`, `retry.py`.

## `src/cairn/core/dataflow/` — owns
- `contract.py`: `DataflowSpec`, `StageRef`, `StageResources`, `StageMap`, `Budget`, `ResumeFrom`, `ArtifactDescriptor`, `DataflowResult` — engine-neutral by rule ("a field must never enter DataflowSpec because engine X needs it").
- `engine.py`: `DataflowEngine` protocol, `EngineJobHandle/Status`, `is_terminal`.
- `engine_registry.py`: `installed_engines`, `load_engine_factory`, `selected_engine(kind)`, `resolve_engine(ctx, kind)` — moved here from `operator_dataflow.engines` (#1512) so core never imports a pack.
- `defs.py`: `FlowDef` (`flows/*.yaml`, `kind: data|ml`), `resolve_flow_ref` (config `$config.*` substitution → spec), stage catalog and `stage_risk_class`/`aggregate_risk` (fail-closed to `external_write` on unknown).
- `manifest.py`, `checkpoints.py`, `export.py` (emit engine-native flow files), `gate_split.py`, `capture.py`, `conformance.py` (the engine conformance suite = the protocol's tests), `sysmetrics.py`.
- `[kernel]` extra in `pyproject.toml` is the measured minimal dependency set a job machine needs to import `cairn.core.dataflow` (no fastapi/langgraph/litellm/temporalio).

## Does not own
Which stage runs (the flow def), stage implementations (operator packs), budget reservation
(leg executor), artifact registration (control plane, `leg_executor.py:347-364`).

## Dependencies
`runtime/` → `storage` (9), `dispatch` (8 — Temporal pipeline activities build a
dispatcher; reverse edge), `engine` (6), `bus`, `approval`, `operator`, `pack`, `dataflow`.
`dataflow/` → `engine.expr` (top-level, for `$config`), `storage.artifacts` (checkpoints),
`pack.registry` (lazy, flow dirs).

## Dependents
`engine/compiler.py` (runtime selection, checkpointer attach), `dispatch/` (leg executor →
engines; bridge → Temporal pipeline start), `cli` (worker, flow commands), `triggers`
(pipelines explain), cairn-operator-dataflow (implements the protocol), cairn-operator-training (stages).

## Side effects
Temporal client connections (`CAIRN_TEMPORAL_ADDRESS`), DBOS system DB, checkpointer DB
connections, SSH sessions (engines), GPU rentals (SkyPilot engine), Metaflow API calls.

## State
Checkpointer pools per event loop (`checkpointer.py:_pg_pool_for_loop`), Temporal template
registry (`temporal/registry.py`), DBOS registry, `_DISPATCHER` singleton in
`temporal/pipeline_activities.py:54`. Pipeline record writes use per-process
`CriticalSections(namespace="pipeline")`; with Postgres, each process obtains an
`advisory-locks:pipeline` pool of up to 8 connections and shares exclusion through
Postgres advisory locks. Without Postgres there is no exclusion between the server and a
separate worker process.

## Entry points
`cairn-worker` → `runtime/temporal/worker.py:main`; `compile_template(... runtime=)` →
`Runtime.invoke`; `PipelineBridge._start_temporal_pipeline` (`pipeline_bridge.py:521`)
when `settings.runtime() == "temporal"`; `execute_leg` → `resolve_engine`.

## Tests
`tests/core/test_runtime.py`, `test_temporal_*.py` (7+), `test_dbos_runtime.py` (marker
`dbos`), `test_checkpointer_pool.py`, `test_run_reconciler.py`, `test_reconciler_*.py`,
`test_durable_restart.py`, `test_engine_registry.py`, `test_dataflow_contract.py`,
`test_dataflow_defs.py`, `tests/dataflow_conformance/`, and the engine conformance suites in
`packages/cairn-operator-dataflow/tests/` (builtin, metaflow, skypilot, ssh).

## Invariants
| Invariant | Status |
|---|---|
| Runtimes are adapters behind one waist (ADR-0084 D1) | CONFIRMED for flows (`DataflowEngine` protocol + conformance suite); for agentic graphs the `Runtime` protocol exists but Temporal also needs graph adaptation at worker boot (`worker.py:176 adapt_graph_for_temporal`) |
| No Temporal at the pipeline tier for data/ml (ADR-0084 D6) | **CONFLICT with code:** under `CAIRN_RUNTIME=temporal` the *pipeline itself* is owned by `PipelineWorkflow` (`pipeline_bridge.py:558-582`), i.e. Temporal is used at the pipeline tier as an environment choice. The ADR text says "No Temporal at this tier"; the code makes it optional, not absent. |
| The Temporal pipeline owner runs every leg kind | **GAP (#59; tracked in the archived monorepo as #1378/#1459):** `pipeline_activities.py` `pipeline_submit_step` routes `flow:` legs to the engine and sends everything else through `run_template`, so template legs run, `gate:` legs are completed by the workflow, and a `task:` leg fails with "references unknown pack 'task:…'". Seen on 2026-10-13 running `hello-cairn/profile-and-publish` under `CAIRN_RUNTIME=temporal`. Pipelines with task legs run on `inprocess` or `dbos` until #59 lands. Since #199 the launch door refuses such a pipeline under `temporal` before any record exists (`dispatch/pipeline_launch.py` `runtime_owner_problems`, a `PolicyDeniedError`: 403 / exit 3), reading what the owner declares it runs (`runtime/temporal/__init__.py` `PIPELINE_OWNER_LEG_KINDS`); a `finalizer:` step is refused the same way (`PIPELINE_OWNER_RUNS_FINALIZERS`, ADR-0151 D10), because the workflow schedules by `depends_on` only. `tests/core/test_temporal_owner_capability.py` derives both declarations from the owner's own routing and scheduling, so they cannot drift from it. |
| DBOS on an SQLite system DB needs SQLite >= 3.42 | CONFIRMED (OBSERVED 2026-10-15, prod box: Debian bookworm Python, SQLite 3.40.1, dbos 2.31.0): DBOS's SQLite `workflow_status.created_at` defaults to `unixepoch('subsec')`, NULL before 3.42, so every workflow insert fails `NOT NULL`. `dbos/runtime.py:35 check_system_db` (called from `ensure_launched`, `:72`) refuses boot and every run submit with the cause and remedies (a Python whose SQLite >= 3.42, e.g. uv-managed, or a Postgres `CAIRN_DBOS_DSN`); the server logs it as `[dbos] boot failed: …` (`triggers/server.py:722`). Test: `tests/core/test_dbos_sqlite_version.py`. |
| The public core's durable runtime is DBOS; Temporal is installable and runs flow, template and gate legs, not `task:` legs or finalizers (ADR-0127) | CONFIRMED by decision: the public example's pipelines run on `inprocess` and `dbos` (`CAIRN_RUNTIME=dbos`, the `runtime-dbos` extra), which is what its end-to-end matrix covers (`tests/e2e/hello`, [hello golden path](../golden-paths/hello.md)); `runtime-temporal` stays a public extra, but task legs on it are the #59 gap in the row above (refused at launch since #199), so no public pipeline is proven on Temporal |
| Engine-specific config never enters `DataflowSpec` | DOCUMENTED + reviewed by hand; no automated test found (UNCLEAR) |
| Core selects engines without importing a pack | CONFIRMED (`test_engine_registry.py`) |
| Both pipeline owners share the same leg side-channels | CONFIRMED by construction after #1429 (`dispatch/leg_hooks.py`); parity tests `test_temporal_owner_leg_kinds.py`, `test_temporal_owner_readopt.py` |
