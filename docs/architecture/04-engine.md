# Engine — `src/cairn/core/engine/` (with `src/cairn/core/dispatch/` as its caller)

> "Engine" is used two ways in this repo. In prose (`README.md`)
> it means all of `src/cairn/core`. As a package, `src/cairn/core/engine/` is the **compiler
> layer**: template schema, template → LangGraph compiler, pipeline definition → plan,
> expression language, skills. This document covers the package; the run/pipeline
> machinery around it is in [13-pipelines.md](13-pipelines.md).

## Purpose
Turn authored records (a `flows/*.md` template, a `pipelines/*.yaml` definition, an
`operations/*.yaml` binding) into executable structures without executing them.

## Owns (OBSERVED)
| Module | Lines | Responsibility |
|---|---|---|
| `schema.py` | 988 | Pydantic models for templates (`TemplateSpec`, BT node types incl. `OperationBTNode`, `SubgraphBTNode`, `MapBTNode`, `PythonBTNode`), skills (`SkillSpec`), policies (`PolicySpec`), runtime types (`TriggerEvent`, `RunResult`, `ApprovalRecord`, `ActionRequest`) |
| `compiler.py` | 1947 | `compile_template(path)` → `CompiledTemplate`; `_build_prompt` (single call) and `_build_state_machine` → `_compile_bt` (703-line function) that wires LangGraph nodes, operation nodes (`operation_node_fn` with the approval `interrupt()` and `invoke_operator`), subgraph flattening, map/fan-out, retries, checkpointer attach, runtime selection (`_resolve_runtime`) |
| `compiled.py` | 141 | `CompiledTemplate`, `compute_content_hash` (content-addressed identity), `ast_json` snapshots (ADR-0022) |
| `pipeline.py` | 857 | `PipelineDef`/`PipelineStep` parsing + validation (`_validate_step`, wires, `on:`/`triggers:`), `plan_pipeline` → `PipelinePlan` |
| `bundle_pipeline.py`, `bundle.py` | 404 | the `PipelinePlan` IR and the older `bundle.toml` contract validation it grew from (ADR-0066/0069) |
| `pipeline_resolve.py` | 233 | ref → `PipelineDef`, ambiguity with a same-named flow (`pipeline:` prefix), `assert_runnable` |
| `pipeline_explain.py` | 781 | dry explanation per leg: engine, target, gates, config sources |
| `pipeline_gate.py`, `pipeline_loop.py`, `pipeline_triggers.py` | | gate predicates, bounded loops (ADR-0084 D7), trigger materialisation |
| `expr.py` | 606 | the one expression language (`$input`, `$steps`, `$config`, JMESPath predicates) |
| `runner.py` | 592 | `Runner` — legacy incident-era orchestrator: event → skill match → compile → run → policy → actions; still builds the process-wide policy view (`policy_source.py` mirrors it) and hosts `run_within_budget` (wall-clock) |
| `skill_engine.py`, `condition_registry.py`, `python_function_registry.py` (778) | | skills; named guard conditions; Python escape hatch functions (domain logic in core — P17 in the 2026-07 inventory) |
| `visualize.py`, `flatten.py`, `artifacts.py`, `workflow_config.py`, `schemas.py` | | graph view; subgraph flattening; artifact harvesting; config layering |

## Does not own
- Admission, budget reservation, identity stamping → `dispatch/dispatcher.py`.
- Pipeline advancement → `dispatch/pipeline_bridge.py`.
- Flow (data/ml) execution → `dispatch/leg_executor.py` + `dataflow/`.
- The approval decision → `approval/` (the compiler only *calls* `resolve_runtime_gate`).

## Public interface
- `compile_template(path, *, runtime=None, …) -> CompiledTemplate` (`compiler.py:92`) — 18 call sites in core + CLI.
- `parse_pipeline_def`, `load_pipeline_def`, `plan_pipeline` (`pipeline.py`).
- `compile_expr`, `compile_predicate`, `resolve_value` (`expr.py`).
- `TemplateSpec` and the BT node models (`schema.py`) — the authoring grammar.
- `seed_initial_state` (`compiler.py:437`) — the single source of truth for initial graph state (also used by Temporal smoke).

## Dependencies (OBSERVED)
Imports `operator/` (21 edges: `invoke_operator`, bindings), `pack/` (12: registry dirs,
schemas), `dataflow/` (12: flow defs, gate split), `runtime/` (9), `dispatch/` (8 — a
reverse edge), `policy/` (3), `guards/` (2), `approval/` (2), `services/` (2 lazy: RAG
inject/roles for ambient knowledge). The `triggers:` grammar is engine-owned
(`engine/triggers_grammar.py`, with `triggers_spec.py` and `triggers_presets.py`; #2416).

## Dependents
`dispatch/` (32), `cli/` (23), `triggers/` (24), `core/<top>` (20), `runtime/` (6),
`actions/` (7), `operator_notify_slack` (1, schema types), `operator_monitor_threshold` (2).

## Side effects
- Compilation reads files (pack dirs, schemas) and may attach a **checkpointer** (`compiler.py:703-705`, DB connection) when a gated operation is present.
- Compiled graphs, when run, call LLMs, operators, and `interrupt()`; `compile_template` itself does not execute.
- `python_function_registry` executes arbitrary registered Python at run time (trusted-code escape hatch, ADR §3.9).

## State
Process-global: registries in `condition_registry.py`, `python_function_registry.py`;
`_flatten_composition_enabled` reads env. No persistent state of its own.

**Pipeline discovery memo (#2340).** `pipeline_resolve.all_pipeline_defs()` parses every
`pipelines/*.yaml` in the registered packs, and each parse resolves its `$inherit` leg's
contract (ADR-0106 D2). An agentic leg went through `build_ui_contract` → `build_plan`, which
made a cold parse cost 3–14 s; it now reads `ui_contract.template_inputs` (the same frontmatter
`input_schema` through the schema registry, existence by `build_plan`'s own resolver, no plan),
which brought a cold discovery to ~2–3 s (best of 3, 8.4 s → 2.2 s). A clean result is memoised in-process under a key of everything the
parse reads: the registered `Pack` objects (an install, uninstall or `load_all_packs`
re-registration replaces them), the schema registry's entries (an agentic leg's input
schema), and a sha256 over the relative paths and bytes of every file under each pack's
`pipelines_dir`, `flows_dir`, `templates_dir` and `dataflows_dir`. The key is recomputed on
every call (≈10 ms) and hits are served as deep copies, so neither an edit nor a caller's
mutation can be served stale; a result that skipped an unparseable file is never kept (the
failure may come from outside the key), so it is retried and warned on every call, as before.
`invalidate_pipeline_discovery()` drops the memo; the builder's flow and pipeline publish
(`routers/builder.py`) call it after writing. `load_all_pipeline_defs_strict()` — the
completeness gate — is not memoised. There is no switch to disable the memo.

## Entry points into the engine
`RunDispatcher._run_template_task` (`dispatcher.py:942` region) → `compile_template`;
`cli/commands/run.py:332` (CLI local path compiles directly); `pipeline_launch.py` →
`plan_pipeline`; Temporal worker `build_registry` compiles every template at boot.

## Tests
`tests/core/test_template_*`, `test_subgraph_*`, `test_engine*`, `test_pipeline_*`,
`test_flatten*.py`, `tests/core/test_template_content_hash.py`.

## Invariants touching the engine
- D8 "the compiler is amended, never replaced" (ADR-0084) — DOCUMENTED; `compile_template` is the only template compile entry (CONFIRMED by grep: no second compiler).
- `agent_loop` retired — CONFIRMED: `compiler.py:163` raises on `flow.type: agent_loop`. The compiler docstring still says "three flow types" and lists two.
- LangGraph is confined to agentic legs: no graph holds a data or ml leg, and `execute_leg` and the job process load no LangGraph — CONFIRMED (`test_leg_executor.py`). **CONFLICT** with ADR-0084's "a pure data/ml pipeline loads neither LangGraph nor Temporal": the module is still loaded around the leg. `engine/compiler.py` imports it at module level; `dispatch/dispatcher.py` reaches that through `engine/runner.py`, `pipeline_bridge.py:_submit_flow_leg` imports the dispatcher, and the CLI reaches it through `core/validate.py`. It's also a required dependency in `pyproject.toml`.

## Known hotspots (see 15-architecture-risks.md)
`_compile_bt` (703 lines), `compiler.py` (1947).
