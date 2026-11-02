# cairn/core/engine — compiler layer (templates → LangGraph, pipelines → plan, expressions)

**Purpose.** Turn authored records into executable structures without executing them.
**Owns.** `TemplateSpec` + BT node grammar (`schema.py`); `compile_template` and the
`_compile_bt` node factory incl. the operation node's approval `interrupt()` and
`invoke_operator` call (`compiler.py`); `CompiledTemplate` + content hash (`compiled.py`);
`PipelineDef` grammar, validation and `plan_pipeline` (`pipeline.py`); the `PipelinePlan`
IR (`bundle_pipeline.py`); ref resolution (`pipeline_resolve.py`); `expr.py` — the ONE
expression language; skills; `runner.py` (legacy incident orchestrator + `run_within_budget`).
**Does not own.** Admission, budgets, records (`dispatch/`, `storage/`); gate decisions
(`approval/`); flow execution (`dispatch/leg_executor.py`, `dataflow/`).

**Contracts.** `compile_template(path, *, runtime=None) -> CompiledTemplate` (18 callers);
`plan_pipeline(PipelineDef) -> PipelinePlan`; `compile_expr`/`compile_predicate`. The BT
compiler is **amended, never replaced** (ADR-0084 D8): gate enforcement, durability
(checkpointer attach), replay and content hashing all live in it. `agent_loop` is retired
(`compiler.py:163`).

**Allowed deps.** `operator` (bindings, invoke), `pack` (registry dirs, schemas), `dataflow`
(flow defs), `runtime` (select), `policy`/`approval`/`guards`, `services` (lazy: RAG).
**Forbidden.** operator packs; `cairn.packs.*`; any import of `cairn.core.triggers` (the
`triggers:` grammar is `triggers_grammar.py` here, #2416; ADR-0120 D6); anything from `cli`.

**Entry points.** `dispatch/dispatcher.py:_run_template_task`, `dispatch/pipeline_launch.py`,
`cli/commands/{run,validate,visualize,pipeline}.py`, `runtime/temporal/worker.py:build_registry`.

**Testing.** `tests/core/test_template_*`, `test_subgraph_*`, `test_pipeline_*`,
`test_engine*`, `test_flatten*.py`, `test_template_content_hash.py`. Any change to node compilation must keep
`test_approval_chokepoint.py` / `test_approval_gate_e2e.py` green.

**Common mistakes.** A new node type that skips `resolve_runtime_gate`; changing state
seeding outside `seed_initial_state`; adding a fourth expression dialect instead of using
`expr.py`; letting a `PipelineStep` field reach the plan without `_validate_step`; reading
env in the compiler instead of via config layering.

**Read.** `docs/architecture/04-engine.md`, `docs/golden-paths/operator.md`, ADR-0020
(node types), ADR-0022, ADR-0084 D2/D3/D7/D8, ADR-0034.
