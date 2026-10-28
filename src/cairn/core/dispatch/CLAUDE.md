# cairn/core/dispatch — the start door, admission, pipeline advancement, leg execution

**Purpose.** Everything between "a user/trigger asked for X" and "a leg is running": ref
resolution (`start.py`), the pipeline launch door (`pipeline_launch.py`), run admission
(`dispatcher.py RunDispatcher.run_template`), pipeline advancement (`pipeline_bridge.py
PipelineBridge`), flow-leg execution (`leg_executor.py execute_leg`), scheduling,
cross-replica locks (`coordination.py`).

**Owns.** Which door a ref goes through; admission doors + budget reservation for agentic
runs; creating the `Run` per leg; leg-kind routing (`_submit_step`); gate/when evaluation on
persisted outputs; adopt-on-restart; Temporal hand-off; target concurrency.
**Does not own.** Plan validation grammar (`engine/pipeline.py`), gate *decisions*
(`approval/`), policy rules (`policy/`), engine implementations (operator packs), store
schemas (`storage/`), HTTP shapes (`triggers/routers`).

**Contracts.** `launch_pipeline(ref, inputs, environment, …) -> PipelineRecord` is THE
pipeline door for CLI, console, cron and operator-triggered starts. `run_template` is the agentic door for `template:` legs and the no-orphan chokepoint (ADR-0106
D7); `launch_workflow` no longer exists — a bare flow ref is refused at the door (ADR-0106 D1). `execute_leg(...)` is the engine waist. Do not add a
third launch path; do not call `compile_template` + run directly (the CLI dev loop
`cairn run --interactive|--mock|--dry-run` does, by decision — ADR-0106 D6, no record).

**Allowed deps.** `engine`, `operator`, `approval`, `policy`, `storage`, `dataflow`, `bus`,
`runtime`, `obs`, `budget_reservations`, `liveness` (the owner id, lock and probe — ADR-0125),
`connections` (function-local: a compute target's connection config — `max_in_flight`,
`hourly_usd` — read core-to-core, no secret resolved; ADR-0151 D1),
doors, `pack` (function-local imports only:
profiles, loader, provisioning, the contract-resolution facade — #2240 C). **Forbidden.** any operator pack (`from
operator_…` — AST test), `cairn.packs.*`, new imports of `cairn.core.triggers` (existing
lazy ones in `scheduler.py` are debt).

**Entry points.** `routers/start.py` → `start.py`; `routers/pipelines.py` →
`pipeline_launch.py`; `cli/commands/pipeline.py`; `scheduler.py:fire_scheduled_workflow`;
`PipelineBridge.start()`/`_adopt_in_flight` at app boot.

**Twin to keep in step.** `runtime/temporal/pipeline_workflow.py` + `pipeline_activities.py`
implement the same advancement durably; shared leg side-channels live in `leg_hooks.py`.
A change to leg semantics in the bridge that is not in the activities is a bug.

**Testing.** `tests/core/test_pipeline_*.py` (34), `test_leg_executor.py`,
`test_dispatcher*.py`, `test_run_template_ref.py`, `test_target_concurrency.py`,
`test_temporal_owner_*.py`, `test_pipeline_governance_wiring.py`. A new door or gate needs
a test that proves it can REFUSE.

**Common mistakes.** Reading `getattr(rec, "field", None) or {}` for a field that does not
exist (#1688 — silent green); adding a reservation/gate on one leg kind only; forgetting
`bundle_pipeline_id`/`template_ref` on a new `Run`; letting an exception escape
`_submit_step` before the record `put`; process-local locks where `coordination.py` exists.

**Read.** `docs/architecture/13-pipelines.md`, `docs/golden-paths/pipeline.md`,
`docs/architecture/08-governance.md`, ADR-0084, ADR-0101, ADR-0105, ADR-0086, ADR-0091.
