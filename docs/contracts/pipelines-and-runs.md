# Pipeline and run contracts

Four things, four responsibilities. None of them is a new execution abstraction: a
**pipeline** is the only orchestrator (ADR-0084 D4); only a declared pipeline runs
(ADR-0106 D1); every leg is its own governed `Run` (invariant 4).

| Thing | Is | Owned by | Source |
|---|---|---|---|
| `PipelineDef` | pack data (`pipelines/*.yaml`): inputs, steps, outputs, governance | Engine | `src/cairn/core/engine/pipeline.py:152` |
| `PipelineStep` (a leg) | one of `flow:` · `template:` · `task:` · `gate:` · `loop:` with `with:`, `consumes/produces`, `when`, `depends_on`, `environment`; a flow/template/task leg (not in a loop body, not a finalizer) may also declare `trigger:` (`auto` · `manual` · `schedule:<cron>` · `on:<event>`) and `emits:` (a domain topic published on completion) — additive, #2605; at their defaults they are outside the content hash | Engine | `src/cairn/core/engine/pipeline.py:62` |
| `PipelinePlan` | the lowered, validated def (`plan_pipeline`) + the `contract_resolution` snapshot (§Contract resolution); `with:` values retain their YAML type. Nested `$steps` references are checked against known producer steps and the effective dependency ancestry; nested `$input` references must name a declared pipeline input. Nested bare `<step>.<field>` strings are ambiguous and refused with the explicit `$steps.<step>.<field>` spelling; top-level bare wires remain supported. Values are still resolved at submit (#1675; records planned before it carry strings, and a binary from before #1675 cannot load a record planned after it) | Engine | `src/cairn/core/engine/pipeline.py:970 `_lower_wire`, :765 `_check_wires``, `bundle_pipeline.py:138` |
| `PipelineRecord` | the durable execution record: plan snapshot, `step_state`, `cursor`, inputs, governance, status | Storage (written by Dispatch) | `src/cairn/core/storage/pipeline_store.py:80` |
| `StepState` | one leg's live state: `run_id`, engine `handle`, status, verdict, approver, timestamps | Storage | `pipeline_store.py:50` |
| `Run` | one governed execution (a leg, or an agentic run) | Storage (written by Dispatch) | `src/cairn/core/storage/protocols.py:136` |
| `RunEvent` | append-only, `seq`-numbered event on a run | Storage | `protocols.py:324` |

## Lifecycle (CURRENT, OBSERVED)
1. **Start** — `POST /start` / CLI resolve the ref (`dispatch/start.py:179`); a bare flow
   ref is refused (`:69`); `launch_pipeline` → `launch_pipeline_def`
   (`dispatch/pipeline_launch.py:162`, `:211`): resolve → drift check → `assert_runnable`
   → presets → coerce inputs → `plan_pipeline` → `inject_policy_gates` (`:278`) →
   `PipelineBridge.start_pipeline` (`dispatch/pipeline_bridge.py:561`). The record is
   stamped `bundle_ref = pipeline:<pack>/<name>`. Before the bridge, the admission door
   (`admission_door.decide_admission`, #2190) decides once over the inputs: the holdout,
   intake and licence doors and the policies' required checks. **Input expectations
   (ADR-0146):** an input spec may declare `role:` (`training` · `validation` · `test` ·
   `golden`) and `verdict: required`; the door then refuses a value that is not a `dataset:`
   ref, a ref the registry does not hold, a version with another role, or one with no intake
   verdict when it is required, and records every check under `admission.expectations`. An
   unknown role or another `verdict:` value fails `parse_pipeline_def`
   (`engine.pipeline.dataset_expectations`). **Priced legs (ADR-0151 D1–D3):** each flow
   leg whose compute target is a `compute.ssh` connection with `hourly_usd` is estimated at
   `hourly_usd × timeout`, times its loop's `max_iterations` and its attempts (the bounded
   worst case); a priced leg with no `timeout`, a price that cannot be read, or a total that
   does not fit the budget's `remaining_usd` (the cap alone when no `series` is set) is refused
   (403 / exit 3) before the record, and the per-leg estimate is recorded under
   `admission.cost_estimate`; each attempt reserves one attempt's recorded figure.
2. **Advance** — `_advance` (`:783`) submits every ready step by kind (`_submit_step`,
   `:862`): a `flow:` leg becomes its own `Run` (`_submit_flow_leg`, `:957`) and runs
   `execute_leg` (`dispatch/leg_executor.py:114`); a `task:` leg invokes an operator;
   a `template:` leg goes through `RunDispatcher.run_template` (`dispatcher.py:737`);
   a `gate:` leg pauses for a verdict or a human (`awaiting_approval`, `:1040-1095`).
3. **Terminal** — a run's `run.done|failed|cancelled` (`_TERMINAL_TOPICS`, `:78`) reaches
   the bridge (`_on_run_terminal`, `:607`), which updates `step_state` and advances.
4. **Release** — `POST /pipelines/{id}/steps/{step}/{advance,approve}` (`routers/pipelines.py:747-756`)
   → `advance_step` (`pipeline_bridge.py:1913`) re-evaluates the gate and resubmits.

Statuses — `Run.status`: `queued · running · awaiting_approval · done · failed · cancelled`
(`protocols.py:61-75`); `StepState.status`: `pending · queued · awaiting_advance ·
awaiting_approval · running · done · failed · skipped` (`pipeline_store.py:59`);
`PipelineRecord.status` is recomputed from the steps (`running · paused · done · failed · cancelled`).

## Identity and provenance
`pipeline_id` (`pl_…`), `run_id` per leg, `template_ref` recorded at admission and never
re-derived (`_submit_flow_leg`; invariant 4), `bundle_pipeline_id` + `step_id` on the run
(`protocols.py:233-234`), `parent_run` (`:244`), `train_pins` for training legs (`:252`),
`resolved_operators` + `schema_hash` (`:172-177`), `trace_id` (`:224`). The `parameters`
event records what a leg ran with (`src/cairn/core/reporting.py:25`; written by the runtime).
`Run.owner` (ADR-0125 D1, additive, in the JSON `data` blob) names the process that runs an
in-process leg — a task leg, a template leg on the in-process runtime, a flow leg's poll
loop — as `"<hostname>:<boot id>"`; it is rewritten when a process re-attaches a leg,
re-arms its slot wait, or resumes an approved template run, and is `""` for a run written
before ADR-0125, by a process holding no owner lock, or owned by Temporal/DBOS.
`StepState.fired_key` (ADR-0125 D5, additive, in the JSON blob) is the write-ahead marker of
a `schedule:`/`on:` fire: `"<trigger>@<fire time, UTC, second precision>"` (`step_fire_key`,
`pipeline_bridge.py`; the cron tick is derived from the trigger and the fire's own time,
`cron_fire_time`; an event keys on its receipt second), written and put inside the pipeline's
critical section before `_submit_step` (`advance_step`). A fire with the stored key is
refused (`fire_refused`); `_mark_awaiting` clears it for the next wait; on adopt a waiting
step with a key takes the run the fire created (`_claim_unrecorded_leg_runs`) or is parked
`awaiting_advance` with `leg_parked_after_restart` (`_park_fired_step`), never re-armed.

## Retries, failure, cancellation
- Retry policy per leg from `with: retry:` (`_leg_retry_policy`, `pipeline_bridge.py:178-192`)
  → `execute_leg` loop (`leg_executor.py:291-412`); ml legs default to one attempt.
  The effective policy is computed, not persisted (C5 GAP; #2192).
- Re-attach after a restart: the engine handle persisted at submit (`StepState.handle`,
  `pipeline_store.py:58`) is adopted before any resubmit (`leg_executor.py` adopt path;
  ADR-0086 H1); the reconciler marks stale runs (`src/cairn/core/runtime/reconciler.py`).
- Liveness of an in-process leg (ADR-0125): its owner process holds an owner lock for its
  life (`src/cairn/core/liveness.py`; a Postgres session advisory lock, or a SQLite `flock`).
  `owner_verdict` (`runtime/reconciler.py`) reads ALIVE / UNDETERMINED / DEAD / LEGACY.
  Restart re-maps a leg whose owner is ALIVE or UNDETERMINED and touches nothing else; the
  reconciler hands a DEAD leg of a running/paused pipeline back to the bridge's restart
  rules and settles a DEAD leg of a terminal pipeline `cancelled`.
- Failure: a failed leg fails the record unless a `finalizer:` step exists
  (`_fail_pipeline`, `pipeline_bridge.py:2309`); `Run.error` and `failure_type` carry the cause.
- Cancellation: `cancel` kills engine compute (`DataflowEngine.cancel`) and settles budget;
  a queued leg cancelled while waiting for a slot is skipped cleanly (`:1573-1600`).
- Budget: reserved per leg before submit and settled after (`leg_executor.py:401`;
  `budget_reservations.py`); a series cap refuses admission. A leg on a priced box settles
  `hourly_usd × wall-clock` per attempt on every outcome and records it as `Run.cost.gpu_usd`
  (ADR-0151 D5).

## Persistence
`PipelineRecord` in the pipeline store (sqlite/postgres twins; the process-wide default is
`default_pipeline_store()`, `pipeline_store.py:396-412`); runs and events in the run store
(`build_storage()`); the record is the truth, the bus is the live signal.

### Public Python names over a run (#297)
Helpers that read a run's own record, for a surface such as a `cairn.routers` or `cairn.seeds`
provider (`docs/compatibility.md` §What is public). Each earlier private spelling stays as an
alias for the same function.

| Name | What it is |
|---|---|
| `cairn.core.eval.scoring.run_template_ref(run)` | the workflow a run executed: `Run.template_ref`, with the legacy inference chain for rows written before it was recorded |
| `cairn.core.run_progress.trace_from_events(events)` | the per-step trace derived from a run's `node` events (the `steps` of `GET /runs/{id}`'s `progress`) |
| `cairn.core.run_progress.duration_ms(start, end)` | milliseconds between two ISO timestamps, or `None` |
| `cairn.core.eval.actuation.proposal_markdown(suggestion)` | a suggestion rendered as reviewable Markdown (the issue or PR body the actuators write) |
| `cairn.core.demo_seed.demo_inputs_for(ref)` | a workflow's first declared demo inputs, or `{}` |

## Contract resolution — CURRENT (#2240 C, ADR-0111 acceptance §4)
`PipelinePlan.contract_resolution` (`bundle_pipeline.py:ContractResolution`, frozen) is the
static contract closure the pipeline was admitted under: `format: cairn.resolution/1`, the
installed contract distributions (name, version, descriptor digest) and their definitions
(id, revision, kind, distribution, version, digest). It is taken ONCE at the launch door
(`dispatch/pipeline_launch.py`, right after `plan_pipeline`, before gates, admission and the
record) from the pack facade `registry.contract_resolution_snapshot()`; legs and the Temporal
owner re-read the record and never re-derive it (the Temporal workflow argument is a
transport copy that nothing writes back). `None` means the closure was not recorded at the
launch door — rows older than the field, the backfill, rows the removed bundle-era
`PipelineBridge.run_bundle` path wrote: not certified, not wrong. An empty installation
records a certified empty closure.
An invalid installed distribution refuses the launch as `PipelinePlanError` before any record
exists. A def that loads but cannot be planned is a 400 carrying the planner's message at both
launch doors, `POST /start` and `POST /pipelines/launch`, arm for arm: `PipelinePlanError` (a
cycle, an unresolvable contract closure), `PipelineDefError` (a dangling `depends_on`, a wired
field the source does not produce) and `WireTypeError` (a wire whose kinds or field types
disagree) (#2280). A row carrying an unknown `format` refuses to hydrate
rather than being relabelled legacy; additive changes keep `/1`, a breaking change is `/2`
plus an ADR. No new route: it rides `/pipelines/{id}` with the rest of the plan.

`contract_resolution.invocations` (#2241 B; additive within `/1`, older rows hydrate as `()`):
one `InvocationEntry` per opted-in operator the plan reaches, keyed by leg id for a task leg
and `<leg>/<node path>` for a template operation node (slice C) — `capability_ref`,
definitions with digests, `operator_ref`, `operator_version` — written at the door after the
closure and before gates, read by the leg from the record (`operators-and-packs.md`).
ADR-0118 adds `surface` (`operation` | `stage`). An `operation` entry omits `surface`,
`stage_index` and `stage`, so its bytes are unchanged. A `stage` entry records one opted-in
dataflow stage: `stage_index` and `stage` key it, and `operator_ref`/`operator_version` carry
the stage name and its kernel's distribution version. A flow leg's stage entries use the leg
id as `path`; a template `dataflow/run`/`ml/run` node's use `<leg>/<node path>`. Only
`operation` entries are read as an operator's contract.

## Events
`RunEvent {run_id, seq, ts, type, data}` (`protocols.py:324-344`); `seq` is per-run
monotonic and drives SSE catch-up (`GET /runs/{id}/stream?after_seq=`). Bus topics:
`run.<type>` for every run event (terminal ones drive the bridge), `pipeline.<event>` for
bridge audit (`_audit`, `pipeline_bridge.py:2330`): `leg_queued`, `leg_admitted`,
`leg_awaiting_approval`, … New event types are additive; a renamed type is breaking.

## TARGET (training architecture; not implemented)
`TrainingRequest`/`TrainingPlan` snapshots on the record (#2210), `AdmissionDecision`
(#2190), `PromotionDecision` + previous champion (#2197), persisted effective retry policy
and `preemption: recover|refuse` (#2192), durable checkpoint requirement on the handoff (#2191).
`contract_resolution.invocations`[].profile_digest` (#2248).
