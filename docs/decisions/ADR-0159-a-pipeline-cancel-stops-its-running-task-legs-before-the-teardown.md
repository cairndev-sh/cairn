# ADR-0159 — A pipeline cancel stops its running task legs before the teardown

- **Status:** ACCEPTED 2026-10-29 by the owner (option 1 of the #219 / #207 decision).
- **Date:** 2026-10-29
- **Issues:** cairndev-sh/cairn#219 (a cancel during `compute/acquire` can leave the box running),
  cairndev-sh/cairn#207 (cancel does not reach a running task leg; this ADR settles its pipeline half).
- **Amends:** the cancel rule in `docs/architecture/13-pipelines.md` (a pipeline cancel left
  `running` legs to their own paths); extends ADR-0151 D9 (cancel stops the job before the box) from
  flow legs to task legs.
- **Keeps:** ADR-0151 D9 for flow legs (`cancel_recorded_job` before the release), #1787 (a
  finalizer's run is never touched by a cancel; the teardown is what a cancel still owes), ADR-0125
  D4 (a leg whose owner died is settled by the reconciler), and the cancel of a single run
  (`RunDispatcher.cancel`), which #207 still covers.

## Context

`PipelineBridge.cancel_pipeline` settled `awaiting_approval` legs, cancelled the engine job of a
flow leg on a box the pipeline releases (ADR-0151 D9), and submitted the finalizers. A `running`
task leg was left to finish on its own.

For `compute/acquire` that is a race. The release finalizer can run while `acquire` is between its
lease stamp and its start, or while the start call is still with EC2. The release then finds the
box stopped and does nothing, and the start lands afterwards. The box bills until its lease runs
out and `cairn compute sweep --apply` stops it: up to 26 h, about $17, for `finetune/sft` (#219).
EC2 also refuses to stop an instance that is still `pending`, so a release that runs straight after
a start can fail.

For any other task leg (a `train/lora` fine-tune since #188), the cancel never reached the operator,
so a pipeline could not be stopped while its longest leg ran (#207).

## Decision

**D1 — A pipeline cancel cancels the running task legs this process executes, and waits for them
before the finalizers.** For every `running` step that is not a finalizer and whose leg task is in
the bridge's `_leg_tasks` (a `task:` leg), the bridge cancels the task and waits for all of them at
most `_TASK_LEG_CANCEL_WAIT_S` (60 s), inside the pipeline's critical section, before
`_run_finalizers`. A cancelled leg's operator sees `asyncio.CancelledError`; one that cleans up
(as `acquire` does, D3) finishes that clean-up before the teardown starts. A leg still running after
the bound is settled anyway: the teardown never waits on it forever.

**D2 — A cancelled leg settles `cancelled`.** Its step becomes `skipped` with verdict `cancelled`
(the settlement `_settle_cancelled_while_queued` gives a queued leg). Its run goes through
`RunDispatcher.cancel` (remote jobs, budget holds, the `leg.cancelled` audit with the actor) and is
then marked `cancelled`, and `run.cancelled` is emitted outside the section. When that terminal
reaches the bridge, a step a cancel already settled stays `cancelled` instead of turning `failed`.
A leg that finished before the cancel reached it keeps its own outcome.

**D3 — `compute/acquire` never leaves a start behind a cancel, and `compute/release` waits out
`pending`.** Once the start request may be with EC2, `acquire` shields it: on cancel it lets the
call finish (it runs in a worker thread a cancel cannot stop), stops the box it started, and then
lets the cancel through. A box that was already running before `acquire` is not its to stop.
`compute/release` (and that clean-up) waits at most 180 s for an instance that is `pending`, then
stops it; a box still `pending` after that gets the stop anyway, so EC2's refusal is the release's
visible error. A cancel before the start never starts the box.

**D4 — Scope.** Finalizer legs are never cancelled (#1787). A task leg owned by another process is
not in this bridge's `_leg_tasks` and is left alone, as before; the reconciler settles it if its
owner dies (ADR-0125 D4). A sync operator's worker thread cannot be interrupted: its leg settles
`cancelled` and whatever the thread returns is discarded. Cancelling a single run
(`POST /runs/{id}/cancel`) and the DBOS workflow cancel stay with #207.

## Consequences

- A pipeline cancel can take up to 60 s longer while a task leg winds down, and holds the
  pipeline's critical section while it waits. Nothing else may advance a pipeline that is being
  cancelled, so the wait blocks only that pipeline.
- `test_cancel_leaves_a_live_local_running_leg_to_finish` encoded the old rule and becomes
  `test_cancel_stops_a_live_local_running_leg`.
- No contract changes: no new store, field, status, entry point or env knob. `StepState` uses the
  existing `skipped`/`cancelled` pair, and the bound is a class constant.
- Twins: the Temporal owner refuses a plan with a `finalizer:` step at launch (ADR-0151 D10), so
  the race this ADR closes cannot happen there. Its running task legs keep today's cancel behaviour
  (cancelling the workflow cancels only the activity awaiting the run); #207 tracks them.

## Alternatives rejected

- **Cancel only an in-flight `compute/acquire`.** Smaller, but it keeps two rules for running
  legs, and #207's `train/lora` cancel still could not be reached from a pipeline.
- **Fix only the operators** (`acquire` re-checks the pipeline's state, `release` waits). `acquire`
  cannot see a pipeline cancel without new plumbing, and the release would still run while the
  start is in flight.
- **Wait outside the critical section.** Another leg's terminal could run `_settle` in the gap and
  submit the release while `acquire` is still stopping the box.
