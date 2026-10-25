# ADR-0151 — A GPU leg is priced, and its box is started and stopped by the pipeline

- **Status:** ACCEPTED 2026-10-24 by the owner (decision 2a; review decisions D-A to D-I, all as
  recommended).
- **Date:** 2026-10-24
- **Issues:** cairndev-sh/cairn#200 (pricing), cairndev-sh/cairn#201 (box lifecycle);
  cairndev-sh/cairn-extensions#26, #27 (the `sft` pipeline).
- **Amends:** ADR-0097 D1 (where `compute/acquire` and `compute/release` live); the budget row of
  `docs/contracts/governance.md` and the spend section of `docs/contracts/execution.md` (a failed
  paid leg records its spend); the public-core vocabulary list (`compute` becomes a public prefix
  for exactly two refs).
- **Keeps:** ADR-0097 D2–D5 (the approval gate before a paid start, finalizer teardown, the lease
  and sweep, the target binding), ADR-0120 D7 (operator packs import only the operator SDK),
  ADR-0125 (adoption, restart and the reconciler), ADR-0146 (the admission record shown at the
  first gate), the one `reserve`/`settle` path.
- **Builds on:** ADR-0149 (cancel authenticates with the job's connection credential).

## Context

A flow leg on a `compute.ssh` box spends money for as long as the box is up, but the budget never
saw it. Both pipeline owners reserve the pipeline-wide `governance.estimated_usd`, which is 0 unless
set; the builtin SSH engine reports no cost; a failed leg releases its reservation as "no spend
occurred"; and `flow_leg_record` writes no `Run.cost` for a failed leg. A twelve-hour fine-tune
settled at $0 against a $25 budget. The connection already carries `hourly_usd`, `instance_id` and
`region` (`compute_lifecycle.ComputeBox`), and `cairn compute status` / `sweep` are public, but the
two operators that start and stop the box exist only in a private pack, so a public pipeline could
not start a box after approval or stop it when the run ended, failed or was cancelled.

## Decision

**D1 — Dispatch prices a leg whose compute target is a priced `compute.ssh` connection.** One core
function computes `estimate = hourly_usd × timeout` for every flow leg whose target names a
connection with `hourly_usd`. It reads the connection config core-to-core, the way `target_cap`
reads `max_in_flight`; no operator pack and no engine is involved. Both pipeline owners (the bridge
and the Temporal activity) call the same function. A target with no `hourly_usd` is **not
priced**: its record says so, never $0.

**D2 — The estimate is checked at the launch door and recorded at admission.** Before any record
exists, the door sums the priced legs' estimates against the series' remaining budget
(`remaining_usd`, read-only, no reservation) and refuses the launch when it does not fit. The
per-leg estimate, the rate, the hours and the connection are recorded on `PipelineRecord.admission`
and never re-derived: a leg reserves the recorded figure even if the connection's rate changes
after launch.

**D3 — A priced leg must declare a timeout.** A priced flow leg with no `timeout` is refused at the
launch door with "declare a timeout to price this leg"; the seven-day default is not used as a
price.

**D4 — The estimate is shown to the person who approves the spend.** The gate before the first
priced leg carries `estimate_usd` and `estimate_basis` in its context `evidence` (the path gate
evidence already uses), not through `show:`.

**D5 — A priced leg settles its wall-clock on every outcome.** At any terminal state (success,
failure, timeout, cancel) the leg settles `hourly_usd × (ended − run.started_at)` through the
existing `settle`, and `flow_leg_record` writes `Run.cost.gpu_usd` on success and on failure.
`release` stays for legs with nothing billed. An adopted leg takes its start from the persisted
`run.started_at`, never a process-local timer, so a restart does not lose the spend.

**D6 — `compute/acquire` and `compute/release` move into the public `cairn-operator-remote` pack.**
Same refs, schemas, risk classes and capabilities as the platform's 0.1.0 operators (`acquire`:
`paid_compute`, rated high, passes `enforce_gate`; `release`: `external_write`, idempotent, never
approval-gated). The `compute_lifecycle` functions they need are re-exported through
`cairn.core.operator`, the operator SDK, so the pack keeps ADR-0120 D7. The vocabulary list moves
`compute` from a private prefix to two exact public refs (`compute/acquire`, `compute/release`);
`compute/estimate`, `compute/actual` and `compute/quote` stay private. The platform drops its two
copies in the same release, because both installed would fail with `DuplicateOperatorError`. This
amends ADR-0097 D1 ("the existing operator-compute pack").

**D7 — No `implements` contract URI yet.** The two operators ship as the platform's do, without a
contract URI; a `cairn-contracts-compute` package is a follow-up on the contract track.

**D8 — `acquire` stamps the lease before it starts the box.** Tag first, then start, so a crash
between the two cannot leave a billing box that `cairn compute sweep` cannot see.

**D9 — Cancel stops the job before the box.** On a pipeline cancel, the running flow legs on the
box are cancelled through `cancel_recorded_job` (authenticated with the job's connection
credential, ADR-0149 D4) before `release` stops the box. A box stop is never the only kill.

**D10 — Under the Temporal pipeline owner, a plan with a `finalizer:` step is refused at launch**
until the workflow implements finalizers (it schedules by `depends_on` only, so a trailing teardown
would run on success alone). In-process and DBOS keep finalizers as today.

**D11 — The training pipeline releases the box as soon as it is done with it.** A pipeline that
acquires a box (the `sft` pipeline, cairn-extensions#26) has `acquire` after the approval gate, a
`release` right after its last GPU leg (so the box does not bill while a person decides on
promotion), and a finalizer `release` last as the backstop; `max_hours` on `acquire` is the legs'
timeouts plus a margin; a `compute_connection` input names the box and must equal the
environment's `ml_target`.

## Consequences

- A GPU run's record shows its estimate at admission, the figure the approver saw, and the settled
  spend per leg, including a leg that failed. Insights and later launches in the series see the
  real spend.
- A launch whose priced legs don't fit the budget, or whose priced leg has no timeout, is refused
  before any record; pipelines that launched before may be refused now (release notes).
- Task legs still reserve nothing (recorded as partial in `08-governance.md`); `acquire` is a paid
  task leg on top of that. The minutes between `acquire` and the first flow leg, and after the last
  one until `release`, are unpriced; D11 keeps them short.
- Under a profile that requires approval for `paid_compute`, `acquire` pauses for its own approval
  after the pipeline's approval gate. That is correct and is documented.
- The SkyPilot engine keeps its own `hourly × seconds` estimate; two copies of one formula are
  recorded as debt until it calls the core function.
- The live proof (a real box started, a run, a stop) is owed by the qualification run
  (cairn-extensions#9), on the owner's approval.

## Alternatives rejected

- **The trainer reports `cost_usd`.** No core change, but it measures trainer time, not box time,
  gives no estimate before submit (no refusal, nothing for the approver) and records nothing on
  failure.
- **SkyPilot as the GPU path.** Already prices and tears down, but pipeline legs never set its
  budget, it reports no cost, and ADR-0097 kept the pet box for good reasons.
- **Estimate at admission only.** Smallest, but spend never reaches `Run.cost`, so the series still
  reads $0 spent.
- **A new public `cairn-operator-compute` pack.** Its import package would clash with the
  platform's `operator_compute`.
