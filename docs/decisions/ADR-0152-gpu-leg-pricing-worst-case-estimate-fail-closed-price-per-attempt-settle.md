# ADR-0152 — GPU leg pricing: worst-case estimate, fail-closed price, per-attempt settle

- **Status:** ACCEPTED 2026-10-24 by the owner (review decisions on #211).
- **Date:** 2026-10-24
- **Issue:** cairndev-sh/cairn#200 (PR #211)
- **Amends:** ADR-0151 D1 (which targets are priced and what an unreadable price means), D2 (what
  the launch door checks and against which cap) and D5 (settled per attempt, and how an adopted
  attempt settles). ADR-0151 is locked; this ADR records how its decisions read.
- **Keeps:** ADR-0151 D3 (a priced leg declares a timeout), D4 (the estimate on the gate before
  the first priced leg) and D6–D11; the one `reserve`/`settle` path; the estimate recorded once on
  `PipelineRecord.admission.cost_estimate` and never re-derived.

## Context

Implementing ADR-0151 D1–D5 (#211) raised five questions it did not answer. What does an
unreadable price mean? Which targets are connections at all? A legacy Metaflow profile uses
`<kind>_target` as its metadata root. Which cap applies when a pipeline declares a budget but no
series? What should the estimate be for a leg that runs more than once, in a loop or on retry?
And how does an attempt adopted after a restart settle, when its window starts at the leg's
`run.started_at` and the run's earlier attempts have already been settled?

## Decision

**D1 — Only the builtin engine's connection target is priced, and its price is read
fail-closed.** Pricing follows the engine the environment selects for the flow's kind
(`engine_registry.selected_engine`). Only the builtin engine dispatches to a `compute.ssh`
connection, so only its target is looked up. Any other engine's target, such as a legacy Metaflow
metadata root, reads "not priced (engine X does not dispatch to a connection)" and launches as
before. On the builtin path the launch door refuses, with a named reason that says to retry once
the price can be read, when:
- the engine or target selection cannot be read;
- the connection store cannot be read;
- the named target connection is not registered;
- `hourly_usd` is malformed (not a number, negative, NaN or infinite).

"Not priced" applies only when the case is definite: no `hourly_usd` (unset or 0), a connection of
another kind, or a `worker:<pool>` target; each gets a "not priced" entry in the estimate. A leg
with no target at all (the local engine) gets no entry: it is left out of the estimate entirely.

**D2 — The door checks the bounded worst case; a leg reserves one attempt.** The door's estimate
for a leg is `hourly_usd × timeout × loop max_iterations × attempts`:
- `max_iterations` is 1 outside a loop;
- attempts are a declared `retry: {max_attempts}`, else the executor's default, which is 1 for
  `ml` and 2 for `data`.

The basis text names the factors. At leg time each loop iteration and each attempt reserves only
its own per-attempt figure (`per_attempt_usd = hourly_usd × timeout`).

**D3 — A budget with no series is checked at the door alone.** When `governance.budget.usd` is
set and no `series` is, the door checks the estimate against the cap alone. The legs reserve
nothing, because they reserve only under a series, so the door is the only budget check for such
a pipeline. That asymmetry is deliberate and documented.

**D4 — Box time is measured and settled per attempt.**
- Each attempt measures its own window (attempt start → attempt end) and is accounted once, after
  its outcome is known. The window joins the leg's `Run.cost.gpu_usd`, then the attempt's hold is
  settled at its cost, or released when it cost nothing.
- A ledger error is logged and named in the leg's error, appended so a later attempt does not
  erase it. It never changes the engine outcome or the box time.
- The backoff between two fresh attempts is not billed. That is a known under-count: the box is
  up while the leg waits to retry.

**D5 — An adopted attempt settles what is left of its window.**
- An adopted attempt has no reservation of its own. Its window runs from the persisted
  `run.started_at`, because the start of each attempt is not persisted.
- When it ends, it closes the hold its dead process left (`held_for_run`). When it has a window,
  the first held row is settled at `engine-reported cost + window − what the run's claims have
  already settled` (`settled_usd_for_run`), floored at 0; any further held row is settled at 0.
  The run's settled rows therefore add up to the adopted attempt's cost, and nothing is settled
  twice. When it has no window (no readable `run.started_at`, or an unpriced leg), nothing
  overlaps the earlier attempts and the engine-reported cost is settled as is. If the ledger
  cannot be read, the rows stay held at their estimate and the leg's error names it.
- `settled_usd_for_run` is a read-only sum: Postgres sums the run's `settled` rows; the in-memory
  backend keeps the total only while the run still holds a claim and drops it with the run's
  last hold, so its state stays bounded by the runs in flight (after a restart it has neither
  holds nor totals).
- The adopted window covers the earlier attempts and the backoff between them. The leg's
  `Run.cost.gpu_usd` is therefore an over-estimate of the box time. That is accepted: an
  over-estimate, never a double count.

## Consequences

- A profile that selects Metaflow and uses `<kind>_target` as its metadata root keeps launching,
  unpriced.
- A builtin SSH launch whose price cannot be read is refused instead of running unpriced. It
  passes once the price is readable.
- A looped or retrying priced leg can now be refused at the door for its worst case, even if one
  attempt would fit.
- A pipeline with a budget and no series is enforced at launch only.
- `budget_reservations` gains `settled_usd_for_run`. It is a read over existing state: the
  `settled` rows carry `run_id` in Postgres, and the in-memory backend keeps a per-run sum only
  while the run holds a claim. There is no new store or table.

## Alternatives rejected

- **Treat an unreadable price as "not priced".** That runs a GPU leg with no budget check
  whenever the store is unavailable: unknown read as free.
- **Estimate one attempt at the door.** A three-iteration loop could then spend three times what
  the door admitted.
- **Persist each attempt's start in the engine handle.** It would give an exact adopted window,
  but it changes the documented `{job_id, engine, meta}` handle shape for a small gain.
