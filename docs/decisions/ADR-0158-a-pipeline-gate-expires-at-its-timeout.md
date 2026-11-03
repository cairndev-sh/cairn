# ADR-0158 — A pipeline gate expires at its declared timeout

- **Status:** PROPOSED 2026-10-29. Approval: pending maintainer.
- **Date:** 2026-10-29
- **Issue:** cairndev-sh/cairn#259
- **Amends:** the pipeline YAML (`on_expiry:` on a `gate:` step), the approval reaper's scope
  (`src/cairn/core/approval/reaper.py`), and the audit vocabulary (`approval.expired` gains
  `pipeline_id` / `step_id` for a pipeline gate).
- **Keeps:** ADR-0156 D5 (the pipeline-gate reject, `PipelineBridge.reject_step`, and its Temporal
  `reject_step` signal), ADR-0034 (the run-level operation gate and its `on_expiry`), and the
  reaper's run sweep (`expire_stale_approvals`, `CAIRN_APPROVAL_TTL_HOURS`) for runs.

## Context

A `gate:` step may declare `timeout: 7d`. The plan keeps it as a raw string
(`src/cairn/core/engine/bundle_pipeline.py`, `PipelinePlanStep.timeout`), and
`gate_context_for` puts it on the approver's card (`src/cairn/core/engine/pipeline_gate.py`).
Nothing enforces it:

- The approval reaper expires `awaiting_approval` **runs** in the `RunStore`
  (`src/cairn/core/approval/reaper.py`, `expire_stale_approvals`), on one global TTL. A pipeline
  gate leg creates no run (`src/cairn/core/dispatch/pipeline_bridge.py`, "A gate leg has no
  executable"); it is a `StepState` in the `PipelineRecord`, which the reaper never reads.
- A pipeline gate has no expiry policy. `ApprovalGate.on_expiry` exists only for the
  operation gate (`src/cairn/core/approval/resolver.py`).

So a gate waits until someone approves, rejects or cancels. Until this ADR is accepted and built,
the YAML reference says so (#259).

## Decision (proposed)

**D1 — The deadline.** A gate step's deadline is `awaiting_since + timeout`. `timeout` comes from
the step, else from the pipeline's `governance.timeout`, parsed with the same grammar as any leg
timeout (seconds or `d`/`h`/`m`/`s`). No timeout means no deadline, as today.

**D2 — The effect is declared, and fails closed.** A gate step may set
`on_expiry: reject | escalate`; the default is `reject`. `approve` is not offered: a deadline
must never turn into consent.

- `reject` ends the gate through `PipelineBridge.reject_step` with
  `decider = "system:expiry"` and `reason = "timeout <value> elapsed"`. The step is `skipped`
  with the verdict `rejected` and the pipeline fails, exactly as a person's reject (ADR-0156 D5).
- `escalate` keeps the gate waiting, emits `approval.escalated` once, and notifies through the
  bound `notify.sink` (`[profile.notify]`). It does not expire again.

**D3 — One mechanism for both owners.** The sweep runs in the control plane beside the run
sweep, started and stopped with it in `triggers/server.py`. It lists pipelines with a gate at
`awaiting_approval` whose deadline has passed and calls `reject_step`. Under a Temporal owner,
`reject_step` already records the verdict and sends the `reject_step` signal, so the durable
workflow needs no timer of its own and stays deterministic.

**D4 — Audit.** Every expiry writes `approval.expired` with `pipeline_id`, `step_id`, the
declared timeout and the effect, on the hash-chained trail (ADR-0028).

**D5 — Concurrency.** The sweep takes the pipeline's section (`PipelineBridge._lock`), so an
expiry and a person's approve on the same gate are serialised. Whoever is first wins; the second
gets the existing "not awaiting approval" refusal.

## Open questions (for the maintainer)

1. Is `escalate` needed in the first slice, or does `reject` alone ship first? Recommendation:
   `reject` first. It is the fail-closed half, and `escalate` depends on notify routing.
2. The sweep interval: reuse the run sweep's, or a separate knob? Recommendation: reuse it; a
   new environment knob needs its own approval (CLAUDE.md §5).

## Consequences

- A pipeline whose gate is never answered fails at its deadline, with an attributable verdict,
  instead of waiting forever.
- The approver's card shows a deadline that is now true.
- Tests: a gate past its deadline is rejected by `system:expiry` under the in-process owner and
  under Temporal; a gate with no timeout never expires; an approve racing the sweep is
  serialised.
