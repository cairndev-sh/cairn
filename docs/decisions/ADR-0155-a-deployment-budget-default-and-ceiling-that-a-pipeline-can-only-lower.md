# ADR-0155 — A deployment budget: a default and a ceiling that a pipeline can only lower

- **Status:** ACCEPTED 2026-10-28. Approval: maintainer, 2026-10-28 (the recommendation in each open question taken as the decision).
- **Date:** 2026-10-28
- **Issues:** cairndev-sh/cairn#250 (a deployment-level default and ceiling), cairndev-sh/cairn#251
  (price a flow leg on the local machine); related: cairndev-sh/cairn#237 (legs that `when:` routes
  off are priced).
- **Amends:** the Budget row of `docs/contracts/governance.md` (the cap no longer comes only from
  `governance.budget`); the profile schema (a new closed `[profile.budget]` table);
  `PipelineRecord.admission` (an additive `budget` entry).
- **Keeps:** ADR-0151 D1–D5 and ADR-0152 (what is priced, the bounded worst case, fail-closed
  reading, per-attempt settle), ADR-0106 D4 (a pipeline states intent, a profile states machinery),
  ADR-0112 D1 (environment-owned governance configuration is profile scope), the one
  `reserve`/`settle` path, the platform's `run.admitting` series provider.

## Context

A deployment cannot say "nothing in this environment runs above $X". The core has no such
setting. The only cap a launch checks is the one the pipeline file declares:

- `plan_pipeline` copies `governance` from the definition verbatim
  (`src/cairn/core/engine/pipeline.py:1198`). `_price_and_check` reads `governance.budget.usd` and
  checks only when it is set (`src/cairn/core/dispatch/pipeline_launch.py:513-515`). Both pipeline
  owners reserve against the same field off the record: the bridge
  (`src/cairn/core/dispatch/pipeline_bridge.py:2510`) and the Temporal activity
  (`src/cairn/core/runtime/temporal/pipeline_activities.py:304`).
- The series cap and the starter budget are platform providers on the `run.admitting` and
  `workspace.created` hooks (`src/cairn/core/dispatch/dispatcher.py:932-949`). A public-only
  install has neither. `src/cairn/core/policy/` has no rule that reads a budget.
  `settings.max_cost_usd` (`src/cairn/core/provider_settings.py:109`) is a per-request LLM backstop.
  The launch door never reads it.
- So the public example's training pipeline is bounded only by its own `budget: {usd: 1}`
  (`examples/hello-cairn/pipelines/tiny-finetune.yaml:52`). A deployment owner has to review every
  installed pipeline file, and review them again after every pack upgrade.
- A flow leg the builtin engine runs on the local machine is never priced. `price_plan` skips a leg
  with no target (`src/cairn/core/dispatch/leg_hooks.py:257`), and only a `compute.ssh` connection
  carries a rate (`leg_hooks.py:96`). No connection kind stands for the engine's own machine
  (`src/cairn/core/connections.py:46-53`).

**DOCUMENTATION / IMPLEMENTATION CONFLICT.** The explain view says a workspace's `cost_cap_usd`
variable can supply a pipeline's budget (`src/cairn/core/engine/pipeline_resolved.py:279`,
`:288-295`). The launch door never reads that variable for a pipeline, and `POST /start` refuses
`cost_cap_usd` on a pipeline as run-only (`src/cairn/core/triggers/routers/start.py:57`). D7
resolves this.

## Decision

**D1 — The deployment declares its budget in the profile.** A new closed table:

```toml
[profile.budget]
default_usd      = 20     # the cap for a pipeline that declares no governance.budget
ceiling_usd      = 100    # the most any launch in this deployment is capped at
local_hourly_usd = 0.80   # D6: the rate of the engine's own machine
```

Each key is optional and must be a finite number ≥ 0. An unknown key fails parsing, as in every
other profile table (`src/cairn/core/pack/profile.py:88`). A `default_usd` above `ceiling_usd` also
fails parsing. Under `extends`, a child profile's value replaces its parent's, as for other
scalars. The profile is the place because ADR-0112 D1 puts environment-owned governance
configuration there, and the budget is one value per deployment, read once at launch.

**D2 — The effective cap is the smaller one; a pipeline can only lower it.**

| pipeline declares | deployment sets | effective cap | `source` |
|---|---|---|---|
| `usd: P` | nothing | P | `pipeline` |
| `usd: P` | `ceiling_usd: C` | min(P, C) | `pipeline` or `deployment_ceiling` |
| nothing | `default_usd: D` (with or without C) | D | `deployment_default` |
| nothing | `ceiling_usd: C` only | C | `deployment_ceiling` |
| nothing | nothing | unbounded, as today | — |

A pipeline that declares more than the ceiling is not refused for that. It runs under the
ceiling, and the record says so.

**D3 — One function computes it at the launch door, and the door records it.**
`effective_budget(governance, profile)` (dispatch, beside `_price_and_check`) returns `{usd,
source, declared_usd, default_usd, ceiling_usd}`. The door writes it to
`PipelineRecord.admission.budget` before any record exists. Nothing re-derives it later.
`_price_and_check` checks the estimate against this figure: against what is left in the series, or
against the cap alone when there is no series (ADR-0152 D3). `governance` stays the pipeline's own
declaration, verbatim.

**D4 — Every reader of the cap reads the recorded figure.** One helper, `budget_cap_usd(rec)`,
returns `admission.budget.usd` when it is present. Otherwise it returns `governance.budget.usd`
(records admitted before this ADR). These read it:

- the bridge's reservation (in-process and DBOS);
- the Temporal activity;
- the gate's budget evidence (`_budget_context`, `pipeline_bridge.py:3440`);
- the explain view.

A profile change after launch does not move the cap of a pipeline that is already running.

**D5 — What the cap bounds is unchanged.** The cap bounds exactly what `governance.budget.usd`
bounds today:

- the door's check of the priced legs' estimate (ADR-0151 D2, ADR-0152 D2);
- the reservations of legs under a series.

A pipeline whose legs are all unpriced still records its effective cap. Under a series it still
reserves `governance.estimated_usd`. A launch is not refused for declaring no budget: the
deployment's default or ceiling gives it one. What the door prices is decided elsewhere. If
cairndev-sh/cairn#237 lands, routed-off legs are not priced, and this cap is checked against
whatever estimate the door computes.

**D6 — The engine's own machine can carry a rate.** When `local_hourly_usd` is set, a flow leg
that the builtin engine runs locally (no target) is priced through the same `price_flow_leg` path.
The leg is recorded with connection `"local"` and basis `"$X/h × Hh on the local machine
(profile)"`. A priced local leg follows the same rules as a priced remote one:

- it must declare a `timeout` (ADR-0151 D3);
- it reserves the recorded estimate;
- it settles `rate × wall-clock` per attempt on every outcome (ADR-0151 D5, ADR-0152 D4).

When the key is unset, a local leg is skipped as today. A `worker:` pool and every non-builtin
engine stay unpriced (ADR-0152 D1). There is no new `compute.local` connection kind. A connection
is account-scoped (ADR-0143), the local machine belongs to the deployment, and the builtin engine
never resolves a connection for an empty target.

**D7 — A workspace's `cost_cap_usd` is not a pipeline budget layer.** The explain view stops
showing that variable as a source of a pipeline's budget. It shows the deployment layer (D2) in its
place. The variable keeps its run-dialog role for workflow runs.

**D8 — Off by default.** With no `[profile.budget]`, a deployment behaves exactly as it does today.
The door runs the same checks, and the record gets no `admission.budget` entry.

## Consequences

- A deployment owner can bound every launch with one profile setting. A pipeline edit can lower
  the bound but cannot raise or remove it.
- An approver at a gate sees the effective cap and where it came from. They no longer see only
  the file's number.
- Two launches in one series may run under different caps when the profile changed between them.
  Each launch checks the series' remaining budget against its own recorded cap.
- With a `local_hourly_usd`, a laptop or workstation training leg is estimated and can be refused,
  and it must declare a timeout. Pipelines that launched before can be refused now. The release
  notes say so.
- Task legs and LLM template legs are still not priced at the door (recorded as partial in
  `08-governance.md`). The per-request `max_cost_usd` is unchanged.

## Alternatives rejected

- **A policy rule (`effect: require` on a budget, or a cap obligation).** Policies match legs
  (`applies_to.pipeline_legs`, `src/cairn/core/policy/pipeline_gates.py`) and packs can ship them
  (`pack_policies`), but a budget is one figure per launch that the environment owns. This would
  need a new rule effect in the locked policy shape (ADR-0020 §24.20.9).
- **An environment variable or a `settings` field.** It would add an env knob (§5), and
  `max_cost_usd` already means a per-request LLM backstop.
- **Leave it to the platform's hooks.** A public-only deployment would then have no bound, and
  every adopter would write their own hook.
- **The workspace variable `cost_cap_usd` as the layer.** It is a run-dialog default that any
  writer of the workspace can change. It is not deployment authority.
- **A `compute.local` connection kind with `hourly_usd`.** It reuses the pricing code, but it
  makes the deployment's machine an account resource, and it teaches the builtin engine to treat
  one connection kind as "no dispatch".
- **Refuse a priced launch that declares no budget.** This is stricter, but it breaks every
  existing pipeline that relies on the default.

## Migration

Nothing to migrate. The profile table is opt-in (D8). Records admitted before this ADR have no
`admission.budget`, and `budget_cap_usd` reads their `governance.budget.usd` as today. The platform
picks this up when it bumps its core pin. Its series provider receives the effective figure
through the same `budget_usd` argument.

## Test plan

- `effective_budget`: every row of the D2 table; `default_usd > ceiling_usd`, an unknown key, a
  negative value and NaN are each refused at profile parse.
- Launch door (`tests/core/test_gpu_leg_pricing.py` beside the existing budget tests): with a
  `ceiling_usd` and no `governance.budget`, an estimate above the ceiling is refused (403 / exit
  3); a declared cap above the ceiling is checked against the ceiling; a lower one is checked
  against itself.
- Record: `admission.budget` holds the figure and its source. Changing the profile after launch
  leaves a running pipeline's reservations on the recorded figure.
- Owner parity: the bridge and the Temporal activity reserve against the same recorded cap.
- Local rate: a no-target flow leg is priced at `local_hourly_usd × timeout`; an untimed leg is
  refused; a failed attempt settles its wall-clock; an unset rate leaves `price_plan` returning
  `{}` as today.
- Explain view: shows the deployment layer and no longer shows a workspace `cost_cap_usd` for a
  pipeline.
- No `[profile.budget]`: the existing budget and pricing tests pass unchanged.

## Open questions for the maintainer

1. Profile (recommended) or a policy rule as the home of the default and ceiling?
2. A declared cap above the ceiling: run under the ceiling (recommended), or refuse the launch?
3. A launch with no declared budget: apply the default (recommended), or offer a
   `require_declared = true` that refuses it?
4. Under `extends`: may a child profile raise its parent's `ceiling_usd`, or only lower it?
5. A local rate in `[profile.budget]` (recommended), or a `compute.local` connection kind?
6. D7: drop the workspace `cost_cap_usd` from the pipeline explain view (recommended), or make it a
   third layer that can only lower the cap?
