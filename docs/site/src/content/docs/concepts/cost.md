---
title: Cost and budgets
description: Cap what a pipeline may spend, reserve a flow leg's estimate before it starts, stop on a price with a data gate, and read what every run cost.
---

cairn treats cost as something you decide on before the money is spent, and
check afterwards. Three mechanisms work together: a budget the pipeline
declares (and the deployment can bound), a reservation every flow leg takes against it before it starts, and
data gates that stop on a number an earlier leg produced.

## Budgets on the pipeline

A pipeline declares its budget in `governance`:

```yaml
governance:
  budget: {usd: 25}
  series: nightly-batch
  estimated_usd: 4
  approvers: data-team
```

- `budget.usd` is the pipeline's spending cap.
- `series` names a group of runs that share that cap.
- Before a flow leg starts its engine, it **reserves** the pipeline's
  `estimated_usd` against the series cap. The reservation is atomic (a lock
  per series on Postgres), so two legs cannot both take the last dollar.

When a reservation would go over the cap, the leg fails before it spends, with
this message (values filled in):

```text
series '<series>': this run's estimate $<estimate> would exceed the $<cap> cap
($<spent> spent, $<held> reserved by in-flight runs). Raise the budget or start a new series.
```

A finished leg settles its reservation. A failed or cancelled leg releases it.
A retried attempt reserves again, because a retry is new spend.

## A budget for the whole deployment

A deployment owner can set a default and a ceiling in the profile, so no
launch in that environment runs above a figure the pipeline file cannot raise
or remove:

```toml
[profile.budget]
default_usd      = 20     # the cap for a pipeline that declares no governance.budget
ceiling_usd      = 100    # the most any launch in this deployment is capped at
local_hourly_usd = 0.80   # the rate of the engine's own machine (see below)
```

Each key is optional and must be a finite number of at least 0; 0 means not
set. An unknown key, or a `default_usd` above `ceiling_usd`, fails when the
profile loads. Under `extends`, a child profile's value replaces its parent's.

The cap a launch is held to is the smaller one:

| The pipeline declares | The profile sets | The cap is | Recorded as |
|---|---|---|---|
| `usd: P` | nothing | P | `pipeline` |
| `usd: P` | `ceiling_usd: C` | the smaller of P and C | `pipeline` or `deployment_ceiling` |
| nothing | `default_usd: D` | D | `deployment_default` |
| nothing | `ceiling_usd: C` only | C | `deployment_ceiling` |
| nothing | nothing | no cap, as before | none |

A pipeline that declares more than the ceiling is not refused for that. It
runs under the ceiling, and its record says so. The launch door computes the
cap once and writes it, with its source, to `admission.budget` on the
pipeline record. The reservations, the Temporal activity, the approval
gates' cost line and `cairn pipeline explain` read that figure. Changing the
profile later does not move the cap of a pipeline that is already running.
Records written before this setting existed keep using their own
`governance.budget.usd`.

The cap bounds what `governance.budget.usd` bounded before: the launch door's
estimate of the priced legs, and the reservations of legs under a `series`. It
does not make task legs or model-call legs priced at launch.

With no `[profile.budget]`, nothing changes. `cairn pipeline explain` shows
the cap the launch door enforces: the pipeline's figure, or the deployment's
default or ceiling. A workspace's `cost_cap_usd` variable is a run-dialog
default for workflow runs and is not a layer of a pipeline's budget.

### Pricing the engine's own machine

By default a flow leg that the builtin engine runs on its own machine, with no
compute target, is not priced. Set `local_hourly_usd` and such a leg is priced
like one on a `compute.ssh` box, on a connection recorded as `local`: its
estimate is `local_hourly_usd × timeout`, it must declare a `timeout`, it
reserves the recorded estimate, and it settles the rate times its wall-clock
on every outcome. A launch whose estimate does not fit the cap is refused
before any record exists. Pipelines that launched before can be refused after
you set a rate, if a local leg declares no `timeout` or does not fit. A
`worker:` pool and any engine other than the builtin one stay unpriced.

## Stop on the price: a data gate

cairn does not guess a price for you. Pricing is a step in the pipeline: a leg
whose output carries the number, and a data gate that reads it. With your own
pricing operator or flow stage in a leg called `estimate`:

```yaml
  - gate: cost
    depends_on: [estimate]
    predicate: steps.estimate.usd <= input.max_cost_usd

  - id: run
    flow: batch-job
    depends_on: [cost]
```

When the estimate is above `max_cost_usd`, the gate blocks and the pipeline
stops before the leg that spends. A data gate can't be approved past, so
nobody can wave an over-budget run through. Put a person gate after it if the
spend also needs a person's agreement.

The operators that price model calls and GPU runs (token estimates, GPU
estimates and live GPU quotes, and a threshold gate over them) are part of
cairn-platform.

## What a run cost

Every run records its cost when it finishes, split into model calls, GPU and
storage, plus a total (`llm_usd`, `gpu_usd`, `storage_usd`, `total_usd`).
Model calls are priced as they are made; GPU and storage come from what the
operator reports. The series total is what the next reservation is checked
against.

## Next

- [Gates and approvals](/concepts/gates/): the gates that read these numbers.
- [Runs and the record](/concepts/runs/): where the cost is kept.
- [Pipeline and flow YAML](/reference/yaml/): every `governance` field.
