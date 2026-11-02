# examples/

## `hello-cairn/`: the example pack, start here

[`hello-cairn/`](hello-cairn/README.md) is a small domain pack that runs on the engine
and its builtin operator packs alone (no other pack, no ML dependency, no API key). Its
pipeline profiles a sample dataset, stops at a **data gate**, runs a flow leg, stages a
summary, pauses at a **person gate**, and publishes only after approval, under a declared
**budget**. Load it with `CAIRN_PACK_PATH=examples/hello-cairn`, or install it as the
`cairn-pack-hello` wheel. Its README is the quickstart walkthrough. Tests:
`tests/examples/test_hello_cairn.py`.

For how a template is written, read its
[`flows/assistant.md`](hello-cairn/flows/assistant.md) (a model and tools) and
[`flows/nightly-job.md`](hello-cairn/flows/nightly-job.md) (operations, no model).

## `ci/`: a gate in CI

[`ci/`](ci/README.md) has GitHub Actions and GitLab CI jobs that block a merge when a
dataset fails a read-only check, and the check itself. They need only
`pip install cairndev`.

## `python/sweep/`: a hyperparameter sweep around cairn

[`python/sweep/`](python/sweep/) shows the shape of a sweep where Optuna or W&B Sweeps does
the search and calls cairn once per trial. These scripts are out of date and don't run as
they are: each trial calls `cairn run training/finetune --input <file>`. `cairn run` takes a
file as `--input @file` and starts a pipeline rather than a bare flow, and no pack here
provides that flow. To use the idea, have each trial start a declared pipeline.

## `sample-alert.json`

A sample alert payload, the shape of a file you'd pass as `cairn run --input @file`. No
pack in this repository reads it.

## Budgets in a template

A template can declare a `budget:` block. A template without one sets no limit of its own:

| Field | Status |
|---|---|
| `max_wall_seconds` | Enforced. The run is cancelled at the limit and recorded as `budget_exceeded`. |
| `max_cost_usd` | Enforced after each LLM call. The call's token spend is added to the running total, and `BudgetExceeded` is raised when the total crosses the cap, so a run can overshoot by the call that crossed it. |
| `max_iterations` | Enforced for the critique loop. |
| `max_tokens` | Declared only. Nothing stops a run on it. |

A pipeline's own budget is a different mechanism: `governance.budget`, reserved before each
flow leg starts. See [Cost and budgets](../docs/site/src/content/docs/concepts/cost.md).
