---
title: Pipeline and flow YAML
description: Every field of a pipeline file and a data or ml flow file, as cairn's parsers define them.
---

Both file kinds live in a pack: pipelines in `<pack>/pipelines/<name>.yaml`,
data and ml flows in `<pack>/flows/<name>.yaml`. Agentic flows are markdown
templates and aren't covered here. Unknown or misspelt fields are errors at
load, not silently ignored.

## Pipeline file

```yaml
name: check-and-keep
kind: pipeline
description: Profile a JSONL file, stop if it is too small, keep it, and hold it for a person.

inputs:
  path: {type: str, description: "JSONL file to check."}
  min_rows: {type: int, default: 10, description: "Fewest rows the file may have."}
  expectation:
    type: dict
    default: {kind: rows, roles: [{name: text, required: true}]}

governance:
  budget: {usd: 1}
  series: acme-check
  approvers: data-team

steps:
  - id: profile
    task: data/profile
    with:
      source: $input.path
      expectation: $input.expectation
  - gate: enough-rows
    depends_on: [profile]
    predicate: steps.profile.record_count >= input.min_rows
  - id: keep
    flow: keep
    depends_on: [enough-rows]
    with:
      source: $input.path
  - gate: accept
    depends_on: [keep]
    show: [profile.record_count]
```

[Write a pipeline](/guides/write-a-pipeline/) builds the full version.

### Top-level fields

| Field | Type | Meaning |
|---|---|---|
| `name` | string, required | The pipeline's name. Its ref is `<pack>/<name>`. |
| `kind` | `pipeline` | Marks the file as a pipeline. |
| `description` | string | Shown in the catalog and in `cairn pipeline show`. |
| `visibility` | `public` · `internal` · `deprecated` | Default `public`. Curation for catalogs; `cairn pipeline ls` lists every installed pipeline. |
| `inputs` | map | The launch inputs: `{name: {type, default, description}}`. See below. |
| `outputs` | map | Names this pipeline's results: `{name: <step>.<field>}`. |
| `governance` | map | `budget: {usd: N}`, the spending cap for the series; `series`, the series name the budget belongs to; `estimated_usd`, what each flow leg reserves before it starts; `approvers`, who may decide person gates that don't name their own. See [Cost and budgets](/concepts/cost/). |
| `triggers` | list | How the pipeline starts without you. See [Triggers](#triggers). |
| `outcome` | string | What a person achieves with it, in their words. The catalog groups on it. |
| `suite` | string | The catalog bucket the outcome belongs to (for example `model`, `data`, `operate`). |
| `compare` | `{before: <leg>, after: <leg>}` | Two legs the before/after view shows side by side, for example base model against tuned model. |
| `steps` | list, required | The legs. See below. |

### Inputs

Each input is `{type, default, description}`. An input without a default is
required at launch, and `cairn pipeline run` refuses to start without it.

Two reserved keys save repeating a flow's own contract:

```yaml
inputs:
  $inherit: run          # adopt the inputs of the leg with id `run`
  $hide: [mock_metrics]  # and drop these
```

### Steps

Every step is exactly one kind, named by the key that carries its target:

| Key | The leg |
|---|---|
| `flow: <name>` | runs a data or ml flow from the same pack |
| `template: <name>` | runs an agentic flow (a markdown template) from the same pack |
| `task: <operator>` | calls one operator, for example `data/profile` |
| `gate: <name>` | decides whether what follows may run; does no work |
| `loop: <name>` | repeats its own `steps:` until a condition holds |

A leg can only reference flows and templates in its own pack; a reference to
another pack is a load error.

Fields any step can carry:

| Field | Meaning |
|---|---|
| `id` | The step's id. Gates and loops are named by their `gate:` / `loop:` value instead. |
| `with` | Inputs passed to the leg. `$input.<name>` reads a pipeline input; `$steps.<id>.<field>` reads an earlier leg's output. |
| `depends_on` | Steps that must finish first. |
| `when` | An expression. False skips this leg (recorded as routed off) and the pipeline continues. |
| `label` | A human-readable name for the run page and the graph. Display only. |
| `environment` | Run this leg in a named environment instead of the pipeline's. |
| `timeout` | A limit for this leg: seconds, or a number with `d`/`h`/`m`/`s`. A value that doesn't parse is a load error. |
| `finalizer` | `true` runs this leg once when the pipeline ends, whether it completed, failed or was cancelled, for example to release a machine. It must be safe to run twice. |
| `trigger` | When the leg fires once its dependencies are met: `auto` (default, at once), `manual` (waits for a person to release it), `schedule:<cron>` (waits for the next tick) or `on:<topic>` (waits for that event on the bus). |
| `emits` | An event topic published on the bus when the leg completes, with the leg's output as the payload. `run.*` is reserved. |
| `consumes` / `produces` | Name the artifacts the leg reads and writes, for wiring and lineage. |

Gate fields (see [Gates and approvals](/concepts/gates/)):

| Field | Meaning |
|---|---|
| `predicate` | An expression. True passes the gate at once; false stops the pipeline. A data gate. |
| `approvers` | Who may decide: usernames or roles. Without it, `governance.approvers` applies. A person gate. |
| `show` | `<step>.<field>` values shown to the approver. Any other form is a load error. |

`predicate` and `approvers` can't both be set on one gate.

Loop fields:

| Field | Meaning |
|---|---|
| `steps` | The legs repeated on each pass. |
| `until` | An expression; the loop ends when it is true. |
| `max_iterations` | Required. The most passes the loop may make. |
| `on_exhausted` | `gate` (default): when the passes run out, a person decides. `fail`: the pipeline fails. |

### Expressions

`when`, `predicate` and `until` are JMESPath expressions over the pipeline's
inputs and its steps' outputs, checked when the file loads:

```yaml
predicate: steps.profile.verdict == 'pass' && steps.profile.record_count >= input.min_rows
when: input.publish
until: steps.score.findings.passed
```

Write a literal number in backticks: `` input.rows > `1000` ``.

### Triggers

A pipeline can start on its own. Each entry has a `kind`:

| Kind | Starts the pipeline | Main fields |
|---|---|---|
| `manual` | only when someone launches it (the default) | `description` |
| `schedule` | on a cron schedule | `cron`; `inputs`; `environment`; `misfire` (`skip`, `fire_once` or `fire_all`); `enabled` (default `false`) |
| `webhook` | when a matching request arrives | `source` (a vendor preset, or `custom` with a `path`); `path`; `match`; `input` (maps request fields to inputs); `enabled` (default `true`) |
| `watch` | when a watched source changes or a signal crosses a threshold | `source` (`kind: dataset`, `s3` or `table`); `every`; `when`; `on_change` (`propose`, `start` or `none`); `inputs`; `enabled` (default `false`) |
| `event` | when another pipeline's leg `emits` the topic | `topic`; `input`; `enabled` (default `false`) |

With auth on (`CAIRN_JWT_SECRET` set), a webhook answers a sender without a
bearer token only when its preset requires a signature (Slack, PagerDuty,
GitHub) and the request carries a valid one. A preset whose signature is
optional (`custom`, `gmail`) keeps the token check, so call it with
`Authorization: Bearer <token>`.

The Gmail preset is the one this affects in practice: Google's Pub/Sub push authenticates with
its own OIDC token in `Authorization`, which is not a cairn token, so on a deployment with
sign-in on, Gmail pushes are refused. Until cairn verifies Google's token itself, run Gmail
push only on a deployment with sign-in off behind a private network, or relay it through a
proxy that holds a cairn token.

A `dataset` watch is armed by the open-source core; an `s3` or `table` watch
needs cairn-platform's governed import. A pipeline may not start on a topic
its own legs emit.

```yaml
triggers:
  - kind: schedule
    cron: "0 3 * * *"
    misfire: skip
```

## Data and ml flow file

```yaml
name: summarise
kind: data
description: Carry the nightly report through the built-in dataflow engine.

stages:
  - stage: noop
  - stage: noop.slow
    config:
      seconds: $config.seconds
```

From the example pack's `flows/summarise.yaml`, with a placeholder added.

| Field | Meaning |
|---|---|
| `name` | The flow's name. A pipeline references it as `flow: <name>`. |
| `kind` | `data` (produces a dataset) or `ml` (produces a model). The flow never names an engine; the environment maps each kind to one. |
| `description` | Shown in `cairn flow ls`. |
| `stages` | Required, at least one. Run in order over one working dataset. |
| `handoff` | `{export: <stage>, ingest: <stage>}`. Needed only when a stage declares `gate:`, so the working dataset can cross the pause. |
| `checks` | `{<check>: not_applicable}` declares that a known check, such as `decontam`, doesn't apply to what this flow produces. It is recorded on the leg's evidence. |

Each stage:

| Field | Meaning |
|---|---|
| `stage` | The stage's registered name, for example `noop` or `train.lora`. |
| `config` | The stage's settings. `$config.<name>` reads the value the pipeline passed in `with:`. |
| `resources` | What the stage needs to run, for engines that schedule resources. |
| `checkpoint` | `true` saves the working set after this stage, so a restart resumes from here. Off by default. |
| `gate` | An approval required before this stage runs. |
| `map` | Fans the stage out over a list at run time: `over` (the list), `as` (the item name, default `item`), `max_parallel` (default 4, at most 64) and `merge`. |

## Next

- [Write a pipeline](/guides/write-a-pipeline/)
- [Write a flow](/guides/write-a-flow/)
- [CLI](/reference/cli/)
