---
title: Flows
description: A flow is a component that a pipeline leg runs. It comes in three kinds, declares what it takes and produces, and never runs on its own.
---

A **flow** is one unit of work: check a file, fine-tune an adapter, answer a
question from a corpus. A flow is a component. It runs only as a leg of a
pipeline.

## Three kinds

| Kind | File | What it is | Where it runs |
|---|---|---|---|
| `data` | `flows/<name>.yaml` | an ordered list of stages over a dataset | the dataflow engine the environment picks (built-in, Metaflow, SkyPilot) |
| `ml` | `flows/<name>.yaml` | the same shape, for training and evaluation | the ML engine the environment picks |
| agentic | `flows/<name>.md` | a markdown file: front matter plus a prompt or a state machine of nodes | cairn's LangGraph runtime |

A pipeline leg runs a data or ML flow with `flow:` and an agentic flow with
`template:`. `cairn flow ls` lists all three kinds:

```text
  hello-cairn/engine-check  kind=data stages=1
  hello-cairn/lora  kind=ml stages=1
  hello-cairn/summarise  kind=data stages=2
  hello-cairn/assistant  kind=agentic  (flows/)
  hello-cairn/nightly-job  kind=agentic  (flows/)
```

## What a flow takes and produces

A **data or ML flow** reads its settings from `$config.<key>` placeholders in
its stage list. The leg's `with:` block fills them. A key the leg leaves out is
dropped, so the stage's own default applies. The example's ML flow:

```yaml
name: lora
kind: ml
description: LoRA fine-tune a small causal LM on a JSONL of {"text": ...} rows (CPU).
stages:
  - stage: train.lora
    config:
      dataset: $config.dataset
      base_model: $config.base_model
      steps: $config.steps
      mock: $config.mock
```

When it finishes, the leg's output has `findings`: one value per stage
result, named `<stage>_<field>` with the dot in the stage name turned into
`_`, for example `noop_content_hash`. It also lists the artifacts the leg
registered (`artifact_ids`) and any dataset versions it recorded. Later legs
and gates read them.

Stages are registered by installed packages. The open-source core ships the
`noop` and `noop.slow` stages (the built-in dataflow pack) and `train.lora`
(the built-in train-local pack). [Write a flow](/guides/write-a-flow/#your-own-stage)
shows how to add your own.

An **agentic flow** is a markdown file whose YAML front matter holds a
`flow:` block: a prompt, or a state machine of nodes. Its `operation` nodes
call operators, its `prompt` nodes call a model, and their results land in the
flow's findings (`$state.findings.<name>`). The example's
`flows/assistant.md` is a state machine of both.

## A pipeline wraps a flow

To run a flow by itself, give it a one-leg pipeline. `$inherit` makes every
placeholder of the leg's flow a pipeline input, so you do not repeat them:

```yaml
name: keep-file
kind: pipeline
inputs:
  $inherit: keep
  source: {type: str, description: "The file to record."}
steps:
  - id: keep
    flow: keep
```

Starting a flow directly is refused, and the message names the fix:

```text
'hello-cairn/engine-check' is a flow (data), and a flow does not run on its own
(ADR-0106 D1): only a declared pipeline runs, so a flow with no wrapper is a
component. To run it: add hello-cairn/pipelines/engine-check.yaml — `inputs:
{$inherit: engine-check}`, one step `- id: engine-check` / `flow: engine-check`
— and start `pipeline:hello-cairn/engine-check`.
```

## The dev loop

While you write an **agentic** flow, `cairn run` can run it in your terminal,
in this process, without a pipeline:

```bash
cairn run hello-cairn/assistant --input input.json --dry-run       # the plan; nothing runs
cairn run hello-cairn/assistant --input input.json --mock          # stub model calls and mock-capable operators
cairn run hello-cairn/assistant --input input.json --interactive   # approve or reject gates at the prompt
```

These runs write **no pipeline record and no run row**. `--mock` and
`--interactive` write a local trace instead, which `cairn replay <run-id>`
prints. `--mock` returns a deterministic response for every LLM call and uses
`.mock()` for operators that declare `supports_mock`. It refuses before running
any node when a reachable operator does not declare mock support, and needs no
provider key or endpoint.

The dev loop runs agentic flows only. Try a data or ML flow through a wrapper
pipeline in the sandbox, as [Write a flow](/guides/write-a-flow/) shows.

## Next

- [Capabilities and environments](/concepts/capabilities/): which operator does the work.
- [Write a flow](/guides/write-a-flow/): a small data flow from scratch.
- [Pipeline and flow YAML](/reference/yaml/): every field.
