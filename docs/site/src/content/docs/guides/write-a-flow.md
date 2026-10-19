---
title: Write a flow
description: Write a small data flow, wrap it in a one-leg pipeline, run it in the sandbox, read its findings, and add a stage of your own.
---

You will write `acme/keep`: a data flow that records a file's content as a
version of a named dataset. Then you will wrap it so it can run, try it in the
sandbox, and see how to add a stage of your own. This guide assumes the `acme`
pack from [Write a pipeline](/guides/write-a-pipeline/) is registered.

## 1. Pick the stages

A data flow is an ordered list of **stages**. Each stage is a function that an
installed package registers. These ship inside the `cairndev` wheel (the pack is
its directory under `packages/`):

| Stage | Pack | Does |
|---|---|---|
| `noop` | `cairn-operator-dataflow` | nothing to the data; with `register:` and `source:` it records `source`'s content as a version of the dataset `register` names |
| `noop.slow` | `cairn-operator-dataflow` | sleeps `seconds:` (default 30), for testing long legs and cancellation |
| `train.lora` | `cairn-operator-train-local` | LoRA fine-tunes a small model on CPU (an `ml` stage; the `train` extra) |

Anything else is a stage you or an operator pack provide; see
[Your own stage](#your-own-stage). A flow's risk is the highest risk among its
stages, and that is what a gate checks before the leg starts.

## 2. Write the flow

Create `acme/flows/keep.yaml`:

```yaml
name: keep
kind: data
description: Record a file's content as a version of a named dataset.

stages:
  - stage: noop
    config:
      source: $config.source
      register: $config.register
```

- `name` is the flow's address inside the pack: `acme/keep`.
- `$config.source` and `$config.register` are placeholders. Whoever runs the
  flow fills them. A placeholder nobody fills is dropped, and the stage uses
  its default.
- The same content always gives the same version: running it twice on an
  unchanged file adds no new version.

`cairn flow ls` should now list it:

```text
  acme/keep  kind=data stages=1
```

## 3. Wrap it in a pipeline

A flow never runs on its own. Give it a one-leg pipeline,
`acme/pipelines/keep-file.yaml`:

```yaml
name: keep-file
kind: pipeline
description: Record one file as a dataset version.

inputs:
  $inherit: keep
  source: {type: str, description: "The file to record."}
  register: {type: str, default: "acme-sample", description: "The dataset name."}

steps:
  - id: keep
    flow: keep
```

`$inherit: keep` makes every placeholder of the `keep` leg's flow a pipeline
input and wires it through, so the leg needs no `with:` block. Declaring
`source` and `register` next to it gives them a type, a default and a
description.

```text
$ cairn pipeline show acme/keep-file
acme/keep-file  —  Record one file as a dataset version.
  hash: sha256:4574efcf2bee1cc8
  inputs: register, source

  •  keep [flow] acme/keep
```

## 4. Try it in the sandbox

```bash
cairn remote launch acme/keep-file -i source=$PWD/train.jsonl --environment sandbox --watch
```

Then read what the stage reported:

```text
$ cairn remote findings pl_2066d9105a384e3e
pipeline pl_2066d9105a384e3e  status=done
  keep  [done] run=run-flow-acme--keep-eb33b0
    artifact_ids                 [1 items]   (--json for the full value)
    dataset_versions             [1 items]   (--json for the full value)
    noop_bytes                   2404
    noop_content_hash            sha256:e52492fd607ee355
    noop_register_as             acme-sample
    noop_rows                    0
    stages_done                  1
    stages_total                 1
```

Every stage's results appear as `<stage>_<field>`, with the dot in the stage
name turned into `_`. These are the names a gate's `predicate:` reads under
`steps.<leg>.findings` and its `show:` list reads as `<leg>.<field>`.
`artifact_ids` lists what the leg registered, which a later
`artifact/promote` leg can tag.

## Your own stage

A stage is a plain Python function, registered under the `cairn.dataflow_stages`
entry point of an installed package:

```python
# acme_stages/count.py
from pathlib import Path


def count_lines(workdir: Path, config: dict, inputs: list[str]) -> dict:
    """Count the lines of `config["source"]`."""
    text = Path(config["source"]).read_text(encoding="utf-8")
    return {"rows": len(text.splitlines())}
```

```toml
# pyproject.toml of the package that holds it
[project.entry-points."cairn.dataflow_stages"]
"acme.count" = "acme_stages.count:count_lines"
```

Every stage of a leg gets the same working directory, its own `config` with
the placeholders filled, and the leg's input artifacts. It returns a dict of
scalar results, which become the leg's findings (`acme_count_rows` here, 48
for the example's `train.jsonl`); an
`output` key names a file it wrote. Install the package where the engine runs
(`pip install -e .`), restart the engine, and `- stage: acme.count` works in
any flow. A dev pack on `CAIRN_PACK_PATH` cannot register a stage: entry points
come only from installed packages. The built-in engine, Metaflow and SkyPilot
all run the same registered functions.

## ML flows and agentic flows

An **ML flow** has the same shape with `kind: ml`, and runs on the ML engine
the environment configures. The example's `hello-cairn/lora` runs one
`train.lora` stage, and its `tiny-finetune` pipeline gates on the held-out
loss before it tags the model.

An **agentic flow** is a markdown file, `flows/<name>.md`, run by a
`template:` leg. You can iterate on one without a pipeline using the dev loop,
`cairn run <pack>/<flow> --input in.json --dry-run` (or `--mock`,
`--interactive`); see [Flows](/concepts/flows/#the-dev-loop).

## Next

- [Write a pipeline](/guides/write-a-pipeline/): put flows and gates together.
- [Pipeline and flow YAML](/reference/yaml/): every flow and stage field.
- [Capabilities and environments](/concepts/capabilities/): where the work runs.
