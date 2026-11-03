---
title: Write a pipeline
description: Build your own pipeline step by step, with a task leg, a data gate, a flow leg and a person gate, put it where cairn finds it, check it and run it.
---

You will build `acme/check-and-keep`: it profiles a JSONL file, stops if the
file is too small, records it as a version of a named dataset, and waits for a
person to accept it. Along the way you will see where pipeline files live, how
legs are wired, and how to check a pipeline before it runs.

## 1. Create a pack

Pipelines and flows live in a **pack**: a directory with a `pack.toml`, a
`pipelines/` directory and a `flows/` directory. `cairn pack new pack` creates one:

```bash
cairn pack new pack acme
```

The pack name must be a valid Python identifier (`snake_case`). The scaffold
contains, among other files:

```text
acme/
  __init__.py
  pack.toml
  pyproject.toml
  flows/hello.md
  pipelines/hello.yaml
  policies/default.yaml
```

`policies/default.yaml` is a starter policy that asks for approval on every
action. Delete it for this guide, or keep it and expect an extra approval
before each step.

## 2. Make cairn find the pack

cairn only sees pipelines that belong to a registered pack. There are two ways
to register one:

- Point `CAIRN_PACK_PATH` at the directory that contains the pack. Several
  entries are separated by `:`.

  ```bash
  export CAIRN_PACK_PATH=$PWD     # the directory that contains acme/
  ```

- Or install it as a package (`pip install -e acme`), which registers it
  through its `cairn.packs` entry point.

Start the engine with the variable set (if one is running, `cairn setup --stop` first; a second `cairn setup` starts nothing), or restart the server, and check:

```text
$ cairn pack ls --pipelines
  acme/hello  1 legs (template)  sha256:a7040e93b0b05eba
  …
```

## 3. Add the flow it runs

A pipeline can only use flows from its own pack. Add `acme/flows/keep.yaml`, a
one-stage data flow ([Write a flow](/guides/write-a-flow/) explains it):

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

## 4. Write the pipeline

Create `acme/pipelines/check-and-keep.yaml`. The file name must match `name:`.

```yaml
name: check-and-keep
kind: pipeline
description: Profile a JSONL file, stop if it is too small, keep it as a dataset version, and hold it for a person.

inputs:
  path: {type: str, description: "JSONL file to check, as the engine sees it."}
  name: {type: str, default: "acme-sample", description: "Dataset to record the file under."}
  min_rows: {type: int, default: 10, description: "Fewest rows the file may have."}
  expectation:
    type: dict
    description: "The columns the data must have, as roles."
    default:
      kind: rows
      roles:
        - {name: text, required: true}

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
    predicate: steps.profile.verdict == 'pass' && steps.profile.record_count >= input.min_rows

  - id: keep
    flow: keep
    depends_on: [enough-rows]
    with:
      source: $input.path
      register: $input.name

  - gate: accept
    depends_on: [keep]
    show: [profile.record_count, profile.note, keep.noop_content_hash]
```

Piece by piece:

- **`inputs`** are what the person launching the pipeline provides. An input
  with no `default` is required. A `-i key=value` value is typed against the
  input's declared `type`: an `int`, `float` or `bool` is parsed, a `dict` is
  JSON, and a `list` is JSON (`-i 'report_to=["mlflow"]'`) or a comma-separated
  string (`-i report_to=mlflow,wandb` gives `["mlflow", "wandb"]`; items are
  trimmed and empty items dropped). A list of numbers or maps, and an empty
  list (`[]`), still need JSON; `-i report_to=` with no items is refused.
  The launch door does this typing for the CLI, the console and schedules, and
  `cairn run --dry-run -i` shows the typed value.
- **`governance`** sets the pipeline's budget and names who may decide its
  person gates.
- **`profile`** is a task leg: it calls one operator, `data/profile`, which
  reads the file and checks it has the roles in `expectation`.
- **`enough-rows`** is a data gate. Its `predicate:` is a JMESPath expression
  over `input` and `steps`. If it is false, the pipeline stops.
- **`keep`** is a flow leg. `with:` fills the flow's `$config.source` and
  `$config.register` placeholders from the pipeline inputs.
- **`accept`** is a person gate. `show:` lists the fields the approver sees,
  as `<leg>.<field>`; for a flow leg, cairn looks in the leg's findings for you.

To find a leg's field names, run the pipeline once and read
`cairn status <pipeline-id> --results`.

:::caution[Name pipelines and agentic flows differently]
`POST /start` and `cairn run` refuse a name that matches both a
pipeline and an **agentic** flow (`flows/<name>.md`) in the same pack: the
scaffold's own `acme/hello` is one. The message says how to name the
pipeline explicitly:

```text
'acme/hello' names BOTH a pipeline and a flow/template. Use `pipeline:acme/hello`
to run the pipeline, or the flow door to run the flow — cairn will not guess which you meant.
```

Give your pipelines their own names. A data flow with the same name does not
cause this.
:::

## 5. Check it before it runs

`cairn pack show` prints the legs and gates. `cairn run --dry-run`
resolves everything the launch will resolve, without running anything:

```text
$ cairn run --dry-run acme/check-and-keep -e sandbox -i path=/data/chats.jsonl
…
  [gate] enough-rows  gate:enough-rows   after profile
       predicate: steps.profile.verdict == 'pass' && steps.profile.record_count >= input.min_rows  → STOP (gate_blocked) when false
…
  [gate] accept  gate:accept   after keep
       gate[this leg] accept                       human (HITL)             authority=pipeline def — approvers: data-team
…
```

`cairn pack check acme` checks every file in the pack offline. Mistakes fail
when the file is read, with the file and field named. Unknown fields are
rejected, and so is a gate that has both kinds of answer:

```text
pipelines/t.yaml: gate 'g' declares both `predicate:` and `approvers:` — a gate is answered by data OR a human, never both. Split into two gates if you need both verdicts.
```

## 6. Run it

```bash
curl -LsSfO https://raw.githubusercontent.com/cairndev-sh/cairn/main/examples/hello-cairn/data/sample.jsonl
cairn run acme/check-and-keep -i path=$PWD/sample.jsonl --environment sandbox
```

```text
  paused
    profile            done                 run=run-task-data--profile-e7c767
    enough-rows        done                 run=None
    keep               done                 run=run-flow-acme--keep-0a95e9
    accept             awaiting_approval    run=None
  gate accept
    …
→ approve   cairn decide pl_1a812abc98244cf1 approve --step accept --approver you
→ findings  cairn status pl_1a812abc98244cf1 --results
→ watch     cairn status pl_1a812abc98244cf1 --follow
Approve this step? [a]pprove / [r]eject / [s]kip (detach)
```

The data gate passed on its own. Answer `a` to accept the result, or from any shell:

```bash
cairn decide pl_1a812abc98244cf1 approve --step accept
```

```text
approved — pipeline status=done
```

With `-i min_rows=1000` the data gate is false and the pipeline fails at
`enough-rows`; `keep` never runs.

## Next

- [Write a flow](/guides/write-a-flow/): stages, placeholders and findings.
- [Pipeline and flow YAML](/reference/yaml/): every field.
- [Gates and approvals](/concepts/gates/): policy gates and durable pauses.
