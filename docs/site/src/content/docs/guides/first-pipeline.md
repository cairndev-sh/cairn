---
title: Run your first pipeline
description: Find a pipeline, see what it will do before it spends, launch it from the CLI or the HTTP API, follow it, answer its gates and read its outputs.
---

This guide runs the example pack's `hello-cairn/profile-and-publish`
pipeline: it checks a JSONL file, stops if the check fails, and publishes a
summary after a person approves. The same steps work for any pipeline in the
[catalog](/reference/catalog/) or in your own pack.

You need cairn installed with the `hello` extra and a running engine:
`cairn dev --engine-only` from the [Quickstart](/quickstart/), or a deployed
server.

## 1. Find it and see what it will do

`cairn pipeline ls` lists the installed pipelines with their legs and content
hash, and `cairn pipeline show` prints one pipeline's inputs, legs and gates:

```text
$ cairn pipeline ls
  hello-cairn/ask-the-docs  3 legs (gate,task,template)  sha256:4d1cf10680a88e38
  hello-cairn/nightly-ops  4 legs (flow,task,template)  sha256:d59da1bb89fa22e6
  hello-cairn/over-budget  1 legs (flow)  sha256:6564ee0f0d22af39
  hello-cairn/profile-and-publish  9 legs (flow,gate,task)  sha256:52dccf2087b839f5
  hello-cairn/tiny-finetune  9 legs (flow,gate,task)  sha256:1c4102fa7e4528d0
```

`cairn pipeline explain` goes further. For each leg it shows the file or
operator that will run, the engine, every gate on the way and why it applies,
and each input and where its value comes from. It runs nothing and spends
nothing:

```text
$ cairn pipeline explain hello-cairn/profile-and-publish -e sandbox
pipeline:hello-cairn/profile-and-publish  —  Profile a small JSONL dataset, stop unless the profile passes, …
  environment: sandbox    runtime=inprocess  storage=…  bus=in-process
  governance:  {'budget': {'usd': 1}, 'series': 'hello', 'approvers': 'data-owners'}

  INPUTS (value ← layer)
    path                     examples/hello-cairn/data/sample.jsonl    ← default (def)
    min_rows                 10                                        ← default (def)
    …
  [task] profile  task:data/profile  (entrypoint)
       risk=read_only capability=data.profile
  [gate] data-ok  gate:data-ok   after profile
       predicate: … → STOP (gate_blocked) when false
  …
```

The default `path` is relative to the engine's working directory, so pass
your own file with `-i path=…` unless the engine runs from a checkout of the
repository.

## 2. Launch it

**Against a server.** Sign in once, then launch:

```bash
cairn remote login -e http://localhost:8471 -u dev -p dev
cairn remote launch hello-cairn/profile-and-publish \
  -i path=$PWD/sample.jsonl --environment sandbox --watch
```

Each `-i key=value` sets one input. `--preset <name>` starts from a saved set
of inputs, and `--save-preset <name>` saves this launch's inputs for next
time.

**Over the HTTP API.** `POST /start` with `{"ref": "hello-cairn/profile-and-publish", "inputs": {…}}`
goes through the same launch door.

**In this process.** `cairn pipeline run <ref> -e sandbox -i key=value`
launches through the same door without a server and stays attached until the
pipeline ends. Set `CAIRN_STORAGE` first, so the record and any dataset
versions it registers persist. It suits pipelines without person gates: a
person gate pauses it, and the approval has to come through a server on the
same storage.

## 3. Follow it, and answer its gates

`--watch` prints each leg as it changes, and stops when the pipeline pauses
or ends:

```text
  paused
    profile            done                 run=run-task-data--profile-1f9a14
    data-ok            done                 run=None
    redact             done                 run=run-task-redact--text-85ecfb
    engine-check       done                 run=run-flow-hello-cairn--engine-check-55b570
    stage              done                 run=run-task-storage--put-1295cd
    review             awaiting_approval    run=None
    …
⏸ paused — 1 step(s) waiting on a human
```

Approve the waiting step, and follow the pipeline again:

```bash
cairn remote advance <pipeline_id> review
cairn remote pipeline <pipeline_id>          # the record, with every step's state
```

To stop instead, `cairn remote cancel-pipeline <pipeline_id>`. See
[Gates and approvals](/concepts/gates/) for who may decide.

## 4. Read the outputs

`cairn remote findings` prints every leg's outputs:

```text
$ cairn remote findings pl_3f5b6ec6a25d414d
pipeline pl_3f5b6ec6a25d414d  status=done
  profile  [done] run=run-task-data--profile-1f9a14
    note              20 records · format completion · all roles resolved
    record_count      20
    verdict           pass
  …
  engine-check  [done] run=run-flow-hello-cairn--engine-check-55b570
    artifact_ids                 [1 items]   (--json for the full value)
    dataset_versions             [1 items]   (--json for the full value)
    noop_content_hash            sha256:d2c09d27b2874cee
    noop_register_as             hello-sample
  …
```

`--json` prints the full values. The field names here are what a later leg
wires with `$steps.<leg>.<field>`, and what a gate's `show:` lists.

## Next

- [Write a pipeline](/guides/write-a-pipeline/): your own, with gates.
- [Pipeline catalog](/reference/catalog/): what else you can run.
- [Runs and the record](/concepts/runs/): everything the run left behind.
