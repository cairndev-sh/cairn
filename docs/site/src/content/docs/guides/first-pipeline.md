---
title: Run your first pipeline
description: Find a pipeline, see what it will do before it spends, launch it from the CLI or the HTTP API, follow it, answer its gates and read its outputs.
---

This guide runs the example pack's `hello-cairn/check-and-publish`
pipeline: it checks a JSONL file, stops if the check fails, and publishes a
summary after a person approves. The same steps work for any pipeline in the
[catalog](/reference/catalog/) or in your own pack.

You need cairn installed with the `hello` extra. `cairn setup` from the
[Quickstart](/quickstart/) starts an engine and signs you in. For a deployed server,
connect with `cairn setup --server <url>` instead.

## 1. Find it and see what it will do

`cairn pack ls --pipelines` lists the installed pipelines with their legs and content
hash, and `cairn pack show` prints one pipeline's inputs, legs and gates:

```text
$ cairn pack ls --pipelines
  hello-cairn/ask-the-docs  3 legs (gate,task,template)  sha256:4d1cf10680a88e38
  hello-cairn/nightly-ops  4 legs (flow,task,template)  sha256:63601bd0b441add9
  hello-cairn/over-budget  1 legs (flow)  sha256:6564ee0f0d22af39
  hello-cairn/check-and-publish  9 legs (flow,gate,task)  sha256:52dccf2087b839f5
  hello-cairn/tiny-finetune  9 legs (flow,gate,task)  sha256:1c4102fa7e4528d0
```

`cairn run --dry-run` goes further. For each leg it shows the file or
operator that will run, the engine, every gate on the way and why it applies,
and each input and where its value comes from. It runs nothing and spends
nothing:

```text
$ cairn run --dry-run hello-cairn/check-and-publish -e sandbox
pipeline:hello-cairn/check-and-publish  —  Profile a small JSONL dataset, stop unless the profile passes, …
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

**Against an engine.** Once `cairn setup` has signed you in, launch:

```bash
cairn run hello-cairn/check-and-publish --preset sample --environment sandbox
```

`--preset <name>` starts from a set of inputs, here the one the example ships, which
points at its own sample rows. Each `-i key=value` sets one input on top of it, and
`--save-preset <name>` saves this launch's inputs for next time.

**Over the HTTP API.** `POST /start` with `{"ref": "hello-cairn/check-and-publish", "inputs": {…}}`
goes through the same launch door.

**With no engine running.** `cairn run <ref> -e sandbox -i key=value` starts a local
engine on `~/.cairn/dev.db` for the length of the command, and goes through the same door.
The record stays, and `cairn decide` starts the engine again to answer. An endpoint you
have signed in to fails instead when it is down, so run `cairn setup` again after a reboot.

## 3. Follow it, and answer its gates

`cairn run` prints each leg as it changes, and asks at each gate:

```text
  paused
    profile            done                 run=run-task-data--profile-1f9a14
    data-ok            done                 run=None
    redact             done                 run=run-task-redact--text-85ecfb
    engine-check       done                 run=run-flow-hello-cairn--engine-check-55b570
    stage              done                 run=run-task-storage--put-1295cd
    review             awaiting_approval    run=None
    …
  gate review  (approvers: data-owners)
    profile.verdict       pass
    profile.record_count  20
    …
→ approve   cairn decide pl_3f5b6ec6a25d414d approve --step review --approver you
→ findings  cairn status pl_3f5b6ec6a25d414d --results
→ watch     cairn status pl_3f5b6ec6a25d414d --follow
Approve this step? [a]pprove / [r]eject / [s]kip (detach)
```

Answer `a` to approve or `r` to reject. With `s` the pipeline stays waiting. With no terminal
to ask on it stays waiting too, and `cairn run` exits 2. These answer from any shell:

```bash
cairn decide <pipeline_id> approve --step review
cairn status <pipeline_id> --json          # the record, with every step's state
```

To stop instead, `cairn decide <pipeline_id> cancel`. See
[Gates and approvals](/concepts/gates/) for who may decide.

## 4. Read the outputs

`cairn status --results` prints every leg's outputs:

```text
$ cairn status pl_3f5b6ec6a25d414d --results
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

## What was recorded

The launch in step 2 wrote to the engine's store (`~/.cairn/dev.db` under
`cairn setup`). The `--dry-run` in step 1 wrote nothing: `cairn run --dry-run`,
`--mock` and `--interactive` start no pipeline and leave no record.

**The pipeline record** (`pl_…`) holds the plan of nine legs (`profile`,
`data-ok`, `redact`, `engine-check`, `stage`, `review`, `promote`, `publish`,
`audit`), the inputs you passed, the governance (budget $1, series `hello`,
approvers `data-owners`) and, for each step, its status, run id, verdict and
approver. Read it with `cairn status <pipeline_id> --json`.

**Runs.** The seven legs that did work each have their own run, such as
`run-task-data--profile-…` and `run-flow-hello-cairn--engine-check-…`. The gates
`data-ok` and `review` have none. `cairn status <run-id> --logs` prints a run's
event log.

**Artifacts.**

- `engine-check` registers the file you passed as an artifact, whose id is its
  content hash, and as a version of the dataset `hello-sample`. Its findings
  `artifact_ids`, `dataset_versions` and `noop_content_hash` name them. A file
  that has not changed adds no new version.
- `promote` points the tag `hello-sample@published` at that artifact once you
  approve it (`promote.artifact_id`, `promote.tag`).
- `stage` and `publish` write the redacted summary, as `summary`, to the stores
  `hello-staging` and `hello-published` under `~/.cairn/artifacts`
  (`CAIRN_ARTIFACTS_DIR`). `stage.uri` and `publish.uri` say where.

`cairn status <pipeline_id> --results` prints all of these.

**Audit rows.** These exist only when the profile the engine runs under binds
`audit.sink`. The `hello.toml` from the Quickstart does; `local-dev` does not.
With it, the log (`./audit.jsonl`, or `CAIRN_AUDIT_LOG`) gets `pipeline.admitted`
at launch, `leg.submitted` and `leg.finished` for each of the seven legs,
`pipeline.gate_approved` for `review` and again for `promote`, each with the
signed-in user as `actor`, `leg.promoted` for `promote`, and `pipeline.finished`
at the end. [What a pipeline run writes](/concepts/runs/#what-a-pipeline-run-writes)
lists the fields.

The `audit` leg adds a row of its own, `hello.dataset.published`, whose target
is `publish.uri`. It is an operator call, so it is written whether or not a
sink is bound, and `audit.hash` in the results is its hash. Check the chain
with:

```bash
cairn admin audit verify
```

If you reject at `review`, the pipeline ends `failed`, and `pipeline.gate_rejected`
joins the rows. If `data-ok` is false (`-i min_rows=1000`), no later leg starts,
so there is no dataset version and no summary.

## Next

- [Write a pipeline](/guides/write-a-pipeline/): your own, with gates.
- [Pipeline catalog](/reference/catalog/): what else you can run.
- [Runs and the record](/concepts/runs/): everything the run left behind.
