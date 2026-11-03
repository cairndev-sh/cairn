---
title: Quickstart
description: Install cairn, start a local engine, launch the example data pipeline, decide at its gates and read the record.
---

In about ten minutes you will install cairn, start an engine on your machine,
launch a pipeline that checks a dataset, decide at its gates, and read what was
recorded. Nothing here needs an API key or spends money.

To look around without installing, try the live demo at
[demo.cairndev.sh](https://demo.cairndev.sh), a shared sandbox (don't enter
real credentials there).

## 1. Install cairn

```bash
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_EXTRAS=hello sh
```

The installer puts `cairn` on your PATH as a [uv](https://docs.astral.sh/uv/)
tool, installing uv first if it is missing. The `hello` extra adds the example
pack, `hello-cairn`, with five pipelines to try. Without the script:

```bash
pip install 'cairndev[hello]'   # inside a virtual environment, Python 3.12 or later
```

Check it: `cairn --version`, then `cairn pack ls --pipelines` lists the example's
pipelines.

## 2. Get the example's profile

The `profile-and-publish` pipeline checks a JSONL file whose rows have a
`text` and a `label` field. The example ships a sample, which `--preset sample` picks
below. Get its deployment profile, which turns the audit trail on:

```bash
mkdir cairn-quickstart && cd cairn-quickstart
curl -LsSfO https://raw.githubusercontent.com/cairndev-sh/cairn/main/examples/hello-cairn/profiles/hello.toml
export CAIRN_PROFILE=$PWD/hello.toml
```

## 3. See what will happen before anything runs

```bash
cairn run --dry-run hello-cairn/profile-and-publish -e sandbox --preset sample
```

`--dry-run` resolves everything a launch would, and runs nothing. For each leg
it prints the operator or flow file it will run, the engine, the gate that
answers it, and where each input value comes from:

```text
  [task] profile  task:data/profile  (entrypoint)
  [gate] data-ok  gate:data-ok   after profile
       predicate: steps.profile.verdict == 'pass' && steps.profile.record_count >= input.min_rows  → STOP (gate_blocked) when false
  [task] redact  task:redact/text   after data-ok
  [flow] engine-check  flow:hello-cairn/engine-check   after redact
  …
```

## 4. Start an engine

```bash
cairn setup --port 8471
```

`cairn setup` starts the engine in the background on a local SQLite database (`~/.cairn/dev.db`
unless you pass `--db`), with the in-process runtime and sign-in on, as the
user `dev` with the password `dev`. It also signs the CLI in, so the
next commands need no flags; `cairn setup --stop` stops the engine. `hello.toml` binds `audit.sink`, which the default
`local-dev` profile leaves off.

## 5. Launch the pipeline

```bash
cairn run hello-cairn/profile-and-publish \
  --preset sample --environment sandbox
```

The pipeline profiles the file, passes its **data gate** (the profile must
pass and count at least 10 rows), redacts the summary, runs a flow leg that
records the file as a version of the dataset `hello-sample`, stages the
summary, and stops at `review`, a **person gate**:

```text
  paused
    profile            done                 run=run-task-data--profile-1f9a14
    data-ok            done                 run=None
    redact             done                 run=run-task-redact--text-85ecfb
    engine-check       done                 run=run-flow-hello-cairn--engine-check-55b570
    stage              done                 run=run-task-storage--put-1295cd
    review             awaiting_approval    run=None
    promote            pending              run=None
    publish            pending              run=None
    audit              pending              run=None
  gate review  (approvers: data-owners)
    profile.verdict       pass
    profile.record_count  20
    …
→ approve   cairn decide pl_3f5b6ec6a25d414d approve --step review --approver you
→ findings  cairn status pl_3f5b6ec6a25d414d --results
→ watch     cairn status pl_3f5b6ec6a25d414d --follow
Approve this step? [a]pprove / [r]eject / [s]kip (detach)
```

Every leg that did work has its own run. The data gate has none: it was
decided from the profile's numbers, so nobody was asked.

## 6. Decide at the gates

`cairn run` is asking: answer `a`, and `a` again at the next stop. With `s`, or with no
terminal to ask on, the pipeline stays waiting, and these answer from any shell:

```bash
cairn decide <pipeline_id> approve --step review       # the person gate
cairn decide <pipeline_id> approve --step promote      # the operator's own gate
```

The pipeline stops a second time, at `promote`. That leg moves the governed
tag `hello-sample@published`, and its operator, `artifact/promote`, asks for
approval on every call. A pipeline can add a gate, but it can never remove
one an operator or a policy demands. cairn records each decision under the
identity you signed in with.

After the second approval the pipeline publishes the summary and writes an
audit row, and ends `done`.

## 7. Read the record

```bash
cairn status <pipeline_id> --results  # what every leg produced
cairn status <pipeline_id> --json     # the whole record: plan, inputs, every step's state
```

```text
pipeline pl_3f5b6ec6a25d414d  status=done
  profile  [done] run=run-task-data--profile-1f9a14
    record_count      20
    verdict           pass
  …
  promote  [done] run=run-task-artifact--promote-3b4d2f
    artifact_id           sha256:d2c09d27b2874cee
    promoted              True
    tag                   hello-sample@published
  …
  audit  [done] run=run-task-audit--store-39b32a
    hash  af362a96212cf0d5298dfe71e81b5109d08a0219f3079387550e753f2fa25466
```

The audit trail is in `./audit.jsonl`, next to where the engine runs
(`CAIRN_AUDIT_LOG` moves it). It has rows for the launch, each step and each decision
a person made (`pipeline.gate_approved`, with the signed-in user as `actor`), and the
row the pipeline's own `audit` step writes, whose hash the record above shows. Each row is
chained to the one before it by its hash, and `cairn admin audit verify` checks the file.
[The audit trail](/concepts/runs/) lists every row. The
staged and published summaries are under `~/.cairn/artifacts`
(`CAIRN_ARTIFACTS_DIR`).

## Try the other outcomes

- `-i min_rows=1000`: the data gate is false, so the pipeline fails at
  `data-ok` (`gate_blocked`) before any later step starts. The audit trail still
  records the launch and the failure.
- `--environment hello-prod`: the example pack's policy makes every
  `storage/put` in that environment wait for a person, though the pipeline
  file has no gate there.
- `cairn run hello-cairn/over-budget --environment sandbox`: a pipeline whose
  estimate ($5) is above its budget ($1). The flow leg's reservation fails, so
  its engine never starts, nothing is spent, and the command exits 1:

  ```text
  pipeline_id=pl_592c6c850aec40a5 status=running
    failed
      engine-check       failed               run=run-flow-hello-cairn--engine-check-78f6c9
    engine-check: series 'hello-over-budget': this run's estimate $5.00 would exceed the $1.00 cap ($0.00 spent, $0.00 reserved by in-flight runs). Raise the budget or start a new series.
  → logs      cairn status run-flow-hello-cairn--engine-check-78f6c9 --logs   # leg engine-check
  → findings  cairn status pl_592c6c850aec40a5 --results
  ```

  The pipeline file declares that estimate, and the flow itself would cost
  nothing. The launch is admitted and the leg is refused, so the audit trail has
  `leg.budget_refused`, with the estimate and the cap, rather than
  `pipeline.refused`. This pipeline needs cairndev 0.5.0 or later. See
  [Cost and budgets](/concepts/cost/).

## Next

- [Pipelines, legs and steps](/concepts/pipelines/): what you just ran.
- [Write a pipeline](/guides/write-a-pipeline/): your own pipeline, with a data gate and a person gate.
- [Install and deploy](/install/): run cairn for a team.
