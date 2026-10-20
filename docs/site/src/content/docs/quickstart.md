---
title: Quickstart
description: Install cairn, start a local engine, launch the example data pipeline, decide at its gates and read the record.
---

In about ten minutes you will install cairn, start an engine on your machine,
launch a pipeline that checks a dataset, decide at its gates, and read what was
recorded. Nothing here needs an API key or spends money.

## 1. Install cairn

```bash
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_EXTRAS=hello sh
```

The installer puts `cairn` on your PATH as a [uv](https://docs.astral.sh/uv/)
tool, installing uv first if it is missing. The `hello` extra adds the example
pack, `hello-cairn`, with four pipelines to try. Without the script (until the
packages are on PyPI, cairn's wheels come from the GitHub Release; third-party
packages still come from PyPI):

```bash
L=https://github.com/cairndev-sh/cairn/releases/expanded_assets/v0.1.3
pip install 'cairndev[hello]==0.1.3' --find-links "$L"   # inside a virtual environment, Python 3.12 or later
```

Check it: `cairn --version`, then `cairn pipeline ls` lists the example's
pipelines.

## 2. Get a dataset to check

The `profile-and-publish` pipeline checks a JSONL file whose rows have a
`text` and a `label` field. Use the example's sample:

```bash
mkdir cairn-quickstart && cd cairn-quickstart
curl -LsSfO https://raw.githubusercontent.com/cairndev-sh/cairn/main/examples/hello-cairn/data/sample.jsonl
```

## 3. See what will happen before anything runs

```bash
cairn pipeline explain hello-cairn/profile-and-publish -e sandbox -i path=$PWD/sample.jsonl
```

`explain` resolves everything a launch would, and runs nothing. For each leg
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
cairn dev --engine-only --port 8471 &
cairn remote login -u dev -p dev -e http://localhost:8471
```

`cairn dev` starts the engine on a local SQLite database (`~/.cairn/dev.db`
unless you pass `--db`), with the in-process runtime and sign-in on, as the
user `dev` with the password `dev`. `remote login` stores the token, so the
next commands need no flags.

## 5. Launch the pipeline

```bash
cairn remote launch hello-cairn/profile-and-publish \
  -i path=$PWD/sample.jsonl --environment sandbox --watch
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
⏸ paused — 1 step(s) waiting on a human
```

Every leg that did work has its own run. The data gate has none: it was
decided from the profile's numbers, so nobody was asked.

## 6. Decide at the gates

```bash
cairn remote advance <pipeline_id> review       # the person gate
cairn remote advance <pipeline_id> promote      # the operator's own gate
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
cairn remote findings <pipeline_id>     # what every leg produced
cairn remote pipeline <pipeline_id>     # the whole record: plan, inputs, every step's state
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

The audit row is in `./audit.jsonl`, next to where the engine runs
(`CAIRN_AUDIT_LOG` moves it), chained to the row before it by its hash. The
staged and published summaries are under `~/.cairn/artifacts`
(`CAIRN_ARTIFACTS_DIR`).

## Try the other outcomes

- `-i min_rows=1000`: the data gate is false, so the pipeline fails at
  `data-ok` (`gate_blocked`) before anything is written.
- `--environment hello-prod`: the example pack's policy makes every
  `storage/put` in that environment wait for a person, though the pipeline
  file has no gate there.

## Next

- [Pipelines, legs and steps](/concepts/pipelines/): what you just ran.
- [Write a pipeline](/guides/write-a-pipeline/): your own pipeline, with a data gate and a person gate.
- [Install and deploy](/install/): run cairn for a team.
