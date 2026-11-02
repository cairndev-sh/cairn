---
title: Pipeline catalog
description: The pipelines that ship with cairn's example pack, hello-cairn, what each one does, and what it needs to run.
---

The open-source core ships one domain pack, `hello-cairn`, with five
pipelines. Each does one thing a new user recognises, and together they use
every kind of leg and gate. Install it with the `hello` extra
(`pip install 'cairndev[hello]'`), then inspect a pipeline before you run it:

```bash
cairn pipeline show hello-cairn/profile-and-publish      # inputs, legs and gates
cairn pipeline explain hello-cairn/profile-and-publish   # what each leg will do, the gates, the budget
```

**Legs** counts the pipeline's top-level legs. **Gates** counts data gates and
person gates (see [Gates and approvals](/concepts/gates/)). **Budget** is the
series budget in `governance` (see [Cost and budgets](/concepts/cost/)). The
first four share the series `hello`.

| Pipeline | What it does | Legs | Gates | Budget |
|---|---|---|---|---|
| `hello-cairn/profile-and-publish` | Profile a small JSONL dataset, stop unless the profile passes, redact the summary, stage it, and publish it only after a person approves. | 9 | 1 data, 1 person | $1 |
| `hello-cairn/tiny-finetune` | Fine-tune a small model with LoRA on CPU, gate on its held-out loss, tag it, and write its card only after a person approves. | 9 | 2 data, 1 person | $1 |
| `hello-cairn/ask-the-docs` | Answer a question from the example's own docs with retrieval, a model call and an MCP tool, check the answer's length, and have a person read it. | 3 | 1 person | $0.50 |
| `hello-cairn/nightly-ops` | Check a secret, run a report on an SSH box, carry it through a flow, propose the follow-up to a person, and audit it. | 4 | — | $0.10 |
| `hello-cairn/over-budget` | Declare an estimate above the budget and watch the flow leg's reservation refuse it before the engine starts. | 1 | — | $1 |

Besides its gates, `profile-and-publish` and `tiny-finetune` stop at
`promote`, whose operator asks for approval on every call, and `nightly-ops`
stops at `propose` for the same reason.

## What each one needs

| Pipeline | Needs |
|---|---|
| `profile-and-publish` | nothing beyond the `hello` extra; no key, no spend. Pass `-i path=<file>` when the engine does not run from a checkout of the repository. |
| `tiny-finetune` | the `train` extra (torch, transformers, peft, datasets) on the engine's machine; CPU is enough. `base_model` defaults to the stand-in `sshleifer/tiny-gpt2`, which trains in seconds. |
| `ask-the-docs` | a model endpoint (`OPENAI_API_KEY`, or `OPENAI_BASE_URL` for any OpenAI-compatible server), the `rag` extra with `CAIRN_RAG_AUTOINDEX=1` to index the pack's docs at boot (and `RAG_EMBED_PROVIDER=openai` to embed with OpenAI), and the example's MCP server in `CAIRN_MCP_SERVERS`. |
| `nightly-ops` | an SSH connection named `hello-ssh` (see [Connect a provider](/guides/connect-a-provider/#example-an-ssh-host)), the `ssh` extra, and `HELLO_REPORT_TOKEN` in the engine's environment. |
| `over-budget` | nothing beyond the `hello` extra; no key, no spend. It is refused every time, on purpose. |

Run the engine under the example's profile,
`CAIRN_PROFILE=examples/hello-cairn/profiles/hello.toml` from a checkout, for
`ask-the-docs` and `nightly-ops`: it binds the secret store and declares the
MCP server they use.

## Triggers

| Pipeline | Trigger | On out of the box |
|---|---|---|
| `nightly-ops` | `schedule`, 02:30 every night | yes |
| `nightly-ops` | `watch` on a database table | no; needs cairn-platform's governed import |
| `profile-and-publish` | `watch` on the `hello-sample` dataset, `on_change: start` | no |
| `profile-and-publish` | `schedule`, Monday 06:00 | no |
| `profile-and-publish` | `webhook` at `POST /triggers/hello-sample` | no |
| `ask-the-docs` | `event` on `dataset.ready`, which `profile-and-publish` emits when it publishes | no |

To turn one on, set `enabled: true` in the pipeline file and restart the
engine.

## Approvers

The example's gates name `data-owners`, `ml-owners` and `ops`. Those are
neither users nor roles on a fresh deployment, so only an owner or admin can
decide them; `cairn dev`'s `dev` user can. Name your own users or roles in
your pipelines (see [Who decides](/concepts/gates/#who-decides)).

## Next

- [Run your first pipeline](/guides/first-pipeline/): launch one and read its record.
- [Write a pipeline](/guides/write-a-pipeline/): your own pack.
- [Operators](/reference/operators/): what the legs call.
