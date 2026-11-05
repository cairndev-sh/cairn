---
title: FAQ
description: "Short answers to what people ask when they evaluate cairn: Kubernetes, runtimes, API keys and GPUs, what a gate costs, your own trainer, the record, crashes and licensing."
---

Short answers to what people ask when they evaluate cairn. Each one links to the
page that shows it.

## Does it need Kubernetes?

No. `cairn setup` starts an engine on your own machine with SQLite, and
[Docker Compose](/install/#single-host-with-docker-compose) runs a server on one
host. There is a [Helm chart](/install/#kubernetes-with-helm) if you do want
Kubernetes.

## Does it need an API key or a GPU?

Not to start: the [Quickstart](/quickstart/) needs neither. A pipeline that calls
a model needs [an OpenAI-compatible endpoint](/guides/connect-a-provider/#a-model-endpoint),
set with an API key or a base URL. The example's `tiny-finetune` pipeline
fine-tunes a small model on CPU with the `train` extra, and
[the GPU guide](/guides/fine-tune-a-model/) uses an EC2 GPU instance of your own.

## Which runtimes does it support?

Three, chosen with `CAIRN_RUNTIME`: the in-process runtime (the default),
[DBOS and Temporal](/install/#choose-the-backends). Under Temporal, a pipeline
with a `task:` leg or a `finalizer:` step is refused at launch, because that
runtime doesn't run them yet. Data flows run on a separate
[dataflow engine](/concepts/capabilities/#environments): the built-in one,
Metaflow or SkyPilot.

## What does a gate cost?

No published number measures this:
[the benchmark](https://github.com/cairndev-sh/cairn/tree/main/benchmark) times the
Quickstart's whole path, not a gate on its own. A [gate](/concepts/gates/) does no
work, so it has no run and starts no engine, and a paused pipeline is
[stored, not held in memory](/concepts/runs/#the-pipeline-record). A person gate
also costs the wait until someone decides.

## Can I bring my own trainer?

Yes. Wrap it as an [operator](/concepts/capabilities/#capabilities-and-operators)
(`cairn pack new operator` scaffolds one) or as a
[flow stage](/guides/write-a-flow/#your-own-stage), and install the package where
the engine runs. A pipeline or flow then names it as it would a built-in one, and the same
[gates](/concepts/gates/) and [record](/concepts/runs/) apply.

## Where does the record live?

Pipeline records and runs are in your own database, which `CAIRN_STORAGE` selects:
SQLite (`~/.cairn/dev.db` under `cairn setup`) or Postgres. A server with it unset
[keeps runs in memory and loses them on restart](/install/#choose-the-backends), and
puts pipeline records in `~/.cairn/cairn.db`.
The local artifact store keeps its files under `~/.cairn/artifacts`
(`CAIRN_ARTIFACTS_DIR`), and the hash-chained audit log is a file, `./audit.jsonl` by
default, which the engine writes to only when the profile
[binds `audit.sink`](/concepts/runs/#the-audit-trail).

## What happens if the process crashes mid-run?

After a restart on the same store, finished legs keep their results and don't run
again, and [a gate that was waiting is still waiting](/concepts/runs/#the-pipeline-record).
For a leg caught in flight, a read-only task leg or a `finalizer:` runs again,
[any other task leg waits for a person](https://github.com/cairndev-sh/cairn/blob/main/tests/core/test_pipeline_task_resume.py),
as does a template leg under the in-process runtime unless it's on a live remote job, and a flow leg is re-attached to its engine (or, with no engine handle yet, waits
for a person) and is never resubmitted. A local job on the built-in engine dies with
the process, so there the flow leg settles `failed` and no later leg runs apart from
`finally:` steps, and
[the crash tests](https://github.com/cairndev-sh/cairn/blob/main/tests/e2e/hello/test_crash.py)
check that outcome by killing a real server.

## How does it differ from Airflow and MLflow?

cairn is [a control plane for the work behind AI models](/): it decides when each
step may run, holds the spend to a budget, pauses for a data check or a person,
and records what happened. [How cairn compares](/compare/) sets that beside Airflow
and MLflow.

## Is it production-ready?

It isn't declared production-ready: cairn is a 0.x release in
[early access](https://github.com/cairndev-sh/cairn#status--license), and the API
and commands may still change between releases. Every pull request runs
[CI](https://github.com/cairndev-sh/cairn/blob/main/.github/workflows/ci.yml): the
test suites, plus end-to-end cells on SQLite and Postgres under the in-process and
DBOS runtimes, including a server killed mid-run. Known limits:
[Temporal](/install/#choose-the-backends) refuses task legs and finalizers and isn't
in those cells, budget-cap overshoot
[isn't measured yet](https://github.com/cairndev-sh/cairn/issues/325), and
[the audit log](/concepts/runs/#the-audit-trail) is off unless the profile binds
`audit.sink`.

## What's open source and what isn't?

The `cairndev` package (the engine, the `cairn` command, the API server, the client
and the [builtin operator packs](/reference/operators/)) is Apache 2.0. Packs and
operators released as separate wheels each state their own licence. The
[web console and the operator packs for GPU providers and managed training](/#what-cairn-does-not-replace)
are part of cairn-platform, a separate product whose source isn't public.

## Next

- [Quickstart](/quickstart/): install cairn, launch a pipeline and approve a gate.
- [What is cairn](/): the model in one page.
- [Install and deploy](/install/): run cairn for your team.
- [Troubleshooting](/troubleshooting/): a message you didn't expect, and what to do.
