---
title: CLI
description: The cairn commands you use day to day — run cairn locally, inspect and launch pipelines, develop a flow, connect providers, read the record, and deploy.
---

The `cairn` command is installed with cairn. This page covers the commands for
everyday work, with the options the code defines. Every command takes
`--help` for its full option list.

Commands that touch the engine's data read the same settings as the server, so
set them first when you run outside `cairn dev`:

| Variable | What it selects |
|---|---|
| `CAIRN_STORAGE` | the database, for example `sqlite:///$HOME/.cairn/cairn.db` or a `postgres://` URL |
| `CAIRN_PROFILE` | the deployment profile (which operator serves each capability) |
| `CAIRN_PACK_PATH` | directories of packs that aren't installed as packages, separated by `:` |

## Run cairn locally

| Command | What it does |
|---|---|
| `cairn dev --engine-only` | Starts the engine on SQLite with the in-process runtime and sign-in as `dev`/`dev`. `--port` sets the port (default 8000), `--db` the SQLite file (default `~/.cairn/dev.db`), `--seed` loads demo records first. Without `--engine-only` it also starts the console's dev server, which is part of cairn-platform. |
| `cairn demo` | Seeds a demo workspace (default `demo`; `-w` to choose). `--archive` removes the demo workspace again, keeping its runs and audit entries. |
| `cairn doctor` | Checks the environment configuration and whether configured services are reachable. `--strict` fails on warnings. |
| `cairn ls` | Lists the installed packs and what each contributes; `cairn ls operators` prints the operator map. |

## Pipelines

| Command | What it does |
|---|---|
| `cairn pipeline ls` | Lists every installed pipeline with its legs and content hash, internal ones included. |
| `cairn pipeline show <ref>` | A pipeline's inputs, legs, gates and wiring. `--plan` shows the execution plan instead. |
| `cairn pipeline explain <ref>` | What will happen before anything spends: each input and where its value comes from, each leg and what triggers it, the gates and the budget. `-i key=value` shows the effect of a launch input, `-e` resolves for an environment, `--json` prints the raw result. |
| `cairn pipeline run <ref>` | Launches a pipeline and follows it to the end. `-i key=value` (repeatable; values are parsed as JSON when possible), `-e` the environment, `--detach` returns at once. |

A **ref** is `<pack>/<name>`, for example `hello-cairn/profile-and-publish`.
When a flow has the same name as a pipeline, `show` and `explain` refuse the
bare name: write `pipeline:<pack>/<name>`. `run` accepts the bare name and
runs the pipeline.

Only declared pipelines run. A ref that names a flow, not a pipeline, is
refused: a flow runs as a leg of a pipeline.

`cairn pipeline run` exits with:

| Code | Meaning |
|---|---|
| `0` | the pipeline completed |
| `1` | it failed or was cancelled, or the ref is unknown, ambiguous or missing required inputs |
| `2` | the ref names a flow, not a pipeline; or `--detach` under the in-process runtime, which would leave nothing driving the pipeline |
| `3` | refused before it started: a policy denied the launch, or admission refused it |
| `130` | interrupted with Ctrl-C |

## Flows

| Command | What it does |
|---|---|
| `cairn flow ls` | Lists every flow, of all three kinds. |
| `cairn flow split <ref>` | Shows where a flow splits at its approval points. `--json` for the raw segments. |
| `cairn validate <paths>` | Validates a pack's pipeline, flow, template, skill and policy files offline: no model calls, nothing runs. A capability with no binding fails, so validate under the profile you will run with. `--strict` fails on warnings. |

### Try a flow without a record

`cairn run <pack>/<template> --input <inputs.json>` runs an agentic flow on its
own, in process, for development. It writes no run record and is not a
governed pipeline.

| Option | Effect |
|---|---|
| `--dry-run` | compile, validate and print the execution plan; runs nothing |
| `--mock` | operators that support it return mock results; no side effects |
| `--mode sandbox` | write operations are stubbed, reads run for real, spend stays near zero |
| `--interactive` / `-I` | at each approval point, approve or reject in the terminal |

## Authoring

| Command | What it does |
|---|---|
| `cairn init pack <name>` | Scaffolds a pack: `pack.toml`, `pyproject.toml`, `pipelines/`, `flows/`, `policies/`. |
| `cairn init operator <name>` | Scaffolds an operator pack: a package bundling one or more operators. |
| `cairn init template` · `skill` · `policy` | Adds one stub to the current pack. |
| `cairn test [pack]` | Runs a pack's tests with pytest. |

## Connections

| Command | What it does |
|---|---|
| `cairn connect providers` | Lists the providers you can connect: model APIs, compute, MCP servers. |
| `cairn connect add <provider>` | Adds a connection. `--name` names it, `--secret KEY=ENV_REF` points a secret at an environment variable, `--set key=value` overrides config. Secrets are stored as references, never as values. |
| `cairn connect test <name>` | Resolves the connection's secrets and runs the provider's authenticated health check. Exit 0 when it connects. |
| `cairn connect ls` | Lists connections: names, kinds and secret keys, never values. |
| `cairn connect rm <name>` | Deletes a connection, leaving the secret itself untouched. |

## The record

| Command | What it does |
|---|---|
| `cairn replay <run-id>` | Replays a past run's recorded event trace as a timeline. `--json` for the raw events. |
| `cairn lineage [subject]` | Shows provenance records from the audit log, matched on a subject such as a model name, and checks the hash chain as it reads. Omit the subject to list them all. The operator that writes lineage records is part of cairn-platform. |

## A remote server

Drive a deployed cairn from your terminal. Log in once; the token is stored in
your profile.

| Command | What it does |
|---|---|
| `cairn remote login -e <url>` | Signs in and stores the token. |
| `cairn remote pipelines` | Lists the pipelines on the server. |
| `cairn remote launch <ref>` | Launches a pipeline. `-i key=value`, `--preset` to start from a saved input set, `--watch` to follow it. |
| `cairn remote pipeline <id>` | One pipeline's full record: its plan and the state of every step. |
| `cairn remote advance <id> <step>` | Approves a step waiting on a person: a gate, or a leg whose operator or policy asks for approval. Under sign-in the server records your identity. |
| `cairn remote findings <id>` | A run's findings, or every step's findings for a pipeline. |
| `cairn remote cancel-pipeline <id>` | Cancels a pipeline and its running legs. |
| `cairn remote ps` / `status` / `logs` | Lists runs, shows one run, prints or follows its event log. |
| `cairn remote approve` / `reject` | Decides a run paused at an approval inside a flow. |

Every `remote` command takes `--endpoint` / `-e`, `--token` and `--profile`.

## Deploy and workers

| Command | What it does |
|---|---|
| `cairn deploy doctor` | Checks a deployment's `.env` (`--env`) or Helm values (`--values`) before it starts: required values, and that the runtime matches the enabled services. `--strict` fails on warnings. |
| `cairn remote hash-password` | Prints the password hash for `CAIRN_API_PASSWORD_HASH` (the `auth` extra). |
| `cairn worker --pull` | Runs a worker that claims jobs from the control plane over HTTP: `--engine-url`, `--token` (an operator token: the per-run token the engine hands a job's container can only post that run's events, read its control requests and heartbeat its job, and gets `403` anywhere else), `-c gpu=T4` to say what the machine offers, `--once` for a single job. Without `--pull` it polls a Temporal task queue. |

## Next

- [Pipeline and flow YAML](/reference/yaml/)
- [Pipeline catalog](/reference/catalog/)
- [Install and deploy](/install/)
