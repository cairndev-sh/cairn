---
title: CLI
description: The cairn commands you use day to day — run cairn locally, inspect and launch pipelines, develop a flow, connect providers, read the record, and deploy.
---

The `cairn` command is installed with cairn. This page covers the commands for
everyday work, with the options the code defines. Every command takes
`--help` for its full option list.

Commands that touch the engine's data read the same settings as the server, so
set them first when you run outside `cairn setup`:

| Variable | What it selects |
|---|---|
| `CAIRN_STORAGE` | the database, for example `sqlite:///$HOME/.cairn/cairn.db` or a `postgres://` URL |
| `CAIRN_PROFILE` | the deployment profile (which operator serves each capability) |
| `CAIRN_PACK_PATH` | directories of packs that aren't installed as packages, separated by `:` |

## Run cairn locally

| Command | What it does |
|---|---|
| `cairn setup` | Starts the engine in the background on SQLite with the in-process runtime, signs in as `dev`/`dev`, runs the checks and returns once it all works. The engine listens on this machine only, on port 9090 (`--port`), with its log in `~/.cairn/engine.log`. `--db` sets the SQLite file (default `~/.cairn/dev.db`), `--seed` fills a demo workspace first, and `--profile` names the deployment profile: a file, or a name cairn or an installed pack ships. A second `cairn setup` reports the running engine and starts no other. |
| `cairn setup --enable <pipeline>` | Also turns on the triggers that pipeline declares. Repeatable. Nothing else turns a declared trigger on, so installing a pack or choosing a profile starts nothing. |
| `cairn setup --stop` | Stops the background engine and gives the CLI back its previous default server. |
| `cairn setup --foreground` | Stays attached until Ctrl-C, with the console dev server when the checkout has one. `--engine-only` and `--web-only` start just that part. |
| `cairn setup --server <url>` | Connects to a running server instead of starting an engine, and signs in. `-u` and `--password` are prompted when absent. |
| `cairn admin demo` | Seeds a demo workspace (default `demo`; `-w` to choose). `--archive` removes the demo workspace again, keeping its runs and audit entries. |
| `cairn setup --check` | Checks the environment configuration and whether configured services are reachable. When an engine answers at the endpoint the CLI uses, it also checks that the engine reads the store this CLI writes (`engine_store`). `--strict` fails on warnings. |
| `cairn pack ls` | Lists the installed packs and what each contributes; `cairn pack ls <pack>` for one pack, `--json` for the raw shape. A pack's deployment profiles (`profiles/*.toml`) are listed with their `visibility` and absolute installed path, the value to give `CAIRN_PROFILE`. `cairn pack ls operators` prints the operator map. |

## What `--help` shows

`cairn --help` leads with the main commands: `setup`, `run`, `status`, `decide` and `pack`. Commands that an installed provider adds come next, under the heading it names, and everything else is under `cairn admin`.

<!-- old-names -->

| Old name | New name |
|---|---|
| `cairn init` | `cairn pack new` |
| `cairn ls`, `cairn test`, `cairn lock` | `cairn pack ls`, `cairn pack test`, `cairn pack lock` |
| `cairn validate` | `cairn pack check` |
| `cairn export`, `cairn flow export` | `cairn pack export --to pipeline`, `cairn pack export --to metaflow` |
| `cairn visualize template <ref>` | `cairn pack show <ref> --graph` |
| `cairn flow ls`, `cairn pipeline ls` | `cairn pack ls --flows`, `cairn pack ls --pipelines` |
| `cairn flow split`, `cairn pipeline show` | `cairn pack show --split`, `cairn pack show` |
| `cairn connect`, `audit`, `lineage`, `compute`, `config`, `deploy`, `worker`, `demo` | `cairn admin connect`, and so on |
| `cairn remote triggers`, `enable-trigger`, `disable-trigger` | `cairn admin trigger ls`, `enable`, `disable` |
| `cairn remote presets`, `delete-preset` | `cairn admin preset ls`, `rm` |
| `cairn remote console`, `worker-token`, `hash-password` | `cairn admin console`, `worker-token`, `hash-password` |
| `cairn pipeline backfill`, `respell-packs` | `cairn admin backfill`, `respell-packs` |

<!-- /old-names -->

Each old name keeps working for one more minor version, and says its new name on stderr.

`cairn pack check` with no paths checks the installed operator packs' manifests against their classes. With paths, it also validates those files offline, as the old `validate` did. `--strict` and `--template-path` apply to the paths. `--json` prints a list of findings with no paths, and `{"artifacts": [...], "drift": [...]}` with them.

## Pipelines

| Command | What it does |
|---|---|
| `cairn pack ls --pipelines` | Lists every installed pipeline with its legs and content hash, internal ones included. |
| `cairn run <ref>` | Starts a pipeline through the launch door and stays with it: it asks at each gate, follows any pipeline its events start, and ends with the results. `-i key=value` or `-i @file.json` (repeatable; values are parsed as JSON when possible, and a `list` input also takes a comma-separated string), `-p` a saved input set, `--save-preset`, `-e` the environment, `--detach` returns at once when an engine is already running, `--json` prints the records. With no ref, it lists what can run. |
| `cairn run <ref> --dry-run` | What will happen before anything spends: each input and where its value comes from, each leg and what triggers it, the gates and the budget. Nothing runs. On a flow, it is the dev loop's plan. |
| `cairn run <ref> --check` | Runs a read-only check and exits on its verdict, for CI: 0 on pass or warn, 1 on fail. `--fail-on warn` also fails on warnings. Exit 2 means nothing was decided: bad inputs, the check couldn't run, a step failed, it paused at an approval, or it gave no verdict or one that isn't a pass, a warning or a failure. A failure inside a `subgraph_node` isn't detected yet. |
| `cairn run <skill> --skill` | Submits a skill to the server. `--wait` blocks until it is done, `--service` names the service. |
| `cairn pack show <ref>` | A pipeline's inputs, legs, gates and wiring. `--plan` shows the execution plan instead; with `--plan`, `--preset <name>` and `-i key=value` first list each input's value and the layer it comes from (`← preset 'name'`). |

A **ref** is `<pack>/<name>`, for example `hello-cairn/profile-and-publish`.
When a flow has the same name as a pipeline, `show` and `run` both
resolve the bare name to the declared pipeline (ADR-0106 D1).
The explicit spelling `pipeline:<pack>/<name>` works too.

Only declared pipelines run. A ref that names a flow, not a pipeline, is
refused: a flow runs as a leg of a pipeline.

`cairn run <ref>` exits with:

| Code | Meaning |
|---|---|
| `0` | the pipeline completed, and any pipeline its events started |
| `1` | one failed or was cancelled, or the launch was refused: the ref is unknown, an input is missing, a policy denied it |
| `2` | a pipeline is waiting on a person and there is no terminal to ask on (it prints the `cairn decide` command); or the command line is wrong: a bad input, a flag that doesn't apply, a ref that names a flow |
| `77` | the session expired; run `cairn setup`, or `cairn setup --server <url>` |

<!-- old-names -->

The old names keep working for one more minor version, and each says its new name on stderr: `cairn pipeline run` and `cairn remote launch` are now `cairn run`, `cairn pipeline explain` and `cairn preflight` are `cairn run --dry-run`, `cairn gate` is `cairn run --check`, and `cairn remote submit` is `cairn run --skill`. `cairn dev` is `cairn setup --foreground` (it keeps its default port, 8000, and like `cairn setup` it listens on this machine only unless `CAIRN_TRIGGER_HOST` is set), `cairn doctor` is `cairn setup --check`, and `cairn remote login` is `cairn setup --server`. They keep their own exit codes: `cairn pipeline run` still exits `3` on a refused launch and `130` on Ctrl-C, and now `2` at a human gate it has no way to ask about.

<!-- /old-names -->

## Flows

| Command | What it does |
|---|---|
| `cairn pack ls --flows` | Lists every flow, of all three kinds. |
| `cairn pack show <ref> --split` | Shows where a flow splits at its approval points. `--json` for the raw segments. |
| `cairn pack check <paths>` | Validates a pack's pipeline, flow, template, skill and policy files offline: no model calls, nothing runs. A capability with no binding fails, so validate under the profile you will run with. `--strict` fails on warnings. |

### Try a flow without a record

`cairn run <pack>/<template> --input <inputs.json>` runs an agentic flow on its
own, in process, for development. It writes no run record and is not a
governed pipeline.

| Option | Effect |
|---|---|
| `--dry-run` | compile, validate and print the execution plan; runs nothing |
| `--mock` | deterministic LLM responses and `.mock()` for operators declaring `supports_mock`; refuses before execution if any reachable operator lacks mock support |
| `--mode sandbox` | write operations are stubbed, reads run for real, spend stays near zero |
| `--interactive` / `-I` | at each approval point, approve or reject in the terminal |

## Authoring

| Command | What it does |
|---|---|
| `cairn pack new pack <name>` | Scaffolds a pack: `pack.toml`, `pyproject.toml`, `pipelines/`, `flows/`, `policies/`. |
| `cairn pack new operator <name>` | Scaffolds an operator pack: a package bundling one or more operators. |
| `cairn pack new template` · `skill` · `policy` | Adds one stub to the current pack. |
| `cairn pack test [pack]` | Runs a pack's tests with pytest. |

## Connections

| Command | What it does |
|---|---|
| `cairn admin connect providers` | Lists the providers you can connect: model APIs, compute, MCP servers. |
| `cairn admin connect add <provider>` | Adds a connection. `--name` names it, `--secret KEY=ENV_REF` points a secret at an environment variable, `--set key=value` overrides config. Secrets are stored as references, never as values. |
| `cairn admin connect test <name>` | Resolves the connection's secrets and runs the provider's authenticated health check. Exit 0 when it connects. |
| `cairn admin connect ls` | Lists connections: names, kinds and secret keys, never values. |
| `cairn admin connect rm <name>` | Deletes a connection, leaving the secret itself untouched. |

`connect add` and `connect rm` write this CLI's store: the one `CAIRN_STORAGE` names,
else the `CAIRN_CONNECTIONS_DB` file, else `~/.cairn/cairn.db`. Before writing, they ask the engine at the endpoint
the CLI uses (its `/healthz` reports a fingerprint of its store, never a path or
DSN) whether it reads the same store. If it reads another one, nothing is written and
the command exits 3, naming this CLI's store; set `CAIRN_STORAGE` to the value the engine
was started with (`cairn setup --db PATH` is `sqlite:///PATH`), or pass `--force` to write
anyway. When no engine answers, the write goes ahead and the line names the store.

## The record

| Command | What it does |
|---|---|
| `cairn status <run-id> --replay` | Prints what a past run recorded: its inputs, what it ran and its events. Nothing runs again. It reads the local trace, then the configured store, then the engine, then, when a local engine is down, the local dev store; `--json` names the source as `meta.source`. A down engine elsewhere is an error, and `cairn status <run-id> --replay --endpoint <url>` reads only that engine. |
| `cairn admin audit verify` | Checks the audit log's hash chain without writing anything. Reads `--audit-log`, else `CAIRN_AUDIT_LOG`, else `./audit.jsonl`. Prints the row count, the first and last timestamp, the head hash and, if the chain is broken, the first broken row and why (`hash`, `prev_hash` or `corrupt line`). `--json` prints the same as `{path, rows, first_ts, last_ts, head_hash, ok, broken}`. Exit 0: verified (an empty log verifies). Exit 2: broken or a corrupt line. Exit 1: no log, or it cannot be read. |
| `cairn admin lineage [subject]` | Shows provenance records from the audit log, matched on a subject such as a model name, and checks the hash chain as it reads. Omit the subject to list them all. `--expect-head <hash>` also checks that the log ends at a receipt you quoted earlier, which shows rows missing from its end. The operator that writes lineage records is part of cairn-platform. |

## A remote server

Drive a deployed cairn from your terminal. Connect once with `cairn setup --server <url>`; the token is stored in
your profile (`~/.cairn/config.toml`, written with mode 0600). The token is not renewed:
a command that meets an expired session, `--watch` and `--follow` included, prints
`session expired at HH:MM — run cairn setup, or cairn setup --server <url>` and exits 77. A watched pipeline keeps
running on the server; log in again and re-attach with `cairn status <id> --follow`.

| Command | What it does |
|---|---|
| `cairn status` | What's running and what just finished. `--status`, `--limit`, `--watch` (`-n` sets the interval). |
| `cairn status <id>` | One pipeline (`pl_…`) or run: its state, who decided what, and what started it. `--results` for every step's findings, `--logs` for the event log (`--after-seq`, `--events-only`), `--follow` to stay attached and answer each gate (`--decision`, `--timeout`, `--on-timeout`, `--native`, `--dump`), `--replay` for a run's persisted trace (`--source`), `--json` for the raw form. Following exits 0 when done, 1 when failed or cancelled, and 2 when it's waiting on a person with no terminal to ask on. |
| `cairn decide <id> approve\|reject\|cancel` | Decides what a pipeline or run is waiting on, or cancels it. The id picks the route. `--step` names the gate when a pipeline waits at more than one, `--reason` is required to reject a pipeline gate, and `--approver` is recorded when sign-in is off. |
| `cairn admin trigger ls` | Lists this workspace's stored triggers (event and webhook), whether each is on, and which pipeline declared it. |
| `cairn admin trigger enable <id>` / `disable <id>` | Turns a stored trigger on or pauses it. A trigger a pipeline declares starts paused; an event trigger applies to the next event, a webhook trigger at the next server start. |

`cairn run`, `cairn status`, `cairn decide` and `cairn admin trigger`, `preset` and `console` take `--endpoint`, `--token` and `--profile`, long form only. `-e` is the environment on `cairn run`.

<!-- old-names -->

The older names `cairn replay`, and `cairn remote pipelines`, `pipeline`, `ps`, `status`, `watch`, `findings`, `logs`, `advance`, `approve`, `reject`, `cancel` and `cancel-pipeline`, still work for one minor version. They print the new spelling on stderr.

<!-- /old-names -->

## Deploy and workers

| Command | What it does |
|---|---|
| `cairn admin deploy doctor` | Checks a deployment's `.env` (`--env`) or Helm values (`--values`) before it starts: required values, and that the runtime matches the enabled services. `--strict` fails on warnings. |
| `cairn admin hash-password` | Prints the password hash for `CAIRN_API_PASSWORD_HASH` (the `auth` extra). |
| `cairn admin worker-token --name <name>` | Prints a worker-pool token for `cairn admin worker --pull --token`. Run it where the control plane's `CAIRN_JWT_SECRET` is set: it signs the token with that secret. `--days` sets how long it is valid (default 30). The token claims and heartbeats pull-lane jobs and gets `403` on every other route. |
| `cairn admin worker --pull` | Runs a worker that claims jobs from the control plane over HTTP: `--engine-url`, `--token`, `-c gpu=T4` to say what the machine offers, `--once` for a single job. With sign-in on, `--token` must be a worker credential: a worker-pool token from `cairn admin worker-token`, or the deployment admin's (`CAIRN_API_USER`) token. A signed-in user's token gets `403`, whatever their role. The per-run token the engine hands a job's container can only post that run's events, read its control requests and heartbeat its job, and gets `403` anywhere else. Without `--pull` it polls a Temporal task queue. |

## Next

- [Pipeline and flow YAML](/reference/yaml/)
- [Pipeline catalog](/reference/catalog/)
- [Install and deploy](/install/)
