---
title: Troubleshooting
description: Read what cairn setup --check prints and its fix hints, then find the refusal you hit (a bare flow, a missing pack, a blocked gate, a budget or cost cap, a policy deny, a holdout overlap) by its message, with what it means and what to do.
---

Find the message you see, read what it means, then do the fix. The first part
explains the report that `cairn setup --check` prints. The second part is the
refusals a launch or a run can end with, in the words cairn uses.

## Read `cairn setup --check`

`cairn setup --check` looks at your environment and the services it names, and
starts nothing. Each row is one line:

```text
  ⚠ run_db: CAIRN_STORAGE and CAIRN_RUN_DB unset → in-memory store (state lost on restart) — fix: set CAIRN_STORAGE to postgres://… or sqlite:///path.db, or CAIRN_RUN_DB to a file path
```

That is a symbol, the row's name, a colon, what cairn found, and, after `— fix:`,
the one step that clears it. The rows are grouped under `[store]`, `[profile]`,
`[packs]` and the other categories, and the last line counts them:
`Summary: <n> ok, <n> warn, <n> fail`.

| Symbol | Meaning |
|---|---|
| `✓` | ok: set, reachable, writable or loaded. |
| `⚠` | warn: something you may want to change, or a service that didn't answer in time. The command still exits 0. |
| `✗` | fail: a required value is missing, a path is unwritable, or a value is invalid. The command exits 1. |

`cairn setup --check --strict` also exits 1 on a warning. A fresh install
prints many `⚠ <NAME>: unset (optional)` rows, and `--strict` fails on those
too, so use it once you've dealt with the warnings you care about.

**The `— fix:` hint.** It names a command, a flag, a setting or a file, and sits at the
end of the same line. A plain `cairn setup` runs these checks too, and prints only the
`✗` lines and the count (`checks: Summary: …`), so a hint on a second line would be
lost. A row with no hint is `✓`, or needs nothing. The `compute_target` rows put the
remedy in the sentence instead.

### Settings rows

Every setting cairn reads gets a row in the category it belongs to
(`[dispatcher]`, `[store]`, `[actions]`, `[notifier]` and others), named after the
environment variable.

| You see | It means | Do |
|---|---|---|
| `✓ CAIRN_X: using default '4'` | Unset, and the default applies. | Nothing. |
| `⚠ CAIRN_X: unset (optional)` | Unset, and nothing needs it. | Nothing, unless you want it. |
| `✓ CAIRN_X: set: <value>` | Set. A secret is masked, and a URL shows no user or password. | Nothing. |
| `✗ CAIRN_X: required but unset (<what it is>) — fix: set CAIRN_X in the environment cairn runs in` | The setting has no default and cairn can't run without it. | Set it in the environment the engine or server starts from, not only in your shell if the engine started elsewhere. |
| `⚠ CAIRN_X: set: <value> via <OLD> — deprecated — fix: rename <OLD> to CAIRN_X` | You used an old name that cairn still reads. | Rename the variable. |

### `[store]`

| Row | What it checks | Status and fix |
|---|---|---|
| `run_db` | Where runs are kept: `CAIRN_STORAGE` (Postgres or `sqlite:///path.db`), else `CAIRN_RUN_DB`, else memory. | `⚠` when both are unset: runs are lost on restart. Set `CAIRN_STORAGE`. `✗` when `CAIRN_STORAGE` is not a storage URL, or when a SQLite file's directory can't be created or written: point it at a directory you can write. |
| `engine_store` | Whether the engine that answers at your endpoint reads the same store this CLI writes. No row when no engine answers. | `⚠` when the stores differ, or the engine reports none. Set `CAIRN_STORAGE` here to the store the engine reads. |
| `artifact_bytes` | Whether registered artifacts are copied off the machine (`CAIRN_ARTIFACT_STORE`). | `⚠` when there is no registry (`CAIRN_STORAGE` unset), or no durable byte store: an artifact on a box's disk is lost with the box. Set `CAIRN_ARTIFACT_STORE` to `s3://<bucket>[/<prefix>]`. |
| `artifact_registry_bytes` | Whether the bytes of every registered artifact can still be fetched. | `⚠` with a count of dangling artifacts and the first few URIs: put the bytes back, or re-run what produced them. Also `⚠` when the registry can't be read: make sure the store `CAIRN_STORAGE` names is up. |

### `[profile]`, `[production]` and `[settings]`

| Row | What it checks | Status and fix |
|---|---|---|
| `profile` | Whether the profile `CAIRN_PROFILE` names loads. | `✓` when it is unset (defaults, no audit or redaction). `✗` when the file fails to load: set it to a valid profile `.toml`, or unset it. |
| `binding:<capability>` | Each `[profile.bindings]` entry resolves to an installed operator. | `✗` when it doesn't: the service would silently do nothing. Install the pack that has the operator, or remove the binding. |
| `binding:audit.sink`, `binding:redact.text` | Whether the profile binds the audit trail and redaction. | `⚠` when unbound: audit events are dropped, and prompts leave unredacted. Add the binding the hint prints. |
| `requires:<pack>:<capability>` | A pack that requires a capability has a provider bound for it. | `✗` when none is bound. Bind it under `[profile.bindings]`. |
| `provisioned:<pack>:memory` and `:rag` | A pack that needs a memory or vector store has one enabled. | `✗` when not: set `memory = true` under `[profile.backends]`, or `rag_vector_store` there or as `RAG_VECTOR_STORE`. |
| `config[<key>]` | A backend setting that isn't the default, and whether the profile or the environment set it. | `✓`. Information only. |
| `config:<operator>` | Each `[profile.config."<operator>"]` table validates against that operator's settings. | `✗` for an unknown key, a wrong type or an operator that isn't installed. The row lists the keys it accepts. |
| `model_role:<role>` | The provider behind each model role (`fast`, `strong`, `router`) has a key. | `⚠` when it doesn't: calls will fail. Set the key the hint names, such as `OPENAI_API_KEY`. |
| `pii-backend`, `benchmark-harness`, `runtime-temporal`, `registry-mlflow` | Only with `CAIRN_PROFILE` set: the profile asks for a backend that isn't installed (Presidio, the lm-eval harness, the Temporal SDK, MLflow). | `✗` (`⚠` for MLflow). `pip install` the extra the hint names, or drop the requirement from the profile. |

### `[packs]`

| Row | What it checks | Status and fix |
|---|---|---|
| `packs_loaded` | The installed packs. | `⚠` when none is installed: install one. A pack you expect is missing from the list: see [a missing pack](#no-pipeline-is-installed-a-missing-pack). |
| `pack_tier:<pack>` | The pack's kind, trust level and publisher. | `✓`. Information only. |
| `pack_compat:<pack>` | The pack's `cairn_engine` range against the installed `cairndev`. | `⚠` when it is out of range, malformed, or `cairndev` isn't installed. Install a release of the pack built for this `cairndev`, or write the range as a PEP 440 range. |
| `operator:<pack>/<name>` | An operator's risk, executors and the one that would run. | `⚠` when it declares no `pack.toml` entry (run `cairn pack sync <pack-dir>`), or only executors this engine doesn't implement. |
| `manifest_drift`, `drift[<kind>]` | The operators declared in each `pack.toml` match the classes registered. | `✗` on drift, because governance reads the manifest. Run `cairn pack sync <pack-dir>` in the pack, then reinstall it. `⚠` for a pack that isn't installed. |
| `composition_conflict` | Two packs provide one capability, a required capability has no provider, or a profile pick names nothing. | `✗`. The hint says which: pick one provider under `[profile.capabilities]`, or install the missing pack. |
| `composition_warning` | An optional capability nobody provides. | `⚠`. Install a pack that provides it, or ignore it. |
| `pipeline_mandate` | Every domain pack ships a pipeline. | `✗` for one that doesn't: add `pipelines/<name>.yaml`. |
| `pack_layout`, `flow_stem_collisions` | A pack keeps flows in a superseded directory, or two flow files share one name. | `⚠`. Move the files into `flows/`, or rename one of the two. |
| `stage_kernels`, `stage_kernel[<name>]` | Every registered flow stage loads on the control plane. | `⚠` for one that can't be imported: install its dependencies here. |
| `seed_providers`, `model_catalogs`, `run_hooks`, `router_providers`, `cli_providers` | The extension points packs plug into. Each lists what loaded. | `⚠` for one that fails to load: install what the error names, or uninstall the package that declares it. `⚠` for no seed provider, and for no model catalogue provider (a `base_model` input is then not licence-checked against a catalogue). |
| `trigger:<pack>:<source>`, `capabilities_provided` | The triggers packs declare and how many capabilities they provide. | `✓`. Information only. |

A pack can add rows of its own, under its own category.

### Runtime and reachability

| Row | What it checks | Status and fix |
|---|---|---|
| `runtime` | The runtime `CAIRN_RUNTIME` selects. | `✓` for the in-process runtime. For Temporal, `✗` if the SDK is missing (`pip install 'cairndev[runtime-temporal]'`) and `⚠` if the cluster address doesn't answer (start it, or set `CAIRN_TEMPORAL_ADDRESS`). |
| `compute_target:<ml_target or dataflow_target>` | The `compute.ssh` connection the environment names resolves and has a host, user and private key. | `✗` when it doesn't resolve (`cairn admin connect add`, `cairn admin connect ls`, `cairn admin connect test <name>`) or is incomplete. |
| `compute_target:<…>:mode` | Whether the box runs from its virtualenv or an image. | `✗` for image mode with no image named, and `⚠` for an image on a moving tag such as `:latest`: pin a tag. |
| `engine_client:<dataflow_engine or ml_engine>` | The client library of the engine you selected (Metaflow) imports here. | `✗` when it doesn't: `pip install 'cairndev[metaflow]'`. |
| `metaflow_service` | The Metaflow metadata service answers. | `✗` when it doesn't: start it, and check `CAIRN_METAFLOW_SERVICE_URL`. |
| `slack_webhook` | `SLACK_WEBHOOK_URL` answers a HEAD request. | `⚠` when it doesn't: check that it is the webhook's full https URL. |
| `mcp_servers`, `mcp:<name>` | Each declared MCP server lists its tools within 3 seconds. | `⚠` when one is unreachable: start it, and check its entry in `CAIRN_MCP_SERVERS`. |

A probe that can't finish is a `⚠`, not a crash, and each one gives up after a few seconds.

### When `cairn setup` itself fails

| You see | It means | Do |
|---|---|---|
| `the engine is running, but the checks above failed, so no trigger was turned on. \`cairn setup --stop\` stops it.` | The engine started and a check printed `✗`. | Fix that row (the `✗` lines above the message) and run `cairn setup --check` to confirm. |
| `an engine is already running at http://127.0.0.1:<port> under profile <name>. To change it, run \`cairn setup --stop\` first.` | You passed a different `--port`, `--db` or `--profile` than the running engine uses. | `cairn setup --stop`, then run it again with the options. |
| `the engine didn't answer at <endpoint> within 120s, so it was stopped. See <log>.` | The first start imports everything and can be slow, or it hung. | Read the log file it names. |
| `the engine exited with code <n> while starting. See <log>.` | The engine crashed on start. | Read the log file it names. |
| `an engine already answers at <endpoint>, and this command didn't start it; using it.` | An engine you started some other way (in another terminal, say) already holds the port. | Fine to continue, or stop that one. `--profile` doesn't apply to it. |

## A flow is refused: it does not run on its own

You see, from `cairn run <pack>/<flow>`, and the command exits 2:

```text
'hello-cairn/summarise' is a flow (data), and a flow does not run on its own (ADR-0106 D1): only a declared pipeline runs, so a flow with no wrapper is a component. To run it: add hello-cairn/pipelines/summarise.yaml — `inputs: {$inherit: summarise}`, one step `- id: summarise` / `flow: summarise` — and start `pipeline:hello-cairn/summarise`. To iterate on the flow instead: `cairn run hello-cairn/summarise --interactive` (or --mock / --dry-run), which runs it in this process and writes no record (ADR-0106 D6). `GET /pipelines/installed` lists what this deployment can start.
```

**It means** the ref names a flow, a building block. Only a pipeline can be started,
because the pipeline is where the budget, the gates and the record live. Over HTTP
the same refusal is a 400 from `POST /start`. The code calls it `FlowNotRunnableError`.

**Do** one of two things:

- To run it for real, wrap it in a pipeline: a file `<pack>/pipelines/<name>.yaml`
  with one step for the flow, as the message spells out. See
  [Write a pipeline](/guides/write-a-pipeline/).
- To try the flow out, run it with `cairn run <ref> --interactive`, `--mock` or
  `--dry-run`. These run in your process and record nothing.

`cairn pack ls --pipelines` lists what you can start; `cairn pack ls --flows` lists the flows.

## No pipeline is installed (a missing pack)

You see one of these, and the command exits 1:

```text
no pipeline 'nosuch/x' is installed — see GET /pipelines/installed for what this deployment can start, and GET /flows for the components a pipeline can compose
```

```text
no installed pipeline 'nosuch/x'. Installed: compress/distill, compress/quantize, …
```

The first is the CLI with no server configured; the second is a server answering
that it doesn't have the ref, as HTTP 404. With no pack installed at all the list
reads `(none — is the pack installed?)`.

**It means** no installed pack ships a pipeline of that name, or you mistyped it,
or the pack is installed on a different machine from the engine. Packs are found
when the engine starts, from the packages installed where it runs.

**Do**

1. `cairn pack ls --pipelines`: is the ref there? Check the spelling against it.
2. If the pack isn't listed, install it in the environment the engine runs in. The
   example pack is `pip install 'cairndev[hello]'`. For your own pack, see
   [Write a pipeline](/guides/write-a-pipeline/) (`cairn pack add --dev <dir>` prints
   the `CAIRN_PACK_PATH` export that loads one from a directory).
3. Restart the engine (`cairn setup --stop`, then `cairn setup`), because packs are
   loaded at start.
4. `cairn setup --check` shows the packs it loaded under `packs_loaded`, and anything
   that failed to load.

A task leg that names an operator no installed pack provides is refused by the
operator lookup in this form:

```text
No operator registered for 'acme/thing'. Known: artifact/promote, audit/store, …
```

Install the pack that ships that operator. `cairn pack ls operators` prints the ones you have.

## A gate stopped the pipeline: `blocked by gate`

You see the pipeline end `failed`, the gate's step `skipped`, and the legs after
it never started. The reason on the record is:

```text
step 'data-ok' blocked by gate: steps.profile.verdict == 'pass' && steps.profile.record_count >= input.min_rows
```

**It means** a data gate (a gate with a `predicate:`) evaluated its condition
over the earlier legs' outputs and it came out false. The gate failed closed, which the
record calls `gate_blocked`. Nobody was asked, and nobody can approve past it.
See [Gates and approvals](/concepts/gates/#a-gate-answered-by-data).

**Do**

1. Read the reason: `cairn status <pipeline_id> --json`, under `trigger.failure_reason`.
2. See the values the predicate read: `cairn status <pipeline_id> --results`
   prints what each leg produced, such as `verdict` and `record_count` above.
3. Fix what the numbers say (the dataset, the input, the earlier leg) and launch again,
   or, if the threshold is wrong, change the predicate in the pipeline file.

A pipeline that *waits* is a different thing: a gate with no `predicate:` is a
person's, and the pipeline pauses with `awaiting_approval`. `cairn run` asks you at the
prompt, and with no terminal it exits 2 and prints the `cairn decide` line.
Answer with `cairn decide <pipeline_id> approve --step <gate>`, or `reject`.

## A launch is refused on cost: `admission door`

You see, when a launch prices legs that run on a box and the budget can't cover them:

```text
admission door: the priced legs are estimated at $<total> (<leg>: <basis>); the budget has $<left> of $<budget> left in series '<series>'
```

**It means** the launch door added up the estimate for the legs that run on a priced
connection, compared it with the pipeline's `governance.budget.usd` (or the
deployment's default or ceiling) less what the series has already spent or reserved,
and refused before any record exists. A leg on a priced box with no `timeout:`
is refused in the same way, with `declare a timeout to price this leg`.

**Do** raise `governance.budget.usd`, lower the estimate (a shorter `timeout:`, a
cheaper box), or start a new `series`. When a deployment sets a ceiling, the message
says so, and the pipeline file can't raise past it. See
[Cost and budgets](/concepts/cost/#budgets-on-the-pipeline).

## A leg is refused its reservation: `would exceed the cap`

You see a flow leg end `failed`, and `cairn run` exit 1, with:

```text
series 'hello-over-budget': this run's estimate $5.00 would exceed the $1.00 cap ($0.00 spent, $0.00 reserved by in-flight runs). Raise the budget or start a new series.
```

**It means** the launch was admitted, and then this leg tried to reserve its
`estimated_usd` against the series cap and the total would pass it. The leg
failed before its engine started, so it spent nothing. The audit trail has a
`leg.budget_refused` row with the estimate, the spend and the cap. The code raises this
as `BudgetExceeded`.

**Do** look at the three numbers in the message. If the estimate is wrong, fix
`estimated_usd`. If the cap is too low, raise `governance.budget.usd`. If other runs in the series
hold the money (the "reserved by in-flight runs" figure), wait for them to
finish, or start a new `series`. See [Cost and budgets](/concepts/cost/).

## A run stopped on a cost cap: `over budget`

You see a template leg fail, and the pipeline stop, with:

```text
session run-acme/summarise over budget: $1.012300 > $1.000000
```

For a flow, the stage reports instead:

```text
budget.max_cost_usd=0.5 exceeded <where> (spent $0.5123)
```

**It means** the model calls of that run cost more than a cap allows. The call
that crosses the cap has already been paid for, so the figure after `$` is a little above the cap. The audit trail has `leg.budget_exceeded` (or
`run.budget_exceeded`) with the spend and this reason.

**Check which cap it was: the number after `>`.** Two limits apply and the lower
one wins:

- the template's own `budget.max_cost_usd`, and
- the deployment's `MAX_COST_USD`, which is **$1 by default**. A template that
  declares `max_cost_usd: 5` still stops at $1 until you raise the deployment's limit.

If the figure after `>` is `1.000000` and you didn't set it, this is the default.

**Do** set `MAX_COST_USD` in the environment the engine starts from, to a figure you
accept (`MAX_COST_USD=5`; `0` turns the limit off), restart the engine
(`cairn setup --stop`, then `cairn setup`), and run again. Or make the run
cheaper. This is separate from the series cap and from `[profile.budget]`. See
[A cap on a template's model calls](/concepts/cost/#a-cap-on-a-templates-model-calls).

## A policy refused the launch

You see a launch fail with HTTP 403 and:

```text
policy '<policy>' denies launching pipelines with leg '<pack>/<leg>' in environment '<environment>': <the rule's reason>
```

**It means** a `deny` rule in a policy matched a leg of the pipeline when it was
planned. The launch is refused before any record exists, and the message names the
policy, the leg and the environment. The reason is shown only if the rule has one.
The audit trail has `pipeline.refused`. See
[Gates an environment adds](/concepts/gates/#gates-an-environment-adds).

**Do** open the policy the message names (its `name:`). Then either launch in an
environment it doesn't cover (`--environment`), take the leg out of the pipeline,
or ask whoever owns the policy to change the rule. A pipeline file can't override it.

## A launch is refused on data: holdout overlap and `eval_cutoff`

These come back as HTTP 403 with `admission door:` in front, because the same door
checks the datasets a launch names.

**Holdout overlap.** You see:

```text
admission door: dataset:train@v3 shares 12 row(s) with the eval set dataset:heldout@v2 (exact): r17, r31, …
```

It means rows of the training version are also in a held-out set, so a score on
that set would be measured on data the model trained on. `exact` means identical
rows; `near-dup` means near-duplicates. The message lists up to ten row ids.
Remove the overlapping rows from the training version and register a new version, or
check that the roles on these versions are right.

**A malformed `eval_cutoff`.** You see:

```text
admission door: eval_cutoff is not an ISO-8601 date or timestamp (e.g. '2026-06-01' or '2026-06-01T10:00:00+00:00'), so nothing was compared against the held-out sets. Correct it, or remove it to compare against all of them.
```

It means an input called `eval_cutoff` (anywhere in the launch inputs) isn't a date
cairn can read. The door refuses rather than guess, because ignoring a bad cutoff would
pass a typo off as a check. Write it as `2026-06-01` or a full timestamp, or remove
the input.

## Still stuck

- `cairn status <pipeline_id> --logs` and `cairn status <run_id> --logs` print the
  event log of each leg. `cairn status <run_id> --replay` prints what a run was given.
- `cairn setup --check` first: most launch problems start in the environment.
- [Ask in Discussions](https://github.com/cairndev-sh/cairn/discussions) or
  [open an issue](https://github.com/cairndev-sh/cairn/issues) with the message and the
  `Summary:` line of `cairn setup --check`.

## Next

- [Cost and budgets](/concepts/cost/): the caps and reservations behind the budget messages.
- [Gates and approvals](/concepts/gates/): data gates, person gates and policies.
- [CLI reference](/reference/cli/): every command and exit code.
