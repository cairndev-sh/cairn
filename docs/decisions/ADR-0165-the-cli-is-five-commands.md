# ADR-0165 — The CLI is five commands: setup, run, status, decide, pack

- **Status:** ACCEPTED 2026-11-02 by the owner, with the proposed answers to its questions.
- **Date:** 2026-11-02
- **Issue:** cairndev-sh/cairn#362 (the epic)
- **Supersedes:** ADR-0156 D1 (the CLI as it is) and ADR-0020 §24.20.14's verb list.
- **Amends:** ADR-0160 D4 (the trigger commands' names), ADR-0120 D4 (a `cairn.cli` provider names a
  help heading; a pack, operator or contract wheel may not be one), ADR-0128 (an extension adds no command).
- **Keeps:** ADR-0106 D1 and D6 (only a declared pipeline runs; `--dry-run`, `--mock` and
  `--interactive` are the record-free dev loop), ADR-0134 D6 (the `ask` stub), the HTTP API,
  `cairn-client`, every gate, every door and every record. This ADR renames and regroups the terminal
  surface. It does not change what a run does.

## Context

Counted from the command tree on `main` (core `a3170b8`, platform `f0779f87`):

| | Top-level names | Commands |
|---|---|---|
| Core | 25, plus a hidden `ask` | 70, including `dev`, `demo` and `worker` |
| Platform | 8 | 31 |
| **Total** | **33** | **101** |

The count is a symptom. The problems are:

- **Four ways to start something:** `run`, `pipeline run`, `remote launch`, `remote submit`.
- **The everyday commands live under `remote`,** even against a local engine: 23 subcommands.
- **Several commands for one decision:** `remote advance`, `approve`, `reject`, `cancel`,
  `cancel-pipeline`.
- **Six ways to look at one thing:** `remote pipelines`, `pipeline`, `ps`, `status`, `watch`, `findings`.
- **Deployment chores sit beside daily use:** `hash-password`, `worker-token`, `pipeline backfill`.
- **`-e` means two things:** the environment in `cairn pipeline explain`, the endpoint in `cairn remote`.

A person does four things: set up, run, check, decide. A fifth audience builds packs.

The total is not unusual. From each project's published CLI reference, 2026-11-02:

| Project | Top-level | Total | How the top level is organised |
|---|---|---|---|
| GitHub CLI | 34 | 242 | headings: Core, Actions, Additional |
| Airflow | 26 | 112 | flat list |
| kubectl | 44 | about 109 | flat list |
| MLflow | 24 | 95 | verbs and noun groups |
| DVC | 43 | 82 | verbs and noun groups |
| Helm | 25 | 58 | flat list |
| Dagster | 14 | 57 | noun groups |
| dbt | 29 | 29 | flat verbs |
| Docker | 33 | not on the page | flat table; a setting hides legacy commands |
| Terraform | 24 | not on the page | "Main commands" (5), then "All other commands" (19) |
| Temporal | 12 groups | not on the page | noun groups |

So this ADR removes overlap and leads with what a new user needs, as Terraform does. It does not aim for a
small total. GitHub CLI and Helm also show how commands from elsewhere can mount: `gh <ext>` and
`helm <plugin>` sit at the top level, and GitHub CLI's extensions cannot override its core commands.

## Decision

**D1 — `cairn --help` leads with five commands.** Everything else is listed below them, not hidden.

```text
Main commands:
  setup     Start or connect to an engine
  run       Run a pipeline and follow it
  status    See what's running, or one run's results
  decide    Approve, reject or cancel
  pack      Build and check packs

Platform commands:            (only when installed)
  ask   eval   insights   knowledge   pilot   series   showcase   suggest

All other commands:
  admin     Deployment and governance chores
```

`--version` stays on the root.

**D2 — `cairn setup`.** Start a local engine, or connect to a server, and check that it works.

```text
cairn setup [options]
  --profile NAME          the deployment profile the engine runs under
  --enable REF            turn on this pipeline's declared triggers; repeatable (ADR-0160)
  --server URL            connect to this server instead of starting an engine, and sign in
  --username, -u NAME     with --server; prompted when absent
  --password TEXT         with --server; prompted when absent
  --foreground            stay attached; the default is to run the engine in the background
  --stop                  stop the background engine
  --port N                engine port
  --db PATH               sqlite path (default ~/.cairn/dev.db)
  --seed                  seed deterministic demo data first
  --engine-only           start only the engine
  --web-only              start only the console dev server
  --check                 only check the environment and its services; start nothing
  --strict                with --check: warnings fail
```

**D3 — `cairn run`.** One door to everything that starts work. It goes through the launch door as
today and adds no second path. It stays attached, prompts at each gate, follows any pipeline its
`emits:` starts (the `correlation_id` from #289), and ends with the results. With no ref it lists what
can run.

```text
cairn run [REF] [options]
  REF                     a pipeline or flow ref; with --skill, a skill name; with no REF, list what can run
  --input, -i K=V|@FILE   an input, or a JSON file of inputs; repeatable
  --set, -s K=V           an input override (kept for existing scripts)
  --preset, -p NAME       start from a saved or pack-shipped input set
  --save-preset NAME      after a launch that worked, save its inputs under this name
  --environment, -e NAME  the environment to run in
  --workspace, -w NAME    whose variables and saved presets apply
  --detach                start it and return
  --interval SECONDS      poll interval while following
  --series ID             attach to a budgeted series
  --from RUN              the run this one is derived from
  --dry-run               show what would happen; run nothing (for a pipeline: the explain view and
                          the preflight verdict; for a flow: the dev loop's plan)
  --no-connections        with --dry-run: skip the connection-health check
  --mock                  stub LLMs and mockable operators (dev loop, or with --check)
  --interactive, -I       dev loop: run in-process and prompt in the terminal
  --mode live|sandbox     dev loop: sandbox blocks writes
  --check                 run a read-only check and exit on its verdict (CI)
  --fail-on fail|warn     with --check: the severity that exits non-zero
  --skill                 treat REF as a skill name
  --service NAME          with --skill or a raw payload: the service it targets
  --wait                  with --skill: block on the server until done
  --source NAME           the trigger source of a raw payload
  --timeout SECONDS       how long to wait at a prompt before --on-timeout
  --on-timeout ACTION     at a prompt timeout: detach, reject or abort
  --native                prompt with an OS dialog
  --dump FILE             write a pause's context to this file
  --json                  print machine-readable results
```

Exit codes: 0 done; 1 failed or cancelled; 2 waiting on a person with no terminal to ask on (it prints
the `cairn decide` command); 77 session expired.

**D4 — `cairn status`.** One door to everything you look at.

```text
cairn status [ID] [options]
  no ID                   what's running and what just finished
    --status S            filter: running, paused, done, failed
    --limit N             how many
    --watch, -n SECONDS   keep refreshing
  ID                      a pl_… pipeline or a run: its state, who decided what, what started it
    --results             every step's findings
    --logs                the event log
    --after-seq N         with --logs: resume after this event
    --events-only         with --logs or --follow: only lifecycle and transition events
    --follow, -f          stay attached, with the same prompts as `cairn run`
    --replay              replay the run's persisted trace
    --source auto|file|db with --replay: where the trace comes from
    --decision approve|reject  with --follow: answer each pause without asking
    --timeout, --on-timeout, --native, --dump   with --follow: as on `cairn run`
  --json                  the full record, or the raw form of what was asked for
```

**D5 — `cairn decide`.** One door to every decision. The id picks the behaviour.

```text
cairn decide ID approve|reject|cancel [options]
  --step STEP             which gate, when a pipeline waits at more than one
  --reason TEXT           why; required to reject a pipeline gate
  --approver NAME         recorded when sign-in is off; under sign-in the server records your identity
```

| Id | `approve` | `reject` | `cancel` |
|---|---|---|---|
| `pl_…` | `POST …/steps/{step}/advance` | `POST …/steps/{step}/reject` | `POST /pipelines/{id}/cancel` |
| a run | the run-level approve | the run-level reject | `POST /runs/{id}/cancel` |

A 404, 403 or 409 is printed as such. No new route or status.

**D6 — `cairn pack`.** For people who build packs.

```text
cairn pack new pack|operator|template|skill|policy NAME [--dest DIR] [--force] [--op NAME] [--template NAME]
cairn pack add DIR [--dev]                     make a scaffolded pack discoverable
cairn pack sync DIR [--check]                  regenerate operator metadata from the classes
cairn pack check [PATHS] [--strict] [--template-path DIR] [-v] [--json]
                                               validate offline, and fail on manifest and code drift
cairn pack test [PATH] [PYTEST_ARGS]           run the pack's tests
cairn pack lock [--check]                      write or check cairn.lock
cairn pack schema REF [--input FILE] [--json]  the UI contract for a template
cairn pack ls [PACK|operators] [--json] [--unused] [--flows] [--pipelines]
                                               what's installed
cairn pack show REF [--plan] [--split] [--graph] [--output FILE] [-i K=V] [-p NAME] [-w NAME] [--json]
                                               a pipeline's legs, gates and wires; a flow's split at
                                               its gates; a template's graph
cairn pack export REF [--to pipeline|metaflow] [--out PATH] [--config FILE] [--stdout] [--demo]
                                               render as a file
cairn pack build                               (hidden stub: part of cairn-platform)
```

**D7 — `cairn admin`.** Listed under "All other commands". Nothing here is new behaviour.

```text
cairn admin connect add PROVIDER [--name] [--set] [--secret] [--description] [--disabled] [--force]
                                 [--via] [--broker] [--connection-id] [--scope] [--preset]
cairn admin connect authorize NAME
cairn admin connect ls
cairn admin connect presets
cairn admin connect providers
cairn admin connect rm NAME [--force]
cairn admin connect test NAME
cairn admin trigger ls
cairn admin trigger enable ID
cairn admin trigger disable ID
cairn admin preset ls REF
cairn admin preset rm REF NAME
cairn admin audit verify [--audit-log FILE] [--json]
cairn admin lineage SUBJECT [--json] [--audit-log FILE] [--expect-head HASH]
cairn admin compute status CONNECTION [--json]
cairn admin compute sweep [--region] [--apply] [--json]
cairn admin config workflow REF [--json]
cairn admin deploy doctor [--env] [--values] [--strict]
cairn admin worker [--remote] [--pull] [--engine-url] [--token] [--capabilities] [--image] [--exec-local]
                   [--no-gpus] [--once] [--lease-seconds] [--poll-seconds] [--worker-id] [--dataset] [--task-queue]
cairn admin worker-token [--name] [--days]
cairn admin hash-password [--password]
cairn admin console
cairn admin demo [--workspace] [--archive]
cairn admin backfill [--apply] [--revert] [--batch-size] [--workspace] [--limit] [--infer-ref] [--verbose]
cairn admin respell-packs [--apply] [--server-stopped] [--verbose]
```

**D8 — A command's name never depends on which repository holds it.** A `cairn.cli` provider's commands
mount at the top level, as they do today, and the public CLI still wins every name. The provider names the
`--help` heading its commands appear under. Moving a command
from the platform to the public core changes its heading, never its spelling, so moving one is not a
rename and needs no alias or migration note. Each app has its own command, named after the app: `ask`,
`eval`, `insights`, `pilot`, `showcase` today, and any new app the same way.

**A pack, operator or contract wheel adds no command.** The public `cairn-extensions` repository has none
today, and it stays governed by core: its pipelines run through `cairn run`, and anything it needs from the
terminal is a core command. Core enforces this: a `cairn.cli` provider from a distribution named
`cairn-pack-*`, `cairn-operator-*` or `cairn-contracts-*` is refused through the existing skip path, logged
and shown by the doctor check. The extensions' `scripts/check_leaks.py` fails a `cairn.cli` entry point in CI,
and any `[project.scripts]` entry except `cairn-train-local`, the on-box program `remote/exec` runs for a
training job, which is not a `cairn` command.

Platform commands also lose their overlap:

```text
cairn ask plan REF [--input] [--json]
cairn ask run INTENT [--model] [--json]
cairn eval PACK [--case] [--dataset] [--env] [--json] [--save-baseline] [--compare-against]
                [--threshold] [--recipe] [--output] [--tier] [--runtime] [--workflow]
cairn pilot new NAME [--hypothesis] [--workspace] [--json]
cairn pilot ls [--pilot] [--workspace] [--json]
cairn pilot show ID --view overall|cohort|delta|compare|rollup [--risky] [--experiment] [--workspace] [--json]
cairn pilot add experiment PILOT [--name] [--variant] [--baseline] [--workspace] [--json]
cairn pilot add run EXPERIMENT RUN [--role] [--workspace] [--json]
cairn pilot add score RUN [--dim] [--reviewer] [--inter-rater] [--notes] [--workspace] [--json]
cairn pilot add baseline RUN [--setup-minutes] [--tools] [--commands] [--retries] [--quality] [--notes]
                             [--workspace] [--json]
cairn pilot add finding PILOT TEXT [--severity] [--run] [--workspace] [--json]
cairn pilot add link RUN SYSTEM [--ref-id] [--url] [--workspace] [--json]
cairn series start NAME [--budget] [--json]
cairn series ls [--json]
cairn series compare SERIES [--json]
cairn knowledge add NAME [--git] [--role] [--workspace] [--license] [--source] [--index]
cairn knowledge list
cairn knowledge index [NAME] [--packs]
cairn knowledge remove NAME
cairn suggest generate WORKFLOW [--force] [--min-size] [--json]
cairn suggest list [--status] [--json]
cairn suggest show ID [--json]
cairn suggest accept ID [--actuate] [--note]
cairn suggest reject ID [--reason]
cairn showcase [--workspace]
cairn insights resync [--scope] [--workspace] [--pipeline] [--full] [--json]
```

The top level stays 8 names, and 31 commands go down to 21.
`suggest` keeps `accept` and `reject` because `cairn decide` is core: routing a suggestion id through it would
make core learn a platform record, or need a new provider seam.

**D9 — Options every engine command takes.** `--endpoint`, `--token` and `--profile`, long form only.
`-e` always means the environment. An old `cairn remote … -e` alias keeps meaning the endpoint until it
is removed.

**D10 — Where every current command goes.** This table is complete: a test fails if a command in the
tree has no row (D11).

### Core

| Today | After |
|---|---|
| `cairn --version` | unchanged |
| `cairn ask` | unchanged (hidden stub) |
| `cairn audit` | `cairn admin audit` |
| `cairn audit verify` | `cairn admin audit verify` |
| `cairn compute` | `cairn admin compute` |
| `cairn compute status` | `cairn admin compute status` |
| `cairn compute sweep` | `cairn admin compute sweep` |
| `cairn config` | `cairn admin config` |
| `cairn config workflow` | `cairn admin config workflow` |
| `cairn connect` | `cairn admin connect` |
| `cairn connect add` | `cairn admin connect add` |
| `cairn connect authorize` | `cairn admin connect authorize` |
| `cairn connect ls` | `cairn admin connect ls` |
| `cairn connect presets` | `cairn admin connect presets` |
| `cairn connect providers` | `cairn admin connect providers` |
| `cairn connect rm` | `cairn admin connect rm` |
| `cairn connect test` | `cairn admin connect test` |
| `cairn demo` | `cairn admin demo` |
| `cairn deploy` | `cairn admin deploy` |
| `cairn deploy doctor` | `cairn admin deploy doctor` |
| `cairn dev` | `cairn setup` (`--foreground` to stay attached) |
| `cairn doctor` | `cairn setup --check` |
| `cairn export` | `cairn pack export --to pipeline` |
| `cairn flow` | `cairn pack` |
| `cairn flow export` | `cairn pack export --to metaflow` |
| `cairn flow ls` | `cairn pack ls --flows` |
| `cairn flow split` | `cairn pack show --split` |
| `cairn gate` | `cairn run --check` |
| `cairn init` | `cairn pack new` |
| `cairn init operator` | `cairn pack new operator` |
| `cairn init pack` | `cairn pack new pack` |
| `cairn init policy` | `cairn pack new policy` |
| `cairn init skill` | `cairn pack new skill` |
| `cairn init template` | `cairn pack new template` |
| `cairn lineage` | `cairn admin lineage` |
| `cairn lock` | `cairn pack lock` |
| `cairn ls` | `cairn pack ls` |
| `cairn pack` | unchanged |
| `cairn pack add` | unchanged |
| `cairn pack build` | unchanged (hidden stub) |
| `cairn pack check` | unchanged; also takes `cairn validate`'s work |
| `cairn pack schema` | unchanged |
| `cairn pack sync` | unchanged |
| `cairn pipeline` | `cairn run` and `cairn pack` |
| `cairn pipeline backfill` | `cairn admin backfill` |
| `cairn pipeline explain` | `cairn run --dry-run` |
| `cairn pipeline ls` | `cairn run` with no ref, or `cairn pack ls --pipelines` |
| `cairn pipeline respell-packs` | `cairn admin respell-packs` |
| `cairn pipeline run` | `cairn run` |
| `cairn pipeline show` | `cairn pack show` |
| `cairn preflight` | `cairn run --dry-run` |
| `cairn remote` | removed as a group; see its rows |
| `cairn remote advance` | `cairn decide <pl_id> approve --step` |
| `cairn remote approve` | `cairn decide <run> approve`; `--watch` and `--decision` are `cairn status --follow --decision` |
| `cairn remote cancel` | `cairn decide <run> cancel` |
| `cairn remote cancel-pipeline` | `cairn decide <pl_id> cancel` |
| `cairn remote console` | `cairn admin console` |
| `cairn remote delete-preset` | `cairn admin preset rm` |
| `cairn remote disable-trigger` | `cairn admin trigger disable` |
| `cairn remote enable-trigger` | `cairn admin trigger enable`, or `cairn setup --enable` |
| `cairn remote findings` | `cairn status --results` |
| `cairn remote hash-password` | `cairn admin hash-password` |
| `cairn remote launch` | `cairn run` |
| `cairn remote login` | `cairn setup --server` |
| `cairn remote logs` | `cairn status --logs` |
| `cairn remote pipeline` | `cairn status <pl_id> --json` |
| `cairn remote pipelines` | `cairn status` |
| `cairn remote presets` | `cairn admin preset ls` |
| `cairn remote ps` | `cairn status` |
| `cairn remote reject` | `cairn decide reject` |
| `cairn remote status` | `cairn status <run>` |
| `cairn remote submit` | `cairn run --skill` |
| `cairn remote triggers` | `cairn admin trigger ls` |
| `cairn remote watch` | `cairn status --watch` |
| `cairn remote worker-token` | `cairn admin worker-token` |
| `cairn replay` | `cairn status --replay` |
| `cairn run` | unchanged, and it also takes pipelines and follows them |
| `cairn test` | `cairn pack test` |
| `cairn validate` | `cairn pack check` |
| `cairn visualize` | `cairn pack show --graph` |
| `cairn worker` | `cairn admin worker` |

### Platform

| Today | After |
|---|---|
| `cairn ask` | unchanged |
| `cairn ask plan` | unchanged |
| `cairn ask run` | unchanged |
| `cairn eval` | unchanged |
| `cairn insights` | unchanged |
| `cairn insights resync` | unchanged |
| `cairn knowledge` | unchanged |
| `cairn knowledge add` | unchanged |
| `cairn knowledge index` | unchanged |
| `cairn knowledge index-packs` | `cairn knowledge index --packs` |
| `cairn knowledge list` | unchanged |
| `cairn knowledge remove` | unchanged |
| `cairn pilot` | unchanged |
| `cairn pilot attach` | `cairn pilot add run` |
| `cairn pilot baseline` | `cairn pilot add baseline` |
| `cairn pilot cohort` | `cairn pilot show --view cohort` |
| `cairn pilot compare` | `cairn pilot show --view compare` |
| `cairn pilot delta` | `cairn pilot show --view delta` |
| `cairn pilot experiment` | `cairn pilot add experiment` |
| `cairn pilot finding` | `cairn pilot add finding` |
| `cairn pilot link` | `cairn pilot add link` |
| `cairn pilot ls` | unchanged |
| `cairn pilot new` | unchanged |
| `cairn pilot overall` | `cairn pilot show --view overall` |
| `cairn pilot rollup` | `cairn pilot show --view rollup` |
| `cairn pilot score` | `cairn pilot add score` |
| `cairn series` | unchanged |
| `cairn series compare` | unchanged |
| `cairn series ls` | unchanged |
| `cairn series start` | unchanged |
| `cairn showcase` | unchanged |
| `cairn suggest` | unchanged |
| `cairn suggest accept` | unchanged |
| `cairn suggest generate` | unchanged |
| `cairn suggest list` | unchanged |
| `cairn suggest reject` | unchanged |
| `cairn suggest show` | unchanged |

Programs other than `cairn` are unchanged: `cairn-server`, `cairn-worker`, `cairn-admin` (the read-only
admin UI), `cairn-dataflow-run`, and `cairn-train-local` (from `cairn-operator-training`).

**D11 — A test pins the surface.** One test walks the command tree and fails when a main command is not
in D1, or when a command has no row in D10 while the aliases exist.

**D12 — Old names keep working for one minor version** (`docs/compatibility.md`). Each is a hidden alias
that prints the new spelling on stderr and then runs the same code. They are removed in the next minor
version, with a migration note.

## Consequences

- A new user reads five lines of help. The laptop demo is three commands:

  ```text
  curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_PACKS="finetune[laptop] dataset-prep" sh
  cairn setup --profile finetune-local --enable finetune/sft-local
  cairn run dataset-prep/prepare --preset laptop-demo
  ```

  The install flag, the `[laptop]` extra, the preset's path to the bundled sample and the local artifact
  store are separate issues, not this ADR.
- **A platform command can go public without breaking anyone.** It moves from one `--help` heading to
  another, and its spelling stays.
- **`cairn run` grows, but its existing behaviour stays.** It already starts a flow as a governed one-leg
  pipeline, and `--dry-run`, `--mock` and `--interactive` stay the record-free dev loop (ADR-0106 D6).
  What changes: it also takes pipelines, follows them across events, and prompts at gates. Its `--input`
  takes `K=V` or `@FILE`; a bare path still works during the alias period. Its `--env` becomes
  `--environment`.
- **`--dry-run` means two things by ref:** the explain and preflight view for a pipeline, and the dev
  loop's plan for a flow. Both run nothing.
- **Deployers type more:** `cairn admin connect add`.
- **`cairn admin` and the `cairn-admin` program are different things** with nearly the same name.
- **A background engine is new state:** a pid file under `~/.cairn`, and `cairn setup --stop`.
- The new names reach the docs, `demos/laptop`, `examples/ci`, the CI proof tests and the platform's
  provider mount (`docs/contracts`, `13-pipelines.md`, `reference/cli.md`).
- ADR-0160's `cairn remote enable-trigger`, released in 0.3.3, becomes an alias one release after it shipped.

## Build order

One issue and one PR at a time:

1. `cairn status` and `cairn decide`, over the existing client methods, with aliases.
2. `cairn run`: takes pipelines, follows, prompts, `--dry-run`, `--check`, `--skill`.
3. `cairn setup`, including the background engine and `--enable`.
4. `cairn pack` and `cairn admin`; the `--help` headings and the provider's heading and `admin` mount; the
   D11 test.
5. The platform's reductions, in `cairn-platform`, on the released core.
6. Docs and the demo move to the new names.
7. Remove the aliases in the next minor version.

## Decided with this ADR

1. **`cairn decide`,** not separate `approve`, `reject` and `cancel` verbs.
2. **The engine runs in the background by default;** `--foreground` stays attached.
3. **`cairn export`, `flow export`, `config workflow`, `pipeline backfill` and `pipeline respell-packs` move for
   now.** Nothing in the public docs, tests, CI, the demo or the packs mentions them, which doesn't prove nobody
   outside uses them. Whether to retire them is decided when the aliases go.
4. **`pilot`, `series`, `knowledge` and `suggest` stay CLI commands.**

## Alternatives rejected

- **Tidy `--help` only.** Cheap and safe, but the four ways to start something remain.
- **Nine everyday commands and six groups.** Tidier, but still about twenty names to read. The owner asked
  for fewer.
- **Hide `cairn admin`.** No project surveyed hides a whole audience, and hidden commands are hard to find.
  Terraform's "All other commands" gives the same short start with nothing lost.
- **A `cairn platform` group.** Moving a command to the public core would rename it, a breaking change for
  everyone using it, and the name would describe where the code lives, not what it does.
- **`cairn suggest` decisions through `cairn decide`.** Fewer commands, but core would have to learn a
  platform record or grow a new provider seam.
- **`cairn start`, leaving `cairn run` as it is.** No rename, at the cost of a sixth command and two verbs
  that sound the same. It also isn't needed: `cairn run` already goes through the launch door.
- **Delete the deployment commands and leave them to the console.** The console is the private platform;
  a public install needs them.
