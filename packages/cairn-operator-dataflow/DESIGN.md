# dataflow — design

- **Distribution:** `cairn-operator-dataflow` (`cairn/operators/builtins/dataflow/`)
- **Capabilities provided:** `dataflow.run`, `ml.run`, `artifact.promote`
- **Status:** proven (builtin, builtin-ssh). The Metaflow and SkyPilot engines are their own
  optional wheels, `cairn-operator-dataflow-metaflow` and `cairn-operator-dataflow-skypilot` (ADR-0134 D2/D3)
- **Decisions:** ADR-0081 (dataflow boundary + engines), ADR-0082 (kind: ml flows),
  ADR-0083/ADR-0089 (Metaflow as substrate), ADR-0088 (checkpoints + resume),
  ADR-0090 (stage fan-out), ADR-0104 D5 (SkyPilot as the provisioning engine),
  ADR-0129 (the engine substrate is core),
  #1048, #1050, #1074, #1512, #2025, #2073, #2102, #2583

## 1. Purpose

This pack is the **one door to the data plane**. A workflow that needs to move,
clean, split, export or train over data does not name a runtime, a box or a
cluster: it calls `dataflow/run` (kind `data`) or `ml/run` (kind `ml`) with a
list of stages, and the *environment* decides where that runs. Everything the
control plane cares about — the artifacts produced, the scalar stats gates
judge, the cost, the lineage — comes back across a boundary that never carries
rows.

The pack deliberately does NOT: define stages (kernels live in the stage
registry, `cairn.dataflow_stages`), read packs from inside an engine (refs are
resolved control-side so every engine receives concrete stages), hold database
or registry credentials on a job machine (the control plane registers the
artifacts an engine merely describes), or expose engines as operators — an
engine is a backend selected by configuration, never a second operator with a
second governance surface (ADR-0081 rejected design R7).

## 2. Architecture

Three thin operators over one contract and a registry of engines:

```
workflow leg
  └─ dataflow/run | ml/run          (remote-job operator: submit → poll → cancel)
       └─ DataflowSpec              (stages · input_artifacts · budget · resume_from · parent_run)
            └─ engine (selected by the ENVIRONMENT, cairn.dataflow_engines)
                 builtin       → cairn-dataflow-run as a local subprocess
                 builtin-ssh   → the same binary on a compute.ssh box (cairn-operator-remote)
                 metaflow      → the CairnFlow bridge on Metaflow's substrate (k8s/Batch) —
                                 in cairn-operator-dataflow-metaflow since ADR-0134 D2
                 skypilot      → a cluster this engine PROVISIONS and gives back
            ← EngineJobStatus (scalar metrics only) → stage events → the console
            ← DataflowResult (artifact descriptors + scalar stats) → registry → findings
```

**The contract is the invariant, not the transport.** `cairn.core.dataflow`
owns `DataflowSpec` / `DataflowResult` / `EngineJobHandle` / `EngineJobStatus`
and the `DataflowEngine` protocol; core also owns *selection*
(`cairn.core.dataflow.engine_registry`), because core's dispatch path must pick
an engine without importing this pack (#1512, ADR-0084 D1). This pack owns
*construction*: each factory in `engines.py` reads its own configuration, which
is why construction stayed here.

**The substrate is core too (#2583, ADR-0129).** The kernel runner
(`run_dataflow`, with the ADR-0118 stage-contract checks and budget
enforcement), the stage loader (`load_stages` over the `cairn.dataflow_stages` /
`cairn.flow_stages` groups, plus the `noop` / `noop.slow` conformance stages),
the workdir file readers (`metrics.json` / `result.json`, `validate`) and the
remote fetch contract (`fetch_list` / `fetch_dir` / `rewrite_fetched`) live in
`cairn.core.dataflow.{runner,stage_loader,results,fetch}`. An engine adapter in
ANY pack imports them from core, so an adapter never needs this pack — the
operator-pack → operator-pack edge the boundary test forbids. This pack keeps
the engines, the factories, the bridges, the `cairn-dataflow-run` entrypoint
and the two conformance entry points; `operator_dataflow.engine.runner`,
`operator_dataflow.stages` and the fetch names in `engines_ssh` re-export the
core objects for one release (ADR-0129 D4), with no warning.

**One binary, several transports.** `cairn-dataflow-run` (the pack's console
script) is the on-machine entrypoint for the builtin, SSH and SkyPilot paths.
The spec travels as `CAIRN_DATAFLOW_SPEC` in the environment and results come
back as `result.json` / `metrics.json` in the job workdir. Metaflow is the
exception by design: its bridge flow calls the same runner in-process so the
substrate can supply `@resources`, `@retry` and a datastore.

**The kernels are the processing power; a substrate only schedules.** That is
why every engine is kind-blind — one adapter serves `data` and `ml` flows, and
the environment maps kinds to engines many-to-many.

**Streaming back.** A running flow reports over the #2025 job channel: every
submit path exports `live_channel_env_for(parent_run, node)` so a kernel's
`LiveReporter` reaches the control plane over NATS or HTTPS. Poll metrics are
scalar-enforced at the type layer — a status can never smuggle rows.

**Durability and partial failure.** A stage may declare `checkpoint: true`; the
runner snapshots the working set synchronously and `resume_from` seeds a fresh
workdir past the last durable stage (ADR-0088). A failed snapshot degrades, it
never fails the flow. Cancel kills the *whole* compute — the process group
locally, the remote group over SSH, the cluster on SkyPilot — because an
orphaned job is a cost leak (#603).
A local job also never outlives its owner (#2614): the builtin engine passes its pid as
`CAIRN_DATAFLOW_OWNER_PID`, and the runner kills its own process group once it is
re-parented — a crashed server no longer leaves a job writing behind a leg the
restarted server settles `failed` ("process table lost").

**Idempotency.** Handles are self-describing: everything poll/cancel need rides
in `EngineJobHandle.meta`, so a control-plane restart re-attaches instead of
re-submitting. The SkyPilot cluster name is derived from run id + node for the
same reason.

### The SkyPilot engine

Moved to its own optional wheel, `cairn-operator-dataflow-skypilot`, offered by
`cairndev[skypilot]` (ADR-0134 D3a); its design record is that pack's `DESIGN.md`. The
on-machine entrypoint it starts (`cairn-dataflow-run`) is still this pack's.

## 3. Operators

| Operator | Capability | Risk | Mock | Input -> Output | Reports (stages · series · scalars) |
|---|---|---|---|---|---|
| `dataflow/run` | `dataflow.run` | external_write | yes | `DataflowRunInput` -> `DataflowRunOutput` | `—` |
| `ml/run` | `ml.run` | external_write | yes | `MlRunInput` -> `MlRunOutput` | `—` |
| `artifact/promote` | `artifact.promote` | external_write | yes | `ArtifactPromoteInput` -> `ArtifactPromoteOutput` | `—` |

All three declare `ReportSpec.NONE`, and that is a decision, not an omission
(ADR-0103): a remote-job operator's progress already flows through the poll
path's `JobStatus.metrics`, which `cairn.core.operator.stage_events` turns into
progress events — and the stage names in that stream belong to the *resolved
flow*, so they are per-pack data rather than a vocabulary these classes could
declare statically.

**`dataflow/run`** — a `remote_job` operator (`submit` → `poll` → `cancel`). It
builds a `DataflowSpec`, resolves a pack-addressed `dataflow_ref` control-side,
hands the spec to the environment's engine, and on success registers the
returned artifact descriptors in the registry before returning scalar stats
into findings. It refuses a spec with neither `dataflow_ref` nor `stages`, and
a spec whose stages the selected engine does not know (`engine.validate` is
capability discovery, reported before submission).

**`ml/run`** — the kind-symmetric door (ADR-0082 D7). Same shape, resolving
`flow_ref` with `kind="ml"` and reading the `ml_engine` / `ml_target` half of
the environment. Its product is a packaged candidate, not a dataset.

**`artifact/promote`** — moves a governed tag onto an artifact. `requires_approval`
is declared on the manifest, so the durable HITL gate fires at the chokepoint
*before* the tag moves: promotion lands in the existing run gate queue instead
of inventing a second approval mechanism.

## 4. Configuration

| Setting | Where | Scope | Default | What it does |
|---|---|---|---|---|
| `dataflow_engine` / `CAIRN_DATAFLOW_ENGINE` | backend field | deployment | `builtin` | which engine runs `kind: data` flows |
| `dataflow_target` / `CAIRN_DATAFLOW_TARGET` | backend field | deployment | `` | the compute connection that engine dispatches to |
| `ml_engine` / `CAIRN_ML_ENGINE` | backend field | deployment | `builtin` | which engine runs `kind: ml` flows |
| `ml_target` / `CAIRN_ML_TARGET` | backend field | deployment | `` | the compute connection for ML flows |
| `metaflow_*` | backend fields | deployment | see schema | read by the Metaflow engine's own wheel, `cairn-operator-dataflow-metaflow` (its DESIGN.md; ADR-0134 D2) |
| `skypilot_*`, `CAIRN_SKYPILOT_*` | backend fields / env | deployment | see schema | read by the SkyPilot engine's own wheel, `cairn-operator-dataflow-skypilot` (its DESIGN.md; ADR-0134 D3a) |
| `CAIRN_DATAFLOW_REMOTE_CMD` | env | deployment | `cairn-dataflow-run` | on-box entrypoint for boxes whose venv bin is not on the non-interactive SSH PATH (#1075) |
| `remote_env` / `CAIRN_REMOTE_ENV` | backend field | deployment | empty (nothing extra) | comma- or space-separated NAMES of control-plane env variables the SSH engine passes to the job on the box (see "Running on a compute.ssh box") |
| `remote_exec_mode` / `CAIRN_REMOTE_EXEC_MODE` | backend field | deployment | `auto` | how the SSH engine runs a flow on the box (ADR-0104 D1): `venv` runs the box's virtualenv; `image` runs the named container image; `auto` is `image` when an image is named, else `venv`. A `compute.ssh` connection's own `mode` overrides it per box. Image mode with no image named anywhere is refused at submit |
| `remote_image` / `CAIRN_TRAIN_IMAGE` | backend field | deployment | `` (none) | the container image image mode runs; a connection's own `image` overrides it per box. There is no built-in image (#202, ADR-0150): with none named, `auto` runs the virtualenv and `image` is refused at submit. Pin a tag, never `:latest` |
| `CAIRN_DATAFLOW_IMAGE_CMD`, `CAIRN_DATAFLOW_IMAGE_GPUS` | env | deployment | `cairn-dataflow-run`, `all` | the entrypoint inside the image, and the `--gpus` value (empty disables the flag) |
| `stages`, `dataflow_ref`/`flow_ref`, `input_artifacts`, `config` | `input_schema` | run | — | per-run bounds: what to run over what |
| `budget.max_cost_usd`, `budget.max_rows`, `budget.max_seconds` | `input_schema` | run | — | per-run ceilings the ENGINE self-enforces; `max_cost_usd` is what the SkyPilot cost door compares its estimate against |

**Per-run bounds belong in `input_schema`, and here they are.** None of these
three operators declares a `config_schema`, which matters: a ceiling declared in
a governance-scoped `config_schema` field is silently DROPPED at run time
(ADR-0068/0070, #905), so the pack would advertise a budget it never enforced.
`budget` rides on the spec, per run, and the engine reads it.

Everything in the first block is **environment-owned** and unsettable from a
run: the whole point of ADR-0081 D4 is that a workflow cannot choose its own
execution substrate.

**Connections.** The SSH and SkyPilot paths need a `compute.ssh` (or
`compute.vast` / `compute.runpod` / AWS-credentialed) connection named by
`<kind>_target`, resolved through `cairn.core.connections.resolve_connection` so
secret refs stay refs in the profile. Absent, they fail closed at submit and —
for SkyPilot — at the launch door, before the run record exists. The Metaflow
engine additionally delivers only the connections `metaflow_secrets` declares;
under strict isolation nothing else of the control plane's environment reaches a
task. Optional extras: `[engine]` (polars/duckdb/pyarrow — what a job machine
installs), `[ssh]`. Metaflow and SkyPilot are `cairndev[metaflow]` / `cairndev[skypilot]` (their own wheels, ADR-0134 D2/D3).

**Running on a compute.ssh box** (`<kind>_target` names the connection; the
control plane is elsewhere). What the deployment provides on each side:

- **Control plane:** `cairndev[ssh]` (asyncssh; without it the engine names the
  extra), the `compute.ssh` connection with its `PRIVATE_KEY` secret, and the
  same contract and kernel wheels the box has: `validate` reads the LOCAL
  registry, so a stage or contract version the box lacks is only found on the box.
- **Box:** `cairn-dataflow-run` (from `cairndev`) plus the kernel wheels, at the
  same versions. `runner.sh` is run by plain `sh` over a non-interactive SSH
  session, which reads no login profile: the console script must be on that
  PATH, or the deployment sets `CAIRN_DATAFLOW_REMOTE_CMD` to its full path
  (exit 127 otherwise). Image mode (`remote_exec_mode`) needs none of this,
  only a container runtime.
- **Environment the job gets:** the spec, the parent run, the live channel,
  `MLFLOW_*` when set, and the names the deployment lists in `remote_env`
  (profile record, else `CAIRN_REMOTE_ENV`), each forwarded when set on the
  control plane: e.g. `CAIRN_CHECKPOINT_BUCKET, CAIRN_CHECKPOINT_PREFIX,
  AWS_DEFAULT_REGION` for a trainer that mirrors checkpoints and publishes to
  S3. Nothing else of the control plane's environment crosses. A flow or a
  tenant cannot extend the list. A name containing `KEY`, `SECRET`, `TOKEN` or
  `PASSWORD`, a malformed entry, or a name the engine sets itself (the spec,
  lineage and live-channel names) refuses the leg before submit, naming the
  variable (a malformed entry only by its position)
  and never its value: credentials stay on the box (an instance role,
  the box's own environment). Values reach the box only in the job's
  `runner.sh`, which is created exclusively with mode 0600 (a stale copy is
  removed first; one that survives fails the submit),
  or as `docker run -e NAME` with no value in image mode; never on a command
  line. The job handle, which is persisted on the pipeline step and returned by
  `GET /pipelines/{id}`, records the exec input without its environment and the
  forwarded NAMES (`env_names`) only.
  `MLFLOW_TRACKING_PASSWORD` is the one credential the engine forwards itself
  (with the other `MLFLOW_*`, #1235), never through `remote_env`; it too lands
  only in the 0600 runner file (ADR-0149 D2).
- **Dataset versions:** the engine declares `needs_durable_inputs`, so the leg
  executor resolves its `dataset:` refs to the version's durable copy
  (`durable_uri`, the object store) when the workspace's registration has one
  (ADR-0149 D3); a co-located leg keeps the registered location unless that file
  is gone. A version whose only location is a
  control-plane path (an absolute path or any `file:` URI, then `#sha256=…`),
  anywhere in the spec's or a stage's config, refuses the leg in
  `validate`, before submit, saying how to give it a durable copy
  (`CAIRN_ARTIFACT_STORE=s3` + `CAIRN_ARTIFACT_S3_BUCKET` on the control plane,
  then register the bytes again or re-run the step that produced them). A plain
  local path with no pin, an upstream leg's hand-off, is still uploaded (#1180).
- **Cancel and checkpoint pull-back** reconnect with the `PRIVATE_KEY` of the
  connection the job was submitted with: `remote/exec` records its name, its
  account and the workspace in the job handle, and resolves it with that
  account, so no workspace needs to be in scope (ADR-0149 D4). The engine's
  current target is used only for a handle saved before that. When the box
  cannot be reached they log a warning naming the host, workdir, connection and
  error type (no secret material); cancel then raises `EngineError`, so the
  caller reports it as not cancelled.

## 5. Governance

All three operators are `external_write`: a dataflow writes datasets and models,
and a promotion moves a tag other runs read. `dataflow/run` and `ml/run` are
`remote_job` + `long_running` + `supports_cancel`, so a governed environment
gates the *submission* with one HITL approval for the whole flow (the fix-batch
model) rather than per stage; `artifact/promote` carries `requires_approval` on
the manifest so the gate is structural.

Auditable: the submitted spec, the engine name, every scalar stat and cost the
result carries, the registered artifact descriptors with their content hashes
and parents (the lineage edges), and — for SkyPilot — the `skypilot.receipt`
event for every cluster teardown. Scalar enforcement is a type-layer invariant,
so nothing row-shaped can enter the audit through a status or a stat.

Three launch-door refusals apply to this pack and are enforced in core, before any
run record exists: the **job-channel door** (a remote-job leg on a NATS channel
needs a reachable broker, #2025), the **flow-engine door**
(`cairn.core.dataflow.engine_door`, ADR-0134 D3c; the SkyPilot door until then: the
selected engine installed, then the engine's own `check_ready()` — for SkyPilot a
target that is set and resolves, the extra, `ssh` and `rsync` on PATH, an enabled
cloud — #2073, #2102) and the **pull-worker
door** (`cairn.core.pull_worker_door`: an `ml_target` of `worker:<pool>` has
nowhere to run until a worker has checked in — #565, ADR-0104 D2). All three name
the remedy and none auto-starts, installs or provisions anything. A deployment
policy may relax none; what it *can* choose is the engine, the target and the
budget ceilings.

They are asked at **two** launch surfaces, and both matter. `RunDispatcher.dispatch`
asks for a template run; `cairn.core.dispatch.pipeline_doors` asks for a PIPELINE,
from `launch_pipeline_def` (#2101) — which is where `dataflow/run` and `ml/run`
are usually reached, because a pipeline `flow:` leg *is* one of those operators
with its ref inlined (ADR-0082 D7's kind-matched doors), submitted by the bridge
without ever passing through the dispatcher. The pipeline ask narrows each door
to the legs the plan actually has: a data-only pipeline is not refused because
`ml_engine` names an engine none of its legs will construct, and a `kind: data`
leg is judged by `dataflow_target` rather than `ml_target`.

## 6. Failure modes

| Symptom | Cause | What the operator returns | Fail closed? |
|---|---|---|---|
| Flow ran on the builtin engine though the profile said Metaflow (#1093) | backend selection read `os.environ` directly, ignoring the profile record | ran in the wrong place, silently | no — fixed: selection goes through the one config plane |
| A pipeline `flow:` leg bypassed all three service doors (#2101) | the doors were asked only in `RunDispatcher.dispatch` and `build_preflight`; the bridge submits a flow leg through `resolve_engine()` / `engine.submit()` directly, so `POST /pipelines/launch` returned `200 running` and the leg died late at submit — after the pipeline record, the leg record and the run existed | now REFUSED in `launch_pipeline_def` before `start_pipeline`, naming the door, the leg and the door's own remedy (503 at both HTTP launch doors); the engine's late `EngineError` stays as the backstop for anything the plan cannot name | yes — and the doors' own unit tests, which drive the dispatcher, are why this stayed green while the surface every fine-tune pipeline uses was uncovered |
| SSH target set but the `[ssh]` extra absent (#2417) | `CAIRN_DATAFLOW_TARGET` routes to the SSH engine, whose dispatch lives in `cairn-operator-remote` (a builtin importing an official pack — debt #2419 retires) | `EngineError` naming the `[ssh]` extra at submit/poll/cancel; a missing module the extra does not provide still propagates as itself | yes |
| A builtin job kept running after its server died (#2614) | the runner is in its own session (so cancel can kill the group, #603), so a SIGKILLed server left it running; the restarted server re-attached the handle, found no process table and failed the leg while the job kept writing | the runner's owner watchdog kills its process group within 0.5 s of being re-parented; the leg still fails "process table lost" (ADR-0125 D4), now with nothing left behind | yes |
| `engine 'X' is selected but not installed` (#568) | environment names an engine no installed pack registers | `EngineError` at submit, listing what IS installed | yes — never falls back to builtin |
| Image mode with no image named (#202) | the leg's mode resolves to `image` (the connection's own `mode = image`, or `remote_exec_mode = image` when the connection sets no `mode`) and neither the connection's `image` nor `remote_image` / `CAIRN_TRAIN_IMAGE` names one; there is no built-in image | `EngineError` at submit, where the connection and the environment are both known, before anything reaches the box. The message names the fix for the trigger: for the environment, set `remote_image` / `CAIRN_TRAIN_IMAGE` or use `auto` / `venv`; for a connection's `mode = image`, set the connection's `image` or change its `mode` (the environment does not override it). A connection's `image` or `mode = venv` still works under an environment `remote_exec_mode = image`. `cairn doctor` fails the target's `:mode` row for the same case, with the same precedence (ADR-0150) | yes |
| Remote leg exits 127 (#1075) | `cairn-dataflow-run` not on the box's non-interactive SSH PATH | engine failure with the exit code | yes — `CAIRN_DATAFLOW_REMOTE_CMD` is the fix |
| Trainer on the box finds `CAIRN_CHECKPOINT_BUCKET` unset | the SSH engine forwarded only the spec, the live channel and `MLFLOW_*` | publish to S3 failed and checkpoint mirroring was a silent no-op | no — fixed: the deployment lists the names in `remote_env`; credential-looking names refuse the leg |
| Box FileNotFounds on a pinned dataset version (`file:///tmp/…#sha256=…`) | the ref resolved to the registered control-plane location, ignoring the durable copy; pinned paths were not uploaded | failed on the box after the leg started | yes — a remote engine's refs resolve to the durable copy (ADR-0149); a version with only a control-plane location is refused in `validate` with the remedy |
| Cancel left a job running on the box | cancel and checkpoint pull-back reconnected with `SSH_PRIVATE_KEY` from the control plane's env while submit used the connection's `PRIVATE_KEY`; the failure was swallowed | the run showed cancelled; the job kept running | yes — both use the connection's key and log a warning when the box cannot be reached |
| Box FileNotFounds on a control-plane path (#1180 bug 10) | an upstream leg handed this leg a path that exists only on the control plane | was a hard failure; now local file inputs are uploaded and the config rewritten | yes |
| Budget/`input_artifacts`/`parent_run` silently dropped on the Metaflow path (#1243) | the bridge passed a hand-picked subset of the spec | fields vanished with no error | no — fixed: the FULL contract is serialized |
| Every cairn-namespaced control-plane value reached a remote task (#1344) | `CAIRN_` was a blanket prefix inside the essential-env allowlist | a wildcard hole in an allowlist | yes — names are enumerated now |
| A resumed run silently restarted from stage 0 (#1314) | an engine rewrote the spec at submit, so the runner's hash diverged from the control plane's key | restarts, and says so in stats | no — `checkpoint_key` is assigned control-side |
| Metaflow run invisible to a later process (#1364, and the CWD bug beside it) | metadata written under a per-job temp dir; pathspec not carried on the handle | `MetaflowNotFound` on a run that had just succeeded | yes — the pathspec rides in handle meta |
| Tests uploaded workdirs to a real S3 bucket (#1465) | ambient `CAIRN_ARTIFACT_S3_*` in a developer shell + uniform capture | polluted frozen-stats assertions | n/a — the pack conftest strips that env |

## 7. Testing

Pack tests live in `cairn/operators/builtins/dataflow/tests/` (CLAUDE.md rule
5) — covering the builtin/Metaflow conformance suites, the
runner's checkpoints and descriptors, stage fan-out, budget ceilings, the SSH
localize/fetch seams and the SkyPilot adapter. The runner tests exercise
`cairn.core.dataflow.runner` directly and patch it there (a re-export cannot be
patched). The conformance suite in `cairn.core.dataflow.conformance` is the
executable definition of the engine protocol: an engine IS whatever passes it.

Golden fixtures cover the pure halves — the exact `--with` decospecs the
Metaflow adapter builds, the exact task YAML the SkyPilot adapter would submit,
the exec input the SSH adapter constructs, and the state maps of all three.
Nothing in CI touches a real substrate: SkyPilot is driven entirely through the
`sky=` seam (a live `sky launch` provisions a paid machine), the SSH engine
monkeypatches `cairn-operator-remote`'s exec functions, and Metaflow's remote compute
is asserted as argv rather than run.

From a worktree, with the pack on the path:

```
env -u CAIRN_STORAGE PYTHONPATH=$PWD:$PWD/cairn/operators/builtins/dataflow/src \
  .venv/bin/python -m pytest -q cairn/operators/builtins/dataflow/tests/
```

**What still needs a live proof:** one real `sky launch` against a credentialed
cloud or an `sky ssh up` node pool — the receipt, the teardown, the cost door
and the rsync fetch-back have never met real hardware (ADR-0104 D5 says that
proof is the owner's). Specifically unproven: that `~/.sky/generated/ssh/<cluster>`
is written on the SAME host as the control plane when SkyPilot's API server is
the auto-started local one, and that the fetch reaches the CONTAINER's `$HOME`
(not the VM's) when `image_id: docker:…` is in play.
The Metaflow substrate's Kubernetes/Batch compute is likewise CI-proven only in
its argv/adapter half. Per CLAUDE.md rule 7, the
manifest-sync and drift gates measure nothing until the pack is installed.

## 8. History

| Date | Change | PR |
|---|---|---|
| 2026-07-18 | The boundary and the builtin engine (ADR-0081 D1/D3) | #1048 |
| 2026-07-19 | SSH dispatch for the builtin engine | #1050 |
| 2026-07-20 | First-class pack dataflows; gated tag promotion | #1051, #1052 |
| 2026-07-22 | `ml/run` — the kind-symmetric ML door (ADR-0082) | #1074, #1085 |
| 2026-07-24 | Backend selection through the one config plane | #1093 → #1099 |
| 2026-07-27 | Metaflow adapter — one adapter, both flow kinds (ADR-0083) | #1078 → #1114 |
| 2026-08-05 | Checkpoints, `resume_from`, checkpoint identity (ADR-0088) | #1248, #1314 |
| 2026-08-08 | Metaflow as substrate: secrets, cross-box, gang scheduling (ADR-0089) | #1345, #1346, #1366 |
| 2026-08-12 | Stage fan-out through the bounded pool (ADR-0090) | #1360 |
| 2026-08-22 | Core selects the engine without importing this pack | #1512 → #1548 |
| 2026-09-06 | Live run events over the job channel | #2025 → #2053 |
| 2026-09-07 | SkyPilot engine: launch/status/down, node pool from `compute.ssh`, cost door, receipts (ADR-0104 D5) — and this record | #2073 |
| 2026-09-08 | SkyPilot: `ssh`/`rsync` in the engine image + a door that names them, and results fetched off the cluster before teardown | #2102 |
| 2026-10 | `dataflow/run` implements `dataflow.run` (`cairn-contracts-dataflow`, V1-B); `artifact/promote` implements `artifact.promote` (`cairn-contracts-artifact` 1.0.0, V1-E) — closed empty configuration, no report definition (`ReportSpec.NONE`) | #2242 |
| 2026-10 | `ml/run` implements `ml.run` (`cairn-contracts-dataflow` 1.1.0, #2243 V2) — closed empty configuration, the stage list open like `dataflow.run`'s, no report definition (`ReportSpec.NONE`) | #2243 |
| 2026-10-15 | The engine substrate (runner, stage loader, result readers, fetch contract) moves to `cairn.core.dataflow`; this pack re-exports it for one release (ADR-0129) | #2583 |
| 2026-10-16 | The Prefect engine, its bridge, entry point and `[prefect]` extra removed (ADR-0134 D4) | #2678 |
| 2026-10-16 | The Metaflow engine, its two bridges, entry point and `[metaflow]` extra leave for their own optional wheel, `cairn-operator-dataflow-metaflow` behind `cairndev[metaflow]` (ADR-0134 D2) | #2678 |
| 2026-10-16 | The SkyPilot engine, its entry point and `[skypilot]` extra leave for their own optional wheel, `cairn-operator-dataflow-skypilot` behind `cairndev[skypilot]` (ADR-0134 D3a) | #2678 |
| 2026-10-23 | SSH path for a remote training box (ADR-0149): `remote_env` forwarding, durable dataset copies and refusal of control-plane-only versions, cancel and checkpoint pull-back on the connection's key, owner-only `runner.sh` | #193 |
| 2026-10-24 | The SSH engine has no built-in image: image mode needs the connection's `image` or `remote_image` / `CAIRN_TRAIN_IMAGE`, else the leg is refused at submit; `auto` without an image runs the virtualenv | #202 |
