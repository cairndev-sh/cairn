# Operator and pack contracts

## The two boundaries
- A **pack** is data: `pack.toml` + `pipelines/`, `flows/`, `schemas/`, `policies/`, … and
  a one-line `__init__.py` calling `Pack.from_manifest` (`src/cairn/core/pack/protocol.py:206`;
  ADR-0020). Shipped as its own data wheel `cairn-pack-<name>` whose `pyproject.toml`
  declares its `cairn.packs` entry point (#2420, ADR-0121); discovered through that group
  (`src/cairn/core/pack/loader.py:591-610`). Invisible until installed.
- An **operator pack** is a wheel: `Operation` subclasses registered under `cairn.operators`,
  kernels under `cairn.dataflow_stages` / `cairn.flow_stages`, engines under
  `cairn.dataflow_engines`; `pack.toml` describes each operator (`src/cairn/core/pack/manifest.py:309-340`:
  name, version, input/output schema names, `long_running`, `requires_gpu`, risk, executors…).

## Operator contract (`Operation`, locked surface)
`src/cairn/core/operator/protocol.py:261`: `name`, `version`, `capabilities`, `input_schema` /
`output_schema` (Pydantic, required, `:319`), `run(inp, ctx)`, optional `mock`, `cancel`,
`heartbeat`; class attrs `risk`, `executors` (`in_process` · `mcp_tool` · `remote_job`;
`src/cairn/core/operator/executor.py:113`), `requires_approval`, `long_running`, `supports_*`,
`reports` (ADR-0103: what it reports, declared). Invoked only through `invoke_operator`
(`src/cairn/core/operator/runtime.py:888`), which passes `enforce_gate`
(`src/cairn/core/approval/enforce.py:116`) — invariant 1. An operator starts other governed work
only through `ctx.launch` (ADR-0120 D7, #2455: the dispatch layer's closure over the launch
door; `None` outside a governed run). Side effects happen inside `run`;
`cancel` must be safe on a terminal job; a `remote_job` operator owns submit/poll and the
job channel. `poll` returns a `JobStatus` whose `state` is `running`, `succeeded`, `failed` or
`cancelled`. `failed` may be `retriable` (a recoverable death, #552). `cancelled` (ADR-0151 D9)
means a cancel stopped the job; it is terminal and never retriable, the control loop closes
the record cancelled, and `run_job_lifecycle` ends with `JobCancelled`. The pull lane does not
carry `cancelled`: the `/jobs` heartbeat accepts `queued`, `running`, `succeeded` and `failed`
only, so a pull `Runner` reports a stopped job as `failed`. Failure: raise; the substrate records `Run.error` and `failure_type`.
Idempotency is the operator's promise where it claims it (`supports_*`); nothing in the
substrate retries an operator on its behalf except a leg's declared `retry:`. A pack imports
only `cairn.core.operator` (plus `cairn.core.llm` / `cairn.core.eval` for those operators and
`cairn.core.dataflow` for engine/stage providers); the helpers and record types it needs from
the core are re-exported there (ADR-0120 D7, #2453 — `_CONTRACT_REEXPORTS`), and every other
`cairn.*` edge is inventoried debt that only shrinks.

## Manifest ↔ class agreement
`pack.toml` and the operator class must agree; the drift tests prove it for **installed**
packs only (`tests/core/test_operator_manifest_sync.py`, `test_pack_manifest_drift.py`,
`test_pack_design_docs.py`; `cairn/operators/CLAUDE.md` §B). Install the pack editable
before trusting them. Every operator pack ships `DESIGN.md` (template
`docs/templates/operator-pack-DESIGN.md`), `README`, `CHANGELOG`, `LICENSE`, and its
tests inside the pack.

## Entry-point groups (the discovery contract)
`cairn.packs` · `cairn.operators` · `cairn.dataflow_engines` · `cairn.dataflow_stages` ·
`cairn.flow_stages` · `cairn.dataset_readers` · `cairn.seeds`
(the `cairn demo` data providers, ADR-0120 D4, `src/cairn/core/pack/seeds.py`; the one named
`platform-demo` also answers `POST /demo/seed?showcase=1` with `showcase=True`) ·
`cairn.model_catalogs` (the licence door's catalogue providers, ADR-0120 D4,
`src/cairn/core/pack/model_catalogs.py`) · `cairn.run_hooks` (the run/leg event listeners the
dispatch layer fires, plus the two query-shaped events whose provider may refuse with
`AdmissionRefused`: `run.admitting` (a template run, before its record; a provider failure
is logged and ignored) and `pipeline.launching` (a pipeline launch, fired by the launch door
before the pipeline record, with the launch's ref, inputs, workspace, account and trigger
as read-only copies; a refusal is the door's `AdmissionRefusedError`, and a provider failure
refuses the launch too); and `workspace.created`, fired by the admin router, #2421; ADR-0120
D4, `src/cairn/core/pack/run_hooks.py`) · `cairn.routers`
(HTTP surfaces beyond the public API, ADR-0120 D4, `src/cairn/core/triggers/router_providers.py`) ·
`cairn.cli` (commands beyond the public CLI — a provider returns a `typer.Typer` or a list of
`(name, typer.Typer)`, mounted after the public commands, which win every name; #2421,
`src/cairn/cli/providers.py`)
(`ENTRY_POINT_GROUP` constants under `src/cairn/core` and `src/cairn/cli`; `tests/core/test_contract_docs.py`
checks this list against the code). Adding a group is an architecture change (root
`CLAUDE.md` §5).

### Public Python names for a `cairn.routers` provider (#297)
A router provider is a surface: it gets a `RouterDeps` (the app, runner, dispatcher, pipeline
bridge, scheduler and review store the server was built with,
`src/cairn/core/triggers/router_providers.py`) and may import these names. They follow
`docs/compatibility.md` like any public name; each earlier private spelling stays as an alias
for the same object.

| Name | What it is |
|---|---|
| `cairn.core.triggers.auth.oauth2_scheme` | the bearer-token dependency: `Depends(oauth2_scheme)` yields the raw token (or `None`) for `caller_identity`, `require_auth` and `require_admin` |
| `cairn.core.triggers.auth.env_admin_username()` | the deployment admin's username (`CAIRN_API_USER`, default `admin`), or `None` when no admin password is configured; never the hash |
| `cairn.core.triggers.server.langfuse_trace_url(trace_id)` | a run's Langfuse deep link, or `None` when tracing is not configured |
| `cairn.core.triggers.server.run_failure_types(ledger, run)` | a run's failure types: the eval ledger's failure labels, unioned with `Run.failure_type` |
| `cairn.core.triggers.router.user_trigger_overlay_dir()` | where user trigger files live (`CAIRN_TRIGGERS_DIR`, default `~/.cairn/triggers`) |
| `cairn.core.engine.triggers_presets.TRIGGER_PRESETS`, `SIGNATURE_SCHEMES` | the webhook-receiver preset catalogue and the signature schemes it verifies |

## Dependency rules (root `CLAUDE.md` §3, test-enforced)
- Packs depend on core; **core never imports `cairn.packs.*`** nor an operator pack
  (`tests/core/test_architecture_boundaries.py`, `test_engine_registry.py`).
- Operator packs import `cairn.core.operator` (+ `cairn.core.llm` / `cairn.core.eval`
  for LLM/eval operators). **Never another operator pack**; never `cairn.core.dispatch`,
  `engine` or `triggers` internals (allowlisted debt only shrinks).
- Heavy libraries are optional extras, imported lazily in stage/operator bodies with a
  reason and a deterministic fallback (`cairn/operators/CLAUDE.md`).

## Installability and versioning
Each pack is a workspace member with its own `pyproject.toml`; `cairn.lock` pins the shipped
closure by content hash (`uv run cairn lock --check` in CI). A pack must install and pass
its own tests against the public core surface alone. Versioning and deprecation follow
`docs/compatibility.md`: operator input/output schemas are public; adding an optional
field is additive; removing or retyping one is breaking and needs a manifest version bump
and a CHANGELOG entry.

## Input / output responsibility
Operators validate their own inputs (Pydantic) and return typed outputs; the substrate
records `parameters` and results (`src/cairn/core/operator/effective.py`; ADR-0103 D3).
Kernels (stages) take `(workdir, config, inputs)` and return flat scalar stats; the runner
prefixes them `<stage>.<key>` (`src/cairn/core/dataflow/runner.py`, `run_dataflow`) — only
scalars cross the engine boundary (ADR-0081 D3).


## Shared contract descriptor inspection — CURRENT, internal slice (#2240 A)

`src/cairn/core/pack/contracts.py:discover_contracts` inspects installed distribution metadata
without loading entry points, installing packages, or reading definition bodies. It is an
internal pack-owner function; runtime consumers are not wired to it yet.

A *contract package* is one descriptor directory `cairn_contracts/<package>/contracts.toml`
and the files beneath it. Its closed TOML shape is
`format = "cairn.contracts/1"`, a `distribution` table (`name`, `version`), nonempty
`definitions` tables (`id`, `revision`, `kind`, `path`), and optional `dependencies` tables
(`distribution`, `version`). Kinds are `capability`, `schema`, or `report`; contract and
dependency revisions use exact numeric `major.minor.patch`. The package's identity is that
`distribution` table, whichever distribution installs it. Namespaced definition IDs have one
package owner. A descriptor at any other depth, or a file under `cairn_contracts/` in no
package directory, refuses (`_descriptors`).

<a id="contract-host-modes"></a>A distribution hosts packages in one of two modes
(ADR-0111 P1 as amended by ADR-0137 D3; `_discover`, `_inspect`):

- **Standalone** — every contract distribution except the engine's. Its inventory lists
  exactly one descriptor, at `cairn_contracts/<normalized-distribution-name>/contracts.toml`,
  and the descriptor's name and version must match the installed metadata. A second
  descriptor refuses.
- **Engine-hosted** — only the distribution named `ENGINE_DISTRIBUTION`
  (`src/cairn/core/pack/loader.py`, `cairndev`). It may host any number of packages. Each
  package's directory name must equal its descriptor's `name` (canonical form), and its
  version is the descriptor's own, never compared with the host's version.

A package name installed by two distributions (a standalone `cairn-contracts-audit` beside
the copy inside `cairndev`, say) refuses, naming both and the remedy: uninstall the
standalone one. Records are one per package (`ContractDistribution.name` = package,
`version` = descriptor version) in both modes, so a package's recorded identity is the same
standalone or hosted.

Definition paths are relative to that package's contract directory, must be listed
in the inventory of the distribution that installs it, and must resolve to files inside
that directory. Malformed descriptors,
duplicate identities/owners, missing assets, traversal and escaping symlinks raise
`ContractDiscoveryError`. Results are sorted frozen records containing tuples.

Distributions without inventoried contract assets are ignored, including those without
file inventory. This does not certify that a wheel is data-only or complete. Dependency
presence, definition contents, reference closure, schema validity, content digests and
runtime conformance remain TARGET work in later slices. Tests use temporary installed-layout
metadata fixtures; actual wheel build/install qualification belongs to slice B.

### Definition bodies — CURRENT internal B1 readers

`contracts.py:_read_definition` validates one capability/report/schema body and returns
its original bytes with parsed local data. JSON/YAML duplicate keys, non-data tags,
non-JSON values and cyclic data reject. The legacy YAML loader is unchanged.
Capability/report fields are closed and their IDs/revisions must match the descriptor.
Report specs use the existing stages/series/scalars serialization after strict checks;
semantic annotations may name only declared series/scalars. This promises no metric delivery.
The reader is internal: inventory validation remains the caller's responsibility.
Descriptor-only `discover_contracts` behavior is unchanged.

### Opted-in operators and conformance — CURRENT (#2241 B, C)

An operator opts in with `implements = ["contract://<capability-id>/<rev>", …]` in its
`pack.toml` entry and on the class (`register_operator(implements=…)`);
`pack/contracts.py:validate_implements` is the one grammar (nonempty, exact revisions, one
capability ID — ADR-0111 D3) and the manifest↔class drift gate covers the field. Legacy
`capabilities.provides` labels are unchanged and do not satisfy semantic validation; an
operator without `implements` takes the byte-identical path it always did.

At the launch door every opted-in operator a plan reaches — a `task:` leg, and since slice C
every operation node a template leg binds (keyed `<leg>/<node path>`, subgraph children under
their subgraph node) — gets an `InvocationEntry` on
`PipelinePlan.contract_resolution` (`pipelines-and-runs.md`): the newest implemented revision
in the installed closure, its definitions with digests, operator ref and version. A revision
absent from the closure, or `reports` differing from the capability's report contract, refuses
as `PipelinePlanError` before any record exists. The leg reads its entry from the record —
never re-resolves — a task leg directly, a template leg through graph state the dispatcher
seeds from the record and stamps with the leg id (`contract_invocations`; the stamp propagates
into subgraph seeds, so an opted-in node inside an admitted leg that finds no entry refuses as
lost provenance, while the dev loop, which has no record, stays legacy). Dispatch
(`registry.invocation_contract`) re-reads the bodies it needs,
each digest-checked against the record's closure, into a `ContractEnvelope`
(`operator/effective.py`). The seam (`invoke_operator_core`) checks identity (the running class
still implements the recorded revision at the recorded version), the shared input and
configuration schemas before the call — a refusal names the contract and no provider runs —
and the shared output schema (JSON projection of a model output; a dict is judged as it is) after
it — non-UTF-8 `bytes` project as base64 text, since JSON has no bytes type (#2564; a
`contentEncoding: base64` marker in the schemas belongs to the #2243 contracts track): a
non-conforming result fails the leg
even though the effect happened — on a template node too, where ADR-0117 exempts
`ContractConformanceError` from the compiler's degrade-to-finding policy (#568). `$ref`
resolves within the envelope; nothing is fetched.

v1 operation files and inline nodes may name a `capability: contract://<id>/<rev>` (plus an
optional `configuration:` mapping) instead of an `operator:`; `version`, `resources`,
`secrets`, `artifacts` and `execution` are then forbidden (ADR-0111 P8). The engine resolves
the capability through the active profile's `[profile.bindings]` entry for the capability ID;
the bound operator must `implements` that exact revision, else the template refuses to compile
— no fallback provider. The resolved binding is the legacy `operator` form, so elaboration,
validation and gates are untouched. The launch preview resolves the envelope the door would,
so it refuses what the run refuses.

The configuration the seam judges is the one the operator sees: its `config_schema`
defaults, the layered config, and any config field the (layered, stripped) input supplies —
which is where a v1 `configuration:` mapping lands. An operator that declares no
`config_schema` field for a shared configuration option is therefore judged only by the shared
input schema (an operator-conformance drift nothing proves yet).

Not covered yet: shipped kernels beyond the nine readiness stages opting in at the stage
boundary (#2283's family; the readiness stages are below, #2242 V1-C); the `mcp_tool`/`remote_job`/
`docker` arms (#2182); `profile_digest` (#2248); a checkpointed (gated) graph persists the
envelope with the rest of its state; persisted template-run output carries the
`contract_invocations` channel (as `operator_config`/`workflow_config` already do).

### Stage contracts — CURRENT (ADR-0118, #2242 V1-A)

A contract distribution may ship a `stage` definition (`format: cairn.stage/1`, an `id` and
`revision`, `configuration` and `output` schema references, and no `input`: rows are not
contracted). A core without the kind refuses the whole distribution, so stage contracts ship
in their own distribution. A kernel opts in with an `implements` attribute on the kernel
function, beside `risk_class` (M3a: no `pack.toml` change). An operation implements only a
capability and a kernel only a stage contract; the launch door refuses either the other way
round. A stage's output schema must stay open, at its root and on every subschema applied in
place: no `additionalProperties` other than `true`, and no `maxProperties`,
`unevaluatedProperties` or `propertyNames`. It may not declare the runner's reserved stat
keys (`pack/contracts.py:RESERVED_STAGE_STATS`: `kernel_version`, `items_total`,
`items_done`, `sysmetrics_samples`, the `artifacts_*` keys and the `cpu_pct`/`rss_mb`/
`gpu_pct`/`vram_mb` `_mean`/`_max` samples). The door records a `stage` `InvocationEntry`
per opted-in stage (`pipelines-and-runs.md`); `execution.md` covers the spec stamp and checks.

### The first shared contract distribution — CURRENT (#2241 D)

`contracts/eval` → `cairn-contracts-eval` (1.0.0 in slice D; 1.1.0 since #2242 V1-B added
`eval.judge`) is a data-only wheel (hatchling
`bypass-selection` + `force-include` of `definitions/` into
`cairn_contracts/cairn-contracts-eval/`). It defines `contract://eval.delta/1.0.0` and its
input, configuration (closed, empty — the regression budget rides the input) and output
schemas; the output pins `verdict` and per-metric `status` to the literals the operator
emits. The root depends on it and installs it **non-editable**
(`{ workspace = true, editable = false }`) because an editable install is refused as not
data-only. `eval/delta` opts in with `implements = ["contract://eval.delta/1.0.0"]`.
Consequence: an environment without the distribution installed refuses to plan a pipeline
that reaches `eval/delta`. Since #2242 V1-B the same holds for the eight readiness-closure
operators below, including the widely reached `audit/store`, `mcp/call` and `dataflow/run`:
an installation without all six contract distributions (five capability distributions and, since
V1-C, `cairn-contracts-dataset-stages`) refuses, at the launch door and before any record, every
shipped pipeline that reaches one of them or one of the nine opted-in stage kernels —
`dataset_readiness/{readiness,check,clean,decontaminate,prep}`,
`training/{finetune,finetune-remote,train}`, `compress/quantize` and
`incident/{rca,post-incident-postmortem}` (plus the `eval/delta` pipelines:
`compress/distill-compare`, `eval/score-a-model`, `training/{closedbook-compare,retrain-cycle}`; and, through the stage kernels, the training,
serving and compress flows that reuse them, such as `training/readiness` and `closedbook-eval`).
Every `uv sync` of the workspace carries it, and the engine and dataflow-task
images copy `contracts/` for the same reason; an install path that omits it refuses such pipelines. Revisions are immutable: a
change is a new revision file and a new `[[definitions]]` row, never an edit — its digest is
recorded in every admitted pipeline. The per-operator recipe the tier batches repeat is
`docs/architecture-fix/operator-migration-recipe.md`.

### The readiness closure's distributions — CURRENT (#2242 V1-B)

One distribution per capability domain (decision B1a), all built and installed like
`cairn-contracts-eval`. The eight operators `dataset-readiness/readiness` reaches implement
revision `1.0.0` of their capability on the class and in `pack.toml`:

| Distribution | Capabilities (operator) |
|---|---|
| `cairn-contracts-eval` 1.1.0 | `eval.delta` (`eval/delta`), `eval.judge` (`eval/judge`; with the report definition `eval.judge.report`, equal to its `reports`) |
| `cairn-contracts-dataset` 1.1.0 | `dataset.decontam`, `dataset.provenance`, `dataset.sample`, `dataset.gate`; since 1.1.0 (V1-D) `dataset.publish_fingerprints`, `dataset.anonymize`, `dataset.split`, `dataset.pii_scan`, `dataset.prep_gate`, `dataset.prep_review` (`dataset/*`) |
| `cairn-contracts-dataset-stages` 1.0.0 | the nine readiness stage contracts below (#2242 V1-C) |
| `cairn-contracts-dataflow` 1.1.0 | `dataflow.run` (`dataflow/run`); since 1.1.0 `ml.run` (`ml/run`) (#2243 V2) |
| `cairn-contracts-audit` 1.0.0 | `audit.sink` (`audit/store`) |
| `cairn-contracts-mcp` 1.0.0 | `mcp.call` (`mcp/call`) |
| `cairn-contracts-ingest` 1.0.0 | `ingest.api`, `ingest.pull`, `ingest.watch` (`ingest/*`; `api` and `pull` with the report definitions `ingest.api.report` and `ingest.pull.report`, equal to their `reports`) (#2242 V1-E) |
| `cairn-contracts-monitor` 1.0.0 | `monitor.threshold`, `monitor.propose`, `monitor.dispatch` (`monitor/*`) (#2242 V1-E) |
| `cairn-contracts-artifact` 1.0.0 | `artifact.promote` (`artifact/promote`) (#2242 V1-E) |
| `cairn-contracts-storage` 1.0.0 | `storage.put`, `storage.get` (`storage/*`) (#2243 V2) |
| `cairn-contracts-memory` 1.0.0 | `memory.search`, `memory.write` (`memory/*`) (#2243 V2) |
| `cairn-contracts-redact` 1.0.0 | `redact.text` (`redact/text`) (#2243 V2) |
| `cairn-contracts-secrets` 1.0.0 | `secrets.store` (`secrets/env`) (#2243 V2); `secrets/infisical` (ADR-0127 D3) |
| `cairn-contracts-rag` 1.0.0 | `rag.search`, `rag.fetch` (`rag/search`, `rag/fetch`), `rag.rerank` (`rag/rerank`) (#2243 V2 PR B) |
| `cairn-contracts-remote` 1.0.0 | `remote.exec` (`remote/exec`) (#2243 V2 PR B) |
| `cairn-contracts-data` 1.0.0 | `data.profile` (`data/profile`) (#2243 V2 PR B) |
| `cairn-contracts-train` 1.0.0 | `train.finetune` (`train/lora`), `train.eval_loss` (`train/eval-loss`); with the report definitions `train.finetune.report` and `train.eval_loss.report`, equal to their `reports` (ADR-0127 D4, #2572 WP3) |

The schemas are written from the operators' Pydantic models. Configurations are closed (empty
for `dataflow/run`, `audit/store` and `mcp/call`); inputs and outputs stay open like their
models. Two outputs are tightened to the literals the code emits: `dataset.gate`'s
`verdict` (`pass | warn | fail`) and `dataset.provenance`'s `metadata_sources` items
(`dataset_info | sidecar`). Pydantic's `scope: governance` and `recommended` annotations are
outside the bounded schema vocabulary and are kept as `$comment`. `dataflow.run`'s input pins
the envelope; the stage list stays open (stage contracts are ADR-0118's). Domain flows keep
`operator:` nodes (decision B2a), so they run under the contracts with no profile binding.

**#2242 V1-D.** The six further `dataset_checks` operators that `dataset_readiness` `clean` and
`prep` reach implement revision `1.0.0` of their capability, added to `cairn-contracts-dataset`
as 1.1.0 (capabilities only, no `stage` kind, so the mixed-version note below still holds). Same
rules: closed configurations (empty for `publish_fingerprints` and `prep_review`; `anonymize`'s
and `prep_gate`'s are governance-scoped, kept as `$comment`), open inputs and outputs, and enums
only for literals the code emits (`prep_gate`'s `action` and `verdict`, `split`'s
`backend_used`, `publish_fingerprints`' `skipped` reasons). None declares `reports`.
`prep_review` is `requires_approval`: the gate fires before its body and before any contract
check, and a released step runs under the contract. With them, `clean`, `prep` and
`decontaminate` reach no uncontracted operator or stage
(`tests/core/test_pack_dataset_operator_contracts.py`).

**#2242 V1-E — the pack's edges.** The seven operators that `dataset_readiness`'s
`api-and-admit`, `pull-and-admit`, `watch-source`, `watch` and `promote` pipelines reach implement
the three distributions above, one per capability domain (decision D2a). Configurations are closed
(`ApiPullConfig`, `SqlPullConfig`, `WatchCheckConfig`; empty for the monitor operators and
`artifact/promote`); inputs and outputs stay open. One output is tightened to the literals the code
emits: `monitor.threshold`'s `source` (`inline | file | http | mock`). An installation without
these three distributions refuses, at the launch door, every pipeline reaching one of the seven.

**#2243 V2 — the builtin tier (PR A).** `ml/run`, `storage/put`, `storage/get`, `memory/search`,
`memory/write`, `redact/text` and `secrets/env` (capability `secrets.store`, its `provides` name)
implement revision `1.0.0` of their capability on the class and in `pack.toml`, from
`cairn-contracts-dataflow` 1.1.0 and four new distributions, one per capability domain (decision
D1a). None declares `config_schema` or `reports`: every configuration is the empty closed object
and no report definition exists. Inputs and outputs stay open like their models; nothing is
tightened (`ml.run`'s stage list stays open like `dataflow.run`'s; `redact.text`'s `kinds` stay
open because the detector vocabulary is the provider's). `storage.put`'s `content` is `str | bytes`:
the shared input admits a string or a value that is no JSON type, so it never refuses the bytes the
model admits. Core services that call `redact/text`, `secrets/env` and the memory operators on the
class directly (the redaction chokepoint, scoped secrets, the composition layer) are not door
paths and run no envelope check — as for `audit.sink` (#2247 follow-up). An installation without
these distributions refuses, at the launch door, every pipeline reaching one of the seven
(`tests/core/test_builtin_operator_contracts_a.py`).

**ADR-0127 D4 — `train.finetune` and `train.eval_loss`.** `train/lora` (`cairn-operator-train-local`,
a builtin) implements revision `1.0.0` of `train.finetune` from `cairn-contracts-train`. It is the one
builtin capability with a non-empty configuration: `base_model`, closed, the `config_schema`
subset of the input a profile pins under `[profile.config."train/lora"]` (a run's bound value
still wins). Input and output stay open like the models. The same fine-tune ships as the ml stage
`train.lora`, which declares no `implements`: a stage contract is the input-less `cairn.stage/1`
kind, and the stage validates its config and stats with the operation's models instead.
`train/eval-loss` in the same pack implements `train.eval_loss` `1.0.0` from the same
distribution (configuration: the empty closed object), so every builtin operator implements a
contract (`tests/core/test_builtins_complete.py`; maintainer decision 2026-10-15). Proof: `tests/core/test_builtin_operator_contracts_train.py`
(under mock mode; the real fine-tune needs the `[train]` extra and is proven by the pack's
`-m train` tests).

### The readiness stages' contracts — CURRENT (#2242 V1-C)

The nine stages `dataset-readiness/readiness` runs implement `contract://dataset.stage.<kernel>/1.0.0`
(`cairn.stage/1`, in their own distribution `cairn-contracts-dataset-stages` 1.0.0) with an
`implements` attribute on the kernel function. These are the `intake` flow's `ingest.normalize`, `dedup.exact`, `dedup.near`,
`decontam.apply`, `pii.redact`, `expectations.check`, `profile.tokens` and `export.jsonl`, and
`check`'s `scan.all`.
- **Configuration.** Closed: the keys the kernel reads, with their types and kernel defaults.
- **Output.** Open, with no reserved runner key. Every key the kernel emits is declared, except
  the dynamic `bucket_*` and `hits_*` families. Keys it always emits are `required`, including
  every key the control plane reads that a kernel emits (`src/cairn/core/intake.py`, the
  check-evidence map, the `scan.all` hoist).
- **The intake verdict's inputs (#2396, maintainer 1a: ADR-0102 D1 as written).**
  `ingest.normalize` emits `quarantined_rows` (malformed source lines routed to quarantine) and
  `export.jsonl` emits `rows` (counted in the hash pass). They are `required` in revision 1.1.0
  of those two stage contracts, which the kernels implement (`cairn-contracts-dataset-stages`
  1.1.0; 1.0.0 stays published). `intake.py` turns any malformed line, or an export that ends
  with 0 rows, into `invalid_format`, which the launch door refuses — for every flow that
  registers an export, not only readiness.
- **Mixed versions.** The stage definitions ship in their own distribution, as ADR-0118
  requires. A core older than the `stage` kind refuses any installation that includes
  `cairn-contracts-dataset-stages` (one invalid distribution refuses the whole resolution); what
  the separate wheel buys is that an older core's installation WITHOUT it still resolves
  `cairn-contracts-dataset` (1.0.0, unchanged) and the other capability contracts.
- **Engines.** Every flow that runs one of these kernels (readiness's `intake` and `check`, and
  training, serving and compress flows that reuse them) now needs an engine that declares
  `carries_stage_contracts`; the in-tree builtin, ssh and metaflow engines do.
- **Engines whose job runs elsewhere** (ADR-0149 D3) also declare `needs_durable_inputs = True`
  (the builtin SSH engine does). Like `carries_stage_contracts`, it is optional, not a protocol
  member, and read through a helper that defaults to `False`
  (`cairn.core.dataflow.engine.needs_durable_inputs`); the leg executor then resolves the leg's
  `dataset:` refs to their durable copies.

`tests/core/test_readiness_stage_contracts.py` runs each kernel on real data against its
schemas. Through the builtin engine subprocess, it also proves that each stage refuses a
non-conforming configuration before the kernel and a non-conforming raw output before its done
marker.

### Installed closure — CURRENT internal (#2240 B2)

`contracts.py:resolve_contracts` closes every installed contract distribution offline and
`registry.py:all_contract_definitions` and `registry.py:contract_resolution_snapshot` (#2240 C:
the same closure as `cairn.resolution/1` data — names, versions, digests, never paths — for
the launch door) are its only consumers, both stateless facades; an empty installation is
`()` / an empty snapshot. One invalid distribution refuses the whole result. Data-only is
judged per [host mode](#contract-host-modes) (`_check_dependencies`):

- **Standalone:** the distribution is accepted only when it is data-only — every inventoried
  path under `cairn_contracts/<name>/` or `<dist>.dist-info/`, no executable or `.pth`
  payload, no entry points, not an editable install (`_data_only`) — and its installed
  `Requires-Dist` equals the descriptor dependencies as exact `name==version` pins (no
  extras, markers or URLs).
- **Engine-hosted:** each package is data-only — no inventoried path under
  `cairn_contracts/<package>/` has an executable or `.pth` suffix (`_hosted_data_only`). The
  host's own code, entry points, editability and `Requires-Dist` are the engine's and are not
  judged. An editable `cairndev` is accepted: every descriptor and definition must still be
  inventoried in its `RECORD` and resolve inside its install root, which holds for an
  installed copy and never for a file on a live source path.

In both modes, declared dependencies must be installed as packages at that exact version;
dependency cycles refuse; references cross only into the package and its declared
dependencies. Obligation evidence must be an inventoried asset.
Results are frozen, sorted records carrying SHA-256 digests of the descriptor, each
definition and each evidence file; paths are local lookup details, not identity. The proof
installs test-built wheels with `uv pip install` into an isolated target and inspects them in
a subprocess with provider-import and network traps. Nothing is executed or fetched;
runtime conformance and the PipelinePlan carrier remain slice C.

**Kept per process (maintainer 2026-10-11, "1a").** A full resolution lists every file of
every installed distribution and validates every body (about 2.2 s over 290 installed
distributions), and the launch door, every leg and every opted-in stage asked for it.
`contracts.py` therefore keeps the installed set's last successful resolution together with
the bodies parsed from the same bytes it digested (`installed_contract_bodies`, which
`registry._closure_bodies` returns). A hit costs about 16 ms. The kept result is reused only
while every installed distribution's metadata directory and `RECORD` stamp and every contract
file's size and mtime are unchanged, so an install, upgrade, removal, reinstall or in-place
edit resolves afresh. A refusal is never kept, nor a result whose files changed while it was
being read (its digests are re-checked against the files before keeping); an explicit
`distributions=` always resolves afresh; callers receive copies of the bodies they hand on.
`tests/core/test_contract_resolution_cache.py` proves each of these.
