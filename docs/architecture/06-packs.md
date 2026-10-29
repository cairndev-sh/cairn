# Packs — `src/cairn/core/pack/` (contract) and `cairn/packs/` (domain packs)

> **DOCUMENTED design update, 2026-10-05:** [ADR-0111 production design proposal](../decisions/ADR-0111-contracts-capabilities-and-pack-separation.md#production-design-proposal--2026-10-05-2230) specifies candidate discovery, exact revisions, offline references, authoring and provenance changes. Design preparation is authorized; the schema is still PROPOSED. #2230 acceptance and the explicit P7 review conditions precede production implementation. Gold fixture grammar is not the production format.

**DOCUMENTED review requirement / PROPOSED production restriction:** profile changes
must be reviewed against [ADR-0112](../decisions/ADR-0112-profile-scope-and-enforcement.md).
Profiles own typed deployment choices, bindings and environment-owned configuration;
workflow logic, per-run inputs, raw secrets and schedules belong elsewhere. Record the
setting owner, consumer, scope, precedence, inheritance and rejecting tests. Preserve
legacy behavior until the exact schema migration is approved. Technical enforcement is
pending implementation; current outer-key validation does not prove nested scope safety.


**DOCUMENTED maintainer scope / PROPOSED destination:** [ADR-0111 separation](19-contracts-and-operator-migration.md)
makes shared contracts independently discoverable by domains and operators. Selected
domain packs use capability-only executable references and simple operation mappings;
local schemas exist for composition-specific interfaces. Profiles select implementations.
All operators migrate, but **not all domain packs**: untouched packs retain tested legacy
compatibility. See the [migration plan](../architecture-fix/operator-contract-migration-plan.md).
The observed pack kinds below do not yet implement a new contract distribution grammar.

## Purpose
A pack is the distributable unit of *domain intent*. The engine is domain-neutral; it
learns about training, incidents or security triage only by loading packs at startup and
reading their records through a registry (DOCUMENTED: `src/cairn/core/pack/protocol.py`
docstring; `README.md` "Architecture — engine + packs"; ADR-0020, ADR-0026, ADR-0084).

## Three pack kinds (OBSERVED in `pack.toml` `[pack].kind`)
| Kind | Contains | Runnable? | Examples |
|---|---|---|---|
| `domain` | `pack.toml`, `pipelines/*.yaml` (mandatory per ADR-0084 D4), `flows/*.{yaml,md}`, `operations/*.yaml`, `schemas/*.schema.yaml`, `policies/`, `evals/`, `examples/`, `skills/`, `rag/`, `models/`, `presets/`, `recommender/` + a one-line `__init__.py` | yes | `training`, `dataset_readiness`, `serving`, `compress`, `compute`, `eval`, `incident`, `security_triage`, `redteam` |
| knowledge | `pack.toml` + `rag/` corpora, attached to environments as roles | no | `security_kb`, `redteam_kb` |
| `operator` | code wheel — see [05-operators.md](05-operators.md) | no (provides capabilities) | 37 under `cairn/operators/` |

## Owns (`src/cairn/core/pack/`)
| Module | Responsibility |
|---|---|
| `protocol.py` | `Pack` dataclass (`:124`): identity, asset dirs (`pipelines_dir`, `flows_dir`, `templates_dir`, `skills_dir`, `policies_dir`, `schemas_dir`, `rag_dir`, …), descriptors (`tools`, `executors`, `triggers`, `backends`, `capability_bindings`), `settings_inventory`, `doctor_checks`, `manifest`; `Pack.from_manifest(__file__)` (`:206`) |
| `manifest.py` (1584) | `PackManifest` + `load_manifest` — the `pack.toml` grammar: `[pack]`, `[compat]`, `[assets]`, `[requires.{capabilities,mcp,memory,artifacts,rag,services}]`, `[provides]`, `[defaults]`, `[operator."x/y"]`, `[[demo.<flow>]]`, `[eval.<flow>]`, `[knowledge]` |
| `loader.py` | `load_all_packs`/`ensure_loaded`: entry-point group `cairn.packs` + dev dirs from `CAIRN_PACK_PATH` (installed wins collisions), engine-compat check (`[compat].cairn_engine`), legacy-layout warnings, load-failure list |
| `registry.py` | process-global registry + aggregators `all_tools()`, `all_capability_bindings()`, `all_backends()`, `all_flow_dirs()`, `all_pipeline_dirs()`, `all_policy_dirs()`, … — the **only** way core reads pack content |
| `composition.py` | load-time validation: who provides which capability, required capabilities satisfied, unique providers unless a profile overrides, trigger providers, profile bindings |
| `profile.py` | `Profile` TOML loader (`extends`, `[profile.bindings]`, `[profile.backends]`, `[profile.config]`, `[profile.mcp_servers]`, `[profile.approval]`, notify, `[profile.budget]`: the deployment's budget default, ceiling and local rate, ADR-0155) |
| `discovery.py`, `curation.py`, `visibility.py`, `drift.py`, `provisioning.py` | template listing for `GET /packs`; curation tiers; `[provides]` encapsulation; manifest drift; required-services provisioning check |

## Does not own
Executing anything. Operator code. Persisting runs. The console's notion of "installed"
(that is `workflow_registry.py` / `registry_install.py`, see [09](09-storage-and-provenance.md)).

## Public interface
- Authoring: `pack.toml` (schema = `manifest.py`), the asset directory conventions, `Pack.from_manifest`.
- Engine-side: `ensure_loaded()`, `get_packs()`, `get_pack(name)`, the `all_*()` aggregators, `load_profile_from_env()`.
- CLI: `cairn init pack`, `cairn pack add [--dev]`, `cairn pack check`, `cairn ls`, `cairn doctor`, `cairn lock`.

## Dependencies
`core/pack` → `core/<top>` (4), `core/triggers` (3 lazy: trigger specs in composition/registry), `core/operator` (3), `core/schema` (1). Domain packs → `cairn.core.pack.Pack` only (11 edges) — except `packs/incident/__init__.py:16` → `cairn_platform.integrations.observability.mock.backend` (lazy).

## Dependents
Everything that resolves records: `engine/compiler.py` (template dirs, schemas), `engine/pipeline_resolve.py`, `dataflow/defs.py` (flow dirs), `policy/engine.py` (policy dirs), `cli`, `triggers`, `obs/redact.py` and `obs/audit.py` (profile bindings).

## Side effects
Importing a pack module (entry point) at load; reading TOML/YAML/MD from disk; none at run time.

## State
The process-global registry (`registry.py`; `clear()` for tests), the composition report,
`pack_load_failures()`.

## Entry points
`load_all_packs(strict=False)` is called by `cairn-server` boot (`triggers/main.py:build_default_runner`), the CLI, the Temporal worker (`runtime/temporal/worker.py:build_registry`), and lazily by every consumption point via `ensure_loaded()`.

## Domain-pack inventory (OBSERVED, HEAD)
| Pack (`cairn/packs/`) | Own `cairn-pack-*` wheel with its `cairn.packs` entry point (#2420)? | `pipelines/` | `flows/` |
|---|---|---|---|
| `incident` | yes | yes | yes (+ `templates/`, `skills/`, `policies/`, `tests/`) |
| `dataset_readiness` | yes (`dataset-readiness`) | 5: import, readiness, pull-and-admit, api-and-admit, watch-source | 10 (`clean.yaml`, `check.md`, `prep.md`, `fix*.md`, `promote.md`, `watch.md`, …) |
| `training` | yes | 13 | 17 (`finetune.yaml` kind ml, `finetune.md` agentic, `readiness.yaml`, dpo, facts, e2e) + `operations/` (31 bindings) + `models/`, `presets/`, `recommender/` + `DESIGN.md` |
| `serving` | yes | 3: serve, batch-inference, export-model | `serve.md`, `batch-infer.yaml` |
| `compress`, `compute`, `eval` | yes | yes | yes |
| `security_kb` | yes | — (knowledge) | `rag/` |
| `security_triage` | yes | yes | yes |
| `redteam` | **no** | yes | — |
| `redteam_kb` | **no** | — | `rag/` |

**Finding (OBSERVED):** `redteam` and `redteam_kb` are on disk but not in the `cairn.packs`
entry-point group, so a normal install does not load them (they would load only via
`CAIRN_PACK_PATH`). README says "Eight packs ship"; nine are entry-pointed. Verify intent.

## Invariants
| Invariant | Status | Evidence |
|---|---|---|
| Packs are discovered via entry points; a pack must be installed | CONFIRMED (+ dev-dir mode) | `loader.py:371`, CONTRIBUTING "Working on a pack" |
| Core never imports `cairn.packs.*` | CONFIRMED (#2414): the training pack's data dirs are found through its `cairn.packs` entry point; the licence door reads catalogues through `cairn.model_catalogs` | `models_catalog.py`, `recommender.py`, `pack/model_catalogs.py`, `test_architecture_boundaries.py` |
| Domain packs are data: zero `.py` | **CONFLICT / PARTIALLY ENFORCED** — ADR-0020 says zero `.py` and "doctor-enforced"; observed: every domain pack has exactly one `__init__.py` (`Pack.from_manifest`), `incident/__init__.py` imports a backend; the only test (`tests/core/test_init_pack_is_data_only.py:44`) allows `__init__.py` and checks the *scaffolded* pack, not shipped packs; no doctor check found | |
| Every domain pack ships ≥1 pipeline (ADR-0084 D4) | OBSERVED TRUE for the 9 runnable packs; enforcement UNCLEAR (no test found that walks `cairn/packs` for `pipelines/`) | |
| Packs are self-sufficient; no cross-pack executable refs (ADR-0084 D5) | PARTIALLY: `packs/training/flows/finetune.md` still composes `dataset-readiness/check` as a `subgraph_node` (the ADR said this one ref would be internalised); `compiler.py:1115 _warn_cross_pack` warns | |
| `pack.toml` uses `kind` (ADR-0083 D4) | OBSERVED: manifests carry `kind = "domain"`; ADR-0020 (locked) still specifies `type = "domain"` → **CONFLICT between a locked ADR and shipped manifests** (operator packs carry both `type` and `kind`) | |
| `[provides]` encapsulation | permissive default (no `[provides]` → everything visible) — DOCUMENTED `docs/architecture.md` §Pack composition; enforced in `pack/visibility.py` | |
| Shipped pack closure is hash-pinned | CONFIRMED: `cairn.lock` + CI `cairn lock --check` | |
