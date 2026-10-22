# Storage and provenance

## Purpose
Persist what makes a run *governed, identified, composable*: the run and its events, the
pipeline record, content-addressed artifacts and lineage, judges/reviews/eval ledgers,
registries of what is installed, and the reproducibility pins.

## Shape (OBSERVED)
There is **no ORM and no single storage substrate**. Each store is a hand-rolled triad —
`InMemory*` / `Sqlite*` (raw `sqlite3` under `asyncio.to_thread` + lock) / `Postgres*`
(`asyncpg`, mixin `PgSchemaInit`) — selected by `CAIRN_STORAGE` (or a per-store env var).
Counted: **102 classes** matching `*Store|*Ledger|*Registry` in `src/cairn/core`; about 30
triads. `storage/backend.py StorageBackend` was meant as "the single injectable" but
carries only `runs`, `memory`, `rag` (+ lazily-built trigger stores).

| Store | Module | Backends | Record |
|---|---|---|---|
| Runs + events | `storage/protocols.py` (`Run`, `RunEvent`, `RunStore` ABC), `storage/{memory,sqlite,postgres}/runs.py`; factory `storage/__init__.py:74 default_store` | memory / sqlite / postgres | `Run` (JSON blob + indexed columns), `run_events` (monotonic `seq`, SSE backlog via `_EventHub`) |
| Pipeline records | `storage/pipeline_store.py` | triad | `PipelineRecord` (plan snapshot, `step_state`, cursor, governance) |
| Artifacts / datasets | `storage/artifacts.py` (1286) | sqlite / postgres / Null | `ArtifactRecord` versions, `artifact_parents` lineage, `artifact_tags`, roles, intake verdicts; `dataset:<name>[@N]` refs |
| Artifact bytes | `storage/artifact_bytes.py`, operator `artifact_local`, `s3` | fs / S3 | blobs |
| Training runs | `storage/training_runs.py` | triad | one row per training run (ADR-0102 D2); `optuna_storage_url` |
| Judges, judge labels, reviews, eval ledger | `storage/judges.py`, `judge_labels.py`, `reviews.py`, `eval_ledger.py` | triad (+Null) | versioned judges; HITL reviews; scores |
| Capture, fingerprints, knowledge corpora, rag catalog, suggestions, episodic memory | `storage/*.py` | triad / Null | traces; MinHash fingerprints; workspace corpora |
| Workflow registry (install-then-run, ADR-0047) | `src/cairn/core/workflow_registry.py` | triad | `InstalledWorkflow` (schema by hash), `WorkflowSchema`, pipeline enable/sync |
| Registry install, profiles, policies, features, accounts, console refresh tokens (ADR-0142), connections, pilots, OAuth tokens, remote jobs, workspaces, workflow config, schedules/triggers/inbox | `registry_install.py`, `profile_store.py`, `policy/store.py`, `features.py`, `account.py`, `refresh_tokens.py`, `connections.py`, `pilot.py`, `pilot_ledger.py`, `oauth/store.py`, `operator/job_store.py`, `workspace.py`, `workflow_config_store.py`, `storage/{schedule_store,trigger_store,trigger_inbox}.py` | triads | |
| Migrations | `storage/migrations.py` (sqlite helpers), `pg_schema_init.py` | | `CREATE TABLE IF NOT EXISTS` + additive column repair |

## Provenance chain (OBSERVED)
```
pack content hash      cairn.lock (CI: cairn lock --check)
template identity      CompiledTemplate.content_hash (engine/compiled.py:62)
installed record       workflow_registry.InstalledWorkflow{schema_hash, capabilities, operators}
run identity           Run.template_ref stamped at admission (dispatcher.py:565..), Run.schema_hash,
                       Run.resolved_operators, Run.bundle_pipeline_id/step_id, Run.trace_id
submit pins            Run.train_pins ← operator/pins.py:143 record_train_pins (dataset hash, image digest, spec)
artifacts              ArtifactRecord.content_hash + parents (lineage DAG) + tags (the only mutable thing)
events                 RunEvent seq stream (store) + bus run.* + events.jsonl + optional audit.jsonl (hash-chained)
bundle                 run_bundle.py build_run_bundle — one zip that reproduces a run (ADR-0102 D5)
```

## Does not own
Enforcement (governance modules), execution, the choice of which store a route reads
(routers reach stores directly — 28 `triggers → storage` edges).

## Public interface
Per-store Protocols (`RunStore`, `ArtifactStore`, `PipelineStore` shapes), factories
(`default_store()`, `build_artifact_store()`, `default_pipeline_store()`,
`get_workflow_registry()` …), `resolve_dataset_ref`, `parse_dataset_ref`.

## Dependencies
`storage` → `core/<top>` (24), `services` (5 lazy: memory/rag), `triggers` (3 lazy:
building trigger stores), `dataflow` (2), `engine` (2), `policy`, `oauth`, `operator`.

## Dependents
Everything: `core/<top>` (41), `triggers` (28), `dispatch` (20), `runtime` (9), `cli` (9),
`operator` (8), `eval` (6), operator packs (`dataset_checks`, `dataflow`, `training`, `ingest`, `monitor_threshold`).

## Side effects
File I/O (sqlite files under `~/.cairn` / `CAIRN_RUN_DB`), Postgres connections (asyncpg pools),
S3 (via operator), advisory locks (`dispatch/coordination.py` uses Postgres for fleet-wide
critical sections).

## State
The stores *are* the state. Singletons: `reset_storage`, `reset_suggestion_store`,
`reset_remote_job_store`, `reset_token_store`, `reset_feature_store`, `_store_instance` in
`console/artifacts_api.py`.

## Configuration (OBSERVED counts)
280 `os.environ`/`getenv` reads in `src/cairn/core`; 153 distinct `CAIRN_*` names in core, of
which `settings.py`'s `INVENTORY` documents 35. `CAIRN_STORAGE` is the main selector;
several stores still honour legacy per-store knobs (`CAIRN_RUN_DB`, `CAIRN_CONNECTIONS_DB`,
`CAIRN_CHECKPOINTER`, `CAIRN_BUS`…). The 2026-07 inventory (`docs/audits/rearchitecture-problem-inventory.md`
P9–P14) already names this; ADR-0083 R3/R4 propose one config plane and one storage
substrate — **proposed, not built**.

## Entry points
Built at app boot (`triggers/server.py:build_app`), by the CLI, by the Temporal worker, and
lazily by any module calling a factory.

## Tests
`tests/core/test_storage_resolver.py`, `test_run_store*`, `test_artifact_registry.py`,
`test_artifact_bytes.py`, `test_pipeline_artifacts.py`, `test_run_lineage.py`,
`test_train_pins.py`, `test_training_history.py`, `test_workflow_config_store.py`,
`test_policy_pg_store.py`, `test_knowledge_corpora.py`, `test_capture_api.py`.

## Invariants
| Invariant | Status |
|---|---|
| Identity recorded at admission, never re-derived (ADR-0084 R2) | CONFIRMED (`Run.template_ref`; `test_run_template_ref.py`) |
| Artifact registration is control-plane only; engines return descriptors | CONFIRMED (`leg_executor.py:347-364`, `storage/artifacts.py` docstring) |
| Tags are the only mutable artifact state | DOCUMENTED (`artifacts.py`); not separately tested (UNCLEAR) |
| Every store follows `CAIRN_STORAGE` | PARTIALLY — legacy knobs remain; `storage/__init__.py:74` documents the fallback ladder |
| Prod HA needs Postgres for every store | DOCUMENTED gap (P13); several stores have sqlite-only or Null tiers (e.g. `playground/upload_store.py`, `judge_labels.py`) |
| Knowledge corpora are workspace-scoped (ADR-0062): role composition lists the installed packs' corpora plus the current workspace's own corpora only; a workspace corpus is indexed into its own workspace's collection (`workspace_collection`, one unambiguous prefix per workspace; `default` keeps bare names) whatever context indexes it | CONFIRMED (`services/rag/roles.py list_roles`, `services/rag/byo.py`; `test_workspace_scoped_knowledge.py`) |
