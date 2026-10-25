# Artifact contracts

## CURRENT (OBSERVED on `main`)

### `ArtifactRecord` — the registry row
`src/cairn/core/storage/artifacts.py:55-70`:

| Field | Meaning | Rule |
|---|---|---|
| `id` | sha256 of the bytes | **the identity**; two registrations of identical bytes are one row |
| `kind` | `dataset` · `model` · `report` · `index` (descriptor literal, `src/cairn/core/dataflow/contract.py:234`) | what the bytes are |
| `uri` | where the bytes were produced (`file://`, `s3://`, `hf://`, `stub://`) | may be a box-local path |
| `durable_uri` | where they can still be read after the producing box is gone | set by the relay (`src/cairn/core/storage/artifact_bytes.py:13-17`) when `CAIRN_ARTIFACT_S3_BUCKET` is configured; empty otherwise |
| `parents` | content ids (or URIs the registry could not map) of what produced it | lineage DAG; URIs are mapped to ids when known (`leg_executor.py:522-545`) |
| `stats`, `data_schema`, `size_bytes`, `row_count` | facts from the producer | never re-derived control-side |
| `producer_run_id`, `producer_job`, `account_id`, `created_at` | provenance | set at registration |

Immutability: rows are written once (`ArtifactRecord` docstring). Retention: no registry
retention policy exists (TARGET, #2196); `retention.py` covers run/event retention only.

### Workspace scope — who sees a record, and whose tag moves (ADR-0147, ADR-0148)
| Rule | Where |
|---|---|
| The artifact and tag methods (`register`, `get`, `find_by_uri`, `list_artifacts`, `lineage`, `resolve`, `promote`) take `workspace_id=` (keyword), else the workspace in scope (`scoped_workspace()`); with auth on and neither they raise `WorkspaceScopeRequired` before anything is read, relayed or written. The by-name dataset methods keep their `current_workspace()` fallback | `_artifact_workspace` in `src/cairn/core/storage/artifacts.py` |
| `register` writes the registering workspace's own registration to `artifact_owners` (`kind`, `uri`, `stats`, `data_schema`, `size_bytes`, `row_count`, `producer_run_id`, `producer_job`, `account_id`, `created_at`, `durable_uri`), also for bytes already registered; identical bytes stay one `artifacts` row (the identity and the count) with one registration per workspace; a workspace keeps its first registration of the same bytes | `register` (both backends) |
| Parents are recorded per registering workspace (`artifact_parents` keyed by `(workspace_id, child_id, parent_id)`); a record's `parents` are those the reader's workspace declared, independent of what other workspaces registered | `_parents_sql` |
| `get`, `find_by_uri`, `list_artifacts`, `resolve`, `lineage` build each record from the reader's own registration; another workspace's artifact reads as not found; `find_by_uri` matches the reader's own URIs; the lineage tree walks only into artifacts the reader owns | `_OWNER_READ`, `_walk_lineage` |
| `register_dataset_version` refuses an artifact the workspace does not own; version reads resolve their artifact in the version's workspace | dataset registry methods |
| Tags are keyed by `(workspace_id, name)`; `resolve` reads and `promote` moves that workspace's tag; `promote` refuses (`KeyError`) an artifact the workspace does not own | `artifact_tags`, `resolve`, `promote` |
| `count` is the deployment's row count | `count` |
| Migration of an existing database (once): tags → `default`; owners backfilled with `default`, the producing run's workspace (runs in the same database) and every workspace with a dataset version of the artifact, each copying the existing registration; parents → every owner of their child | `_migrate_sqlite_workspace_scope`, `PostgresArtifactStore._ensure_owners` |

### `ArtifactDescriptor` — what an engine may say
The only artifact-shaped thing that crosses the engine boundary
(`src/cairn/core/dataflow/contract.py:226-250`): `uri, kind, content_hash, rows, size_bytes,
data_schema, parents, stats`. Engines describe; the control plane registers
(`leg_executor.py:436-459`; the door path did this first, #1316). Engines never hold
registry credentials (ADR-0081 D3, ADR-0082 D3).

### CandidateModelArtifact (CURRENT since T04, #2189 — OBSERVED `tests/core/test_training_core_golden_path.py::test_C6_*`, `tests/core/test_candidate_identity.py`)
A model-producing flow leg registers its output as `ArtifactRecord(kind=model)`: `package.adapter`
hashes the weight files (`*.safetensors`, `*.bin`, `adapter_config.json`; never logs) and
emits `output` + `content_hash` + `kind: model` (`operator_training/flow_stages.py`
`hash_candidate_dir`, `package_adapter`, `cairn/operators/official/training/src/operator_training/flow_stages.py:333-432`);
the runner carries `kind` (`src/cairn/core/dataflow/runner.py`, the descriptor block of `run_dataflow`);
`register_leg_descriptors` (`src/cairn/core/dispatch/leg_executor.py:436-459`) registers it with `producer_run_id`, `parents` and the
provenance stats (`base_model`, `method`, `spec_digest`) and surfaces `candidate_artifact_id`,
`candidate_artifact_uri`, `candidate_producer_run_id` on the leg's findings — before any evaluate
leg. Identical bytes register once (content-addressed): the registry row keeps the **first**
producer's `producer_run_id`, while the leg's `candidate_producer_run_id` names the run that
surfaced the id this time. A leg that emits several model descriptors surfaces the last one as
*the* candidate (one producer per leg today; #2205/#2206 decide for compositions). A remote-only
adapter with no bytes on the box reports `candidate_hashed: false` and gets no id — never a
fabricated one. Task-leg
producers (`compress/*`, `model/merge`) call the same function (#1488, #2206).

### Local vs external registry identity
- Local registry: the content id above; a dataset name maps to it through
  `dataset_versions` ([data.md](data.md)).
- MLflow: `model/register` (`cairn/operators/official/model_registry/src/operator_model_registry/operators/register.py`)
  registers a model version in MLflow; the MLflow version number is MLflow's identity, not
  cairn's, and is returned as a finding (`registered_model_version`). The cairn identity of
  the same adapter is the `ArtifactRecord(kind=model)` the producing leg registered (below).

### Producer / consumer expectations
- A producer reports `output` + `content_hash` per stage (`runner.py:440-455`); the first
  stage's parent is the flow's declared input.
- A consumer that receives a URI must verify content when a hash is given
  ([data.md](data.md) §References) and must not assume `uri` is reachable from another box —
  use `durable_uri` where a relay ran.

## TARGET (frozen training architecture)
- **Evaluation keyed on the candidate id** (#2194) and validity metrics (#2195): not implemented.
- **Released-artifact retention and durable copy on registration** (#2196), registry tags
  `cairn.run_id`, `cairn.pipeline_id`, `cairn.candidate_artifact_id`, `cairn.dataset_version`.
- **Transformation artifacts** (quantize/merge, #2206): consume a registered id, produce a
  registered artifact with `parents=[source]` and format/compatibility metadata.
