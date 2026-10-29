# Data contracts

What a dataset is to cairn, how it is named, resolved, judged and handed between legs.
CURRENT sections are OBSERVED at the cited lines on `main`; TARGET sections are the
frozen training architecture (`docs/architecture/training/FREEZE-v1.md`) and are not
implemented unless a PR is named.

## CURRENT

### Identity — two layers
- **Artifact** (bytes): a content-addressed `ArtifactRecord` whose `id` is the sha256 of the
  bytes (`src/cairn/core/storage/artifacts.py:53-71`) — see [artifacts.md](artifacts.md).
- **Dataset version** (name): an append-only row `name@N` over one artifact
  (`register_dataset_version`, `artifacts.py:93-100`; columns `artifacts.py:404-407`:
  `name, version, artifact_id, created_at, created_by, role, schema, column_mapping,
  parent_version, split_policy, workspace_id, verdict, verdict_reasons, verdict_report,
  verdict_at, dated_at, origin, origin_detail`). Re-registering the artifact already at a
  name's head in the same workspace is a no-op, never a new version (`artifacts.py:640-656`). A version never moves.

### Roles and declared metadata
`role ∈ {training, validation, test, golden}`, a declared `schema` and `column_mapping`,
inherited from the dataset name's defaults unless the producer declares otherwise
(`set_dataset_meta` / `set_dataset_version_meta`, `artifacts.py:122-154`; ADR-0102 D1).
`origin` is a fact about the bytes and cannot be re-declared (`artifacts.py:126-129`);
`dated_at` says when the material is from.

### References and resolution
A leg input may be a path, an `s3://` URI, an HF id or a **registered ref**
`dataset:<name>[@N]`. Refs resolve on the control plane before the engine sees them
(flow legs `src/cairn/core/dispatch/leg_executor.py:229-246`, task legs
`src/cairn/core/dispatch/pipeline_bridge.py:2029-2031`): `resolve_dataset_refs_in_config`
rewrites each ref to `<uri>#sha256=<content_hash>` (`resolve_dataset_refs_in_config`), where
`<uri>` is the location the workspace registered the version at, or its own durable copy
(`durable_uri`, the object store) when there is one and either the leg's engine declares
`needs_durable_inputs` (a job on another machine: the SSH engine, ADR-0149 D3) or that
location is a local path that no longer exists (`resolve_dataset_ref`); an unknown
ref fails the leg at launch (`KeyError`). On the builtin SSH engine a version whose only
location is a control-plane path is refused before submit (`engines_ssh.py` `validate`). The consuming kernel strips the fragment and
**verifies the bytes** it read against it, refusing on mismatch (`ingest.normalize`,
`cairn/operators/official/dataset_checks/src/operator_dataset_checks/dataflow_stages.py:70-85`);
`file://` and `file:/` forms are local paths (`_io.py:54-68`, fixed by #2171).

### Workspace scope
A ref resolves within one workspace: the run's (scoped from its record), or the one passed
as `workspace_id`. The name, a pinned `@N` and `@latest` see only that workspace's
versions; another workspace's version reads as not found, the same as an unknown ref.
With auth on, resolving outside any workspace scope raises `WorkspaceScopeRequired`.
The registry's by-name reads (`get_dataset_version`, `list_dataset_versions`,
`get_dataset_version_meta`, `list_datasets`) and writes (`set_dataset_version_meta`,
`set_dataset_version_verdict`) take an optional `workspace_id` and otherwise answer in the
current workspace; a write never changes another workspace's version. Any workspace may
register under any name. The identical-bytes no-op compares with, and updates only, the
registering workspace's own head. A leg registers its exports, and stamps their intake
verdicts, in the run's workspace.

What is shared per name across workspaces:
- **Version numbers.** The key is `(name, version)`: one sequence per name, so a
  workspace's versions of a name need not be consecutive. Registrations of one name are
  serialised (a per-name advisory lock on Postgres; on SQLite a number taken by another
  process is re-read and retried a bounded number of times).
- **Name-level metadata.** `set_dataset_meta` keeps one row per name: description, labels
  and the declared defaults (role, schema, column mapping). `list_datasets` returns the
  description and labels of that row. A version registered without an explicit role,
  schema or column mapping inherits the name's defaults, whichever workspace registers it.

### Producer → consumer rules
- A readiness/intake leg's export is described by the engine as an `ArtifactDescriptor`
  and registered control-side (`leg_executor.py:436-459`); a `register:` config on the
  export stage names the version (`stats.register_as`), and the leg fails loudly if a
  requested registration cannot happen (`leg_executor.py:436-449`).
- A training leg consumes the upstream export by URI; the bridge pins `dataset_uri` and
  `dataset_hash` on the train run (`Run.train_pins`, `protocols.py:252`;
  `pipeline_bridge.py:1016-1022`), so what was trained on is recoverable by content.
- Lineage: an export's `parents` are the producing stage's inputs by content id
  (`runner.py:447-449`; `_resolve_uri_parents`, `leg_executor.py:522-545`).

### Facts vs admission
- **Facts** about a version: rows in/out, dedup counts, PII backend and counts,
  contamination checks, expectation results, token profile — folded by
  `verdict_from_flow_stats` (`src/cairn/core/intake.py:279`) into `verdict_report`.
- **Check evidence** (C2, #2188): `verdict_report.checks` carries one entry per known check
  (`ingest, dedup_exact, dedup_near, decontam, pii, expectations, profile_tokens, export` —
  `intake.py:84`) with `ran | skipped | not_applicable | degraded | failed`, backend, counts
  and the kernel's distribution version (`check_map_from_flow_stats`, `intake.py:242`);
  the same map rides the data leg's stats flat (`checks.<name>.<field>`,
  `leg_executor.py:441-452`). A flow may declare `checks: {<name>: not_applicable}`
  (`FlowDef.checks`, `src/cairn/core/dataflow/defs.py:88-93`). Which checks are *required* is
  policy's (#2190) — core records evidence only.
- **Verdict** (`admitted | degraded | blocked | invalid_format`, `intake.py:55-58`) is
  stamped on the version at registration (`leg_executor.py:453-470`). Two doors read it:
  the agentic path per run (`check_intake_for_inputs`, `intake.py:588`; `dispatcher.py:856`)
  and the pipeline launch door once per launch (`src/cairn/core/admission_door.py`, T05 #2190),
  which also applies the policies' required checks and writes the decision on the record.
- Holdout: a `role=training` version may not overlap a golden/test version; checked on the
  same agentic door (`src/cairn/core/holdout.py:473`; `dispatcher.py:837`).

### Failure and refusal (CURRENT)
Unknown ref → leg fails at launch. Hash mismatch → kernel raises, leg fails, nothing
trained. `blocked` / `invalid_format` → refused at the agentic door with the reasons;
`degraded` runs with reasons on the run (`Run.intake`, `protocols.py:318`).

### Extension rules for packs
Packs add **kernels** (stages) under the `cairn.dataflow_stages` entry-point group and
report scalar stats per stage (`<stage>.<key>`); they never write to the registry (the box
has no database, ADR-0081 D3) — registration and verdicts are control-side. New facts are
new stage stats; new *decisions* are governance's (see [governance.md](governance.md)).

## TARGET (frozen training architecture; not implemented unless noted)
- **`#sha256=` mandatory** for a re-runnable proof (C1 target; optional today).
- **Policy versions** for the admission decision (today: a digest of the applicable
  requirements, `policy@<digest>`); the planner's reuse rule (#2211).
- **Dataset version id in the training spec** (`TrainSpec.extra.dataset_version`), not
  only a URI (C4).
- **Dataset replay ≠ RL experience** (I-rules): replay versions carry `parents` = sources +
  ratio/strategy/seed; RL rollouts are a different artifact family.

## Shared definition schema bodies — CURRENT internal reader (#2240 B1)

Shared bodies use Draft 2020-12 meta-validation and the accepted bounded keyword policy.
Boolean schemas, type/constant, object/array, numeric/string bounds, composition, `$defs`,
`$ref` and annotation keywords are supported. `format`, regex/content, dynamic-reference
and unknown keywords reject at schema positions. Property/definition names and annotation
payloads are data, not schema keywords. Present `$id` must match the descriptor contract URI;
a declared dialect must be Draft 2020-12. Defaults and annotations have no execution effect.
Legacy `schema://` is unchanged.

### Reference closure — CURRENT internal (#2240 B2)

`resolve_contracts` accepts only `contract://<id>/<revision>` references, optionally with a
JSON Pointer fragment (`#/...`, `~0`/`~1` escapes honoured), and same-document `#/...`
pointers inside schema bodies. Anchors, `file:`/`http(s):`/relative references, unknown
pointers and fragments that land outside a schema position refuse. A target must exist in
the same distribution or a directly declared dependency and be of the required kind:
schema `$ref` → schema; capability `input`/`configuration`/`output` → schema and `report` →
report, both without fragments. A cycle through in-place applicators (`$ref`, `allOf`,
`anyOf`, `oneOf`, `not`, `if`/`then`/`else`) refuses because validation would never
terminate; recursion through `properties`/`items` is allowed. Contract YAML refuses aliases.
