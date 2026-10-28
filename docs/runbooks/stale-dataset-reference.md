# Runbook — stale or unresolvable dataset reference

**Symptoms.**
1. A leg fails **at launch** with `dataset ref resolution failed: dataset ref 'dataset:x@N' not found in the registry`.
2. A readiness leg fails seconds in with `dataset verification FAILED for … the registry pinned <hash>`.
3. (Fixed in #2171) `FileNotFoundError: … 'file:/…'` — a `file://` URI collapsed to a path.
4. A leg on a `compute.ssh` box is refused before submit with `dataset version … has no durable
   copy and exists only on the control plane`.

## Why
`dataset:` refs resolve on the control plane before the engine sees them
(`resolve_dataset_refs_in_config` in `src/cairn/core/storage/artifacts.py`, called from
`execute_leg`) to `<uri>#sha256=<content_hash>`. `<uri>` is the location *this* workspace
registered, or its own durable copy (`durable_uri`) when the leg runs on another machine or
the registered local file is gone (ADR-0148, ADR-0149 D3). The
kernel verifies the bytes it reads against the fragment (`operator_dataset_checks/dataflow_stages.py:70-85`).
So (1) is a name/version that does not exist in *this* workspace's registry; (2) means the
object at the URI changed since registration (overwrite, pruned box, re-exported file) — the
registry never re-points a version. (4) means the version was registered from a local file
with no object store configured, so no machine but the control plane can read it.

## Diagnose
```
curl -s $CAIRN_ENDPOINT/datasets | jq '.[] | {name, versions}'
curl -s $CAIRN_ENDPOINT/datasets/<name>/versions | jq '.[] | {version, uri, durable_uri, verdict, workspace_id}'
sha256sum <local path from uri>          # compare with the version's artifact id
```

## Fix
- (1) Use the right name/version or workspace; a version registered in another workspace
  is invisible by design.
- (2) Do not edit the registry. Re-run the producing flow (import/readiness) and launch on
  the version it registers: changed bytes become a new version. Identical bytes keep this
  workspace's first registration (its URI does not change), so re-registering does not fix a
  moved file by itself; with the object store configured it gives the version a durable copy,
  which a remote leg or a missing local file then resolves to.
- (4) Configure the object store on the control plane (`CAIRN_ARTIFACT_STORE=s3` and
  `CAIRN_ARTIFACT_S3_BUCKET`, plus `CAIRN_ARTIFACT_S3_ENDPOINT` for MinIO), then register the
  version again while its file is still there, or re-run the step that produced it.
- (3) Upgrade to a build containing #2171.

## Prevent
Register through flows (`register:` on the export), keep `CAIRN_ARTIFACT_S3_BUCKET` set so
every version has a `durable_uri`, and never hand-edit files under a registered URI.
