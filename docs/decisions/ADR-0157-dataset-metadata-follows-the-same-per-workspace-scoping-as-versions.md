# ADR-0157 — Dataset metadata follows the same per-workspace scoping as dataset versions

- **Status:** PROPOSED 2026-10-28. Approval: pending maintainer.
- **Date:** 2026-10-28
- **Issue:** to be filed on cairndev-sh/cairn (implementation).
- **Amends:** the `ArtifactStore` protocol (`set_dataset_meta` and `get_dataset_meta` take
  `workspace_id=`), the key of the `dataset_meta` table, and the "Name-level metadata" paragraph
  of `docs/contracts/data.md:60-63`.
- **Keeps:** ADR-0148 (versions and references answer per workspace; the by-name methods fall back
  to `current_workspace()`), ADR-0147 (tags per workspace), ADR-0102 D1 (a version inherits its
  name's declared defaults at registration), and version numbers as one sequence per name
  (`docs/contracts/data.md:56-59`).
- **Builds on:** cairndev-sh/cairn#182 (dataset references resolve within the run's workspace).

## Context

The dataset registry answers a name within one workspace. `get_dataset_version`,
`list_dataset_versions`, `get_dataset_version_meta` and `list_datasets` read in one workspace, and
`set_dataset_version_meta` and `set_dataset_version_verdict` write in one workspace. Each takes
`workspace_id=` and otherwise uses the current workspace
(`src/cairn/core/storage/artifacts.py:138-142`, `_dataset_workspace` at `:605-608`;
`docs/contracts/data.md:43-54`).

The metadata of a name is the one by-name record that is not scoped this way:

- `dataset_meta` is keyed by `name` alone, on SQLite (`artifacts.py:451-455`) and on Postgres
  (`:1063-1066`).
- `set_dataset_meta` and `get_dataset_meta` take no workspace: the protocol at `:186-191`, the null
  store at `:265-272`, SQLite at `:974-1009`, Postgres at `:1442-1480`.
- `list_datasets` filters the versions by workspace but joins the metadata on the name alone
  (`:969`, `:1437`).
- At registration, `_declare` reads the name's default role, schema and column mapping without a
  workspace (`:611-627`), although the version itself is registered in the registering workspace
  (`:788-794`, `:1267`).

The data contract documents this as shared per name (`docs/contracts/data.md:60-63`). So within a
workspace, the description and labels that `list_datasets` shows, and the defaults a new version
inherits, are a deployment-wide record. Every other fact about the name belongs to the workspace.
Two teams that use the same dataset name cannot each describe it or declare its defaults: the last
write is what both read. This is the one remaining inconsistency in the registry's per-workspace
design.

## Decision

**D1 — `dataset_meta` is keyed by `(workspace_id, name)`.** Each workspace keeps its own
description, labels and declared defaults for a name. The same name in two workspaces is two rows.

**D2 — `set_dataset_meta` and `get_dataset_meta` answer in one workspace.** Both take
`workspace_id=` (keyword) and otherwise use `_dataset_workspace`: the workspace named, else
`current_workspace()`, else `default`. That is the fallback every by-name dataset method already
uses (ADR-0148 D4 keeps it). A name with no row in that workspace reads as the empty defaults,
exactly as an unknown name reads today. The null store accepts and ignores the argument.

**D3 — A version inherits its own workspace's defaults.** `register_dataset_version` passes the
workspace it registers in to `_declare`, and `_declare` reads that workspace's row. An explicit
role, schema or column mapping from the producer still wins (ADR-0102 D1).

**D4 — `list_datasets` joins on `(workspace_id, name)`.** A workspace's listing shows that
workspace's own description and labels.

**D5 — An existing database is migrated once, keeping what every workspace reads today.** In one
transaction, each existing row is copied to `default` and to every workspace that holds a version
of that name. On the first read after the upgrade, every workspace therefore sees the description,
labels and defaults it saw before. From then on, each workspace's rows change independently.

- **SQLite:** inside the existing `_migrate_sqlite_workspace_scope` `IMMEDIATE` transaction, when
  `pk_columns(dataset_meta)` is not `[workspace_id, name]`, `rebuild_table` moves the table onto
  the new key with the copies above.
- **Postgres:** under the fleet-wide schema lock, add `workspace_id TEXT NOT NULL DEFAULT
  'default'`, insert the copies for the other workspaces, and swap the primary key.

Both are no-ops once the key is current.

**D6 — Version numbers stay one sequence per name.** This ADR scopes the metadata only. The
shared `(name, version)` sequence (`data.md:56-59`) is unchanged.

## Consequences

- A single-workspace deployment is unchanged: every row is in `default`, and so is every read.
- The registry's by-name surface is uniform. A name's versions, verdicts, metadata and tags all
  answer in one workspace.
- A caller outside any workspace scope (a deployment-admin script, a seed) passes `workspace_id=`.
  Otherwise it writes `default`, as the other by-name methods do.
- Every process that uses the registry must run this version. An older replica's
  `ON CONFLICT (name)` no longer matches the table's key, so its metadata writes fail. Upgrade
  every replica together, as for ADR-0147 and ADR-0148.
- Callers of `set_dataset_meta` outside this repository pick up the workspace from their request
  scope, or pass it, when they bump their core pin.

## Alternatives rejected

- **Keep the metadata shared, and document it.** No change, but it leaves the one by-name record
  that does not follow the registry's scoping.
- **A per-workspace overlay over a shared row.** A two-level lookup for every read, and a new
  precedence rule to explain, for a record that is small and cheap to copy.
- **Migrate every row to `default` only.** Simpler, but on the first read after the upgrade, every
  other workspace that used a name would lose the description and defaults it reads today, and a
  version registered there would stop inheriting its declared role.
- **Move the defaults onto version rows only.** It drops the "declare the role before v1 exists"
  behaviour that ADR-0102 D1 relies on.

## Migration

D5, run on the first connect after the upgrade. Nothing changes for a database with only the
`default` workspace, apart from the key. The contract paragraph at `docs/contracts/data.md:60-63`
moves from "What is shared per name across workspaces" to the workspace-scope section.

## Test plan

Extend `tests/core/test_artifact_workspace_scope.py`, which already runs in-memory SQLite, a SQLite
file, and Postgres when `PGSTORE_DSN` is set:

- two workspaces set different descriptions, labels and `default_role` for one name, and each
  reads its own;
- `list_datasets` in each workspace shows that workspace's metadata;
- a version registered in workspace B without an explicit role inherits B's `default_role`;
- migration: a database written before this change, with metadata for a name that has versions in
  `default` and in `tenant-a`, ends with one row in each, with identical values;
- a second open is a no-op;
- the null store keeps its empty defaults;
- the protocol conformance tests pass with the new keyword.

## Open questions for the maintainer

1. D5: copy each existing row to `default` and to every workspace that holds a version of the name
   (recommended, no visible change on upgrade), or to `default` only?
2. Should a name's metadata be writable in a workspace that holds no version of that name? Today
   it is, which is how the import dialog declares a role before v1 exists. The recommendation keeps
   that.
3. Is the separate per-name version-number sequence (D6) worth its own follow-up ADR, or should it
   stay shared by design?
