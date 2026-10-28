# ADR-0148 — An artifact is visible to the workspaces that registered it

- **Status:** ACCEPTED 2026-10-23 by the owner (option 2a: an ownership table written on every
  registration, reads filtered through it).
- **Date:** 2026-10-23
- **Issue:** tracked privately (deferred from cairndev-sh/cairn#190).
- **Amends:** the `ArtifactStore` protocol (`register`, `get`, `find_by_uri`, `list_artifacts`
  and `lineage` take a workspace) and the registry schema (a new `artifact_owners` table).
- **Keeps:** ADR-0081 D2/D3 (content-addressed, immutable versions registered control-side from
  engine descriptors; identical bytes are one row), ADR-0088 H6 (checkpoint relay and resume),
  the dataset registry's by-name workspace scope.
- **Builds on:** ADR-0051 (workspace isolation), the 0.2.0 rule that auth on with no workspace in
  scope is refused.

## Context

An artifact's id is the hash of its bytes, and the registry answered any id, URI or listing for
the whole deployment. Dataset names were already per workspace, but an id read from a run, a log
or a guess returned another workspace's record: its URI, durable copy, stats, schema and lineage.
`find_by_uri` could also map a URI to another workspace's artifact while recording a leg's
parents. The registry keeps no workspace on the row, and it cannot: content addressing makes
identical bytes from two workspaces one row.

## Decision

**D1 — Each registering workspace keeps its own registration.** `artifact_owners(artifact_id,
workspace_id, kind, uri, stats, data_schema, size_bytes, row_count, producer_run_id,
producer_job, account_id, created_at, durable_uri)`, keyed by `(artifact_id, workspace_id)`.
Every `register` writes the registering workspace's row, including a registration of bytes that
are already registered (by itself or by another workspace). The `artifacts` row stays one per
hash: it is the identity and the deployment's count, and no read returns it. A workspace keeps its
first registration of the same bytes; a later one by the same workspace adds parents and fills a
missing durable copy, as before.

**D2 — Parents are recorded per registering workspace.** `artifact_parents` is keyed by
`(workspace_id, child_id, parent_id)`. A record's `parents` are the parents that the reader's
workspace declared for it, whether or not another workspace has registered an artifact with a
parent's id; another workspace's declared parents, external URIs included, are never returned.

**D3 — Reads answer from the reader's own registrations.** `get`, `find_by_uri`,
`list_artifacts`, `resolve` and `lineage` build each record from the reader's own owner row:
its URI, durable copy, stats, schema, producing run and job, account and time. Another workspace's
artifact reads as not found. `find_by_uri` matches the reader's own URIs only. The lineage tree
walks only into artifacts the reader owns. The dataset registry reads a version's artifact in the
version's workspace, and `register_dataset_version` refuses an artifact the workspace does not
own. `count` stays the deployment's row count.

**D4 — The workspace comes from the caller or the scope.** The artifact and tag methods
(`register`, `get`, `find_by_uri`, `list_artifacts`, `lineage`, `resolve`, `promote`) take
`workspace_id=` (keyword), else `scoped_workspace()`; with auth on and neither, they raise
`WorkspaceScopeRequired` before relaying bytes or writing a row. The by-name dataset methods keep
their existing fallback to `current_workspace()` when no `workspace_id=` is given; this ADR does
not change it. Core callers pass the run's workspace: a leg's registration and URI-parent mapping
(`register_leg_descriptors`), checkpoint relay (the leg's run record) and resume lookup, and
`registry_health` (`cairn doctor` checks the process's workspace, `CAIRN_WORKSPACE` else
`default`).

**D5 — An existing database is migrated once.** In one transaction (SQLite `BEGIN IMMEDIATE`;
Postgres under the fleet-wide schema lock), when `artifact_owners` is created on a database that
already holds artifacts: `default` owns every artifact; so does the workspace of the run that
produced it, when that run is in the same database; so does every workspace that registered a
dataset version of it. Each owner row copies the registration from the existing `artifacts` row.
`artifact_parents` is rebuilt onto its new key, each existing parent going to every owner of its
child. The backfill runs only when the table is created, so a later registration is never handed
to `default`.

## Consequences

- A single-workspace deployment is unchanged: everything is registered and read in `default`.
- Another workspace's artifact reads exactly like an id nobody registered.
- Identical bytes registered by two workspaces are visible to both, and each reads only its own
  registration and parents. A dataset reference or a checkpoint resume in a workspace resolves to
  that workspace's own location.
- A parent id the reader declared stays in `parents` even when the reader cannot read that
  artifact; the lineage tree has no node for it.
- Every process that uses the registry must run this version. An older replica on a migrated
  database writes no owner row, so what it registers is visible to no workspace; its parent rows
  are dropped (SQLite) or its registration fails after the artifact row is written (Postgres); its
  `promote` fails, because its conflict clause no longer matches the tag key; and its reads still
  answer across workspaces. Upgrade every replica together.
- A caller outside any workspace scope (a deployment-admin route, a seed script) must pass
  `workspace_id=`, or with auth on the call is refused.
- An artifact registered before this change by a run in another database, with no dataset
  version, is owned by `default` only. Existing rows registered by several workspaces before the
  change carry the first registration, which every backfilled owner reads.
