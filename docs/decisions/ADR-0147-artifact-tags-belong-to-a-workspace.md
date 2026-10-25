# ADR-0147 — Artifact tags belong to a workspace

- **Status:** ACCEPTED 2026-10-23 by the owner (option 1a: tags keyed by `(workspace_id, name)`).
- **Date:** 2026-10-23
- **Issue:** tracked privately (deferred from cairndev-sh/cairn#190).
- **Amends:** the `ArtifactStore` protocol (`resolve` and `promote` take a workspace) and the
  `artifact_tags` table (its key).
- **Keeps:** ADR-0081 D2 (content-addressed versions; a tag is the only mutable artifact state),
  ADR-0081 P2b (promotion is a gated `artifact/promote` run), the dataset registry's by-name
  workspace scope.
- **Builds on:** ADR-0148 (artifact ownership), which decides what a workspace may promote.

## Context

A tag (`dataset@prod`, `model@prod`) names the version everything downstream consumes. The
`artifact_tags` table was keyed by the tag name alone, so a deployment had one `prod`: a tenant
that promoted its own artifact moved the tag every other workspace resolved, and could read which
artifact another workspace had promoted. Dataset names were already answered per workspace; tags
were the remaining deployment-wide name in the registry.

## Decision

**D1 — A tag is keyed by `(workspace_id, name)`.** Each workspace has its own namespace of tags;
the same name in two workspaces is two tags.

**D2 — `resolve` and `promote` answer in one workspace.** Both take `workspace_id=`, else the
workspace in scope (`scoped_workspace()`). With auth on and neither, they raise
`WorkspaceScopeRequired` before reading or writing, the same rule as dataset references (0.2.0).
`resolve` reads that workspace's tag only; another workspace's tag reads as not found.

**D3 — `promote` refuses an artifact the workspace cannot see.** The target must be an artifact
the workspace owns (ADR-0148); otherwise `promote` raises `KeyError` ("not registered in
workspace …"), the same answer as an id nobody registered.

**D4 — Existing tags move to `default`.** SQLite rebuilds `artifact_tags` onto the new key
(create, copy with `workspace_id = 'default'`, drop, rename) in one `IMMEDIATE` transaction;
Postgres adds the column with default `'default'` and swaps the primary key, both under the
fleet-wide schema lock. Both are no-ops once the key is current.

## Consequences

- A single-workspace deployment is unchanged: every tag is in `default`, as is every read.
- A tenant's `artifact/promote` run moves only its own workspace's tag; the operator takes the
  workspace from its run's scope and needs no change.
- A caller outside any workspace scope (a deployment-admin route, a script) must pass
  `workspace_id=`, or with auth on the call is refused.
- A tag promoted in another workspace before this change now lives in `default`. Re-promote it in
  the workspace that should own it.
