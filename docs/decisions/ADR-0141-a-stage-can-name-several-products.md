# ADR-0141 — A dataflow stage can name several products, and the role each is registered for

- **Status:** ACCEPTED 2026-10-22 by the owner (option 1: a separate list on the result; a public
  ADR; roles from stage outputs accepted).
- **Date:** 2026-10-22
- **Issue:** cairndev-sh/cairn-extensions#6 (the split stage exports train, val and test).
- **Amends:** ADR-0081 D2 (`DataflowResult` gains `named_outputs`).
- **Keeps:** ADR-0081 D3 (engines return descriptors and scalar stats; the control plane
  registers), ADR-0118 D5 (the stage-contract re-check of `output_artifacts`, one descriptor per
  stage, in order), ADR-0102 D1/D3 (dataset roles; `golden` is human-rated only), #1432 lineage.

## Context

A data flow's runner returns one artifact descriptor per stage: the stage's product, whose parent
is the previous stage's product. A split stage makes three products — train, val and test files —
and a fine-tuning run needs all three as registered dataset versions carrying their roles, so the
holdout door (`cairn.core.holdout`, which compares a run's `training` versions against every
`test` and `golden` one) can prove the trainer never meets the test set. Two things were missing:
a stage could describe only one product, and a version registered from a flow took the dataset's
default role, so every split landed as `training`.

The one-descriptor-per-stage shape is load-bearing: the leg's stage-contract re-check
(`conformance_check.descriptor_problems`) requires it and indexes descriptors by stage, and the
engine conformance suite asserts it. Stage stats cross the boundary as scalars or flat lists
(`contract._assert_scalar_payload`), so a stage cannot return a list of objects.

## Decision

**D1 — A stage names its products in flat stats.** Besides its own product, a stage may return
`outputs: [<name>, …]` and, per name, `outputs.<name>.output`, `.content_hash` (both required),
`.kind`, `.rows`, `.bytes`, `.register_as` and `.register_role`. The runner
(`runner._named_outputs`) turns each name into a descriptor whose parents are the stage's parents:
the products are siblings of the stage's descriptor, never each other's parent, and the next stage
descends from the stage's descriptor as before. A named output without `output` or `content_hash`
fails the flow. An `outputs` stat with no `outputs.<name>.*` key is a kernel's own stat and is
left alone.

**D2 — They ride in their own list.** `DataflowResult.named_outputs` holds them;
`output_artifacts` keeps exactly one descriptor per stage, in order, so the stage-contract
re-check and the conformance suite are unchanged. The control plane registers both lists through
the one registration path (`leg_executor.register_leg_descriptors`).

**D3 — A stage may give a version its role, within limits.** A descriptor carrying `register_as`
and `register_role` is registered as a dataset version with that role. A stage may give only
`training`, `validation` or `test`; any other role, `golden` included, refuses the leg before
anything of it registers (`leg_executor._check_stage_roles`). Without `register_role` the
dataset's default role applies, as before. This is the trust a pack's code already has: the
`dataset/split` operator registers the same names with the same roles today.

**D4 — Compatible on the wire.** `named_outputs` is omitted from the result's JSON while empty, so
a result with no named products is byte-identical and an older control plane reads it. A result
that carries named products needs a control plane at this release or later.

## Consequences

- A split stage can export its splits as three versions with their roles in one leg; the holdout
  door sees the test version.
- Every engine that rebuilds or validates a `DataflowResult` (builtin, ssh, metaflow, skypilot)
  carries the field through `model_validate` / `model_copy`; none constructs the result itself.
- A kernel decides the role of what it registers, within D3. A pack that mislabels its training
  file `test` takes it out of the door's training-side check; review of the pack is the guard, as
  it is for `register_as` and for the operator path.
- A leg's candidate model (`candidate_artifact_id`) is a stage's own model descriptor; a named
  output of `kind: model` registers but never displaces it.
- Registering identical bytes at a version's head re-declares its role, as it did before with the
  default role: a re-run that names a role overwrites one a person set on that head.
- The version registered from a named output records no `parent_version` or `split_policy` (the
  `dataset/split` operator path records both); a later revision may carry them as more fields.
- On a remote engine a named output's file:// URI names the box's disk; a split whose versions the
  control plane must read (the holdout door reads rows) exports with `store: s3`, as an export does.
