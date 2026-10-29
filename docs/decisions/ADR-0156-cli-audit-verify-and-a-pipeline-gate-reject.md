# ADR-0156 — CLI: `cairn audit verify`, a pipeline-gate reject, and the verb list as it is

- **Status:** ACCEPTED 2026-10-28. Approval: maintainer, 2026-10-28 (the recommendation in each open question taken as the decision).
- **Date:** 2026-10-28
- **Issues:** cairndev-sh/cairn#246 (verify the audit hash chain), cairndev-sh/cairn#257 (an
  attributable reject for a pipeline gate); related: cairndev-sh/cairn#247 (the `cairn lineage`
  exit code, a bug fix inside an existing verb that needs no ADR), cairndev-sh/cairn#195 (a
  `remote` sub-command under the locked list), cairndev-sh/cairn#233 (gate decisions in the audit
  sink).
- **Supersedes:** ADR-0020 §24.20.14 as the record of which verbs exist. Its rule stays: a new
  top-level verb needs a decision.
- **Amends:** the HTTP API (a new `POST /pipelines/{id}/steps/{step}/reject`; the platform
  regenerates `console/openapi.json`), `cairn-client` (`reject_step`), and the operator SDK
  (`cairn.core.operator` re-exports the audit chain helper).
- **Keeps:** ADR-0028 (audit is a core interface bound to the `audit/store` sink; no new audit
  store), ADR-0034 (the run-level operation-gate reject), and the reviews router's rule that only
  the gate's own door decides a review (`src/cairn/core/triggers/routers/reviews.py:1-10`).

## Context

**The verb list.** ADR-0020 §24.20.14 locks "10 verbs": `init`, `validate`, `doctor`, `test`,
`eval`, `run`, `lock`, `visualize` and `replay`. `pyproject.toml:96` still cites that lock. The code
registers a different set (`src/cairn/cli/main.py`):

- 12 commands at `:82-100`: `doctor`, `ls`, `validate`, `export`, `run`, `preflight`, `gate`,
  `replay`, `lineage`, `test`, `lock`, `visualize`;
- 12 groups at `:103-137`: `init`, `remote`, `dev`, `connect`, `compute`, `config`, `flow`,
  `pipeline`, `worker`, `pack`, `deploy`, `demo`;
- whatever the installed `cairn.cli` providers add (`:142`; the public CLI wins every name,
  `src/cairn/cli/providers.py:114-131`);
- a hidden `ask` stub, mounted only when no provider supplies `ask` (`:150-166`).

`eval` is no longer public: it is a platform provider command (`main.py:138-140`).
**DOCUMENTATION / IMPLEMENTATION CONFLICT:** ADR-0020 says 10 verbs. The code has 24 public names
and one hidden stub.

**The audit chain.** `audit/store` writes `hash = sha256(prev_hash + canonical(row))`, starting
from a genesis of 64 zeros
(`packages/cairn-operator-audit-store/src/operator_audit_store/operators/audit_store.py:37-46`).
`cairn lineage` keeps a second copy of `_canonical`, marked "MUST match"
(`src/cairn/cli/commands/lineage.py:64-66`). It verifies the chain only as a side effect, and it
reads only `lineage.record` rows, which only a platform operator writes. In text mode, a log with
no such rows returns 1 before it reports a broken chain (`lineage.py:180-185` against `:197-200`;
cairndev-sh/cairn#247). There is no `audit` verb and no `/audit` route.

**A pipeline-gate reject.** A person can approve a pipeline gate. The approve door records who
decided (`decision_actor`, `reviews.py:25-41`) and checks the gate's `approvers:` first
(`_assert_may_decide`, `src/cairn/core/triggers/routers/pipelines.py:827`, `:887-902`). There is
no way to reject one:

- `advance_step` either submits the step or fails it on a false data predicate
  (`src/cairn/core/dispatch/pipeline_bridge.py:2829-2840`).
- The only "no" is a cancel, and its audit event records an empty actor
  (`pipeline_bridge.py:2910`).
- `cairn remote reject <run_id>` (`src/cairn/cli/commands/remote.py:859-868`) is the run-level
  operation-gate verb over `/runs/{id}/resume`. It does not reach a pipeline gate.

## Decision

**D1 — The verb list is recorded as it is, and new top-level names stay closed.** The 24 public
names above are the CLI surface, plus the hidden `ask` stub and the provider mount point. A new
top-level name needs an ADR, as ADR-0020 required. `pyproject.toml:96` and the CLI reference cite
this ADR. With D2 the list has 25 public names.

**D2 — `cairn audit verify` is a new group with one read-only command.** `audit` is the group, so
there is room for `audit tail` and `audit export` later. `verify` reads the log from
`--audit-log`, else `CAIRN_AUDIT_LOG`, else `./audit.jsonl` (the resolution `cairn lineage` uses).
It never writes. It prints the row count, the first and last `ts`, the head hash, and the first
broken row with its reason (`hash`, `prev_hash` or `corrupt line`). `--json` prints `{path, rows,
first_ts, last_ts, head_hash, ok, broken: [{row, reason}]}`. Exit codes:

- **0** — the chain verifies;
- **2** — it is broken, including a corrupt line;
- **1** — there is no log, or it cannot be read.

`cairn lineage` keeps its own role: provenance records. Its exit-code bug is fixed under
cairndev-sh/cairn#247.

**D3 — One hash rule, in core.** `cairn.core.obs.audit` gains `GENESIS_HASH`, `canonical(row)`,
`chain_hash(prev, row)` and `verify_chain(lines) -> ChainReport`. `cairn.core.operator` re-exports
them, because operator packs import only the SDK (ADR-0120 D7). `audit/store`, `cairn lineage` and
`cairn audit verify` all use this one helper, and no reader keeps a copy. What is written does not
change, so every existing log verifies under the shared helper.

**D4 — A pipeline gate is rejected through its own door.** A sibling route,
`POST /pipelines/{id}/steps/{step}/reject`, takes `{reason: str, approver?: str}`. `reason` is
required and non-empty. The route has approve's guards, in approve's order:

- `_pipeline_in_ws_or_404`;
- `_assert_may_decide`, before anything is recorded (403);
- the decider is `decision_actor(token, body.approver, fallback="console")`, so under auth the
  token's identity is recorded and the body's `approver` is ignored.

Only a step at `awaiting_approval` can be rejected. Any other wait answers 409: a manual, schedule
or event wait is released or cancelled, not rejected. The approve body and its meaning do not
change.

**D5 — A reject ends the pipeline as failed, with the decider and the reason recorded.** Under the
pipeline lock, `PipelineBridge.reject_step(pipeline_id, step_id, *, decider, reason)` does the
following:

- sets the step to `status=skipped`, `verdict=rejected`, `approver=decider`;
- fails the pipeline through `_fail_pipeline` with `failure_reason = "step '<step>' rejected by
  <decider>: <reason>"`, so finalizers run and in-flight legs are handled as on any failure;
- decides the gate's review as `rejected` with the reviewer and the rationale;
- publishes `pipeline.rejected` with `{step, decider, reason}`.

The pipeline status vocabulary is unchanged: the status is `failed`, and the step's verdict tells
a rejection from other failures. Under the Temporal owner, the bridge records the step's verdict
and decider first, then sends a new `reject_step` signal (the twin of `approve_step`,
`src/cairn/core/runtime/temporal/pipeline_workflow.py:84-88`). The workflow schedules nothing more
and ends as failed. Writing the decision to the hash-chained sink stays with
cairndev-sh/cairn#233.

**D6 — `cairn remote reject` takes an optional step.** Two forms:

- `cairn remote reject <pipeline_id> <step> --reason TEXT [--approver NAME]` rejects a pipeline
  gate through D4;
- `cairn remote reject <run_id>` is unchanged.

A single argument that is a pipeline id (`pl_…`, `src/cairn/core/storage/pipeline_store.py:40-42`)
is refused with "name the gate step". `--reason` is required when a step is named. `cairn remote
findings` already tells run ids and pipeline ids apart the same way (`remote.py:1267`).
`cairn-client` gains `reject_step` beside `approve_step`, in its sync and async clients. A 404, 403
or 409 is reported as such. This adds no new top-level name.

## Consequences

- An auditor or a CI job can check the audit trail with one command and a stable exit code, on a
  public install with no lineage writer.
- A drift between the writer's hash rule and a reader's copy can no longer happen.
- A rejected pipeline gate names who rejected it and why, on the record, the review and the
  lifecycle event. A cancel stays the operator's abort.
- `console/openapi.json` gains one route. The platform regenerates it when it bumps its core pin,
  and the console can offer a Reject button through it.
- Docs: rows in `docs/site/.../reference/cli.md` (`audit verify`; the two `remote reject` forms,
  correcting `:128`), a "verify the audit trail" section in the governance docs, and approve vs.
  reject in `concepts/gates.md`.

## Alternatives rejected

- **`cairn lineage --verify`.** No new verb, but it ties a general check to a reader of
  platform-written records.
- **`cairn remote audit verify` over a new route.** It is for a log that lives on the server's
  disk. That is a separate, later decision.
- **A `decision` field on `/approve`.** One route, but an "approve" call that rejects reads wrong
  in the API and the audit, and it changes an existing body.
- **`cairn remote advance --reject`, or a new `reject-step` sub-command.** Either works. Both
  split approve-vs-reject across two names, and `reject` is the verb people already look for.
- **Keep cancel as the only "no", and record its actor.** Smaller, but a reject and an abort are
  different governance acts, and the record could not tell them apart.

## Migration

No data migration. The audit log format is unchanged. The run-level `cairn remote reject <run_id>`
behaves as before. The platform's console and client pick up the new route after the core release.

## Test plan

- `tests/core`: `chain_hash` matches the hashes of rows that `audit/store` wrote. `lineage` and
  `audit verify` import the shared helper. A grep test finds no other `_canonical` copy in
  `src/cairn`.
- `tests/cli`: `audit verify` reports OK and exits 0 on a log that `audit/store` wrote. A log with
  row N edited reports row N and exits 2. A corrupt line exits 2. A missing log exits 1. `--json`
  has the keys above.
- Route: a reject by an allowed approver fails the pipeline with the reason, records the decider
  from the token (the body's `approver` is ignored under auth), and decides the review as
  `rejected`. A disallowed caller gets 403. A step not at `awaiting_approval` gets 409. Another
  workspace's pipeline gets 404. The finalizers run.
- Temporal: the `reject_step` signal ends the workflow as failed, with the step's verdict
  `rejected`.
- CLI: both `remote reject` forms run against the test server. A single `pl_…` argument is
  refused. 404 and 403 are reported.
- The client contract test (`tests/core/test_cairn_client_contract.py`) covers `reject_step`.

## Open questions for the maintainer

1. A new `audit` group (recommended), or `cairn lineage --verify`?
2. An empty audit log: exit 0 with zero rows (recommended), or 1?
3. A reject ends the pipeline as `failed` with a `rejected` verdict (recommended), or as a new
   `rejected` pipeline status (a public contract change)?
4. `cairn remote reject` overloaded by arity (recommended), or a separate sub-command?
5. Is `--reason` required (recommended), or optional with an empty rationale?
6. Does a new sub-command of an existing group need an ADR (as cairndev-sh/cairn#195 assumes), or
   only a CLI reference row, with ADRs kept for new top-level names?
