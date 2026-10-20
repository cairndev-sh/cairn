# Testing strategy — the layers and what each one proves

Run: `.venv/bin/pytest tests/core -q --no-header` (never `uv run pytest`); operator-pack
tests live inside the pack (`cairn/operators/<tier>/<pack>/tests/`) and are collected by
`testpaths`. Discipline (`tests/CLAUDE.md`): never inherit a failure baseline; a gate must
be able to fail; hermetic by default.

**The one rule that decides whether a test is evidence.** A mock or stub may replace
*expensive compute* or an *external service*. It may never replace the **contract or
handoff the test claims to prove**. If the claim is `dataset producer → DatasetVersion →
training consumer`, the test may stub the GPU; it may not fabricate the `DatasetVersion`
by hand and call the producer/consumer handoff proven. `tests/core/test_training_core_golden_path.py`
is the reference: readiness really exports, the train leg really reads that export
(`test_C4_…` pins the export's content hash on the train run), only the trainer is a stub.

| Layer | Purpose — proves | Does NOT prove | Required when | Cairn example |
|---|---|---|---|---|
| **UNIT** | one function/kernel/operator behaves per its own spec | that callers use it correctly; any handoff | every change | `operator_dataset_checks/tests/test_ingest_registered_ref.py`, `tests/core/test_check_evidence.py` (map semantics) |
| **CONTRACT** | a shared shape or promise holds: schema, identity, verdict, report, OpenAPI | that both sides of a boundary agree at runtime | any contract family touched (`docs/contracts/README.md`) | `scripts/dump_openapi.py --check` + oasdiff (API); `test_check_evidence.py::test_a_version_registered_by_a_leg_carries_the_map_in_its_report` (real store round-trip); `tests/core/test_contract_docs.py` (docs ↔ code) |
| **BOUNDARY** | dependency direction and ownership are as the constitution says | behaviour | any new import, entry point, cross-package call | `tests/core/test_architecture_boundaries.py` (AST rules + frozen debt), `test_engine_registry.py::test_core_never_imports_an_operator_pack`, `test_cairn_client_contract.py`, `test_leg_executor.py::test_a_data_leg_never_imports_langgraph` |
| **INTEGRATION** | two real components cooperate across a real boundary (door → bridge → engine; store → reader) | the whole path end to end | any change to a door, the bridge, the leg executor, an engine, a store | dataflow conformance (`src/cairn/core/dataflow/conformance.py`, run per engine in the dataflow pack); `test_leg_executor.py` (execute_leg with a stub engine — the annotation/registration path is real) |
| **COMPOSITION** | leg A's real output is what leg B consumes; declared wires resolve | correctness of A or B alone | any change to produces/consumes, wires, `$steps`, artifact registration | harness `test_C4_the_train_run_pins_the_admitted_spec` (train pins readiness' export by content); `test_pipeline_e2e_seams.py` |
| **FAILURE / RECOVERY** | refusals, retries, re-attach, cancellation, partial state behave as promised | the happy path | any change to retry/adopt/cancel/door logic; every bug fix (the regression test) | `test_leg_executor.py` (retry/adopt), `test_a_wrong_fragment_still_refuses_a_file_uri`, `test_a_resumed_stage_reports_the_version_that_produced_it` |
| **END-TO-END** | a user-visible path through the real surface | performance, production infra | a golden path or a public surface changed | the CORE harness through `POST /start` on the builtin engine; the same defs through the Metaflow engine (`importorskip`); `tests/e2e/hello` (ADR-0127 D1): a real `cairn-server` per storage × runtime × bus cell driven through `POST /start` and the CLI, the crash cells pinning ADR-0125 — opt-in (`CAIRN_E2E=1`), run by `scripts/oss/feature_proof.sh` and the `public-e2e` job |
| **ARCHITECTURE PROOF** | a frozen invariant holds mechanically (engine-neutral defs, one door, no orphan runs) | that the invariant is the right one | any change near a frozen invariant | `test_the_defs_name_no_engine_and_the_profile_selects_it`; `test_declared_pipelines_only.py`; the strict-xfail-names-an-issue rule |

The hello-cairn proof has two runners. `scripts/oss/feature_proof.sh` runs every cell
against the light stack's `proof` profile (Postgres, sshd), so `nightly-ops` (SSH, the
env-secrets check, the run hook) runs there. The `public-e2e` CI job runs every
storage × runtime × bus cell and the crash cells against a pgvector sibling only: a key
generated in the job cannot be mounted into a CI service container, so `nightly-ops` skips in CI
with that reason. The Infisical variant reads NOT PROVED in both: neither starts an
Infisical server or seeds a machine identity (`scripts/oss/proof_seed.py` says how). The run
hook (`hooks`) is proved on `profile-and-publish`, so it needs the `hello` extra (the
installed wheel declares the listener), which `public-e2e` installs. A receipt counts only when the case that recorded it passed in
that run's junit. A SQLite-DBOS cell needs a Python whose SQLite is >= 3.42 (#2655); both
runners use a uv-managed Python.

Conventions: a strict `xfail` must name its issue (`reason="#NNNN …"`, enforced); a skip
must say what is missing (an optional extra, a service); paid/remote/GPU runs are never
started by tests. Bug fixes land with the regression test's RED output in the PR.


## Qualification beyond functional correctness

Apply only the rows relevant to the changed behavior; explain N/A. These are evidence
requirements, not a claim that all checks already exist. The implementation plan names
existing test symbols, missing proofs and their owner. A missing required proof blocks
readiness. Documentation-only changes do not require load or GPU tests.

| Concern | Required evidence when affected |
|---|---|
| Scale | Workload and peak concurrency, tenant count, dataset/event volume, resource assumptions, bounded memory/batches/queues, pagination completeness, backpressure and latency/throughput criteria. Measure on named hardware and input sizes; report the tested envelope, not an unlimited scalability claim. |
| Recovery | Behavior before submission, after ambiguous external success, during partial persistence, cancellation, worker death and restart; idempotency and the owner of durable state/coordination. |
| ML data | Dataset/source identities, applicable consent, curation and label provenance, correlated-example train/eval isolation, contamination refusal, immutable split policy and replay lineage. |
| Training | Dataset/base/policy/reference/teacher/reward identities as applicable; configuration and backend versions; complete checkpoint/resume evidence; candidate identity before evaluation. |
| Evaluation and release | Candidate/champion and harness identities, target/general/safety/failure slices as applicable, evaluator failure distinct from model quality, promotion evidence, deployment identity and rollback proof. Offline tests alone do not qualify a live deployment. |
| Maintainability | Owning boundary, explicit dependencies, justified abstraction, understandable execution/failure path, compatible public contracts and current documentation. Independent review is required; line counts and complexity scores alone do not prove quality. |

For each affected invariant record: authority section → owning boundary → test/check
symbol → evidence/ref → PASS/FAIL/UNKNOWN. Reuse the existing contract and frozen-domain
identifiers. Do not create a parallel contract registry. An unimplemented test is a gap,
not evidence. GPU/live qualification remains separately authorized per launch.

The local dependency gate is `tests/core/test_architecture_boundaries.py`; it checks
static absolute and relative imports and parent-package import forms. Its synthetic
violation cases prove rejection. Computed/dynamic imports and semantic ownership still
require independent review. Allowlist changes must be compared with the review base;
the test reads the current allowlist and does not independently authorize its contents.
