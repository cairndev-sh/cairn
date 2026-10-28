# cairn/core/operator — the Operation contract and the invocation chokepoint

**Purpose.** Define what an operator is (`Operation[I, O]`, `OperationContext`), discover
operators (`cairn.operators` entry points + installed `pack.toml` manifests), and invoke
them through ONE function (`runtime.py:invoke_operator`: `enforce_gate → pick_executor →
in_process | mcp_tool | remote_job`).
**Owns.** Contract, registry, loader, binding grammar (`operations/*.yaml`), executors,
remote-job store/channel/control, config layering (`config.py`, `effective.py`), manifest
sync, ADR-0103 reporting, pins, scoped secrets, the testing harness.
**Does not own.** Operator implementations (packs), approval *decisions* (`approval/`),
where operators are called from (compiler, bridge), engines/stages (`dataflow/`).

**Contracts.** `Operation` class attrs (`name`, `version`, `capabilities`, `input_schema`,
`output_schema`, `config_schema`, `risk`, `executors`, `requires_approval`, `long_running`,
`requires_gpu`, `supports_*`, `reports`); `OperationContext` (`artifacts`, `secrets`,
`cost`, `compute`, `report`, `launch` (ADR-0120 D7: the dispatch layer's handle over the launch door,
`None` outside a governed run), the store handles `run_store()` / `schedule_store()` /
`fingerprint_store()` (#2453 B: the deployment default on first use, closed by the runtime;
an injected handle wins and is never closed), `logger`; `metrics` deprecated); the contract's lazy re-exports
(`__init__.py` `_CONTRACT_REEXPORTS`, ADR-0120 D7 / #2453: connections, the artifact registry,
workspace, redaction/audit/notify types, provider settings; the compute lifecycle, ADR-0151 D6) — a pack imports only this package. `pack.toml` `[operator."x/y"]`
must match the class (`manifest_sync.py`; tests are vacuous unless the pack is installed —
`cairn/operators/CLAUDE.md` §B). Governance-scoped `config_schema` fields are environment-owned; a
run-supplied value is DROPPED (ADR-0068/0070).

**Allowed deps.** `approval` (lazy, cycle), `storage` (job store, artifacts), `obs`,
`connections`, `services.mcp` (lazy). **Forbidden.** operator packs, `cairn.packs.*`,
`engine` (except type-only), `triggers` (`jobs.py:140` is debt).

**Entry points.** `engine/compiler.py:1633` (operation node), `dispatch/pipeline_bridge.py:1294`
(task leg), stage runners (engines) for flow kernels — NOT through this package.

**Testing.** `tests/core/test_invoke_operator_core.py`, `test_operator_*.py`,
`test_approval_chokepoint.py`, `test_operator_manifest_sync.py`, `test_pack_manifest_drift.py`,
`test_installed_pack_manifests.py`, `test_operator_testing_harness.py`.

**Common mistakes.** Calling `op.run()` directly from a new call site (bypasses the gate);
adding an executor branch without `is_implemented`; a class attr without the matching
`pack.toml` field; reading `ctx.metrics` in new code; building an `OperationContext` by hand
without the guard/reporter.

**Read.** `docs/architecture/05-operators.md`, `docs/golden-paths/operator.md`, ADR-0020
§24.20.4–13, ADR-0026 §3, ADR-0034, ADR-0038, ADR-0103, `cairn/operators/CLAUDE.md`.
