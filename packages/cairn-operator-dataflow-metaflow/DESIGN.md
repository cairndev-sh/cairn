# cairn-operator-dataflow-metaflow — design

- **Distribution:** `cairn-operator-dataflow-metaflow` (`cairn/operators/optional/dataflow_metaflow/`),
  offered by `cairndev[metaflow]`
- **Capabilities provided:** none as operators — it registers the `metaflow` dataflow engine
  (`cairn.dataflow_engines`), which `dataflow.run` and `ml.run` submit to when selected
- **Status:** proven at unit level (the conformance suite, cross-box, secrets, gang scheduling);
  not in the hello-cairn feature proof
- **Decisions:** ADR-0083 / ADR-0089 (Metaflow as substrate), ADR-0088 (checkpoints + resume),
  ADR-0134 D2 (an optional wheel, #2678)

## 1. Purpose

Run a cairn flow — a stage list, either kind — on Metaflow's substrate: Kubernetes or AWS Batch
compute, a shared S3 datastore, a metadata service that makes a run readable and resumable
from another box, `@retry` and `resume`. It is the scale rung above the builtin engine
(in-process or one SSH box). It does NOT add operators, govern anything itself or register
artifacts: the `dataflow/run` / `ml/run` operators (cairn-operator-dataflow) gate, budget and
register, and core decides which engine an environment selects. Until ADR-0134 it lived inside
cairn-operator-dataflow; it moved out so a default `cairndev` install carries only what the
feature proof covers.

## 2. Architecture

```
dataflow/run | ml/run  (cairn-operator-dataflow; governance, budget, artifacts)
        │ resolve_engine(kind)  — core: cairn.core.dataflow.engine_registry
        ▼
metaflow_engine(kind, target)        factory.py   reads the metaflow_* backend keys
        ▼
MetaflowEngine.submit/poll/result    engines_metaflow.py
        │ spawns  python -m operator_dataflow_metaflow.metaflow_bridge run …
        │         (metaflow_train_bridge for a gang, resources.workers > 1)
        ▼
CairnFlow (Metaflow FlowSpec)        metaflow_bridge.py — ONE static flow interpreting any
        │                            spec; the step calls cairn.core.dataflow.runner.run_dataflow
        ▼
Metaflow local | k8s | Batch compute, S3 datastore, metadata service
```

- **One adapter, both kinds.** The engine is kind-blind; the environment maps `ml_engine` and
  `dataflow_engine` to it independently (ADR-0082 D7).
- **One static flow.** The bridge is never code-generated (ADR-0082 D1): it reads the spec from
  `CAIRN_DATAFLOW_SPEC` and runs the core kernel runner, so stage contracts, budgets and
  checkpoints are core's. `cairn pack export --to metaflow` is the separate, one-way projection.
- **Shared helpers are core's** (`cairn.core.dataflow.resources`, ADR-0134 D3b): the resource
  envelope (`aggregate_resources`), the gang size (`gang_workers`), and the environment
  allowlist this engine extends with its own `METAFLOW_` / `CAIRN_METAFLOW_` / `OBS_METAFLOW_`
  prefixes.
- **Re-attach and cancel** recognise the bridge process by its cmdline module path. Jobs
  launched before ADR-0134 run `operator_dataflow.metaflow_bridge`; both paths are recognised
  for one release (`_BRIDGE_MODULES`).
- **Imports:** `cairn.core.*` only (the operator contract, `cairn.core.dataflow`); never another
  operator pack. `metaflow` is imported lazily, so the wheel registers without it.

## 3. Operators

None. The pack registers one `cairn.dataflow_engines` entry, `metaflow` →
`operator_dataflow_metaflow.factory:metaflow_engine`. The operators that reach it are
`dataflow/run` and `ml/run` in cairn-operator-dataflow.

## 4. Configuration

| Key | Kind | Scope | Default | Meaning |
|---|---|---|---|---|
| `ml_engine` / `dataflow_engine` = `metaflow` | backend field | deployment | `builtin` | select this engine for a flow kind |
| `metaflow_metadata_root` (`CAIRN_METAFLOW_ROOT`) | backend field | deployment | the kind's target | where Metaflow's local metadata lives |
| `metaflow_datastore`, `metaflow_datastore_root` | backend fields | deployment | local | the datastore and its root (S3 for cross-box) |
| `metaflow_compute`, `metaflow_compute_image`, `metaflow_compute_queue` | backend fields | deployment | `local` | the compute substrate (local / kubernetes / batch), its image and queue |
| `metaflow_retry_times` | backend field | deployment | 0 | `@retry` count; a non-integer is refused at `validate()` |
| `metaflow_secrets` | backend field | deployment | none | the connections a run may receive (`CAIRN_CONN_*`), and nothing else |
| `metaflow_env_isolation` | backend field | deployment | `auto` | how much of the control plane's environment a task inherits |
| `metaflow_metadata_service` | backend field | deployment | none | the metadata service URL that removes box affinity (`cairn setup --check` checks it is reachable) |

The backend fields are read through `cairn.core.operator.selected_backend` (profile → env →
default); a worker box receives them as the env vars named in `factory._SELECTION_ENV`.

## 5. Governance

The engine is reached only through `dataflow/run` / `ml/run`, so every flow leg passes
`enforce_flow_gate` on the flow's aggregate stage risk and the flow-engine door
(`cairn.core.dataflow.engine_door`, ADR-0134 D3c) before any record exists: an environment
that selects `metaflow` without this wheel installed is refused there, naming
`cairndev[metaflow]`. Budgets are the kernel runner's (`BudgetExceeded`). The environment
allowlist withholds every credential a run did not declare.

## 6. Failure modes

| Failure | Cause | Effect | Recovered? |
|---|---|---|---|
| Engine not installed | `ml_engine = metaflow` without `cairndev[metaflow]` | refused at the launch door | n/a — install the extra |
| `metaflow` library absent | the wheel installed without its `[metaflow]` extra | `validate()`/submit names the extra | n/a |
| Run invisible to a later process (#1364) | metadata under a per-job temp dir | fixed: pathspec on the handle, metadata root | yes |
| Bridge died, no terminal marker | process killed | re-attach reports failed, retriable | yes — `resume` |
| Pre-upgrade job after ADR-0134 | the old module path in its cmdline | still recognised for one release | yes |

## 7. Testing

`tests/` in this pack: the conformance suite (`test_metaflow_conformance.py`, needs `metaflow`;
skipped without it), substrate/config (`test_metaflow_substrate.py`), secrets
(`test_metaflow_secrets.py`), cross-box (`test_metaflow_cross_box.py`), client polling,
remote results, gang scheduling (`test_metaflow_train_bridge.py`), and the process-identity
gates — cancel of a detached bridge and re-attach liveness — each run for the current AND the
pre-upgrade bridge module path. Run with the worktree on `PYTHONPATH` or the pack installed.
Remote compute (Kubernetes/Batch) is not exercised in CI.

## 8. History

| Date | Change | Issue |
|---|---|---|
| 2026-07-27 | Metaflow adapter — one adapter, both flow kinds (ADR-0083), in cairn-operator-dataflow | #1078 → #1114 |
| 2026-08-08 | Metaflow as substrate: secrets, cross-box, gang scheduling (ADR-0089) | #1345, #1346, #1366 |
| 2026-10-16 | Its own optional wheel behind `cairndev[metaflow]` (ADR-0134 D2) | #2678 |
