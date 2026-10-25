# cairn/core/dataflow — the data-plane contract and the engine substrate

**Purpose.** The typed boundary between the control plane and dataflow engines
(`DataflowSpec` / `DataflowResult` / `EngineJobHandle` / `EngineJobStatus`, the
`DataflowEngine` protocol — ADR-0081 D2/D3), flow definitions and the global stage namespace
(`defs.py`), engine *selection* by name (`engine_registry.py`, #1512), the conformance suite
that defines an engine (`conformance.py`), the stage-contract validation function
(`conformance_check.py`, ADR-0118 D5), and — since #2583 (ADR-0129) — the engine
**substrate** every adapter runs: the kernel runner (`runner.py` `run_dataflow`,
`BudgetExceeded`: stage-contract checks, budget ceilings, `.done` markers, checkpoints,
map fan-out, descriptors), the stage loader (`stage_loader.py` over the
`cairn.dataflow_stages` / `cairn.flow_stages` groups, plus the `noop` / `noop.slow`
conformance stages), the workdir file contract (`results.py`: `validate`, `read_metrics`,
`read_result`, `read_error`) the remote fetch contract (`fetch.py`: `fetch_list`,
`fetch_dir`, `rewrite_fetched`), and — since #2678 (ADR-0134 D3b) — the helpers remote engines
share (`resources.py`: `gang_workers`, `aggregate_resources`, `essential_env`, the engine-neutral
environment allowlist an engine extends with its own prefixes; it names no engine), and the
deterministic, recorded subsample a pack's stage takes (`fidelity.py`: `Fidelity`, `SamplePlan`,
`plan`, `keeps`, `row_hash`, `digest_rows`, `STAT_KEYS`, #2029): pure, stdlib only, shared by stages of packs that may not
import each other.

**Owns.** The contract types; which engine NAME an environment selects; what an engine must
pass; how a stage list is interpreted (one runner, ADR-0118 D7); which files a job leaves and
hands back; which rows a fidelity keeps and how that choice is recorded (`fidelity.py`). **Does not own.** Engine *construction* and the engines themselves
(`cairn-operator-dataflow`: `BuiltinLocalEngine`, `BuiltinSshEngine`, the metaflow /
skypilot adapters, their bridges, the `cairn-dataflow-run` entrypoint, the
`cairn.dataflow_engines` factories — #1512 keeps construction where the configuration is);
the kernels (any pack's `cairn.flow_stages` entry points); leg execution (`dispatch/`).

**Contracts.** `DataflowSpec` / `DataflowResult` (`docs/contracts/execution.md`, scalars and
descriptors only, never rows — ADR-0081 D3); `run_dataflow(spec, workdir, *, on_progress)`;
`load_stages() -> {name: kernel}`; the workdir files `metrics.json` / `result.json` and
`BOOKKEEPING_OUTPUTS`. The runner is line for line what the pack shipped before #2583, apart
from the import source and the logger name; a change to it is a change to every engine.

**Allowed deps.** Stdlib, pydantic, `jsonschema`, `cairn.core.operator.batch` (the map pool,
lazy), `cairn.core.backend_selection` (lazy), `cairn.core.obs` (lazy, the runner's per-stage
LLM metering scope only — #1441, maintainer-approved 2026-10-15). **Forbidden.** Any operator pack — by import
(`test_engine_registry.py::test_core_never_imports_an_operator_pack`) and by name as a string
(`test_public_core_boundary.py`); core reads the entry-point GROUPS and never the pack that
fills them. No SSH / cloud SDK: `fetch.py` names what crosses a transport, the adapter owns
the transport. LangGraph never loads on a data leg
(`test_leg_executor.py::test_a_data_leg_never_imports_langgraph`).

**Verify.** `tests/core/test_dataflow_substrate.py` (the substrate imports with no
`operator_*` module loaded; the pack's re-exports are the core objects),
`tests/core/test_dataflow_*.py`, `test_engine_registry.py`, `test_checkpoint_relay_resume.py`,
and the builtin dataflow pack's suite (`cairn/operators/builtins/dataflow/tests/`, which
exercises the runner, the conformance suite and the adapters against these modules).

**Read.** `docs/architecture/07-runtime.md`, `docs/contracts/execution.md`, ADR-0081,
ADR-0082 D7, ADR-0088, ADR-0090, ADR-0118, ADR-0129.
