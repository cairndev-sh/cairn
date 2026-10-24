"""DBOS runtime scenarios, run as a SUBPROCESS by test_dbos_runtime.py.

NOT a test module (leading underscore, no ``test_`` prefix → pytest won't
collect it). Each scenario runs in its own process because DBOS is a process
singleton (one ``DBOS()`` + ``launch()`` per process) and its async checkpointer
binds to one event loop — subprocess-per-scenario is the clean isolation model,
and the crash scenario genuinely needs two separate processes.

Usage: ``python _dbos_scenarios.py <scenario> <workdir>`` → prints
``RESULT: PASS|FAIL`` and exits 0/1.
"""
import asyncio
import hashlib
import os
import sys
from pathlib import Path
from typing import Annotated, TypedDict


def _merge(a, b):
    return (a or []) + (b or [])


def _setup(workdir: str) -> Path:
    d = Path(workdir)
    d.mkdir(parents=True, exist_ok=True)
    # Postgres tier when OBS_DBOS_TEST_PG_DSN is set (DBOS system DB +
    # AsyncPostgresSaver checkpointer share it); SQLite otherwise.
    pg = os.environ.get("OBS_DBOS_TEST_PG_DSN")
    if pg:
        os.environ["OBS_DBOS_DSN"] = pg
        os.environ["OBS_CHECKPOINTER"] = "postgres"
        os.environ["OBS_CHECKPOINTER_DSN"] = pg
    else:
        os.environ["OBS_DBOS_DSN"] = f"sqlite:///{d / 'dbos.sqlite'}"
        os.environ["OBS_CHECKPOINTER"] = "sqlite"  # → async SqliteSaver on recompile
        os.environ["OBS_CHECKPOINTER_PATH"] = str(d / "ckpt.sqlite")
    return d


class S(TypedDict, total=False):
    x: int
    approved: bool
    final_output: str


class SP(TypedDict, total=False):
    # parallel-merge state: branches a/b/c write `seen` concurrently in one
    # superstep, so it needs a reducer (else LangGraph raises InvalidUpdateError).
    seen: Annotated[list, _merge]
    approved: bool
    final_output: str


_CMS: list = []


def _nongated():
    from langgraph.graph import END, START, StateGraph
    g = StateGraph(S)
    g.add_node("n", lambda st: {"final_output": f"ran:{st.get('x')}"})
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g.compile()


def _costed():
    from cairn.core import obs
    from langgraph.graph import END, START, StateGraph

    async def n(state: S) -> dict:
        obs.record_llm("test-model", 10, 5, 0.0, usd=0.125)
        return {"final_output": f"ran:{state.get('x')}"}

    graph = StateGraph(S)
    graph.add_node("n", n)
    graph.add_edge(START, "n")
    graph.add_edge("n", END)
    return graph.compile()


def _gated_sync(d: Path):
    # Gated graph compiled with a SYNC SqliteSaver — exactly what the compiler
    # produces; run_template recompiles it to async at runtime.
    from langgraph.checkpoint.sqlite import SqliteSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import interrupt

    cm = SqliteSaver.from_conn_string(str(d / "compiler.sqlite"))
    _CMS.append(cm)
    saver = cm.__enter__()
    saver.setup()

    def step1(state: S) -> dict:
        with open(d / "side.log", "a") as f:
            f.write("step1\n")
        return {}

    def gate(state: S) -> dict:
        dec = interrupt({"need": "approval"})
        ok = bool(dec.get("approved"))
        return {"approved": ok, "final_output": "approved" if ok else "rejected"}

    g = StateGraph(S)
    g.add_node("step1", step1)
    g.add_node("gate", gate)
    g.add_edge(START, "step1")
    g.add_edge("step1", "gate")
    g.add_edge("gate", END)
    return g.compile(checkpointer=saver)


async def non_gated(d: Path) -> bool:
    from cairn.core.runtime.dbos.runtime import DBOSRuntime
    r = await DBOSRuntime().invoke(_nongated(), {"x": 7}, run_id="ng",
                                   template_name="t", template_sha="s")
    return r.get("final_output") == "ran:7"


async def gated_resume(d: Path) -> bool:
    from cairn.core.runtime.dbos.runtime import DBOSRuntime
    rt = DBOSRuntime()
    task = asyncio.create_task(rt.invoke(_gated_sync(d), {}, run_id="gr",
                                         template_name="g", template_sha="s"))
    await asyncio.sleep(2.5)
    parked = not task.done()
    await rt.resume(run_id="gr", decision={"approved": True, "approver": "t"})
    res = await task
    return parked and res.get("final_output") == "approved"


def _gated_parallel(d: Path):
    # an internal issue regression: a gated graph with PARALLEL fan-out
    # (fan → {a,b,c} → gate). All-async nodes, like the real compiler output.
    from langgraph.checkpoint.sqlite import SqliteSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import interrupt

    cm = SqliteSaver.from_conn_string(str(d / "compiler_par.sqlite"))
    _CMS.append(cm)
    saver = cm.__enter__()
    saver.setup()

    def make(name: str):
        async def node(state: SP) -> dict:
            await asyncio.gather(asyncio.sleep(0.05), asyncio.sleep(0.05))
            return {"seen": [name]}
        return node

    async def fan(state: SP) -> dict:
        return {}

    async def gate(state: SP) -> dict:
        dec = interrupt({"need": "approval"})
        ok = bool(dec.get("approved"))
        return {"approved": ok, "final_output": "approved" if ok else "rejected"}

    g = StateGraph(SP)
    g.add_node("fan", fan)
    for n in ("a", "b", "c"):
        g.add_node(n, make(n))
        g.add_edge("fan", n)
        g.add_edge(n, "gate")
    g.add_node("gate", gate)
    g.add_edge(START, "fan")
    g.add_edge("gate", END)
    return g.compile(checkpointer=saver)


async def parallel_gated(d: Path) -> bool:
    # Submit a PARALLEL gated graph, park at the interrupt, resume → complete.
    # Proves the durable runtime handles parallel graphs (#41), and exercises the
    # session-scoped checkpointer across the HITL wait (#51).
    from cairn.core.runtime.dbos.runtime import DBOSRuntime
    rt = DBOSRuntime()
    task = asyncio.create_task(rt.invoke(_gated_parallel(d), {"seen": []}, run_id="pg",
                                         template_name="p", template_sha="s"))
    await asyncio.sleep(2.5)
    parked = not task.done()
    await rt.resume(run_id="pg", decision={"approved": True, "approver": "t"})
    res = await task
    return (parked
            and res.get("final_output") == "approved"
            and sorted(res.get("seen", [])) == ["a", "b", "c"])


async def crash_submit(d: Path) -> bool:
    # Start a gated run, let it park at recv, then HARD-EXIT while PENDING.
    from dbos import DBOS, SetWorkflowID

    from cairn.core.runtime.dbos.registry import register
    from cairn.core.runtime.dbos.runtime import ensure_launched
    from cairn.core.runtime.dbos.workflow import run_template

    register("ct", "s", _gated_sync(d))
    ensure_launched()
    with SetWorkflowID("crun"):
        DBOS.start_workflow(run_template, "ct", "s", {}, "crun")
    await asyncio.sleep(3)
    sys.stdout.flush()
    os._exit(0)  # crash while PENDING


async def crash_recover(d: Path) -> bool:
    from dbos import DBOS

    from cairn.core.runtime.dbos.registry import register
    from cairn.core.runtime.dbos.runtime import ensure_launched

    register("ct", "s", _gated_sync(d))  # what a boot-time rebuild() would do
    ensure_launched()  # launch() recovers the PENDING run; checkpointer resumes
    await asyncio.sleep(3)
    before = (d / "side.log").read_text().split()
    await DBOS.send_async("crun", {"approved": True, "approver": "t"}, topic="approve")
    res = await DBOS.get_result_async("crun")
    after = (d / "side.log").read_text().split()
    # step1 ran exactly once (not re-run on recovery) + completed approved.
    return before == ["step1"] and after == ["step1"] and res.get("final_output") == "approved"


def _ungated_slow(d: Path):
    """An UNGATED two-node graph — compiled with NO checkpointer, exactly what
    the compiler produces for a flow with no approval gate. n1 records a side
    effect; n2 is slow enough to be killed mid-node."""
    import time

    from langgraph.graph import END, START, StateGraph

    def n1(state: S) -> dict:
        with open(d / "side.log", "a") as f:
            f.write("n1\n")
        return {"x": 1}

    def n2(state: S) -> dict:
        time.sleep(8)
        with open(d / "side.log", "a") as f:
            f.write("n2\n")
        return {"final_output": "done"}

    g = StateGraph(S)
    g.add_node("n1", n1)
    g.add_node("n2", n2)
    g.add_edge(START, "n1")
    g.add_edge("n1", "n2")
    g.add_edge("n2", END)
    return g.compile()


async def ungated_crash_submit(d: Path) -> bool:
    # #1850 — start an UNGATED run, let n1 complete (and checkpoint), then
    # hard-exit while n2 is mid-sleep.
    from dbos import DBOS, SetWorkflowID

    from cairn.core.runtime.dbos.registry import register
    from cairn.core.runtime.dbos.runtime import ensure_launched
    from cairn.core.runtime.dbos.workflow import run_template

    register("ut", "s", _ungated_slow(d))
    ensure_launched()
    with SetWorkflowID("urun"):
        DBOS.start_workflow(run_template, "ut", "s", {}, "urun")
    await asyncio.sleep(3)
    sys.stdout.flush()
    os._exit(0)


async def ungated_crash_recover(d: Path) -> bool:
    # #1850 — recovery must RESUME at n2, not restart at n1. Before the fix an
    # ungated graph had no checkpointer, so recovery re-ran n1: side.log read
    # ["n1", "n1", "n2"] — the doubled-LLM-bill failure, in miniature.
    from dbos import DBOS

    from cairn.core.runtime.dbos.registry import register
    from cairn.core.runtime.dbos.runtime import ensure_launched

    register("ut", "s", _ungated_slow(d))
    ensure_launched()
    res = await DBOS.get_result_async("urun")
    log = (d / "side.log").read_text().split()
    return log == ["n1", "n2"] and res.get("final_output") == "done"


def _composed_pack(d: Path) -> Path:
    """A parent template whose ``subgraph_node`` composes an UNGATED child — the
    #585/#685 fixture shape from test_subgraph_findings_output.py, inlined so
    this subprocess needs no pytest. Returns the parent template path."""
    from pydantic import BaseModel

    from cairn.core.engine import skill_engine
    from cairn.core.operator import Operation, OperationContext, default_registry, register_operator

    class _In(BaseModel):
        pass

    class _VOut(BaseModel):
        verdict: str = "warn"

    if not default_registry.has("test/findings_verdict_op"):
        @register_operator(name="test/findings_verdict_op", version="0.1.0", capabilities=["test.fv"])
        class _VerdictOp(Operation):
            input_schema = _In
            output_schema = _VOut

            async def run(self, inp, ctx: OperationContext) -> _VOut:
                return _VOut(verdict="warn")

    (d / "templates").mkdir(exist_ok=True)
    (d / "operations").mkdir(exist_ok=True)
    (d / "operations" / "verdict-op.yaml").write_text(
        "operator: test/findings_verdict_op\ninput: {}\noutput:\n  verdict: $state.findings.verdict\n"
    )
    (d / "templates" / "childverdict.md").write_text(
        "---\nname: child-verdict\nversion: 0.1.0\nflow:\n  type: state_machine\n"
        "  source: behavior_tree\n  root: { type: operation, ref: verdict-op }\n  nodes: {}\n"
        "tools:\n  required: []\n---\n# child\n"
    )
    (d / "templates" / "parentverdict.md").write_text(
        "---\nname: parent-verdict\nversion: 0.1.0\nflow:\n  type: state_machine\n"
        "  source: behavior_tree\n  root:\n    type: sequence\n    children:\n"
        "      - type: subgraph_node\n        template: childverdict\n        inputs: {}\n"
        "        outputs:\n          verdict: dataset_verdict\n  nodes: {}\n"
        "tools:\n  required: []\n---\n# parent\n"
    )
    orig = skill_engine.resolve_template_path

    def _resolve(name, *, from_pack=None):
        m = d / "templates" / f"{name}.md"
        return m if m.exists() else orig(name, from_pack=from_pack)

    skill_engine.resolve_template_path = _resolve
    return d / "templates" / "parentverdict.md"


async def composed_ungated(d: Path) -> bool:
    """#1864 — a composed parent with an UNGATED child, dispatched exactly as
    prod does it: ``OBS_RUNTIME=dbos`` → ``parent.run()`` → DBOSRuntime. Before
    the fix the subgraph node re-dispatched the child with the PARENT's run id,
    DBOS deduplicated it to the running parent, and the node awaited its own
    workflow — PENDING forever. A 30s deadline turns that hang into a FAIL."""
    os.environ.pop("OBS_PROFILE", None)      # a profile could pin the runtime (#1852)
    os.environ["OBS_RUNTIME"] = "dbos"
    os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")
    from cairn.core.engine.compiler import compile_template

    parent = compile_template(_composed_pack(d))
    try:
        r = await asyncio.wait_for(parent.run({"run_id": "comp-1864"}), timeout=30)
    except asyncio.TimeoutError:
        print("composed run hung for 30s — the #1864 self-deadlock")
        return False
    except Exception as e:  # noqa: BLE001 — report, don't crash the harness
        print(f"composed run raised {type(e).__name__}: {e}")
        return False
    print("composed run result dataset_verdict =", r.get("dataset_verdict"))
    return r.get("dataset_verdict") == "warn"


async def node_events(d: Path) -> bool:
    """#1865 — a DBOS run records one ``node`` event per completed graph node
    and stamps ``last_heartbeat_at``. Before the fix the workflow ran
    ``ainvoke`` on DBOS's background loop with no sink: zero node events."""
    from cairn.core.runtime.dbos.registry import set_run_store
    from cairn.core.runtime.dbos.runtime import DBOSRuntime
    from cairn.core.storage import default_store
    from cairn.core.storage.protocols import Run, TriggerEvent

    store = default_store()
    set_run_store(store)   # what the server does at boot: the workflow writes to OUR store
    await store.create(Run(id="ne", trigger=TriggerEvent(source="manual", payload={}), status="running"))
    r = await DBOSRuntime().invoke(_nongated(), {"x": 3}, run_id="ne", template_name="t", template_sha="s")
    events = await store.get_events("ne")
    nodes = [e.data.get("node") for e in events if e.type == "node"]
    beat = (await store.get("ne")).last_heartbeat_at
    print("result", r.get("final_output"), "node events", nodes, "heartbeat", bool(beat))
    return r.get("final_output") == "ran:3" and nodes == ["n"] and bool(beat)


async def node_events_cost(d: Path) -> bool:
    from cairn.core.runtime.dbos.registry import set_run_store
    from cairn.core.runtime.dbos.runtime import DBOSRuntime
    from cairn.core.storage import default_store
    from cairn.core.storage.protocols import Run, TriggerEvent
    store = default_store()
    set_run_store(store)
    await store.create(
        Run(id="nec", trigger=TriggerEvent(source="manual", payload={}), status="running")
    )
    result = await DBOSRuntime().invoke(
        _costed(),
        {"x": 3},
        run_id="nec",
        template_name="cost",
        template_sha="s",
    )
    events = await store.get_events("nec")
    node_events = [event.data for event in events if event.type == "node"]
    cost = node_events[0].get("llm_cost_usd") if node_events else None
    print("result", result.get("final_output"), "node cost", cost)
    return result.get("final_output") == "ran:3" and cost == 0.125


async def graceful_cancel(d: Path) -> bool:
    """#1872 — what a graceful shutdown does: uvicorn's ``asyncio.run()`` cancels
    every task left on the loop. With the sync ``start_workflow`` the workflow
    coroutine is driven from a thread-pool worker blocked on ``.result()``; the
    cancellation surfaces there as ``concurrent.futures.CancelledError`` — an
    Exception — and DBOS persists the workflow as ERROR: nothing to recover
    (prod lost a parked run and an in-flight run on one ``docker restart``).
    With ``start_workflow_async`` the workflow is a native task that receives
    ``asyncio.CancelledError`` (a BaseException DBOS does not persist) and the
    row stays PENDING for ``launch()`` to recover."""
    from dbos import DBOS

    from cairn.core.runtime.dbos.runtime import DBOSRuntime

    rt = DBOSRuntime()
    asyncio.create_task(rt.invoke(_gated_sync(d), {}, run_id="gc", template_name="g", template_sha="s"))
    await asyncio.sleep(2.5)                      # parked at the gate (recv)
    before = (await DBOS.get_workflow_status_async("gc")).status
    me = asyncio.current_task()
    for t in asyncio.all_tasks():
        if t is not me:
            t.cancel()
    await asyncio.sleep(2.0)                      # a thread-pool worker (old path) gets to persist its verdict
    after = (await DBOS.get_workflow_status_async("gc")).status
    print("parked:", before, "| after cancelling every task on the loop:", after)
    return before == "PENDING" and after == "PENDING"


#: The agentic template the rebuild scenario owns (#2536). The public tree ships
#: no agentic template, so "rebuild() found at least one installed" failed there
#: and proved nothing about the path; this one is always present.
_REBUILD_FIXTURE = """---
name: dbos-rebuild-fixture
version: 0.0.1
flow:
  type: state_machine
  root: { type: action, ref: answer }
  nodes:
    answer:
      type: prompt
      model: small
      capabilities: []
models:
  small: gpt-4o-mini
tools:
  required: []
---

# dbos rebuild fixture
"""


async def registry_rebuild(d: Path) -> bool:
    """Boot-time repopulation: recompile installed templates into the registry.
    A dev pack (``CAIRN_PACK_PATH``) installs the fixture template, so the
    rebuild path runs on every tree; where packs ship agentic templates too, at
    least one of THOSE must also rebuild, as before."""
    os.environ.setdefault("OBS_BACKEND", "mock")
    from cairn.core.pack import all_template_dirs
    from cairn.core.pack.loader import DEV_PACK_PATH_ENV
    from cairn.core.runtime.dbos.registry import _GRAPHS, rebuild

    pack = d / "dbosrebuildfixture"
    (pack / "flows").mkdir(parents=True, exist_ok=True)
    flow = pack / "flows" / "dbos-rebuild-fixture.md"
    flow.write_text(_REBUILD_FIXTURE)
    (pack / "pack.toml").write_text(
        '[pack]\nname = "dbosrebuildfixture"\nversion = "0.1.0"\n'
        'description = "test fixture"\n\n[assets]\nflows = "flows"\n')
    (pack / "__init__.py").write_text(
        "from cairn.core.pack import Pack\n\npack = Pack.from_manifest(__file__)\n")
    os.environ[DEV_PACK_PATH_ENV] = str(pack)

    n = rebuild()
    sha = hashlib.sha256(flow.read_bytes()).hexdigest()[:16]
    fixture_ok = ("dbos-rebuild-fixture", sha) in _GRAPHS
    shipped = [m for t in all_template_dirs() if Path(t) != pack / "flows"
               for m in Path(t).glob("*.md")]
    print("rebuilt:", n, "| fixture registered:", fixture_ok, "| shipped templates:", len(shipped))
    return fixture_ok and n >= 1 + (1 if shipped else 0)


SCENARIOS = {
    "ungated_crash_submit": ungated_crash_submit,
    "ungated_crash_recover": ungated_crash_recover,
    "non_gated": non_gated,
    "gated_resume": gated_resume,
    "parallel_gated": parallel_gated,
    "crash_submit": crash_submit,
    "crash_recover": crash_recover,
    "rebuild": registry_rebuild,
    "composed_ungated": composed_ungated,
    "node_events": node_events,
    "node_events_cost": node_events_cost,
    "graceful_cancel": graceful_cancel,
}

if __name__ == "__main__":
    scenario, workdir = sys.argv[1], sys.argv[2]
    d = _setup(workdir)
    ok = asyncio.run(SCENARIOS[scenario](d))
    print("RESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
