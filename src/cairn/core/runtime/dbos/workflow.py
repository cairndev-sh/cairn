"""The DBOS run-workflow — durable counterpart of ``InProcessRuntime.invoke``.

One workflow handles every template. The submitter passes
``(template_name, template_sha, initial_state, run_id)``; the workflow looks up
the compiled graph in the process-local registry and runs it.

Division of labour (ADR-0035, #1850):
  * the LangGraph **checkpointer** owns resume from the last super-step —
    ``ainvoke(None, …)`` on recovery resumes; ``init`` on the first run. EVERY
    graph gets one, keyed on the run id — not only approval-gated graphs;
  * **DBOS** owns liveness (``launch()`` re-runs PENDING workflows) + HITL via
    ``recv``/``send`` + the run-id idempotency key.

The graph nodes are deliberately NOT wrapped as ``@DBOS.step`` — that would
fight the checkpointer's node-skipping on resume (the two recovery models can't
both own "skip done work"). The mid-node-crash window is covered by operator
idempotency instead.
"""

from __future__ import annotations

import asyncio
import logging
from functools import wraps
from typing import Any

from cairn.core.runtime.dbos import require

logger = logging.getLogger("cairn.runtime.dbos.workflow")

require()
from dbos import DBOS  # noqa: E402

# Topic the HITL decision is delivered on (DBOSRuntime.resume → DBOS.send).
APPROVE_TOPIC = "approve"


def _wrap_nodes_for_costs(graph: Any) -> None:
    """Capture async node spend into the stream's per-node cost collector."""
    from cairn.core import obs
    from cairn.core.runtime.node_stream import record_node_cost

    def wrap(orig: Any, node_name: str) -> Any:
        @wraps(orig)
        async def _costed(*args: Any, **kwargs: Any) -> Any:
            with obs.cost_scope(f"dbos-node-{node_name}") as cost_ctx:
                output = await orig(*args, **kwargs)
            record_node_cost(node_name, cost_ctx.cost_usd)
            return output

        _costed._cairn_cost_wrapper = True
        return _costed

    for node_name, node in graph.builder.nodes.items():
        runnable = getattr(node, "runnable", node)
        orig = getattr(runnable, "afunc", None)
        if (
            orig is not None
            and asyncio.iscoroutinefunction(orig)
            and not getattr(orig, "_cairn_cost_wrapper", False)
        ):
            runnable.afunc = wrap(orig, node_name)


@DBOS.workflow(name="cairn.run_template")
async def run_template(
    template_name: str,
    template_sha: str,
    initial_state: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    from langgraph.types import Command  # noqa: PLC0415

    from cairn.core.runtime.dbos.registry import lookup, run_store  # noqa: PLC0415

    graph = lookup(template_name, template_sha)
    # #1865 — the server's run store (registered at boot); None → default_store().
    store = run_store()

    # #1850 — every graph is checkpointed, keyed on the run id. Until now only
    # approval-GATED graphs got a checkpointer (the compiler attaches one solely
    # to mark the gate), and an ungated graph ran one-shot with no thread to
    # resume from: DBOS recovered the workflow after a crash and the graph
    # restarted at zero, re-running every completed operator and re-paying
    # every LLM call. Most flows are ungated, so "durable" applied to the
    # minority. The compiler's sync saver is irrelevant either way — this
    # workflow runs ``ainvoke`` and a sync saver raises on async — so the graph
    # is always recompiled here with a SESSION-SCOPED async checkpointer
    # (an internal issue): bound to THIS loop (no cross-loop hang, #41) and held
    # only for the ainvoke segment, never across a HITL wait.
    from cairn.core.runtime.checkpointer import checkpoint_session  # noqa: PLC0415
    from cairn.core.runtime.node_record import record_node_event  # noqa: PLC0415
    from cairn.core.runtime.node_stream import (  # noqa: PLC0415
        adrive,
        node_cost_scope,
        node_sink_scope,
        take_node_cost,
    )

    cfg = {"configurable": {"thread_id": run_id}}
    _wrap_nodes_for_costs(graph)

    # #1865 — this body runs on DBOS's background loop, a different thread from
    # the dispatcher, so its contextvar node sink never reaches here: a DBOS run
    # had no per-node trace, no heartbeat, and a stuck one looked exactly like a
    # slow one. Bind the shared recorder as THIS task's sink and drive through
    # ``adrive`` (streams with subgraphs=True, so nodes inside a composed child
    # light up too) — the same path the in-process runtime takes. On crash
    # recovery the re-run nodes are recorded again, which is the honest trace.
    async def _sink(node_name: str, output: Any = None) -> None:
        await record_node_event(
            run_id,
            node_name,
            output,
            store=store,
            llm_cost_usd=take_node_cost(node_name),
        )

    # Segment 1 — initial run (or resume-from-checkpoint on DBOS crash recovery,
    # which re-runs this body from the top: if a checkpoint already exists for
    # this thread, resume from it (None) rather than re-seeding initial state).
    async with checkpoint_session() as saver:
        g = graph.builder.compile(checkpointer=saver)
        snapshot = await g.aget_state(cfg)
        resuming = getattr(snapshot, "created_at", None) is not None
        with node_cost_scope(), node_sink_scope(_sink):
            result = await adrive(g, None if resuming else initial_state, cfg)

    # HITL (ADR-0034): a paused approval interrupt durably waits for a decision
    # delivered via DBOS.send (DBOSRuntime.resume). The recv holds NO checkpointer
    # connection; the resume opens a fresh session and reads the paused state by
    # thread_id (the checkpoint was committed before segment 1 released its conn).
    while isinstance(result, dict) and "__interrupt__" in result:
        # #1847 — surface the pause BEFORE blocking on recv. DBOS pauses inside
        # the workflow, so the dispatcher never reaches its in-process branch
        # that sets awaiting_approval; the run stayed at "running", and
        # POST /runs/{id}/resume refuses anything not awaiting_approval — so a
        # gated DBOS run was unapprovable and sat until this 24h timeout. Same
        # bug Temporal fixed in #456; same write, via the shared chokepoint.
        from cairn.core.approval.surface import (  # noqa: PLC0415 — cycle: approval → storage → dispatch
            surface_awaiting_approval,
        )

        intr = result["__interrupt__"]
        items = list(intr) if isinstance(intr, (list, tuple)) else [intr]
        await surface_awaiting_approval(
            run_id, getattr(items[0], "value", None) if items else None, store=store)
        decision = await DBOS.recv_async(APPROVE_TOPIC, timeout_seconds=86400)
        async with checkpoint_session() as saver:
            g = graph.builder.compile(checkpointer=saver)
            with node_cost_scope(), node_sink_scope(_sink):
                result = await adrive(g, Command(resume=decision), cfg)
    return dict(result)


WORKFLOWS = [run_template]
