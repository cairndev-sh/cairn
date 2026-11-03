"""Per-node run events for the in-process runtime (ADR-0044).

A contextvar holds an optional *node sink* — ``await sink(node_name)`` is called
as each graph node completes. The dispatcher sets it around a console-driven
template run (and its resume); everything else leaves it unset.

``adrive`` is the one execution helper the in-process runtime calls instead of
``graph.ainvoke``:

  - **no sink** → ``await graph.ainvoke(...)`` — byte-identical to the old path,
    so CLI / durable runtimes / every existing test are untouched.
  - **sink set** → drive via
    ``astream(stream_mode=["updates","values"], subgraphs=True)``, emit one
    ``node`` event per completed node (at any nesting depth), and reconstruct
    the exact value ``ainvoke`` returns — including merging the ``__interrupt__``
    chunk so the HITL pause/resume path (ADR-0034) is unaffected.

``subgraphs=True`` is load-bearing for the gated path (#183). A gated template
(e.g. ``training/finetune``) embeds a gated child template as a parent node
(``subgraph_node_fn`` ``ainvoke``s a checkpointer-less child under the parent's
config so the child's ``interrupt()`` pauses the PARENT run — #104). When that
interrupt fires, LangGraph's *parent-namespace* ``updates`` stream collapses to a
single ``{"__interrupt__": …}`` chunk — the parent node never reports "done", so
**zero** node events reach the canvas even though ~11 inner nodes ran. Streaming
with ``subgraphs=True`` surfaces those inner-namespace ``updates`` so each
completed node lights up; the terminal value is still rebuilt from the
**parent-namespace** ``values`` (``ns == ()``) so ``ainvoke`` parity holds.
"""
from __future__ import annotations

import contextlib
import logging
from collections import deque
from contextvars import ContextVar
from typing import Any, Awaitable, Callable, Iterator, Optional

logger = logging.getLogger("cairn.runtime.node_stream")

# ``await sink(node_name, output)`` — appends one per-node "done" event to the run
# log. ``output`` is the node's state-update delta (what it produced this step),
# surfaced into the run trace (#172); the sink decides how/whether to record it.
NodeSink = Callable[[str, Any], Awaitable[None]]

_SINK: ContextVar[Optional[NodeSink]] = ContextVar("cairn_node_sink", default=None)
NodeCosts = dict[str, deque[float]]
_COSTS: ContextVar[Optional[NodeCosts]] = ContextVar("cairn_node_costs", default=None)

# LangGraph surfaces an approval interrupt as an updates chunk under this key.
_INTERRUPT_KEY = "__interrupt__"


@contextlib.contextmanager
def node_sink_scope(sink: NodeSink):
    """Bind ``sink`` as the active node sink for the duration of the block.

    Restores the previous value on exit (contextvars propagate across ``await``
    and through ``asyncio.wait_for``'s context copy, so the runtime sees it)."""
    token = _SINK.set(sink)
    try:
        yield
    finally:
        _SINK.reset(token)


def current_node_sink() -> Optional[NodeSink]:
    return _SINK.get()


@contextlib.contextmanager
def node_cost_scope() -> Iterator[NodeCosts]:
    """Collect completed node costs for the matching streamed node events."""
    costs: NodeCosts = {}
    token = _COSTS.set(costs)
    try:
        yield costs
    finally:
        _COSTS.reset(token)


def record_node_cost(node_name: str, cost_usd: float) -> None:
    """Queue a node's measured cost for its event emitted by adrive."""
    costs = _COSTS.get()
    if costs is not None:
        costs.setdefault(node_name, deque()).append(cost_usd)


def take_node_cost(node_name: str) -> float | None:
    """Return the next cost for a completed node event, when one was measured."""
    costs = _COSTS.get()
    pending = costs.get(node_name) if costs is not None else None
    return pending.popleft() if pending else None


async def adrive(graph: Any, input_or_command: Any, config: Optional[dict[str, Any]]) -> Any:
    """Run ``graph`` to its next stop, returning what ``ainvoke`` would.

    Fast path (no sink): a plain ``ainvoke``. Streaming path (sink set): consume
    ``astream`` in mixed mode with ``subgraphs=True``, emit a ``node`` event per
    completed node (at any nesting depth), and rebuild the terminal value
    (parent-namespace final values + any ``__interrupt__``).

    With ``subgraphs=True`` each item is a 3-tuple ``(namespace, mode, chunk)``:
    ``namespace == ()`` is the parent graph; a non-empty namespace is a node
    inside an embedded child (a gated subgraph_node, #104). We emit node events
    for **every** namespace (so nested nodes light the canvas — the #183 fix) but
    rebuild the return value from the **parent** ``values`` only, since the
    parent-namespace terminal values + ``__interrupt__`` are exactly what
    ``ainvoke`` returns."""
    sink = current_node_sink()
    if sink is None:
        # byte-identical to the historical call (config=None for ungated graphs).
        return await graph.ainvoke(input_or_command) if config is None else await graph.ainvoke(input_or_command, config)

    final_values: Any = None
    interrupt: Any = None
    # subgraphs=True surfaces inner-namespace updates so a node nested inside an
    # embedded gated child still reports "done" (#183); without it, a child
    # interrupt collapses the parent stream to just __interrupt__ → 0 node events.
    stream_kwargs = {"stream_mode": ["updates", "values"], "subgraphs": True}
    stream = graph.astream(input_or_command, **stream_kwargs) if config is None \
        else graph.astream(input_or_command, config, **stream_kwargs)
    async for namespace, mode, chunk in stream:
        if mode == "values":
            # Only the PARENT graph's values are ainvoke's return; a child
            # namespace yields the child's own substate, which we must ignore.
            if not namespace:
                final_values = chunk  # last parent values == ainvoke's return
        elif mode == "updates" and isinstance(chunk, dict):
            for node_name, delta in chunk.items():
                if node_name == _INTERRUPT_KEY:
                    interrupt = delta  # surfaces in both child + parent ns; last wins
                    continue
                try:
                    await sink(node_name, delta)  # delta = the node's output this step (#172)
                except Exception as e:  # noqa: BLE001 — streaming never breaks a run
                    logger.debug("node sink raised on %s; ignoring: %s", node_name, e)

    result = final_values if isinstance(final_values, dict) else (final_values or {})
    if interrupt is not None and isinstance(result, dict):
        # ainvoke returns the values dict WITH __interrupt__ when paused; rebuild it
        # so the dispatcher's `"__interrupt__" in output` HITL check still fires.
        result = {**result, _INTERRUPT_KEY: interrupt}
    return result
