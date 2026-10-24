"""Temporal activities — generic ``run_node`` plus its support types.

A single activity ``run_node`` is enough for the whole engine: every node
tagged ``LLM``/``TOOL``/``IO`` routes here, parameterized by ``(template_name,
template_sha, node_name)``. The activity rehydrates the node fn from the
worker-side :class:`TemplateRegistry` and calls it with the input state.

Why one activity instead of one per node:

  * **Determinism:** the workflow only ever calls ``execute_activity(run_node,
    ...)`` with stable args; replay sees the same call. If we minted a new
    ``@activity.defn`` per node we'd grow the worker's activity table
    proportionally to template count, with no benefit.
  * **Decoupling:** the activity definition doesn't have to know any pack
    specifics. Adding a new node type or new pack doesn't touch this file.

The retry policy per node is *not* set on this activity — Temporal lets the
caller (the LangGraph plugin) attach a per-invocation retry policy + timeout.
Those are computed by :func:`activity_metadata_for` at worker boot, one entry
per tagged node.
"""

from __future__ import annotations

import logging
from typing import Any

from cairn.core.runtime.node_events import node_event_data
from cairn.core.runtime.temporal import require

logger = logging.getLogger("cairn.runtime.temporal.activities")


# Importing temporalio inside the module body means importing this module
# requires the SDK. The temporal subpackage's __init__ doesn't import this
# file; only the worker bootstrap does.
require()
from temporalio import activity  # noqa: E402


# Worker-process-local run store, built lazily on first node event (#415). The
# Postgres store holds a connection pool, so build it once and reuse.
_STORE: Any = None


async def _record_node_event(
    node_name: str,
    delta: dict[str, Any],
    *,
    llm_cost_usd: float | None = None,
) -> None:
    """Append a per-node event to the run/event store from the WORKER (#415).

    The in-process node sink (``dispatcher._node_sink``) can't reach this
    process, so under Temporal the Pilot per-node trace was empty. The Temporal
    ``workflow_id`` IS the run_id (``TemporalRuntime`` sets ``id=run_id``), so
    the activity recovers it from ``activity.info()`` and writes straight to the
    shared store — the same ``node`` payload the in-process path emits.

    Best-effort: a store hiccup must never fail the node (its real work already
    succeeded). Note: a node's activity re-runs on Temporal RETRY (not on
    replay), so a retried node may log more than one ``node`` event — the
    duplicate reflects the retry and is harmless for the trace.
    """
    global _STORE
    try:
        run_id = activity.info().workflow_id
    except Exception:  # noqa: BLE001 — no activity context (shouldn't happen); skip
        return
    # #1865 — the write itself lives in ONE place both durable runtimes call
    # (store append + heartbeat + the #967 ``run.node`` bus publish); the
    # activity only supplies its process-local store cache.
    from cairn.core.runtime.node_record import record_node_event  # noqa: PLC0415 — keep activity import cheap

    try:
        if _STORE is None:
            from cairn.core.storage import default_store  # noqa: PLC0415
            _STORE = default_store()
    except Exception as e:  # noqa: BLE001
        logger.debug("run_node store init failed (node=%s): %s", node_name, e)
        return
    await record_node_event(
        run_id,
        node_name,
        delta,
        store=_STORE,
        llm_cost_usd=llm_cost_usd,
    )


@activity.defn(name="cairn.surface_approval")
async def surface_approval(approval: Any) -> None:
    """Surface a temporal-mode approval interrupt to cairn (#456).

    Under Temporal the graph pauses INSIDE the workflow, so ``execute_workflow``
    blocks and the dispatcher never reaches its in-process pause-handling (which
    sets ``awaiting_approval`` + captures the ApprovalRequest). Without this the
    run hangs at ``running`` forever with no way to approve.

    This activity does that surfacing from the worker — the ``workflow_id`` IS the
    run_id — so ``/runs/{id}/pending-approval`` + the console approval UI show
    WHAT is being approved, and ``POST /runs/{id}/resume`` can deliver the
    decision (the ``approve`` signal the workflow is waiting on). Mirrors the
    in-process dispatcher's pause branch exactly (status + event with ``approval``).

    Best-effort on the store write: a hiccup must not crash the workflow (the
    approval signal path still functions), but a failure is logged loudly since
    it means the run won't visibly surface for approval.
    """
    try:
        run_id = activity.info().workflow_id
    except Exception:  # noqa: BLE001 — no activity context; nothing to surface
        return
    # #1847 — the write itself now lives in ONE place both durable runtimes call.
    # DBOS had the identical pause shape and never got #456's fix, so its gated
    # runs were unapprovable; copying this body a second time is how that
    # divergence happened, so it is a shared function instead.
    from cairn.core.approval.surface import (  # noqa: PLC0415 — keep activity import cheap
        surface_awaiting_approval,
    )

    await surface_awaiting_approval(run_id, approval)


@activity.defn(name="cairn.surface_remote_job")
async def surface_remote_job(payload: Any) -> None:
    """Surface a temporal-mode REMOTE-JOB interrupt (ADR-0065).

    The run paused waiting on a long remote job the control loop drives — it is
    RUNNING, not awaiting approval. Mark it ``running`` and emit an informational
    ``remote_job`` event (NOT an ApprovalRequest, so no bogus approval card). The
    completion arrives via the same ``approve`` signal (POST /runs/{id}/resume from
    the control loop) that the workflow waits on — only the SURFACING differs from
    ``surface_approval``.
    """
    global _STORE
    try:
        run_id = activity.info().workflow_id
    except Exception:  # noqa: BLE001 — no activity context; nothing to surface
        return
    try:
        if _STORE is None:
            from cairn.core.storage import default_store  # noqa: PLC0415
            _STORE = default_store()
        await _STORE.update(run_id, status="running")
        await _STORE.append_event(
            run_id, "remote_job", payload if isinstance(payload, dict) else {},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("surface_remote_job: store write failed for %s: %s", run_id, e)


@activity.defn(name="cairn.run_node")
async def run_node(
    template_name: str,
    template_sha: str,
    node_name: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Run one StateGraph node by name and return its state delta.

    Lookup chain:
      1. :func:`registry.current` — the process-local TemplateRegistry.
      2. ``registry.get(template_name, template_sha)`` — fails fast if the
         worker is on a different template version than the submitter.
      3. ``entry.node_fns[node_name]`` — the actual closure built by the
         compiler (an action-node fn, a subgraph wrapper, a python_node, …).
    """
    from cairn.core.runtime.temporal import registry as registry_module

    entry = registry_module.current().get(template_name, template_sha)
    try:
        fn = entry.node_fns[node_name]
    except KeyError as exc:
        raise KeyError(
            f"node {node_name!r} not in template {template_name!r} ({template_sha}). "
            "Check that the node was tagged via NodeKind in the compiler."
        ) from exc

    logger.debug("run_node template=%s node=%s", template_name, node_name)
    result = fn(state)
    if hasattr(result, "__await__"):
        result = await result
    delta = result if isinstance(result, dict) else {"value": result}
    # #415: record the per-node trace event to the run store from the worker,
    # since the in-process node sink is process-local and never fires here.
    await _record_node_event(node_name, delta)
    return delta


ACTIVITIES = [run_node, surface_approval, surface_remote_job]
