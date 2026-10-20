"""Record one completed graph node for a run — the ONE write both durable
runtimes use (#1865, the #415 gap in DBOS form).

Three execution paths emit ``node`` events and they must leave identical
records:

  * **in-process** — the dispatcher's contextvar sink (``_node_sink``) via
    ``adrive`` (ADR-0044); it goes through ``Dispatcher._emit``.
  * **Temporal** — the ``run_node`` activity in the WORKER process (#415).
  * **DBOS** — ``run_template`` on DBOS's background event loop, a different
    thread from the dispatcher, so the dispatcher's contextvar sink is invisible
    there and until #1865 a DBOS run had NO per-node trace and NO heartbeat.

The Temporal activity and the DBOS workflow both call this. It does what the
activity did inline: append the size-capped ``node`` payload to the run's
event log, stamp ``last_heartbeat_at`` (a completed node IS a liveness signal —
the reconciler's H1 verdict can then see a durable run that nothing else
reports on), and publish ``run.node`` on the Bus so the server's SSE relay
streams it live (#967). Every leg is best-effort: a store or bus hiccup must
never fail the node whose real work already succeeded.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from cairn.core.runtime.node_events import node_event_data

logger = logging.getLogger("cairn.runtime.node_record")


async def record_node_event(
    run_id: str,
    node_name: str,
    delta: Any,
    *,
    store: Optional[Any] = None,
    heartbeat: bool = True,
    llm_cost_usd: float | None = None,
) -> None:
    payload = node_event_data(node_name, delta, llm_cost_usd=llm_cost_usd)
    try:
        if store is None:
            from cairn.core.storage import default_store  # noqa: PLC0415 — cycle: storage → dispatch → runtime
            store = default_store()
        await store.append_event(run_id, "node", payload)
        if heartbeat:
            await store.update(run_id, last_heartbeat_at=datetime.now(timezone.utc).isoformat())
    except Exception as e:  # noqa: BLE001 — never break the node on a store hiccup
        logger.debug("node event append failed (run=%s node=%s): %s", run_id, node_name, e)
    try:
        from cairn.core.bus import get_bus  # noqa: PLC0415 — the bus is optional, never fatal
        await get_bus().publish("run.node", dict(payload), correlation_id=run_id)
    except Exception:  # noqa: BLE001
        pass
