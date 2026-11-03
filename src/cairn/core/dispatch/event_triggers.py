"""Bus → pipeline: fire declared ``kind: event`` triggers (ADR-0131 D4/D5).

A pipeline that declares ``triggers: [{kind: event, topic: dataset.ready}]`` owns a
row in the EXISTING trigger store (``source: event``, a ``bus`` ingress on the
topic, ``run.template`` = the pipeline). This module is the one place those rows
fire: it subscribes to every bus topic once, and for each domain message reads the
enabled event rows FRESH from the store — so pausing, resuming or removing a row
takes effect on the next message with no re-subscription — and starts each
matching row's pipeline through the one launch door (:func:`launch_ref`), in the
row's workspace, with inputs mapped from the payload by the receiver's own
``$event.`` resolver.

It runs on the leader only (``Leadership.manage`` in the server), like the
scheduler, so N replicas start a pipeline once per message. ``run.*`` is the
engine's lifecycle firehose and never starts anything (the loop guard
:mod:`cairn.core.dispatch.bus_bridge` uses too).
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from cairn.core.engine.triggers_spec import load_trigger_spec_text, map_inputs

logger = logging.getLogger("cairn.event_triggers")

_RESERVED_PREFIX = "run."


class EventTriggerBridge:
    """Start the pipeline of every enabled ``kind: event`` row whose topic was published."""

    def __init__(
        self,
        dispatcher: Any,
        *,
        bus: Any = None,
        trigger_store: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.dispatcher = dispatcher
        self._bus = bus
        self._store_factory = trigger_store
        self._sub: Any = None

    async def start(self) -> None:
        from cairn.core.bus import get_bus  # noqa: PLC0415 — the bus is selected at first use

        bus = self._bus or get_bus()
        self._sub = await bus.subscribe("*", self._on_message)
        logger.info("event triggers: listening on the bus for declared `kind: event` rows")

    def stop(self) -> None:
        if self._sub is not None:
            self._sub.unsubscribe()
            self._sub = None

    def _open_store(self) -> Any:
        if self._store_factory is not None:
            return self._store_factory()
        from cairn.core.storage.trigger_store import default_trigger_store  # noqa: PLC0415

        return default_trigger_store()

    async def _rows_for(self, topic: str) -> list[tuple[Any, Any]]:
        """``(row, spec)`` for every ENABLED event row on ``topic``, all workspaces."""
        store = self._open_store()
        try:
            rows = await store.list(None)
        finally:
            close = getattr(store, "close", None)
            if close is not None:
                close()
        out: list[tuple[Any, Any]] = []
        for row in rows:
            if getattr(row, "source", "") != "event" or not getattr(row, "enabled", False):
                continue
            try:
                spec = load_trigger_spec_text(row.yaml, name=row.id)
            except Exception as e:  # noqa: BLE001 — one bad row must not stop the rest
                logger.warning("event trigger %s: unreadable record — %s", row.id, e)
                continue
            if spec.ingress.transport == "bus" and spec.ingress.topic == topic and spec.run:
                out.append((row, spec))
        return out

    async def _on_message(self, msg: Any) -> None:
        topic = str(getattr(msg, "topic", "") or "")
        if not topic or topic.startswith(_RESERVED_PREFIX):
            return
        rows = await self._rows_for(topic)
        if not rows:
            return
        # lazy: the start door pulls the pack/contract stack (as the scheduler does)
        from cairn.core.dispatch.start import launch_ref  # noqa: PLC0415
        from cairn.core.workspace import reset_current_workspace, set_current_workspace  # noqa: PLC0415

        payload = getattr(msg, "payload", None)
        event = payload if isinstance(payload, dict) else {"value": payload}
        # #289 — who published the event goes on the started pipeline's record, so a
        # pipeline started by another pipeline's `emits:` names it (`pipeline:<id>`).
        source = {k: v for k in ("publisher", "correlation_id")
                  if (v := getattr(msg, k, None))}
        for row, spec in rows:
            inputs = map_inputs(event, dict(spec.run.input or {}))
            token = set_current_workspace(row.workspace_id)
            try:
                rec = await launch_ref(
                    spec.run.template, inputs, dispatcher=self.dispatcher,
                    workspace_id=row.workspace_id,
                    trigger={"source": "event", "topic": topic, "trigger_id": row.id, **source},
                )
                logger.info("event %s started %s (trigger %s)", topic,
                            getattr(rec, "pipeline_id", None) or getattr(rec, "id", None), row.id)
            except Exception as e:  # noqa: BLE001 — one refused start must not stop the others
                logger.warning("event %s: trigger %s could not start %s — %s",
                               topic, row.id, spec.run.template, e)
            finally:
                reset_current_workspace(token)


__all__ = ["EventTriggerBridge"]
