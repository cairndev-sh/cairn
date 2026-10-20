"""The durable side-channels of ONE ``flow:`` leg, assembled once and used by
BOTH pipeline owners (#1429, ADR-0086 H1/H2/H6, #1048 anti-deviation).

Before this module the in-process ``PipelineBridge`` hand-built these closures
inline (``_execute_and_settle_flow_leg``) and the Temporal owner
(``pipeline_activities._run_flow_leg``) built none of them — so under
``CAIRN_RUNTIME=temporal`` no engine handle was ever persisted, no heartbeat was
stamped, no leg retried and no checkpoint was relayed or resumed. Found live in
the 1M campaign (#1409): identical flow, two owners, two different durability
stories. One assembly means one story.

What a leg needs beyond ``execute_leg``'s core loop, and why:

* ``on_submit`` — persist the engine handle the moment submit returns
  (``StepState.handle`` + ``run_id``): the re-attach token a restart adopts by
  (``plan_resume`` classifies a running step WITHOUT one as orphaned).
* ``heartbeat`` — ``last_heartbeat_at`` on the run (the H1 reconciler's
  liveness signal) + best-effort checkpoint relay OFF the working box (H6b).
* ``on_retry`` / ``on_resume`` — the visibility invariant: every retry and every
  checkpoint resume is a run event + pipeline audit, never silent.
* ``resume_provider`` — newest durable checkpoint of this exact spec (H6c).
* ``persisted_handle()`` — what a restarted owner adopts instead of resubmitting.

The class holds only what both owners can supply: stores, an emit callable, and
ids. Nothing here imports an owner.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from cairn.core._time import utc_now_iso

logger = logging.getLogger("cairn.dispatch.leg_hooks")

EmitFn = Callable[[str, str, dict], Awaitable[None]]      # (run_id, event, data)


async def publish_pipeline_audit(pipeline_id: str, step_id: str, event: str,
                                 detail: dict, *, bus: Any = None) -> None:
    """Best-effort ``pipeline.<event>`` on the bus — the live audit signal both
    owners emit (the durable trail is the PipelineRecord itself)."""
    try:
        if bus is None:
            from cairn.core.bus import get_bus  # noqa: PLC0415 — cycle: bus → storage → dispatch
            bus = get_bus()
        await bus.publish(
            f"pipeline.{event}",
            {"pipeline_id": pipeline_id, "step_id": step_id, **detail},
            correlation_id=pipeline_id,
        )
    except Exception:  # noqa: BLE001 — audit must never break the transition
        logger.debug("pipeline audit publish failed for %s/%s", pipeline_id, event)


def flow_leg_record(step_output: Any, outcome: Any) -> dict:
    """The terminal ``result_payloads`` + ``cost`` of a flow leg's ``Run`` — ONE
    shape for both owners (#1441).

    A leg's cost is what its engine REPORTED (``LegOutcome.cost_usd`` ←
    ``DataflowResult.cost_usd`` or partial terminal metrics), carried under the
    explicit ``llm_cost_usd`` key ``_aggregate_cost`` reads; it is never scanned
    out of the leg's stats or findings. Unknown cost remains absent, not $0.
    """
    payload: dict[str, Any] = {
        "output": step_output,
        "stats": dict(getattr(outcome, "stats", None) or {}),
        "metrics": dict(getattr(outcome, "metrics", None) or {}),
    }
    from cairn.core.dispatch.dispatcher import _aggregate_cost  # noqa: PLC0415 — cycle: dispatcher → bridge → here

    payload["llm_cost_usd"] = round(float(getattr(outcome, "cost_usd", 0.0) or 0.0), 6)
    if payload["llm_cost_usd"] <= 0:
        return {"result_payloads": [payload]}
    return {"result_payloads": [payload],
            "cost": _aggregate_cost([{"llm_cost_usd": payload["llm_cost_usd"]}])}


async def pipeline_spent_usd(rec: Any, run_store: Any, *, exclude_step: str = "") -> float:
    """What a pipeline's legs have spent so far: the sum of each leg run's own
    ``cost.total_usd`` — the figure ``GET /pipelines/{id}`` shows per step. Read
    from records, never estimated. Feeds a gate's cost line (#2358) and, since
    #1441, the ``spent_usd`` a leg's budget reservation is checked against.
    Raises when a run cannot be read: the caller decides what unknown means."""
    spent = 0.0
    for step_id, st in (getattr(rec, "step_state", None) or {}).items():
        run_id = getattr(st, "run_id", None)
        if step_id == exclude_step or not run_id:
            continue
        run = await run_store.get(run_id)
        cost = getattr(run, "cost", None) or {}
        if isinstance(cost, dict):
            spent += float(cost.get("total_usd") or 0.0)
    return round(spent, 6)


class FlowLegHooks:
    """Side-channels for one flow leg. Build, then pass ``execute_kwargs()`` to
    ``execute_leg`` — or ``persisted_handle()`` to adopt after a restart."""

    def __init__(
        self,
        *,
        pipeline_id: str,
        step_id: str,
        leg_ref: str,
        run_id: str,
        pipeline_store: Any,
        run_store: Any,
        emit: EmitFn,
        artifact_store: Any,
        engine: Any = None,
        bus: Any = None,
        audit: Optional[Callable[[str, dict], Awaitable[None]]] = None,
        section: Optional[Callable[[str], Any]] = None,
    ) -> None:
        self.pipeline_id = pipeline_id
        self.step_id = step_id
        self.leg_ref = leg_ref
        self.run_id = run_id
        self.pipeline_store = pipeline_store
        self.run_store = run_store
        self.emit = emit
        self.artifact_store = artifact_store
        self.engine = engine
        self.bus = bus
        #: owner-provided audit sink (event, detail); default = bus publish
        self._audit = audit
        #: the handle as last seen (submit or adopt) — drives checkpoint relay
        self.leg_handle: dict[str, Any] = {}
        #: the owner's per-pipeline critical section (``PipelineBridge._lock``). The
        #: handle write is a read-modify-write of the whole record: outside the section,
        #: an advance that read the record before it puts it back WITHOUT the handle,
        #: and the re-attach token is lost while the leg runs.
        self._section = section

    # ── audit ────────────────────────────────────────────────────────────────
    async def audit(self, event: str, detail: dict) -> None:
        if self._audit is not None:
            try:
                await self._audit(event, detail)
            except Exception:  # noqa: BLE001 — audit must never break the leg
                logger.debug("leg audit sink failed for %s/%s", self.pipeline_id, event)
            return
        await publish_pipeline_audit(self.pipeline_id, self.step_id, event, detail, bus=self.bus)

    # ── H1: persist the re-attach token ──────────────────────────────────────
    def _remember(self, handle: Any) -> dict:
        h = {
            "job_id": str(getattr(handle, "job_id", "") or ""),
            "engine": str(getattr(handle, "engine", "") or ""),
            "meta": dict(getattr(handle, "meta", {}) or {}),
        }
        self.leg_handle = dict(h)
        return h

    async def on_submit(self, handle: Any) -> None:
        """Persist ``{job_id, engine, meta}`` + this run_id on the pipeline's
        step the moment submit returns — a crash between submit and settle must
        not lose the only pointer to paid, in-flight compute (#1239)."""
        h = self._remember(handle)
        if self._section is None:
            await self._persist_handle(h)
            return
        async with self._section(self.pipeline_id):
            await self._persist_handle(h)

    async def _persist_handle(self, h: dict) -> None:
        cur = await self.pipeline_store.get(self.pipeline_id)
        st = cur.step_state.get(self.step_id) if cur else None
        if cur is None or st is None:
            return
        st.handle = h
        st.run_id = self.run_id
        if not st.started_at:
            st.started_at = utc_now_iso()
        cur.step_state[self.step_id] = st
        await self.pipeline_store.put(cur)

    async def persisted_handle(self) -> Optional[dict]:
        """The handle a previous owner process persisted for this step, if the
        step is still in flight — the adopt-by-handle token."""
        try:
            cur = await self.pipeline_store.get(self.pipeline_id)
        except Exception:  # noqa: BLE001 — no record ⇒ nothing to adopt
            return None
        st = cur.step_state.get(self.step_id) if cur else None
        h = getattr(st, "handle", None) if st is not None else None
        if h and str(getattr(st, "status", "")) in ("running", "queued", "pending"):
            self.leg_handle = dict(h)
            return dict(h)
        return None

    # ── H1 liveness + H6b relay ──────────────────────────────────────────────
    async def heartbeat(self) -> None:
        await self.run_store.update(self.run_id, last_heartbeat_at=utc_now_iso())
        await self.relay_checkpoints()

    async def relay_checkpoints(self) -> None:
        """Best-effort relay of new checkpoints into durable storage (H6b).
        Local workdirs copy directly; a remote workdir is pulled over the
        engine's own channel (H6d ``fetch_checkpoints``) into a staging dir."""
        if not self.leg_handle or self.artifact_store is None:
            return
        from cairn.core.dataflow.checkpoints import relay_local_checkpoints  # noqa: PLC0415 — cycle: checkpoints → contract ← dispatch

        wd = str((self.leg_handle.get("meta") or {}).get("workdir", "") or "")
        if wd and Path(wd).is_dir():
            src: Optional[Path] = Path(wd)
        else:
            src = await self._pull_remote_checkpoints()
            if src is None:
                return
        try:
            new = await relay_local_checkpoints(src, run_id=self.run_id, store=self.artifact_store)
        except Exception:  # noqa: BLE001 — relay must never disturb the leg
            logger.debug("checkpoint relay failed for %s", self.run_id, exc_info=True)
            return
        for m in new:
            await self.audit("checkpoint_registered", {
                "stage": m.get("stage", ""), "stage_index": m.get("stage_index"),
                "uri": m.get("_relayed_uri", "")})

    async def _pull_remote_checkpoints(self) -> Optional[Path]:
        job_id = str(self.leg_handle.get("job_id", "") or "")
        fetch = getattr(self.engine, "fetch_checkpoints", None) if self.engine is not None else None
        if not job_id or fetch is None:
            return None
        from cairn.core.dataflow.engine import EngineJobHandle  # noqa: PLC0415 — cycle

        handle = EngineJobHandle(
            job_id=job_id,
            engine=str(self.leg_handle.get("engine") or getattr(self.engine, "name", "engine")),
            meta=dict(self.leg_handle.get("meta") or {}),
        )
        dest = Path(tempfile.gettempdir()) / f"cairn-ckpt-pull-{self.run_id}"
        try:
            got = await fetch(handle, dest)
        except Exception:  # noqa: BLE001 — pull must never disturb the leg
            logger.debug("checkpoint pull failed for %s", self.run_id, exc_info=True)
            return None
        return dest if got else None

    # ── H2/H6c visibility ────────────────────────────────────────────────────
    async def on_retry(self, attempt: int, cause: str) -> None:
        await self.emit(self.run_id, "leg_retrying", {
            "leg": self.leg_ref, "step_id": self.step_id, "attempt": attempt, "error": cause})

    async def on_resume(self, rf: Any) -> None:
        detail = {"stage_index": int(getattr(rf, "stage_index", -1)),
                  "spec_key": str(getattr(rf, "spec_key", ""))}
        await self.audit("leg_resumed_from_checkpoint", detail)
        await self.emit(self.run_id, "leg_resumed_from_checkpoint", {
            "leg": self.leg_ref, "step_id": self.step_id, "stage_index": detail["stage_index"]})

    async def on_adopt(self, handle: dict) -> None:
        """A restarted owner re-attached to in-flight compute instead of
        resubmitting it — audited so the trail shows ONE engine job, two
        owner processes."""
        self.leg_handle = dict(handle)
        detail = {"job_id": str(handle.get("job_id", "")), "engine": str(handle.get("engine", ""))}
        await self.audit("leg_readopted", detail)
        await self.emit(self.run_id, "leg_readopted", {
            "leg": self.leg_ref, "step_id": self.step_id, **detail})

    def resume_provider(self) -> Callable[[str], Awaitable[Any]]:
        from cairn.core.dataflow.checkpoints import find_resume_checkpoint  # noqa: PLC0415 — cycle

        store = self.artifact_store

        async def _lookup(key: str):
            if store is None:
                return None
            return await find_resume_checkpoint(store, key)

        return _lookup

    # ── the assembled call ───────────────────────────────────────────────────
    def execute_kwargs(self, *, retry: Any = None, adopt: Optional[dict] = None) -> dict:
        """Everything ``execute_leg`` takes beyond ref/inputs/engine/run_id and
        the governance numbers — the same dict from either owner."""
        return {
            "on_submit": self.on_submit,
            "heartbeat": self.heartbeat,
            "retry": retry,
            "on_retry": self.on_retry,
            "resume_provider": self.resume_provider(),
            "on_resume": self.on_resume,
            "artifact_store": self.artifact_store,
            "adopt": adopt,
            "on_adopt": self.on_adopt,
        }


__all__ = ["FlowLegHooks", "publish_pipeline_audit"]
