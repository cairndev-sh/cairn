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
from cairn.core.workspace import DEFAULT_WORKSPACE

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

    A leg's engine cost is what its engine REPORTED (``LegOutcome.cost_usd`` ←
    ``DataflowResult.cost_usd`` or partial terminal metrics), carried under the
    explicit ``llm_cost_usd`` key ``_aggregate_cost`` reads; it is never scanned
    out of the leg's stats or findings. A failed leg may carry known partial
    engine spend; when it cannot be read, that cost remains unknown, not $0.

    ADR-0151 D5 — a leg on a priced box also carries its box time
    (``LegOutcome.gpu_usd``: ``hourly_usd`` × each attempt's wall-clock), under
    ``gpu_usd``, on success AND on failure: the box billed either way. A leg with
    neither figure writes no cost — an explicit $0 would read as "free".
    """
    payload: dict[str, Any] = {
        "output": step_output,
        "stats": dict(getattr(outcome, "stats", None) or {}),
        "metrics": dict(getattr(outcome, "metrics", None) or {}),
    }
    gpu = round(float(getattr(outcome, "gpu_usd", 0.0) or 0.0), 6)
    llm = payload["llm_cost_usd"] = round(float(getattr(outcome, "cost_usd", 0.0) or 0.0), 6)
    if gpu > 0:
        payload["gpu_usd"] = gpu
    if llm <= 0 and gpu <= 0:
        return {"result_payloads": [payload]}
    from cairn.core.dispatch.dispatcher import _aggregate_cost  # noqa: PLC0415 — cycle: dispatcher → bridge → here

    return {"result_payloads": [payload],
            "cost": _aggregate_cost([{"llm_cost_usd": llm, "gpu_usd": gpu}])}


# ── ADR-0151 D1–D4: a leg on a priced compute.ssh box ──────────────────────

#: The connection kind whose ``hourly_usd`` prices a leg (D1).
PRICED_TARGET_KIND = "compute.ssh"
#: Where the launch door records the estimate: one sub-dict of
#: ``PipelineRecord.admission`` (D2), never a column.
COST_ESTIMATE_KEY = "cost_estimate"
NOT_PRICED = "not priced"
#: A target spelled ``worker:<pool>`` is a pull-worker pool, not a connection.
WORKER_TARGET_PREFIX = "worker:"
#: The one engine whose target names a ``compute.ssh`` connection it dispatches to
#: (ADR-0152): any other engine's ``<kind>_target`` means something else (a legacy
#: Metaflow metadata root, a pool, a cluster) and is never priced.
PRICED_ENGINE = "builtin"


class LegPriceUnreadable(RuntimeError):
    """A leg that may run on a priced box whose price cannot be READ: the target
    selection, the connection store or the named connection failed, or its
    ``hourly_usd`` is malformed. Unknown is never "not priced": the launch door
    refuses (ADR-0151, fail-closed), and a retry can succeed once it is readable."""


def compute_target_for_kind(kind: str) -> str:
    """The compute TARGET a flow of ``kind`` dispatches to (``ml_target`` for ``kind:
    ml``, ``dataflow_target`` otherwise) — the target half of
    :func:`engine_and_target_for_kind`, the bridge's per-target cap key. ``""`` = no
    target. A selection that cannot be read RAISES (the bridge degrades it to "no cap")."""
    return engine_and_target_for_kind(kind)[1]


def engine_and_target_for_kind(kind: str) -> tuple[str, str]:
    """``(engine, target)`` the environment selects for a flow of ``kind`` — the
    selection ``resolve_engine`` builds from (``engine_registry.selected_engine``).
    A selection that cannot be read RAISES."""
    from cairn.core.dataflow.engine_registry import selected_engine  # noqa: PLC0415 - cycle: registry → selection → pack

    return selected_engine(kind)


def _hourly_rate(name: str, config: dict) -> float:
    """``hourly_usd`` off a connection config: 0.0 when unset (not priced), the
    rate when it is a finite number >= 0, :class:`LegPriceUnreadable` otherwise."""
    raw = str((config or {}).get("hourly_usd") or "").strip()
    if not raw:
        return 0.0
    try:
        rate = float(raw)
    except ValueError:
        rate = float("nan")
    if not (0.0 <= rate < float("inf")):   # NaN fails both comparisons
        raise LegPriceUnreadable(f"connection {name!r} has a malformed hourly_usd {raw!r}; "
                                 f"set a number of dollars per hour (or unset it)")
    return rate


async def price_flow_leg(target: str, timeout_s: Optional[float], *,
                         store: Any = None, multiplier: int = 1,
                         multiplier_basis: str = "") -> Optional[dict]:
    """ADR-0151 D1 — the price of one flow leg on ``target``: ``hourly_usd ×
    timeout``, read from the target's connection config core-to-core (the way
    ``target_cap`` reads ``max_in_flight``; no secret is resolved).

    ``None`` when the leg has no target. "Not priced" only for the definite cases:
    a target that is not a ``compute.ssh`` connection (a ``worker:`` pool, another
    kind) or one that sets no ``hourly_usd`` — ``{"priced": False, "connection",
    "basis": "not priced: …"}``, never $0. A store, connection or rate that cannot
    be read raises :class:`LegPriceUnreadable`.

    A priced entry: ``{"priced": True, "connection", "hourly_usd", "hours",
    "per_attempt_usd", "runs", "estimate_usd", "basis"}``. ``per_attempt_usd`` =
    ``hourly_usd × timeout`` is what one attempt reserves at leg time; ``runs`` =
    ``multiplier`` (loop iterations × attempts, the bounded worst case) and
    ``estimate_usd`` = ``per_attempt_usd × runs`` is what the door checks. The money
    fields are ``None`` for a priced leg with no timeout, which the door refuses (D3)."""
    name = str(target or "").strip()
    if not name:
        return None
    if name.startswith(WORKER_TARGET_PREFIX):
        return {"priced": False, "connection": name,
                "basis": f"{NOT_PRICED}: {name!r} is a worker pool, not a compute.ssh connection"}
    try:
        from cairn.core.connections import (  # noqa: PLC0415 - cycle: connections → secrets stack
            default_connection_store, lookup_account, run_account,
        )

        account = lookup_account(await run_account())
        owned = store is None
        store = store or default_connection_store()
        try:
            conn = await store.get_by_name(name, account_id=account)
        finally:
            close = getattr(store, "close", None)
            if owned and callable(close):
                close()
    except Exception as e:  # noqa: BLE001 — unreadable is unknown, never "not priced"
        raise LegPriceUnreadable(
            f"connection {name!r} could not be read ({type(e).__name__}: {e})") from e
    if conn is None:
        raise LegPriceUnreadable(f"connection {name!r} is not registered in this account; "
                                 f"add it with `cairn connect ssh --name {name}`")
    if str(getattr(conn, "kind", "")) != PRICED_TARGET_KIND:
        return {"priced": False, "connection": name,
                "basis": f"{NOT_PRICED}: {name!r} is kind {conn.kind!r}"}
    hourly = _hourly_rate(name, dict(getattr(conn, "config", None) or {}))
    if hourly <= 0:
        return {"priced": False, "connection": name,
                "basis": f"{NOT_PRICED}: connection {name!r} sets no hourly_usd"}
    runs = max(1, int(multiplier or 1))
    if not timeout_s or float(timeout_s) <= 0:
        return {"priced": True, "connection": name, "hourly_usd": hourly, "hours": None,
                "per_attempt_usd": None, "runs": runs, "estimate_usd": None,
                "basis": f"${hourly:g}/h on {name}, no timeout declared"}
    hours = float(timeout_s) / 3600.0
    per = round(hourly * hours, 6)
    est = round(per * runs, 6)
    basis = f"${hourly:g}/h × {hours:g}h on {name}"
    if runs > 1:
        basis += f" × {runs} ({multiplier_basis or 'runs'})"
    return {"priced": True, "connection": name, "hourly_usd": hourly, "hours": round(hours, 6),
            "per_attempt_usd": per, "runs": runs, "estimate_usd": est,
            "basis": f"{basis} = ${est:.2f}"}


def _leg_runs(plan: Any, s: Any, flow_kind: str) -> tuple[int, str]:
    """The bounded worst-case number of times a leg runs: its loop's
    ``max_iterations`` (1 outside a loop) × the attempts ``execute_leg`` allows —
    the leg's ``retry: {max_attempts}`` when it declares one, else the kind's
    default (1 for ``ml``, 2 for ``data``)."""
    iterations = 1
    for ldef in (getattr(plan, "loops", None) or {}).values():
        body = {str(b.get("id") or "") for b in ((ldef or {}).get("steps") or []) if isinstance(b, dict)}
        if s.id in body:
            iterations = max(1, int((ldef or {}).get("max_iterations") or 1))
            break
    raw = (getattr(s, "inputs", None) or {}).get("retry")
    if isinstance(raw, dict):
        attempts = max(1, int(raw.get("max_attempts", 2) or 2))   # `_leg_retry_policy`'s default
    else:
        attempts = 1 if flow_kind == "ml" else 2                  # `execute_leg`'s default
    parts = [f"{iterations} loop iterations"] if iterations > 1 else []
    if attempts > 1:
        parts.append(f"{attempts} attempts")
    return iterations * attempts, " × ".join(parts)


async def price_plan(plan: Any, *, store: Any = None) -> dict:
    """The launch door's estimate (D2): every flow leg with a compute target,
    priced by :func:`price_flow_leg` at its bounded worst case (loop iterations ×
    attempts), in plan order. ``{}`` when no leg has a target, so a pipeline with
    no box records exactly what it recorded before. Raises
    :class:`LegPriceUnreadable` naming the leg when a price cannot be read."""
    from cairn.core.dispatch.leg_executor import flow_kind_of, leg_kind_of  # noqa: PLC0415 - cycle: executor → storage → here

    legs: list[dict] = []
    for s in getattr(plan, "steps", None) or []:
        if leg_kind_of(getattr(s, "ref", "")) != "flow":
            continue
        kind = flow_kind_of(s.ref)
        try:
            engine, target = engine_and_target_for_kind(kind)
        except Exception as e:  # noqa: BLE001 — a selection error is unknown, never "no target"
            raise LegPriceUnreadable(f"leg {s.id!r}: its engine and compute target cannot be read "
                                     f"({type(e).__name__}: {e})") from e
        if not target:
            continue
        if engine != PRICED_ENGINE:
            # ADR-0152 — only the builtin engine dispatches to a connection; another
            # engine's target is not one, so it is neither priced nor looked up.
            legs.append({"step": s.id, "priced": False, "connection": target,
                         "basis": f"{NOT_PRICED} (engine {engine!r} does not dispatch to a connection)"})
            continue
        runs, why = _leg_runs(plan, s, kind)
        try:
            price = await price_flow_leg(target, getattr(s, "timeout_s", None), store=store,
                                         multiplier=runs, multiplier_basis=why)
        except LegPriceUnreadable as e:
            raise LegPriceUnreadable(f"leg {s.id!r}: {e}") from e
        if price is not None:
            legs.append({"step": s.id, **price})
    if not legs:
        return {}
    total = sum(float(e.get("estimate_usd") or 0.0) for e in legs if e.get("priced"))
    return {"estimate_usd": round(total, 6), "legs": legs}


def _declared(rec: Any, step_id: str) -> str:
    from cairn.core.engine.pipeline_loop import declared_step_id  # noqa: PLC0415 — engine helper, lazy like its siblings

    return declared_step_id(step_id, getattr(getattr(rec, "plan", None), "loops", None) or {})


def recorded_cost_estimate(rec: Any) -> dict:
    """The estimate the launch door recorded on ``rec.admission`` (``{}`` for a
    record written before ADR-0151, or a pipeline with no target)."""
    est = (getattr(rec, "admission", None) or {}).get(COST_ESTIMATE_KEY)
    return dict(est) if isinstance(est, dict) else {}


def recorded_leg_price(rec: Any, step_id: str) -> Optional[dict]:
    """This leg's entry in the recorded estimate — read, never re-derived (D2)."""
    want = {step_id, _declared(rec, step_id)}
    for e in recorded_cost_estimate(rec).get("legs") or []:
        if isinstance(e, dict) and e.get("step") in want:
            return dict(e)
    return None


def leg_price_kwargs(rec: Any, step_id: str, run: Any) -> dict:
    """What a flow leg reserves and settles with — the SAME call from both owners.

    A leg the launch door priced reserves the RECORDED estimate and settles its box
    time at the recorded ``hourly_usd`` (D2, D5); an adopted attempt starts from the
    persisted ``run.started_at``. Any other leg keeps the pipeline-wide
    ``governance.estimated_usd`` it reserved before."""
    price = recorded_leg_price(rec, step_id)
    if price and price.get("priced") and price.get("estimate_usd") is not None:
        # one attempt reserves one attempt's figure; the loop × retry worst case
        # (`estimate_usd`) is what the launch door checked
        per = price.get("per_attempt_usd")
        return {"estimated_usd": float(per if per is not None else price["estimate_usd"]),
                "hourly_usd": float(price.get("hourly_usd") or 0.0),
                "adopted_started_at": getattr(run, "started_at", None)}
    gov = getattr(rec, "governance", None) or {}
    return {"estimated_usd": float(gov.get("estimated_usd", 0.0) or 0.0)}


def estimate_evidence(rec: Any, step_id: str) -> dict:
    """D4 — the gate evidence for the approval at ``step_id``: ``estimate_usd`` (the
    sum of the priced legs) and ``estimate_basis`` (each leg's rate × hours, or "not
    priced"), on every gate that comes before the first priced leg in plan order (the
    first leg with a target when none is priced). ``{}`` for any other gate."""
    est = recorded_cost_estimate(rec)
    legs = [e for e in est.get("legs") or [] if isinstance(e, dict)]
    order = [s.id for s in getattr(getattr(rec, "plan", None), "steps", None) or []]
    gate = _declared(rec, step_id)
    if not legs or gate not in order:
        return {}
    priced = [e for e in legs if e.get("priced")]
    anchor = [order.index(e["step"]) for e in (priced or legs) if e.get("step") in order]
    if not anchor or order.index(gate) > min(anchor):
        return {}
    out: dict[str, Any] = {"estimate_basis": "; ".join(f"{e['step']}: {e.get('basis', '')}" for e in legs)}
    if priced:
        out["estimate_usd"] = float(est.get("estimate_usd") or 0.0)
    return out


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
        # owned by the workspace the leg's run record names (ADR-0148), read once
        # per leg; an unreadable record leaves it to the workspace in scope
        if getattr(self, "_relay_workspace", None) is None:
            self._relay_workspace = await self._run_workspace(self.run_id)
        try:
            new = await relay_local_checkpoints(src, run_id=self.run_id, store=self.artifact_store,
                                                workspace_id=self._relay_workspace)
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

    async def _run_workspace(self, run_id: str) -> Optional[str]:
        """The workspace a run's record names (``None`` when it cannot be read)."""
        try:
            run = await self.run_store.get(run_id)
        except Exception:  # noqa: BLE001 — unreadable is unknown
            return None
        if run is None:
            return None
        return str(getattr(run, "workspace_id", "") or "") or DEFAULT_WORKSPACE

    def resume_provider(self) -> Callable[[str], Awaitable[Any]]:
        from cairn.core.dataflow.checkpoints import find_resume_checkpoint  # noqa: PLC0415 — cycle

        store = self.artifact_store

        async def _lookup(key: str):
            if store is None:
                return None
            # Only this leg's workspace's checkpoints: the one its run record names.
            ws = await self._run_workspace(self.run_id)
            if ws is None:
                return None
            return await find_resume_checkpoint(store, key, workspace_id=ws,
                                                run_workspace=self._run_workspace)

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


__all__ = ["COST_ESTIMATE_KEY", "FlowLegHooks", "LegPriceUnreadable", "NOT_PRICED", "PRICED_ENGINE",
           "PRICED_TARGET_KIND", "engine_and_target_for_kind",
           "compute_target_for_kind", "estimate_evidence", "leg_price_kwargs", "price_flow_leg",
           "price_plan", "publish_pipeline_audit", "recorded_cost_estimate", "recorded_leg_price"]
