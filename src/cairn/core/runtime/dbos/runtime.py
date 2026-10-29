"""DBOSRuntime — the caller-side :class:`Runtime` impl for ``CAIRN_RUNTIME=dbos``.

Submits each run as a DBOS workflow (``cairn.run_template``): single-node
durable execution — crash-recovery on restart, durable HITL via
``recv``/``send``, run-id as the idempotency key (ADR-0035). Like
``TemporalRuntime`` it keys on ``template_name``/``template_sha`` (the graph is
looked up in :mod:`cairn.core.runtime.dbos.registry`, not passed, since the
compiled graph isn't a pickleable workflow argument); unlike Temporal, DBOS runs
the workflow **in this process** — no separate worker.

``invoke`` awaits the workflow result (mirrors Temporal's blocking
``execute_workflow``): a gated run parks at ``recv`` until ``resume`` sends the
decision, so the server drives runs as background tasks and approves out-of-band.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Optional

from cairn.core import settings
from cairn.core.runtime.dbos import require

logger = logging.getLogger("cairn.runtime.dbos.runtime")

_LAUNCHED = False

# DBOS's SQLite system DB declares ``workflow_status.created_at NOT NULL DEFAULT
# (unixepoch('subsec') ...)``; ``unixepoch('subsec')`` is NULL before SQLite
# 3.42, so every workflow insert fails with a bare IntegrityError.
_MIN_SQLITE = (3, 42, 0)


def check_system_db(dsn: str) -> None:
    """Refuse a SQLite DBOS system DB on a SQLite older than 3.42.

    A Postgres system DB is not checked. Raises :class:`RuntimeError` naming
    the cause and the remedies, so the failure is loud at boot and on every
    run submit instead of a bare ``NOT NULL constraint failed`` per workflow.
    """
    if not dsn.lower().startswith("sqlite"):
        return
    have = tuple(sqlite3.sqlite_version_info)
    if have >= _MIN_SQLITE:
        return
    raise RuntimeError(
        f"DBOS runtime refused: the DBOS system database is SQLite "
        f"({settings.masked_dsn(dsn)}) but this Python links SQLite "
        f"{'.'.join(map(str, have))}; DBOS's SQLite system DB needs SQLite >= "
        f"{'.'.join(map(str, _MIN_SQLITE))} (its workflow_status.created_at "
        "default uses unixepoch('subsec'), which is NULL on older SQLite, so "
        "every workflow insert fails with 'NOT NULL constraint failed: "
        "workflow_status.created_at'). Remedies: run cairn on a Python whose "
        "SQLite is >= 3.42 (e.g. a uv-managed Python), or point CAIRN_DBOS_DSN "
        "at a Postgres DBOS system database."
    )


def ensure_launched() -> None:
    """Instantiate + launch DBOS once per process; ``launch()`` recovers PENDING
    runs. Importing the workflow module first registers ``run_template`` with
    DBOS's global registry (decoration is import-time). Idempotent — a second
    call (or a process the server already launched) is a no-op.
    """
    global _LAUNCHED
    if _LAUNCHED:
        return
    require()
    # Checked before launch so a refused process never sets _LAUNCHED: every
    # later submit re-checks and fails with the same explanation.
    check_system_db(settings.dbos_dsn())
    from dbos import DBOS, DBOSConfig  # noqa: PLC0415

    import cairn.core.runtime.dbos.workflow  # noqa: F401, PLC0415 — registers run_template

    DBOS(config=DBOSConfig(name="cairn", system_database_url=settings.dbos_dsn()))
    DBOS.launch()
    _LAUNCHED = True
    # #1867 — never the raw DSN: it carries the Postgres password.
    logger.info("DBOS launched (dsn=%s)", settings.masked_dsn(settings.dbos_dsn()))


def boot() -> int:
    """Server-boot entrypoint for ``CAIRN_RUNTIME=dbos``: rebuild the template
    registry, then launch DBOS (``launch()`` recovers PENDING runs, looking
    their graphs up in the now-repopulated registry). Returns the number of
    templates registered. Call from a running event loop (the server lifespan)
    so recovered async workflows have a loop to run on.
    """
    from cairn.core.runtime.dbos.registry import rebuild  # noqa: PLC0415

    n = rebuild()
    ensure_launched()
    return n


class DBOSRuntime:
    """:class:`Runtime` implementation backed by DBOS (single-node durable)."""

    def __init__(self) -> None:
        require()

    async def invoke(
        self,
        compiled_graph: Any,
        initial_state: dict[str, Any],
        *,
        run_id: Optional[str] = None,
        template_name: Optional[str] = None,
        template_sha: Optional[str] = None,
    ) -> dict[str, Any]:
        if template_name is None:
            raise ValueError(
                "DBOSRuntime.invoke requires template_name — the run-workflow "
                "keys its graph registry on (template_name, template_sha)."
            )
        # #1851 — refuse, don't fall back. run_id is the DBOS workflow id: the
        # idempotency key (a repeated id is the SAME workflow, so two runs of one
        # template would collide) and the id every downstream write keys on
        # (surface_awaiting_approval, the reconciler, the run store). A missing
        # id used to fall back to `<template>-<sha>`, and a caller that put the
        # template name in state produced `workflow_uuid = promote` on prod —
        # a gated run whose approval failed with KeyError('promote') and parked
        # invisibly. Neither shape can ever be right for a durable runtime, so
        # this is checked BEFORE anything touches DBOS.
        if not run_id or run_id == template_name:
            raise ValueError(
                f"DBOSRuntime.invoke needs a real run id, got {run_id!r} for "
                f"template {template_name!r} — the run id is the durable workflow "
                "id and must be unique per run (the dispatcher passes Run.id; the "
                "CLI must thread new_run_id() into state). See #1851."
            )
        from dbos import DBOS, SetWorkflowID  # noqa: PLC0415

        from cairn.core.runtime.dbos.registry import register  # noqa: PLC0415
        from cairn.core.runtime.dbos.workflow import run_template  # noqa: PLC0415
        from cairn.core import obs  # noqa: PLC0415

        ensure_launched()
        sha = template_sha or ""
        rid = run_id
        # In-process: register the live compiled graph so the workflow (running
        # on a DBOS executor thread in this process) can look it up.
        register(template_name, sha, compiled_graph)
        # #1872 — start ASYNC, never the sync ``start_workflow``. The sync API
        # runs an async workflow on a thread-pool worker that blocks on a future
        # submitted to DBOS's background loop; on a graceful shutdown (SIGTERM,
        # i.e. every deploy) that future is cancelled, the worker sees
        # ``concurrent.futures.CancelledError`` — an ``Exception`` — and DBOS
        # persists the workflow as ERROR, so nothing is left to recover: prod
        # lost an in-flight run AND a run parked at its gate on one restart.
        # ``start_workflow_async`` runs the workflow as a native task on THIS
        # loop, where shutdown raises ``asyncio.CancelledError`` (a
        # BaseException DBOS does not persist) and the row stays PENDING for
        # ``launch()`` to recover — the same path DBOS itself uses for recovered
        # workflows, which is why SIGKILL-recovered runs survived all along.
        with SetWorkflowID(rid):
            ctx = obs.get()
            await DBOS.start_workflow_async(
                run_template, template_name, sha, dict(initial_state), rid,
                ctx.max_cost_usd if ctx is not None else None)
        result = await DBOS.get_result_async(rid)
        return dict(result) if isinstance(result, dict) else {"value": result}

    async def resume(
        self,
        *,
        run_id: Optional[str] = None,
        decision: Any = None,
    ) -> None:
        """Deliver a HITL decision to a run paused at an approval interrupt.

        The DBOS counterpart of ``InProcessRuntime.resume`` /
        ``TemporalRuntime.resume`` — sends the ApprovalRecord-shaped decision to
        the workflow's ``recv`` on the ``approve`` topic, keyed on ``run_id``.
        """
        from dbos import DBOS  # noqa: PLC0415

        from cairn.core.runtime.dbos.workflow import APPROVE_TOPIC  # noqa: PLC0415

        ensure_launched()
        await DBOS.send_async(run_id, decision, topic=APPROVE_TOPIC)

    async def workflow_result(self, run_id: str) -> Optional[Any]:
        """The finished workflow's return value, or None (#1869).

        ``invoke`` awaits ``get_result_async`` and the dispatcher persists what
        comes back — but that await dies with the process. A workflow DBOS
        recovers after a restart runs to SUCCESS with nobody waiting, and the
        reconciler used to flip the run to ``done`` from the STATUS alone, so
        the findings it produced were dropped. DBOS keeps the output in
        ``workflow_status.output``; this hands it to the reconciler so a
        recovered run and a live run leave identical records. None for a
        workflow that is not SUCCESS, unknown, or on any DBOS error.
        """
        from dbos import DBOS  # noqa: PLC0415

        try:
            ensure_launched()
            status = await DBOS.get_workflow_status_async(run_id)
            if status is None or str(getattr(status, "status", "")).upper() != "SUCCESS":
                return None
            return await DBOS.get_result_async(run_id)
        except Exception as e:  # noqa: BLE001 — a missing result must never block reconciliation
            logger.debug("workflow_result(%s): %s", run_id, e)
            return None

    async def workflow_terminal_status(self, run_id: str) -> Optional[str]:
        """Return the cairn terminal status for ``run_id``'s workflow, or None.

        The DBOS counterpart of :meth:`TemporalRuntime.workflow_terminal_status`
        (#976), and the fix for a split brain that only a crash reveals (#1847).

        ``invoke`` awaits the workflow and writes the terminal status itself —
        but that await dies with the process. DBOS then recovers the workflow on
        the next ``launch()`` and runs it to completion with NOBODY waiting for
        the result, so the workflow reaches SUCCESS while the run record is left
        at whatever it said when the process died. Verified live: a gated run
        killed at its approval gate, restarted, then approved, ended
        ``dbos.workflow_status = SUCCESS`` and ``run.status = awaiting_approval``
        — permanently, since the approval reaper would eventually expire a run
        that had in fact succeeded.

        PENDING and ENQUEUED return None: a gated run parked on ``recv`` is
        PENDING, and the reconciler must leave it alone (the approval reaper owns
        that lifecycle) — exactly as it does for a Temporal workflow paused
        internally. Only a genuinely terminal workflow reconciles.
        """
        from dbos import DBOS  # noqa: PLC0415

        terminal = {
            "SUCCESS": "done",
            "ERROR": "failed",
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED": "failed",
            "CANCELLED": "cancelled",
        }
        try:
            ensure_launched()
            status = await DBOS.get_workflow_status_async(run_id)
        except Exception as e:  # noqa: BLE001 — an unknown id or a DBOS hiccup is "don't touch it"
            logger.debug("workflow_terminal_status(%s): %s", run_id, e)
            return None
        if status is None:
            return None
        return terminal.get(str(getattr(status, "status", "")).upper())
