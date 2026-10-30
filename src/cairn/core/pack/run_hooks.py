"""Run hooks, discovered through the ``cairn.run_hooks`` entry-point group
(ADR-0120 D4; owner decision 2026-10-12 on #2421).

The dispatch layer used to call the platform's eval ledger, judges, continuous
eval, pilot ledger and playground BY NAME when a run or leg reached a terminal
state. Those are product features; the public core knows none of them. Instead
the dispatch layer FIRES events and whoever is installed listens:

    [project.entry-points."cairn.run_hooks"]
    scoring = "cairn_platform.hooks:on_run_event"

A provider is an async callable ``(event: str, payload: dict) -> None``. Events
and their payloads (every payload also carries ``"event"``):

- ``run.admitting`` — ``run_id``, ``template_ref``, ``workspace_id``, ``series_id``
  (``""`` when none), ``estimated_usd``, ``inputs``: the one QUERY-shaped event
  (#2421 decision 6). The dispatcher fires it at admission BEFORE the record
  exists; a provider may refuse the run by raising :class:`AdmissionRefused`,
  which :func:`fire` re-raises and the dispatcher turns into its admission
  refusal (a ``ValueError`` carrying the reason). The platform's ``series``
  provider checks the run against its series cap and reserves the estimate here
  (through :mod:`cairn.core.budget_reservations`, stamped with ``run_id``; the
  dispatcher settles or releases it by run id on every terminal). Absent means:
  a run is not checked against a series cap.
- ``pipeline.launching`` — ``pipeline_ref`` (``pipeline:<pack>/<name>``, or the
  ``flow:`` stamp of a one-leg wrapper), ``workspace_id``, ``account_id`` (``""``
  when it cannot be resolved), ``environment`` (``""`` when none), ``source`` (the
  trigger's ``source``: ``start`` / ``console`` / ``cli`` / ``schedule`` / ``watch`` /
  ``webhook`` / ``event`` / ``dispatch`` (a template run wrapped in its one-leg pipeline) / an operator's
  own, ``""`` when unnamed), ``trigger``, ``preset`` (``""`` when
  none), ``inputs`` (the values the launch will run with: caller's values over the
  preset and the declared defaults, typed), ``supplied`` (what the caller and the
  preset supplied, before defaults). ``trigger``, ``inputs`` and ``supplied`` are
  read-only copies (``MappingProxyType``; lists become tuples). The second
  QUERY-shaped event: the launch door (:func:`cairn.core.dispatch.pipeline_launch.launch_pipeline_def`,
  which every pipeline launch goes through) fires it after the plan-time policy
  gates, the service doors and the admission decision, BEFORE the pipeline record
  exists, and only when at least one provider is installed. A provider may refuse
  the launch by raising :class:`AdmissionRefused`; the door raises it as its
  admission refusal (``AdmissionRefusedError``, the error a refused admission
  already gives every caller), and no record is created. This event FAILS CLOSED:
  a provider that raises anything else refuses the launch too, with a generic
  reason naming the provider. The door takes no side effect before this event,
  so a refusal leaves nothing to release; a provider should not take one either
  (a provider after it in name order may still refuse). Absent means: a launch is
  not checked by anyone but the door itself.
- ``run.launched`` — ``run_id``, ``template_ref``, ``workspace_id``, ``series_id``
  (``""`` when none), ``lane``: the run is admitted and queued.
- ``run.finished`` / ``run.failed`` — ``run_id``, ``status``, ``store`` (the
  dispatcher's OWN run store, which owns the record — not the process singleton):
  the run reached a terminal state; a provider that scores or labels it reads
  the run and its events from ``store`` and writes its own verdicts to its own
  store. The continuous-eval consumer (:mod:`cairn.core.eval.continuous`, opt-in
  ``CAIRN_CONTINUOUS_EVAL``) re-fires both from the bus's completion firehose with
  ``source="continuous_eval"``: the same run may already have been announced by
  the dispatcher that ran it, so a listener that writes skips a run it already
  handled (absent ``source`` = the dispatcher's own terminal path).
- ``leg.finished`` — ``pipeline_id``, ``step_id``, ``run_id``, ``status``
  (``done`` / ``failed`` / ``cancelled``): one leg of a pipeline settled.
- ``pipeline.finished`` — ``pipeline_id``, ``ref`` (the pipeline ref as recorded on
  the record, ``PipelineRecord.bundle_ref``: the stamp ``pipeline.launching`` saw as
  ``pipeline_ref``), ``status`` (``done`` / ``failed`` / ``cancelled``),
  ``workspace_id``: a pipeline reached a terminal state (#306). Fired by the
  in-process pipeline bridge once per pipeline per process, AFTER the record is
  persisted with its final status and, on ``done``, its resolved ``outputs``
  (ADR-0161), so a provider that reads the pipeline store sees both. It fires
  however the pipeline ended: its last leg, a gate, a deferred failure, a cancel,
  a ``finalizer:`` settling. Notify-only: it cannot refuse, and a provider failure
  is logged, never raised into the bridge. A Temporal-owned pipeline's ``done`` /
  ``failed`` is not announced (its activities fire no run hooks); a cancel the
  bridge applies to one is. Delivery is not durable (a crash between the terminal
  write and the hook loses it) and a later process may announce a pipeline again,
  so a provider should be idempotent.
- ``run.events_ingested`` — ``run_id``, ``events`` (the accepted batch, as posted),
  ``store``, ``pipeline_store``: a worker's report batch was appended through
  ``POST /runs/{id}/events/ingest`` (a listener reacts to, e.g., a ``divergence``).
- ``workspace.created`` — ``workspace_id``, ``actor`` (who created it: the new
  user's id on sign-up / OAuth, the caller's token subject on ``POST /workspaces``,
  ``"dev"`` with auth off), ``source`` (``"signup"`` / ``"oauth"`` / ``"api"``):
  fired by the ``admin`` router right after the workspace record exists (#2421
  decision 7). Fire-and-forget: a listener provisions what the workspace should
  start with (the platform's ``workspace`` provider: the starter budget series,
  #1571); absent means a new workspace starts with nothing but its record.

Providers run in entry-point name order, one at a time — after the record is
persisted, except on ``run.admitting`` and ``pipeline.launching``. :func:`fire`
raises only an :class:`AdmissionRefused`, and only on those two events (the
refusal ends the pass). Every other failure is handled by one of two policies:

- **fail open** — every event except ``pipeline.launching``, ``run.admitting``
  included: the failure is logged and the others still run (a refusal raised on
  an event that cannot refuse is logged and ignored, like any failure);
- **fail closed** — ``pipeline.launching`` only: the failure is logged and
  re-raised as an :class:`AdmissionRefused` with a generic reason, so a broken
  launch check refuses the launch rather than waving it through.

Discovery fails open on every event, like :mod:`cairn.core.pack.loader` and
:mod:`cairn.core.pack.seeds` (a provider that does not import or is not
callable is skipped, recorded in :func:`run_hook_load_failures`, and shown by
``cairn doctor``). An empty group means: a finished run is recorded, and scored
only by its own declared evals (ADR-0120 D4, "absent means").
"""
from __future__ import annotations

import inspect
import logging
from importlib.metadata import entry_points
from typing import Any, Callable

_log = logging.getLogger("cairn.core.pack.run_hooks")

ENTRY_POINT_GROUP = "cairn.run_hooks"

RUN_ADMITTING = "run.admitting"
RUN_LAUNCHED = "run.launched"
RUN_FINISHED = "run.finished"
RUN_FAILED = "run.failed"
LEG_FINISHED = "leg.finished"
RUN_EVENTS_INGESTED = "run.events_ingested"
WORKSPACE_CREATED = "workspace.created"
PIPELINE_LAUNCHING = "pipeline.launching"
PIPELINE_FINISHED = "pipeline.finished"
EVENTS = (RUN_ADMITTING, RUN_LAUNCHED, RUN_FINISHED, RUN_FAILED, LEG_FINISHED, RUN_EVENTS_INGESTED,
          WORKSPACE_CREATED, PIPELINE_LAUNCHING, PIPELINE_FINISHED)
#: the events a provider may refuse (QUERY-shaped: fired before the record exists)
REFUSABLE_EVENTS = (RUN_ADMITTING, PIPELINE_LAUNCHING)
#: the events on which any provider failure refuses, rather than being logged
FAIL_CLOSED_EVENTS = (PIPELINE_LAUNCHING,)

#: ``async (event, payload) -> None`` — see the module docstring.
RunHook = Callable[[str, dict[str, Any]], Any]

_LOAD_FAILURES: list[dict[str, str]] = []


class AdmissionRefused(Exception):
    """Raised by a ``run.admitting`` or ``pipeline.launching`` provider to refuse the
    run or launch; ``reason`` is what the caller is told (the dispatcher raises it as
    its ``ValueError``, the launch door as its ``AdmissionRefusedError``)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def run_hook_load_failures() -> list[dict[str, str]]:
    """Entry points that did not produce a provider on the last discovery pass."""
    return list(_LOAD_FAILURES)


def load_run_hooks() -> dict[str, RunHook]:
    """Every installed ``cairn.run_hooks`` provider, by entry-point name, in name
    order. Read fresh on every call; a broken entry point is skipped and recorded,
    never raised; two distributions claiming one name: the first wins, loudly."""
    _LOAD_FAILURES.clear()
    out: dict[str, RunHook] = {}
    targets: dict[str, str] = {}
    for ep in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda e: (e.name, e.value)):
        if ep.name in targets:
            if targets[ep.name] == ep.value:
                continue
            _log.warning("run hook %r (%s) skipped: the name is already taken by %s", ep.name, ep.value, targets[ep.name])
            _LOAD_FAILURES.append({"name": ep.name, "target": ep.value, "error": "duplicate provider name"})
            continue
        try:
            obj: Any = ep.load()
        except Exception as e:  # noqa: BLE001 — a broken hook must not break a run
            _log.warning("failed to load run hook %r (%s): %s", ep.name, ep.value, e)
            _LOAD_FAILURES.append({"name": ep.name, "target": ep.value, "error": f"{type(e).__name__}: {e}"})
            continue
        if not callable(obj):
            _log.warning("run hook %r resolved to %r, not a callable — skipping", ep.name, type(obj).__name__)
            _LOAD_FAILURES.append({"name": ep.name, "target": ep.value, "error": f"entry point resolved to {type(obj).__name__}, not a callable"})
            continue
        out[ep.name], targets[ep.name] = obj, ep.value
    return out


async def fire(event: str, **payload: Any) -> list[str]:
    """Run every installed hook for ``event``, in name order; return the names
    that ran without raising. Fails open — a run's own completion has already
    been persisted by the time a hook runs, and a hook must not undo that — except
    for :class:`AdmissionRefused` raised on a refusable event (``run.admitting``,
    ``pipeline.launching``), which is re-raised (the run or launch does not exist
    yet; refusing it is the event's purpose). On ``pipeline.launching`` it fails
    CLOSED: any other provider failure is re-raised as an :class:`AdmissionRefused`
    with a generic reason naming the provider."""
    if event not in EVENTS:
        raise ValueError(f"unknown run-hook event {event!r}; one of {EVENTS}")
    ran: list[str] = []
    data = {"event": event, **payload}
    for name, hook in load_run_hooks().items():
        try:
            result = hook(event, data)
            if inspect.isawaitable(result):
                await result
            ran.append(name)
        except AdmissionRefused as e:
            if event not in REFUSABLE_EVENTS:
                _log.warning("run hook %r refused on %s, which cannot refuse — ignored: %s", name, event, e.reason)
                continue
            _log.info("run hook %r refused %s %s: %s", name, event,
                      payload.get("run_id") or payload.get("pipeline_ref", ""), e.reason)
            raise
        except Exception as e:  # noqa: BLE001 — one failing hook must not stop the others or the run
            if event in FAIL_CLOSED_EVENTS:
                _log.warning("run hook %r failed on %s — refusing (this event fails closed)", name, event,
                             exc_info=True)
                raise AdmissionRefused(
                    f"run hook {name!r} could not check this launch ({type(e).__name__}); "
                    f"the launch is refused until it can") from e
            _log.warning("run hook %r failed on %s", name, event, exc_info=True)
    return ran


__all__ = ["AdmissionRefused", "ENTRY_POINT_GROUP", "EVENTS", "FAIL_CLOSED_EVENTS", "LEG_FINISHED",
           "PIPELINE_FINISHED", "PIPELINE_LAUNCHING", "REFUSABLE_EVENTS", "RUN_ADMITTING", "RUN_FAILED",
           "RUN_FINISHED", "RUN_LAUNCHED",
           "RUN_EVENTS_INGESTED", "WORKSPACE_CREATED",
           "RunHook", "fire", "load_run_hooks", "run_hook_load_failures"]
