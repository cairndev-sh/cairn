"""``cairn demo`` seeder (§394) — populate an opt-in ``demo`` workspace so the
console's Runs / Pilot / Audit surfaces have something to look at, and a HITL
approval is demoable, without anyone hand-running a workflow.

Design decisions (issue #394, resolved — do NOT re-ask):

  - The demo **workspace** is strictly **opt-in**: this module runs only when a
    user invokes ``cairn demo``. It NEVER auto-seeds on OSS install / server
    startup — that is the whole point of the opt-in gate.
  - **Idempotent.** Re-running seeds no duplicates: workspace ``put`` is an
    upsert, enabling is content-addressed (unchanged definitions dedup), and
    every provider writes **deterministic** ids — a second run overwrites the
    first.
  - **Archivable.** Teardown reuses the existing workspace soft-delete
    lifecycle (:func:`cairn.core.workflow_registry.archive_workspace` +
    ``Workspace.status = "archived"``); runs / events / audit are never deleted.

This module owns **no new store**. It composes the existing workspace store, the
workflow registry, and the run store (:func:`cairn.core.storage.default_store`) —
the same surfaces the HTTP API uses.

2026-10-12 (#2418, ADR-0120 D4): the core names no pipeline it seeds. Every
provider is an entry point of the ``cairn.seeds`` group
(:mod:`cairn.core.pack.seeds`); :func:`seed_demo_workspace` upserts the
workspace and runs each installed provider in it. The core ships one provider,
:func:`seed_hello_cairn`, which seeds the example pack (``examples/hello-cairn``,
#2423): two finished pipelines and one paused at its ``review`` gate. The
platform's provider (``platform-demo``: the three flagship flows and the
governed scenarios of #2303/#2305, :mod:`cairn_platform.showcase_seed`) registers the
same way and leaves with the platform (#2421). Every seeded run is a leg of a
declared pipeline (ADR-0106 D1).
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from cairn.core._time import utc_now_iso
from cairn.core.engine.pipeline import plan_pipeline
from cairn.core.engine.pipeline_gate import gate_context_for
from cairn.core.engine.pipeline_resolve import all_pipeline_defs
from cairn.core.engine.schema import TriggerEvent
from cairn.core.pack.seeds import SeedProvider, SeedReport, load_seed_providers
from cairn.core.storage import default_store
from cairn.core.storage.pipeline_store import PipelineRecord, StepState, default_pipeline_store
from cairn.core.storage.protocols import Run, RunStore
from cairn.core.storage.reviews import ReviewRecord, build_review_store, pipeline_review_id
from cairn.core.workflow_registry import (
    archive_workspace,
    enable_pipeline,
)
from cairn.core.workspace import (
    Workspace,
    get_workspace_store,
    set_current_workspace,
    reset_current_workspace,
)

logger = logging.getLogger(__name__)

#: The opt-in demo workspace id. A fixed slug so re-runs target the same one.
DEMO_WORKSPACE = "demo"

@dataclass
class SeedResult:
    """What one :func:`seed_demo_workspace` run did — rendered by the CLI."""

    workspace_id: str
    enabled: list[str]
    #: The scenario pipelines' leg runs (each terminal: done or failed). The
    #: name predates pipelines; ``POST /demo/seed`` returns it.
    completed_runs: list[str]
    #: Bare runs left at ``awaiting_approval``. Always empty now — the approval
    #: to action is a pipeline gate; kept because ``POST /demo/seed`` returns it.
    paused_runs: list[str] = field(default_factory=list)
    #: Scenario pipelines and the gates left for a human.
    pipelines: list[str] = field(default_factory=list)
    pending_gates: list[str] = field(default_factory=list)
    #: The ``cairn.seeds`` providers that ran, in order (#2418).
    providers: list[str] = field(default_factory=list)
    #: What a provider could not seed here, prefixed with the provider's name.
    skipped: list[str] = field(default_factory=list)

    @property
    def total_runs(self) -> int:
        return len(self.completed_runs) + len(self.paused_runs)


def demo_inputs_for(ref: str) -> dict[str, Any]:
    """The first declared demo's (resolved) inputs for a workflow, or ``{}``.

    Reuses the UIContract (§394) so a seeded run's inputs match exactly what
    the console's "Try with sample data" button would prefill — one source of
    truth, no drift. Public (#297): a ``cairn.seeds`` provider seeds with it."""
    try:
        from cairn.core.ui_contract import build_ui_contract  # lazy: keep ask off the core import path
        contract = build_ui_contract(ref)
        if contract.demos:
            return dict(contract.demos[0].inputs)
    except Exception:  # noqa: BLE001 — best-effort; a run with empty inputs still populates the views
        pass
    return {}


_demo_inputs_for = demo_inputs_for  # the earlier private name; same function


async def seed_demo_workspace(
    *,
    workspace_id: str = DEMO_WORKSPACE,
    store: Optional[RunStore] = None,
    registry: Optional[Any] = None,
    pipelines: Optional[Any] = None,
    reviews: Optional[Any] = None,
    suggestions: Optional[Any] = None,
    ledger: Optional[Any] = None,
    history: Optional[Any] = None,
    now: Optional[datetime] = None,
    providers: Optional[Mapping[str, SeedProvider]] = None,
) -> SeedResult:
    """Seed (idempotently) the opt-in demo workspace and return what it did.

    Steps:
      1. Upsert the ``demo`` :class:`Workspace` (active).
      2. Run every ``cairn.seeds`` provider (:mod:`cairn.core.pack.seeds`) inside
         it, in name order; ``providers`` replaces discovery (tests). A provider
         that raises is logged and reported in ``skipped``; the others still run.
         With no provider installed nothing is seeded and a warning says so.

    Every store defaults to the process store the HTTP API reads; tests pass
    their own. ``suggestions`` / ``ledger`` / ``history`` are passed through to
    the providers as given (a provider defaults the ones it uses). ``now``
    anchors every timestamp (default: the current time). Every record is stamped
    with ``workspace_id`` and uses a deterministic id, so a second call is a
    no-op-shaped upsert (no duplicates)."""
    found = dict(providers) if providers is not None else load_seed_providers()

    # 1. workspace (idempotent upsert). Preserve an existing (e.g. archived →
    #    active) record's name but force status back to active on re-seed.
    ws_store = get_workspace_store()
    existing_ws = await ws_store.get(workspace_id)
    ws = existing_ws or Workspace(id=workspace_id, name="Demo")
    ws.status, ws.archived_at = "active", None
    await ws_store.put(ws)

    result = SeedResult(workspace_id=workspace_id, enabled=[], completed_runs=[])
    if not found:
        logger.warning("no `cairn.seeds` provider is installed: the %r workspace is empty "
                       "(see `cairn doctor`)", workspace_id)
        return result
    run_store = store or default_store()
    review_store = reviews if reviews is not None else build_review_store()
    stores = {"now": now or datetime.now(timezone.utc), "runs": run_store,
              "pipelines": pipelines if pipelines is not None else default_pipeline_store(),
              "reviews": review_store, "registry": registry,
              "suggestions": suggestions, "ledger": ledger, "history": history}
    # The seeder runs "as" the demo workspace so enable_* /
    # current_workspace()-stamped records land in the right scope.
    token = set_current_workspace(workspace_id)
    try:
        for name, provider in found.items():
            try:
                rep = await provider(workspace_id, **stores)
            except Exception as e:  # noqa: BLE001 — one provider must not empty the demo
                logger.warning("seed provider %r failed: %s", name, e, exc_info=True)
                result.skipped.append(f"{name}: failed ({type(e).__name__}: {e})")
                continue
            result.providers.append(name)
            result.enabled += rep.enabled
            result.completed_runs += rep.runs
            result.pipelines += rep.pipelines
            result.pending_gates += rep.pending_gates
            result.skipped += [f"{name}: {s}" for s in rep.skipped]
    finally:
        reset_current_workspace(token)
        if reviews is None and hasattr(review_store, "close"):
            await review_store.close()  # we built it; the server keeps its own
    return result


# ─── the core's own provider: the example pack (#2423) ──────────────────────

#: The example pack's pipeline. Not a dependency: the pack is found through the
#: pack registry (an installed ``cairn-pack-hello`` or ``CAIRN_PACK_PATH``), and
#: when it is absent the provider skips it.
HELLO_PIPELINE = "hello-cairn/profile-and-publish"

#: The example's three stories. Demo data: the outputs are what the shipped
#: ``data/sample.jsonl`` profiles to; nothing is executed. A story names only
#: where it stops (``stop``: step id and its status); every step before it in
#: the LOADED plan is ``done`` and every step after it ``pending``
#: (:func:`_story_steps`), so the seeded records follow the shipped pipeline
#: as it gains steps (#2608). ``stop: None`` runs the whole plan.
_HELLO_PROFILE = {"ok": True, "verdict": "pass", "record_count": 20, "detected_format": "jsonl",
                  "note": "20 records; roles text, label resolved"}
_HELLO_STORIES: tuple[dict[str, Any], ...] = (
    {"key": "published", "ago_h": 50, "status": "done", "inputs": {}, "stop": None,
     "approver": "alice", "rationale": "Profile passed and the staged summary reads right."},
    {"key": "blocked", "ago_h": 26, "status": "failed", "inputs": {"min_rows": 1000},
     "stop": ("data-ok", "skipped")},
    {"key": "waiting", "ago_h": 2, "status": "paused", "inputs": {},
     "stop": ("review", "awaiting_approval")},
)


def _story_steps(story: Mapping[str, Any], plan: Any) -> dict[str, str]:
    """Each plan step's status for one story, in plan order: ``done`` up to the
    story's stop step, the stop's own status there, ``pending`` after it. A stop
    the plan does not contain raises (the provider is then reported skipped)
    rather than seeding a record the pipeline could never produce."""
    ids = [step.id for step in plan.steps]
    stop = story["stop"]
    if stop is None:
        return dict.fromkeys(ids, "done")
    stop_id, stop_status = stop
    if stop_id not in ids:
        raise ValueError(f"demo story {story['key']!r} stops at {stop_id!r}, which the plan of "
                         f"{HELLO_PIPELINE} does not contain")
    at = ids.index(stop_id)
    return {sid: "done" if i < at else stop_status if i == at else "pending" for i, sid in enumerate(ids)}


def _hello_id(prefix: str, *parts: str, n: int = 12) -> str:
    return prefix + hashlib.sha1("|".join(("hello", *parts)).encode("utf-8")).hexdigest()[:n]


def _hello_def() -> Any:
    """The example pipeline's definition, or ``None`` when the pack is not
    loaded. Loads packs when none are registered (the CLI does not load them)."""
    defs = all_pipeline_defs()
    if not defs:
        from cairn.core.pack.loader import load_all_packs  # noqa: PLC0415 — only when nothing is loaded yet
        load_all_packs(strict=False)
        defs = all_pipeline_defs()
    return next((d for d in defs if f"{d.pack}/{d.name}" == HELLO_PIPELINE), None)


def _hello_output(step_id: str) -> dict[str, Any]:
    if step_id == "profile":
        return dict(_HELLO_PROFILE)
    if step_id == "redact":  # the sample's summary holds nothing the recognisers match
        return {"ok": True, "text": _HELLO_PROFILE["note"], "redacted": False, "span_count": 0, "kinds": []}
    if step_id in ("stage", "publish"):
        store = "hello-staging" if step_id == "stage" else "hello-published"
        return {"ok": True, "key": "summary", "uri": f"file://~/.cairn/artifacts/{store}/summary",
                "size_bytes": len(_HELLO_PROFILE["note"])}
    if step_id == "promote":
        return {"tag": "hello-sample@published", "artifact_id": _hello_id("", "hello-sample", n=40),
                "previous_artifact_id": "", "promoted": True}
    if step_id == "audit":
        return {"ok": True, "hash": _hello_id("", "audit", "hello.dataset.published", n=40)}
    return {}  # engine-check: one noop stage, no output


async def _seed_hello_story(ws: str, story: dict[str, Any], plan: Any, now: datetime, rep: SeedReport, *,
                            runs: Any, pipelines: Any, reviews: Any) -> None:
    """One story: its legs back to back in plan order, the review a fired
    approval gate opens (only when absent), then the pipeline record."""
    key = story["key"]
    pid = _hello_id("pl_", ws, key, n=16)
    t = created = now - timedelta(hours=story["ago_h"])
    state: dict[str, StepState] = {}
    outputs: dict[str, dict[str, Any]] = {}
    statuses = _story_steps(story, plan)
    for step in plan.steps:
        status = statuses[step.id]
        if status == "pending":
            state[step.id] = StepState()
            continue
        start, t = t, t + timedelta(minutes=1)
        if step.ref.startswith("gate:"):
            waiting, blocked = status == "awaiting_approval", status == "skipped"
            verdict = None if waiting else "gate_blocked" if blocked else "approved" if step.approval else "data"
            if step.approval:
                rid = pipeline_review_id(pid, step.id)
                if waiting:
                    rep.pending_gates.append(f"{pid}/{step.id}")
                if await reviews.get_review(rid, workspace_id=ws) is None:
                    await reviews.open_review(ReviewRecord(
                        review_id=rid, subject_ref=f"{pid}/{step.id}", title=f"Approve {step.id}",
                        pipeline_id=pid, step_id=step.id, workspace_id=ws,
                        gate_snapshot=gate_context_for(step, outputs=outputs, governance=plan.governance).as_dict(),
                        lineage={sid: st.run_id for sid, st in state.items() if st.run_id},
                        status="open" if waiting else "approved", reviewer=story.get("approver", ""),
                        rationale=story.get("rationale", ""), created_at=start.isoformat(),
                        decided_at=None if waiting else t.isoformat()))
            state[step.id] = StepState(
                status=status, verdict=verdict, approver=None if waiting else story.get("approver"),
                started_at=start.isoformat(), awaiting_since=start.isoformat() if step.approval else None,
                ended_at=None if waiting else t.isoformat())
            continue
        output = _hello_output(step.id)
        run = Run(
            id=_hello_id("demo-", ws, key, step.id),
            trigger=TriggerEvent(source="manual", payload={"leg": step.ref, "demo": True}),
            lane="background", priority=100, status=status,
            fingerprint=step.ref, template_ref=step.ref, workspace_id=ws,
            created_at=start.isoformat(), started_at=start.isoformat(), ended_at=t.isoformat(),
            bundle_pipeline_id=pid, step_id=step.id,
            result_payloads=[{"output": output}],
            final_verdict=str(output.get("verdict") or ""),
        )
        await runs.create(run)  # upsert on the deterministic id
        if not await runs.get_events(run.id):
            leg = {"leg": step.ref, "step_id": step.id}
            for typ in ("running", status):
                await runs.append_event(run.id, typ, leg)
        outputs[step.id] = output
        state[step.id] = StepState(run_id=run.id, status=status, verdict=run.final_verdict or None,
                                   started_at=start.isoformat(), ended_at=t.isoformat())
        rep.runs.append(run.id)
    await pipelines.put(PipelineRecord(
        pipeline_id=pid, bundle_ref=f"pipeline:{HELLO_PIPELINE}", pipeline_name=plan.name,
        status=story["status"], plan=plan, step_state=state, inputs=dict(story["inputs"]),
        trigger={"source": "manual", "payload": {"demo": True}}, workspace_id=ws,
        governance=dict(plan.governance or {}),
        cursor=[sid for sid, st in state.items() if st.status.startswith("awaiting_")],
        created_at=created.isoformat(), ended_at=None if story["status"] == "paused" else t.isoformat()))
    rep.pipelines.append(pid)


async def seed_hello_cairn(workspace_id: str, *, now: datetime, runs: Any, pipelines: Any, reviews: Any,
                           registry: Any = None, **_: Any) -> SeedReport:
    """The core's ``cairn.seeds`` provider (#2418): install the example pack's
    pipeline into the workspace and seed three runs of it — one published after
    a person approved it, one stopped by its data gate, one waiting at the
    ``review`` gate. Skipped (reported, not raised) when the example pack is not
    loaded: install ``cairn-pack-hello`` or set ``CAIRN_PACK_PATH``."""
    rep = SeedReport()
    d = _hello_def()
    if d is None:
        logger.info("demo: %s skipped: the example pack is not loaded", HELLO_PIPELINE)
        rep.skipped.append(f"{HELLO_PIPELINE}: the example pack is not loaded")
        return rep
    if await enable_pipeline(d, workspace_id=workspace_id, store=registry):
        rep.enabled.append(HELLO_PIPELINE)
    plan = plan_pipeline(d)
    for story in _HELLO_STORIES:
        await _seed_hello_story(workspace_id, story, plan, now, rep, runs=runs, pipelines=pipelines,
                                reviews=reviews)
    return rep


async def archive_demo_workspace(
    *,
    workspace_id: str = DEMO_WORKSPACE,
    registry: Optional[Any] = None,
) -> int:
    """Tear the demo workspace down via the shared soft-delete lifecycle (#258).

    Marks the workspace ``archived`` (hidden from the switcher) and flips its
    installed-workflow records inactive. Runs / events / audit are NEVER deleted
    — the demo can be restored (re-seeded) later. Returns the count of workflow
    records deactivated."""
    ws_store = get_workspace_store()
    ws = await ws_store.get(workspace_id)
    if ws is not None:
        ws.status, ws.archived_at = "archived", utc_now_iso()
        await ws_store.put(ws)
    return await archive_workspace(workspace_id, store=registry)
