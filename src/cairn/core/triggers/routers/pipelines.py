"""Bundle pipelines (ADR-0069, #746) — launch + inspect + HITL control.

Extracted verbatim from server.py (#1191). The router owns the pipeline
workspace guard; the runs guard stays with the runs surface.
"""
import asyncio
import re
import threading
from typing import Annotated, Any, Callable, Optional, TypeVar

from fastapi import APIRouter, Depends, HTTPException

from cairn.core.approval.enforce import approver_allowed, approver_names
from cairn.core.dispatch.pipeline_bridge import findings_of, reconstruct_step_output
from cairn.core.engine.pipeline_gate import required_approvers
from cairn.core.refs import bare_ref, pack_key, ref_key  # ONE ref spelling (#2126)
from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import _oauth2_scheme, auth_enabled, caller_identity, require_auth
from cairn.core.triggers.routers.reviews import decision_actor
from cairn.core.workspace import current_workspace

# #1789 — same slug rule the workflow preset routes use: `:` and `/` would break
# the (workspace:ref:name) preset key, so names stay slug-safe.
_PRESET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _-]{0,39}$")

_T = TypeVar("_T")
# #2317 — the definition listings (`/pipelines/installed`, `/flows`) are
# synchronous, CPU-bound bodies: a cold discovery imports every operator and
# resolves every `$inherit` contract (13–35 s on a laptop), a warm one still
# re-reads the packs (~0.5–2 s). Run on the event loop, they stalled EVERY
# other request behind them — a 10 ms `/runs` took as long as the listing it
# queued behind, which is the "Runs shows a skeleton for 16 s" of the issue.
# One at a time, as they always were, but on a worker thread: discovery imports
# operator modules, and two threads importing the same circular graph can
# deadlock in importlib, so the lock keeps the old serial order.
_LISTING_LOCK = threading.Lock()


async def _off_loop(fn: Callable[..., _T], *args: Any) -> _T:
    """Run ``fn(*args)`` on a worker thread, one listing at a time (#2317).

    ``asyncio.to_thread`` copies the context, so ``current_workspace()`` reads
    the caller's workspace inside ``fn`` exactly as it did on the loop."""
    def locked() -> _T:
        with _LISTING_LOCK:
            return fn(*args)
    return await asyncio.to_thread(locked)


def _valid_preset_name(name: str) -> bool:
    return bool(_PRESET_NAME.match(name or ""))


# ── the flow LIST (#2110, ADR-0084 D2) ───────────────────────────────────────


def pipeline_demos(d: Any, memo: Optional[dict[tuple[str, str], list[Any]]] = None) -> list[dict[str, Any]]:
    """The sample tiers a pipeline can offer, resolved from its LEGS.

    ADR-0106 D5 moved `[[demo.<flow>]]` next to the flow it prefills, and D1
    means a flow is never launched on its own — so the tiers a human clicks are
    the ones on the pipeline that RUNS that flow. Resolved HERE, on the engine,
    for the reason #2126 exists: a console joining a pipeline to its leg's flow
    would be joining on a ref spelling, and that join has silently returned
    nothing three times in three days.

    A tier is offered only when EVERY key it prefills is an input this pipeline
    declares. A wrapper that renames a key, or `$hide`s it, would otherwise show
    a chip that fills fields the form does not have — a button that half-works
    is worse than no button.

    ``memo`` (#2317) is a caller-owned ``(pack, flow) -> tiers`` dict, so one
    listing reads each flow file once: 39 pipelines asked for 35 flows 79 times.
    """
    from cairn.core.pack.demos import demos_for  # noqa: PLC0415 - cycle: pack → engine
    from cairn.core.pack.registry import get_packs  # noqa: PLC0415 - as above

    declared = set((d.inputs or {}).keys())
    if not declared:
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for step in d.all_steps():
        ref = bare_ref(str(getattr(step, "ref", "") or ""))
        if "/" not in ref:
            continue
        pack_name, _, flow_name = ref.partition("/")
        # `pack_key` is THE fold (#2126) — a hand-rolled `.replace("_", "-")`
        # here was the ninth copy of it, and its own gate said so.
        want = pack_key(pack_name)
        pack = next((p for p in get_packs() if pack_key(getattr(p, "name", "")) == want), None)
        if pack is None:
            continue
        key = (getattr(pack, "name", ""), flow_name)
        if memo is None:
            tiers = demos_for(pack, flow_name)
        elif (tiers := memo.get(key)) is None:
            tiers = memo[key] = demos_for(pack, flow_name)
        for demo in tiers:
            prefill = dict(getattr(demo, "inputs", None) or {})
            if not prefill or not set(prefill).issubset(declared):
                continue
            key = str(getattr(demo, "tier", "") or getattr(demo, "title", "") or len(out))
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "tier": getattr(demo, "tier", "") or "",
                "title": getattr(demo, "title", "") or "",
                "description": getattr(demo, "description", "") or "",
                "inputs": prefill,
            })
    return out


def flow_usage() -> dict[tuple[str, str], list[dict[str, str]]]:
    """``(leg kind, flow key) -> the installed pipelines whose legs reference it``.

    Reads the SAME discovery ``GET /pipelines/installed`` lists from
    (:func:`all_pipeline_defs`), because "who uses this flow" is a projection
    of the pipelines, not a second record to keep in sync. Definition steps,
    not planned ones: the plan flattens a gate leg's ref to its step name, and
    a leg's own ``ref`` is what names the flow.

    Keyed by the CANONICAL ref key (:func:`~cairn.core.refs.ref_key`, #2126),
    never the raw string: a leg carries its pack's DIRECTORY spelling
    (``dataset_readiness``) and its ``flow:`` kind prefix, while a flow def is
    keyed by the ``pack.toml`` name (``dataset-readiness``). One normaliser, on
    the engine, imported by every reader — this used to be a private
    ``flow_key`` in this router agreeing by hand with ``sameFlow`` in the
    console.

    Keyed by LEG KIND as well as ref. ``dataset-readiness/clean`` used to name
    BOTH ``clean.yaml`` (data) and ``clean.md`` (agentic); #2134 S2 renamed the
    stage list to ``dataset-readiness/intake`` and the pack lint now REFUSES the
    shape (ADR-0106 D9), so no first-party pack collides. The kind stays in the
    key as defence in depth: a ``flow:`` leg runs the stage list and a
    ``template:`` leg the behaviour tree, and keying on the ref alone would
    credit each of them with the other's users the moment a pack slipped past
    the lint (a dev pack, a lenient boot).
    """
    from cairn.core.engine.pipeline_resolve import (  # noqa: PLC0415 - routers ↔ engine cycle at import time
        PIPELINE_PREFIX, all_pipeline_defs,
    )

    out: dict[tuple[str, str], list[dict[str, str]]] = {}
    for d in all_pipeline_defs():
        pipeline_ref = f"{PIPELINE_PREFIX}{d.pack}/{d.name}"
        try:
            steps = d.all_steps()
        except Exception:  # noqa: BLE001 — one bad pipeline must not empty the index
            continue
        for s in steps:
            # `flow` = data/ml leg, `template` = agentic leg (ADR-0084 D2).
            # `task`/`gate` legs point at an operator or a door, not a flow.
            if s.leg_kind not in ("flow", "template") or not s.ref:
                continue
            row = {"ref": pipeline_ref, "pack": d.pack, "name": d.name,
                   "leg": s.id or s.ref}
            bucket = out.setdefault((s.leg_kind, ref_key(s.ref)), [])
            if not any(b["ref"] == row["ref"] and b["leg"] == row["leg"] for b in bucket):
                bucket.append(row)
    return out


def build_pipelines_router(dispatcher, pipeline_bridge) -> APIRouter:
    """The /pipelines + /flows surface, closed over the shared dispatcher and
    pipeline bridge."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # request models the inline block shared.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        PipelineApproveRequest,
    )

    r = APIRouter()

    def _pipeline_row(rec: Any) -> dict[str, Any]:
        """Compact pipeline summary for the list view (steps as id→status).

        `step_order` carries the PLAN's step order (#1233): `step_state` is a
        dict keyed by touch-order, so without it the console's step chips
        rendered scrambled (train · readiness · promote-model) while the detail
        view — which reads the plan — ordered correctly. Plan steps first, then
        any state-only strays, so nothing ever disappears from the row.
        """
        plan_ids = [s.id for s in getattr(rec.plan, "steps", None) or []]
        order = [sid for sid in plan_ids if sid in rec.step_state]
        order += [sid for sid in rec.step_state if sid not in plan_ids]
        # #2030 — the run a step became, so a console reading a run can find the
        # pipeline that owns it (clone-from-checkpoint, the chain strip) without
        # an N+1 fetch of every pipeline's detail. Additive: only present when set.
        run_ids = {sid: rec.step_state[sid].run_id for sid in order if rec.step_state[sid].run_id}
        return {
            "pipeline_id": rec.pipeline_id, "bundle_ref": rec.bundle_ref,
            "name": rec.pipeline_name, "status": rec.status,
            "steps": {sid: rec.step_state[sid].status for sid in order},
            "step_order": order,
            "run_ids": run_ids,
            "created_at": rec.created_at, "ended_at": rec.ended_at,
        }

    async def _pipeline_in_ws_or_404(pipeline_id: str) -> Any:
        rec = await pipeline_bridge.store.get(pipeline_id)
        if rec is None or rec.workspace_id != current_workspace():
            raise HTTPException(status_code=404, detail="pipeline not found")
        return rec

    @r.post("/pipelines/preview-config", dependencies=[Depends(require_auth)])
    async def preview_pipeline_config(body: dict[str, Any]) -> dict[str, Any]:
        """Compose the config surface for a bundle's pipeline BEFORE it runs (#930),
        so the launch modal can show governance (env-owned) vs operational knobs.
        Body: ``{steps: [{id, ref}, ...]}`` — the pipeline's step id+ref pairs (from
        ``GET /pipelines/bundles``). Same composer the running-pipeline config view uses."""
        from cairn.core.engine.workflow_config import compose_pipeline_config  # noqa: PLC0415

        steps = [(str(s.get("id", "")), str(s.get("ref", ""))) for s in (body.get("steps") or []) if s.get("ref")]
        return {"steps": compose_pipeline_config(steps)}

    @r.get("/flows", dependencies=[Depends(require_auth)])
    async def list_flows() -> dict[str, Any]:
        """Every installed flow — ALL THREE KINDS (#2110, ADR-0084 D2).

        The console's Flows list was fed by ``/registry/installed-workflows``,
        which is the AGENTIC library only, so every row's Kind column read
        ``agentic``; the data and ml flows (``flows/*.yaml`` — the ``flow:``
        legs of every fine-tune pipeline) existed in the engine, drew correctly
        inside a pipeline graph, and were listed NOWHERE. ADR-0084 D2 says ONE
        concept in three kinds; this is the list that says so.

        No new model and no new store: data/ml rows come from
        :func:`load_flow_defs` (the loader ``/flows/<ref>/stages`` resolves
        through), agentic rows from the workflow registry ``/registry/
        installed-workflows`` reads, and ``used_by`` from the
        :func:`all_pipeline_defs` discovery ``/pipelines/installed`` lists.

        A def that would NOT load is listed WITH ITS REASON rather than
        silently missing (#2069): a dropped def is indistinguishable from one
        nobody wrote, and the resulting "flow X does not resolve" names neither
        the stage nor the pack to install.
        """
        from cairn.core.dataflow.defs import (  # noqa: PLC0415 - routers ↔ core cycle at import time
            load_flow_defs, unresolvable_flow_defs,
        )
        from cairn.core.eligibility import workflow_meta  # noqa: PLC0415 - keeps the operator registry off the import path
        from cairn.core.engine.workflow_config import collect_template_operators  # noqa: PLC0415 - as above
        from cairn.core.features import hidden_for  # noqa: PLC0415 - as above
        from cairn.core.pack.curation import (  # noqa: PLC0415 - as above
            DEFAULT_VISIBILITY, effective_visibility,
        )
        from cairn.core.workflow_registry import (  # noqa: PLC0415 - as above
            get_workflow_registry, sync_discovered_workflows,
        )
        from cairn.core.workspace import DEFAULT_WORKSPACE  # noqa: PLC0415 - as above

        # #2317 — discovery and the flow-dir scan are synchronous; off the loop.
        used = await _off_loop(flow_usage)
        # #2173 — the admin's per-workspace hide rules, read ONCE for the list.
        # Keyed by `flow_key`, so a rule written against either spelling of a
        # pack name applies to the row whichever spelling it carries (#2126).
        hidden = (await hidden_for(workspace_id=current_workspace()))["flow"]
        rows: list[dict[str, Any]] = []

        def _row(ref: str, kind: str, **extra: Any) -> dict[str, Any]:
            pack, _, name = ref.partition("/")
            row: dict[str, Any] = {
                "ref": ref, "kind": kind, "pack": pack, "name": name or ref,
                "title": "", "description": "", "version": "",
                "capabilities": [], "visibility": DEFAULT_VISIBILITY,
                # data/ml: stages in the declared chain. agentic: the operator
                # nodes its behaviour tree binds (a frontmatter walk — the list
                # must not compile every template).
                "stage_count": 0, "node_count": 0,
                # a `flow:` leg runs a stage list, a `template:` leg a behaviour
                # tree — so a data/ml row and an agentic row at the same address
                # get their own users. The lint refuses that address clash now
                # (ADR-0106 D9), so this is defence in depth, not a live case.
                "used_by": used.get(("template" if kind == "agentic" else "flow", ref_key(ref)), []),
                # #2126 — the canonical key, stamped SERVER-side. The console
                # joins "Used by" on this and never re-derives a spelling of
                # its own; `ref` stays the spelling a reader recognises.
                "flow_key": ref_key(ref),
                "error": "",
            }
            row.update(extra)
            # #2173 — computed AFTER the update, because an agentic row's
            # DECLARED tier arrives through `extra`; computing it above would
            # have curated every agentic flow on the default instead.
            row["effective_visibility"] = effective_visibility(
                row["visibility"], hidden=row["flow_key"] in hidden)
            return row

        # ── data + ml: `flows/*.yaml` across every installed pack ────────────
        stage_defs, broken = await _off_loop(lambda: (load_flow_defs(), unresolvable_flow_defs()))
        for ref, d in stage_defs.items():
            rows.append(_row(ref, d.kind, title=d.name or "",
                             description=(d.description or "").strip(),
                             stage_count=len(d.stages)))
        # #2069 — listed WITH the reason, never dropped. Kind is unknown: the
        # def never parsed far enough to declare one.
        for ref, why in broken.items():
            rows.append(_row(ref, "", error=why))

        # ── agentic: the same registry read `/registry/installed-workflows`
        # does, including the `default`-workspace auto-sync (#258) so a fresh
        # single-node deployment shows its discoverable flows here too.
        ws = current_workspace()
        store = get_workflow_registry()
        recs = await store.list_installed(ws)
        if not recs and ws == DEFAULT_WORKSPACE:
            try:
                await sync_discovered_workflows(workspace_id=ws, store=store)
                recs = await store.list_installed(ws)
            except Exception:  # noqa: BLE001 — a sync failure must not empty the list
                pass
        seen: set[str] = set()
        for rec in recs:
            ref = rec.workflow_ref
            if rec.status != "active" or ref in seen:
                continue  # one row per ref: a ref can be installed at several versions
            seen.add(ref)
            try:
                m = workflow_meta(ref)
                title, desc, visibility = m.title, m.description, m.visibility
            except Exception:  # noqa: BLE001 — metadata is decoration, not the listing
                title, desc, visibility = "", "", DEFAULT_VISIBILITY
            try:
                nodes = len(collect_template_operators(ref))
            except Exception:  # noqa: BLE001 — an uncompilable flow still gets a row
                nodes = 0
            rows.append(_row(
                ref, "agentic", title=title, description=desc,
                visibility=visibility, node_count=nodes,
                version=rec.version or "",
                capabilities=list(rec.required_capabilities or []),
            ))

        rows.sort(key=lambda f: (f["ref"], f["kind"]))
        return {"count": len(rows), "flows": rows}

    @r.get("/flows/{pack}/{name}/stages", dependencies=[Depends(require_auth)])
    async def flow_stages(pack: str, name: str) -> dict[str, Any]:
        """The stages a data/ml flow leg expands into (#1136).

        #1077 annotated a RUNNING flow with live stage progress; this is the
        static list, so a pipeline's leg chain can be expanded before anything
        runs. Same resolution the leg executor uses, so what the console shows
        is what the engine will submit.
        """
        # kind-agnostic on purpose: a leg chain shows data AND ml legs, and the
        # kind-matched doors (ADR-0082 D7) police EXECUTION, not inspection.
        from cairn.core.dataflow.defs import load_flow_defs, resolve_flow_ref  # noqa: PLC0415 - cycle

        ref = f"{pack}/{name}"
        try:
            stages = resolve_flow_ref(ref, {})
        except Exception as e:  # noqa: BLE001 — an unresolvable flow is a 404, not a 500
            raise HTTPException(status_code=404, detail=f"no flow {ref}: {e}") from e
        kind = getattr(load_flow_defs().get(ref), "kind", "")
        return {"ref": ref, "kind": kind, "stages": [
            # StageRef.stage is the kernel address (e.g. `ingest.normalize`);
            # `gate` marks a stage the control plane pauses at.
            {"stage": s.stage, "gate": getattr(s, "gate", None) or "",
             "resources": getattr(s, "resources", None) or None}
            for s in (stages or [])
        ]}

    @r.get("/pipelines/{pack}/{name}/explain", dependencies=[Depends(require_auth)])
    async def explain_pipeline_route(
        pack: str, name: str, environment: str = "", preset: str = "",
    ) -> dict[str, Any]:
        """Dry-run how every step of an INSTALLED pipeline will be triggered (#1792).

        The same projection ``cairn pipeline explain`` renders, over the
        authenticated door — one function, so the console (later) and the
        terminal cannot be told different things about the same pipeline. Every
        field is computed by calling the resolution code that executes: plan,
        flow-def lookup, engine selection, the approval plane, the policy
        injector, the timeout/retry parsers.

        ``preset`` expands through the SAME resolver a launch uses, so
        "explain what `--preset nightly` would do" answers about the values
        that would really be sent.

        ADR-0101 D3 (#1975): the projection is built by
        :func:`cairn.core.dispatch.start.explain_pipeline_ref`, which ``POST
        /start`` with ``dry_run: true`` calls for a pipeline ref — so the
        console's explain view and the one-start dry run cannot be told
        different things about the same pipeline.
        """
        from cairn.core.dispatch.pipeline_launch import (  # noqa: PLC0415 - cycle
            PipelineNotFoundError,
        )
        from cairn.core.dispatch.start import explain_pipeline_ref  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_resolve import AmbiguousRefError  # noqa: PLC0415 - cycle
        from cairn.core.pipeline_presets import PresetNotFoundError  # noqa: PLC0415 - cycle

        try:
            return await explain_pipeline_ref(
                f"{pack}/{name}", environment=environment or "", preset=preset,
                workspace_id=current_workspace())
        except (PipelineNotFoundError, PresetNotFoundError) as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except AmbiguousRefError as e:  # pragma: no cover — the prefix disambiguates
            raise HTTPException(status_code=400, detail=str(e)) from e

    @r.get("/pipelines/installed", dependencies=[Depends(require_auth)])
    async def list_installed_pipelines() -> dict[str, Any]:
        """The installed pipeline DEFINITIONS — what the console can launch.

        Distinct from ``GET /pipelines``, which lists running instances. P8
        (ADR-0084): install is what makes something console-visible, so this
        reads the same discovery the registry installs from.
        """
        from cairn.core.dataflow.defs import load_flow_defs  # noqa: PLC0415 - cycle
        from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline import plan_pipeline  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_resolve import (  # noqa: PLC0415 - cycle
            PIPELINE_PREFIX, all_pipeline_defs, assert_runnable,
        )
        from cairn.core.features import hidden_for  # noqa: PLC0415 - routers ↔ core cycle
        from cairn.core.pack.curation import effective_visibility  # noqa: PLC0415 - as above

        # #2173 — the admin's per-workspace hide rules, read ONCE for the list.
        hidden = (await hidden_for(workspace_id=current_workspace()))["pipeline"]
        # #2317 — everything below is synchronous discovery and planning: off
        # the event loop, so the rest of the console is not queued behind it.
        def _listing() -> dict[str, Any]:
            # #2340 — ONE flow-dir scan for the whole list; per leg it was 31 full
            # re-parses of every flow YAML on each page load.
            flow_defs = load_flow_defs()
            demos_memo: dict[tuple[str, str], list[Any]] = {}  # #2317 — one read per flow
            out = []
            for d in all_pipeline_defs():
                try:
                    assert_runnable(d, flow_defs=flow_defs)
                    blocked = ""
                except NotImplementedError as e:
                    blocked = str(e)
                try:
                    # kind comes from the DEFINITION: the plan flattens a gate leg's
                    # ref to its step name, which would read as a template.
                    kinds = {s.name: s.leg_kind for s in d.all_steps()}
                    # ADR-0107 D4 (#2145) — labels come from the DEFINITION for the
                    # same reason `kinds` does, plus one of its own: the plan is the
                    # execution IR and deliberately carries no display data, so an
                    # annotated pipeline plans identically to an unannotated one
                    # (pinned by `test_display_fields`).
                    labels = {s.name: (getattr(s, "label", "") or "") for s in d.all_steps()}
                    legs = [{"id": s.id, "ref": s.ref,
                             "kind": (k := kinds.get(s.id) or leg_kind_of(s.ref)),
                             # #2126 — the canonical key for the FLOW this leg runs,
                             # so the flow page's "Used by" joins on a server answer
                             # instead of normalising `dataset_readiness` itself.
                             # Empty for a task/gate leg: those name an operator or
                             # a door, and a console must not join one onto a flow.
                             "flow_key": ref_key(s.ref) if k in ("flow", "template") else "",
                             # "" when the pack has not annotated this leg; every
                             # console surface falls back to `id`, so an unlabelled
                             # pipeline renders exactly as it does today.
                             "label": labels.get(s.id, ""),
                             "depends_on": list(s.depends_on), "gate": s.gate or "",
                             # #1251 — STOP vs ROUTE are different fields now; the
                             # console must be able to show an optional leg as such.
                             "when": s.when or ""}
                            for s in plan_pipeline(d).steps]
                except Exception as e:  # noqa: BLE001 — a bad pipeline must not 500 the list
                    legs, blocked = [], blocked or f"cannot plan: {e}"
                out.append({
                    "ref": f"{PIPELINE_PREFIX}{d.pack}/{d.name}",
                    "pack": d.pack, "name": d.name,
                    "description": getattr(d, "description", "") or "",
                    # #1566 — annotated, not filtered: the picker curates on this,
                    # `cairn pipeline ls` can still show everything. This is the
                    # tier the PACK declared and it never moves — #2173's admin
                    # override lands on `effective_visibility` below, so the admin
                    # screen can say "hidden by admin" vs "internal by pack" and
                    # un-hiding restores the pack's answer instead of guessing.
                    "visibility": getattr(d, "visibility", "public"),
                    # #2173 — the tier a human surface curates on: `internal` when
                    # an admin hid this ref in the CALLER's workspace, else the
                    # declared tier. Computed here so every consumer — console,
                    # CLI, API — inherits the same answer.
                    "effective_visibility": effective_visibility(
                        getattr(d, "visibility", "public"),
                        hidden=ref_key(f"{d.pack}/{d.name}") in hidden),
                    # ADR-0107 D1/D2/D9 (#2145) — display metadata. Empty on a
                    # pipeline that declares none of it, which is every pipeline
                    # until a pack opts in; the console groups by pack as before.
                    "outcome": getattr(d, "outcome", "") or "",
                    "suite": getattr(d, "suite", "") or "",
                    "compare": dict(getattr(d, "compare", {}) or {}),
                    "inputs": d.inputs or {}, "legs": legs,
                    # ADR-0106 D5 — the sample tiers this pipeline can prefill, from
                    # the flows its legs run (see `pipeline_demos`).
                    "demos": pipeline_demos(d, demos_memo),
                    # non-empty ⇒ the console disables Run and shows why, rather than
                    # offering a button that fails after the first leg has spent money
                    "blocked_reason": blocked,
                })
            return {"count": len(out), "pipelines": sorted(out, key=lambda p: p["ref"])}

        return await _off_loop(_listing)

    @r.post("/pipelines/launch", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def launch_installed_pipeline(body: dict[str, Any]) -> dict[str, Any]:
        """Launch an INSTALLED pipeline by ref (#1136, ADR-0084 D9).

        Replaces the removed bundle-path launcher: you can only start something
        that is installed, so every console launch has a registry record behind
        it.
        """
        from cairn.core.dispatch.pipeline_launch import (  # noqa: PLC0415 - cycle
            GovernanceInputError, InputCoercionError, MissingInputsError,
            PipelineDriftError, PipelineNotFoundError, ServiceDoorError, launch_pipeline,
        )
        from cairn.core.engine.artifacts import WireTypeError  # noqa: PLC0415 - cycle
        from cairn.core.engine.bundle_pipeline import PipelinePlanError  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline import PipelineDefError  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_resolve import AmbiguousRefError  # noqa: PLC0415
        from cairn.core.policy.pipeline_gates import PolicyDeniedError  # noqa: PLC0415 - cycle
        from cairn.core.admission_door import AdmissionRefusedError  # noqa: PLC0415 — cycle

        # #1789 — `preset` names a saved/pack-shipped input set; `inputs` then
        # carries only the per-key overrides. Resolved SERVER-side inside
        # launch_pipeline, so this endpoint and `cairn remote launch --preset`
        # are literally the same code path (criterion 2), not two that agree.
        from cairn.core.pipeline_presets import (  # noqa: PLC0415 - cycle, as above
            PresetNotFoundError, UnknownPresetInputsError,
        )

        ref = str(body.get("ref") or "").strip()
        if not ref:
            raise HTTPException(status_code=400, detail="ref is required")
        try:
            rec = await launch_pipeline(
                ref,
                inputs=dict(body.get("inputs") or {}),
                bridge=pipeline_bridge,
                environment=body.get("environment") or None,
                workspace_id=current_workspace(),
                trigger={"source": "console"},
                expected_hash=str(body.get("expected_hash") or "") or None,
                preset=str(body.get("preset") or "") or None,
            )
        except (PipelineNotFoundError, PresetNotFoundError) as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except UnknownPresetInputsError as e:
            # criterion 5 — a stale preset key is a LOUD 400 naming the key, never
            # a silent drop. Listed before the generic ValueError arm below so the
            # message survives (it is a ValueError subclass).
            raise HTTPException(status_code=400, detail=str(e)) from e
        except PipelineDriftError as e:
            # #1255 — the caller's pinned hash disagrees with the installed
            # record: a CONFLICT with server state, not a bad request
            raise HTTPException(status_code=409, detail=str(e)) from e
        except AdmissionRefusedError as e:
            # #2190 — the admission door refused: 403 with every reason and the
            # decision itself, so the caller sees which version/check/policy.
            raise HTTPException(status_code=403, detail={
                "error": str(e), "admission": e.decision.as_record()}) from e
        except PolicyDeniedError as e:
            # #1252 — a deny policy refuses the launch, citing itself: 403, not 400
            raise HTTPException(status_code=403, detail=str(e)) from e
        except ServiceDoorError as e:
            # #2101 (ADR-0099 §1) — a service this pipeline needs is not here.
            # 503, not 400: the request is fine and the DEPLOYMENT is not, and
            # the detail carries the door's own remedy. Listed before the
            # generic ValueError arm so the message survives.
            raise HTTPException(status_code=503, detail=str(e)) from e
        except GovernanceInputError as e:
            # #2023 (closing #905) — an env-owned field arrived from the Advanced
            # form. 400 NAMING the field, never the old silent drop. Listed
            # before the generic ValueError arm so its message survives.
            raise HTTPException(status_code=400, detail=str(e)) from e
        except (AmbiguousRefError, InputCoercionError, MissingInputsError,
                NotImplementedError, PipelineDefError, PipelinePlanError,
                WireTypeError) as e:
            # #2280 — what the planner refuses (PipelinePlanError: a cycle, an
            # unresolvable contract closure; PipelineDefError: a dangling dep or
            # field; WireTypeError: a mistyped wire) is the twin of
            # PipelineNotRunnableError (a NotImplementedError): an installed def
            # this door cannot start.
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"pipeline_id": rec.pipeline_id, "status": rec.status}

    @r.post("/pipelines/preview", dependencies=[Depends(require_auth)])
    async def preview_installed_pipeline(body: dict[str, Any]) -> dict[str, Any]:
        """"Show what will run" for an installed pipeline (ADR-0102 D2, #2023).

        Body: ``{ref, inputs?, preset?}``. Returns the effective pipeline inputs
        after the preset and the declared defaults, where each value came from,
        the env-owned fields this pipeline LOCKS (what the Advanced form renders
        read-only and labelled "set by environment"), any conflict the launch
        door would refuse right now, and — for every leg whose operator resolves
        — the ``parameters`` block that leg will record when it runs.

        Side-effect free: nothing is submitted, no record is created, and no
        operator is instantiated. It is the same computation the runtime does
        (``cairn.core.operator.effective``), so the preview and the run's
        recorded parameters cannot disagree.
        """
        from cairn.core.dispatch.pipeline_launch import (  # noqa: PLC0415 - cycle
            PipelineNotFoundError, resolve_or_raise,
        )
        from cairn.core.dispatch.pipeline_preview import preview_launch  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_resolve import AmbiguousRefError  # noqa: PLC0415
        from cairn.core.pipeline_presets import (  # noqa: PLC0415 - cycle
            PIPELINE_PREFIX, PresetNotFoundError, resolve_preset,
        )

        ref = str(body.get("ref") or "").strip()
        if not ref:
            raise HTTPException(status_code=400, detail="ref is required")
        try:
            d = resolve_or_raise(ref)
            preset_inputs: dict[str, Any] = {}
            if (name := str(body.get("preset") or "").strip()):
                canon = f"{PIPELINE_PREFIX}{d.pack}/{d.name}"
                rec, _src = await resolve_preset(current_workspace(), canon, name)
                preset_inputs = dict(rec.inputs or {})
            return preview_launch(d, inputs=dict(body.get("inputs") or {}),
                                  preset_inputs=preset_inputs)
        except (PipelineNotFoundError, PresetNotFoundError) as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except AmbiguousRefError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    # ── launch presets (#1789, ADR-0071 §6) ──────────────────────────────────
    # Registered BEFORE `/pipelines/{pipeline_id}` so `/pipelines/presets` is not
    # read as a pipeline id. The pipeline ref rides a QUERY parameter rather than
    # a path segment: a canonical ref carries both `:` and `/`
    # (`pipeline:training/retrain-cycle`), and a greedy `{ref:path}` cannot
    # then be followed by a `{name}` segment for the delete.
    @r.get("/pipelines/presets", dependencies=[Depends(require_auth)])
    async def list_pipeline_presets(ref: str) -> dict[str, Any]:
        """Named input sets addressable for this pipeline — saved ones and the
        pack's seeds, merged the same way a launch resolves them (saved wins), so
        the picker lists exactly what would run."""
        from cairn.core.pipeline_presets import (  # noqa: PLC0415 - cycle: presets → engine → dispatch
            canonical_pipeline_ref, list_presets, preset_row,
        )

        canon = canonical_pipeline_ref(ref)
        rows = await list_presets(current_workspace(), canon)
        return {"ref": canon, "count": len(rows),
                "presets": [preset_row(p, src) for p, src in rows]}

    @r.post("/pipelines/presets", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def save_pipeline_preset(body: dict[str, Any]) -> dict[str, Any]:
        """Save (or replace) a named input set for a pipeline.

        Validated against the pipeline's DECLARED inputs at save — the same check
        the launch door applies at resolve. Catching a stale key here as well as
        there is deliberate: it is the difference between "you cannot save this"
        and "this will fail the next time someone launches it".
        """
        from cairn.core.engine.pipeline_resolve import AmbiguousRefError  # noqa: PLC0415
        from cairn.core.dispatch.pipeline_launch import (  # noqa: PLC0415 - cycle
            PipelineNotFoundError, resolve_or_raise,
        )
        from cairn.core.pipeline_presets import (  # noqa: PLC0415 - cycle, as above
            SOURCE_SAVED, UnknownPresetInputsError, assert_preset_keys_declared,
            canonical_pipeline_ref, preset_row,
        )
        from cairn.core.workflow_config_store import (  # noqa: PLC0415 - cycle
            InputPreset, default_workflow_config_store,
        )

        ref = str(body.get("ref") or "").strip()
        name = str(body.get("name") or "").strip()
        if not ref:
            raise HTTPException(status_code=400, detail="ref is required")
        if not _valid_preset_name(name):
            raise HTTPException(
                status_code=400,
                detail="invalid preset name (letters/digits/space/_/- , ≤40)")
        try:
            d = resolve_or_raise(ref)
        except PipelineNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except AmbiguousRefError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        preset = InputPreset(
            name=name, template_ref=canonical_pipeline_ref(ref),
            workspace_id=current_workspace(),
            inputs=dict(body.get("inputs") or {}),
            description=str(body.get("description") or "").strip(),
            created_by=str(body.get("created_by") or "").strip(),
        )
        try:
            assert_preset_keys_declared(preset, d.inputs or {}, SOURCE_SAVED)
        except UnknownPresetInputsError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        saved = await default_workflow_config_store().put_input_preset(preset)
        return preset_row(saved, SOURCE_SAVED)

    @r.delete("/pipelines/presets", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def delete_pipeline_preset(ref: str, name: str) -> dict[str, Any]:
        """Delete a SAVED preset. A pack-shipped seed of the same name is not
        removed — it reappears in the listing, which is the intended way to undo
        a local retune of a shipped default."""
        from cairn.core.pipeline_presets import canonical_pipeline_ref  # noqa: PLC0415 - cycle
        from cairn.core.workflow_config_store import default_workflow_config_store  # noqa: PLC0415

        canon = canonical_pipeline_ref(ref)
        ok = await default_workflow_config_store().delete_input_preset(
            current_workspace(), canon, name)
        return {"ref": canon, "name": name, "deleted": ok}

    @r.get("/pipelines", dependencies=[Depends(require_auth)])
    async def list_pipelines(status: Optional[str] = None) -> dict[str, Any]:
        recs = await pipeline_bridge.store.list(workspace_id=current_workspace(), status=status)
        return {"count": len(recs), "pipelines": [_pipeline_row(r) for r in recs]}

    @r.get("/pipelines/{pipeline_id}", dependencies=[Depends(require_auth)])
    async def get_pipeline(pipeline_id: str) -> dict[str, Any]:
        rec = await _pipeline_in_ws_or_404(pipeline_id)
        out = rec.model_dump(mode="json")
        # #930 — enrich each step with a summary of its governed run (cost, failure
        # reason, final verdict) so the detail view shows real per-step info, not just
        # a status dot. Best-effort: a missing/pruned run just omits the summary; the
        # per-step run lookups are few (a pipeline has a handful of steps).
        step_runs: dict[str, dict[str, Any]] = {}
        for sid, st in (rec.step_state or {}).items():
            rid = getattr(st, "run_id", None)
            if not rid:
                continue
            try:
                inv = await dispatcher.get(rid)
            except Exception:  # noqa: BLE001 — a missing run never breaks the view
                inv = None
            if inv is None:
                continue
            cost = getattr(inv, "cost", None) or {}
            step_runs[sid] = {
                "cost_usd": float(cost.get("total_usd", 0) or 0) if isinstance(cost, dict) else 0.0,
                "error": str(getattr(inv, "error", "") or ""),
                "final_verdict": str(getattr(inv, "final_verdict", "") or ""),
            }
        out["step_runs"] = step_runs
        return out

    @r.get("/pipelines/{pipeline_id}/findings", dependencies=[Depends(require_auth)])
    async def get_pipeline_findings(pipeline_id: str) -> dict[str, Any]:
        """Every non-pending step's findings, readable MID-RUN (#1790).

        The durable record already holds each step's ``run_id`` the moment the
        leg is submitted, so this is a fan-out of ``GET /runs/{id}/findings``
        over `step_state` — the same `reconstruct_step_output` door, one call
        per step, no waiting for the pipeline to finish. This is what replaces
        `docker exec` + in-container python for "what did leg N actually
        produce?" while leg N+1 is still running.

        ``pending`` steps are omitted (nothing has run, so there is nothing to
        report); every other step appears, with ``{}`` when its run carries no
        findings yet — a step present with an empty dict is a real answer
        ("running, nothing persisted yet"), a step that is absent means it
        hasn't started. A step whose run is missing/pruned reports its status
        and empty findings rather than 500ing the whole view.
        """
        rec = await _pipeline_in_ws_or_404(pipeline_id)
        steps: dict[str, Any] = {}
        for sid, st in (rec.step_state or {}).items():
            status = str(getattr(st, "status", "") or "")
            if status == "pending":
                continue
            rid = getattr(st, "run_id", None)
            inv = None
            if rid:
                try:
                    inv = await dispatcher.get(rid)
                except Exception:  # noqa: BLE001 — a missing run never breaks the view
                    inv = None
            output = reconstruct_step_output(inv) if inv is not None else {}
            found = findings_of(output or {})  # #1804 — task legs publish at top level
            steps[sid] = {
                "run_id": rid or "",
                "status": status,
                "output": output or {},
                "findings": found,
            }
        return {"pipeline_id": pipeline_id, "status": rec.status, "steps": steps}

    @r.get("/pipelines/{pipeline_id}/config", dependencies=[Depends(require_auth)])
    async def get_pipeline_config(pipeline_id: str) -> dict[str, Any]:
        """The composed config surface across the pipeline's steps (ADR-0069/0070) —
        one governance/operational view spanning every pack in the pipeline. Read-only:
        editing is per-workflow (the step's Configuration tab), applied at run."""
        from cairn.core.engine.workflow_config import compose_pipeline_config  # noqa: PLC0415

        rec = await _pipeline_in_ws_or_404(pipeline_id)
        steps = [(s.id, s.ref) for s in (rec.plan.steps if rec.plan else [])]
        return {"pipeline_id": pipeline_id, "steps": compose_pipeline_config(steps)}

    async def _assert_may_decide(rec: Any, step_id: str, token: Optional[str]) -> None:
        """#2393 — a GATE step's `approvers:` names who MAY decide, enforced
        here, in the one door a gate is decided through (console, CLI, client,
        MCP; `/advance` refuses a gate with 409 (#2388) and the Temporal twin
        only ever receives the signal from `advance_step`). The audience is
        resolved by the same rule the approver's queue row shows
        (`required_approvers`); what a name means is `approver_allowed`
        (username or role; owner/admin always; an unknown name → owner/admin
        only). Scope is the owner's decision (2026-10-15): a DECLARED gate
        (`step.approval`, the only step that gets a queue row with an audience).
        A manual release (`awaiting_advance`), a clock/topic wait and a leg a
        policy parked mid-flight (no gate context) keep today's behaviour. Auth
        off → no principal → unchanged. A gate without `approvers` → unchanged.
        The refusal is audited with the existing gate-decision kind and the
        documented outcome `deny`, not a new event."""
        if not auth_enabled():
            return
        plan = getattr(rec, "plan", None)
        step = plan.step(step_id) if plan is not None else None
        if step is None or not getattr(step, "approval", False):
            return
        required = required_approvers(step, getattr(rec, "governance", None) or {})
        if not required:
            return
        ident = await caller_identity(token)
        username, role = str(ident.get("username") or ""), str(ident.get("role") or "")
        if approver_allowed(required, username=username, role=role, is_admin=bool(ident.get("is_admin"))):
            return
        from cairn.core.obs.audit import audit_emit  # noqa: PLC0415 — keep obs off the import path
        await audit_emit(
            "operation.approval", outcome="deny", actor=username,
            target=f"{rec.pipeline_id}/{step_id}",
            pipeline_id=rec.pipeline_id, step_id=step_id, role=role,
            required=approver_names(required),
        )
        raise HTTPException(status_code=403, detail=(
            f"gate {step_id!r} may be decided by {', '.join(approver_names(required))} "
            f"(or an owner/admin); {username or 'the caller'} has role {role!r} "
            f"and is not among them"))

    @r.post("/pipelines/{pipeline_id}/steps/{step_id}/advance", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def advance_pipeline_step(pipeline_id: str, step_id: str) -> dict[str, Any]:
        rec = await _pipeline_in_ws_or_404(pipeline_id)
        # #2388 — this door releases a step parked `awaiting_advance`; it records no
        # approver, so it never DECIDES a gate. A gate (`awaiting_approval`) is decided
        # only at /approve, which records who decided (#2310, #2358) — refuse it here
        # instead of releasing it with an empty approver.
        st = (rec.step_state or {}).get(step_id)
        if str(getattr(st, "status", "") or "") == "awaiting_approval":
            raise HTTPException(
                status_code=409,
                detail=(f"step {step_id!r} is awaiting approval — a gate is decided at "
                        f"POST /pipelines/{pipeline_id}/steps/{step_id}/approve, "
                        f"which records the approver"))
        try:
            rec = await pipeline_bridge.advance_step(pipeline_id, step_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"pipeline_id": pipeline_id, "step_id": step_id, "status": rec.status}

    @r.post("/pipelines/{pipeline_id}/steps/{step_id}/approve", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def approve_pipeline_step(
        pipeline_id: str, step_id: str, req: PipelineApproveRequest,
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        rec = await _pipeline_in_ws_or_404(pipeline_id)
        await _assert_may_decide(rec, step_id, token)  # #2393 — before anything is recorded
        # #2310 — the approver is the caller's identity, not the body's `approver`
        approver = await decision_actor(token, req.approver, fallback="console")
        try:
            rec = await pipeline_bridge.advance_step(
                pipeline_id, step_id, approver=approver,
                rationale=req.rationale or "")
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"pipeline_id": pipeline_id, "step_id": step_id, "status": rec.status}

    @r.post("/pipelines/{pipeline_id}/cancel", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def cancel_pipeline_route(pipeline_id: str) -> dict[str, Any]:
        await _pipeline_in_ws_or_404(pipeline_id)
        rec = await pipeline_bridge.cancel_pipeline(pipeline_id)
        return {"pipeline_id": pipeline_id, "status": rec.status}

    return r
