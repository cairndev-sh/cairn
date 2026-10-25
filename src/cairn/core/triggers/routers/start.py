"""``POST /start`` — one call that starts anything (ADR-0101 D3, #1975).

One verb, one body, one result. A caller says *what* it wants to start and the
façade works out *which kind of thing* that ref names, then delegates to the
door that already exists for it:

===============  ==========================================================
``ref``          :func:`cairn.core.dispatch.pipeline_launch.launch_pipeline` —
                 the body of ``POST /pipelines/launch``, already shared with
                 ``cairn remote launch`` since #1789
``mode:``        the same ``sandbox=True`` the Playground has passed since
``sandbox``      #826, now riding the pipeline record as a run option — there
                 is still exactly one sandbox switch
``dry_run``      :func:`~cairn.core.dispatch.start.explain_pipeline_ref` (#1792)
===============  ==========================================================

**Only a declared pipeline runs (ADR-0106 D1, #2134).** ``ref`` used to accept
a bare flow, which ``resolve_start_ref`` answered ``kind: run`` for and #2113
turned into a synthesized one-leg pipeline. It is now REFUSED — 400, with the
wrapper to add and the dev-loop alternative in the message — so which flows are
runnable is a fact in git rather than a property of this door. The
``kind: run`` branch of this handler went with it; ``kind`` is still accepted
(and still disambiguates nothing, because a pipeline now WINS over a flow at
the same ref) so that an old client gets the rule rather than a 422 about a
parameter.

**The path is declared FLAT on purpose.** ``/start`` is workspace-scoped and is
addressed ``/workspaces/{id}/start``, but ``_WorkspaceMiddleware`` lifts the
``{id}`` and rewrites the scope path to ``/start`` *before* routing (ADR-0051
§1) — a route declared at the long spelling would be unreachable dead code.
Same as ``/variables`` and ``/triggers/scheduled``.

**The pipelines feature gate is re-applied here.** ``POST /pipelines/launch``
sits behind ``require_feature('pipelines')`` (#1878: a disabled feature answers
404, because hiding a nav item is not a gate). A façade that could launch a
pipeline without that check would be a hole in it, so the handler asks the same
question once a ref has resolved to a pipeline.
"""
# NO `from __future__ import annotations`: FastAPI resolves this module's
# handler annotations against its globals when it builds the OpenAPI schema, and
# the same rule the sibling routers document (runs.py, admin.py) applies here.

from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from cairn.core.dispatch.start import LIVE, PIPELINE, RUN, SANDBOX, START_KINDS, START_MODES
from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import _oauth2_scheme, require_auth
from cairn.core.triggers.feature_gate import require_feature

#: Fields only a workflow run understands. Passing one with a pipeline ref is a
#: 400 rather than a silent drop — a knob that is accepted and ignored is the
#: #905 shape of bug, and these carry money (``cost_cap_usd`` is what a series
#: cap is enforced on) and provenance (``parent_run`` is the lineage edge).
_RUN_ONLY_FIELDS = ("series", "cost_cap_usd", "parent_run")


class StartRequest(BaseModel):
    """The one launch body (ADR-0101 D3)."""

    ref: str = Field(..., description=(
        "What to start: 'pack/flow' for a workflow, 'pack/pipeline' for a "
        "pipeline, or 'pipeline:pack/name' to name a pipeline explicitly."))
    inputs: dict[str, Any] = Field(default_factory=dict, description=(
        "The declared inputs. Validated against the workflow's input schema or "
        "the pipeline's declared inputs by the door this delegates to."))
    mode: str = Field(LIVE, description=(
        "'live' (default) is a normal governed run. 'sandbox' is the Playground "
        "sandbox (ADR-0073, #826): write-class operators take their mock path, "
        "reads run live. Workflows only."))
    kind: Optional[str] = Field(None, description=(
        "Disambiguate a ref that names BOTH a workflow and a pipeline: 'run' or "
        "'pipeline'. Omit it and an ambiguous ref is refused with 409."))
    environment: Optional[str] = Field(None, description=(
        "The execution profile / environment the run resolves under."))
    config_preset: Optional[str] = Field(None, description=(
        "A named saved set. For a workflow this is the ADR-0071 config preset "
        "(#790); for a pipeline it is the launch input preset (#1789) — each "
        "kind's own preset concept, resolved server-side by its own door."))
    series: Optional[str] = Field(None, description=(
        "Attach the run to a budgeted series (#993). Workflow runs only."))
    cost_cap_usd: Optional[float] = Field(None, description=(
        "The ceiling reserved against a capped series at admission (#1655). "
        "cairn has no pre-run estimator, so this declared number is what the cap "
        "is enforced on. Workflow runs only."))
    parent_run: Optional[str] = Field(None, description=(
        "Run lineage (#989): the id of the run this one is derived from. "
        "Workflow runs only."))
    expected_hash: Optional[str] = Field(None, description=(
        "Pipeline drift pin (#1255): refuse the launch if the installed record "
        "is not this hash. Pipelines only."))
    dry_run: bool = Field(False, description=(
        "Return the preflight verdict (workflow, ADR-0072) or the step-by-step "
        "explain projection (pipeline, #1792) and start NOTHING."))


def build_start_router(dispatcher, pipeline_bridge) -> APIRouter:
    """``POST /start``, closed over the shared dispatcher and pipeline bridge —
    the same two objects the routes it delegates to are closed over."""
    r = APIRouter()

    # The #1878 gate that guards `POST /pipelines/launch`, bound here so the
    # façade asks the SAME question rather than re-deriving the answer. Not
    # attached as a router dependency: this door must stay usable for workflows
    # in a deployment with pipelines switched off, so it is only asked once a
    # ref has resolved to a pipeline. Bound outside the route so the #1574
    # console-scoping gate, which reads a route's body up to the next decorator,
    # sees only what the handler itself does.
    _assert_pipelines_enabled = require_feature("pipelines")

    @r.post("/start", dependencies=[Depends(require_auth)])
    async def start_anything(
        body: StartRequest,
        workspace_id: Annotated[str, Depends(support.writable_workspace)],
        token: Annotated[Optional[str], Depends(_oauth2_scheme)] = None,
    ) -> dict[str, Any]:
        """Start a pipeline, live or sandboxed — one call (ADR-0101 D3).

        The result carries the started pipeline's id under BOTH the generic
        ``id`` and ``pipeline_id``, so a caller never needs a second request to
        learn what it started. ``kind`` stays on the response for callers that
        switch on it; since ADR-0106 D1 the only startable kind is
        ``pipeline``.

        Refusals: 400 for a bare FLOW ref (it exists and is a component — the
        wrapper to add and the dev-loop alternative are in the detail), 404 for
        a ref nothing installs, 409 for a ref that names two installed
        pipelines (pack names differing only by ``_``/``-``, #2126), 422 for a
        self-contradicting ``kind``, 400 for inputs that fail the pipeline's
        declared contract, 403 for a policy denial or a workspace the caller is
        not in.
        """
        from cairn.core.dispatch.pipeline_launch import (  # noqa: PLC0415 - cycle
            InputCoercionError, MissingInputsError, PipelineDriftError,
            PipelineNotFoundError, ServiceDoorError, launch_pipeline,
        )
        from cairn.core.dispatch.start import (  # noqa: PLC0415 — engine off the router import path
            FlowNotRunnableError, UnknownStartRefError, explain_pipeline_ref,
            resolve_start_ref,
        )
        from cairn.core.engine.artifacts import WireTypeError  # noqa: PLC0415 - cycle
        from cairn.core.engine.bundle_pipeline import PipelinePlanError  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline import PipelineDefError  # noqa: PLC0415 - cycle
        from cairn.core.engine.pipeline_resolve import (  # noqa: PLC0415 - cycle
            PIPELINE_PREFIX, AmbiguousRefError,
        )
        from cairn.core.pipeline_presets import (  # noqa: PLC0415 - cycle
            PresetNotFoundError, UnknownPresetInputsError,
        )
        from cairn.core.policy.pipeline_gates import PolicyDeniedError  # noqa: PLC0415 - cycle
        from cairn.core.admission_door import AdmissionRefusedError  # noqa: PLC0415 — cycle

        # `workspace_id` is the tenant this start lands in, authorised by the
        # `writable_workspace` dependency (#1610/#1974, #221): a start is the
        # sharpest write cairn has — it spends money in the named workspace.

        mode = (body.mode or LIVE).strip().lower()
        if mode not in START_MODES:
            raise HTTPException(
                status_code=422, detail=f"mode must be one of {list(START_MODES)}")

        try:
            _kind, ref = resolve_start_ref(body.ref, kind=body.kind)
        except FlowNotRunnableError as e:
            # 400, not 404: the flow EXISTS and this deployment can see it — it
            # is the request that is impossible, and the remedy is in the body.
            raise HTTPException(status_code=400, detail={
                "error": str(e), "ref": e.ref, "flow_kind": e.flow_kind,
                "remedy": "declare a one-leg pipeline (ADR-0106 D1/D2)",
            }) from e
        except UnknownStartRefError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except AmbiguousRefError as e:
            # Two installed pipelines at one canonical address (#2126): no
            # spelling of the request can pick one, so it conflicts with what is
            # installed. A pipeline and a flow at one ref never reach here — the
            # pipeline wins (ADR-0106 D1). Before the ValueError arm it subclasses.
            raise HTTPException(status_code=409, detail=str(e)) from e
        except ValueError as e:  # a self-contradicting kind/prefix pair
            raise HTTPException(status_code=422, detail=str(e)) from e

        # ── explain, or the launch door ───────────────────────────────────
        await _assert_pipelines_enabled(token)
        run_only = [f for f in _RUN_ONLY_FIELDS if getattr(body, f, None) not in (None, "")]
        if run_only:
            raise HTTPException(status_code=400, detail=(
                f"{', '.join(run_only)} were per-RUN knobs on the bare flow door, "
                f"which ADR-0106 D1 closed. A pipeline declares its budget, "
                f"series and approvers in `governance:` (ADR-0106 D4) — set them "
                f"there, in the file, rather than per launch."))

        if body.dry_run:
            try:
                explained = await explain_pipeline_ref(
                    ref, environment=body.environment or "",
                    preset=body.config_preset or "", workspace_id=workspace_id)
            except (PipelineNotFoundError, PresetNotFoundError) as e:  # pragma: no cover
                raise HTTPException(status_code=404, detail=str(e)) from e
            except AmbiguousRefError as e:  # pragma: no cover — the prefix disambiguates
                raise HTTPException(status_code=400, detail=str(e)) from e
            return {"kind": PIPELINE, "ref": ref, "mode": mode, "dry_run": True,
                    "id": None, "pipeline_id": None, "status": "not_started",
                    "explain": explained}

        # The error map below is the one POST /pipelines/launch already applies,
        # arm for arm — the same refusal must mean the same status code at both
        # doors, or "it works from the console but not from /start" becomes real.
        try:
            rec = await launch_pipeline(
                # #2566 — `resolve_start_ref` has DECIDED this is a pipeline, so
                # the door gets the explicit spelling, exactly as `launch_ref`
                # hands it: a bare ref shared with a flow would be re-ambiguated
                # by the resolver behind `launch_pipeline` and refused.
                f"{PIPELINE_PREFIX}{ref}",
                inputs=dict(body.inputs or {}),
                bridge=pipeline_bridge,
                environment=body.environment or None,
                workspace_id=workspace_id,
                # #826 — the ONE sandbox switch, now reaching every leg through
                # the record's run options (the same channel the one-leg path
                # used since #2113) instead of a workflow-only door. ADR-0106
                # rejected "sandbox as a documented exception": the Playground
                # launches a pipeline like everything else.
                trigger={"source": "start", "mode": mode,
                         **({"run_options": {"sandbox": True}} if mode == SANDBOX else {})},
                expected_hash=(body.expected_hash or "") or None,
                preset=(body.config_preset or "") or None,
            )
        except (PipelineNotFoundError, PresetNotFoundError) as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except UnknownPresetInputsError as e:
            # criterion 5 (#1789) — a stale preset key is a LOUD 400 naming the
            # key. Listed before the generic ValueError arms so it survives.
            raise HTTPException(status_code=400, detail=str(e)) from e
        except PipelineDriftError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except AdmissionRefusedError as e:
            # #2190 — the admission door refused: 403 with every reason and the
            # decision itself, so the caller sees which version/check/policy.
            raise HTTPException(status_code=403, detail={
                "error": str(e), "admission": e.decision.as_record()}) from e
        except PolicyDeniedError as e:
            raise HTTPException(status_code=403, detail=str(e)) from e
        except ServiceDoorError as e:
            # #2101 — same arm, same code as POST /pipelines/launch: a missing
            # service is the DEPLOYMENT's problem, not the request's.
            raise HTTPException(status_code=503, detail=str(e)) from e
        except (AmbiguousRefError, InputCoercionError, MissingInputsError,
                NotImplementedError, PipelineDefError, PipelinePlanError,
                WireTypeError) as e:
            # #2280 — a def that cannot be planned (a cycle, a dangling dep, a
            # mistyped wire) is refused like one that is not runnable
            # (PipelineNotRunnableError, a NotImplementedError).
            raise HTTPException(status_code=400, detail=str(e)) from e
        # ADR-0106 D1 — with no flow door, a ONE-LEG pipeline is how a single
        # flow is started, and a caller that used to get a run id back still
        # needs one: the playground reads the run's report, `/runs/{id}/pilot`.
        # Only for a single leg, and only once it has been submitted: on a
        # multi-leg pipeline "the run" is not a thing, and inventing one would
        # send a reader to whichever leg happened to start first.
        steps = getattr(rec, "step_state", None) or {}
        legs = [st for st in steps.values() if getattr(st, "run_id", "")]
        leg_run = legs[0].run_id if len(steps) == 1 and legs else None
        return {"kind": PIPELINE, "ref": ref, "mode": mode, "dry_run": False,
                "id": rec.pipeline_id, "pipeline_id": rec.pipeline_id,
                "run_id": leg_run,
                "status": rec.status, "sandbox": mode == SANDBOX}

    return r


__all__ = ["StartRequest", "build_start_router"]
