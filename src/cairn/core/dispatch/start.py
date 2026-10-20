"""One start model — resolve a ref, then start what it names (ADR-0101 D3, #1975).

cairn grew one launch call per *noun*: ``POST /packs/{pack}/{template}/run`` for
a workflow, ``POST /pipelines/launch`` for a pipeline, and the same workflow
route with ``?sandbox=true`` for the Playground. Three doors, three request
shapes, three sets of error codes — and an agent (or an MCP client, ADR-0101 D5)
has to know which noun it is holding before it can start anything.

This module is the join, in the same spirit as
:mod:`cairn.core.dispatch.pipeline_launch`: the *logic* each door runs, lifted
out of the route handlers so ``POST /start``, ``cairn run``, ``cairn pipeline
run`` and the unattended ingresses are literally the same code rather than
several implementations that agree today.

:func:`preflight_workflow` is the body of the ``/packs/{pack}/{template}/
preflight`` handler and :func:`explain_pipeline_ref` is the body of ``GET
/pipelines/{pack}/{name}/explain``; each route calls the function it used to
inline. The pipeline launch door was already shared
(:func:`~cairn.core.dispatch.pipeline_launch.launch_pipeline`), so ``/start``
calls it directly.

**Only a declared pipeline runs (ADR-0106 D1, #2134).** A bare flow ref — all
three kinds (ADR-0084 D2), agentic included — is REFUSED here with
:class:`FlowNotRunnableError`, whose message names the remedy: the one-leg
wrapper to add, or the dev-loop path that runs a flow in-process and writes no
record (ADR-0106 D6). A flow with no wrapper is a component, and that product
decision is now visible in git rather than implied by which surface you started
from.

**Precedence, not ambiguity.** A pack may legally hold ``flows/check.md`` and
``pipelines/check.yaml``. That pair used to be a refusal (``AmbiguousStartRef``,
#1126) because both were startable and guessing would spend real money on the
wrong one. Under D1 only one of them is startable, so the pipeline WINS and the
pair resolves — there is nothing left to be torn between.

Errors are DOMAIN errors, not ``HTTPException``: this module is imported by the
CLI too, and a core function that raises HTTP status codes is a core function
only a web server can call.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger("cairn.core.dispatch.start")

#: The two things a ref can name. ``"run"`` (a workflow run) rather than
#: ``"workflow"`` because it is the noun the RESULT is — the discriminant on the
#: response tells a caller what it now holds, and what it holds is a run.
RUN = "run"
PIPELINE = "pipeline"
START_KINDS = (RUN, PIPELINE)

#: The modes a start may run in. ``sandbox`` is ADR-0073's Playground sandbox
#: (#826) and nothing else.
LIVE = "live"
SANDBOX = "sandbox"
START_MODES = (LIVE, SANDBOX)


class UnknownStartRefError(LookupError):
    """Neither a workflow nor a pipeline of this ref is installed."""


class WorkflowNotFoundError(LookupError):
    """No workflow of this ref is registered."""


class FlowNotRunnableError(LookupError):
    """A bare flow ref: a flow is a COMPONENT and does not run on its own.

    ADR-0106 D1. The message names the remedy on both paths — the wrapper to
    add, and the dev loop for someone who only wants to iterate — because a
    refusal that says only "no" moves the work to whoever reads the log.
    ``flow_kind`` is the ADR-0084 D2 kind (``agentic`` | ``data`` | ``ml``), so
    the suggested wrapper uses the leg word that kind actually takes.
    """

    def __init__(self, ref: str, *, flow_kind: str = "") -> None:
        self.ref = ref
        self.flow_kind = flow_kind or "agentic"
        pack, _, name = ref.partition("/")
        leg = "template" if self.flow_kind == "agentic" else "flow"
        super().__init__(
            f"{ref!r} is a flow ({self.flow_kind}), and a flow does not run on "
            f"its own (ADR-0106 D1): only a declared pipeline runs, so a flow "
            f"with no wrapper is a component. "
            f"To run it: add {pack}/pipelines/{name}.yaml — `inputs: "
            f"{{$inherit: {name}}}`, one step `- id: {name}` / `{leg}: {name}` — "
            f"and start `pipeline:{pack}/{name}`. "
            f"To iterate on the flow instead: `cairn run {ref} --interactive` "
            f"(or --mock / --dry-run), which runs it in this process and writes "
            f"no record (ADR-0106 D6). "
            f"`GET /pipelines/installed` lists what this deployment can start."
        )


# ── ref resolution ───────────────────────────────────────────────────────

def _workflow_installed(ref: str) -> bool:
    """Is a flow/template of this ref registered? Mirrors the private probe
    :func:`cairn.core.engine.pipeline_resolve._template_exists` uses for its own
    ambiguity check — same question, so the two doors cannot disagree about what
    "a workflow exists" means."""
    # lazy: keeps the compiler/engine import graph off this module's import path
    # (the CLI imports this module for `--mode`, and `--help` must stay cheap).
    from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415

    try:
        return bool(resolve_template_path(ref))
    except Exception:  # noqa: BLE001 — "not found" and "unparseable" both mean no match
        return False


def flow_kind_installed(ref: str) -> str:
    """``"agentic"`` | ``"data"`` | ``"ml"`` for the flow this ref names, else ``""``.

    ADR-0084 D2 is ONE flow concept in three kinds, so the D1 refusal has to
    know all three. Before #2134 this door consulted only the agentic registry:
    a data/ml ref (``training/readiness``) came back "no workflow or pipeline is
    installed", which is false and sends the reader looking for a missing pack
    instead of writing the wrapper the flow needs.

    Both readers are the existing ones — ``resolve_template_path`` for agentic,
    ``flow_def_for`` for data/ml (the same lookup the leg executor and the
    bridge use).
    """
    if _workflow_installed(ref):
        return "agentic"
    from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 — engine/dataflow off this module's import path
    from cairn.core.pack import ensure_loaded  # noqa: PLC0415 — as above

    try:
        # `load_flow_defs` scans REGISTERED packs and never loads them itself;
        # a cold CLI process would otherwise see zero flows.
        ensure_loaded()
        d = flow_def_for(ref)
    except Exception:  # noqa: BLE001 — an unreadable def is not a match
        return ""
    return str(getattr(d, "kind", "") or "") if d is not None else ""


def _pipeline_identity(ref: str) -> str:
    """The installed pipeline this bare ref names, in ITS OWN spelling — or ``""``.

    Asked through the ``pipeline:`` prefix ON PURPOSE: the prefixed form is the
    explicit one, so ``resolve_pipeline_ref`` answers "does a pipeline exist"
    without raising its own ambiguity error. Ambiguity is decided here, once,
    with both answers in hand.

    Returns the DEF's ``<pack>/<name>`` rather than the ref that was asked
    (#2126). ``dataset-readiness/check`` and ``dataset_readiness/check`` are one
    pipeline, so a start that echoed back whichever spelling the caller happened
    to type would file two runs of it under two refs — and every later join on
    that ref (a preset key, "runs of this pipeline", the usage map) would be this
    same bug one layer down.

    An :class:`~cairn.core.engine.pipeline_resolve.AmbiguousRefError` from the
    fold — two installed pipelines at one canonical address — is NOT swallowed:
    that is the one thing here that cannot be made to agree, and the caller must
    hear about it instead of being told "not installed".
    """
    from cairn.core.engine.pipeline_resolve import (  # noqa: PLC0415 — engine off the import path
        PIPELINE_PREFIX,
        AmbiguousRefError,
        resolve_pipeline_ref,
    )

    bare = ref[len(PIPELINE_PREFIX):] if ref.startswith(PIPELINE_PREFIX) else ref
    try:
        d = resolve_pipeline_ref(f"{PIPELINE_PREFIX}{bare}")
    except AmbiguousRefError:
        raise
    except Exception:  # noqa: BLE001 — a broken pipeline file is not a match
        return ""
    return f"{d.pack}/{d.name}" if d is not None else ""


def resolve_start_ref(ref: str, *, kind: Optional[str] = None) -> tuple[str, str]:
    """``(kind, bare_ref)`` for what ``ref`` names — and under ADR-0106 D1 the
    only kind that can be STARTED is :data:`PIPELINE`.

    The ``pipeline:`` prefix and ``kind="pipeline"`` remain the two explicit
    spellings; ``kind="run"`` is kept accepted so an old caller gets the
    :class:`FlowNotRunnableError` with the remedy rather than a confusing
    "kind must be one of".

    Raises :class:`UnknownStartRefError` when nothing matches (the ref is in the
    message — "not found" without the thing you asked for is a support ticket),
    :class:`FlowNotRunnableError` when the ref names a flow and no pipeline, and
    :class:`ValueError` when the caller contradicts itself (``pipeline:x`` with
    ``kind="run"``).

    **A declared pipeline WINS over a flow at the same ref** (ADR-0106): both
    used to be startable, so a tie was refused; now only one of them is, so
    there is nothing to refuse. That is the one behaviour change a pack with a
    ``flows/check.md`` + ``pipelines/check.yaml`` pair will notice.

    **Every spelling of a ref reaches the same door (#2126).** A pipeline is
    stamped with its pack's DIRECTORY name and a flow with the ``pack.toml``
    name, so ``dataset_readiness/check`` started and ``dataset-readiness/check``
    — the spelling ``GET /flows`` and the console publish — was refused as a
    bare flow ref, naming a wrapper that already existed. The returned ref is
    the PIPELINE's own, never the caller's spelling.
    """
    from cairn.core.engine.pipeline_resolve import PIPELINE_PREFIX  # noqa: PLC0415 — as above

    raw = (ref or "").strip()
    if not raw:
        raise UnknownStartRefError("ref is required")

    want = (kind or "").strip().lower()
    if want and want not in START_KINDS:
        raise ValueError(f"kind must be one of {START_KINDS}, got {kind!r}")

    prefixed = raw.startswith(PIPELINE_PREFIX)
    bare = raw[len(PIPELINE_PREFIX):].strip() if prefixed else raw
    if prefixed:
        if want == RUN:
            raise ValueError(
                f"ref {raw!r} names a pipeline but kind is 'run' — drop one of them")
        want = PIPELINE
    if not bare:
        raise UnknownStartRefError("ref is required")

    if want == PIPELINE:
        if not (found := _pipeline_identity(bare)):
            raise UnknownStartRefError(
                f"no pipeline {bare!r} is installed — see GET /pipelines/installed")
        return PIPELINE, found

    # `kind="run"` no longer names anything startable. Answer with the D1
    # refusal when the flow exists, so the caller learns the rule and not just
    # that its parameter stopped working.
    if want == RUN:
        if (fk := flow_kind_installed(bare)):
            raise FlowNotRunnableError(bare, flow_kind=fk)
        raise UnknownStartRefError(
            f"no flow {bare!r} is installed, and kind='run' asks for one. Since "
            f"ADR-0106 D1 only a declared pipeline runs — drop `kind`, or pass "
            f"{PIPELINE_PREFIX}{bare}")

    if (found := _pipeline_identity(bare)):
        return PIPELINE, found
    if (fk := flow_kind_installed(bare)):
        raise FlowNotRunnableError(bare, flow_kind=fk)
    raise UnknownStartRefError(
        f"no pipeline {bare!r} is installed — see GET /pipelines/installed for "
        f"what this deployment can start, and GET /flows for the components a "
        f"pipeline can compose")


# ── the one-leg definition (ADR-0105 D1, now driven by a DECLARED file) ───

#: JSON-schema scalar → the pipeline input type names `plan_pipeline` checks
#: wires against. Anything else is left untyped: the leg's own contract has
#: already validated the value by the time a definition is synthesized.
_JSON_TO_INPUT_TYPE = {"string": "str", "integer": "int", "number": "float", "boolean": "bool"}


def synthesize_flow_pipeline(ref: str, *, typed: dict[str, Any],
                             input_schema: Optional[dict[str, Any]] = None,
                             kind: str = "agentic") -> Any:
    """The ONE-LEG ``PipelineDef`` a flow becomes when it is run as itself.

    ADR-0105 D1 built this for a BARE ref at the door. ADR-0106 D1 removed that
    caller: a bare flow ref is refused (:func:`resolve_start_ref`), and a flow
    meant to be runnable ships a DECLARED one-leg file instead — which
    ``$inherit`` (ADR-0106 D2) makes ~14 lines. The function keeps its job for
    the one remaining producer that has no declared file to reach for: the
    no-orphan chokepoint in :meth:`RunDispatcher.run_template` (#2128 A), which
    wraps a pipeline-less call — an operator dispatching a flow, a test harness
    — rather than letting an orphan run exist. It is NOT the start path.

    A pipeline is the only orchestrator (ADR-0084 D4) and a flow is what it
    runs, so a flow run on its own is a pipeline with one leg — the same
    record, the same doors, the same graph. The definition is built in memory,
    never installed: its inputs are exactly the values handed in (each wired
    ``$input.k`` onto the leg), its one step is the flow, and its name is the
    flow's, so the record reads ``flow:<pack>/<name>`` wherever a pipeline ref
    is shown. ``kind`` picks the leg shape ADR-0084 D2 gives the flow:
    ``agentic`` is a bare template ref, ``data``/``ml`` a ``flow:`` ref.
    """
    from cairn.core.engine.pipeline import PipelineDef, PipelineStep  # noqa: PLC0415 — engine off the import path

    pack, _, name = (ref or "").partition("/")
    if not pack or not name:
        raise WorkflowNotFoundError(f"a flow ref is <pack>/<name>, got {ref!r}")
    schema = dict(input_schema or {})
    inputs: dict[str, Any] = {}
    for key in typed:
        spec: dict[str, Any] = {}
        raw = schema.get(key) or {}
        t = _JSON_TO_INPUT_TYPE.get(str((raw or {}).get("type", "") or "").lower())
        if t:
            spec["type"] = t
        inputs[key] = spec
    leg_ref = {"template": ref} if kind == "agentic" else {"flow": ref}
    step = PipelineStep(id=name, **leg_ref, **{"with": {k: f"$input.{k}" for k in typed}})
    return PipelineDef(
        name=name, pack=pack,
        description=f"{ref} started on its own — one leg, the flow itself (ADR-0105 D1)",
        inputs=inputs, steps=[step],
    )


def _bridge_for(dispatcher: Any, bridge: Any) -> Any:
    """The pipeline bridge a start should go through, or ``None`` when the
    dispatcher was wired without one (some CLI paths and test harnesses). The
    server attaches its bridge to the dispatcher at wiring time so every door
    that only holds the dispatcher delegates too."""
    return bridge if bridge is not None else getattr(dispatcher, "pipeline_bridge", None)


# ── the unattended door (ADR-0105 D4, #2115 · ADR-0106 D1/D3, #2134) ─────

async def launch_ref(
    ref: str,
    inputs: Optional[dict[str, Any]] = None,
    *,
    dispatcher: Any,
    environment: str = "",
    config_preset: str = "",
    lane: str = "",
    workspace_id: str = "default",
    trigger: Optional[dict[str, Any]] = None,
    bridge: Any = None,
) -> Any:
    """Start what ``ref`` names, unattended — cron fires and matched webhooks.

    ONE resolver with the interactive doors (:func:`resolve_start_ref`), so an
    unattended start obeys the same rule a console start does. Under ADR-0106
    D1 that rule is: a declared pipeline runs, a bare flow ref is refused with
    the remedy. ADR-0106 D3 makes the same point from the other side — triggers
    and hooks belong to a PIPELINE, never to a flow — so a flow-authored
    ``triggers:`` block firing here now raises :class:`FlowNotRunnableError`
    instead of quietly starting a component. The caller logs it; #2134 S2
    refuses such a block at pack install, which is where it should have been
    caught.

    ``lane`` is kept in the signature (a cron fire has always run on the
    ``background`` lane) and rides the launch as a run option, so a scheduled
    pipeline's legs land on the same lane a scheduled run did.
    """
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge  # noqa: PLC0415 — cycle
    from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: PLC0415 — cycle
    from cairn.core.engine.pipeline_resolve import PIPELINE_PREFIX  # noqa: PLC0415 — engine off the import path

    _kind, bare = resolve_start_ref(ref)     # raises for a bare flow ref (D1)
    pipeline_bridge = _bridge_for(dispatcher, bridge) or PipelineBridge(dispatcher)
    trig = dict(trigger or {})
    if lane:
        # the leg reads this through `pipeline_bridge._run_options`
        trig["run_options"] = {**(trig.get("run_options") or {}), "lane": lane}
    return await launch_pipeline(
        f"{PIPELINE_PREFIX}{bare}", inputs=dict(inputs or {}), bridge=pipeline_bridge,
        environment=environment or None, workspace_id=workspace_id or "default",
        trigger=trig or None, preset=config_preset or None,
    )


def unattended_starter(dispatcher: Any, *, source: str, lane: str = "") -> Any:
    """The callable an unattended ingress hands ``(ref, inputs)`` to.

    Shaped exactly like the ``dispatcher.run_template`` bound method the trigger
    router used to be wired with — ``(template_ref, inputs, *, environment)`` —
    so the router itself is unchanged and only the door behind it moves. Returns
    a coroutine function; ``source`` names the ingress on the record's trigger
    blob.
    """
    async def _start(template_ref: str, inputs: Optional[dict[str, Any]] = None, *,
                     environment: Optional[str] = None) -> Any:
        from cairn.core.workspace import current_workspace  # noqa: PLC0415 — as the module's other lazy imports
        # #209 — the pipeline record lands where its legs do: the workspace the
        # ingress bound (a webhook binds the one that declared its receiver).
        return await launch_ref(
            template_ref, inputs, dispatcher=dispatcher, environment=environment or "",
            lane=lane, workspace_id=current_workspace(), trigger={"source": source},
        )
    return _start


async def preflight_workflow(
    ref: str,
    values: Optional[dict[str, Any]] = None,
    *,
    environment: Optional[str] = None,
    config_preset: Optional[str] = None,
    workspace_id: str = "default",
) -> Any:
    """The side-effect-free "can this run?" verdict (ADR-0072).

    Lifted verbatim from the ``/packs/{pack}/{template}/preflight`` handler so
    ``dry_run: true`` on ``/start`` is the SAME verdict that route returns, not a
    second composition of the same checks. ``verdict.found`` is False for an
    unregistered ref — the caller decides whether that is a 404.
    """
    from cairn.core.preflight import build_preflight  # noqa: PLC0415 — cairn.ask off the import path
    from cairn.core.workflow_config_store import (  # noqa: PLC0415 — as above
        default_preset,
        default_workflow_config_store,
    )

    # The config that WOULD apply (ADR-0071): the picked preset (#790) if any,
    # else the marked default, else the legacy saved values — echoed into the
    # verdict so the dry-run reflects exactly what the run will use.
    try:
        store = default_workflow_config_store()
        preset = await store.get_preset(workspace_id, ref, config_preset) if config_preset else None
        if preset is None:
            preset = await default_preset(workspace_id, ref, store=store)
        if preset:
            resolved_config = {"preset": preset.name, "values": dict(preset.values)}
        else:
            rec = await store.get(workspace_id, ref)
            resolved_config = {"preset": None, "values": dict(rec.values) if rec else {}}
    except Exception:  # noqa: BLE001 — additive config; never fail the verdict on it
        resolved_config = {"preset": None, "values": {}}
    return await build_preflight(ref, values or {}, profile=environment,
                                 resolved_config=resolved_config)


# ── pipeline dry-run (was the body of GET /pipelines/{pack}/{name}/explain) ──

async def explain_pipeline_ref(
    ref: str,
    *,
    environment: str = "",
    preset: str = "",
    workspace_id: str = "default",
) -> dict[str, Any]:
    """Dry-run how every step of an installed pipeline would be triggered (#1792).

    The pipeline half of ``dry_run: true``. Lifted from the ``/explain`` route so
    the console's explain view and ``/start --dry-run`` cannot be told different
    things about the same pipeline.

    Raises :class:`~cairn.core.dispatch.pipeline_launch.PipelineNotFoundError`,
    :class:`~cairn.core.engine.pipeline_resolve.AmbiguousRefError` or
    :class:`~cairn.core.pipeline_presets.PresetNotFoundError` — the same three
    the route already mapped.
    """
    from cairn.core.dispatch.pipeline_launch import resolve_or_raise  # noqa: PLC0415 - cycle
    from cairn.core.engine.pipeline_explain import explain_pipeline  # noqa: PLC0415 - cycle
    from cairn.core.engine.pipeline_resolve import PIPELINE_PREFIX  # noqa: PLC0415 - cycle
    from cairn.core.pipeline_presets import resolve_preset  # noqa: PLC0415 - cycle

    bare = ref[len(PIPELINE_PREFIX):] if ref.startswith(PIPELINE_PREFIX) else ref
    canon = f"{PIPELINE_PREFIX}{bare}"
    # `pipeline:` prefixed on purpose: a pack may hold a flow and a pipeline of
    # the same name, and this door explains PIPELINES.
    d = resolve_or_raise(canon)
    preset_inputs: dict[str, Any] = {}
    if preset:
        rec, _src = await resolve_preset(workspace_id, canon, preset)
        preset_inputs = dict(rec.inputs or {})
    return await explain_pipeline(
        d, environment=environment or "", preset_name=preset,
        preset_inputs=preset_inputs,
        # #2134 S4 — the workspace is a real layer of the resolved view
        # (ADR-0101 D2), and only this door knows which workspace is asking.
        workspace_id=workspace_id)


__all__ = [
    "LIVE", "PIPELINE", "RUN", "SANDBOX", "START_KINDS", "START_MODES",
    "FlowNotRunnableError", "UnknownStartRefError", "WorkflowNotFoundError",
    "explain_pipeline_ref", "flow_kind_installed", "launch_ref", "preflight_workflow",
    "resolve_start_ref", "synthesize_flow_pipeline", "unattended_starter",
]
