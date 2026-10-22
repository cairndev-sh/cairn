"""Launch an INSTALLED pipeline from its ref (#1136, ADR-0084 D9).

D9 makes the pipeline the primary runnable noun, and P8 says anything that
bypasses install is console-invisible — but until now there was no way to get
from a ref to a running pipeline at all. `resolve_pipeline_ref` produced a
definition and `PipelineBridge.start_pipeline` consumed a *plan*; nothing
joined them, so the only launch path was programmatic. This is that join, and
it is deliberately the ONE path: the CLI and the HTTP endpoint both call
:func:`launch_pipeline`, so a pipeline started from the console and one started
from a terminal are the same governed object with the same identity.

``bundle_ref`` is stamped ``pipeline:<pack>/<name>`` — the installed ref, not a
filesystem path — so a run traces back to the registry record that authorised
it rather than to whatever file happened to be on disk.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Optional

from cairn.core.engine.bundle_pipeline import (
    ContractDefinitionRef, ContractResolution, InvocationEntry, PipelinePlan, PipelinePlanError,
)
from cairn.core.engine.pipeline import PipelineDef, plan_pipeline
from cairn.core.reporting import ReportSpec
from cairn.core.engine.pipeline_resolve import (
    PIPELINE_PREFIX,
    AmbiguousRefError,
    all_pipeline_defs,
    assert_runnable,
    resolve_pipeline_ref,
)
from cairn.core.dispatch.pipeline_doors import ServiceDoorError  # re-export: the routers' one import site
from cairn.core.admission_door import AdmissionRefusedError  # noqa: F401 — re-export (#2190), same reason
from cairn.core.dispatch.pipeline_preview import (
    GovernanceInputError,
    apply_input_defaults,
    assert_no_governance_inputs,
)
from cairn.core.pipeline_presets import (
    assert_preset_keys_declared,
    merge_over_preset,
    resolve_preset,
    touch_preset,
)

logger = logging.getLogger("cairn.core.dispatch.pipeline_launch")


class PipelineNotFoundError(LookupError):
    """No installed pipeline matches the ref."""


class MissingInputsError(ValueError):
    """Required pipeline inputs were not supplied."""


class InputCoercionError(ValueError):
    """#1802 — a supplied input cannot be parsed into its declared type."""


class PipelineDriftError(ValueError):
    """#1255 (ADR-0087 D6) — the caller's expected hash does not match the
    installed record: the local class/file differs from what would run."""


def _known_refs() -> list[str]:
    return sorted(f"{p.pack}/{p.name}" for p in all_pipeline_defs())


def resolve_or_raise(ref: str) -> PipelineDef:
    """Resolve a ref to an installed pipeline, or raise something actionable.

    A bad ref is the single most likely user error at this door, so the error
    names what IS installed rather than only what is not.
    """
    try:
        d = resolve_pipeline_ref(ref)
    except AmbiguousRefError:
        raise  # already explains itself, and names the pipeline: prefix
    if d is None:
        known = _known_refs()
        raise PipelineNotFoundError(
            f"no installed pipeline {ref!r}. Installed: "
            + (", ".join(known) if known else "(none — is the pack installed?)")
        )
    return d


_BOOL_WORDS = {"true": True, "1": True, "yes": True, "on": True,
               "false": False, "0": False, "no": False, "off": False}


def coerce_declared_inputs(d: PipelineDef, inputs: dict) -> dict:
    """#1802 — parse string values into their DECLARED input types, at the door.

    Every entry path delivers strings somewhere: `--input k=v` is untyped by
    nature, and a preset saved from such a launch stores the string verbatim.
    Before this, a `type: dict` input supplied as `'{"a": "b"}'` sailed through
    launch and the preset store and only failed Pydantic validation on the leg
    that consumed it — in the acceptance run, 40 minutes and one approved GPU
    acquisition later. The launch door is the one place CLI, console, cron and
    preset merge converge (see the #1180 comment below), so the refusal happens
    here, before any record exists and before anything is spent.

    Only `str` values are touched, and only when the declared type demands a
    non-string; well-typed values (console JSON bodies, YAML preset files,
    declared defaults) pass through identically. An unparseable value raises
    :class:`InputCoercionError` naming the key and the declared type.
    """
    out = dict(inputs)
    for key, spec in (d.inputs or {}).items():
        declared = (spec or {}).get("type") if isinstance(spec, dict) else None
        val = out.get(key)
        if not isinstance(val, str) or declared in (None, "str", "string"):
            continue
        try:
            if declared in ("dict", "list"):
                parsed = json.loads(val)
                expect = dict if declared == "dict" else list
                if not isinstance(parsed, expect):
                    raise InputCoercionError(
                        f"input {key!r} is declared {declared} but parses to "
                        f"{type(parsed).__name__} — supply a JSON {declared}"
                    )
                out[key] = parsed
            elif declared == "int":
                out[key] = int(val)
            elif declared == "float":
                out[key] = float(val)
            elif declared == "bool":
                if val.strip().lower() not in _BOOL_WORDS:
                    raise InputCoercionError(
                        f"input {key!r} is declared bool; got {val!r} "
                        f"(use true/false)"
                    )
                out[key] = _BOOL_WORDS[val.strip().lower()]
        except InputCoercionError:
            raise
        except (ValueError, TypeError) as e:
            raise InputCoercionError(
                f"input {key!r} is declared {declared} but {val!r} does not "
                f"parse as one: {e}"
            ) from e
    return out


def missing_inputs(d: PipelineDef, inputs: dict) -> list[str]:
    """Declared inputs with no supplied value and no default.

    Checked BEFORE any run record exists: a pipeline that dies on its second
    leg because input 3 was never provided has already spent money on legs 1
    and 2. Cheap to check up front, expensive to discover late.
    """
    supplied = set(inputs or {})
    out = []
    for key, spec in (d.inputs or {}).items():
        if key in supplied:
            continue
        if isinstance(spec, dict) and ("default" in spec or not spec.get("required", True)):
            continue
        out.append(key)
    return sorted(out)


async def launch_pipeline(
    ref: str,
    *,
    inputs: Optional[dict] = None,
    bridge: Any,
    environment: Optional[str] = None,
    workspace_id: str = "default",
    trigger: Optional[dict] = None,
    expected_hash: Optional[str] = None,
    preset: Optional[str] = None,
) -> Any:
    """Resolve → validate → plan → start. Returns the persisted ``PipelineRecord``.

    Ordering matters: everything that can fail cheaply (resolution, required
    inputs, planning) happens before the record exists, so a rejected launch
    leaves no half-started pipeline in the run store for an operator to
    interpret.

    ``preset`` (#1789, ADR-0071 §6) names a saved or pack-shipped input set to
    launch from; ``inputs`` then carries only what OVERRIDES it, per key. The
    expansion happens HERE and nowhere else — this is the door the CLI, the
    console and the cron trigger already converge on, so a preset means the same
    thing regardless of who launched. A client that resolved presets itself
    would be a second, silently diverging implementation of the same rule.
    """
    d = resolve_or_raise(ref)

    # #1255 (ADR-0087 D6) — run-by-ref drift guard: the caller may pin the
    # exact record it believes it is launching. Accepts EITHER identity —
    # `content_hash` (canonical, parsed) or `source_hash` (raw bytes — what an
    # SDK build can compute without importing this model). Mismatch is a
    # refusal BEFORE any record exists: CI and the console must never silently
    # run different pipelines.
    if expected_hash and expected_hash not in (d.content_hash, d.source_hash):
        raise PipelineDriftError(
            f"local pipeline differs from installed {d.pack}/{d.name}: expected "
            f"{expected_hash}, installed is {d.content_hash} (source "
            f"{d.source_hash}) — rebuild and reinstall the pack, or drop the pin"
        )

    # structural rejection first: a leg kind the executor cannot run is not
    # fixable by supplying inputs, so saying "missing input" would send the
    # caller down the wrong path entirely.
    return await launch_pipeline_def(
        d, inputs=inputs, bridge=bridge, environment=environment,
        workspace_id=workspace_id, trigger=trigger, preset=preset,
    )


def _contract_resolution() -> ContractResolution:
    """The installed contract closure as the plan's typed snapshot (#2240 C). An
    invalid installed contract distribution cannot be planned against: it refuses
    like any other plan defect, before anything exists."""
    from cairn.core.pack.contracts import ContractDiscoveryError  # noqa: PLC0415 — cycle: dispatch → pack, lazy like its siblings
    from cairn.core.pack.registry import contract_resolution_snapshot  # noqa: PLC0415 — as above
    try:
        return ContractResolution.model_validate(contract_resolution_snapshot())
    except ContractDiscoveryError as e:
        raise PipelinePlanError(f"installed contract distributions cannot be resolved: {e}") from e


def _reachable_operators(step: Any) -> list[tuple[str, str]]:
    """``(invocation path, operator name)`` for a plan step: a task leg is its own
    operator (B); a template leg reaches every operation node its tree binds, keyed
    ``<leg>/<node path>`` (C, D4); flow and gate legs reach none through this seam
    (slice A: kernels are in-engine helpers)."""
    if step.ref.startswith("task:"):
        return [(step.id, step.ref[len("task:"):])]
    if step.ref.startswith(("flow:", "gate:")):
        return []
    from cairn.core.engine.workflow_config import collect_template_operation_nodes  # noqa: PLC0415 — keeps the compiler off the door's import path
    try:
        return [(f"{step.id}/{node}", name) for node, name in collect_template_operation_nodes(step.ref)]
    except Exception:  # noqa: BLE001 — an uncompilable template fails where it fails today, not here
        return []


def _newest_installed(implements: Any, resolution: ContractResolution, kind: str) -> str | None:
    """The newest revision ``implements`` names that is installed as a ``kind`` definition."""
    installed = {f"contract://{d.id}/{d.revision}" for d in resolution.definitions if d.kind == kind}
    implemented = [ref for ref in implements if ref in installed]
    return max(implemented, key=lambda r: tuple(int(x) for x in r.rsplit("/", 1)[1].split("."))) if implemented else None


def _invocation_entry(path: str, op_cls: Any, resolution: ContractResolution) -> InvocationEntry:
    from cairn.core.pack.registry import invocation_definitions  # noqa: PLC0415 — cycle: dispatch → pack, lazy like its siblings

    ref = _newest_installed(op_cls.implements, resolution, "capability")
    if ref is None:
        raise PipelinePlanError(
            f"operator {op_cls.name!r} implements {list(op_cls.implements)} but no such capability revision is installed")
    data = invocation_definitions(ref)
    reports = getattr(op_cls, "reports", None)
    contracted = ReportSpec.from_manifest(data["report_spec"]).to_manifest() if data["report_spec"] is not None else None
    if contracted is not None and (reports is None or reports.to_manifest() != contracted):
        raise PipelinePlanError(f"operator {op_cls.name!r} reports differ from the capability's report contract {ref}")
    return InvocationEntry(path=path, capability_ref=ref,
                           definitions=tuple(ContractDefinitionRef(**d) for d in data["definitions"]),
                           operator_ref=op_cls.name, operator_version=str(op_cls.version))


def _reachable_stages(step: Any) -> list[tuple[str, list[tuple[str, bool]]]]:
    """ADR-0118 D3 — ``(entry path, [(stage, gated), …])`` for the flows a plan step reaches:
    a ``flow:`` leg's own stages (keyed by the leg id), or each ``dataflow/run``/``ml/run``
    node of a template leg (keyed ``<leg>/<node path>``)."""
    if step.ref.startswith("flow:"):
        from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 — cycle: defs → pack registry

        try:
            d = flow_def_for(step.ref)
        except Exception:  # noqa: BLE001 — an unresolvable flow fails at its leg, as it does today
            return []
        return [(step.id, [(s.stage, bool(s.gate)) for s in d.stages])] if d is not None else []
    if step.ref.startswith(("task:", "gate:")):
        return []
    from cairn.core.engine.workflow_config import collect_template_flow_stages  # noqa: PLC0415 — keeps the compiler off the door's import path
    try:
        return [(f"{step.id}/{node}", stages) for node, stages in collect_template_flow_stages(step.ref)]
    except Exception:  # noqa: BLE001 — an uncompilable template fails where it fails today, not here
        return []


def _stage_entries(step: Any, resolution: ContractResolution) -> list[InvocationEntry]:
    """ADR-0118 D3 — one ``surface="stage"`` entry per opted-in stage a step reaches; repeats
    of the declared stage (loop iterations, retries, resumes) share it."""
    from cairn.core.dataflow.defs import stage_implements, stage_version  # noqa: PLC0415 — cycle: defs → pack registry
    from cairn.core.pack.contracts import validate_implements  # noqa: PLC0415 — cycle: dispatch → pack, lazy like its siblings
    from cairn.core.pack.registry import invocation_definitions  # noqa: PLC0415 — as above

    entries = []
    for path, stages in _reachable_stages(step):
        opted = [(i, name) for i, (name, _) in enumerate(stages) if name and stage_implements(name)]
        if opted and any(gated for _, gated in stages):
            raise PipelinePlanError(f"leg {path!r}: a flow with gate: segments cannot carry opted-in stages "
                                    f"{[n for _, n in opted]} (ADR-0118 D3)")
        for index, name in opted:
            try:
                implements = validate_implements(list(stage_implements(name)))
            except ValueError as e:
                raise PipelinePlanError(f"stage {name!r}: {e}") from e
            ref = _newest_installed(implements, resolution, "stage")
            if ref is None:
                raise PipelinePlanError(f"stage {name!r} implements {list(implements)} but no such stage contract "
                                        "revision is installed")
            data = invocation_definitions(ref, kind="stage")
            entries.append(InvocationEntry(
                path=path, capability_ref=ref, definitions=tuple(ContractDefinitionRef(**d) for d in data["definitions"]),
                operator_ref=name, operator_version=stage_version(name), surface="stage", stage_index=index, stage=name))
    return entries


def _refuse_task_legs_off_their_executors(plan: PipelinePlan) -> None:
    """#2182 — a ``task:`` leg runs its operator in-process, so one whose operator
    does not declare ``in_process`` is refused here, before any record, instead of
    running somewhere it never asked to run. An unregistered operator is the
    resolver's refusal, not this one."""
    from cairn.core.dispatch.pipeline_doors import task_leg_executor_problem  # noqa: PLC0415 — keeps the operator stack off the import path
    from cairn.core.operator.registry import UnknownOperatorError, default_registry  # noqa: PLC0415 — as above

    for step in plan.steps:
        if not step.ref.startswith("task:"):
            continue
        try:
            op_cls = default_registry.get(step.ref[len("task:"):])
        except UnknownOperatorError:
            continue
        if (problem := task_leg_executor_problem(op_cls)):
            raise PipelinePlanError(f"leg {step.id!r}: {problem}")


def _with_invocations(plan: PipelinePlan, resolution: ContractResolution) -> ContractResolution:
    """#2241 B (D4) — one ``InvocationEntry`` per opted-in task-leg operator, recorded
    here before gates, admission and any record; the leg reads it from the record and
    never re-resolves. The resolver already refused an unregistered task operator; an
    opted-in operator whose revision is not in the installed closure is a plan defect."""
    from cairn.core.operator.registry import UnknownOperatorError, default_registry  # noqa: PLC0415 — operator stack off the import path

    entries = []
    for step in plan.steps:
        for path, name in _reachable_operators(step):
            try:
                op_cls = default_registry.get(name)
            except UnknownOperatorError:
                continue
            if getattr(op_cls, "implements", ()):
                entries.append(_invocation_entry(path, op_cls, resolution))
        entries += _stage_entries(step, resolution)
    return resolution.model_copy(update={"invocations": tuple(entries)}) if entries else resolution


def _read_only(value: Any) -> Any:
    """A read-only copy for a ``pipeline.launching`` payload: mappings become
    ``MappingProxyType`` over a fresh dict, lists and tuples become tuples, all
    the way down. A provider can read what the launch will use, never change it."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: _read_only(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_read_only(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_read_only(v) for v in value)
    return value


async def _ask_launch_hooks(
    *, pipeline_ref: str, workspace_id: str, environment: Optional[str], trigger: dict,
    preset: str, inputs: dict, supplied: dict, admission: Any,
) -> None:
    """Fire ``pipeline.launching`` (see :mod:`cairn.core.pack.run_hooks`) and turn a
    provider's refusal into this door's admission refusal, before any record exists.
    No provider installed: nothing is resolved and nothing is fired."""
    from cairn.core.pack import run_hooks  # noqa: PLC0415 — off the hot import path, as in the dispatcher

    if not run_hooks.load_run_hooks():
        return
    from cairn.core.workspace import workspace_account  # noqa: PLC0415 — off the hot import path

    try:
        account_id = await workspace_account(workspace_id)
    except Exception:  # noqa: BLE001 — the account is context for the provider, "" = unknown
        account_id = ""
    try:
        await run_hooks.fire(
            run_hooks.PIPELINE_LAUNCHING,
            pipeline_ref=pipeline_ref, workspace_id=workspace_id, account_id=account_id,
            environment=environment or "", source=str(trigger.get("source") or ""),
            trigger=_read_only(trigger), preset=preset,
            inputs=_read_only(inputs), supplied=_read_only(supplied),
        )
    except run_hooks.AdmissionRefused as e:
        from datetime import datetime, timezone  # noqa: PLC0415

        from cairn.core.admission_door import REFUSED, AdmissionDecision  # noqa: PLC0415

        raise AdmissionRefusedError(AdmissionDecision(
            verdict=REFUSED, refusing_reasons=[e.reason], environment=environment or "",
            workspace_id=workspace_id, policy=dict(getattr(admission, "policy", {}) or {}),
            decided_at=datetime.now(timezone.utc).isoformat(),
        )) from None


async def launch_pipeline_def(
    d: PipelineDef,
    *,
    inputs: Optional[dict] = None,
    bridge: Any,
    environment: Optional[str] = None,
    workspace_id: str = "default",
    trigger: Optional[dict] = None,
    preset: Optional[str] = None,
    bundle_ref: str = "",
) -> Any:
    """Validate → plan → start an already-RESOLVED definition.

    Split out of :func:`launch_pipeline` for ADR-0105 D1 (#2113): a bare flow
    started through ``/start`` becomes a synthesized one-leg definition
    (:func:`cairn.core.dispatch.start.synthesize_flow_pipeline`) that is never
    installed, so it cannot go through ``resolve_or_raise``; everything after
    resolution — inputs, presets, governance gates, the record — is the same
    door. ``bundle_ref`` overrides the ``pipeline:<pack>/<name>`` stamp for a
    definition that is not an installed pipeline (a flow leg stamps
    ``flow:<pack>/<name>``, the ADR-0084 D0 spelling).
    """
    assert_runnable(d)

    # #1789 — the preset layer, resolved before the required-input check so a
    # preset SATISFIES a required input rather than being merged in after we
    # have already refused for its absence. Its own two refusals (unknown preset
    # name; a preset key the pipeline no longer declares) also land here, ahead
    # of the record, for the same reason every other refusal at this door does.
    supplied = dict(inputs or {})
    preset_name = (preset or "").strip()
    preset_rec = preset_source = None
    preset_overrides: dict = {}
    if preset_name:
        canon = f"{PIPELINE_PREFIX}{d.pack}/{d.name}"
        preset_rec, preset_source = await resolve_preset(workspace_id, canon, preset_name)
        assert_preset_keys_declared(preset_rec, d.inputs or {}, preset_source)
        supplied, preset_overrides = merge_over_preset(preset_rec.inputs, supplied)

    # #2023 (ADR-0102 D2, closing #905 visibly) — an input this pipeline wires
    # into a GOVERNANCE-scoped operator field is the environment's, not the
    # run's. Such a value used to be dropped in silence at invocation time; the
    # Advanced form now shows those fields locked, and a value that arrives
    # anyway is REFUSED here, naming the field, before any record exists.
    # Checked against what the caller supplied (preset included), not against
    # the declared defaults, which are the pipeline's own and not a run value.
    assert_no_governance_inputs(d, supplied)

    if (missing := missing_inputs(d, supplied)):
        raise MissingInputsError(
            f"pipeline {d.pack}/{d.name} requires input(s) {missing!r} "
            f"— supply them or give the pipeline a default"
        )

    # #1180 — MERGE declared input defaults, then #1802's typed coercion. Both
    # live in `apply_input_defaults` so the launch preview (#2023) shows the
    # values this launch will use, computed by this launch's own code.
    merged = apply_input_defaults(d, supplied)

    plan = plan_pipeline(d)
    _refuse_task_legs_off_their_executors(plan)

    # #2240 (slice C, ADR-0111 acceptance §4) — the static contract closure is
    # resolved ONCE here, before gates, admission and any record, and rides the
    # plan into the durable record; legs never re-derive it.
    plan.contract_resolution = _with_invocations(plan, _contract_resolution())

    # #1252 (ADR-0087 P0b) — the environment's `pipeline_legs` policies fire at
    # plan time: require_hitl inserts an un-removable approval gate before each
    # matching leg; deny raises here, BEFORE any record exists (fail-closed, no
    # half-started pipeline). Same shared evaluator dispatch enforcement uses.
    from cairn.core.policy.pipeline_gates import inject_policy_gates  # noqa: PLC0415 - cycle: policy → approval → engine.runner → dispatch

    injected = inject_policy_gates(plan, environment=environment)
    if injected:
        logger.info("policy injected %d gate(s) into %s/%s: %s",
                    len(injected), d.pack, d.name, injected)

    # #2101 (ADR-0099 §1, ADR-0104 D2/D5) — the SERVICE doors: job-channel,
    # SkyPilot, pull-worker. `RunDispatcher.dispatch` has asked all three since
    # they were written, but a pipeline `flow:` leg goes through NEITHER the
    # dispatcher nor the preflight — the bridge calls `resolve_engine()` /
    # `engine.submit()` directly — so a launch that had to be refused was
    # admitted with HTTP 200 and died late at submit, after the pipeline record,
    # the leg record and the run all existed. Every fine-tune pipeline is a flow
    # leg. Asked HERE, beside the policy gates and ahead of `start_pipeline`, so
    # a refused launch leaves nothing half-started; the engine's own late
    # `EngineError` stays as the backstop.
    from cairn.core.dispatch.pipeline_doors import assert_pipeline_services  # noqa: PLC0415 — the doors probe sockets and the job store; keep them off the import path

    await assert_pipeline_services(plan, pipeline_ref=f"{d.pack}/{d.name}")

    # #2190 (T05, C3) — the ADMISSION DECISION: once, here, over the pipeline's
    # declared inputs, before any record exists (the maintainer's decision on
    # #2190). Reuses the intake/holdout/licence doors the agentic path runs
    # per leg; adds the policy's required-check rule; refuses with every reason.
    from cairn.core.admission_door import decide_admission  # noqa: PLC0415 — off the hot import path

    admission = await decide_admission(merged, workspace_id=workspace_id, environment=environment or "")
    if admission.refused:
        raise AdmissionRefusedError(admission)
    # #1789 criterion 4 — the record answers "launched from what". `trigger` is
    # already the record's provenance dict (what started it), so the preset name
    # and the keys the caller overrode on top of it go there rather than into a
    # new column: every store persists the record as one JSON blob, so this
    # needs no migration and reads back on rows written before today.
    trig = dict(trigger or {"source": "launch"})
    if preset_name:
        trig["preset"] = preset_name
        trig["preset_source"] = preset_source
        if preset_overrides:
            trig["preset_overrides"] = preset_overrides

    # `pipeline.launching` — an installed run hook sees this launch (its ref,
    # inputs, workspace, account and trigger) and may refuse it. Asked LAST among
    # the refusals, after the policy gates, the service doors and the admission
    # decision, and before the record: a refusal leaves nothing behind, and it
    # reaches every caller as the admission refusal they already handle. Nothing
    # above takes a side effect (the preset is touched only after the record),
    # so there is nothing to release here. The event fails closed.
    stamp = bundle_ref or f"{PIPELINE_PREFIX}{d.pack}/{d.name}"
    await _ask_launch_hooks(
        pipeline_ref=stamp, workspace_id=workspace_id, environment=environment, trigger=trig,
        preset=preset_name, inputs=merged, supplied=supplied, admission=admission,
    )

    logger.info("launching pipeline %s/%s (%d legs)%s", d.pack, d.name, len(plan.steps),
                f" from preset {preset_name!r} ({preset_source})" if preset_name else "")
    rec = await bridge.start_pipeline(
        plan,
        inputs=merged,
        bundle_ref=stamp,
        environment=environment,
        workspace_id=workspace_id,
        trigger=trig,
        admission=admission.as_record(),
    )
    if preset_rec is not None:
        await touch_preset(workspace_id, preset_rec, preset_source or "")
    return rec


__all__ = [
    "GovernanceInputError", "InputCoercionError", "MissingInputsError",
    "PipelineDriftError", "PipelineNotFoundError", "ServiceDoorError",
    "coerce_declared_inputs", "launch_pipeline", "launch_pipeline_def",
    "missing_inputs", "resolve_or_raise",
]
