"""Preview a launch, and refuse an env-owned value at the door (ADR-0102 D2, #2023).

Two things live here, and they are the same thing seen from two sides.

**The preview** — "show me what will run". The launch dialog's Advanced tier
asks for the effective configuration *before* Run: the pipeline's inputs after
its preset and its declared defaults, and, for every leg whose operator is
known, that operator's effective inputs and config after ADR-0068/0070 layering
and its own schema defaults. That per-step body is produced by
:func:`~cairn.core.operator.effective.effective_parameters` — the very function
the runtime uses — and is byte-identical to the ``parameters`` run event
ADR-0103 D3 records when the step actually runs. A preview computed a second
way would be a promise nobody checked.

**The refusal** — #905's trap, closed visibly. A GOVERNANCE-scoped config field
(ADR-0070) is owned by the environment. A run that supplied one had it silently
dropped by ``strip_governance_inputs`` at invocation time: the run proceeded, the
guardrail held, and the user was told nothing — so an Advanced form that shows
env-owned fields (as ADR-0102 D2 requires: shown, locked, labelled) would be one
mis-click away from a launch that lies about what it did. Now the launch door
refuses, **naming the field**, before any record exists.

The refusal is deliberately narrow: it fires on an input the pipeline itself
wires into a governance-scoped field, never on a same-named input that goes
somewhere else. A training pipeline's ``max_eval_loss`` that reaches a flow kernel (the
train leg's fail-fast ceiling) is not ``model/gate.thresholds.max_eval_loss``, and must
keep working.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

from cairn.core.engine.pipeline import PipelineDef, plan_pipeline

logger = logging.getLogger("cairn.core.dispatch.pipeline_preview")

#: ``$input.<key>`` anywhere inside a wire — the plain form and the
#: ``$default($input.x, …)`` form both match.
_INPUT_REF = re.compile(r"\$input\.([A-Za-z_][A-Za-z0-9_]*)")


class GovernanceInputError(ValueError):
    """A launch carried a value for a field the ENVIRONMENT owns (ADR-0068/0070).

    Raised **fail-closed** at the launch door, before any record exists and
    before anything spends, with every offending field named. Carries
    ``fields`` (the human-readable list) for the caller → HTTP 400.
    """

    def __init__(self, conflicts: list["GovernedInput"]):
        self.conflicts = list(conflicts)
        self.fields = [c.describe() for c in conflicts]
        super().__init__(
            "these inputs are owned by the environment and cannot be set per run "
            "(ADR-0068/0070): " + "; ".join(self.fields)
            + " — set them in the environment's [profile.config], or leave them "
              "to it. A per-run value here was silently dropped before #2023."
        )


@dataclass(frozen=True)
class GovernedInput:
    """One declared pipeline input that lands on an env-owned operator field."""

    input: str
    operator: str
    field: str
    step_id: str

    def describe(self) -> str:
        return f"{self.input} → {self.operator}.{self.field} (step {self.step_id})"

    def as_dict(self) -> dict[str, str]:
        return {"input": self.input, "operator": self.operator,
                "field": self.field, "step_id": self.step_id}


def _governed_top_level_fields(op_name: str) -> set[str]:
    """Top-level ``config_schema`` field names of ``op_name`` that are governance
    scoped (directly, or through any nested leaf). Empty when the operator is not
    installed or declares no config — unknown never means governed."""
    from cairn.core.operator.config import GOVERNANCE, partition_by_scope  # noqa: PLC0415 — cycle: dispatch → operator
    from cairn.core.operator.registry import default_registry  # noqa: PLC0415 — same

    try:
        if not default_registry.has(op_name):
            return set()
        schema = getattr(default_registry.get(op_name), "config_schema", None)
        if schema is None:
            return set()
        return {f["path"].split(".", 1)[0] for f in partition_by_scope(schema)[GOVERNANCE]}
    except Exception:  # noqa: BLE001 — an unresolvable operator is unchecked, never a launch failure
        logger.debug("could not read the config scope surface of %s", op_name, exc_info=True)
        return set()


def _template_operators(ref: str) -> list[str]:
    from cairn.core.engine.workflow_config import collect_template_operators  # noqa: PLC0415 — cycle

    try:
        return collect_template_operators(ref)
    except Exception:  # noqa: BLE001 — an uninstalled pack is unchecked, not fatal
        return []


def governed_inputs(d: PipelineDef) -> list[GovernedInput]:
    """Every declared input of ``d`` that this pipeline wires into an env-owned
    operator field — what the Advanced form renders locked, and what the door
    refuses a value for.

    A ``task:`` leg is resolved exactly: its ``with:`` keys ARE the operator's
    input fields, so a key that names a governance-scoped config field is a lock.
    A ``template:`` leg is resolved by name over the operators the template
    reaches — the ``with:`` sugar auto-binds an unmapped field to
    ``$default($input.<f>, …)``, so a leg input sharing the field's name is the
    wire. ``flow:`` and ``gate:`` legs carry no operator config surface.
    """
    out: list[GovernedInput] = []
    seen: set[tuple[str, str, str, str]] = set()
    # A `loop:` step is a container — its body legs carry the wires (#1129).
    flat: list[Any] = []
    for s in d.steps:
        flat.append(s)
        flat.extend(getattr(s, "steps", None) or [])
    for step in flat:
        kind = step.leg_kind
        wires = dict(step.inputs or {})
        if not wires:
            continue
        if kind == "task":
            pairs = [(step.ref or "", _governed_top_level_fields(step.ref or ""))]
        elif kind == "template":
            pairs = [(op, _governed_top_level_fields(op)) for op in _template_operators(step.ref or "")]
        else:
            continue
        for op_name, governed in pairs:
            if not governed:
                continue
            for key, wire in wires.items():
                if key not in governed:
                    continue
                for ref in _INPUT_REF.findall(str(wire)):
                    k = (ref, op_name, key, step.name)
                    if k not in seen:
                        seen.add(k)
                        out.append(GovernedInput(input=ref, operator=op_name,
                                                 field=key, step_id=step.name))
    return sorted(out, key=lambda g: (g.input, g.operator, g.field))


def governance_conflicts(d: PipelineDef, supplied: dict[str, Any]) -> list[GovernedInput]:
    """The subset of :func:`governed_inputs` the caller actually supplied a value for."""
    keys = set(supplied or {})
    return [g for g in governed_inputs(d) if g.input in keys]


def assert_no_governance_inputs(d: PipelineDef, supplied: dict[str, Any]) -> None:
    """The launch-door gate. Raises :class:`GovernanceInputError` naming every field."""
    if (bad := governance_conflicts(d, supplied)):
        raise GovernanceInputError(bad)


def apply_input_defaults(d: PipelineDef, supplied: dict[str, Any]) -> dict[str, Any]:
    """Declared defaults merged under ``supplied``, then typed coercion (#1180/#1802).

    Extracted from :func:`~cairn.core.dispatch.pipeline_launch.launch_pipeline` so
    the preview shows the values the launch will use, computed by the launch's own
    code rather than a lookalike.
    """
    from cairn.core.dispatch.pipeline_launch import coerce_declared_inputs  # noqa: PLC0415 — cycle: launch ↔ preview

    merged = dict(supplied or {})
    for key, spec in (d.inputs or {}).items():
        if key not in merged and isinstance(spec, dict) and "default" in spec:
            merged[key] = spec["default"]
    return coerce_declared_inputs(d, merged)


def _profile_config() -> Optional[dict[str, Any]]:
    """The environment's ``[profile.config]`` layer, sourced the way the bridge
    sources it for a task leg — best-effort; a broken profile previews as none."""
    try:
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415 — cycle: dispatch → pack

        return dict(load_profile_from_env().config)
    except Exception:  # noqa: BLE001 — config is additive; a broken profile never fails a preview
        return None


def _preview_contract(op_cls: Any) -> Any:
    """#2241 C (D6) — the envelope the launch door would record for this operator, so the
    preview refuses exactly what the run refuses (#2023). ``None`` for a legacy operator."""
    if not getattr(op_cls, "implements", ()):
        return None
    from cairn.core.dispatch.pipeline_launch import _contract_resolution, _invocation_entry  # noqa: PLC0415 — cycle: launch imports preview
    from cairn.core.operator.effective import ContractEnvelope  # noqa: PLC0415 — as above
    from cairn.core.pack.registry import invocation_contract  # noqa: PLC0415 — dispatch → pack, lazy like its siblings

    resolution = _contract_resolution()
    entry = _invocation_entry("preview", op_cls, resolution)
    uris = lambda defs: [(f"contract://{d.id}/{d.revision}", d.digest) for d in defs]  # noqa: E731 — two uses, one line
    data = invocation_contract(uris(entry.definitions), uris(resolution.definitions))
    return ContractEnvelope.from_dict({**data, "operator_ref": entry.operator_ref, "operator_version": entry.operator_version})


def _preview_step(plan_step: Any, *, inputs: dict[str, Any],
                  profile_config: Optional[dict[str, Any]]) -> dict[str, Any]:
    """One leg's row of the preview: the ``parameters`` body it will record, or
    an honest reason why it cannot be known yet."""
    ref = str(getattr(plan_step, "ref", "") or "")
    kind = (ref.split(":", 1)[0] if ":" in ref else "template")
    row: dict[str, Any] = {"step_id": plan_step.id, "ref": ref, "kind": kind,
                           "operator": "", "known": False, "reason": ""}
    from cairn.core.engine.bundle import resolve_step_inputs  # noqa: PLC0415 — cycle: dispatch → engine
    from cairn.core.operator.effective import (  # noqa: PLC0415 — cycle: dispatch → operator
        ContractConformanceError, EffectiveParametersError, effective_parameters, flow_parameters,
    )

    if kind != "task":
        # A flow leg (kind ml | data) runs its stages inside its engine, so the
        # operator-level config of those stages is not knowable here — but the
        # leg's OWN resolved inputs are, and they are exactly what the leg
        # records when it executes (leg_executor → record_flow_parameters).
        # Same producer, same shape: the preview row equals the run's event.
        try:
            resolved = resolve_step_inputs(plan_step, bundle_input=dict(inputs), steps={})
        except Exception as e:  # noqa: BLE001 — an unresolvable upstream wire is a reason, not a failure
            row["reason"] = (f"resolved when the leg runs: {type(e).__name__}: {e}")[:400]
            return row
        row.update(flow_parameters(ref, kind, resolved).as_payload())
        row["known"] = True
        row["reason"] = ("a flow leg's stage-level operator config resolves inside its engine; "
                         "its inputs are what it records")
        return row

    op_name = ref[len("task:"):]
    row["operator"] = op_name
    from cairn.core.operator.registry import default_registry  # noqa: PLC0415 — same

    try:
        if not default_registry.has(op_name):
            row["reason"] = f"operator {op_name!r} is not installed here"
            return row
        op_cls = default_registry.get(op_name)
        from cairn.core.dispatch.pipeline_doors import task_leg_executor_problem  # noqa: PLC0415 — #2182, the launch door's own rule
        if (problem := task_leg_executor_problem(op_cls)):
            row["reason"] = problem
            return row
        resolved = resolve_step_inputs(plan_step, bundle_input=dict(inputs), steps={})
        eff = effective_parameters(op_cls, resolved, profile_config=profile_config,
                                   contract=_preview_contract(op_cls))
    except (EffectiveParametersError, ContractConformanceError) as e:
        row["reason"] = str(e)
        return row
    except Exception as e:  # noqa: BLE001 — an unresolvable upstream wire is a reason, not a failure
        row["reason"] = (f"resolved when the leg runs: {type(e).__name__}: {e}")[:400]
        return row
    row.update(eff.as_payload())
    row["known"] = True
    return row


def preview_launch(
    d: PipelineDef, *, inputs: Optional[dict[str, Any]] = None,
    preset_inputs: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """"Show what will run": effective pipeline inputs, where each came from, the
    env-owned fields this pipeline locks, and each resolvable leg's ``parameters``.

    Side-effect free by construction — nothing is submitted, no record is
    created, no operator is instantiated.
    """
    supplied = dict(inputs or {})
    base = dict(preset_inputs or {})
    base.update(supplied)
    merged = apply_input_defaults(d, base)

    sources: dict[str, str] = {}
    for key in merged:
        sources[key] = ("run" if key in supplied
                        else "preset" if key in (preset_inputs or {})
                        else "default")

    profile_config = _profile_config()
    try:
        plan = plan_pipeline(d)
        steps = [_preview_step(s, inputs=merged, profile_config=profile_config)
                 for s in plan.steps]
    except Exception as e:  # noqa: BLE001 — a plan failure is a preview finding, not a 500
        logger.info("preview could not plan %s/%s: %s", d.pack, d.name, e)
        steps = []

    return {
        "ref": f"{d.pack}/{d.name}",
        "inputs": merged,
        "sources": sources,
        "locked": [g.as_dict() for g in governed_inputs(d)],
        "conflicts": [g.as_dict() for g in governance_conflicts(d, base)],
        "steps": steps,
    }


__all__ = [
    "GovernanceInputError", "GovernedInput", "apply_input_defaults",
    "assert_no_governance_inputs", "governance_conflicts", "governed_inputs",
    "preview_launch",
]
