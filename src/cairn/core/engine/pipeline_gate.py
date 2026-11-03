"""Gate context for a pipeline gate step (#1128, ADR-0084).

The bridge already pauses on approval and lands in ``awaiting_approval`` — the
queue the console and ``/runs/{id}/pending-approval`` already read. What it did
not carry is what an approver needs in order to decide: **which gate**, **what to
look at**, **who may decide**, **by when**.

``approvers`` is enforced, not only shown (#2393): the approve door
(``routers/pipelines.py``) resolves the gate's audience through
:func:`required_approvers` — the same rule :func:`gate_context_for` renders — and
refuses a caller it does not name (``approval/enforce.py:approver_allowed``). What
a name means is defined there: a username or a role; an owner/admin always may.

This module builds exactly that, and nothing else. In particular it does NOT
introduce a status, an endpoint, or a store: the whole point of ADR-0084's HITL
story is that every gate — in-flow, structural, explicit, cross-leg — lands in
ONE queue. A pipeline gate that grew its own surface would be the failure this
design exists to avoid.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from cairn.core.engine.expr import step_path

#: The EXISTING approval status. Named here so the invariant is greppable and a
#: test can pin it — a pipeline gate must never invent a parallel vocabulary.
GATE_STATUS = "awaiting_approval"


@dataclass
class GateContext:
    """What the approver sees. Serializes to the shape the existing endpoint and
    console already render."""

    gate: str
    fields: dict[str, Any] = field(default_factory=dict)
    approvers: str = ""
    #: The gate's declared ``timeout:``, shown as-is. Not enforced: a pipeline
    #: gate does not expire yet (#259, ADR-0158 proposed).
    timeout: str = ""
    step_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "fields": dict(self.fields),
            "approvers": self.approvers,
            "timeout": self.timeout,
            "step_id": self.step_id or self.gate,
        }


def resolve_show(refs: list[str], outputs: dict[str, Any]) -> dict[str, Any]:
    """Resolve ``<step>.<field>`` refs against prior legs' persisted outputs.

    Uses the ``steps``-scope lookup (#1689): the exact path first, else the same
    path under ``findings``. That fallback is not a convenience — it is the
    difference between the two gate surfaces speaking the same grammar. A FLOW
    leg's stats land under ``findings`` (``pipeline_bridge`` builds the step
    output that way), and gate PREDICATES have always had the fallback via
    ``expr._step_lookup``. Without it here, ``predicate:
    steps.delta.eval_acc`` resolved to a number while the identical spelling in
    ``show:`` rendered ``None`` — the approver deciding a paid promotion saw a
    wall of nulls where the predicate saw values.

    Borrowed rather than re-derived, deliberately: a third copy of the lookup
    rule is how the two surfaces drifted apart in the first place.

    A ref that does not resolve is still reported as ``None`` rather than
    dropped: silently omitting a field the author asked for would show the
    approver an incomplete picture with nothing indicating something was
    missing.
    """
    out: dict[str, Any] = {}
    for ref in refs or []:
        step, _, rest = str(ref).partition(".")
        parts = [p for p in rest.split(".") if p]
        step_out = (outputs or {}).get(step)
        out[str(ref)] = (
            step_out if not parts
            else step_path(step_out, parts, source=f"gate show {ref!r}")
        )
    return out


def required_approvers(step: Any, governance: Optional[dict[str, Any]] = None) -> str:
    """The gate's audience: the step's ``approvers`` first, else the pipeline's
    ``governance.approvers``, else ``""`` (unstaffed). ONE rule for the two
    readers — the context the approver sees and the door that enforces it
    (#2393) — so they can never name different people."""
    gov = governance or {}
    return str(getattr(step, "approvers", None) or gov.get("approvers", "") or "")


def gate_context_for(
    step: Any,
    *,
    outputs: dict[str, Any],
    governance: Optional[dict[str, Any]] = None,
) -> GateContext:
    """Build the approver's context for one gate step.

    ``approvers`` resolves step-first, then the pipeline's ``governance`` default.
    An unstaffed gate yields an empty audience and still pauses — a gate must
    never fail OPEN because nobody was named.

    Accepts EITHER step shape, because two callers hand it different ones: the
    authored :class:`~cairn.core.engine.pipeline.PipelineStep` (``.gate`` is the
    gate's NAME, ``.name`` is a property) and the execution-IR
    :class:`~cairn.core.engine.bundle_pipeline.PipelinePlanStep` the bridge
    actually passes (``.gate`` holds the PREDICATE, there is no ``.name``, and
    ``.id`` carries the iteration suffix inside a loop). Identity therefore
    comes from the ID first and the authored name second — never from ``gate``,
    which means two different things depending on who is asking (#1627).
    """
    gov = governance or {}
    step_id = str(getattr(step, "id", "") or getattr(step, "name", "") or "")
    return GateContext(
        gate=step_id or str(getattr(step, "gate", "") or ""),
        fields=resolve_show(list(getattr(step, "show", []) or []), outputs),
        approvers=required_approvers(step, gov),
        timeout=str(getattr(step, "timeout", None) or gov.get("timeout", "") or ""),
        step_id=step_id,
    )


__all__ = ["GATE_STATUS", "GateContext", "gate_context_for", "required_approvers", "resolve_show"]
