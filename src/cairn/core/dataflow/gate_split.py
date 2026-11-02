"""In-flow HITL by SPLITTING the flow, not by engine-native pause (#1079).

A gate *inside* a flow becomes ``flow A → cairn gate → flow B``: two engine jobs
with an artifact handoff and the **existing** approval queue between them.

**Why not a ``supports_pause`` capability per engine** (the original #1079
shape). builtin has no suspend primitive, and Metaflow's only one is
Argo-specific — so an engine-native design lands on a capability one engine
at most could honour while presenting itself as a protocol capability. Splitting instead:

* works on **every** engine, present and future, including the two we ship;
* adds **zero** protocol surface — no ``pause``/``resume``, no new conformance
  cases, nothing an adapter author can get wrong;
* satisfies #1079's hard requirement — the operator sees ONE approval model —
  **by construction**, because the thing between the segments is a plain cairn
  gate. A split flow's approval is the same object, endpoint and console queue
  as every other approval;
* costs little: our stages already chain through files, and Prefect's own
  ``suspend_flow_run`` tears down infrastructure anyway.

The governance invariant this module protects: **a ``gate:`` marker is never
silently dropped**. A gated flow handed to the ordinary whole-flow door fails
closed at compile time (:func:`assert_runnable_whole`) — because executing every
stage and asking no one is a gate that does not gate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from cairn.core.dataflow.contract import StageRef

if TYPE_CHECKING:  # pragma: no cover - typing only
    from cairn.core.dataflow.defs import FlowDef


class GateSplitError(ValueError):
    """An in-flow gate declaration that cannot be split into a valid chain."""


def handoff_config_key(index: int) -> str:
    """Config key carrying the handoff artifact path across gate ``index``.

    A placeholder rather than a computed path: the segments are separate engine
    jobs on possibly different machines, so the *caller* (template binding) owns
    where the intermediate dataset lands — exactly how every other run-specific
    value reaches a static def.
    """
    return f"handoff_{index}"


@dataclass
class FlowSegment:
    """One runnable piece of a split flow.

    ``gate_after`` is the approval that must clear before the NEXT segment runs;
    ``None`` marks the final segment. ``handoff_key``/``resumes_from`` name the
    config placeholder the segment writes to / reads from — they must agree
    across a boundary or the working dataset vanishes between jobs.
    """

    name: str
    stages: list[StageRef]
    gate_after: Optional[str] = None
    handoff_key: Optional[str] = None
    resumes_from: Optional[str] = None
    source_flow: str = ""
    index: int = 1

    def __post_init__(self) -> None:
        self.stages = list(self.stages)

    @property
    def is_final(self) -> bool:
        return self.gate_after is None


def has_in_flow_gates(flow: "FlowDef") -> bool:
    """Does this flow declare an approval gate BETWEEN stages?"""
    return any(getattr(s, "gate", None) for s in flow.stages)


def _handoff_pair(flow: "FlowDef", first_gate: str) -> tuple[str, str]:
    """The declared ``export``/``ingest`` stage names, or a directive error."""
    handoff = getattr(flow, "handoff", None)
    export = str(getattr(handoff, "export", "") or "") if handoff else ""
    ingest = str(getattr(handoff, "ingest", "") or "") if handoff else ""
    if not (export and ingest):
        raise GateSplitError(
            f"flow {flow.name!r} declares gate {first_gate!r} between stages but no "
            f"`handoff:` block. Segments run as SEPARATE engine jobs with separate "
            f"workdirs, so the working dataset must be materialized at the boundary "
            f"— declare `handoff: {{export: <stage>, ingest: <stage>}}` "
            f"(e.g. export.jsonl / ingest.normalize)"
        )
    return export, ingest


def split_at_gates(flow: "FlowDef") -> list[FlowSegment]:
    """Split ``flow`` into segments at each ``gate:`` marker.

    A stage carrying ``gate: <name>`` means *require approval BEFORE this stage*,
    so the cut lands immediately before it. Handoff stages are injected at each
    boundary — the export closing one segment and the ingest opening the next
    point at the same config placeholder.

    A flow with no gates returns exactly one segment holding its stages
    **unmodified**: every flow shipped today must pass through untouched.
    """
    stages = list(flow.stages)
    cuts: list[tuple[int, str]] = []
    seen: set[str] = set()

    for i, st in enumerate(stages):
        raw = getattr(st, "gate", None)
        if raw is None:
            continue
        gate = str(raw).strip()
        if not gate:
            raise GateSplitError(
                f"flow {flow.name!r}: stage {st.stage!r} declares an empty gate name — "
                f"a gate is audited BY name, so it must be a stable identifier"
            )
        if i == 0:
            raise GateSplitError(
                f"flow {flow.name!r}: stage {st.stage!r} declares gate {gate!r} on the "
                f"FIRST stage — nothing runs before it, so that is the binding's own "
                f"approval gate (`approval: required: true`), which is already "
                f"supported. Two ways to express one thing is how one of them rots."
            )
        if gate in seen:
            raise GateSplitError(
                f"flow {flow.name!r}: duplicate gate name {gate!r} — approvals are "
                f"audited by name, so each gate needs a distinct one"
            )
        seen.add(gate)
        cuts.append((i, gate))

    if not cuts:
        return [FlowSegment(name=flow.name, stages=stages,
                            source_flow=flow.name, index=1)]

    export_stage, ingest_stage = _handoff_pair(flow, cuts[0][1])

    segments: list[FlowSegment] = []
    start = 0
    for n, (cut_at, gate) in enumerate(cuts, start=1):
        key = handoff_config_key(n)
        body = [_clean(s) for s in stages[start:cut_at]]
        body.append(StageRef(stage=export_stage, config={"path": f"$config.{key}"}))
        segments.append(FlowSegment(
            name=f"{flow.name}@{len(segments) + 1}",
            stages=body,
            gate_after=gate,
            handoff_key=key,
            resumes_from=handoff_config_key(n - 1) if n > 1 else None,
            source_flow=flow.name,
            index=len(segments) + 1,
        ))
        start = cut_at

    tail = [StageRef(stage=ingest_stage,
                     config={"path": f"$config.{handoff_config_key(len(cuts))}"})]
    tail += [_clean(s) for s in stages[start:]]
    segments.append(FlowSegment(
        name=f"{flow.name}@{len(segments) + 1}",
        stages=tail,
        gate_after=None,
        resumes_from=handoff_config_key(len(cuts)),
        source_flow=flow.name,
        index=len(segments) + 1,
    ))

    # every non-final segment opens with the ingest that matches its predecessor
    for prev, nxt in zip(segments, segments[1:]):
        if nxt.stages[0].stage != ingest_stage:
            nxt.stages.insert(0, StageRef(
                stage=ingest_stage, config={"path": f"$config.{prev.handoff_key}"}))
            nxt.resumes_from = prev.handoff_key

    return segments


def _clean(stage: StageRef) -> StageRef:
    """Copy a stage with its gate marker consumed (the split IS the gate)."""
    return StageRef(stage=stage.stage, config=dict(stage.config),
                    resources=stage.resources)


def assert_runnable_whole(flow: "FlowDef", *, site: str) -> None:
    """Fail closed when a gate-carrying flow is run through the whole-flow door.

    Running it whole would execute every stage and ask no one — the marker would
    become a comment. Raised at COMPILE time so it can never reach production.
    """
    from cairn.core.dataflow.defs import DataflowDefError  # noqa: PLC0415 - cycle

    gates = [str(getattr(s, "gate", "") or "") for s in flow.stages
             if getattr(s, "gate", None)]
    if gates:
        raise DataflowDefError(
            f"{site}: flow {flow.name!r} declares in-flow approval gate(s) "
            f"{gates!r} but is bound to run WHOLE — every stage would execute "
            f"and no one would be asked. Split it into gated segments "
            f"(`cairn pack show --split {flow.name}`) and bind the segments with the "
            f"approval gate between them (#1079)."
        )


__all__ = [
    "FlowSegment", "GateSplitError", "assert_runnable_whole",
    "handoff_config_key", "has_in_flow_gates", "split_at_gates",
]
