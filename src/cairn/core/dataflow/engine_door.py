"""The flow-engine door (ADR-0134 D3c; ADR-0099 §1 — derive, refuse, never provision).

A launch that reaches a flow engine depends on things nobody checks until the leg runs: a
package that provides the selected engine is installed, and the engine is ready — for a
provisioning engine, that its target resolves and its cloud is enabled. Missing any of them,
the run fails at submit, after the run record exists and possibly after a machine has started
billing. ADR-0099 §1: a requirement implied by configuration is DERIVED, enforced at the
launch door BEFORE any record exists, with the remedy named; the control plane refuses
rather than installing or provisioning.

This replaces the SkyPilot door of ADR-0104 D5 (``cairn.core.skypilot_door``) with one that
names no engine:

- **Scope — only the kinds a launch reaches.** An engine is resolved only by a flow leg or by
  the ``dataflow/run`` / ``ml/run`` operators (:data:`ENGINE_OPERATORS`); a launch reaching
  neither asks nothing. Callers pass the reached kinds (:func:`kinds_reached`).
- **``builtin`` is exempt.** An install without the builtin dataflow pack is legitimate (an
  agentic-only control plane, :func:`~cairn.core.dataflow.engine_registry.installed_engines`);
  its absence keeps surfacing where it always did.
- **Core asks; the pack answers.** Engine readiness is engine-specific — only the adapter
  knows what an infra is or which credential file a cloud reads — and core never imports an
  operator pack. So the door goes through the registry: a missing factory IS "not installed",
  and an engine that offers the optional ``check_ready()`` capability is asked for its
  reasons. An engine without it is ready once installed.

The verdict is :class:`~cairn.core.job_channel_door.ServiceVerdict`, so preflight, the
dispatcher and the pipeline door render it like the other service doors.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional

from cairn.core.dataflow.engine_registry import load_engine_factory, selected_engine, selection_keys
from cairn.core.job_channel_door import ServiceVerdict

#: The engine every install may lack without being refused for it.
BUILTIN = "builtin"

#: The flow kinds an environment selects an engine for (ADR-0082 D7).
FLOW_KINDS: tuple[str, ...] = ("data", "ml")

#: kind → the operator whose presence means a launch resolves an engine for that kind, whether
#: it arrives as a ``flow:`` leg or as a node inside a template.
ENGINE_OPERATORS: dict[str, str] = {"data": "dataflow/run", "ml": "ml/run"}
_KIND_OF_OPERATOR: dict[str, str] = {op: kind for kind, op in ENGINE_OPERATORS.items()}


def engines_selected() -> dict[str, str]:
    """``{kind: engine}`` for both flow kinds, through the one config plane."""
    return {kind: selected_engine(kind)[0] for kind in FLOW_KINDS}


def kinds_reached(operators: Iterable[str]) -> list[str]:
    """The flow kinds a launch invoking ``operators`` resolves an engine for — empty when it
    reaches none (gates, task legs, agentic templates)."""
    return sorted({k for op in operators if (k := _KIND_OF_OPERATOR.get(str(op)))})


async def check_flow_engines(*, kinds: Optional[Iterable[str]] = None,
                             factory: Optional[Callable[[str], Any]] = None) -> ServiceVerdict:
    """The verdict for a launch reaching ``kinds`` (``None`` = both).

    ``factory`` is the registry lookup, injectable so the door is provable without an engine
    installed — a live provisioning engine starts a paid machine, so the door is never tested
    against one.
    """
    wanted = FLOW_KINDS if kinds is None else tuple(
        k for k in FLOW_KINDS if k in {str(x).strip().lower() for x in kinds})
    verdict = ServiceVerdict()
    lookup = factory or load_engine_factory
    for kind in wanted:
        engine, target = selected_engine(kind)
        if engine == BUILTIN:
            continue
        if engine not in verdict.required:
            verdict.required.append(engine)
        engine_key = selection_keys(kind)[0]
        try:
            make = lookup(engine)
        except Exception as exc:  # noqa: BLE001 — a registration that will not load is a refusal, never a 500
            verdict.ok = False
            verdict.reasons.append(
                f"{kind} flows are set to the {engine!r} engine, but the package registering it "
                f"does not load ({type(exc).__name__}: {exc}) — reinstall its package "
                f"or set [profile.backends] {engine_key} to "
                "an engine that is installed."
            )
            continue
        if make is None:
            verdict.ok = False
            verdict.reasons.append(
                f"{kind} flows are set to the {engine!r} engine, but no installed package "
                f"provides it — install the package that does, or set "
                f"[profile.backends] {engine_key} back to an engine that is installed. The "
                "control plane does not install packages for you."
            )
            continue
        try:
            built = make(kind=kind, target=target)
            check = getattr(built, "check_ready", None)
            reasons = list(await check()) if check is not None else []
        except Exception as exc:  # noqa: BLE001 — an unreadable check is a refusal, never a 500
            verdict.ok = False
            verdict.reasons.append(
                f"{kind} flows are set to the {engine!r} engine and its readiness check "
                f"could not complete: {exc}"
            )
            continue
        if reasons:
            verdict.ok = False
            verdict.reasons.extend(f"{kind} flows are set to the {engine!r} engine: {r}" for r in reasons)
    return verdict


__all__ = [
    "BUILTIN",
    "ENGINE_OPERATORS",
    "FLOW_KINDS",
    "ServiceVerdict",
    "check_flow_engines",
    "engines_selected",
    "kinds_reached",
]
