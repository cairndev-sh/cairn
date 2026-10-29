"""``cairn pipeline explain`` — a DRY RUN of the resolution that executes (#1792).

The owner's words: *"I need to review carefully how each steps are triggered."*
Before spending, a person must be able to see, per leg: what will run, WHERE it
will run, who is asked before it runs, what values it receives and where each
came from, where its outputs land, and what a failure will do.

**It is a projection, not a description.** Every field below is produced by
calling the SAME function the runtime calls — ``plan_pipeline`` for the plan,
``resolve_flow_key``/``flow_def_for`` for the target file, ``selected_engine``
for the engine, ``resolve_gate``/``enforce_flow_gate``/``resolve_runtime_gate``
for the gates, ``inject_policy_gates`` for the ones the environment adds,
``timeout_seconds`` for the limit, ``_leg_retry_policy`` for the retry. Nothing
here re-implements a rule, so explain cannot drift from execution; when a
resolution rule changes, this output changes with it or the shared function is
not shared.

The corollary is that explain tells the TRUTH, not the intent. A ``retry:``
that is not a map (``retry: 3``) parses, reaches the leg and is silently
dropped by ``_leg_retry_policy`` — :func:`explain_pipeline` renders that as
``retry: IGNORED (not a map)`` because that is what will happen. (Until #1675
every inline ``retry: {…}`` landed here: the planner ``str()``'d it.)

#2134 S4 adds the OTHER cut of the same facts: ``doc["resolved"]`` re-lists every
effective value beside the **document that owns it** — ``pipeline`` ·
``profile:<name>`` · ``workspace`` · ``run`` · ``default`` · ``inherited:<leg>``.
ADR-0106 D4 keeps those owners separate on purpose, so the antidote to holding
four files in your head is one view that merges them for reading without merging
them for editing. It re-derives nothing: see
:mod:`cairn.core.engine.pipeline_resolved`.

What explain deliberately does NOT do: resolve wires to values (the producing leg
has not run), take a budget reservation, or touch an engine. It reads installed
packs, the environment's selection plane, the deployment's policy set and the
workspace's variables.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import inspect
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from cairn.core.dataflow.engine_registry import selection_keys
from cairn.core.engine.pipeline import (
    PipelineDef,
    PipelineStep,
    pipeline_pack_name,
    plan_pipeline,
    timeout_seconds,
)
from cairn.core.engine.pipeline_resolved import (
    PIPELINE,
    SOURCES,
    UNKNOWN,
    Layers,
    ambient_profile,
    backend_rows,
    environment_row,
    governance_rows,
    input_rows,
    profile_source,
    resolve_layer,
)

logger = logging.getLogger("cairn.core.engine.pipeline_explain")

#: Which runtime executes each leg kind (the `cairn pipeline show` table, reused
#: so the two verbs cannot disagree about the adapter).
LEG_RUNTIME = {
    "flow": "engine adapter",
    "template": "langgraph adapter",
    "task": "operator, in-process (control plane)",
    "gate": "control plane (approval queue)",
    "loop": "control plane (bounded unroll)",
}

#: Config keys whose VALUE is a destination — where a leg's product lands. Named
#: rather than guessed: an explain that invented destinations would be worse than
#: one that lists the keys the packs actually use for them (a `dataset_uri` is an
#: INPUT, so a blanket "*_uri" rule would report reads as writes).
_DESTINATION_KEYS = (
    "out_path", "output_path", "output_dir", "output_name", "dest", "destination",
    "artifact_target", "hf_repo", "register", "register_as", "export_path",
    "checkpoint_dir", "adapter_out", "index_path", "replay_out_path",
)


def _op_name(ref: str) -> str:
    """``task:<op>`` → ``<op>`` (the D0 prefix ``leg_kind_of`` routes on, #1278)."""
    raw = str(ref or "")
    return raw[len("task:"):] if raw.startswith("task:") else raw


def _one_line(text: Any, width: int = 160) -> str:
    """Collapse an authored multi-line predicate/description onto one row — a
    table whose cells wrap is not a table."""
    s = " ".join(str(text or "").split())
    return s if len(s) <= width else s[: width - 1] + "…"


# ─── gate-on-path: ONE rule, two surfaces ────────────────────────────────


def gate_path_problems(d: PipelineDef, plan: Any = None) -> list[str]:
    """Legs whose flow may WRITE and that no gate sits in front of.

    The rule the console's builder has enforced client-side since #1771
    ("a paid/external leg needs a gate on its ancestor path") — as a function in
    core, so ``cairn validate``, ``POST /builder/validate`` and
    ``cairn pipeline explain`` report it in the same words. A rule that lives in
    one client is a rule the other doors do not have (#1376: a chokepoint is a
    function others call).

    Risk comes from :func:`~cairn.core.dataflow.defs.flow_leg_risk` — the flow's
    ``aggregate_risk``, i.e. the same number the approval plane keys on — and
    "gated" means :func:`~cairn.core.policy.pipeline_gates.gated_upstream`, the
    same walk the policy injector uses to decide it need not double-gate.

    Reported at WARN level by ``cairn validate`` rather than as an error,
    measured rather than assumed: 18 of the 21 first-party pipelines carry at
    least one leg this flags today (every `flow:` in the tree aggregates to
    ``external_write``), so promoting it to an error is a fleet change, not a
    validator change. Explain shows it inline on the leg either way.
    """
    from cairn.core.dataflow.defs import flow_leg_risk  # noqa: PLC0415 - cycle: defs → pack registry
    from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 - cycle: dispatch → engine
    from cairn.core.policy.pipeline_gates import gated_upstream  # noqa: PLC0415 - cycle: policy → engine

    try:
        plan = plan if plan is not None else plan_pipeline(d)
    except Exception:  # noqa: BLE001 — a def that cannot plan has a louder problem
        return []
    by_id = {s.id: s for s in plan.steps}
    out: list[str] = []
    for s in plan.steps:
        if leg_kind_of(s.ref) != "flow":
            continue
        try:
            risk = flow_leg_risk(str(s.ref))
        except Exception:  # noqa: BLE001 — an unreadable def is not a gate verdict
            risk = None
        if not risk or risk == "read_only":
            continue
        if gated_upstream(s, by_id):
            continue
        out.append(
            f"step '{s.id}': {risk} leg has no approval gate on its path — "
            f"nothing pauses before it writes (add a `gate:` step upstream, or "
            f"an environment policy that injects one)"
        )
    return out


# ─── input layering ──────────────────────────────────────────────────────


def input_layers(
    d: PipelineDef,
    *,
    supplied: Optional[dict] = None,
    preset_inputs: Optional[dict] = None,
    preset_name: str = "",
) -> dict[str, dict[str, Any]]:
    """Each declared pipeline input with its effective value and SOURCE LAYER.

    The layering is ``launch_pipeline``'s, in its order and by its functions:
    the preset is expanded first, an explicit ``--input`` shadows it per key
    (:func:`~cairn.core.pipeline_presets.merge_over_preset`), and only then does
    a declared ``default`` fill what is still absent (#1180). A key with no
    value on any layer is what :func:`~cairn.core.dispatch.pipeline_launch
    .missing_inputs` refuses the launch for, and it is reported as ``required``
    rather than silently rendered empty.
    """
    from cairn.core.dispatch.pipeline_launch import missing_inputs  # noqa: PLC0415 - cycle: dispatch → engine
    from cairn.core.pipeline_presets import merge_over_preset  # noqa: PLC0415 - cycle: presets → engine

    merged, overrides = merge_over_preset(preset_inputs or {}, supplied or {})
    missing = set(missing_inputs(d, merged))
    out: dict[str, dict[str, Any]] = {}
    for key, spec in (d.inputs or {}).items():
        spec = spec if isinstance(spec, dict) else {}
        if key in (supplied or {}):
            source = "input (supplied)" + (
                f" — overrides preset '{preset_name}'" if key in overrides else "")
            value = merged[key]
        elif key in (preset_inputs or {}):
            source = f"preset '{preset_name}'"
            value = merged[key]
        elif "default" in spec:
            source, value = "default (def)", spec["default"]
        else:
            source, value = ("required — UNSATISFIED" if key in missing
                             else "optional (no default)"), None
        out[key] = {"value": value, "source": source,
                    "type": str(spec.get("type", "") or "")}
    return out


def _nested_wires(value: Any, path: str = "") -> list[tuple[str, str]]:
    """``(path, wire)`` for every ``$…`` string inside a map or list value."""
    if isinstance(value, str):
        return [(path, value)] if value.startswith("$") else []
    if isinstance(value, dict):
        return [w for k, v in value.items() for w in _nested_wires(v, f"{path}.{k}" if path else str(k))]
    if isinstance(value, list):
        return [w for i, v in enumerate(value) for w in _nested_wires(v, f"{path}[{i}]")]
    return []


def _config_source(raw: Any, layers: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """One ``with:`` value → ``{value, source, detail}``.

    Reads the LOWERED value (what ``lower_step`` put on the plan), because that
    is what ``resolve_step_inputs`` will resolve at submit — the authored
    ``<step>.<field>`` spelling no longer exists by then. Since #1675 a value
    keeps its YAML type: a scalar or a structure renders as JSON, and the wires
    inside a structure (resolved at submit; an unresolved one becomes null) are
    each named with their own source.
    """
    if not isinstance(raw, str):
        shown = json.dumps(raw, default=str, sort_keys=True)
        wires = _nested_wires(raw)
        if not wires:
            return {"value": shown, "source": "literal (def)", "detail": ""}
        return {"value": shown, "source": "structure with wires",
                "detail": "; ".join(f"{path} ← {_config_source(w, layers)['source']}" for path, w in wires)
                          + " (resolved at submit; an unresolved one becomes null)"}
    v = raw
    if v.startswith("$steps."):
        head = v[len("$steps."):]
        step_id, _, field = head.partition(".")
        return {"value": v, "source": "wire (upstream leg)",
                "detail": f"resolves at submit from {step_id}.{field}"}
    if v.startswith("$input."):
        key = v[len("$input."):]
        layer = layers.get(key)
        if layer is None:
            return {"value": v, "source": "wire (UNDECLARED input)",
                    "detail": f"no `inputs:` entry named {key!r}"}
        return {"value": v, "source": f"input wire ← {layer['source']}",
                "detail": f"{key} = {layer['value']!r}"}
    if v.startswith("$"):
        return {"value": v, "source": "expression",
                "detail": "resolved by the ONE expression language at submit"}
    return {"value": v, "source": "literal (def)", "detail": ""}


# ─── per-leg projection ──────────────────────────────────────────────────


def _def_source_path(d: PipelineDef) -> str:
    from cairn.core.engine.pipeline_resolve import all_pipeline_yaml_paths  # noqa: PLC0415 - cycle

    for p in all_pipeline_yaml_paths():
        if p.stem == d.name and pipeline_pack_name(p) == d.pack:  # #2513: the manifest's name
            return str(p)
    return ""


def _target_of(kind: str, ref: str) -> dict[str, str]:
    """The FILE (or class) this leg will execute — resolved the way the executor
    resolves it, so "what will run" is a path a reviewer can open."""
    if kind == "flow":
        from cairn.core.dataflow.defs import (  # noqa: PLC0415 - cycle: defs → pack registry
            load_flow_def_sources, resolve_flow_key,
        )
        key = resolve_flow_key(ref)
        path = load_flow_def_sources().get(key)
        return {"kind": "flow def", "ref": key, "path": str(path or ""),
                "note": "" if path else "no flows/<name>.yaml resolves — the leg cannot run"}
    if kind == "template":
        from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415 - cycle
        try:
            return {"kind": "template", "ref": ref, "path": str(resolve_template_path(ref)), "note": ""}
        except Exception as e:  # noqa: BLE001 — an unresolvable template is a reported fact
            return {"kind": "template", "ref": ref, "path": "", "note": f"unresolved: {e}"}
    if kind == "task":
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415 - cycle
        ref = _op_name(ref)
        if not default_registry.has(ref):
            return {"kind": "operator", "ref": ref, "path": "",
                    "note": "not registered — is its operator pack installed?"}
        cls = default_registry.get(ref)
        try:
            path = inspect.getfile(cls)
        except Exception:  # noqa: BLE001 — a dynamically built class has no file
            path = ""
        return {"kind": "operator", "ref": ref, "path": path,
                "note": f"{cls.__module__}.{cls.__name__}"}
    return {"kind": "control plane", "ref": ref, "path": "",
            "note": "no executable — the approval queue IS the step"}


def _engine_of(kind: str, ref: str) -> dict[str, Any]:
    """Which runtime picks this leg up — for a flow leg, the environment's
    engine for the flow's OWN declared kind (``_leg_engine``'s selection, read
    through ``selected_engine`` so no engine is constructed)."""
    if kind != "flow":
        return {"engine": LEG_RUNTIME.get(kind, "?"), "flow_kind": "", "target": "",
                "selected_via": ""}
    from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 - cycle
    from cairn.core.dataflow.engine_registry import (  # noqa: PLC0415 - cycle
        installed_engines, selected_engine,
    )
    flow_kind = getattr(flow_def_for(ref), "kind", None) or "data"
    name, target = selected_engine(flow_kind)
    installed = installed_engines()
    return {
        "engine": name,
        "flow_kind": flow_kind,
        "target": target,
        "selected_via": "ml_engine/ml_target" if flow_kind == "ml"
                        else "dataflow_engine/dataflow_target",
        "note": "" if name in installed else
                f"engine {name!r} is selected but NOT installed (have: "
                f"{', '.join(installed) or 'none'}) — the leg will refuse to start",
    }


async def _compute_of(kind: str, ref: str, engine: dict) -> dict[str, Any]:
    """The compute binding: a flow leg's target + its ``max_in_flight`` cap
    (``_leg_target`` + :func:`target_cap`), or a task leg's declared
    ``compute_target`` (#1718's ``resolve_declared_compute`` input)."""
    if kind == "flow":
        target = str(engine.get("target") or "")
        cap = None
        if target:
            try:
                from cairn.core.dispatch.target_concurrency import target_cap  # noqa: PLC0415 - cycle
                cap = await target_cap(target)
            except Exception:  # noqa: BLE001 — an unreachable store means "cap unknown"
                cap = None
        return {"target": target or "(none — local engine)",
                "max_in_flight": cap,
                "source": engine.get("selected_via", "")}
    if kind == "task":
        # #1718: the operator's own declared compute target is what
        # `resolve_declared_compute` reads off the BINDING; a pipeline task leg
        # has no binding, so what remains declarative is the operator's
        # executors + whether it needs a compute connection at all.
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415 - cycle
        ref = _op_name(ref)
        md = default_registry.get_metadata(ref) if default_registry.has(ref) else None
        execs = list(getattr(md, "executors", None) or []) if md else []
        return {"target": "(control plane — task legs carry no compute binding)",
                "max_in_flight": None,
                "source": f"operator executors: {', '.join(execs) or 'in_process'}"}
    return {"target": "", "max_in_flight": None, "source": ""}


def _authored(d: PipelineDef) -> dict[str, PipelineStep]:
    """Authored step by name, loop bodies included — the plan flattens a gate's
    ref to ``gate:<name>``, so the human facts (approvers, show, predicate) have
    to come from the def."""
    return {s.name: s for s in d.all_steps()}


def _gate_rows(
    d: PipelineDef, plan: Any, step: Any, authored: dict[str, PipelineStep],
    *, environment: str,
) -> list[dict[str, Any]]:
    """EVERY gate on this leg's path, each with the authority that demands it.

    Three kinds, all read from the machinery that enforces them:

    * **plan gates upstream** — author-declared ``gate:`` steps (human or data
      predicate) and any the environment's policy injected (``injected_by``),
      walked through the plan's effective ``depends_on``;
    * **the leg's own runtime gate** — resolved by the same call the bridge
      makes before dispatch: :func:`enforce_flow_gate` for a flow leg (caught
      rather than raised: the raise IS the pause), :func:`resolve_runtime_gate`
      for a task leg's operator. ``sources`` is the ADR-0034 authority list —
      operator / profile / policy / binding.
    """
    from cairn.core.approval.enforce import (  # noqa: PLC0415 - cycle: approval → operator
        ApprovalIntent, NeedsApprovalError, approved_state, enforce_flow_gate,
        resolve_runtime_gate,
    )
    from cairn.core.dataflow.defs import flow_leg_risk  # noqa: PLC0415 - cycle
    from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 - cycle

    by_id = {s.id: s for s in plan.steps}
    rows: list[dict[str, Any]] = []

    seen: set[str] = set()
    # #1788 — a FINALIZER has no upstream gates, because its trigger is not its
    # dependencies: it runs on any terminal state, "even if its dependency chain
    # never fired" (the line explain already prints two rows above). Walking its
    # decorative `depends_on` — which implicit-linear filled in — reported the
    # pipeline's last human gate as a gate on the teardown's path, i.e. as
    # something that could hold the teardown up. It cannot, and R1's whole point
    # is that it must not. Its own runtime gate below is still resolved and
    # shown; that one is real.
    frontier = [] if getattr(step, "finalizer", False) else list(step.depends_on)
    while frontier:
        dep_id = frontier.pop(0)
        if dep_id in seen:
            continue
        seen.add(dep_id)
        dep = by_id.get(dep_id)
        if dep is None:
            continue
        frontier.extend(dep.depends_on)
        if leg_kind_of(dep.ref) != "gate":
            continue
        a = authored.get(dep_id)
        if getattr(dep, "injected_by", None):
            rows.append({"gate": dep_id, "kind": "human (policy-injected)",
                         "authority": f"policy '{dep.injected_by}'",
                         "detail": "un-removable by the pipeline author (#1252)",
                         "when": "upstream"})
        elif dep.approval:
            audience = (getattr(a, "approvers", None)
                        or (d.governance or {}).get("approvers") or "(unset)")
            rows.append({"gate": dep_id, "kind": "human (HITL)",
                         "authority": f"pipeline def — approvers: {audience}",
                         "detail": f"expiry {getattr(a, 'timeout', None) or '(none)'}; "
                                   f"shows {len(getattr(a, 'show', None) or [])} field(s)",
                         "when": "upstream"})
        else:
            rows.append({"gate": dep_id, "kind": "data (predicate)",
                         "authority": "pipeline def — resolved from findings",
                         "detail": _one_line(dep.gate or ""), "when": "upstream"})

    kind = leg_kind_of(step.ref)
    if kind == "gate":
        # A gate leg's own row — the same three facts, for the step that IS the
        # gate rather than one behind it.
        a = authored.get(step.id)
        if getattr(step, "injected_by", None):
            rows.insert(0, {"gate": step.id, "kind": "human (policy-injected)",
                            "authority": f"policy '{step.injected_by}'",
                            "detail": "un-removable by the pipeline author (#1252)",
                            "when": "this leg"})
        elif step.approval:
            rows.insert(0, {"gate": step.id, "kind": "human (HITL)",
                            "authority": f"pipeline def — approvers: "
                                         f"{getattr(a, 'approvers', None) or (d.governance or {}).get('approvers') or '(unset)'}",
                            "detail": f"expiry {getattr(a, 'timeout', None) or '(none)'}"
                                      f"; PAUSES the pipeline until a human answers",
                            "when": "this leg"})
        else:
            rows.insert(0, {"gate": step.id, "kind": "data (predicate)",
                            "authority": "pipeline def — resolved from findings",
                            "detail": _one_line(step.gate or "")
                                      + "  → false STOPS the pipeline (gate_blocked)",
                            "when": "this leg"})
        return rows
    state = approved_state(step.environment or environment or "", step.id, None)
    if kind == "flow":
        risk = None
        try:
            risk = flow_leg_risk(str(step.ref))
        except Exception:  # noqa: BLE001 — unreadable def = unknown risk, never "safe"
            logger.debug("explain: could not resolve risk for %s", step.ref, exc_info=True)
        try:
            enforce_flow_gate(str(step.ref), risk,
                              getattr(step, "approval", None) or None,
                              state=state, node_name=step.id, run_id="explain")
        except NeedsApprovalError as e:
            rows.append({"gate": f"{step.id} (dispatch)", "kind": "human (HITL)",
                         "authority": ", ".join(e.request.sources) or "unknown",
                         "detail": f"risk={risk or 'unknown'} — the leg PAUSES "
                                   f"(awaiting_approval) before the engine is touched",
                         "when": "this leg"})
        else:
            rows.append({"gate": f"{step.id} (dispatch)", "kind": "none",
                         "authority": "—",
                         "detail": f"risk={risk or 'unknown'}: no authority "
                                   f"(profile/policy/step) demands a gate here",
                         "when": "this leg"})
    elif kind == "task":
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415 - cycle
        op = str(step.ref)[len("task:"):] if str(step.ref).startswith("task:") else str(step.ref)
        if default_registry.has(op):
            gate = resolve_runtime_gate(
                default_registry.get(op),
                ApprovalIntent(approval=getattr(step, "approval", None) or None),
                state=state,
            )
            rows.append({
                "gate": f"{step.id} (dispatch)",
                "kind": "human (HITL)" if gate.required else "none",
                "authority": ", ".join(gate.sources) or "—",
                "detail": f"risk={gate.risk or 'unknown'} capability="
                          f"{gate.capability or '—'}"
                          + (f" effect={gate.policy_effect}" if gate.policy_effect else ""),
                "when": "this leg"})
    return rows


def _storage_of(kind: str, ref: str, config: dict[str, dict]) -> dict[str, Any]:
    """Where this leg's output goes: the destinations wired on the leg, plus the
    durable identity a flow leg's checkpoints are keyed by."""
    dests = {k: v["value"] for k, v in config.items()
             if k in _DESTINATION_KEYS or k.endswith(("_out_path", "_output_path"))}
    out: dict[str, Any] = {"destinations": dests}
    if kind != "flow":
        return out
    from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 - cycle
    fd = flow_def_for(ref)
    stages = list(getattr(fd, "stages", None) or [])
    out["checkpoint"] = {
        # #1314 — the control plane pins the key at submit, BEFORE any engine
        # sees the spec; the shape is all explain can know (the spec's config is
        # still unresolved wires here).
        "key_shape": "blake2b-8 over the DataflowSpec JSON "
                     "(resume_from + checkpoint_key excluded), pinned control-side at submit",
        "declared_stages": [s.stage for s in stages if getattr(s, "checkpoint", None)],
        "relay_root": os.environ.get(
            "CAIRN_CHECKPOINT_DIR", str(Path.home() / ".cairn" / "checkpoints")),
    }
    out["registers_artifacts"] = (
        "engine output descriptors register control-side after the leg succeeds "
        "(#1316); `register:` config becomes a named dataset version (#1466)")
    return out


def _retry_of(step: Any, authored_step: Optional[PipelineStep]) -> dict[str, Any]:
    """The retry policy AS PARSED — through ``_leg_retry_policy``, the function
    the bridge calls on the value the plan actually carries.

    A ``retry:`` block authored inline is a dict and, since #1675, reaches the
    leg as one. A declared ``retry:`` that is NOT a map makes
    ``_leg_retry_policy`` return ``None`` — one attempt, no backoff, silently.
    Explain says ``IGNORED (not a map)``: the declared intent and the effective
    behaviour, side by side — the truth is computed, not written down.
    """
    from cairn.core.dispatch.pipeline_bridge import _leg_retry_policy  # noqa: PLC0415 - cycle: dispatch → engine

    declared = (authored_step.inputs or {}).get("retry") if authored_step else None
    lowered = (getattr(step, "inputs", None) or {}).get("retry")
    policy = _leg_retry_policy(lowered)
    from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 - cycle

    kind = leg_kind_of(step.ref)
    if declared is not None and policy is None:
        return {"declared": declared, "effective": None,
                "status": "IGNORED (not a map)",
                "detail": "retry: is not a map, so _leg_retry_policy "
                          "returns None — one attempt, no backoff"}
    if policy is not None:
        return {"declared": declared,
                "effective": {"max_attempts": policy.maximum_attempts,
                              "initial_s": policy.initial_interval_seconds,
                              "backoff": policy.backoff_coefficient,
                              "max_interval_s": policy.maximum_interval_seconds},
                "status": "declared", "detail": ""}
    if kind != "flow":
        return {"declared": None, "effective": None, "status": "none",
                "detail": "task/template legs do not retry (a failure is an outcome)"}
    flow_kind = "ml"
    try:
        from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 - cycle
        flow_kind = getattr(flow_def_for(str(step.ref)), "kind", None) or "data"
    except Exception:  # noqa: BLE001 — unknown kind falls back to the ml (safest) default
        pass
    attempts = 1 if flow_kind == "ml" else 2
    return {"declared": None, "effective": {"max_attempts": attempts},
            "status": "default (kind-aware)",
            "detail": f"execute_leg's default for a kind: {flow_kind} flow "
                      f"({'paid compute is never re-spent silently' if attempts == 1 else 'one automatic retry'})"}


def _timeout_of(step: Any, authored_step: Optional[PipelineStep], kind: str) -> dict[str, Any]:
    declared = getattr(authored_step, "timeout", None) if authored_step else None
    if kind == "gate":
        return {"declared": declared or "", "seconds": None,
                "applies": "approval expiry (a gate has no execution to bound)"}
    from cairn.core.dispatch.pipeline_bridge import (  # noqa: PLC0415 - cycle: dispatch → engine
        _FLOW_LEG_DEFAULT_BUDGET_S,
    )

    seconds = getattr(step, "timeout_s", None)
    if seconds is None and declared:
        seconds = timeout_seconds(declared)
    return {
        "declared": declared or "",
        "seconds": seconds,
        "applies": ("poll budget — expiry CANCELS the engine job" if kind == "flow"
                    else "wall clock around the operator call"),
        # what applies when nothing is declared — the bridge's own constant, so
        # "unbounded" is never claimed where a limit exists (#1288).
        "default_s": (None if seconds else
                      (_FLOW_LEG_DEFAULT_BUDGET_S if kind == "flow" else None)),
    }


# ─── the projection ──────────────────────────────────────────────────────


def _deployment_budget() -> dict[str, float]:
    """The profile's ``[profile.budget]`` for the explain view (ADR-0155); ``{}`` when
    it cannot be read — the view reports, the launch door is what refuses."""
    from cairn.core.dispatch.leg_hooks import deployment_budget  # noqa: PLC0415 - cycle: dispatch → engine

    try:
        return deployment_budget()
    except Exception:  # noqa: BLE001 — a read-only projection never fails on the profile
        return {}


def _resolved_view(
    d: PipelineDef,
    plan: Any,
    steps: list[dict[str, Any]],
    *,
    environment: str,
    environment_source: dict[str, Any],
    layers: dict[str, dict[str, Any]],
    preset_name: str,
    workspace: dict[str, Any],
    injected: list[str],
) -> dict[str, Any]:
    """#2134 S4 — the same projection, re-cut by OWNER.

    Everything below is a re-labelling of facts already computed above: the
    input layers, the definition's ``governance:``, the engine/target the leg
    rows report, the plan's gates and the injector's own return value. Nothing
    here resolves a value a second time — see
    :mod:`cairn.core.engine.pipeline_resolved` for the vocabulary and the
    resolver behind each row.
    """
    profile_name, _profile_backends = ambient_profile()

    # the backend fields the legs on THIS plan actually resolve through — never
    # the whole inventory, which would report machinery no leg here touches
    keys: list[str] = []
    for s in steps:
        kind = (s.get("engine") or {}).get("flow_kind") or ""
        if not kind:
            continue
        for key in selection_keys(kind):
            if key not in keys:
                keys.append(key)
    backends = backend_rows(keys, profile_name=profile_name)
    by_backend_key = {r["key"]: r for r in backends}

    legs: list[dict[str, Any]] = []
    authored_env_of = {a.name: str(a.environment) for a in d.all_steps() if a.environment}
    for s in steps:
        # A leg's own `environment:` overrides the pipeline's for that leg
        # alone; without one the leg inherits whatever the pipeline resolved to,
        # so the row is the SAME row rather than a restatement of it.
        authored_env = authored_env_of.get(s["id"], "")
        if authored_env:
            env_row = resolve_layer(
                "environment",
                Layers(pipeline={"environment": authored_env},
                       declared=frozenset({"environment"})),
                detail="this leg declares its own environment; the pipeline runs in "
                       f"{environment or '(the deployment default)'}")
        else:
            env_row = dict(environment_source)
            env_row["key"] = "environment"
        env_row["differs"] = bool(authored_env)
        kind = (s.get("engine") or {}).get("flow_kind") or ""
        if kind:
            engine_row = dict(by_backend_key.get(selection_keys(kind)[0], {}))
            target_row = dict(by_backend_key.get(selection_keys(kind)[1], {}))
        else:
            # a task / gate / loop leg is executed by the control plane itself —
            # no backend field decides it, and pretending one did would be a lie
            engine_row = {"key": "runtime", "value": s.get("runtime", ""),
                          "source": PIPELINE, "declared_by": PIPELINE,
                          "detail": "a control-plane leg — no backend selects it"}
            target_row = {}
        legs.append({
            "id": s["id"], "leg_kind": s["leg_kind"], "ref": s["ref"],
            "flow_kind": kind, "environment": env_row,
            "engine": engine_row, "target": target_row,
        })

    gates: list[dict[str, Any]] = []
    for s in plan.steps:
        if not (str(s.ref) or "").startswith("gate:"):
            continue
        policy = getattr(s, "injected_by", None) or ""
        if policy:
            source, detail = (profile_source(profile_name) if profile_name else UNKNOWN,
                              f"policy {policy!r} injected this gate — the "
                              f"environment's, un-removable by the pipeline author "
                              f"(#1252)" + ("" if profile_name else
                                            "; no profile is loaded, so which "
                                            "environment activated it is unknown"))
        else:
            source, detail = PIPELINE, "declared in the pipeline file"
        gates.append({
            # `key` mirrors `id` so every section of the resolved view is the
            # same row shape — one renderer, one console table component.
            "id": s.id, "key": s.id, "source": source, "detail": detail,
            "injected_by": policy,
            "kind": ("human" if getattr(s, "approval", False) else "data"),
            "value": ("approval" if getattr(s, "approval", False)
                      else _one_line(getattr(s, "gate", "") or "")),
            "declared_by": PIPELINE if not policy else "profile",
        })

    return {
        "vocabulary": list(SOURCES),
        "profile": {"name": profile_name or "(none)",
                    "source": profile_source(profile_name) if profile_name else UNKNOWN,
                    "detail": ("loaded from CAIRN_PROFILE" if profile_name
                               else "no profile is loaded — every machinery value "
                                    "falls through to its env var or schema default")},
        "environment": environment_source,
        "inputs": input_rows(d.inputs or {}, layers,
                             inherited=dict(getattr(d, "inherited_inputs", None) or {}),
                             preset_name=preset_name),
        "governance": governance_rows(dict(d.governance or {}), workspace=workspace,
                                      profile_name=profile_name,
                                      deployment_budget=_deployment_budget()),
        "backends": backends,
        "legs": legs,
        "gates": gates,
        "policy_injected_gates": list(injected),
    }


async def explain_pipeline(
    d: PipelineDef,
    *,
    environment: str = "",
    inputs: Optional[dict] = None,
    preset_name: str = "",
    preset_inputs: Optional[dict] = None,
    workspace_id: str = "default",
    workspace_variables: Optional[dict] = None,
) -> dict[str, Any]:
    """The dry-run projection of one pipeline. JSON-able; the CLI and the HTTP
    route render the SAME dict (``--json`` is the raw thing, not a second view).

    Async because two of the facts it reports are: a compute target's
    ``max_in_flight`` is a store read, and a caller may hand in a preset it
    resolved through the (async) preset store.
    """
    from cairn.core.dispatch.leg_executor import leg_kind_of  # noqa: PLC0415 - cycle
    from cairn.core.engine.pipeline_resolve import (  # noqa: PLC0415 - cycle
        PIPELINE_PREFIX, PipelineNotRunnableError, assert_runnable,
    )
    from cairn.core.operator.loader import ensure_loaded  # noqa: PLC0415 - cycle

    ensure_loaded(strict=False)  # task legs resolve against the operator registry

    plan = plan_pipeline(d)
    problems: list[str] = []
    try:
        assert_runnable(d)
    except PipelineNotRunnableError as e:
        problems.append(str(e))

    injected: list[str] = []
    try:
        # #1252 — the environment's own gates, inserted by the SAME call
        # `launch_pipeline` makes, so explain shows the plan that would run.
        from cairn.core.policy.pipeline_gates import (  # noqa: PLC0415 - cycle
            PolicyDeniedError, inject_policy_gates,
        )
        injected = inject_policy_gates(plan, environment=environment or None)
    except Exception as e:  # noqa: BLE001 — a deny is a REPORTED refusal here, not a raise
        problems.append(f"policy would refuse this launch: {e}")

    # #199 — the launch door's runtime-owner refusal, by the door's own function.
    from cairn.core.dispatch.pipeline_launch import runtime_owner_problems  # noqa: PLC0415 - cycle: dispatch → engine

    problems += [f"the runtime would refuse this launch: {p}" for p in runtime_owner_problems(plan)]
    problems += gate_path_problems(d, plan)

    layers = input_layers(d, supplied=inputs, preset_inputs=preset_inputs,
                          preset_name=preset_name)
    # #197 — the values the launch will use, built as `launch_pipeline_def` builds
    # them (the preset, the caller's inputs over it, then the declared defaults and
    # the typed coercion, by the door's own functions), so `-i k=a,b` on a `list`
    # input reads as the list and explain refuses exactly what the launch refuses.
    # A refused value is reported and shown as written.
    from cairn.core.dispatch.pipeline_launch import InputCoercionError  # noqa: PLC0415 - cycle: dispatch → engine
    from cairn.core.dispatch.pipeline_preview import apply_input_defaults  # noqa: PLC0415 - cycle: dispatch → engine
    from cairn.core.pipeline_presets import merge_over_preset  # noqa: PLC0415 - cycle: presets → engine

    merged, _ = merge_over_preset(preset_inputs or {}, inputs or {})
    try:
        typed = apply_input_defaults(d, merged)
    except InputCoercionError as e:
        problems.append(f"launch would refuse the inputs: {e}")
    else:
        for key, layer in layers.items():
            if key in typed:
                layer["value"] = typed[key]
    authored = _authored(d)

    # #2134 S4 — the workspace's variables are a real layer (ADR-0101 D2): the
    # run dialog's environment / series / cost cap default from them. Read
    # best-effort and never awaited twice; a store that is not reachable from
    # this process means "the workspace says nothing", not a failed explain.
    workspace = dict(workspace_variables or {})
    if workspace_variables is None:
        try:
            from cairn.core.workspace_variables import variables_map  # noqa: PLC0415 - cycle

            workspace = await variables_map(workspace_id)
        except Exception:  # noqa: BLE001 — an unreachable store is not an explain failure
            logger.debug("explain: workspace variables unavailable", exc_info=True)
            workspace = {}
    env_row = environment_row(environment, workspace)

    steps: list[dict[str, Any]] = []
    for s in plan.steps:
        kind = leg_kind_of(s.ref)
        a = authored.get(s.id)
        engine = _engine_of(kind, str(s.ref))
        config = {k: _config_source(v, layers) for k, v in (s.inputs or {}).items()}
        steps.append({
            "id": s.id,
            "leg_kind": kind,
            "ref": s.ref,
            "runtime": LEG_RUNTIME.get(kind, "?"),
            "target": _target_of(kind, str(s.ref)),
            "engine": engine,
            "compute": await _compute_of(kind, str(s.ref), engine),
            "depends_on": list(s.depends_on),
            "when": s.when or "",
            "predicate": s.gate or "" if kind == "gate" else "",
            "approval": bool(s.approval),
            # #1787 — a finalizer is the one leg whose TRIGGER is not its
            # dependencies, so an explain that showed only `depends_on` would
            # describe it wrongly. Read off the plan, like everything else here.
            "finalizer": bool(getattr(s, "finalizer", False)),
            "injected_by": getattr(s, "injected_by", None) or "",
            "gates": _gate_rows(d, plan, s, authored, environment=environment),
            "config": config,
            "optional_inputs": list(getattr(s, "optional_inputs", None) or []),
            "produces": list(getattr(s, "produces", None) or []),
            "show": list(getattr(a, "show", None) or []) if kind == "gate" else [],
            "storage": _storage_of(kind, str(s.ref), config),
            "timeout": _timeout_of(s, a, kind),
            "retry": _retry_of(s, a),
        })

    return {
        "ref": f"{PIPELINE_PREFIX}{d.pack}/{d.name}",
        "pack": d.pack,
        "name": d.name,
        "description": d.description or "",
        "content_hash": d.content_hash,
        "source_hash": d.source_hash,
        "source": _def_source_path(d),
        "environment": environment or "(default)",
        "governance": dict(d.governance or {}),
        "loops": {name: {"max_iterations": (spec or {}).get("max_iterations"),
                         "until": (spec or {}).get("until"),
                         "on_exhausted": (spec or {}).get("on_exhausted"),
                         "body": list(plan.loop_tail.get(name) or [])}
                  for name, spec in (plan.loops or {}).items()},
        "inputs": layers,
        "preset": preset_name,
        "policy_injected_gates": injected,
        "problems": problems,
        "deployment": _deployment(),
        "steps": steps,
        # #2134 S4 — the same facts, re-cut by OWNER. Last, so every input it
        # re-labels has already been computed by the resolver that owns it.
        "resolved": _resolved_view(
            d, plan, steps, environment=environment,
            environment_source=env_row, layers=layers, preset_name=preset_name,
            workspace=workspace, injected=injected),
    }


def _deployment() -> dict[str, str]:
    """Where the RECORD lives while all this runs. Reported by FAMILY, never as
    a URL: ``CAIRN_STORAGE`` is a DSN with a password in every real deployment."""
    from cairn.core import settings  # noqa: PLC0415 - cycle: settings ↔ storage

    raw = getenv("CAIRN_STORAGE", "").strip()
    if raw.startswith(("postgres://", "postgresql://")):
        storage = "postgres"
    elif raw.startswith("sqlite://"):
        storage = "sqlite"
    elif raw:
        storage = "unrecognized scheme — falls back to CAIRN_RUN_DB / in-memory"
    else:
        storage = "in-memory (CAIRN_STORAGE unset — the record does not survive this process)"
    from cairn.core.bus import bus_backend  # noqa: PLC0415 - the one reading of CAIRN_BUS

    bus = {"postgres": "postgres",
           "refused": "refused: redis is not a bus backend (ADR-0127 D5)"}.get(
        bus_backend(getenv("CAIRN_BUS", "")), "in-process")
    return {"runtime": settings.runtime(), "storage": storage, "bus": bus}


# ─── human rendering ─────────────────────────────────────────────────────


def _fmt_value(v: Any, width: int = 58) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= width else s[: width - 1] + "…"


def render_explain(doc: dict[str, Any]) -> str:
    """A compact tree. No colour-only information: every fact is in the text, so
    a piped or logged explain says exactly what a terminal one does."""
    L: list[str] = []
    L.append(f"{doc['ref']}  —  {_one_line(doc.get('description'), 110) or '(no description)'}")
    L.append(f"  file:        {doc.get('source') or '(not on disk)'}")
    L.append(f"  hash:        {doc.get('content_hash')}   source {doc.get('source_hash')}")
    L.append(f"  environment: {doc.get('environment')}    runtime={doc['deployment']['runtime']}"
             f"  storage={doc['deployment']['storage']}  bus={doc['deployment']['bus']}")
    if doc.get("governance"):
        L.append(f"  governance:  {doc['governance']}")
    for name, loop in (doc.get("loops") or {}).items():
        L.append(f"  loop {name}: max_iterations={loop['max_iterations']} "
                 f"until={loop['until']!r} on_exhausted={loop['on_exhausted']} "
                 f"body={', '.join(loop['body'])}")
    if doc.get("preset"):
        L.append(f"  preset:      {doc['preset']}")

    L.append("")
    L.append("  INPUTS (value ← layer)")
    for key, layer in (doc.get("inputs") or {}).items():
        L.append(f"    {key:24s} {_fmt_value(layer['value'], 40):40s}  ← {layer['source']}")

    if doc.get("policy_injected_gates"):
        L.append("")
        L.append(f"  POLICY-INJECTED GATES: {', '.join(doc['policy_injected_gates'])}")

    for s in doc.get("steps", []):
        L.append("")
        head = f"  [{s['leg_kind']}] {s['id']}"
        if s.get("finalizer"):
            # #1787 — say the trigger, not the dependency list: a finalizer's
            # `depends_on` is decorative (implicit-linear fills it in), and
            # printing it as "after X" would read as a promise the bridge does
            # not make.
            L.append(f"{head}  {s['ref']}   FINALIZER")
            L.append("       runs:      on ANY terminal state of the pipeline "
                     "(done / failed / cancelled), exactly once — even if its "
                     "dependency chain never fired")
        else:
            L.append(f"{head}  {s['ref']}"
                     + (f"   after {', '.join(s['depends_on'])}" if s["depends_on"] else "  (entrypoint)"))
        if s["when"]:
            L.append(f"       when:      {_one_line(s['when'])}  → ROUTED OFF (skipped, pipeline continues) when false")
        if s["predicate"]:
            L.append(f"       predicate: {_one_line(s['predicate'])}  → STOP (gate_blocked) when false")
        tgt = s["target"]
        L.append(f"       target:    {tgt['path'] or tgt['ref']}"
                 + (f"   [{tgt['note']}]" if tgt["note"] else ""))
        eng = s["engine"]
        L.append(f"       engine:    {eng['engine']}"
                 + (f"  (kind: {eng['flow_kind']}, via {eng['selected_via']})" if eng.get("flow_kind") else "")
                 + (f"   ! {eng['note']}" if eng.get("note") else ""))
        comp = s["compute"]
        if comp.get("target"):
            L.append(f"       compute:   {comp['target']}"
                     + (f"   max_in_flight={comp['max_in_flight']}" if comp["max_in_flight"] else "")
                     + (f"   [{comp['source']}]" if comp["source"] else ""))
        for g in s["gates"]:
            L.append(f"       gate[{g['when']:8s}] {g['gate']:28s} {g['kind']:24s} "
                     f"authority={g['authority']}")
            if g["detail"]:
                L.append(f"                  {_one_line(g['detail'])}")
        for k, c in (s.get("config") or {}).items():
            L.append(f"       with.{k:<18s} {_fmt_value(c['value'], 34):34s} ← {c['source']}"
                     + (f"  ({c['detail']})" if c["detail"] else ""))
        if s.get("optional_inputs"):
            L.append(f"       optional:  {', '.join(s['optional_inputs'])} (omitted when the producer is routed off)")
        if s.get("produces"):
            L.append(f"       produces:  {', '.join(s['produces'])}")
        if s.get("show"):
            L.append(f"       shows:     {len(s['show'])} field(s) to the approver")
        st = s["storage"]
        if st.get("destinations"):
            L.append(f"       writes:    {st['destinations']}")
        if st.get("checkpoint"):
            L.append(f"       checkpoint: {st['checkpoint']['key_shape']}")
            L.append(f"                   stages={st['checkpoint']['declared_stages'] or '(none declared)'}"
                     f"  relay→{st['checkpoint']['relay_root']}")
        t = s["timeout"]
        L.append(f"       timeout:   {t['declared'] or '(none declared)'}"
                 + (f" = {t['seconds']}s" if t["seconds"] else "")
                 + (f" → default {t['default_s']}s" if t.get("default_s") else "")
                 + f"   [{t['applies']}]")
        r = s["retry"]
        L.append(f"       retry:     {r['status']}"
                 + (f"  declared={r['declared']}" if r["declared"] is not None else "")
                 + (f"  effective={r['effective']}" if r["effective"] else "")
                 + (f"\n                  {r['detail']}" if r["detail"] else ""))

    L += _render_resolved(doc.get("resolved") or {})

    if doc.get("problems"):
        L.append("")
        L.append("  PROBLEMS")
        for p in doc["problems"]:
            L.append(f"    ! {p}")
    return "\n".join(L)


def _render_resolved(r: dict[str, Any]) -> list[str]:
    """#2134 S4 — the four-file merge, as one table of value ← owner.

    Printed after the per-leg detail because it answers a different question:
    the legs say *what will happen*, this says *which document to open to
    change it*. Every row carries its source token verbatim, so a piped explain
    is greppable by owner (``| grep 'profile:'``).
    """
    if not r:
        return []
    L = ["", "  RESOLVED (effective value ← where it came from)",
         f"    profile:     {r['profile']['name']}   {r['profile']['detail']}",
         f"    environment: {_fmt_value(r['environment']['value'], 30):30s} "
         f"← {r['environment']['source']}   {_one_line(r['environment']['detail'], 80)}"]

    # The detail line earns its place only when the owner is not the obvious
    # one: repeating "the declared fallback" under thirty default rows buries
    # the two that came from somewhere else, which are the reason to read this.
    quiet = {"default", "pipeline"}

    def rows(title: str, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        L.append("")
        L.append(f"    {title}")
        for row in items:
            # The DECLARING document, when it is not this file — an input
            # adopted through `$inherit` lives in the flow, and a reader who
            # wants to change it must open that one. Printed even when the
            # value's own source is `unknown`, which is precisely the case for
            # a data/ml flow: its contract declares names and no defaults, so
            # the only thing known about the key is where it came from.
            declared = str(row.get("declared_by") or "pipeline")
            owner = (f"   (declared {declared})"
                     if declared not in ("pipeline", row["source"]) else "")
            L.append(f"      {str(row['key']):26s} {_fmt_value(row.get('value'), 34):34s} "
                     f"← {row['source']}{owner}")
            if row.get("detail") and row["source"].split(":", 1)[0] not in quiet:
                L.append(f"        {_one_line(row['detail'], 110)}")

    rows("INPUTS", r.get("inputs") or [])
    rows("GOVERNANCE", r.get("governance") or [])
    rows("BACKENDS (the machinery the legs run on)", r.get("backends") or [])
    rows("GATES", r.get("gates") or [])

    legs = [leg for leg in (r.get("legs") or []) if leg["environment"].get("differs")]
    if legs:
        L.append("")
        L.append("    PER-LEG ENVIRONMENT (where it differs from the pipeline's)")
        for leg in legs:
            L.append(f"      {leg['id']:26s} {_fmt_value(leg['environment']['value'], 34):34s} "
                     f"← {leg['environment']['source']}")
    return L


__all__ = [
    "explain_pipeline", "gate_path_problems", "input_layers", "render_explain",
]
