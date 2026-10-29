"""The RESOLVED view — every effective value with where it came from (#2134 S4).

The owner, reviewing the design on 2026-09-09: *"responsibilities are
distributed over multiple files for one flow to run."* ADR-0106 D4 says that is
correct — a **flow** states needs, a **pipeline** states intent, a **profile**
states machinery, ``pack.toml`` states facts about the artifact — plus workspace
variables and per-run overrides. Different owners, different change cadence;
collapsing them would let a data scientist edit the production budget in the
same diff as a training step.

The cost is comprehension: to know what a run will do you hold four documents in
your head and merge them yourself. This module is the antidote, and it does not
collapse anything — it reports the merge, naming the owner of every effective
value.

**Derive, never re-implement.** Nothing here decides a value. Each layer is
filled by the function that already owns it:

=====================  ====================================================
what                   who answers
=====================  ====================================================
input value + layer    :func:`~cairn.core.engine.pipeline_explain.input_layers`
                       (``launch_pipeline``'s own preset → supplied → default
                       order)
inherited inputs       :func:`~cairn.core.engine.pipeline_inherit
                       .resolve_inherited_inputs`, which records the leg it
                       adopted each key from at expansion time
governance             ``PipelineDef.governance`` — carried verbatim into the
                       plan and the record (#1688) — then the workspace's
                       ``cost_cap_usd`` / ``series`` variables (ADR-0101 D2)
engine + target        :func:`~cairn.core.dataflow.engine_registry
                       .selected_engine` for the value,
                       :func:`~cairn.core.backend_selection.provenance` for
                       the layer that decided it
policy gates           :func:`~cairn.core.policy.pipeline_gates
                       .inject_policy_gates`, evaluated in dry-run form by
                       :func:`~cairn.core.engine.pipeline_explain
                       .explain_pipeline` — never launched
=====================  ====================================================

**A provenance view that lies is worse than none**, so a value whose owner
cannot be established honestly is reported as ``unknown`` rather than assigned
to the nearest plausible layer.

The vocabulary
--------------

===================  ===================================================
token                meaning
===================  ===================================================
``run``              supplied at launch — including through a preset,
                     which is a saved set of run inputs and reaches the
                     engine as one (the preset's name is the detail)
``pipeline``         the pipeline file states it
``profile:<name>``   the ambient profile (``CAIRN_PROFILE``) states it
``workspace``        a workspace variable supplies it (ADR-0101 D2)
``default``          a declared fallback filled it
``inherited:<leg>``  adopted from that leg's flow through ``$inherit``
                     (ADR-0106 D2)
``env:<VAR>``        an OS environment variable — deployment machinery
                     outside all four files. It is its own token because
                     :func:`~cairn.core.backend_selection.provenance`
                     distinguishes it from the profile, and folding the
                     two together would name the wrong owner
``unknown``          no layer holds it, or the owner cannot be
                     established (a required input nobody has supplied
                     yet; a flow whose def does not resolve)
===================  ===================================================
"""
from __future__ import annotations
from cairn.core.settings import getenv

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

logger = logging.getLogger("cairn.core.engine.pipeline_resolved")

RUN = "run"
PIPELINE = "pipeline"
PROFILE = "profile"
WORKSPACE = "workspace"
DEFAULT = "default"
INHERITED = "inherited"
ENV = "env"
UNKNOWN = "unknown"

#: Every token a row's ``source`` may reduce to (:func:`source_kind`). The
#: console renders the token, so a section that invented a word would paint an
#: unlabelled cell; a test walks every row of a real projection against this.
SOURCES: tuple[str, ...] = (RUN, PIPELINE, PROFILE, WORKSPACE, DEFAULT,
                            INHERITED, ENV, UNKNOWN)

#: Precedence, top down — ADR-0068/0070's order (run › pipeline › profile ›
#: default) with the two layers those ADRs predate slotted where they actually
#: act: a preset is run input, and a workspace variable is the last thing that
#: can supply a value before the declared fallback.
ORDER: tuple[str, ...] = ("run", "preset", "pipeline", "profile", "workspace",
                          "default")


def profile_source(name: str) -> str:
    """``profile:prod`` — or bare ``profile`` when the profile has no name."""
    return f"{PROFILE}:{name}" if name else PROFILE


def inherited_source(leg: str) -> str:
    """``inherited:train`` — the leg whose flow contract the key came from."""
    return f"{INHERITED}:{leg}" if leg else INHERITED


def env_source(var: str) -> str:
    """``env:CAIRN_ML_ENGINE`` — the variable that decided it."""
    return f"{ENV}:{var}" if var else ENV


def source_kind(source: str) -> str:
    """The bare token behind a qualified one: ``profile:prod`` → ``profile``."""
    return str(source or UNKNOWN).split(":", 1)[0]


@dataclass(frozen=True)
class Layers:
    """The layers one key can be answered by, as plain mappings.

    Deliberately dumb: every value in here was produced by the resolver that
    owns it, and :func:`resolve_layer` only says which one won. That is what
    makes the rule testable without packs, a profile or a store.
    """

    #: values supplied at launch
    run: Mapping[str, Any] = field(default_factory=dict)
    #: values a saved preset would send (run inputs by another name)
    preset: Mapping[str, Any] = field(default_factory=dict)
    preset_name: str = ""
    #: values the pipeline FILE states outright (its ``governance:`` block, a
    #: leg's ``environment:``)
    pipeline: Mapping[str, Any] = field(default_factory=dict)
    #: values the ambient profile states (``[profile.backends]``, …)
    profile: Mapping[str, Any] = field(default_factory=dict)
    profile_name: str = ""
    #: workspace variables (ADR-0101 D2)
    workspace: Mapping[str, Any] = field(default_factory=dict)
    #: declared fallbacks — an input's ``default:``, a backend's schema default
    defaults: Mapping[str, Any] = field(default_factory=dict)
    #: key → the leg id whose flow contract ``$inherit`` adopted it from
    inherited: Mapping[str, str] = field(default_factory=dict)
    #: keys the pipeline file declares itself (so a key both inherited AND
    #: redeclared is owned by the file — that is what an override means)
    declared: frozenset = frozenset()


def resolve_layer(
    key: str,
    layers: Layers,
    *,
    override: Optional[dict[str, Any]] = None,
    detail: str = "",
) -> dict[str, Any]:
    """One key → ``{key, value, source, detail, declared_by}``.

    ``override`` short-circuits the walk for a key whose owner another resolver
    already established (``backend_selection.provenance`` answers
    profile / env / default for a backend field, and re-deriving that here would
    be a second opinion about the same question). It is passed through verbatim
    apart from ``key``/``declared_by``.

    No layer holding the key is not an error and not a zero: it is ``unknown``.
    """
    declared_by = (PIPELINE if key in layers.declared
                   else inherited_source(layers.inherited[key])
                   if key in layers.inherited else PIPELINE)
    if override is not None:
        row = {"key": key, "value": None, "source": UNKNOWN, "detail": ""}
        row.update(override)
        row["key"] = key
        row.setdefault("declared_by", declared_by)
        return row

    for layer in ORDER:
        if layer == "run" and key in layers.run:
            return _row(key, layers.run[key], RUN,
                        detail or "supplied at launch", declared_by)
        if layer == "preset" and key in layers.preset:
            note = (f"from preset {layers.preset_name!r}" if layers.preset_name
                    else "from a preset")
            return _row(key, layers.preset[key], RUN, detail or note, declared_by)
        if layer == "pipeline" and key in layers.pipeline:
            return _row(key, layers.pipeline[key], PIPELINE,
                        detail or "stated in the pipeline file", declared_by)
        if layer == "profile" and key in layers.profile:
            return _row(key, layers.profile[key], profile_source(layers.profile_name),
                        detail or "stated by the environment's profile", declared_by)
        if layer == "workspace" and key in layers.workspace:
            return _row(key, layers.workspace[key], WORKSPACE,
                        detail or "a workspace variable supplies it "
                                  "(Settings › Variables)", declared_by)
        if layer == "default" and key in layers.defaults:
            # A fallback declared by the FLOW and adopted through `$inherit` is
            # the flow's value, not this file's — say so, or a reader edits the
            # wrong document.
            if key in layers.inherited and key not in layers.declared:
                leg = layers.inherited[key]
                return _row(key, layers.defaults[key], inherited_source(leg),
                            detail or f"adopted from the flow leg {leg!r} "
                                      f"($inherit, ADR-0106 D2)", declared_by)
            return _row(key, layers.defaults[key], DEFAULT,
                        detail or "the declared fallback", declared_by)
    return _row(key, None, UNKNOWN,
                detail or "no layer supplies it", declared_by)


def _row(key: str, value: Any, source: str, detail: str, declared_by: str) -> dict[str, Any]:
    return {"key": key, "value": value, "source": source, "detail": detail,
            "declared_by": declared_by}


# ─── gathering the layers (each from its own resolver) ────────────────────


def ambient_profile() -> tuple[str, dict[str, Any]]:
    """``(name, [profile.backends])`` of the profile the runtime is running
    under — ``CAIRN_PROFILE``'s file stem, read through the loader
    :mod:`cairn.core.backend_selection` itself uses. A missing or malformed
    profile is ``("", {})``: "the profile says nothing", never a guess."""
    import os
    from pathlib import Path

    raw = getenv("CAIRN_PROFILE", "").strip()
    name = Path(raw).stem if raw else ""
    try:
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415 - cycle: engine → pack

        return name, dict(getattr(load_profile_from_env(), "backends", None) or {})
    except Exception:  # noqa: BLE001 — a broken profile must never break an explain
        logger.debug("resolved view: profile %r could not be read", raw, exc_info=True)
        return name, {}


def backend_rows(keys: list[str], *, profile_name: str) -> list[dict[str, Any]]:
    """One row per backend-selection field, with the layer that decided it.

    Both halves are delegated: the value to
    :func:`~cairn.core.backend_selection.selected` (what the engine registry
    reads) and the layer to :func:`~cairn.core.backend_selection.provenance`
    (P9's "which config plane wins is per-call-site folklore", already made
    inspectable). This only translates its three words into the vocabulary.
    """
    from cairn.core.backend_selection import _BY_KEY, provenance, selected  # noqa: PLC0415 - cycle

    rows: list[dict[str, Any]] = []
    for key in keys:
        f = _BY_KEY.get(key)
        try:
            value = selected(key, default="")
            plane = provenance(key)
        except Exception:  # noqa: BLE001 — an unreadable plane is unknown, not a default
            value, plane = None, UNKNOWN
        if plane == "profile":
            source = profile_source(profile_name)
            detail = "[profile.backends] in this environment's profile"
        elif plane == "env":
            source = env_source(getattr(f, "env", "") or "")
            detail = "an OS environment variable overrides the profile here"
        elif plane == "default":
            source = DEFAULT
            detail = f"the schema default ({getattr(f, 'default', '')!r})"
        else:
            source = UNKNOWN
            detail = "not a declared backend field"
        rows.append({"key": key, "label": getattr(f, "label", "") or key,
                     "value": value, "source": source, "detail": detail,
                     "declared_by": PROFILE, "env": getattr(f, "env", "") or ""})
    return rows


#: The governance number a workspace can answer, and the variable that answers it
#: (:data:`cairn.core.workspace_variables.RESERVED_KEYS`). The budget is not one:
#: the launch door never reads ``cost_cap_usd`` for a pipeline (ADR-0155 D7), so the
#: budget row shows the cap the door enforces — the pipeline's, bounded by the
#: deployment profile's ``[profile.budget]``.
_WORKSPACE_GOVERNANCE = {"series": "series"}


def governance_rows(
    governance: Mapping[str, Any],
    *,
    workspace: Mapping[str, Any],
    profile_name: str,
    deployment_budget: Optional[Mapping[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Budget · series · approvers, with the file that states each.

    ``governance:`` is the pipeline's alone — ``plan_pipeline`` copies it
    verbatim into the plan and ``start_pipeline`` into the record (#1688), and
    the bridge reads ``series`` / ``budget.usd`` straight off it. What a
    pipeline omits, the workspace's run-dialog default can supply (``series``,
    ADR-0101 D2); what neither supplies is unbounded, and says so.

    The budget row is the cap the launch door enforces (ADR-0155): the pipeline's
    figure, or the deployment's default or ceiling from ``deployment_budget`` (the
    profile's ``[profile.budget]``), whichever :func:`~cairn.core.dispatch
    .pipeline_launch.effective_budget` returns.
    """
    from cairn.core.dispatch.pipeline_launch import effective_budget  # noqa: PLC0415 - cycle: dispatch → engine

    budget = governance.get("budget") if isinstance(governance.get("budget"), dict) else {}
    stated: dict[str, Any] = {}
    if isinstance(budget, dict) and budget.get("usd") is not None:
        stated["budget_usd"] = budget.get("usd")
    for key in ("series", "approvers"):
        if governance.get(key) not in (None, ""):
            stated[key] = governance.get(key)

    ws = {k: workspace[v] for k, v in _WORKSPACE_GOVERNANCE.items()
          if v in workspace and workspace[v] not in (None, "")}
    layers = Layers(pipeline=stated, workspace=ws, profile_name=profile_name,
                    declared=frozenset(stated))
    rows = []
    for key, label, unset in (
        ("budget_usd", "Budget (USD)", "no cap — this pipeline is not bounded by a budget"),
        ("series", "Series", "no series — spend is not rolled up"),
        ("approvers", "Approvers", "unset — a gate falls back to the deployment's default audience"),
    ):
        row = resolve_layer(key, layers)
        row["label"] = label
        if key == "budget_usd":
            eff = effective_budget(governance, deployment_budget or {})
            if eff.get("source", PIPELINE) != PIPELINE:
                row = _deployment_budget_row(eff, profile_name)
                row["label"] = label
                rows.append(row)
                continue
        if row["source"] == UNKNOWN:
            row["detail"] = unset
        rows.append(row)
    return rows


def _deployment_budget_row(eff: Mapping[str, Any], profile_name: str) -> dict[str, Any]:
    """The budget row when the deployment's profile decides the cap (ADR-0155 D2)."""
    if eff["source"] == "deployment_default":
        detail = (f"the pipeline declares no budget; the deployment's default "
                  f"(${eff['default_usd']:g}) applies")
    else:
        declared = eff.get("declared_usd")
        detail = (f"the pipeline declares ${declared:g}, above the deployment's ceiling; "
                  f"the launch runs under the ceiling (${eff['ceiling_usd']:g})" if declared
                  else f"the pipeline declares no budget; the deployment's ceiling "
                       f"(${eff['ceiling_usd']:g}) applies")
    return {"key": "budget_usd", "value": eff["usd"], "source": profile_source(profile_name),
            "detail": detail, "declared_by": PROFILE}


def input_rows(
    inputs: Mapping[str, Any],
    layers_by_key: Mapping[str, Mapping[str, Any]],
    *,
    inherited: Mapping[str, str],
    preset_name: str,
) -> list[dict[str, Any]]:
    """Each declared input, re-labelled from
    :func:`~cairn.core.engine.pipeline_explain.input_layers`.

    That function already applies ``launch_pipeline``'s own order (preset →
    supplied → declared default) and already refuses to invent a value for a
    key no layer holds. This maps its prose onto the vocabulary and adds the
    one fact it cannot know: whether the key was the pipeline's own or adopted
    from a leg (``$inherit``).
    """
    declared = frozenset(k for k in inputs if k not in inherited)
    rows: list[dict[str, Any]] = []
    for key, layer in layers_by_key.items():
        spec = inputs.get(key) if isinstance(inputs.get(key), dict) else {}
        prose = str(layer.get("source", ""))
        value = layer.get("value")
        shape = Layers(inherited=inherited, declared=declared)
        if prose.startswith("input (supplied)"):
            row = resolve_layer(key, shape, override={
                "value": value, "source": RUN,
                "detail": ("supplied at launch"
                           + (f" — overrides preset {preset_name!r}"
                              if "overrides preset" in prose else ""))})
        elif prose.startswith("preset"):
            row = resolve_layer(key, shape, override={
                "value": value, "source": RUN,
                "detail": f"from preset {preset_name!r} — a saved set of run inputs"})
        elif prose.startswith("default"):
            row = resolve_layer(key, Layers(defaults={key: value},
                                            inherited=inherited, declared=declared))
        else:
            # `required — UNSATISFIED` / `optional (no default)`: no layer holds
            # it, which is exactly `unknown` (see the module docstring).
            row = resolve_layer(key, shape, override={
                "value": None, "source": UNKNOWN,
                "detail": ("required — nothing supplies it yet; the launch must"
                           if "UNSATISFIED" in prose else
                           "optional — no layer supplies it and it has no default")})
        row["type"] = str(layer.get("type", "") or spec.get("type", "") or "")
        row["required"] = "UNSATISFIED" in prose
        rows.append(row)
    return rows


def environment_row(environment: str, workspace: Mapping[str, Any]) -> dict[str, Any]:
    """WHICH environment this explain resolved gates, policies and backends for.

    Explicit at the door beats the workspace's ``environment`` variable, which
    is what the run dialog pre-selects (ADR-0101 D2); with neither, the
    deployment's own default answers and the row says so rather than naming a
    file that does not exist.
    """
    layers = Layers(run=({"environment": environment} if environment else {}),
                    workspace=({"environment": workspace["environment"]}
                               if workspace.get("environment") else {}))
    row = resolve_layer("environment", layers)
    row["label"] = "Environment"
    if row["source"] == RUN:
        row["detail"] = "chosen for this explain (a launch chooses it the same way)"
    elif row["source"] == UNKNOWN:
        # Not unknown: nobody named one, and the deployment's own default is
        # the declared fallback. `unknown` is reserved for a value whose owner
        # cannot be established at all.
        row["value"] = "(deployment default)"
        row["source"] = DEFAULT
        row["detail"] = ("neither this request nor the workspace names an "
                         "environment — the deployment's default profile answers")
    return row


__all__ = [
    "DEFAULT", "ENV", "INHERITED", "ORDER", "PIPELINE", "PROFILE", "RUN",
    "SOURCES", "UNKNOWN", "WORKSPACE", "Layers", "ambient_profile",
    "backend_rows", "env_source", "environment_row", "governance_rows",
    "inherited_source", "input_rows", "profile_source", "resolve_layer",
    "source_kind",
]
