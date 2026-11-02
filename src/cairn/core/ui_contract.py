"""cairn UI contract (#109, Phase 0) — the single per-template bundle every
surface (CLI/TUI/Web/Slack) renders from.

LLM-free, pure assembly over what already exists:
  - input/output **JSON Schema** dicts — from the YAML `schema://` registry,
  - the governed **plan** (operators, missing bindings, approvals, can_run) —
    from :func:`cairn.core.plan.build_plan`,
  - the ordered **steps** — the template's operations (capability + operator),
  - **integrations** — per-capability ready/missing status.

This is the read-only contract; remediation (`missing → fix` commands) and the
richer step titles layer on top later (see #109).
"""
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Optional

import frontmatter
from pydantic import BaseModel, Field

from cairn.core.pack.curation import DEFAULT_VISIBILITY, normalize
from cairn.core.plan import Plan, _resolve_template, build_plan
from cairn.core.refs import pack_key  # ONE ref spelling (#2126)
from cairn.core.schema import build_schema_uri, default_registry, is_schema_uri


class Step(BaseModel):
    """One governed step in the run timeline (a template operation)."""

    id: str
    title: str
    capability: str = ""
    operator: str = ""
    registered: bool = True
    requires_approval: bool = False


class Integration(BaseModel):
    """A capability the template needs + whether its operator is available."""

    capability: str = ""
    operator: str = ""
    status: str = "ready"  # "ready" | "missing"
    detail: str = ""


class Demo(BaseModel):
    """A one-click "Try with sample data" for a workflow (§394).

    Sourced from the pack's ``[demo."<template>"]`` manifest block. ``inputs``
    is a map of prefill values (relative example paths already resolved to
    absolute; HF Hub ids left as-is) the Run form drops into its fields. When a
    pack ships several tiers for one workflow they surface as a picker, keyed by
    ``tier`` (easy | medium | complex)."""

    title: str = ""
    description: str = ""
    inputs: dict[str, Any] = Field(default_factory=dict)
    eval_path: str = ""
    tier: str = ""


class Remediation(BaseModel):
    """A missing dependency + the suggested fix (#109). Grounded only in
    DECLARED data: missing operators (build_plan) and `mcp_server` connections.
    Credential remediation waits on operators declaring `requires_secrets`."""

    kind: str   # "operator" | "mcp"
    what: str
    fix: str


def pack_slug(ref: str) -> str:
    """Stable per-template slug for URLs (distinct from a run id): the ref with
    ``/`` → ``--``. e.g. ``training/finetune`` → ``training--finetune``."""
    return ref.replace("/", "--")


def _profile_visibility(path: Path) -> str:
    """A profile's catalog tier, read from its ``[profile] visibility`` key.

    Unreadable or unset → public, matching the template rule: curation is
    opt-in, so a new environment is never invisible because its author forgot
    a key, and one malformed file cannot blank the whole selector.
    """
    try:
        with path.open("rb") as fh:
            return normalize(tomllib.load(fh).get("profile", {}).get("visibility"))
    except Exception:  # noqa: BLE001 — a broken profile is doctor's problem, not the selector's
        return DEFAULT_VISIBILITY


def list_profiles() -> list[dict[str, str]]:
    """Available execution profiles (``{name, path, visibility}``) from
    ``cairn/profiles/*.toml`` — the selector options for a run. The
    plan/can_run/remediation resolve under the chosen one (bindings + policy env
    differ per profile).

    Every profile is returned, ANNOTATED with its tier (#1570) rather than
    filtered: the CLI's ``--profile`` and an operator debugging a deployment
    must still see all of them. The console picker curates on the annotation.
    """
    prof_dir = Path(__file__).resolve().parents[1] / "profiles"
    if not prof_dir.is_dir():
        return []
    return [{"name": f.stem, "path": str(f), "visibility": _profile_visibility(f)}
            for f in sorted(prof_dir.glob("*.toml"))]


def _profile_path(profile: Optional[str]) -> Optional[str]:
    """Resolve a profile NAME (from the selector) → its .toml path; pass a path
    through unchanged (so the CLI's ``--profile <path>`` still works)."""
    if not profile:
        return None
    for p in list_profiles():
        if p["name"] == profile:
            return p["path"]
    return profile


class UIContract(BaseModel):
    """Everything a surface needs to render a pack/template end-to-end."""

    template_ref: str
    slug: str = ""           # stable per-template id (the ref with / → --)
    pack: str = ""
    template: str = ""
    title: str = ""
    description: str = ""
    inputs: dict[str, Any] = Field(default_factory=dict)   # JSON Schema
    outputs: dict[str, Any] = Field(default_factory=dict)  # JSON Schema
    steps: list[Step] = Field(default_factory=list)
    integrations: list[Integration] = Field(default_factory=list)
    #: §394 — the workflow's declared demos (0..N tiers). One renderer change
    #: (the "Try with sample data" button) lights up every workflow that ships
    #: a ``[demo]`` block; empty for workflows without one.
    demos: list[Demo] = Field(default_factory=list)
    remediation: list[Remediation] = Field(default_factory=list)
    profiles: list[str] = Field(default_factory=list)   # selectable execution profiles
    execution_profile: str = ""                          # the one this contract resolved under
    plan: dict[str, Any] = Field(default_factory=dict)
    can_run: bool = False        # operators registered + bindings resolve (plan-level)
    ready: bool = False          # can_run AND no outstanding remediation
    recommended_command: str = ""


def _schema_dict(ref: Optional[str]) -> dict[str, Any]:
    """Raw JSON Schema dict for a ``schema://`` ref, or ``{}`` if absent/legacy."""
    if not ref or not is_schema_uri(ref):
        return {}
    try:
        d = default_registry.get(ref)
    except Exception:  # noqa: BLE001 — a missing/legacy schema must not break the contract
        return {}
    return d if isinstance(d, dict) else {}


def _output_ref(meta: dict[str, Any], pack: str) -> Optional[str]:
    """Resolve the output schema ref: prefer ``output_schema:`` (a URI); else
    build one from ``output.schema`` (a bare name) + the pack."""
    direct = meta.get("output_schema")
    if direct:
        return str(direct)
    out = meta.get("output")
    if isinstance(out, dict) and out.get("schema"):
        name = str(out["schema"])
        return name if is_schema_uri(name) else build_schema_uri(pack, name)
    return None


def _template_file(template_ref: str) -> Optional[Path]:
    """``<pack>/<template>`` → the template ``.md`` path via the pack registry."""
    from cairn.core.pack.registry import get_packs

    if "/" not in template_ref:
        return None
    pack_name, tmpl = template_ref.split("/", 1)
    for p in get_packs():
        if pack_key(p.name) != pack_key(pack_name):  # #970 dir vs pack.toml name (#2126)
            continue
        # #1163 — `flows/` AND the `templates/` alias
        from cairn.core.pack.registry import flow_dirs_of  # noqa: PLC0415 - cycle

        for td in flow_dirs_of(p):
            if (Path(td) / f"{tmpl}.md").exists():
                return Path(td) / f"{tmpl}.md"
    return None


def template_inputs(template_ref: str) -> Optional[dict[str, Any]]:
    """``build_ui_contract(template_ref).inputs`` WITHOUT building the plan, or
    ``None`` where that contract's plan reports ``template_found: False``.

    For readers that need only the input schema — the pipeline ``$inherit``
    resolver, which runs on every cold pipeline discovery. ``build_plan`` walks
    the template's operators and discovers every operator pack's manifest to do
    it (seconds per call); none of that reaches ``inputs``, which is the
    template's frontmatter ``input_schema`` through the schema registry.

    Existence is ``build_plan``'s own test (:func:`~cairn.core.plan._resolve_template`),
    and the frontmatter is read from that file with the same loader and NOT
    guarded, so an unreadable template raises here exactly where ``build_plan``
    would. ``tests/core/test_pipeline_inherit.py`` pins this equal to
    ``build_ui_contract(...).inputs`` for every template a shipped ``$inherit``
    reaches.
    """
    pack, path = _resolve_template(template_ref)
    if pack is None or not path or not path.exists():
        return None
    meta = frontmatter.load(path).metadata or {}
    return _schema_dict(meta.get("input_schema"))


def _demos(template_ref: str) -> list[Demo]:
    """Resolve a workflow's declared demos (§394).

    Read through :func:`cairn.core.pack.demos.demos_for`, which asks the FLOW
    first (ADR-0106 D5 — sample data belongs beside the flow it prefills) and
    falls back to the pack's superseded ``[demo]`` manifest block for one
    release. Relative example paths are resolved against the pack root; HF Hub
    ids and other scalars pass through. ``[]`` when nothing is declared."""
    from cairn.core.pack.demos import demos_for
    from cairn.core.pack.registry import get_packs

    if "/" not in template_ref:
        return []
    pack_name, tmpl = template_ref.split("/", 1)
    for p in get_packs():
        if pack_key(p.name) != pack_key(pack_name):  # #970 dir vs pack.toml name (#2126)
            continue
        specs = demos_for(p, tmpl)
        if not specs:
            return []
        # Resolve relative example paths against the pack root (parent of the
        # templates dir), so `path = "examples/sample.jsonl"` points at the file.
        from cairn.core.pack.registry import flow_dirs_of  # noqa: PLC0415 - cycle

        dirs = flow_dirs_of(p)          # #1163 — pack root = parent of its flow dir
        root = Path(dirs[0]).parent if dirs else Path(".")
        out: list[Demo] = []
        for spec in specs:
            resolved = spec.resolved(root)
            out.append(Demo(
                title=resolved.title or tmpl,
                description=resolved.description,
                inputs=resolved.inputs,
                eval_path=resolved.eval_path or "",
                tier=resolved.tier or "",
            ))
        return out
    return []


def _remediation(operators, connected_names):  # noqa: ANN001
    """Grounded missing→fix items: missing operators (build_plan) + MCP servers
    an operator needs but that aren't connected (only when ``connected_names``
    is given). Deduped. Credential remediation waits on operator-declared
    ``requires_secrets`` (a prerequisite — see the #109 sub-issue)."""
    out: list[Remediation] = []
    seen: set[tuple[str, str]] = set()
    for op in operators:
        if not op.registered:
            key = ("operator", op.operator)
            if key in seen:
                continue
            seen.add(key)
            out.append(Remediation(
                kind="operator",
                what=f"operator {op.operator!r} is not installed",
                fix=f"install an operator pack providing {op.operator!r}",
            ))
        elif op.mcp_server and connected_names is not None and op.mcp_server not in connected_names:
            key = ("mcp", op.mcp_server)
            if key in seen:
                continue
            seen.add(key)
            out.append(Remediation(
                kind="mcp",
                what=f"MCP server {op.mcp_server!r} (needed by {op.operator}) is not connected",
                fix=f"cairn admin connect add {op.mcp_server}",
            ))
    return out


def build_ui_contract(
    template_ref: str,
    *,
    inputs: Optional[dict] = None,
    profile: Optional[str] = None,
    connected_names: Optional[set[str]] = None,
) -> UIContract:
    """Assemble the UI contract for a template — LLM-free; reuses ``build_plan``
    + the schema registry + the template frontmatter.

    ``connected_names`` (resolved by the caller from the ConnectionStore, since
    it's async) lets the contract flag MCP servers an operator needs but that
    aren't connected. Omit it to skip the connection check.

    ``profile`` is an execution-profile NAME (from the selector) or a path; the
    plan/can_run/remediation resolve under it."""
    plan: Plan = build_plan(template_ref, inputs or {}, profile=_profile_path(profile))

    meta: dict[str, Any] = {}
    f = _template_file(template_ref)
    if f is not None:
        try:
            meta = frontmatter.load(f).metadata or {}
        except Exception:  # noqa: BLE001
            meta = {}

    steps = [
        Step(
            id=op.operator or f"step-{i}",
            title=(op.capabilities[0] if op.capabilities else op.operator),
            capability=op.capabilities[0] if op.capabilities else "",
            operator=op.operator,
            registered=op.registered,
            requires_approval=op.requires_approval,
        )
        for i, op in enumerate(plan.operators)
    ]
    integrations = [
        Integration(
            capability=op.capabilities[0] if op.capabilities else "",
            operator=op.operator,
            status="ready" if op.registered else "missing",
            detail="" if op.registered else f"operator {op.operator!r} is not registered/installed",
        )
        for op in plan.operators
    ]

    remediation = _remediation(plan.operators, connected_names)

    # execution_profile must be a NAME (so the web selector highlights it); a
    # selected known name wins over the plan's value (which is a path when set).
    known = {p["name"] for p in list_profiles()}
    exec_profile = profile if profile in known else plan.execution_profile

    return UIContract(
        template_ref=template_ref,
        slug=pack_slug(template_ref),
        pack=plan.selected_pack,
        template=plan.template,
        title=str(meta.get("name") or plan.template),
        description=" ".join(str(meta.get("description") or "").split()),
        inputs=_schema_dict(meta.get("input_schema")),
        outputs=_schema_dict(_output_ref(meta, plan.selected_pack)),
        steps=steps,
        integrations=integrations,
        demos=_demos(template_ref),
        remediation=remediation,
        profiles=[p["name"] for p in list_profiles()],
        execution_profile=exec_profile,
        plan=plan.model_dump(),
        can_run=plan.can_run,
        ready=plan.can_run and not remediation,
        recommended_command=plan.recommended_command,
    )
