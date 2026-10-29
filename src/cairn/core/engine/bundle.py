"""Bundle-pipeline data contract (ADR-0066 Phase 2 / #581).

A **bundle** groups packs into one installable distribution AND — via
``[[bundle.pipeline]]`` — carries the cross-pack **data contract**: an ordered
list of steps (each a ``pack/template``), with each step's inputs wired from the
bundle input (``$input.*``) or a prior step's output (``$steps.<id>.<field>``),
plus declarative ``gate`` predicates.

This module owns the **compile-time** half: parse ``bundle.toml`` and *validate
the contract* — every ``$steps.<id>.<field>`` wire must resolve to a real field
of that step's OUTPUT schema and be type-compatible with the target step's INPUT
field. A type-incompatible wire fails validation; that is what makes it a
contract, not just a wire (ADR-0066 §3). Execution (contract → runnable graph)
is a separate concern built on the flat-node emitter.

The validator takes an injectable ``resolve_step_schemas`` so it is unit-testable
without the schema registry; :func:`default_step_schemas` wires the production
path (template frontmatter → :func:`resolve_schema`).
"""
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field, field_validator

from cairn.core.engine.expr import (  # the ONE expression language (#1118)
    MISSING,
    ExprError,
    Scopes as ExprScopes,
    coerce_literal,
    compile_expr,
    compile_predicate,
    resolve_value,
)
from cairn.core.engine.schemas import ResolvedSchema, resolve_schema, schema_fields


class BundleContractError(Exception):
    """A ``[[bundle.pipeline]]`` wire violates the data contract (unknown field,
    forward/self reference, or a type-incompatible output→input wire)."""


# ─── parsed models ────────────────────────────────────────────────────────


#: ADR-0069's step trigger grammar — the ONE check, shared by a bundle step and a
#: declared pipeline's leg (#2605), so the two spellings cannot drift apart.
STEP_TRIGGER_FORMS = "'auto' | 'manual' | 'schedule:<cron>' | 'on:<event>'"


def check_step_trigger(v: str) -> str:
    """Return ``v`` if it is a step trigger the bridge can arm; raise ``ValueError``
    otherwise. ``schedule:`` and ``on:`` need something after the colon — an empty
    cron or topic would arm a waiter that can never fire."""
    if v in ("auto", "manual"):
        return v
    for prefix in ("schedule:", "on:"):
        if v.startswith(prefix):
            if not v[len(prefix):].strip():
                raise ValueError(f"invalid trigger {v!r}: `{prefix}` needs a value after the colon")
            return v
    raise ValueError(f"invalid trigger {v!r}: expected {STEP_TRIGGER_FORMS}")


class BundlePipelineStep(BaseModel):
    """One step of a bundle pipeline — a ``pack/template`` invocation whose
    inputs are wired from ``$input.*`` / ``$steps.<id>.<field>`` bindings.

    ADR-0069: a step is its OWN governed run. The fields below are the declarative
    trigger-binding surface — all optional, so a legacy bundle (no trigger/depends_on)
    still parses and defaults to ``auto`` + implicit-linear (back-compat). They carry
    no execution behaviour on their own; :func:`~cairn.core.engine.bundle_pipeline.plan_bundle`
    turns them into a :class:`~cairn.core.engine.bundle_pipeline.PipelinePlan` and the
    Phase-2 registrar binds them to the bus."""

    id: str
    pack: str
    template: str
    gate: Optional[str] = None  # predicate, e.g. "$steps.gate.verdict == 'pass'" → a TRIGGER CONDITION (ADR-0069)
    inputs: dict[str, str] = Field(default_factory=dict)  # target_field -> binding (resolved from persisted output)

    # ── ADR-0069 declarative trigger-binding surface (all optional) ──
    trigger: str = "auto"  # auto | manual | schedule:<cron> | on:<domain-event>
    depends_on: list[str] = Field(default_factory=list)  # upstream step ids → DAG + joins (empty → implicit linear)
    environment: Optional[str] = None  # per-step env/profile (its own box); None → the pipeline's env
    approval: bool = False  # inter-stage HITL: pause (durable) for approval before this run submits
    emits: Optional[str] = None  # domain event published on completion (other bundles chain off it)

    @field_validator("trigger")
    @classmethod
    def _valid_trigger(cls, v: str) -> str:
        return check_step_trigger(v)

    @property
    def ref(self) -> str:
        return f"{self.pack}/{self.template}"


class BundlePipeline(BaseModel):
    name: str
    step: list[BundlePipelineStep] = Field(default_factory=list)
    # ADR-0069 §5 — execution mode. "pipeline" (default): N governed runs chained by
    # the core PipelineBridge. "graph": the legacy flatten (ONE run, ADR-0069 Slice 5);
    # its compiler (compile_bundle.py) is removed, so nothing executes a "graph"
    # pipeline.
    mode: str = "pipeline"


def wire_refs(text: str, *, scope: str, source: str) -> list[tuple[str, str]]:
    """Parse ``text`` with the ONE expression language (#1118) and return the
    ``(id, field)`` pairs it references in ``scope`` (``"steps"``/``"input"``).
    Replaces the local `_STEPS_REF`/`_INPUT_REF` regexes — the grammar has
    exactly one definition now. Malformed text → empty (the caller's own
    malformed-binding check raises with its site-specific message)."""
    try:
        parsed = compile_predicate(text, source=source)
    except ExprError:
        return []
    out = []
    for sc, parts in parsed.references():
        if sc == scope and parts:
            out.append((parts[0], ".".join(parts[1:])))
    return out


# ─── type compatibility ───────────────────────────────────────────────────

_PY_TOKEN = {
    "str": "str", "int": "int", "float": "number", "bool": "bool",
    "list": "list", "dict": "dict", "tuple": "list", "set": "list",
    "Any": "any", "NoneType": "any",
}
_JSON_TOKEN = {
    "string": "str", "integer": "int", "number": "number", "boolean": "bool",
    "array": "list", "object": "dict", "null": "any",
}
# numeric widening: an int wire into a number/float target is fine
_COMPAT = {("int", "number"), ("number", "int")}




def _field_type_token(schema: Optional[ResolvedSchema], field: str) -> str:
    """A normalized type token for ``field`` on either a Pydantic class or a
    JSON-Schema dict. ``"any"`` when the type is unknown/unconstrained (which is
    treated as compatible with everything — we only fail on a *clear* mismatch)."""
    if schema is None:
        return "any"
    if isinstance(schema, dict):
        prop = (schema.get("properties") or {}).get(field, {})
        t = prop.get("type")
        if isinstance(t, list):  # JSON Schema union e.g. ["string","null"]
            non_null = [x for x in t if x != "null"]
            t = non_null[0] if non_null else "null"
        return _JSON_TOKEN.get(t, "any")
    fld = schema.model_fields.get(field)
    if fld is None:
        return "any"
    ann = fld.annotation
    # unwrap Optional[X] / Union[X, None] to X
    args = getattr(ann, "__args__", None)
    if args:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            ann = non_none[0]
    name = getattr(ann, "__name__", None) or getattr(ann, "_name", None) or str(ann)
    origin = getattr(ann, "__origin__", None)
    if origin in (list, tuple, set):
        return "list"
    if origin is dict:
        return "dict"
    return _PY_TOKEN.get(name, "any")


def _types_compatible(src: str, dst: str) -> bool:
    if src == "any" or dst == "any" or src == dst:
        return True
    return (src, dst) in _COMPAT


# ─── the contract validator ───────────────────────────────────────────────

# resolve_step_schemas(pack, template) -> (input_schema | None, output_schema | None)
StepSchemaResolver = Callable[[str, str], tuple[Optional[ResolvedSchema], Optional[ResolvedSchema]]]


def validate_pipeline_contract(
    pipeline: BundlePipeline, resolve_step_schemas: StepSchemaResolver
) -> None:
    """Validate one pipeline's wires against the participating templates'
    schemas. Raises :class:`BundleContractError` on the first violation.

    Checks, per step (in order):
    - duplicate step ids → error;
    - each ``inputs`` target field is a real INPUT field of the step's template;
    - a ``$steps.<id>.<field>`` binding references a PRIOR step, ``<field>`` is a
      real OUTPUT field of that step, and its type is compatible with the target
      INPUT field;
    - a ``gate`` predicate's ``$steps.<id>.<field>`` tokens reference prior
      steps' real output fields.

    ``$input.*`` bindings are accepted (bundle-level input validation is a
    follow-up — a bundle may declare an input schema later).
    """
    seen: dict[str, tuple[Optional[ResolvedSchema], Optional[ResolvedSchema]]] = {}

    for step in pipeline.step:
        site = f"pipeline {pipeline.name!r} step {step.id!r} ({step.ref})"
        if step.id in seen:
            raise BundleContractError(f"{site}: duplicate step id {step.id!r}")

        in_schema, out_schema = resolve_step_schemas(step.pack, step.template)
        in_fields = schema_fields(in_schema) if in_schema is not None else None

        # gate predicate references must resolve to prior outputs
        if step.gate:
            for src_id, src_field in wire_refs(
                step.gate, scope="steps", source=f"{site} gate"
            ):
                _check_source(seen, src_id, src_field, f"{site} gate")

        for target_field, binding in step.inputs.items():
            # the target must be a real input field of this step's template
            if in_fields is not None and target_field not in in_fields:
                raise BundleContractError(
                    f"{site}: wires unknown input field {target_field!r} — "
                    f"not on {step.template} input schema"
                )
            b = binding.strip() if isinstance(binding, str) else binding
            if isinstance(b, str) and b.startswith("$"):
                try:
                    refs = compile_expr(b, source=site).references()
                except ExprError:
                    raise BundleContractError(
                        f"{site}: malformed binding {binding!r} — expected "
                        f"$input.<field>, $steps.<id>.<field>, or a literal"
                    ) from None
                step_refs = [
                    (parts[0], ".".join(parts[1:]))
                    for sc, parts in refs if sc == "steps" and parts
                ]
                if not step_refs and not any(sc == "input" for sc, _ in refs):
                    raise BundleContractError(
                        f"{site}: malformed binding {binding!r} — expected "
                        f"$input.<field>, $steps.<id>.<field>, or a literal"
                    )
                for src_id, src_field in step_refs:
                    src_out = _check_source(seen, src_id, src_field, site)
                    # type compatibility (output field -> input field)
                    src_t = _field_type_token(src_out, src_field)
                    dst_t = _field_type_token(in_schema, target_field)
                    if not _types_compatible(src_t, dst_t):
                        raise BundleContractError(
                            f"{site}: incompatible wire {binding} ({src_t}) → "
                            f"{target_field} ({dst_t})"
                        )

        seen[step.id] = (in_schema, out_schema)


def _check_source(
    seen: dict[str, tuple[Optional[ResolvedSchema], Optional[ResolvedSchema]]],
    src_id: str,
    src_field: str,
    site: str,
) -> Optional[ResolvedSchema]:
    """A ``$steps.<src_id>.<src_field>`` reference must point at a PRIOR step
    whose OUTPUT schema declares ``src_field``. Returns that output schema."""
    if src_id not in seen:
        raise BundleContractError(
            f"{site}: references step {src_id!r} which is not a prior step "
            f"(forward/self reference or unknown id)"
        )
    _in, out = seen[src_id]
    if out is not None and src_field not in schema_fields(out):
        raise BundleContractError(
            f"{site}: references {src_id}.{src_field} — not an output field of "
            f"step {src_id!r}"
        )
    return out


# ─── runtime binding engine (the contract, evaluated) ─────────────────────
#
# The compile-time validator above proves the wires are *sound*; these resolve
# them at *run* time. A step's output is the template runnable's result dict —
# deterministic operators write to ``findings.*`` (dataset/gate → findings.verdict,
# ADR-0066/#585), so `$steps.<id>.<field>` reads top-level first, then findings.


def resolve_binding(
    binding: Any, *, bundle_input: dict, steps: dict, on_missing: str = "none"
) -> Any:
    """Resolve one ``$input.*`` / ``$steps.<id>.<field>`` / literal binding
    against the bundle input + accumulated step outputs.

    Delegates to the ONE expression language (#1118): the ``$steps``
    findings-fallback and missing→None semantics are the ``steps`` scope's
    documented behavior there, not a bundle-local hack anymore. Non-``$``
    tokens keep the loose gate-literal coercion (``5``→int, bare word→str).

    ``on_missing="omit"`` returns :data:`MISSING` instead of ``None`` so the
    caller can DROP the key — what an OPTIONAL wire onto a routed-off leg needs
    (#1251), and the same semantic ``$config`` already has.

    #1675 — a non-string binding is a structured (or typed) YAML literal: it
    resolves through the shared container walker, so a ``$…`` string nested in
    a map or list is still a wire (an unresolved one becomes ``None``, the
    ``$input`` semantic) and every other value is passed through as authored.
    """
    if not isinstance(binding, str):
        scopes = ExprScopes(input=bundle_input or {}, steps=steps or {})
        return resolve_value(binding, scopes, source="bundle wire", on_missing="none")
    b = binding.strip()
    if b.startswith("$"):
        scopes = ExprScopes(input=bundle_input or {}, steps=steps or {})
        val = compile_expr(b, source="bundle wire").try_evaluate(scopes)
        if val is MISSING:
            return MISSING if on_missing == "omit" else None
        return val
    return coerce_literal(b)


class WireResolutionError(ValueError):
    """A non-optional step wire resolved to NOTHING at run time (#1540).

    The producing leg completed (dependency order guarantees it), yet its
    output carries no such field — a renamed output, or a field the producer
    only sometimes emits. Passing a silent ``None`` downstream is the exact
    3am failure the typed boundary exists to prevent, so this raises with
    both ends named; the bridge turns it into a failed pipeline with the
    message as the cited reason.
    """


def resolve_step_inputs(
    step: BundlePipelineStep, *, bundle_input: dict, steps: dict,
    optional_inputs: Optional[set[str]] = None,
) -> dict[str, Any]:
    """The typed input dict to hand a step's template, resolved from the bundle
    input and prior step outputs per the step's ``inputs`` wiring.

    Keys named in ``optional_inputs`` (default: the step's own
    ``optional_inputs``, the `consumes: {x: Report?}` declarations) are OMITTED
    when their wire resolves to nothing — the producing leg was routed off by a
    false ``when`` (#1251, ADR-0087 D2). The downstream leg then falls back to
    its own default instead of receiving a null it never declared.

    A NON-optional ``$steps.<id>.<field>`` wire that resolves to nothing is a
    :class:`WireResolutionError` (#1540) — never a silent ``None``. ``$input``
    wires keep their legacy missing→None semantics (inputs have defaults
    resolved upstream), and ``$default(...)`` remains the author's explicit
    escape hatch for a field a producer only sometimes emits.
    """
    optional = set(
        optional_inputs if optional_inputs is not None
        else (getattr(step, "optional_inputs", None) or ())
    )
    out: dict[str, Any] = {}
    for target, binding in step.inputs.items():
        val = resolve_binding(
            binding, bundle_input=bundle_input, steps=steps, on_missing="omit",
        )
        if val is MISSING:
            if target in optional:
                continue
            b = str(binding).strip()
            if b.startswith("$steps."):
                step_id = getattr(step, "id", "") or getattr(step, "name", "?")
                raise WireResolutionError(
                    f"step {step_id!r} input {target!r} is wired from "
                    f"`{b}`, which resolved to NOTHING — the producing leg "
                    f"completed but its output has no such field (renamed? "
                    f"conditional?). Declare the wire optional "
                    f"(`consumes: {{{target}: Kind?}}`) or use "
                    f"`$default({b}, <fallback>)` if the field is legitimately "
                    f"sometimes absent; refusing to pass a silent null."
                )
            out[target] = None
            continue
        out[target] = val
    return out


def eval_gate_predicate(predicate: str, *, bundle_input: dict, steps: dict) -> bool:
    """Evaluate a declarative ``gate`` predicate — governance as a conditional
    edge, never an LLM decision. Supports ``<ref> == <literal>`` /
    ``<ref> != <literal>`` and a bare ``<ref>`` (truthiness); the ONE
    expression language (#1118) also admits new-grammar JMESPath predicates
    over ``{input, steps}`` (e.g. ``steps.gate.verdict == 'pass'``)."""
    if not predicate or not predicate.strip():
        return True
    scopes = ExprScopes(input=bundle_input or {}, steps=steps or {})
    pred = compile_predicate(predicate, source="bundle gate")
    val = pred.try_evaluate(scopes)
    if val is MISSING:
        return False
    return bool(val)


def default_step_schemas(
    pack: str, template: str
) -> tuple[Optional[ResolvedSchema], Optional[ResolvedSchema]]:
    """Production step-schema resolver: read the template's frontmatter
    (``input_schema`` / ``output_schema`` refs) and resolve each via the schema
    registry. Used by the compiler; tests inject a synthetic resolver instead."""
    import frontmatter  # noqa: PLC0415 - heavy-ish, only needed on the compile path

    from cairn.core.engine.skill_engine import resolve_template_path

    path = resolve_template_path(f"{pack}/{template}")
    post = frontmatter.load(path)
    meta = post.metadata or {}
    return (
        resolve_schema(meta.get("input_schema")),
        resolve_schema(meta.get("output_schema")),
    )


__all__ = [
    "STEP_TRIGGER_FORMS",
    "check_step_trigger",
    "BundlePipeline",
    "BundlePipelineStep",
    "BundleContractError",
    "validate_pipeline_contract",
    "default_step_schemas",
]
