"""
Template compiler — turns markdown + YAML frontmatter into an executable runnable.

Supports three flow types:
  - prompt        : single LLM call with structured output
  - state_machine : behavior tree compiled to a LangGraph StateGraph (minimal subset:
                    sequence + parallel + action; action nodes inner-flow as
                    prompt — the hybrid pattern)

The compiler is a single entry point `compile_template(path)` that dispatches
to the right builder based on `flow.type`.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import json
import logging
import warnings
import operator
import re
from pathlib import Path
from typing import Annotated, Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # annotation-only: a real import here is a cycle (#1515)
    from cairn.core.operator.binding import OperationBinding


import frontmatter
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import Runnable, RunnableConfig
from langgraph.errors import GraphBubbleUp
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from cairn.core.engine.compiled import CompiledTemplate, compute_content_hash
from cairn.core.engine.condition_registry import resolve_condition
from cairn.core.clients.llm_client import build_chat_model
from cairn.core.engine.python_function_registry import resolve_python_function
from cairn.core.registry import CapabilityRegistry, default_registry
from cairn.core.runtime import InProcessRuntime, NodeKind, Runtime, tag
from cairn.core.runtime.retry import (
    RetryPolicy,
    RetrySpec as RuntimeRetrySpec,
    resolve as resolve_retry,
)
from cairn.core.engine.schema import (
    ActionBTNode,
    ActionNodeDef,
    AgentLoopFlow,
    BTNode,
    GuardBTNode,
    MapBTNode,
    OperationBTNode,
    ParallelBTNode,
    PromptFlow,
    PythonBTNode,
    SelectorBTNode,
    SequenceBTNode,
    StateMachineFlow,
    SubgraphBTNode,
    TemplateSpec,
)
from cairn.core.engine.expr import (  # the ONE expression language (#1118)
    ExprError,
    compile_expr,
)
from cairn.core.operator import (
    OperationBindingError,
    UnknownOperatorError,
    default_registry as default_operator_registry,
    invoke_operator,
    invoke_operator_inprocess,
    load_binding,
    validate_operation_binding,
)
from cairn.core.operator.elaborate import elaborate_binding  # 2b (#1107)
from cairn.core.operator.effective import ContractConformanceError, ContractEnvelope  # #2241 C
from cairn.core.engine.workflow_config import resolve_capability_binding  # #2241 C (D5)
from cairn.core.engine.schemas import (
    SchemaResolutionError,
    required_fields,
    resolve_schema,
    schema_fields,
)

logger = logging.getLogger(__name__)


# ─── Entry point ────────────────────────────────────────────────────


def compile_template(
    path: Path | str,
    *,
    registry: CapabilityRegistry | None = None,
    extra_context: str | None = None,
) -> CompiledTemplate:
    """Parse a template .md file and compile it to a runnable.

    Args:
        path: Path to the template .md file.
        registry: Capability registry (defaults to module-level singleton).
        extra_context: Optional additional system-prompt content (e.g. injected
                       by a skill via context_append).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")

    registry = registry or default_registry

    # Parse markdown + frontmatter
    post = frontmatter.load(path)
    body = post.content                # template identity is based on the
                                        # ORIGINAL body, not prose-with-context
    prose = body
    if extra_context:
        prose = f"{prose}\n\n---\n\n## Additional context\n\n{extra_context}\n"

    # Validate frontmatter against schema
    spec = TemplateSpec.model_validate(post.metadata)

    # Validate that all requested capabilities exist before compiling
    all_caps: list[str] = list(spec.tools.required) + list(spec.tools.dynamic)
    if isinstance(spec.flow, StateMachineFlow):
        for node_def in spec.flow.nodes.values():
            all_caps.extend(node_def.capabilities)
    registry.validate(all_caps)

    # Consistency: a capability declared `forbidden` must not also be used.
    # This is the one cheap rule we enforce on the forbidden list today — full
    # runtime enforcement (blocking a tool the model tries to call) lands later
    # at the MCP gateway. Catches the obvious authoring mistake now.
    forbidden = set(spec.tools.forbidden)
    if forbidden:
        clash = forbidden.intersection(all_caps)
        if clash:
            raise ValueError(
                f"Template '{spec.name}' lists capabilities as both forbidden "
                f"and required/dynamic: {sorted(clash)}"
            )

    # Phase 2a: validate every declared schema reference resolves to a real
    # Pydantic BaseModel subclass. We resolve eagerly (rather than at run
    # time) so a typo in `input_schema:` fails the compile, not the run —
    # consistent with how capability and condition references work above.
    # Three resolution sites:
    #   - template-level input_schema / output_schema (subgraph contract)
    #   - each action node's output_schema (existed since Phase 1; only now
    #     does anything read it)
    _validate_schema_refs(spec, path)

    # Dispatch to the right builder
    unsupported_mock_operators: list[str] = []
    if isinstance(spec.flow, PromptFlow):
        runnable = _build_prompt(spec, prose)
        flow_type = "prompt"
    elif isinstance(spec.flow, AgentLoopFlow):
        # ADR-0084 D3 — REMOVED. The workflow model has no tool loop: an LLM step
        # is a single-shot `prompt` node, so the AUTHOR always declares control
        # flow and the model decides content only. (`cairn ask` keeps its own loop
        # in cairn/ask/agent.py — product code, outside the pack grammar.)
        raise ValueError(
            f"template {spec.name!r} uses `flow.type: agent_loop`, which was "
            f"REMOVED by ADR-0084 D3 — the workflow model has no tool loop. "
            f"Use `flow.type: state_machine` with a `prompt` node: the author "
            f"declares control flow, the model decides content."
        )
    elif isinstance(spec.flow, StateMachineFlow):
        # §24.20.5: operations/ lives alongside templates/ under the pack root.
        # Operation BT nodes resolve their binding YAML from here at compile time.
        operations_dir = path.parent.parent / "operations"
        runnable = _build_state_machine(
            spec, prose, registry,
            operations_dir=operations_dir,
            source_path=path,
            unsupported_mock_operators=unsupported_mock_operators,
        )
        flow_type = "state_machine"
    else:
        raise ValueError(f"Unsupported flow type: {type(spec.flow).__name__}")

    # §24.11.3: content-addressed identity. Hash is over the canonical
    # frontmatter projection + the ORIGINAL body (extra_context is runtime
    # injection, not part of template identity). Computed at compile time
    # so callers can pin runs to a specific hash without re-reading the file.
    content_hash = compute_content_hash(spec, body)

    return CompiledTemplate(
        spec=spec,
        prose=prose,
        flow_type=flow_type,
        runnable=runnable,
        content_hash=content_hash,
        unsupported_mock_operators=tuple(dict.fromkeys(unsupported_mock_operators)),
    )


def _budget_cost_cap(spec: TemplateSpec):
    """Per-investigation cost ceiling from ``budget.max_cost_usd`` (or None)."""
    budget = getattr(spec, "budget", None)
    return getattr(budget, "max_cost_usd", None) if budget else None


def _validate_schema_refs(spec: TemplateSpec, path: Path) -> None:
    """Eagerly resolve every schema reference declared on this template.

    Catches typos, missing imports, and wrong-type targets at compile time
    rather than letting them surface at run time (or worse, silently work
    as untyped state). Each error is re-raised with the template path
    prepended so authors get a single-line "where + why".
    """
    refs: list[tuple[str, str | None]] = [
        ("input_schema", getattr(spec, "input_schema", None)),
        ("output_schema", getattr(spec, "output_schema", None)),
    ]
    # ActionNodeDef.output_schema has existed since the original schema
    # design but the compiler never read it. Now it does — same eager-resolve
    # contract as the template-level fields.
    if isinstance(spec.flow, StateMachineFlow):
        for node_name, node_def in spec.flow.nodes.items():
            refs.append((f"nodes.{node_name}.output_schema", node_def.output_schema))

    for site, dotted in refs:
        if dotted is None:
            continue
        try:
            resolve_schema(dotted)
        except SchemaResolutionError as e:
            raise SchemaResolutionError(
                f"{path}: {site} — {e}"
            ) from e


def _validate_subgraph_mapping(bt: SubgraphBTNode, sub_spec: TemplateSpec) -> None:
    """Phase 2b — pin the parent's inputs/outputs mapping to the child's
    typed contract at compile time.

    Three rules, applied symmetrically to inputs ↔ ``input_schema`` and
    outputs ↔ ``output_schema``:

    1. **Schema declared, mapping missing.** The child template has signed
       a contract; the parent embedding it as a subgraph must honor it.
       Backward-compatible only when neither side declares anything (then
       the legacy hardcoded behavior fires at runtime).

    2. **Every required field covered.** A mapping that drops a non-default
       field on the child's ``Input`` schema would crash at runtime when
       Pydantic refuses to construct the model. We catch it now.

    3. **No unknown fields.** A mapping key that names a field the child
       doesn't declare is almost certainly a typo (``symptm`` for
       ``symptom``). Loud, immediate failure beats a silent miss at runtime.

    Errors name both the subgraph site (``subgraph_node template=X``) and
    the offending field, so authors get a single-line diagnostic.
    """
    site = f"subgraph_node template={bt.template!r}"

    # Inputs side — strictest when the child declares input_schema.
    in_schema = resolve_schema(sub_spec.input_schema)
    if in_schema is not None:
        if bt.inputs is None:
            raise SchemaResolutionError(
                f"{site}: child template declares input_schema "
                f"{sub_spec.input_schema!r}; parent must provide an "
                f"inputs: mapping covering its required fields"
            )
        mapped = set(bt.inputs.keys())
        required = required_fields(in_schema)
        known = schema_fields(in_schema)
        missing = required - mapped
        if missing:
            raise SchemaResolutionError(
                f"{site}: inputs: mapping omits required field(s) "
                f"{sorted(missing)} on {sub_spec.input_schema!r}"
            )
        unknown = mapped - known
        if unknown:
            raise SchemaResolutionError(
                f"{site}: inputs: mapping references unknown field(s) "
                f"{sorted(unknown)} (not declared on {sub_spec.input_schema!r})"
            )

    # Outputs side — same rules, mirror-image. Today only required fields
    # are pinned; we don't reject extra outputs the child writes that the
    # parent declines to capture (those are silently dropped by the merge).
    out_schema = resolve_schema(sub_spec.output_schema)
    if out_schema is not None:
        if bt.outputs is None:
            raise SchemaResolutionError(
                f"{site}: child template declares output_schema "
                f"{sub_spec.output_schema!r}; parent must provide an "
                f"outputs: mapping covering its required fields"
            )
        mapped = set(bt.outputs.keys())
        required = required_fields(out_schema)
        known = schema_fields(out_schema)
        missing = required - mapped
        if missing:
            raise SchemaResolutionError(
                f"{site}: outputs: mapping omits required field(s) "
                f"{sorted(missing)} on {sub_spec.output_schema!r}"
            )
        unknown = mapped - known
        if unknown:
            raise SchemaResolutionError(
                f"{site}: outputs: mapping references unknown field(s) "
                f"{sorted(unknown)} (not declared on {sub_spec.output_schema!r})"
            )


# ─── Builder 1: prompt (single LLM call) ────────────────────────────


def _build_prompt(spec: TemplateSpec, prose: str):
    """Build a single-LLM-call runnable for flow.type: prompt."""
    flow = spec.flow
    assert isinstance(flow, PromptFlow)

    llm = build_chat_model(
        flow.model,
        temperature=flow.temperature,
        max_tokens=flow.max_tokens,
        max_cost_usd=_budget_cost_cap(spec),
    )

    knowledge_roles = list(getattr(flow, "knowledge", None) or [])

    async def run(input_data: dict[str, Any]) -> dict[str, Any]:
        user_message = _format_user_input(input_data)
        system = prose
        citations: list[dict] = []
        if knowledge_roles:
            # Ambient injection (ADR-0061 model C, #509).
            ktext, cits = await compose_ambient_knowledge(knowledge_roles, user_message)
            if ktext:
                system = f"{prose}\n\n{ktext}"
            citations = [_knowledge_evidence(c) for c in cits]
        messages = [
            SystemMessage(content=system),
            HumanMessage(content=user_message),
        ]
        response = await llm.ainvoke(messages)
        out: dict[str, Any] = {"output": response.content, "type": "prompt"}
        if citations:
            out["knowledge_citations"] = citations
        return out

    return run



# ─── Builder 3: state_machine (behavior tree → LangGraph) ───────────


# Shared state schema for state_machine templates.
# All fields that may be written by multiple parallel branches need reducers.
def _last_writer_wins(_a: str, b: str) -> str:
    return b


def _merge_dicts(a: dict, b: dict) -> dict:
    return {**a, **b}


# Base channels every state_machine template gets. Built as a dict of
# *evaluated* type objects (not class-body annotations, which PEP 563
# stringifies in this module) so the per-template dynamic schema below can
# extend it with types LangGraph can introspect directly.
#
#   evidence / findings / map_results — reducers so parallel branches merge.
#   map_results — fan-out/fan-in accumulator; join filters by map_id.
#   _map_item — transient per-branch Send payload; never merged back.
_BASE_STATE_ANNOTATIONS: dict[str, Any] = {
    "symptom": str,
    "user_message": str,
    # The run identity, seeded once from input_data (the runner injects it at
    # runner.py:326; the Temporal workflow at workflow.py). It MUST be a declared
    # channel — LangGraph silently drops undeclared input keys, so without this a
    # node's ``state.get("run_id")`` is None and the durable remote-job record id
    # collapses to ``p?:node``, colliding across runs (#564). Templates rarely
    # ``$state.run_id`` it, so _collect_state_keys wouldn't declare it otherwise.
    "run_id": str,
    "evidence": Annotated[list[dict], operator.add],
    "findings": Annotated[dict, _merge_dicts],
    "final_output": Annotated[str, _last_writer_wins],
    "last_action_status": Annotated[str, _last_writer_wins],
    "map_results": Annotated[list[dict], operator.add],
    "_map_item": Annotated[Any, _last_writer_wins],
    # The verbatim template input, seeded once at run start and never
    # written by nodes. Operation bindings resolve ``$input.<field>``
    # from here (resolve_simple_binding falls back to
    # state["template_input"]) — without this channel, $input refs in
    # operation YAMLs silently resolved against {} (D2.D parking bug #2).
    "template_input": dict,
    # ADR-0068 (#742) — the active profile's per-operator config, seeded once at
    # run start (never written by nodes). `invoke_operator_inprocess` reads
    # state["operator_config"][<op>] and deep-merges it under the bound input for
    # operators that declare a `config_schema`. Empty when no profile / no config.
    "operator_config": dict,
    # ADR-0070 (#748) — the saved per-workflow OPERATIONAL config, seeded once at
    # run start from the reserved ``__workflow_config__`` input key the dispatcher
    # injects. Read by the operator runtime as the WORKFLOW layer (run > workflow >
    # env). Empty when no workflow config was saved for this template.
    "workflow_config": dict,
    # #2241 C — the leg's recorded contract invocations by node path, dispatcher-seeded
    # from the record and stamped with the leg (`__leg__`); absent = legacy (dev loop)
    "contract_invocations": dict,
}

# The graph itself uses the dynamic per-template extension built in
# _build_state_machine; this named base type exists for callers/tests.
#
# WARNING (D2.D parking bug #1, the deep one): node functions must be
# annotated ``state: dict`` — NEVER with this TypedDict. LangGraph infers
# a per-node *input schema* from the first parameter's annotation and
# FILTERS the state it passes to the node down to that schema's keys.
# Annotating nodes with the 8-key base type silently hid every dynamic
# channel (subgraph outputs, python_node outputs, operation $state
# targets) from every node, while the graph's own state kept them — the
# final result looked right but no node could read a dynamic key.
IncidentMachineState = TypedDict(  # type: ignore[misc]
    "IncidentMachineState", _BASE_STATE_ANNOTATIONS, total=False,
)


def _load_operator_config() -> dict[str, Any]:
    """The active profile's per-operator config (ADR-0068), best-effort.

    Reads ``CAIRN_PROFILE`` via the same loader packs/doctor use. A malformed or
    missing profile must never break a run — config is additive — so any load
    error degrades to ``{}`` (operators fall back to their declared defaults).
    """
    try:
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415 — avoid import cycle at module load

        return dict(load_profile_from_env().config)
    except Exception:  # noqa: BLE001 — config is additive; never fail a run on it
        return {}


def _node_contract(state: dict[str, Any], node_ref: str, op_cls: Any) -> "ContractEnvelope | None":
    """#2241 C (D3) — this node's recorded contract. Inside an admitted leg (the channel
    carries the ``__leg__`` stamp) an opted-in operator without its entry is lost
    provenance, not legacy: refuse. An unstamped channel (the dev loop, no record) is legacy."""
    channel = state.get("contract_invocations") or {}
    data = channel.get(node_ref)  # exact: a flattened child's node carries its own prefix
    if data:
        try:
            return ContractEnvelope.from_dict(data)
        except (KeyError, TypeError, ValueError) as e:  # a malformed recorded entry is a refusal, never a finding
            raise ContractConformanceError(f"operation node {node_ref!r}: malformed recorded contract entry: {e}") from e
    if "__leg__" in channel and getattr(op_cls, "implements", ()):
        raise ContractConformanceError(
            f"operation node {node_ref!r} runs opted-in operator {op_cls.name!r} but leg "
            f"{channel['__leg__']!r} was admitted with no entry for it; refusing to run unrecorded")
    return None


def _child_contracts(state: dict[str, Any], sub_ref: str) -> dict[str, Any]:
    """The parent channel's entries under ``sub_ref/``, re-keyed for the child graph."""
    channel = state.get("contract_invocations") or {}
    child = {k[len(sub_ref) + 1:]: v for k, v in channel.items() if k.startswith(sub_ref + "/")}
    if "__leg__" in channel:
        child["__leg__"] = channel["__leg__"]
    if "__stages__" in channel:   # ADR-0118 — the stage entries, re-keyed the same way
        stages = channel["__stages__"]
        child["__stages__"] = {**stages, "nodes": {k[len(sub_ref) + 1:]: v for k, v in stages["nodes"].items()
                                                   if k.startswith(sub_ref + "/")}}
    return child


def _node_dataflow_contracts(state: dict[str, Any], node_ref: str) -> "dict[str, Any] | None":
    """ADR-0118 D4 — a ``dataflow/run``/``ml/run`` node's carrier: the admitted leg's stamp
    plus this node's stage contracts by stage index, for ``OperationContext.dataflow_contracts``.
    ``None`` outside an admitted leg (the dev loop, no record): the spec goes out legacy."""
    channel = state.get("contract_invocations") or {}
    if "__leg__" not in channel:
        return None
    from cairn.core.dataflow.conformance_check import node_stage_contracts  # noqa: PLC0415 — dataflow is an optional consumer of the compiler

    return node_stage_contracts(channel.get("__stages__") or {"leg": channel["__leg__"], "nodes": {}}, node_ref)


def seed_initial_state(input_data: dict[str, Any]) -> dict[str, Any]:
    """Shape raw template input into the graph's initial state.

    Seeds the base channels every state-machine graph expects, crucially
    ``template_input`` (the ``$input.*`` source for operation bindings — see
    the channel comment in ``_BASE_STATE_ANNOTATIONS``). Extra typed inputs
    (Phase 2b: a parent ``subgraph_node`` passing ``inputs: {...}``) ride along
    as top-level keys so nodes can read them via ``$state.<name>``.

    Used by the in-process ``run`` path AND by low-level harnesses that submit
    to a runtime directly (``scripts/temporal_smoke.py``) — so the shaping has a
    single source of truth and a direct ``runtime.invoke`` can't silently skip
    the ``template_input`` channel.
    """
    # ADR-0070 (#748) — the dispatcher injects the saved per-workflow config under
    # a reserved key; lift it into its own channel and keep it out of
    # template_input so it never pollutes $input.* resolution.
    raw = dict(input_data)
    wf_cfg = raw.pop("__workflow_config__", None) or {}
    contract_invocations = raw.pop("__contract_invocations__", None) or {}
    initial_state: dict[str, Any] = {
        "symptom": raw.get("symptom", ""),
        "user_message": _format_user_input(raw),
        "evidence": [],
        "findings": {},
        "map_results": [],
        "last_action_status": "pending",
        "template_input": raw,
        # ADR-0068 (#742) — seed the active profile's per-operator config so the
        # operator runtime can layer it under bound inputs. Cheap when CAIRN_PROFILE
        # is unset (load_profile_from_env returns an empty Profile, no file read).
        "operator_config": _load_operator_config(),
        "workflow_config": wf_cfg,
        "contract_invocations": contract_invocations,
    }
    for k, v in raw.items():
        if k not in initial_state:
            initial_state[k] = v
    return initial_state


def _binding_state_keys(binding: "OperationBinding") -> set[str]:
    """Top-level ``$state.*`` keys a binding reads or writes — extracted with
    the ONE expression language's parser (#1118), replacing the regex over a
    JSON blob. Same coverage (refs at any nesting depth, incl. inside
    ``$default(...)``) but no false positives from literals that merely
    contain the text ``$state.``."""
    keys: set[str] = set()

    def visit(v: object) -> None:
        if isinstance(v, dict):
            for x in v.values():
                visit(x)
        elif isinstance(v, list):
            for x in v:
                visit(x)
        elif isinstance(v, str) and v.startswith("$"):
            try:
                refs = compile_expr(v, source="channel scan").references()
            except ExprError:
                return  # malformed binding fails loud later, on its own path
            for scope, parts in refs:
                if scope == "state" and parts:
                    keys.add(parts[0])

    visit(binding.input)
    visit(binding.output)
    return keys


def _collect_state_keys(
    spec: TemplateSpec,
    root: BTNode,
    operations_dir: Path | None,
) -> set[str]:
    """Every top-level state key this template can read or write (§24.22).

    LangGraph persists updates only to *declared* channels — anything else
    is silently dropped, which was the subgraph_node propagation bug: a
    parent mapping child outputs to ``rca_*`` keys watched LangGraph throw
    the merge away. Instead of one fixed schema, the compiler derives the
    channel set from the template AST:

      - template ``input_schema`` fields        (seeded into initial state)
      - subgraph_node ``inputs``/``outputs``    (parent-side keys)
      - python_node declared ``outputs``
      - operation binding ``$state.*`` refs     (inputs + output targets)
      - map ``over`` source key
    """
    keys: set[str] = set()

    in_schema = resolve_schema(getattr(spec, "input_schema", None))
    if in_schema is not None:
        keys |= set(schema_fields(in_schema))

    def walk(bt: BTNode) -> None:
        if isinstance(bt, SubgraphBTNode):
            if bt.inputs:
                keys.update(bt.inputs.values())
            if bt.outputs:
                keys.update(bt.outputs.values())
            # #569/#581: a flattened subgraph splices the child's nodes into THIS
            # graph, so the child's own state keys must be declared here too or
            # LangGraph drops them. Recurse into the child template (best-effort).
            if _flatten_composition_enabled():
                try:
                    import frontmatter  # noqa: PLC0415

                    from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415

                    csp = resolve_template_path(bt.template)
                    cspec = TemplateSpec.model_validate(frontmatter.load(csp).metadata)
                    if isinstance(cspec.flow, StateMachineFlow):
                        keys.update(
                            _collect_state_keys(
                                cspec, cspec.flow.root, csp.parent.parent / "operations"
                            )
                        )
                except Exception:  # noqa: BLE001 — flatten guards this path too
                    pass
            return
        if isinstance(bt, PythonBTNode):
            keys.update(bt.outputs)
            return
        if isinstance(bt, OperationBTNode):
            inline = bt.inline_binding()  # #1107: binding may live ON the node
            if inline is not None:
                # 2b: elaborate best-effort so producer-named outputs
                # ($state.<id>.<f>) get their channels declared — LangGraph
                # silently drops undeclared keys. Unresolvable operator →
                # raw map; the compile branch fails loud on it later.
                if not bt.input or not bt.output:
                    try:
                        inline = resolve_capability_binding(inline, f"inline {bt.node_ref!r}")  # #2241 C (D5)
                        _cls = default_operator_registry.get(inline.operator)
                        inline = elaborate_binding(
                            inline, _cls, node_ref=bt.node_ref,
                            auto_input=not bt.input, auto_output=not bt.output,
                        )
                    except Exception:  # noqa: BLE001 — best-effort scan
                        pass
                keys.update(_binding_state_keys(inline))
                return
            if operations_dir is None:
                return  # the OperationBTNode branch fails loud later
            binding_path = operations_dir / f"{bt.ref}.yaml"
            if binding_path.exists():
                binding = load_binding(binding_path)
                keys.update(_binding_state_keys(binding))
            return
        if isinstance(bt, MapBTNode):
            if bt.over:
                keys.add(bt.over)
            walk(bt.body)
            return
        if isinstance(bt, GuardBTNode):
            walk(bt.then)
            if bt.else_ is not None:
                walk(bt.else_)
            return
        for child in getattr(bt, "children", None) or []:
            walk(child)

    walk(root)
    keys.discard("")
    return keys


def _build_state_machine(
    spec: TemplateSpec,
    prose: str,
    registry: CapabilityRegistry,
    runtime: Runtime | None = None,
    operations_dir: Path | None = None,
    source_path: Path | None = None,
    unsupported_mock_operators: list[str] | None = None,
):
    """Compile a behavior-tree state_machine to a LangGraph StateGraph.

    Supported BT primitives:
      - action: invokes an inner flow (prompt) per node definition
      - sequence: linear chain
      - parallel: static fan-out (fixed children) via Send(); join via reducers
      - selector / guard: ordered fallback / conditional branch
      - subgraph_node / python_node: nested template / deterministic escape hatch
      - map: dynamic fan-out — run a body sub-flow once per runtime list item,
             then fan-in via an aggregate function (map-reduce)
    """
    flow = spec.flow
    assert isinstance(flow, StateMachineFlow)

    # Runtime is the seam where Temporal swaps in. An explicit ``runtime``
    # argument always wins (tests, embedding callers); otherwise the env
    # knob ``CAIRN_RUNTIME`` is consulted lazily at run() time — see
    # _resolve_runtime — so compile stays env-independent.
    runtime_override = runtime

    # Resolve model pool: e.g. {"small": "gpt-4o-mini", "large": "gpt-4o"}
    model_pool: dict[str, str] = {}
    if spec.models:
        for role in ("router", "fast", "strong"):
            val = getattr(spec.models, role, None)
            if val:
                model_pool[role] = val
        # also pick up extras from extra='allow' on ModelConfig
        for k, v in spec.models.model_dump().items():
            if v and k not in model_pool:
                model_pool[k] = v

    section_map = _extract_sections(prose)

    # §24.22 follow-on — per-template state schema derived from the AST.
    # Base channels keep their reducers; every additional key the template
    # can touch gets a last-writer-wins channel so LangGraph persists it
    # (undeclared keys are silently dropped — the old fixed schema is how
    # subgraph_node outputs got lost).
    extra_keys = sorted(
        k for k in _collect_state_keys(spec, flow.root, operations_dir)
        if k not in _BASE_STATE_ANNOTATIONS
    )
    state_schema = TypedDict(  # type: ignore[misc]
        "MachineState",
        {
            **_BASE_STATE_ANNOTATIONS,
            **{k: Annotated[Any, _last_writer_wins] for k in extra_keys},
        },
        total=False,
    )
    graph = StateGraph(state_schema)

    # §24.3 P2.e — the pack that owns THIS template, so cross-pack subgraph
    # refs resolve under the calling pack's identity: the within-pack rule lets
    # a pack subgraph its own *unlisted* internal templates, while an
    # encapsulated pack's internals stay private to outside callers. None
    # (template outside any registered pack) → today's permissive resolution.
    from cairn.core.pack.registry import pack_owning_template
    from_pack = pack_owning_template(source_path) if source_path else None


    # Build all action nodes first, then wire edges
    factory = _ActionNodeFactory(
        nodes_map=flow.nodes,
        model_pool=model_pool,
        section_map=section_map,
        registry=registry,
        max_cost_usd=_budget_cost_cap(spec),
        operations_dir=operations_dir,
        from_pack=from_pack,
        unsupported_mock_operators=unsupported_mock_operators,
    )

    # Track ID counter for synthetic nodes (parallel dispatch/join etc.)
    counter = {"n": 0}

    def fresh(name: str) -> str:
        counter["n"] += 1
        return f"{name}__{counter['n']}"

    # First pass: add all action nodes to the graph (so we can edge to them by name).
    # We do this by walking the tree once and registering each ActionBTNode.
    _add_action_nodes(flow.root, graph, factory)

    # Second pass: build the connectivity and return entry/exit names.
    entry, exit_ = _compile_bt(flow.root, graph, factory, fresh)
    graph.add_edge(START, entry)
    graph.add_edge(exit_, END)

    # ADR-0034: a template with a gated operation compiles WITH a checkpointer
    # so the approval interrupt can durably pause + resume. Ungated templates
    # keep the bare one-shot compile (byte-identical to before).
    if factory.has_gate:
        from cairn.core.runtime.checkpointer import get_checkpointer

        compiled_graph = graph.compile(checkpointer=get_checkpointer())
    else:
        compiled_graph = graph.compile()


    async def run(input_data: dict[str, Any]) -> dict[str, Any]:
        initial_state = seed_initial_state(input_data)
        active_runtime = runtime_override or _resolve_runtime(spec.name)
        result = await active_runtime.invoke(
            compiled_graph,
            dict(initial_state),
            # thread_id for a checkpointed (gated) graph (ADR-0034); ignored when
            # the graph has no checkpointer. Stable per run so resume targets it.
            run_id=str(initial_state.get("run_id") or spec.name),
            template_name=spec.name,
            template_sha=_template_sha(source_path),
        )
        # Phase 2b — child returns the full final state so a parent
        # ``subgraph_node`` with explicit ``outputs:`` mapping can pluck
        # any top-level state key. The curated meta-keys (output,
        # evidence, findings, map_results, type) coexist with raw state
        # keys; readers that asked by name keep working.
        out: dict[str, Any] = dict(result)
        out.update({
            "output": result.get("final_output", ""),
            "evidence": result.get("evidence", []),
            "findings": result.get("findings", {}),
            "map_results": result.get("map_results", []),
            "type": "state_machine",
        })
        return out

    # Temporal Step 3 — worker-side surface. The Temporal worker registers
    # the *builder* StateGraph with the LangGraph plugin (it re-compiles
    # under the plugin's activity wrappers) and the generic run_node
    # activity dispatches to individual node fns by name.
    run._state_graph = graph                      # type: ignore[attr-defined]
    run._compiled_graph = compiled_graph           # type: ignore[attr-defined]
    run._node_fns = _node_fns_from(graph)          # type: ignore[attr-defined]
    # #104: expose the gate flag so a PARENT embedding this template via
    # subgraph_node can propagate it (and thus compile WITH a checkpointer, so
    # the child's interrupt() pauses the parent run rather than being swallowed).
    run._has_gate = factory.has_gate               # type: ignore[attr-defined]
    return run


def _noop_node():
    """Fresh async no-op for graph glue (dispatch/join/entry/exit nodes).

    Async on purpose: Temporal's workflow sandbox event loop does not
    implement ``run_in_executor``, which langgraph uses to invoke *sync*
    node callables from async context — a sync ``lambda s: {}`` node
    deadlocks the workflow with NotImplementedError. Native-async glue
    runs identically in-process and inside the sandbox."""
    async def _noop(state: dict) -> dict:
        return {}
    return _noop


def _node_fns_from(graph: StateGraph) -> dict[str, Any]:
    """``node_name → original callable`` for every node on a builder graph.

    LangGraph coerces ``add_node`` callables into RunnableCallable specs;
    unwrap back to the closure the compiler registered so the Temporal
    ``run_node`` activity can call it directly with a state dict."""
    out: dict[str, Any] = {}
    for name, spec_node in graph.nodes.items():
        runnable = getattr(spec_node, "runnable", spec_node)
        fn = (
            getattr(runnable, "afunc", None)
            or getattr(runnable, "func", None)
            or runnable
        )
        out[name] = fn
    return out


def _template_sha(source_path: Path | None) -> str:
    """File-bytes sha the Temporal worker registry keys on. Empty when the
    template wasn't compiled from a file (in-memory tests) — the in-process
    runtime ignores it, and the Temporal runtime fails loud on submit."""
    if source_path is None:
        return ""
    from cairn.core.runtime.temporal.registry import sha_for
    return sha_for(source_path)


def _resolve_runtime(template_name: str) -> Runtime:
    """Pick the runtime from ``CAIRN_RUNTIME`` at run() time.

    ``inprocess`` (default) → today's path. ``temporal`` → submit as a durable
    workflow. The old submit-time gate here rejected ``agent_loop`` nodes,
    which Temporal's deterministic-replay sandbox could not host; ADR-0084 D3
    removed that node type from the schema entirely (#1153), so every action
    node is now a single ``prompt`` call and the gate has nothing to reject."""
    from cairn.core import settings

    rt = settings.runtime()
    if rt == "dbos":
        # Single-node durable runtime (ADR-0035): the run executes in this
        # process under a DBOS workflow; crash-recovery + HITL come from DBOS,
        from cairn.core.runtime.dbos.runtime import DBOSRuntime
        return DBOSRuntime()
    if rt != "temporal":
        return InProcessRuntime()
    from cairn.core.runtime.temporal.runtime import TemporalRuntime
    return TemporalRuntime()


def _in_temporal_activity() -> bool:
    """True when this code is executing inside a Temporal activity right now (#685).

    A subgraph node is tagged ``TOOL`` → it runs as the generic ``run_node``
    Temporal activity. Its (ungated) child must then run IN-PROCESS inside that
    activity: calling ``sub_compiled.run()`` would pick the ambient
    ``TemporalRuntime`` and start a child workflow with ``id=run_id`` — the
    PARENT's run_id, threaded into the child — which Temporal rejects
    (``WorkflowAlreadyStartedError``), retries forever, and the composed run
    hangs at ``running``. ``temporalio`` is an optional extra; when it's absent
    (in-process deploys) or we're not in an activity, this is False and the
    byte-identical ``run()`` path is used.
    """
    try:
        from temporalio import activity  # optional [runtime-temporal] extra
        return activity.in_activity()
    except Exception:  # noqa: BLE001 — no SDK / no activity context → in-process
        return False


def _in_dbos_workflow() -> bool:
    """True when this code is executing inside a DBOS workflow right now (#1864).

    The DBOS twin of :func:`_in_temporal_activity`, for the same subgraph
    branch. Under ``CAIRN_RUNTIME=dbos`` the WHOLE graph runs inside
    ``run_template`` — one workflow per run, id=run_id — so a subgraph node's
    ``sub_compiled.run()`` would ask ``DBOSRuntime`` to start another workflow
    with the parent's id, which DBOS deduplicates to the running parent; the
    node then awaits its own workflow's result. ``DBOS.workflow_id`` is the
    SDK's own "current workflow" primitive: None outside a workflow, no launch
    required. ``dbos`` is an optional extra; absent → not DBOS.
    """
    try:
        from dbos import DBOS  # optional [runtime-dbos] extra
        return DBOS.workflow_id is not None
    except Exception:  # noqa: BLE001 — no SDK / no workflow context → not DBOS
        return False


# ─── State machine helpers ──────────────────────────────────────────


class _ActionNodeFactory:
    """Builds and caches LangGraph node functions for state_machine action refs."""

    def __init__(
        self,
        nodes_map: dict[str, ActionNodeDef],
        model_pool: dict[str, str],
        section_map: dict[str, str],
        registry: CapabilityRegistry,
        max_cost_usd: float | None = None,
        operations_dir: Path | None = None,
        from_pack: str | None = None,
        contract_prefix: str = "",
        unsupported_mock_operators: list[str] | None = None,
    ):
        self.nodes_map = nodes_map
        # #2241 C — a flattened child's nodes keep their bare refs in the parent graph;
        # this prefix (`<child template>/`, nested as needed) is what the launch door
        # recorded them under, so lookups stay exact and legacy nodes never match.
        self.contract_prefix = contract_prefix
        self.model_pool = model_pool
        self.section_map = section_map
        self.registry = registry
        self.max_cost_usd = max_cost_usd
        # Where OperationBTNode looks up its <ref>.yaml binding files.
        # When None, OperationBTNode compile fails loud — the pack didn't
        # declare an operations/ dir.
        self.operations_dir = operations_dir
        # §24.3 P2.e — the pack that owns the template being compiled. Threaded
        # to resolve_template_path so subgraph refs resolve under the calling
        # pack's identity (within-pack rule + cross-pack encapsulation). None
        # (templates outside any pack) → permissive resolution.
        self.from_pack = from_pack
        self.unsupported_mock_operators: list[str] = (
            unsupported_mock_operators if unsupported_mock_operators is not None else []
        )
        self._built: dict[str, Any] = {}
        # Set True when any OperationBTNode is gateable at compile time
        # (operator requires_approval, or its binding declares approval.required),
        # so compile_template attaches a checkpointer for durable pause/resume
        # (ADR-0034). Stays False for ungated templates → bare compile, no change.
        self.has_gate: bool = False

    def build(self, ref: str):
        if ref in self._built:
            return self._built[ref]
        if ref not in self.nodes_map:
            raise KeyError(f"Action ref '{ref}' not found in flow.nodes")

        node_def = self.nodes_map[ref]
        model_name = self.model_pool.get(node_def.model, node_def.model)
        prompt_text = self._resolve_prompt(ref, node_def)

        node_fn = self._build_prompt_node(prompt_text, model_name, ref, node_def)
        self._built[ref] = node_fn
        return node_fn

    def _resolve_prompt(self, ref: str, node_def: ActionNodeDef) -> str:
        section_name = node_def.prompt_section or ref
        section = self.section_map.get(section_name) or self.section_map.get(ref)
        if section:
            return section
        # Fallback: a generic instruction
        return f"You are the '{ref}' node in a multi-step investigation."

    def _build_prompt_node(self, prompt: str, model_name: str, ref: str, node_def=None):
        llm = build_chat_model(model_name, temperature=0.0, max_cost_usd=self.max_cost_usd)
        knowledge_roles = list(getattr(node_def, "knowledge", None) or [])

        async def node_fn(state: dict) -> dict:
            user_text = _build_node_user_text(state, ref)
            system = prompt
            knowledge_evidence: list[dict] = []
            if knowledge_roles:
                # Ambient injection (ADR-0061 model C, #509): ground the step in
                # the bound corpora for its declared roles before the LLM sees it.
                ktext, cits = await compose_ambient_knowledge(knowledge_roles, user_text)
                if ktext:
                    system = f"{prompt}\n\n{ktext}"
                knowledge_evidence = [_knowledge_evidence(c) for c in cits]
            messages = [
                SystemMessage(content=system),
                HumanMessage(content=user_text),
            ]
            response = await llm.ainvoke(messages)
            content = response.content if hasattr(response, "content") else str(response)
            out: dict = {
                "findings": {ref: content},
                "final_output": content,  # the last prompt node's output is the final
                "last_action_status": "success",
            }
            if knowledge_evidence:
                out["evidence"] = knowledge_evidence
            return out

        return node_fn


    def build_invoker(self, ref: str) -> Callable[[str], Any]:
        """Return ``async fn(user_text: str) -> str`` that runs an action node's
        inner flow once and returns its raw content.

        Unlike :meth:`build`, this does *not* return a LangGraph node that writes
        ``findings`` — it's used by ``map`` bodies, which call the invoker once
        per item and capture each output into ``map_results`` themselves (so
        concurrent items don't collide on a single ``findings[ref]`` key).
        """
        if ref not in self.nodes_map:
            raise KeyError(f"Action ref '{ref}' not found in flow.nodes")
        node_def = self.nodes_map[ref]
        model_name = self.model_pool.get(node_def.model, node_def.model)
        prompt_text = self._resolve_prompt(ref, node_def)

        llm = build_chat_model(model_name, temperature=0.0, max_cost_usd=self.max_cost_usd)

        async def invoke_prompt(user_text: str) -> str:
            resp = await llm.ainvoke(
                [SystemMessage(content=prompt_text), HumanMessage(content=user_text)]
            )
            return resp.content if hasattr(resp, "content") else str(resp)

        return invoke_prompt


def _resolve_node_retry(node_def: ActionNodeDef) -> RetryPolicy:
    """Resolve effective retry policy: per-template override over LLM default."""
    if node_def.retry is None:
        return resolve_retry(NodeKind.LLM, None)
    override = RuntimeRetrySpec(
        maximum_attempts=node_def.retry.maximum_attempts,
        initial_interval_seconds=node_def.retry.initial_interval_seconds,
        backoff_coefficient=node_def.retry.backoff_coefficient,
        maximum_interval_seconds=node_def.retry.maximum_interval_seconds,
        start_to_close_timeout_seconds=node_def.retry.start_to_close_timeout_seconds,
    )
    return resolve_retry(NodeKind.LLM, override)


def _add_action_nodes(bt: BTNode, graph: StateGraph, factory: _ActionNodeFactory) -> None:
    """Pre-walk the BT and add all action nodes to the graph by their ref name."""
    if isinstance(bt, ActionBTNode):
        if bt.ref not in graph.nodes:
            node_def = factory.nodes_map[bt.ref]
            graph.add_node(
                bt.ref,
                tag(
                    factory.build(bt.ref),
                    NodeKind.LLM,
                    retry=_resolve_node_retry(node_def),
                ),
            )
    elif isinstance(bt, (SequenceBTNode, ParallelBTNode, SelectorBTNode)):
        for child in bt.children:
            _add_action_nodes(child, graph, factory)
    elif isinstance(bt, GuardBTNode):
        _add_action_nodes(bt.then, graph, factory)
        if bt.else_ is not None:
            _add_action_nodes(bt.else_, graph, factory)
    # SubgraphBTNode and PythonBTNode don't contain action refs to pre-register


def _flatten_composition_enabled() -> bool:
    """Whether a GATED subgraph is flattened into top-level nodes (#569/#581).

    Under ``CAIRN_RUNTIME=temporal`` this is now the DEFAULT (#1558): the nested
    ``subgraph_node`` runs as ONE activity, and an approval ``interrupt()``
    inside it cannot resume — the activity re-runs from scratch on approval
    (its in-process checkpoint died with the first invocation), hits the same
    gate, and re-surfaces the same approval forever. Observed live 2026-08-22
    on the prod box: history showed the subgraph activity + surface_approval
    pair repeating identically after a delivered signal. Flattening puts the
    gate at the top level, where the workflow's signal loop can actually
    deliver ``Command(resume=…)`` — the exact fix this emitter was built for,
    gated behind the in-process equivalence suite
    (``test_flatten_equivalence.py``), which passes.

    In-process the default stays OFF — the nested path works there (the parent
    checkpointer survives across the pause) and stays byte-identical.

    ``CAIRN_FLATTEN_COMPOSITION`` still overrides in BOTH directions: ``1`` turns
    flatten on anywhere, ``0`` forces the nested path even under temporal (the
    escape hatch if a template surfaces a flatten bug — but note that gated
    subgraphs then cannot resume under temporal).
    """
    import os  # noqa: PLC0415

    raw = getenv("CAIRN_FLATTEN_COMPOSITION", "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    from cairn.core import settings as _settings  # noqa: PLC0415 — cycle-safe, tiny

    return _settings.runtime() == "temporal"


def _flatten_subgraph(
    bt: Any, graph: StateGraph, factory: _ActionNodeFactory, fresh: Any,
    sub_compiled: Any, sub_path: Path,
    in_map: Optional[dict], out_map: Optional[dict],
) -> tuple[str, str]:
    """Inline a child template's nodes as TOP-LEVEL parent nodes (#569 / #581).

    Instead of running the child via one nested ``ainvoke`` inside a single
    activity (which sandboxes a child gate's interrupt under Temporal), splice the
    child's BT into the PARENT graph: its LLM/IO nodes stay their own activities,
    and its gate becomes a top-level node whose ``interrupt()`` bubbles to the
    workflow signal loop. Wrapped by an entry-adapter (scope the child's inputs
    into ``template_input``) and an exit-adapter (map the child's outputs up)."""
    from cairn.core.engine.flatten import (  # noqa: PLC0415 - sibling module
        flatten_entry_update, flatten_exit_update, namespace_child_bt,
    )
    from cairn.core.pack.registry import pack_owning_template  # noqa: PLC0415

    child_flow = sub_compiled.spec.flow
    prefix = fresh("flat_" + bt.template.replace("/", "_").replace("-", "_"))

    # Namespace the child's ACTION refs so `synthesize` etc. don't collide with
    # the parent's (operation nodes are already uniquely named via fresh()).
    child_root, renamed_nodes = namespace_child_bt(
        child_flow.root, dict(child_flow.nodes), prefix,
    )

    # Rebuild the child's model_pool + section_map from its spec + prose
    # (mirrors _build_state_machine) so its action nodes resolve correctly.
    cspec = sub_compiled.spec
    child_model_pool: dict[str, str] = {}
    if cspec.models:
        for role in ("router", "fast", "strong"):
            v = getattr(cspec.models, role, None)
            if v:
                child_model_pool[role] = v
        for k, v in cspec.models.model_dump().items():
            if v and k not in child_model_pool:
                child_model_pool[k] = v
    child_section_map = _extract_sections(sub_compiled.prose)

    child_factory = _ActionNodeFactory(
        nodes_map=renamed_nodes,
        model_pool=child_model_pool,
        section_map=child_section_map,
        registry=factory.registry,
        max_cost_usd=factory.max_cost_usd,
        contract_prefix=factory.contract_prefix + bt.template + "/",  # #2241 C (D2)
        operations_dir=sub_path.parent.parent / "operations",
        from_pack=pack_owning_template(sub_path),
    )

    # Splice the child's nodes into the PARENT graph.
    _add_action_nodes(child_root, graph, child_factory)
    child_entry, child_exit = _compile_bt(child_root, graph, child_factory, fresh)
    # The child may raise the parent's has_gate (a nested operation gate).
    factory.has_gate = factory.has_gate or child_factory.has_gate

    async def _entry(state: dict) -> dict:
        return flatten_entry_update(state, in_map)

    async def _exit(state: dict) -> dict:
        return flatten_exit_update(state, out_map)

    entry_name = fresh(prefix + "_enter")
    exit_name = fresh(prefix + "_exit")
    graph.add_node(entry_name, tag(_entry, NodeKind.PURE))
    graph.add_node(exit_name, tag(_exit, NodeKind.PURE))
    graph.add_edge(entry_name, child_entry)
    graph.add_edge(child_exit, exit_name)
    return entry_name, exit_name


def _warn_cross_pack(child_ref: str, from_pack: str | None) -> None:
    """ADR-0084 D5 — a pack composes only its OWN executables.

    A WARNING, not an error, until #1148 migrates the three known offenders
    (training's finetune / finetune-remote / composed-hitl-smoke compose
    `dataset-readiness/check` for the verdict that gates their train→promote
    subtree). Erroring today would break shipped templates for a rule whose
    migration path is still an open decision.
    """
    owner, sep, _ = str(child_ref or "").partition("/")
    if sep and from_pack and owner != from_pack:
        warnings.warn(
            f"template in pack {from_pack!r} composes {child_ref!r} from pack "
            f"{owner!r}. Packs are SELF-SUFFICIENT (ADR-0084 D5) — this becomes a "
            f"compile error once #1148 lands. Compose your own pack's flow, or "
            f"share the logic as an OPERATOR (code is a shareable layer).",
            DeprecationWarning, stacklevel=2,
        )


def _compile_bt(bt: BTNode, graph: StateGraph, factory: _ActionNodeFactory, fresh) -> tuple[str, str]:
    """Recursively compile a behavior tree into the StateGraph.

    Returns the (entry_node_name, exit_node_name) for the produced subgraph region.
    Action nodes are already added by _add_action_nodes; this pass wires edges.
    """
    if isinstance(bt, ActionBTNode):
        return bt.ref, bt.ref

    if isinstance(bt, SequenceBTNode):
        if not bt.children:
            raise ValueError("Empty sequence")
        ranges = [_compile_bt(c, graph, factory, fresh) for c in bt.children]
        for i in range(len(ranges) - 1):
            graph.add_edge(ranges[i][1], ranges[i + 1][0])
        return ranges[0][0], ranges[-1][1]

    if isinstance(bt, ParallelBTNode):
        if not bt.children:
            raise ValueError("Empty parallel")
        child_ranges = [_compile_bt(c, graph, factory, fresh) for c in bt.children]

        dispatcher_name = fresh("parallel_dispatch")
        joiner_name = fresh("parallel_join")
        child_entries = [r[0] for r in child_ranges]

        def make_dispatcher(entries: list[str]):
            async def dispatch(state: dict):
                return [Send(entry, state) for entry in entries]
            return dispatch

        graph.add_node(dispatcher_name, tag(_noop_node(), NodeKind.PURE))
        graph.add_conditional_edges(
            dispatcher_name,
            make_dispatcher(child_entries),
            child_entries,
        )

        graph.add_node(joiner_name, tag(_noop_node(), NodeKind.PURE))
        for child_exit in [r[1] for r in child_ranges]:
            graph.add_edge(child_exit, joiner_name)

        return dispatcher_name, joiner_name

    if isinstance(bt, GuardBTNode):
        condition_fn = resolve_condition(bt.condition)
        then_entry, then_exit = _compile_bt(bt.then, graph, factory, fresh)

        guard_entry = fresh(f"guard_{bt.condition}")
        guard_exit = fresh(f"guard_{bt.condition}_exit")
        graph.add_node(guard_entry, tag(_noop_node(), NodeKind.PURE))
        graph.add_node(guard_exit, tag(_noop_node(), NodeKind.PURE))

        if bt.else_ is not None:
            else_entry, else_exit = _compile_bt(bt.else_, graph, factory, fresh)
            async def _route_else(s, fn=condition_fn):
                return fn(dict(s))
            graph.add_conditional_edges(
                guard_entry,
                _route_else,
                {"true": then_entry, "false": else_entry},
            )
            graph.add_edge(then_exit, guard_exit)
            graph.add_edge(else_exit, guard_exit)
        else:
            # No else branch: false routes directly to guard exit (skip)
            async def _route_skip(s, fn=condition_fn):
                return fn(dict(s))
            graph.add_conditional_edges(
                guard_entry,
                _route_skip,
                {"true": then_entry, "false": guard_exit},
            )
            graph.add_edge(then_exit, guard_exit)

        return guard_entry, guard_exit

    if isinstance(bt, SelectorBTNode):
        # Selector: try children in order; succeed on the first whose
        # `last_action_status` ends up "success" after running.
        if not bt.children:
            raise ValueError("Empty selector")
        child_ranges = [_compile_bt(c, graph, factory, fresh) for c in bt.children]

        sel_entry = fresh("selector_entry")
        sel_exit = fresh("selector_exit")
        graph.add_node(sel_entry, tag(_noop_node(), NodeKind.PURE))
        graph.add_node(sel_exit, tag(_noop_node(), NodeKind.PURE))

        graph.add_edge(sel_entry, child_ranges[0][0])

        # After each child, if it succeeded route to exit, otherwise try the next.
        for i, (_entry, exit_) in enumerate(child_ranges):
            is_last = i == len(child_ranges) - 1
            if is_last:
                graph.add_edge(exit_, sel_exit)
                continue
            next_entry = child_ranges[i + 1][0]

            def make_router(next_node: str):
                async def route(state):
                    return "exit" if state.get("last_action_status") == "success" else "next"
                return route

            graph.add_conditional_edges(
                exit_,
                make_router(next_entry),
                {"exit": sel_exit, "next": next_entry},
            )

        return sel_entry, sel_exit

    if isinstance(bt, SubgraphBTNode):
        _warn_cross_pack(bt.template, factory.from_pack)  # ADR-0084 D5 (#1132)
        # Compile the referenced template at this point. The sub-template's
        # CompiledTemplate.runnable is invoked as a single LangGraph node.
        from cairn.core.engine.skill_engine import resolve_template_path
        sub_path = resolve_template_path(bt.template, from_pack=factory.from_pack)
        sub_compiled = compile_template(sub_path, registry=factory.registry)
        factory.unsupported_mock_operators.extend(
            f"{bt.template}/{operator}"
            for operator in sub_compiled.unsupported_mock_operators
        )

        # Phase 2b — validate the parent's inputs/outputs mapping against the
        # child's declared schemas. Eager, at compile time. The validator
        # raises on any mismatch with a message that names both the parent's
        # subgraph site and the offending field.
        _validate_subgraph_mapping(bt, sub_compiled.spec)

        node_name = fresh(f"subgraph_{bt.template}")
        in_map = dict(bt.inputs) if bt.inputs else None
        out_map = dict(bt.outputs) if bt.outputs else None

        # #104 — HITL gates inside a subgraph. If the child declares a gate, its
        # interrupt() must pause the PARENT run (resumable under the parent
        # run_id), not a private nested invocation. Verified on langgraph 1.2.2:
        # a child run as a SEPARATE invoke with its OWN checkpointer/thread
        # swallows the interrupt; a child compiled WITHOUT its own checkpointer
        # and invoked inheriting the parent's config propagates it. So we (a)
        # propagate has_gate up so the parent compiles WITH a checkpointer, and
        # (b) embed a checkpointer-less copy of the child and ainvoke it with the
        # parent's config. Ungated children keep the byte-identical run() path.
        child_has_gate = bool(getattr(sub_compiled.runnable, "_has_gate", False))
        factory.has_gate = factory.has_gate or child_has_gate
        # #569/#581: a GATED cross-pack subgraph flattens to top-level nodes so its
        # gate interrupt reaches the Temporal signal loop (opt-in until proven
        # equivalent in-process; ungated subgraphs keep the byte-identical path).
        if child_has_gate and _flatten_composition_enabled():
            return _flatten_subgraph(
                bt, graph, factory, fresh, sub_compiled, sub_path, in_map, out_map,
            )
        embedded_child = (
            sub_compiled.runnable._state_graph.compile() if child_has_gate else None
        )

        async def subgraph_node_fn(state: dict, config: RunnableConfig = None) -> dict:
            # Build child input — explicit mapping wins; otherwise fall back
            # to the legacy hardcoded symptom/user_message shape so templates
            # authored before Phase 2b keep running unchanged.
            if in_map is not None:
                sub_input = {
                    child_field: state.get(parent_key, "")
                    for child_field, parent_key in in_map.items()
                }
            else:
                sub_input = {
                    "symptom": state.get("symptom", ""),
                    "user_message": state.get("user_message", ""),
                }
            # #569: thread the PARENT run identity into the child so a HITL gate
            # (and every audit/approval event) INSIDE the composed subgraph carries
            # the real run_id — not "" (the child seed default). The comment above
            # says the child "resumes under the parent run_id (#104)", but without
            # this the child's ApprovalRequest.run_id was empty. run_id is a base
            # channel (seed_initial_state carries any extra key through), so this
            # is enough for the child's operation_node_fn to read it.
            sub_input.setdefault("run_id", state.get("run_id", ""))
            # #2241 C (D2) — the child's share of the leg's recorded entries, re-keyed
            # under this subgraph node, with the leg stamp; nothing for a legacy run.
            # the key the door recorded, including any flattened ancestor prefix this node carries
            sub_input["__contract_invocations__"] = _child_contracts(state, factory.contract_prefix + bt.template)

            if embedded_child is not None:
                # Gated child: run it as part of THIS run — sharing the parent's
                # checkpointer + config — so its interrupt() pauses the parent
                # and resumes under the parent run_id (#104). Mirror run()'s
                # post-processing so the out_map mapping below is unchanged.
                raw = await embedded_child.ainvoke(seed_initial_state(sub_input), config)
                sub_result = dict(raw)
                sub_result.update({
                    "output": raw.get("final_output", ""),
                    "evidence": raw.get("evidence", []),
                    "findings": raw.get("findings", {}),
                    "map_results": raw.get("map_results", []),
                    "type": "state_machine",
                })
            elif _in_temporal_activity() or _in_dbos_workflow():
                # #685 / #1864: an UNGATED subgraph child, when the parent runs
                # under a DURABLE runtime, executes inside THIS node. Run it
                # in-process right here — ``sub_compiled.run()`` would re-dispatch
                # to the ambient runtime with id=run_id (the PARENT's, threaded
                # above). Temporal rejects that (WorkflowAlreadyStarted retry-storm
                # that never terminates the run, #685). DBOS treats the id as an
                # idempotency key: ``start_workflow`` hands back the RUNNING
                # PARENT's own handle and the node awaits it — a self-deadlock
                # that stays PENDING forever with no error and nothing for the
                # reconciler to repair (#1864, prod 2026-09-02). The child's nodes run
                # in-process within this one activity (same shape as the gated
                # path); the activity is the retry/isolation boundary. Mirror
                # run()'s post-processing so the out_map mapping is unchanged.
                child_graph = sub_compiled.runnable._state_graph.compile()
                raw = await child_graph.ainvoke(seed_initial_state(sub_input), config)
                sub_result = dict(raw)
                sub_result.update({
                    "output": raw.get("final_output", ""),
                    "evidence": raw.get("evidence", []),
                    "findings": raw.get("findings", {}),
                    "map_results": raw.get("map_results", []),
                    "type": "state_machine",
                })
            else:
                # In-process parent: byte-identical run() path (unchanged).
                sub_result = await sub_compiled.run(sub_input)

            # Merge child result into parent state. Explicit mapping wins;
            # otherwise legacy shape (findings[template_name] + evidence).
            update: dict = {"last_action_status": "success"}
            if out_map is not None:
                # #585: out_map names fields from the child's output_schema, but
                # deterministic operators conventionally write those to
                # state.findings.* (e.g. dataset/gate → findings.verdict), so a
                # top-level state key of that name often doesn't exist. Prefer the
                # top-level key; fall back to findings.<field> so the mapping
                # resolves against the child's ACTUAL state. Without this a
                # composed gate's verdict never threaded up (finetune's report
                # showed dataset_verdict "unknown" though the gate decided warn/fail).
                child_findings = sub_result.get("findings")
                if not isinstance(child_findings, dict):
                    child_findings = {}
                mirror: dict = {}
                for child_field, parent_key in out_map.items():
                    if child_field in sub_result:
                        value = sub_result[child_field]
                    elif child_field in child_findings:
                        value = child_findings[child_field]
                    else:
                        continue
                    update[parent_key] = value
                    # #600: mirror into findings.<parent_key> too. The non-flatten
                    # path does NOT merge the child's findings into the parent, so
                    # only this top-level write carries the value; flatten writes
                    # the same findings.<parent_key> (flatten_exit_update). Both
                    # paths now populate findings.<parent_key>, so a downstream
                    # consumer reads ONE key ($state.findings.<parent_key>)
                    # regardless of composition mode or runtime.
                    mirror[parent_key] = value
                if mirror:
                    update["findings"] = mirror
            else:
                update["findings"] = {bt.template: sub_result.get("output", "")}
                if sub_result.get("evidence"):
                    update["evidence"] = sub_result["evidence"]
            return update

        graph.add_node(node_name, tag(subgraph_node_fn, NodeKind.TOOL))
        return node_name, node_name

    if isinstance(bt, PythonBTNode):
        py_fn = resolve_python_function(bt.function)
        node_name = fresh(f"python_{bt.function}")
        config = dict(bt.config)

        async def python_node_fn(state: dict) -> dict:
            try:
                result = await py_fn(dict(state), **config)
            except Exception as e:
                return {
                    "last_action_status": "failure",
                    "findings": {f"{bt.function}_error": str(e)},
                }
            if not isinstance(result, dict):
                result = {"findings": {bt.function: str(result)}}
            result.setdefault("last_action_status", "success")
            return result

        graph.add_node(node_name, tag(python_node_fn, NodeKind.IO))
        return node_name, node_name

    if isinstance(bt, OperationBTNode):
        # §24.20.8 #1 — leaf operator invocation. OP3 #182 substrate:
        #
        #   1. Locate the binding YAML at compile time (fail loud if missing)
        #   2. Resolve operator class from the registry (fail loud if unknown)
        #   3. Build a closure that resolves bindings + invokes inprocess
        #
        # OP4 #183 landed the full §24.20.6 grammar (six scopes + literals +
        # nested output writes); the resolver reads $artifacts / $env from
        # state["artifacts"] and os.environ respectively unless explicit
        # sources are threaded in. OP6 #185 adds external-job routing for
        # image-bound ops.
        inline = bt.inline_binding()  # #1107: exactly one of ref | operator
        if inline is not None:
            binding = inline
            binding_site = f"inline operator {bt.operator!r}"
        else:
            if factory.operations_dir is None:
                raise ValueError(
                    f"OperationBTNode {bt.ref!r}: template was compiled without "
                    "an operations_dir. The pack root must include an "
                    "operations/ directory alongside templates/."
                )
            binding_path = factory.operations_dir / f"{bt.ref}.yaml"
            # Eagerly load + validate at compile time
            binding = load_binding(binding_path)
            binding_site = f"binding {binding_path}"
        binding = resolve_capability_binding(binding, binding_site)  # #2241 C (D5): v1 → bound operator
        try:
            op_cls = default_operator_registry.get(binding.operator)
        except UnknownOperatorError as e:
            raise UnknownOperatorError(
                f"OperationBTNode {bt.node_ref!r} ({binding_site}) "
                f"references unregistered operator {binding.operator!r}: {e}"
            ) from e

        if not getattr(op_cls, "supports_mock", False):
            factory.unsupported_mock_operators.append(
                f"{bt.node_ref} ({binding.operator})"
            )

        # 2b (#1107): `with:`-form / bare inline nodes elaborate against the
        # operator's schemas — unmapped required → $input.<f>, optional →
        # $default($input.<f>, <schema default>), missing output → producer-
        # named $state.<node_ref>.<f>. Explicit input:/output: maps (the 2a
        # strict form) pass through untouched. Runs BEFORE validation so the
        # required-fields check sees the elaborated map.
        if inline is not None and (not bt.input or not bt.output):
            binding = elaborate_binding(
                binding, op_cls,
                node_ref=bt.node_ref,
                auto_input=not bt.input,
                auto_output=not bt.output,
            )

        # SCH4 #192: compile-time pinning. Catches missing required
        # inputs and unknown input/output field references now, instead
        # of failing at first run.
        validate_operation_binding(
            binding,
            op_cls,
            site=f"OperationBTNode {bt.node_ref!r} ({binding_site})",
        )

        node_name = fresh(f"operation_{bt.node_ref.replace('-', '_')}")
        bound_binding = binding  # close over
        bound_op_cls = op_cls
        bound_ref = bt.node_ref
        contract_ref = factory.contract_prefix + bt.node_ref  # #2241 C — the door's key for this node

        # Compile-time gate detection (ADR-0034): if this operator declares
        # requires_approval, or the binding declares approval.required, the
        # template needs a checkpointer so the gate can durably pause/resume.
        _md = default_operator_registry.get_metadata(binding.operator)
        # #565: a remote-job node's activity routes to the dedicated remote/GPU
        # task queue (a pull-based worker polls it) — tag it so the temporal
        # worker can stamp task_queue on its activity metadata.
        is_remote_job = _md is not None and "remote_job" in (
            getattr(_md, "executors", None) or []
        )
        # #962: a Shardable dataset operator's activity routes to the dedicated
        # dataset-batch queue (dispatcher A) so a heavy CPU scan runs on an
        # isolated worker pool instead of starving the interactive worker.
        try:
            from cairn.core.operator.sharded import Shardable  # noqa: PLC0415 — avoid import cycle
            is_dataset_batch = isinstance(bound_op_cls, type) and issubclass(bound_op_cls, Shardable)
        except Exception:  # noqa: BLE001 — sharding is optional; default to no routing
            is_dataset_batch = False
        _binding_gated = (
            _md is not None and getattr(_md, "requires_approval", False)
        ) or (binding.approval is not None and binding.approval.required)
        if _binding_gated:
            factory.has_gate = True

        # ADR-0081 D2 (#1051) + ADR-0082 D7 (#1073): a DESTRUCTIVE flow cannot
        # ship ungated — structural, not convention. Doors are KIND-MATCHED
        # (`dataflow/run` runs kind: data, `ml/run` runs kind: ml) and the
        # check is engine-blind: it reads the spec, so every engine inherits
        # it. When the binding's flow is statically known (a literal ref, or
        # an inline stage list), aggregate its stage risk classes; any
        # external_write stage REQUIRES the approval gate at compile time.
        _flow_doors = {"dataflow/run": ("dataflow_ref", "data"), "ml/run": ("flow_ref", "ml")}
        if binding.operator in _flow_doors:
            from cairn.core.dataflow.contract import StageRef as _StageRef  # noqa: PLC0415 — dataflow is an optional consumer of the compiler
            from cairn.core.dataflow.defs import (  # noqa: PLC0415
                DataflowDefError,
                aggregate_risk,
                load_flow_defs,
            )

            _ref_key, _want_kind = _flow_doors[binding.operator]
            _site = f"OperationBTNode {bt.node_ref!r} ({binding_site})"
            _stage_refs: list[_StageRef] = []
            _ref_def = None
            _ref = binding.input.get(_ref_key)
            if isinstance(_ref, str) and _ref and not _ref.startswith("$"):
                _defs = load_flow_defs()
                if _ref not in _defs:
                    raise DataflowDefError(
                        f"{_site}: {_ref_key} {_ref!r} not found — installed "
                        f"packs declare {sorted(_defs) or '(none)'}"
                    )
                if _defs[_ref].kind != _want_kind:
                    raise DataflowDefError(
                        f"{_site}: flow {_ref!r} is kind: {_defs[_ref].kind} — "
                        f"{binding.operator} only runs kind: {_want_kind} flows "
                        "(kind-matched doors, ADR-0082 D7)"
                    )
                _ref_def = _defs[_ref]
                _stage_refs = list(_ref_def.stages)
            elif isinstance(binding.input.get("stages"), list):
                _stage_refs = [
                    _StageRef(stage=str(s.get("stage", "")), config={})
                    for s in binding.input["stages"] if isinstance(s, dict)
                ]
            # #1079 — an in-flow `gate:` marker must never be silently dropped.
            # Running a gate-carrying flow WHOLE would execute every stage and ask
            # no one, turning the marker into a comment. Fail closed here; the
            # author splits into segments and binds the gate between them.
            if _ref_def is not None:
                from cairn.core.dataflow.gate_split import (  # noqa: PLC0415
                    assert_runnable_whole,
                )

                assert_runnable_whole(_ref_def, site=_site)
            if _stage_refs and aggregate_risk(_stage_refs) == "external_write" and not _binding_gated:
                raise DataflowDefError(
                    f"{_site}: flow carries external_write stage(s) but the "
                    "binding declares no approval gate — add `approval: required: "
                    "true` (a destructive flow cannot ship ungated, #1051)"
                )
        # #564: a DURABLE remote-job operator ALSO pauses via interrupt() (the
        # control loop resumes it) — so it needs a checkpointer just like a gate,
        # even with NO approval gate in the template. Without this a remote-job-only
        # template (e.g. train-remote-smoke) compiles checkpointer-less and cannot
        # durably pause/resume — the run can't complete. In practice the finetune
        # template rode on its promote-gate's checkpointer; this makes it correct
        # for any remote-job template. Gated on CAIRN_REMOTE_JOB_DURABLE (the inline
        # poll path doesn't interrupt, so it needs no checkpointer).
        elif _md is not None and "remote_job" in (getattr(_md, "executors", None) or []):
            from cairn.core.operator.runtime import _remote_job_durable  # avoid cycle at import

            if _remote_job_durable():
                factory.has_gate = True

        async def operation_node_fn(state: dict) -> dict:
            run_id = str(state.get("run_id") or state.get("symptom", "p?"))
            inv_state = dict(state)

            # ── Approval gate (ADR-0034, P2) — pause via LangGraph interrupt() ──
            from cairn.core.approval.enforce import (
                APPROVALS_STATE_KEY,
                gate_to_request,
                resolve_runtime_gate,
            )
            from cairn.core.operator.runtime import mock_mode_active

            # Only FULL mock (`--mock`) skips the interrupt. A Playground sandbox
            # run enforces HITL for REAL (ADR-0073) — the pause + human approve is
            # the governance moment we want tangible, so the gate fires and the
            # run pauses awaiting_approval just like production.
            if not mock_mode_active():
                gate = resolve_runtime_gate(bound_op_cls, bound_binding, state=state)
                if gate.required:
                    from dataclasses import asdict

                    from langgraph.types import interrupt

                    from cairn.core.obs.audit import audit_emit

                    req = gate_to_request(
                        gate, operator=bound_binding.operator,
                        node_name=bound_ref, run_id=run_id,
                    )
                    req_dict = asdict(req)
                    req_dict["sources"] = list(req.sources)
                    req_dict["allowed_decisions"] = list(req.allowed_decisions)
                    req_dict["kind"] = "approval"  # ADR-0065 — a HITL gate, not a remote job
                    # PAUSE: on resume, interrupt() returns the decision dict
                    # passed to Command(resume=...). Nothing side-effecting runs
                    # before this, so re-run-from-top on resume is harmless.
                    decision = interrupt(req_dict) or {}
                    approved = bool(decision.get("approved"))
                    await audit_emit(
                        "operation.approval",
                        outcome="approve" if approved else "reject",
                        actor=decision.get("approver"),
                        target=bound_binding.operator,
                        run_id=run_id,
                        operation=bound_ref,
                        sources=list(gate.sources),
                    )
                    if not approved:
                        return {
                            "last_action_status": "rejected",
                            "findings": {f"{bound_ref}_approval": "rejected"},
                        }
                    # Record the approval locally so the dispatch chokepoint
                    # lets this operation through (not a graph state channel).
                    inv_state.setdefault(APPROVALS_STATE_KEY, {})[bound_ref] = decision

            # ADR-0118 D4 — a flow door's admitted stage contracts ride its OperationContext.
            flow_contracts = (_node_dataflow_contracts(state, contract_ref)
                              if bound_binding.operator in ("dataflow/run", "ml/run") else None)
            try:
                # §Phase-2.7 — go through the dispatcher rather than in-process
                # directly (only in_process implemented today).
                patch = await invoke_operator(
                    bound_binding,
                    bound_op_cls,
                    state=inv_state,
                    pipeline_id=run_id,
                    node_name=bound_ref,
                    contract=_node_contract(state, contract_ref, bound_op_cls),
                    **({"dataflow_contracts": flow_contracts} if flow_contracts is not None else {}),
                )
            except GraphBubbleUp:
                # #564: a DURABLE remote-job operator pauses via interrupt() deep
                # inside invoke_operator (unlike the approval gate above, which
                # interrupts before this try). interrupt() raises GraphInterrupt (a
                # GraphBubbleUp) — LangGraph control flow that MUST propagate to
                # pause the graph. The broad handler below would otherwise swallow
                # it into a failure finding, so the run could never durably pause.
                raise
            except ContractConformanceError:
                # #2241 C (D1, ADR-0084 D8 amendment of the #568 policy): a shared-
                # contract refusal — identity, input, configuration, output, or an
                # entry the leg was admitted with but this node cannot find — is a
                # governed failure, not probe data. The run fails; nothing else here
                # changes.
                raise
            except Exception as e:
                # #568: a JOB/ACTION operator (remote_job / docker) that fails is a
                # REAL failure — the governed action (train, deploy, publish job)
                # never happened — so FAIL the run instead of swallowing it into a
                # finding. Swallowing let a run report `done` with e.g. the training
                # never dispatched (a false success, seen live on the Temporal
                # bundle). Investigation / in_process / mcp_tool operators keep
                # degrade-to-finding (resilient probing where a tool error is data).
                from cairn.core.operator.executor import declared_executors  # noqa: PLC0415 — avoid a cycle at module import
                if any(x in ("remote_job", "docker") for x in declared_executors(bound_op_cls)):
                    raise
                return {
                    "last_action_status": "failure",
                    "findings": {f"{bound_ref}_error": str(e)},
                }
            patch.setdefault("last_action_status", "success")
            return patch

        from cairn.core.runtime.base import QUEUE_DATASET, QUEUE_REMOTE  # noqa: PLC0415 - avoid import cycle

        # ADR-0080 P0 (#1028) — resolve this operator's activity budget: its
        # DECLARED `activity_timeout_seconds` / `activity_heartbeat_seconds` over
        # the per-kind IO default. A heavy full-corpus scan asks for what it needs
        # instead of relying on a blanket timeout; an undeclared op inherits the
        # (modest) default unchanged.
        op_retry = resolve_retry(NodeKind.IO, RuntimeRetrySpec(
            start_to_close_timeout_seconds=getattr(bound_op_cls, "activity_timeout_seconds", None),
            heartbeat_timeout_seconds=getattr(bound_op_cls, "activity_heartbeat_seconds", None),
        ))
        graph.add_node(
            node_name,
            tag(operation_node_fn, NodeKind.IO,
                retry=op_retry,
                queue=(
                    QUEUE_REMOTE if is_remote_job
                    else QUEUE_DATASET if is_dataset_batch
                    else None
                )),
        )
        return node_name, node_name

    if isinstance(bt, MapBTNode):
        # Dynamic fan-out / fan-in. One body node, invoked once per runtime item
        # via Send; each invocation appends a map_id-tagged entry to map_results;
        # a join node filters to this map's entries and reduces them.
        map_id = fresh("map")
        as_key = bt.as_
        over_key = bt.over
        body = bt.body

        dispatch_name = f"{map_id}__dispatch"
        body_name = f"{map_id}__body"
        join_name = f"{map_id}__join"

        # Build the per-item body function based on body type.
        if isinstance(body, ActionBTNode):
            invoke = factory.build_invoker(body.ref)
            label = body.ref

            async def map_body_fn(
                state: dict,
                _invoke=invoke,
                _as=as_key,
                _mid=map_id,
                _label=label,
            ) -> dict:
                item = state.get("_map_item")
                base = _build_node_user_text(state, _label)
                user_text = f"Focus on this {_as}: {item!r}\n\n{base}"
                content = await _invoke(user_text)
                return {
                    "map_results": [
                        {"map_id": _mid, "item": item, "output": str(content)[:2000]}
                    ],
                    "last_action_status": "success",
                }

        elif isinstance(body, SubgraphBTNode):
            from cairn.core.engine.skill_engine import resolve_template_path

            sub_path = resolve_template_path(body.template, from_pack=factory.from_pack)
            sub_compiled = compile_template(sub_path, registry=factory.registry)
            label = body.template

            async def map_body_fn(
                state: dict,
                _sub=sub_compiled,
                _as=as_key,
                _mid=map_id,
                _label=label,
                _prefix=factory.contract_prefix,
            ) -> dict:
                item = state.get("_map_item")
                sub_input = {
                    "symptom": state.get("symptom", ""),
                    "user_message": f"{_as} = {item!r}\n\n" + state.get("user_message", ""),
                    _as: item,
                }
                sub_input["__contract_invocations__"] = _child_contracts(state, _prefix + _label)  # #2241 C (D2)
                sub_result = await _sub.run(sub_input)
                return {
                    "map_results": [
                        {"map_id": _mid, "item": item, "output": sub_result.get("output", "")}
                    ],
                    "last_action_status": "success",
                }

        elif isinstance(body, PythonBTNode):
            py_fn = resolve_python_function(body.function)
            cfg = dict(body.config)
            label = body.function

            async def map_body_fn(
                state: dict,
                _fn=py_fn,
                _cfg=cfg,
                _as=as_key,
                _mid=map_id,
                _label=label,
            ) -> dict:
                item = state.get("_map_item")
                call_state = {**dict(state), _as: item}
                try:
                    out = await _fn(call_state, **_cfg)
                except Exception as e:  # noqa: BLE001 — surface per-item failure as data
                    return {
                        "map_results": [{"map_id": _mid, "item": item, "error": str(e)}],
                        "last_action_status": "failure",
                    }
                return {
                    "map_results": [{"map_id": _mid, "item": item, "output": out}],
                    "last_action_status": "success",
                }

        else:
            raise ValueError(
                "map body must be an action, subgraph_node, or python_node; "
                f"got {type(body).__name__}"
            )

        aggregate_fn = resolve_python_function(bt.aggregate) if bt.aggregate else None

        async def map_join_fn(
            state: dict,
            _agg=aggregate_fn,
            _mid=map_id,
            _label=label,
        ) -> dict:
            results = [
                r for r in (state.get("map_results") or []) if r.get("map_id") == _mid
            ]
            if _agg is not None:
                update = await _agg({**dict(state), "map_results": results})
                if not isinstance(update, dict):
                    update = {"findings": {_label: update}}
                update.setdefault("last_action_status", "success")
                return update
            # Default reduction: collect each item's output under the body label.
            return {
                "findings": {_label: [r.get("output", r.get("error")) for r in results]},
                "last_action_status": "success",
            }

        def make_map_dispatch(
            _body=body_name, _join=join_name, _over=over_key, _as=as_key
        ):
            async def dispatch(state: dict):
                raw = _resolve_over(state, _over)
                items = raw if isinstance(raw, list) else ([raw] if raw else [])
                if not items:
                    # Empty list: skip the body, run the join over zero results.
                    return [Send(_join, dict(state))]
                sends = []
                for it in items:
                    branch = dict(state)
                    branch[_as] = it
                    branch["_map_item"] = it
                    sends.append(Send(_body, branch))
                return sends

            return dispatch

        graph.add_node(dispatch_name, tag(_noop_node(), NodeKind.PURE))
        graph.add_node(body_name, tag(map_body_fn, NodeKind.TOOL))
        graph.add_node(join_name, tag(map_join_fn, NodeKind.IO))
        graph.add_conditional_edges(
            dispatch_name, make_map_dispatch(), [body_name, join_name]
        )
        graph.add_edge(body_name, join_name)
        return dispatch_name, join_name

    raise ValueError(f"Unknown BT node type: {type(bt).__name__}")


# ─── Shared helpers ─────────────────────────────────────────────────


def _resolve_over(state: dict, over: str) -> Any:
    """Resolve a ``map.over`` reference against the machine state.

    Lookup order:
      1. Dotted path against top-level state channels (e.g. ``findings.services``).
      2. Bare-key fallback into the ``findings`` dict — the common case, since
         upstream nodes write their outputs there and arbitrary top-level keys
         are not persisted by the StateGraph (only declared channels survive).

    Returns whatever is found (caller normalizes to a list); ``None`` if absent.
    """
    cur: Any = state
    for part in over.split("."):
        cur = cur.get(part) if isinstance(cur, dict) else None
        if cur is None:
            break
    if cur is not None:
        return cur
    return (state.get("findings") or {}).get(over)


def _format_user_input(input_data: dict[str, Any]) -> str:
    """Format the input dict as a user message for the LLM."""
    if "user_message" in input_data:
        return str(input_data["user_message"])
    if "symptom" in input_data:
        return f"Symptom: {input_data['symptom']}\n\nFull context:\n{input_data}"
    return str(input_data)


def _build_node_user_text(state: dict, ref: str) -> str:
    """Construct a user message for a state_machine node based on the current state."""
    parts = []
    if state.get("symptom"):
        parts.append(f"Symptom: {state['symptom']}")
    if state.get("user_message"):
        parts.append(f"\n{state['user_message']}")
    findings = state.get("findings", {})
    if findings:
        parts.append("\nPrior findings from other nodes:")
        for k, v in findings.items():
            if k == ref:
                continue
            parts.append(f"  [{k}]: {str(v)[:400]}")
    return "\n".join(parts) if parts else "Begin your investigation."


async def compose_ambient_knowledge(roles: list[str], query: str, *, top_k: int = 4):
    """Ambient knowledge for a declared augmentation step (ADR-0061 model C, #509).

    Compose the bound corpora for ``roles`` (gated by the ENVIRONMENT's active-role
    attach-list), returning ``(knowledge_text, citations)``. Fail-open by design:
    no corpora / retrieval error / the optional rag operator absent → ``("", [])``,
    so the workflow runs unchanged rather than breaking on missing knowledge."""
    if not roles or not query.strip():
        return "", []
    from cairn.services.rag.inject import compose_knowledge
    from cairn.services.rag.roles import active_knowledge_roles

    try:  # optional dependency (env / profile may be absent in bare runs)
        from cairn.core.pack.profile import load_profile_from_env
        profile = load_profile_from_env()
    except Exception:  # noqa: BLE001
        profile = None
    try:
        ctx = await compose_knowledge(
            roles, query, active_roles=active_knowledge_roles(profile), top_k=top_k
        )
        return ctx.text or "", list(ctx.citations)
    except Exception:  # noqa: BLE001 — knowledge is additive; never fail the run
        logger.warning("[knowledge] ambient injection failed for roles=%s", roles, exc_info=True)
        return "", []


def _knowledge_evidence(c) -> dict:
    """A retrieved knowledge chunk as an ``evidence`` item — so injected context
    is auditable in the run (which pack/corpus grounded the step, #509)."""
    return {
        "type": "knowledge",
        "citation": f"{c.source_pack}:{c.corpus}",
        "summary": (getattr(c, "preview", "") or "")[:200],
        "source": getattr(c, "source", "") or "",
        "score": float(getattr(c, "score", 0.0) or 0.0),
    }


_H2_RE = re.compile(r"^##\s+(.+?)\s*$", re.MULTILINE)


def _extract_sections(prose: str) -> dict[str, str]:
    """Extract markdown ## sections into a {name: body} dict.

    Section names are normalized (lowercase, spaces→underscores, strip trailing 'prompt').
    """
    sections: dict[str, str] = {}
    matches = list(_H2_RE.finditer(prose))
    for i, match in enumerate(matches):
        name = match.group(1).strip().lower()
        name = re.sub(r"\s+", "_", name)
        if name.endswith("_prompt"):
            name = name[:-len("_prompt")]
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(prose)
        sections[name] = prose[start:end].strip()
    return sections
