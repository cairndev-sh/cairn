"""
Pydantic schemas for the v2 template, skill, and policy formats.

Three artifact types:
  - Template: markdown + YAML frontmatter, defines a flow (prompt/agent_loop/state_machine)
  - Skill:    markdown + YAML frontmatter, defines triggers + template ref + action candidates
  - Policy:   YAML, defines rules that gate actions

Plus runtime types used during execution:
  - ActionRequest, ActionCandidate, PolicyDecision, RunResult, RCA, etc.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:  # annotation-only: a real import here is a cycle (#1515)
    from cairn.core.operator.binding import OperationBinding


from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer, model_validator

from cairn.core.engine.triggers_grammar import (  # ADR-0101 D1 (#1973)
    DeclaredTrigger,
    parse_declared_triggers,
)


# ─── Common types ────────────────────────────────────────────────────


class ModelConfig(BaseModel):
    """Logical model name → concrete model identifier."""
    model_config = ConfigDict(extra="allow")
    router: Optional[str] = None
    fast: Optional[str] = None
    strong: Optional[str] = None


class ToolsConfig(BaseModel):
    """Tools required by a template (referenced by capability name).

    ``required``/``dynamic`` are the capabilities the template *uses*.
    ``forbidden`` is the inverse: capabilities the template must NEVER be given
    — e.g. a read-only triage template forbidding any write/action tool. It
    documents intent today and pre-stages the future enforcement chokepoint
    (the MCP gateway). The compiler enforces one cheap consistency rule now:
    a capability cannot be both forbidden and required/dynamic.
    """
    required: list[str] = Field(default_factory=list)
    dynamic: list[str] = Field(default_factory=list)
    forbidden: list[str] = Field(default_factory=list)
    tool_k: int = 5


class OutputConfig(BaseModel):
    """Output schema for templates that produce structured output."""
    schema_name: str = Field(alias="schema")
    format: Literal["structured_json", "text", "markdown"] = "structured_json"

    model_config = ConfigDict(populate_by_name=True)


class EvalSpec(BaseModel):
    """Eval dataset and pass criteria for a template."""
    dataset: str
    metrics: list[str] = Field(default_factory=list)
    pass_threshold: float = 0.85


class BudgetConfig(BaseModel):
    """Per-investigation resource ceiling for a template.

    Opt-in: a template without a ``budget`` block behaves exactly as before.

    Enforcement status (be honest about what actually fires today):
      - ``max_wall_seconds`` — **ENFORCED**. The Runner wraps the template run
        in ``asyncio.wait_for``; on timeout the investigation is recorded as
        ``budget_exceeded`` instead of running unbounded.
      - ``max_cost_usd`` — **ENFORCED**. The Runner runs each investigation inside
        an ``obs.cost_scope``; the ``BudgetCallback`` on every LLM client tallies
        token spend (``obs.record_llm``) and raises ``BudgetExceeded`` when the
        running total crosses this cap (enforcement granularity: per-LLM-call, and
        the call that crosses it has been paid for). The deployment's
        ``settings.max_cost_usd`` ($1 by default) also applies, and the lower figure
        wins. The run fails and, for a template leg, its pipeline stops; a skill run
        also skips its critique, actions and cascade. Under Temporal the worker only
        meters, so a template leg's run fails when it ends, with its calls already made.
      - ``max_iterations`` — **ENFORCED for the critique loop** (caps revise
        passes, see ``CritiqueConfig``). For agent_loop tool iteration the
        operative limit is still the per-flow/per-node ``max_iterations``.
      - ``max_tokens`` — **DECLARED, not enforced** (no token-ceiling abort; cost
        is the enforced money dimension).
    """
    max_wall_seconds: Optional[float] = None
    max_cost_usd: Optional[float] = None
    max_tokens: Optional[int] = None
    max_iterations: Optional[int] = None


class CritiqueConfig(BaseModel):
    """Opt-in self-critique loop — a reflection pass over the template's output.

    Faithful port of the old graph's ``self_critique_node``: after the template
    produces its output, a *second* LLM call reviews it and returns a structured
    verdict. If the verdict is below the confidence bar (and passes remain), the
    template is re-invoked with the critique's issues injected as feedback, so the
    next attempt can improve. Bounded by ``max_passes`` AND ``budget.max_iterations``
    (whichever is smaller) so a stubbornly-low-confidence answer can't loop forever.

    Disabled by default: a template without a ``critique`` block behaves as before.
    """
    enabled: bool = True
    model: str = "gpt-4o-mini"
    max_passes: int = 1
    # Critique confidence (that the output is GOOD) at/above which we accept and
    # stop. Below it (and the verdict not ``acceptable``) triggers a revise pass.
    confidence_threshold: float = 0.7


# ─── Flow types (discriminated by flow.type) ─────────────────────────


class PromptFlow(BaseModel):
    """flow.type: prompt — single LLM call with structured output."""
    type: Literal["prompt"]
    model: str
    temperature: float = 0.0
    max_tokens: int = 2048
    #: Ambient knowledge roles to inject (ADR-0061 model C, #509). See
    #: :attr:`ActionNodeDef.knowledge`.
    knowledge: list[str] = Field(default_factory=list)


class AgentLoopFlow(BaseModel):
    """flow.type: agent_loop — REMOVED by ADR-0084 D3 (#1132).

    The model is retained ONLY so the compiler can reject it with an actionable
    message naming the successor (`state_machine` + a `prompt` node). Dropping it
    from the union would surface as an opaque pydantic parse failure instead.
    """
    type: Literal["agent_loop"]
    model: str
    max_iterations: int = 12
    temperature: float = 0.0
    #: Ambient knowledge roles to inject (ADR-0061 model C, #509). See
    #: :attr:`ActionNodeDef.knowledge`.
    knowledge: list[str] = Field(default_factory=list)


# ─── Behavior tree primitives (for state_machine flow) ───────────────


class ActionBTNode(BaseModel):
    """Leaf behavior tree node — invokes a named action defined in flow.nodes."""
    type: Literal["action"]
    ref: str  # references a key in StateMachineFlow.nodes


class SequenceBTNode(BaseModel):
    """Run children in order. Stop on first failure."""
    type: Literal["sequence"]
    children: list[BTNode]


class ParallelBTNode(BaseModel):
    """Run children concurrently. Join via state reducers."""
    type: Literal["parallel"]
    children: list[BTNode]


class GuardBTNode(BaseModel):
    """Conditional gate: evaluate a registered condition function on state,
    branch to `then` (truthy) or `else_` (falsy / missing).

    The condition function is looked up in the conditions registry. It receives
    the current state dict and returns a string (or bool); 'true'/'yes'/True
    routes to `then`, anything else routes to `else_`.
    """
    type: Literal["guard"]
    condition: str  # registered condition function name
    then: BTNode
    else_: Optional[BTNode] = Field(default=None, alias="else")

    model_config = ConfigDict(populate_by_name=True)


class SelectorBTNode(BaseModel):
    """Try children in order; succeed on the first that succeeds.

    A child 'succeeds' when its last action node writes `last_action_status:
    success` (the default for all action nodes). Implementation uses
    conditional edges that check the status and either exit or try next.
    """
    type: Literal["selector"]
    children: list[BTNode]


class SubgraphBTNode(BaseModel):
    """Reference another template by name. The referenced template is compiled
    at outer-compile-time and runs as a single node in the parent graph.

    Phase 2b — explicit input/output mapping between parent state and the
    child template's typed contract.

    ``inputs`` (optional): ``{child_field: parent_state_key}``. Each entry
    pulls one value out of parent state and feeds it to the child under the
    named field. When the child declares an ``input_schema``, the compiler
    **requires** this mapping to be present and to cover every required
    schema field (typos in field names are caught at compile time).

    ``outputs`` (optional): ``{child_output_field: parent_state_key}``. Each
    entry takes one value from the child's result dict and merges it into
    parent state under the named key. When the child declares an
    ``output_schema``, the compiler validates the mapping in the same way.

    Backward compatibility: a subgraph_node without either mapping AND a
    child without either schema falls back to the legacy hardcoded
    behavior (parent's ``symptom`` / ``user_message`` flow into the child;
    child's ``output`` / ``evidence`` merge into ``findings`` / ``evidence``
    on the parent). Every existing pre-Phase-2 template still compiles.
    """
    type: Literal["subgraph_node"]
    template: str  # template name to resolve via the template search path
    inputs: Optional[dict[str, str]] = None
    outputs: Optional[dict[str, str]] = None


class PythonBTNode(BaseModel):
    """Escape hatch to a deterministic Python function registered in the
    python_functions registry. The function receives the state dict and an
    optional config dict, returns a partial state update dict.

    Signature: fn(state: dict, **config) -> dict

    ``outputs`` (§24.22 follow-on): the top-level state keys the function
    writes. LangGraph persists updates only to *declared* state channels,
    and a python function's writes are opaque to the compiler — without
    this list, writes to non-base keys are silently dropped. The compiler
    folds these names into the template's dynamic state schema; returned
    keys that are neither base channels nor listed here still vanish
    (same LangGraph rule).
    """
    type: Literal["python_node"]
    function: str  # registered function name
    config: dict[str, Any] = Field(default_factory=dict)
    outputs: list[str] = Field(default_factory=list)


class OperationBTNode(BaseModel):
    """§24.20.8 #1 — leaf node invoking an operator.

    TWO forms, and a node carries **exactly one** of ``ref`` | ``operator``
    (#1107, pack-redesign §3 Fix A):

    - ``ref:`` — the SHARED form: refs ``<pack>/operations/<ref>.yaml``, which
      fully declares the operator + I/O bindings. Survives for genuinely
      reused bindings (one file serving several templates).
    - ``operator:`` — the INLINE form: the binding lives ON the node
      (``input``/``output`` + the same optional §24.20.5 fields), so a
      single-use call site is one self-contained node instead of a pointer
      plus a file elsewhere (the P19 call-site-per-file defect).

    Either way the compiler builds the SAME :class:`OperationBinding` and
    everything downstream (registry lookup, §24.20.6 resolution via the ONE
    expression language, gate detection, remote-job routing) is shared —
    which is what makes the codemod a pure refactor: a converted node
    compiles to a structurally identical graph.

    ``id`` names the compiled node. It defaults to the ref (shared form) or
    the operator with ``/``→``_`` (inline form); the codemod sets it to the
    old ref so a converted template's node names — and therefore its compiled
    graph — are byte-identical.
    """
    type: Literal["operation"]
    ref: Optional[str] = None  # → operations/<ref>.yaml (shared form)
    # ── inline form (#1107) — mirrors OperationBinding's §24.20.5 fields ──
    operator: Optional[str] = None
    capability: Optional[str] = None  # #2241 C — v1 inline form: contract://<id>/<rev>
    configuration: dict[str, Any] = Field(default_factory=dict)  # v1 semantic config
    id: Optional[str] = None  # compiled node name override (codemod: old ref)
    version: Optional[str] = None
    input: dict[str, Any] = Field(default_factory=dict)
    #: 2b sugar (#1107): ``with:`` = overrides-only spelling of ``input:``.
    #: GitHub-Actions semantics — call-site cost ∝ deviation from defaults.
    #: Unmapped fields auto-bind at compile: required → ``$input.<f>``,
    #: optional → ``$default($input.<f>, <schema default>)`` (elaboration in
    #: operator/elaborate.py). A node uses ``with:`` OR ``input:``, not both.
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    output: dict[str, Any] = Field(default_factory=dict)
    resources: Optional[dict[str, Any]] = None
    secrets: list[str] = Field(default_factory=list)
    artifacts: Optional[dict[str, Any]] = None
    budget: Optional[dict[str, Any]] = None
    approval: Optional[dict[str, Any]] = None
    policy: Optional[dict[str, Any]] = None
    execution: Optional[dict[str, Any]] = None

    @model_serializer(mode="wrap")
    def _omit_unset_v1_fields(self, handler: Any) -> dict[str, Any]:
        """#2241 C fields are omitted while unset, so a legacy node's AST — and with it every
        legacy template's content hash and golden snapshot — stays byte-identical."""
        data = handler(self)
        if self.capability is None:
            data.pop("capability", None)
        if not self.configuration:
            data.pop("configuration", None)
        return data

    @model_validator(mode="after")
    def _exactly_one_of_ref_or_operator(self) -> "OperationBTNode":
        if bool(self.ref) == bool(self.operator or self.capability):
            raise ValueError(
                "an operation node carries exactly ONE of `ref` (shared "
                "binding file) or `operator` (inline binding) — #1107"
            )
        if self.input and self.with_:
            raise ValueError(
                "an operation node uses `with:` (overrides + auto-bind, 2b) "
                "OR `input:` (full explicit map) — not both"
            )
        if self.ref and (self.input or self.with_ or self.output or self.operator is not None or self.capability):
            raise ValueError(
                f"operation node ref={self.ref!r}: inline binding fields "
                "(operator/input/output/…) are only allowed on the inline "
                "form — put them in the operations/ file or go inline"
            )
        return self

    @property
    def node_ref(self) -> str:
        """The name this node compiles under (events, labels, errors)."""
        if self.id:
            return self.id
        if self.ref:
            return self.ref
        return (self.operator or "").replace("/", "_")

    def inline_binding(self) -> Optional["OperationBinding"]:
        """The §24.20.5 binding carried ON the node (inline form), else None."""
        if not self.operator and not self.capability:
            return None
        from cairn.core.operator.binding import OperationBinding  # noqa: PLC0415 — leaf import, avoids widening schema.py's import surface at module load

        return OperationBinding.model_validate({
            **({"operator": self.operator} if self.operator else {}),
            **({"capability": self.capability} if self.capability else {}),
            **({"configuration": self.configuration} if self.configuration else {}),
            **({"version": self.version} if self.version else {}),
            # `with:` seeds the same map — elaboration (2b) fills the rest at
            # compile; explicit `input:` is the strict 2a form, taken as-is.
            "input": self.input or self.with_,
            "output": self.output,
            **({"resources": self.resources} if self.resources else {}),
            **({"secrets": self.secrets} if self.secrets else {}),
            **({"artifacts": self.artifacts} if self.artifacts else {}),
            **({"budget": self.budget} if self.budget else {}),
            **({"approval": self.approval} if self.approval else {}),
            **({"policy": self.policy} if self.policy else {}),
            **({"execution": self.execution} if self.execution else {}),
        })


class MapBTNode(BaseModel):
    """Dynamic fan-out / fan-in (map-reduce) over a runtime-sized list.

    Unlike :class:`ParallelBTNode` (which fans out to a *fixed* set of named
    children authored into the template), ``map`` runs a *single* sub-flow once
    per element of a list discovered **at runtime** in state key ``over``, all
    branches concurrently, then reduces the per-item results.

    - ``over``      — name of the state key holding the list to map over
                      (e.g. ``"suspect_services"``). A non-list value is wrapped
                      to a single-element list; an empty/missing value runs the
                      aggregate over zero results (no branches).
    - ``body``      — the per-item sub-flow. Must be an ``action`` ref (inner
                      prompt/agent_loop), a ``subgraph_node`` (a whole template),
                      or a ``python_node`` (deterministic). Each branch sees a
                      copy of state with the current item bound under ``as_``.
    - ``as_``       — the key the current item is exposed under in each branch
                      (default ``"item"``); also surfaced to action/subgraph
                      prompts.
    - ``aggregate`` — optional registered python function that reduces the
                      collected per-item results into a partial state update
                      (``findings`` / ``final_output`` / ``evidence``). It
                      receives the state with ``map_results`` scoped to *this*
                      map's items. When omitted, results are collected into
                      ``findings`` under the body's label.
    """
    type: Literal["map"]
    over: str
    body: BTNode
    as_: str = Field(default="item", alias="as")
    aggregate: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


# Discriminated union over BT node types (now includes map + operation)
BTNode = Annotated[
    Union[
        ActionBTNode,
        SequenceBTNode,
        ParallelBTNode,
        GuardBTNode,
        SelectorBTNode,
        SubgraphBTNode,
        PythonBTNode,
        MapBTNode,
        OperationBTNode,
    ],
    Field(discriminator="type"),
]


# Required because BTNode references itself recursively
SequenceBTNode.model_rebuild()
ParallelBTNode.model_rebuild()
GuardBTNode.model_rebuild()
SelectorBTNode.model_rebuild()
MapBTNode.model_rebuild()


# ─── Action node definitions (used by state_machine action refs) ─────


class RetrySpec(BaseModel):
    """Per-action-node retry/timeout override for durable runtimes.

    All fields optional — any field left ``None`` inherits the per-NodeKind
    default from :mod:`cairn.core.runtime.retry`. The in-process runtime
    ignores this entirely; only the Temporal runtime consumes it.

    Example frontmatter::

      nodes:
        slow_synthesis:
          type: prompt
          model: strong
          retry:
            start_to_close_timeout_seconds: 180
            maximum_attempts: 2
    """
    maximum_attempts: Optional[int] = None
    initial_interval_seconds: Optional[float] = None
    backoff_coefficient: Optional[float] = None
    maximum_interval_seconds: Optional[float] = None
    start_to_close_timeout_seconds: Optional[float] = None


class ActionNodeDef(BaseModel):
    """Definition of a single action node in a state_machine flow.

    An action node is a single deterministic LLM call producing structured
    output. There is no inner agent loop: ADR-0084 D3 removed ReAct from cairn
    entirely, so a node's tools come from capability nodes in the behaviour
    tree, where each call is a separately governed step.
    """
    type: Literal["prompt"] = "prompt"
    model: str
    capabilities: list[str] = Field(default_factory=list)
    #: Ambient knowledge roles to inject before the LLM call (ADR-0061 model C,
    #: #509). When set, the engine composes the bound corpora for these roles
    #: (gated by the environment's active-role attach-list), prepends the cited
    #: top-k hits to the system prompt, and records the citations — so the step
    #: is grounded in governed knowledge without hand-wiring a rag/search node.
    knowledge: list[str] = Field(default_factory=list)
    prompt_section: Optional[str] = None  # name of markdown ## section, default = node name
    max_iterations: int = 8
    output_schema: Optional[str] = None
    # Optional Temporal-runtime override; ignored by InProcessRuntime.
    retry: Optional[RetrySpec] = None


class StateMachineFlow(BaseModel):
    """flow.type: state_machine — behavior tree compiled to LangGraph."""
    type: Literal["state_machine"]
    source: Literal["behavior_tree"] = "behavior_tree"
    root: BTNode
    nodes: dict[str, ActionNodeDef]


# Top-level flow is a discriminated union
Flow = Annotated[
    Union[PromptFlow, AgentLoopFlow, StateMachineFlow],
    Field(discriminator="type"),
]


# ─── Template spec (frontmatter of a template .md file) ──────────────


class TemplateSpec(BaseModel):
    """Top-level template frontmatter.

    The markdown body is parsed separately by the compiler and becomes the
    system prompt (for prompt/agent_loop) or section source (for state_machine).
    """
    name: str
    version: str
    description: Optional[str] = None

    # Optional grouping label within a pack — purely organizational metadata.
    # A pack's templates cluster into modules (e.g. incident → triage /
    # investigate / postmortem). This is a free-form string, not a registered
    # type: no loader, registry, or manifest hangs off it. It exists so the
    # Pack → Module → Template mental model is legible in listings and docs.
    module: Optional[str] = None

    # Free-form discovery tags (e.g. "data:logs", "runtime:langgraph",
    # "cost:low"). Pure metadata for listing/filtering/search — never used for
    # resolution or enforcement. Mirrors `Pack.tags` at the template grain.
    tags: list[str] = Field(default_factory=list)

    # #497 (ADR-0061) — ambient knowledge role slots. Corpora bound to these
    # roles (across all installed packs, gated by the env's Profile.knowledge
    # attach-list) are retrieved via rag/search and injected — with citations —
    # into this template's context at run time. Empty = no ambient knowledge
    # (the legacy explicit-node path is unaffected).
    knowledge: list[str] = Field(default_factory=list)

    flow: Flow

    # Typed subgraph contract — fully-qualified dotted paths to Pydantic
    # BaseModel subclasses (e.g.
    # ``cairn.packs.incident.code.schemas.db_investigation.Input``). The
    # compiler resolves these at compile time and fails fast if the path
    # doesn't point at a real Pydantic model. Both fields are optional; a
    # template without declared schemas keeps working exactly as today.
    #
    # Phase 2a (now): declaration + resolution validation, so the contract
    # exists and is verified to be importable. Phase 2b will add ``inputs:``
    # / ``outputs:`` mapping fields on subgraph nodes and validate parent
    # mappings against these schemas at compile time.
    input_schema: Optional[str] = None
    output_schema: Optional[str] = None

    # ADR-0101 D1 (#1973) — how this workflow STARTS, declared in the same file
    # that says what it does (Kestra's `triggers:` block). Each entry is a
    # `kind: manual | schedule | webhook | watch`; `manual` is implicit on every
    # workflow, so declaring it documents intent and does nothing else.
    # Validated on this seam — beside `input_schema` — so a bad cron or an
    # unknown receiver preset fails the COMPILE, not the fire. Enabling the
    # workflow into a workspace materialises these into the stores that already
    # fire (see cairn.core.engine.triggers_grammar); nothing here is a second store.
    #
    # Deliberately NOT part of the content hash / AST projection (see
    # `compiled.ast_dict`): identity is what the workflow EXECUTES, and editing
    # a cron must not invalidate every pinned template hash.
    triggers: list[DeclaredTrigger] = Field(default_factory=list)

    @field_validator("triggers", mode="before")
    @classmethod
    def _validate_triggers(cls, v: Any) -> Any:
        """Precise errors for the declared-trigger grammar. Pydantic's own
        discriminated-union message names a union member and a field path; the
        author needs "entry 1: '0 3 * * * *' is not a valid 5-field cron"."""
        return parse_declared_triggers(v)

    # Optional for prompt/agent_loop (single model in flow); used by state_machine
    # to provide a model pool that ActionNodeDef.model can reference (e.g. "small" → "gpt-4o-mini")
    models: Optional[ModelConfig] = None

    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    output: Optional[OutputConfig] = None
    evals: Optional[EvalSpec] = None

    # Opt-in resource ceiling. None = unbounded (today's default behavior).
    budget: Optional[BudgetConfig] = None

    # Opt-in self-critique / reflection loop. None = no critique pass.
    critique: Optional[CritiqueConfig] = None


# ─── Skill triggers ──────────────────────────────────────────────────


class MetricTrigger(BaseModel):
    """Metric-based trigger: expr is a threshold expression."""
    expr: str
    duration: str = "0s"


class AlertTrigger(BaseModel):
    """Alert-source trigger (PagerDuty, Alertmanager webhook, etc.)."""
    source: str
    service: Optional[str] = None


class TriggersSpec(BaseModel):
    """All trigger types a skill can declare. Any match → skill activates."""
    patterns: list[str] = Field(default_factory=list)
    metrics: list[MetricTrigger] = Field(default_factory=list)
    alerts: list[AlertTrigger] = Field(default_factory=list)
    schedule: Optional[str] = None  # cron expression
    manual: bool = False
    # Cross-pack event-bus subscriptions (ADR-0010 P3.c, closes §24.3.7). Bus
    # topic patterns (exact or wildcard, e.g. "incident.rca_completed",
    # "incident.*"); a bus message on a matching topic activates this skill via
    # a source="event" TriggerEvent. Targets pack/domain topics — the engine's
    # own run.* lifecycle firehose is reserved and not an activation source.
    subscribes_to: list[str] = Field(default_factory=list)


# ─── Skill spec ──────────────────────────────────────────────────────


class TemplateRef(BaseModel):
    """Reference from a skill to a template (by name + optional hash/tag)."""
    name: str
    tag: Optional[str] = None
    hash: Optional[str] = None


class ActionCandidate(BaseModel):
    """An action a skill might suggest. The Action Engine evaluates against policy."""
    id: str
    type: str            # e.g. "k8s_scale", "rollback_deployment"
    risk: Literal["low", "medium", "high"] = "medium"
    description: Optional[str] = None
    params: dict[str, Any] = Field(default_factory=dict)

    # First-class idempotency key for durable execution. A durable runtime
    # (Temporal — ADR-0024) keys the workflow ID on this for dedup + crash
    # replay: two submissions with the same key resolve to the *same* saga
    # (replayed, not re-run). Defaults to `id` when
    # unset via `idempotency()` below, but skills/callers can set an explicit
    # stable key (e.g. derived from run_id + target) to make replay intent
    # explicit and decouple it from the candidate's surrogate id.
    idempotency_key: Optional[str] = None

    # §24.3.4 (P2.f) — the capability this action invokes when executed.
    # Optional: skills that don't declare it keep today's policy-matching
    # behavior (capabilities filter is skipped for the action). When set,
    # the policy engine cross-references against
    # :attr:`PolicyAppliesTo.capabilities` so cross-cutting policies
    # (compliance.redact-pii-before-comms, etc.) can target the call by
    # capability name rather than action type.
    capability: Optional[str] = None

    def idempotency(self) -> str:
        """Resolve the effective idempotency / durable-workflow key.

        Explicit `idempotency_key` wins; otherwise the candidate `id` is the
        stable key. Never returns empty.
        """
        return self.idempotency_key or self.id


class PostIncidentHooks(BaseModel):
    """Actions to take after investigation resolves or fails."""
    on_resolve: list[str] = Field(default_factory=list)
    on_failure: list[str] = Field(default_factory=list)


class SkillCustomLogic(BaseModel):
    """Python escape hatch for skills.

    Points to a Python module that may export any of these optional callables:
      - evaluate_trigger(event: TriggerEvent) -> bool
            Custom trigger predicate. Called AFTER the declarative trigger
            matched; can veto with False or extend matching with additional
            checks (business hours, ML threshold, etc.).
      - validate_action(candidate: ActionCandidate, context: dict) -> bool
            Pre-policy validation. False rejects the candidate outright.
      - on_resolve(result: dict) -> None
            Post-resolution hook. Side-effecting; return value ignored.
      - on_failure(result: dict) -> None
            Post-failure hook. Side-effecting.
    """
    module: str  # python import path, e.g. "cairn.packs.incident.code.skills.db_pool_logic"
    evaluate_trigger: Optional[str] = None
    validate_action: Optional[str] = None
    on_resolve: Optional[str] = None
    on_failure: Optional[str] = None


class SkillSpec(BaseModel):
    """Top-level skill frontmatter."""
    name: str
    version: str
    description: Optional[str] = None

    # Optional grouping label within a pack (see TemplateSpec.module). A skill
    # inherits its module from its template when unset; setting it here lets a
    # skill override or declare its own grouping. Pure metadata for discovery.
    module: Optional[str] = None

    triggers: TriggersSpec
    template: TemplateRef

    # Skill-specific additions (no deep-path overrides; additive only)
    additional_capabilities: list[str] = Field(default_factory=list)

    action_candidates: list[ActionCandidate] = Field(default_factory=list)
    post_incident: Optional[PostIncidentHooks] = None

    # Optional Python escape hatch — see SkillCustomLogic docstring
    custom_logic: Optional[SkillCustomLogic] = None

    # Episodic memory — default on; skills opt out with `memory: enabled: false`
    # in their YAML frontmatter. When enabled, the Runner writes each
    # RunResult to Qdrant via cairn.services.memory.store.save_episodic so future
    # investigations of similar symptoms can recall it.
    memory_enabled: bool = True

    # Cascade — after this skill resolves successfully, fire one or more
    # follow-up skills. Each receives a cascade TriggerEvent carrying THIS
    # skill's findings/actions/outcomes (so all chained skills see the real
    # investigation, not each other's derivative output). Bounded by the
    # Runner's cascade depth limit (default 3).
    #   - chain_skill:  single successor (back-compat, kept for existing skills)
    #   - chain_skills: list of successors (fan-out — all are siblings of the
    #     same parent investigation)
    # Both are honored; the effective set is chain_skills + ([chain_skill] if set).
    chain_skill: Optional[str] = None
    chain_skills: list[str] = Field(default_factory=list)

    def effective_chain_skills(self) -> list[str]:
        """De-duplicated successor list combining chain_skill + chain_skills."""
        out: list[str] = list(self.chain_skills)
        if self.chain_skill and self.chain_skill not in out:
            out.append(self.chain_skill)
        return out


# ─── Policy spec ─────────────────────────────────────────────────────


class PolicyAppliesTo(BaseModel):
    """Which actions a policy applies to.

    Filter semantics: each field is a whitelist for one dimension. An
    empty list means "don't filter on this dimension" (matches anything);
    a populated list means "the request must hit one of these values".
    The dimensions AND together — a policy declaring both ``action_types``
    and ``environments`` matches only requests that hit both.

    §24.3.4 (P2.f) — ``capabilities`` is the cross-cutting attachment
    knob: a compliance pack's policy can target ``comms.post_message``
    by name and auto-fire on any action that's executed via that
    capability, no matter which domain pack proposed the action.
    """
    action_types: list[str] = Field(default_factory=list)
    environments: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)  # ["low", "medium", "high"]
    #: Capability names whose invocations this policy gates. When
    #: non-empty, an action only matches if its
    #: :attr:`ActionCandidate.capability` is in this list.
    capabilities: list[str] = Field(default_factory=list)
    #: #1252 (ADR-0087 P0b) — glob patterns over pipeline LEG refs
    #: (``training/*``, ``*/finetune``). When non-empty, the policy fires at
    #: PLAN time: a ``require_hitl`` rule injects an approval gate immediately
    #: before every matching leg (un-removable by the pipeline author — it is
    #: not in their record), and a ``deny`` rule refuses the launch outright.
    #: Matched against the bare ``pack/name`` form (any D0 kind prefix —
    #: ``flow:``, ``task:``, ``gate:`` — stripped; a glob that spells a prefix
    #: is also tried against the raw ref and so matches that kind only, #2539).
    #: The pack segment folds spelling on both sides (#2546, via
    #: ``cairn.core.refs.pack_key``/``spellings``): ``dataset-readiness/*`` and
    #: ``dataset_readiness/*`` each match a leg in either spelling; a glob whose
    #: pack segment holds a wildcard is matched as written.
    #: Empty = this policy does not touch pipeline plans.
    pipeline_legs: list[str] = Field(default_factory=list)


class PolicyRule(BaseModel):
    """A single rule in a policy.

    Effect is one of:
      - allow_auto  : action proceeds without human approval
      - require_hitl: action needs human approval via channel
      - require_dry_run: must show dry-run before requesting approval
      - deny: action is blocked
    """
    effect: Literal["allow_auto", "require_hitl", "require_dry_run", "deny"]
    reason: Optional[str] = None
    channels: list[str] = Field(default_factory=list)  # e.g. ["slack:#prod-oncall"]
    approval_role: Optional[str] = None
    timeout: Optional[str] = None  # e.g. "15min"


class PolicyAdmission(BaseModel):
    """#2190 (T05) — what a policy REQUIRES of a dataset version before a
    pipeline may train on it: the names of checks (`cairn.core.intake.CHECKS`)
    whose evidence must be `ran` on the version. Which checks are required is
    policy data, never core (#2188 special note)."""

    required_checks: list[str] = Field(default_factory=list)


class PolicySpec(BaseModel):
    """Top-level policy YAML."""
    name: str
    description: Optional[str] = None
    applies_to: PolicyAppliesTo
    rules: list[PolicyRule]
    #: #2190 (T05) — optional; a policy with no admission block requires nothing.
    admission: Optional[PolicyAdmission] = None


# ─── Runtime types (used during execution, not in artifact files) ────


class TriggerEvent(BaseModel):
    """An event that may activate one or more skills."""
    source: Literal["pattern", "alert", "metric", "schedule", "manual", "cascade", "event"]
    payload: dict[str, Any] = Field(default_factory=dict)
    matched_pattern: Optional[str] = None
    service: Optional[str] = None
    timestamp: Optional[str] = None


class Evidence(BaseModel):
    """One piece of evidence collected during investigation."""
    type: Literal["log", "trace", "metric", "change", "runbook"]
    citation: str        # ID for reference (log_pattern_id, trace_id, etc.)
    summary: str         # one-line human-readable description
    confidence: float = 1.0
    payload: dict[str, Any] = Field(default_factory=dict)


class BlastRadiusItem(BaseModel):
    service: str
    impact: Literal["direct", "upstream", "downstream"]
    user_pct_affected: int = 0


class RootCause(BaseModel):
    description: str
    service: Optional[str] = None
    confidence: float
    category: Optional[str] = None  # "deploy", "resource", "downstream", etc.


class RCA(BaseModel):
    """Structured RCA output from an investigation template."""
    symptom: str
    timeline: dict[str, Any] = Field(default_factory=dict)
    root_cause: RootCause
    evidence: list[Evidence] = Field(default_factory=list)
    blast_radius: list[BlastRadiusItem] = Field(default_factory=list)
    suggested_actions: list[dict[str, Any]] = Field(default_factory=list)


class TriageDecision(BaseModel):
    """Generic triage output shape for prompt-type templates."""
    severity: Literal["low", "medium", "high", "critical"]
    category: Literal["deploy", "resource", "downstream", "unknown"]
    should_trigger_full_rca: bool
    reasoning: str


#: The three policy TIERS a deployment's environments are judged at. A tier is
#: not the same thing as an environment NAME: a deployment may run `campaign`,
#: `staging-eu`, `pilot-3` — all of which must still be judged as one of these.
POLICY_TIERS = ("dev", "staging", "production")


def policy_tier(environment: str) -> str:
    """Map a launch-environment NAME to the tier policies are written against.

    A tier name passes through; anything else resolves to ``production`` —
    the STRICTEST tier, because an unrecognized environment is not a licence
    to relax (fail-safe: more gating, never less).

    Exists because `environment` was typed `Literal[dev|staging|production]`
    while `launch_pipeline` has always accepted a free-form name. A named
    environment therefore raised a pydantic ValidationError inside
    `resolve_gate`, which its except-branch counts as an evaluation FAILURE →
    the fail-closed sentinel → `require_hitl` on EVERY task leg. Two real
    pipelines died that way before the cause was found (#1766, 2026-08-28);
    the workaround was to launch with no environment at all, which silently
    dropped the environment from the record.
    """
    e = (environment or "").strip().lower()
    return e if e in POLICY_TIERS else "production"


class ActionRequest(BaseModel):
    """A proposed action being evaluated against policies."""
    candidate: ActionCandidate
    #: The environment NAME as launched — free-form on purpose (#1766). Policies
    #: match it by name OR by its `policy_tier`, so a policy scoped to
    #: `production` still governs a deployment's own `campaign` environment.
    environment: str
    context: dict[str, Any] = Field(default_factory=dict)

    @property
    def tier(self) -> str:
        """The policy tier this request is judged at."""
        return policy_tier(self.environment)


class PolicyDecision(BaseModel):
    """Outcome of evaluating policies against an ActionRequest."""
    effect: Literal["allow_auto", "require_hitl", "require_dry_run", "deny"]
    matched_policies: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    channels: list[str] = Field(default_factory=list)
    approval_role: Optional[str] = None


class ActionDecision(BaseModel):
    """An action candidate paired with its policy decision."""
    candidate: ActionCandidate
    decision: PolicyDecision


class RunResult(BaseModel):
    """Top-level result of running a skill end-to-end."""
    skill_name: str
    template_name: str
    #: §24.11.3 — content-addressed template identity at the moment this
    #: run executed. Empty when an older runner path doesn't populate it
    #: (back-compat). Used by ``cairn eval --compare-against <hash>``
    #: to pin a regression run to a specific version, and by replay to
    #: show *which* template version produced the trace.
    template_hash: str = ""
    #: §24.22 (AST3) — canonical AST projection of the template that ran,
    #: same dict the content hash is computed over. Persisted into run
    #: meta so ``cairn replay`` and post-hoc debugging are
    #: self-contained (the exact compiled shape, not just its hash).
    #: Empty for paths that don't populate it (back-compat).
    template_ast: dict[str, Any] = Field(default_factory=dict)
    trigger: TriggerEvent

    # Run output (RCA or other)
    rca: Optional[RCA] = None
    triage: Optional[TriageDecision] = None
    raw_output: Optional[dict[str, Any]] = None  # for templates with custom output schemas

    # Action decisions (policy evaluation results)
    actions: list[ActionDecision] = Field(default_factory=list)

    # Action execution outcomes (populated when an ActionHandler was supplied)
    action_outcomes: list[ActionOutcome] = Field(default_factory=list)

    # Execution metadata
    duration_seconds: float = 0.0
    llm_cost_usd: float = 0.0
    iterations: int = 0


# ─── Action execution types (for the executor layer) ──────────────────


class DryRunResult(BaseModel):
    """Output of an executor's dry_run step — what would change."""
    diff: str  # Human-readable summary or unified diff
    estimated_changes: dict[str, Any] = Field(default_factory=dict)
    reversible: bool = True


class ExecuteResult(BaseModel):
    """Output of an executor's execute step."""
    success: bool
    output: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


class VerifyResult(BaseModel):
    """Output of an executor's verify step."""
    status: Literal["success", "pending", "failed"]
    summary: str = ""
    metrics: dict[str, Any] = Field(default_factory=dict)


class ApprovalRecord(BaseModel):
    """Record of an HITL approval (for audit trail)."""
    approved: bool
    approver: Optional[str] = None
    channel: Optional[str] = None
    timestamp_iso: Optional[str] = None
    note: Optional[str] = None


class ExecutorContext(BaseModel):
    """Runtime context passed into executors. Stays distinct from ActionCandidate
    (skill-published) and PolicyDecision (operator-owned).

    Executors consult `config` for connection details (kubeconfig, helm repo, etc.).
    """
    run_id: str
    skill_name: str
    environment: Literal["dev", "staging", "production"]
    symptom: str
    findings: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)


class ActionOutcome(BaseModel):
    """Final outcome of attempting an action end-to-end (handler workflow)."""
    candidate_id: str
    action_type: str

    # Terminal status
    status: Literal[
        "success",
        "rejected",            # HITL rejected
        "denied",              # Policy denied
        "failed_execute",      # Execute step raised / returned success=False
        "failed_verification", # Execute succeeded but verify failed
        "error",               # Unexpected error in the workflow shell
        "skipped",             # Skipped (e.g., dry-run only mode)
        "awaiting_approval",   # Saga paused at the durable approval node (ADR-0024 RM3)
    ]

    duration_seconds: float = 0.0

    # Step results — None if step not reached
    dry_run: Optional[DryRunResult] = None
    approval: Optional[ApprovalRecord] = None
    execute_result: Optional[ExecuteResult] = None
    verify_result: Optional[VerifyResult] = None

    compensated: bool = False
    error: Optional[str] = None
    notes: list[str] = Field(default_factory=list)


# ─── Deprecated alias (removed in v0.2) ──────────────────────────────
# Kept for one release so pre-rename importers of ``InvestigationResult``
# keep resolving. New code should import ``RunResult``.
InvestigationResult = RunResult
