"""Plan a bundle pipeline (ADR-0069, #746 Phase 1).

:func:`plan_bundle` turns a parsed :class:`~cairn.core.engine.bundle.BundlePipeline`
into a :class:`PipelinePlan` — a topologically-ordered, validated snapshot of the
steps and their declarative trigger-bindings. It is a **pure function**: no I/O, no
dispatch, no graph. The Phase-2 registrar consumes a ``PipelinePlan`` to bind each
step to the bus; :class:`~cairn.core.storage.pipeline_store.PipelineRecord` persists
the plan as its durable ``plan`` snapshot.

This is the declarative counterpart to ``compile_bundle.py`` (which flattens a bundle
into ONE LangGraph). A plan keeps each step a separate governed run — the run boundary
is the governance boundary (ADR-0069 §1).

Effective dependencies:
  - ``depends_on`` given  → that explicit DAG (multi-parent joins allowed).
  - ``depends_on`` empty  → implicit linear: each step depends on the previously
    declared step (the pre-ADR-0069 behaviour, now as separate runs). The first
    step depends on nothing (an entrypoint).

Validation (fail-closed, raises :class:`PipelinePlanError`):
  - every ``depends_on`` id names a real step;
  - no dependency cycle;
  - every ``$steps.<id>.<field>`` referenced in a step's ``gate`` or ``inputs``
    names a step in that step's transitive dependency set (you cannot wire from a
    step that is not guaranteed to have run first).
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, model_serializer

from cairn.core.engine.bundle import BundlePipeline, BundlePipelineStep, wire_refs


class PipelinePlanError(Exception):
    """A bundle pipeline that cannot be planned (bad dep, cycle, dangling wire)."""


class PipelinePlanStep(BaseModel):
    """One planned step — its ref + resolved effective deps + trigger-binding.

    A flattened, execution-ready view of a :class:`BundlePipelineStep`: ``depends_on``
    is the *effective* dependency set (implicit-linear filled in), and ``ref`` is the
    ``pack/template`` the step submits as its own run."""

    id: str
    ref: str  # pack/template
    #: #1288 — the leg's declared wall-clock limit in SECONDS, parsed from the
    #: def's `timeout: "8h"` at plan time. None = no declared limit (flow legs
    #: then poll under the generous pipeline default, task legs run unbounded).
    #: Before this field existed the record's timeout died here: the plan
    #: dropped it and every pipeline flow leg polled under execute_leg's 3600s
    #: default — which CANCELS the engine job on expiry, killing any >1h leg.
    timeout_s: Optional[float] = None
    trigger: str = "auto"
    depends_on: list[str] = Field(default_factory=list)  # effective deps (implicit-linear resolved)
    #: STOP (#1251, ADR-0087 D2). A false `gate` predicate FAILS the pipeline —
    #: governance is fail-closed and a blocked gate is an incident, not a route.
    gate: Optional[str] = None
    #: ROUTE (#1251, ADR-0087 D2). A false `when` predicate SKIPS this leg with
    #: verdict `routed_off` and the pipeline continues. Deliberately a separate
    #: field from `gate`: folding the two meant "run the benchmark only if asked"
    #: killed the whole run.
    when: Optional[str] = None
    #: #1675 — values keep their YAML type (a ``retry:`` map, an inline list);
    #: only strings were wire-lowered. Rows written before carry strings.
    inputs: dict[str, Any] = Field(default_factory=dict)
    #: Input keys whose wire was declared OPTIONAL (`consumes: {x: Report?}`).
    #: When such a wire resolves to nothing — its producer was routed off — the
    #: key is OMITTED from the leg's resolved inputs (the `$config` omit
    #: semantic), so kernel/template defaults apply instead of a silent None.
    optional_inputs: list[str] = Field(default_factory=list)
    environment: Optional[str] = None
    approval: bool = False
    #: #1128's approver context, GATE legs only — and it had to reach the plan
    #: to work at all. ``gate_context_for`` reads ``show``/``approvers``/
    #: ``timeout`` off whatever step it is handed; its unit tests hand it the
    #: AUTHORED :class:`~cairn.core.engine.pipeline.PipelineStep` (which has all
    #: three), while its ONE production caller — the bridge — hands it a plan
    #: step, which carried none of them. So every pipeline gate reached the
    #: queue with an empty field map, an empty audience and no expiry: the
    #: feature was green in tests and inert in production (#1627).
    show: list[str] = Field(default_factory=list)
    approvers: Optional[str] = None
    #: The gate's raw ``timeout:`` string (`timeout: 7d`). Distinct from
    #: ``timeout_s`` above, which is an execution wall-clock limit for a leg;
    #: a gate has no execution to bound. #259 — it is SHOWN to the approver
    #: (``gate_context_for``) but nothing expires a pipeline gate yet: the
    #: approval reaper sweeps runs, and a gate leg has none. Expiry is
    #: ADR-0158 (proposed).
    timeout: Optional[str] = None
    #: #1252 (ADR-0087 P0b) — the POLICY that inserted this gate at plan time,
    #: or None for author-declared steps. Un-removable by the pipeline author
    #: (it is not in their record); surfaced so an approver sees WHY the gate
    #: exists and the audit trail records where it came from.
    injected_by: Optional[str] = None
    #: #1180 — declared produce names, so a flow leg's engine stats can be
    #: mapped onto the names downstream wires reference ($steps.<id>.<name>).
    produces: list[str] = Field(default_factory=list)
    emits: Optional[str] = None
    #: #1787 (ADR-0097) — `finally:`, as a leg. The bridge does NOT advance a
    #: finalizer off its dependencies; it runs every finalizer step once the
    #: pipeline has reached a terminal INTENT (done / failed / cancelled), and
    #: only applies that terminal status to the record once they have settled.
    #: A pipeline with no finalizer step behaves byte-identically to before —
    #: every branch this field opens is guarded on there being one (#1759's
    #: discipline: nothing new where existing machinery serves).
    finalizer: bool = False


class ContractDistributionRef(BaseModel, frozen=True):
    """One installed contract distribution as recorded at the launch door (#2240 C)."""
    name: str
    version: str
    digest: str  # sha256 of the descriptor bytes


class ContractDefinitionRef(BaseModel, frozen=True):
    id: str
    revision: str
    kind: str
    distribution: str
    version: str
    digest: str  # sha256 of the definition bytes


class InvocationEntry(BaseModel, frozen=True):
    """#2241 B — one opted-in task-leg invocation the pipeline was admitted with, keyed
    by its leg id. Records the exact capability revision and the definitions (with
    digests) the leg must validate against; the leg reads this entry from the record and
    never re-resolves. ``profile_digest`` waits for #2248; template operation nodes
    (``<leg>/<node>`` keys) wait for slice C."""
    path: str
    capability_ref: str
    definitions: tuple[ContractDefinitionRef, ...]
    operator_ref: str
    operator_version: str
    #: ADR-0118 D4 — a ``stage`` entry is one opted-in dataflow stage: ``path`` is the leg
    #: id (or ``<leg>/<node path>`` for a ``dataflow/run``/``ml/run`` node), ``stage_index``
    #: and ``stage`` key it, ``operator_ref``/``operator_version`` carry the stage name and
    #: its kernel distribution version. Stage entries never enter the operation lookup.
    surface: Literal["operation", "stage"] = "operation"
    stage_index: Optional[int] = None
    stage: Optional[str] = None

    @model_serializer(mode="wrap")
    def _omit_operation_defaults(self, handler: Any) -> dict[str, Any]:
        """An operation entry serializes exactly as before ADR-0118 (records stay byte-identical)."""
        data = handler(self)
        if self.surface == "operation":
            for key in ("surface", "stage_index", "stage"):
                data.pop(key, None)
        return data


class ContractResolution(BaseModel, frozen=True):
    """#2240 slice C — the static contract closure a pipeline was admitted under,
    taken ONCE at the launch door from the installed contract distributions and
    never re-derived (ADR-0111 acceptance §4). ``None`` on a plan means the closure
    was not recorded at the launch door — rows older than the field, the backfill
    (`storage/backfill_pipelines.py`) and rows the removed bundle-era
    `PipelineBridge.run_bundle` path wrote — i.e. not certified, not wrong.
    Frozen, tuples: immutable once built. Per-invocation bindings (``invocations``) are #2241's additive delta under this same
    ``format``; a different ``format`` is a different contract and refuses to
    hydrate rather than being relabelled legacy."""
    format: Literal["cairn.resolution/1"]
    distributions: tuple[ContractDistributionRef, ...]   # no defaults: a partial blob refuses
    definitions: tuple[ContractDefinitionRef, ...]
    #: #2241 B — additive within format /1: rows written by slice C (no entries)
    #: hydrate as ``()``, which is also a pipeline with no opted-in operator.
    invocations: tuple[InvocationEntry, ...] = ()


class PipelinePlan(BaseModel):
    """A validated, topologically-ordered plan for one bundle pipeline. ``steps`` is
    in a valid execution order (every step appears after all its dependencies); the
    Phase-2 registrar binds them to the bus, and the plan is snapshotted into the
    ``pipelines`` row (:class:`PipelineRecord.plan`)."""

    name: str
    steps: list[PipelinePlanStep] = Field(default_factory=list)
    #: #1688 — the def's `governance:` block, carried verbatim into the plan so
    #: it survives into the durable record. The bridge's spend path reads
    #: `series`/`budget.usd` off that record and hands them to `execute_leg`,
    #: which only reserves against the series ledger WHEN `series_id` is
    #: non-empty — so while this field did not exist, every pipeline's declared
    #: cap was inert and no flow leg ever took a budget reservation. The
    #: consumers were all correct; nothing fed them (reachability, not
    #: capability). Plan-level rather than step-level because governance is a
    #: property of the pipeline, and a snapshotted plan must carry the terms the
    #: run was admitted under even if the def is edited afterwards.
    governance: dict[str, Any] = Field(default_factory=dict)
    #: #1155 — loop name → its authored definition (serialized `PipelineStep`).
    #: The plan holds only iteration 1; the bridge appends later iterations, so
    #: it needs the spec (bound, `until`, `on_exhausted`, body) to decide.
    loops: dict[str, Any] = Field(default_factory=dict)
    #: loop name → the body step ids of the LATEST unrolled iteration, so a
    #: downstream step can be re-pointed at the iteration that actually ran.
    loop_tail: dict[str, list[str]] = Field(default_factory=dict)
    #: #2240 (slice C) — see :class:`ContractResolution`. Inside the record's JSON
    #: blob, so no migration: rows written before this field hydrate to ``None``
    #: (so do records the launch door did not write — see the class docstring).
    contract_resolution: Optional[ContractResolution] = None

    @property
    def entrypoints(self) -> list[str]:
        """Step ids with no dependencies — where the pipeline starts."""
        return [s.id for s in self.steps if not s.depends_on]

    def step(self, step_id: str) -> Optional[PipelinePlanStep]:
        return next((s for s in self.steps if s.id == step_id), None)


def _wired_step_refs(step: BundlePipelineStep) -> set[str]:
    """Step ids referenced via ``$steps.<id>.<field>`` in this step's gate + inputs."""
    refs: set[str] = set()
    for text in [step.gate or "", *step.inputs.values()]:
        refs.update(
            src_id for src_id, _field in
            wire_refs(str(text), scope="steps", source="pipeline plan")
        )
    return refs


def _effective_deps(steps: list[BundlePipelineStep]) -> dict[str, list[str]]:
    """Explicit ``depends_on`` where given, else implicit-linear (prev declared step)."""
    deps: dict[str, list[str]] = {}
    for i, s in enumerate(steps):
        if s.depends_on:
            deps[s.id] = list(s.depends_on)
        else:
            deps[s.id] = [steps[i - 1].id] if i > 0 else []
    return deps


def _toposort(ids: list[str], deps: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm; raises :class:`PipelinePlanError` on a cycle. Preserves
    declaration order among ready nodes so a linear pipeline stays in author order."""
    indeg = {i: 0 for i in ids}
    for i in ids:
        for d in deps[i]:
            indeg[i] += 1
    ordered: list[str] = []
    ready = [i for i in ids if indeg[i] == 0]  # declaration order preserved
    while ready:
        n = ready.pop(0)
        ordered.append(n)
        for i in ids:  # re-scan in declaration order to keep output stable
            if n in deps[i]:
                indeg[i] -= 1
                if indeg[i] == 0:
                    ready.append(i)
    if len(ordered) != len(ids):
        remaining = [i for i in ids if i not in ordered]
        raise PipelinePlanError(f"dependency cycle among steps: {sorted(remaining)}")
    return ordered


def plan_bundle(pipeline: BundlePipeline) -> PipelinePlan:
    """Plan a single bundle pipeline into a validated, ordered :class:`PipelinePlan`.

    Raises :class:`PipelinePlanError` on a duplicate/dangling dep, a cycle, or a
    ``$steps`` wire that references a non-dependency step.
    """
    steps = list(pipeline.step)
    ids = [s.id for s in steps]
    if len(ids) != len(set(ids)):
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        raise PipelinePlanError(f"duplicate step id(s): {dupes}")

    id_set = set(ids)
    deps = _effective_deps(steps)

    # every declared dependency must name a real step
    for sid, ds in deps.items():
        missing = [d for d in ds if d not in id_set]
        if missing:
            raise PipelinePlanError(f"step {sid!r} depends_on unknown step(s): {missing}")

    order = _toposort(ids, deps)  # also detects cycles

    # a $steps wire may only reference a step in the referrer's transitive deps
    reachable = _transitive(deps)
    by_id = {s.id: s for s in steps}
    for sid in ids:
        wired = _wired_step_refs(by_id[sid])
        bad = [r for r in wired if r not in id_set]
        if bad:
            raise PipelinePlanError(f"step {sid!r} wires from unknown step(s): {sorted(bad)}")
        not_upstream = [r for r in wired if r != sid and r not in reachable[sid]]
        if not_upstream:
            raise PipelinePlanError(
                f"step {sid!r} wires from {sorted(not_upstream)} but does not depend on "
                f"them (add to depends_on so they are guaranteed to run first)"
            )

    planned = [
        PipelinePlanStep(
            id=s.id, ref=s.ref, trigger=s.trigger, depends_on=deps[s.id],
            gate=s.gate, inputs=s.inputs, environment=s.environment,
            approval=s.approval, emits=s.emits,
        )
        for s in sorted(steps, key=lambda s: order.index(s.id))
    ]
    return PipelinePlan(name=pipeline.name, steps=planned)


def _transitive(deps: dict[str, list[str]]) -> dict[str, set[str]]:
    """Transitive dependency closure per step (all steps guaranteed to run before it)."""
    memo: dict[str, set[str]] = {}

    def walk(n: str, seen: frozenset[str]) -> set[str]:
        if n in memo:
            return memo[n]
        acc: set[str] = set()
        for d in deps.get(n, []):
            if d in seen:  # a cycle — toposort will raise; avoid infinite recursion here
                continue
            acc.add(d)
            acc |= walk(d, seen | {d})
        memo[n] = acc
        return acc

    return {n: walk(n, frozenset({n})) for n in deps}
