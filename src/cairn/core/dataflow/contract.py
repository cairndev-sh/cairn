"""Dataflow contract models (ADR-0081 D2, #1046).

What crosses the control-plane ↔ engine boundary — and nothing else. Mirrors
the ``JobHandle``/``JobStatus`` precedent (:mod:`cairn.core.operator.jobs`):
small strict pydantic, JSON round-trippable (the spec travels to job machines
as env/JSON — the ``CAIRN_FIX_SPEC`` pattern proven live on the fix-remote
path, PR #1044).

Engine-neutrality is a hard rule, not a convention: a field must never enter
``DataflowSpec`` "because engine X needs it" — engine-specific knobs (work
pool, executor, runtime_env) belong in the environment backend config
(``[profile.backends.dataflow.<engine>]``, ADR-0081 D4/D5).
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer, model_validator

# stats/metrics values that may cross the boundary: scalars, or flat lists of
# scalars (e.g. a loss curve). Anything row-shaped is rejected — the R5/D6
# discipline from ADR-0080, enforced at the type layer instead of a lint.
_SCALAR_TYPES = (str, int, float, bool, type(None))


def _assert_scalar_payload(value: dict, *, where: str) -> dict:
    for key, v in value.items():
        if isinstance(v, _SCALAR_TYPES):
            continue
        if isinstance(v, list) and all(isinstance(x, _SCALAR_TYPES) for x in v):
            continue
        raise ValueError(
            f"{where}[{key!r}] is {type(v).__name__} — only scalars or flat "
            "lists of scalars may cross the dataflow boundary (rows/dicts "
            "belong behind an artifact URI; ADR-0081 D3 / ADR-0080 R5)"
        )
    return value


class StageResources(BaseModel):
    """Per-stage resource HINTS (the Metaflow ``@resources`` steal, #1054).

    Engine-neutral: each engine maps them natively (builtin: n_process/limits ·
    k8s Job: requests/limits · Metaflow: ``@resources`` · SkyPilot: task
    resources). Hints, not requirements — ``engine.validate()`` reports
    what the selected engine cannot honor (capability discovery)."""

    model_config = ConfigDict(extra="forbid")

    cpu: Optional[int] = Field(None, ge=1, description="vCPUs the stage wants.")
    memory_mb: Optional[int] = Field(None, ge=1, description="RAM the stage wants, MiB.")
    gpu: Optional[int] = Field(None, ge=0, description="GPUs the stage wants (0 = explicitly none).")
    #: #1366 — coordinated GANG size for ONE distributed stage (data-parallel
    #: training): N workers run the SAME kernel simultaneously and rendezvous
    #: via the env the engine exports (torch-conventional RANK/WORLD_SIZE/
    #: MASTER_* + the CAIRN_DIST_* mirror). Engine-neutral wording on purpose
    #: (ADR-0081): Metaflow maps it to `num_parallel` gang scheduling; an
    #: engine that cannot gang-schedule refuses it in validate(). Distinct
    #: from parallel INDEPENDENT tasks (fan-out — #1290's map), which do not
    #: coordinate. None = not distributed; 1 is meaningless, hence ge=2.
    workers: Optional[int] = Field(
        None, ge=2,
        description="Coordinated gang workers for a distributed stage (e.g. torch DDP).",
    )


class StageMap(BaseModel):
    """#1290/ADR-0090 — bounded fan-out of ONE stage over a runtime list.

    Independent parallel tasks (each item runs the same kernel once, no
    rendezvous) — the opposite of :attr:`StageResources.workers`, which is a
    coordinated gang; a stage carrying both is refused. Semantics mirror the
    agentic ``MapBTNode`` so both kinds stay one concept: a non-list value
    wraps to one item, an empty/missing list runs the merge over zero results.
    The stage list stays LINEAR — the audit is linear per item."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    over: str = Field(
        "shards",
        description="'shards' (the working manifest's ShardRef list) or "
                    "'$config.<key>' (an explicit bounded list in this stage's config).",
    )
    as_: str = Field("item", alias="as",
                     description="Config key the current item is bound under.")
    max_parallel: int = Field(4, ge=1, le=64,
                              description="Pool cap AND wave size — bounded fan-out only.")
    merge: str = Field(
        "stats_sum",
        description="How per-item stats fan in: 'stats_sum' (numeric sum) | "
                    "'concat' (sum + per-item list) | a kernel-declared merge name.",
    )


def _sorted_json(value: Any) -> Any:
    """Recursively key-sorted copy: the bundle is hashed by ``checkpoint_key`` (ADR-0118 D4)."""
    if isinstance(value, dict):
        return {k: _sorted_json(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_sorted_json(v) for v in value]
    return value


class StageContract(BaseModel):
    """ADR-0118 D4 — the stage contract an opted-in stage was admitted with: the contract
    revision, the kernel distribution version, and the URIs of its configuration and
    output schemas in :attr:`SpecContracts.schemas`."""

    model_config = ConfigDict(extra="forbid")

    capability_ref: str = Field(..., min_length=1)
    version: str = ""
    configuration: str = Field(..., min_length=1)
    output: str = Field(..., min_length=1)


class SpecContracts(BaseModel):
    """ADR-0118 D4 — the admission stamp of an opted-in spec: the admitted leg (its declared
    step id, shared by loop iterations) and every self-contained schema its stages name,
    deduplicated by URI and key-sorted."""

    model_config = ConfigDict(extra="forbid")

    format: Literal[1] = 1
    leg: str = Field(..., min_length=1)
    schemas: dict[str, Any] = Field(default_factory=dict)

    @field_validator("schemas")
    @classmethod
    def _deterministic(cls, v: dict) -> dict:
        return _sorted_json(v)


class StageRef(BaseModel):
    """One stage invocation in a dataflow — a registry name + its config.

    Nothing validates ``config`` for a legacy stage (no kernel declares a config
    schema). An opted-in stage carries :attr:`contract` (ADR-0118), and its config and
    raw output are checked against the stage contract on the control plane and in the
    interpreter."""

    model_config = ConfigDict(extra="forbid")

    stage: str = Field(..., min_length=1, description="Stage registry name, e.g. 'dedup.exact'.")
    config: dict[str, Any] = Field(default_factory=dict)
    resources: Optional[StageResources] = None
    #: ADR-0088 D1 (#1248 H6a) — DECLARED durability: after this stage, the
    #: runner snapshots the working set synchronously (D2: a checkpoint that
    #: claims durability is durable when claimed; a FAILED snapshot degrades,
    #: never fails the flow). Off by default — the author opts expensive
    #: stages in; every intermediate of a large flow is not free to persist.
    checkpoint: bool = False
    # #1079 — require approval BEFORE this stage. The materializer SPLITS the
    # flow here (flow A → cairn gate → flow B) rather than asking the engine to
    # suspend: builtin and Metaflow have no suspend primitive, and the split
    # reuses the existing approval queue untouched. A flow carrying this marker
    # cannot be run whole (fail-closed in the compiler) — a dropped gate marker
    # would be a gate that does not gate.
    gate: Optional[str] = Field(
        default=None,
        description="Approval gate name required before this stage runs.",
    )
    #: #1290/ADR-0090 — bounded fan-out of this stage over a runtime list.
    map: Optional[StageMap] = None
    #: ADR-0118 D4 — the admitted stage contract; ``None`` is a legacy stage.
    contract: Optional[StageContract] = None

    @model_serializer(mode="wrap")
    def _omit_legacy_contract(self, handler: Any) -> dict[str, Any]:
        """Omitted while unset, so a legacy stage — and its spec_key — stays byte-identical."""
        data = handler(self)
        if self.contract is None:
            data.pop("contract", None)
        return data

    @model_validator(mode="after")
    def _map_and_gang_are_different_things(self) -> "StageRef":
        # ADR-0090 §2: a mapped item never rendezvouses with its siblings;
        # a gang is nothing but rendezvous. One stage cannot be both.
        if self.map is not None and self.resources and (self.resources.workers or 0) > 1:
            raise ValueError(
                f"stage {self.stage!r}: `map:` (independent fan-out) and "
                "`resources.workers` (coordinated gang) cannot combine — "
                "ADR-0090 §2 / #1366"
            )
        return self


class Budget(BaseModel):
    """Hard ceilings the engine must respect; breach → abort + failed status.

    The generalization of the training learning-gate (ADR-0081 §3 D3): the
    control loop also watches the metrics stream and may cancel from outside,
    but a conforming engine self-enforces these."""

    model_config = ConfigDict(extra="forbid")

    max_cost_usd: Optional[float] = Field(None, gt=0)
    max_rows: Optional[int] = Field(None, gt=0)
    max_seconds: Optional[int] = Field(None, gt=0)


class ResumeFrom(BaseModel):
    """ADR-0088 D3 (#1248 H6a) — seed a FRESH workdir from a durable
    checkpoint and continue past it. Rides the existing engine file contract
    (``CAIRN_DATAFLOW_SPEC``), so every engine path inherits it (#1243).

    ``spec_key`` MUST match the runner's hash of the submitted spec — a
    changed spec invalidates the resume entirely (same validity rule the
    ``.done`` markers follow); the runner then restarts from stage 0 and says
    so in stats. ``completed`` carries each finished stage's FROZEN stats so
    the synthetic markers reproduce what the original run recorded.
    """

    model_config = ConfigDict(extra="forbid")

    spec_key: str = Field(..., min_length=1)
    stage_index: int = Field(..., ge=0, description="Last durably completed stage.")
    artifacts: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Files to seed the workdir with: {uri, content_hash, dest}.",
    )
    completed: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Frozen per-stage stats: {index, stage, stats}.",
    )


class DataflowSpec(BaseModel):
    """The submission payload: WHICH dataflow over WHICH artifacts, plus budget
    and lineage. Either a pack-addressed ``dataflow_ref`` (``<pack>/<name>``,
    first-class in P2a) or inline ``stages`` (the v1 path — the operation
    binding carries the list)."""

    model_config = ConfigDict(extra="forbid")

    dataflow_ref: Optional[str] = Field(
        None, description="Pack-addressed dataflow, e.g. 'dataset-readiness/intake'.",
    )
    stages: list[StageRef] = Field(
        default_factory=list, description="Inline stage list (v1; used when dataflow_ref is unset).",
    )
    input_artifacts: list[str] = Field(
        default_factory=list,
        description="Artifact ids/tags/URIs the dataflow consumes (resolved by the registry).",
    )
    config: dict[str, Any] = Field(
        default_factory=dict, description="Dataflow-scoped config (workflow-owned, ADR-0070).",
    )
    budget: Budget = Field(default_factory=Budget)
    #: ADR-0088 D3 (#1248 H6a) — continue past the last durable checkpoint.
    resume_from: Optional[ResumeFrom] = None
    #: #1314 — control-plane-assigned checkpoint identity. Engines may REWRITE
    #: the spec at submit (SSH `_localize_inputs` turns abs local file configs
    #: into uploaded basenames), so a runner-side hash of the received spec can
    #: diverge from the control plane's lookup key and resume silently misses.
    #: When set, the runner stamps/validates checkpoints with THIS key.
    #: Excluded from the key hash itself (stamping must not change the key).
    checkpoint_key: Optional[str] = None
    parent_run: str = Field(
        "", description="Run lineage (#989): the run this dataflow descends from.",
    )
    engine: Optional[str] = Field(
        None,
        description="Optional engine HINT. The environment backend normally decides "
        "(ADR-0081 D4); a hint never overrides an environment that pins one.",
    )
    #: ADR-0118 D4 — the admission stamp of an opted-in spec; ``None`` is legacy.
    contracts: Optional[SpecContracts] = None

    @model_serializer(mode="wrap")
    def _omit_legacy_contracts(self, handler: Any) -> dict[str, Any]:
        """Omitted while unset, so a legacy spec's JSON and spec_key stay byte-identical."""
        data = handler(self)
        if self.contracts is None:
            data.pop("contracts", None)
        return data

    @model_validator(mode="after")
    def _ref_or_stages(self) -> "DataflowSpec":
        if not self.dataflow_ref and not self.stages:
            raise ValueError("DataflowSpec needs dataflow_ref or inline stages")
        return self

    @model_validator(mode="after")
    def _stage_contracts_are_bundled(self) -> "DataflowSpec":
        schemas = self.contracts.schemas if self.contracts is not None else {}
        for ref in self.stages:
            if ref.contract is not None and not {ref.contract.configuration, ref.contract.output} <= schemas.keys():
                raise ValueError(f"stage {ref.stage!r} names a contract schema the spec does not carry (ADR-0118 D4)")
        return self


class ArtifactDescriptor(BaseModel):
    """What an engine RETURNS about an output artifact. The control plane —
    never the engine — registers it (ADR-0081 D3: job machines get no
    registry/database credentials)."""

    model_config = ConfigDict(extra="forbid")

    uri: str = Field(..., min_length=1, description="Where the bytes live (file://, s3://, …).")
    kind: Literal["dataset", "model", "report", "index"] = "dataset"
    content_hash: str = Field(..., min_length=1, description="Content address (registry id).")
    rows: Optional[int] = Field(None, ge=0)
    size_bytes: Optional[int] = Field(None, ge=0)
    data_schema: Optional[dict[str, Any]] = Field(
        None, description="Schema of the artifact (named data_schema: BaseModel reserves 'schema').",
    )
    parents: list[str] = Field(
        default_factory=list, description="Content hashes of the input artifacts (lineage edges).",
    )
    stats: dict[str, Any] = Field(
        default_factory=dict, description="Write-time stats, frozen — what gates judge.",
    )

    @field_validator("stats")
    @classmethod
    def _stats_scalar(cls, v: dict) -> dict:
        return _assert_scalar_payload(v, where="ArtifactDescriptor.stats")


class DataflowResult(BaseModel):
    """Terminal payload of a successful dataflow: artifacts + scalar stats.

    ``stats`` lands in workflow findings (the gate's food) — scalar-enforced so
    a result can never smuggle rows into graph state."""

    model_config = ConfigDict(extra="forbid")

    output_artifacts: list[ArtifactDescriptor] = Field(default_factory=list)
    #: ADR-0141 — the products a stage NAMES (``outputs: [<name>, …]``, e.g. a split's train,
    #: val and test files), each a sibling of its stage's descriptor in ``output_artifacts``,
    #: which keeps exactly one descriptor per stage, in order.
    named_outputs: list[ArtifactDescriptor] = Field(default_factory=list)
    stats: dict[str, Any] = Field(default_factory=dict)
    cost_usd: float = Field(0.0, ge=0)
    warnings: list[str] = Field(default_factory=list)
    summary: str = ""

    @field_validator("stats")
    @classmethod
    def _stats_scalar(cls, v: dict) -> dict:
        return _assert_scalar_payload(v, where="DataflowResult.stats")

    @model_serializer(mode="wrap")
    def _omit_empty_named_outputs(self, handler: Any) -> dict[str, Any]:
        """Omitted while empty, so a result with no named products stays readable by a control
        plane that predates the field (ADR-0141 D4)."""
        data = handler(self)
        if not self.named_outputs:
            data.pop("named_outputs", None)
        return data
