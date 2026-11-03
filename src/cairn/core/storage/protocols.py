"""Run + event store contract — the stateful spine the runner has been missing.

A run exists as a tracked record (``Run``) the moment a trigger is accepted,
not only as a ``RunResult`` at the end. The ``RunStore`` ABC defines the
persistence contract; ``InMemoryRunStore`` and ``SqliteRunStore`` (in their
respective backend modules) implement it.

§24.4 design call: **runs and events stay coupled in one Store.** The
subscribe-with-backlog pattern naturally couples them — you're always
subscribing to a *run's* events, not orphan events. Splitting into separate
``RunStore`` + ``EventStore`` Protocols (as the §24.4 paper sketched) was
considered and deliberately rejected: the existing code already works as one
Store, the split adds complexity for no real benefit at our scale, and Tier-3
swap-ins (Clickhouse for events) can still be implemented as a Store subclass.

Tier hierarchy (mirrored for the rest of the §24.4 stores):
  - Tier 0: :class:`InMemoryRunStore` — dict + lock. Loses in-flight state on
    restart; fine for the realtime lane (the alert source re-fires).
  - Tier 1: :class:`SqliteRunStore` — single embedded file, no service.
    Restart-safe + audit trail. The right default for a single node.
  - Tier 2 (P5): Postgres-backed store, same shape.
  - Tier 3 (P7): optional swap-ins (Clickhouse for events, Temporal for
    durable checkpoints).

Nothing here imports the Runner or LLM machinery — pure state.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncIterator,
    Literal,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso as _now  # `_now`: imported by dispatch/dispatcher.py
from cairn.core.engine.schema import TriggerEvent
from cairn.core.workspace import DEFAULT_WORKSPACE

logger = logging.getLogger(__name__)

#: Payload key carrying a leg's owning pipeline on ``run.*`` bus events (#1532).
#: Same value as :attr:`Run.bundle_pipeline_id`; it rides the MESSAGE so any
#: replica can route a terminal to the right pipeline without a store read or a
#: process-local map. Named once here because the publisher (dispatcher) and the
#: consumer (pipeline bridge) must agree and neither may import the other.
PIPELINE_ID_KEY = "bundle_pipeline_id"

Lane = Literal["realtime", "background"]
Status = Literal[
    "queued",            # accepted, waiting on the lane's concurrency budget
    "running",           # the runner is executing it
    "awaiting_approval", # blocked on HITL (reserved; set by the future interrupt() path — ADR-0024 RM3)
    "done",              # completed successfully
    "failed",            # runner raised
    "cancelled",         # operator/ shutdown cancelled it
    "expired",           # #896 — an awaiting_approval run auto-reaped after its TTL (no decision in time)
]

# Statuses that still count as "in flight" for dedup purposes.
ACTIVE_STATUSES: frozenset[str] = frozenset({"queued", "running", "awaiting_approval"})

# Lower number = scheduled ahead of higher. Realtime preempts background.
_DEFAULT_PRIORITY: dict[str, int] = {"realtime": 10, "background": 100}


def default_priority(lane: Lane) -> int:
    return _DEFAULT_PRIORITY.get(lane, 50)


def _symptom(event: TriggerEvent) -> str:
    """Best one-line identity for an event (mirrors Runner._extract_symptom)."""
    if event.matched_pattern:
        return event.matched_pattern
    for key in ("message", "summary"):
        if key in event.payload:
            return str(event.payload[key])
    return f"{event.source}:{event.service or 'unknown'}"


def fingerprint(event: TriggerEvent) -> str:
    """Stable dedup key for an event.

    Two triggers with the same ``(source, service, symptom)`` produce the same
    fingerprint, so a burst of identical alerts coalesces onto one run instead
    of spawning N LLM loops.

    Cascade events are intentionally *never* coalesced — they are internal
    follow-ups, so we salt them with a uuid to force a unique fingerprint.
    """
    if event.source == "cascade":
        return "cascade-" + uuid.uuid4().hex[:12]
    basis = f"{event.source}|{event.service or ''}|{_symptom(event)}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]


def new_run_id(fp: str) -> str:
    """Human-scannable, collision-resistant id: run-<fp>-<short-uuid>."""
    return f"run-{fp}-{uuid.uuid4().hex[:6]}"


# ─── The record ──────────────────────────────────────────────────────


class RunResultSummary(BaseModel):
    """Light summary of one RunResult kept on the handle.

    The full result still flows to memory / notifiers; this is just enough for
    the status surface so listing runs stays cheap.
    """
    skill_name: str
    template_name: str = ""
    #: §24.11.3 — content-addressed template identity for this skill's
    #: invocation. Empty for runs that pre-date P3.d wiring; populated
    #: by :class:`~cairn.core.engine.runner.Runner` going forward.
    template_hash: str = ""
    duration_seconds: float = 0.0
    action_count: int = 0


class Run(BaseModel):
    """A tracked run — queued, running, or finished."""
    id: str
    trigger: TriggerEvent
    lane: Lane = "realtime"
    priority: int = 50
    status: Status = "queued"
    fingerprint: str = ""

    #: The workflow this run executed, as ``<pack>/<template>`` (#1096, ADR-0083
    #: D3/R2). Written ONCE by the dispatcher — at admission for a direct template
    #: run (the ref is known up front), or as soon as skill matching resolves it
    #: for a trigger-matched run. Authoritative: readers use
    #: :func:`cairn.core.eval.scoring.run_template_ref`, which prefers this field
    #: and only falls back to the legacy three-way inference (results →
    #: trigger.payload → path-like fingerprint) for rows written before #1096.
    #: ``""`` = a legacy row, or a skill run that never resolved a template.
    template_ref: str = ""

    #: The workspace this run belongs to (ADR-0048 rev 2, #223) — the tenancy
    #: scope key. ``"default"`` for single-node / OSS (single implicit
    #: workspace); request-derived per-tenant under SaaS. Stamped at create.
    workspace_id: str = DEFAULT_WORKSPACE

    #: The validated input *values* the run was launched with (#220) — persisted at
    #: create so the run is reproducible/auditable. Secret-marked fields are stored
    #: as references, never raw values (ADR-0033) — redaction lands with the operator
    #: `requires_secrets` declaration; today's template inputs are config and persist
    #: directly. The input *schema* is NOT stored here — the run references it by hash
    #: into the separate content-addressed schema store (ADR-0047), never inline.
    inputs: dict[str, Any] = Field(default_factory=dict)
    #: The environment/profile the run resolved its bindings under (#220).
    environment: str = ""
    #: Reference (hash) into the content-addressed schema store (#220, ADR-0047) —
    #: the I/O schema this run validated against. The schema lives in that store
    #: (data), not here; this pins the run to it so a later reinstall can't rewrite it.
    schema_hash: str = ""
    #: The concrete operators bound under `environment` at run time
    #: (`{capability, operator, risk, requires_approval, …}`) — the env-dependent
    #: half of provenance (the version-intrinsic capabilities live on the installed
    #: -workflow record, not here).
    resolved_operators: list[dict[str, Any]] = Field(default_factory=list)

    created_at: str = Field(default_factory=_now)
    started_at: Optional[str] = None
    ended_at: Optional[str] = None

    #: ADR-0086 H5 (#1247) — the compute TARGET this run's engine dispatches to:
    #: the ``<kind>_target`` backend selection (a ``compute.*`` connection name),
    #: or ``""`` for a run with no target (the local builtin engine, template
    #: runs). Stamped at admission by the pipeline bridge so the per-target
    #: concurrency cap can be counted straight off the run store — the store IS
    #: the bookkeeping, so the count survives a control-plane restart and H1's
    #: orphan reconciler keeps it honest against zombies.
    #:
    #: ADR-0091 tier-2 (#1531): no longer a blob-only field. The SQLite and
    #: Postgres tiers MIRROR it into a real indexed column (see their DDL,
    #: migrated via `add_missing_columns` / `ADD COLUMN IF NOT EXISTS`), because
    #: :meth:`RunStore.count_in_flight_on_target` has to be a store-side
    #: aggregate — the in-Python scan it replaced hydrated up to 500 runs per
    #: admission and silently under-counted past that ceiling, which is an
    #: over-admit, i.e. the exact failure the cap exists to prevent.
    compute_target: str = ""

    #: Liveness stamp for engine-owned legs (ADR-0086 H1, #1239): only a FLOW leg's
    #: poll loop refreshes this, while it watches the engine, so the orphan
    #: reconciler can tell a DEAD run (heartbeat expired) from a slow one. ``None``
    #: for runs that predate the field or whose path doesn't beat — task and
    #: template legs never beat, nor do awaiting_approval waits — and the
    #: reconciler never judges those by silence. Their liveness is ``owner``.
    last_heartbeat_at: Optional[str] = None

    #: ADR-0125 D1 — the process that runs this in-process leg (a task leg, a
    #: template leg on the in-process runtime, a flow leg's poll loop):
    #: ``"<hostname>:<boot id>"`` (``core/liveness.py``). Written when the run is
    #: created for a leg and rewritten when a process takes over running the leg
    #: (re-attach, queued re-arm, read_only re-fire, approved-run resume), never
    #: when it claims or re-maps it (ADR-0126 D2). A standalone run (no
    #: ``bundle_pipeline_id``) is not a leg and names no owner. Restart and the reconciler ask
    #: whether that process still holds its owner lock (``owner_verdict``,
    #: ``runtime/reconciler.py``). ``""`` for a run written before ADR-0125, by a
    #: process with no owner lock, or owned by a durable runtime (Temporal, DBOS),
    #: whose own liveness keeps precedence. Rides the JSON ``data`` blob (no
    #: column/migration), like ``last_heartbeat_at``.
    owner: str = ""

    # How many duplicate triggers coalesced onto this one (0 = none).
    attached_count: int = 0

    #: Structured cost rollup for the whole run (#242), aggregated at finish:
    #: ``{llm_usd, gpu_usd, storage_usd, total_usd}``. NOT an LLM-only scalar —
    #: the fine-tune pilot's dominant cost is GPU, so cost has to break out by
    #: source. ``llm_usd`` is summed from ``RunResult.llm_cost_usd`` (reliable);
    #: ``gpu_usd``/``storage_usd`` are best-effort from result payloads (e.g. a
    #: compute operator's ``actual_usd``), 0 when unknown. Empty until finish.
    cost: dict[str, float] = Field(default_factory=dict)

    #: The OTel trace_id (32-hex) of this run's root ``cairn.run`` span (#248),
    #: captured at dispatch when tracing is active. Empty string when tracing is
    #: off (no exporter configured) — the console hides the "View in Langfuse"
    #: deep-link in that case. Like ``cost``/``inputs`` this rides in the store's
    #: JSON ``data`` blob (it's neither filtered nor indexed on), so no schema
    #: column / migration is needed — new model fields serialize along for free.
    trace_id: str = ""

    #: ADR-0069 (#746) — when this run is a step of a bundle pipeline, the owning
    #: pipeline id + the step id it ran as. Empty for a standalone run. Like
    #: `cost`/`trace_id` these ride in the store's JSON `data` blob (new model
    #: fields serialize along for free — no column/migration); the core
    #: PipelineBridge reads them off a completed run to advance the pipeline, and
    #: the console groups a pipeline's child runs by `bundle_pipeline_id`. NB: this
    #: is NOT `OperationContext.pipeline_id` (that names the in-graph run identity).
    bundle_pipeline_id: str = ""
    step_id: str = ""

    #: #989 (ADR-0076, D1) — run lineage. The id of the run this run was derived
    #: from (`cairn run --from <run-id>` / `POST …/run?from=<run-id>`), so a hand-
    #: driven loop `dataset-eval → dataset-fix → finetune → promote` forms a
    #: provenance CHAIN, not orphaned runs. Empty for a root run. Unlike
    #: `bundle_pipeline_id` (a *formal* pipeline membership), this is an *ad-hoc*
    #: parent pointer the user sets. Ancestry walks up via `store.get(parent_run)`
    #: (each hop hydrates the blob — no index needed), so like `trace_id`/`cost`
    #: it rides the JSON `data` blob free — no column/migration.
    parent_run: str = ""

    #: ADR-0082 P0 (#1072) — reproducibility pins stamped at train-submit time,
    #: one entry per submitting node: ``{node, operator, pinned_at, dataset_uri,
    #: dataset_hash, image, image_digest, spec}``. Written by the remote-job
    #: runtime the moment the job is dispatched, so the SUBMISSION stays
    #: reproducible even when the job later fails or is cancelled. Rides the
    #: JSON `data` blob free (no column/migration), like `cost`/`trace_id`.
    train_pins: list[dict[str, Any]] = Field(default_factory=list)

    #: ADR-0071 (#788) — the config preset applied to this run, ``{name, version}``,
    #: or empty when none was selected/default. The default preset is resolved by the
    #: dispatcher for EVERY run (incl. triggers, which have no run-form), so audit /
    #: pilot can show "ran under preset cheap v3". Rides the JSON `data` blob free
    #: (no column/migration), like `cost`/`trace_id`/`bundle_pipeline_id`.
    config_preset: dict[str, Any] = Field(default_factory=dict)

    #: ADR-0073 (#826) — a sandbox (Playground) run. When set, the dispatcher runs
    #: the graph in operator MOCK mode (every ``supports_mock`` operator takes its
    #: side-effect-free ``.mock()`` path), so writes/notifications never fire and
    #: spend is ~0. Tagged so the console badges it ``sandbox`` and Insights can
    #: exclude it. Rides the JSON `data` blob free (no column/migration), like
    #: `config_preset`/`cost`/`trace_id`.
    sandbox: bool = False

    results: list[RunResultSummary] = Field(default_factory=list)
    #: Full structured RunResult(s) as model_dump(mode="json") dicts — the DB
    #: "result backend" (#76): carries the template_hash + template_ast (shape/
    #: schema) + rca/triage/raw_output/actions, queryable via GET /runs/{id}.
    #: Persisted in the store's `data` blob (no migration). EXCLUDED from the
    #: /runs list projection so listing stays cheap. ADDITIVE to — not a
    #: replacement for — the on-disk .cairn/runs/<id>/ JSONL trace that
    #: `cairn replay` reads.
    result_payloads: list[dict[str, Any]] = Field(default_factory=list)
    matched_skills: int = 0
    error: Optional[str] = None

    #: Eval ledger delta (ADR-0059 §D3, #244/#474). Populated by the failure
    #: taxonomy / eval-recipe modules, not by the runner itself — this store
    #: only persists what it's given. Rides in the JSON `data` blob like
    #: `cost`/`trace_id`, but the SQLite/Postgres run stores ALSO mirror these
    #: three out into real columns (see their DDL, migrated via
    #: `add_missing_columns` / `ADD COLUMN IF NOT EXISTS`) so they're
    #: queryable/gate-able without deserializing every row's JSON.
    #:
    #: Failure category tags for this run, e.g. ["TOOL_FAILURE"] (#474's
    #: taxonomy vocabulary; empty = not yet classified or no failure).
    failure_type: list[str] = Field(default_factory=list)
    #: Counterfactual "what this would have cost without cairn" (Slice F) —
    #: None when not computed for this run.
    cost_baseline_usd: Optional[float] = None
    #: Final eval verdict for the run: "pass" | "fail" | "degraded" | "" (not evaluated).
    final_verdict: str = ""
    #: #497 (ADR-0061) — knowledge chunks injected into this run's context, each
    #: cited by source pack + corpus + locus + score. Empty when the workflow
    #: declared no knowledge slot or nothing was retrieved.
    citations: list[dict[str, Any]] = Field(default_factory=list)

    #: #2020 (ADR-0102 D3) — the holdout door's verdict, stamped at admission:
    #: ``{checked, ok, near_dup_checked, training_versions, eval_versions,
    #: overlaps, warnings}``, where every ``eval_versions`` entry carries the
    #: eval set's ``ref`` AND its ``content_hash``. This is how a finished model
    #: can be asked which holdouts it was held out FROM, at which bytes — the
    #: pin research C found no platform records. ``checked`` is False with the
    #: reason in ``warnings`` when the door could not check; empty only when it
    #: had nothing to say. Rides the JSON ``data`` blob free (no column/migration),
    #: like ``cost``/``trace_id``/``config_preset``.
    holdout: dict[str, Any] = Field(default_factory=dict)

    #: #2021 (ADR-0102 D1) — the intake verdict of every dataset version this
    #: run reads: ``{checked, ok, versions: [{ref, verdict, reasons, note}],
    #: warnings}``. ``blocked`` and ``invalid_format`` never get this far (the
    #: door refuses at admission); what lands here is the ``degraded`` reasons
    #: a human should see beside the result, and the note on a version that
    #: predates verdicts. Rides the JSON ``data`` blob like ``holdout``.
    intake: dict[str, Any] = Field(default_factory=dict)


# ─── Append-only event log (streaming partial outputs) ───────────────


class RunEvent(BaseModel):
    """One entry in a run's append-only event log.

    The log is the streaming spine: evidence, lifecycle transitions, action
    decisions and outcomes are appended *as they happen* so a consumer can
    follow an in-flight run instead of blocking on the final ``RunResult``.

    ``seq`` is a per-run monotonic counter (1-based) — subscribers use it to
    deduplicate the catch-up backlog against the live tail and to resume a
    dropped SSE stream via ``?after_seq=N``.
    """
    run_id: str
    seq: int
    ts: str = Field(default_factory=_now)
    type: str   # "queued"|"running"|"skill_started"|"findings"|
                # "action_decision"|"action_outcome"|"skill_completed"|
                # "done"|"failed"|"cancelled"
                # + "node" (#183 per-node completion) and "stage" (ADR-0082
                # D8a, #1076: engine-side flow stage progress {node, stage,
                # stages_done, stages_total} bridged from remote-job polls)
    data: dict[str, Any] = Field(default_factory=dict)


# Event types that close a run's stream — a subscriber stops after one.
TERMINAL_EVENT_TYPES: frozenset[str] = frozenset({"done", "failed", "cancelled", "expired"})


class _EventHub:
    """In-process fan-out for live run events: one queue per subscriber.

    The store owns *persistence* (the backlog); the hub only handles live
    delivery to in-process subscribers (SSE handlers). Cross-process streaming
    would need a broker (Redis / NATS) — out of scope for the single-node
    default, same tier as the run store itself.
    """

    def __init__(self) -> None:
        self._subs: dict[str, set[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    async def register(self, run_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._subs.setdefault(run_id, set()).add(q)
        return q

    async def unregister(self, run_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            subs = self._subs.get(run_id)
            if subs is not None:
                subs.discard(q)
                if not subs:
                    self._subs.pop(run_id, None)

    async def publish(self, event: RunEvent) -> None:
        async with self._lock:
            queues = list(self._subs.get(event.run_id, ()))
        for q in queues:
            q.put_nowait(event)


# ─── Store ───────────────────────────────────────────────────────────


#: Ceiling on the LEGACY in-Python target count (:func:`scan_in_flight_on_target`).
#: Kept only for stores that do not implement the store-side aggregate; a store
#: with more than this many runs in one slot-holding status silently under-counts,
#: which is precisely why the real tiers count in SQL now (#1531).
SCAN_COUNT_LIMIT = 500


async def scan_in_flight_on_target(
    store: Any,
    target: str,
    statuses: Sequence[str],
    *,
    exclude_run_id: str = "",
    limit: int = SCAN_COUNT_LIMIT,
) -> int:
    """Count runs holding ``target`` by LISTING and filtering in Python.

    The pre-#1531 algorithm, kept in exactly one place: it is both the default
    :meth:`RunStore.count_in_flight_on_target` body (so a third-party ``RunStore``
    subclass keeps working without implementing anything) and the fallback the
    dispatch layer uses for a duck-typed store that predates the method.

    Truncating at ``limit`` makes it approximate above that many runs — do not
    use it for a new tier; implement the aggregate.
    """
    name = str(target or "").strip()
    if not name:
        return 0
    seen: set[str] = set()
    for status in statuses:
        for run in await store.list(status=status, limit=limit):
            rid = str(getattr(run, "id", "") or "")
            if rid and rid != exclude_run_id and \
                    str(getattr(run, "compute_target", "") or "") == name:
                seen.add(rid)
    return len(seen)


class RunStore(ABC):
    """Persistence contract for runs. Impls must be concurrency-safe.

    Subclasses MUST set ``self._hub = _EventHub()`` so the concrete
    ``subscribe`` method below can deliver live events.
    """

    #: In-process live-event fan-out. Subclasses construct one in __init__.
    _hub: _EventHub

    @abstractmethod
    async def create(self, run: Run) -> None: ...

    @abstractmethod
    async def update(self, run_id: str, **fields) -> Run:
        """Patch fields and return the updated record. Raises KeyError if absent."""

    @abstractmethod
    async def get(self, run_id: str, workspace_id: Optional[str] = None) -> Optional[Run]:
        """Fetch one run. When ``workspace_id`` is given, a run from another
        workspace is treated as not-found (query-enforced scoping, ADR-0051 §4)."""

    @abstractmethod
    async def list(
        self,
        *,
        lane: Optional[Lane] = None,
        status: Optional[Status] = None,
        workspace_id: Optional[str] = None,
        limit: int = 100,
        include_payloads: bool = True,
    ) -> list[Run]: ...
    #: ``include_payloads`` (#1508): a tier MAY project ``result_payloads`` out
    #: of list rows, since list views strip them at serialization anyway and
    #: they are persisted uncapped. Tiers that cannot project cheaply return
    #: them — that is the safe direction, because the field defaults to ``[]``
    #: and a caller reading a projected row sees "no payloads" rather than an
    #: error. Any caller that READS payloads must pass ``True`` explicitly.

    @abstractmethod
    async def find_active_by_fingerprint(self, fp: str) -> Optional[Run]:
        """Return an in-flight (queued/running/awaiting) run for ``fp``."""

    async def list_pipeline_less_runs(
        self,
        *,
        after_id: str = "",
        limit: int = 200,
        workspace_id: Optional[str] = None,
    ) -> list[Run]:
        """One keyset page of runs that belong to NO pipeline (#2128 item C).

        The scan side of the history backfill
        (:mod:`cairn.core.storage.backfill_pipelines`): runs whose
        ``bundle_pipeline_id`` is empty, ordered by ``id`` ascending, starting
        strictly after ``after_id``.

        **Keyset, not ``OFFSET``.** ``id`` is the primary key of every tier, so
        the page is index-served, a prod-sized table never lands in memory, and
        a run this pass cannot backfill (no ``template_ref``) is stepped OVER
        rather than re-fetched — which an ``OFFSET 0`` loop over a shrinking
        predicate would do forever.

        ``bundle_pipeline_id`` rides the JSON ``data`` blob (it is neither
        filtered nor indexed on anywhere else), so each tier expresses the
        predicate against the blob — ``json_extract`` on SQLite, ``->>`` on
        Postgres. It is a one-shot operator sweep, not a hot path, so the
        sequential scan that costs is the right trade against an index that
        would be dead weight for the life of the table.

        The default body is the in-Python scan over :meth:`list`, capped at
        :data:`SCAN_COUNT_LIMIT` — correct only for the in-memory tier and small
        stores. It is a NON-abstract addition on purpose: an out-of-tree
        ``RunStore`` subclass must not stop instantiating because of a new
        method (the same reason :meth:`count_in_flight_on_target` has a body).
        """
        rows = await self.list(limit=SCAN_COUNT_LIMIT, workspace_id=workspace_id,
                               include_payloads=False)
        out = [r for r in rows if not r.bundle_pipeline_id and r.id > after_id]
        out.sort(key=lambda r: r.id)
        return out[:limit]

    async def count_in_flight_on_target(
        self,
        target: str,
        statuses: Sequence[str],
        *,
        exclude_run_id: str = "",
    ) -> int:
        """How many runs stamped with ``compute_target == target`` are in ``statuses``.

        The store-side half of the ADR-0086 H5 per-target concurrency cap
        (#1247), made exact and fleet-wide in #1531. Every tier that can express
        it MUST override this with a single indexed aggregate: the caller runs it
        once per admission decision *inside* the admission lock, and it must be
        EXACT — an under-count admits a leg the cap should have held.

        ``statuses`` is a parameter, not a constant, because *which* statuses hold
        a slot is a dispatch-layer policy
        (:data:`cairn.core.dispatch.target_concurrency.IN_FLIGHT_STATUSES` — a
        ``queued`` leg is one WAITING for a slot, so counting it would make the
        cap self-blocking). The storage layer must not pin that vocabulary.

        ``exclude_run_id`` drops the asking leg's own run, which is what lets a
        leg that already exists as ``queued`` wait for a slot without blocking on
        itself. It is an id, not a status filter, so a store implements it as one
        more predicate.

        The default body is the legacy in-Python scan
        (:func:`scan_in_flight_on_target`) — correct but capped at
        :data:`SCAN_COUNT_LIMIT`, and it hydrates every listed run. It exists so
        this stays a NON-abstract addition (an out-of-tree ``RunStore`` subclass
        must not stop instantiating because of a new method), not because it is
        an acceptable implementation for a real tier.
        """
        return await scan_in_flight_on_target(
            self, target, statuses, exclude_run_id=exclude_run_id
        )

    # ── Append-only event log ────────────────────────────────────────

    @abstractmethod
    async def append_event(
        self, run_id: str, type: str, data: Optional[dict] = None
    ) -> RunEvent:
        """Append an event to ``run_id``'s log and notify live subscribers.

        Assigns the next ``seq`` atomically. Persistence is impl-specific;
        live delivery flows through ``self._hub``.
        """

    @abstractmethod
    async def get_events(
        self, run_id: str, *, after_seq: int = 0
    ) -> list[RunEvent]:
        """Return persisted events for ``run_id`` with ``seq > after_seq``."""

    async def subscribe(
        self, run_id: str, *, after_seq: int = 0
    ) -> AsyncIterator[RunEvent]:
        """Yield this run's events: the catch-up backlog first, then live.

        Registers the live queue BEFORE reading the backlog so no event
        published in between is lost; overlap is de-duplicated by ``seq``.
        Returns once a terminal event (done/failed/cancelled) is emitted, so
        an SSE handler's generator completes naturally when the run finishes.

        #1574 — it also returns when the run ALREADY finished at or before
        ``after_seq``. The rule was "return once a terminal event is emitted",
        which is silent about resumption: a caller resuming a finished run has
        an empty backlog, emits nothing, and would poll an ended run forever
        holding the connection open. That became reachable once the SSE endpoint
        started honouring ``Last-Event-ID``.
        """
        q = await self._hub.register(run_id)
        try:
            last = after_seq
            saw_backlog = False
            for ev in await self.get_events(run_id, after_seq=after_seq):
                saw_backlog = True
                last = ev.seq
                yield ev
                if ev.type in TERMINAL_EVENT_TYPES:
                    return
            if not saw_backlog:
                # Caught up. Only now is the full log worth a read — and only for
                # a run with nothing pending, so it is bounded and rare.
                log = await self.get_events(run_id)
                if log and log[-1].type in TERMINAL_EVENT_TYPES and log[-1].seq <= after_seq:
                    return
            while True:
                # #967 — the in-process ``_hub`` only sees events appended by THIS
                # process. Under Temporal the node events are appended by the
                # WORKER process, so the server's SSE would never tail them live.
                # When the hub is idle, poll the SHARED store for events another
                # process wrote (seq > last). Same-process runs still get instant
                # hub delivery; this only adds a ~1.5s idle poll for cross-process.
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.5)
                except asyncio.TimeoutError:
                    for ev in await self.get_events(run_id, after_seq=last):
                        last = ev.seq
                        yield ev
                        if ev.type in TERMINAL_EVENT_TYPES:
                            return
                    continue
                if ev.seq <= last:
                    continue  # already delivered via backlog / poll
                last = ev.seq
                yield ev
                if ev.type in TERMINAL_EVENT_TYPES:
                    return
        finally:
            await self._hub.unregister(run_id, q)


# ─── Episodic memory store ──────────────────────────────────────────


@runtime_checkable
class MemoryStore(Protocol):
    """Vendor-agnostic contract for episodic memory.

    Today's only impl wraps :mod:`cairn.services.memory.store` (Qdrant).
    Future P4.8 lands ``SqliteVecMemoryStore`` (Tier 1: sqlite-vec), P5 lands
    ``PgvectorMemoryStore`` (Tier 2: Postgres + pgvector), and the existing
    Qdrant adapter becomes a Tier 3 swap-in via ``CAIRN_MEMORY=qdrant://…``.

    Payload is an arbitrary dict; the impl stamps ``type=episodic_memory``
    plus ``created_at`` / ``updated_at`` if absent. Cosine-similarity dedup
    happens at the impl layer (today, threshold 0.85).
    """

    async def save(
        self,
        payload: dict[str, Any],
        *,
        embed_text: str,
        dedup_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Upsert with cosine-similarity dedup.

        ``payload`` is stored verbatim; ``embed_text`` is what gets embedded
        for similarity search + dedup; ``dedup_id`` overrides the auto-derived
        stable id.

        Returns ``{"action": "created"|"merged", "id": str, "detail": str}``.
        """
        ...

    async def recall(
        self,
        query: str,
        *,
        top_k: int = 5,
        filters: Optional[dict] = None,
    ) -> list[dict[str, Any]]:
        """Semantic search over stored memories.

        ``filters`` is an impl-specific filter dict (Qdrant filter syntax for
        the legacy adapter; SQL ``WHERE`` clauses for sqlite-vec / pgvector).
        Returns matching payloads with a ``score`` field added per hit.
        """
        ...

    async def list(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Enumerate stored memories (most recent first when impl supports ordering).

        Operator/debugging surface — production reads should use
        :meth:`recall` for relevance ranking.
        """
        ...


# ─── RAG store ──────────────────────────────────────────────────────


@runtime_checkable
class RagStore(Protocol):
    """Pack-facing RAG contract: text-in, grounded-answer-out.

    The Protocol sits ABOVE the low-level ``VectorStore`` driver (in
    :mod:`cairn.services.rag.stores.base`). VectorStore takes
    pre-embedded vectors; RagStore takes raw text and owns the full
    embed → retrieve → rerank → synthesize pipeline.

    Today's only impl wraps :func:`cairn.services.rag.query.search` and
    :func:`cairn.services.rag.ingest.ingest_document` (hybrid dense+sparse
    retrieval, LLM-based reranking + synthesis). Future P4.8 adds
    ``SqliteVecRagStore``; P5 adds ``PgvectorRagStore``; the existing
    Qdrant/Pinecone drivers become Tier-3 swap-ins under VectorStore.

    Multi-collection by design: packs each own one or more collections
    (incident pack has ``runbooks``, ``service-docs``; comms might have
    ``policy-docs``). The ``collection`` arg picks which one to query.
    """

    async def search(
        self,
        query: str,
        *,
        collection: Optional[str] = None,
        top_k: int = 3,
        synthesis_prompt: Optional[str] = None,
        job_tag: str = "rag",
    ) -> dict[str, Any]:
        """Hybrid-search a vector collection and synthesize a grounded answer.

        Returns ``{"answer": str, "sources": list[dict]}``. When the
        collection is unavailable, returns a graceful "knowledge base
        not available" answer instead of raising.

        ``synthesis_prompt`` is an override for the default grounded-answer
        template; ``job_tag`` is a budget/observability label.
        """
        ...

    async def ingest_document(
        self,
        file_path: str,
        *,
        collection: Optional[str] = None,
    ) -> str:
        """Ingest one document into a vector collection.

        Returns an opaque ingest job/result identifier. Pack code typically
        calls this from an admin/operator path, not from inside a template.
        """
        ...
