"""Execute one pipeline leg (#1127, ADR-0084 D1).

**Where the architecture becomes executable.** Before this, the bridge submitted
every step through ``run_template``, so a data or ml leg rode a LangGraph state
machine whose only job was to hold a door node while an engine worked elsewhere —
making a graph runtime a mandatory dependency of pure data pipelines.

A ``flow:`` leg now goes: **resolve the environment's engine → validate → reserve
budget → submit → poll → result**. No graph runtime, no template wrapper, no
Temporal. The module imports nothing from ``langgraph`` (a test asserts the
absence, because otherwise "LangGraph is not on the data path" is a design
statement nobody checks).

Durability at this tier is the run record plus the engine handle: the control
plane holds only *position*, the engine owns the work, and a restart re-attaches
(ADR-0084 D6).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from cairn.core.budget_reservations import (
    BudgetExceeded, held_for_run, release, reserve, settle, settled_usd_for_run,
)
from cairn.core.dataflow import DataflowSpec
from cairn.core.runtime.retry import RetryPolicy
from cairn.core.dataflow.engine import is_terminal

logger = logging.getLogger("cairn.core.dispatch.leg_executor")

FLOW_PREFIX = "flow:"

#: How often the control plane asks the engine where it is. The engine owns the
#: work; this loop owns only the position. Starts here, backs off ×1.5 per idle
#: poll to _POLL_MAX_S (#1180 bug 11 — a fixed 1s tick fd-exhausted the worker
#: 40 min into an SSH-dispatched train leg).
_POLL_INTERVAL_S = 1.0
_POLL_MAX_S = 20.0


class LegKindError(ValueError):
    """A leg ref that cannot be interpreted."""


@dataclass
class LegOutcome:
    """The result of one leg — a run outcome the pipeline decides about."""

    state: str = "failed"
    stats: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    handle: Any = None
    #: #1441/#131 — known spend the engine reported for this leg, including
    #: partial cost from failed attempts and earlier attempts replaced by retry.
    cost_usd: float = 0.0
    #: ADR-0151 D5 — the box time of a leg on a priced target: ``hourly_usd`` ×
    #: each attempt's wall-clock, summed over the attempts, on ANY outcome. Kept
    #: apart from ``cost_usd`` (engine-reported, LLM) so the two never fold.
    gpu_usd: float = 0.0

    @property
    def ok(self) -> bool:
        return self.state == "succeeded"


#: D0 ref prefix for operator (task) legs — mirrors ``flow:``/``gate:`` (#1278).
TASK_PREFIX = "task:"


def leg_kind_of(ref: str) -> str:
    """``flow``/``gate``/``task`` by D0 ref prefix, ``template`` otherwise
    (#1180, task legs #1278)."""
    raw = str(ref or "")
    if raw.startswith(FLOW_PREFIX):
        return "flow"
    if raw.startswith("gate:"):
        return "gate"
    if raw.startswith(TASK_PREFIX):
        return "task"
    return "template"


def parse_flow_ref(ref: str, *, strict: bool = False) -> Optional[tuple[str, str]]:
    """Split ``flow:<pack>/<name>`` into ``(pack, name)``; ``None`` if not a flow ref."""
    raw = str(ref or "")
    if not raw.startswith(FLOW_PREFIX):
        return None
    pack, sep, name = raw[len(FLOW_PREFIX):].partition("/")
    if not (pack and sep and name):
        if strict:
            raise LegKindError(
                f"malformed flow leg ref {ref!r} — expected `flow:<pack>/<name>`")
        return None
    return pack, name


def flow_kind_of(ref: str) -> str:
    """The declared kind (``data`` | ``ml``) of the flow a leg ref names.

    ``data`` for anything that is not a ``flow:`` leg, and for a def that
    cannot be read — the same fallback the bridge has always used.

    It lives here, beside :func:`leg_kind_of`, because two callers need it now:
    the bridge (which engine to RESOLVE for this leg) and the pipeline service
    doors (#2101 — which engine the LAUNCH has to check). A second copy of this
    lookup is exactly how an ml leg was once handed the data engine (#1180), so
    there is one function and no copy.
    """
    from cairn.core.dataflow.defs import flow_def_for  # noqa: PLC0415 - cycle: defs → pack registry

    raw = str(ref or "")
    if parse_flow_ref(raw) is None:
        return "data"
    # #1792 — the same lookup the leg executor and the risk read use (it owns
    # the dir-vs-manifest name split, #1180 bug 6).
    return getattr(flow_def_for(raw), "kind", None) or "data"


def recorded_stage_contracts(plan: Any, step_id: str) -> Optional[dict]:
    """ADR-0118 D3/D4 — the stage contracts a leg was admitted with, read from the record
    (never re-resolved) for both doors: ``{"leg", "nodes": {<node path, "" for a flow
    leg>: {<stage index>: {stage, contract, schemas}}}}``, each part bundled into one
    self-contained schema. Loop iterations share the declared step's entries (#2344), and
    ``leg`` is that DECLARED step (``prep``, never ``prep#2``): it is what the spec stamp
    carries, so iterations share a ``checkpoint_key`` as legacy iterations do (N5).
    ``None`` for a plan with no recorded resolution (legacy rows, the dev loop). A drifted
    installation is not raised here: it comes back as ``error`` and refuses at the door."""
    from cairn.core.dataflow.conformance_check import bundle  # noqa: PLC0415 - cycle: dataflow → contract ← this module
    from cairn.core.engine.pipeline_loop import declared_step_id  # noqa: PLC0415 - engine helper, lazy like its siblings

    resolution = getattr(plan, "contract_resolution", None)
    if resolution is None:
        return None
    declared = declared_step_id(step_id, getattr(plan, "loops", None) or {})
    entries = [e for e in resolution.invocations
               if e.surface == "stage" and (e.path == declared or e.path.startswith(f"{declared}/"))]
    out: dict[str, Any] = {"leg": declared, "nodes": {}}
    if not entries:
        return out
    from cairn.core.pack.contracts import ContractDiscoveryError  # noqa: PLC0415 - cycle: dispatch → pack, lazy like its siblings
    from cairn.core.pack.registry import invocation_contract  # noqa: PLC0415 - as above

    closure = [(f"contract://{d.id}/{d.revision}", d.digest) for d in resolution.definitions]
    try:
        for e in entries:
            data = invocation_contract([(f"contract://{d.id}/{d.revision}", d.digest) for d in e.definitions], closure)
            refs = data["refs"]
            out["nodes"].setdefault(e.path[len(declared) + 1:], {})[str(e.stage_index)] = {
                "stage": e.stage,
                "contract": {"capability_ref": data["capability_ref"], "version": e.operator_version,
                             "configuration": refs["configuration"], "output": refs["output"]},
                "schemas": {uri: bundle(data["schemas"], uri) for uri in (refs["configuration"], refs["output"])}}
    except ContractDiscoveryError as exc:
        return {"leg": declared, "nodes": {}, "error": f"leg {step_id!r}: its admitted stage contracts cannot be read: {exc}"}
    return out


async def execute_leg(
    ref: str,
    *,
    inputs: dict[str, Any],
    engine: Any,
    run_id: str,
    kind: str = "data",
    series_id: str = "",
    estimated_usd: float = 0.0,
    spent_usd: Optional[float] = 0.0,
    budget_usd: float = 0.0,
    poll_budget_s: float = 3600.0,
    on_submit: Optional[Callable[[Any], Awaitable[None]]] = None,
    heartbeat: Optional[Callable[[], Awaitable[None]]] = None,
    retry: Optional[RetryPolicy] = None,
    on_retry: Optional[Callable[[int, str], Awaitable[None]]] = None,
    # H6c (#1248) — async (spec_key) -> Optional[ResumeFrom]; on_resume fires
    # when a checkpoint is attached so the caller can audit it.
    resume_provider: Optional[Callable[[str], Awaitable[Any]]] = None,
    # #1316 — control-side registration of the engine's DESCRIBED outputs
    # (door parity, ADR-0080/0082 D3: engines get no registry credentials).
    # None = caller owns the registry decision (hermetic tests); the bridge
    # always passes its store.
    artifact_store: Any = None,
    on_resume: Optional[Callable[[Any], Awaitable[None]]] = None,
    # #1429 (ADR-0086 H1 adopt-by-handle) — a handle a PREVIOUS owner process
    # persisted at submit: the first attempt RE-ATTACHES (poll → result →
    # register) instead of submitting again, so a restart never duplicates
    # paid, in-flight compute. Only if that attempt fails retriably do later
    # attempts submit fresh — seeded by resume_provider (H6c).
    adopt: Optional[dict] = None,
    on_adopt: Optional[Callable[[dict], Awaitable[None]]] = None,
    # ADR-0118 D4 — the stage contracts this leg was admitted with, from the record
    # (`recorded_stage_contracts`); None = no record (legacy) and the spec is untouched.
    stage_contracts: Optional[dict] = None,
    # ADR-0151 D5 — the RECORDED rate of a leg on a priced box (0 = not priced) and,
    # for an adopted attempt, the persisted start it is priced from (`run.started_at`).
    hourly_usd: float = 0.0,
    adopted_started_at: Optional[str] = None,
) -> LegOutcome:
    """Run one ``flow:`` leg on the environment's engine.

    Ordering is deliberate and load-bearing:

    1. **validate** — capability discovery (ADR-0081 D3): what THIS engine cannot
       run is caught before a job is burning, not after.
    2. **reserve** — the budget claim happens BEFORE submit, or the race #1094
       exists to close simply reopens one tier up.
    3. **submit → poll → result** — the engine owns the loop; this owns position.
    4. **settle or release** — a failed leg that keeps its reservation leaks
       budget until the series can run nothing at all. An attempt that cost
       something settles it, on any outcome; one that cost nothing releases
       (ADR-0151 D5: a priced box bills for every attempt's wall-clock).

    Never raises for an execution failure: a failed leg is an outcome the
    pipeline decides about, not a crash that takes the control plane with it.
    """
    parsed = parse_flow_ref(ref)
    if parsed is None:
        return LegOutcome(state="failed", error=f"not a flow leg ref: {ref!r}")
    pack, name = parsed

    # Resolve the pack flow ref to its STAGE LIST before submitting. The door
    # operator did this (run.py:58) and engines never learned to — an engine
    # interprets a stage list, it does not read pack assets. Found by driving
    # the executor against the real builtin engine: it rejected the bare ref.
    #
    # #1180 — resolve by the flow's OWN declared kind, not a pinned "data".
    # This path called resolve_dataflow_ref (the kind:data back-compat door),
    # so a `kind: ml` leg was structurally impossible: the first two-leg live
    # run failed with "this door only runs kind: data flows". A pipeline leg is
    # kind-agnostic on purpose — the flow def, not the leg, declares its kind
    # (ADR-0082 D7 kind-matched doors still hold: the def's kind is what routes
    # engine selection).
    from cairn.core.dataflow.defs import (  # noqa: PLC0415 - cycle: defs → pack registry → operators
        load_flow_defs, resolve_flow_key, resolve_flow_ref,
    )

    config = dict(inputs or {})
    # #1466 — dataset refs resolve HERE, on the control plane (remote boxes
    # have no DB access, D4): `dataset:<name>[@N]` → the registered URI with
    # its content hash as a fragment, before $config substitution bakes the
    # values into stage configs. An unknown ref fails the leg at launch.
    # #1625 — the gate is the resolver's OWN predicate (config_has_dataset_ref),
    # so a form the rewriter handles can never be one the gate skips: the inline
    # check here saw only top-level strings, so `dataset/replay`'s LIST of
    # sources reached the box unresolved.
    from cairn.core.storage.artifacts import (  # noqa: PLC0415 - cycle: storage → dispatch on some paths
        build_artifact_store,
        config_has_dataset_ref,
        resolve_dataset_refs_in_config,
    )
    if config_has_dataset_ref(config):
        # An engine whose job runs on another machine (the SSH engine) declares
        # `needs_durable_inputs`: a control-plane path means nothing there, so its
        # refs resolve to the durable copy whenever one exists (ADR-0149).
        from cairn.core.dataflow.engine import needs_durable_inputs  # noqa: PLC0415 - cycle: engine → contract ← this module

        durable = needs_durable_inputs(engine)
        try:
            config = await resolve_dataset_refs_in_config(
                build_artifact_store(), config, prefer_durable=durable)
        except Exception as e:  # noqa: BLE001 — an unresolvable dataset is a leg failure, not a crash
            return LegOutcome(state="failed", error=f"dataset ref resolution failed: {e}")
    # #1467 — judge refs resolve HERE too (same D4 rationale): `judge:<name>[@N]`
    # → the full immutable definition dict (model, prompt, contract, sampling,
    # name+version+prompt_hash provenance) the operator executes through the
    # governed gateway. An unknown ref fails the leg before any paid judge call.
    if any(isinstance(v, str) and v.startswith("judge:") for v in config.values()):
        from cairn.core.storage.judges import build_judge_store, resolve_judge_refs_in_config  # noqa: PLC0415 - cycle: storage → dispatch on some paths
        try:
            config = await resolve_judge_refs_in_config(build_judge_store(), config)
        except Exception as e:  # noqa: BLE001 — an unresolvable judge is a leg failure, not a crash
            return LegOutcome(state="failed", error=f"judge ref resolution failed: {e}")
    # #1180 follow-up (bug 6): a pipeline's legs carry the pack DIR name
    # (dataset_readiness) while flow defs register under the MANIFEST name
    # (dataset-readiness). `training` masked this — no hyphen. The alias walk
    # is `resolve_flow_key` since #1792, so the bridge's kind/risk reads and
    # `pipeline explain` resolve the same file this submission runs.
    defs = load_flow_defs()
    ref_key = resolve_flow_key(f"{pack}/{name}")
    flow_def = defs.get(ref_key)
    flow_kind = getattr(flow_def, "kind", None) or kind or "data"
    # #1379 — a pipeline leg runs the flow WHOLE. The compiler already refuses
    # a gate-carrying flow bound to a whole-flow door (compiler.py, #1079), but
    # this path never asked: the engine runner has no gate handling, so every
    # stage would execute and no one would be asked. Same guard, same fail-closed
    # message, now on the path that actually submits engine work.
    # Only a def whose stages we can actually READ can be judged: callers pass
    # duck-typed defs (tests, and any non-FlowDef shape), and an object with no
    # `stages` declares no gate we could drop — refusing those would fail
    # ungated legs that work today, which is fail-closed in the wrong
    # direction. The compiler keeps the authoritative check for real FlowDefs.
    if isinstance(getattr(flow_def, "stages", None), (list, tuple)):
        from cairn.core.dataflow.defs import DataflowDefError  # noqa: PLC0415 - cycle: defs → pack registry
        from cairn.core.dataflow.gate_split import assert_runnable_whole  # noqa: PLC0415 - cycle: gate_split → defs
        try:
            assert_runnable_whole(flow_def, site=f"pipeline leg {pack}/{name}")
        except DataflowDefError as e:
            # A gated flow bound whole is a leg OUTCOME, not a control-plane
            # crash — same posture as every other refusal on this path.
            return LegOutcome(state="failed", error=str(e))
    # #2021 (ADR-0102 D1) — the decontamination probe defaults to EVERY golden
    # and test version in this workspace. It has to happen here, control-side,
    # for the same reason dataset refs resolve here: the registry is a database
    # and the engine's box has none (D4). An explicit `eval_path` from the
    # pipeline always wins — it knows which set it means; the default only
    # fills a silence that used to mean "contamination not checked".
    if (isinstance(getattr(flow_def, "stages", None), (list, tuple))
            and any(getattr(s, "stage", "") == "decontam.apply" for s in flow_def.stages)
            and not str(config.get("eval_path") or "").strip()
            and not config.get("eval_paths")):
        from cairn.core.intake import holdout_eval_uris  # noqa: PLC0415 — off the hot import path
        from cairn.core.workspace import current_workspace  # noqa: PLC0415 — off the hot import path

        probes = await holdout_eval_uris(build_artifact_store(), workspace_id=current_workspace())
        if probes:
            config = {**config, "eval_paths": probes}

    try:
        stages = resolve_flow_ref(ref_key, config, kind=flow_kind)
    except Exception as e:  # noqa: BLE001 — an unresolvable ref is a leg failure
        return LegOutcome(state="failed",
                          error=f"cannot resolve flow {pack}/{name}: {e}")

    spec = DataflowSpec(
        dataflow_ref=f"{pack}/{name}",
        stages=stages,
        config=config,
        parent_run=run_id,          # lineage reaches the engine
    )
    # ADR-0118 D4/D5 — stamp the admitted stage contracts BEFORE checkpoint_key (the
    # contract is part of what runs), then refuse — non-retriable, before any attempt or
    # reservation — a configuration violation, an engine that does not declare it carries
    # stage contracts, or a spec too large for the environment string.
    from cairn.core.dataflow.conformance_check import (  # noqa: PLC0415 - cycle: dataflow → contract ← this module
        StageContractError, descriptor_problems, node_stage_contracts, stamp, submission_problems,
    )
    from cairn.core.dataflow.defs import stage_implements  # noqa: PLC0415 - cycle: defs → pack registry
    leg_contracts = node_stage_contracts(stage_contracts)
    try:
        spec = stamp(spec, leg_contracts, stage_implements)
    except StageContractError as e:
        return LegOutcome(state="failed", error=f"stage contract: {e}")
    refused = submission_problems(spec, engine)
    if refused:
        return LegOutcome(state="failed", error=f"stage contract: {'; '.join(refused)}")
    # #1314 — pin checkpoint identity HERE, before any engine sees the spec:
    # engines may rewrite it at submit (SSH input localization), and the
    # runner stamps manifests with the carried key, so the resume lookup
    # below and the registered checkpoints can never diverge.
    from cairn.core.dataflow.checkpoints import spec_key as _spec_key  # noqa: PLC0415 - cycle: checkpoints → contract ← this module
    spec.checkpoint_key = _spec_key(spec)

    problems = engine.validate(spec)
    if problems:
        return LegOutcome(state="failed",
                          error=f"engine cannot run {pack}/{name}: {problems}")

    # ADR-0086 H2 (#1244) — bounded retry, driven by the engine's `retriable`
    # flag (which existed and was consumed by NOBODY) and the existing
    # RetryPolicy shape. Defaults are conservative and kind-aware: data legs
    # get one automatic retry; an ml leg NEVER retries unless its binding
    # opts in with an explicit `retry:` block — a failed train leg must not
    # silently re-spend GPU dollars.
    policy = retry or RetryPolicy(maximum_attempts=(1 if flow_kind == "ml" else 2))
    attempts = max(1, int(policy.maximum_attempts or 1))
    delay = max(0.0, float(policy.initial_interval_seconds or 0.0))

    out = LegOutcome(state="failed", error="leg never attempted")
    known_attempt_cost_usd = 0.0
    adopting = _adopt_handle(adopt)
    #: ADR-0151 D5 — box time settled by the attempts so far; each attempt settles
    #: only its own window, and the next one's reservation counts what came before.
    gpu_total = 0.0
    #: ledger errors of earlier attempts: kept on whatever outcome the leg ends with
    ledger_notes: list[str] = []
    for attempt in range(1, attempts + 1):
        # Governance invariant: EVERY attempt is admitted — the reservation is
        # taken per attempt (a retry is new spend, never free) and a budget
        # refusal ends the leg immediately: budget exhaustion is not transient.
        # An ADOPTED attempt is the exception by construction: the spend was
        # admitted when the job was first submitted; re-attaching to it is not
        # new spend (and the reservation lived in the dead process anyway).
        reservation = None
        # The attempt's window starts here; an adopted attempt's started in the
        # process that submitted it, so it starts at the persisted run start.
        adopted_attempt = adopting is not None
        attempt_started = (_wall_now() if not adopted_attempt
                           else _parse_started(adopted_started_at, hourly_usd, ref))
        if series_id and adopting is None:
            if spent_usd is None:
                # #1441 — the caller could not read what earlier legs spent (an
                # adopted leg proceeds without it); a FRESH attempt is new spend
                # and unknown spend is never "nothing spent".
                return _with_notes(LegOutcome(
                    state="failed",
                    error=("budget: the spend of earlier legs is unreadable; "
                           "a new attempt is not admitted"),
                    cost_usd=known_attempt_cost_usd,
                    gpu_usd=round(gpu_total, 6),
                ), ledger_notes)
            try:
                reservation = await reserve(
                    series_id, est_usd=float(estimated_usd or 0.0),
                    spent_usd=round(float(spent_usd or 0.0)
                                    + known_attempt_cost_usd + gpu_total, 6),
                    budget_usd=float(budget_usd or 0.0),
                    # #1530 — stamp the run so a hold left behind by a dead
                    # process is attributable (and reapable) by run id, not just
                    # by the object this frame happens to hold.
                    run_id=str(run_id or ""),
                )
            except BudgetExceeded as e:
                # an earlier attempt's engine and box time stay on the leg's record
                return _with_notes(LegOutcome(
                    state="failed", error=str(e), cost_usd=known_attempt_cost_usd,
                    gpu_usd=round(gpu_total, 6)), ledger_notes)

        retriable = False
        attempt_cost_usd = 0.0
        status = None
        resumed_cost_usd = 0.0
        try:
            # H6c (#1248, ADR-0088) — resume selection: before EVERY attempt,
            # ask for the newest durable checkpoint of this exact spec. Fresh
            # legs cost one registry query and find nothing; a retry after a
            # mid-flow death seeds the new workdir and runs only what remains.
            if adopting is not None:
                # #1429 — re-attach: the engine already holds this job; the
                # control plane resumes WATCHING it (ADR-0084 D6), nothing is
                # replayed and nothing is resubmitted. One-shot: a failure of
                # the adopted job falls through to the normal retry path.
                handle = adopting
                adopting = None
                if on_adopt is not None:
                    try:
                        await on_adopt(dict(adopt or {}))
                    except Exception:  # noqa: BLE001 — announcing must not break adopting
                        logger.debug("leg %s: on_adopt emit failed", ref, exc_info=True)
            else:
                spec.resume_from = None
                if resume_provider is not None:
                    try:
                        from cairn.core.dataflow.checkpoints import spec_key  # noqa: PLC0415 - cycle: checkpoints → contract ← this module
                        rf = await resume_provider(spec_key(spec))
                        spec.resume_from = rf
                        if rf is not None:
                            resumed_cost_usd = _settled_resume_cost(
                                spec, known_attempt_cost_usd,
                            )
                            if on_resume is not None:
                                await on_resume(rf)
                    except Exception:  # noqa: BLE001 — no-resume is always a safe fallback
                        logger.debug("leg %s: resume lookup failed", ref, exc_info=True)
                # ADR-0103 D3 (#2023) — a flow leg records what it runs with, the
                # way a task leg does in invoke_operator_core: the preview promised
                # "this leg records its own parameters when it executes". Same
                # payload shape, same producer (effective.py); never fails the leg.
                from cairn.core.operator.effective import record_flow_parameters  # noqa: PLC0415 — cycle: dispatch → operator
                await record_flow_parameters(run_id=str(run_id or ""), node=ref, ref=ref,
                                             kind=flow_kind, inputs=config)
                handle = await engine.submit(spec, None)
                if reservation is not None and getattr(reservation, "id", None):
                    handle.meta["_cairn_reservation_id"] = reservation.id
                if on_submit is not None:
                    # ADR-0086 H1 (#1239) — hand the durable re-attach token to the
                    # caller the moment it exists, not in the terminal outcome: a
                    # crash between submit and settle must not lose the only pointer
                    # to paid, in-flight compute. Best-effort — persisting the handle
                    # must never fail the leg the handle describes.
                    try:
                        await on_submit(handle)
                    except Exception:  # noqa: BLE001
                        logger.debug("leg %s: on_submit persistence failed", ref, exc_info=True)
            def _remember_status(polled: Any) -> None:
                nonlocal status
                status = polled

            status = await _poll_until_terminal(
                engine, handle, poll_budget_s, heartbeat=heartbeat,
                on_status=_remember_status,
            )
            attempt_cost_usd = max(
                0.0,
                _reported_metrics_cost(getattr(status, "metrics", {}) or {})
                - resumed_cost_usd,
            )
            if str(getattr(status, "state", "")) == "succeeded":
                result = await engine.result(handle)
                reported_attempt_cost_usd = max(
                    _reported_metrics_cost(getattr(status, "metrics", {}) or {}),
                    _reported_cost(result),
                )
                attempt_cost_usd = max(
                    0.0, reported_attempt_cost_usd - resumed_cost_usd,
                )
                out = LegOutcome(
                    state="succeeded", handle=handle,
                    stats=dict(getattr(result, "stats", {}) or {}),
                    metrics=dict(getattr(status, "metrics", {}) or {}),
                    cost_usd=round(known_attempt_cost_usd + attempt_cost_usd, 6),
                )
                # ADR-0118 D5 — each opted-in stage's descriptor stats re-checked before the
                # intake verdict and before anything registers; a refusal fails the leg.
                descriptors = list(getattr(result, "output_artifacts", []) or [])
                nonconforming = descriptor_problems(leg_contracts, len(spec.stages), descriptors)
                if nonconforming:
                    raise StageContractError("; ".join(nonconforming))
                if flow_kind == "data":
                    # #2188 (T03, C2) — the check-evidence map on the leg's stats:
                    # the flow's `not_applicable` declarations first (they ride
                    # the same dict into the version's verdict below), then the
                    # map itself, flat, so the record's findings show it.
                    _annotate_check_evidence(out.stats, flow_def)
                # #1316 — register the engine's output descriptors control-side
                # (the door path always did; the pipeline path dropped them, so
                # the registry stayed EMPTY for pipeline-dispatched flows).
                # Strict like the door: an unrecorded data product is a
                # governance hole, so a registry failure fails the leg loudly
                # (the except below turns it into a failed outcome).
                # ADR-0141 — the products stages named ride beside the per-stage descriptors
                # (which the check above reads by position) and register through the same path.
                named = list(getattr(result, "named_outputs", []) or [])
                if (descriptors or named) and artifact_store is not None:
                    await register_leg_descriptors(
                        descriptors + named, artifact_store, run_id=run_id,
                        producer_job=str(getattr(handle, "job_id", "") or ""),
                        out_stats=out.stats)
                # settled below, once, with every other outcome of this attempt
            else:
                out = LegOutcome(
                    state=str(getattr(status, "state", "failed")), handle=handle,
                    metrics=dict(getattr(status, "metrics", {}) or {}),
                    error=str(getattr(status, "error", "") or ""),
                    cost_usd=round(known_attempt_cost_usd + attempt_cost_usd, 6),
                )
                # Only the ENGINE may call a failure transient (cancellation never
                # is); an exception below has no flag and is not retried.
                retriable = (out.state == "failed") and bool(getattr(status, "retriable", False))
        except Exception as e:  # noqa: BLE001 — a leg failure is an outcome, not a crash
            logger.exception("leg %s failed", ref)
            attempt_cost_usd = max(
                attempt_cost_usd,
                max(
                    0.0,
                    _reported_metrics_cost(getattr(status, "metrics", {}) or {})
                    - resumed_cost_usd,
                ),
            )
            out = LegOutcome(
                state="failed", error=f"{type(e).__name__}: {e}",
                metrics=dict(getattr(status, "metrics", {}) or {}),
                cost_usd=round(known_attempt_cost_usd + attempt_cost_usd, 6),
            )

        # ADR-0151 D5 — the attempt is accounted ONCE, here, outside the try: its
        # window joins the leg's box time, then its hold closes. #1441 — the hold
        # gives way to what the attempt ACTUALLY spent (the template path's rule,
        # dispatcher.py `_close_reservations`): engine-reported plus box time, on
        # any outcome (a failed, timed-out or cancelled attempt on a priced box still
        # billed its wall-clock). Nothing billed → release.
        gpu = _box_time_usd(hourly_usd, attempt_started)
        gpu_total += gpu
        out.gpu_usd = round(gpu_total, 6)
        note = await _close_attempt(out, reservation, engine_usd=attempt_cost_usd,
                                    box_usd=gpu,
                                    adopted_run_id=str(run_id or "") if adopted_attempt else "",
                                    adopted_reservation_id=(
                                        str(((adopt or {}).get("meta") or {}).get("_cairn_reservation_id", ""))
                                        if adopted_attempt else ""),
                                    priced=bool(hourly_usd and hourly_usd > 0))
        known_attempt_cost_usd = round(known_attempt_cost_usd + attempt_cost_usd, 6)
        if note:
            ledger_notes.append(note)
        if (out.ok or not retriable or attempt >= attempts
                or note.startswith("budget ledger: adopted handle does not match")):
            return _with_notes(out, ledger_notes)
        if on_retry is not None:
            # visibility invariant: every retry is announced (run event) with
            # the attempt number and the cause it is retrying past.
            try:
                await on_retry(attempt, out.error)
            except Exception:  # noqa: BLE001 — announcing must not break retrying
                logger.debug("leg %s: on_retry emit failed", ref, exc_info=True)
        await asyncio.sleep(min(delay, float(policy.maximum_interval_seconds or delay)))
        delay = delay * float(policy.backoff_coefficient or 1.0)
    return _with_notes(out, ledger_notes)


def _with_notes(out: LegOutcome, notes: list[str]) -> LegOutcome:
    """Append every ledger note to the outcome the leg ends with (never replace)."""
    for note in notes:
        if note not in (out.error or ""):
            out.error = f"{out.error}; {note}" if out.error else note
    return out


async def _close_attempt(out: LegOutcome, reservation: Any, *, engine_usd: float,
                         box_usd: float, adopted_run_id: str = "",
                         adopted_reservation_id: str = "", priced: bool = False) -> str:
    """Close one attempt's hold: ``settle`` at ``engine_usd + box_usd`` when it
    succeeded or cost something, else ``release`` (nothing billed).

    An ADOPTED attempt holds no reservation of its own: the hold is the one the
    process that submitted it took (stamped with the run id) and left ``held`` when
    it died. It is closed the way ``_close_reservations`` closes a template's,
    through ``held_for_run``. Its box window runs from the leg's start, so it already
    covers the run's earlier attempts: when it has a window (``box_usd > 0``), what
    the run's claims already settled (``settled_usd_for_run``) is subtracted from
    ``engine_usd + box_usd``, floored at 0, so nothing is settled twice (ADR-0152).
    With no window (no readable start, or an unpriced leg) the engine cost settles
    as is. New handles carry their reservation ID, so a stale handle cannot close
    another attempt's hold. For an unpriced legacy handle, an existing settlement
    makes the identity ambiguous: leave the holds open rather than bill twice.
    The first held row settles at that figure, any further one at 0. If a read fails
    or the identified hold is absent while other holds exist, those holds stay open.

    Returns a note when the ledger fails — logged, never a change to the engine
    outcome or the box time already accounted; ``""`` otherwise."""
    actual_usd = float(engine_usd or 0.0) + float(box_usd or 0.0)
    billed = out.ok or actual_usd > 0
    try:
        if adopted_run_id:
            held_rows = await held_for_run(adopted_run_id)
            if adopted_reservation_id:
                matching = [held for held in held_rows if held.id == adopted_reservation_id]
                if not matching and held_rows:
                    return ("budget ledger: adopted handle does not match a held reservation; "
                            "newer holds remain open")
                held_rows = matching
            elif held_rows and not priced and await settled_usd_for_run(adopted_run_id) > 0:
                return ("budget ledger: adopted handle does not match a held reservation; "
                        "legacy handle has ambiguous settlement, holds remain open")
            if held_rows and billed and box_usd > 0:
                already = await settled_usd_for_run(adopted_run_id)
                actual_usd = max(0.0, round(actual_usd - already, 6))
            for i, held in enumerate(held_rows):
                if billed:
                    await settle(held, actual_usd=actual_usd if i == 0 else 0.0)
                else:
                    await release(held)
        elif billed:
            await settle(reservation, actual_usd=actual_usd)
        else:
            await release(reservation)
    except Exception as e:  # noqa: BLE001 — a ledger error is not an engine outcome
        logger.exception("budget ledger: could not close the reservation for run %s", adopted_run_id
                         or getattr(reservation, "run_id", ""))
        return f"budget ledger: the reservation could not be closed ({type(e).__name__}: {e})"
    return ""


def _wall_now() -> datetime:
    """The wall clock an attempt's box time is measured on (a seam for tests)."""
    return datetime.now(timezone.utc)


def _parse_started(started_at: Optional[str], hourly_usd: float, ref: str) -> Optional[datetime]:
    """The persisted start of an ADOPTED attempt; ``None`` (its box time unknown,
    never guessed from this process's clock) when it is missing or unreadable."""
    if not started_at:
        if hourly_usd > 0:
            logger.warning("leg %s: adopted on a priced box with no persisted start; "
                           "its box time is not recorded", ref)
        return None
    try:
        dt = datetime.fromisoformat(str(started_at))
    except ValueError:
        logger.warning("leg %s: unreadable started_at %r; box time not recorded", ref, started_at)
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _box_time_usd(hourly_usd: float, started: Optional[datetime]) -> float:
    """ADR-0151 D5 — ``hourly_usd`` × the attempt's wall-clock (0 when not priced)."""
    if not hourly_usd or hourly_usd <= 0 or started is None:
        return 0.0
    seconds = max(0.0, (_wall_now() - started).total_seconds())
    return round(float(hourly_usd) * seconds / 3600.0, 6)


def _reported_cost(result: Any) -> float:
    """``DataflowResult.cost_usd`` as a non-negative float; 0.0 when absent or junk
    (a duck-typed result from an older engine carries no cost)."""
    try:
        value = float(getattr(result, "cost_usd", 0.0) or 0.0)
    except (OverflowError, TypeError, ValueError):
        return 0.0
    return value if 0.0 < value < float("inf") else 0.0   # NaN compares False


def _reported_metrics_cost(metrics: Any) -> float:
    """Known positive partial cost from an engine's progressive metrics."""
    if not isinstance(metrics, dict):
        return 0.0
    raw = metrics.get("cost_usd", 0.0)
    if isinstance(raw, bool):
        return 0.0
    try:
        value = float(raw or 0.0)
    except (OverflowError, TypeError, ValueError):
        return 0.0
    return value if 0.0 < value < float("inf") else 0.0


def _settled_resume_cost(spec: DataflowSpec, settled_usd: float) -> float:
    """Checkpoint cost already settled by an earlier attempt of this leg.

    The runner restores the checkpoint stage's frozen stats, so a resumed
    result includes that cost again. Only trust a checkpoint for this exact
    spec, and only subtract spend already settled in this retry loop.
    """
    resume = spec.resume_from
    if resume is None or settled_usd <= 0:
        return 0.0
    try:
        from cairn.core.dataflow.checkpoints import spec_key  # noqa: PLC0415 - cycle: checkpoints → contract ← this module
        if resume.spec_key != spec_key(spec):
            return 0.0
        frozen = {int(item.get("index", -1)): item for item in resume.completed}
        checkpoint_cost = sum(
            _reported_metrics_cost(frozen.get(index, {}).get("stats", {}))
            for index in range(min(resume.stage_index + 1, len(spec.stages)))
        )
    except (AttributeError, TypeError, ValueError):
        return 0.0
    return round(min(checkpoint_cost, settled_usd), 6)


def _adopt_handle(persisted: Optional[dict]) -> Any:
    """Rebuild an ``EngineJobHandle`` from the ``{job_id, engine, meta}`` dict a
    previous owner persisted (``StepState.handle``); None when nothing to adopt."""
    if not persisted or not str(persisted.get("job_id", "") or ""):
        return None
    from cairn.core.dataflow.engine import EngineJobHandle  # noqa: PLC0415 - cycle: engine → contract ← this module

    return EngineJobHandle(
        job_id=str(persisted.get("job_id")),
        engine=str(persisted.get("engine") or "engine"),
        meta=dict(persisted.get("meta") or {}),
    )


async def cancel_recorded_job(engine: Any, persisted: Optional[dict]) -> bool:
    """Best-effort cancel of the engine job a previous owner recorded
    (``StepState.handle``), for a re-adopted leg that is refused before it is
    watched: nothing would watch the job any more, and it would keep running
    and billing. ``True`` when a cancel was sent; never raises."""
    handle = _adopt_handle(persisted)
    if handle is None or engine is None:
        return False
    try:
        await engine.cancel(handle)
    except Exception:  # noqa: BLE001 — best-effort: the refusal itself must still land
        logger.exception("could not cancel recorded engine job %s", handle.job_id)
        return False
    return True


#: Minimum seconds between heartbeat stamps — the poll loop can tick
#: sub-second on quick legs, and a store write per tick is churn for a
#: liveness signal whose reconciler TTL is minutes.
_HEARTBEAT_MIN_S = 10.0


async def register_leg_descriptors(descriptors: list, artifact_store, *, run_id: str,
                                   producer_job: str = "", out_stats: dict) -> list[str]:
    """The ONE control-side registration path for what a leg produced (#1316,
    #1466, #2189): resolve URI parents to registry ids, register every
    descriptor (content-addressed — identical bytes register once), name the
    dataset versions a stage asked for, and surface the CANDIDATE identity when
    a descriptor is a model (`candidate_artifact_id`, C6/I14) so evaluation,
    registration and serving key on an id, never on a URI. Flow legs call it
    from :func:`execute_leg`; a task leg whose operator returns descriptors
    registers through the same function (#1488, wiring per producer).

    Everything lands in the run's workspace, scoped from its record (ADR-0148):
    URI parents resolve among that workspace's artifacts and the workspace owns
    what is registered. With auth on and no workspace in scope it is refused
    before anything registers."""
    from cairn.core.workspace import scoped_workspace  # noqa: PLC0415 — off the hot import path

    _check_stage_roles(descriptors)
    ws = scoped_workspace()
    await _resolve_uri_parents(descriptors, artifact_store, workspace_id=ws)
    ids = [await artifact_store.register(d, producer_run_id=run_id, producer_job=producer_job,
                                         workspace_id=ws)
           for d in descriptors]
    out_stats["artifact_ids"] = ids
    out_stats["artifact_uris"] = [str(getattr(d, "uri", "")) for d in descriptors]
    # ADR-0141 — the candidate is a STAGE's model; a stage's named product (`output_name` in its
    # stats, set by the runner) never displaces it.
    models = [(i, d) for i, d in zip(ids, descriptors) if str(getattr(d, "kind", "")) == "model"
              and "output_name" not in (getattr(d, "stats", None) or {})]
    if models:
        cid, cdesc = models[-1]
        out_stats["candidate_artifact_id"] = cid
        out_stats["candidate_artifact_uri"] = str(getattr(cdesc, "uri", ""))
        out_stats["candidate_producer_run_id"] = run_id
    await _register_dataset_versions(descriptors, artifact_store, run_id, out_stats)
    return ids


def _annotate_check_evidence(stats: dict, flow_def: Any) -> None:
    """Write ``checks.<name>.state = not_applicable`` for every check the flow
    declared, then the full check map (flat) over the leg's stats."""
    from cairn.core.intake import (  # noqa: PLC0415 — off the hot import path
        CHECK_NOT_APPLICABLE, CHECKS_PREFIX, check_map_from_flow_stats, flatten_check_map,
    )
    for name, state in dict(getattr(flow_def, "checks", None) or {}).items():
        if state == CHECK_NOT_APPLICABLE:
            stats[f"{CHECKS_PREFIX}{name}.state"] = CHECK_NOT_APPLICABLE
    stats.update(flatten_check_map(check_map_from_flow_stats(stats)))


#: ADR-0141 D3 — the roles a stage may give the version it asks for. ``golden`` is human-rated
#: only (ADR-0102 D3) and is never declared by a kernel's stats.
STAGE_REGISTER_ROLES = ("training", "validation", "test")


def _check_stage_roles(descriptors: list) -> None:
    """Refuse a stage-given role outside :data:`STAGE_REGISTER_ROLES` BEFORE anything registers,
    so a refusal leaves no artifact or version of the leg half-written."""
    bad = sorted({str(r) for d in descriptors
                  if (r := str((getattr(d, "stats", None) or {}).get("register_role") or "").strip())
                  and r not in STAGE_REGISTER_ROLES})
    if bad:
        raise ValueError(f"a stage asked to register a dataset version with role(s) {bad}; a stage "
                         f"may give only {list(STAGE_REGISTER_ROLES)} (ADR-0141 D3)")


async def _register_dataset_versions(descriptors: list, artifact_store,
                                     run_id: str, out_stats: dict) -> None:
    """#1466 — an export that asked for registration (`register:` config →
    ``stats.register_as`` on its descriptor) becomes a named dataset VERSION
    here, on the control plane (the box has no DB access, D4). Same strict
    posture as the artifact registration above it: a REQUESTED registration
    that cannot happen must fail the leg loudly, never silently vanish.

    The version lands in the run's workspace (scoped from its record); with auth
    on and no workspace in scope the registration is refused."""
    from cairn.core.workspace import scoped_workspace  # noqa: PLC0415 — off the hot import path

    for d in descriptors:
        d_stats = getattr(d, "stats", None) or {}
        reg_name = str(d_stats.get("register_as") or "")
        if reg_name and hasattr(artifact_store, "register_dataset_version"):
            # ADR-0141 D3 — a split's products say what they are FOR (`register_role`:
            # training / validation / test, checked in `_check_stage_roles`), so the holdout
            # door sees a test version as one. Unset keeps the dataset's default role.
            role = str(d_stats.get("register_role") or "").strip()
            v = await artifact_store.register_dataset_version(
                reg_name, str(getattr(d, "content_hash", "") or ""), actor=run_id,
                workspace_id=scoped_workspace(), **({"role": role} if role else {}))
            out_stats.setdefault("dataset_versions", []).append(f"{reg_name}@{v}")
            await _stamp_intake_verdict(artifact_store, reg_name, v, out_stats)


async def _stamp_intake_verdict(artifact_store, name: str, version: int, flow_stats: dict) -> None:
    """#2021 (ADR-0102 D1) — fold this flow's stats into the version's INTAKE
    VERDICT and write it on the row the registration just created.

    Control-side, right beside the registration, and for the same reason: the
    verdict is state in the database, and the box that computed the numbers has
    no database (ADR-0081 D3). The engine reported flat scalars per stage;
    :func:`cairn.core.intake.verdict_from_flow_stats` is the ONE place that
    decides what they add up to, so the door and the console read a rule that
    exists once.

    Best-effort by design, unlike the registration above it. A registration
    that cannot happen is a governance hole — a data product with no record. A
    verdict that cannot be written leaves the version with NO verdict, which
    the door already handles as "admitted with a note": the run that produced
    the data still succeeded, and failing it here would turn a missing
    annotation into lost work.
    """
    setter = getattr(artifact_store, "set_dataset_version_verdict", None)
    if setter is None:
        return
    from cairn.core.intake import verdict_from_flow_stats  # noqa: PLC0415 — off the hot import path
    from cairn.core.workspace import scoped_workspace  # noqa: PLC0415 — off the hot import path

    try:
        verdict = verdict_from_flow_stats(flow_stats)
        await setter(name, int(version), verdict=verdict.verdict,
                     reasons=verdict.reasons, report=verdict.report,
                     workspace_id=scoped_workspace())
    except Exception:  # noqa: BLE001 — see the docstring: never fail a landed dataset over this
        logger.warning("could not stamp the intake verdict on %s@%s", name, version, exc_info=True)
    else:
        flow_stats.setdefault("intake_verdicts", []).append(f"{name}@{version}={verdict.verdict}")


async def _resolve_uri_parents(descriptors: list, artifact_store, *,
                               workspace_id: Optional[str] = None) -> None:
    """#1432 — a descriptor's parent given as a URI (an upstream leg's export,
    e.g. ``s3://…/train-ready.jsonl``) becomes that artifact's REGISTRY ID when
    the registry knows the URI among ``workspace_id``'s artifacts (else the one
    in scope), so the lineage DAG walks across legs. Unknown URIs (external
    inputs, or another workspace's) stay as recorded. Best effort: a lookup
    failure never fails the leg."""
    find = getattr(artifact_store, "find_by_uri", None)
    if find is None:
        return
    for d in descriptors:
        parents = list(getattr(d, "parents", None) or [])
        if not any("://" in p for p in parents):
            continue
        resolved = []
        for p in parents:
            if "://" in p:
                try:
                    hit = await find(p, workspace_id=workspace_id)
                except Exception:  # noqa: BLE001 — lineage is best effort
                    hit = None
                resolved.append(hit or p)
            else:
                resolved.append(p)
        try:
            d.parents = resolved
        except Exception:  # noqa: BLE001 — frozen descriptor: leave as-is
            pass


async def _poll_until_terminal(
    engine: Any, handle: Any, budget_s: float,
    *, heartbeat: Optional[Callable[[], Awaitable[None]]] = None,
    on_status: Optional[Callable[[Any], None]] = None,
) -> Any:
    """Poll to a terminal state or the budget. The engine owns the work; if the
    control plane restarts mid-leg, a re-attach by handle resumes this loop
    (ADR-0084 D6) — nothing is replayed.

    Backoff, not a fixed 1s tick (#1180 bug 11): a remote engine's poll SSHes,
    and an hour-long train leg at 1 poll/s meant ~3,900 SSH round-trips — the
    worker exhausted its fd limit (`Too many open files: known_hosts`) 40 min
    into an otherwise-healthy training. Quick legs still get sub-second polls;
    long legs settle at one poll per _POLL_MAX_S."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + max(1.0, float(budget_s))
    interval = _POLL_INTERVAL_S
    last_beat = 0.0
    while True:
        status = await engine.poll(handle)
        if on_status is not None:
            on_status(status)
        if heartbeat is not None and loop.time() - last_beat >= _HEARTBEAT_MIN_S:
            # ADR-0086 H1 (#1239) — "I am still watching this engine": stamped
            # AFTER a poll returned, so the beat vouches for the whole chain
            # (loop alive AND engine reachable). Best-effort: a store hiccup
            # must not kill a healthy leg.
            try:
                await heartbeat()
                last_beat = loop.time()
            except Exception:  # noqa: BLE001
                logger.debug("leg heartbeat stamp failed", exc_info=True)
        if is_terminal(status):
            return status
        if loop.time() > deadline:
            try:
                await engine.cancel(handle)
            except Exception:  # noqa: BLE001 — cancel is best-effort on timeout
                pass
            raise TimeoutError(f"leg exceeded its {budget_s}s poll budget")
        await asyncio.sleep(interval)
        interval = min(interval * 1.5, _POLL_MAX_S)


__all__ = ["FLOW_PREFIX", "TASK_PREFIX", "LegKindError", "LegOutcome", "cancel_recorded_job", "execute_leg",
           "leg_kind_of", "parse_flow_ref", "recorded_stage_contracts"]
