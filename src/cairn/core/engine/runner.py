"""
Runner: top-level orchestrator that ties everything together.

Given a TriggerEvent, the runner:
  1. Asks SkillEngine to find matching skills and compile their templates (with skill context)
  2. Runs the compiled template (LangGraph subgraph or LLM call) to produce findings
  3. For each skill's action candidates, asks PolicyEngine for a PolicyDecision
  4. Returns an RunResult bundling RCA + action decisions

This is the integration point. The demo script invokes Runner.run(event)
and prints the result.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal, Optional, Protocol

# Streaming sink: ``await sink(event_type, data)`` appends one event to the
# run's log. Supplied by the dispatcher; None means "don't stream".
EventSink = Callable[[str, dict], Awaitable[None]]

from cairn.core.engine.compiled import CompiledTemplate
from cairn.core.notifier import NotifierRegistry
from cairn.core.policy.engine import PolicyEngine
from cairn.core.engine.schema import (
    ActionCandidate,
    ActionDecision,
    ActionOutcome,
    ActionRequest,
    ExecutorContext,
    PolicyDecision,
    RunResult,
    TriggerEvent,
)
from cairn.core.engine.skill_engine import LoadedSkill, SkillEngine
from cairn.core.pack.profile import load_profile_from_env
from cairn.core.profile_backends import effective_backends

_log = logging.getLogger("cairn.runner")


async def run_within_budget(
    compiled: CompiledTemplate, input_data: dict[str, Any]
) -> dict[str, Any]:
    """Run a compiled template, honoring its opt-in ``budget``.

    Only ``budget.max_wall_seconds`` is enforced here (the one dimension that's
    actually enforceable without a cost/token accounting subsystem — see
    ``BudgetConfig``). When the wall-clock ceiling is exceeded the template run
    is cancelled and a ``budget_exceeded`` output is returned instead of letting
    a runaway agent loop burn unbounded time/spend.

    Templates with no budget (or no ``max_wall_seconds``) run exactly as before.
    """
    budget = getattr(compiled.spec, "budget", None)
    wall = getattr(budget, "max_wall_seconds", None) if budget else None
    if not wall:
        return await compiled.run(input_data)
    try:
        return await asyncio.wait_for(compiled.run(input_data), timeout=wall)
    except asyncio.TimeoutError:
        _log.warning(
            "template %s exceeded budget.max_wall_seconds=%.1fs — aborting",
            compiled.name, wall,
        )
        return {
            "type": "budget_exceeded",
            "budget_exceeded": True,
            "limit_wall_seconds": wall,
            "output": "",
        }


def template_cost_cap(compiled: Any) -> Optional[float]:
    """A compiled template's ``budget.max_cost_usd``, or ``None``."""
    return getattr(getattr(getattr(compiled, "spec", None), "budget", None), "max_cost_usd", None)


def cost_cap_reason(output: Any) -> str:
    """Why a cost cap stopped the run that produced ``output``, or ``""``. The wall-clock
    limit's output has no ``reason``: that one still ends ``done``."""
    if isinstance(output, dict) and output.get("budget_exceeded"):
        return str(output.get("reason") or "")
    return ""


def budget_exceeded_output(reason: str) -> dict[str, Any]:
    """The output of a run a cost cap stopped, which `cairn gate` and scoring read."""
    return {"type": "budget_exceeded", "budget_exceeded": True, "reason": reason, "output": ""}


def _mark_if_over_cap(output: Any, ctx: Any) -> Any:
    """Spend that never raised, because a handler swallowed it, still counts."""
    from cairn.core import obs  # noqa: PLC0415 — matches the run path

    reason = "" if cost_cap_reason(output) else obs.over_cap_reason(ctx, ctx.cost_usd)
    if reason and isinstance(output, dict):
        return {**output, "budget_exceeded": True, "reason": reason}
    return output


def _attached_store_policies_from_env() -> set[str] | None:
    """The active environment's store-policy attach-list (#317, ADR-0053).

    Read from the ambient profile (``CAIRN_PROFILE``). ``None`` when the profile
    omits the ``policies`` key → all enabled store policies apply (back-compat).
    A bad/missing profile must never break the runner, so failures fall back to
    ``None`` (permissive-load, not enforcement — pack policies still apply).
    """
    try:
        prof = load_profile_from_env()
    except Exception:  # noqa: BLE001 — a malformed profile mustn't crash engine construction
        return None
    return set(prof.policies) if prof.policies is not None else None


def _adopted_pack_policies_from_env() -> set[str] | None:
    """The environment's PACK-policy adoption list (#1103, ADR-0083 D4).

    ``None`` when the profile omits ``pack_policies`` → adopt all, which is the
    pre-#1103 behavior: an upgrade must never silently remove a gate. A bad or
    missing profile falls back to ``None`` for the same reason (permissive-load,
    fail-SAFE — more gating, never less).
    """
    try:
        prof = load_profile_from_env()
    except Exception:  # noqa: BLE001 — a malformed profile mustn't crash engine construction
        return None
    return set(prof.pack_policies) if prof.pack_policies is not None else None


def _environment_memory_enabled() -> bool:
    """Whether the active environment provisions episodic memory (#306, ADR-0052 §5).

    The environment (profile ``[profile.backends] memory``) is the gate: memory is
    available — and auto-capture is a mandatory lifecycle guarantee — only when the
    environment enables it. Default OFF. A bad/missing profile falls back to OFF
    (fail-safe: no silent capture into an unprovisioned store).
    """
    try:
        prof = load_profile_from_env()
        return bool(effective_backends(prof.backends).get("memory"))
    except Exception:  # noqa: BLE001 — a malformed profile mustn't crash the run
        return False


class ActionHandler(Protocol):
    """Executes one policy-decided action end to end (#2497). The Runner only calls it; the
    public core ships no implementation and executes no actions by itself — a deployment
    injects one as ``Runner.action_handler`` (a ``cairn.routers`` provider, via ``RouterDeps.runner``)."""

    async def execute(
        self,
        candidate: ActionCandidate,
        decision: PolicyDecision,
        context: ExecutorContext,
    ) -> ActionOutcome: ...


class Runner:
    """Top-level orchestrator: skill → template → policy → (optional) action handler."""

    #: Default max chain length to prevent buggy cascade loops.
    DEFAULT_CASCADE_DEPTH_LIMIT = 3

    def __init__(
        self,
        skill_engine: SkillEngine | None = None,
        policy_engine: PolicyEngine | None = None,
        action_handler: ActionHandler | None = None,
        notifiers: NotifierRegistry | None = None,
        *,
        skills_dir: Path | str | None = None,
        policies_dir: Path | str | None = None,
        default_environment: Literal["dev", "staging", "production"] = "production",
        cascade_depth_limit: int = DEFAULT_CASCADE_DEPTH_LIMIT,
    ):
        self.skill_engine = skill_engine or SkillEngine(skills_directory=skills_dir)
        self.policy_engine = policy_engine or PolicyEngine(
            policies_directory=policies_dir,
            attached_store_policies=_attached_store_policies_from_env(),
            adopted_pack_policies=_adopted_pack_policies_from_env(),
        )
        # action_handler is optional — without it we just evaluate policies and stop
        self.action_handler = action_handler
        # notifiers are optional — empty registry = silent (back-compat)
        self.notifiers = notifiers or NotifierRegistry()
        self.default_environment = default_environment
        self.cascade_depth_limit = cascade_depth_limit

    async def run(
        self,
        event: TriggerEvent,
        *,
        environment: Literal["dev", "staging", "production"] | None = None,
        event_sink: Optional[EventSink] = None,
        run_id: Optional[str] = None,
        _cascade_depth: int = 0,
    ) -> list[RunResult]:
        """Run the full pipeline against an event.

        Returns one RunResult per matched skill (usually 0 or 1).
        Cascade: if a resolved skill declares ``chain_skill: <name>``, the
        named skill is fired with a synthetic TriggerEvent. Bounded by
        ``cascade_depth_limit`` to prevent runaway loops.

        ``event_sink`` (optional) streams partial progress — evidence, action
        decisions/outcomes, per-skill lifecycle — into an append-only log as
        the run proceeds, instead of only returning the final result. Cascade
        skills emit into the same sink so the whole chain is one stream.
        """
        env = environment or self.default_environment
        loaded_skills = self.skill_engine.activate(event)

        if not loaded_skills:
            return []

        results: list[RunResult] = []
        for loaded in loaded_skills:
            # Propagate the caller's run_id (the dispatcher's Run.id) as the
            # template's thread_id / durable workflow_id so a paused run is
            # resumable by that id and concurrent runs don't collide on
            # thread_id=spec.name (an internal issue).
            result = await self._run_single_skill(loaded, env, event_sink, run_id=run_id)
            results.append(result)

            # ── Cascade dispatch (fan-out) ────────────────────────────
            # All successors are siblings of THIS investigation — each gets a
            # cascade event carrying this skill's findings/actions/outcomes.
            chain_skills = (
                loaded.spec.effective_chain_skills()
                if hasattr(loaded.spec, "effective_chain_skills")
                else ([loaded.spec.chain_skill] if getattr(loaded.spec, "chain_skill", None) else [])
            )
            import logging
            log = logging.getLogger("cairn.runner")
            for chain in [] if cost_cap_reason(result.raw_output) else chain_skills:
                if _cascade_depth >= self.cascade_depth_limit:
                    log.warning(
                        "cascade depth limit (%d) reached; skipping chain %s → %s",
                        self.cascade_depth_limit, loaded.spec.name, chain,
                    )
                    continue
                cascade_event = self._build_cascade_event(chain, loaded, result)
                log.info(
                    "cascade firing: %s → %s (depth=%d)",
                    loaded.spec.name, chain, _cascade_depth + 1,
                )
                cascade_results = await self.run(
                    cascade_event,
                    environment=env,
                    event_sink=event_sink,
                    _cascade_depth=_cascade_depth + 1,
                )
                results.extend(cascade_results)
        return results

    @staticmethod
    async def _emit(sink: Optional[EventSink], type: str, data: dict) -> None:
        """Best-effort event emit — a streaming failure never breaks a run."""
        if sink is None:
            return
        try:
            await sink(type, data)
        except Exception as e:  # noqa: BLE001 - streaming is non-critical
            _log.debug("event_sink raised on %s; ignoring: %s", type, e)

    @staticmethod
    def _build_cascade_event(
        chain_skill: str,
        parent: LoadedSkill,
        parent_result: RunResult,
    ) -> TriggerEvent:
        """Build a synthetic TriggerEvent for a cascade-dispatched skill.

        Payload carries enough of the parent investigation that downstream
        skills (post-mortem writer, summary, escalation, etc.) can produce
        meaningful output without re-running the investigation. Specifically:

          - parent_trigger:  the trigger that fired the parent skill
          - parent_findings: parent_result.raw_output (full investigation
                              output — RCA fields, evidence array, etc.)
          - parent_actions:  list of {candidate, decision} dicts
          - parent_outcomes: list of action execution outcomes
          - parent_duration_seconds: how long the parent took
          - parent_summary:  convenience headline (kept for back-compat)
        """
        return TriggerEvent(
            source="cascade",
            matched_pattern=chain_skill,
            service=parent.trigger.service,
            payload={
                "cascade_from": parent.spec.name,
                "parent_trigger": parent.trigger.model_dump(mode="json"),
                "parent_findings": parent_result.raw_output or {},
                "parent_actions": [ad.model_dump(mode="json") for ad in parent_result.actions],
                "parent_outcomes": [o.model_dump(mode="json") for o in parent_result.action_outcomes],
                "parent_duration_seconds": parent_result.duration_seconds,
                "parent_summary": (
                    parent_result.raw_output.get("summary")
                    if parent_result.raw_output else None
                ),
            },
        )

    @staticmethod
    def _build_cascade_user_message(loaded: LoadedSkill, symptom: str) -> str:
        """Format the parent investigation into a prompt for a cascade skill.

        Downstream skills (post-mortem writer, summary) receive the parent's
        findings, actions, and outcomes inline so they can produce their
        output without re-investigating.
        """
        import json as _json

        p = loaded.trigger.payload
        findings = p.get("parent_findings") or {}
        actions = p.get("parent_actions") or []
        outcomes = p.get("parent_outcomes") or []
        # Keep the JSON compact-but-readable and bounded so we don't blow the
        # context window on a huge investigation.
        findings_str = _json.dumps(findings, indent=2)[:4000]
        actions_str = _json.dumps(actions, indent=2)[:2000]
        outcomes_str = _json.dumps(outcomes, indent=2)[:2000]
        return (
            f"This is a cascaded follow-up. The parent skill "
            f"'{p.get('cascade_from', 'unknown')}' has already investigated and "
            f"resolved an incident. Your job is to produce your output from the "
            f"parent's results below — do NOT re-investigate.\n\n"
            f"Service: {loaded.trigger.service or 'unknown'}\n"
            f"Parent took: {p.get('parent_duration_seconds', 0):.1f}s\n"
            f"Parent summary: {p.get('parent_summary') or '(none)'}\n\n"
            f"── Parent findings ──\n{findings_str}\n\n"
            f"── Actions proposed + policy decisions ──\n{actions_str}\n\n"
            f"── Action execution outcomes ──\n{outcomes_str}\n"
        )

    async def _run_single_skill(
        self, loaded: LoadedSkill, env: str, event_sink: Optional[EventSink] = None,
        run_id: Optional[str] = None,
    ) -> RunResult:
        """Run one matched skill end-to-end: investigation + action evaluation."""
        start = time.monotonic()

        await self._emit(
            event_sink,
            "skill_started",
            {
                "skill_name": loaded.spec.name,
                "template_name": loaded.compiled_template.name,
                "service": loaded.trigger.service,
            },
        )

        # 1. Run the compiled template
        symptom = self._extract_symptom(loaded.trigger)
        if loaded.trigger.source == "cascade":
            user_message = self._build_cascade_user_message(loaded, symptom)
        else:
            user_message = (
                f"A trigger event has fired. Investigate.\n\n"
                f"Symptom: {symptom}\n"
                f"Source: {loaded.trigger.source}\n"
                f"Matched pattern: {loaded.trigger.matched_pattern or 'n/a'}\n"
                f"Service: {loaded.trigger.service or 'unknown'}"
            )
        invocation_input = {
            # Merge the trigger payload so templates can bind $input.<field> — a
            # manual `-i path=…` (remote submit) or webhook fields. Without this
            # the template only saw symptom/user_message, so $input.path (etc.)
            # failed to resolve and every operation errored (#43). The canonical
            # fields below win on a key clash.
            **(getattr(loaded.trigger, "payload", None) or {}),
            "symptom": symptom,
            "user_message": user_message,
        }
        # Becomes the graph thread_id / durable workflow_id (compiler.run reads
        # initial_state.run_id), so a HITL pause is resumable by this id (#38).
        if run_id:
            invocation_input["run_id"] = run_id
        # Run inside an obs cost scope so per-call LLM spend is tallied (and the
        # per-investigation budget.max_cost_usd ceiling enforced via BudgetCallback).
        from cairn.core import obs

        cost_cap = template_cost_cap(loaded.compiled_template)
        with obs.cost_scope(f"run-{loaded.spec.name}", max_cost_usd=cost_cap) as cost_ctx:
            try:
                template_output = await run_within_budget(
                    loaded.compiled_template, invocation_input
                )
            except obs.BudgetExceeded as be:
                _log.warning("skill %s aborted on cost budget: %s", loaded.spec.name, be)
                template_output = budget_exceeded_output(str(be))
            template_output = _mark_if_over_cap(template_output, cost_ctx)
            if isinstance(template_output, dict):
                template_output["cost_usd"] = round(cost_ctx.cost_usd, 6)

        # 1b. Deterministic hallucination guard: verify every cited evidence ID
        #     actually exists in the data the investigation fetched (agent_loop
        #     tool results). Non-destructive — attaches a citation_report so the
        #     signal surfaces in output/notifications/memory. No-op when there's
        #     no ground truth (non-agent_loop) or nothing was cited.
        try:
            from cairn.core.guards.citation_validator import validate_citations

            cite_report = validate_citations(template_output)
            if isinstance(template_output, dict):
                template_output["citation_report"] = cite_report.to_dict()
            if cite_report.invalid:
                _log.warning(
                    "skill %s: %d/%d hallucinated citation(s) (rate=%.0f%%): %s",
                    loaded.spec.name,
                    len(cite_report.invalid),
                    cite_report.checked,
                    cite_report.hallucination_rate * 100,
                    [c.get("citation") for c in cite_report.invalid],
                )
        except Exception as e:  # validation must never break an investigation
            _log.warning("citation validation raised %s; ignoring", e)

        # 1c. Opt-in self-critique / reflection loop. A second LLM call reviews the
        #     output and (when below the confidence bar) re-runs the template with
        #     feedback, bounded by critique.max_passes ∧ budget.max_iterations.
        #     No-op unless the template declares an enabled `critique` block.
        # #417: a run a cost cap stopped gets no critique, which would spend again. Any
        # other run's critique counts towards the same cap, so its scope starts from
        # what the run has spent.
        if (getattr(loaded.compiled_template.spec, "critique", None) is not None
                and not cost_cap_reason(template_output)):
            crit_ctx = None
            try:
                from cairn.core.guards.critique import run_critique_loop

                budget = getattr(loaded.compiled_template.spec, "budget", None)
                max_iters = getattr(budget, "max_iterations", None) if budget else None
                with obs.cost_scope(f"critique-{loaded.spec.name}", max_cost_usd=cost_cap) as crit_ctx:
                    crit_ctx.cost_usd = cost_ctx.cost_usd
                    template_output = await run_critique_loop(
                        loaded.compiled_template,
                        template_output,
                        invocation_input,
                        max_iterations_cap=max_iters,
                    )
            except obs.BudgetExceeded as be:
                _log.warning("skill %s critique aborted on cost budget: %s",
                             loaded.spec.name, be)
                template_output = budget_exceeded_output(str(be))
            except Exception as e:  # critique must never break an investigation
                _log.warning("critique loop raised %s; ignoring", e)
            if crit_ctx is not None:
                template_output = _mark_if_over_cap(template_output, crit_ctx)
                if isinstance(template_output, dict):
                    template_output["cost_usd"] = round(crit_ctx.cost_usd, 6)

        # Stream the evidence as soon as the investigation produced it — this
        # is the "partial output" a follower sees before the run completes.
        if isinstance(template_output, dict):
            await self._emit(
                event_sink,
                "findings",
                {
                    "skill_name": loaded.spec.name,
                    "findings": template_output.get("findings"),
                    "output_type": template_output.get("type"),
                    "cost_usd": template_output.get("cost_usd"),
                },
            )

        # 2. Evaluate each action candidate against policies
        action_decisions: list[ActionDecision] = []
        # #417: a run a cost cap stopped doesn't act on what it found so far.
        for candidate in () if cost_cap_reason(template_output) else loaded.action_candidates:
            request = ActionRequest(
                candidate=candidate,
                environment=env,  # type: ignore[arg-type]
                context={"symptom": symptom, "skill": loaded.spec.name},
            )
            decision = self.policy_engine.evaluate(request)
            action_decisions.append(
                ActionDecision(candidate=candidate, decision=decision)
            )
            await self._emit(
                event_sink,
                "action_decision",
                {
                    "candidate_id": candidate.id,
                    "action_type": candidate.type,
                    "effect": decision.effect,
                },
            )

        # 3. If an ActionHandler was supplied, execute each non-denied decision.
        #    Otherwise we stop at decisions (preserves backwards-compatible behavior).
        action_outcomes: list[ActionOutcome] = []
        if self.action_handler is not None and action_decisions:
            findings: dict[str, Any] = {}
            if isinstance(template_output, dict):
                if isinstance(template_output.get("findings"), dict):
                    findings = template_output["findings"]
                # surface the service that triggered this so executors can default to it
                if loaded.trigger.service and "service" not in findings:
                    findings = {**findings, "service": loaded.trigger.service}

            executor_context = ExecutorContext(
                run_id=f"run-{loaded.spec.name}-{int(start)}",
                skill_name=loaded.spec.name,
                environment=env,  # type: ignore[arg-type]
                symptom=symptom,
                findings=findings,
            )

            for ad in action_decisions:
                outcome = await self.action_handler.execute(
                    ad.candidate, ad.decision, executor_context
                )
                action_outcomes.append(outcome)
                await self._emit(
                    event_sink,
                    "action_outcome",
                    {
                        "candidate_id": outcome.candidate_id,
                        "action_type": outcome.action_type,
                        "status": outcome.status,
                    },
                )

        duration = time.monotonic() - start

        # §24.11.3b: capture the template's content-addressed identity at
        # the moment of execution. Empty when the compiler path didn't
        # populate it (defensive — every modern path does).
        template_hash = getattr(loaded.compiled_template, "content_hash", "") or ""

        # §24.22 (AST3): snapshot the exact compiled AST so the run record
        # is self-contained. Same projection as the content hash; cheap
        # (frontmatter dict), guarded so a stray spec can't break a run.
        try:
            from cairn.core.engine.compiled import ast_dict
            template_ast = ast_dict(loaded.compiled_template.spec)
        except Exception:  # noqa: BLE001 — snapshot is non-critical
            template_ast = {}

        result = RunResult(
            skill_name=loaded.spec.name,
            template_name=loaded.compiled_template.name,
            template_hash=template_hash,
            template_ast=template_ast,
            trigger=loaded.trigger,
            raw_output=template_output,
            llm_cost_usd=float(template_output.get("cost_usd") or 0.0) if isinstance(template_output, dict) else 0.0,
            actions=action_decisions,
            action_outcomes=action_outcomes,
            duration_seconds=duration,
        )

        await self._emit(
            event_sink,
            "skill_completed",
            {
                "skill_name": loaded.spec.name,
                "template_name": loaded.compiled_template.name,
                "template_hash": template_hash,
                "duration_seconds": round(duration, 3),
                "action_count": len(action_decisions),
                "outcome_count": len(action_outcomes),
            },
        )

        # Fire post-resolution hook if the skill declared one via custom_logic
        if loaded.custom_logic is not None and loaded.custom_logic.on_resolve is not None:
            try:
                loaded.custom_logic.on_resolve(result.model_dump())
            except Exception as e:
                # Hooks should not break the investigation
                import logging
                logging.getLogger("cairn.runner").warning(
                    "custom_logic.on_resolve raised %s; ignoring", e,
                )

        # Episodic memory capture (#306, ADR-0052 §5): the ENVIRONMENT provisions
        # memory (profile [profile.backends] memory) — that's the gate. When on,
        # capture is a mandatory lifecycle guarantee for every run; a skill may
        # still hard opt out via memory_enabled=False (genuinely memory-free
        # skills). When the environment disables memory, never capture.
        skill_opted_out = getattr(loaded.spec, "memory_enabled", True) is False
        if _environment_memory_enabled() and not skill_opted_out:
            try:
                from cairn.core.clients.memory_client import (
                    write_investigation_to_memory,
                )
                mem_result = await write_investigation_to_memory(result)
                if isinstance(mem_result, dict) and mem_result.get("error"):
                    import logging
                    logging.getLogger("cairn.runner").info(
                        "memory write skipped: %s", mem_result["error"],
                    )
            except Exception as e:
                import logging
                logging.getLogger("cairn.runner").warning(
                    "memory write raised %s; ignoring", e,
                )

        # Notifier fan-out — fire-and-forget output to inbox / Slack / etc.
        # See src/templates_v2/notifier.py (Roadmap §3.6, 5th seam).
        if len(self.notifiers) > 0:
            try:
                outcomes = await self.notifiers.notify_all(result)
                import logging
                log = logging.getLogger("cairn.runner")
                for o in outcomes:
                    if not o.ok:
                        log.warning("notifier %s failed: %s", o.notifier, o.detail)
            except Exception as e:
                import logging
                logging.getLogger("cairn.runner").warning(
                    "notifier fan-out raised %s; ignoring", e,
                )

        return result

    @staticmethod
    def _extract_symptom(event: TriggerEvent) -> str:
        """Build a one-line symptom string from a trigger event."""
        # Explicit symptom fields in the payload win — a manual `-i symptom=…`
        # (remote submit) or a webhook body. For a MANUAL fire, matched_pattern
        # is the *skill name* (carried for routing/scoping, #76), NOT a symptom,
        # so it must not be used here — otherwise the RCA investigates the skill
        # name ("incident-orchestrated") instead of the incident and gathers no
        # evidence. matched_pattern is only a real symptom for pattern triggers.
        p = event.payload or {}
        for key in ("symptom", "message", "summary"):
            if p.get(key):
                return str(p[key])
        if event.source != "manual" and event.matched_pattern:
            return event.matched_pattern
        return f"{event.source} trigger ({event.service or 'unknown service'})"
