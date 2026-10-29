"""Policy-injected structural gates (#1252, ADR-0087 P0b).

The environment can require a gate the pipeline author cannot remove: a policy
whose ``applies_to.pipeline_legs`` globs match a leg ref fires at PLAN time —
a ``require_hitl`` rule inserts an approval gate immediately before every
matching leg, a ``deny`` rule refuses the launch outright, citing the policy.
"It is not in this file, and cannot be removed here" (the vocabulary brief) —
because it is not in the author's record at all; it is the environment's.

Design constraints, each load-bearing:

* **The SHARED evaluator** (:func:`runtime_policy_engine`, #1095) supplies the
  policy set — plan-time injection and dispatch-time enforcement can never
  disagree about which policies this deployment has.
* **Deterministic** — policies sorted by name, gate ids derived from
  ``(policy, step)``: same environment + same pipeline ⇒ same plan, so the
  record snapshot and any hash over it are stable.
* **No double-gating, but only by a decision that will be asked** — a leg that
  already sits behind an approval gate (transitively, through ``depends_on``)
  is not gated again when that gate always runs (it carries no ``when:``) and,
  if the policy names an ``approval_role``, is staffed by that role. A gate
  that can be routed off, or that asks someone else, does not stand in for the
  policy's own decision: a policy-injected gate is satisfied only by its own
  decision.
* **The audience is the policy's** — the injected gate's ``approvers`` is the
  rule's ``approval_role``, so the approve door (``_assert_may_decide``) applies
  the same ``approver_allowed`` rule it applies to any gate.
* **Visible provenance** — the injected step carries ``injected_by=<policy>``,
  so the approver and the audit trail see WHY the gate exists.

Matching speaks the bare ``pack/name`` form: a leg ref arrives as
``flow:<pack>/<name>``, ``task:<pack>/<op>`` (D0) or ``<pack>/<template>``; the
kind prefix is stripped (:func:`cairn.core.refs.bare_ref`, the one stripper)
before the glob so one pattern (``training/*``) covers every leg kind (#2539:
``task:`` was kept, so a bare glob never matched a task leg). A glob that spells
the prefix itself is also tried against the raw ref: ``task:storage/put`` keeps
matching as before, and a prefixed ``flow:``/``gate:``/``template:``/``pipeline:``
glob, which used to match nothing, now matches its own kind only.

Both sides fold PACK spelling (#2546): a pack is ``dataset-readiness`` in its
``pack.toml`` (what ``/flows`` and the console publish since #2527) and
``dataset_readiness`` in its directory (what shipped pipeline files still
write), and a policy written in one spelling must not be inert against a leg
written in the other. The leg's pack segment (before the first ``/``, after any
kind prefix) is tried in every spelling :func:`cairn.core.refs.spellings` gives
for its :func:`~cairn.core.refs.pack_key`; the glob's pack segment is folded by
``pack_key`` when it is literal. A glob whose pack segment itself holds a
wildcard (``dataset_*/*``) is left as written and still meets the leg in either
spelling. ``pack_key`` also lowercases, so a literal pack segment matches
regardless of case (``Training/*`` meets ``training/x``), the same notion of
"one pack" as :func:`~cairn.core.refs.ref_key`. Only the pack folds — the name
after the ``/`` is matched as written — and the fold is the one in
:mod:`cairn.core.refs`, not a copy of it.
"""
from __future__ import annotations

import fnmatch
import logging
from typing import Any, Optional

from cairn.core.refs import LEG_PREFIXES, bare_ref, pack_key, spellings

logger = logging.getLogger("cairn.policy.pipeline_gates")


class PolicyDeniedError(PermissionError):
    """A ``deny`` rule refused the launch; the message cites the policy."""


#: fnmatch metacharacters: a pack segment holding one is a pattern, not a name,
#: and cannot be folded (``dataset_*`` is not a pack).
_GLOB_CHARS = frozenset("*?[")


def _pack_parts(text: str) -> Optional[tuple[str, str, str]]:
    """``(kind prefix, pack segment, rest)`` of a ref or glob, or None with no ``/``."""
    prefix = next((p for p in LEG_PREFIXES if text.startswith(p)), "")
    pack, sep, rest = text[len(prefix):].partition("/")
    return (prefix, pack, rest) if sep else None


def _leg_forms(ref: str) -> set[str]:
    """The leg ref as written (raw and bare), plus each with its pack segment in
    every spelling the pack is recorded under (#2546)."""
    raw = str(ref or "")
    forms = {raw, bare_ref(raw)}
    for text in tuple(forms):
        parts = _pack_parts(text)
        if parts is None:
            continue
        prefix, pack, rest = parts
        forms.update(f"{prefix}{spelt}/{rest}" for spelt in spellings(pack_key(pack)))
    return forms


def _glob_forms(pattern: str) -> set[str]:
    """The glob as written, plus its literal pack segment folded by ``pack_key``."""
    forms = {pattern}
    parts = _pack_parts(pattern)
    if parts is not None and not _GLOB_CHARS.intersection(parts[1]):
        prefix, pack, rest = parts
        forms.add(f"{prefix}{pack_key(pack)}/{rest}")
    return forms


def leg_matches(ref: str, patterns: list[str]) -> bool:
    """Does any ``pipeline_legs`` glob match this leg ref?

    The bare ``pack/name`` form is the documented one (any D0 kind prefix
    stripped, so ``storage/put`` matches a ``task:storage/put`` leg); the raw
    ref is tried too, so a glob written with its prefix keeps matching. Pack
    spelling folds on both sides (#2546): ``dataset-readiness/*`` matches
    ``flow:dataset_readiness/check`` and ``dataset_readiness/*`` matches
    ``flow:dataset-readiness/check``.
    """
    legs = _leg_forms(ref)
    return any(fnmatch.fnmatch(leg, glob)
               for pat in patterns for glob in _glob_forms(str(pat))
               for leg in legs)


def _env_matches(spec: Any, environment: Optional[str]) -> bool:
    envs = list(getattr(spec.applies_to, "environments", None) or [])
    return not envs or (environment or "") in envs


def _leg_policies(engine: Any, environment: Optional[str]) -> list[Any]:
    """Active policies that declare ``pipeline_legs``, in NAME order (determinism)."""
    if engine is None:
        return []
    out = [
        p for p in getattr(engine, "policies", [])
        if getattr(p.applies_to, "pipeline_legs", None)
        and engine._is_active(p)
        and _env_matches(p, environment)
    ]
    return sorted(out, key=lambda p: p.name)


def gated_upstream(step: Any, by_id: dict[str, Any]) -> bool:
    """Does an approval gate already sit (transitively) upstream of ``step``?

    Public since #1792: it is the ONE answer to "is there a gate on this leg's
    path". The injector uses it to avoid double-gating, ``cairn validate``
    (via :func:`~cairn.core.engine.pipeline_explain.gate_path_problems`) uses
    it to flag a risky leg nobody will be asked about, and
    ``cairn pipeline explain`` renders it. Three readers, one walk.
    """
    seen: set[str] = set()
    frontier = list(step.depends_on)
    while frontier:
        dep_id = frontier.pop()
        if dep_id in seen:
            continue
        seen.add(dep_id)
        dep = by_id.get(dep_id)
        if dep is None:
            continue
        if getattr(dep, "approval", False):
            return True
        frontier.extend(dep.depends_on)
    return False


def _standing_gate_upstream(
    step: Any, by_id: dict[str, Any], *, role: Optional[str], governance: Any,
) -> bool:
    """Does a gate that will certainly be asked sit upstream of ``step``?

    Narrower than :func:`gated_upstream` on purpose (that one answers "is there
    a gate on the path", for ``validate``/``explain``). For injection a gate
    counts only when it cannot be routed off (no ``when:``) and, when the policy
    names an ``approval_role``, when its audience (the same
    ``required_approvers`` rule the approve door uses) names that role.
    """
    from cairn.core.approval.enforce import approver_names  # noqa: PLC0415 - cycle: policy → approval → policy
    from cairn.core.engine.pipeline_gate import required_approvers  # noqa: PLC0415 - cycle: policy → engine

    seen: set[str] = set()
    frontier = list(step.depends_on)
    while frontier:
        dep_id = frontier.pop()
        if dep_id in seen:
            continue
        seen.add(dep_id)
        dep = by_id.get(dep_id)
        if dep is None:
            continue
        if getattr(dep, "approval", False) and not getattr(dep, "when", None):
            if not role or role in approver_names(required_approvers(dep, governance)):
                return True
        frontier.extend(dep.depends_on)
    return False


def inject_policy_gates(
    plan: Any,
    *,
    environment: Optional[str] = None,
    engine: Any = None,
) -> list[str]:
    """Apply every ``pipeline_legs`` policy to ``plan`` IN PLACE.

    Returns the injected gate step ids (empty when no policy fired). Raises
    :class:`PolicyDeniedError` when a matching policy carries a ``deny`` rule —
    launch refusal is fail-closed and happens before any record exists.

    ``engine`` overrides the shared runtime evaluator (tests); the default is
    the ONE cached engine dispatch enforcement also consults (#1095).
    """
    if engine is None:
        from cairn.core.approval.policy_source import runtime_policy_engine  # noqa: PLC0415 - cycle: policy → approval → policy

        engine = runtime_policy_engine()
    policies = _leg_policies(engine, environment)
    if not policies:
        return []
    governance = getattr(plan, "governance", None) or {}

    from cairn.core.engine.bundle_pipeline import PipelinePlanStep  # noqa: PLC0415 - cycle: engine.bundle_pipeline imports nothing from policy, but policy is imported BY engine paths

    injected: list[str] = []
    for policy in policies:
        patterns = list(policy.applies_to.pipeline_legs)
        rules = list(policy.rules or [])
        deny = next((r for r in rules if r.effect == "deny"), None)
        hitl = next((r for r in rules if r.effect == "require_hitl"), None)
        if deny is None and hitl is None:
            continue  # allow_auto / require_dry_run have no plan-time meaning

        role = getattr(hitl, "approval_role", None) if hitl is not None else None
        by_id = {s.id: s for s in plan.steps}
        for step in list(plan.steps):
            if getattr(step, "approval", False) or getattr(step, "injected_by", None):
                continue  # never gate a gate
            if not leg_matches(step.ref, patterns):
                continue
            bare = bare_ref(step.ref)
            if deny is not None:
                raise PolicyDeniedError(
                    f"policy '{policy.name}' denies launching pipelines with leg "
                    f"'{bare}' in environment '{environment or 'default'}'"
                    + (f": {deny.reason}" if deny.reason else "")
                )
            if _standing_gate_upstream(step, by_id, role=role, governance=governance):
                logger.debug(
                    "policy %s: leg %s already behind a gate that always asks — not doubling",
                    policy.name, step.id,
                )
                continue
            gate_id = f"policy-gate-{policy.name}-{step.id}"
            gate_step = PipelinePlanStep(
                id=gate_id,
                ref=f"gate:{gate_id}",
                approval=True,
                approvers=role,
                depends_on=list(step.depends_on),
                environment=step.environment,
                injected_by=policy.name,
            )
            # the leg now waits on the gate; its original deps ride through it
            step.depends_on = [gate_id]
            plan.steps.insert(plan.steps.index(step), gate_step)
            by_id[gate_id] = gate_step
            injected.append(gate_id)
            logger.info(
                "policy %s injected gate %s before leg %s (env=%s)",
                policy.name, gate_id, step.id, environment or "default",
            )
    return injected


__all__ = ["PolicyDeniedError", "inject_policy_gates", "leg_matches"]
