"""
Policy engine: evaluates YAML policies against action requests.

Loads .yaml files from a policies directory. For each ActionRequest, runs all
applicable policies and merges their effects into a single PolicyDecision.

Effect precedence (most restrictive wins):
    deny > require_hitl > require_dry_run > allow_auto

If no policy matches, default is `allow_auto` for low-risk + non-prod,
otherwise `require_hitl` (safe default).

Future: OPA escape hatch for complex policies. Not in MVP.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import os
from pathlib import Path

import yaml

from cairn.core.engine.schema import (
    ActionRequest,
    PolicyDecision,
    PolicySpec,
    policy_tier,
)


# Effect ordering: lower number = more restrictive
_EFFECT_PRIORITY = {
    "deny": 0,
    "require_hitl": 1,
    "require_dry_run": 2,
    "allow_auto": 3,
}


def _more_restrictive(a: str, b: str) -> str:
    """Return the more restrictive of two effects."""
    return a if _EFFECT_PRIORITY[a] <= _EFFECT_PRIORITY[b] else b


# ─── Policy loading from disk ────────────────────────────────────────


def load_policy(path: Path | str) -> PolicySpec:
    """Parse a policy YAML file and return its validated PolicySpec."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Policy file not found: {path}")
    with path.open() as f:
        data = yaml.safe_load(f)
    if not data:
        raise ValueError(f"Empty policy file: {path}")
    return PolicySpec.model_validate(data)


def _pack_policy_dirs() -> list[Path]:
    """Policy directories contributed by installed packs (best-effort)."""
    try:
        from cairn.core.pack import all_policy_dirs, ensure_loaded

        ensure_loaded()
        return [Path(d) for d in all_policy_dirs()]
    except Exception:
        return []


def _user_policy_overlay_dir() -> Path:
    """Layer ② — the user-overlay file dir (hand / git authored). Override with
    ``CAIRN_POLICIES_DIR``; defaults to ``~/.cairn/policies``."""
    return Path(os.path.expanduser(getenv("CAIRN_POLICIES_DIR", "").strip() or "~/.cairn/policies"))


def _store_policies() -> list[PolicySpec]:
    """Layer ③ — console-authored policies from the DB store, parsed through the
    same ``PolicySpec.model_validate`` the file loader uses. Best-effort: a bad
    stored policy is logged + skipped, never crashes engine construction."""
    try:
        from cairn.core.policy.store import default_policy_store  # lazy: store pulls sqlite/env
    except Exception:
        return []
    store = default_policy_store()
    try:
        stored = store.list_sync()
    except Exception:
        return []
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
    out: list[PolicySpec] = []
    for sp in stored:
        if not sp.enabled:
            continue
        try:
            out.append(PolicySpec.model_validate(yaml.safe_load(sp.yaml)))
        except Exception as e:  # noqa: BLE001 — one bad stored policy mustn't blank the rest
            print(f"[policy_engine] stored policy {sp.name!r} failed to load: {e}")
    return out


def _dedupe_by_name(policies: list[PolicySpec]) -> list[PolicySpec]:
    """Collapse same-named policies keeping the LAST (so the overlay/store wins
    over a pack default of the same name)."""
    by_name: dict[str, PolicySpec] = {}
    for p in policies:
        by_name[p.name] = p
    return list(by_name.values())


def load_policies_directory(directory: Path | str) -> list[PolicySpec]:
    """Load all .yaml/.yml files in a directory."""
    directory = Path(directory)
    if not directory.exists():
        return []
    policies: list[PolicySpec] = []
    for path in sorted(directory.rglob("*.yaml")):
        try:
            policies.append(load_policy(path))
        except Exception as e:
            print(f"[policy_engine] Failed to load {path}: {e}")
    for path in sorted(directory.rglob("*.yml")):
        try:
            policies.append(load_policy(path))
        except Exception as e:
            print(f"[policy_engine] Failed to load {path}: {e}")
    return policies


# ─── Matching ────────────────────────────────────────────────────────


def policy_applies(spec: PolicySpec, request: ActionRequest) -> bool:
    """Return True if this policy applies to the given action request.

    Filters AND together. Each ``applies_to`` field, when non-empty, must
    match the request — including the §24.3.4 ``capabilities`` filter
    introduced in P2.f.
    """
    applies = spec.applies_to

    # `pipeline_legs` is matched against a pipeline's legs when it is PLANNED
    # (policy/pipeline_gates.py); a per-operation request carries no leg ref. A
    # policy that sets nothing else is therefore a plan-time policy only and
    # does not apply to individual operator calls.
    if applies.pipeline_legs and not (
        applies.action_types or applies.environments or applies.risks or applies.capabilities
    ):
        return False
    if applies.action_types and request.candidate.type not in applies.action_types:
        return False
    # #1766 — match the environment NAME or its TIER. A deployment runs names
    # (`campaign`, `staging-eu`); policies are written against tiers
    # (`production`). Matching both means a `production`-scoped policy still
    # governs a named production-tier environment, while a policy may still
    # name one exactly for precision. Widening the match can only ADD coverage,
    # never remove it — the safe direction for a governance filter.
    if applies.environments and not (
        request.environment in applies.environments
        or policy_tier(request.environment) in applies.environments
    ):
        return False
    if applies.risks and request.candidate.risk not in applies.risks:
        return False
    if applies.capabilities:
        # Cross-cutting policy targets a capability by name. The action must
        # have declared which capability it invokes; otherwise we can't tell
        # whether the policy is relevant, and the safe answer is "this
        # policy does not apply" — skills opt in to capability-scoped
        # policies by populating ActionCandidate.capability.
        if not request.candidate.capability:
            return False
        if request.candidate.capability not in applies.capabilities:
            return False
    return True


# ─── Engine ──────────────────────────────────────────────────────────


class PolicyEngine:
    """Loads policies and evaluates action requests against them."""

    def __init__(
        self,
        policies_directory: Path | str | None = None,
        *,
        attached_store_policies: set[str] | None = None,
        adopted_pack_policies: set[str] | None = None,
    ):
        # #1103 (ADR-0083 D4): pack/overlay policies are PROPOSALS, not mandates.
        #   None → adopt all (today's behavior, and the back-compat default: an
        #          upgrade must never silently REMOVE a gate someone relied on).
        #   a set (incl. empty) → strict: only these pack policies apply.
        # Why this axis exists: before #1103 a deployment could not opt out of a
        # policy shipped inside an installed pack — so a third-party pack could
        # impose gates on prod, or ship a LENIENT policy that weakened the
        # deployment's own default. Governance mandates belong to the
        # environment; pack authors propose.
        self._adopted_pack_policies = adopted_pack_policies
        # #317 (ADR-0053): the active environment's store-policy attach-list.
        #   None → all enabled store policies apply (back-compat — a profile that
        #          never opts in keeps today's behavior, no silent gap).
        #   a set (incl. empty) → strict: a store-sourced policy applies only if
        #          its name is in the set. Pack ①/overlay ② policies are always
        #          inherited regardless — only store-③-*only* names are gated.
        self._attached_store_policies = attached_store_policies

        # Names contributed by pack ① / overlay ② — always-on (inherited, not
        # detachable). A store policy that *overrides* one of these by name rides
        # along as always-on (its content wins via last-wins, but it is not gated).
        always_on_names: set[str] = set()

        # An explicit directory overrides pack discovery (dev/test); otherwise
        # policies come from every installed pack's contributed policies dir.
        if policies_directory is not None:
            self.policies: list[PolicySpec] = load_policies_directory(policies_directory)
        else:
            # ① pack-bundled defaults, then ② the user-overlay dir, then ③ the
            # console DB store — last-wins by name (store > overlay > pack).
            self.policies = []
            for d in _pack_policy_dirs():
                self.policies.extend(load_policies_directory(d))
            self.policies.extend(load_policies_directory(_user_policy_overlay_dir()))
            always_on_names = {p.name for p in self.policies}  # ① + ② names
            self.policies.extend(_store_policies())
            self.policies = _dedupe_by_name(self.policies)

        # Store-③-*only* names — the ones the environment attach-list gates.
        self._store_gated_names: set[str] = {
            p.name for p in self.policies if p.name not in always_on_names
        }

    def _is_active(self, spec: PolicySpec) -> bool:
        """Attachment gate (before ``applies_to`` matching) — two INDEPENDENT axes.

        * **pack/overlay policies** (#1103): proposals the environment adopts.
          ``None`` adoption list = adopt all (back-compat); a list = strict.
        * **store-only policies** (#317): attached to the environment by name;
          ``None`` attach-list = all (back-compat).
        """
        if spec.name not in self._store_gated_names:
            # pack/overlay-sourced
            if self._adopted_pack_policies is None:
                return True
            return spec.name in self._adopted_pack_policies
        if self._attached_store_policies is None:
            return True
        return spec.name in self._attached_store_policies

    def list_policies(self) -> list[str]:
        return [p.name for p in self.policies]

    def evaluate(self, request: ActionRequest) -> PolicyDecision:
        """Evaluate all applicable policies and return a merged decision."""
        applicable = [
            p for p in self.policies if self._is_active(p) and policy_applies(p, request)
        ]

        if not applicable:
            # Safe defaults: prod requires HITL even with no matching policy.
            # Keyed on the TIER (#1766), so a named production-tier environment
            # gets production's default instead of dev's — the old `== "production"`
            # test would have silently relaxed every named environment.
            default_effect = (
                "require_hitl" if policy_tier(request.environment) == "production"
                else "allow_auto"
            )
            return PolicyDecision(
                effect=default_effect,
                matched_policies=[],
                reasons=["no matching policy; safe default applied"],
            )

        # Merge: start permissive, narrow to most restrictive across all policies
        effect = "allow_auto"
        matched: list[str] = []
        reasons: list[str] = []
        channels: list[str] = []
        approval_role: str | None = None

        for policy in applicable:
            matched.append(policy.name)
            for rule in policy.rules:
                effect = _more_restrictive(effect, rule.effect)
                if rule.reason:
                    reasons.append(f"[{policy.name}] {rule.reason}")
                channels.extend(rule.channels)
                if rule.approval_role and not approval_role:
                    approval_role = rule.approval_role

        # Deduplicate channels while preserving order
        seen: set[str] = set()
        unique_channels: list[str] = []
        for c in channels:
            if c not in seen:
                seen.add(c)
                unique_channels.append(c)

        return PolicyDecision(
            effect=effect,
            matched_policies=matched,
            reasons=reasons or [f"matched {len(matched)} polic(y/ies)"],
            channels=unique_channels,
            approval_role=approval_role,
        )
