"""Profile — the reusable infrastructure bundle layer between pack and env.

A profile decides *which concrete backends serve a pack's semantic
requirements*. The same pack runs unchanged across stacks because pack ↔
profile ↔ env splits the three different decisions:

  - **pack**     declares semantic intent (``[requires.memory.foo]``,
                 ``[requires.mcp].servers = ["comms"]``, capabilities).
  - **profile**  picks technology (which MCP server URL serves ``comms``,
                 which operator pack provides ``logs.search``, which
                 embedder, which durable runtime).
  - **env**      applies a profile + supplies auth + per-env overrides.

This module is the loader for the profile TOML file. It's intentionally
narrow: it parses the file, validates the closed schema, and returns a
typed ``Profile``. Doctor + composition consume it.

Schema (Phase-1 §2 — profile layer extension):

::

    # profile.toml

    [profile]
    # Optional parent profile to inherit from. Resolved relative to this
    # file. One-level deep; no diamond chains.
    extends = "./local-dev.toml"

    [profile.capabilities]
    # Disambiguation when multiple packs provide the same capability —
    # the value is the *winning pack name*. Composition consumes this.
    "comms.post_message" = "operator-slack"

    [profile.bindings]
    # Substrate-level binding seam — capability → operator-pack ref.
    # Distinct from [profile.capabilities] (above): bindings are for the
    # "which operator handles this capability" question that the
    # composition pass doesn't answer today (it only disambiguates).
    "memory.search"   = "memory/sqlite-vector"
    "storage.put"     = "storage/fs"

    [profile.mcp_servers.comms]
    transport = "stdio"
    command   = ["slack-mcp"]
    tool_allow_list_strict = true

    [profile.mcp_servers.observability]
    transport = "http"
    url       = "https://grafana.internal.acme.com/mcp"

    [profile.notify]
    # #1791 — which approval moments reach the bound `notify.sink`. Omit
    # `events` for the default (the two pause kinds); pipeline failures are
    # opt-in. Secrets are env-var NAMES, never values.
    events      = ["awaiting_approval", "awaiting_advance", "pipeline_failed"]
    channel     = "#cairn-approvals"
    webhook_env = "SLACK_WEBHOOK_URL"

    [profile.budget]
    # ADR-0155 — the deployment's budget: the cap of a pipeline that declares
    # none, the most any launch is capped at, and the rate of the engine's own
    # machine. All optional; omit the table for no deployment budget.
    default_usd      = 20
    ceiling_usd      = 100
    local_hourly_usd = 0.80

    [profile.approval]
    # #256 (ADR-0034) — gate every operator of these risk classes behind a
    # human approval, on top of each operator's own `requires_approval`.
    mode            = "enforce"
    default_on_risk = ["paid_compute", "production_mutation"]

    [profile.backends]
    llm_gateway     = "https://bifrost.internal.acme.com"
    embedder        = "bge-large-en"
    durable_runtime = "temporal://temporal.internal.acme.com:7233"

The shape is *closed* in the same spirit as ``pack.toml``: unknown keys
or wrong shapes raise :class:`ProfileError` with the offending key
named. Typos surface at boot, not at runtime.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import logging
import math
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger(__name__)


class ProfileError(ValueError):
    """A ``profile.toml`` failed to parse or violated the schema."""


# Closed schema for the top-level keys of [profile] and each sub-table.
_PROFILE_TOP_KEYS = {"extends", "capabilities", "bindings", "mcp_servers", "backends", "policies",
                     # #1570 — catalog tier, same vocabulary as templates and
                     # pipelines (cairn/core/pack/curation.py). The environment
                     # picker offered SEVEN environments including three layers
                     # of a base-profile inheritance chain and two private ones;
                     # only `sandbox` and `prod` are things a person picks
                     # between. The schema is closed, so a typo fails at parse.
                     "visibility",
                     # #1791 — approval-notification opt-ins. Closed like the
                     # rest: a typo'd event name fails at parse rather than
                     # silently muting the gate nobody was told about.
                     "notify",
                     # ADR-0155 D1 — the deployment's budget default, ceiling and
                     # local-machine rate; a closed table of three optional numbers.
                     "budget",
                     # #256 (ADR-0034) — the deployment's coarse approval switch: gate
                     # every operator of a listed risk class. The resolver read it
                     # since ADR-0034; the closed schema refused the key until now.
                     "approval",
                     "pack_policies", "knowledge", "config"}
_NOTIFY_KEYS = {"events", "channel", "webhook_env"}
_BUDGET_KEYS = {"default_usd", "ceiling_usd", "local_hourly_usd"}
_APPROVAL_KEYS = {"mode", "default_on_risk"}
_APPROVAL_MODES = {"off", "enforce"}
_MCP_SERVER_KEYS = {"transport", "url", "command", "env", "tool_allow_list_strict"}
_MCP_TRANSPORTS = {"http", "stdio"}


@dataclass(frozen=True)
class McpServerSpec:
    """One MCP server's transport + connection details, as the profile
    binds it. Pack-side identifiers (server names) are mapped to one of
    these by the profile loader.
    """

    transport: str
    url: Optional[str] = None
    command: Optional[list[str]] = None
    env: Optional[dict[str, str]] = None
    tool_allow_list_strict: bool = False


@dataclass(frozen=True)
class Profile:
    """A loaded, merged profile.

    ``capabilities`` is the disambiguation dict the existing composition
    pass consumes (capability name → winning pack name). Everything else
    is new infrastructure shape that downstream layers (binding seam,
    doctor, MCP client) consume.

    ``source`` is the path the profile was loaded from — useful for
    error messages once doctor reports unbound capabilities or missing
    MCP servers.
    """

    capabilities: dict[str, str] = field(default_factory=dict)
    bindings: dict[str, str] = field(default_factory=dict)
    mcp_servers: dict[str, McpServerSpec] = field(default_factory=dict)
    backends: dict[str, Any] = field(default_factory=dict)  # mixed scalars: toggles(bool)+choosers/DSNs(str)
    # #317 (ADR-0053): store-policy names attached to this environment.
    # ``None`` = the profile omits the key → all enabled store policies apply
    # (back-compat). A list (incl. empty) = strict gating: only these store
    # policies apply. ``[]`` therefore means "pack policies only" (test-env
    # opt-out).
    # NB since #1103 pack/overlay policies are gated SEPARATELY by
    # ``pack_policies`` — they are no longer "always inherited regardless".
    policies: Optional[list[str]] = None
    # #1103 (ADR-0083 D4): the PACK-contributed policies this environment ADOPTS.
    # ``None`` = adopt all (back-compat — an upgrade must never silently remove a
    # gate); a list (incl. ``[]`` = adopt none) = strict. Pack policies are
    # PROPOSALS: the environment owns its mandates, so it must be able to decline
    # one an installed pack ships — including a LENIENT policy that would
    # otherwise weaken this deployment's own default.
    pack_policies: Optional[list[str]] = None
    # #496 (ADR-0061): knowledge ROLES activated for this environment (the
    # cross-pack composer's attach-list, mirroring ``policies``). ``None`` =
    # profile omits the key → all roles active (back-compat). A list (incl.
    # empty) = strict gating: only these roles are live; ``[]`` = no ambient
    # knowledge in this env. A corpus provides into a role via
    # ``[requires.rag.<name>].role``; workflows declare the slot in-template.
    knowledge: Optional[list[str]] = None
    # ADR-0068 (#742): per-operator deployment config, keyed by operator name
    # (e.g. ``config["dataset/gate"] = {"thresholds": {...}}``). Each value is a
    # PARTIAL of that operator's ``config_schema`` (deltas from the operator
    # default). Validated against the operator's declared ``config_schema`` by
    # doctor; deep-merged UNDER the run's bound input at invocation. ``extends``
    # deep-merges per operator (a child that overrides one knob keeps the
    # parent's sibling knobs). Empty dict = no config overrides (back-compat).
    config: dict[str, dict[str, Any]] = field(default_factory=dict)
    # #1791 (R5): `[profile.notify]` — which approval events fan out to the bound
    # `notify.sink`, and where. Keys: `events` (list of GATE_EVENT_KINDS; absent =
    # the pause events, failures opt-in), `channel` (destination override) and
    # `webhook_env` — the NAME of the env var holding the webhook, never the
    # secret itself (the secrets rule: a profile is committed, a secret is not).
    notify: dict[str, Any] = field(default_factory=dict)
    # ADR-0155 D1: `[profile.budget]` — `default_usd` (the cap of a pipeline that
    # declares none), `ceiling_usd` (the most any launch is capped at) and
    # `local_hourly_usd` (the rate of the engine's own machine). Each key optional,
    # a finite number >= 0; absent keys are absent here. Empty = no deployment
    # budget (today's behaviour, D8). `extends`: a child's key replaces its parent's.
    budget: dict[str, float] = field(default_factory=dict)
    # #256 (ADR-0034): `[profile.approval]` — `mode` ("off" | "enforce") and
    # `default_on_risk` (operator risk classes, pack.toml `risk`). With "enforce",
    # every operator whose risk is listed needs a human approval; the resolver's
    # "profile" authority (`approval/resolver.py`). Empty = no coarse switch.
    # `extends`: a child's key replaces its parent's.
    approval: dict[str, Any] = field(default_factory=dict)
    source: Optional[Path] = None


# ─── public loader ──────────────────────────────────────────────────


def load_profile(path: Path) -> Profile:
    """Read and validate a ``profile.toml``; return the merged ``Profile``.

    If the file sets ``[profile].extends = "<relative path>"``, the parent
    profile is loaded first (recursively, one level deep) and the child
    overrides each key.

    Raises :class:`FileNotFoundError` if ``path`` doesn't exist (callers
    that don't ship a profile simply don't call this).
    Raises :class:`ProfileError` on any schema violation.
    """
    if not path.exists():
        raise FileNotFoundError(f"profile file not found: {path}")

    visited: set[Path] = set()
    return _load_profile_recursive(path.resolve(), visited)


def load_profile_from_env() -> Profile:
    """Read the profile from ``CAIRN_PROFILE`` if set; otherwise empty.

    Used by :func:`cairn.core.pack.loader.load_all_packs` so callers
    don't have to thread the profile through every entry point. An
    unset env var means "no overrides" — the empty profile is well-formed.
    """
    path = getenv("CAIRN_PROFILE")
    if not path:
        return Profile()
    return load_profile(Path(path))


_PROFILE_HINT = (
    "Point it at a shipped deployment profile "
    "(/app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml in the engine image) "
    "or your own — see docs/runbooks/deployment-profile.md. For local development, "
    "`cairn dev` passes local-dev.toml."
)


def require_deployment_profile() -> Profile:
    """The profile a deployed process (``cairn-server``, ``cairn-worker``) runs under (#2296).

    Unset, blank, missing or malformed refuses: without a profile a deployment has no
    capability bindings (ADR-0111 D10 refuses every ``capability:`` operation) and
    redaction/audit off. The in-process dev loop never calls this.
    """
    path = getenv("CAIRN_PROFILE", "").strip()
    if not path:
        raise ProfileError(f"CAIRN_PROFILE is unset — a deployment must name its profile. {_PROFILE_HINT}")
    try:
        return load_profile(Path(path))
    except (OSError, UnicodeDecodeError, ProfileError) as e:
        raise ProfileError(f"CAIRN_PROFILE={path} cannot be loaded — {e}. {_PROFILE_HINT}") from e


# ─── recursive loader + merge ───────────────────────────────────────


def _load_profile_recursive(path: Path, visited: set[Path]) -> Profile:
    if path in visited:
        raise ProfileError(
            f"profile inheritance cycle detected at {path!r}; "
            f"chain: {sorted(p.name for p in visited)!r}"
        )
    visited.add(path)

    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise ProfileError(f"{path}: invalid TOML — {e}") from e
    except OSError as e:
        raise ProfileError(f"{path}: cannot read — {e}") from e

    profile_tbl = raw.get("profile", {}) or {}
    if not isinstance(profile_tbl, dict):
        raise ProfileError(f"{path}: [profile] must be a table")

    bad = sorted(set(profile_tbl.keys()) - _PROFILE_TOP_KEYS)
    if bad:
        raise ProfileError(
            f"{path}: unknown key(s) in [profile]: {bad!r}. "
            f"Allowed: {sorted(_PROFILE_TOP_KEYS)!r}."
        )

    # Reject any top-level table that isn't [profile] — keeps the file
    # focused and surfaces typos like [Profile] or [profle] loudly.
    other_top = sorted(set(raw.keys()) - {"profile"})
    if other_top:
        raise ProfileError(
            f"{path}: unknown top-level table(s): {other_top!r}. "
            f"Profiles only carry a [profile] table."
        )

    # Parent (extends) — resolved relative to this file's directory.
    parent: Optional[Profile] = None
    extends_val = profile_tbl.get("extends")
    if extends_val is not None:
        if not isinstance(extends_val, str) or not extends_val:
            raise ProfileError(
                f"{path}: [profile].extends must be a non-empty string"
            )
        parent_path = (path.parent / extends_val).resolve()
        if not parent_path.exists():
            raise ProfileError(
                f"{path}: extends path {parent_path!r} does not exist"
            )
        parent = _load_profile_recursive(parent_path, visited)

    own_caps = _parse_str_dict(
        profile_tbl.get("capabilities"), path, "profile.capabilities",
    )
    own_bindings = _parse_str_dict(
        profile_tbl.get("bindings"), path, "profile.bindings",
    )
    own_mcp = _parse_mcp_servers(profile_tbl.get("mcp_servers"), path)
    # backends hold mixed scalars: toggles (bool), backend choosers (str),
    # DSNs/paths (str). NOT str-only like capabilities/bindings — the backends
    # configurator (#303) writes booleans for toggle fields (e.g. memory = true).
    own_backends = _parse_scalar_dict(
        profile_tbl.get("backends"), path, "profile.backends",
    )
    # None when the key is absent (→ inherit / back-compat); a list (incl. []) when present.
    own_policies = _parse_str_list(
        profile_tbl.get("policies"), path, "profile.policies",
    )
    # #1103 — which PACK-contributed policies this environment adopts. None when
    # the key is absent (→ adopt all: the back-compat default, so an upgrade can
    # never silently remove a gate); a list (incl. []) when present.
    own_pack_policies = _parse_str_list(
        profile_tbl.get("pack_policies"), path, "profile.pack_policies",
    )
    own_knowledge = _parse_str_list(
        profile_tbl.get("knowledge"), path, "profile.knowledge",
    )
    own_config = _parse_config_table(profile_tbl.get("config"), path)
    own_notify = _parse_notify_table(profile_tbl.get("notify"), path)
    own_budget = _parse_budget_table(profile_tbl.get("budget"), path)
    own_approval = _parse_approval_table(profile_tbl.get("approval"), path)

    merged_caps: dict[str, str] = {}
    merged_bindings: dict[str, str] = {}
    merged_mcp: dict[str, McpServerSpec] = {}
    merged_backends: dict[str, Any] = {}
    if parent is not None:
        merged_caps.update(parent.capabilities)
        merged_bindings.update(parent.bindings)
        merged_mcp.update(parent.mcp_servers)
        merged_backends.update(parent.backends)
    merged_caps.update(own_caps)
    merged_bindings.update(own_bindings)
    merged_mcp.update(own_mcp)
    merged_backends.update(own_backends)
    # policies: present-wins (child fully controls its attach-list); else inherit parent.
    merged_policies = own_policies if own_policies is not None else (
        parent.policies if parent is not None else None
    )
    # pack-policy adoption list: present-wins, same as policies (#1103).
    merged_pack_policies = own_pack_policies if own_pack_policies is not None else (
        parent.pack_policies if parent is not None else None
    )
    # knowledge attach-list: present-wins, same as policies (#496).
    merged_knowledge = own_knowledge if own_knowledge is not None else (
        parent.knowledge if parent is not None else None
    )
    # config (#742): DEEP-merge per operator — a child overriding one knob keeps
    # the parent's other knobs for the same operator (unlike backends' shallow
    # per-key update, config values are nested tables).
    merged_config: dict[str, dict[str, Any]] = {}
    if parent is not None:
        for op_name, cfg in parent.config.items():
            merged_config[op_name] = dict(cfg)
    for op_name, cfg in own_config.items():
        merged_config[op_name] = _deep_merge_dict(merged_config.get(op_name, {}), cfg)
    # notify (#1791): shallow per-key, like backends — a child that names its own
    # channel keeps the parent's event list.
    merged_notify: dict[str, Any] = dict(parent.notify) if parent is not None else {}
    merged_notify.update(own_notify)
    # budget (ADR-0155 D1): shallow per-key, a child's value replaces its parent's.
    merged_budget: dict[str, float] = dict(parent.budget) if parent is not None else {}
    merged_budget.update(own_budget)
    _check_budget_order(merged_budget, path)
    # approval (#256): shallow per-key, a child's value replaces its parent's.
    merged_approval: dict[str, Any] = dict(parent.approval) if parent is not None else {}
    merged_approval.update(own_approval)

    logger.info(
        "profile loaded path=%s capabilities=%d bindings=%d "
        "mcp_servers=%d backends=%d extends=%r",
        path,
        len(merged_caps),
        len(merged_bindings),
        len(merged_mcp),
        len(merged_backends),
        extends_val,
    )

    return Profile(
        capabilities=merged_caps,
        bindings=merged_bindings,
        mcp_servers=merged_mcp,
        backends=merged_backends,
        policies=merged_policies,
        pack_policies=merged_pack_policies,
        knowledge=merged_knowledge,
        config=merged_config,
        notify=merged_notify,
        budget=merged_budget,
        approval=merged_approval,
        source=path,
    )


def _parse_budget_table(raw: Any, path: Path) -> dict[str, float]:
    """Parse ``[profile.budget]`` (ADR-0155 D1). Closed: an unknown key, a
    non-number, a negative or a non-finite value fails at parse."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [profile.budget] must be a table")
    bad = sorted(set(raw.keys()) - _BUDGET_KEYS)
    if bad:
        raise ProfileError(
            f"{path}: unknown key(s) in [profile.budget]: {bad!r}. "
            f"Allowed: {sorted(_BUDGET_KEYS)!r}."
        )
    out: dict[str, float] = {}
    for key, v in raw.items():
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise ProfileError(f"{path}: profile.budget.{key} must be a finite number >= 0")
        out[key] = float(v)
    _check_budget_order(out, path)
    return out


def _check_budget_order(budget: dict[str, float], path: Path) -> None:
    """``default_usd`` above ``ceiling_usd`` would name a cap the ceiling then
    overrides — refused at parse (ADR-0155 D1), after ``extends`` is applied too."""
    d, c = budget.get("default_usd"), budget.get("ceiling_usd")
    if d is not None and c is not None and d > c:
        raise ProfileError(
            f"{path}: profile.budget.default_usd ({d:g}) is above ceiling_usd ({c:g})"
        )


def _parse_approval_table(raw: Any, path: Path) -> dict[str, Any]:
    """Parse ``[profile.approval]`` (#256, ADR-0034). Closed: an unknown key, a
    missing or unknown ``mode``, or a ``default_on_risk`` entry that is not an
    operator risk class fails at parse, so a typo never leaves a gate off."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [profile.approval] must be a table")
    bad = sorted(set(raw.keys()) - _APPROVAL_KEYS)
    if bad:
        raise ProfileError(
            f"{path}: unknown key(s) in [profile.approval]: {bad!r}. "
            f"Allowed: {sorted(_APPROVAL_KEYS)!r}."
        )
    mode = raw.get("mode")
    if mode not in _APPROVAL_MODES:
        raise ProfileError(
            f"{path}: profile.approval.mode must be one of {sorted(_APPROVAL_MODES)!r}, got {mode!r}"
        )
    from cairn.core.pack.manifest import _OPERATOR_RISK_VALUES  # noqa: PLC0415 - the one risk vocabulary

    out: dict[str, Any] = {"mode": mode}
    if "default_on_risk" in raw:  # absent keys stay absent, so `extends` keeps the parent's list
        risks = raw["default_on_risk"]
        if not isinstance(risks, list) or not all(isinstance(r, str) for r in risks):
            raise ProfileError(f"{path}: profile.approval.default_on_risk must be a list of strings")
        unknown = sorted(set(risks) - _OPERATOR_RISK_VALUES)
        if unknown:
            raise ProfileError(
                f"{path}: unknown profile.approval.default_on_risk: {unknown!r}. "
                f"Allowed: {sorted(_OPERATOR_RISK_VALUES)!r}."
            )
        out["default_on_risk"] = list(risks)
    return out


def _parse_notify_table(raw: Any, path: Path) -> dict[str, Any]:
    """Parse ``[profile.notify]`` (#1791) — the approval-notification opt-ins.

    Closed on purpose: ``events`` is validated against the notifier's own
    vocabulary, so ``events = ["awaiting-approval"]`` fails at boot instead of
    quietly delivering nothing. ``webhook_env`` is an env-var NAME; a value that
    looks like a URL is rejected so a secret cannot be committed by accident.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [profile.notify] must be a table")
    bad = sorted(set(raw.keys()) - _NOTIFY_KEYS)
    if bad:
        raise ProfileError(
            f"{path}: unknown key(s) in [profile.notify]: {bad!r}. "
            f"Allowed: {sorted(_NOTIFY_KEYS)!r}."
        )
    out: dict[str, Any] = {}
    if "events" in raw:
        from cairn.core.notifier import GATE_EVENT_KINDS  # noqa: PLC0415 - cycle: notifier → pack.profile

        events = raw["events"]
        if not isinstance(events, list) or not all(isinstance(e, str) for e in events):
            raise ProfileError(f"{path}: profile.notify.events must be a list of strings")
        unknown = sorted(set(events) - set(GATE_EVENT_KINDS))
        if unknown:
            raise ProfileError(
                f"{path}: unknown profile.notify.events: {unknown!r}. "
                f"Allowed: {sorted(GATE_EVENT_KINDS)!r}."
            )
        out["events"] = list(events)
    for key in ("channel", "webhook_env"):
        if key in raw:
            v = raw[key]
            if not isinstance(v, str) or not v:
                raise ProfileError(f"{path}: profile.notify.{key} must be a non-empty string")
            out[key] = v
    hook = out.get("webhook_env", "")
    if "://" in hook:
        raise ProfileError(
            f"{path}: profile.notify.webhook_env must be the NAME of an env var "
            f"(e.g. \"SLACK_WEBHOOK_URL\"), not the URL itself — profiles are committed."
        )
    return out


# ─── per-table parsers ──────────────────────────────────────────────


def _parse_str_dict(
    raw: Any,
    path: Path,
    label: str,
) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [{label}] must be a table")
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(v, str) or not v:
            raise ProfileError(
                f"{path}: {label}.{k!r} must be a non-empty string"
            )
        out[k] = v
    return out


def _parse_scalar_dict(
    raw: Any,
    path: Path,
    label: str,
) -> dict[str, Any]:
    """Like :func:`_parse_str_dict` but accepts mixed scalars — str, bool, int,
    float — for the ``[profile.backends]`` table (toggles are booleans, backend
    choosers/DSNs are strings). Rejects nested tables / arrays."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [{label}] must be a table")
    out: dict[str, Any] = {}
    for k, v in raw.items():
        if isinstance(v, (dict, list)):
            raise ProfileError(
                f"{path}: {label}.{k!r} must be a scalar (string / bool / number)"
            )
        out[k] = v
    return out


def _deep_merge_dict(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``over`` onto ``base`` (``over`` wins at each leaf).

    Local to the profile loader (mirrors ``cairn.core.operator.config.deep_merge``)
    so the pack layer doesn't import the operator layer. Nested dicts merge
    key-wise; every other value replaces wholesale. Returns a new dict.
    """
    out = dict(base)
    for k, v in over.items():
        existing = out.get(k)
        if isinstance(v, dict) and isinstance(existing, dict):
            out[k] = _deep_merge_dict(existing, v)
        else:
            out[k] = v
    return out


def _parse_config_table(raw: Any, path: Path) -> dict[str, dict[str, Any]]:
    """Parse ``[profile.config]`` — a table of operator-name → config table
    (ADR-0068). Each value is an arbitrary nested table (a partial of the
    operator's ``config_schema``); the shape is NOT validated here — doctor
    validates each against the operator's declared ``config_schema``, so a typo'd
    key surfaces with the operator's field list rather than a generic error. We
    only enforce the two-level table structure."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [profile.config] must be a table")
    out: dict[str, dict[str, Any]] = {}
    for op_name, sub in raw.items():
        if not isinstance(sub, dict):
            raise ProfileError(
                f"{path}: [profile.config.{op_name!r}] must be a table "
                "(the operator's config knobs)"
            )
        out[op_name] = sub
    return out


def _parse_str_list(
    raw: Any,
    path: Path,
    label: str,
) -> Optional[list[str]]:
    """Parse an attach-list of non-empty strings. Returns ``None`` when the key
    is absent (distinct from an empty list, which is a valid strict-empty
    attach-list) so the caller can tell "not declared" from "declared empty"."""
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise ProfileError(f"{path}: [{label}] must be a list of strings")
    out: list[str] = []
    for x in raw:
        if not x.strip():
            raise ProfileError(f"{path}: [{label}] entries must be non-empty")
        if x not in out:
            out.append(x)
    return out


def _parse_mcp_servers(
    raw: Any,
    path: Path,
) -> dict[str, McpServerSpec]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: [profile.mcp_servers] must be a table")
    out: dict[str, McpServerSpec] = {}
    for server_name, sub in raw.items():
        if not isinstance(sub, dict):
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}] must be a table"
            )
        bad = sorted(set(sub.keys()) - _MCP_SERVER_KEYS)
        if bad:
            raise ProfileError(
                f"{path}: unknown key(s) in "
                f"[profile.mcp_servers.{server_name}]: {bad!r}. "
                f"Allowed: {sorted(_MCP_SERVER_KEYS)!r}."
            )

        transport = sub.get("transport")
        if not isinstance(transport, str) or transport not in _MCP_TRANSPORTS:
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}].transport must "
                f"be one of {sorted(_MCP_TRANSPORTS)!r}, got {transport!r}"
            )

        url = sub.get("url")
        if url is not None and (not isinstance(url, str) or not url):
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}].url "
                f"must be a non-empty string"
            )

        command = sub.get("command")
        if command is not None:
            if not isinstance(command, list) or not all(
                isinstance(x, str) for x in command
            ):
                raise ProfileError(
                    f"{path}: [profile.mcp_servers.{server_name}].command "
                    f"must be a list of strings"
                )

        # Transport-specific completeness check — caught at load time so
        # doctor doesn't have to re-derive.
        if transport == "http" and not url:
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}] with "
                f"transport='http' requires `url`"
            )
        if transport == "stdio" and not command:
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}] with "
                f"transport='stdio' requires `command`"
            )

        env_dict = sub.get("env")
        if env_dict is not None:
            if not isinstance(env_dict, dict) or not all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in env_dict.items()
            ):
                raise ProfileError(
                    f"{path}: [profile.mcp_servers.{server_name}].env "
                    f"must be a table of string → string"
                )

        strict = sub.get("tool_allow_list_strict", False)
        if not isinstance(strict, bool):
            raise ProfileError(
                f"{path}: [profile.mcp_servers.{server_name}]."
                f"tool_allow_list_strict must be bool"
            )

        out[server_name] = McpServerSpec(
            transport=transport,
            url=url,
            command=list(command) if command else None,
            env=dict(env_dict) if env_dict else None,
            tool_allow_list_strict=strict,
        )
    return out
