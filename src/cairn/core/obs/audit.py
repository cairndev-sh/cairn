"""Audit logging — the core interface, emission helper, and resolver.

Core owns the **interface** (`AuditSink`), the **event shape** (`AuditEvent`),
and the **emission seams** (the LLM gateway, the MCP boundary, and the action
handler call `audit_emit(...)` at the security-relevant points). Core owns **no
storage** — even the zero-dep default ships as a builtin service-provider
operator (`cairn-operator-audit-store`, a hash-chained append-only log that reuses an
existing file convention). See ADR-0028.

**Optional to turn ON, not optional to ESCAPE (ADR-0028):**
  - No sink bound  → `get_audit_sink()` returns `NullAuditSink` (drops events,
    zero cost). This is the default; `local-dev.toml` leaves `audit.sink`
    unbound.
  - A sink bound    → every seam emits; a pack cannot suppress an audit sink the
    deployment activated. The data owner (deployment) controls auditing via the
    `audit.sink` profile binding.

Resolution mirrors `obs.redact` exactly: the bound operator name comes from the
active profile's `[profile.bindings]` (`audit.sink`), the provider class comes
from the operator registry, and the result is cached — call `reset_audit_sink()`
after changing the binding (tests do).

Emission is fail-safe: audit must never break a request, so a misbehaving sink
is logged and the event is dropped, exactly like `obs.events.write`.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Optional, Protocol, runtime_checkable

from cairn.core._time import utc_now_iso

logger = logging.getLogger("cairn.obs.audit")

_BINDING_CAPABILITY = "audit.sink"


@dataclass(frozen=True)
class AuditEvent:
    """One security-relevant event at a seam.

    ``action`` is a dotted verb (``llm.complete``, ``tool.call``,
    ``action.requested``, ``action.executed``). ``outcome`` is one of
    ``ok`` | ``deny`` | ``error``. ``actor`` is who triggered it (client id /
    skill), ``target`` is what it acted on (model name, ``server/tool``,
    action type). ``metadata`` carries action-specific detail. ``ts`` is set at
    emission and is part of the hash-chained payload a sink may persist.
    """
    action: str
    outcome: str = "ok"
    actor: Optional[str] = None
    target: Optional[str] = None
    run_id: Optional[str] = None
    #: Tenancy scope (#229 B4, ADR-0048). Stamped from the request-resolved
    #: workspace at emission so the hash-chained audit is workspace-attributable.
    workspace_id: Optional[str] = None
    ts: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@runtime_checkable
class AuditSink(Protocol):
    """The core audit contract. Implementations ship as service-provider
    operators (ADR-0028); core never holds storage logic."""
    async def record(self, event: AuditEvent) -> None: ...


class NullAuditSink:
    """Inert default — returned when no sink is bound. Drops events."""
    async def record(self, event: AuditEvent) -> None:
        return None


_NULL: AuditSink = NullAuditSink()
_CACHE: Optional[AuditSink] = None


def _bound_provider_name() -> Optional[str]:
    """The operator name bound to `audit.sink` in the active profile, if any."""
    try:
        from cairn.core.pack.profile import load_profile_from_env
        return load_profile_from_env().bindings.get(_BINDING_CAPABILITY)
    except Exception as e:  # noqa: BLE001 — no profile / no binding = unbound
        logger.debug("no audit binding resolved: %s", e)
        return None


def get_audit_sink() -> AuditSink:
    """The bound `AuditSink`, or `NullAuditSink` when unbound (ADR-0028). Cached.

    Fail-open on *resolution*: a misconfigured binding degrades to the null sink
    (logged loud) so it never breaks a request. Emission itself is also
    fail-safe — see `audit_emit`."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    name = _bound_provider_name()
    if not name:
        _CACHE = _NULL
        return _CACHE
    try:
        from cairn.core.operator import default_registry
        try:
            op_cls = default_registry.get(name)
        except Exception:  # noqa: BLE001 — #1970: resolved before the packs were
            # discovered (a fresh process, or an early seam) → load them once and
            # retry, instead of caching the null sink for the process lifetime.
            from cairn.core.operator.loader import load_all_operators
            load_all_operators()
            op_cls = default_registry.get(name)
        inst = op_cls() if isinstance(op_cls, type) else op_cls
        if not hasattr(inst, "record"):
            logger.warning(
                "audit binding %r resolves to a non-AuditSink operator — using null sink",
                name,
            )
            _CACHE = _NULL
        else:
            logger.info("audit sink bound: %s", name)
            _CACHE = inst  # type: ignore[assignment]
    except Exception as e:  # noqa: BLE001 — fail-open
        logger.warning("audit binding %r unresolved (%s) — using null sink", name, e)
        _CACHE = _NULL
    return _CACHE


def reset_audit_sink() -> None:
    """Drop the cached sink (tests, or after a binding change)."""
    global _CACHE
    _CACHE = None


async def audit_emit(
    action: str,
    *,
    outcome: str = "ok",
    actor: Optional[str] = None,
    target: Optional[str] = None,
    run_id: Optional[str] = None,
    **metadata: Any,
) -> None:
    """Emit one audit event at a seam. No-op fast path when no sink is bound.

    Fail-safe: a sink that raises is logged and the event dropped — auditing
    never breaks the operation being audited."""
    sink = get_audit_sink()
    if sink is _NULL:
        return
    from cairn.core.workspace import current_workspace  # local: keep workspace off the low-level obs import path
    event = AuditEvent(
        action=action,
        outcome=outcome,
        actor=actor,
        target=target,
        run_id=run_id,
        workspace_id=current_workspace(),
        ts=utc_now_iso(),
        metadata=dict(metadata),
    )
    try:
        await sink.record(event)
    except Exception as e:  # noqa: BLE001 — telemetry must never break a request
        logger.warning("audit sink dropped %r event (%s)", action, e)


# ─── the hash-chain rule (ADR-0156 D3) ───────────────────────────────
#
# ONE definition of how an audit row is canonicalised and chained. The `audit/store`
# writer, `cairn lineage` and `cairn audit verify` all use it; no reader keeps a copy.

#: The ``prev_hash`` of the first row of a log: 64 zeros.
GENESIS_HASH = "0" * 64

_CHAIN_FIELDS = ("prev_hash", "hash")


def canonical(row: dict) -> str:
    """Stable serialization for hashing: key order and separators are fixed."""
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def chain_hash(prev: str, row: dict) -> str:
    """``sha256(prev_hash + canonical(row))``. ``row`` is the event payload, without the
    ``prev_hash`` / ``hash`` fields the writer adds."""
    return hashlib.sha256((prev + canonical(row)).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ChainBreak:
    """One broken link: ``row`` is the 1-based position among the non-blank lines;
    ``reason`` is ``hash``, ``prev_hash`` or ``corrupt line``."""
    row: int
    reason: str


@dataclass(frozen=True)
class ChainReport:
    """The result of :func:`verify_chain`."""
    rows: int = 0
    first_ts: Optional[str] = None
    last_ts: Optional[str] = None
    head_hash: str = GENESIS_HASH
    broken: tuple[ChainBreak, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.broken


def verify_chain(lines: Iterable[str]) -> ChainReport:
    """Recompute every row's hash from the previous one (genesis = 64 zeros).

    A row's ``prev_hash`` must equal the previous row's ``hash`` and its ``hash`` must equal
    :func:`chain_hash` of the row without those two fields. Blank lines are skipped; a line
    that is not a JSON object is a ``corrupt line`` break. Every break is collected, so the
    first is ``report.broken[0]``. Read-only: it only computes."""
    prev = GENESIS_HASH
    n = 0
    first_ts: Optional[str] = None
    last_ts: Optional[str] = None
    broken: list[ChainBreak] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        n += 1
        try:
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("not an object")
        except Exception:  # noqa: BLE001 — a corrupt line is itself a break
            broken.append(ChainBreak(n, "corrupt line"))
            continue
        payload = {k: v for k, v in row.items() if k not in _CHAIN_FIELDS}
        if row.get("prev_hash") != prev:
            broken.append(ChainBreak(n, "prev_hash"))
        elif row.get("hash") != chain_hash(prev, payload):
            broken.append(ChainBreak(n, "hash"))
        ts = row.get("ts")
        if ts:
            first_ts = first_ts or str(ts)
            last_ts = str(ts)
        prev = row.get("hash") or prev
    return ChainReport(rows=n, first_ts=first_ts, last_ts=last_ts, head_hash=prev,
                       broken=tuple(broken))
