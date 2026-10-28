"""SecretStore resolution + per-operation scoping (§24.20.13, ADR-0033).

Core owns the ``SecretStore`` interface (``protocol.py``) and this
resolver/scoper; it holds NO secret backend. The default provider ships as a
service-provider operator — ``cairn-operator-secrets-env`` (capability
``secrets.store``), env-var backed, zero-dep. Heavier backings (sops+age,
OpenBao) swap via the ``secrets.store`` profile binding, no core change.

Scoping (§24.20.13): an operator may read ONLY the keys its operation binding
declares in ``secrets:``. ``build_scoped_secrets(declared)`` wraps the bound
provider in a ``ScopedSecretStore`` that enforces this — an undeclared key
raises ``KeyError`` before the backend is ever consulted, so a typo can't leak
an unrelated secret.

Resolution mirrors ``obs.audit.get_audit_sink``: the bound operator name comes
from the active profile's ``[profile.bindings]`` (``secrets.store``), the
provider class from the operator registry, cached. Call
``reset_secret_provider()`` after a binding change (tests do).
"""
from __future__ import annotations

import logging
from typing import Mapping, Optional

from cairn.core.operator.protocol import NullSecretStore, SecretStore

logger = logging.getLogger("cairn.operator.secrets")

_BINDING_CAPABILITY = "secrets.store"
_UNSET = object()
_CACHE: object = _UNSET


class ScopedSecretStore:
    """A ``SecretStore`` restricted to a declared allow-list of keys.

    Enforces §24.20.13: only keys in the operation binding's ``secrets:`` block
    are readable; anything else raises ``KeyError`` without touching the backend.
    """

    def __init__(self, backend: SecretStore, allowed: list[str]) -> None:
        self._backend = backend
        self._allowed = set(allowed)

    def get(self, name: str) -> str:
        if name not in self._allowed:
            raise KeyError(
                f"secret {name!r} is not declared in this operation's "
                f"`secrets:` block (declared: {sorted(self._allowed)})"
            )
        return self._backend.get(name)

    def available_keys(self) -> list[str]:
        return sorted(self._allowed)


def _bound_provider_name() -> Optional[str]:
    try:
        from cairn.core.pack.profile import load_profile_from_env
        return load_profile_from_env().bindings.get(_BINDING_CAPABILITY)
    except Exception as e:  # noqa: BLE001 — no profile / no binding = unbound
        logger.debug("no secrets.store binding resolved: %s", e)
        return None


def get_secret_provider() -> Optional[SecretStore]:
    """The bound ``SecretStore`` provider, or ``None`` when unbound. Cached.

    Fail-open on resolution: a misconfigured binding logs and degrades to
    ``None`` (so a declared-but-unbacked secret raises at read time), never
    crashing the invocation pipeline.
    """
    global _CACHE
    if _CACHE is not _UNSET:
        return _CACHE  # type: ignore[return-value]
    name = _bound_provider_name()
    if not name:
        _CACHE = None
        return None
    try:
        from cairn.core.operator.registry import default_registry
        op_cls = default_registry.get(name)
        inst = op_cls() if isinstance(op_cls, type) else op_cls
        if not (hasattr(inst, "get") and hasattr(inst, "available_keys")):
            logger.warning(
                "secrets.store binding %r is not a SecretStore — ignoring", name
            )
            _CACHE = None
        else:
            logger.info("secrets.store bound: %s", name)
            _CACHE = inst
    except Exception as e:  # noqa: BLE001 — fail-open
        logger.warning("secrets.store binding %r unresolved (%s) — ignoring", name, e)
        _CACHE = None
    return _CACHE  # type: ignore[return-value]


def reset_secret_provider() -> None:
    """Drop the cached provider (tests, or after a binding change)."""
    global _CACHE
    _CACHE = _UNSET


class _AccountSecretValues:
    """The backend of a tenant account's ``ctx.secrets``: the values its own
    connections supplied for this invocation, and nothing else."""

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def get(self, name: str) -> str:
        if name not in self._values:
            raise KeyError(f"secret {name!r} is not supplied by a connection of this account")
        return self._values[name]

    def available_keys(self) -> list[str]:
        return sorted(self._values)


def build_scoped_secrets(
    declared: list[str],
    *,
    account_id: Optional[str] = None,
    values: Optional[Mapping[str, str]] = None,
) -> SecretStore:
    """Build the ``ctx.secrets`` for one operation invocation.

    Scoped to ``declared`` (the binding's ``secrets:`` keys), backed by the
    bound provider. Nothing declared → an empty ``NullSecretStore`` (no key
    readable). Keys declared but no provider bound → reads raise (fail-loud: the
    operation asked for a secret but the deployment bound no store).

    ``account_id`` is the run's account. For the deployment (none, empty or
    ``default``) nothing changes. For a tenant account the deployment's bound
    provider is never consulted (an env-backed one would hand out the server's
    own variables): the store holds only ``values``, what that account's own
    connections supplied for the declared keys.
    """
    if not declared:
        return NullSecretStore()
    from cairn.core.secrets_store import scoped_account  # noqa: PLC0415 — lazy: crypto off the import path

    if scoped_account(account_id) is not None:
        allowed = set(declared)
        return ScopedSecretStore(
            _AccountSecretValues({k: v for k, v in (values or {}).items() if k in allowed}), declared)
    provider = get_secret_provider()
    backend: SecretStore = provider if provider is not None else NullSecretStore()
    return ScopedSecretStore(backend, declared)


__all__ = [
    "ScopedSecretStore",
    "get_secret_provider",
    "reset_secret_provider",
    "build_scoped_secrets",
]
