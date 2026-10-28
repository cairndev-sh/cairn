"""OperatorRegistry + ``@register_operator`` decorator.

Mirrors the action ``ExecutorRegistry`` shape (the platform's since #2497)
(verified via /verify-upstream before this module was added) — same
register/get/list pattern, different role.

Two ways to populate the registry:

  1. **In-process registration** via ``@register_operator`` — used by
     operator-pack ``__init__.py`` factories that import operator classes.
  2. **Entry-points discovery** via the ``cairn.operators`` group —
     OP2 #181. Loader scans installed packages and registers every
     declared operator at startup.

Both feed into the same :data:`default_registry` instance.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional, Type, TypeVar

from cairn.core.operator.protocol import Operation
from cairn.core.pack.manifest import OperatorEntry

logger = logging.getLogger("cairn.operator.registry")


class UnknownOperatorError(KeyError):
    """Raised when an operation binding references an unregistered operator."""


class DuplicateOperatorError(ValueError):
    """Raised when two operators try to register under the same name.

    Different ``version``s of the same name are allowed via :meth:`register`
    keeping the latest; this exception fires only when registration is
    strict. Every production load is non-strict: the last registration
    wins with a warning (see ``loader.load_all_operators``).
    """


T = TypeVar("T", bound=Operation)


class OperatorRegistry:
    """Maps ``<pack>/<operator-name>`` → Operation class.

    Lookup is by name; version is stored on the class for audit and
    can be enforced at compile time by the operation binding (§24.20.5
    ``version:`` field).
    """

    def __init__(self) -> None:
        #: Whether a full entry-point load has been ATTEMPTED against this
        #: registry (#1500). ``ensure_loaded`` used to infer this from the
        #: registry being non-empty, which conflates "the real operator set is
        #: present" with "somebody registered one thing" — a test that
        #: registers a fake operator left the registry non-empty-but-wrong, so
        #: every later consumer silently got 2 operators instead of 83.
        #: ``clear()`` resets this, so the self-healing the old heuristic was
        #: chosen for is preserved.
        self._load_attempted: bool = False
        self._operators: dict[str, Type[Operation]] = {}
        # §Phase-2.2 — manifest metadata keyed by the same operator name.
        # Populated by :meth:`attach_metadata` when the loader walks the
        # operator pack's ``pack.toml`` after the entry-point pass. Carries
        # risk + executors + lifecycle support flags. Records-only in
        # Phase 2; runtime dispatch + policy gates wire onto it in Phase 3+.
        self._metadata: dict[str, OperatorEntry] = {}

    # ── registration ──────────────────────────────────────────────────

    def register(
        self,
        op_cls: Type[Operation],
        *,
        strict: bool = False,
    ) -> None:
        """Register an Operation subclass.

        :param op_cls: An :class:`Operation` subclass decorated with
            :func:`register_operator` (so ``name`` / ``version`` are set).
        :param strict: If True, raise :class:`DuplicateOperatorError` on
            name collision. If False (default), the new registration
            replaces the old and a warning is logged.
        """
        name = getattr(op_cls, "name", None)
        if not name:
            raise ValueError(
                f"Operation class {op_cls.__name__} is missing `name` — "
                "decorate with @register_operator(name=..., version=...)."
            )
        version = getattr(op_cls, "version", None)
        if not version:
            raise ValueError(
                f"Operation class {op_cls.__name__} is missing `version` — "
                "decorate with @register_operator(name=..., version=...)."
            )
        if not hasattr(op_cls, "input_schema"):
            raise ValueError(
                f"Operation class {op_cls.__name__} is missing "
                "`input_schema` class attribute."
            )
        if not hasattr(op_cls, "output_schema"):
            raise ValueError(
                f"Operation class {op_cls.__name__} is missing "
                "`output_schema` class attribute."
            )

        if name in self._operators:
            existing = self._operators[name]
            # Idempotent: re-registering the SAME class is a no-op so that
            # decorator-side-effect-then-loader-walk doesn't double-fire
            # collision detection. Only DIFFERENT classes under the same
            # name count as a collision.
            if existing is op_cls:
                return
            if strict:
                raise DuplicateOperatorError(
                    f"Operator {name!r} already registered as "
                    f"{existing.__module__}.{existing.__name__} "
                    f"(v{getattr(existing, 'version', '?')}); "
                    f"refusing to overwrite with "
                    f"{op_cls.__module__}.{op_cls.__name__} (v{version})."
                )
            logger.warning(
                "operator %r re-registered: %s.%s (v%s) → %s.%s (v%s)",
                name,
                existing.__module__,
                existing.__name__,
                getattr(existing, "version", "?"),
                op_cls.__module__,
                op_cls.__name__,
                version,
            )
        self._operators[name] = op_cls

    def register_many(
        self,
        op_classes: list[Type[Operation]],
        *,
        strict: bool = False,
    ) -> None:
        for cls in op_classes:
            self.register(cls, strict=strict)

    # ── lookup ────────────────────────────────────────────────────────

    def get(self, name: str) -> Type[Operation]:
        """Return the Operation class for ``name`` or raise.

        :raises UnknownOperatorError: ``name`` is not registered.
        """
        if name not in self._operators:
            known = ", ".join(sorted(self._operators.keys())) or "(none)"
            raise UnknownOperatorError(
                f"No operator registered for {name!r}. Known: {known}"
            )
        return self._operators[name]

    def has(self, name: str) -> bool:
        return name in self._operators

    def list_names(self) -> list[str]:
        return sorted(self._operators.keys())

    def all(self) -> dict[str, Type[Operation]]:
        """Return a copy of the registered map (for doctor / introspection)."""
        return dict(self._operators)

    def clear(self) -> None:
        """Reset state. Tests use this; production code should not."""
        self._operators.clear()
        self._metadata.clear()
        # Also forget that a load was attempted, so the next consumption call
        # reloads — this is the self-healing `ensure_loaded` needs (#1500).
        self._load_attempted = False

    @property
    def load_attempted(self) -> bool:
        """Whether a full entry-point load has been attempted (#1500)."""
        return self._load_attempted

    def mark_load_attempted(self) -> None:
        """Record that the loader has run against this registry.

        Set even when the load yields nothing: a deployment with no operator
        packs installed must not re-walk entry points on every single call.
        """
        self._load_attempted = True

    # ── §Phase-2.2 manifest metadata ──────────────────────────────────

    def attach_metadata(self, entry: OperatorEntry) -> None:
        """Attach the ``pack.toml``-declared metadata for an operator.

        Idempotent and order-independent: metadata can be attached before
        OR after the Operation class registers — the loader walks
        entry-points then walks operator pack manifests, but custom test
        fixtures may do the reverse. Either way, ``get_metadata(name)``
        returns the entry once both halves are present.

        A duplicate attach for the same name overwrites silently: the
        ``pack.toml`` is the source of truth, and the second loader pass
        on the same pack is a no-op.
        """
        self._metadata[entry.name] = entry

    def get_metadata(self, name: str) -> Optional[OperatorEntry]:
        """Return the manifest metadata for ``name`` or ``None``.

        Returns ``None`` rather than raising — an operator may be
        registered via :func:`register_operator` in a test without ever
        having a ``pack.toml`` walked, and consumers should degrade
        gracefully on that path.
        """
        return self._metadata.get(name)

    def all_metadata(self) -> dict[str, OperatorEntry]:
        """Return a copy of the name → OperatorEntry map."""
        return dict(self._metadata)


# Module-level singleton. The @register_operator decorator registers here
# by default; tests/loaders that want isolation construct their own.
default_registry = OperatorRegistry()


# ─── @register_operator decorator ─────────────────────────────────────────


def register_operator(
    *,
    name: str,
    version: str,
    capabilities: Optional[list[str]] = None,
    registry: Optional[OperatorRegistry] = None,
    implements: Optional[list[str]] = None,
    strict: bool = False,
) -> Callable[[Type[T]], Type[T]]:
    """Class decorator: attach metadata and register the class.

    Usage (per §24.20.12):

    .. code-block:: python

        @register_operator(
            name="clickstack/query_logs",
            version="0.1.0",
            capabilities=["logs.search"],
        )
        class QueryLogsOp(Operation[QueryLogsInput, QueryLogsOutput]):
            input_schema = QueryLogsInput
            output_schema = QueryLogsOutput

            def run(self, inp, ctx):
                ...

    :param name: Operator name as it appears in operation bindings
        (``<pack>/<operator-name>`` convention).
    :param version: Semver. Stored on the class; bindings may declare
        a SpecifierSet against it.
    :param capabilities: Optional list of capability names this operator
        provides. Mirrors operator-pack ``pack.toml`` ``[operator.<name>.capabilities].provides``.
    :param registry: Override the registry to write to. Defaults to
        :data:`default_registry`. Tests use this for isolation.
    :param strict: Passed to :meth:`OperatorRegistry.register`.
    """
    target_registry = registry if registry is not None else default_registry

    def _decorate(op_cls: Type[T]) -> Type[T]:
        # Attach metadata to the class (mirrors how the operator-pack
        # pack.toml entry is mirrored on the class — single source of
        # truth at runtime).
        op_cls.name = name
        op_cls.version = version
        op_cls.capabilities = list(capabilities) if capabilities else []
        # #2241 B — validated once, here; a bad reference is a registration error
        from cairn.core.pack.contracts import validate_implements  # noqa: PLC0415 - the grammar is the pack owner's; lazy, as the rest of this module's pack use
        op_cls.implements = validate_implements(implements)
        # #1542 — fail-closed config declaration: an invalid `scope` value or
        # a config field absent from input_schema is a REGISTRATION error, not
        # a silent operational default at read time.
        from cairn.core.operator.config import validate_config_declaration  # noqa: PLC0415 - cycle (config imports protocol)
        validate_config_declaration(op_cls)
        target_registry.register(op_cls, strict=strict)
        return op_cls

    return _decorate


__all__ = [
    "OperatorRegistry",
    "default_registry",
    "register_operator",
    "UnknownOperatorError",
    "DuplicateOperatorError",
    "OperatorEntry",
]
