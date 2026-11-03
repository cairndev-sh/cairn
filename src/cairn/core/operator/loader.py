"""Operator discovery via Python entry-points.

Mirrors :mod:`cairn.core.pack.loader` shape (verified by /verify-upstream
before this module was added).

Operator packs declare themselves under the ``cairn.operators``
entry-point group:

    [project.entry-points."cairn.operators"]
    "clickstack/query_logs" = "obs_operator_clickstack.operators.query_logs:QueryLogsOp"

Each entry-point resolves to an :class:`Operation` subclass. The class
must already be decorated with :func:`register_operator`, which means
importing it is sufficient to register it (the decorator runs at class
definition time). This loader:

  - Walks the entry-points group
  - Imports each class (auto-registers via decorator side-effect)
  - Catches broken imports so one bad operator pack doesn't take the
    engine down
  - Verifies the loaded object IS an Operation subclass
  - Returns the list of operator names discovered

Idempotent: safe to call multiple times. The registry is last-writer-wins
in non-strict mode, with a warning per duplicate name; in strict mode a
duplicate name raises :class:`DuplicateOperatorError`. Every production
path loads NON-strictly (the server, the Temporal worker, the DBOS registry,
``ensure_loaded``'s default), so two installed packs that declare the same
operator name do not stop a boot: the last registration wins.
"""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from pathlib import Path
from typing import Optional

from cairn.core.operator.protocol import Operation
from cairn.core.operator.registry import (
    DuplicateOperatorError,
    OperatorEntry,
    OperatorRegistry,
    default_registry,
)
from cairn.core.pack.manifest import (
    ManifestError,
    load_manifest,
)

_log = logging.getLogger("cairn.core.operator.loader")

ENTRY_POINT_GROUP = "cairn.operators"

# §Phase-2.2 / ADR-0027 + ADR-0030 — first-party operator packs ship inside
# cairn itself, kind-first under `operators/`, across two trust tiers:
# operators/builtins/ (substrate operators the engine's own services need) and
# operators/official/ (domain add-ons, e.g. the incident operators). Community
# packs (Phase 3+) install as separate wheels; their pack.toml location
# resolves via importlib.metadata distribution files. For now we walk both
# shipped tiers. A tier move is a directory rename within `operators/`, never a
# code change — operators register by stable name regardless of tier (ADR-0030).
_SHIPPED_OPERATOR_PACK_DIRS = tuple(
    Path(__file__).resolve().parent.parent.parent / "operators" / tier
    for tier in ("builtins", "optional", "official")
)


def load_all_operators(
    *,
    strict: bool = False,
    registry: Optional[OperatorRegistry] = None,
) -> list[str]:
    """Discover, load, and register every installed operator. Idempotent.

    :param strict: If True, raise :class:`DuplicateOperatorError` on
        cross-pack name collision. No production path passes it today:
        the server, workers and ``ensure_loaded`` load non-strictly, so
        a collision logs a warning and the last registration wins.
    :param registry: Override the registry to load into. Defaults to
        :data:`default_registry`. Tests use this for isolation.
    :return: Sorted list of operator names registered.
    """
    target = registry if registry is not None else default_registry

    for ep in entry_points(group=ENTRY_POINT_GROUP):
        try:
            obj = ep.load()
        except Exception as e:  # broken operator shouldn't take down the engine
            _log.warning(
                "failed to load operator %r (%s): %s",
                ep.name,
                ep.value,
                e,
            )
            continue

        # Entry-point resolved object MUST be an Operation subclass.
        if not isinstance(obj, type) or not issubclass(obj, Operation):
            _log.warning(
                "entry-point %r resolved to %r, not an Operation subclass — skipping",
                ep.name,
                obj if not isinstance(obj, type) else obj.__name__,
            )
            continue

        # The @register_operator decorator runs at class definition time,
        # so importing the class IS the registration step IF the operator
        # author decorated it. Detect undecorated classes (no `name`) and
        # warn rather than crash.
        op_name = getattr(obj, "name", None)
        if not op_name:
            _log.warning(
                "operator %r (%s) is not decorated with @register_operator — skipping",
                ep.name,
                ep.value,
            )
            continue

        # Always call register — the registry handles idempotency for the
        # same class and collision-detection for different classes under
        # the same name. This is what lets cross-pack duplicate names get
        # caught in strict mode even when each pack's @register_operator
        # targeted a different (private) registry.
        try:
            target.register(obj, strict=strict)
        except DuplicateOperatorError:
            if strict:
                raise
            _log.warning(
                "operator %r duplicate registration skipped (non-strict)", op_name
            )

    # §Phase-2.2 — attach declared manifest metadata to each registered
    # name. Runs after the entry-point pass so the metadata lands on
    # already-registered classes; a metadata entry without a matching
    # class is logged but not fatal (the pack.toml declared an operator
    # the entry-point machinery didn't surface — useful diagnostic for
    # a pack author who forgot to wire the [project.entry-points]).
    for entry in discover_operator_pack_manifests().values():
        if not target.has(entry.name):
            _log.warning(
                "operator pack manifest declares %r but no Operation "
                "class is registered under that name; check the "
                "[project.entry-points.\"cairn.operators\"] table",
                entry.name,
            )
            continue
        target.attach_metadata(entry)

    # Record the attempt even when nothing was found (#1500): a deployment
    # with no operator packs installed must not re-walk entry points on every
    # consumption call.
    target.mark_load_attempted()
    return target.list_names()


def discover_operator_pack_manifests() -> dict[str, OperatorEntry]:
    """Walk known operator pack ``pack.toml`` files and flatten their
    ``[operator."<n>"]`` tables into a single ``name → OperatorEntry`` map.

    Scans the shipped operator-pack tiers (:data:`_SHIPPED_OPERATOR_PACK_DIRS`:
    ``builtins/`` substrate + ``official/`` add-ons, ADR-0027). Phase 3+
    extends to community packs installed as wheels by walking entry-point
    distributions and locating each one's ``pack.toml``.

    Broken manifests are logged and skipped — one bad pack must not
    take the whole engine down at boot.
    """
    out: dict[str, OperatorEntry] = {}

    for tier_dir in _SHIPPED_OPERATOR_PACK_DIRS:
        if not tier_dir.is_dir():
            # Test fixtures or unusual installs may not have the directory;
            # nothing to walk for this tier.
            continue
        for pack_dir in sorted(tier_dir.iterdir()):
            if not pack_dir.is_dir():
                continue
            manifest_path = pack_dir / "pack.toml"
            if not manifest_path.is_file():
                continue
            try:
                manifest = load_manifest(manifest_path)
            except ManifestError as e:
                _log.warning(
                    "operator pack manifest %s failed to load: %s",
                    manifest_path,
                    e,
                )
                continue
            for op_name, entry in manifest.operators.items():
                if op_name in out:
                    # Two pack.tomls declared the same operator name. The
                    # entry-point pass catches duplicate Operation classes;
                    # we just log here and let the entry-point pass own the
                    # collision policy.
                    _log.warning(
                        "operator %r declared by multiple pack manifests; "
                        "last writer wins for metadata",
                        op_name,
                    )
                out[op_name] = entry

    # #1293 — the 'Phase 3+' extension, now load-bearing: packs installed as
    # WHEELS (community, or SDK-built via #1279) register their ops through
    # the entry-point pass, but until this walk their pack.toml was invisible
    # — every installed op carried NO risk class, so risk-keyed policy
    # (#1252) and the approval gate could not see it. Shipped tiers keep
    # priority: an installed manifest never overwrites an in-tree entry.
    for manifest_path in _installed_pack_manifests():
        try:
            manifest = load_manifest(manifest_path)
        except ManifestError as e:
            _log.warning(
                "installed pack manifest %s failed to load: %s", manifest_path, e)
            continue
        for op_name, entry in manifest.operators.items():
            if op_name in out:
                continue  # shipped tiers (and earlier dists) win
            out[op_name] = entry
    return out


def _installed_pack_manifests() -> list[Path]:
    """``pack.toml`` paths of packs installed as distributions.

    For each module that backs a ``cairn.operators``/``cairn.packs`` entry
    point, the manifest is looked up WHEEL-layout first (``<pkg>/pack.toml``,
    the #1279 force-include) with the source/editable fallback (``src/<pkg>``
    → pack root, two levels up) — the same resolution order the #1279 PACK
    shim uses. ``find_spec`` locates modules without executing them; a module
    the finder cannot resolve is skipped, never fatal.
    """
    import importlib.util  # noqa: PLC0415 — stdlib, but heavy finder machinery off the hot import path

    seen_roots: set[str] = set()
    found: list[Path] = []
    for group in (ENTRY_POINT_GROUP, "cairn.packs"):
        for ep in entry_points(group=group):
            root_module = ep.value.split(":", 1)[0].split(".", 1)[0]
            if root_module in seen_roots:
                continue
            seen_roots.add(root_module)
            try:
                spec = importlib.util.find_spec(root_module)
            except (ImportError, ValueError):
                continue
            if spec is None or not spec.origin:
                continue
            pkg_dir = Path(spec.origin).resolve().parent
            for candidate in (pkg_dir / "pack.toml",
                              pkg_dir.parent.parent / "pack.toml"):
                if candidate.is_file():
                    # the shipped tiers resolve HERE too (their packages sit
                    # next to their pack.toml) — harmless: the caller's
                    # first-wins rule keeps the in-tree walk authoritative.
                    found.append(candidate)
                    break
    return found


def ensure_loaded(
    *,
    strict: bool = False,
    registry: Optional[OperatorRegistry] = None,
) -> list[str]:
    """Load operators once, then serve from the registry.

    Keys off whether a load has been ATTEMPTED, not off registry emptiness
    (#1500). Emptiness conflates two different states: "nothing has loaded
    yet" and "somebody registered one thing". A test that registered a fake
    operator into the global registry left it non-empty-but-wrong, so this
    short-circuited and every later consumer got 2 operators instead of 83 —
    with no error, just a silently incomplete registry. That produced five
    `test_auto_eval` failures whose cause was three files away.

    ``clear()`` resets the flag, so the self-healing the old emptiness check
    was chosen for still works.
    """
    target = registry if registry is not None else default_registry
    if not target.load_attempted:
        return load_all_operators(strict=strict, registry=target)
    return target.list_names()


__all__ = [
    "ENTRY_POINT_GROUP",
    "load_all_operators",
    "ensure_loaded",
    "discover_operator_pack_manifests",
]
