"""Where the tests find the tree (#2424): the engine, the operator packs, the domain packs,
and the one spelling a test uses for a file.

The monorepo keeps the engine at ``cairn/`` with every operator pack under
``cairn/operators/<tier>/<pack>``; the public tree (ADR-0121 D2) has ``src/cairn``, one
``packages/cairn-operator-<pack>`` per builtin and per optional pack (ADR-0134 D1), and no
official operator pack or domain pack. A tier names one glob or several; a pack directory
belongs to the FIRST tier that matches it, so the public tree lists its optional packs by
name ahead of the builtins' catch-all ``packages/cairn-operator-*``. A test that walks the tree reads the layout from here, never from a path literal, and
names a file in the monorepo spelling (``cairn/core/x.py``,
``cairn/operators/builtins/<pack>/src/…``) in both layouts, so its allowlists, synthetic
cases and module names derived from the path need no rewrite.
``scripts/oss/assemble_public_tree.py`` replaces the LAYOUT block below for the public
tree; it rewrites no literal in a test that imports this module.
"""
from __future__ import annotations

import functools
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# >>> LAYOUT (the public-tree assembler replaces this block)
#: The directory holding the ``cairn`` package.
ENGINE_PARENT = REPO / "src"
#: Operator pack directories by tier, as globs under the repository root (one or a tuple).
OPERATOR_PACK_GLOBS = {"optional": (), "builtins": "packages/cairn-operator-*"}
#: What an operator pack's directory name carries before the pack's own name.
PACK_DIR_PREFIX = "cairn-operator-"
#: The domain packs (the platform's); None where the tree has none.
DOMAIN_PACKS: Path | None = None
# <<< LAYOUT

ENGINE = ENGINE_PARENT / "cairn"
#: The platform's trees (official operator packs, domain packs) sit beside the engine:
#: a rule that derives private vocabulary from them, or proves debt in them, can.
PRIVATE_TREES = "official" in OPERATOR_PACK_GLOBS and DOMAIN_PACKS is not None


@functools.cache
def _packs() -> tuple[tuple[str, Path], ...]:
    found: dict[Path, str] = {}
    for tier, patterns in OPERATOR_PACK_GLOBS.items():
        for pattern in (patterns,) if isinstance(patterns, str) else patterns:
            for p in sorted(REPO.glob(pattern)):
                if (p / "pack.toml").is_file():
                    found.setdefault(p, tier)  # the first tier that matches owns the directory
    return tuple((tier, p) for p, tier in found.items())


def operator_packs(*tiers: str) -> list[Path]:
    """The operator pack directories (each holds a ``pack.toml``) of ``tiers``; every tier
    this tree has when none is named."""
    return [p for tier, p in _packs() if not tiers or tier in tiers]


def pack_name(pack_dir: Path) -> str:
    """An operator pack's name as the monorepo spells its directory (``rag_rerank``)."""
    return pack_dir.name.removeprefix(PACK_DIR_PREFIX).replace("-", "_")


def rel(path: Path) -> str:
    """``path`` in the monorepo spelling: ``cairn/…`` for the engine,
    ``cairn/operators/<tier>/<pack>/…`` for an operator pack, else repository-relative."""
    path = Path(path)
    if path.is_relative_to(ENGINE):
        return f"cairn/{path.relative_to(ENGINE).as_posix()}"
    for tier, pack in _packs():
        if path.is_relative_to(pack):
            return f"cairn/operators/{tier}/{pack_name(pack)}/{path.relative_to(pack).as_posix()}"
    return path.relative_to(REPO).as_posix()
