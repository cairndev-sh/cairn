"""``cairn lock`` / ``lock --check`` — pin the shipped pack closure by
content hash (§24.17 / ADR-0017).

Scope (ADR-0029): this is the **content-integrity** half of §24.17, not the
registry resolver (registry + cross-pack version solving stay parked — with one
monorepo there's nothing to resolve). ``lock`` writes ``cairn.lock`` pinning
every shipped pack (domain + operator, both tiers) by a deterministic content
sha; ``lock --check`` recomputes and fails if anything drifted — the CI gate
that catches "edited a pack but forgot to re-lock." Each entry also names the
``distribution`` that ships the pack (#2420: domain packs are ``cairn-pack-*``
data wheels, operator packs ``operator-*``), read from the pack's own
``pyproject.toml``.

The sha is content-addressed (aligns with ADR-0022): for each pack dir we hash
the sorted ``(relative_path, sha256(bytes))`` of its files, skipping caches and
build noise. No timestamps in the file → ``lock --check`` is a pure diff.
"""
from __future__ import annotations

import hashlib
import sys
import tomllib
from importlib.metadata import entry_points
from pathlib import Path

import typer

_LOCKFILE = "cairn.lock"
_LOCK_VERSION = 1

# Shipped pack locations (filesystem, mirrors the operator loader's tiers).
# Kind-first layout (ADR-0030): operators are grouped by tier under
# `operators/`; domain packs sit flat under `packs/` (all trust=builtin until
# a second pack forces the {builtins,official} split — ADR-0030 §3).
# Each is (tier, kind, dir-under-cairn).
_TIERS = [
    ("builtin", "domain", "packs"),
    ("builtin", "operator", "operators/builtins"),
    ("optional", "operator", "operators/optional"),
    ("official", "operator", "operators/official"),
]
# The lock pins the *declarative contract* (pack.toml + templates / skills /
# policies / operations / schemas), NOT the operator Python implementation —
# so a code-only change to an operator doesn't force a re-lock, but a manifest
# or template change does (ADR-0017: pack_sha over manifest + templates).
_SKIP_DIRS = {"__pycache__", ".pytest_cache", ".ruff_cache", "dist", "build",
              "src", "tests", "examples", "docs"}
_SKIP_FILES = {"README.md", "CHANGELOG.md", "LICENSE"}
_SKIP_SUFFIX = (".pyc", ".pyo")


def _cairn_root() -> Path:
    # this file: cairn/cli/commands/lock.py → parents[2] == cairn/
    return Path(__file__).resolve().parents[2]


def _engine_version() -> str:
    try:
        from importlib.metadata import version
        return version("cairndev")  # the distribution (ADR-0121 D3); the import name stays `cairn`
    except Exception:  # noqa: BLE001 — editable/uninstalled dev tree
        return "0+unknown"


def _pack_content_sha(pack_dir: Path) -> str:
    """Deterministic sha over a pack directory's content."""
    h = hashlib.sha256()
    files = []
    for p in sorted(pack_dir.rglob("*")):
        if not p.is_file() or p.suffix in _SKIP_SUFFIX or p.name in _SKIP_FILES:
            continue
        rel_parts = p.relative_to(pack_dir).parts
        if any(part in _SKIP_DIRS or part.endswith(".egg-info") for part in rel_parts):
            continue
        files.append(p)
    for p in files:
        rel = p.relative_to(pack_dir).as_posix()
        h.update(rel.encode())
        h.update(b"\0")
        h.update(hashlib.sha256(p.read_bytes()).digest())
    return "sha256:" + h.hexdigest()


def _read_pack_meta(pack_dir: Path) -> tuple[str, str]:
    """(name, version) from pack.toml; falls back to the dir name."""
    toml = pack_dir / "pack.toml"
    if not toml.is_file():
        return pack_dir.name, "0"
    try:
        data = tomllib.loads(toml.read_text())
        pack = data.get("pack", {})
        return pack.get("name", pack_dir.name), str(pack.get("version", "0"))
    except Exception:  # noqa: BLE001
        return pack_dir.name, "0"


def _read_distribution(pack_dir: Path) -> str | None:
    """The distribution that ships the pack: ``[project].name`` of the pack's own
    ``pyproject.toml`` (#2420 — every shipped pack, domain or operator, is a wheel)."""
    toml = pack_dir / "pyproject.toml"
    if not toml.is_file():
        return None
    try:
        return tomllib.loads(toml.read_text())["project"]["name"]
    except Exception:  # noqa: BLE001
        return None


def _resolve_closure() -> list[dict]:
    """Every shipped pack, sorted by name — the deterministic lock body."""
    root = _cairn_root()
    entries: list[dict] = []
    for tier, kind, rel in _TIERS:
        tier_dir = root / rel
        if not tier_dir.is_dir():
            continue
        for pack_dir in sorted(tier_dir.iterdir()):
            if not pack_dir.is_dir() or not (pack_dir / "pack.toml").is_file():
                continue
            name, version = _read_pack_meta(pack_dir)
            entries.append({
                "name": name, "version": version, "tier": tier, "kind": kind,
                "distribution": _read_distribution(pack_dir),
                "sha": _pack_content_sha(pack_dir),
            })
    entries.sort(key=lambda e: e["name"])
    return entries


def _installed_pack_wheels() -> int:
    """Distributions that register a domain or operator pack by entry point — what an
    empty closure is NOT pinning (#2536)."""
    dists = {ep.dist.name for group in ("cairn.packs", "cairn.operators")
             for ep in entry_points(group=group) if ep.dist is not None}
    return len(dists)


def _nothing_pinned_note() -> str:
    """Why a lock can pin nothing (#2536): the closure is the packs SHIPPED in this source
    tree (``_TIERS``). Dev packs on ``CAIRN_PACK_PATH`` are work in progress, not a shipped
    closure, and an installed wheel is versioned by the package lock, so neither is
    hashed here."""
    return (f"  this source tree ships no packs ({', '.join(f'cairn/{rel}' for _t, _k, rel in _TIERS)}"
            f" are absent); dev packs on CAIRN_PACK_PATH are not pinned, and installed pack "
            f"wheels ({_installed_pack_wheels()}) are versioned by your package lock, not hashed here")


def _render(entries: list[dict]) -> str:
    lines = [
        "# Generated by `cairn pack lock` — do not edit by hand.",
        "# Pins the shipped pack closure by content hash (ADR-0017 / ADR-0029).",
        f"lockfile_version = {_LOCK_VERSION}",
        f'engine_version = "{_engine_version()}"',
        "",
    ]
    for e in entries:
        lines += [
            "[[pack]]",
            f'name = "{e["name"]}"',
            f'version = "{e["version"]}"',
            *([f'distribution = "{e["distribution"]}"'] if e.get("distribution") else []),
            f'tier = "{e["tier"]}"',
            f'kind = "{e["kind"]}"',
            f'sha = "{e["sha"]}"',
            "",
        ]
    return "\n".join(lines)


def cmd(
    check: bool = typer.Option(
        False, "--check",
        help="CI gate: recompute and fail if cairn.lock is missing or stale.",
    ),
) -> None:
    """Write (or --check) cairn.lock pinning the shipped pack closure."""
    entries = _resolve_closure()
    path = Path(_LOCKFILE)

    if check:
        if not path.exists():
            print(f"✗ {_LOCKFILE} missing — run `cairn pack lock`", file=sys.stderr)
            raise typer.Exit(1)
        current = {e["name"]: e["sha"] for e in entries}
        try:
            locked_data = tomllib.loads(path.read_text())
            locked = {p["name"]: p["sha"] for p in locked_data.get("pack", [])}
        except Exception as e:  # noqa: BLE001
            print(f"✗ {_LOCKFILE} unreadable: {e}", file=sys.stderr)
            raise typer.Exit(1)

        drift = []
        for name in sorted(set(current) | set(locked)):
            if current.get(name) != locked.get(name):
                drift.append(name)
        if drift:
            print(f"✗ {_LOCKFILE} is stale — re-run `cairn pack lock`:", file=sys.stderr)
            for name in drift:
                if name not in locked:
                    print(f"    + {name} (new, unlocked)", file=sys.stderr)
                elif name not in current:
                    print(f"    - {name} (locked, gone)", file=sys.stderr)
                else:
                    print(f"    ~ {name} (content changed)", file=sys.stderr)
            raise typer.Exit(1)
        print(f"✓ {_LOCKFILE} up to date ({len(entries)} packs)")
        return

    path.write_text(_render(entries))
    print(f"wrote {_LOCKFILE} — {len(entries)} packs pinned")
    if not entries:
        print(_nothing_pinned_note())
