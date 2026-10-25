"""Read the cairn-registry catalog — the AVAILABLE packs/bundles — and annotate
each entry installed-vs-available against the engine's live discovery.

Read-only. This is the AVAILABLE side of the registry; the install lifecycle
(write: resolve → install → record) is #211 / #185.

Source resolution (first hit wins):
  1. ``CAIRN_REGISTRY_CATALOG`` — a filesystem path OR an http(s) URL
  2. dev fallback: a sibling ``cairn-registry/index/catalog.json`` checkout
  3. default: the public extensions catalog (``cairndev-sh/cairn-extensions``, #2676). A
     deployment with its own (private) registry sets ``CAIRN_REGISTRY_CATALOG``.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

import httpx

_DEFAULT_URL = "https://raw.githubusercontent.com/cairndev-sh/cairn-extensions/main/catalog.json"

_log = logging.getLogger("cairn.registry_catalog")


def _repo_root() -> Path:
    # cairn/core/registry_catalog.py → parents[2] == the git repo root.
    return Path(__file__).resolve().parents[2]


def _source() -> str:
    env = getenv("CAIRN_REGISTRY_CATALOG")
    if env:
        return env
    sibling = _repo_root().parent / "cairn-registry" / "index" / "catalog.json"
    if sibling.is_file():
        return str(sibling)
    return _DEFAULT_URL


def _load_raw(src: str) -> dict[str, Any]:
    if src.startswith(("http://", "https://")):
        resp = httpx.get(src, timeout=10.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()
    return json.loads(Path(src).read_text())


def _installed_index() -> tuple[set[str], set[str]]:
    """(installed operator refs, installed template refs) from live discovery.

    Best-effort: an unloaded registry (standalone import) yields empty sets, so
    annotation degrades to "nothing installed" rather than crashing."""
    ops: set[str] = set()
    tpls: set[str] = set()
    try:
        # lazy: keep the operator registry off this module's import path (cycle-safe)
        from cairn.core.operator.registry import default_registry

        ops = set(default_registry.list_names())
    except Exception:  # noqa: BLE001 — discovery is best-effort
        pass
    try:
        # lazy: candidate_templates pulls the pack/engine surface
        from cairn.core.pack.discovery import candidate_templates

        tpls = {c["ref"] for c in candidate_templates()}
    except Exception:  # noqa: BLE001
        pass
    return ops, tpls


def _installed_pipelines() -> set[str]:
    """``<pack>/<pipeline>`` refs of every installed pipeline FILE (best-effort, like
    :func:`_installed_index`).

    It lists files and reads each pack's manifest name; it parses no pipeline. The ref is the
    one a launch uses: the pack is the ``[pack] name`` (``pipeline_pack_name``) and the pipeline
    name is the file's stem, which the loader requires to equal its ``name:``."""
    try:
        # lazy: the engine's pipeline discovery helpers
        from cairn.core.engine.pipeline import pipeline_pack_name  # noqa: PLC0415
        from cairn.core.engine.pipeline_resolve import all_pipeline_yaml_paths  # noqa: PLC0415

        return {f"{pipeline_pack_name(p)}/{p.stem}" for p in all_pipeline_yaml_paths()}
    except Exception:  # noqa: BLE001 — discovery is best-effort
        return set()


def _annotate(entries: list[dict[str, Any]], ops: set[str], tpls: set[str],
              pipelines: set[str] | None = None) -> None:
    """Mark each entry `installed` against live discovery (in place).

    A ``domain_pack`` (a pack that ships pipelines and no agentic flows) is installed when one of
    its ``pipelines`` is discovered as ``<ref>/<name>``; ``pipelines`` is that discovered set."""
    for e in entries:
        kind = e.get("kind")
        if kind == "operator_pack":
            e["installed"] = any(o in ops for o in e.get("operators", []))
        elif kind == "template_pack":
            ref = e.get("ref", "")
            e["installed"] = any(
                w in tpls or f"{ref}/{w}" in tpls for w in e.get("workflows", [])
            )
        elif kind == "domain_pack":
            ref = e.get("ref", "")
            e["installed"] = any(f"{ref}/{p}" in (pipelines or set()) for p in e.get("pipelines", []))
        else:
            e.setdefault("installed", False)


def _pack_composes(flow_dirs: Iterable[str | Path]) -> list[str]:
    """The distinct operators a template pack's flows invoke (its ``composes`` set).

    Walks each flow with the ONE canonical walker (#1178), which reads the
    operator off the node (inline form, #1107) or its binding file (the shared
    survivors). The previous version globbed ``operations/*.yaml`` only, so it
    would have reported an EMPTY set for every #1109-converted pack — had it
    been reachable at all (see the caller: it was passed an undefined name).

    Takes ALL of the pack's flow dirs (`flows/` + the `templates/` alias), not
    one, so a half-migrated pack reports its whole surface.

    ``follow_subgraphs=False``: this is the pack's OWN surface — a cross-pack
    subgraph's operators belong to that pack, not this one.
    """
    from cairn.core.engine.workflow_config import (  # noqa: PLC0415 — import cycle: registry_catalog ← engine
        collect_flow_operators,
    )

    refs: set[str] = set()
    for d in flow_dirs:
        for f in sorted(Path(d).glob("*.md")):
            try:
                refs.update(collect_flow_operators(f, follow_subgraphs=False))
            except Exception:  # noqa: BLE001 — one unreadable flow must not break the list
                continue
    return sorted(refs)


def _pipelines_by_pack() -> dict[str, list[str]]:
    """``{pack name: [pipeline names]}`` from discovery (best-effort)."""
    out: dict[str, list[str]] = {}
    for ref in sorted(_installed_pipelines()):
        pack, _, name = ref.partition("/")
        out.setdefault(pack, []).append(name)
    return out


def _live_entries() -> list[dict[str, Any]]:
    """Catalog entries derived from the ENGINE's own installed packs (#933).

    The official/installed tier can never go stale in the console because it's
    read from live discovery, not the mirrored snapshot. Operator packs come from
    the official operator-pack manifests; template packs from the discovered domain
    packs that ship agentic ``flows/*.md``; domain packs from those that ship only
    pipelines (listed by file, not parsed). A live domain_pack carries no ``install``:
    the pack's distribution name is not known here, so an external entry's is kept. Best-effort: any failure yields fewer live entries, never an exception
    (the endpoint must not 500). Each entry carries a private ``_live`` marker so
    the caller can keep ``installed=True`` past annotation."""
    entries: list[dict[str, Any]] = []

    # ── operator packs (official tier only — builtins are engine substrate, not
    # published/installable per the registry Mirror model) ──
    try:
        # lazy: keep the operator-pack loader off this module's import path (cycle-safe)
        from cairn.core.operator.loader import _SHIPPED_OPERATOR_PACK_DIRS
        from cairn.core.pack.manifest import load_manifest

        for tier_dir in _SHIPPED_OPERATOR_PACK_DIRS:
            if tier_dir.name != "official" or not tier_dir.is_dir():
                continue
            for pack_dir in sorted(tier_dir.iterdir()):
                mf = pack_dir / "pack.toml"
                if not mf.is_file():
                    continue
                try:
                    m = load_manifest(mf)
                except Exception:  # noqa: BLE001 — one bad manifest must not drop the tier
                    continue
                ops = sorted(m.operators)
                if not ops:
                    continue
                entries.append({
                    "ref": m.name, "kind": "operator_pack", "version": m.version,
                    "trust": (m.trust_level or "official"), "description": m.description,
                    "operators": ops,
                    "provides": sorted({c for e in m.operators.values() for c in e.provides_capabilities}),
                    "executors": sorted({x for e in m.operators.values() for x in e.executors}),
                    "gpu": any(e.requires_gpu for e in m.operators.values()),
                    "install": {"index": "pypi", "name": m.name},
                    "installed": True, "_live": True,
                })
    except Exception:  # noqa: BLE001 — discovery is best-effort
        pass

    # ── template / domain packs (discovered via the pack entry-point group) ──
    try:
        # lazy: load_all_packs pulls the pack/engine surface
        from cairn.core.pack.loader import load_all_packs

        by_pack: dict[str, list[str]] | None = None
        for p in load_all_packs():
            m = p.manifest
            # #1163 — read `flows/` AND the `templates/` alias; a migrated pack
            # was invisible in the catalog.
            from cairn.core.pack.registry import flow_dirs_of  # noqa: PLC0415 - cycle

            if m is None:
                continue
            dirs = [d for d in flow_dirs_of(p) if Path(d).is_dir()]
            workflows = sorted({f.stem for d in dirs for f in Path(d).glob("*.md")})
            if not workflows:
                # a pack that ships pipelines and no agentic flows is a domain_pack
                if by_pack is None:
                    by_pack = _pipelines_by_pack()   # once, and only if a pack needs it
                pipelines = sorted(by_pack.get(m.name, []))
                if pipelines:
                    entries.append({
                        "ref": m.name, "kind": "domain_pack", "version": m.version,
                        "trust": (m.trust_level or "official"), "description": m.description,
                        "pipelines": pipelines,
                        "installed": True, "_live": True,
                    })
                continue  # only surface packs that actually ship something runnable
            entries.append({
                "ref": m.name, "kind": "template_pack", "version": m.version,
                "trust": (m.trust_level or "official"), "description": m.description,
                "workflows": workflows, "composes": _pack_composes(dirs),
                "install": {"index": "pypi", "name": m.name},
                "installed": True, "_live": True,
            })
    except Exception:  # noqa: BLE001 — a broken pack must not empty the catalog
        # LOUD, not silent: a bare `pass` here hid an undefined-name bug
        # (`tdir`, dangling since the #1163 flows rename) that killed the whole
        # template-pack branch — the live catalog surfaced 20 operator packs and
        # ZERO template packs, and nobody noticed because catalog.json covered
        # for it. #1178.
        _log.warning("live registry catalog: template-pack scan aborted", exc_info=True)

    return entries


def load_registry_catalog(annotate: bool = True) -> dict[str, Any]:
    """The registry catalog, with `source` set and each entry's `installed` flag.

    #933 — the entries are the UNION of the engine's own installed/official tier
    (live discovery — always fresh) and the external mirror snapshot (the
    not-yet-installed / third-party "available" side). Live wins on ref collision,
    so the console never shows a stale official pack. Never raises: a bad/unreachable
    external source degrades to the live tier alone (with an `error` string)."""
    src = _source()
    error = ""
    try:
        catalog = _load_raw(src)
    except Exception as exc:  # noqa: BLE001 — a bad external source must not blank the live tier
        catalog, error = {"schema": 0, "channel": "", "entries": []}, str(exc)[:200]
    catalog.setdefault("entries", [])
    catalog["source"] = src
    if error:
        catalog["error"] = error

    # union: external entries first, then live overrides/extends by ref.
    by_ref: dict[str, dict[str, Any]] = {e["ref"]: e for e in catalog["entries"] if e.get("ref")}
    for le in _live_entries():
        prev = by_ref.get(le["ref"])
        if prev is None:
            by_ref[le["ref"]] = le
            continue
        # take the fresh volatile fields from live; keep rich external-only metadata
        # (per-workflow demo tiers) the live manifest walk doesn't carry.
        merged = {**prev, **le}
        if prev.get("demos") and not le.get("demos"):
            merged["demos"] = prev["demos"]
        if prev.get("description") and not le.get("description"):
            merged["description"] = prev["description"]
        by_ref[le["ref"]] = merged
    catalog["entries"] = list(by_ref.values())

    if annotate:
        ops, tpls = _installed_index()
        pipelines = (_installed_pipelines()
                     if any(e.get("kind") == "domain_pack" for e in catalog["entries"]) else set())
        _annotate(catalog["entries"], ops, tpls, pipelines)
    # live entries are installed by definition — never let annotation (which can't
    # match hyphen/underscore-normalised template refs) flip them back to available.
    for e in catalog["entries"]:
        if e.pop("_live", False):
            e["installed"] = True
    return catalog
