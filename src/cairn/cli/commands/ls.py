"""``cairn ls`` — list what each installed pack contributes.

A registry walker. Renders, per pack: name, version, tags, its RUNNABLE SURFACE
(pipelines first, then flows as the components they are — ADR-0105 D3/D4,
#2115), code contributions (tools / executors / triggers / backends), declared
asset directories, and doctor checks. Reads only from the pack registry — never
imports any pack directly — so the surface is exactly what the engine sees.

Use cases:
  - Confirm a pack is discovered and loaded after ``pip install``.
  - Find the deployment profiles a pack ships, by installed path, to point
    ``CAIRN_PROFILE`` at one (#196).
  - See which capabilities are available before authoring a template.
  - Audit declared asset dirs vs what ``pack.toml`` claims.

``cairn ls operators`` (#1543, ADR-0092 principle 3) switches to a second view:
the operator ↔ kernel ↔ usage map. Same walker discipline — registry and entry
points only — answering "what runs ``scan.all``?" without a three-way grep.

``--json`` emits a machine-readable shape for scripting; the default is a
human-readable indented report. The set of fields rendered is intentionally
the same as the seven Phase-1 asset aggregators pinned in
``tests/core/test_pack_loader.py`` — adding a new aggregator there means
adding a corresponding asset line here.
"""

from __future__ import annotations

import json as _json
import os
from importlib.metadata import entry_points, packages_distributions
from pathlib import Path
from typing import Any

import typer

from cairn.core.pack import Pack, ensure_loaded, get_pack, get_packs
from cairn.core.refs import pack_key  # ONE ref spelling (#2126)


# Asset attributes to render. Mirrors the seven Phase-1 aggregators plus the
# four pre-Phase-1 dirs (templates/prompts/skills/policies). One row per attr
# only when the pack actually declares it.
_ASSET_FIELDS: tuple[tuple[str, str], ...] = (
    ("templates_dir", "templates"),
    ("prompts_dir",   "prompts"),
    ("skills_dir",    "skills"),
    ("policies_dir",  "policies"),
    ("schemas_dir",   "schemas"),
    ("triggers_dir",  "triggers"),
    ("evals_dir",     "evals"),
    ("rag_dir",       "rag"),
    ("memory_dir",    "memory"),
    ("examples_dir",  "examples"),
    ("data_dir",      "data"),
)


# ─── the runnable surface (ADR-0105 D3/D4, #2115) ──────────────────────────
#
# `cairn ls` used to render a pack's CODE contributions and its asset DIRS and
# stop there, so the two things a reader is actually looking for — what can I
# start, and what is it made of — were in two other commands (`cairn pipeline
# ls`, `cairn flow ls`) and in neither order. ADR-0105 says the pipeline is what
# you start and a flow is a component of one, so this section leads with
# PIPELINES and lists FLOWS beneath them, labelled as components.
#
# Best-effort by construction: resolving pipelines and flows parses pack files,
# and one unparseable file must not blank a listing whose whole job is to show
# what is installed.


def _pipelines_by_pack() -> dict[str, list[tuple[str, int, str]]]:
    """``pack -> [(name, leg count, leg kinds)]`` for every installed pipeline."""
    try:
        from cairn.core.engine.pipeline_resolve import all_pipeline_defs  # noqa: PLC0415 — engine off the CLI's import path
        defs = all_pipeline_defs()
    except Exception:  # noqa: BLE001 — a broken pipeline file is a validate problem
        return {}
    out: dict[str, list[tuple[str, int, str]]] = {}
    for d in defs:
        kinds = ",".join(sorted({s.leg_kind for s in d.steps}))
        out.setdefault(pack_key(d.pack), []).append((d.name, len(d.steps), kinds))
    for rows in out.values():
        rows.sort()
    return out


def _flows_by_pack() -> dict[str, list[tuple[str, str]]]:
    """``pack -> [(name, kind)]`` for every installed flow, all three kinds
    (ADR-0084 D2): data / ml from ``flows/*.yaml``, agentic from ``*.md``."""
    out: dict[str, list[tuple[str, str]]] = {}
    try:
        from cairn.core.dataflow.defs import load_flow_defs  # noqa: PLC0415 — as above
        for ref, d in load_flow_defs().items():
            pack, _, name = ref.partition("/")
            out.setdefault(pack_key(pack), []).append((name or ref, d.kind))
    except Exception:  # noqa: BLE001 — as above
        pass
    try:
        from cairn.cli.commands.flow import agentic_flows  # noqa: PLC0415 — sibling command, one scan
        for pack, name, _where in agentic_flows():
            out.setdefault(pack_key(pack), []).append((name, "agentic"))
    except Exception:  # noqa: BLE001 — as above
        pass
    for k, rows in out.items():
        out[k] = sorted(set(rows))     # flows/ and templates/ may name one file twice
    return out


# ─── shipped profiles (#196) ────────────────────────────────────────────────
#
# A pack may ship deployment profiles in `profiles/*.toml` beside its pack.toml
# (the training pack's `finetune-ssh.toml`, the example's `hello.toml`). Setting
# `CAIRN_PROFILE` needs the installed path, which otherwise means hunting in
# site-packages. Read-only: the pack root comes from the asset dirs the registry
# already holds, and each file is read only for its `[profile] visibility`.


def _module_root(p: Pack) -> Path | None:
    """The directory of the module that holds ``p`` (the pack's one-line
    ``Pack.from_manifest(__file__)`` shim), as imported: an installed pack's
    ``cairn.packs`` entry-point module, or a ``CAIRN_PACK_PATH`` pack's dev module.
    Not resolved, so a pack reached through a symlink keeps the path it was
    imported by."""
    import sys  # noqa: PLC0415

    from cairn.core.pack.loader import DEV_MODULE_PREFIX  # noqa: PLC0415 — loader internals, CLI-only read

    names = [ep.module for ep in entry_points(group="cairn.packs")]
    names += [n for n in list(sys.modules) if n.startswith(DEV_MODULE_PREFIX)]
    for name in names:
        mod = sys.modules.get(name)
        where = getattr(mod, "__file__", None)
        if where and any(v is p for v in vars(mod).values()):
            root = Path(os.path.abspath(where)).parent
            if (root / "pack.toml").is_file():
                return root
    return None


def _pack_root(p: Pack) -> Path | None:
    """The directory holding the pack's ``pack.toml``: any asset dir the manifest
    names, with its manifest-relative path taken off; else, when every asset dir
    resolves somewhere else (a symlink out of the pack), the directory of the
    module that holds the pack. ``None`` when neither finds a ``pack.toml``."""
    m = p.manifest
    if m is not None:
        for key, attr in Pack._ASSET_FIELDS.items():
            rel, path = getattr(m, key, None), getattr(p, attr, None)
            if not rel or not path or ".." in Path(rel).parts:
                continue
            root = Path(path).parents[len(Path(rel).parts) - 1]
            if (root / "pack.toml").is_file():
                return root
    return _module_root(p)


def _profiles_of(p: Pack) -> list[dict[str, str]]:
    """``[{name, path, absolute_path, visibility}]`` for each ``profiles/*.toml``
    the pack ships; ``visibility`` is read the way the console's picker reads it.
    Best-effort like the rest of this listing: a pack whose profiles cannot be
    read lists none rather than failing ``cairn ls``."""
    try:
        root = _pack_root(p)
        if root is None or not (root / "profiles").is_dir():
            return []
        from cairn.core.ui_contract import _profile_visibility  # noqa: PLC0415 — the one reading of a profile's tier (#1570)

        return [{"name": f.stem, "path": f"profiles/{f.name}", "absolute_path": os.path.abspath(f),
                 "visibility": _profile_visibility(f)}
                for f in sorted((root / "profiles").glob("*.toml"))]
    except Exception:  # noqa: BLE001 — an unreadable profiles dir is not a reason to blank the listing
        return []


def _pack_to_dict(p: Pack, *, pipelines: dict[str, list[tuple[str, int, str]]] | None = None,
                  flows: dict[str, list[tuple[str, str]]] | None = None) -> dict[str, Any]:
    """Project a Pack into a JSON-safe summary. Descriptor *factories* are
    elided (not JSON-safe); only names + categories survive.

    `pipelines` leads `flows` in the shape too (#2115): a script reading this is
    answering the same question a person is — what can I start here.
    """
    return {
        "name": p.name,
        "pipelines": [{"name": n, "legs": k, "leg_kinds": kinds.split(",") if kinds else []}
                      for n, k, kinds in (pipelines or {}).get(pack_key(p.name), [])],
        "flows": [{"name": n, "kind": k} for n, k in (flows or {}).get(pack_key(p.name), [])],
        "profiles": _profiles_of(p),
        "version": p.version,
        "description": p.description,
        "tags": list(p.tags),
        "assets": {
            label: getattr(p, attr)
            for attr, label in _ASSET_FIELDS
            if getattr(p, attr)
        },
        "tools": [
            {"name": t.name, "category": t.category, "description": t.description}
            for t in p.tools
        ],
        "executors": [e.action_type for e in p.executors],
        "triggers": [{"source": t.source, "path": t.path} for t in p.triggers],
        "backends": [b.name for b in p.backends],
        "settings_inventory": [s.env_var for s in p.settings_inventory],
        "doctor_checks": len(p.doctor_checks),
    }


def _render_text(packs: list[Pack]) -> str:
    if not packs:
        return "No packs installed.\n(Discoverable via the 'cairn.packs' entry-point group.)"

    pipelines, flows = _pipelines_by_pack(), _flows_by_pack()
    lines: list[str] = []
    for p in packs:
        lines.append(f"{p.name} ({p.version})")
        if p.description:
            lines.append(f"  {p.description}")
        if p.tags:
            lines.append(f"  tags: {', '.join(p.tags)}")

        # ADR-0105 — what you START, first. A pipeline is the runnable noun;
        # everything below it is what one is assembled from.
        if pipelines.get(pack_key(p.name)):
            lines.append("  pipelines (what you run):")
            for name, legs, kinds in pipelines[pack_key(p.name)]:
                lines.append(f"    - {p.name}/{name}  {legs} leg{'' if legs == 1 else 's'}"
                             + (f" ({kinds})" if kinds else ""))
        if flows.get(pack_key(p.name)):
            lines.append("  flows (components a pipeline runs; started alone, "
                         "one becomes a one-leg pipeline):")
            for name, kind in flows[pack_key(p.name)]:
                lines.append(f"    - {p.name}/{name}  [{kind}]")
        if not pipelines.get(pack_key(p.name)) and not flows.get(pack_key(p.name)):
            lines.append("  runnable: nothing — this pack contributes code, not "
                         "pipelines or flows")
        if (profiles := _profiles_of(p)):
            lines.append("  profiles (export CAIRN_PROFILE=<installed path>):")
            for prof in profiles:
                lines.append(f"    - {prof['path']}  [{prof['visibility']}]  {prof['absolute_path']}")

        # Contributions summary — one line per non-empty category.
        contrib: list[str] = []
        if p.tools:
            contrib.append(f"tools={len(p.tools)}")
        if p.executors:
            contrib.append(f"executors={len(p.executors)}")
        if p.triggers:
            contrib.append(f"triggers={len(p.triggers)}")
        if p.backends:
            contrib.append(f"backends={len(p.backends)}")
        if p.settings_inventory:
            contrib.append(f"settings={len(p.settings_inventory)}")
        if p.doctor_checks:
            contrib.append(f"doctor_checks={len(p.doctor_checks)}")
        if contrib:
            lines.append(f"  contributes: {', '.join(contrib)}")

        # Asset dirs — only render declared ones.
        declared = [
            (label, getattr(p, attr))
            for attr, label in _ASSET_FIELDS
            if getattr(p, attr)
        ]
        if declared:
            lines.append("  assets:")
            for label, path in declared:
                lines.append(f"    {label:<10s} {path}")

        # Tool capability names — what templates can request.
        if p.tools:
            lines.append("  capabilities:")
            for t in sorted(p.tools, key=lambda x: (x.category, x.name)):
                lines.append(f"    - {t.name} [{t.category}]")

        # Action types — what executors handle.
        if p.executors:
            lines.append("  action types:")
            for e in p.executors:
                lines.append(f"    - {e.action_type}")

        lines.append("")  # blank line between packs

    return "\n".join(lines).rstrip() + "\n"


# ─── `cairn ls operators` — the kernel ↔ operator ↔ usage map (#1543) ───────
#
# Three namespaces name the same unit of work and none of them could see each
# other: an OPERATOR (`dataset/scan_all`) is a registered class, a KERNEL
# (`scan.all`) is a `cairn.dataflow_stages` entry point that wraps one, and a
# flow/pipeline file names whichever of the two its runtime speaks. "What runs
# scan.all?" therefore had no answer short of grepping three places.

#: Reserved word in the positional slot. Not a pack name — no pack may take it.
_OPERATORS_VIEW = "operators"

#: Files a usage scan reads. Substring matching over the raw text: cheap, and
#: honest about what it is — it answers "is this name written down anywhere
#: shipped?", not "does this leg resolve" (that is `assert_runnable`'s job).
_USAGE_GLOBS: tuple[tuple[str, str], ...] = (
    ("flows_dir", "*.md"), ("flows_dir", "*.yaml"), ("flows_dir", "*.yml"),
    ("templates_dir", "*.md"),
    ("pipelines_dir", "*.yaml"), ("pipelines_dir", "*.yml"),
)


def _distribution_of(module: str, mapping: dict[str, list[str]]) -> str:
    """Wheel that ships a module — an operator's real packaging identity."""
    return (mapping.get(module.split(".")[0]) or [""])[0]


def _kernel_entry_points() -> list[tuple[str, str, str]]:
    """``(kernel name, target, distribution)`` for every dataflow stage."""
    out: list[tuple[str, str, str]] = []
    for ep in entry_points(group="cairn.dataflow_stages"):
        dist = getattr(getattr(ep, "dist", None), "name", "") or ""
        out.append((ep.name, ep.value, dist))
    return sorted(out)


def _shipped_text() -> list[tuple[str, str]]:
    """``(pack/filename, text)`` for every shipped flow / template / pipeline."""
    ensure_loaded()
    out: list[tuple[str, str]] = []
    for p in get_packs():
        for attr, pattern in _USAGE_GLOBS:
            d = getattr(p, attr, None)
            if not d or not Path(d).is_dir():
                continue
            for f in sorted(Path(d).glob(pattern)):
                try:
                    out.append((f"{p.name}/{f.name}", f.read_text(encoding="utf-8")))
                except OSError:      # unreadable file is a lint problem, not ours
                    continue
    return out


def _operator_rows() -> list[dict[str, Any]]:
    """One row per registered operator: name, wheel, wrapping kernels, usages.

    ``strict=False`` so one operator pack that fails to import does not blank
    the whole map — the point of the view is to SHOW what is installed.
    """
    from cairn.core.operator import default_registry, load_all_operators  # noqa: PLC0415 — heavy entry-point scan, CLI-only

    load_all_operators(strict=False)
    ops = default_registry.all()
    pkg_dists = packages_distributions()
    kernels = _kernel_entry_points()
    corpus = _shipped_text()

    op_dist = {
        name: _distribution_of(getattr(cls, "__module__", ""), pkg_dists)
        for name, cls in ops.items()
    }

    # Kernel → operator by (same wheel) + (dotted kernel name == operator leaf,
    # dots read as underscores): `scan.all` ⇄ `dataset/scan_all`. Correlation,
    # not a declared link — an unmatched kernel is reported, never guessed at.
    by_op: dict[str, list[str]] = {name: [] for name in ops}
    matched_kernels: set[str] = set()
    for kname, _target, kdist in kernels:
        leaf = kname.replace(".", "_")
        for oname in ops:
            if op_dist.get(oname) == kdist and oname.rpartition("/")[2] == leaf:
                by_op[oname].append(kname)
                matched_kernels.add(kname)

    rows: list[dict[str, Any]] = []
    for name in sorted(ops):
        needles = [name, *by_op[name]]
        rows.append({
            "operator": name,
            "distribution": op_dist.get(name, ""),
            "kernels": by_op[name],
            "used_by": [ref for ref, text in corpus
                        if any(n in text for n in needles)],
        })

    # Orphan kernels ride along as pseudo-rows so the view never hides a name
    # the engine will happily accept.
    for kname, target, kdist in kernels:
        if kname in matched_kernels:
            continue
        rows.append({
            "operator": "",
            "distribution": kdist,
            "kernels": [kname],
            "kernel_target": target,
            "used_by": [ref for ref, text in corpus if kname in text],
        })
    return rows


def _render_operators(rows: list[dict[str, Any]], *, unused: bool) -> str:
    real = [r for r in rows if r["operator"]]
    orphans = [r for r in rows if not r["operator"]]
    if not real and not orphans:
        return ("No operators registered.\n"
                "(Discoverable via the 'cairn.operators' entry-point group.)")

    def _table(rs: list[dict[str, Any]], first: str) -> list[str]:
        w_op = max([len(r["operator"] or r["kernels"][0]) for r in rs] + [len(first)])
        w_dist = max([len(r["distribution"]) for r in rs] + [len("PACK (WHEEL)")])
        head = f"{first:<{w_op}s}  {'PACK (WHEEL)':<{w_dist}s}  KERNELS / USED BY"
        body = [head, "-" * len(head)]
        for r in rs:
            label = r["operator"] or r["kernels"][0]
            body.append(f"{label:<{w_op}s}  {r['distribution']:<{w_dist}s}  "
                        f"kernels: {', '.join(r['kernels']) or '—'}")
            used = r["used_by"]
            body.append(f"{'':<{w_op}s}  {'':<{w_dist}s}  "
                        f"used by: {', '.join(used) if used else '(nothing shipped)'}")
        return body

    lines: list[str] = []
    if real:
        lines += _table(real, "OPERATOR")
    if orphans and not unused:
        lines += ["", "kernels with no name-matched operator "
                      "(the wrapped callable is not in the operator registry, "
                      "or its name does not correlate):"]
        lines += _table(orphans, "KERNEL")
    if unused:
        lines += ["", f"{len(real)} operator(s) referenced by no shipped "
                      f"flow / template / pipeline file."]
    return "\n".join(lines) + "\n"


def cmd(
    pack: str = typer.Argument(
        None,
        help="Pack name to restrict the listing to, or the view `operators` "
             "(one row per registered operator + the kernels and files using it).",
    ),
    json: bool = typer.Option(
        False, "--json",
        help="Emit a JSON shape instead of the human-readable report.",
    ),
    unused: bool = typer.Option(
        False, "--unused",
        help="`ls operators` only: show only operators no shipped file references.",
    ),
) -> None:
    """List packs and their contributions (or, with `operators`, the operator map)."""
    # #1543 — `operators` is a VIEW, not a pack name. The namespace question
    # "what runs scan.all?" has no other answer today: kernels live in an
    # entry-point group, operators in a registry, and usage in pack YAML/MD.
    if (pack or "").strip().lower() == _OPERATORS_VIEW:
        rows = _operator_rows()
        if unused:
            rows = [r for r in rows if not r["used_by"]]
        if json:
            typer.echo(_json.dumps(rows, indent=2))
        else:
            typer.echo(_render_operators(rows, unused=unused))
        return

    if unused:
        typer.echo("--unused applies to `cairn ls operators` only.", err=True)
        raise typer.Exit(code=2)

    ensure_loaded()

    if pack:
        one = get_pack(pack)
        if one is None:
            typer.echo(f"No pack named '{pack}'. Installed: "
                       f"{', '.join(p.name for p in get_packs()) or '(none)'}",
                       err=True)
            raise typer.Exit(code=1)
        packs = [one]
    else:
        packs = list(get_packs())

    # #1538 — a pack that FAILED to load used to vanish with only a startup
    # log warning; "why is my pack missing?" is answered here now.
    from cairn.core.pack.loader import pack_load_failures  # noqa: PLC0415 - loader internals, CLI-only read

    failures = pack_load_failures()
    if json:
        pipelines, flows = _pipelines_by_pack(), _flows_by_pack()
        out: Any = [_pack_to_dict(p, pipelines=pipelines, flows=flows) for p in packs]
        if failures:
            out = {"packs": out, "packs_failed_to_load": failures}
        typer.echo(_json.dumps(out, indent=2))
    else:
        typer.echo(_render_text(packs))
        for f in failures:
            typer.echo(
                f"\n✗ FAILED TO LOAD: {f['name']} ({f['target']})\n"
                f"    {f['error']}\n"
                f"    fix the pack (try `cairn validate` on its files) and reinstall",
                err=True,
            )
