"""cairn pack — pack introspection (#109).

``cairn pack schema <ref>`` emits the **UI contract** (input/output JSON Schema
+ plan + steps + integrations) that every surface renders from — the keystone
of schema-driven UI: one contract, no per-pack hand-built screens.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import importlib
import importlib.util
import os
import sys
import tomllib
from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

# #109
app = typer.Typer(
    help="Inspect packs: the UI contract behind every surface.",
    no_args_is_help=True,
)
console = Console()


@app.command("schema")
def schema_cmd(
    template_ref: str = typer.Argument(
        ..., help="<pack>/<template>, e.g. training/finetune"
    ),
    inputs: list[str] = typer.Option(
        None, "--input", "-i", help="key=value input (repeatable)"
    ),
    profile: str = typer.Option(None, "--profile", help="execution profile path"),
    json_out: bool = typer.Option(False, "--json", help="emit the contract as JSON"),
) -> None:
    """Emit the UI contract for a template (inputs/outputs schema, plan, steps,
    integrations). Exit 0 if runnable."""
    from cairn.core.operator import load_all_operators
    from cairn.core.pack.loader import load_all_packs

    from cairn.core.ui_contract import build_ui_contract

    load_all_operators(strict=False)
    load_all_packs(strict=False)

    parsed: dict[str, str] = {}
    for kv in inputs or []:
        k, _, v = kv.partition("=")
        if k:
            parsed[k] = v

    # resolve connected MCP servers so remediation can flag unconnected ones
    connected: set[str] = set()
    try:
        from cairn.core.connections import default_connection_store
        conns = asyncio.run(default_connection_store().list())
        connected = {c.name for c in conns if getattr(c, "kind", "") == "mcp"}
    except Exception:  # noqa: BLE001 — remediation is best-effort, never block
        connected = set()

    contract = build_ui_contract(
        template_ref, inputs=parsed, profile=profile, connected_names=connected,
    )
    if json_out:
        console.print_json(contract.model_dump_json())
    else:
        _render(contract)
    raise typer.Exit(0 if contract.can_run else 1)


def _render(c) -> None:  # noqa: ANN001
    if c.ready:
        status = "[green]✓ ready[/]"
    elif c.can_run:
        status = "[yellow]⚠ needs setup[/]"
    else:
        status = "[red]✗ blocked[/]"
    console.print(Panel(
        f"[bold]{c.title}[/]  ·  {c.pack}/{c.template}  ·  {status}"
        + (f"\n{c.description}" if c.description else ""),
        title="cairn UI contract", border_style="cyan",
    ))

    props = c.inputs.get("properties", {})
    if props:
        required = set(c.inputs.get("required", []))
        t = Table("input", "type", "required", "default", "choices", title="inputs (form)")
        for name, p in props.items():
            t.add_row(
                name, str(p.get("type", "")), "yes" if name in required else "-",
                str(p.get("default", "")) if "default" in p else "-",
                ", ".join(map(str, p.get("enum", []))) or "-",
            )
        console.print(t)

    if c.steps:
        t = Table("#", "step", "capability", "operator", "approval", title="steps (timeline)")
        for i, s in enumerate(c.steps, 1):
            t.add_row(
                str(i), s.title, s.capability or "-",
                s.operator if s.registered else f"[red]{s.operator} (missing)[/]",
                "[yellow]yes[/]" if s.requires_approval else "-",
            )
        console.print(t)

    if c.remediation:
        console.print(Panel(
            "\n".join(f"• {r.what}\n  → [bold]{r.fix}[/]" for r in c.remediation),
            title="[yellow]remediation — missing → fix[/]", border_style="yellow",
        ))

    out_props = c.outputs.get("properties", {})
    if out_props:
        t = Table("field", "type", "description", title="report (output)")
        for name, p in out_props.items():
            t.add_row(name, str(p.get("type", "")), (p.get("description", "") or "")[:50])
        console.print(t)

    base = (getenv("CAIRN_REMOTE_ENDPOINT") or "http://localhost:9090").rstrip("/")
    terminal = (f"cairn run {c.template_ref} --input in.json"
                if c.inputs.get("properties") else c.recommended_command)
    console.print(Panel(
        f"[bold]browser [/] {base}/ui/{c.slug}\n"
        f"[bold]terminal[/] {terminal}",
        title="run it — fill in the browser or the terminal", border_style="green"))
    if c.profiles:
        console.print(
            f"[dim]profiles: {', '.join(c.profiles)}  ·  active: {c.execution_profile or '(default)'}"
            f"  ·  pick with [/][bold]--profile <name>[/]")


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - parity with siblings
    app()
    return 0


# #1105
@app.command("check")
def check_cmd(
    json_out: bool = typer.Option(
        False, "--json", help="Emit findings as JSON (for CI).",
    ),
) -> None:
    """Fail on manifest ↔ code drift in operator packs.

    Every operator is declared twice — in code (`@register_operator` + the
    Operation class) and in `pack.toml`. They drift, and because the manifest is
    what governance reads (`risk` decides whether a gate opens), a drifted
    manifest is a governance-relevant inconsistency rather than cosmetics.

    Exit 0 = clean, 1 = drift found. Made for CI: the two cases this catches both
    previously surfaced as a log warning nobody acted on.
    """
    import json as _json

    from cairn.core.pack.drift import check_installed_operator_drift

    findings = check_installed_operator_drift()

    if json_out:
        typer.echo(_json.dumps(
            [{"kind": f.kind, "operator": f.operator, "detail": f.detail} for f in findings],
            indent=2,
        ))
        raise typer.Exit(1 if any(f.is_drift for f in findings) else 0)

    real = [f for f in findings if f.is_drift]
    if not findings:
        console.print("[green]✓[/] every operator manifest matches its registered class")
        raise typer.Exit(0)

    by_kind: dict[str, list] = {}
    for f in findings:
        by_kind.setdefault(f.kind, []).append(f)
    if real:
        console.print(f"[red]✗ {len(real)} manifest/code drift finding(s)[/]\n")
    else:
        console.print("[green]✓[/] no manifest/code drift; notes below\n")
    for kind, group in sorted(by_kind.items()):
        console.print(f"[bold]{kind}[/] ({len(group)})")
        for f in group:
            # escape: rich reads [..] as markup, which ate the
            # [project.entry-points."cairn.operators"] hint.
            console.print(f"  • {f.detail}", markup=False)
        console.print()
    raise typer.Exit(1 if real else 0)


# ─── `cairn pack add` — the local dev loop (#1545, ADR-0092 principle 4) ─────
#
# A freshly-scaffolded pack is INVISIBLE to the engine: discovery is the
# `cairn.packs` entry-point group, and an entry-point is an install-time fact.
# The manual fix was three hand-edits of the root pyproject.toml plus a
# workspace reinstall — undocumented, easy to get wrong, and nothing said so.
# `pack add` does the three edits; `--dev` prints the zero-install alternative.
#
# It deliberately does NOT run the sync itself: a bare `uv sync` REMOVES every
# extra not named on that exact command line (CONTRIBUTING.md's quickstart
# warning), so a convenience sync here would silently strip presidio, temporal
# and auth out of the caller's venv. It prints the restore-safe command instead.

#: Copied verbatim from CONTRIBUTING.md's quickstart — the ONLY sync that keeps
#: a dev environment whole. Keep the two in step.
RESTORE_SAFE_SYNC = (
    "uv sync --extra dev --extra rag --extra rag-sqlite-vec \\\n"
    "        --extra runtime-sqlite --extra runtime-temporal \\\n"
    "        --extra runtime-dbos --extra telemetry --extra auth \\\n"
    "        --extra ssh"
)


def _workspace_root() -> Path | None:
    """The repo root whose pyproject.toml owns ``[tool.uv.workspace]``.

    Tried in order: cwd and its parents (the workspace the user is standing in —
    their intent), then the checkout this module was imported from (a `cairn`
    installed editable, invoked from somewhere else).
    """
    cwd = Path.cwd().resolve()
    candidates = [cwd, *cwd.parents]
    candidates.append(Path(__file__).resolve().parents[3])  # …/cairn/cli/commands/pack.py
    for c in candidates:
        pp = c / "pyproject.toml"
        if pp.is_file() and "[tool.uv.workspace]" in pp.read_text(encoding="utf-8"):
            return c
    return None


def _distribution_name(pack_dir: Path) -> str | None:
    """The pack's own `[project] name` — the string that goes in the root
    dependency list and `[tool.uv.sources]`."""
    pp = pack_dir / "pyproject.toml"
    if not pp.is_file():
        return None
    try:
        data = tomllib.loads(pp.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError:
        return None
    name = (data.get("project") or {}).get("name")
    return str(name) if name else None


def _section_bounds(lines: list[str], header: str) -> tuple[int, int] | None:
    """[start, end) line range of a TOML section, header line excluded."""
    for i, line in enumerate(lines):
        if line.strip() == header:
            for j in range(i + 1, len(lines)):
                s = lines[j].lstrip()
                if s.startswith("[") and not s.startswith("[["):
                    return (i + 1, j)
            return (i + 1, len(lines))
    return None


def _array_bounds(lines: list[str], start: int, end: int, key: str) -> tuple[int, int] | None:
    """Line range of a multi-line array `key = [` … `]` inside [start, end).

    The closing bracket is looked for inside the section too: an inline array
    (`key = ["a", "b"]`) is not multi-line, and scanning past the section's end
    would find the NEXT array's `]` and write the entry into it."""
    for i in range(start, min(end, len(lines))):
        if lines[i].lstrip().startswith(f"{key} = ["):
            for j in range(i + 1, min(end, len(lines))):
                if lines[j].strip() == "]":
                    return (i + 1, j)          # entries only, brackets excluded
            return None
    return None


def _has_entry(lines: list[str], start: int, end: int, value: str) -> bool:
    """Is `value` already one of the array's string entries? Comments ignored."""
    for i in range(start, end):
        s = lines[i].strip().rstrip(",")
        if s in (f'"{value}"', f"'{value}'"):
            return True
    return False


def wire_pack_into_pyproject(text: str, *, member: str, dist: str) -> tuple[str, list[str]]:
    """Insert one pack into the root pyproject's three wiring points.

    Line-based on purpose: the root pyproject.toml is ~200 lines of load-bearing
    comments explaining every dependency, and a tomlkit/tomllib round-trip
    reflows them. Returns ``(new_text, changes)``; ``changes == []`` means the
    pack was already wired (adding twice is a no-op).
    """
    lines = text.splitlines()
    changes: list[str] = []

    # 1. [tool.uv.workspace] members — the path.
    ws = _section_bounds(lines, "[tool.uv.workspace]")
    if ws is None:
        raise ValueError("root pyproject.toml has no [tool.uv.workspace] section")
    arr = _array_bounds(lines, ws[0], ws[1], "members")
    if arr is None:
        raise ValueError("[tool.uv.workspace] has no multi-line `members = [` array")
    if not _has_entry(lines, arr[0], arr[1], member):
        lines.insert(arr[1], f'    "{member}",')
        changes.append(f"[tool.uv.workspace] members += {member!r}")

    # 2. [project] dependencies — the distribution name (recompute: 1 shifted lines).
    lines_text = "\n".join(lines)
    lines = lines_text.splitlines()
    proj = _section_bounds(lines, "[project]")
    if proj is None:
        raise ValueError("root pyproject.toml has no [project] section")
    arr = _array_bounds(lines, proj[0], proj[1], "dependencies")
    if arr is None:
        raise ValueError("[project] has no multi-line `dependencies = [` array")
    if not _has_entry(lines, arr[0], arr[1], dist):
        lines.insert(arr[1], f'    "{dist}",')
        changes.append(f"[project] dependencies += {dist!r}")

    # 3. [tool.uv.sources] — resolve that dependency from the workspace.
    src = _section_bounds(lines, "[tool.uv.sources]")
    if src is None:
        raise ValueError("root pyproject.toml has no [tool.uv.sources] section")
    start, end = src
    already = False
    pad = 0
    last = start - 1
    for i in range(start, end):
        s = lines[i].strip()
        if not s or s.startswith("#"):
            continue
        name = s.split("=", 1)[0].strip()
        pad = max(pad, len(name))
        last = i
        if name == dist:
            already = True
    if not already:
        width = max(pad, len(dist))
        lines.insert(last + 1, f"{dist:<{width}} = {{ workspace = true }}")
        changes.append(f"[tool.uv.sources] {dist} = {{ workspace = true }}")

    out = "\n".join(lines)
    if text.endswith("\n"):
        out += "\n"
    return out, changes


# #1545
@app.command("add")
def add_cmd(
    pack_dir: str = typer.Argument(
        ".", help="The pack directory (holds pack.toml + pyproject.toml)."),
    dev: bool = typer.Option(
        False, "--dev",
        help="Don't touch pyproject.toml — print the CAIRN_PACK_PATH export "
             "that makes this pack loadable with no install at all."),
) -> None:
    """Make a scaffolded pack discoverable by the engine.

    Two modes, and `--dev` is the one to reach for first:

      * ``--dev`` — print the ``CAIRN_PACK_PATH`` export. Zero install, no file
        edits; the loader scans the dir on every start. This is the edit-and-rerun
        loop.
      * default — wire the pack permanently into the root ``pyproject.toml``
        (workspace member + dependency + `[tool.uv.sources]`), then print the
        one sync command that installs it *without* stripping your extras.
    """
    target = Path(pack_dir).expanduser().resolve()
    if not (target / "pack.toml").is_file():
        console.print(f"[red]✗ {target} has no pack.toml[/] — not a pack. "
                      "Scaffold one with `cairn init pack <name>`.")
        raise typer.Exit(2)

    if dev:
        # deliberate CLI-startup deferral — importing cairn.core.pack pulls the
        # whole pack contract; every other command in this file defers likewise.
        from cairn.core.pack import DEV_PACK_PATH_ENV

        console.print(Panel(
            f"export {DEV_PACK_PATH_ENV}=\"{target}\"\n"
            f"[dim]# append to an existing list:[/]\n"
            f"export {DEV_PACK_PATH_ENV}=\"${DEV_PACK_PATH_ENV}:{target}\"",
            title="zero-install dev loop — export, then `cairn ls`",
            border_style="green"))
        console.print(
            "[dim]The engine scans each listed dir on every start; edits take "
            "effect on the next run, no reinstall. An INSTALLED pack of the same "
            "name always wins — this never shadows a deployed pack. Make it "
            "permanent later with[/] [bold]cairn pack add[/] [dim](no --dev).[/]")
        return

    if not (target / "pyproject.toml").is_file():
        console.print(f"[red]✗ {target} has no pyproject.toml[/] — a permanent "
                      "install needs one (`cairn init pack` writes it). Use "
                      "[bold]--dev[/] for the zero-install loop instead.")
        raise typer.Exit(2)

    dist = _distribution_name(target)
    if not dist:
        console.print(f"[red]✗ {escape(str(target))}/pyproject.toml declares no "
                      f"{escape('[project]')} name[/]")
        raise typer.Exit(2)

    root = _workspace_root()
    if root is None:
        console.print("[red]✗ no uv workspace root found[/] — run this from inside "
                      "a cairn checkout (the pyproject.toml with "
                      f"{escape('[tool.uv.workspace]')}), or use [bold]--dev[/].")
        raise typer.Exit(2)

    try:
        member = target.relative_to(root).as_posix()
    except ValueError:
        # uv requires workspace members to live inside the workspace root, so
        # there is no honest pyproject edit to make here — say so instead of
        # writing a line uv will reject.
        console.print(
            f"[red]✗ {target} is outside the workspace root[/] {root} — uv "
            "workspace members must live inside it. Either move the pack into "
            "the checkout, or use [bold]cairn pack add --dev[/] "
            f"({target} stays where it is).")
        raise typer.Exit(2)

    pp = root / "pyproject.toml"
    text = pp.read_text(encoding="utf-8")
    try:
        new_text, changes = wire_pack_into_pyproject(text, member=member, dist=dist)
    except ValueError as e:
        console.print(f"[red]✗ cannot wire {escape(str(pp))}[/]: {escape(str(e))}")
        raise typer.Exit(1)

    if not changes:
        console.print(f"[green]✓[/] [bold]{dist}[/] is already wired into "
                      f"{pp} — nothing to do.")
    else:
        pp.write_text(new_text, encoding="utf-8")
        console.print(f"[green]✓[/] wired [bold]{dist}[/] ({member}) into {pp}:")
        for c in changes:
            console.print(f"    • {c}", markup=False)

    console.print(Panel(
        RESTORE_SAFE_SYNC,
        title="now install it — this exact command (see CONTRIBUTING.md)",
        border_style="cyan"))
    console.print(
        "[yellow]⚠[/] run it as written. A bare [bold]uv sync[/] REMOVES every "
        "extra not named on the command line — presidio, temporal and auth vanish "
        "silently. That is why this command does not sync for you.")
    console.print(f"[dim]Then confirm:[/] [bold]cairn ls {dist}[/]")


# #1254, ADR-0087 (the drift guard: D6)
def build_cmd(
    target: str = typer.Argument(
        ..., help="Pipeline class: `module:Class` or `path/to/file.py:Class`.",
    ),
    out: str = typer.Option(
        "", "--out", "-o",
        help="Pack directory to write (default: ./<pack-name>).",
    ),
) -> None:
    """Compile a ``@pipeline`` class to its pack records.

    The class is a VIEW — this writes ``pipelines/*.yaml`` + ``flows/*.yaml``,
    and for lifted ``@step`` defs the private-op module plus wheel wiring
    (``pack.toml`` / ``pyproject.toml`` only when absent, never clobbering).
    Prints the content hash to pin launches with (the drift guard).
    """
    # optional dependency — cairn-sdk is a sibling workspace member; a bare
    # engine install may not carry it
    try:
        from cairn_sdk import CompileError, CompileScopeError, compile_pipeline
    except ImportError:
        console.print("[red]✗ cairn-sdk is not installed[/] — "
                      "`uv pip install cairn-sdk` (or sync the workspace).")
        raise typer.Exit(2)

    mod_ref, _, cls_name = target.rpartition(":")
    if not mod_ref or not cls_name:
        console.print(f"[red]✗ bad target {target!r}[/] — expected "
                      "`module:Class` or `path/to/file.py:Class`.")
        raise typer.Exit(2)

    try:
        if mod_ref.endswith(".py"):
            p = Path(mod_ref).resolve()
            spec = importlib.util.spec_from_file_location(p.stem, p)
            if spec is None or spec.loader is None:
                raise ImportError(f"cannot load {p}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[p.stem] = module   # inspect.getsource needs it findable
            spec.loader.exec_module(module)
        else:
            if os.getcwd() not in sys.path:
                sys.path.insert(0, os.getcwd())
            module = importlib.import_module(mod_ref)
    except Exception as e:  # noqa: BLE001 — one door, one clear message
        console.print(f"[red]✗ cannot import {mod_ref!r}[/]: {e}")
        raise typer.Exit(2)

    cls = getattr(module, cls_name, None)
    if cls is None:
        console.print(f"[red]✗ {mod_ref} has no class {cls_name!r}[/]")
        raise typer.Exit(2)

    try:
        compiled = compile_pipeline(cls)
    except (CompileError, CompileScopeError) as e:
        console.print(f"[red]✗ compile failed[/]: {e}")
        raise typer.Exit(1)

    dest = Path(out) if out else Path.cwd() / compiled.pack_name
    compiled.write(dest)

    console.print(f"[green]✓[/] built [bold]{compiled.pack_name}[/] → {dest}")
    console.print(f"  pipelines/{compiled.name}.yaml   "
                  f"(content_hash [bold]{compiled.content_hash}[/])")
    for fname in compiled.flows:
        console.print(f"  flows/{fname}.yaml")
    for op in compiled.operators:
        console.print(f"  op {op['name']}  risk={op['risk']} "
                      f"requires={op['requires']}", markup=False)
    for note in compiled.notes:
        console.print(f"  [yellow]note[/] {escape(str(note))}")

    _warn_shadowing_authoring_file(compiled, dest)


# #1539; the doctor drift check: #1105
@app.command("sync")
def sync_cmd(
    pack_dir: str = typer.Argument(
        ".", help="Operator pack directory (holds pack.toml + pyproject.toml + src/)."),
    check: bool = typer.Option(
        False, "--check", help="Report drift without writing (exit 1 on drift)."),
) -> None:
    """Regenerate operator metadata from the classes.

    The Operation class is the single source of truth for the whole
    ``[operator."<n>"]`` table — identity, schemas, lifecycle flags, risk,
    executors, mcp — and for the ``cairn.operators`` entry-point line.
    ``sync`` walks the pack's ``src/<package>/``, derives each table, and
    rewrites only the stale/missing blocks (comments between tables
    survive). ``--check`` is the CI form of the same comparison; the doctor
    drift check (manifest drift + governance fields) covers the installed tree.
    """
    from cairn.core.operator.manifest_sync import check_pack, sync_pack  # noqa: PLC0415 — heavy walk, load on use

    target = Path(pack_dir).resolve()
    if not (target / "pack.toml").exists():
        console.print(f"[red]✗ {target} has no pack.toml[/] — not an operator pack.")
        raise typer.Exit(2)

    report = check_pack(target) if check else sync_pack(target)
    for line in report.import_problems:
        console.print(f"  [yellow]import[/] {escape(str(line))}")
    for line in report.synced:
        console.print(f"  [green]synced[/] {escape(str(line))}")
    if report.clean:
        console.print(f"[green]✓[/] {target.name}: class ↔ pack.toml ↔ entry points in sync")
        return
    for name in report.missing_tables:
        table = '[operator."' + str(name) + '"]'
        console.print(f"  [red]missing table[/] {escape(table)}")
    for name, diffs in report.drifted.items():
        console.print(f"  [red]drift[/] {escape(str(name))}: " + escape("; ".join(diffs)))
    for ep in report.missing_entry_points:
        console.print(f"  [red]missing entry point[/] {escape(str(ep))}")
    for name in report.orphan_tables:
        console.print(f"  [yellow]orphan table[/] {escape(str(name))} (no class in src/)")
    raise typer.Exit(1)


def _warn_shadowing_authoring_file(compiled, dest: Path) -> None:  # noqa: ANN001
    """#1279 (F2) — an authoring file named ``<pkg>.py`` sitting next to the
    build SHADOWS the just-installed package of the same name for any
    process whose cwd contains it: `import <pkg>` resolves the loose module
    first, and the pipeline's own ops fail with a confusing
    ``ModuleNotFoundError: '<pkg>' is not a package``. Hit live during the
    SDK E2E (#1278) and cost a full debugging round — worth a build-time
    nudge instead of a runtime mystery."""
    pkg_file = f"{compiled._pkg}.py"
    # resolve() before the set-dedup: `--out ./pkg` leaves dest.parent a
    # relative "." that would otherwise not dedupe against Path.cwd().
    for d in {Path.cwd().resolve(), dest.parent.resolve()}:
        candidate = d / pkg_file
        if candidate.exists():
            console.print(
                f"[yellow]⚠ warning[/] {candidate} will SHADOW the "
                f"installed package [bold]{compiled._pkg}[/] for any "
                f"process run from {d} — `import {compiled._pkg}` resolves "
                "this loose file first and the pipeline's ops fail with "
                f"ModuleNotFoundError: '{compiled._pkg}' is not a package. "
                "Rename the authoring file (it does not need to match the "
                "pack name) or run from a different directory.",
                markup=True)


# `pack build` compiles a cairn-sdk `@pipeline` class. The authoring SDK is the platform's
# (ADR-0135): the command registers only when its toolchain is importable; otherwise a HIDDEN
# stub says so and exits 1 — a public install does not list a verb it cannot run (#2536), the
# same rule as `cairn ask` (cairn/cli/main.py).
if importlib.util.find_spec("cairn_sdk") is not None:
    app.command("build")(build_cmd)
else:

    @app.command(
        name="build",
        help="Compile a Python pipeline class — part of cairn-platform, not this install.",
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
        add_help_option=False,
        hidden=True,
    )
    def _build_not_installed() -> None:
        console.print(
            "[red]✗[/] `cairn pack build` (the authoring SDK) is part of cairn-platform; this "
            "install does not include it. `cairn pack --help` lists the commands it has."
        )
        raise typer.Exit(1)
