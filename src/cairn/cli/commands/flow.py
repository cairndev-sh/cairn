"""``cairn flow`` — inspect flows, split them at in-flow approval gates (#1079),
and export them as native orchestrator files (ADR-0087 P4, #1256).

The split is what makes in-flow HITL engine-independent: rather than asking each
engine for a suspend primitive (builtin has none; Metaflow's is Argo-only), a
gate-carrying flow becomes ``segment 1 → cairn gate → segment 2``, with the
working dataset materialized at the boundary and the EXISTING approval queue in
between.

``export`` is the other direction — a record projected OUT to a Metaflow
``FlowSpec``, one step per stage. One-way and stamped: the
generated file names the ref + def hash it was cut from, and says plainly that
governance stayed home. A gated flow refuses to export for exactly the reason
the split exists.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from cairn.core.dataflow.defs import load_flow_defs
from cairn.core.dataflow.export import EXPORT_TARGETS, FlowExportError, export_flow
from cairn.core.dataflow.gate_split import (
    GateSplitError,
    has_in_flow_gates,
    split_at_gates,
)

# #1079
app = typer.Typer(
    help="Inspect flows; split them at in-flow approval gates.",
    no_args_is_help=True,
)
console = Console()


# #1133, ADR-0084 D2
@app.command("ls")
def ls_cmd() -> None:
    """List every flow — all three kinds.

    `flows/` holds data/ml as `.yaml` (stage body) and agentic as `.md`
    (frontmatter + prose). `templates/` is still searched as the transition
    alias, so a pack can migrate one file at a time.
    """
    defs = load_flow_defs()
    for ref in sorted(defs):
        d = defs[ref]
        gated = " [magenta](in-flow gates)[/]" if has_in_flow_gates(d) else ""
        console.print(f"  {ref}  [dim]kind={d.kind} stages={len(d.stages)}[/]{gated}")

    for pack, name, where in agentic_flows():
        console.print(f"  {pack}/{name}  [dim]kind=agentic  ({where})[/]")

    if not defs and not agentic_flows():
        console.print("[yellow]no flows installed[/]")
        raise typer.Exit(0)


def agentic_flows() -> list[tuple[str, str, str]]:
    """Agentic flows (`.md`) from every pack's flows/ and templates/ dirs.

    Public because `cairn ls` renders the same surface (#2115): a pack's flows
    are its COMPONENTS, and a second scan would drift from this one."""
    from pathlib import Path

    from cairn.core.pack import all_flow_dirs, all_template_dirs, ensure_loaded, get_packs

    ensure_loaded()
    # #2513 — the owning pack's registered (manifest) name, never its directory: an
    # installed wheel keeps the pack under its import name (`cairn_pack_training`).
    owner = {d: p.name for p in get_packs() for d in (p.flows_dir, p.templates_dir) if d}
    out: list[tuple[str, str, str]] = []
    for where, dirs in (("flows/", all_flow_dirs()), ("templates/", all_template_dirs())):
        for d in dirs:
            for md in sorted(Path(d).glob("*.md")):
                out.append((owner[d], md.stem, where))
    return out


@app.command("split")
def split_cmd(
    ref: str = typer.Argument(..., help="Flow ref, e.g. dataset-readiness/intake"),
    json_out: bool = typer.Option(False, "--json", help="Emit segments as JSON."),
) -> None:
    """Show how a flow splits at its in-flow approval gates.

    Each segment is a separately-submitted engine job; the gate named after a
    segment must clear before the next one starts. Ungated flows report a single
    segment — proof the no-gate path is untouched.
    """
    defs = load_flow_defs()
    if ref not in defs:
        console.print(
            f"[red]unknown flow {ref!r}[/] — installed: {sorted(defs) or '(none)'}")
        raise typer.Exit(2)

    try:
        segments = split_at_gates(defs[ref])
    except GateSplitError as e:
        console.print(f"[red]cannot split {ref!r}[/]\n{e}")
        raise typer.Exit(1) from None

    if json_out:
        typer.echo(json.dumps([
            {
                "name": s.name,
                "stages": [{"stage": st.stage, "config": st.config} for st in s.stages],
                "gate_after": s.gate_after,
                "handoff_key": s.handoff_key,
                "resumes_from": s.resumes_from,
            } for s in segments
        ], indent=2))
        raise typer.Exit(0)

    if len(segments) == 1:
        console.print(f"[green]{ref}[/] has no in-flow gates — runs as one job "
                      f"({len(segments[0].stages)} stages)")
        raise typer.Exit(0)

    console.print(f"[bold]{ref}[/] splits into {len(segments)} segments:\n")
    for s in segments:
        console.print(f"  [cyan]{s.name}[/]"
                      + (f"  [dim]resumes from ${s.resumes_from}[/]" if s.resumes_from else ""))
        for st in s.stages:
            console.print(f"      · {st.stage}")
        if s.gate_after:
            console.print(f"  [magenta]⏸  APPROVAL GATE: {s.gate_after}[/]"
                          f"  [dim](hands off via ${s.handoff_key})[/]\n")
    console.print("[dim]Bind each segment to its own flow-door node and declare the "
                  "approval gate between them; the pause uses the ordinary run "
                  "approval queue.[/]")


def _parse_config(pairs: Optional[list[str]]) -> dict:
    """``k=v`` pairs → the config dict the resolver substitutes ``$config.*``
    from. Values stay strings — the resolver does no coercion either, so what
    the export bakes in is exactly what a run would."""
    out: dict = {}
    for item in pairs or []:
        if "=" not in item:
            console.print(f"[red]--config must be key=value, got {item!r}[/]")
            raise typer.Exit(2)
        k, v = item.split("=", 1)
        out[k.strip()] = v
    return out


@app.command("export")
def export_cmd(
    ref: str = typer.Argument(..., help="Flow ref, e.g. training/readiness"),
    to: str = typer.Option(..., "--to", help=f"Target: {' | '.join(EXPORT_TARGETS)}"),
    out: Path = typer.Option(Path("."), "--out", help="Directory for the generated file."),
    config: Optional[list[str]] = typer.Option(
        None, "--config", "-c",
        help="key=value feeding $config.* (repeatable). Unset keys drop out, "
             "exactly as on the run path."),
    stdout: bool = typer.Option(False, "--stdout", help="Print instead of writing."),
) -> None:
    """Export a flow as a native Metaflow file — one step per stage.

    A ONE-WAY, stamped derivative: the header carries the source ref and the def
    file's sha256, and states that gates/budgets/admission/audit do NOT travel.
    The generated file imports only the kernel wheel and the target framework,
    so it runs on a box with no cairn control plane on it.
    """
    try:
        exported = export_flow(ref, to, _parse_config(config))
    except FlowExportError as e:
        console.print(f"[red]cannot export {ref!r}[/]\n{e}")
        raise typer.Exit(1) from None

    if stdout:
        typer.echo(exported.source)
        raise typer.Exit(0)

    out.mkdir(parents=True, exist_ok=True)
    dest = out / exported.filename
    dest.write_text(exported.source, encoding="utf-8")
    console.print(
        f"[green]exported[/] {ref} → [bold]{dest}[/]\n"
        f"  [dim]target={exported.target} kind={exported.kind} "
        f"stages={len(exported.stages)}[/]\n"
        f"  [dim]{exported.def_sha}[/]"
    )
    console.print("[dim]Generated — do not edit; re-export instead. Governance "
                  "(gates/budgets/admission/audit) stays in cairn.[/]")
