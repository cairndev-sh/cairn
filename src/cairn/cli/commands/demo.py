"""``cairn demo`` (§394) — seed an opt-in ``demo`` workspace so the console has
runs / a pilot / an audit trail / a pending approval to show, with one command.

This is the **opt-in** half of issue #394. The pack ``[demo]`` block + the
console's "Try with sample data" button ship in every build (zero-risk prefill);
the full seeded *workspace* only ever exists because a user ran this command. It
NEVER auto-seeds on install or server startup.

  - ``cairn demo``              seed (idempotent — re-run freely, no dupes)
  - ``cairn demo --archive``    tear the demo workspace down (soft-delete; runs
                                / events / audit are preserved, restorable by
                                re-seeding)

Thin CLI: wire args → call :mod:`cairn.core.demo_seed` → render. All business
logic lives in core (CLI-layer rule).

The full console showcase (``--showcase``, #1969) left with the platform (#2421
decision 2): it is the platform's ``cairn showcase`` command, mounted through the
``cairn.cli`` entry-point group when the platform is installed.
"""
from __future__ import annotations

import asyncio

import typer

from cairn.core.demo_seed import (
    DEMO_WORKSPACE,
    archive_demo_workspace,
    seed_demo_workspace,
)

app = typer.Typer(
    name="demo",
    help="Seed an opt-in demo workspace (runs, governed pipelines, a pending approval).",
    no_args_is_help=False,
)


@app.callback(invoke_without_command=True)
def cmd(
    ctx: typer.Context,
    workspace: str = typer.Option(
        DEMO_WORKSPACE, "--workspace", "-w",
        help="Workspace id to seed / archive.",
    ),
    archive: bool = typer.Option(
        False, "--archive",
        help="Tear the demo workspace down (soft-delete; runs/audit preserved).",
    ),
) -> None:
    """Seed (default) or archive the demo workspace."""
    # Let real sub-commands (if any are added later) win over the callback body.
    if ctx.invoked_subcommand is not None:
        return

    from rich.console import Console

    console = Console()

    if archive:
        n = asyncio.run(archive_demo_workspace(workspace_id=workspace))
        console.print(
            f"[yellow]Archived[/] demo workspace [b]{workspace}[/] "
            f"({n} workflow record(s) deactivated). Runs and audit are preserved; "
            f"re-run [b]cairn demo[/] to restore."
        )
        return

    result = asyncio.run(seed_demo_workspace(workspace_id=workspace))
    console.print(f"[green]Seeded[/] demo workspace [b]{result.workspace_id}[/]:")
    console.print(f"  • enabled workflows : {len(result.enabled)}")
    for ref in result.enabled:
        console.print(f"      [dim]-[/] {ref}")
    console.print(f"  • pipelines         : {len(result.pipelines)} ({len(result.completed_runs)} leg runs)")
    console.print(f"  • waiting on a human: {', '.join(result.pending_gates) or 'none'}")
    console.print(f"  • seed providers    : {', '.join(result.providers) or 'none installed (see cairn doctor)'}")
    for why in result.skipped:
        console.print(f"      [yellow]skipped[/] {why}")
    console.print(
        f"\nThe data is in workspace [b]{result.workspace_id}[/], not the default one "
        f"that [b]cairn remote[/] reads: send the header [b]X-Cairn-Workspace: "
        f"{result.workspace_id}[/] to the HTTP API (a console, where one is installed, "
        f"switches workspace in its header). Re-run this command any time — it is "
        f"idempotent. Tear it down with [b]cairn demo --archive[/]."
    )
