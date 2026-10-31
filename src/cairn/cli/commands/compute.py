"""``cairn compute`` — the lifecycle of a governed compute box (#1787, ADR-0097).

Two verbs, both thin faces on :mod:`cairn.core.compute_lifecycle` (the same
module ``compute/acquire`` and ``compute/release`` call, so the CLI and the
operators can never disagree about what "stopped" means — #1376):

* ``cairn compute status <connection>`` — what state is the box in, what does it
  bill, and when does its lease expire?
* ``cairn compute sweep`` — **the watchdog**. Stop every cairn-managed instance
  whose ``cairn:release-after`` lease has expired.

Why the sweep exists, stated plainly. ``max_hours`` on ``compute/acquire`` stamps
a deadline TAG on the instance; this verb is what acts on it. Because the
deadline lives on the instance and the credentials are all this verb needs, the
guard does not depend on the pipeline's control plane surviving — any host, any
cron, can enforce every lease:

    */10 * * * *  cairn compute sweep --apply --region eu-central-1

That is a watchdog, not instance-side enforcement, and this docstring is the
place that says so. True cloud-side enforcement — an EventBridge one-shot
schedule targeting ``ec2:StopInstances``, or an ``arn:aws:automate:…:ec2:stop``
alarm action — needs an IAM role created in the owner's account, which is
deliberately not something this PR does on its own.
"""
from __future__ import annotations

import asyncio
import json as _json
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from cairn.core.compute_lifecycle import (
    ComputeLifecycleError,
    describe_state,
    load_box,
    sweep_expired,
)

# ADR-0097
app = typer.Typer(
    help="Governed compute boxes — status + the expired-lease sweep.",
    no_args_is_help=True,
)
_console = Console()


def _run(coro):
    return asyncio.run(coro)


@app.command("status")
def status(
    connection: Optional[str] = typer.Argument(
        None, help="Name of the `compute.ssh` connection (list them: `cairn connect ls`)."),
    json: bool = typer.Option(False, "--json", help="Emit the raw dict."),
) -> None:
    """What state is this box in, and what is it costing while it is up?"""
    if not connection:
        # #2536 — the usage error a missing argument always gave (exit 2), plus where
        # the names come from.
        typer.echo("cairn compute status needs a `compute.ssh` connection name — list "
                   "them with `cairn connect ls`", err=True)
        raise typer.Exit(2)
    try:
        row = _run(_status(connection))
    except ComputeLifecycleError as e:
        _console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(2) from None
    if json:
        typer.echo(_json.dumps(row))
        return
    price = f"${row['hourly_usd']:.2f}/hr" if row["hourly_usd"] else "(hourly_usd not declared)"
    _console.print(f"[bold]{row['connection']}[/bold]  {row['instance_id'] or '(no instance_id)'}"
                   f"  region={row['region'] or '(default)'}")
    _console.print(f"  state: [cyan]{row['state'] or 'unknown'}[/cyan]   {price}"
                   f"   host={row['host'] or '(none)'}")


async def _status(name: str) -> dict:
    from cairn.core.connections import DEFAULT_ACCOUNT  # noqa: PLC0415 — off the CLI import path

    # the CLI is the deployment's own tool: its boxes are the deployment's connections
    box = await load_box(name, account_id=DEFAULT_ACCOUNT)
    state = ""
    if box.instance_id:
        state = await describe_state(box.instance_id, region=box.region)
    return {"connection": box.name, "instance_id": box.instance_id,
            "region": box.region, "host": box.host,
            "hourly_usd": box.hourly_usd, "state": state}


def _aws_setup_hint(e: BaseException) -> Optional[str]:
    """A one-line fix for botocore's "not configured" errors, else None."""
    try:
        from botocore.exceptions import NoCredentialsError, NoRegionError  # noqa: PLC0415
    except ImportError:
        return None
    if isinstance(e, NoRegionError):
        return ("no AWS region: pass --region (e.g. --region eu-central-1) or set "
                "AWS_DEFAULT_REGION.")
    if isinstance(e, NoCredentialsError):
        return ("no AWS credentials: set AWS_PROFILE or AWS_ACCESS_KEY_ID/"
                "AWS_SECRET_ACCESS_KEY (the sweep needs ec2:DescribeInstances).")
    return None


@app.command("sweep")
def sweep(
    region: str = typer.Option("", "--region", help="AWS region (else AWS_DEFAULT_REGION)."),
    apply: bool = typer.Option(
        False, "--apply",
        help="Actually stop the expired instances. Without it the sweep only REPORTS — "
             "the shape a human runs first and a cron runs with --apply."),
    json: bool = typer.Option(False, "--json", help="Emit the rows as JSON."),
) -> None:
    """Stop every cairn-managed box whose `cairn:release-after` lease has expired.

    Scoped by the `cairn:managed` tag AND a readable, past deadline, so it can
    never touch a box cairn did not turn on and never acts on a tag it could not
    parse. Exit code 1 when anything was found expired, so a cron can alert.
    """
    try:
        rows = _run(sweep_expired(region=region, apply=apply))
    except Exception as e:  # noqa: BLE001 — only botocore's own setup errors are answered here
        hint = _aws_setup_hint(e)
        if hint is None:
            raise
        _console.print(f"[red]✗[/red] {hint}")
        raise typer.Exit(2) from None
    if json:
        typer.echo(_json.dumps(rows))
    else:
        if not rows:
            _console.print("no cairn-managed instances are running.")
        else:
            t = Table("instance", "state", "release-after", "expired", "acted", "note")
            for r in rows:
                t.add_row(r["instance_id"], r["state"], r["release_after"] or "—",
                          "YES" if r["expired"] else "no",
                          "stopped" if r["stopped"] else "—", r["note"])
            _console.print(t)
        if not apply and any(r["expired"] for r in rows):
            _console.print("[yellow]report only[/yellow] — re-run with "
                           "[cyan]--apply[/cyan] to stop them.")
    if any(r["expired"] for r in rows):
        raise typer.Exit(1)


__all__ = ["app"]
