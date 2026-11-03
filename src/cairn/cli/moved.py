"""The names ADR-0165 moved commands away from, and what to type instead.

They worked, with a warning, for one minor version (D12). Now typing one runs nothing and says the
new name. ``tests/cli/test_cli_surface.py`` holds this table to the ADR's D10.
"""
from __future__ import annotations

from typing import Any, Optional

import click
import typer
from typer.core import TyperGroup

#: Exit code: the command line is wrong, as for any unknown command.
EXIT_MOVED = 2

#: {the old command, after `cairn `: what to type now}.
MOVED: dict[str, str] = {
    "audit": "`cairn admin audit`",
    "audit verify": "`cairn admin audit verify`",
    "compute": "`cairn admin compute`",
    "compute status": "`cairn admin compute status`",
    "compute sweep": "`cairn admin compute sweep`",
    "config": "`cairn admin config`",
    "config workflow": "`cairn admin config workflow`",
    "connect": "`cairn admin connect`",
    "connect add": "`cairn admin connect add`",
    "connect authorize": "`cairn admin connect authorize`",
    "connect ls": "`cairn admin connect ls`",
    "connect presets": "`cairn admin connect presets`",
    "connect providers": "`cairn admin connect providers`",
    "connect rm": "`cairn admin connect rm`",
    "connect test": "`cairn admin connect test`",
    "demo": "`cairn admin demo`",
    "deploy": "`cairn admin deploy`",
    "deploy doctor": "`cairn admin deploy doctor`",
    "dev": "`cairn setup --foreground`",
    "doctor": "`cairn setup --check`",
    "export": "`cairn pack export --to pipeline`",
    "flow": "`cairn pack`",
    "flow export": "`cairn pack export --to metaflow`",
    "flow ls": "`cairn pack ls --flows`",
    "flow split": "`cairn pack show --split`",
    "gate": "`cairn run --check`",
    "init": "`cairn pack new`",
    "init operator": "`cairn pack new operator`",
    "init pack": "`cairn pack new pack`",
    "init policy": "`cairn pack new policy`",
    "init skill": "`cairn pack new skill`",
    "init template": "`cairn pack new template`",
    "lineage": "`cairn admin lineage`",
    "lock": "`cairn pack lock`",
    "ls": "`cairn pack ls`",
    "pipeline": "`cairn run` and `cairn pack`",
    "pipeline backfill": "`cairn admin backfill`",
    "pipeline explain": "`cairn run --dry-run`",
    "pipeline ls": "`cairn pack ls --pipelines`",
    "pipeline respell-packs": "`cairn admin respell-packs`",
    "pipeline run": "`cairn run`",
    "pipeline show": "`cairn pack show`",
    "preflight": "`cairn run --dry-run`",
    "remote": "`cairn status`, `cairn decide`, `cairn run`, `cairn setup --server` and `cairn admin`",
    "remote advance": "`cairn decide <pl_id> approve --step`",
    "remote approve": "`cairn decide <run> approve`",
    "remote cancel": "`cairn decide <run> cancel`",
    "remote cancel-pipeline": "`cairn decide <pl_id> cancel`",
    "remote console": "`cairn admin console`",
    "remote delete-preset": "`cairn admin preset rm`",
    "remote disable-trigger": "`cairn admin trigger disable`",
    "remote enable-trigger": "`cairn admin trigger enable`",
    "remote findings": "`cairn status --results`",
    "remote hash-password": "`cairn admin hash-password`",
    "remote launch": "`cairn run`",
    "remote login": "`cairn setup --server`",
    "remote logs": "`cairn status --logs`",
    "remote pipeline": "`cairn status <pl_id> --json`",
    "remote pipelines": "`cairn status`",
    "remote presets": "`cairn admin preset ls`",
    "remote ps": "`cairn status`",
    "remote reject": "`cairn decide reject`",
    "remote status": "`cairn status <run>`",
    "remote submit": "`cairn run --skill`",
    "remote triggers": "`cairn admin trigger ls`",
    "remote watch": "`cairn status --watch`",
    "remote worker-token": "`cairn admin worker-token`",
    "replay": "`cairn status --replay`",
    "test": "`cairn pack test`",
    "validate": "`cairn pack check`",
    "visualize": "`cairn pack show --graph`",
    "worker": "`cairn admin worker`",
}


def moved_to(args: list[str]) -> Optional[str]:
    """The line to print when ``args`` start with an old name, or None. The longest match wins, so
    ``remote launch`` names its own replacement and ``remote nonsense`` names the group's."""
    for length in range(len(args), 0, -1):
        old = " ".join(args[:length])
        if old in MOVED:
            return f"`cairn {old}` is now {MOVED[old]}."
    return None


class MovedGroup(TyperGroup):
    """The root command: a name it doesn't have may be one that moved. A command an installed
    provider mounted is found first, so a moved name never shadows a real one."""

    def resolve_command(self, ctx: click.Context, args: list[str]) -> Any:
        if args and args[0] not in self.commands:
            line = moved_to(args)
            if line:
                typer.secho(line, fg="yellow", err=True)
                ctx.exit(EXIT_MOVED)
        return super().resolve_command(ctx, args)
