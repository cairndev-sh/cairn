"""Top-level Typer app for ``cairn``.

Single dispatcher. Each command is one file in :mod:`cairn.cli.commands`
and exposes a ``cmd`` function; this module wires them onto the Typer app
under their command name. Adding a new command = one file + one line here.

Wired in ``pyproject.toml`` as the ``cairn`` script entry point, so
``pip install -e .`` puts ``cairn`` on PATH.
"""

from __future__ import annotations

import importlib.metadata
from typing import Optional

import typer

# Load .env into os.environ BEFORE importing the command modules: litellm/openai
# read os.environ directly, so a real key must be present first. No module
# stamps a dummy key into os.environ (a keyless model gets a compile-only
# placeholder from build_chat_model, which never reaches a provider). Shared
# with the worker/server entrypoints via load_env().
from cairn.core.provider_settings import load_env

load_env()

from cairn.cli.commands import admin as _admin
from cairn.cli.commands import setup as _setup
from cairn.cli.commands import pack as _pack
from cairn.cli.commands import status as _status
from cairn.cli.commands import run as _run
from cairn.cli.moved import MovedGroup
from cairn.cli.providers import MAIN_COMMANDS, OTHER_COMMANDS, mount_cli_providers

app = typer.Typer(
    cls=MovedGroup,
    name="cairn",
    help="cairn — engine + community packs for declarative agent workflows.",
    no_args_is_help=True,
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    """``cairn --version``: the installed ``cairndev`` distribution (ADR-0121 D3)."""
    if not value:
        return
    try:
        version = importlib.metadata.version("cairndev")
    except importlib.metadata.PackageNotFoundError:  # a source tree on PYTHONPATH, not installed
        version = "0+unknown"
    typer.echo(f"cairn {version}")
    raise typer.Exit()


@app.callback()
def _root(
    version: Optional[bool] = typer.Option(
        None, "--version", callback=_version_callback, is_eager=True,
        help="Print the installed cairndev version and exit.",
    ),
) -> None:
    """cairn — engine + community packs for declarative agent workflows."""


# ADR-0165 D1 — `--help` leads with these. Typer lists a heading where it first meets it, and
# a group after every plain command, so `admin` is moved behind the providers' below.
app.command(name="setup", short_help="Start or connect to an engine", rich_help_panel=MAIN_COMMANDS)(_setup.cmd)
app.command(name="run", short_help="Run a pipeline and follow it", rich_help_panel=MAIN_COMMANDS)(_run.cmd)
# ADR-0165 D4, D5 — one door to look, one door to decide.
app.command(name="status", short_help="See what's running, or one run's results",
            rich_help_panel=MAIN_COMMANDS)(_status.status_cmd)
app.command(name="decide", short_help="Approve, reject or cancel", rich_help_panel=MAIN_COMMANDS)(_status.decide_cmd)
app.add_typer(_pack.app, name="pack", rich_help_panel=MAIN_COMMANDS)
app.add_typer(_admin.app, name="admin", rich_help_panel=OTHER_COMMANDS)
# The product-only commands (`ask`, `pilot`, `series`, `showcase`, `eval`, `suggest`,
# `knowledge`) left for the platform (ADR-0120 D3, #2421 decision 2, ADR-0134 D6): every
# installed `cairn.cli` provider mounts here, after the public commands, which win every name.
#: The command names the installed providers mounted (`cairn/cli/providers.py`).
PROVIDER_COMMANDS: list[str] = mount_cli_providers(app)
app.registered_groups.sort(key=lambda group: group.name == "admin")  # "All other commands" comes last

# `ask` (the governed NL→plan→run agent, #95) is the platform's: its `cairn.cli` provider
# supplies it (ADR-0134 D6). The ONE exception to "the public CLI wins every name": this
# hidden stub YIELDS — it mounts only when no provider supplied `ask`, so it registers
# after the providers rather than before them. It says Ask is part of cairn-platform and
# exits 1: a public install does not list a verb it cannot run (#2536), and the public
# cairndev has no `ask` extra to install, so the stub names none (#2579).
if "ask" not in PROVIDER_COMMANDS:

    @app.command(
        name="ask",
        help="AI-assisted planning — part of cairn-platform, not this install.",
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
        add_help_option=False,
        hidden=True,
    )
    def _ask_not_installed() -> None:
        typer.echo(
            "`cairn ask` (AI-assisted planning) is part of cairn-platform; this install "
            "does not include it. `cairn --help` lists the commands it has.",
            err=True,
        )
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
