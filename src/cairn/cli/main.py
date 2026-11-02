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

from cairn.cli.aliases import renamed, renamed_group
from cairn.cli.commands import admin as _admin
from cairn.cli.commands import audit as _audit
from cairn.cli.commands import compute as _compute
from cairn.cli.commands import config as _config
from cairn.cli.commands import dev as _dev
from cairn.cli.commands import flow as _flow
from cairn.cli.commands import pipeline as _pipeline
from cairn.cli.commands import connect as _connect
from cairn.cli.commands import demo as _demo
from cairn.cli.commands import deploy as _deploy
from cairn.cli.commands import doctor as _doctor
from cairn.cli.commands import export as _export
from cairn.cli.commands import init as _init
from cairn.cli.commands import lock as _lock
from cairn.cli.commands import lineage as _lineage
from cairn.cli.commands import ls as _ls
from cairn.cli.commands import pack as _pack
from cairn.cli.commands import remote as _remote
from cairn.cli.commands import replay as _replay
from cairn.cli.commands import status as _status
from cairn.cli.commands import gate as _gate
from cairn.cli.commands import run as _run
from cairn.cli.commands import test as _test
from cairn.cli.commands import validate as _validate
from cairn.cli.commands import visualize as _visualize
from cairn.cli.commands import worker as _worker
from cairn.cli.providers import MAIN_COMMANDS, OTHER_COMMANDS, mount_cli_providers

app = typer.Typer(
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
app.command(name="doctor")(_doctor.cmd)
app.command(name="run", short_help="Run a pipeline and follow it", rich_help_panel=MAIN_COMMANDS)(_run.cmd)
# ADR-0165 D4, D5 — one door to look, one door to decide.
app.command(name="status", short_help="See what's running, or one run's results",
            rich_help_panel=MAIN_COMMANDS)(_status.status_cmd)
app.command(name="decide", short_help="Approve, reject or cancel", rich_help_panel=MAIN_COMMANDS)(_status.decide_cmd)
app.add_typer(_pack.app, name="pack", rich_help_panel=MAIN_COMMANDS)
app.add_typer(_admin.app, name="admin", rich_help_panel=OTHER_COMMANDS)
# `dev` — one-command sqlite dev environment (#1188, ADR-0085 D5): engine + vite
# on a pinned config the repo .env can't poison; login dev/dev.
app.add_typer(_dev.app, name="dev")

# ADR-0165 D12 — the old names, hidden, for one minor version. Each says its new spelling on
# stderr and runs the same code.
for _old, _new, _cmd in (
    ("preflight", "cairn run --dry-run", _run.preflight_cmd),
    ("gate", "cairn run --check", _gate.cmd),
    ("replay", "cairn status <run> --replay", _replay.cmd),
    ("ls", "cairn pack ls", _ls.cmd),
    ("validate", "cairn pack check", _validate.cmd),
    ("export", "cairn pack export --to pipeline", _export.cmd),
    ("test", "cairn pack test", _test.cmd),
    ("lock", "cairn pack lock", _lock.cmd),
    ("visualize", "cairn pack show --graph", _visualize.cmd),
    ("lineage", "cairn admin lineage", _lineage.cmd),
):
    app.command(name=_old, hidden=True)(renamed(f"cairn {_old}", _new)(_cmd))
app.add_typer(renamed_group(_init.app, "cairn init", "cairn pack new"), name="init", hidden=True)
for _old, _group in (("audit", _audit.app), ("compute", _compute.app), ("config", _config.app),
                     ("connect", _connect.app), ("demo", _demo.app), ("deploy", _deploy.app),
                     ("worker", _worker.app)):
    app.add_typer(renamed_group(_group, f"cairn {_old}", f"cairn admin {_old}"), name=_old, hidden=True)
# These three say so themselves, command by command: their rows go several ways.
app.add_typer(_flow.app, name="flow", hidden=True)
app.add_typer(_pipeline.app, name="pipeline", hidden=True)
app.add_typer(_remote.app, name="remote", hidden=True)

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
