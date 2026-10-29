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
from cairn.cli.commands import gate as _gate
from cairn.cli.commands import run as _run
from cairn.cli.commands import test as _test
from cairn.cli.commands import validate as _validate
from cairn.cli.commands import visualize as _visualize
from cairn.cli.commands import worker as _worker
from cairn.cli.providers import mount_cli_providers

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


app.command(name="doctor")(_doctor.cmd)
app.command(name="ls")(_ls.cmd)
app.command(name="validate")(_validate.cmd)
# #1092 — render an installed workflow as one runnable pipeline file.
app.command(name="export")(_export.cmd)
app.command(name="run")(_run.cmd)
# `preflight` is the CLI face of the ADR-0072 dry-run verdict (#800): the same
# side-effect-free ready/blocked composition the console Run tab + `cairn ask`
# render. `cairn run --dry-run` prints it too.
app.command(name="preflight")(_run.preflight_cmd)
# `gate` (#990, ADR-0076 D2) is the CI/CD face of the gate primitive: run a check
# workflow read-only, emit a JSON verdict + a blocking exit code so a PR/pipeline
# fails on a bad dataset/model. CI-agnostic; see examples/ci/.
app.command(name="gate")(_gate.cmd)
app.command(name="replay")(_replay.cmd)
app.command(name="lineage")(_lineage.cmd)
app.command(name="test")(_test.cmd)
app.command(name="lock")(_lock.cmd)
app.command(name="visualize")(_visualize.cmd)
# `init` is a sub-app (pack / template / skill / policy) so it mounts via
# add_typer rather than command(...).
app.add_typer(_init.app, name="init")
# `remote` is a sub-app (login / submit / ps / status / logs / cancel / approve / reject)
# that drives a running cairn-server over its HTTP + SSE control plane.
app.add_typer(_remote.app, name="remote")
# `audit` (ADR-0156 D2) — read the audit trail; `audit verify` checks the hash chain,
# read-only (exit 0 verified, 2 broken, 1 no log).
app.add_typer(_audit.app, name="audit")
# `dev` — one-command sqlite dev environment (#1188, ADR-0085 D5): engine + vite
# on a pinned config the repo .env can't poison; login dev/dev.
app.add_typer(_dev.app, name="dev")
# `connect` manages typed infra connections (model APIs / compute / MCP / oauth)
# over the ConnectionStore (#68): add / ls / test / rm / providers / authorize.
app.add_typer(_connect.app, name="connect")
# `compute` (#1787, ADR-0097) — the lifecycle of a governed box: `compute status`
# and `compute sweep`, the cron-shaped watchdog that stops every instance whose
# `max_hours` lease expired. The sweep needs only AWS credentials, so the guard
# survives the control plane that started the box.
app.add_typer(_compute.app, name="compute")
# `bundle` — bundle-pipeline tooling (ADR-0066): `bundle validate` checks the
# [[bundle.pipeline]] data contract against the participating templates' schemas.
app.add_typer(_config.app, name="config")
# #1079 — inspect flows + show how they split at in-flow approval gates.
app.add_typer(_flow.app, name="flow")
# #1126 — pipelines are the pack's runnable surface (ADR-0084 D4/D9).
app.add_typer(_pipeline.app, name="pipeline")
# `worker` runs a Temporal worker; `worker --remote` is the #565 pull-based
# remote worker that drains remote-job (SSH/GPU) activities off the remote queue.
app.add_typer(_worker.app, name="worker")
# `pack` introspects packs — `pack schema <ref>` emits the UI contract (#109)
# that every surface (CLI/TUI/web/Slack) renders from.
app.add_typer(_pack.app, name="pack")
# `deploy` operates the unified prod stack (#390): `deploy doctor` preflights
# that the .env and enabled compose profiles agree before `docker compose up`.
app.add_typer(_deploy.app, name="deploy")
# `demo` seeds an opt-in demo workspace (#394) from the installed `cairn.seeds`
# providers (#2418) so the console has something to show. Opt-in only: never
# auto-seeds on install or startup.
app.add_typer(_demo.app, name="demo")
# The product-only commands (`ask`, `pilot`, `series`, `showcase`, `eval`, `suggest`,
# `knowledge`) left for the platform (ADR-0120 D3, #2421 decision 2, ADR-0134 D6): every
# installed `cairn.cli` provider mounts here, after the public commands, which win every name.
#: The command names the installed providers mounted (`cairn/cli/providers.py`).
PROVIDER_COMMANDS: list[str] = mount_cli_providers(app)

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
