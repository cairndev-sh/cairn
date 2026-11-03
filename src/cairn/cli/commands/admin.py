"""``cairn admin``: deployment and governance chores (ADR-0165 D7).

Nothing here is new behaviour: each command is the code that served its old name. The old names
stay as hidden aliases for one minor version (``cairn/cli/main.py``, ``commands/remote.py`` and
``commands/pipeline.py``). Options that reach the engine are long-form (D9): ``-e`` never means the
endpoint here.
"""
from __future__ import annotations

from typing import Optional

import typer

from cairn.cli.commands import audit as _audit
from cairn.cli.commands import compute as _compute
from cairn.cli.commands import config as _config
from cairn.cli.commands import connect as _connect
from cairn.cli.commands import demo as _demo
from cairn.cli.commands import deploy as _deploy
from cairn.cli.commands import lineage as _lineage
from cairn.cli.commands import pipeline as _pipeline
from cairn.cli.commands import remote as _remote
from cairn.cli.commands import worker as _worker
from cairn.cli.commands.remote import _PROF, _TOK
from cairn.cli.commands.status import _ENDPOINT

app = typer.Typer(help="Deployment and governance chores.", no_args_is_help=True)

trigger_app = typer.Typer(help="List, enable and disable stored triggers.", no_args_is_help=True)
preset_app = typer.Typer(help="List and delete a pipeline's launch presets.", no_args_is_help=True)


@trigger_app.command("ls")
def trigger_ls_cmd(
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """List this workspace's stored triggers (event, webhook) and whether each is on."""
    _remote.list_triggers(endpoint, token, profile)


@trigger_app.command("enable")
def trigger_enable_cmd(
    trigger_id: str = typer.Argument(..., help="a stored trigger id, from `cairn admin trigger ls`"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """Turn a stored trigger on: a declared trigger is paused until a person does."""
    _remote.set_trigger(trigger_id, True, endpoint, token, profile)


@trigger_app.command("disable")
def trigger_disable_cmd(
    trigger_id: str = typer.Argument(..., help="a stored trigger id, from `cairn admin trigger ls`"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """Pause a stored trigger: it starts nothing until it is enabled again."""
    _remote.set_trigger(trigger_id, False, endpoint, token, profile)


@preset_app.command("ls")
def preset_ls_cmd(
    ref: str = typer.Argument(..., help="installed pipeline ref, e.g. training/retrain-cycle"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """List the launch presets for a pipeline (saved + pack-shipped)."""
    _remote.list_presets(ref, endpoint, token, profile)


@preset_app.command("rm")
def preset_rm_cmd(
    ref: str = typer.Argument(..., help="installed pipeline ref"),
    name: str = typer.Argument(..., help="the saved preset's name"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """Delete a saved preset (a pack-shipped one of the same name reappears)."""
    _remote.delete_preset(ref, name, endpoint, token, profile)


@app.command("console")
def console_cmd(
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """Launch the interactive terminal console over the server.

    A live runs view, a command bar with autocomplete, and clickable approve and reject. Needs
    textual and textual-autocomplete, which cairndev does not install: the error says how to add them."""
    _remote.open_console(endpoint, token, profile)


app.add_typer(_connect.app, name="connect")
app.add_typer(trigger_app, name="trigger")
app.add_typer(preset_app, name="preset")
app.add_typer(_audit.app, name="audit")
app.command("lineage")(_lineage.cmd)
app.add_typer(_compute.app, name="compute")
app.add_typer(_config.app, name="config")
app.add_typer(_deploy.app, name="deploy")
app.add_typer(_worker.app, name="worker")
app.command("worker-token")(_remote.worker_token_cmd)
app.command("hash-password")(_remote.hash_password_cmd)
app.add_typer(_demo.app, name="demo")
app.command("backfill")(_pipeline.backfill_cmd)
app.command("respell-packs")(_pipeline.respell_packs_cmd)
