"""Old command names kept working for one minor version (ADR-0165 D12).

An alias is the same code under the old name, hidden from ``--help``. It says the new spelling on
stderr, so a script that reads stdout is undisturbed.
"""
from __future__ import annotations

import copy
import functools
from typing import Any, Callable

import typer
from typer.main import get_command_name


def renamed(old: str, new: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a command: run it, after saying on stderr that ``old`` is now ``new``."""
    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def inner(*args: Any, **kwargs: Any) -> Any:
            typer.secho(f"`{old}` is now `{new}`; the old name goes in the next minor version.",
                        fg="yellow", err=True)
            return fn(*args, **kwargs)
        inner.renamed_to = new  # type: ignore[attr-defined]  # what the surface test reads
        return inner
    return wrap


def renamed_group(group: typer.Typer, old: str, new: str) -> typer.Typer:
    """A copy of ``group`` for its old name: each command in it says its new spelling, then runs.
    ``group`` itself stays as it was, for its new name."""
    out = copy.copy(group)
    out.registered_commands = []
    for command in group.registered_commands:
        name = command.name or get_command_name(command.callback.__name__)
        info = copy.copy(command)
        info.name, info.callback = name, renamed(f"{old} {name}", f"{new} {name}")(command.callback)
        out.registered_commands.append(info)
    if group.registered_callback is not None:  # a group that is itself a command: `cairn worker`
        out.registered_callback = copy.copy(group.registered_callback)
        out.registered_callback.callback = renamed(old, new)(group.registered_callback.callback)
    return out
