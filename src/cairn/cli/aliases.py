"""Old command names kept working for one minor version (ADR-0165 D12).

An alias is the same code under the old name, hidden from ``--help``. It says the new spelling on
stderr, so a script that reads stdout is undisturbed.
"""
from __future__ import annotations

import functools
from typing import Any, Callable

import typer


def renamed(old: str, new: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a command: run it, after saying on stderr that ``old`` is now ``new``."""
    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def inner(*args: Any, **kwargs: Any) -> Any:
            typer.secho(f"`{old}` is now `{new}`; the old name goes in the next minor version.",
                        fg="yellow", err=True)
            return fn(*args, **kwargs)
        return inner
    return wrap
