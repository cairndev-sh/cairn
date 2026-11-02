"""CLI providers: the ``cairn.cli`` entry-point group (ADR-0120 D4, #2421 decision 2).

The public ``cairn`` registers its own commands (``cairn/cli/main.py``) and then mounts
every installed provider's commands after them, so a deployment that installs only the
public core lists only the public commands, and an absent provider is never a crash.

A provider is a callable registered under the group::

    [project.entry-points."cairn.cli"]
    platform = "cairn_platform.cli.providers:commands"

that takes no arguments and returns either a ``typer.Typer`` (mounted under the
entry-point name) or a list of ``(name, typer.Typer)`` pairs (each mounted under its
name). A Typer with exactly one command and no callback mounts as that one command —
typer's own single-command convention — so ``cairn <name> <args>`` keeps a plain
command's shape; any other Typer mounts as a command group (``add_typer``). Providers
mount in entry-point name order.

A provider names the ``--help`` heading its commands appear under with Typer's own
``rich_help_panel`` (``typer.Typer(rich_help_panel="Platform commands")``, ADR-0165 D8);
one that names none, or names a heading the public CLI holds, gets ``<Entry-point name>
commands``. Moving a command between a provider and the public CLI changes its heading,
never its spelling.

A pack, operator or contract wheel adds no command (ADR-0165 D8): a provider from a
distribution named ``cairn-pack-*``, ``cairn-operator-*`` or ``cairn-contracts-*`` is
skipped before its code is imported.

The public CLI wins every name: a provider command whose name a public command (or an
earlier provider) already holds is refused with a logged warning. One public name
yields instead: ``ask``'s hidden stub (``cairn/cli/main.py``) registers AFTER this pass,
and only when no provider supplied ``ask`` (ADR-0134 D6). Like the router, seed
and catalogue loaders this one fails open: a provider that does not import, raises, or
returns anything else is skipped with a logged warning naming it, and ``cairn doctor``
renders every skip (``cli_providers``).
"""
from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any, Optional

import typer

_log = logging.getLogger("cairn.cli.providers")

ENTRY_POINT_GROUP = "cairn.cli"

#: The ``--help`` headings the public CLI holds (ADR-0165 D1).
MAIN_COMMANDS = "Main commands"
OTHER_COMMANDS = "All other commands"

#: The distributions that may not add a command (ADR-0165 D8), by normalised name.
_NO_COMMANDS_FROM = ("cairn-pack-", "cairn-operator-", "cairn-contracts-")

#: Commands refused by the last :func:`mount_cli_providers` pass (a name already taken),
#: as ``{name, target, error}`` — the shape ``seed_load_failures`` uses.
_MOUNT_FAILURES: list[dict[str, str]] = []


@dataclass(frozen=True)
class CliProvider:
    """One registered provider: its entry-point name and value, and the commands it
    returned (empty with ``error`` set when it did not load or returned something else)."""

    name: str
    value: str
    commands: tuple[tuple[str, typer.Typer], ...] = ()
    error: Optional[str] = None


def _as_commands(ep_name: str, result: Any) -> Optional[tuple[tuple[str, typer.Typer], ...]]:
    if isinstance(result, typer.Typer):
        return ((ep_name, result),)
    if isinstance(result, (list, tuple)) and all(
        isinstance(item, (list, tuple)) and len(item) == 2
        and isinstance(item[0], str) and item[0] and isinstance(item[1], typer.Typer)
        for item in result
    ):
        return tuple((n, t) for n, t in result)
    return None


def _refused_distribution(ep: Any) -> Optional[str]:
    """The distribution behind ``ep`` when it is one that may not add a command."""
    dist = getattr(ep, "dist", None)
    name = re.sub(r"[-_.]+", "-", dist.name or "").lower() if dist is not None else ""
    return name if name.startswith(_NO_COMMANDS_FROM) else None


def load_cli_providers() -> list[CliProvider]:
    """Every registered provider, called, in mount order. Never raises."""
    out: list[CliProvider] = []
    seen: dict[str, str] = {}
    for ep in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda e: (e.name, e.value)):
        # before the dedupe below: a refused wheel must not shadow a provider of the same name and value
        refused = _refused_distribution(ep)
        if refused:
            error = f"distribution {refused} is a pack, operator or contract wheel, which adds no command"
            _log.warning("cli provider %r (%s) skipped: %s", ep.name, ep.value, error)
            out.append(CliProvider(ep.name, ep.value, error=error))
            continue
        if seen.get(ep.name) == ep.value:
            continue  # one distribution seen twice on sys.path: the same provider
        seen.setdefault(ep.name, ep.value)
        try:
            obj = ep.load()
            # a Typer is itself callable (it runs the CLI): take it as the result, never call it
            result = obj if isinstance(obj, typer.Typer) else obj() if callable(obj) else obj
        except Exception as e:  # noqa: BLE001 — a broken provider must not break `cairn`
            error = f"{type(e).__name__}: {e}"
        else:
            commands = _as_commands(ep.name, result)
            if commands is not None:
                out.append(CliProvider(ep.name, ep.value, commands))
                continue
            error = (f"returned {type(result).__name__}, not a typer.Typer or a list of "
                     "(name, typer.Typer)")
        _log.warning("cli provider %r (%s) skipped: %s", ep.name, ep.value, error)
        out.append(CliProvider(ep.name, ep.value, error=error))
    return out


def command_names(app: typer.Typer) -> set[str]:
    """The top-level names ``app`` already serves (commands and groups)."""
    from typer.main import get_command_name  # the name Typer itself derives from a callback

    names = {c.name or (get_command_name(c.callback.__name__) if c.callback else None)
             for c in app.registered_commands}
    names |= {g.name or g.typer_instance.info.name for g in app.registered_groups}
    return {n for n in names if isinstance(n, str) and n}


def _heading(provider: str, *named: Any) -> str:
    """The first heading the provider named, or one from its entry-point name. A heading the
    public CLI holds is not the provider's to use."""
    for heading in named:
        if heading in (MAIN_COMMANDS, OTHER_COMMANDS):
            _log.warning("cli provider %r: the heading %r belongs to the public CLI", provider, heading)
        elif heading:
            return heading
    return f"{provider.capitalize()} commands"


def _mount(app: typer.Typer, provider: str, name: str, sub: typer.Typer) -> None:
    if sub.registered_callback is None and not sub.registered_groups and len(sub.registered_commands) == 1:
        info = copy.copy(sub.registered_commands[0])
        info.name = name
        info.rich_help_panel = _heading(provider, info.rich_help_panel, sub.info.rich_help_panel)
        app.registered_commands.append(info)
    else:
        app.add_typer(sub, name=name, rich_help_panel=_heading(provider, sub.info.rich_help_panel))


def mount_cli_providers(app: typer.Typer) -> list[str]:
    """Mount every registered provider's commands on ``app`` after its own; return the
    command names that mounted. A name already taken is refused — the public CLI wins."""
    _MOUNT_FAILURES.clear()
    taken = command_names(app)
    mounted: list[str] = []
    for p in load_cli_providers():
        for name, sub in p.commands:
            if name in taken:
                _log.warning("cli provider %r (%s): command %r refused — the name is taken",
                             p.name, p.value, name)
                _MOUNT_FAILURES.append({"name": name, "target": p.value,
                                        "error": "the name is taken by a public command or an earlier provider"})
                continue
            _mount(app, p.name, name, sub)
            taken.add(name)
            mounted.append(name)
    return mounted


def cli_mount_failures() -> list[dict[str, str]]:
    """Commands refused by the last :func:`mount_cli_providers` pass."""
    return list(_MOUNT_FAILURES)


__all__ = [
    "ENTRY_POINT_GROUP", "MAIN_COMMANDS", "OTHER_COMMANDS", "CliProvider", "cli_mount_failures",
    "command_names", "load_cli_providers", "mount_cli_providers",
]
