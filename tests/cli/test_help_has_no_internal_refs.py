"""`--help` names behaviour, never maintainer references (#318).

ADR numbers, issue numbers and section references mean nothing to a user; they live as
code comments beside the command instead. This walks every command and group the public
CLI registers and renders its `--help`."""
from __future__ import annotations

import importlib
import re

import pytest
import typer
from typer.testing import CliRunner

import cairn.cli.main as cli_main
import cairn.cli.providers as cli_providers

#: `ADR-0105`, an issue number `#1188` (not a URL fragment or `a#123`), a section `§24.11`.
_INTERNAL_REF = re.compile(r"ADR-\d+|(?<![\w/])#\d{3,}|§\s?\d")
_ANSI = re.compile(r"\x1b\[[0-9;]*m")  # typer forces colour under GITHUB_ACTIONS


def _paths(cmd, prefix: tuple[str, ...] = ()):
    """Every command path under ``cmd`` (the root is the empty path) that has `--help`.

    Typer vendors its own click, so a group is recognised by its ``commands`` mapping."""
    if not prefix or any(p.name == "help" for p in cmd.get_params(cmd.context_class(cmd))):
        yield prefix
    for name, sub in sorted(getattr(cmd, "commands", {}).items()):
        yield from _paths(sub, prefix + (name,))


def _help_pages(app: typer.Typer) -> dict[str, str]:
    root = typer.main.get_command(app)
    runner = CliRunner()
    pages: dict[str, str] = {}
    for path in _paths(root):
        res = runner.invoke(app, [*path, "--help"], env={"COLUMNS": "400", "TERMINAL_WIDTH": "400"})
        assert res.exit_code == 0, (path, res.output)
        pages[" ".join(("cairn", *path))] = _ANSI.sub("", res.output)
    return pages


@pytest.fixture(scope="module")
def pages() -> dict[str, str]:
    """Every `--help` page of `cairn.cli.main` as a public install sees it: no `cairn.cli`
    provider mounted, so only the commands this repository defines are walked."""
    mp = pytest.MonkeyPatch()
    mp.setattr(cli_providers, "entry_points", lambda group: [])
    try:
        return _help_pages(importlib.reload(cli_main).app)
    finally:
        mp.undo()
        importlib.reload(cli_main)


def test_the_walk_reaches_nested_commands(pages) -> None:
    assert "cairn" in pages and "cairn run" in pages and "cairn audit verify" in pages


def test_no_help_page_carries_an_internal_reference(pages) -> None:
    offenders = {
        name: sorted(set(_INTERNAL_REF.findall(out)))
        for name, out in pages.items()
        if _INTERNAL_REF.search(out)
    }
    assert not offenders, "\n".join(f"{n}: {', '.join(r)}" for n, r in sorted(offenders.items()))
