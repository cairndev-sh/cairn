"""The root of the `cairn` command: `--version`, and no `ask` verb on a public install (#2536).

Ask is the platform's (ADR-0134 D6): its `cairn.cli` provider supplies `ask`, and the
public CLI's hidden stub mounts only when no provider did."""
from __future__ import annotations

import importlib
import importlib.metadata
import re
import sys

import pytest
import typer
from typer.testing import CliRunner

import cairn.cli.main as cli_main
import cairn.cli.providers as cli_providers

#: A command row of the rich `--help` panel: `│ <name>   <help…>`.
_ROW = re.compile(r"^│\s+([a-z][a-z-]*)\s", re.MULTILINE)


def _commands(app) -> set[str]:
    out = CliRunner().invoke(app, ["--help"], env={"COLUMNS": "200"}).output
    return set(_ROW.findall(out))


class _FakeEntryPoint:
    """The three attributes `load_cli_providers` reads from an entry point."""

    def __init__(self, name: str, provider) -> None:
        self.name, self.value, self._provider = name, f"fake:{name}", provider

    def load(self):
        return self._provider


def _reload_with_providers(monkeypatch, *eps: _FakeEntryPoint):
    """`cairn.cli.main` re-imported with exactly these `cairn.cli` providers installed."""
    monkeypatch.setattr(cli_providers, "entry_points",
                        lambda group: list(eps) if group == cli_providers.ENTRY_POINT_GROUP else [])
    return importlib.reload(cli_main)


@pytest.fixture
def without_ask_provider(monkeypatch):
    """`cairn.cli.main` re-imported as a public install sees it: no provider supplies `ask`."""
    yield _reload_with_providers(monkeypatch)
    monkeypatch.undo()
    importlib.reload(cli_main)


@pytest.fixture
def with_ask_provider(monkeypatch):
    """`cairn.cli.main` re-imported with a fake provider that supplies `ask` (the platform's shape)."""
    ask = typer.Typer()

    @ask.callback(invoke_without_command=True)
    def _root() -> None:
        """the provider's ask"""

    @ask.command("plan")
    def _plan(ref: str) -> None:
        typer.echo(f"provider plan {ref}")

    yield _reload_with_providers(monkeypatch, _FakeEntryPoint("fake", lambda: [("ask", ask)]))
    monkeypatch.undo()
    importlib.reload(cli_main)


def test_help_lists_no_ask_verb_without_an_ask_provider(without_ask_provider) -> None:
    names = _commands(without_ask_provider.app)
    assert "run" in names and "pack" in names  # the panel was read
    assert "ask" not in names


def test_ask_without_an_ask_provider_says_it_is_platform_only(without_ask_provider) -> None:
    """Ask is platform-only (ADR-0127 D7) and the public cairndev drops the `ask` extra, so
    the stub names cairn-platform and no install command that cannot work (#2579)."""
    res = CliRunner().invoke(without_ask_provider.app, ["ask", "plan", "x"])
    assert res.exit_code == 1
    assert "part of cairn-platform" in res.output
    assert "cairndev[ask]" not in res.output and "pip install" not in res.output


def test_a_provider_that_supplies_ask_replaces_the_stub(with_ask_provider) -> None:
    """ADR-0134 D6: the stub is the one public name that yields — a `cairn.cli` provider's
    `ask` mounts, is listed, and answers; nothing is refused."""
    res = CliRunner().invoke(with_ask_provider.app, ["ask", "plan", "x"])
    assert res.exit_code == 0, res.output
    assert res.output.strip() == "provider plan x"
    assert cli_providers.cli_mount_failures() == []
    assert "ask" in _commands(with_ask_provider.app)
    assert "ask" in with_ask_provider.PROVIDER_COMMANDS


def test_a_provider_cannot_take_any_other_public_name(monkeypatch) -> None:
    """Only the stub yields: a provider's `run` is still refused — the public CLI wins it."""
    hijack = typer.Typer()
    hijack.command("run")(lambda: typer.echo("hijacked"))
    try:
        main = _reload_with_providers(monkeypatch, _FakeEntryPoint("fake", lambda: [("run", hijack)]))
        assert [f["name"] for f in cli_providers.cli_mount_failures()] == ["run"]
        assert "run" not in main.PROVIDER_COMMANDS
    finally:
        monkeypatch.undo()
        importlib.reload(cli_main)


def test_version_prints_the_cairndev_distribution(monkeypatch) -> None:
    monkeypatch.setattr(importlib.metadata, "version",
                        lambda dist: "9.8.7" if dist == "cairndev" else "?")
    res = CliRunner().invoke(cli_main.app, ["--version"])
    assert res.exit_code == 0
    assert res.output.strip() == "cairn 9.8.7"


def test_version_on_an_uninstalled_source_tree(monkeypatch) -> None:
    def missing(dist):
        raise importlib.metadata.PackageNotFoundError(dist)

    monkeypatch.setattr(importlib.metadata, "version", missing)
    res = CliRunner().invoke(cli_main.app, ["--version"])
    assert res.exit_code == 0
    assert res.output.strip() == "cairn 0+unknown"
