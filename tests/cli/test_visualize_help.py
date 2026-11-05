"""`cairn visualize` says up front that it renders agentic templates only (#2536)."""
from __future__ import annotations

import typer
from typer.testing import CliRunner

from cairn.cli.commands import visualize

_app = typer.Typer()
_app.command()(visualize.cmd)


def test_help_says_only_template_is_supported() -> None:
    out = CliRunner().invoke(_app, ["--help"], env={"COLUMNS": "200"}).output
    assert "Only `template` is supported" in out
    assert "cairn pack show" in out


def test_a_pipeline_kind_is_refused_with_the_command_that_shows_it() -> None:
    res = CliRunner().invoke(_app, ["pipeline", "hello-cairn/check-and-publish"])
    assert res.exit_code == 2
    assert "only 'template' is supported" in res.output
    assert "cairn pack show <ref>" in res.output
