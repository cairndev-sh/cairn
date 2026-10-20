"""`cairn test` on a fresh install says why it did not run anything (#2536)."""
from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest
import typer

from cairn.cli.commands import test as test_cmd
from tests.cli.conftest import EXAMPLE_PACK


def test_without_pytest_it_names_the_install(monkeypatch, capsys) -> None:
    monkeypatch.setattr(test_cmd.importlib.util, "find_spec",
                        lambda name: None if name == "pytest" else object())
    with pytest.raises(typer.Exit) as e:
        test_cmd.cmd(str(EXAMPLE_PACK), None)
    assert e.value.exit_code == 1
    err = capsys.readouterr().err
    assert "pytest is not installed in cairn's environment" in err
    # the installer's uv tool is isolated: the hint targets the interpreter cairn runs on
    assert f"uv pip install --python {test_cmd.sys.executable} pytest pytest-asyncio" in err


def test_a_pack_without_tests_says_where_they_go(monkeypatch, capsys) -> None:
    monkeypatch.setattr(subprocess, "run", lambda argv: SimpleNamespace(returncode=5))
    with pytest.raises(typer.Exit) as e:
        test_cmd.cmd(str(EXAMPLE_PACK), None)
    assert e.value.exit_code == 5  # pytest's own code, unchanged
    err = capsys.readouterr().err
    assert f"no tests found under {EXAMPLE_PACK}" in err
    assert f"{EXAMPLE_PACK / 'tests'}/test_*.py" in err
