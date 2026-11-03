"""Where cairn runs its own CLI, it runs a command that exists (ADR-0165 D12).

An old name only prints its new one and exits 2, so a caller still using one fails with nothing to
show for it: ``cairn setup --seed`` seeded nothing and the console's Doctor checked nothing. These
hold the callers to the real command tree. Nothing here runs a command.
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.main import get_command

from cairn.cli.commands import dev as dev_cli
from cairn.cli.main import app
from cairn.cli.moved import MOVED
from cairn.cli.console_argv import LOCAL_CMDS, cli_argv

ROOT = Path(__file__).resolve().parents[2]
CLI = get_command(app)
#: Verbs the console offers that a provider adds: the platform mounts these, the core has neither.
_MOUNTED = {"ask", "eval"}
_GONE = {old.split()[0] for old in MOVED} - set(CLI.commands)


def _walk(argv: list[str]) -> tuple[Any, list[str]]:
    cmd, rest = CLI, list(argv)
    while hasattr(cmd, "commands") and rest and not rest[0].startswith("-"):
        sub = cmd.commands.get(rest[0])
        assert sub is not None, f"`cairn {' '.join(argv)}`: {cmd.name} has no {rest[0]!r}"
        cmd, rest = sub, rest[1:]
    return cmd, rest


def _parses(argv: list[str]) -> None:
    cmd, rest = _walk(argv)
    cmd.make_context(cmd.name, rest)  # a missing argument or unknown option raises


def _cairn_argv_heads(source: str) -> list[tuple[int, str]]:
    """The subcommand of every list that runs cairn: ``[bin, "x"]`` or ``[..., "cairn.cli.main", "x"]``."""
    heads = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.List) or not node.elts:
            continue
        values = [e.value if isinstance(e, ast.Constant) else None for e in node.elts]
        if "cairn.cli.main" in values:
            at = values.index("cairn.cli.main") + 1
        elif "cairn" in ast.unparse(node.elts[0]):
            at = 1
        else:
            continue
        if at < len(values) and isinstance(values[at], str):
            heads.append((node.lineno, values[at]))
    return heads


def test_no_code_runs_cairn_with_a_command_that_is_gone() -> None:
    found = []
    for path in (ROOT / "src/cairn").rglob("*.py"):
        found += [f"{path.relative_to(ROOT)}:{line}: cairn {head}"
                  for line, head in _cairn_argv_heads(path.read_text(encoding="utf-8")) if head in _GONE]
    assert not found, "\n".join(found)


def test_the_scan_can_fail() -> None:
    gone = '[str(cairn_bin), "demo"]\n[sys.executable, "-P", "-m", "cairn.cli.main", "doctor"]'
    assert [head for _, head in _cairn_argv_heads(gone) if head in _GONE] == ["demo", "doctor"]
    assert not [h for _, h in _cairn_argv_heads('[str(cairn_bin), "admin", "demo"]\n["git", "demo"]') if h in _GONE]


def test_the_walk_can_fail() -> None:
    with pytest.raises(AssertionError, match="has no 'demo'"):
        _walk(["demo"])
    with pytest.raises(AssertionError, match="has no 'nope'"):
        _walk(["admin", "nope"])
    assert _walk(["pack", "show", "--graph"])[0].name == "show"


@pytest.mark.parametrize("has_script", [True, False], ids=["script", "module"])
def test_the_seed_runs_a_command_that_exists(has_script, monkeypatch, tmp_path) -> None:
    python = tmp_path / "python"
    if has_script:
        (tmp_path / "cairn").touch()
    monkeypatch.setattr(sys, "executable", str(python))
    ran = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, env: ran.append(cmd) or SimpleNamespace(returncode=0))
    dev_cli._seed({})
    argv = ran[0][1:] if has_script else ran[0][ran[0].index("cairn.cli.main") + 1:]
    assert argv == ["admin", "demo"]
    _parses(argv)


@pytest.mark.parametrize("toks, argv", [
    (["ls"], ["pack", "ls"]),
    (["doctor"], ["setup", "--check"]),
    (["connect", "test", "openai"], ["admin", "connect", "test", "openai"]),
    (["validate", "."], ["pack", "check", "."]),
    (["init", "pack", "x"], ["pack", "new", "pack", "x"]),
    (["lock", "--check"], ["pack", "lock", "--check"]),
    (["replay", "run-1"], ["status", "run-1", "--replay"]),
    (["visualize", "template", "a/b"], ["pack", "show", "--graph", "a/b"]),
    (["visualize", "a/b"], ["pack", "show", "--graph", "a/b"]),
    (["run", "a/b"], ["run", "a/b"]),
])
def test_the_console_shells_out_to_a_command_that_exists(toks, argv) -> None:
    assert cli_argv(toks) == argv
    _parses(argv)


def test_every_verb_the_console_runs_has_a_command() -> None:
    tree = ast.parse((ROOT / "src/cairn/cli/tui/app.py").read_text(encoding="utf-8"))
    heads = {node.args[0].elts[0].value for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
             and node.func.attr == "_shell" and isinstance(node.args[0], ast.List)}
    assert "doctor" in heads  # the walk found the calls
    for verb in (heads | LOCAL_CMDS) - _MOUNTED:
        _walk(cli_argv([verb])[:1])


@pytest.mark.parametrize("old", sorted(MOVED))
def test_every_new_name_exists(old: str) -> None:
    named = re.findall(r"`cairn ([^`]+)`", MOVED[old])
    assert named, MOVED[old]
    for text in named:
        cmd, rest = _walk(text.split())
        known = {opt for param in cmd.params for opt in (*param.opts, *param.secondary_opts)}
        assert not [tok for tok in rest if tok.startswith("-") and tok not in known], text


def test_the_new_name_of_a_rejection_is_one_you_can_type() -> None:
    assert MOVED["remote reject"] == "`cairn decide <run> reject`"
