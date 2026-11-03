"""Nothing cairn prints tells someone to run a command that's gone (ADR-0165 D12).

Those commands only say their new name now, so a message naming one is a dead end. The names come
from the table the CLI answers with. Docstrings and comments aren't scanned: nobody is told to type
them. ``test_docs_use_current_commands.py`` holds the docs to the same rule.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from typer.main import get_command

from cairn.cli.main import app
from cairn.cli.moved import MOVED

ROOT = Path(__file__).resolve().parents[2]
_FIRST_WORDS = sorted({old.split()[0] for old in MOVED})
_OLD = re.compile(rf"\bcairn ({'|'.join(_FIRST_WORDS)})\b")
#: Prose that happens to read like a command.
_PROSE = ("a cairn worker", "the cairn flow", "cairn flow as", "cairn test suite")


def _strings(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = {id(node.body[0].value) for node in ast.walk(tree)
                  if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                  and node.body and isinstance(node.body[0], ast.Expr)
                  and isinstance(node.body[0].value, ast.Constant)}
    return [(node.lineno, node.value) for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings]


def _names_a_gone_command(text: str) -> bool:
    return bool(_OLD.search(text)) and not any(prose in text for prose in _PROSE)


def test_no_string_in_the_code_names_a_command_that_is_gone() -> None:
    found = []
    for root in ("src/cairn", "packages"):
        for path in (ROOT / root).rglob("*.py"):
            if path.name == "moved.py" or "tests" in path.parts:
                continue
            found += [f"{path.relative_to(ROOT)}:{line}: {text[:100]!r}" for line, text in _strings(path)
                      if _names_a_gone_command(text)]
    assert not found, "\n".join(found)


def _help_texts(cmd) -> list[tuple[str, str]]:
    name = cmd.name or "cairn"
    found = [(name, cmd.help or ""), *((name, getattr(p, "help", None) or "") for p in cmd.params)]
    for sub in getattr(cmd, "commands", {}).values():
        found += _help_texts(sub)
    return found


def test_no_help_page_names_a_command_that_is_gone() -> None:
    found = [f"{name}: {text[:100]!r}" for name, text in _help_texts(get_command(app)) if _names_a_gone_command(text)]
    assert not found, "\n".join(found)


def test_the_check_can_fail() -> None:
    assert _names_a_gone_command("Release it with: cairn remote advance pl s")
    assert _names_a_gone_command("see cairn doctor")
    assert not _names_a_gone_command("see cairn setup --check")
    assert not _names_a_gone_command("Run a cairn worker: a task queue")
