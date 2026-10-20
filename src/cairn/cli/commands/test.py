"""``cairn test <pack-path>`` — run a pack's pytest suite (§24.20.14).

A thin, honest wrapper: locate the pack's tests (``<path>/tests`` if present,
else the path itself) and shell out to pytest in the current interpreter.
Extra args after ``--`` pass straight through.

pytest is a ``dev`` extra, not a base dependency, so a fresh ``pip install cairndev``
has none: say so instead of letting ``python -m pytest`` exit 1 with "No module named
pytest" (#2536). pytest's own "no tests collected" (exit 5) is explained the same way.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import typer

#: pytest's exit code when it collected nothing.
_NO_TESTS = 5


def cmd(
    pack_path: str = typer.Argument(".", help="Pack directory (default: cwd)."),
    pytest_args: list[str] = typer.Argument(
        None, help="Extra args passed through to pytest (after --).",
    ),
) -> None:
    """Run the pack's tests via pytest."""
    root = Path(pack_path)
    if not root.exists():
        print(f"pack path not found: {root}", file=sys.stderr)
        raise typer.Exit(2)

    if importlib.util.find_spec("pytest") is None:
        # The environment cairn runs in, which the installer's uv tool keeps isolated: a
        # bare `pip install` from the shell lands somewhere else.
        print("cairn test runs a pack's pytest suite, and pytest is not installed in "
              "cairn's environment. Add it:\n"
              f"  uv pip install --python {sys.executable} pytest pytest-asyncio",
              file=sys.stderr)
        raise typer.Exit(1)

    target = root / "tests" if (root / "tests").is_dir() else root
    cmd_argv = [sys.executable, "-m", "pytest", str(target), *(pytest_args or [])]
    print(f"$ {' '.join(cmd_argv)}")
    rc = subprocess.run(cmd_argv).returncode
    if rc == _NO_TESTS:
        print(f"no tests found under {target} — a pack's tests live in "
              f"{root / 'tests'}/test_*.py", file=sys.stderr)
    raise typer.Exit(rc)
