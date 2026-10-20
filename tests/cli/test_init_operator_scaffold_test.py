"""``cairn init operator`` scaffolds a test that ``cairn test`` runs (0.1.2
verification, bug 14: an empty ``tests/`` made ``cairn test <pack>`` exit 5)."""
from __future__ import annotations

import subprocess
import sys

from typer.testing import CliRunner

from cairn.cli.commands import init


def test_the_scaffold_ships_a_passing_test(tmp_path):
    dest = tmp_path / "vop"
    res = CliRunner().invoke(init.app, ["operator", "vop", "--op", "verify/echo_text",
                                        "--op", "verify/two", "--dest", str(dest)])
    assert res.exit_code == 0, res.output
    test_file = dest / "tests" / "test_operators.py"
    assert test_file.is_file()
    # the pack is not installed: the scaffolded test finds its own src/
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                        str(dest / "tests")], capture_output=True, text=True, cwd=tmp_path,
                       timeout=180)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "2 passed" in r.stdout, r.stdout
