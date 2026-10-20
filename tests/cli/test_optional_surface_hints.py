"""``cairn-admin`` and ``cairn remote console`` name what they need, not an extra that
does not exist (0.1.2 verification, bug 6: ``cairndev[admin]`` and ``.[tui]``).

Both are public code that needs packages cairndev does not install; the message
names the packages and the command that adds them to the environment cairn runs in.
"""
from __future__ import annotations

import importlib
import sys

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import remote


def test_admin_names_its_packages_not_a_missing_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "starlette_admin", None)  # not installed
    monkeypatch.delitem(sys.modules, "cairn.core.admin", raising=False)
    with pytest.raises(ImportError) as e:
        importlib.import_module("cairn.core.admin")
    msg = str(e.value)
    assert "cairndev[admin]" not in msg
    assert f"uv pip install --python {sys.executable} starlette-admin itsdangerous" in msg


def test_remote_console_names_its_packages_not_a_missing_extra(monkeypatch, tmp_path):
    monkeypatch.setenv("OBS_CONFIG_HOME", str(tmp_path))
    monkeypatch.setitem(sys.modules, "textual", None)  # not installed
    monkeypatch.delitem(sys.modules, "cairn.cli.tui.app", raising=False)
    res = CliRunner().invoke(remote.app, ["console", "--endpoint", "http://127.0.0.1:1"])
    assert res.exit_code == 1, res.output
    assert ".[tui]" not in res.output
    assert f"uv pip install --python {sys.executable} textual textual-autocomplete" in res.output
