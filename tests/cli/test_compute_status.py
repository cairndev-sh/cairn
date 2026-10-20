"""`cairn compute status` with no connection names where the names come from (#2536)."""
from __future__ import annotations

from typer.testing import CliRunner

from cairn.cli.commands import compute


def test_missing_connection_hints_at_connect_ls() -> None:
    res = CliRunner().invoke(compute.app, ["status"])
    assert res.exit_code == 2  # the usage-error code a missing argument always had
    assert "cairn connect ls" in res.output


def test_sweep_without_a_region_says_how_to_give_one(monkeypatch) -> None:
    """0.1.2 printed botocore's raw NoRegionError traceback (verification bug 11)."""
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    res = CliRunner().invoke(compute.app, ["sweep"])
    assert res.exit_code == 2, res.output
    assert "no AWS region: pass --region" in res.output
    assert isinstance(res.exception, SystemExit), res.exception  # an exit, not a traceback
