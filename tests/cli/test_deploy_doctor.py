"""`cairn deploy doctor` on a fresh install (#2536).

With no deploy .env it reported one FAIL per required variable; the variables were never
the problem, the missing file was. The required variables themselves stay FAIL — the
compose stack refuses to start without them — and the report says whose requirement it is.
"""
from __future__ import annotations

from typer.testing import CliRunner

from cairn.cli.commands import deploy
from cairn.cli.main import app


def test_no_env_file_is_one_finding(tmp_path) -> None:
    missing = tmp_path / "deploy" / ".env"
    res = CliRunner().invoke(app, ["admin", "deploy", "doctor", "--env", str(missing)])
    assert res.exit_code == 1
    assert f"no deploy .env at {missing}" in res.output
    assert "--values" in res.output
    assert "CADDY_DOMAIN" not in res.output  # not one FAIL per variable


def test_unset_required_vars_still_fail_and_say_whose_they_are(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("CAIRN_RUNTIME=inprocess\n")
    res = CliRunner().invoke(app, ["admin", "deploy", "doctor", "--env", str(env)])
    assert res.exit_code == 1
    assert "CADDY_DOMAIN: REQUIRED" in res.output
    assert "required = what the compose stack refuses to start without" in res.output


def test_a_complete_required_block_prints_no_note(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("".join(f"{k}=x\n" for k in deploy.REQUIRED))
    res = CliRunner().invoke(app, ["admin", "deploy", "doctor", "--env", str(env)])
    assert "refuses to start without" not in res.output


def test_no_env_file_still_checks_a_caddyfile_beside_it(tmp_path) -> None:
    """A box whose .env went missing keeps its hand-edited Caddyfile: still preflight it."""
    (tmp_path / "Caddyfile").write_text("a.example.com {\n}\na.example.com {\n}\n")
    res = CliRunner().invoke(app, ["admin", "deploy", "doctor", "--env", str(tmp_path / ".env")])
    assert res.exit_code == 1
    assert "no deploy .env at" in res.output
    assert "caddyfile" in res.output.lower()


def test_no_env_file_and_no_caddyfile_prints_no_report(tmp_path) -> None:
    res = CliRunner().invoke(app, ["admin", "deploy", "doctor", "--env", str(tmp_path / ".env")])
    assert "Summary:" not in res.output
