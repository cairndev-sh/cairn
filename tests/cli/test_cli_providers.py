"""`cairn.cli` — commands beyond the public CLI come from entry points (#2421 decision 2).

Pins:

  1. The loader fails open: a provider that does not import, raises, or returns
     anything but a ``typer.Typer`` / list of ``(name, typer.Typer)`` is skipped and
     recorded; the rest mount.
  2. The public CLI wins every name: a provider command named like a public command
     (or an earlier provider's) is refused, recorded, and the public command still runs.
  3. A single-command Typer mounts as that command (``cairn eval <pack>`` keeps its shape);
     any other mounts as a group.
  4. The public ``cairn`` with no ``cairn.cli`` provider lists none of the product-only
     commands (a subprocess: importing ``cairn.cli.main`` here would re-run ``load_env``
     in the test session — see ``tests/cli/test_ls_operators.py``).
  5. ``cairn doctor`` renders the ``cli_providers`` row and every skip.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.metadata import EntryPoint

import pytest
import typer
from typer.testing import CliRunner

from cairn.cli import providers

PRODUCT_COMMANDS = ("pilot", "series", "showcase", "eval", "suggest", "knowledge")

# --- provider targets the fake entry points resolve to --------------------------------
_grp = typer.Typer()


@_grp.command("one")
def _one() -> None:
    typer.echo("group one ran")


@_grp.command("two")
def _two() -> None:
    typer.echo("group two ran")


_single = typer.Typer()


@_single.command("whatever")
def _only(pack: str) -> None:
    typer.echo(f"single ran {pack}")


_clash = typer.Typer()


@_clash.command("x")
def _clash_x() -> None:
    typer.echo("the provider's doctor ran")


def good() -> list[tuple[str, typer.Typer]]:
    return [("grp", _grp), ("single", _single), ("doctor", _clash)]


def as_typer() -> typer.Typer:
    return _grp


def not_a_typer() -> object:
    return {"grp": _grp}


def raises() -> None:
    raise RuntimeError("provider exploded")


_HERE = __name__


def _eps(monkeypatch, *pairs: tuple[str, str]) -> None:
    monkeypatch.setattr(providers, "entry_points", lambda group: [
        EntryPoint(name=n, value=v, group=group) for n, v in pairs
    ] if group == providers.ENTRY_POINT_GROUP else [])


def _public_app() -> typer.Typer:
    app = typer.Typer()

    @app.command("doctor")
    def _doctor() -> None:
        typer.echo("the public doctor ran")

    @app.command("run")
    def _run() -> None:
        typer.echo("the public run ran")

    return app


# --- 1-3 the loader -------------------------------------------------------------------

def test_the_loader_fails_open_and_records_each_skip(monkeypatch, caplog):
    _eps(monkeypatch,
         ("a-good", f"{_HERE}:good"),
         ("b-missing", "cairn.no_such_module:commands"),
         ("c-shape", f"{_HERE}:not_a_typer"),
         ("d-raises", f"{_HERE}:raises"))
    loaded = {p.name: p for p in providers.load_cli_providers()}
    assert [n for n, _ in loaded["a-good"].commands] == ["grp", "single", "doctor"]
    assert "ModuleNotFoundError" in loaded["b-missing"].error
    assert "not a typer.Typer or a list of (name, typer.Typer)" in loaded["c-shape"].error
    assert loaded["d-raises"].error == "RuntimeError: provider exploded"
    warned = " ".join(r.getMessage() for r in caplog.records if "skipped" in r.getMessage())
    assert all(f"'{n}'" in warned for n in ("b-missing", "c-shape", "d-raises")) and "a-good" not in warned


def test_a_bare_typer_mounts_under_the_entry_point_name(monkeypatch):
    _eps(monkeypatch, ("grp2", f"{_HERE}:as_typer"))
    app = _public_app()
    assert providers.mount_cli_providers(app) == ["grp2"]
    assert CliRunner().invoke(app, ["grp2", "two"]).output == "group two ran\n"


def test_the_public_cli_wins_a_name_and_the_rest_mount(monkeypatch):
    _eps(monkeypatch, ("a-good", f"{_HERE}:good"), ("b-again", f"{_HERE}:as_typer"))
    monkeypatch.setattr(sys.modules[_HERE], "as_typer", lambda: [("grp", _grp)])  # a second claim on `grp`
    app = _public_app()
    assert providers.mount_cli_providers(app) == ["grp", "single"]
    failures = providers.cli_mount_failures()
    assert [(f["name"], f["target"]) for f in failures] == [
        ("doctor", f"{_HERE}:good"), ("grp", f"{_HERE}:as_typer")]
    run = CliRunner().invoke
    assert run(app, ["doctor"]).output == "the public doctor ran\n"  # not the provider's
    assert run(app, ["grp", "one"]).output == "group one ran\n"
    # a single-command Typer is that command: `single <pack>`, not `single whatever <pack>`
    assert run(app, ["single", "mypack"]).output == "single ran mypack\n"


def test_no_provider_mounts_nothing(monkeypatch):
    _eps(monkeypatch)
    app = _public_app()
    assert providers.mount_cli_providers(app) == [] and providers.cli_mount_failures() == []
    assert providers.command_names(app) == {"doctor", "run"}


# --- 4 the public `cairn --help` ----------------------------------------------------

_CHILD = """
import json, sys
from importlib.metadata import EntryPoint
import cairn.cli.providers as P
eps = json.loads(sys.argv[1])
P.entry_points = lambda group: [EntryPoint(n, v, group) for n, v in eps] if group == P.ENTRY_POINT_GROUP else []
from typer.testing import CliRunner
from cairn.cli.main import app
for argv in json.loads(sys.argv[2]):
    r = CliRunner().invoke(app, argv)
    print(json.dumps({"code": r.exit_code, "out": r.output}))
"""


def run_cairn(eps: list[tuple[str, str]], *argvs: list[str]) -> list[dict]:
    """``cairn <argv>`` in a fresh interpreter whose ``cairn.cli`` group is exactly ``eps``."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p), "COLUMNS": "200"}
    proc = subprocess.run([sys.executable, "-c", _CHILD, json.dumps(eps), json.dumps(list(argvs))],
                          capture_output=True, text=True, env=env, timeout=120)
    assert proc.returncode == 0, proc.stderr[-3000:]
    return [json.loads(line) for line in proc.stdout.splitlines() if line.startswith("{")]


def _listed(help_out: str) -> set[str]:
    """The command names in a rich ``--help`` Commands panel."""
    return {line.strip("│ ").split(" ")[0] for line in help_out.splitlines() if line.startswith("│ ")}


def test_the_public_cli_lists_none_of_the_product_commands():
    (top, demo) = run_cairn([], ["--help"], ["admin", "demo", "--help"])
    assert top["code"] == 0
    listed = _listed(top["out"])
    assert {"run", "status", "decide", "pack", "admin"} <= listed, sorted(listed)
    assert not listed & set(PRODUCT_COMMANDS), sorted(listed & set(PRODUCT_COMMANDS))
    assert demo["code"] == 0 and "--showcase" not in demo["out"]  # the flag left with the platform


# --- 5 doctor -----------------------------------------------------------------------

def test_doctor_lists_the_providers_and_every_skip(monkeypatch):
    from cairn.cli.commands.doctor import check_cli_providers

    _eps(monkeypatch, ("a-good", f"{_HERE}:good"), ("b-missing", "cairn.no_such_module:commands"))
    providers.mount_cli_providers(_public_app())  # `doctor` is refused, as at startup
    rows = {r.name: r for r in check_cli_providers()}
    assert rows["cli_providers"].status == "ok"
    assert "a-good (grp, single, doctor)" in rows["cli_providers"].detail
    assert rows["cli_provider[b-missing]"].status == "warn"
    assert rows["cli_provider[doctor]"].status == "warn" and "refused" in rows["cli_provider[doctor]"].detail

    _eps(monkeypatch)
    providers.mount_cli_providers(_public_app())
    (only,) = check_cli_providers()
    assert (only.name, only.status) == ("cli_providers", "ok")
    assert "no cli providers installed" in only.detail


@pytest.mark.parametrize("bad", [[("grp",)], [("", _grp)], [("grp", "not a typer")], "grp", None])
def test_the_shape_rule_can_fail(bad):
    assert providers._as_commands("ep", bad) is None
