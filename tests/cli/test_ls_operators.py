"""`cairn ls operators` — the kernel ↔ operator ↔ usage map (#1543).

Three namespaces name the same unit of work: an OPERATOR (`dataset/scan_all`)
is a registered class, a KERNEL (`scan.all`) is a `cairn.dataflow_stages` entry
point wrapping one, and a shipped flow/pipeline names whichever its runtime
speaks. None of the three could see the others, so ADR-0092's question — "what
runs `scan.all`?" — had no answer short of grepping all three by hand.

The acceptance criterion is exactly that question, so it is the first test.

NB the command is mounted on a LOCAL Typer app rather than imported from
``cairn.cli.main`` (the pattern ``tests/cli/test_connect.py`` already uses).
``cairn/cli/main.py`` calls ``load_env()`` at module scope, so importing it
re-runs ``load_dotenv`` against the repo ``.env`` — which repopulates the
storage-family keys ``tests/conftest.py`` deliberately neutralizes (#1199) and
redirects later tests at a live Postgres. Mounting the one command under test
keeps this file from arming that landmine for the whole suite.
"""
from __future__ import annotations

import json
from importlib.metadata import packages_distributions

import typer
from typer.testing import CliRunner

from cairn.cli.commands import ls as ls_mod

app = typer.Typer()
app.command(name="ls")(ls_mod.cmd)


@app.command(name="_noop")
def _noop() -> None:
    """Second command so Typer keeps the group shape — with one command it
    collapses to a bare command and `ls` would be parsed as the PACK arg."""


runner = CliRunner()


def _rows() -> list[dict]:
    res = runner.invoke(app, ["ls", "operators", "--json"])
    assert res.exit_code == 0, res.output
    return json.loads(res.stdout)


def test_it_answers_what_runs_a_kernel(example_pack):
    """THE acceptance criterion (#1543 c), on what every install has: the builtin
    `noop` kernel resolves to the wheel that ships it and the files that name it.
    The name-matched operator half is pinned on an official pack in
    tests/packs/test_cli_on_shipped_packs.py."""
    row = next(r for r in _rows() if "noop" in r["kernels"])
    # the distribution that ships the builtin dataflow pack, whatever this layout names it
    # (`cairndev` since the fold, ADR-0137; its own wheel before)
    assert [row["distribution"]] == packages_distributions()["operator_dataflow"]
    assert any(u.startswith("hello-cairn/") for u in row["used_by"]), row


def test_every_registered_operator_gets_a_row():
    from cairn.core.operator import default_registry, load_all_operators

    load_all_operators(strict=False)
    registered = set(default_registry.all())
    listed = {r["operator"] for r in _rows() if r["operator"]}
    assert registered == listed


def test_a_kernel_with_no_name_matched_operator_still_prints():
    """Correlation is by wheel + name, not a declared link, so it can miss.
    A kernel the engine will happily accept must never be invisible here —
    silence would read as "that name does not exist"."""
    res = runner.invoke(app, ["ls", "operators"])
    assert res.exit_code == 0, res.output
    section = res.stdout.split("kernels with no name-matched operator", 1)
    assert len(section) == 2, res.stdout
    # the builtin `noop` wraps a callable that is not a registered operator
    assert "noop" in section[1]


def test_unused_shows_only_operators_no_shipped_file_references(example_pack):
    res = runner.invoke(app, ["ls", "operators", "--unused", "--json"])
    assert res.exit_code == 0, res.output
    rows = json.loads(res.stdout)
    assert rows, "expected at least one unreferenced operator"
    assert all(r["used_by"] == [] for r in rows)
    # ...and it is a strict subset: something IS referenced.
    assert len(rows) < len(_rows())


def test_unused_outside_the_operators_view_is_rejected_not_ignored():
    """A flag that silently does nothing is how dead config starts."""
    res = runner.invoke(app, ["ls", "--unused"])
    assert res.exit_code == 2
    assert "operators" in res.output


def test_the_pack_listing_is_unchanged_by_the_new_view(example_pack):
    """`operators` is a reserved word in the positional slot; every other value
    still means "restrict to this pack"."""
    res = runner.invoke(app, ["ls", "hello-cairn"])
    assert res.exit_code == 0, res.output
    assert res.stdout.startswith("hello-cairn (")
