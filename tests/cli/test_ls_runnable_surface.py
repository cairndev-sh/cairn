"""`cairn ls` leads with what you RUN (ADR-0105 D3/D4, #2115).

`cairn ls` rendered a pack's code contributions and its asset directories and
nothing else, so the two questions a reader actually brings — what can I start
here, and what is that made of — were answered by two other commands and in
neither order. ADR-0105 makes the pipeline the runnable noun and the flow its
component, so the listing says so: pipelines first, flows beneath them, labelled.

The join is the interesting part. A pack's registered NAME can be
`scratch-dev` while its directory is `scratch_dev` (the platform's
`dataset-readiness` is shaped so), so a naive join on the raw string silently
renders an empty runnable surface for every such pack — green, and wrong.

The runnable surface is pinned on `examples/hello-cairn` (a pipeline and a data
flow) and on scratch packs (tests/cli/conftest.py), so it holds where only the
public core is installed (#2428).
"""
from __future__ import annotations

import json

import typer
from typer.testing import CliRunner

from cairn.cli.commands import ls as ls_mod

# Mounted on a LOCAL Typer app, not imported from `cairn.cli.main`: that module
# calls `load_env()` at import, which repopulates the storage keys
# `tests/conftest.py` neutralizes (#1199). Same pattern as test_ls_operators.py.
app = typer.Typer()
app.command(name="ls")(ls_mod.cmd)


@app.command(name="_noop")
def _noop() -> None:
    """Second command so Typer keeps the group shape — with one command it
    collapses to a bare command and `ls` would be parsed as the PACK arg."""


runner = CliRunner()


def _out(*args: str) -> str:
    r = runner.invoke(app, ["ls", *args])
    assert r.exit_code == 0, r.output
    return r.output


def test_pipelines_are_listed_before_the_flows_they_are_made_of(example_pack):
    out = _out("hello-cairn")
    assert "pipelines (what you run):" in out
    assert "flows (components" in out
    assert out.index("pipelines (what you run):") < out.index("flows (components")
    assert "hello-cairn/check-and-publish" in out      # the pipeline it ships
    assert "hello-cairn/engine-check" in out             # the flow it is made of


def test_a_pack_whose_directory_is_spelled_differently_still_shows_its_runnable_surface(
        dev_packs, scratch_pack):
    """The join bug this test exists for: `scratch-dev` the pack vs `scratch_dev`
    the directory it lives in."""
    dev_packs(scratch_pack("scratch_dev", "scratch-dev").parent)
    out = _out("scratch-dev")
    assert "scratch-dev/echo" in out


def test_a_flow_is_named_as_a_component_that_becomes_a_one_leg_pipeline(example_pack):
    out = _out("hello-cairn")
    assert "one-leg pipeline" in out


def test_the_json_shape_leads_with_pipelines_too(example_pack):
    payload = json.loads(_out("hello-cairn", "--json"))
    (pack,) = payload
    keys = list(pack)
    assert keys.index("pipelines") < keys.index("flows")
    assert {p["name"] for p in pack["pipelines"]} >= {"check-and-publish"}
    assert any(f["kind"] == "data" for f in pack["flows"])
    legs = {p["name"]: p["legs"] for p in pack["pipelines"]}
    assert legs["check-and-publish"] >= 1


def test_a_code_only_pack_says_so_instead_of_rendering_an_empty_heading(dev_packs, scratch_pack):
    """A pack that ships operators and no pipelines is a real, common thing;
    two empty headings would read as a bug."""
    assert ls_mod._render_text([]).startswith("No packs installed.")
    dev_packs(scratch_pack("code_only", "code-only", flow=False).parent)
    assert "runnable: nothing" in _out("code-only")
