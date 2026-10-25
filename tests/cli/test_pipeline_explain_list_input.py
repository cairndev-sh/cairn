"""#197 — `cairn pipeline explain -i` types a `list` input as the launch door does.

`-i report_to=mlflow,wandb` reaches the door as a string; the door splits it on
commas. `explain` shows the value the launch will use, by calling the door's own
typing function, so the two cannot disagree.
"""
from __future__ import annotations

import json

from typer.testing import CliRunner

from cairn.cli.commands import pipeline

_PIPELINE = """\
name: tracked
kind: pipeline
description: a pipeline with a list input
inputs:
  report_to: {type: list, default: [mlflow]}
steps:
  - id: run
    template: echo
"""


def _explain(*inputs: str) -> dict:
    args = ["explain", "scratch-list/tracked", "--json"]
    for i in inputs:
        args += ["-i", i]
    res = CliRunner().invoke(pipeline.app, args)
    assert res.exit_code == 0, res.output
    return json.loads(res.output)


def test_explain_types_a_comma_separated_list_input(dev_packs, scratch_pack):
    d = scratch_pack("scratch_list", "scratch-list")
    (d / "pipelines").mkdir()
    (d / "pipelines" / "tracked.yaml").write_text(_PIPELINE)
    dev_packs(d.parent)

    assert _explain("report_to=mlflow")["inputs"]["report_to"]["value"] == ["mlflow"]
    assert _explain("report_to=mlflow,wandb")["inputs"]["report_to"]["value"] == [
        "mlflow", "wandb"]
    assert _explain('report_to=["mlflow"]')["inputs"]["report_to"]["value"] == ["mlflow"]
