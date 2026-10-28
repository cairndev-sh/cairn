"""#231 — `cairn pipeline explain` and `cairn pipeline show --plan` take `--preset`.

They resolve it the way the launch door does: the same lookup (a saved preset, else
the pack's), the same refusal for a key the pipeline no longer declares, and the same
layering (an `-i` key wins over the preset's). Each input shows the layer it came
from. The comparison test launches the pipeline through `launch_pipeline_def` with the
same preset and `-i`, and checks that explain shows the inputs the launch used.
"""
from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

from cairn.cli.commands import pipeline  # noqa: E402

_PIPELINE = """\
name: tune
kind: pipeline
description: a pipeline the presets fill
inputs:
  model: {type: str}
  epochs: {type: int, default: 1}
  lr: {type: str, default: "1e-4"}
steps:
  - {id: approve, gate: approve}
"""

_PRESETS = """\
pipeline: scratch-tune/tune
presets:
  - name: small
    description: the small model
    inputs: {model: qwen3-4b, epochs: 3}
  - name: stale
    inputs: {model: qwen3-4b, warmup: 10}
"""


@pytest.fixture
def tune_pack(dev_packs, scratch_pack, monkeypatch):
    """A pack with one pipeline and its shipped presets; saved presets in memory."""
    from cairn.core import workflow_config_store as W

    monkeypatch.setattr(W, "_default_store", W.InMemoryWorkflowConfigStore())
    d = scratch_pack("scratch_tune", "scratch-tune", flow=False)
    (d / "pipelines").mkdir()
    (d / "pipelines" / "tune.yaml").write_text(_PIPELINE)
    (d / "presets").mkdir()
    (d / "presets" / "tune.yaml").write_text(_PRESETS)
    dev_packs(d.parent)
    return d


def _explain(*args: str) -> dict:
    res = CliRunner().invoke(pipeline.app, ["explain", "scratch-tune/tune", "--json", *args])
    assert res.exit_code == 0, res.output
    return json.loads(res.output)


def test_explain_resolves_a_pack_preset_and_shows_each_layer(tune_pack):
    doc = _explain("--preset", "small", "-i", "epochs=5")
    inputs = doc["inputs"]
    assert inputs["model"]["value"] == "qwen3-4b"
    assert inputs["model"]["source"] == "preset 'small'"
    assert inputs["epochs"]["value"] == 5
    assert "overrides preset 'small'" in inputs["epochs"]["source"]
    assert inputs["lr"]["source"].startswith("default")
    assert doc["preset"] == "small"


def test_explain_text_marks_the_preset_layer(tune_pack):
    res = CliRunner().invoke(pipeline.app, ["explain", "scratch-tune/tune", "--preset", "small"])
    assert res.exit_code == 0, res.output
    assert "← preset 'small'" in res.output
    assert "cairn remote launch scratch-tune/tune --preset small" in res.output


def test_explain_shows_the_inputs_the_launch_uses(tune_pack, monkeypatch):
    """The acceptance test: explain's inputs equal the merged inputs the launch door
    hands to the bridge, for the same preset and `-i`."""
    from cairn.core import pull_worker_door as pw
    from cairn.core import workspace as WS
    from cairn.core.dispatch.pipeline_launch import launch_pipeline_def
    from cairn.core.engine.pipeline_resolve import resolve_pipeline_ref

    async def _never():
        return None

    async def _account(workspace_id=None, **_kw):
        return f"acct-of-{workspace_id}"

    monkeypatch.setattr(pw, "last_worker_checkin", _never)
    monkeypatch.setattr(WS, "workspace_account", _account)

    class _Bridge:
        def __init__(self):
            self.calls: list[dict] = []

        async def start_pipeline(self, plan, **kw):
            self.calls.append(kw)
            return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={})

    bridge = _Bridge()
    d = resolve_pipeline_ref("pipeline:scratch-tune/tune")
    asyncio.run(launch_pipeline_def(d, inputs={"epochs": "5"}, bridge=bridge, preset="small"))
    launched = bridge.calls[0]["inputs"]

    explained = {k: v["value"] for k, v in _explain("--preset", "small", "-i", "epochs=5")["inputs"].items()}
    assert explained == {k: launched[k] for k in explained}


def test_explain_refuses_an_unknown_preset_naming_what_exists(tune_pack):
    res = CliRunner().invoke(pipeline.app, ["explain", "scratch-tune/tune", "--preset", "nope"])
    assert res.exit_code == 2
    assert "no preset 'nope'" in res.output and "small" in res.output


def test_explain_refuses_a_stale_preset_key_as_the_launch_does(tune_pack):
    res = CliRunner().invoke(pipeline.app, ["explain", "scratch-tune/tune", "--preset", "stale"])
    assert res.exit_code == 2
    assert "warmup" in res.output


def test_show_plan_takes_the_preset_and_input_layers(tune_pack):
    res = CliRunner().invoke(pipeline.app, ["show", "scratch-tune/tune", "--plan",
                                            "--preset", "small", "-i", 'lr="3e-4"'])
    assert res.exit_code == 0, res.output
    lines = res.output.splitlines()
    assert any("model" in ln and "qwen3-4b" in ln and "← preset 'small'" in ln for ln in lines)
    assert any("lr" in ln and "3e-4" in ln and "← input (supplied)" in ln for ln in lines)
    assert "approve" in res.output  # the plan still follows


def test_show_refuses_a_preset_without_plan(tune_pack):
    res = CliRunner().invoke(pipeline.app, ["show", "scratch-tune/tune", "--preset", "small"])
    assert res.exit_code == 2
    assert "--plan" in res.output
