"""#197 — a `list` input takes a comma-separated string at the launch door.

`-i report_to=mlflow` was refused for an input declared `type: list`: the value had
to be shell-quoted JSON. The door's typing function (`coerce_declared_inputs`, the
one place the CLI, the console, cron and presets converge) now splits a plain
string on commas, and a value that parses as JSON keeps the JSON rule.
"""
from __future__ import annotations

import pytest

from cairn.core.dispatch.pipeline_launch import InputCoercionError, coerce_declared_inputs
from cairn.core.engine.pipeline import parse_pipeline_def

_DEF = """\
name: tracked
kind: pipeline
description: a list input
inputs:
  report_to: {type: list, default: [mlflow]}
  target_modules: {type: list, default: [q_proj]}
  epochs: {type: int, default: 1}
steps:
  - id: run
    template: echo
"""


@pytest.fixture
def d():
    return parse_pipeline_def(_DEF, pack="scratch")


@pytest.mark.parametrize("raw, typed", [
    ("mlflow", ["mlflow"]),
    ("mlflow,wandb", ["mlflow", "wandb"]),
    (" mlflow , wandb ,, ", ["mlflow", "wandb"]),
    ("[]", []),                               # an empty list is JSON
    ('["mlflow"]', ["mlflow"]),               # the JSON form is unchanged
    ('["mlflow", "wandb"]', ["mlflow", "wandb"]),
])
def test_a_list_input_takes_a_comma_separated_string(d, raw, typed):
    assert coerce_declared_inputs(d, {"report_to": raw})["report_to"] == typed


def test_a_value_that_parses_as_json_keeps_the_json_rule(d):
    for raw in ('"mlflow"', "1", "true", "{}"):
        with pytest.raises(InputCoercionError, match="declared list"):
            coerce_declared_inputs(d, {"report_to": raw})


@pytest.mark.parametrize("raw", ["", "   ", " , ,"])
def test_a_string_with_no_items_stays_refused(d, raw):
    """`-i report_to=` was refused before #197 and still is: it is not `[]`."""
    with pytest.raises(InputCoercionError, match="report_to"):
        coerce_declared_inputs(d, {"report_to": raw})


def test_broken_json_is_refused_not_split(d):
    with pytest.raises(InputCoercionError, match="report_to"):
        coerce_declared_inputs(d, {"report_to": '["mlflow",'})


def test_typed_values_and_other_types_are_untouched(d):
    out = coerce_declared_inputs(d, {"report_to": ["wandb"], "epochs": "3",
                                     "target_modules": "q_proj,v_proj"})
    assert out == {"report_to": ["wandb"], "epochs": 3,
                   "target_modules": ["q_proj", "v_proj"]}


# ── through the launch door: a REQUIRED list input ───────────────────────────

_REQUIRED = """\
name: needs-trackers
kind: pipeline
inputs:
  report_to: {type: list}
steps:
  - {id: approve, gate: approve}
"""


@pytest.fixture()
def installed(tmp_path, monkeypatch):
    import cairn.core.engine.pipeline_resolve as R
    from cairn.core import pull_worker_door as pw

    pdir = tmp_path / "listpack" / "pipelines"
    pdir.mkdir(parents=True)
    (pdir / "needs-trackers.yaml").write_text(_REQUIRED)
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(pdir)])

    async def _never():
        return None

    monkeypatch.setattr(pw, "last_worker_checkin", _never)


class _Bridge:
    def __init__(self):
        self.calls: list[dict] = []

    async def start_pipeline(self, plan, **kw):
        from types import SimpleNamespace

        self.calls.append({"plan": plan, **kw})
        return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={})


async def test_an_empty_value_does_not_satisfy_a_required_list_input(installed):
    from cairn.core.dispatch.pipeline_launch import launch_pipeline

    bridge = _Bridge()
    with pytest.raises(InputCoercionError, match="report_to"):
        await launch_pipeline("listpack/needs-trackers", inputs={"report_to": ""}, bridge=bridge)
    assert bridge.calls == []
    await launch_pipeline("listpack/needs-trackers", inputs={"report_to": "mlflow,wandb"}, bridge=bridge)
    assert bridge.calls[0]["inputs"]["report_to"] == ["mlflow", "wandb"]


# ── explain refuses exactly what the launch refuses ──────────────────────────

_DEFAULTS = """\
name: typed
kind: pipeline
inputs:
  report_to: {type: list, default: [mlflow]}
  epochs: {type: int, default: "many"}
steps:
  - {id: approve, gate: approve}
"""


def _coercion_problems(doc: dict) -> list[str]:
    return [p for p in doc["problems"] if p.startswith("launch would refuse the inputs")]


async def test_explain_reports_a_declared_default_the_launch_refuses():
    from cairn.core.dispatch.pipeline_preview import apply_input_defaults
    from cairn.core.engine.pipeline_explain import explain_pipeline

    td = parse_pipeline_def(_DEFAULTS, pack="scratch")
    with pytest.raises(InputCoercionError, match="epochs"):
        apply_input_defaults(td, {})                       # what the launch does
    doc = await explain_pipeline(td, workspace_variables={})
    (problem,) = _coercion_problems(doc)
    assert "epochs" in problem


async def test_explain_ignores_a_preset_value_the_caller_overrides(d):
    """The launch merges the caller's inputs over the preset before typing, so a bad
    preset value that is overridden never reaches the coercion; explain agrees."""
    from cairn.core.engine.pipeline_explain import explain_pipeline

    doc = await explain_pipeline(d, inputs={"report_to": "wandb"},
                                 preset_inputs={"report_to": '["broken'},
                                 preset_name="p", workspace_variables={})
    assert _coercion_problems(doc) == []
    assert doc["inputs"]["report_to"]["value"] == ["wandb"]
    doc = await explain_pipeline(d, preset_inputs={"report_to": '["broken'},
                                 preset_name="p", workspace_variables={})
    assert len(_coercion_problems(doc)) == 1               # not overridden: refused
