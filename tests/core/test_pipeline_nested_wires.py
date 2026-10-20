from __future__ import annotations

import pytest

from cairn.core.engine.pipeline import (
    PipelineDefError,
    PipelinePlan,
    parse_pipeline_def,
    plan_pipeline,
)


def _plan(text: str) -> PipelinePlan:
    return plan_pipeline(parse_pipeline_def(text, pack="nested", source="<test>"))


def test_unknown_nested_step_wire_is_rejected_at_plan_time():
    with pytest.raises(PipelineDefError, match="unknown step 'ghost'"):
        _plan("""
name: nested
kind: pipeline
steps:
  - id: source
    task: nested/echo
  - id: consumer
    task: nested/echo
    depends_on: [source]
    with:
      options:
        sources: ["$steps.ghost.out"]
""")


def test_unknown_nested_input_wire_is_rejected_at_plan_time():
    with pytest.raises(PipelineDefError, match="undeclared pipeline input 'missing'"):
        _plan("""
name: nested
kind: pipeline
inputs:
  seed: {type: str}
steps:
  - id: consumer
    task: nested/echo
    with:
      options:
        seed: "$input.missing"
""")


def test_nested_step_wire_requires_dependency_ancestry():
    with pytest.raises(PipelineDefError, match="does not depend on producer step 'producer'"):
        _plan("""
name: nested
kind: pipeline
steps:
  - id: root
    task: nested/echo
  - id: producer
    task: nested/echo
    depends_on: [root]
  - id: consumer
    task: nested/echo
    depends_on: [root]
    with:
      options:
        source: "$steps.producer.out"
""")


def test_valid_nested_wires_allow_transitive_dependencies():
    plan = _plan("""
name: nested
kind: pipeline
inputs:
  seed: {type: str}
steps:
  - id: source
    task: nested/echo
  - id: middle
    task: nested/echo
    depends_on: [source]
  - id: consumer
    task: nested/echo
    depends_on: [middle]
    with:
      options:
        sources:
          - "$steps.source.out"
          - "$input.seed"
""")

    consumer = next(step for step in plan.steps if step.id == "consumer")
    assert consumer.inputs["options"] == {
        "sources": ["$steps.source.out", "$input.seed"],
    }


def test_nested_step_field_literal_is_not_rewritten():
    with pytest.raises(PipelineDefError, match=r"ambiguous nested value.*\$steps\.train\.jsonl"):
        _plan("""
name: nested
kind: pipeline
steps:
  - id: train
    task: nested/echo
  - id: consumer
    task: nested/echo
    depends_on: [train]
    with:
      options:
        datasets: ["train.jsonl"]
""")


def test_top_level_bare_step_wire_remains_supported():
    plan = _plan("""
name: nested
kind: pipeline
steps:
  - id: source
    task: nested/echo
  - id: consumer
    task: nested/echo
    depends_on: [source]
    with: {value: source.output}
""")

    consumer = next(step for step in plan.steps if step.id == "consumer")
    assert consumer.inputs["value"] == "$steps.source.output"
