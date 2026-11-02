"""`cairn gate` — the CI verdict + exit-code contract (#990, ADR-0076 D2).

The verdict extraction + exit-code mapping are pure functions of a run's output, so
most of this pins them directly (no engine/pack load). The end-to-end run (clean
fixture → PASS/0, dirty fixture → FAIL/1) is exercised by the pack's own operator
tests + verified live. A check that didn't run is proved here on the real engine,
since the engine's own error handling is what used to hide it.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import gate as gate_cli
from cairn.cli.commands.gate import (
    _classify,
    _exit_code,
    _extract_verdict,
    _load_inputs,
)
from cairn.cli.main import app


@pytest.mark.parametrize("raw,expected", [
    ("FAIL", "fail"), ("fail", "fail"), ("blocked", "fail"), ("reject", "fail"),
    ("WARN", "warn"), ("degraded", "warn"),
    ("pass", "pass"), ("PASS", "pass"), ("ready", "pass"), ("", "pass"),
])
def test_classify(raw, expected):
    assert _classify(raw) == expected


def test_extract_verdict_top_level_scalar():
    # dataset-readiness writes a top-level `verdict` scalar + blocking list.
    out = {"verdict": "fail", "blocking": ["leaked key in row 3"], "warnings": []}
    v = _extract_verdict(out)
    assert v["verdict"] == "fail"
    assert v["label"] == "FAIL"
    assert v["blocking"] == ["leaked key in row 3"]


def test_extract_verdict_outcome_channel_head_and_reasons():
    # the ADR-0075 outcome channel: a verdict head + cited non-ok checks.
    out = {"outcome": [
        {"name": "readiness", "value": "FAIL"},
        {"name": "PII rows", "value": 7, "severity": "warn"},
        {"name": "format conformant", "value": 98.1, "severity": "block"},
        {"name": "duplicates", "value": 0.0, "severity": "ok"},  # ok → not a reason
    ]}
    v = _extract_verdict(out)
    assert v["verdict"] == "fail"
    names = {r["name"] for r in v["reasons"]}
    assert names == {"PII rows", "format conformant"}  # the "ok" check is excluded


def test_extract_verdict_has_no_verdict_when_no_signal():
    assert _extract_verdict({})["verdict"] is None
    assert _extract_verdict({"something": 1})["verdict"] is None


def test_exit_code_fail_on_fail():
    assert _exit_code("pass", "fail") == 0
    assert _exit_code("warn", "fail") == 0   # a warning does NOT block by default
    assert _exit_code("fail", "fail") == 1


def test_exit_code_fail_on_warn():
    assert _exit_code("pass", "warn") == 0
    assert _exit_code("warn", "warn") == 1   # --fail-on warn makes a warning block
    assert _exit_code("fail", "warn") == 1


def test_load_inputs_merges_file_and_set(tmp_path):
    p = tmp_path / "in.json"
    p.write_text(json.dumps({"path": "data/a.jsonl", "format": "trl"}))
    merged = _load_inputs(str(p), ["format=axolotl", "extra=1"])
    assert merged == {"path": "data/a.jsonl", "format": "axolotl", "extra": "1"}  # --set overrides


def test_load_inputs_rejects_bad_set():
    with pytest.raises(ValueError):
        _load_inputs(None, ["missing-equals"])


def test_load_inputs_missing_file():
    with pytest.raises(ValueError):
        _load_inputs("/no/such/file.json", None)


# ── a gate never passes a check that didn't run ──────────────────────────────

_CHECK = """---
name: {name}
version: 0.1.0
module: ci
description: A test check.
flow:
  type: state_machine
  source: behavior_tree
  root:
    type: {root}
    children: {children}
  nodes: {{}}
tools:
  required: []
---

# Check
"""
_INPUT = "expectation: {kind: rows, roles: [{name: text, required: true}]}"


def _step(id_: str, *, data: str = "rows: [{text: a}]", output: str = "verdict: $state.verdict", extra: str = "") -> str:
    return (f"{{type: operation, operator: data/profile, id: {id_}, "
            f"input: {{{data}, {_INPUT}}}, output: {{{output}}}{extra}}}")


def _seq(*steps: str) -> str:
    return "{type: sequence, children: [" + ", ".join(steps) + "]}"


def _guard_always(step: str) -> str:
    return "{type: guard, condition: test_always, then: " + step + "}"


def _guard_never(step: str) -> str:
    return "{type: guard, condition: test_never, then: " + step + "}"


_MISSING = "source: no-such-file.jsonl"
_BLOCKS = "rows: [{other: a}]"
#: name -> (root, steps, expected exit, what the JSON must say)
_REAL_CHECKS = {
    "passes": ("sequence", [_step("a")], 0, '"label": "PASS"'),
    "blocks": ("sequence", [_step("a", data=_BLOCKS)], 1, '"label": "BLOCK"'),
    "its-operator-failed": ("sequence", [_step("a", data=_MISSING)], 2, "a_error: no such data file"),
    "a-later-step-passed": ("sequence", [_step("a", data=_MISSING), _step("b")], 2, "a_error: no such data file"),
    "no-verdict": ("sequence", [_step("a", output="verdict: $state.findings.verdict")], 2, "gave no usable verdict"),
    "not-a-verdict-word": ("sequence", [_step("a", output="detected_format: $state.verdict")], 2, "isn't a pass"),
    "paused-at-an-approval": ("sequence", [_step("a"), _step("b", extra=", approval: {required: true, reason: x}")],
                              2, "paused at an approval"),
    "a-measurement-named-error": ("sequence", [_step("a"), _step("b", output="record_count: $state.findings.mean_absolute_error")],
                                  0, '"label": "PASS"'),
    "selector-recovered": ("selector", [_step("a", data=_MISSING), _step("b")], 0, '"label": "PASS"'),
    "selector-all-failed": ("selector", [_step("a", data=_MISSING), _step("b", data=_MISSING)], 2, "b_error"),
    # a sequence carries on after a failed step, and a compound child's status can't be trusted,
    # so a selector with one counts every failed step, even where the fallback did recover
    "compound-failed-early": ("selector", [_seq(_step("a", data=_MISSING), _step("b")), _step("c")], 2, "a_error"),
    "compound-failed-last": ("selector", [_seq(_step("a"), _step("b", data=_MISSING)), _step("c")], 2, "b_error"),
    "compound-parallel": ("selector", ["{type: parallel, children: [" + _step("a", data=_MISSING) + ", " + _step("b") + "]}",
                                       _step("c")], 2, "a_error"),
    "fallback-skipped-then-a-pass": ("sequence", [_step("v"), "{type: selector, children: [" + _step("a", data=_MISSING) + ", "
                                                  + _guard_never(_step("b")) + "]}", _step("d")], 2, "a_error"),
    "selector-in-parallel": ("parallel", ["{type: selector, children: [" + _guard_always(_step("a", data=_MISSING)) + ", " + _step("b") + "]}",
                                          _seq(_step("z"), _step("y"), _step("w"))], 2, "a_error"),
}
_DRIVER = """
import contextlib, io, json, sys
import typer
from cairn.cli.commands import gate
from cairn.core.engine.condition_registry import condition

condition("test_always")(lambda state: "true")
condition("test_never")(lambda state: "false")
results = {}
for name in sys.argv[1:]:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        try:
            gate.cmd(template_ref=name, input_file=None, sets=None, profile=None,
                     fail_on="fail", json_out=True, mock=False)
        except typer.Exit as e:
            results[name] = [e.exit_code, out.getvalue()]
print("RESULTS " + json.dumps(results))
"""


@pytest.fixture(scope="module")
def real_checks(tmp_path_factory):
    """Each check on the real engine, nothing stubbed, in one fresh process."""
    cwd = tmp_path_factory.mktemp("gate")
    (cwd / "templates").mkdir()
    for name, (root, steps, _, _) in _REAL_CHECKS.items():
        (cwd / "templates" / f"{name}.md").write_text(
            _CHECK.format(name=name, root=root, children="[" + ", ".join(steps) + "]"))
    done = subprocess.run(
        [sys.executable, "-c", _DRIVER, *_REAL_CHECKS], cwd=cwd, capture_output=True, text=True, timeout=120,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},  # this code, not an installed copy
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.rsplit("RESULTS ", 1)[1])


@pytest.mark.parametrize("name", _REAL_CHECKS)
def test_a_real_check_exits_on_what_it_did(real_checks, name):
    code, body = real_checks[name]
    assert code == _REAL_CHECKS[name][2], body
    assert _REAL_CHECKS[name][3] in body


def _run_check(monkeypatch, output: dict, *flags: str, keys: frozenset = frozenset({"scan_error"})):
    async def _ran(*_a, **_kw):
        return output, set(keys)

    monkeypatch.setattr(gate_cli, "_run_readonly", _ran)
    return CliRunner().invoke(app, ["run", "some/check", "--check", "--json", *flags])


@pytest.mark.parametrize("output,expect", [
    ({}, "no usable verdict"),
    ({"something": 1}, "no usable verdict"),
    ({"verdict": ""}, "no usable verdict"),
    ({"outcome": [{"name": "verdict", "value": None}]}, "no usable verdict"),
    ({"verdict": "skipped"}, "isn't a pass"),
    ({"verdict": False}, "isn't a pass"),
    ({"outcome": [{"name": "gate", "value": False}, {"name": "x", "severity": "ok"}]}, "isn't a pass"),
    ({"outcome": [{"name": "verdict", "value": None}, {"name": "x", "severity": "ok"}]}, "no usable verdict"),
    ({"outcome": [{"name": "x", "severity": "skipped"}]}, "no usable verdict"),
    ({"outcome": [{"name": "x", "severity": "error"}]}, "no usable verdict"),
    ({"outcome": [{"name": "x", "severity": "critical"}]}, "no usable verdict"),
    ({"type": "budget_exceeded", "budget_exceeded": True}, "budget"),
    ({"verdict": "pass", "last_action_status": "failure"}, "last step ended failure"),
    ({"verdict": "pass", "last_action_status": "rejected"}, "last step ended rejected"),
    ({"verdict": "pass", "findings": {"scan_error": "boom"}}, "scan_error: boom"),
    ({"verdict": "pass", "findings": {"scan_error": ""}}, "scan_error"),  # str(e) of a bare assert
    ({"verdict": "warn", "findings": {"scan_error": "boom"}}, "scan_error: boom"),
    ({"verdict": "pass", "map_results": [{"map_id": "m", "item": 1, "error": "boom"}]}, "boom"),
    ({"verdict": "pass", "__interrupt__": [object()]}, "paused at an approval"),
])
def test_no_verdict_or_a_step_that_failed_is_an_error(monkeypatch, output, expect):
    r = _run_check(monkeypatch, output)
    assert r.exit_code == 2, r.output
    body = json.loads(r.stdout.splitlines()[-1])
    assert body["gate"] == "error" and expect in body["error"]


def test_a_fail_verdict_stands_even_when_a_step_also_failed(monkeypatch):
    r = _run_check(monkeypatch, {"verdict": "block", "findings": {"scan_error": "boom"},
                                 "last_action_status": "failure"})
    assert r.exit_code == 1, r.output
    assert json.loads(r.stdout.splitlines()[-1])["label"] == "BLOCK"


@pytest.mark.parametrize("outcome,code", [
    ([{"name": "PII rows", "value": 7, "severity": "block"}], 1),
    ([{"name": "format", "value": 1, "pass": False}], 1),
    ([{"name": "watch", "value": "PROPOSED", "severity": "warn"}], 0),
    ([{"name": "watch", "value": "OK", "severity": "ok"}], 0),
    ([{"name": "format", "value": 1, "pass": True}], 0),
])
def test_an_outcome_with_no_head_gates_on_its_grades(monkeypatch, outcome, code):
    assert _run_check(monkeypatch, {"outcome": outcome}).exit_code == code


@pytest.mark.parametrize("output", [
    {"verdict": "pass", "last_action_status": "success"},
    {"verdict": "OK", "findings": {"mean_absolute_error": 0.3, "job_error": ""}},  # not step failures
])
def test_a_clean_pass_exits_0(monkeypatch, output):
    assert _run_check(monkeypatch, output).exit_code == 0
