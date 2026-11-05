"""The coverage badge, and the workflow that publishes it, gate nothing (#329)."""
from __future__ import annotations

import importlib.util
import json

import pytest
import yaml

from tests import _layout

_spec = importlib.util.spec_from_file_location("coverage_badge", _layout.REPO / "scripts" / "coverage_badge.py")
cb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cb)
WORKFLOW = _layout.REPO / ".github" / "workflows" / "coverage.yml"


@pytest.mark.parametrize("percent, message, colour", [
    (91.6, "92%", "16a34a"), (80.0, "80%", "16a34a"), (79.9, "80%", "ca8a04"), (60.0, "60%", "ca8a04"), (12.0, "12%", "dc2626"),
])
def test_the_badge_says_the_figure(percent, message, colour):
    assert cb.badge({"totals": {"percent_covered": percent}}) == {
        "schemaVersion": 1, "label": "coverage", "message": message, "color": colour}


def test_the_script_writes_the_endpoint_file(tmp_path):
    source = tmp_path / "coverage.json"
    source.write_text(json.dumps({"totals": {"percent_covered": 70.2}}), encoding="utf-8")
    target = tmp_path / "site" / "badge.json"
    assert cb.main(["x", str(source), str(target)]) == 0
    assert json.loads(target.read_text(encoding="utf-8"))["message"] == "70%"


def test_no_threshold_can_fail_the_run():
    text = WORKFLOW.read_text(encoding="utf-8") + (_layout.REPO / "scripts" / "coverage_badge.py").read_text(encoding="utf-8")
    assert "fail-under" not in text and "fail_under" not in text


def test_only_main_publishes():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    deploy = workflow["jobs"]["deploy"]
    assert "pull_request" in deploy["if"] and "refs/heads/main" in deploy["if"]
    assert workflow["permissions"] == {"contents": "read"}
    assert "permissions" not in workflow["jobs"]["measure"]
