"""The Quickstart check runs the README's own commands (#330): what it reads out of the README, and
the workflow that runs it, are pinned here. The check itself needs an engine and runs in CI."""
from __future__ import annotations

import importlib.util
import re

import pytest
import yaml

from tests import _layout

_spec = importlib.util.spec_from_file_location("check_quickstart", _layout.REPO / "scripts" / "check_quickstart.py")
cq = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cq)

README = (_layout.REPO / "README.md").read_text(encoding="utf-8")
WORKFLOWS = _layout.REPO / ".github" / "workflows"


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def test_the_check_reads_the_commands_the_readme_tells_a_reader_to_type():
    launch, decide = cq.plan(README)
    assert "cairn setup --port 8471" in launch
    assert any(c.startswith("cairn run hello-cairn/profile-and-publish ") and "--environment sandbox" in c
               for c in launch)
    assert decide == ["cairn decide <pipeline_id> approve --step review",
                      "cairn decide <pipeline_id> approve --step promote",
                      "cairn status <pipeline_id> --results"]


def test_every_file_the_readme_downloads_is_in_this_checkout():
    launch, _ = cq.plan(README)
    urls = [c.split()[-1] for c in launch if c.startswith("curl -LsSfO ")]
    assert urls
    for url in urls:
        assert (_layout.REPO / url.removeprefix(cq.RAW)).is_file(), url


def test_a_command_the_check_cannot_run_fails_it_instead_of_being_skipped():
    drifted = README.replace("cairn setup --port 8471", "cairn setup --port 8471\ncairn doctor --fix", 1)
    with pytest.raises(cq.QuickstartError, match="cairn doctor --fix"):
        cq.plan(drifted)


@pytest.mark.parametrize("claim", ["`cairn admin audit verify`", "`-i min_rows=1000`"])
def test_a_claim_the_check_proves_cannot_leave_the_readme_unnoticed(claim):
    with pytest.raises(cq.QuickstartError, match=re.escape(claim)):
        cq.plan(README.replace(claim, "something else"))


def test_a_missing_decide_line_fails_the_check():
    with pytest.raises(cq.QuickstartError, match="step promote"):
        cq.plan(README.replace("cairn decide <pipeline_id> approve --step promote", "true"))


def test_the_environment_it_runs_in_has_no_model_key(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-x")
    monkeypatch.setenv("SOME_PROVIDER_API_KEY", "k")
    monkeypatch.setenv("CAIRN_STORAGE", "postgres://x")
    monkeypatch.setenv("KEEP_ME", "1")
    env = cq.sanitized_env(tmp_path)
    assert "OPENAI_API_KEY" not in env and "SOME_PROVIDER_API_KEY" not in env and "CAIRN_STORAGE" not in env
    assert env["KEEP_ME"] == "1" and env["HOME"] == str(tmp_path)


def test_the_quickstart_workflow_runs_the_check_on_the_same_events_as_ci():
    ci, quickstart = _workflow("ci.yml"), _workflow("quickstart.yml")
    assert quickstart["on"] == ci["on"]
    assert quickstart["name"] == "quickstart"
    steps = quickstart["jobs"]["quickstart"]["steps"]
    assert any("scripts/check_quickstart.py" in str(s.get("run", "")) for s in steps)
    sync = next(s["run"] for s in steps if "uv sync" in str(s.get("run", "")))
    assert "--locked" in sync and "--extra hello" in sync
    assert "OPENAI_API_KEY" not in quickstart["jobs"]["quickstart"].get("env", {})


def test_the_quickstart_workflow_pins_what_ci_pins():
    def uses(workflow: dict, job: str) -> dict:
        return {s["uses"]: s.get("with") for s in workflow["jobs"][job]["steps"] if "uses" in s}

    ci, quickstart = _workflow("ci.yml"), _workflow("quickstart.yml")
    ours, theirs = uses(quickstart, "quickstart"), uses(ci, "public")
    for action in ("actions/checkout@v6", "astral-sh/setup-uv@v7"):
        assert ours[action] == theirs[action], action
    assert quickstart["jobs"]["quickstart"]["env"]["UV_PYTHON"] == ci["jobs"]["public"]["env"]["UV_PYTHON"]


def test_the_required_checks_are_untouched():
    assert {"public", "public-e2e"} <= set(_workflow("ci.yml")["jobs"])
    assert "dco" in _workflow("dco.yml")["jobs"]


def test_the_readme_badge_points_at_the_quickstart_workflow():
    assert ("https://github.com/cairndev-sh/cairn/actions/workflows/quickstart.yml?query=branch%3Amain" in README)
    assert "actions/workflow/status/cairndev-sh/cairn/quickstart.yml?branch=main" in README
    assert (WORKFLOWS / "quickstart.yml").is_file()


def test_a_relative_cairn_path_still_works_from_the_scratch_directory(monkeypatch, tmp_path):
    """The workflow passes `.venv/bin/cairn`, and the commands run from a scratch directory."""
    monkeypatch.chdir(tmp_path)
    assert cq.absolute_command(".venv/bin/cairn") == str(tmp_path / ".venv" / "bin" / "cairn")
    assert cq.absolute_command("/opt/bin/cairn") == "/opt/bin/cairn"
    assert cq.absolute_command("cairn") == "cairn"
