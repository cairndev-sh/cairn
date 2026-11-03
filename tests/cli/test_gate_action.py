"""`actions/gate/action.yml`: the composite action that gates a job on `cairn run --check` (#350).

The action is a published artefact, so what a caller relies on is held here: its inputs and
outputs, that an input never becomes script text (a script-injection hole), that it installs
one pinned release, that third-party actions are pinned as this repository's workflows pin
them, and that the flags it passes are the CLI's own.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml
from typer.main import get_command

ROOT = Path(__file__).resolve().parents[2]
ACTION = ROOT / "actions" / "gate" / "action.yml"

#: The first release whose `run --check` exits 2 on a check that did not finish (#394).
FIRST_SAFE_RELEASE = (0, 4, 1)


def _action() -> dict:
    return yaml.safe_load(ACTION.read_text())


def _steps() -> list[dict]:
    return _action()["runs"]["steps"]


def _check_step() -> dict:
    return next(s for s in _steps() if s.get("id") == "check")


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def test_it_is_a_composite_action_under_a_subdirectory():
    assert _action()["runs"]["using"] == "composite"
    assert ACTION.parent == ROOT / "actions" / "gate"  # `uses: cairndev-sh/cairn/actions/gate@<ref>`
    assert not (ROOT / "action.yml").exists()  # the repository root stays clean


def test_the_inputs_and_outputs_exist():
    action = _action()
    assert set(action["inputs"]) == {
        "check", "set", "fail-on", "working-directory", "cairndev-version", "python-version"}
    assert action["inputs"]["check"]["required"] is True
    assert set(action["outputs"]) == {"verdict", "exit-code", "report"}
    for name, out in action["outputs"].items():  # each output is wired to the step that writes it
        assert out["value"] == "${{ steps.check.outputs." + name + " }}"
        assert f"{name}=" in _check_step()["run"], name


def test_no_run_step_interpolates_an_expression():
    for step in _steps():
        if "run" in step:
            assert "${{" not in step["run"], step["run"]
            assert step["shell"] == "bash"


def test_every_input_reaches_the_script_through_env():
    step = _check_step()
    for name in ("check", "set", "fail-on", "cairndev-version"):
        assert "${{ inputs." + name + " }}" in step["env"].values(), name
    # `$CHECK` is quoted wherever it is used, after `--` so that a flag-like name is a name.
    run = step["run"]
    assert '-- "$CHECK"' in run
    assert not re.search(r'(?<!")\$CHECK\b(?!")', run)


def test_it_installs_one_pinned_release_from_pypi():
    default = _action()["inputs"]["cairndev-version"]["default"]
    assert re.fullmatch(r"\d+\.\d+\.\d+", default), "the default must be one exact release"
    own = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    assert FIRST_SAFE_RELEASE <= _version(default) <= _version(own)
    run = _check_step()["run"]
    assert '--from "cairndev==$CAIRNDEV_VERSION"' in run
    assert "latest" not in run
    # the script refuses what is not one exact release before anything is installed
    assert run.index("CAIRNDEV_VERSION\" =~") < run.index("uv tool run")


def test_third_party_actions_are_pinned_as_the_workflows_pin_them():
    used = {}
    for workflow in (ROOT / ".github" / "workflows").glob("*.yml"):
        for ref in re.findall(r"uses: ([\w./-]+@[\w.]+)", workflow.read_text()):
            used.setdefault(ref.split("@")[0], set()).add(ref.split("@")[1])
    third_party = [s["uses"] for s in _steps() if "uses" in s]
    assert third_party, "the action installs through a pinned third-party action"
    for ref in third_party:
        name, _, version = ref.partition("@")
        assert version, f"{ref} is not pinned"
        assert version in used.get(name, set()), f"{ref}: the workflows use {used.get(name)}"
    setup = next(s for s in _steps() if s.get("uses", "").startswith("astral-sh/setup-uv@"))
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
    ci_setup = next(s for s in ci["jobs"]["public"]["steps"] if s.get("uses", "").startswith("astral-sh/setup-uv@"))
    assert setup["with"]["version"] == ci_setup["with"]["version"]  # the same pinned uv


@pytest.fixture(scope="module")
def run_options() -> set[str]:
    from cairn.cli.main import app

    run = get_command(app).commands["run"]
    return {opt for param in run.params for opt in getattr(param, "opts", [])}


def test_the_flags_the_script_passes_are_the_clis(run_options):
    run = _check_step()["run"]
    passed = set(re.findall(r"(?<![\w-])(--[a-z][\w-]*)", run.split("args=(", 1)[1].split("report=", 1)[0]))
    assert passed == {"--check", "--json", "--fail-on", "--set"}
    assert passed <= run_options


def test_the_script_runs_the_check_command(run_options):
    run = _check_step()["run"]
    assert "args=(run --check" in run
    assert "cairn \"${args[@]}\"" in run
    assert {"--check", "--fail-on", "--set", "--json"} <= run_options


def test_the_exit_code_is_passed_through():
    run = _check_step()["run"]
    assert 'exit "$rc"' in run
    assert "|| true" not in run
    assert "continue-on-error" not in _check_step()
    assert run.rstrip().endswith('exit "$rc"')


def test_the_guides_name_the_action():
    for page in (ROOT / "docs/site/src/content/docs/guides/gate-ci.md", ROOT / "examples/ci/README.md"):
        assert "cairndev-sh/cairn/actions/gate@" in page.read_text(), page.name
