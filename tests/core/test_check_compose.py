"""The checks in ``scripts/check_compose.py`` (#371), and the workflow that runs it. The check itself
builds the engine image and runs in CI (``compose.yml``)."""
from __future__ import annotations

import importlib.util

import pytest
import yaml

from tests import _layout

_spec = importlib.util.spec_from_file_location("check_compose", _layout.REPO / "scripts" / "check_compose.py")
cc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cc)

PID = "pl_1"
OK = {"ok": True, "rows": 5}


def _rows(actor="admin", gates=("review", "promote"), finished=True):
    rows = [{"action": "pipeline.gate_approved", "target": f"{PID}/{g}", "actor": actor} for g in gates]
    return rows + ([{"action": "pipeline.finished", "target": PID}] if finished else [])


def test_a_log_with_the_decisions_and_the_end_passes():
    cc.check_audit(OK, _rows(), PID, actor="admin")
    cc.check_audit(OK, _rows(actor=None), PID)


@pytest.mark.parametrize("report", [{"ok": False, "rows": 5}, {"ok": True, "rows": 0}, {}])
def test_a_chain_that_does_not_verify_or_has_no_rows_fails(report):
    with pytest.raises(cc.ComposeError, match="didn't pass"):
        cc.check_audit(report, _rows(), PID)


@pytest.mark.parametrize("rows, why", [
    (_rows(gates=("review",)), "promote"),
    (_rows(finished=False), "pipeline.finished"),
    (_rows(actor="someone"), "actor"),
    ([{**r, "target": str(r["target"]).replace(PID, "pl_2")} for r in _rows()], "gate_approved"),
])
def test_a_log_missing_this_pipelines_rows_fails(rows, why):
    with pytest.raises(cc.ComposeError, match=why):
        cc.check_audit(OK, rows, PID, actor="admin")


@pytest.mark.parametrize("status", [None, 200, 403, 500])
def test_auth_on_must_refuse_a_call_with_no_token(status):
    with pytest.raises(cc.ComposeError, match="no token"):
        cc.check_refused_without_a_token(status)


def test_a_401_is_the_refusal():
    cc.check_refused_without_a_token(401)


def test_a_dot_env_would_make_the_proof_about_something_else(tmp_path):
    cc.check_clean_checkout(tmp_path)
    (tmp_path / ".env").write_text("OPENAI_API_KEY=sk-x\n", encoding="utf-8")
    with pytest.raises(cc.ComposeError, match="exists"):
        cc.check_clean_checkout(tmp_path)


def test_the_env_carries_no_key_and_no_cairn_setting(monkeypatch):
    for name in ("OPENAI_API_KEY", "CAIRN_PROFILE", "OBS_STORAGE", "POSTGRES_PASSWORD", "COMPOSE_PROFILES", "LLM_GATEWAY_URL"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("PATH", "/bin")
    env = cc.clean_env(CAIRN_PORT="8080")
    assert env["CAIRN_PORT"] == "8080" and env["PATH"] == "/bin"
    assert not {"OPENAI_API_KEY", "CAIRN_PROFILE", "OBS_STORAGE", "POSTGRES_PASSWORD", "COMPOSE_PROFILES",
                "LLM_GATEWAY_URL"} & env.keys()


def test_the_workflow_runs_nightly_and_on_what_the_stack_depends_on():
    workflow = yaml.safe_load((_layout.REPO / ".github" / "workflows" / "compose.yml").read_text(encoding="utf-8"))
    on = workflow["on"]
    assert on["schedule"] and "workflow_dispatch" in on
    for paths in (on["push"]["paths"], on["pull_request"]["paths"]):
        assert {"deploy/light/**", "Dockerfile.engine", "scripts/check_compose.py"} <= set(paths)
    steps = workflow["jobs"]["compose"]["steps"]
    assert any("scripts/check_compose.py" in step.get("run", "") for step in steps)
