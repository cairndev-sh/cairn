"""A pipeline record read through the API never shows a job's env values (ADR-0149).

Engine job handles written before ADR-0149 can still hold ``meta.exec_input.env``:
the job-channel token, ``MLFLOW_TRACKING_PASSWORD``, forwarded settings. Storage is
not rewritten; the record is masked where it leaves the control plane
(``GET /pipelines/{id}``). Names stay readable; values are replaced by a marker.
A handle written since ADR-0149 has no values and reads unchanged.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner

OLD = {
    "job_id": "box:.cairn-jobs/pl-old-train", "engine": "builtin-ssh",
    "meta": {
        "job_handle": {"job_id": "box:.cairn-jobs/pl-old-train", "backend": "ssh",
                       "meta": {"host": "box", "workdir": ".cairn-jobs/pl-old-train"}},
        "exec_input": {"command": "cairn-dataflow-run", "upload": [], "fetch": ["result.json"],
                       "workdir": "",
                       "env": {"CAIRN_DATAFLOW_SPEC": "{\"stages\": []}",
                               "CAIRN_ENGINE_TOKEN": "engine-tok-value",
                               "MLFLOW_TRACKING_PASSWORD": "mlflow-pw-value",
                               "CAIRN_CHECKPOINT_BUCKET": "ckpt-bucket-value"}},
        "mode": "venv", "image": "",
    },
}
NEW = {
    "job_id": "box:.cairn-jobs/pl-new-train", "engine": "builtin-ssh",
    "meta": {
        "job_handle": {"job_id": "box:.cairn-jobs/pl-new-train", "backend": "ssh",
                       "meta": {"host": "box", "workdir": ".cairn-jobs/pl-new-train"}},
        "exec_input": {"command": "cairn-dataflow-run", "upload": [], "fetch": ["result.json"],
                       "workdir": ""},
        "env_names": ["CAIRN_DATAFLOW_SPEC", "CAIRN_ENGINE_TOKEN"], "mode": "venv", "image": "",
    },
}


@pytest.fixture
def store(monkeypatch):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    s = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: s)
    asyncio.run(s.put(PipelineRecord(
        pipeline_id="pl_env", pipeline_name="p", status="running", workspace_id="default",
        step_state={"old": StepState(status="running", handle=OLD),
                    "new": StepState(status="running", handle=NEW),
                    "gate": StepState(status="awaiting_approval")})))
    return s


def test_an_old_handle_is_served_without_its_env_values(store):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.get("/pipelines/pl_env")
    assert r.status_code == 200, r.text
    body = r.text
    for value in ("engine-tok-value", "mlflow-pw-value", "ckpt-bucket-value", "\\\"stages\\\""):
        assert value not in body, value
    env = r.json()["step_state"]["old"]["handle"]["meta"]["exec_input"]["env"]
    assert sorted(env) == ["CAIRN_CHECKPOINT_BUCKET", "CAIRN_DATAFLOW_SPEC",
                           "CAIRN_ENGINE_TOKEN", "MLFLOW_TRACKING_PASSWORD"]   # names stay
    assert set(env.values()) == {"[hidden]"}
    # storage is not rewritten: the stored record still holds what it held
    stored = asyncio.run(store.get("pl_env")).step_state["old"].handle
    assert stored["meta"]["exec_input"]["env"]["CAIRN_ENGINE_TOKEN"] == "engine-tok-value"


def test_a_new_handle_and_a_step_without_one_read_unchanged(store):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        out = c.get("/pipelines/pl_env").json()["step_state"]
    assert out["new"]["handle"] == NEW
    assert out["gate"]["handle"] is None
