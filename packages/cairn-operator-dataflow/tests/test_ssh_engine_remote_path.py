"""What a flow leg on a compute.ssh box receives, and how the box is reached again.

Three gaps found while preparing the first fine-tune run on a remote GPU box
with the control plane elsewhere:

* **Forwarded env.** The box got only the spec, the parent run, the live channel
  and ``MLFLOW_*``. A trainer that writes checkpoints to a bucket needs
  ``CAIRN_CHECKPOINT_BUCKET`` and a region too. The deployment now declares the
  extra NAMES in ``remote_env``; a name that looks like a credential is refused,
  because credentials stay on the box (an instance role), never in the job.
* **Dataset versions only the control plane can read.** A ``dataset:`` ref
  resolves to a pinned location (``…#sha256=…``). When that location is a
  control-plane path, the box cannot read it; the leg is refused before submit
  with what to do, instead of failing on the box with ``FileNotFoundError``.
* **Cancel and checkpoint pull-back credentials.** Both reconnected with
  ``SSH_PRIVATE_KEY`` from the control plane's env while submit used the
  connection's ``PRIVATE_KEY``; with the documented setup, cancel could not
  authenticate (and said nothing) and the job kept running on a paid box.

Hermetic: ``remote_exec`` seams are monkeypatched; no SSH, no network.
"""
from __future__ import annotations

import asyncio
import logging

import pytest

import cairn.core.connections as connections
from cairn.core.dataflow import DataflowSpec, EngineError, EngineJobHandle, StageRef
from cairn.core.operator.jobs import JobHandle

from operator_dataflow.engines_ssh import BuiltinSshEngine

import operator_remote.operators.exec as remote_exec


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("OBS_TRAIN_IMAGE", "OBS_REMOTE_EXEC_MODE", "OBS_REMOTE_ENV", "OBS_PROFILE",
                "CAIRN_REMOTE_ENV", "CAIRN_PROFILE", "SSH_PRIVATE_KEY", "MLFLOW_TRACKING_URI"):
        monkeypatch.delenv(var, raising=False)


def _bind_connection(monkeypatch, key: str = "CONN-KEY", **cfg):
    seen: list[str] = []

    async def fake_resolve(name, **_kw):
        seen.append(name)
        return type("RC", (), {
            "name": name,
            "config": {"host": "box", "user": "ubuntu", "port": 22, "known_hosts": "/tmp/kh", **cfg},
            "secrets": {"PRIVATE_KEY": key},
        })()
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)
    return seen


def _capture_submit(monkeypatch) -> dict:
    captured: dict = {}

    async def fake_submit(inp, ctx):
        captured["inp"] = inp
        return JobHandle(job_id="box:/jobs/x", backend="ssh",
                         meta={"host": "box", "user": "ubuntu", "port": 22,
                               "workdir": "/jobs/x", "known_hosts": "/tmp/kh"})
    monkeypatch.setattr(remote_exec, "_submit", fake_submit)
    return captured


def _submit(spec: DataflowSpec, target: str = "gpu"):
    return asyncio.run(BuiltinSshEngine(target).submit(spec, None))


def _spec(**config) -> DataflowSpec:
    return DataflowSpec(stages=[StageRef(stage="noop", config=config)])


# ── forwarded env (remote_env) ───────────────────────────────────────────────


def test_declared_names_are_forwarded_when_set(monkeypatch):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV",
                       "CAIRN_CHECKPOINT_BUCKET, CAIRN_CHECKPOINT_PREFIX AWS_DEFAULT_REGION")
    monkeypatch.setenv("CAIRN_CHECKPOINT_BUCKET", "ckpt-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("CAIRN_CHECKPOINT_PREFIX", raising=False)    # declared, unset: skipped
    monkeypatch.setenv("CAIRN_ARTIFACT_S3_BUCKET", "not-declared")  # set, undeclared: withheld

    _submit(_spec())
    env = captured["inp"].env
    assert env["CAIRN_CHECKPOINT_BUCKET"] == "ckpt-bucket"
    assert env["AWS_DEFAULT_REGION"] == "us-east-1"
    assert "CAIRN_CHECKPOINT_PREFIX" not in env
    assert "CAIRN_ARTIFACT_S3_BUCKET" not in env
    # the values ride the job's env file, never its command line
    assert "ckpt-bucket" not in captured["inp"].command


def test_nothing_extra_is_forwarded_when_nothing_is_declared(monkeypatch):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_CHECKPOINT_BUCKET", "ckpt-bucket")
    _submit(_spec())
    assert "CAIRN_CHECKPOINT_BUCKET" not in captured["inp"].env


def test_image_mode_names_the_forwarded_variables_without_values(monkeypatch):
    _bind_connection(monkeypatch, mode="image", image="ghcr.io/x/train:1")
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", "CAIRN_CHECKPOINT_BUCKET")
    monkeypatch.setenv("CAIRN_CHECKPOINT_BUCKET", "ckpt-bucket")
    _submit(_spec())
    cmd = captured["inp"].command
    assert "-e CAIRN_CHECKPOINT_BUCKET" in cmd and "ckpt-bucket" not in cmd
    assert captured["inp"].env["CAIRN_CHECKPOINT_BUCKET"] == "ckpt-bucket"


@pytest.mark.parametrize("name", [
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "HF_TOKEN", "OPENAI_API_KEY", "DB_PASSWORD", "my_secret_value",
])
def test_a_credential_looking_name_is_refused(monkeypatch, name):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", f"AWS_DEFAULT_REGION,{name}")
    monkeypatch.setenv(name, "s3cr3t-value")
    with pytest.raises(EngineError) as e:
        _submit(_spec())
    assert name in str(e.value) and "remote_env" in str(e.value)
    assert "s3cr3t-value" not in str(e.value)
    assert "inp" not in captured        # nothing was submitted


@pytest.mark.parametrize("name", ["A=B", "X;rm", "1ABC", "$HOME"])
def test_a_malformed_name_is_refused(monkeypatch, name):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", name)
    with pytest.raises(EngineError, match="remote_env"):
        _submit(_spec())
    assert "inp" not in captured


@pytest.mark.parametrize("name", ["CAIRN_DATAFLOW_SPEC", "CAIRN_PARENT_RUN", "CAIRN_RUN_ID"])
def test_a_name_the_engine_sets_is_refused(monkeypatch, name):
    _bind_connection(monkeypatch)
    _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", name)
    with pytest.raises(EngineError, match=name):
        _submit(_spec())


@pytest.mark.parametrize("mode", [{}, {"mode": "image", "image": "ghcr.io/x/train:1"}])
def test_the_mlflow_password_rides_the_runner_file_only(monkeypatch, mode):
    """The one credential the engine forwards itself (never through remote_env):
    it is in the job's env (written to the 0600 runner.sh), never on the
    command line — image mode names it with `-e` and no value."""
    _bind_connection(monkeypatch, **mode)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setenv("MLFLOW_TRACKING_PASSWORD", "mlflow-pw-value")
    _submit(_spec())
    assert captured["inp"].env["MLFLOW_TRACKING_PASSWORD"] == "mlflow-pw-value"
    assert "mlflow-pw-value" not in captured["inp"].command


# ── dataset versions the box cannot read ─────────────────────────────────────


def test_the_ssh_engine_asks_for_durable_dataset_copies():
    """ADR-0149: the leg executor resolves `dataset:` refs to the durable copy
    for an engine that declares it; the local engine does not."""
    from operator_dataflow.engines import BuiltinLocalEngine

    assert BuiltinSshEngine.needs_durable_inputs is True
    assert getattr(BuiltinLocalEngine(), "needs_durable_inputs", False) is False


@pytest.mark.parametrize("value", [
    "file:///tmp/cairn-dataflow-q4/splits/train.jsonl#sha256=sha256:abc",
    "/tmp/cairn-dataflow-q4/splits/train.jsonl#sha256=sha256:abc",
])
def test_a_control_plane_local_dataset_version_is_refused(monkeypatch, value):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    with pytest.raises(EngineError) as e:
        _submit(_spec(dataset_uri=value))
    msg = str(e.value)
    assert "dataset_uri" in msg and "durable" in msg and "CAIRN_ARTIFACT_STORE" in msg
    assert "inp" not in captured


def test_a_local_version_inside_a_list_is_refused(monkeypatch):
    _bind_connection(monkeypatch)
    _capture_submit(monkeypatch)
    with pytest.raises(EngineError, match="sources"):
        _submit(_spec(sources=["s3://b/a.jsonl#sha256=h1", "file:///srv/b.jsonl#sha256=h2"]))


def test_a_durable_dataset_version_and_a_plain_upload_still_submit(monkeypatch, tmp_path):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    upstream = tmp_path / "output.jsonl"
    upstream.write_text("{}\n")
    _submit(_spec(dataset_uri="s3://bucket/artifacts/h/train.jsonl#sha256=h",
                  upstream=str(upstream)))
    assert captured["inp"].upload == [str(upstream)]   # #1180: a leg hand-off still ships


# ── cancel and checkpoint pull-back use the connection's key ─────────────────


def _handle(workdir: str = ".cairn-jobs/j1") -> EngineJobHandle:
    return EngineJobHandle(job_id=f"box:{workdir}", engine="builtin-ssh", meta={
        "job_handle": {"job_id": f"box:{workdir}", "backend": "ssh",
                       "meta": {"host": "box", "user": "ubuntu", "port": 22,
                                "workdir": workdir, "known_hosts": "/tmp/kh"}},
        "exec_input": {"command": "cairn-dataflow-run", "env": {}, "fetch": ["result.json"]},
    })


class _Run:
    def __init__(self, stdout: str = "") -> None:
        self.stdout = stdout


class _Conn:
    def __init__(self) -> None:
        self.cmds: list[str] = []

    async def run(self, cmd, check=False):
        self.cmds.append(cmd)
        return _Run("")


def test_cancel_authenticates_with_the_connections_key(monkeypatch):
    seen = _bind_connection(monkeypatch, key="CONN-KEY")
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    conn, params_seen = _Conn(), []

    async def fake_connect(params):
        params_seen.append(params)
        return conn
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()

    asyncio.run(BuiltinSshEngine("gpu").cancel(_handle()))
    assert seen == ["gpu"]
    assert params_seen[0]["private_key"] == "CONN-KEY"
    assert params_seen[0]["host"] == "box"
    assert any('kill -TERM -"$p"' in c for c in conn.cmds)


def test_a_cancel_that_cannot_reach_the_box_says_so(monkeypatch, caplog):
    _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_connect(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()

    with caplog.at_level(logging.WARNING), pytest.raises(EngineError, match="may still be running"):
        asyncio.run(BuiltinSshEngine("gpu").cancel(_handle(".cairn-jobs/j9")))
    text = caplog.text
    assert ".cairn-jobs/j9" in text and "box" in text and "PermissionError" in text
    assert "CONN-KEY" not in text


def test_checkpoint_pull_back_authenticates_with_the_connections_key(monkeypatch, tmp_path):
    _bind_connection(monkeypatch, key="CONN-KEY")
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    params_seen: list[dict] = []

    class _Sftp:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def listdir(self, path): raise FileNotFoundError(path)

    class _SftpConn:
        def start_sftp_client(self): return _Sftp()

    async def fake_get_conn(params):
        params_seen.append(params)
        return _SftpConn()
    monkeypatch.setattr(remote_exec, "_get_conn", fake_get_conn)

    assert asyncio.run(BuiltinSshEngine("gpu").fetch_checkpoints(_handle(), tmp_path)) is False
    assert params_seen and params_seen[0]["private_key"] == "CONN-KEY"


def test_a_checkpoint_pull_back_that_cannot_reach_the_box_says_so(monkeypatch, tmp_path, caplog):
    seen = _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_get_conn(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_get_conn", fake_get_conn)

    h = _handle()
    h.meta["job_handle"]["meta"]["connection"] = "gpu-at-submit"
    with caplog.at_level(logging.WARNING):
        assert asyncio.run(BuiltinSshEngine("gpu-now").fetch_checkpoints(h, tmp_path)) is False
    assert "PermissionError" in caplog.text and "CONN-KEY" not in caplog.text
    # the warning names the connection actually used: the handle's, not the target
    assert seen == ["gpu-at-submit"]
    assert "'gpu-at-submit'" in caplog.text and "gpu-now" not in caplog.text



# ── the persisted handle carries no env value (StepState.handle, GET /pipelines/{id}) ──


def test_the_persisted_handle_records_no_env_value(monkeypatch):
    """The handle's meta is persisted verbatim on the pipeline step and served by
    GET /pipelines/{id}: it must name what was forwarded, never the values."""
    import json as _json

    import operator_dataflow.engines_ssh as mod

    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    monkeypatch.setattr(mod, "live_channel_env_for",
                        lambda run, node: {"CAIRN_ENGINE_TOKEN": "engine-tok-value"})
    monkeypatch.setenv("CAIRN_REMOTE_ENV", "CAIRN_CHECKPOINT_BUCKET")
    monkeypatch.setenv("CAIRN_CHECKPOINT_BUCKET", "ckpt-bucket-value")
    monkeypatch.setenv("MLFLOW_TRACKING_PASSWORD", "mlflow-pw-value")
    handle = _submit(DataflowSpec(stages=[StageRef(stage="noop")], parent_run="run-p"))
    # the job itself got the values …
    env = captured["inp"].env
    assert env["CAIRN_ENGINE_TOKEN"] == "engine-tok-value"
    assert env["MLFLOW_TRACKING_PASSWORD"] == "mlflow-pw-value"
    # … the persisted handle does not
    stored = _json.dumps(handle.model_dump())
    for value in ("engine-tok-value", "mlflow-pw-value", "ckpt-bucket-value"):
        assert value not in stored, value
    # and the handle still drives poll/cancel/result after a restart
    _, _, inp = BuiltinSshEngine("gpu")._rebuild(handle)
    assert inp.env == {} and inp.fetch


# ── cancel follows the handle's own connection and reports failure ───────────


def test_cancel_prefers_the_connection_recorded_in_the_handle(monkeypatch):
    seen = _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_connect(params):
        return _Conn()
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    h = _handle()
    h.meta["job_handle"]["meta"]["connection"] = "gpu-at-submit"
    asyncio.run(BuiltinSshEngine("gpu-now").cancel(h))
    assert seen == ["gpu-at-submit"]


def test_a_failed_cancel_is_not_reported_as_sent(monkeypatch):
    from cairn.core.dispatch.leg_executor import cancel_recorded_job

    _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_connect(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    h = _handle()
    persisted = {"job_id": h.job_id, "engine": h.engine, "meta": h.meta}
    assert asyncio.run(cancel_recorded_job(BuiltinSshEngine("gpu"), persisted)) is False


# ── a malformed remote_env entry never echoes what follows the name ──────────


@pytest.mark.parametrize("entry,leaked", [
    ("HF_TOKEN=hf_abcSECRETVALUE", "HF_TOKEN"),
    ("MY_VAR:hf_abcSECRETVALUE", "MY_VAR"),
    ("=hf_abcSECRETVALUE", "hf_abc"),
    # an all-upper-case base32 seed with padding reads exactly like NAME=…
    ("JBSWY3DPEHPK3PXP====", "JBSWY3DPEHPK3PXP"),
    ("AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI", "AKIAIOSFODNN7EXAMPLE"),
])
def test_a_malformed_entry_is_reported_by_position_only(monkeypatch, entry, leaked):
    _bind_connection(monkeypatch)
    _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", f"AWS_DEFAULT_REGION {entry}")
    with pytest.raises(EngineError) as e:
        _submit(_spec())
    assert "entry 2 is not an environment variable name" in str(e.value)
    assert leaked not in str(e.value)
    assert "SECRETVALUE" not in str(e.value)


@pytest.mark.parametrize("pasted", ["wJalrXUtn/K7MDENG+bPxRfi", "wJalrXUtnFEMIK7MDENG=",
                                    "wJalrXUtn:K7MDENGbPxRfi"])
def test_a_pasted_value_is_reported_by_position_only(monkeypatch, pasted):
    """A secret pasted where a name belongs: no part of it is echoed, not even
    the name-like run of characters it starts with."""
    _bind_connection(monkeypatch)
    _capture_submit(monkeypatch)
    monkeypatch.setenv("CAIRN_REMOTE_ENV", f"AWS_DEFAULT_REGION,{pasted}")
    with pytest.raises(EngineError) as e:
        _submit(_spec())
    msg = str(e.value)
    assert "entry 2 is not an environment variable name" in msg
    for part in ("wJalrXUtn", "K7MDENG", "bPxRfi", "FEMI"):
        assert part not in msg


# ── control-plane-only versions anywhere in the config ───────────────────────


@pytest.mark.parametrize("config", [
    {"inputs": {"train": "file:///srv/a.jsonl#sha256=h"}},
    {"inputs": [{"uri": "/srv/a.jsonl#sha256=h"}]},
    {"dataset_uri": "file:/srv/a.jsonl#sha256=h"},
])
def test_a_local_version_nested_or_single_slash_is_refused(monkeypatch, config):
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    with pytest.raises(EngineError, match="durable"):
        _submit(_spec(**config))
    assert "inp" not in captured


def test_a_local_version_in_the_spec_config_is_refused(monkeypatch):
    _bind_connection(monkeypatch)
    _capture_submit(monkeypatch)
    spec = DataflowSpec(stages=[StageRef(stage="noop")],
                        config={"dataset_uri": "file:///srv/a.jsonl#sha256=h"})
    with pytest.raises(EngineError, match="durable"):
        _submit(spec)


def test_a_handle_stored_before_adr_0149_still_rebuilds():
    """Storage is not rewritten: a stored pre-ADR-0149 handle, env values and all,
    still drives poll/cancel/result, and so does its masked API form."""
    from cairn.core.triggers.routers.pipelines import HIDDEN, public_handle

    old = _handle().model_dump()
    old["meta"]["exec_input"]["env"] = {"CAIRN_ENGINE_TOKEN": "engine-tok-value"}
    _, jh, inp = BuiltinSshEngine("gpu")._rebuild(EngineJobHandle(**old))
    assert jh.meta["workdir"] == ".cairn-jobs/j1" and inp.fetch == ["result.json"]
    shown = public_handle(old)
    assert shown["meta"]["exec_input"]["env"] == {"CAIRN_ENGINE_TOKEN": HIDDEN}
    _, _, inp2 = BuiltinSshEngine("gpu")._rebuild(EngineJobHandle(**shown))
    assert inp2.fetch == ["result.json"]
