"""ADR-0104 D1 (#2071) — the IMAGE is the unit of remote work.

The proven SSH path ran ``cairn-dataflow-run`` from a virtualenv a human
pip-installed on the GPU box; it drifted since August and the fine-tune flow
stopped resolving on the box (#2069). D1 replaces the box install with the
deployment's container image: the box carries drivers + a container
runtime and nothing of cairn's.

These tests pin BOTH modes at the same seam the existing suite uses
(monkeypatched ``remote_exec._submit``), because the acceptance for D1 is
"the exact remote command line", not "it ran":

* ``image`` — the docker command shape, the named tag, the forwarded env
  (including the #2025 live-channel vars, without which the curve never
  streams back), and the tag recorded on the run for provenance.
* ``venv`` — byte-for-byte what it submitted before D1, so a box that still
  runs from its virtualenv is untouched (venv is the default, ADR-0150).
"""
from __future__ import annotations

import asyncio
import json

import pytest

import cairn.core.connections as connections
from cairn.core.dataflow import DataflowSpec, StageRef
from cairn.core.operator.jobs import JobHandle

from cairn.core.dataflow import EngineError

from operator_dataflow.engines_ssh import (
    CONNECTION_IMAGE_REQUIRED,
    IMAGE_REQUIRED,
    BuiltinSshEngine,
    resolve_exec_mode,
)

import operator_remote.operators.exec as remote_exec


def _spec(**kw) -> DataflowSpec:
    return DataflowSpec(stages=[StageRef(stage="noop")], **kw)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """No ambient environment leaks into the mode decision."""
    for var in ("OBS_TRAIN_IMAGE", "OBS_REMOTE_EXEC_MODE", "OBS_DATAFLOW_IMAGE_CMD",
                "OBS_DATAFLOW_IMAGE_GPUS", "OBS_DATAFLOW_REMOTE_CMD", "OBS_PROFILE",
                "MLFLOW_TRACKING_URI"):
        monkeypatch.delenv(var, raising=False)


def _bind_connection(monkeypatch, **cfg):
    async def fake_resolve(name, **_kw):
        return type("RC", (), {
            "config": {"provider": "ssh", "host": "box", "user": "ubuntu",
                       "port": 22, "known_hosts": "/tmp/kh", **cfg},
            "secrets": {"PRIVATE_KEY": "k"},
        })()
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)


def _capture_submit(monkeypatch) -> dict:
    captured: dict = {}

    async def fake_submit(inp, ctx):
        captured["inp"], captured["ctx"] = inp, ctx
        return JobHandle(job_id="box:/jobs/x", backend="ssh",
                         meta={"host": "box", "user": "ubuntu", "port": 22,
                               "workdir": "/jobs/x", "known_hosts": "/tmp/kh"})
    monkeypatch.setattr(remote_exec, "_submit", fake_submit)
    return captured


def _submit(engine: BuiltinSshEngine, spec: DataflowSpec):
    ctx = type("C", (), {"pipeline_id": "run-1", "node_name": "finetune"})()
    return asyncio.run(engine.submit(spec, ctx))


# ── mode selection ───────────────────────────────────────────────────


def test_mode_is_venv_when_nothing_names_an_image(monkeypatch):
    """No image anywhere = the box virtualenv, the default. D1 must not flip a
    deployment to containers behind its operator's back."""
    mode, image, _gpus = resolve_exec_mode({"host": "box"})
    assert mode == "venv" and image == ""


def test_mode_is_image_when_the_connection_names_one(monkeypatch):
    mode, image, _gpus = resolve_exec_mode({"host": "box", "image": "ghcr.io/x/y:1"})
    assert mode == "image" and image == "ghcr.io/x/y:1"


def test_mode_is_image_when_obs_train_image_is_set(monkeypatch):
    monkeypatch.setenv("OBS_TRAIN_IMAGE", "ghcr.io/example/cairn-train-gpu:abc123")
    mode, image, _gpus = resolve_exec_mode({"host": "box"})
    assert mode == "image" and image == "ghcr.io/example/cairn-train-gpu:abc123"


def test_connection_image_beats_the_environment_default(monkeypatch):
    """The connection names the BOX's image; the environment's key is the
    fleet default. More specific wins, and the decision is one function so
    the precedence is readable instead of scattered."""
    monkeypatch.setenv("OBS_TRAIN_IMAGE", "ghcr.io/example/cairn-train-gpu:fleet")
    mode, image, _gpus = resolve_exec_mode({"host": "box", "image": "ghcr.io/x/box-local:9"})
    assert mode == "image" and image == "ghcr.io/x/box-local:9"


def test_explicit_mode_image_with_no_image_anywhere_is_refused(monkeypatch):
    """`mode = "image"` with no image anywhere has no fallback (#202): the
    deployment names the build, so the run record can name it too."""
    with pytest.raises(EngineError) as e:
        resolve_exec_mode({"host": "box", "mode": "image"})
    assert str(e.value) == CONNECTION_IMAGE_REQUIRED


def test_explicit_mode_venv_wins_over_a_configured_image(monkeypatch):
    monkeypatch.setenv("OBS_TRAIN_IMAGE", "ghcr.io/example/cairn-train-gpu:fleet")
    mode, image, _gpus = resolve_exec_mode({"host": "box", "mode": "venv"})
    assert mode == "venv" and image == ""


# ── the remote command line ──────────────────────────────────────────


def test_image_mode_submits_the_expected_docker_command(monkeypatch):
    """(a) of the acceptance: the EXACT command line, pull-once, GPUs, the
    workdir bind-mount, and every forwarded env name."""
    _bind_connection(monkeypatch, image="ghcr.io/example/cairn-train-gpu:v0.1.0")
    captured = _capture_submit(monkeypatch)

    handle = _submit(BuiltinSshEngine("gpu"), _spec(parent_run="run-parent-9"))
    cmd = captured["inp"].command

    img = "ghcr.io/example/cairn-train-gpu:v0.1.0"
    # pull happens ONCE — inspect first, pull only on a miss (a `docker pull`
    # every dispatch is a multi-GB download on a metered box).
    assert cmd.startswith(f"docker image inspect {img} >/dev/null 2>&1 || docker pull {img}; ")
    assert " docker run --rm --gpus all " in cmd
    # runner.sh's cwd IS the job workdir, so $PWD binds it without the engine
    # having to predict exec.py's workdir naming.
    assert '-v "$PWD":/work -w /work' in cmd
    assert cmd.endswith(f"--entrypoint sh {img} -c cairn-dataflow-run")
    # the spec + the live channel reach the CONTAINER, not just the box shell
    for name in ("CAIRN_DATAFLOW_SPEC", "CAIRN_PARENT_RUN", "CAIRN_RUN_ID"):
        assert f" -e {name} " in cmd, name
    # values ride the exported shell env (runner.sh), never the argv — a
    # per-run token in a command line is visible to every `ps` on the box.
    assert "run-parent-9" not in cmd
    assert json.loads(captured["inp"].env["CAIRN_DATAFLOW_SPEC"])["parent_run"] == "run-parent-9"
    # provenance: the tag is on the durable handle, so the run record names
    # the exact build that produced the artifacts.
    assert handle.meta["image"] == img
    assert handle.meta["mode"] == "image"


def test_image_mode_forwards_every_live_channel_var(monkeypatch):
    """#2025's live-channel vars are what make the training curve stream back;
    an env name exported on the box but not forwarded with -e is invisible
    inside the container and the run goes dark."""
    _bind_connection(monkeypatch, image="ghcr.io/x/y:1")
    captured = _capture_submit(monkeypatch)
    _submit(BuiltinSshEngine("gpu"), _spec(parent_run="run-live-1"))

    cmd = captured["inp"].command
    for name in captured["inp"].env:
        assert f" -e {name} " in cmd, f"{name} exported but not forwarded into the container"


def test_image_mode_forwards_mlflow_env_when_set(monkeypatch):
    _bind_connection(monkeypatch, image="ghcr.io/x/y:1")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "https://mlflow.example")
    captured = _capture_submit(monkeypatch)
    _submit(BuiltinSshEngine("gpu"), _spec(parent_run="r"))
    assert " -e MLFLOW_TRACKING_URI " in captured["inp"].command
    assert "https://mlflow.example" not in captured["inp"].command


def test_image_mode_honours_the_gpus_and_inner_command_knobs(monkeypatch):
    _bind_connection(monkeypatch, image="ghcr.io/x/y:1", gpus="device=0")
    monkeypatch.setenv("OBS_DATAFLOW_IMAGE_CMD", "cairn-dataflow-run --verbose")
    captured = _capture_submit(monkeypatch)
    _submit(BuiltinSshEngine("gpu"), _spec())
    cmd = captured["inp"].command
    assert "--gpus device=0" in cmd
    assert cmd.endswith("-c 'cairn-dataflow-run --verbose'")


def test_image_mode_ignores_the_venv_path_override(monkeypatch):
    """OBS_DATAFLOW_REMOTE_CMD points at a venv bin ON THE BOX (#1075). That
    path does not exist inside the image — leaking it in is exit 127."""
    _bind_connection(monkeypatch, image="ghcr.io/x/y:1")
    monkeypatch.setenv("OBS_DATAFLOW_REMOTE_CMD", "/home/ubuntu/train-venv/bin/cairn-dataflow-run")
    captured = _capture_submit(monkeypatch)
    _submit(BuiltinSshEngine("gpu"), _spec())
    assert "/home/ubuntu/train-venv" not in captured["inp"].command


def test_venv_mode_command_is_unchanged(monkeypatch):
    """(b) of the acceptance: with no image configured the submitted command
    is what it was before D1 — the bare entrypoint, no docker anywhere."""
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    handle = _submit(BuiltinSshEngine("gpu"), _spec(parent_run="run-parent-9"))
    assert captured["inp"].command == "cairn-dataflow-run"
    assert "docker" not in captured["inp"].command
    assert handle.meta.get("mode") == "venv"
    assert not handle.meta.get("image")


def test_venv_mode_still_honours_the_box_path_override(monkeypatch):
    _bind_connection(monkeypatch)
    monkeypatch.setenv("OBS_DATAFLOW_REMOTE_CMD", "/home/ubuntu/train-venv/bin/cairn-dataflow-run")
    captured = _capture_submit(monkeypatch)
    _submit(BuiltinSshEngine("gpu"), _spec())
    assert captured["inp"].command == "/home/ubuntu/train-venv/bin/cairn-dataflow-run"


# ── provenance on the result ─────────────────────────────────────────


def test_result_records_the_image_tag(monkeypatch, tmp_path):
    """The run's stats name the build that produced the artifacts — otherwise
    "which image made this adapter?" is unanswerable after the fact (#1776's
    whole diagnosis was 'which build served this?')."""
    workdir = "/home/ubuntu/.cairn-jobs/run-img-1"
    fetch_dir = BuiltinSshEngine._fetch_dir(workdir)
    fetch_dir.mkdir(parents=True, exist_ok=True)
    (fetch_dir / "result.json").write_text(json.dumps({
        "output_artifacts": [], "stats": {"rows": 3}, "summary": "ok",
    }))
    from cairn.core.dataflow import EngineJobHandle
    handle = EngineJobHandle(job_id=f"box:{workdir}", engine="builtin-ssh", meta={
        "job_handle": {"job_id": f"box:{workdir}", "backend": "ssh",
                       "meta": {"host": "box", "user": "ubuntu", "port": 22,
                                "workdir": workdir, "known_hosts": "/tmp/kh"}},
        "exec_input": {"command": "docker run …", "env": {}, "fetch": ["result.json"]},
        "mode": "image", "image": "ghcr.io/example/cairn-train-gpu:v0.1.0",
    })
    result = asyncio.run(BuiltinSshEngine("gpu").result(handle))
    assert result.stats["engine.image"] == "ghcr.io/example/cairn-train-gpu:v0.1.0"
    assert result.stats["rows"] == 3


# ── no default image: image mode names one or refuses (#202) ─────────
#
# The refusal lives at submit (resolve_exec_mode), where the connection and the
# environment are both known; validate() sees only the spec.


def test_environment_image_mode_with_no_image_anywhere_is_refused_before_the_exec_seam(monkeypatch):
    """The environment selects image mode and nothing names an image: submit
    refuses with a message naming what to set, and the exec layer is never asked
    to run anything. There is no built-in fallback image."""
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "image")
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    with pytest.raises(EngineError) as e:
        _submit(BuiltinSshEngine("gpu"), _spec())
    assert str(e.value) == IMAGE_REQUIRED
    assert "remote_image" in str(e.value) and "CAIRN_TRAIN_IMAGE" in str(e.value)
    assert "inp" not in captured


def test_connection_image_mode_with_no_image_is_refused_naming_the_connection(monkeypatch):
    """A connection's own ``mode = "image"`` is not overridden by the
    environment, so the message says to fix the connection."""
    _bind_connection(monkeypatch, mode="image")
    captured = _capture_submit(monkeypatch)
    with pytest.raises(EngineError) as e:
        _submit(BuiltinSshEngine("gpu"), _spec())
    assert str(e.value) == CONNECTION_IMAGE_REQUIRED
    assert "connection's 'image'" in str(e.value) and "'mode'" in str(e.value)
    assert "inp" not in captured


def test_environment_image_mode_with_a_connection_image_submits_that_image(monkeypatch):
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "image")
    _bind_connection(monkeypatch, image="ghcr.io/example/box-local:9")
    engine = BuiltinSshEngine("gpu")
    assert engine.validate(_spec()) == []
    captured = _capture_submit(monkeypatch)
    handle = _submit(engine, _spec())
    assert " ghcr.io/example/box-local:9 " in captured["inp"].command
    assert handle.meta["mode"] == "image"
    assert handle.meta["image"] == "ghcr.io/example/box-local:9"


def test_environment_image_mode_with_a_connection_mode_venv_runs_venv(monkeypatch):
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "image")
    _bind_connection(monkeypatch, mode="venv")
    engine = BuiltinSshEngine("gpu")
    assert engine.validate(_spec()) == []
    captured = _capture_submit(monkeypatch)
    handle = _submit(engine, _spec())
    assert captured["inp"].command == "cairn-dataflow-run"
    assert handle.meta["mode"] == "venv" and handle.meta["image"] == ""


def test_environment_image_mode_with_remote_image_set_submits_it(monkeypatch):
    """Positive control: the environment names both the mode and the image."""
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "image")
    monkeypatch.setenv("OBS_TRAIN_IMAGE", "ghcr.io/example/cairn-train-gpu:fleet")
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    handle = _submit(BuiltinSshEngine("gpu"), _spec())
    assert " ghcr.io/example/cairn-train-gpu:fleet " in captured["inp"].command
    assert handle.meta["image"] == "ghcr.io/example/cairn-train-gpu:fleet"


def test_auto_mode_with_no_image_picks_venv(monkeypatch):
    """``auto`` (the default) with no image anywhere is the box virtualenv —
    validate passes and the submitted command is the bare entrypoint."""
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "auto")
    engine = BuiltinSshEngine("gpu")
    assert engine.validate(_spec()) == []
    _bind_connection(monkeypatch)
    captured = _capture_submit(monkeypatch)
    handle = _submit(engine, _spec())
    assert captured["inp"].command == "cairn-dataflow-run"
    assert handle.meta["mode"] == "venv" and handle.meta["image"] == ""
