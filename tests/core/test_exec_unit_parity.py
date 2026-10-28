"""ADR-0150 D1 — `cairn doctor` and the SSH engine agree on what a box runs.

Core may not import an operator pack, so ``cairn.core.deploy_check._exec_unit``
restates the precedence of ``operator_dataflow.engines_ssh.resolve_exec_mode``
(connection mode > environment mode; connection image > environment image;
auto with no image → venv; image with no image anywhere → refused). A test may
import both: this one runs the whole matrix through the engine and through the
doctor row and asserts the same answer — venv, the same image, or refused with
the same remedy — for every case.
"""
from __future__ import annotations

import itertools

import pytest

from cairn.core.dataflow import EngineError
from cairn.core.deploy_check import check_compute_ssh

engines_ssh = pytest.importorskip("operator_dataflow.engines_ssh")

ENV_IMG = "ghcr.io/example/cairn-train-gpu:env"
CONN_IMG = "ghcr.io/example/cairn-train-gpu:box"

MATRIX = list(itertools.product(
    ("", "auto", "venv", "image"),   # environment remote_exec_mode
    ("", ENV_IMG),                   # environment remote_image
    ("", "venv", "image", "auto"),   # connection mode
    ("", CONN_IMG),                  # connection image
))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("OBS_TRAIN_IMAGE", "OBS_REMOTE_EXEC_MODE", "OBS_PROFILE"):
        monkeypatch.delenv(var, raising=False)


def _engine(monkeypatch, env_mode, env_image, conn_mode, conn_image):
    """What the engine does: ('venv', ''), ('image', <image>) or ('refused', <which>)."""
    if env_mode:
        monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", env_mode)
    else:
        monkeypatch.delenv("OBS_REMOTE_EXEC_MODE", raising=False)
    if env_image:
        monkeypatch.setenv("OBS_TRAIN_IMAGE", env_image)
    else:
        monkeypatch.delenv("OBS_TRAIN_IMAGE", raising=False)
    cfg = {"host": "box"}
    if conn_mode:
        cfg["mode"] = conn_mode
    if conn_image:
        cfg["image"] = conn_image
    try:
        mode, image, _gpus = engines_ssh.resolve_exec_mode(cfg)
    except EngineError as e:
        which = ("connection" if str(e) == engines_ssh.CONNECTION_IMAGE_REQUIRED
                 else "environment")
        return "refused", which
    return mode, image


def _doctor(env_mode, env_image, conn_mode, conn_image):
    """What doctor reports, classified the same way."""
    cfg = {"host": "box", "user": "ubuntu"}
    if conn_mode:
        cfg["mode"] = conn_mode
    if conn_image:
        cfg["image"] = conn_image
    rows = {r.name: r for r in check_compute_ssh(
        "ml_target", "gpu", cfg, has_key=True,
        env_mode=env_mode or "auto", env_image=env_image)}
    row = rows["compute_target:ml_target:mode"]
    if row.status == "fail":
        which = "connection" if "connection's mode" in row.detail else "environment"
        return "refused", which
    if "virtualenv" in row.detail:
        return "venv", ""
    for image in (CONN_IMG, ENV_IMG):
        if image in row.detail:
            return "image", image
    raise AssertionError(f"unclassifiable doctor row: {row}")


@pytest.mark.parametrize("env_mode, env_image, conn_mode, conn_image", MATRIX)
def test_doctor_and_engine_agree(monkeypatch, env_mode, env_image, conn_mode, conn_image):
    case = (env_mode, env_image, conn_mode, conn_image)
    assert _doctor(*case) == _engine(monkeypatch, *case), case


def test_the_matrix_covers_every_outcome(monkeypatch):
    """Guard against a matrix that only ever exercises one branch."""
    seen = {_engine(monkeypatch, *case)[0] for case in MATRIX}
    assert seen == {"venv", "image", "refused"}
