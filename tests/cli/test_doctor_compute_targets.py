"""ADR-0104 D1 (#2071) — `cairn doctor` resolves the environment's compute
targets and refuses a hostless `compute.ssh` connection BY NAME.

The pure predicate is tested in ``tests/core/test_deploy_check_compute_ssh.py``;
this pins the wiring: doctor reads the SAME backend-selection plane the engine
does (``dataflow_target`` / ``ml_target``), resolves each named connection, and
never lets a resolution error take the whole report down.
"""
from __future__ import annotations

import asyncio

import pytest

from cairn.cli.commands import doctor as doctor_mod


@pytest.fixture(autouse=True)
def _no_ambient_targets(monkeypatch):
    for var in ("OBS_ML_TARGET", "OBS_DATAFLOW_TARGET", "OBS_PROFILE",
                "OBS_TRAIN_IMAGE", "OBS_REMOTE_EXEC_MODE"):
        monkeypatch.delenv(var, raising=False)


def _bind(monkeypatch, **by_name):
    async def fake_resolve(name, **_kw):
        entry = by_name.get(name)
        if entry is None:
            return None
        cfg, secrets = entry
        return type("RC", (), {"config": cfg, "secrets": secrets})()
    monkeypatch.setattr(
        "cairn.core.connections.resolve_connection", fake_resolve, raising=True)


def _run():
    return {r.name: r for r in asyncio.run(doctor_mod.check_compute_targets())}


def test_no_targets_reports_nothing(monkeypatch):
    _bind(monkeypatch)
    assert _run() == {}


def test_hostless_ml_target_fails(monkeypatch):
    """The 2026-09-07 shape exactly: ml_target=gpu, the gpu connection has a
    user and a key and NO host."""
    monkeypatch.setenv("OBS_ML_TARGET", "gpu")
    _bind(monkeypatch, gpu=({"provider": "ssh", "user": "ubuntu"}, {"PRIVATE_KEY": "k"}))
    r = _run()["compute_target:ml_target"]
    assert r.status == "fail" and "host" in r.detail and "gpu" in r.detail


def test_complete_ml_target_passes(monkeypatch):
    monkeypatch.setenv("OBS_ML_TARGET", "gpu")
    _bind(monkeypatch, gpu=({"provider": "ssh", "host": "10.0.0.1", "user": "ubuntu",
                             "image": "ghcr.io/example/cairn-train-gpu:2026.09"},
                            {"PRIVATE_KEY": "k"}))
    rows = _run()
    assert rows["compute_target:ml_target"].status == "ok"
    assert rows["compute_target:ml_target:mode"].status == "ok"


def test_unknown_connection_fails(monkeypatch):
    monkeypatch.setenv("OBS_DATAFLOW_TARGET", "nosuch")
    _bind(monkeypatch)
    r = _run()["compute_target:dataflow_target"]
    assert r.status == "fail" and "nosuch" in r.detail


def test_a_resolution_error_degrades_to_a_warn(monkeypatch):
    """A doctor check that raises takes the whole report down — the store may
    be unreachable from wherever doctor runs, which is a warn, not a crash."""
    monkeypatch.setenv("OBS_ML_TARGET", "gpu")

    async def boom(name, **_kw):
        raise RuntimeError("store down")
    monkeypatch.setattr("cairn.core.connections.resolve_connection", boom, raising=True)
    r = _run()["compute_target:ml_target"]
    assert r.status == "warn" and "store down" in r.detail


_BOX = ({"provider": "ssh", "host": "10.0.0.1", "user": "ubuntu"}, {"PRIVATE_KEY": "k"})


def test_doctor_reads_the_environment_image_mode(monkeypatch):
    """ADR-0150 D1 — remote_exec_mode=image with no image anywhere is refused by
    the engine at submit, so doctor FAILS the row instead of calling it venv."""
    monkeypatch.setenv("OBS_ML_TARGET", "gpu")
    monkeypatch.setenv("OBS_REMOTE_EXEC_MODE", "image")
    _bind(monkeypatch, gpu=_BOX)
    r = _run()["compute_target:ml_target:mode"]
    assert r.status == "fail" and "refused at submit" in r.detail


def test_doctor_reads_the_environment_image(monkeypatch):
    monkeypatch.setenv("OBS_ML_TARGET", "gpu")
    monkeypatch.setenv("OBS_TRAIN_IMAGE", "ghcr.io/example/cairn-train-gpu:fleet")
    _bind(monkeypatch, gpu=_BOX)
    r = _run()["compute_target:ml_target:mode"]
    assert r.status == "ok" and "ghcr.io/example/cairn-train-gpu:fleet" in r.detail
