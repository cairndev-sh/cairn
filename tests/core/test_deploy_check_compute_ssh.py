"""ADR-0104 D1 (#2071) — a compute.ssh target is checked BEFORE launch.

On 2026-09-07 a GPU run could not start and the first thing anyone saw was
``compute.ssh connection has no host``, raised by ``as_ssh_params`` at submit
time — after the run was created, gated and dispatched. The environment named
``gpu`` as its ``ml_target``; the ``gpu`` connection carried no host. Nothing
between "the operator saved that connection" and "a run died on it" looked at
it, so `cairn doctor` said the deployment was healthy.

``check_compute_ssh`` is the pure, data-in/data-out half of the fix (the same
shape as ``check_deploy``): it takes the target NAME and the resolved
connection config and names what is missing. ``cairn doctor`` resolves the
connections and calls it (``tests/cli/test_doctor_compute_targets.py``).
"""
from __future__ import annotations

from cairn.core.deploy_check import check_compute_ssh


def _by_name(results):
    return {r.name: r for r in results}


#: The environment's selection at its defaults: remote_exec_mode=auto, no remote_image.
DEFAULT_ENV = {"env_mode": "auto", "env_image": ""}


def test_unresolvable_target_fails_by_name():
    """A target that names no connection at all — the environment points at a
    connection somebody deleted or never created."""
    rows = _by_name(check_compute_ssh("ml_target", "gpu", None))
    r = rows["compute_target:ml_target"]
    assert r.status == "fail"
    assert "gpu" in r.detail and "ml_target" in r.detail
    assert "cairn connect add" in r.detail  # the fix is in the message


def test_hostless_connection_fails_naming_the_field_and_the_target():
    """THE 2026-09-07 failure: the connection exists, is bound, and has no
    host. It must fail here, not as 'no host' at submit."""
    rows = _by_name(check_compute_ssh("ml_target", "gpu", {"provider": "ssh", "user": "ubuntu"}))
    r = rows["compute_target:ml_target"]
    assert r.status == "fail"
    assert "host" in r.detail and "gpu" in r.detail
    # the message says WHERE it would have surfaced otherwise
    assert "submit" in r.detail.lower()


def test_missing_user_fails():
    rows = _by_name(check_compute_ssh("ml_target", "gpu", {"host": "10.0.0.1"}))
    assert rows["compute_target:ml_target"].status == "fail"
    assert "user" in rows["compute_target:ml_target"].detail


def test_missing_private_key_fails():
    rows = _by_name(check_compute_ssh(
        "ml_target", "gpu", {"host": "10.0.0.1", "user": "ubuntu"}, has_key=False))
    assert rows["compute_target:ml_target"].status == "fail"
    assert "PRIVATE_KEY" in rows["compute_target:ml_target"].detail


def test_complete_connection_passes_and_names_the_execution_unit():
    rows = _by_name(check_compute_ssh(
        "ml_target", "gpu", {"host": "10.0.0.1", "user": "ubuntu", "port": 22},
        has_key=True, **DEFAULT_ENV))
    r = rows["compute_target:ml_target"]
    assert r.status == "ok"
    assert "ubuntu@10.0.0.1" in r.detail
    # a target with no image runs the box's virtualenv — the default, so OK.
    mode = rows["compute_target:ml_target:mode"]
    assert mode.status == "ok" and "venv" in mode.detail


def test_image_mode_target_reports_the_pinned_tag():
    rows = _by_name(check_compute_ssh(
        "ml_target", "gpu",
        {"host": "10.0.0.1", "user": "ubuntu", "image": "ghcr.io/example/cairn-train-gpu:2026.09"},
        has_key=True, **DEFAULT_ENV))
    mode = rows["compute_target:ml_target:mode"]
    assert mode.status == "ok"
    assert "ghcr.io/example/cairn-train-gpu:2026.09" in mode.detail


def test_latest_tag_is_a_warning_not_a_green_tick():
    """`:latest` means the run record cannot say which build ran — the exact
    ambiguity that made #1776 (adapter far below the experiment) take three
    runs to diagnose."""
    rows = _by_name(check_compute_ssh(
        "ml_target", "gpu",
        {"host": "10.0.0.1", "user": "ubuntu", "image": "ghcr.io/example/cairn-train-gpu:latest"},
        has_key=True, **DEFAULT_ENV))
    mode = rows["compute_target:ml_target:mode"]
    assert mode.status == "warn" and "latest" in mode.detail


def test_untagged_image_is_a_warning():
    rows = _by_name(check_compute_ssh(
        "ml_target", "gpu", {"host": "10.0.0.1", "user": "ubuntu",
                             "image": "ghcr.io/example/cairn-train-gpu"}, has_key=True,
        **DEFAULT_ENV))
    assert rows["compute_target:ml_target:mode"].status == "warn"


def test_no_target_is_not_a_finding():
    """An environment with no compute target runs in-process. That is a legal
    (dev) shape — doctor must not invent a failure for it."""
    assert check_compute_ssh("ml_target", "", None) == []


def test_every_row_lands_in_the_runtime_category():
    rows = check_compute_ssh("ml_target", "gpu", {"host": "h", "user": "u"}, has_key=True)
    assert rows and {r.category for r in rows} == {"runtime"}


def _mode_row(cfg, **env):
    return _by_name(check_compute_ssh("ml_target", "gpu", cfg, has_key=True, **env))[
        "compute_target:ml_target:mode"]


BOX = {"host": "10.0.0.1", "user": "ubuntu"}
IMG = "ghcr.io/example/cairn-train-gpu:1"


def test_venv_rows_are_ok_and_suggest_no_image():
    """Venv is the default (ADR-0150 D1): the row is a plain OK that names no
    image and tells nobody to set one."""
    for cfg, env in (
        (BOX, DEFAULT_ENV),
        ({**BOX, "mode": "venv"}, DEFAULT_ENV),
        ({**BOX, "mode": "venv", "image": IMG}, {"env_mode": "image", "env_image": IMG}),
        ({**BOX, "mode": "venv"}, {}),  # the environment is irrelevant here, so unread is fine
    ):
        mode = _mode_row(cfg, **env)
        assert mode.status == "ok", (cfg, env)
        assert "virtualenv" in mode.detail
        for word in ("ghcr.io", "cairn-train-gpu", "CAIRN_TRAIN_IMAGE", "remote_image", "Set "):
            assert word not in mode.detail, (cfg, word)


def test_environment_image_mode_with_no_image_anywhere_fails():
    """(A) remote_exec_mode=image, remote_image empty, a bare connection: the
    engine refuses every dispatch at submit, so doctor must not say OK."""
    mode = _mode_row(BOX, env_mode="image", env_image="")
    assert mode.status == "fail"
    assert "refused at submit" in mode.detail
    assert "remote_image" in mode.detail and "CAIRN_TRAIN_IMAGE" in mode.detail
    assert "`remote_exec_mode`" in mode.detail
    assert "ghcr.io" not in mode.detail and "cairn-train-gpu" not in mode.detail


def test_connection_image_mode_with_no_image_anywhere_fails_naming_the_connection():
    mode = _mode_row({**BOX, "mode": "image"}, **DEFAULT_ENV)
    assert mode.status == "fail"
    assert "refused at submit" in mode.detail
    assert "connection's `image`" in mode.detail and "`mode`" in mode.detail
    assert "ghcr.io" not in mode.detail and "cairn-train-gpu" not in mode.detail


def test_environment_image_reaches_a_bare_connection():
    """(B)/(C) remote_image set, mode auto or image: the engine runs that image."""
    for env_mode in ("auto", "image", ""):
        mode = _mode_row(BOX, env_mode=env_mode, env_image=IMG)
        assert mode.status == "ok" and IMG in mode.detail, env_mode


def test_environment_venv_beats_a_connection_image_without_a_mode():
    """(D) the connection names an image but no mode, remote_exec_mode=venv:
    the engine runs venv."""
    mode = _mode_row({**BOX, "image": IMG}, env_mode="venv", env_image="")
    assert mode.status == "ok" and "virtualenv" in mode.detail and IMG not in mode.detail


def test_unread_environment_gives_a_conditional_warn():
    """A caller that cannot read the environment gets a row that says the answer
    depends on it, never a green venv or image."""
    for cfg in (BOX, {**BOX, "image": IMG}, {**BOX, "mode": "image"}):
        mode = _mode_row(cfg)
        assert mode.status == "warn", cfg
        assert "not read here" in mode.detail
        assert "refused at submit" in mode.detail
    # independent of the environment: a connection that pins both mode and image
    assert _mode_row({**BOX, "mode": "image", "image": IMG}).status == "ok"
