"""The engine image starts as documented (0.1.2 verification, bug 5).

A bare ``docker run`` of 0.1.2's image refused (``CAIRN_PROFILE is unset``). The
image's start command now falls back to the packaged ``secure`` profile, after a
deployment's ``CAIRN_PROFILE`` and the legacy ``OBS_PROFILE``. The auth-dependent
bind is NOT defaulted by the image: a keyless container keeps binding 127.0.0.1.

The start command is run here, with a stand-in ``cairn-server`` that prints the
environment it was given, so the precedence is the shell's own. CI's ``image`` job
then starts the built image and curls ``/healthz`` (auth on, keyless, keyless trial).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

import cairn.profiles as profiles_pkg
from tests import _layout

DOCKERFILE = _layout.REPO / "Dockerfile.engine"
VENV_SITE = "/app/.venv/lib/python3.13/site-packages/"


def _cmd() -> list[str]:
    lines = [ln for ln in DOCKERFILE.read_text().splitlines() if ln.startswith("CMD ")]
    assert len(lines) == 1, lines
    return json.loads(lines[0][len("CMD "):])


def _start(tmp_path: Path, **env: str) -> dict[str, str]:
    """Run the image's CMD with a stand-in cairn-server; return the env it saw."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    fake = bindir / "cairn-server"
    fake.write_text('#!/bin/sh\necho "CAIRN_PROFILE=${CAIRN_PROFILE}"\n'
                    'echo "CAIRN_TRIGGER_HOST=${CAIRN_TRIGGER_HOST:-}"\n')
    fake.chmod(0o755)
    base = {"PATH": f"{bindir}{os.pathsep}/usr/bin:/bin"}
    r = subprocess.run(_cmd(), env={**base, **env}, capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    return dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX shell")
def test_a_bare_run_gets_the_packaged_secure_profile(tmp_path):
    seen = _start(tmp_path)
    assert seen["CAIRN_PROFILE"] == VENV_SITE + "cairn/profiles/secure.toml"
    # the path the image falls back to is the wheel's packaged profile
    assert (Path(profiles_pkg.__file__).parent / "secure.toml").is_file()


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX shell")
def test_a_deployment_profile_wins_either_spelling(tmp_path):
    assert _start(tmp_path, CAIRN_PROFILE="/p/a.toml")["CAIRN_PROFILE"] == "/p/a.toml"
    assert _start(tmp_path, OBS_PROFILE="/p/b.toml")["CAIRN_PROFILE"] == "/p/b.toml"
    assert _start(tmp_path, CAIRN_PROFILE="/p/a.toml",
                  OBS_PROFILE="/p/b.toml")["CAIRN_PROFILE"] == "/p/a.toml"


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX shell")
def test_the_image_does_not_default_the_bind_address(tmp_path):
    """Auth decides the bind (#1507): an image default would open a keyless control
    plane on every published port."""
    assert _start(tmp_path)["CAIRN_TRIGGER_HOST"] == ""
    assert "TRIGGER_HOST" not in " ".join(_cmd())


def test_no_cairn_setting_is_baked_in_as_env():
    """An image-level CAIRN_*/OBS_* ENV would shadow a deployment's legacy spelling (ADR-0122)."""
    env_lines = [ln for ln in DOCKERFILE.read_text().splitlines() if ln.startswith("ENV ")]
    assert not [ln for ln in env_lines if re.search(r"\b(CAIRN|OBS)_", ln)], env_lines


def test_the_header_example_is_one_that_can_answer():
    head = DOCKERFILE.read_text().split("\nFROM ", 1)[0]
    runs = [ln for ln in head.splitlines() if "docker run" in ln]
    assert runs
    for ln in runs:
        # a keyless run only works with an explicit bind, published on loopback
        if "CAIRN_JWT_SECRET" not in ln:
            assert "CAIRN_TRIGGER_HOST=0.0.0.0" in ln and "-p 127.0.0.1:" in ln, ln
