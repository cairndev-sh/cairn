"""The feature proof can prove what it lists (0.1.2 verification, bug 8a).

`hooks` was NOT PROVED with the CI install: the run-hook listener is an entry point of
the cairn-pack-hello wheel, which only the `hello` extra installs, and it was checked on
nightly-ops, which CI cannot run (no proof sshd). It is now checked on
profile-and-publish, which every cell runs, and `public-e2e` installs the extra."""
from __future__ import annotations

import inspect

import yaml

from tests import _layout
from tests.e2e.hello import _receipts, test_matrix


def _e2e_sync() -> str:
    ci = yaml.safe_load((_layout.REPO / ".github/workflows/ci.yml").read_text())
    steps = ci["jobs"]["public-e2e"]["steps"]
    return next(s["run"] for s in steps if "uv sync" in str(s.get("run", "")))


def test_public_e2e_installs_the_hello_wheel():
    assert "--extra hello" in _e2e_sync()


def test_hooks_are_proved_on_a_pipeline_every_cell_runs():
    assert _receipts.FEATURES["hooks"][1] == "profile-and-publish"
    assert "_hooks_receipt(ctx)" in inspect.getsource(test_matrix.check_profile_and_publish)
