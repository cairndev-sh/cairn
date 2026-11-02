"""`examples/ci` gates on a check that ships, and a bad file blocks it.

Runs `cairn gate` the way the copy-paste workflows do, from a directory holding
`templates/dataset-check.md`, on the public core alone. A missing file has to block,
through the template's own fallback, on released versions as well as this one.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

CI = Path(__file__).resolve().parents[2] / "examples" / "ci"
SAMPLE = CI.parent / "hello-cairn" / "data" / "sample.jsonl"


def _gate(path: Path, *flags: str) -> tuple[int, dict]:
    env = {k: v for k, v in os.environ.items() if k != "CAIRN_PACK_PATH"}
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)  # this code, not an installed copy
    done = subprocess.run(
        [sys.executable, "-m", "cairn.cli.main", "gate", "dataset-check",
         "--set", f"path={path}", "--json", *flags],
        cwd=CI, env=env, capture_output=True, text=True, timeout=120,
    )
    assert done.stdout, done.stderr
    return done.returncode, json.loads(done.stdout.splitlines()[-1])


def test_a_good_file_passes():
    rc, verdict = _gate(SAMPLE)
    assert (rc, verdict["label"]) == (0, "PASS")


def test_a_missing_required_role_blocks(tmp_path):
    bad = tmp_path / "bad.jsonl"
    bad.write_text('{"foo": "bar"}\n')
    rc, verdict = _gate(bad)
    assert (rc, verdict["label"]) == (1, "BLOCK")
    assert verdict["blocking"] == ["text", "label"]


def test_a_missing_optional_role_warns_and_fail_on_warn_blocks(tmp_path):
    partial = tmp_path / "partial.jsonl"
    partial.write_text("".join(
        json.dumps({"text": f"row {i}", "label": "positive"}) + "\n" for i in range(5)))
    assert _gate(partial)[0] == 0
    rc, verdict = _gate(partial, "--fail-on", "warn")
    assert (rc, verdict["label"], verdict["warnings"]) == (1, "DEGRADE", ["id"])


@pytest.mark.parametrize("content", [None, '{"text": "a"'], ids=["missing", "malformed"])
def test_a_file_that_cannot_be_read_blocks(tmp_path, content):
    path = tmp_path / "train.jsonl"
    if content is not None:
        path.write_text(content)
    rc, verdict = _gate(path)
    assert (rc, verdict["label"]) == (1, "BLOCK")
    assert verdict["blocking"] == ["dataset file could not be read"]


@pytest.mark.parametrize("name", ["github-actions-gate.yml", "gitlab-ci-gate.yml", "README.md"])
def test_the_copy_paste_files_name_the_check_that_ships(name):
    text = (CI / name).read_text()
    assert "cairn gate dataset-check" in text
    assert "dataset-readiness" not in text
