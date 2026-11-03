"""`cairn validate` knows a pack's preset file, and checks it (#317)."""
from __future__ import annotations

from pathlib import Path

import pytest

from cairn.core.validate import detect_artifact_type, validate_path


def _preset(tmp_path: Path, text: str) -> Path:
    (tmp_path / "presets").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "sample.jsonl").write_text("{}\n", encoding="utf-8")
    path = tmp_path / "presets" / "p.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_preset_file_is_a_preset_and_passes(tmp_path):
    path = _preset(tmp_path, "pipeline: p/x\npresets:\n  - {name: s, inputs: {path: $pack/data/sample.jsonl, n: 3}}\n")
    assert detect_artifact_type(path) == "preset"
    report = validate_path(path)
    assert report.ok, report.errors


@pytest.mark.parametrize("text, why", [
    ("presets:\n  - {name: s, inputs: {}}\n", "pipeline"),
    ("pipeline: p/x\npresets: []\n", "at least one"),
    ("pipeline: p/x\npresets:\n  - {inputs: {}}\n", "name"),
    ("pipeline: p/x\npresets:\n  - {name: s, inputs: {path: $pack/data/nope.jsonl}}\n", "isn't in this pack"),
    ("pipeline: p/x\npresets:\n  - {name: s, inputs: {path: $pack/../secret.txt}}\n", "isn't in this pack"),
])
def test_a_bad_preset_file_says_why(tmp_path, text, why):
    report = validate_path(_preset(tmp_path, text))
    assert not report.ok and any(why in e.message for e in report.errors), report.errors
