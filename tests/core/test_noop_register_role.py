"""The `noop` stage can say what a registered version is for, so the holdout door sees a test set (#333)."""
from __future__ import annotations

from cairn.core.dataflow.stage_loader import noop


def test_noop_passes_a_role_on_with_the_registration(tmp_path):
    data = tmp_path / "rows.jsonl"
    data.write_text('{"id": 1}\n', encoding="utf-8")
    stats = noop(tmp_path, {"register": "eval-set", "source": str(data), "register_role": "test"}, [])
    assert stats["register_as"] == "eval-set" and stats["register_role"] == "test"


def test_noop_without_a_role_leaves_the_datasets_default(tmp_path):
    data = tmp_path / "rows.jsonl"
    data.write_text('{"id": 1}\n', encoding="utf-8")
    for config in ({}, {"register_role": ""}):
        assert "register_role" not in noop(tmp_path, {"register": "d", "source": str(data), **config}, [])
