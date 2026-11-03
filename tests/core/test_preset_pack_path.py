"""ADR-0165 D2 — a pack's preset can name a file in its pack, so the same preset works on any machine."""
from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from cairn.core import pipeline_presets as pp
from cairn.core.workflow_config_store import InMemoryWorkflowConfigStore

REF = "pipeline:p/x"


@pytest.fixture
def pack(tmp_path, monkeypatch):
    root = tmp_path / "wheel" / "cairn_pack_p"
    (root / "presets").mkdir(parents=True)
    (root / "data").mkdir()
    (root / "data" / "sample.jsonl").write_text("{}\n")
    (tmp_path / "wheel" / "secret.txt").write_text("outside\n")
    os.symlink(tmp_path / "wheel" / "secret.txt", root / "data" / "link-out")
    monkeypatch.setattr(pp, "all_preset_dirs", lambda: [str(root / "presets")])
    monkeypatch.setattr(pp, "pipeline_pack_name", lambda path: "p")
    monkeypatch.setattr(pp, "canonical_pipeline_ref", lambda ref: REF)
    (root / "presets" / "p.yaml").write_text(
        "pipeline: p/x\npresets:\n"
        "  - {name: good, inputs: {path: $pack/data/sample.jsonl, other: plain, n: 3}}\n"
        "  - {name: escape, inputs: {path: $pack/../secret.txt}}\n"
        "  - {name: absolute, inputs: {path: $pack//etc/hosts}}\n"
        "  - {name: symlink, inputs: {path: $pack/data/link-out}}\n"
        "  - {name: missing, inputs: {path: $pack/data/nope.jsonl}}\n"
        "  - {name: root, inputs: {path: $pack/}}\n")
    return root


def _expand(name: str):
    d = SimpleNamespace(pack="p", name="x", inputs={"path": {}, "other": {}, "n": {}})
    return asyncio.run(pp.expand_preset("default", d, name, store=InMemoryWorkflowConfigStore()))


def test_a_pack_path_resolves_to_the_file_in_this_install(pack):
    rec, source = _expand("good")
    assert source == pp.SOURCE_PACK
    assert rec.inputs == {"path": str((pack / "data" / "sample.jsonl").resolve()), "other": "plain", "n": 3}


@pytest.mark.parametrize("name", ["escape", "absolute", "symlink", "missing", "root"])
def test_a_path_that_is_not_inside_the_pack_is_refused_at_the_door(pack, name):
    with pytest.raises(pp.UnknownPresetInputsError, match=r"isn't a file inside its pack"):
        _expand(name)


def test_a_prefix_sibling_of_the_pack_is_outside_it(pack, tmp_path):
    (tmp_path / "wheel" / "cairn_pack_p2").mkdir()
    (tmp_path / "wheel" / "cairn_pack_p2" / "x").write_text("")
    assert pp._pack_path("$pack/../cairn_pack_p2/x", pack) == "$pack/../cairn_pack_p2/x"


def test_a_nul_in_a_path_is_refused_not_a_crash(pack):
    assert pp._pack_path("$pack/a\0b", pack) == "$pack/a\0b"


def test_the_pack_root_is_where_the_manifest_is(tmp_path):
    root = tmp_path / "p"
    (root / "conf" / "presets").mkdir(parents=True)
    (root / "pack.toml").write_text("")
    assert pp._pack_root(root / "conf" / "presets" / "x.yaml") == root


@pytest.mark.parametrize("call", [
    lambda c: c.post("/pipelines/preview", json={"ref": "p/x", "preset": "escape"}),
    lambda c: c.get("/pipelines/p/x/explain", params={"preset": "escape"}),
    lambda c: c.post("/start", json={"ref": "p/x", "config_preset": "escape", "dry_run": True}),
])
def test_a_dry_run_refuses_what_the_launch_refuses(pack, monkeypatch, call):
    """Every read that shows what a launch would do says no where the launch says no (#231)."""
    from cairn.core.triggers.server import build_app
    from tests.core.test_trigger_server_dispatch import FakeRunner

    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    d = SimpleNamespace(pack="p", name="x", inputs={"path": {}})
    monkeypatch.setattr("cairn.core.dispatch.pipeline_launch.resolve_or_raise", lambda ref: d)
    monkeypatch.setattr("cairn.core.dispatch.start.resolve_start_ref", lambda ref, **k: ("pipeline", "p/x"))
    monkeypatch.setattr(pp, "default_workflow_config_store", InMemoryWorkflowConfigStore)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = call(c)
    assert r.status_code == 400, r.text
    assert "isn't a file inside its pack" in r.text


def test_a_bad_preset_leaves_its_neighbours_usable(pack):
    assert set(pp.pack_presets(REF)) == {"good", "escape", "absolute", "symlink", "missing", "root"}
    assert _expand("good")[0].inputs["other"] == "plain"
