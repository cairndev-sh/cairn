"""The registry catalog's ``domain_pack`` kind: a pack that ships pipelines and no agentic flows.

It is listed with its ``pipelines`` and reads as installed when one of them is present as
``<pack>/<pipeline>``. Finding pipelines lists files and reads pack manifests; it parses none.
The integration tests load a REAL pipeline-only pack from disk into a fresh pack registry, so the
pack name, the file listing and the flow-directory check all run unstubbed.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from cairn.core import registry_catalog as rc

ENTRY = {"ref": "finetune", "kind": "domain_pack", "version": "0.1.0", "trust": "official",
         "description": "d", "pipelines": ["sft-experiment"],
         "install": {"index": "pypi", "name": "cairn-pack-finetune"}}


@pytest.fixture
def installed_pack(tmp_path, monkeypatch):
    """A pipeline-only pack named ``finetune`` (directory named otherwise on purpose), loaded and
    registered the way ``CAIRN_PACK_PATH`` loads one, into a registry holding only it."""
    import cairn.core.pack.loader as loader
    import cairn.core.pack.registry as registry

    pack_dir = tmp_path / "some_dir_name"
    (pack_dir / "pipelines").mkdir(parents=True)
    (pack_dir / "pack.toml").write_text('[pack]\nname = "finetune"\nversion = "0.1.0"\nkind = "domain"\n'
                                        'description = "d"\n')
    (pack_dir / "__init__.py").write_text("from cairn.core.pack import Pack\npack = Pack.from_manifest(__file__)\n")
    (pack_dir / "pipelines" / "sft-experiment.yaml").write_text("name: sft-experiment\nsteps: []\n")
    monkeypatch.setattr(registry, "_PACKS", {})
    registry.register_pack(loader._import_dev_pack(pack_dir).pack)
    monkeypatch.setattr(loader, "load_all_packs", registry.get_packs)
    return pack_dir


def _catalog(tmp_path, monkeypatch, entries):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"schema": 1, "channel": "official", "entries": entries}))
    monkeypatch.setenv("CAIRN_REGISTRY_CATALOG", str(path))
    monkeypatch.setattr(rc, "_installed_index", lambda: (set(), set()))


def test_installed_pipelines_are_named_by_manifest_and_file_not_by_directory(installed_pack):
    assert rc._installed_pipelines() == {"finetune/sft-experiment"}


def test_a_real_pipeline_only_pack_is_a_live_domain_pack_without_an_install_name(installed_pack):
    [entry] = [e for e in rc._live_entries() if e["ref"] == "finetune"]
    assert (entry["kind"], entry["pipelines"], entry["installed"]) == ("domain_pack", ["sft-experiment"], True)
    assert "install" not in entry


def test_the_catalog_entry_keeps_its_distribution_name_when_live_discovery_merges(installed_pack, tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, [dict(ENTRY)])
    [entry] = rc.load_registry_catalog()["entries"]
    assert entry["installed"] is True
    assert entry["install"] == {"index": "pypi", "name": "cairn-pack-finetune"}


def test_no_pipeline_is_parsed_to_build_the_catalog(installed_pack, tmp_path, monkeypatch):
    import cairn.core.engine.pipeline_resolve as pipeline_resolve

    def boom():
        raise AssertionError("the catalog parsed pipelines")

    monkeypatch.setattr(pipeline_resolve, "all_pipeline_defs", boom)
    _catalog(tmp_path, monkeypatch, [dict(ENTRY)])
    for annotate in (False, True):
        assert [e["ref"] for e in rc.load_registry_catalog(annotate=annotate)["entries"]] == ["finetune"]


def test_a_domain_pack_is_not_installed_when_none_of_its_pipelines_is_present(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, [dict(ENTRY)])
    monkeypatch.setattr(rc, "_live_entries", lambda: [])
    monkeypatch.setattr(rc, "_installed_pipelines", lambda: {"other/sft-experiment"})
    [entry] = rc.load_registry_catalog()["entries"]
    assert entry["installed"] is False


def test_a_catalog_without_a_domain_pack_does_not_list_pipelines_to_annotate(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, [{"ref": "x", "kind": "operator_pack", "operators": ["a/b"]}])
    monkeypatch.setattr(rc, "_live_entries", lambda: [])

    def boom():
        raise AssertionError("annotation listed pipelines for a catalog with no domain_pack")

    monkeypatch.setattr(rc, "_installed_pipelines", boom)
    assert rc.load_registry_catalog()["entries"][0]["installed"] is False
