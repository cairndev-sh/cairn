"""#286 — the artifact byte relay copies a DIRECTORY as a tree, as it copies a file.

A LoRA adapter (a training pipeline's candidate) is a directory. Before #286
``relay()`` skipped directories, so a released model's row read ``local`` and
then ``dangling`` once its box was gone. Hermetic: the S3 upload is a recorder,
nothing leaves the host.
"""
from __future__ import annotations

import os

import pytest

import cairn.core.storage.artifact_bytes as B

ROOT = "s3://bucket/pfx/artifacts"


@pytest.fixture()
def puts(monkeypatch):
    """Every (local file, object key) the relay uploads, in order."""
    monkeypatch.setenv("CAIRN_ARTIFACT_STORE", "s3://bucket/pfx")
    monkeypatch.delenv("CAIRN_ARTIFACT_RELAY_MAX_BYTES", raising=False)
    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(B, "_put", lambda src, dest: seen.append((src, dest)))
    return seen


@pytest.fixture()
def adapter(tmp_path):
    d = tmp_path / "adapter"
    (d / "sub").mkdir(parents=True)
    (d / "adapter_config.json").write_text("{}")
    (d / "adapter_model.safetensors").write_bytes(b"\0" * 64)
    (d / "sub" / "tokenizer.json").write_text("{}")
    return d


async def test_a_directory_relays_as_a_tree_under_its_content_addressed_root(puts, adapter):
    got = await B.relay(adapter.as_uri(), "sha256:abc")
    assert got == f"{ROOT}/sha256:abc/adapter"
    assert [(os.path.relpath(s, adapter), d) for s, d in puts] == [
        ("adapter_config.json", f"{got}/adapter_config.json"),
        ("adapter_model.safetensors", f"{got}/adapter_model.safetensors"),
        (os.path.join("sub", "tokenizer.json"), f"{got}/sub/tokenizer.json"),
    ]


async def test_a_file_relays_exactly_as_before(puts, adapter):
    f = adapter / "adapter_config.json"
    assert await B.relay(str(f), "sha256:f") == f"{ROOT}/sha256:f/adapter_config.json"
    assert puts == [(str(f), f"{ROOT}/sha256:f/adapter_config.json")]


async def test_the_cap_counts_the_whole_tree(puts, adapter, monkeypatch):
    monkeypatch.setenv("CAIRN_ARTIFACT_RELAY_MAX_BYTES", "65")   # one file fits, the tree (68 bytes) does not
    assert await B.relay(adapter.as_uri(), "sha256:abc") == "" and puts == []


async def test_a_tree_holding_a_symlink_is_not_relayed(puts, adapter, tmp_path):
    (tmp_path / "outside.txt").write_text("secret")
    (adapter / "sub" / "link").symlink_to(tmp_path / "outside.txt")
    assert await B.relay(adapter.as_uri(), "sha256:abc") == "" and puts == []


async def test_an_empty_directory_and_a_failed_upload_record_no_durable_copy(puts, tmp_path, adapter, monkeypatch):
    (tmp_path / "empty").mkdir()
    assert await B.relay(str(tmp_path / "empty"), "sha256:e") == ""

    def _boom(src, dest):
        raise RuntimeError("bucket down")
    monkeypatch.setattr(B, "_put", _boom)
    assert await B.relay(adapter.as_uri(), "sha256:abc") == ""


async def test_no_store_configured_relays_nothing(puts, adapter, monkeypatch):
    monkeypatch.delenv("CAIRN_ARTIFACT_STORE", raising=False)
    assert await B.relay(adapter.as_uri(), "sha256:abc") == "" and puts == []


async def test_a_relayed_directory_reads_durable_after_its_box_is_gone(puts, adapter, monkeypatch):
    """The acceptance: registered through the real store, the adapter dir is ``durable``
    in the registry health check once the directory itself no longer exists."""
    import shutil

    from cairn.core.dataflow.contract import ArtifactDescriptor
    from cairn.core.storage.artifacts import SqliteArtifactStore

    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)
    store = SqliteArtifactStore(":memory:")
    try:
        await store.register(ArtifactDescriptor(content_hash="sha256:abc", kind="model", uri=adapter.as_uri()))
        assert (await B.registry_health(store))["durable"] == 1
        shutil.rmtree(adapter)
        health = await B.registry_health(store)
        assert (health["durable"], health["dangling"]) == (1, 0), health
    finally:
        await store.close()
