"""Which location a ``dataset:`` ref resolves to (ADR-0149).

A version registered from a control-plane path keeps that path as its ``uri``
(provenance). When the bytes were also copied to the object store, the
registration carries ``durable_uri`` (reachability). Resolution hands out:

* the registered location, for a leg that runs where that path is readable
  (a co-located leg, a task leg) — unchanged;
* the durable copy, when there is one and either the leg's engine runs the job
  elsewhere (``prefer_durable``: the SSH engine declares
  ``needs_durable_inputs``) or the registered local path no longer exists.

Each workspace reads its own registration (ADR-0148): another workspace's
durable copy of the same bytes is never handed out.

Hermetic: sqlite in-memory, no network (the byte relay is a stub).
"""
from __future__ import annotations

import pytest

import cairn.core.storage.artifacts as A
from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.storage.artifacts import (
    SqliteArtifactStore,
    resolve_dataset_ref,
    resolve_dataset_refs_in_config,
)
from cairn.core.workspace import workspace_scope

DURABLE = "s3://bucket/artifacts/hash-1/train.jsonl"


def _desc(h: str, uri: str) -> ArtifactDescriptor:
    return ArtifactDescriptor(content_hash=h, kind="dataset", uri=uri)


@pytest.fixture()
async def store(monkeypatch):
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)
    s = SqliteArtifactStore(":memory:")
    yield s
    await s.close()


@pytest.fixture()
def local(tmp_path):
    f = tmp_path / "train.jsonl"
    f.write_text("{}\n")
    return f


def _relay_to(monkeypatch, target: str) -> None:
    async def _relay(uri, content_hash):
        return target

    monkeypatch.setattr(A, "_relay_bytes", _relay)


async def _register(store, monkeypatch, uri: str, durable: str, *, ws: str = "default") -> None:
    _relay_to(monkeypatch, durable)
    await store.register(_desc("hash-1", uri), workspace_id=ws)
    with workspace_scope(ws):
        await store.register_dataset_version("train", "hash-1")


async def test_a_co_located_leg_still_gets_the_local_location(store, monkeypatch, local):
    uri = f"file://{local}"
    await _register(store, monkeypatch, uri, DURABLE)
    assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == uri
    cfg = await resolve_dataset_refs_in_config(store, {"dataset_uri": "dataset:train@1"})
    assert cfg["dataset_uri"] == f"{uri}#sha256=hash-1"


async def test_a_remote_leg_gets_the_durable_copy(store, monkeypatch, local):
    uri = f"file://{local}"
    await _register(store, monkeypatch, uri, DURABLE)
    out = await resolve_dataset_ref(store, "dataset:train@1", prefer_durable=True)
    assert out["uri"] == DURABLE and out["content_hash"] == "hash-1"
    cfg = await resolve_dataset_refs_in_config(
        store, {"dataset_uri": "dataset:train", "sources": ["dataset:train@1"]}, prefer_durable=True)
    assert cfg["dataset_uri"] == f"{DURABLE}#sha256=hash-1"
    assert cfg["sources"] == [f"{DURABLE}#sha256=hash-1"]
    # the registration itself is unchanged: where the bytes were made stays recorded
    rec = await store.get("hash-1")
    assert (rec.uri, rec.durable_uri) == (uri, DURABLE)


@pytest.mark.parametrize("form", ["file://{}", "{}"])
async def test_a_missing_local_file_falls_back_to_the_durable_copy(store, monkeypatch, tmp_path, form):
    """The 2026-10-23 shape: the registered path was under a cleaned /tmp."""
    gone = form.format(tmp_path / "gone" / "train.jsonl")
    await _register(store, monkeypatch, gone, DURABLE)
    assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == DURABLE


async def test_without_a_durable_copy_the_registered_location_is_returned(store, monkeypatch, tmp_path):
    gone = f"file://{tmp_path / 'gone.jsonl'}"
    await _register(store, monkeypatch, gone, "")
    assert (await resolve_dataset_ref(store, "dataset:train"))["uri"] == gone
    assert (await resolve_dataset_ref(store, "dataset:train", prefer_durable=True))["uri"] == gone


async def test_a_durable_copy_filled_by_a_later_registration_is_used(store, monkeypatch, local):
    """First registered with no object store configured; a later registration of
    the same bytes with the store on fills the durable copy, and a remote leg
    then gets it."""
    uri = f"file://{local}"
    await _register(store, monkeypatch, uri, "")
    _relay_to(monkeypatch, DURABLE)
    await store.register(_desc("hash-1", uri), workspace_id="default")
    assert (await resolve_dataset_ref(store, "dataset:train", prefer_durable=True))["uri"] == DURABLE


async def test_another_workspaces_durable_copy_is_not_handed_out(store, monkeypatch, local):
    """Workspace scoping holds: ws-b registered the same bytes with no durable
    copy of its own, so even a remote leg resolves ws-b's own location."""
    await _register(store, monkeypatch, f"file://{local}", DURABLE, ws="ws-a")
    await _register(store, monkeypatch, "file:///srv/b/train.jsonl", "", ws="ws-b")
    with workspace_scope("ws-a"):
        assert (await resolve_dataset_ref(store, "dataset:train", prefer_durable=True))["uri"] == DURABLE
    with workspace_scope("ws-b"):
        got = await resolve_dataset_ref(store, "dataset:train", prefer_durable=True)
        assert got["uri"] == "file:///srv/b/train.jsonl"


# ── the leg executor asks for the durable form only for an engine that needs it ──


class _Engine:
    def __init__(self, durable: bool | None) -> None:
        if durable is not None:
            self.needs_durable_inputs = durable


@pytest.mark.parametrize("durable,expected", [(True, True), (False, False), (None, False)])
async def test_the_leg_executor_follows_the_engines_declaration(monkeypatch, durable, expected):
    from cairn.core.dispatch.leg_executor import execute_leg

    seen: dict = {}

    async def fake_resolve(store, config, **kw):
        seen.update(kw)
        raise RuntimeError("stop here")
    monkeypatch.setattr(A, "resolve_dataset_refs_in_config", fake_resolve)
    monkeypatch.setattr(A, "build_artifact_store", lambda *a, **k: object())

    out = await execute_leg("flow:p/f", inputs={"dataset_uri": "dataset:train"},
                            engine=_Engine(durable), run_id="run-1")
    assert out.state == "failed" and "stop here" in out.error
    assert seen == {"prefer_durable": expected}
