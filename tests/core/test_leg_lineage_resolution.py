"""#1432 — cross-leg lineage: URI parents on a leg's descriptors resolve to the
registry ids of the artifacts stored at those URIs (sqlite store + the leg
executor's resolver)."""
from __future__ import annotations

import pytest

from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.dispatch.leg_executor import _resolve_uri_parents
from cairn.core.storage.artifacts import SqliteArtifactStore


@pytest.mark.asyncio
async def test_find_by_uri_and_parent_resolution(tmp_path):
    store = SqliteArtifactStore(path=str(tmp_path / "a.db"))
    await store.ensure_schema()
    prep_export = ArtifactDescriptor(uri="s3://bkt/dataflow/prep/train-ready.jsonl", content_hash="a" * 64,
                                     size_bytes=10, rows=3)
    pid = await store.register(prep_export, producer_run_id="run-prep")
    assert await store.find_by_uri("s3://bkt/dataflow/prep/train-ready.jsonl") == pid
    assert await store.find_by_uri("s3://bkt/nope") is None

    # the NEXT leg's stage 0 names the S3 export as its parent (a URI) …
    curate0 = ArtifactDescriptor(uri="file:///tmp/wd/stage-00", content_hash="b" * 64,
                                 parents=["s3://bkt/dataflow/prep/train-ready.jsonl", "s3://external/raw.jsonl"])
    curate1 = ArtifactDescriptor(uri="s3://bkt/dataflow/curate/curated.jsonl", content_hash="c" * 64,
                                 parents=["b" * 64])
    await _resolve_uri_parents([curate0, curate1], store)
    # … resolved to the registry id; an unknown external URI is kept verbatim
    assert curate0.parents == [pid, "s3://external/raw.jsonl"]
    assert curate1.parents == ["b" * 64]
    for d in (curate0, curate1):
        await store.register(d, producer_run_id="run-curate")
    # the DAG now walks curate → prep across legs
    lin = await store.lineage("c" * 64, depth=5)
    assert lin is not None
    flat = str(lin)
    assert pid in flat and "b" * 64 in flat


@pytest.mark.asyncio
async def test_a_leg_registers_and_resolves_parents_in_its_runs_workspace(tmp_path, monkeypatch):
    """ADR-0148 — a leg's outputs are owned by its run's workspace (scoped from
    the record), and a URI parent maps to an id only among that workspace's
    artifacts: another workspace's export at the same URI stays a URI."""
    from cairn.core.dispatch.leg_executor import register_leg_descriptors
    from cairn.core.workspace import workspace_scope

    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    store = SqliteArtifactStore(path=str(tmp_path / "a.db"))
    shared_uri = "s3://bkt/dataflow/prep/train-ready.jsonl"
    with workspace_scope("ws-a"):
        theirs = await store.register(ArtifactDescriptor(uri=shared_uri, content_hash="a" * 64))
    with workspace_scope("ws-b"):
        out: dict = {}
        child = ArtifactDescriptor(uri="s3://bkt/b/out.jsonl", content_hash="d" * 64, parents=[shared_uri])
        ids = await register_leg_descriptors([child], store, run_id="run-b", out_stats=out)
        assert child.parents == [shared_uri]           # ws-a's artifact is not resolved
        assert (await store.get(ids[0])).parents == [shared_uri]
    assert await store.get(ids[0], workspace_id="ws-a") is None
    assert (await store.get(ids[0], workspace_id="ws-b")).producer_run_id == "run-b"
    with workspace_scope("ws-a"):
        own = ArtifactDescriptor(uri="s3://bkt/a/out.jsonl", content_hash="e" * 64, parents=[shared_uri])
        await register_leg_descriptors([own], store, run_id="run-a", out_stats={})
        assert own.parents == [theirs]
    await store.close()
