"""Dataset references resolve within the run's workspace.

A ``dataset:<name>[@<version>|@latest]`` ref, and every by-name read of the
registry, answers in one workspace: the one named, else the one in scope (a
request's, or a run's scoped from its record). Another workspace's name, pinned
version or head reads as "not found". Any workspace may register under any
name (one version sequence per name); its versions stay invisible to the others.
With auth on, run-time resolution and leg registration outside any workspace
scope are refused (``WorkspaceScopeRequired``). Single-node and auth-off
deployments, where everything is ``default``, are unchanged.

Hermetic: sqlite in-memory, no network.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from types import SimpleNamespace

import pytest

from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.storage.artifacts import (
    SqliteArtifactStore,
    resolve_dataset_ref,
    resolve_dataset_refs_in_config,
)
from cairn.core.workspace import WorkspaceScopeRequired, workspace_scope


def _desc(h: str, uri: str, **stats) -> ArtifactDescriptor:
    return ArtifactDescriptor(content_hash=h, kind="dataset", uri=uri, stats=stats)


@pytest.fixture()
async def store(monkeypatch):
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)
    s = SqliteArtifactStore(":memory:")
    for h, uri in (("hash-a1", "s3://a/facts/one.jsonl"), ("hash-a2", "s3://a/facts/two.jsonl"),
                   ("hash-b1", "s3://b/mine/one.jsonl"), ("hash-x", "s3://x/x.jsonl")):
        await s.register(_desc(h, uri), producer_run_id="run-0")
    # the deployment's (default) dataset, registered with nothing in scope: as today
    assert await s.register_dataset_version("facts", "hash-a1") == 1
    assert await s.register_dataset_version("facts", "hash-a2") == 2
    # a tenant's own dataset
    with workspace_scope("ws-b"):
        assert await s.register_dataset_version("mine", "hash-b1") == 1
    yield s
    await s.close()


def _legacy_row(s: SqliteArtifactStore, name: str, version: int, artifact_id: str, ws: str) -> None:
    """Write another workspace's version of ``name`` directly, at a chosen number
    (as rows written before this change, or by another writer, may sit)."""
    s._conn.execute(
        "INSERT INTO dataset_versions (name, version, artifact_id, created_at, created_by, workspace_id) "
        "VALUES (?,?,?,?,?,?)", (name, version, artifact_id, "2026-01-01T00:00:00+00:00", "legacy", ws))
    s._conn.commit()


# ── resolution ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ref", ["dataset:facts", "dataset:facts@latest", "dataset:facts@1",
                                 "dataset:facts@2"])
async def test_a_tenant_run_cannot_resolve_another_workspaces_dataset(store, ref):
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError, match="not found in the registry"):
            await resolve_dataset_ref(store, ref)
        with pytest.raises(KeyError):
            await resolve_dataset_refs_in_config(store, {"p": ref})
        with pytest.raises(KeyError):
            await resolve_dataset_refs_in_config(store, {"sources": ["s3://ok", ref]})
    # an explicit workspace is honoured the same way
    with pytest.raises(KeyError):
        await resolve_dataset_ref(store, ref, workspace_id="ws-b")


async def test_unknown_and_foreign_refs_read_the_same(store):
    """"Another workspace's" and "does not exist" are indistinguishable."""
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError) as foreign:
            await resolve_dataset_ref(store, "dataset:facts@1")
        with pytest.raises(KeyError) as missing:
            await resolve_dataset_ref(store, "dataset:nope@1")
    assert str(foreign.value).replace("facts", "X") == str(missing.value).replace("nope", "X")


async def test_each_workspace_resolves_its_own(store):
    with workspace_scope("ws-b"):
        got = await resolve_dataset_ref(store, "dataset:mine@latest")
        assert (got["version"], got["content_hash"]) == (1, "hash-b1")
    got = await resolve_dataset_ref(store, "dataset:facts")
    assert (got["version"], got["content_hash"]) == (2, "hash-a2")
    with pytest.raises(KeyError):
        await resolve_dataset_ref(store, "dataset:mine")          # default can't read the tenant's
    with workspace_scope("default"):
        out = await resolve_dataset_refs_in_config(store, {"p": "dataset:facts@1", "n": 3})
    assert out == {"p": "s3://a/facts/one.jsonl#sha256=hash-a1", "n": 3}


async def test_latest_is_the_workspaces_own_head_even_with_a_newer_foreign_row(store):
    _legacy_row(store, "facts", 3, "hash-x", "ws-b")
    got = await resolve_dataset_ref(store, "dataset:facts@latest")
    assert (got["version"], got["content_hash"]) == (2, "hash-a2")
    with pytest.raises(KeyError):
        await resolve_dataset_ref(store, "dataset:facts@3")       # pinned foreign version
    with workspace_scope("ws-b"):
        got = await resolve_dataset_ref(store, "dataset:facts")
        assert got["version"] == 3                                 # the legacy row stays its writer's
        with pytest.raises(KeyError):
            await resolve_dataset_ref(store, "dataset:facts@2")


# ── listing and per-version reads ────────────────────────────────────────────


async def test_listing_and_meta_reads_are_scoped(store):
    _legacy_row(store, "facts", 3, "hash-x", "ws-b")
    assert [v["version"] for v in await store.list_dataset_versions("facts")] == [2, 1]
    assert [d["name"] for d in await store.list_datasets()] == ["facts"]
    assert (await store.list_datasets())[0]["latest_version"] == 2
    assert (await store.get_dataset_version_meta("facts"))["version"] == 2
    assert await store.get_dataset_version_meta("facts", 3) is None
    assert await store.get_dataset_version("facts", 3) is None
    with workspace_scope("ws-b"):
        assert [v["version"] for v in await store.list_dataset_versions("facts")] == [3]
        assert sorted(d["name"] for d in await store.list_datasets()) == ["facts", "mine"]
        assert await store.get_dataset_version_meta("facts", 1) is None
    # explicit workspace_id wins over the scope
    assert [v["version"] for v in await store.list_dataset_versions("mine", workspace_id="ws-b")] == [1]
    assert [d["name"] for d in await store.list_datasets(workspace_id="ws-b")] == ["facts", "mine"]


async def test_a_tenant_cannot_change_another_workspaces_version(store):
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError):
            await store.set_dataset_version_meta("facts", 1, role="test")
        with pytest.raises(KeyError):
            await store.set_dataset_version_verdict("facts", 2, verdict="blocked")
    row = await store.get_dataset_version_meta("facts", 2)
    assert row["role"] == "training" and row["verdict"] is None
    # the owner still can
    assert (await store.set_dataset_version_verdict("facts", 2, verdict="admitted"))["verdict"] == "admitted"


async def test_meta_and_verdict_writes_take_an_explicit_workspace(store):
    """``workspace_id`` names the workspace the write answers in, whatever is in scope."""
    with pytest.raises(KeyError):
        await store.set_dataset_version_meta("facts", 1, role="test", workspace_id="ws-b")
    with pytest.raises(KeyError):
        await store.set_dataset_version_verdict("facts", 2, verdict="blocked", workspace_id="ws-b")
    assert (await store.get_dataset_version_meta("facts", 1))["role"] == "training"
    # from the default scope, the tenant's own version by explicit workspace
    got = await store.set_dataset_version_verdict("mine", 1, verdict="degraded", workspace_id="ws-b")
    assert got["verdict"] == "degraded"
    got = await store.set_dataset_version_meta("mine", 1, role="test", workspace_id="ws-b")
    assert got["role"] == "test"
    row = await store.get_dataset_version_meta("mine", 1, workspace_id="ws-b")
    assert (row["role"], row["verdict"]) == ("test", "degraded")
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError):
            await store.set_dataset_version_verdict("facts", 2, verdict="blocked", workspace_id="ws-b")
        assert (await store.set_dataset_version_verdict(
            "facts", 2, verdict="admitted", workspace_id="default"))["verdict"] == "admitted"


# ── registration ─────────────────────────────────────────────────────────────


async def test_registration_under_a_name_another_workspace_uses(store):
    """Any workspace may use any name. Its versions take the name's next numbers
    and stay invisible to the other workspace, whose head does not move."""
    with workspace_scope("ws-b"):
        assert await store.register_dataset_version("facts", "hash-x") == 3
        got = await resolve_dataset_ref(store, "dataset:facts@latest")
        assert (got["version"], got["content_hash"]) == (3, "hash-x")
        assert [v["version"] for v in await store.list_dataset_versions("facts")] == [3]
        with pytest.raises(KeyError):
            await resolve_dataset_ref(store, "dataset:facts@2")
    assert await store.register_dataset_version("facts", "hash-b1", workspace_id="ws-b") == 4
    # the deployment's view is unchanged
    assert [v["version"] for v in await store.list_dataset_versions("facts")] == [2, 1]
    got = await resolve_dataset_ref(store, "dataset:facts@latest")
    assert (got["version"], got["content_hash"]) == (2, "hash-a2")
    with pytest.raises(KeyError):
        await resolve_dataset_ref(store, "dataset:facts@3")
    assert (await store.list_datasets())[0]["latest_version"] == 2
    # and the other way round: the deployment uses a tenant's name
    assert await store.register_dataset_version("mine", "hash-x") == 2
    with workspace_scope("ws-b"):
        got = await resolve_dataset_ref(store, "dataset:mine")
        assert (got["version"], got["content_hash"]) == (1, "hash-b1")


async def test_reregistration_compares_with_the_workspaces_own_head(store):
    """The identical-bytes no-op compares with the registering workspace's own
    head, never another workspace's row; the number continues past every row."""
    _legacy_row(store, "facts", 3, "hash-x", "ws-b")
    assert await store.register_dataset_version("facts", "hash-x") == 4
    assert await store.register_dataset_version("facts", "hash-x") == 4
    with workspace_scope("ws-b"):
        assert await store.register_dataset_version("facts", "hash-x") == 3   # its own head
        assert await store.register_dataset_version("facts", "hash-b1") == 5
    rows = {v["version"]: v["workspace_id"] for v in await store.list_dataset_versions("facts")}
    assert rows == {4: "default", 2: "default", 1: "default"}
    with workspace_scope("ws-b"):
        assert [v["version"] for v in await store.list_dataset_versions("facts")] == [5, 3]


async def test_reregistering_the_head_changes_only_the_workspaces_own_row(store):
    """The identical-bytes no-op updates the declaration of the registering
    workspace's own head and nothing of another workspace's version."""
    store._conn.execute(
        "INSERT INTO dataset_versions (name, version, artifact_id, created_at, created_by, "
        "workspace_id, role, parent_version, split_policy) VALUES (?,?,?,?,?,?,?,?,?)",
        ("facts", 3, "hash-x", "2026-01-01T00:00:00+00:00", "w", "ws-b", "test",
         "dataset:src@1", '{"seed": 1}'))
    store._conn.commit()
    assert await store.register_dataset_version(
        "facts", "hash-a2", role="validation", parent_version="dataset:other@9",
        split_policy={"seed": 99}) == 2
    with workspace_scope("ws-b"):
        theirs = await store.get_dataset_version_meta("facts", 3)
    assert (theirs["role"], theirs["parent_version"], theirs["split_policy"]) == (
        "test", "dataset:src@1", {"seed": 1})
    mine = await store.get_dataset_version_meta("facts", 2)
    assert (mine["role"], mine["parent_version"], mine["split_policy"]) == (
        "validation", "dataset:other@9", {"seed": 99})
    with workspace_scope("ws-b"):
        assert await store.register_dataset_version("facts", "hash-x", role="validation") == 3
    assert (await store.get_dataset_version_meta("facts", 2))["parent_version"] == "dataset:other@9"


class _RacingConn:
    """A connection whose first version INSERT finds the number already taken
    by another process writing the same file."""

    def __init__(self, conn, other) -> None:
        self._conn, self._other, self.raced = conn, other, 0

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("INSERT INTO dataset_versions") and not self.raced:
            self.raced = 1
            self._other.execute(
                "INSERT INTO dataset_versions (name, version, artifact_id, created_at, created_by, "
                "workspace_id) VALUES (?,?,?,?,?,?)",
                (params[0], params[1], "hash-a1", "2026-01-01T00:00:00+00:00", "other", "ws-c"))
            self._other.commit()
        return self._conn.execute(sql, params)

    def __getattr__(self, item):
        return getattr(self._conn, item)


async def test_a_version_number_taken_by_another_process_is_retried(tmp_path):
    import sqlite3

    path = str(tmp_path / "a.db")
    s = SqliteArtifactStore(path)
    await s.register(_desc("hash-a1", "s3://a/1"), producer_run_id="r")
    await s.register(_desc("hash-x", "s3://x"), producer_run_id="r")
    assert await s.register_dataset_version("facts", "hash-a1") == 1
    other = sqlite3.connect(path)
    s._conn = _RacingConn(s._conn, other)
    try:
        assert await s.register_dataset_version("facts", "hash-x") == 3   # 2 went to the other writer
        assert s._conn.raced == 1
        assert [v["version"] for v in await s.list_dataset_versions("facts")] == [3, 1]
    finally:
        other.close()
        await s.close()


async def test_default_workspace_registration_is_unchanged(store):
    assert await store.register_dataset_version("facts", "hash-a2") == 2   # identical head: no bump
    assert await store.register_dataset_version("facts", "hash-x") == 3
    assert await store.register_dataset_version("new", "hash-x") == 1
    assert (await resolve_dataset_ref(store, "dataset:new"))["version"] == 1


# ── auth on, nothing in scope ────────────────────────────────────────────────


async def test_auth_on_resolution_outside_any_workspace_scope_is_refused(store, monkeypatch):
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    with pytest.raises(WorkspaceScopeRequired):
        await resolve_dataset_ref(store, "dataset:facts")
    with pytest.raises(WorkspaceScopeRequired):
        await resolve_dataset_refs_in_config(store, {"p": "dataset:facts@1"})
    # no refs: nothing to resolve, nothing refused
    assert await resolve_dataset_refs_in_config(store, {"p": "s3://x"}) == {"p": "s3://x"}
    # a non-dataset value is not a ref
    assert await resolve_dataset_ref(store, "s3://x") is None
    with workspace_scope("default"):
        assert (await resolve_dataset_ref(store, "dataset:facts"))["version"] == 2
    with workspace_scope("ws-b"):
        with pytest.raises(KeyError):
            await resolve_dataset_ref(store, "dataset:facts")


async def test_leg_registration_lands_in_the_runs_workspace(store):
    from cairn.core.dispatch.leg_executor import _register_dataset_versions

    d = _desc("hash-x", "s3://x/x.jsonl", register_as="pairs")
    out: dict = {}
    with workspace_scope("ws-b"):
        await _register_dataset_versions([d], store, "run-b", out)
        assert out["dataset_versions"] == ["pairs@1"]
        assert (await resolve_dataset_ref(store, "dataset:pairs"))["content_hash"] == "hash-x"
    assert await store.get_dataset_version("pairs") is None
    # a tenant leg exporting under a name the deployment uses gets its own version
    d2 = _desc("hash-x", "s3://x/x.jsonl", register_as="facts")
    out2: dict = {}
    with workspace_scope("ws-b"):
        await _register_dataset_versions([d2], store, "run-b", out2)
    assert out2["dataset_versions"] == ["facts@3"]
    assert (await resolve_dataset_ref(store, "dataset:facts"))["content_hash"] == "hash-a2"


async def test_auth_on_leg_registration_outside_any_workspace_scope_is_refused(store, monkeypatch):
    from cairn.core.dispatch.leg_executor import _register_dataset_versions

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    d = _desc("hash-x", "s3://x/x.jsonl", register_as="pairs")
    with pytest.raises(WorkspaceScopeRequired):
        await _register_dataset_versions([d], store, "run-x", {})
    with workspace_scope("default"):
        assert await store.get_dataset_version("pairs") is None


# ── the launch doors read the run's own version ──────────────────────────────


async def test_intake_door_checks_the_runs_own_head(store):
    """A newer row of another workspace must not hide the run's own @latest
    from the intake door (it used to be skipped as "not this workspace's")."""
    from cairn.core.intake import check_intake

    await store.set_dataset_version_verdict("facts", 2, verdict="blocked", reasons=["bad rows"])
    _legacy_row(store, "facts", 3, "hash-x", "ws-b")
    gate = await check_intake(["dataset:facts"], store=store, workspace_id="default")
    assert gate.checked and [v.ref for v in gate.versions] == ["dataset:facts@2"]
    assert not gate.ok
    # a tenant's launch door does not read the deployment's verdicts by name
    gate_b = await check_intake(["dataset:facts@2"], store=store, workspace_id="ws-b")
    assert not gate_b.checked and gate_b.ok


# ── the core dataset watch reads the schedule's workspace ────────────────────


async def test_dataset_watch_reads_the_schedules_workspace(store, monkeypatch):
    import cairn.core.operator as op
    from cairn.core.dispatch import scheduler

    async def _close():
        return None

    monkeypatch.setattr(op, "build_artifact_store", lambda: SimpleNamespace(
        list_dataset_versions=store.list_dataset_versions, close=_close))

    async def _no_write(row, updates):
        return None

    monkeypatch.setattr(scheduler, "_write_watch_state", _no_write)
    mine = SimpleNamespace(id="s1", workspace_id="default", watch_state={})
    other = SimpleNamespace(id="s2", workspace_id="ws-b", watch_state={})
    assert await scheduler._probe_core_watch(mine, {"dataset": "facts"}) == {"version": 2}
    assert await scheduler._probe_core_watch(other, {"dataset": "facts"}) is None


# ── a pipeline task leg, through the real bridge ─────────────────────────────


async def test_a_tenant_pipeline_leg_cannot_read_another_workspaces_dataset(store, monkeypatch):
    import cairn.core.storage.artifacts as A
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
    from cairn.core.storage import build_storage
    from cairn.core.storage.pipeline_store import InMemoryPipelineStore
    from tests.core.test_task_leg_dataset_refs import _YAML, _Echo  # noqa: F401 — registers the op
    from tests.core.test_trigger_server_dispatch import FakeRunner

    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Echo, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass
    monkeypatch.setattr(A, "build_artifact_store", lambda: store)

    async def _run(inputs: dict, workspace_id: str):
        d = parse_pipeline_def(_YAML, pack="t1687", source="<test>")
        dispatcher = RunDispatcher(FakeRunner(), backend=build_storage())
        bridge = PipelineBridge(dispatcher, store=InMemoryPipelineStore())
        await bridge.start()
        try:
            rec = await bridge.start_pipeline(plan_pipeline(d), inputs=inputs, workspace_id=workspace_id)
            deadline = asyncio.get_event_loop().time() + 8.0
            while True:
                rec = await bridge.store.get(rec.pipeline_id)
                if rec.status in ("done", "failed"):
                    break
                assert asyncio.get_event_loop().time() < deadline, rec.status
                await asyncio.sleep(0.02)
            return rec, await dispatcher.store.get(rec.step_state["replay"].run_id)
        finally:
            bridge.stop()

    rec, run = await _run({"sources": ["dataset:facts@1"], "dataset_uri": "dataset:mine"}, "ws-b")
    assert rec.status == "failed"
    assert "dataset:facts@1" in str(run.error)
    assert run.result_payloads == []

    rec, run = await _run({"sources": ["s3://plain"], "dataset_uri": "dataset:mine"}, "ws-b")
    assert rec.status == "done", rec.trigger.get("failure_reason")
    assert run.result_payloads[0]["output"]["dataset_uri"] == "s3://b/mine/one.jsonl#sha256=hash-b1"

    rec, run = await _run({"sources": ["dataset:facts@1"], "dataset_uri": "s3://x"}, "default")
    assert rec.status == "done", rec.trigger.get("failure_reason")
    assert run.result_payloads[0]["output"]["sources"] == ["s3://a/facts/one.jsonl#sha256=hash-a1"]
