"""Structural tests for the PostgresRunStore + OBS_STORAGE dispatch.

Without a live Postgres instance, we can still verify:

  1. The driver imports + instantiates lazily (no asyncpg import on
     construction).
  2. The lazy pool surfaces a clear error when no DSN is supplied.
  3. ``OBS_STORAGE`` env routes :func:`default_store` to the right impl
     for every scheme (postgres / sqlite / unknown / unset).
  4. The DDL/DML SQL constants compile to plausible shapes.

End-to-end roundtrip (initialize → create → update → append_event →
get_events) is gated on ``PGSTORE_DSN`` being set + the
``storage-postgres`` dep group installed (skipped in CI by default).
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.storage import (
    InMemoryRunStore,
    SqliteRunStore,
    default_store,
)
from cairn.core.storage.postgres.runs import (
    PostgresRunStore,
    _CREATE_EVENTS_SQL,
    _CREATE_RUNS_SQL,
    _INSERT_EVENT_NEXT_SEQ_SQL,
    _INSERT_RUN_SQL,
    _SELECT_EVENTS_SQL,
    _SELECT_RUN_DATA_SQL,
    _UPDATE_RUN_SQL,
)


# ─── Instantiation + lazy pool ────────────────────────────────────────


def test_postgres_run_store_instantiates_without_dep_installed():
    """Constructor doesn't import asyncpg — the dep stays lazy."""
    store = PostgresRunStore(dsn="postgres://nowhere/db")
    assert store._pool is None
    assert store._schema_ready is False


@pytest.mark.asyncio
async def test_get_pool_raises_when_no_dsn():
    """If neither dsn= nor OBS_STORAGE is set, the lazy pool refuses
    loudly instead of silently failing later."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        # Dep missing → error fires from _require_asyncpg first.
        store = PostgresRunStore()
        with pytest.raises(RuntimeError, match="storage-postgres"):
            await store._get_pool()
        return
    # Dep installed → error fires from the DSN check.
    store = PostgresRunStore()
    with pytest.raises(RuntimeError, match="CAIRN_STORAGE=postgres"):
        await store._get_pool()


@pytest.mark.asyncio
async def test_get_pool_raises_clearly_when_dep_missing():
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        store = PostgresRunStore(dsn="postgres://x/y")
        with pytest.raises(RuntimeError, match="storage-postgres"):
            await store._get_pool()
        return
    pytest.skip("storage-postgres is installed; can't test missing-dep branch here")


# ─── SQL shape ────────────────────────────────────────────────────────


def test_create_runs_sql_uses_jsonb():
    """``data`` must be JSONB so future queries (filter by status from JSON,
    etc.) work without extra parsing. (Whitespace-normalized so column
    alignment changes don't break the assertion.)"""
    norm = " ".join(_CREATE_RUNS_SQL.split())
    assert "runs" in norm
    assert "id TEXT PRIMARY KEY" in norm
    assert "workspace_id TEXT NOT NULL DEFAULT 'default'" in norm  # ADR-0051 indexed column
    assert "data JSONB NOT NULL" in norm


def test_create_events_sql_uses_composite_pk():
    """(run_id, seq) PK — seq is per-run monotonic, restart-safe."""
    assert "run_events" in _CREATE_EVENTS_SQL
    assert "PRIMARY KEY (run_id, seq)" in _CREATE_EVENTS_SQL
    assert "data   JSONB NOT NULL" in _CREATE_EVENTS_SQL


def test_insert_run_uses_on_conflict():
    """``create`` is idempotent — same id upsert-updates rather than failing.
    Mirrors SqliteRunStore's INSERT OR REPLACE semantics."""
    norm = " ".join(_INSERT_RUN_SQL.split())
    assert "INSERT INTO runs" in norm
    assert "ON CONFLICT (id) DO UPDATE" in norm
    assert "workspace_id" in norm  # ADR-0051 indexed column
    assert "$7::jsonb" in norm


def test_select_run_data_returns_text_blob():
    """data::text cast lets Pydantic re-parse the JSONB without an
    additional bytes->str step."""
    assert "data::text" in _SELECT_RUN_DATA_SQL


def test_update_run_targets_indexed_columns():
    """The mirror columns (fingerprint/lane/status/created_at + the
    failure_type/cost_baseline_usd/final_verdict diagnosis mirrors, ADR-0059)
    stay in sync with the JSON blob on update — otherwise list-by-status and
    failure filtering drift. Positions are pinned to catch a binding mismatch."""
    norm = " ".join(_UPDATE_RUN_SQL.split())
    for col in ("fingerprint = $2", "lane = $3", "status = $4",
                "workspace_id = $5", "created_at = $6",
                "failure_type = $7::jsonb", "cost_baseline_usd = $8",
                "final_verdict = $9", "data = $10::jsonb"):
        assert col in norm, f"missing: {col!r}"


def test_next_event_seq_coalesces_for_empty_run():
    """COALESCE handles the first-event case without a special branch."""
    assert "COALESCE((SELECT MAX(seq) FROM run_events WHERE run_id = $1), 0)" in _INSERT_EVENT_NEXT_SEQ_SQL


def test_insert_event_uses_jsonb_cast():
    assert "INSERT INTO run_events" in _INSERT_EVENT_NEXT_SEQ_SQL
    assert "$4::jsonb" in _INSERT_EVENT_NEXT_SEQ_SQL


def test_event_insert_allocates_its_own_seq_atomically():
    """#1509 — the seq must be chosen INSIDE the insert, and a lost race
    must be reported (no row) rather than raising a PK violation the
    callers then swallow."""
    assert "ON CONFLICT (run_id, seq) DO NOTHING" in _INSERT_EVENT_NEXT_SEQ_SQL
    assert "RETURNING seq" in _INSERT_EVENT_NEXT_SEQ_SQL


def test_select_events_filters_after_seq_and_orders():
    """Critical for subscribe-with-backlog: after_seq filter + ORDER BY seq
    is what makes the catch-up phase deterministic."""
    assert "WHERE run_id = $1 AND seq > $2" in _SELECT_EVENTS_SQL
    assert "ORDER BY seq ASC" in _SELECT_EVENTS_SQL


# ─── OBS_STORAGE dispatch (the new env knob) ──────────────────────────


def test_obs_storage_unset_keeps_legacy_behavior(monkeypatch):
    """Nothing set → InMemoryRunStore (historical default)."""
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.delenv("OBS_RUN_DB", raising=False)
    s = default_store()
    assert isinstance(s, InMemoryRunStore)


def test_obs_run_db_legacy_knob_still_works(monkeypatch):
    """OBS_RUN_DB unchanged — pre-OBS_STORAGE clients keep working."""
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.setenv("OBS_RUN_DB", ":memory:")
    s = default_store()
    assert isinstance(s, SqliteRunStore)


def test_obs_storage_sqlite_memory(monkeypatch):
    """OBS_STORAGE=sqlite://:memory: → SqliteRunStore(:memory:)."""
    monkeypatch.setenv("OBS_STORAGE", "sqlite://:memory:")
    s = default_store()
    assert isinstance(s, SqliteRunStore)


def test_obs_storage_sqlite_absolute_file_path(tmp_path, monkeypatch):
    """OBS_STORAGE=sqlite://<abs> resolves to that EXACT absolute path (#76).

    Regression guard: the old parser stripped the leading slash, so
    sqlite:///tmp/x.db silently became the *relative* "tmp/x.db" (wrong dir)
    and sqlite:///x.db crashed ("unable to open database file"). The
    :memory: test above never caught it — only a real file path does.
    `sqlite://` + an absolute path (own leading /) == the documented 3-slash
    sqlite:///abs/path form.
    """
    db = tmp_path / "sub" / "runs.db"            # absolute, nested subdir
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{db}")
    s = default_store()
    assert isinstance(s, SqliteRunStore)
    assert s.path == str(db)                      # leading slash intact, not relativized
    s.close()


def test_obs_storage_sqlite_relative_path_stays_relative(tmp_path, monkeypatch):
    """A relative form is preserved verbatim (no spurious slash juggling)."""
    monkeypatch.chdir(tmp_path)                   # keep the created file under tmp
    monkeypatch.setenv("OBS_STORAGE", "sqlite://rel/runs.db")
    s = default_store()
    assert isinstance(s, SqliteRunStore) and s.path == "rel/runs.db"
    s.close()


def test_obs_storage_postgres_routes_to_postgres(monkeypatch):
    """postgres:// scheme → PostgresRunStore (lazy pool — doesn't connect)."""
    monkeypatch.setenv("OBS_STORAGE", "postgres://user:pass@localhost/db")
    s = default_store()
    assert isinstance(s, PostgresRunStore)
    # The DSN flows through to the store
    assert s._dsn == "postgres://user:pass@localhost/db"


def test_obs_storage_postgresql_scheme_also_accepted(monkeypatch):
    """Both postgres:// and postgresql:// schemes route correctly."""
    monkeypatch.setenv("OBS_STORAGE", "postgresql://x/y")
    assert isinstance(default_store(), PostgresRunStore)


@pytest.mark.parametrize("bad", [
    "redis://something",   # unknown scheme
    "sqlite",              # #1855 — the case seen live: bare kind, no URL
    "postgres",            # same shape, other backend
    "/tmp/runs.db",        # a path with no scheme
])
def test_obs_storage_unrecognized_value_is_refused(monkeypatch, bad):
    """#1855 — a SET but unrecognized OBS_STORAGE must fail loudly, naming the
    accepted forms. It used to warn and fall through to in-memory, so
    `OBS_STORAGE=sqlite` silently meant volatile storage: findings evaporated
    on restart, and under DBOS a second default_store() built a second empty
    store and a gated run's approval died with KeyError. Unset is still the
    documented in-memory default — only a set-but-wrong value refuses."""
    monkeypatch.setenv("OBS_STORAGE", bad)
    monkeypatch.delenv("OBS_RUN_DB", raising=False)
    with pytest.raises(ValueError, match="sqlite:///") as ei:
        default_store()
    assert "CAIRN_STORAGE" in str(ei.value) and "#1855" in str(ei.value)


def test_obs_storage_unset_is_still_the_in_memory_default(monkeypatch):
    """The refusal is for SET-but-wrong only; unset keeps the historical
    non-breaking default."""
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.delenv("OBS_RUN_DB", raising=False)
    assert isinstance(default_store(), InMemoryRunStore)


def test_obs_storage_takes_precedence_over_obs_run_db(monkeypatch):
    """When both are set, OBS_STORAGE wins."""
    monkeypatch.setenv("OBS_STORAGE", "postgres://x/y")
    monkeypatch.setenv("OBS_RUN_DB", ":memory:")
    s = default_store()
    assert isinstance(s, PostgresRunStore)


# ─── End-to-end (gated on PGSTORE_DSN + dep) ──────────────────────────


@pytest.fixture
def _pg_dsn():
    dsn = os.environ.get("PGSTORE_DSN")
    if not dsn:
        pytest.skip("PGSTORE_DSN not set — skipping live Postgres integration")
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        pytest.skip("storage-postgres optional dep group not installed")
    return dsn


@pytest.mark.asyncio
async def test_roundtrip_create_get_list_update(_pg_dsn):
    """Live roundtrip against a real Postgres. Run with::

        PGSTORE_DSN='postgresql://user:pass@host/db' \
        pip install 'cairndev[storage-postgres]' \
        pytest tests/core/test_postgres_run_store.py -k roundtrip
    """
    from cairn.core.engine.schema import TriggerEvent

    store = PostgresRunStore(dsn=_pg_dsn)
    from cairn.core.storage import Run
    run = Run(
        id="run-pgtest-1",
        trigger=TriggerEvent(source="pattern", matched_pattern="x", service="svc"),
        lane="realtime", fingerprint="abc", status="queued",
    )
    await store.create(run)
    got = await store.get("run-pgtest-1")
    assert got is not None and got.id == "run-pgtest-1"

    updated = await store.update("run-pgtest-1", status="running")
    assert updated.status == "running"

    listed = await store.list(status="running", limit=10)
    assert any(r.id == "run-pgtest-1" for r in listed)

    await store.close()


@pytest.mark.asyncio
async def test_roundtrip_append_get_events(_pg_dsn):
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.storage import Run

    import uuid
    run_id = f"run-pgtest-events-{uuid.uuid4().hex[:8]}"  # unique per run: a reused database keeps old events (#2551)
    store = PostgresRunStore(dsn=_pg_dsn)
    run = Run(
        id=run_id,
        trigger=TriggerEvent(source="pattern", matched_pattern="x", service="svc"),
        lane="realtime", fingerprint="evt", status="queued",
    )
    await store.create(run)
    e1 = await store.append_event(run_id, "queued", {"k": 1})
    e2 = await store.append_event(run_id, "running", {"k": 2})
    assert e1.seq == 1 and e2.seq == 2

    evs = await store.get_events(run_id)
    assert [e.seq for e in evs] == [1, 2]
    after = await store.get_events(run_id, after_seq=1)
    assert [e.seq for e in after] == [2]

    await store.close()
