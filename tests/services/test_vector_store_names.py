"""Vector-store table names stay distinct when collection names are long.

Postgres keeps only the first 63 bytes of an identifier. A collection name is
``[ws_<workspace>__]rag_<corpus>__<embedder fingerprint>``, so two long corpus
names of one workspace — or one corpus under two embedders — could otherwise
share a table. pgvector stores a name longer than 63 bytes under a readable head
plus a hash of the whole name; SQLite has no such limit and keeps names as they
are. No database is needed: these test the name builders.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from cairn.core.workspace import MAX_WORKSPACE_ID_LEN, workspace_collection
from cairn.services.rag.stores import pgvector as pg
from cairn.services.rag.stores import sqlite_vec as sv

WS = "w" * MAX_WORKSPACE_ID_LEN
NAME = "x" * 47  # a long corpus name (the cap is 48)


def _coll(corpus: str, fingerprint: str = "ab12cd34", workspace: str = WS) -> str:
    return workspace_collection(f"rag_{corpus}__{fingerprint}", workspace)


def _kept(name: str) -> str:
    """What Postgres keeps of an identifier."""
    return name.encode("utf-8")[:63].decode("utf-8", "ignore")


def test_a_name_within_63_bytes_is_used_as_is():
    short = _coll("handbook", workspace="acme")
    assert len(short) <= 63
    assert pg.pg_table_name(short) == short
    assert pg._index_name(short, "embedding_idx") == f"{short}_embedding_idx"[:63]


def test_long_names_sharing_their_first_63_bytes_get_distinct_tables_and_indexes():
    a, b = _coll(NAME + "a"), _coll(NAME + "b")
    assert a[:63] == b[:63] and a != b  # what Postgres alone would merge
    ta, tb = pg.pg_table_name(a), pg.pg_table_name(b)
    assert len(ta) <= 63 and len(tb) <= 63 and _kept(ta) != _kept(tb)
    assert ta.startswith(a[:30])  # still readable
    ia = {pg._index_name(ta, s) for s in ("embedding_idx", "source_idx")}
    ib = {pg._index_name(tb, s) for s in ("embedding_idx", "source_idx")}
    assert len(ia | ib) == 4 and all(len(i) <= 63 for i in ia | ib)


def test_the_embedder_fingerprint_survives_in_a_long_name():
    one, two = _coll(NAME, "aaaa1111"), _coll(NAME, "bbbb2222")
    assert _kept(pg.pg_table_name(one)) != _kept(pg.pg_table_name(two))


def test_the_sql_uses_the_bounded_names():
    t = pg.pg_table_name(_coll(NAME))
    assert f"CREATE TABLE IF NOT EXISTS {t} " in pg._create_table_sql(t, 4)
    assert pg._index_name(t, "embedding_idx") in pg._create_index_sql(t)
    assert pg._index_name(t, "source_idx") in pg._create_source_index_sql(t)


def test_sqlite_keeps_long_names_whole_so_they_stay_distinct():
    a, b = _coll(NAME + "a"), _coll(NAME + "b")
    assert f"EXISTS {a} " in sv._create_companion_table_sql(a)
    assert f"EXISTS {a}_vec " in sv._create_vec_table_sql(a, 4)
    assert sv._create_companion_table_sql(a) != sv._create_companion_table_sql(b)


def test_a_62_or_63_byte_table_gets_distinct_index_names():
    for size in (62, 63):
        t = "t" * size
        names = {pg._index_name(t, "embedding_idx"), pg._index_name(t, "source_idx")}
        assert len(names) == 2 and t not in names and all(len(n) <= 63 for n in names), (size, names)
    t61 = "t" * 61
    assert pg._index_name(t61, "embedding_idx") == f"{t61}_embedding_idx"[:63]  # unchanged up to 61 bytes


def test_sqlite_creates_a_collection_once_per_store(tmp_path):
    import asyncio

    import pytest

    pytest.importorskip("sqlite_vec")
    store = sv.SqliteVecStore(path=str(tmp_path / "v.db"))
    calls = []
    real = store._initialize_sync
    store._initialize_sync = lambda table, dim: (calls.append(table), real(table, dim))

    async def _go():
        for _ in range(3):
            await store.initialize("c_one", 4)
            assert store._collection == "c_one"  # a memoised call still selects
            await store.initialize("c_two", 4)
            assert store._collection == "c_two"
        await store.close()
        await store.initialize("c_one", 4)  # a closed store checks again

    asyncio.run(_go())
    assert calls == ["c_one", "c_two", "c_one"]
