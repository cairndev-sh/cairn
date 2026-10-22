"""Artifact registry — content-addressed data artifacts + lineage + tags
(ADR-0081 D2, #1047).

The data-plane substrate: every dataflow output (and, by migration, every
training adapter) becomes an immutable, content-addressed **version** with
write-time stats — so gates judge a dataset without reading it — plus a
lineage DAG (``artifact_parents``) and mutable **tags** (``artifact_tags``),
the ONLY mutable thing and the object HITL promotion attaches to ("may v41
become @prod" — the model-registry stage-promotion shape).

Registration is CONTROL-PLANE-ONLY: engines return
:class:`~cairn.core.dataflow.ArtifactDescriptor`\\ s and this store persists
them — job machines and clusters get no database credentials (ADR-0081 D3).

Backends mirror :mod:`cairn.core.storage.fingerprints` exactly: stdlib-sqlite
(hermetic tests + local single-file), asyncpg Postgres (the existing
``CAIRN_STORAGE`` database), and a ``_Null`` fallback when storage is
unconfigured (registration becomes advisory; nothing breaks).
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Protocol, Sequence

from cairn.core._time import utc_now_iso
from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.dataset_roles import (
    DEFAULT_ROLE,
    GOLDEN_ROLE,
    assert_golden_rows_rated,
    assert_role_allowed_for_origin,
    normalize_column_mapping,
    normalize_dated_at,
    normalize_origin,
    normalize_origin_detail,
    normalize_role,
    normalize_schema,
)
from cairn.core.intake import VERDICTS, normalize_verdict
from cairn.core.pg_schema_init import schema_init_lock
from cairn.core.storage.artifact_bytes import relay as _relay_bytes
from cairn.core.storage.migrations import add_missing_columns
from cairn.core.workspace import DEFAULT_WORKSPACE, current_workspace, scoped_workspace


@dataclass
class ArtifactRecord:
    """One registered artifact version (immutable once written)."""

    id: str                       # content hash — the registry identity
    kind: str
    uri: str
    stats: dict[str, Any] = field(default_factory=dict)
    data_schema: Optional[dict[str, Any]] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    producer_run_id: str = ""
    producer_job: str = ""
    account_id: str = ""
    created_at: str = ""
    parents: list[str] = field(default_factory=list)
    #: #1395 — where the bytes were relayed so they survive the box that made
    #: them (``""`` = no durable copy). ALONGSIDE ``uri``, never instead of it:
    #: ``uri`` is provenance (where it was produced), this is reachability.
    durable_uri: str = ""


class ArtifactStore(Protocol):
    async def ensure_schema(self) -> None: ...
    async def register(
        self, descriptor: ArtifactDescriptor, *,
        producer_run_id: str = "", producer_job: str = "", account_id: str = "",
    ) -> str: ...
    async def get(self, artifact_id: str) -> Optional[ArtifactRecord]: ...
    async def find_by_uri(self, uri: str) -> Optional[str]: ...
    async def list_artifacts(
        self, *, kind: Optional[str] = None, limit: int = 50, offset: int = 0,
    ) -> list[ArtifactRecord]: ...
    async def resolve(self, tag: str) -> Optional[ArtifactRecord]: ...
    async def promote(self, tag: str, artifact_id: str, *, actor: str = "") -> ArtifactRecord: ...
    async def lineage(self, artifact_id: str, *, depth: int = 5) -> Optional[dict]: ...
    async def count(self) -> int: ...
    async def close(self) -> None: ...
    # ─ dataset registry (#1466): named, append-only VERSIONS over registered
    #   artifacts. A version never moves (unlike a tag); re-registering the
    #   artifact already at a name's head is a no-op, not a new version.
    async def register_dataset_version(
        self, name: str, artifact_id: str, *, actor: str = "",
        role: Optional[str] = None, schema: Optional[str] = None,
        column_mapping: Optional[dict] = None, parent_version: str = "",
        split_policy: Optional[dict] = None, workspace_id: Optional[str] = None,
        dated_at: Optional[str] = None, origin: Optional[str] = None,
        origin_detail: Optional[dict] = None,
    ) -> int: ...
    #: Reads by NAME answer within ONE workspace: ``workspace_id``, else the
    #: current one (:func:`~cairn.core.workspace.current_workspace`). Another
    #: workspace's name, version or head reads as "not found". Any workspace may
    #: register under any name: version numbers are one sequence per name (the
    #: key is ``(name, version)``), and each workspace sees only its own versions.
    async def get_dataset_version(
        self, name: str, version: Optional[int] = None, *, workspace_id: Optional[str] = None,
    ) -> Optional[tuple[int, ArtifactRecord]]: ...
    async def list_dataset_versions(self, name: str, *, workspace_id: Optional[str] = None) -> list[dict]: ...
    async def list_datasets(self, *, workspace_id: Optional[str] = None) -> list[dict]: ...
    #: #2020 (ADR-0102 D1) — the role/schema/mapping + lineage sidecar of ONE
    #: version, joined to the artifact's uri so the holdout door can read rows
    #: without a second round trip.
    async def get_dataset_version_meta(
        self, name: str, version: Optional[int] = None, *, workspace_id: Optional[str] = None,
    ) -> Optional[dict]: ...
    #: Every version in ``workspace_id`` carrying one of ``roles`` — the holdout
    #: door's question ("what is held out here?"). Workspace-scoped at the
    #: STORE, so a cross-tenant comparison is not reachable from the door.
    async def list_dataset_versions_by_role(
        self, roles: Sequence[str], *, workspace_id: Optional[str] = None) -> list[dict]: ...
    #: Re-declare a version's PURPOSE without minting a new one — the bytes stay
    #: frozen, what you call them does not. ``golden`` runs the same human-rating
    #: refusal the register path runs.
    #: #2033 adds ``dated_at``: WHEN this version's material is from, which is
    #: what a run's eval cutoff is compared against. ``origin`` is not settable
    #: here — where rows came from is a fact about the bytes, not a label, and a
    #: version that could be re-declared non-synthetic would be a hole straight
    #: through the never-trainable rule.
    async def set_dataset_version_meta(
        self, name: str, version: int, *, role: Optional[str] = None,
        schema: Optional[str] = None, column_mapping: Optional[dict] = None,
        dated_at: Optional[str] = None, workspace_id: Optional[str] = None) -> dict: ...
    #: #2021 (ADR-0102 D1) — stamp the INTAKE VERDICT and its structured report
    #: on a version. Written control-side by the leg that registered it (the
    #: box has no database, ADR-0081 D3); read by the launch door
    #: (:mod:`cairn.core.intake`). Like the role, it is metadata ABOUT the
    #: bytes, so it lands on the existing row — never a second table.
    async def set_dataset_version_verdict(
        self, name: str, version: int, *, verdict: str,
        reasons: Optional[list[str]] = None, report: Optional[dict] = None,
        workspace_id: Optional[str] = None) -> dict: ...
    #: #1969 — human-facing metadata on a dataset NAME (description + labels),
    #: editable at any time, independent of the append-only version rows.
    #: #2020 adds the DECLARED defaults: what this dataset IS (role), the shape
    #: of a row (schema) and its column mapping. A version registered under this
    #: name inherits them unless its producer declares otherwise — which is how
    #: the Import dialog can say "this is a test set" before v1 exists.
    async def set_dataset_meta(
        self, name: str, *, description: Optional[str] = None, labels: Optional[list[str]] = None,
        default_role: Optional[str] = None, default_schema: Optional[str] = None,
        default_column_mapping: Optional[dict] = None,
    ) -> dict: ...
    async def get_dataset_meta(self, name: str) -> dict: ...


# ─── null (unconfigured storage — advisory no-op) ─────────────────────────────


class _NullArtifactStore:
    """No storage configured: register echoes the id so callers proceed;
    reads return nothing; promote fails loudly (a governance action must
    never silently no-op)."""

    async def ensure_schema(self) -> None:
        return None

    async def register(self, descriptor, *, producer_run_id="", producer_job="", account_id="") -> str:
        return descriptor.content_hash

    async def get(self, artifact_id) -> Optional[ArtifactRecord]:
        return None

    async def find_by_uri(self, uri) -> Optional[str]:
        return None

    async def list_artifacts(self, *, kind=None, limit=50, offset=0) -> list[ArtifactRecord]:
        return []

    async def resolve(self, tag) -> Optional[ArtifactRecord]:
        return None

    async def promote(self, tag, artifact_id, *, actor="") -> ArtifactRecord:
        raise RuntimeError("artifact registry not configured (CAIRN_STORAGE unset) — cannot promote")

    async def lineage(self, artifact_id, *, depth=5) -> Optional[dict]:
        return None

    async def count(self) -> int:
        return 0

    async def close(self) -> None:
        return None

    async def register_dataset_version(self, name, artifact_id, *, actor="", role=None,
                                       schema=None, column_mapping=None, parent_version="",
                                       split_policy=None, workspace_id=None, dated_at=None,
                                       origin=None, origin_detail=None) -> int:
        raise RuntimeError("dataset registry not configured (CAIRN_STORAGE unset) — "
                           f"cannot register {name!r} (a named registration must never silently vanish)")

    async def get_dataset_version(self, name, version=None, *, workspace_id=None):
        return None

    async def list_dataset_versions(self, name, *, workspace_id=None) -> list[dict]:
        return []

    async def list_datasets(self, *, workspace_id=None) -> list[dict]:
        return []

    async def get_dataset_version_meta(self, name, version=None, *, workspace_id=None) -> Optional[dict]:
        return None

    async def list_dataset_versions_by_role(self, roles, *, workspace_id=None) -> list[dict]:
        return []

    async def set_dataset_version_meta(self, name, version, *, role=None, schema=None,
                                       column_mapping=None, dated_at=None, workspace_id=None) -> dict:
        raise RuntimeError("dataset registry not configured (CAIRN_STORAGE unset) — "
                           f"cannot set the role of {name!r}@{version}")

    async def set_dataset_version_verdict(self, name, version, *, verdict, reasons=None,
                                          report=None, workspace_id=None) -> dict:
        raise RuntimeError("no artifact storage configured — "
                           f"cannot stamp the intake verdict of {name!r}@{version}")

    async def set_dataset_meta(self, name, *, description=None, labels=None,
                               default_role=None, default_schema=None,
                               default_column_mapping=None) -> dict:
        raise RuntimeError("dataset registry not configured (CAIRN_STORAGE unset) — cannot describe a dataset")

    async def get_dataset_meta(self, name) -> dict:
        return {"name": name, "description": "", "labels": [], "default_role": None,
                "default_schema": None, "default_column_mapping": None}


def _clean_labels(labels: Optional[list[str]]) -> list[str]:
    """Lower-case, trimmed, de-duplicated, order kept; a label is a short token."""
    out: list[str] = []
    for raw in labels or []:
        lb = str(raw).strip().lower()[:40]
        if lb and lb not in out:
            out.append(lb)
    return out


# ─── shared row/record helpers ────────────────────────────────────────────────


_COLS = (
    "id, kind, uri, stats, data_schema, size_bytes, row_count, "
    "producer_run_id, producer_job, account_id, created_at, durable_uri"
)

#: #1395 — the durable byte location, added to an EXISTING ``artifacts`` table in
#: place (``CREATE TABLE IF NOT EXISTS`` is a no-op on an older shape, #255).
#: Nullable: every row written before the relay existed reads back with no
#: durable copy, which is the truth about it.
_ARTIFACT_COLUMNS = {"durable_uri": "TEXT"}


def _record_from_row(row: Sequence, parents: list[str]) -> ArtifactRecord:
    return ArtifactRecord(
        id=row[0], kind=row[1], uri=row[2],
        stats=json.loads(row[3]) if row[3] else {},
        data_schema=json.loads(row[4]) if row[4] else None,
        size_bytes=row[5], row_count=row[6],
        producer_run_id=row[7] or "", producer_job=row[8] or "",
        account_id=row[9] or "", created_at=row[10] or "",
        parents=parents, durable_uri=row[11] or "",
    )


async def _walk_lineage(store: ArtifactStore, artifact_id: str, depth: int, seen: set[str]) -> Optional[dict]:
    """Recursive parent walk shared by both backends — depth-capped and
    cycle-guarded (a content-addressed DAG shouldn't cycle, but a bad publish
    must not hang the API)."""
    rec = await store.get(artifact_id)
    if rec is None:
        return None
    node: dict[str, Any] = {"id": rec.id, "kind": rec.kind, "uri": rec.uri, "parents": []}
    if depth <= 0:
        return node
    for pid in rec.parents:
        if pid in seen:
            continue
        seen.add(pid)
        parent = await _walk_lineage(store, pid, depth - 1, seen)
        if parent is not None:
            node["parents"].append(parent)
    return node


# ─── sqlite (hermetic tests + local single-file) ──────────────────────────────


_CREATE_SQLITE = [
    """
    CREATE TABLE IF NOT EXISTS artifacts (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, uri TEXT NOT NULL,
        stats TEXT, data_schema TEXT, size_bytes INTEGER, row_count INTEGER,
        producer_run_id TEXT, producer_job TEXT, account_id TEXT,
        created_at TEXT NOT NULL, durable_uri TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_parents (
        child_id TEXT NOT NULL, parent_id TEXT NOT NULL,
        PRIMARY KEY (child_id, parent_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_tags (
        name TEXT PRIMARY KEY, artifact_id TEXT NOT NULL,
        updated_at TEXT NOT NULL, updated_by TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_versions (
        name TEXT NOT NULL, version INTEGER NOT NULL, artifact_id TEXT NOT NULL,
        created_at TEXT NOT NULL, created_by TEXT,
        PRIMARY KEY (name, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_meta (
        name TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
        labels TEXT NOT NULL DEFAULT '[]', updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_art_kind ON artifacts(kind, created_at)",
]

#: #2020 (ADR-0102 D1) — role / schema / column_mapping and the frozen-split
#: lineage, added to an EXISTING ``dataset_versions`` in place. Every column has
#: a constant default, so a database written before this migration keeps working
#: and every row it already holds reads back as a ``training`` version in the
#: default workspace — which is what they all were.
_DATASET_VERSION_COLUMNS_SQLITE = {
    "role": f"TEXT NOT NULL DEFAULT '{DEFAULT_ROLE}'",
    "schema": "TEXT",
    "column_mapping": "TEXT",
    "parent_version": "TEXT NOT NULL DEFAULT ''",
    "split_policy": "TEXT",
    "workspace_id": f"TEXT NOT NULL DEFAULT '{DEFAULT_WORKSPACE}'",
    # #2021 (ADR-0102 D1) — the INTAKE VERDICT and its structured report, on
    # the version the clean flow produced. Nullable: a version registered
    # before #2021 has no verdict and the launch door admits it with a note
    # (cairn.core.intake.NO_VERDICT_NOTE) rather than refusing retroactively.
    "verdict": "TEXT",
    "verdict_reasons": "TEXT",
    "verdict_report": "TEXT",
    "verdict_at": "TEXT",
    # #2033 (ADR-0102 D1) — WHEN this version's material is from, and WHERE its
    # rows came from. `dated_at` is what a run's eval cutoff is compared against
    # (cairn.core.eval_cutoff); `origin` carries the never-trainable rule for
    # `synthetic`. Both nullable: a version registered before #2033 is undated
    # and originless, which excludes it from no cutoff and constrains no role.
    "dated_at": "TEXT",
    "origin": "TEXT",
    "origin_detail": "TEXT",
}

#: #2020 — the DECLARED defaults on a dataset NAME. Set at import (before v1
#: exists), inherited by any version registered under the name whose producer
#: declares nothing. Nullable throughout: an undeclared dataset stays undeclared
#: rather than acquiring an invented shape.
_DATASET_META_COLUMNS_SQLITE = {
    "default_role": "TEXT",
    "default_schema": "TEXT",
    "default_column_mapping": "TEXT",
}

_DATASET_META_COLUMNS_PG = {
    "default_role": "TEXT",
    "default_schema": "TEXT",
    "default_column_mapping": "JSONB",
}

_DATASET_VERSION_COLUMNS_PG = {
    "role": f"TEXT NOT NULL DEFAULT '{DEFAULT_ROLE}'",
    "schema": "TEXT",
    "column_mapping": "JSONB",
    "parent_version": "TEXT NOT NULL DEFAULT ''",
    "split_policy": "JSONB",
    "workspace_id": f"TEXT NOT NULL DEFAULT '{DEFAULT_WORKSPACE}'",
    # #2021 — the intake verdict + report (see the sqlite twin above).
    "verdict": "TEXT",
    "verdict_reasons": "JSONB",
    "verdict_report": "JSONB",
    "verdict_at": "TEXT",
    # #2033 (ADR-0102 D1) — WHEN this version's material is from, and WHERE its
    # rows came from. `dated_at` is what a run's eval cutoff is compared against
    # (cairn.core.eval_cutoff); `origin` carries the never-trainable rule for
    # `synthetic`. Both nullable: a version registered before #2033 is undated
    # and originless, which excludes it from no cutoff and constrains no role.
    "dated_at": "TEXT",
    "origin": "TEXT",
    "origin_detail": "JSONB",
}

#: The columns every ``dataset_versions`` read projects, in one place so the two
#: backends cannot drift on order.
_DV_COLS = ("name, version, artifact_id, created_at, created_by, role, schema, "
            "column_mapping, parent_version, split_policy, workspace_id, "
            "verdict, verdict_reasons, verdict_report, verdict_at, "
            "dated_at, origin, origin_detail")


def _version_row(row: Sequence) -> dict[str, Any]:
    """One ``dataset_versions`` row in the shape every reader consumes.

    ``role`` is never ``None`` on the way out (:data:`DEFAULT_ROLE` fills a
    legacy row), and the two JSON columns come back parsed whichever backend
    wrote them (sqlite stores TEXT, Postgres JSONB rendered ``::text``).
    """
    return {
        "name": row[0], "version": int(row[1]), "artifact_id": row[2],
        "created_at": row[3], "created_by": row[4] or "",
        "role": row[5] or DEFAULT_ROLE,
        "schema": row[6] or None,
        "column_mapping": json.loads(row[7]) if row[7] else None,
        "parent_version": row[8] or "",
        "split_policy": json.loads(row[9]) if row[9] else None,
        "workspace_id": row[10] or DEFAULT_WORKSPACE,
        # #2021 — None (not "admitted") when nothing has judged this version:
        # "no verdict" and "admitted" are different states and the door tells
        # them apart (it admits the first WITH a note).
        "verdict": row[11] or None,
        "verdict_reasons": json.loads(row[12]) if row[12] else [],
        "verdict_report": json.loads(row[13]) if row[13] else None,
        "verdict_at": row[14] or None,
        # #2033 — None, never a default date: "undated" is a real state and the
        # cutoff filter treats it differently from "dated before the cutoff".
        "dated_at": row[15] or None,
        "origin": row[16] or None,
        "origin_detail": json.loads(row[17]) if row[17] else None,
    }


#: How often a registration re-reads the next version number after another
#: writer took it (SQLite shared by several processes).
_VERSION_INSERT_ATTEMPTS = 5


def _dataset_workspace(workspace_id: Optional[str]) -> str:
    """The workspace a by-name dataset read or write answers in: the one named,
    else the current one (the request's or the run's)."""
    return (workspace_id or "").strip() or current_workspace() or DEFAULT_WORKSPACE


async def _declare(store, name: str, role, schema, column_mapping) -> tuple:
    """Resolve a registration's declaration: explicit → the NAME's declared
    default → the fallback (``training`` / undeclared).

    The name-level default is what lets the Import dialog say "this is a test
    set" before v1 exists (#2020): the import pipeline registers the cleaned
    export without knowing the role, and the version inherits what the human
    declared at the door. An explicit value from a producer always wins — it
    knows more than the name does.
    """
    if role is None or schema is None or column_mapping is None:
        meta = await store.get_dataset_meta(name)
        role = meta.get("default_role") if role is None else role
        schema = meta.get("default_schema") if schema is None else schema
        column_mapping = (meta.get("default_column_mapping")
                          if column_mapping is None else column_mapping)
    return (normalize_role(role), normalize_schema(schema),
            normalize_column_mapping(column_mapping))


async def _guard_golden(role: Optional[str], name: str, rec: ArtifactRecord,
                        workspace_id: str) -> None:
    """Refuse a ``golden`` write whose rows carry no human rating (ADR-0102 D1).

    Lives here, on the write path of BOTH backends, rather than in a caller: a
    golden set is the ground truth a judge's trust is measured against, so
    "assembled without a human" has to be impossible to register, not merely
    discouraged in the one UI that happens to know the rule.
    """
    if role == GOLDEN_ROLE:
        await assert_golden_rows_rated(name, rec.uri, workspace_id=workspace_id)


class SqliteArtifactStore:
    """Stdlib-``sqlite3`` behind an ``asyncio.Lock`` (mirrors
    :class:`SqliteFingerprintStore`)."""

    def __init__(self, path: str = ":memory:") -> None:
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        for stmt in _CREATE_SQLITE:
            self._conn.execute(stmt)
        # #1395 — bring an already-created artifacts table up to the byte schema.
        add_missing_columns(self._conn, "artifacts", _ARTIFACT_COLUMNS)
        # #2020 — bring an already-created dataset_versions up to the role schema.
        # `CREATE TABLE IF NOT EXISTS` silently does nothing on an older shape
        # (#255), so the columns have to be added explicitly on every connect.
        add_missing_columns(self._conn, "dataset_versions", _DATASET_VERSION_COLUMNS_SQLITE)
        add_missing_columns(self._conn, "dataset_meta", _DATASET_META_COLUMNS_SQLITE)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dsv_role ON dataset_versions(workspace_id, role)")
        self._conn.commit()
        self._lock = asyncio.Lock()

    async def ensure_schema(self) -> None:
        return None  # done in __init__

    async def register(self, descriptor, *, producer_run_id="", producer_job="", account_id="") -> str:
        durable = await _relay_bytes(descriptor.uri, descriptor.content_hash)  # #1395 — outside the lock
        async with self._lock:
            # idempotent on content hash: a re-register of identical bytes is a
            # no-op, never a second version (immutability by construction).
            self._conn.execute(
                f"INSERT OR IGNORE INTO artifacts ({_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    descriptor.content_hash, descriptor.kind, descriptor.uri,
                    json.dumps(descriptor.stats) if descriptor.stats else None,
                    json.dumps(descriptor.data_schema) if descriptor.data_schema else None,
                    descriptor.size_bytes, descriptor.rows,
                    producer_run_id, producer_job, account_id, utc_now_iso(), durable or None,
                ),
            )
            if durable:
                # Fill a HOLE, never overwrite a fact: an existing row that has
                # no durable copy yet gains one (the relay may have been off, or
                # the bucket unreachable, when it was first registered).
                self._conn.execute(
                    "UPDATE artifacts SET durable_uri=? WHERE id=? "
                    "AND (durable_uri IS NULL OR durable_uri='')",
                    (durable, descriptor.content_hash))
            self._conn.executemany(
                "INSERT OR IGNORE INTO artifact_parents (child_id, parent_id) VALUES (?,?)",
                [(descriptor.content_hash, p) for p in descriptor.parents],
            )
            self._conn.commit()
        return descriptor.content_hash

    async def get(self, artifact_id) -> Optional[ArtifactRecord]:
        async with self._lock:
            row = self._conn.execute(
                f"SELECT {_COLS} FROM artifacts WHERE id=?", (artifact_id,)
            ).fetchone()
            if row is None:
                return None
            parents = [r[0] for r in self._conn.execute(
                "SELECT parent_id FROM artifact_parents WHERE child_id=? ORDER BY parent_id",
                (artifact_id,),
            ).fetchall()]
        return _record_from_row(row, parents)

    async def find_by_uri(self, uri) -> Optional[str]:
        """The registry ID of the (most recent) artifact stored at ``uri`` —
        how a downstream leg's INPUT URI becomes a lineage parent (#1432)."""
        async with self._lock:
            row = self._conn.execute(
                "SELECT id FROM artifacts WHERE uri=? ORDER BY created_at DESC LIMIT 1", (uri,)
            ).fetchone()
        return row[0] if row else None

    async def list_artifacts(self, *, kind=None, limit=50, offset=0) -> list[ArtifactRecord]:
        async with self._lock:
            where, params = ("WHERE kind=?", [kind]) if kind else ("", [])
            rows = self._conn.execute(
                f"SELECT {_COLS} FROM artifacts {where} "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        return [_record_from_row(r, []) for r in rows]  # parents omitted in lists (cheap view)

    async def resolve(self, tag) -> Optional[ArtifactRecord]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT artifact_id FROM artifact_tags WHERE name=?", (tag,)
            ).fetchone()
        return await self.get(row[0]) if row else None

    async def promote(self, tag, artifact_id, *, actor="") -> ArtifactRecord:
        rec = await self.get(artifact_id)
        if rec is None:
            raise KeyError(f"artifact {artifact_id!r} not registered — cannot promote to {tag!r}")
        async with self._lock:
            self._conn.execute(
                "INSERT INTO artifact_tags (name, artifact_id, updated_at, updated_by) "
                "VALUES (?,?,?,?) ON CONFLICT(name) DO UPDATE SET "
                "artifact_id=excluded.artifact_id, updated_at=excluded.updated_at, "
                "updated_by=excluded.updated_by",
                (tag, artifact_id, utc_now_iso(), actor),
            )
            self._conn.commit()
        return rec

    async def lineage(self, artifact_id, *, depth=5) -> Optional[dict]:
        return await _walk_lineage(self, artifact_id, depth, {artifact_id})

    async def count(self) -> int:
        async with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0])

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    # ─ dataset registry (#1466) ─
    async def register_dataset_version(self, name, artifact_id, *, actor="", role=None,
                                       schema=None, column_mapping=None, parent_version="",
                                       split_policy=None, workspace_id=None, dated_at=None,
                                       origin=None, origin_detail=None) -> int:
        rec = await self.get(artifact_id)
        if rec is None:
            raise KeyError(f"artifact {artifact_id!r} not registered — cannot version as {name!r}")
        ws = _dataset_workspace(workspace_id)
        role, schema, column_mapping = await _declare(self, name, role, schema, column_mapping)
        origin = normalize_origin(origin)
        # #2033 — the never-trainable rule, at the WRITE. Before the golden
        # guard because it is cheaper and because a caller asking for a
        # synthetic training set should hear the reason, not a rating error.
        assert_role_allowed_for_origin(role, origin)
        dated = normalize_dated_at(dated_at)
        detail = normalize_origin_detail(origin_detail)
        detail_json = json.dumps(detail) if detail else None
        await _guard_golden(role, name, rec, ws)
        async with self._lock:
            # The lock serialises this process; another process on the same file
            # can take the same next number between the read and the insert, so
            # a duplicate key is retried (bounded) with the numbers re-read.
            for attempt in range(_VERSION_INSERT_ATTEMPTS):
                # The no-op compares with THIS workspace's head; the next number
                # is the name's, since (name, version) is the key.
                head = self._conn.execute(
                    "SELECT version, artifact_id FROM dataset_versions WHERE name=? AND workspace_id=? "
                    "ORDER BY version DESC LIMIT 1", (name, ws)).fetchone()
                last = self._conn.execute(
                    "SELECT MAX(version) FROM dataset_versions WHERE name=?", (name,)).fetchone()
                if head and head[1] == artifact_id:
                    # Identical bytes at the head — no new version (immutability by
                    # construction). The DECLARATION still lands: a re-register that
                    # names a role is how a caller says what the head is for, and
                    # dropping it would be the silent-vanish this store refuses to do.
                    self._conn.execute(
                        "UPDATE dataset_versions SET role=?, schema=?, column_mapping=?, "
                        "parent_version=?, split_policy=?, dated_at=COALESCE(?, dated_at), "
                        "origin=COALESCE(?, origin), origin_detail=COALESCE(?, origin_detail) "
                        "WHERE name=? AND version=? AND workspace_id=?",
                        (role, schema,
                         json.dumps(column_mapping) if column_mapping else None,
                         parent_version or "",
                         json.dumps(split_policy) if split_policy else None,
                         dated, origin, detail_json,
                         name, int(head[0]), ws))
                    self._conn.commit()
                    return int(head[0])
                version = (int(last[0]) + 1) if last and last[0] is not None else 1
                try:
                    self._conn.execute(
                        "INSERT INTO dataset_versions (name, version, artifact_id, created_at, "
                        "created_by, role, schema, column_mapping, parent_version, split_policy, "
                        "workspace_id, dated_at, origin, origin_detail) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (name, version, artifact_id, utc_now_iso(), actor, role, schema,
                         json.dumps(column_mapping) if column_mapping else None,
                         parent_version or "",
                         json.dumps(split_policy) if split_policy else None, ws,
                         dated, origin, detail_json))
                    self._conn.commit()
                except sqlite3.IntegrityError:
                    self._conn.rollback()
                    if attempt + 1 == _VERSION_INSERT_ATTEMPTS:
                        raise
                    continue
                return version
        raise AssertionError("unreachable")  # pragma: no cover — the loop returns or raises

    async def get_dataset_version(self, name, version=None, *, workspace_id=None):
        ws = _dataset_workspace(workspace_id)
        async with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT version, artifact_id FROM dataset_versions WHERE name=? AND workspace_id=? "
                    "ORDER BY version DESC LIMIT 1", (name, ws)).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT version, artifact_id FROM dataset_versions "
                    "WHERE name=? AND version=? AND workspace_id=?",
                    (name, int(version), ws)).fetchone()
        if row is None:
            return None
        rec = await self.get(row[1])
        return (int(row[0]), rec) if rec else None

    async def list_dataset_versions(self, name, *, workspace_id=None) -> list[dict]:
        ws = _dataset_workspace(workspace_id)
        async with self._lock:
            rows = self._conn.execute(
                f"SELECT {_DV_COLS} FROM dataset_versions "
                "WHERE name=? AND workspace_id=? ORDER BY version DESC", (name, ws)).fetchall()
        return [_version_row(r) for r in rows]

    async def get_dataset_version_meta(self, name, version=None, *, workspace_id=None) -> Optional[dict]:
        ws = _dataset_workspace(workspace_id)
        async with self._lock:
            if version is None:
                row = self._conn.execute(
                    f"SELECT {_DV_COLS} FROM dataset_versions WHERE name=? AND workspace_id=? "
                    "ORDER BY version DESC LIMIT 1", (name, ws)).fetchone()
            else:
                row = self._conn.execute(
                    f"SELECT {_DV_COLS} FROM dataset_versions "
                    "WHERE name=? AND version=? AND workspace_id=?",
                    (name, int(version), ws)).fetchone()
        if row is None:
            return None
        out = _version_row(row)
        rec = await self.get(out["artifact_id"])
        out["uri"] = rec.uri if rec else ""
        out["row_count"] = rec.row_count if rec else None
        return out

    async def list_dataset_versions_by_role(self, roles, *, workspace_id=None) -> list[dict]:
        wanted = [str(r) for r in roles]
        if not wanted:
            return []
        ws = _dataset_workspace(workspace_id)
        marks = ",".join("?" for _ in wanted)
        async with self._lock:
            rows = self._conn.execute(
                f"SELECT {_DV_COLS} FROM dataset_versions "
                f"WHERE role IN ({marks}) AND workspace_id=? ORDER BY name, version",
                (*wanted, ws)).fetchall()
        out = [_version_row(r) for r in rows]
        for item in out:
            rec = await self.get(item["artifact_id"])
            item["uri"] = rec.uri if rec else ""
        return out

    async def set_dataset_version_meta(self, name, version, *, role=None, schema=None,
                                       column_mapping=None, dated_at=None, workspace_id=None) -> dict:
        cur = await self.get_dataset_version_meta(
            name, int(version), workspace_id=_dataset_workspace(workspace_id))
        if cur is None:
            raise KeyError(f"dataset {name!r}@{version} not found")
        new_role = cur["role"] if role is None else normalize_role(role, default=None)
        new_schema = cur["schema"] if schema is None else normalize_schema(schema)
        new_map = cur["column_mapping"] if column_mapping is None else normalize_column_mapping(column_mapping)
        new_dated = cur.get("dated_at") if dated_at is None else normalize_dated_at(dated_at)
        # #2033 — the RE-DECLARATION seam. The registration guard alone would
        # leave the obvious hole: register synthetic rows as `test`, then patch
        # the role to `training`. Same function, both doors.
        assert_role_allowed_for_origin(new_role, cur.get("origin"))
        rec = await self.get(cur["artifact_id"])
        if rec is not None:
            await _guard_golden(new_role, name, rec, cur["workspace_id"])
        async with self._lock:
            self._conn.execute(
                "UPDATE dataset_versions SET role=?, schema=?, column_mapping=?, dated_at=? "
                "WHERE name=? AND version=? AND workspace_id=?",
                (new_role, new_schema, json.dumps(new_map) if new_map else None, new_dated,
                 name, int(version), cur["workspace_id"]))
            self._conn.commit()
        return {**cur, "role": new_role, "schema": new_schema, "column_mapping": new_map,
                "dated_at": new_dated}

    async def set_dataset_version_verdict(self, name, version, *, verdict, reasons=None,
                                          report=None, workspace_id=None) -> dict:
        cur = await self.get_dataset_version_meta(
            name, int(version), workspace_id=_dataset_workspace(workspace_id))
        if cur is None:
            raise KeyError(f"dataset {name!r}@{version} not found")
        value = normalize_verdict(verdict)
        if value is None:
            raise ValueError("a verdict must be one of " + ", ".join(VERDICTS))
        stamped = utc_now_iso()
        async with self._lock:
            self._conn.execute(
                "UPDATE dataset_versions SET verdict=?, verdict_reasons=?, verdict_report=?, "
                "verdict_at=? WHERE name=? AND version=? AND workspace_id=?",
                (value, json.dumps(list(reasons or [])), json.dumps(report or {}),
                 stamped, name, int(version), cur["workspace_id"]))
            self._conn.commit()
        return {**cur, "verdict": value, "verdict_reasons": list(reasons or []),
                "verdict_report": report or {}, "verdict_at": stamped}

    async def list_datasets(self, *, workspace_id=None) -> list[dict]:
        ws = _dataset_workspace(workspace_id)
        async with self._lock:
            rows = self._conn.execute(
                "SELECT v.name, MAX(v.version), MAX(v.created_at), m.description, m.labels "
                "FROM dataset_versions v LEFT JOIN dataset_meta m ON m.name = v.name "
                "WHERE v.workspace_id=? GROUP BY v.name ORDER BY v.name", (ws,)).fetchall()
        return [{"name": r[0], "latest_version": r[1], "updated_at": r[2],
                 "description": r[3] or "", "labels": json.loads(r[4]) if r[4] else []} for r in rows]

    async def set_dataset_meta(self, name, *, description=None, labels=None,
                               default_role=None, default_schema=None,
                               default_column_mapping=None) -> dict:
        cur = await self.get_dataset_meta(name)
        desc = cur["description"] if description is None else str(description).strip()
        lbs = cur["labels"] if labels is None else _clean_labels(labels)
        role = cur["default_role"] if default_role is None else normalize_role(default_role, default=None)
        schema = cur["default_schema"] if default_schema is None else normalize_schema(default_schema)
        mapping = (cur["default_column_mapping"] if default_column_mapping is None
                   else normalize_column_mapping(default_column_mapping))
        now = datetime.now(timezone.utc).isoformat()
        async with self._lock:
            self._conn.execute(
                "INSERT INTO dataset_meta (name, description, labels, updated_at, default_role, "
                "default_schema, default_column_mapping) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET description=excluded.description, labels=excluded.labels, "
                "updated_at=excluded.updated_at, default_role=excluded.default_role, "
                "default_schema=excluded.default_schema, default_column_mapping=excluded.default_column_mapping",
                (name, desc, json.dumps(lbs), now, role, schema,
                 json.dumps(mapping) if mapping else None))
            self._conn.commit()
        return {"name": name, "description": desc, "labels": lbs, "updated_at": now,
                "default_role": role, "default_schema": schema, "default_column_mapping": mapping}

    async def get_dataset_meta(self, name) -> dict:
        async with self._lock:
            row = self._conn.execute(
                "SELECT description, labels, updated_at, default_role, default_schema, "
                "default_column_mapping FROM dataset_meta WHERE name=?", (name,)).fetchone()
        if not row:
            return {"name": name, "description": "", "labels": [], "updated_at": None,
                    "default_role": None, "default_schema": None, "default_column_mapping": None}
        return {"name": name, "description": row[0] or "",
                "labels": json.loads(row[1]) if row[1] else [], "updated_at": row[2],
                "default_role": row[3] or None, "default_schema": row[4] or None,
                "default_column_mapping": json.loads(row[5]) if row[5] else None}


# ─── postgres (production; the existing CAIRN_STORAGE database) ─────────────────


_CREATE_PG = [
    """
    CREATE TABLE IF NOT EXISTS artifacts (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, uri TEXT NOT NULL,
        stats TEXT, data_schema TEXT, size_bytes BIGINT, row_count BIGINT,
        producer_run_id TEXT, producer_job TEXT, account_id TEXT,
        created_at TEXT NOT NULL, durable_uri TEXT
    )
    """,
    # #1395 — the durable byte location on an EXISTING artifacts table (the
    # sqlite twin is _ARTIFACT_COLUMNS via add_missing_columns).
    *[f"ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS {name} {decl}"
      for name, decl in _ARTIFACT_COLUMNS.items()],
    """
    CREATE TABLE IF NOT EXISTS artifact_parents (
        child_id TEXT NOT NULL, parent_id TEXT NOT NULL,
        PRIMARY KEY (child_id, parent_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_tags (
        name TEXT PRIMARY KEY, artifact_id TEXT NOT NULL,
        updated_at TEXT NOT NULL, updated_by TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_versions (
        name TEXT NOT NULL, version INTEGER NOT NULL, artifact_id TEXT NOT NULL,
        created_at TEXT NOT NULL, created_by TEXT,
        PRIMARY KEY (name, version)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_art_kind ON artifacts(kind, created_at)",
    """
    CREATE TABLE IF NOT EXISTS dataset_meta (
        name TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
        labels JSONB NOT NULL DEFAULT '[]'::jsonb, updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    # #2020 — role schema on an existing table, in place (ADD COLUMN IF NOT
    # EXISTS is a no-op on a fresh database and the migration on an old one).
    *[f"ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS {name} {decl}"
      for name, decl in _DATASET_VERSION_COLUMNS_PG.items()],
    *[f"ALTER TABLE dataset_meta ADD COLUMN IF NOT EXISTS {name} {decl}"
      for name, decl in _DATASET_META_COLUMNS_PG.items()],
    "CREATE INDEX IF NOT EXISTS idx_dsv_role ON dataset_versions(workspace_id, role)",
]

#: Postgres projection: JSONB rendered as text so ``_version_row`` parses both
#: backends identically.
_DV_COLS_PG = ("name, version, artifact_id, created_at, created_by, role, schema, "
               "column_mapping::text, parent_version, split_policy::text, workspace_id, "
               "verdict, verdict_reasons::text, verdict_report::text, verdict_at, "
               "dated_at, origin, origin_detail::text")


class PostgresArtifactStore:
    """asyncpg-backed registry in the existing ``CAIRN_STORAGE`` Postgres.
    The ``id`` primary key serializes concurrent registrations of the same
    content (``ON CONFLICT DO NOTHING`` — same immutability-by-construction
    as sqlite)."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._schema_ready = False

    async def _pool(self):
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415 - shared process pool (#614)
        return await shared_pool(self._dsn)

    async def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        pool = await self._pool()
        async with pool.acquire() as conn, schema_init_lock(conn):  # #2551 — replicas serialise DDL
            for stmt in _CREATE_PG:
                await conn.execute(stmt)
        self._schema_ready = True

    async def register(self, descriptor, *, producer_run_id="", producer_job="", account_id="") -> str:
        await self.ensure_schema()
        durable = await _relay_bytes(descriptor.uri, descriptor.content_hash)  # #1395
        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO artifacts ({_COLS}) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING",
                descriptor.content_hash, descriptor.kind, descriptor.uri,
                json.dumps(descriptor.stats) if descriptor.stats else None,
                json.dumps(descriptor.data_schema) if descriptor.data_schema else None,
                descriptor.size_bytes, descriptor.rows,
                producer_run_id, producer_job, account_id, utc_now_iso(), durable or None,
            )
            if durable:
                # Fill a HOLE, never overwrite a fact (see the sqlite twin).
                await conn.execute(
                    "UPDATE artifacts SET durable_uri=$1 WHERE id=$2 "
                    "AND (durable_uri IS NULL OR durable_uri='')",
                    durable, descriptor.content_hash)
            if descriptor.parents:
                await conn.executemany(
                    "INSERT INTO artifact_parents (child_id, parent_id) VALUES ($1,$2) "
                    "ON CONFLICT DO NOTHING",
                    [(descriptor.content_hash, p) for p in descriptor.parents],
                )
        return descriptor.content_hash

    async def get(self, artifact_id) -> Optional[ArtifactRecord]:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT {_COLS} FROM artifacts WHERE id=$1", artifact_id)
            if row is None:
                return None
            parents = [r["parent_id"] for r in await conn.fetch(
                "SELECT parent_id FROM artifact_parents WHERE child_id=$1 ORDER BY parent_id",
                artifact_id,
            )]
        return _record_from_row(tuple(row), parents)

    async def find_by_uri(self, uri) -> Optional[str]:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM artifacts WHERE uri=$1 ORDER BY created_at DESC LIMIT 1", uri)
        return row["id"] if row else None

    async def list_artifacts(self, *, kind=None, limit=50, offset=0) -> list[ArtifactRecord]:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            if kind:
                rows = await conn.fetch(
                    f"SELECT {_COLS} FROM artifacts WHERE kind=$1 "
                    "ORDER BY created_at DESC LIMIT $2 OFFSET $3", kind, limit, offset,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT {_COLS} FROM artifacts "
                    "ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset,
                )
        return [_record_from_row(tuple(r), []) for r in rows]

    async def resolve(self, tag) -> Optional[ArtifactRecord]:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT artifact_id FROM artifact_tags WHERE name=$1", tag)
        return await self.get(row["artifact_id"]) if row else None

    async def promote(self, tag, artifact_id, *, actor="") -> ArtifactRecord:
        rec = await self.get(artifact_id)
        if rec is None:
            raise KeyError(f"artifact {artifact_id!r} not registered — cannot promote to {tag!r}")
        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO artifact_tags (name, artifact_id, updated_at, updated_by) "
                "VALUES ($1,$2,$3,$4) ON CONFLICT (name) DO UPDATE SET "
                "artifact_id=EXCLUDED.artifact_id, updated_at=EXCLUDED.updated_at, "
                "updated_by=EXCLUDED.updated_by",
                tag, artifact_id, utc_now_iso(), actor,
            )
        return rec

    async def lineage(self, artifact_id, *, depth=5) -> Optional[dict]:
        return await _walk_lineage(self, artifact_id, depth, {artifact_id})

    async def count(self) -> int:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            return int(await conn.fetchval("SELECT COUNT(*) FROM artifacts") or 0)

    async def close(self) -> None:
        return None  # pool is process-shared (#614); not owned here

    # ─ dataset registry (#1466) ─
    async def register_dataset_version(self, name, artifact_id, *, actor="", role=None,
                                       schema=None, column_mapping=None, parent_version="",
                                       split_policy=None, workspace_id=None, dated_at=None,
                                       origin=None, origin_detail=None) -> int:
        rec = await self.get(artifact_id)
        if rec is None:
            raise KeyError(f"artifact {artifact_id!r} not registered — cannot version as {name!r}")
        ws = _dataset_workspace(workspace_id)
        role, schema, column_mapping = await _declare(self, name, role, schema, column_mapping)
        origin = normalize_origin(origin)
        assert_role_allowed_for_origin(role, origin)  # #2033 — see the sqlite twin
        dated = normalize_dated_at(dated_at)
        detail = normalize_origin_detail(origin_detail)
        detail_json = json.dumps(detail) if detail else None
        await _guard_golden(role, name, rec, ws)
        mapping_json = json.dumps(column_mapping) if column_mapping else None
        policy_json = json.dumps(split_policy) if split_policy else None
        pool = await self._pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # One registration per name at a time (held to commit), so two
                # concurrent writers never compute the same next number. See the
                # sqlite twin: the no-op compares with this workspace's head; the
                # next number is the name's.
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                                   f"cairn.dataset_versions:{name}")
                last = await conn.fetchrow(
                    "SELECT version FROM dataset_versions WHERE name=$1 "
                    "ORDER BY version DESC LIMIT 1 FOR UPDATE", name)
                head = await conn.fetchrow(
                    "SELECT version, artifact_id FROM dataset_versions WHERE name=$1 "
                    "AND workspace_id=$2 ORDER BY version DESC LIMIT 1", name, ws)
                if head and head["artifact_id"] == artifact_id:
                    # Identical bytes at the head — no new version; the
                    # DECLARATION still lands (see the sqlite twin).
                    await conn.execute(
                        "UPDATE dataset_versions SET role=$1, schema=$2, column_mapping=$3::jsonb, "
                        "parent_version=$4, split_policy=$5::jsonb, "
                        "dated_at=COALESCE($6, dated_at), origin=COALESCE($7, origin), "
                        "origin_detail=COALESCE($8::jsonb, origin_detail) "
                        "WHERE name=$9 AND version=$10 AND workspace_id=$11",
                        role, schema, mapping_json, parent_version or "", policy_json,
                        dated, origin, detail_json,
                        name, int(head["version"]), ws)
                    return int(head["version"])
                version = (int(last["version"]) + 1) if last else 1
                await conn.execute(
                    "INSERT INTO dataset_versions (name, version, artifact_id, created_at, created_by, "
                    "role, schema, column_mapping, parent_version, split_policy, workspace_id, "
                    "dated_at, origin, origin_detail) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10::jsonb,$11,$12,$13,$14::jsonb)",
                    name, version, artifact_id, utc_now_iso(), actor, role, schema,
                    mapping_json, parent_version or "", policy_json, ws,
                    dated, origin, detail_json)
        return version

    async def get_dataset_version(self, name, version=None, *, workspace_id=None):
        ws = _dataset_workspace(workspace_id)
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            if version is None:
                row = await conn.fetchrow(
                    "SELECT version, artifact_id FROM dataset_versions WHERE name=$1 "
                    "AND workspace_id=$2 ORDER BY version DESC LIMIT 1", name, ws)
            else:
                row = await conn.fetchrow(
                    "SELECT version, artifact_id FROM dataset_versions "
                    "WHERE name=$1 AND version=$2 AND workspace_id=$3",
                    name, int(version), ws)
        if row is None:
            return None
        rec = await self.get(row["artifact_id"])
        return (int(row["version"]), rec) if rec else None

    async def list_dataset_versions(self, name, *, workspace_id=None) -> list[dict]:
        ws = _dataset_workspace(workspace_id)
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_DV_COLS_PG} FROM dataset_versions "
                "WHERE name=$1 AND workspace_id=$2 ORDER BY version DESC", name, ws)
        return [_version_row(tuple(r)) for r in rows]

    async def get_dataset_version_meta(self, name, version=None, *, workspace_id=None) -> Optional[dict]:
        ws = _dataset_workspace(workspace_id)
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            if version is None:
                row = await conn.fetchrow(
                    f"SELECT {_DV_COLS_PG} FROM dataset_versions WHERE name=$1 "
                    "AND workspace_id=$2 ORDER BY version DESC LIMIT 1", name, ws)
            else:
                row = await conn.fetchrow(
                    f"SELECT {_DV_COLS_PG} FROM dataset_versions "
                    "WHERE name=$1 AND version=$2 AND workspace_id=$3",
                    name, int(version), ws)
        if row is None:
            return None
        out = _version_row(tuple(row))
        rec = await self.get(out["artifact_id"])
        out["uri"] = rec.uri if rec else ""
        out["row_count"] = rec.row_count if rec else None
        return out

    async def list_dataset_versions_by_role(self, roles, *, workspace_id=None) -> list[dict]:
        wanted = [str(r) for r in roles]
        if not wanted:
            return []
        await self.ensure_schema()
        ws = _dataset_workspace(workspace_id)
        pool = await self._pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_DV_COLS_PG} FROM dataset_versions "
                "WHERE role = ANY($1::text[]) AND workspace_id=$2 ORDER BY name, version",
                wanted, ws)
        out = [_version_row(tuple(r)) for r in rows]
        for item in out:
            rec = await self.get(item["artifact_id"])
            item["uri"] = rec.uri if rec else ""
        return out

    async def set_dataset_version_meta(self, name, version, *, role=None, schema=None,
                                       column_mapping=None, dated_at=None, workspace_id=None) -> dict:
        cur = await self.get_dataset_version_meta(
            name, int(version), workspace_id=_dataset_workspace(workspace_id))
        if cur is None:
            raise KeyError(f"dataset {name!r}@{version} not found")
        new_role = cur["role"] if role is None else normalize_role(role, default=None)
        new_schema = cur["schema"] if schema is None else normalize_schema(schema)
        new_map = cur["column_mapping"] if column_mapping is None else normalize_column_mapping(column_mapping)
        new_dated = cur.get("dated_at") if dated_at is None else normalize_dated_at(dated_at)
        assert_role_allowed_for_origin(new_role, cur.get("origin"))  # #2033 — see the sqlite twin
        rec = await self.get(cur["artifact_id"])
        if rec is not None:
            await _guard_golden(new_role, name, rec, cur["workspace_id"])
        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE dataset_versions SET role=$1, schema=$2, column_mapping=$3::jsonb, "
                "dated_at=$4 WHERE name=$5 AND version=$6 AND workspace_id=$7",
                new_role, new_schema, json.dumps(new_map) if new_map else None, new_dated,
                name, int(version), cur["workspace_id"])
        return {**cur, "role": new_role, "schema": new_schema, "column_mapping": new_map,
                "dated_at": new_dated}

    async def set_dataset_version_verdict(self, name, version, *, verdict, reasons=None,
                                          report=None, workspace_id=None) -> dict:
        cur = await self.get_dataset_version_meta(
            name, int(version), workspace_id=_dataset_workspace(workspace_id))
        if cur is None:
            raise KeyError(f"dataset {name!r}@{version} not found")
        value = normalize_verdict(verdict)
        if value is None:
            raise ValueError("a verdict must be one of " + ", ".join(VERDICTS))
        stamped = utc_now_iso()
        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE dataset_versions SET verdict=$1, verdict_reasons=$2::jsonb, "
                "verdict_report=$3::jsonb, verdict_at=$4 WHERE name=$5 AND version=$6 "
                "AND workspace_id=$7",
                value, json.dumps(list(reasons or [])), json.dumps(report or {}),
                stamped, name, int(version), cur["workspace_id"])
        return {**cur, "verdict": value, "verdict_reasons": list(reasons or []),
                "verdict_report": report or {}, "verdict_at": stamped}

    async def list_datasets(self, *, workspace_id=None) -> list[dict]:
        ws = _dataset_workspace(workspace_id)
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT v.name, MAX(v.version) AS v, MAX(v.created_at) AS t, "
                "MAX(m.description) AS d, MAX(m.labels::text) AS l "
                "FROM dataset_versions v LEFT JOIN dataset_meta m ON m.name = v.name "
                "WHERE v.workspace_id=$1 GROUP BY v.name ORDER BY v.name", ws)
        return [{"name": r["name"], "latest_version": r["v"], "updated_at": r["t"],
                 "description": r["d"] or "", "labels": json.loads(r["l"]) if r["l"] else []} for r in rows]

    async def set_dataset_meta(self, name, *, description=None, labels=None,
                               default_role=None, default_schema=None,
                               default_column_mapping=None) -> dict:
        await self.ensure_schema()
        cur = await self.get_dataset_meta(name)
        desc = cur["description"] if description is None else str(description).strip()
        lbs = cur["labels"] if labels is None else _clean_labels(labels)
        role = cur["default_role"] if default_role is None else normalize_role(default_role, default=None)
        schema = cur["default_schema"] if default_schema is None else normalize_schema(default_schema)
        mapping = (cur["default_column_mapping"] if default_column_mapping is None
                   else normalize_column_mapping(default_column_mapping))
        now = datetime.now(timezone.utc)
        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO dataset_meta (name, description, labels, updated_at, default_role, "
                "default_schema, default_column_mapping) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7::jsonb) "
                "ON CONFLICT (name) DO UPDATE SET description=EXCLUDED.description, labels=EXCLUDED.labels, "
                "updated_at=EXCLUDED.updated_at, default_role=EXCLUDED.default_role, "
                "default_schema=EXCLUDED.default_schema, default_column_mapping=EXCLUDED.default_column_mapping",
                name, desc, json.dumps(lbs), now, role, schema,
                json.dumps(mapping) if mapping else None)
        return {"name": name, "description": desc, "labels": lbs, "updated_at": now.isoformat(),
                "default_role": role, "default_schema": schema, "default_column_mapping": mapping}

    async def get_dataset_meta(self, name) -> dict:
        await self.ensure_schema()
        pool = await self._pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT description, labels::text AS l, updated_at, default_role, default_schema, "
                "default_column_mapping::text AS m FROM dataset_meta WHERE name=$1", name)
        if row is None:
            return {"name": name, "description": "", "labels": [], "updated_at": None,
                    "default_role": None, "default_schema": None, "default_column_mapping": None}
        return {"name": name, "description": row["description"] or "", "labels": json.loads(row["l"]) if row["l"] else [],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                "default_role": row["default_role"] or None, "default_schema": row["default_schema"] or None,
                "default_column_mapping": json.loads(row["m"]) if row["m"] else None}


# ─── resolver ─────────────────────────────────────────────────────────────────


# ─── dataset refs (#1466): "dataset:<name>[@<version>|@latest]" ──────────────


def parse_dataset_ref(ref: str) -> Optional[tuple[str, Optional[int]]]:
    """``(name, version)`` for a ``dataset:`` ref, ``None`` for anything else
    (plain URIs pass through untouched). ``@latest`` / no suffix → version
    ``None`` (the head). Raises on a malformed version — a typo must fail
    loudly, not fall through to "treat it as a URI"."""
    if not isinstance(ref, str) or not ref.startswith("dataset:"):
        return None
    body = ref[len("dataset:"):].strip()
    name, _, ver = body.partition("@")
    if not name:
        raise ValueError(f"dataset ref {ref!r} has no name")
    if ver in ("", "latest"):
        return name, None
    if not ver.isdigit():
        raise ValueError(f"dataset ref {ref!r}: version must be a number or 'latest'")
    return name, int(ver)


async def resolve_dataset_ref(store: ArtifactStore, ref: str, *,
                              workspace_id: Optional[str] = None) -> Optional[dict]:
    """Resolve a ``dataset:`` ref against the registry: the concrete location
    PLUS the hash the bytes must match — the retrieval rule that makes a name
    a guarantee, not just a pointer (#1466). ``None`` when ``ref`` isn't a
    dataset ref; raises ``KeyError`` when the name/version doesn't exist
    (fail-closed: never silently run on unresolved data).

    The ref resolves within ONE workspace: ``workspace_id``, else the one in
    scope (the run's, scoped from its record). The name, a pinned version and
    ``@latest`` see only that workspace's versions; another workspace's is "not
    found". With auth on and no workspace in scope it raises
    :class:`~cairn.core.workspace.WorkspaceScopeRequired`."""
    parsed = parse_dataset_ref(ref)
    if parsed is None:
        return None
    name, version = parsed
    ws = (workspace_id or "").strip() or scoped_workspace()
    got = await store.get_dataset_version(name, version, workspace_id=ws)
    if got is None:
        raise KeyError(f"dataset ref {ref!r} not found in the registry")
    v, rec = got
    return {"name": name, "version": v, "uri": rec.uri,
            "content_hash": rec.id, "size_bytes": rec.size_bytes}


def _is_dataset_ref(value) -> bool:
    return isinstance(value, str) and value.startswith("dataset:")


def config_has_dataset_ref(config: dict) -> bool:
    """Does this leg config carry anything :func:`resolve_dataset_refs_in_config`
    would rewrite? The caller's "should I resolve?" gate and the rewriter MUST
    agree on what a ref looks like, so they share this one predicate — a gate
    that recognises less than the rewriter silently skips resolution (#1625:
    the list form was invisible to the old inline check)."""
    return any(
        _is_dataset_ref(v) or (isinstance(v, list) and any(_is_dataset_ref(x) for x in v))
        for v in config.values()
    )


async def resolve_dataset_refs_in_config(store: ArtifactStore, config: dict, *,
                                         workspace_id: Optional[str] = None) -> dict:
    """Rewrite every ``dataset:`` ref in a leg's config to its resolved location
    WITH the expected hash as a URI fragment (``s3://…#sha256=<hash>``) — the
    launch-time half of #1466's retrieval rule. The fragment convention rides
    the existing ``$config`` substitution into stage configs with no contract
    change; the FETCHING kernel strips it and verifies the downloaded bytes
    (fail-closed). Values that aren't dataset refs pass through untouched;
    unknown refs raise (KeyError) so a typo'd name fails at LAUNCH, before any
    compute is spent.

    Both a top-level STRING value and a top-level ``list`` of them are rewritten
    (#1625 — ``dataset/replay``'s ``sources`` is a list of prior-cycle versions,
    which used to pass straight through UNRESOLVED to a box with no DB access).
    Deliberately NOT recursive into dicts: the resolved config is substituted
    into stage configs by key, so a ref buried in a nested structure has no
    defined launch-time meaning, and quietly rewriting one would invent a
    contract nobody declared.

    Every ref resolves in one workspace (see :func:`resolve_dataset_ref`).
    """
    out = dict(config)
    if not config_has_dataset_ref(config):
        return out
    ws = (workspace_id or "").strip() or scoped_workspace()
    for key, value in config.items():
        if _is_dataset_ref(value):
            resolved = await resolve_dataset_ref(store, value, workspace_id=ws)
            if resolved is not None:
                out[key] = f"{resolved['uri']}#sha256={resolved['content_hash']}"
        elif isinstance(value, list) and any(_is_dataset_ref(x) for x in value):
            items = list(value)
            for i, item in enumerate(items):
                if _is_dataset_ref(item):
                    r = await resolve_dataset_ref(store, item, workspace_id=ws)
                    if r is not None:
                        items[i] = f"{r['uri']}#sha256={r['content_hash']}"
            out[key] = items
    return out


def build_artifact_store(dsn: Optional[str] = None) -> ArtifactStore:
    """Pick a backend by ``dsn`` (falls back to ``CAIRN_STORAGE``), mirroring
    :func:`cairn.core.storage.fingerprints.build_fingerprint_store`."""
    url = (dsn or getenv("CAIRN_STORAGE", "")).strip()
    if not url:
        return _NullArtifactStore()
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresArtifactStore(dsn=url)
    if url.startswith("sqlite://"):
        # #1471 — ONE parser (settings.sqlite_path_from_url) so every store
        # resolves the same CAIRN_STORAGE to the same file.
        from cairn.core import settings  # noqa: PLC0415 - cycle: settings imports storage on some paths
        return SqliteArtifactStore(path=settings.sqlite_path_from_url(url) or ":memory:")
    return _NullArtifactStore()
