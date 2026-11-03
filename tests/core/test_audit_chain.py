"""ADR-0156 D3 — ONE hash rule for the audit chain, in ``cairn.core.obs.audit``.

The ``audit/store`` writer, ``cairn lineage`` and ``cairn audit verify`` all use it; these
tests pin it against what the writer actually wrote and fail if a reader grows its own copy.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from cairn.core.obs.audit import (
    GENESIS_HASH,
    ChainReport,
    canonical,
    chain_hash,
    verify_chain,
)
from operator_audit_store.operators.audit_store import AuditStoreOp
from operator_audit_store.schemas.audit_store import AuditWriteInput

from cairn.core.operator import OperationContext


async def _write(path: Path, n: int, monkeypatch) -> None:
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(path))
    op = AuditStoreOp()
    for i in range(n):
        await op.run(AuditWriteInput(action="a.b", target=f"t{i}", metadata={"i": i}),
                     OperationContext(pipeline_id="p", node_name="n"))


async def test_chain_hash_matches_the_rows_audit_store_wrote(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, 3, monkeypatch)
    prev = GENESIS_HASH
    for line in log.read_text().splitlines():
        row = json.loads(line)
        payload = {k: v for k, v in row.items() if k not in ("prev_hash", "hash")}
        assert row["prev_hash"] == prev
        assert row["hash"] == chain_hash(prev, payload)
        prev = row["hash"]
    report = verify_chain(log.read_text().splitlines())
    assert isinstance(report, ChainReport)
    assert report.ok and report.rows == 3 and report.head_hash == prev


def test_canonical_is_key_order_independent_and_compact():
    assert canonical({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'
    assert canonical({"a": 1, "b": 2}) == canonical({"b": 2, "a": 1})
    assert GENESIS_HASH == "0" * 64


def test_the_operator_sdk_re_exports_the_rule():
    import cairn.core.operator as op

    assert op.chain_hash is chain_hash and op.canonical is canonical
    assert op.verify_chain is verify_chain and op.GENESIS_HASH == GENESIS_HASH


def test_an_empty_log_verifies_with_zero_rows():
    r = verify_chain([])
    assert r.ok and r.rows == 0 and r.head_hash == GENESIS_HASH
    assert r.first_ts is None and r.last_ts is None


async def test_an_edited_row_is_reported_by_position(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, 4, monkeypatch)
    lines = log.read_text().splitlines()
    row = json.loads(lines[1])
    row["target"] = "tampered"
    lines[1] = json.dumps(row)
    r = verify_chain(lines)
    assert not r.ok and (r.broken[0].row, r.broken[0].reason) == (2, "hash")


async def test_a_removed_row_breaks_the_next_link(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, 4, monkeypatch)
    lines = log.read_text().splitlines()
    del lines[1]
    r = verify_chain(lines)
    assert not r.ok and (r.broken[0].row, r.broken[0].reason) == (2, "prev_hash")


def test_a_corrupt_line_is_a_break():
    r = verify_chain(["{not json", "[1, 2]"])
    assert [(b.row, b.reason) for b in r.broken] == [(1, "corrupt line"), (2, "corrupt line")]


def test_no_reader_keeps_its_own_copy_of_the_rule():
    """The rule is defined in cairn.core.obs.audit and nowhere else under src/cairn or the
    audit-store pack (the lineage reader's old `_canonical` copy is gone)."""
    root = Path(__file__).resolve().parents[2]
    defs = re.compile(r"^\s*def (_canonical|canonical|_chain_hash|chain_hash)\(", re.M)
    offenders = []
    for base in (root / "src" / "cairn", root / "packages" / "cairn-operator-audit-store" / "src"):
        for f in base.rglob("*.py"):
            if f.name == "audit.py" and f.parent.name == "obs":
                continue
            text = f.read_text(encoding="utf-8")
            if defs.search(text) or ("prev_hash" in text and "hashlib" in text):
                offenders.append(str(f.relative_to(root)))
    assert offenders == []


def test_lineage_and_the_writer_import_the_shared_helper():
    root = Path(__file__).resolve().parents[2]
    lineage = (root / "src/cairn/cli/commands/lineage.py").read_text()
    writer = (root / "packages/cairn-operator-audit-store/src/operator_audit_store/operators/audit_store.py").read_text()
    verify = (root / "src/cairn/cli/commands/audit.py").read_text()
    assert "from cairn.core.obs.audit import verify_chain" in lineage
    assert "chain_hash" in writer and "GENESIS_HASH" in writer
    assert "from cairn.core.obs.audit import verify_chain" in verify
