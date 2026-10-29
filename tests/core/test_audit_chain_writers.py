"""One chain head across writers: every ``audit/store`` append extends the file's own chain.

Two instances on one path (the cached service sink and the per-call node instance),
and a second process, all produce a single chain that ``cairn lineage`` verifies.
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap

from cairn.cli.commands.lineage import _read_chain
from cairn.core.operator import AuditEvent
from operator_audit_store.operators.audit_store import AuditStoreOp


def _ev(action: str) -> AuditEvent:
    return AuditEvent(action=action, outcome="ok", actor="t", target="x", run_id="r",
                      workspace_id="w", ts="t", metadata={})


async def test_two_instances_on_one_path_share_one_chain(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    service, node = AuditStoreOp(), AuditStoreOp()
    await service.record(_ev("service.1"))
    await node.record(_ev("node.1"))
    await service.record(_ev("service.2"))
    rows, broken = _read_chain(log)
    assert [r["action"] for r in rows] == ["service.1", "node.1", "service.2"]
    assert broken == []


async def test_concurrent_instances_keep_one_chain(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    ops = [AuditStoreOp() for _ in range(4)]
    await asyncio.gather(*(op.record(_ev(f"w{i}.{n}")) for n in range(5) for i, op in enumerate(ops)))
    rows, broken = _read_chain(log)
    assert len(rows) == 20 and broken == []


_CHILD = textwrap.dedent("""
    import asyncio, sys
    from cairn.core.operator import AuditEvent
    from operator_audit_store.operators.audit_store import AuditStoreOp

    async def main(tag):
        op = AuditStoreOp()
        for n in (1, 2):
            await op.record(AuditEvent(action=f"{tag}.{n}", outcome="ok", actor="t", target="x",
                                       run_id="r", workspace_id="w", ts="t", metadata={}))
    asyncio.run(main(sys.argv[1]))
""")


async def test_a_second_process_extends_the_same_chain(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    server = AuditStoreOp()
    await server.record(_ev("server.1"))
    subprocess.run([sys.executable, "-c", _CHILD, "cli"], check=True,
                   env={**__import__("os").environ, "CAIRN_AUDIT_LOG": str(log)})
    await server.record(_ev("server.2"))
    rows, broken = _read_chain(log)
    assert [r["action"] for r in rows] == ["server.1", "cli.1", "cli.2", "server.2"]
    assert broken == []


async def test_a_long_row_and_a_corrupt_tail_line_are_handled(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    op = AuditStoreOp()
    # a row wider than the tail read chunk
    await op.record(AuditEvent(action="big", outcome="ok", actor="t", target="x", run_id="r",
                               workspace_id="w", ts="t", metadata={"blob": "x" * 40_000}))
    with log.open("a") as f:
        f.write("not json")                        # a partial line, no newline
    await AuditStoreOp().record(_ev("after"))
    rows, broken = _read_chain(log)
    assert [r.get("action") for r in rows] == ["big", None, "after"]
    assert broken == [1]                           # only the corrupt line itself
