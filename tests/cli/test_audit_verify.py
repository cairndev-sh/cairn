"""``cairn audit verify`` (ADR-0156 D2) — read-only check of the audit hash chain.

Run against logs the REAL writer (``audit/store``) produced.
"""
from __future__ import annotations

import json
from typer.testing import CliRunner

from cairn.cli.main import app
from operator_audit_store.operators.audit_store import AuditStoreOp
from operator_audit_store.schemas.audit_store import AuditWriteInput

from cairn.core.operator import OperationContext

runner = CliRunner()


async def _write(path, n=3, *, monkeypatch):
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(path))
    op = AuditStoreOp()
    for i in range(n):
        await op.run(AuditWriteInput(action="x.y", target=f"t{i}", ts=f"2026-10-28T10:00:0{i}+00:00"),
                     OperationContext(pipeline_id="p", node_name="n"))


async def test_a_log_audit_store_wrote_verifies(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, monkeypatch=monkeypatch)
    before = log.read_bytes()
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(log)])
    assert r.exit_code == 0, r.output
    assert "rows:      3" in r.output and "chain OK" in r.output
    assert "2026-10-28T10:00:00+00:00" in r.output and "2026-10-28T10:00:02+00:00" in r.output
    head = json.loads(log.read_text().splitlines()[-1])["hash"]
    assert head in r.output
    assert log.read_bytes() == before                       # read-only


async def test_json_has_the_documented_keys(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, monkeypatch=monkeypatch)
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(log), "--json"])
    assert r.exit_code == 0, r.output
    out = json.loads(r.output)
    assert set(out) == {"path", "rows", "first_ts", "last_ts", "head_hash", "ok", "broken"}
    assert out["ok"] is True and out["rows"] == 3 and out["broken"] == []
    assert out["first_ts"].endswith("00+00:00") and out["last_ts"].endswith("02+00:00")


async def test_an_edited_row_is_named_and_exits_2(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, 4, monkeypatch=monkeypatch)
    lines = log.read_text().splitlines()
    row = json.loads(lines[2])
    row["target"] = "edited"
    lines[2] = json.dumps(row)
    log.write_text("\n".join(lines) + "\n")
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(log)])
    assert r.exit_code == 2
    assert "row 3" in r.output and "hash" in r.output
    j = json.loads(runner.invoke(app, ["audit", "verify", "--audit-log", str(log), "--json"]).output)
    assert j["ok"] is False and j["broken"] == [{"row": 3, "reason": "hash"}]


async def test_a_corrupt_line_exits_2(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, 2, monkeypatch=monkeypatch)
    log.write_text(log.read_text() + "{this is not json\n")
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(log)])
    assert r.exit_code == 2 and "corrupt line" in r.output


def test_a_missing_log_exits_1(tmp_path):
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(tmp_path / "nope.jsonl")])
    assert r.exit_code == 1 and "no audit log" in r.output


def test_an_unreadable_log_exits_1(tmp_path):
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(tmp_path)])   # a directory
    assert r.exit_code == 1


def test_an_empty_log_verifies_with_zero_rows(tmp_path):
    log = tmp_path / "audit.jsonl"
    log.write_text("")
    r = runner.invoke(app, ["audit", "verify", "--audit-log", str(log), "--json"])
    assert r.exit_code == 0 and json.loads(r.output)["rows"] == 0


async def test_the_env_var_and_the_default_path_are_honoured(tmp_path, monkeypatch):
    log = tmp_path / "audit.jsonl"
    await _write(log, monkeypatch=monkeypatch)
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    assert runner.invoke(app, ["audit", "verify"]).exit_code == 0
    monkeypatch.delenv("CAIRN_AUDIT_LOG")
    monkeypatch.delenv("OBS_AUDIT_LOG", raising=False)
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["audit", "verify"]).exit_code == 0       # ./audit.jsonl
