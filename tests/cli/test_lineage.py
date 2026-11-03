"""``cairn lineage`` — read the provenance record(s) back from the audit chain (#146).

End-to-end against the REAL writer: we write a ``lineage.record`` row with the
``audit/store`` operator (the same node ``run-lineage-audit`` uses), then read it
with ``cairn lineage``. This proves the reader's hash-chain verification matches
the writer's canonicalization — and that tampering is detected.
"""
from __future__ import annotations

import json

from cairn.cli.commands import lineage as lineage_cmd
from operator_audit_store.operators.audit_store import AuditStoreOp
from operator_audit_store.schemas.audit_store import AuditWriteInput

from cairn.core.operator import OperationContext
import pytest


_RECORD = {
    "model_name": "sql-bot",
    "run_id": "run-xyz-999",
    "dataset": {"ref": "hf://acme/sql", "hash": "sha256:" + "a" * 64},
    "config": {"base_model": "Qwen/Qwen2.5-3B-Instruct", "method": "qlora", "stage": "staging"},
    "adapter_uri": "hf://acme/sql-bot",
    "eval": {"dataset_verdict": "pass", "model_verdict": "warn"},
    "cost": {"estimated_usd": 0.42, "actual_usd": 0.0},
    "promotion": {"registered": True, "model_uri": "local://sql-bot", "stage": "staging", "approver": "alice"},
    "metrics": {"train_loss": 0.3, "eval_loss": 0.4},
}


def _ctx() -> OperationContext:
    return OperationContext(pipeline_id="run-xyz-999", node_name="run-lineage-audit")


async def _write_lineage_row(path, *, target="sql-bot", record=None):
    """Write a real lineage.record row via audit/store (sets OBS_AUDIT_LOG first
    because the op resolves its path at construction)."""
    import os

    os.environ["OBS_AUDIT_LOG"] = str(path)
    op = AuditStoreOp()  # reads OBS_AUDIT_LOG now
    await op.run(
        AuditWriteInput(
            action="lineage.record", outcome="warn", target=target,
            metadata=record if record is not None else _RECORD,
        ),
        _ctx(),
    )


async def test_lineage_detail_for_model(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log)

    rc = lineage_cmd.main(["sql-bot"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sql-bot" in out
    assert "run-xyz-999" in out
    assert "sha256:aaaaaaaaaaaa" in out          # dataset content hash
    # the eval dict is rendered generically as k=v pairs
    assert "dataset_verdict=pass" in out and "model_verdict=warn" in out
    assert "approver=alice" in out
    assert "cairn replay run-xyz-999" in out      # replayable from the record


async def test_lineage_json_and_chain_ok(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log)

    rc = lineage_cmd.main(["sql-bot", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["chain_ok"] is True
    assert payload["records"][0]["model_name"] == "sql-bot"


async def test_lineage_list_all(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log, target="sql-bot")
    await _write_lineage_row(log, target="chat-bot", record={**_RECORD, "model_name": "chat-bot"})

    rc = lineage_cmd.main([])  # no model → list every record
    out = capsys.readouterr().out
    assert rc == 0
    assert "sql-bot" in out and "chat-bot" in out


async def test_tamper_is_detected(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log)

    # Alter the recorded cost without recomputing the chain hash → break detected.
    rows = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    rows[0]["metadata"]["cost"]["estimated_usd"] = 999.0
    log.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    rc = lineage_cmd.main(["sql-bot"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "chain broken" in err


async def _write_non_lineage_log(path):
    """Three real audit/store rows, none a lineage.record."""
    import os

    os.environ["OBS_AUDIT_LOG"] = str(path)
    op = AuditStoreOp()
    for action in ("llm.call", "operation.approval", "tool.call"):
        await op.run(AuditWriteInput(action=action, outcome="ok", target="t", metadata={}), _ctx())


async def test_broken_chain_without_lineage_rows_is_reported(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_non_lineage_log(log)
    rows = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    assert len(rows) == 3
    rows[1]["actor"] = "mallory"  # alter row 1 without recomputing the chain
    log.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    rc = lineage_cmd.main(["--audit-log", str(log)])
    cap = capsys.readouterr()
    assert rc == 2
    assert "no lineage records" in cap.out
    assert "chain broken at row(s) [1]" in cap.err


async def test_intact_chain_without_lineage_rows_still_exits_1(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_non_lineage_log(log)

    rc = lineage_cmd.main(["--audit-log", str(log)])
    cap = capsys.readouterr()
    assert rc == 1
    assert "no lineage records" in cap.out
    assert "chain broken" not in cap.err


async def test_generic_non_model_record(tmp_path, capsys, monkeypatch):
    """The reader is domain-neutral: a record with a totally different shape (an
    incident RCA — no dataset/adapter/model keys) is found, rendered from whatever
    keys it carries, chain-verified, and replayable. The model spine is just the
    first writer of `lineage.record`."""
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    rca = {
        "subject": "INC-4521",
        "run_id": "run-inc-1",
        "alert": {"source": "pagerduty", "severity": "high"},
        "rca": {"root_cause": "bad deploy", "confidence": 0.8},
        "remediation": {"action": "rollback", "approver": "bob"},
    }
    await _write_lineage_row(log, target="INC-4521", record=rca)

    rc = lineage_cmd.main(["INC-4521"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "INC-4521" in out
    assert "source=pagerduty" in out          # rendered generically, no model assumptions
    assert "action=rollback" in out and "approver=bob" in out
    assert "cairn replay run-inc-1" in out


def test_missing_log(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lineage_cmd, "_writer_installed", lambda: True)
    monkeypatch.setenv("OBS_AUDIT_LOG", str(tmp_path / "nope.jsonl"))
    rc = lineage_cmd.main(["sql-bot"])
    assert rc == 1
    assert "no audit log" in capsys.readouterr().err


def test_missing_log_without_the_writer_says_lineage_is_not_installed(tmp_path, capsys, monkeypatch):
    """#2536 — a public install has no lineage operator, so there is no log to
    point at: say what writes lineage records and where the run graph is instead."""
    monkeypatch.setattr(lineage_cmd, "_writer_installed", lambda: False)
    monkeypatch.setenv("OBS_AUDIT_LOG", str(tmp_path / "nope.jsonl"))
    rc = lineage_cmd.main(["run-1"])
    err = capsys.readouterr().err
    assert rc == 1
    assert "written by the platform's lineage operator" in err
    assert "this install does not have" in err
    assert "GET /runs/{id}/lineage" in err and "cairn remote status <run>" in err
    assert "no audit log" not in err and "CAIRN_AUDIT_LOG" not in err


def test_writer_installed_reads_the_lineage_capability_from_the_manifests(monkeypatch):
    """The writer is whatever operator PROVIDES `lineage.record` — the reader never names it."""
    import cairn.core.operator.loader as L

    class _Entry:
        def __init__(self, provides):
            self.provides_capabilities = provides

    monkeypatch.setattr(L, "discover_operator_pack_manifests",
                        lambda: {"audit/store": _Entry(["audit.sink"])})
    assert lineage_cmd._writer_installed() is False
    monkeypatch.setattr(L, "discover_operator_pack_manifests",
                        lambda: {"audit/store": _Entry(["audit.sink"]), "any/writer": _Entry(["lineage.record"])})
    assert lineage_cmd._writer_installed() is True


async def test_expect_head_detects_rows_missing_from_the_tail(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log, target="a")
    await _write_lineage_row(log, target="b")
    head = json.loads(log.read_text().splitlines()[-1])["hash"]
    assert lineage_cmd.main(["--expect-head", head[:12]]) == 0
    capsys.readouterr()

    log.write_text(log.read_text().splitlines()[0] + "\n")   # the last row is gone
    assert lineage_cmd.main(["--expect-head", head[:12]]) == 2
    assert "expected head" in capsys.readouterr().err


async def test_a_broken_chain_does_not_say_why(tmp_path, monkeypatch, capsys):
    """A last line cut mid-write, as a crash leaves it, breaks the chain with nobody altering
    anything, so the message states what was found and no cause (#327)."""
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_non_lineage_log(log)
    text = log.read_text().rstrip("\n")
    log.write_text(text[:-20] + "\n")
    capsys.readouterr()

    assert lineage_cmd.main(["--audit-log", str(log)]) == 2
    err = capsys.readouterr().err
    assert "audit chain broken at row(s) [2]; provenance is NOT trustworthy." in err
    assert "altered" not in err


@pytest.mark.parametrize("value", ["", " ", "abc", "a" * 11])
async def test_an_expect_head_that_cannot_mean_anything_is_refused(tmp_path, capsys, monkeypatch, value):
    """#380: a script that reads its saved head from an empty file must not get a pass on a cut log."""
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log, target="a")
    await _write_lineage_row(log, target="b")
    log.write_text(log.read_text().splitlines()[0] + "\n")   # rows cut from the end
    capsys.readouterr()
    for form in (["--expect-head", value], ["--json", "--expect-head", value]):
        assert lineage_cmd.main(form) == 2
        out = capsys.readouterr()
        assert "Nothing was checked" in out.err and "chain_ok" not in out.out


async def test_the_first_characters_of_a_cut_logs_own_head_are_refused(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_lineage_row(log, target="a")
    await _write_lineage_row(log, target="b")
    log.write_text(log.read_text().splitlines()[0] + "\n")
    last = json.loads(log.read_text().splitlines()[-1])["hash"]
    assert lineage_cmd.main(["--expect-head", last[:1]]) == 2


async def test_a_matching_head_exits_0_in_both_forms_with_no_lineage_records(tmp_path, capsys, monkeypatch):
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("OBS_AUDIT_LOG", str(log))
    await _write_non_lineage_log(log)
    head = json.loads(log.read_text().splitlines()[-1])["hash"]
    assert lineage_cmd.main(["--expect-head", head]) == 0
    assert lineage_cmd.main(["--json", "--expect-head", head]) == 0
    assert lineage_cmd.main(["--expect-head", "f" * 64]) == 2
    assert lineage_cmd.main([]) == 1, "no head asked for: an empty listing keeps its exit"
