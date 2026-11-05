"""The FEATURE_PROOF.md builder (``_receipts.build_report``) — no server, so not ``e2e``.

A feature is PROVED only when a cell recorded it and no cell of its test failed; a failed cell
anywhere in the feature's scope makes the row FAILED with the cell named; a crash row reads
its own crash test's cells; a cell-scoped feature (``runtime.dbos``) ignores failures in the
cells that cannot prove it.
"""
from __future__ import annotations

import json
from pathlib import Path

from tests.e2e.hello._receipts import build_report

CLS = "tests.e2e.hello.test_matrix"


def _junit(tmp: Path, cases: list[tuple[str, str, str]]) -> Path:
    body = []
    for name, outcome, msg in cases:
        inner = {"failed": f'<failure message="{msg}"/>', "skipped": f'<skipped message="{msg}"/>'}.get(outcome, "")
        cls = "tests.e2e.hello.test_crash" if name.startswith("test_killed") or "dbos_resumes" in name else CLS
        body.append(f'<testcase classname="{cls}" name="{name}">{inner}</testcase>')
    path = tmp / "junit.xml"
    path.write_text(f'<testsuites><testsuite>{"".join(body)}</testsuite></testsuites>')
    return path


def _receipts(tmp: Path, rows: list[dict]) -> Path:
    path = tmp / "receipts.jsonl"
    path.write_text("".join(json.dumps({"pipeline": "p", "proved": True, **r}) + "\n" for r in rows))
    return path


def _row(report: str, key: str) -> str:
    return next(line for line in report.splitlines() if line.startswith(f"| `{key}` |"))


def test_a_feature_proved_in_one_cell_and_failed_in_another_is_failed(tmp_path):
    junit = _junit(tmp_path, [
        ("test_pipeline[sqlite-inprocess-inprocess-check-and-publish]", "passed", ""),
        ("test_pipeline[postgres-inprocess-inprocess-check-and-publish]", "failed", "boom: the audit row is missing"),
    ])
    rec = _receipts(tmp_path, [{"feature": "pipeline.data", "cell": "sqlite-inprocess-inprocess", "evidence": "ok",
                                "test": "test_pipeline[sqlite-inprocess-inprocess-check-and-publish]"}])
    row = _row(build_report(junit, rec), "pipeline.data")
    assert "FAILED" in row and "postgres-inprocess-inprocess" in row and "audit row is missing" in row
    assert "PROVED" not in row.replace("NOT PROVED", "")


def test_a_failed_crash_cell_shows_its_reason_on_the_crash_row(tmp_path):
    junit = _junit(tmp_path, [
        ("test_killed_mid_flow_leg[sqlite-dbos-inprocess]", "failed", "the slow leg must settle failed"),
        ("test_killed_mid_flow_leg[sqlite-inprocess-inprocess]", "passed", ""),
    ])
    rec = _receipts(tmp_path, [{"feature": "crash.mid-leg", "cell": "sqlite-inprocess-inprocess", "evidence": "ok",
                                "test": "test_killed_mid_flow_leg[sqlite-inprocess-inprocess]"}])
    row = _row(build_report(junit, rec), "crash.mid-leg")
    assert "FAILED" in row and "sqlite-dbos-inprocess" in row and "must settle failed" in row


def test_a_cell_scoped_feature_ignores_cells_that_cannot_prove_it(tmp_path):
    junit = _junit(tmp_path, [
        ("test_pipeline[sqlite-inprocess-inprocess-ask-the-docs]", "failed", "unrelated"),
        ("test_pipeline[sqlite-dbos-inprocess-ask-the-docs]", "passed", ""),
    ])
    rec = _receipts(tmp_path, [{"feature": "runtime.dbos", "cell": "sqlite-dbos-inprocess", "evidence": "wf SUCCESS",
                                "test": "test_pipeline[sqlite-dbos-inprocess-ask-the-docs]"}])
    report = build_report(junit, rec)
    assert "wf SUCCESS" in _row(report, "runtime.dbos") and "FAILED" not in _row(report, "runtime.dbos")
    assert "FAILED" in _row(report, "pipeline.agentic")


def test_an_unproved_feature_names_why(tmp_path):
    junit = _junit(tmp_path, [("test_pipeline[sqlite-inprocess-inprocess-check-and-publish]", "passed", "")])
    rec = _receipts(tmp_path, [{"feature": "event.emit", "cell": "sqlite-inprocess-inprocess",
                                "evidence": "#2605: no emits", "proved": False}])
    report = build_report(junit, rec)
    assert "NOT PROVED — #2605: no emits" in _row(report, "event.emit")
    assert "Model provider:" in report and "model_stub.py" in report


def test_a_receipt_from_a_case_that_failed_later_proves_nothing(tmp_path):
    """A cell can record `gate.person` and then fail a later assertion: that receipt must not
    prove the row, nor appear among its cells (PR #2656 review)."""
    junit = _junit(tmp_path, [
        ("test_pipeline[sqlite-inprocess-inprocess-check-and-publish]", "passed", ""),
        ("test_pipeline[sqlite-inprocess-inprocess-ask-the-docs]", "failed", "later assertion"),
    ])
    rec = _receipts(tmp_path, [{"feature": "gate.person", "cell": "sqlite-inprocess-inprocess",
                                "evidence": "from the failed case",
                                "test": "test_pipeline[sqlite-inprocess-inprocess-ask-the-docs]"}])
    row = _row(build_report(junit, rec), "gate.person")
    assert "from the failed case" not in row and "NOT PROVED" in row


def test_receipts_without_a_junit_prove_nothing(tmp_path):
    """The run died before pytest wrote junit (or it is from an earlier run): no row reads
    PROVED from receipts alone (PR #2656 review)."""
    rec = _receipts(tmp_path, [{"feature": "pipeline.data", "cell": "sqlite-inprocess-inprocess", "evidence": "ok",
                                "test": "test_pipeline[sqlite-inprocess-inprocess-check-and-publish]"}])
    row = _row(build_report(tmp_path / "missing.xml", rec), "pipeline.data")
    assert "NOT PROVED" in row
