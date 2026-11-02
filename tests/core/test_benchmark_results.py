"""The README's benchmark numbers are generated, and each one can be traced (#323)."""
from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark import render
from cairn.cli.commands.lineage import _head_mismatch, _read_chain
from cairn.cli.commands.lineage import _run as _lineage_run
from cairn.core.obs.audit import GENESIS_HASH, chain_hash, verify_chain


def _result(tmp_path: Path, **changes) -> Path:
    (tmp_path / "benchmark").mkdir()
    (tmp_path / "benchmark" / "m.py").write_text("", encoding="utf-8")
    results = tmp_path / "results"
    results.mkdir()
    result = {
        "title": "M",
        "script": "benchmark/m.py",
        "provenance": {
            "cairndev_version": "1.2.3", "uv_lock_sha256": "a" * 64, "git_sha": "b" * 40, "code_clean": True,
            "machine": {"os": "Linux", "arch": "x86_64", "cpus": 4, "python": "3.13.1"},
            "measured_at": "2026-11-02",
        },
        "results": [{"what": "A thing", "result": "1.0 s"}],
    }
    result.update(changes)
    (results / "m.json").write_text(json.dumps(result), encoding="utf-8")
    return results


def test_the_readme_table_is_what_the_results_say() -> None:
    readme = render.README.read_text(encoding="utf-8")
    assert readme == render.replace_block(readme, render.render_block(render.load_results()))


def test_every_published_number_links_its_script_and_its_provenance(tmp_path: Path) -> None:
    block = render.render_block(render.load_results(_result(tmp_path), tmp_path))
    assert "([reproduce](./benchmark/m.py))" in block and "| A thing | 1.0 s |" in block
    for fact in ("1.2.3", "a" * 12, "Linux x86_64", "4 CPUs", "3.13.1", "2026-11-02"):
        assert fact in block


def test_an_edited_number_is_caught(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    results = _result(tmp_path)
    monkeypatch.setattr(render, "RESULTS", results)
    monkeypatch.setattr(render, "REPO", tmp_path)
    monkeypatch.setattr(render.load_results, "__defaults__", (results, tmp_path))
    readme = tmp_path / "README.md"
    monkeypatch.setattr(render, "README", readme)
    monkeypatch.setattr("sys.argv", ["render.py", "--check"])
    good = render.render_block(render.load_results())

    readme.write_text(good, encoding="utf-8")
    assert render.main() == 0
    readme.write_text(good.replace("1.0 s", "0.1 s"), encoding="utf-8")
    assert render.main() == 1
    assert "0.1 s" in readme.read_text(encoding="utf-8"), "--check changes nothing"


@pytest.mark.parametrize("change, refused", [
    ({"provenance": {"cairndev_version": "1"}}, "does not record"),
    ({"provenance": {"cairndev_version": "1", "uv_lock_sha256": "a", "git_sha": "b", "code_clean": False,
                     "machine": {"os": "L", "arch": "x", "cpus": 1, "python": "3"}, "measured_at": "d"}},
     "uncommitted code"),
    ({"script": "benchmark/gone.py"}, "does not exist"),
    ({"results": []}, "no results"),
])
def test_a_result_that_cannot_be_traced_is_refused(tmp_path: Path, change: dict, refused: str) -> None:
    with pytest.raises(ValueError, match=refused):
        render.load_results(_result(tmp_path, **change), tmp_path)


def test_no_results_and_no_markers_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no results"):
        render.load_results(tmp_path, tmp_path)
    with pytest.raises(ValueError, match="no benchmark markers"):
        render.replace_block("a readme with no table", "block")


class _Client:
    """Answers a measurement with one scripted pipeline record."""

    def __init__(self, status: str, steps: dict[str, str]) -> None:
        self.record = {"status": status, "step_state": {k: {"status": v} for k, v in steps.items()}}

    def launch_pipeline(self, *args, **kwargs) -> dict:
        return {"pipeline_id": "pl_x"}

    def pipeline(self, pipeline_id: str) -> dict:
        return self.record


@pytest.fixture
def quickstart_path(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(render.REPO / "benchmark"))
    return importlib.import_module("quickstart_path")


def test_a_pipeline_that_publishes_before_its_gate_fails_the_measurement(quickstart_path) -> None:
    client = _Client("paused", {"review": "awaiting_approval", "publish": "done"})
    with pytest.raises(RuntimeError, match="published before its person gate"):
        quickstart_path._one_run(client)


def test_a_pipeline_that_never_reaches_its_gate_fails_the_measurement(quickstart_path) -> None:
    with pytest.raises(RuntimeError, match="ended failed before review"):
        quickstart_path._one_run(_Client("failed", {"profile": "failed"}))


@pytest.mark.parametrize("status, steps", [
    ("done", {"data-ok": "done", "publish": "done"}),
    ("failed", {"data-ok": "failed", "stage": "done"}),
    ("failed", {"data-ok": "failed", "engine-check": "done"}),
    ("failed", {"data-ok": "failed", "redact": "running"}),
    ("failed", {"data-ok": "failed", "audit": "failed"}),
    ("failed", {"data-ok": "failed", "stage": "queued"}),
])
def test_a_data_gate_that_lets_work_through_fails_the_measurement(quickstart_path, status, steps) -> None:
    rest = {step: "pending" for step in quickstart_path.AFTER_DATA_GATE}
    with pytest.raises(RuntimeError, match="data gate did not stop"):
        quickstart_path._data_gate_refuses(_Client(status, {**rest, **steps}))


def test_a_renamed_step_fails_the_data_gate_check(quickstart_path) -> None:
    steps = {step: "pending" for step in quickstart_path.AFTER_DATA_GATE if step != "engine-check"}
    with pytest.raises(RuntimeError, match="data gate did not stop"):
        quickstart_path._data_gate_refuses(_Client("failed", steps))


def test_a_data_gate_that_stops_the_pipeline_passes(quickstart_path) -> None:
    steps = {step: "pending" for step in quickstart_path.AFTER_DATA_GATE}
    quickstart_path._data_gate_refuses(_Client("failed", {"data-ok": "failed", **steps, "audit": "skipped"}))


def test_a_broken_audit_chain_fails_the_measurement(quickstart_path, tmp_path: Path) -> None:
    log = tmp_path / "audit.jsonl"
    log.write_text('{"not": "a chained row"}\n', encoding="utf-8")
    engine = SimpleNamespace(audit_log=log)
    with pytest.raises(RuntimeError, match="does not verify"):
        quickstart_path._audit_rows(engine, [])


def _chained(tmp_path: Path, rows: list[dict]) -> SimpleNamespace:
    prev, lines = GENESIS_HASH, []
    for row in rows:
        digest = chain_hash(prev, row)
        lines.append(json.dumps({**row, "prev_hash": prev, "hash": digest}))
        prev = digest
    log = tmp_path / "audit.jsonl"
    log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return SimpleNamespace(audit_log=log)


def _gate_row(step: str, actor: str = "dev") -> dict:
    return {"action": "pipeline.gate_approved", "actor": actor, "target": f"pl_x/{step}"}


def test_every_gate_logged_with_the_signed_in_user_passes(quickstart_path, tmp_path: Path) -> None:
    engine = _chained(tmp_path, [_gate_row("review"), _gate_row("promote")])
    assert quickstart_path._audit_rows(engine, ["pl_x"]) == 2


def test_a_stopped_pipeline_missing_from_the_log_fails_the_measurement(quickstart_path, tmp_path: Path) -> None:
    admitted = {"action": "pipeline.admitted", "target": "pl_y"}
    finished = {"action": "pipeline.finished", "target": "pl_y"}
    assert quickstart_path._audit_rows(_chained(tmp_path, [admitted, finished]), [], "pl_y") == 2
    with pytest.raises(RuntimeError, match="launch and end"):
        quickstart_path._audit_rows(_chained(tmp_path, [admitted]), [], "pl_y")


@pytest.mark.parametrize("rows", [
    [{"action": "hello.dataset.published", "actor": "data-owners", "target": "file:///x"}],
    [_gate_row("review")],
    [_gate_row("review"), _gate_row("promote", actor="data-owners")],
])
def test_a_gate_decision_missing_from_the_log_fails_the_measurement(quickstart_path, tmp_path: Path, rows) -> None:
    with pytest.raises(RuntimeError, match="no audit row"):
        quickstart_path._audit_rows(_chained(tmp_path, rows), ["pl_x"])


def test_uncommitted_measured_code_is_recorded(quickstart_path, monkeypatch: pytest.MonkeyPatch) -> None:
    common = importlib.import_module("_common")
    seen: list[tuple[str, ...]] = []

    def git(*args: str) -> str:
        seen.append(args)
        return " M benchmark/quickstart_path.py" if args[0] == "status" else "abc"

    monkeypatch.setattr(common, "_git", git)
    assert common.provenance()["code_clean"] is False
    status = next(a for a in seen if a[0] == "status")
    assert "benchmark" in status and "src" in status and ":(exclude)benchmark/results" in status


@pytest.mark.parametrize("boot_failure", [KeyboardInterrupt, RuntimeError])
def test_an_engine_that_fails_to_boot_is_stopped(quickstart_path, monkeypatch: pytest.MonkeyPatch, boot_failure) -> None:
    common = importlib.import_module("_common")
    killed: list[int] = []

    class _Proc:
        pid = 4242
        returncode = None

        def poll(self):
            raise boot_failure("boot failed")

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(common.subprocess, "Popen", lambda *a, **k: _Proc())
    monkeypatch.setattr(common.os, "killpg", lambda pgid, sig: killed.append(pgid))
    with pytest.raises(boot_failure):
        common.LocalEngine().__enter__()
    assert killed == [4242]


# ─── audit_tamper: the check logic, on scripted answers. What the real commands answer rests on
# the real run in the `public` CI job. ──────────────────────────────────────────────────────────

CAUGHT = ("changed", "deleted", "inserted", "swapped")
LIMITS = ("appended", "truncated", "emptied", "rewritten", "rewritten-all")


@pytest.fixture
def audit_tamper(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(render.REPO / "benchmark"))
    return importlib.import_module("audit_tamper")


@pytest.fixture
def untouched(tmp_path: Path) -> Path:
    """Ten rows with an approval at row 5, and the row that ends the pipeline."""
    legs = [{"action": "leg.finished", "target": f"pl_x/{i}", "ts": f"t{i}"} for i in range(8)]
    rows = [*legs[:4], _gate_row("review"), *legs[4:], {"action": "pipeline.finished", "target": "pl_x"}]
    return _chained(tmp_path, rows).audit_log


@pytest.fixture
def rule(audit_tamper) -> SimpleNamespace:
    """The answers the commands give, from the code behind them, without starting a process each time."""
    def verify(log: Path):
        report = verify_chain(log.read_text(encoding="utf-8").splitlines())
        return audit_tamper.Verdict(0 if report.ok else 2, report.rows, report.head_hash, report.ok,
                                    tuple((b.row, b.reason) for b in report.broken))

    def check_head(log: Path, head: str):
        rows, broken = _read_chain(log)
        refused = bool(broken) or _head_mismatch(rows, head)
        return audit_tamper.HeadVerdict(2 if refused else 0, not refused)

    def head_exit_without_json(log: Path, head: str) -> int:
        return _lineage_run(None, json=False, audit_log=str(log), expect_head=head)

    return SimpleNamespace(verify=verify, check_head=check_head, head_exit=head_exit_without_json)


def _when(stem: str, answer, otherwise):
    """A scripted command: ``answer`` for the file named ``stem``, the real rule for every other."""
    return lambda log, *args: answer(otherwise(log, *args)) if log.stem == stem else otherwise(log, *args)


def test_each_alteration_is_reported_at_its_row_and_each_limit_is_stated(audit_tamper, untouched, rule) -> None:
    results = audit_tamper._measure(untouched, rule.verify, rule.check_head, rule.head_exit)
    answers = {row["what"]: row["result"] for row in results}
    assert "detected: exit 2, row 5 (hash)" == answers["Changed who gave an approval, row 5"]
    assert "detected: exit 2, row 5 (prev_hash)" == answers["Deleted that row"]
    assert "detected: exit 2, rows 5, 6, 7 (prev_hash)" == answers["Swapped it with the next row"]
    assert any(what.startswith("Inserted") and result == "detected: exit 2, row 6 (prev_hash)"
               for what, result in answers.items())
    limits = [what for what in answers if what.startswith("Limit:")]
    assert len(limits) == 9
    assert sum(answers[what].startswith("NOT detected") for what in limits) == 8
    assert answers["Limit: emptied the file"].startswith("NOT detected by the chain")
    appended = answers["Limit: appended a forged approval at the end, correctly linked"]
    assert appended.startswith("NOT detected by the chain") and "only grew" in appended
    assert answers["The same, without `--json`, on a log with no lineage records"].startswith("exit 1")


@pytest.mark.parametrize("stem", CAUGHT)
@pytest.mark.parametrize("wrong, refused", [
    (lambda v: type(v)(0, v.rows, v.head, True, ()), "not detected"),
    (lambda v: type(v)(1, v.rows, v.head, False, v.breaks), "not detected"),
    (lambda v: type(v)(2, v.rows, v.head, True, v.breaks), "not detected"),
    (lambda v: type(v)(2, v.rows, v.head, False, ((v.breaks[0][0] + 1, v.breaks[0][1]), *v.breaks[1:])), "reported"),
    (lambda v: type(v)(2, v.rows, v.head, False, ((v.breaks[0][0], "corrupt line"), *v.breaks[1:])), "reported"),
    (lambda v: type(v)(2, v.rows, v.head, False, v.breaks[:1] + v.breaks[:1]), "reported"),
    (lambda v: type(v)(2, v.rows + 1, v.head, False, v.breaks), "rows, expected"),
], ids=["silent-pass", "wrong-exit", "says-ok", "wrong-row", "wrong-reason", "extra-break", "wrong-count"])
def test_an_alteration_answered_wrongly_fails_the_measurement(audit_tamper, untouched, rule, stem, wrong, refused) -> None:
    with pytest.raises(RuntimeError, match=refused):
        audit_tamper._measure(untouched, _when(stem, wrong, rule.verify), rule.check_head, rule.head_exit)


@pytest.mark.parametrize("stem", LIMITS)
def test_a_limit_the_chain_stops_having_fails_the_measurement(audit_tamper, untouched, rule, stem) -> None:
    detects = lambda v: type(v)(2, v.rows, v.head, False, ((1, "hash"),))  # noqa: E731
    with pytest.raises(RuntimeError, match="expected the chain to verify"):
        audit_tamper._measure(untouched, _when(stem, detects, rule.verify), rule.check_head, rule.head_exit)


@pytest.mark.parametrize("stem", LIMITS)
@pytest.mark.parametrize("exit_code, chain_ok", [(0, True), (2, True), (1, False)])
def test_a_saved_head_that_misses_a_limit_fails_the_measurement(audit_tamper, untouched, rule, stem, exit_code,
                                                                chain_ok) -> None:
    head = audit_tamper.HeadVerdict(exit_code, chain_ok)
    with pytest.raises(RuntimeError, match="not detected with the saved head"):
        audit_tamper._measure(untouched, rule.verify, _when(stem, lambda _: head, rule.check_head), rule.head_exit)


def test_an_untouched_log_that_does_not_verify_fails_the_measurement(audit_tamper, untouched, rule) -> None:
    broken = lambda v: type(v)(2, v.rows, v.head, False, ((1, "hash"),))  # noqa: E731
    with pytest.raises(RuntimeError, match="the untouched log: expected the chain to verify"):
        audit_tamper._measure(untouched, _when("audit", broken, rule.verify), rule.check_head, rule.head_exit)


def test_a_head_that_refuses_the_untouched_log_fails_the_measurement(audit_tamper, untouched, rule) -> None:
    refused = audit_tamper.HeadVerdict(2, False)
    with pytest.raises(RuntimeError, match="the untouched log: expected the head to match"):
        audit_tamper._measure(untouched, rule.verify, _when("audit", lambda _: refused, rule.check_head),
                              rule.head_exit)


def test_the_limits_of_a_saved_head_are_measured_too(audit_tamper, untouched, rule) -> None:
    """The cut log against the head it ends at must match; the full log against that head must not."""
    newest = rule.verify(untouched).head

    def refuses_its_own_head(log: Path, head: str):
        answer = rule.check_head(log, head)
        return audit_tamper.HeadVerdict(2, False) if log.stem == "truncated" and head != newest else answer

    with pytest.raises(RuntimeError, match="rows written after a head was saved"):
        audit_tamper._measure(untouched, rule.verify, refuses_its_own_head, rule.head_exit)

    def accepts_an_older_head(log: Path, head: str):
        answer = rule.check_head(log, head)
        return audit_tamper.HeadVerdict(0, True) if log.stem == "audit" and head != newest else answer

    with pytest.raises(RuntimeError, match="a log that has grown"):
        audit_tamper._measure(untouched, rule.verify, accepts_an_older_head, rule.head_exit)


@pytest.mark.parametrize("value", ["", "  "])
def test_an_empty_head_that_starts_checking_fails_the_measurement(audit_tamper, untouched, rule, value) -> None:
    """The published limit would be out of date, so the docs and the script must change with it."""
    def refuses_it(log: Path, head: str):
        return audit_tamper.HeadVerdict(2, False) if head == value else rule.check_head(log, head)

    with pytest.raises(RuntimeError, match="empty or blank value: expected the head to match"):
        audit_tamper._measure(untouched, rule.verify, refuses_it, rule.head_exit)


def test_a_prefix_match_is_checked_three_ways(audit_tamper, untouched, rule) -> None:
    """The forged log verifies, the prefix accepts it, and the full head refuses it."""
    measure = audit_tamper._measure
    broken = lambda v: type(v)(2, v.rows, v.head, False, ((1, "hash"),))  # noqa: E731
    with pytest.raises(RuntimeError, match="expected the chain to verify"):
        measure(untouched, _when("prefix-matched", broken, rule.verify), rule.check_head, rule.head_exit)

    full = rule.verify(untouched).head

    def refuses_the_prefix(log: Path, head: str):
        return (audit_tamper.HeadVerdict(2, False) if log.stem == "prefix-matched" and head != full
                else rule.check_head(log, head))

    with pytest.raises(RuntimeError, match="expected the head to match"):
        measure(untouched, rule.verify, refuses_the_prefix, rule.head_exit)

    def accepts_the_forgery(log: Path, head: str):
        return (audit_tamper.HeadVerdict(0, True) if log.stem == "prefix-matched"
                else rule.check_head(log, head))

    with pytest.raises(RuntimeError, match="not detected with the saved head"):
        measure(untouched, rule.verify, accepts_the_forgery, rule.head_exit)


def test_a_prefix_nothing_matches_fails_the_measurement(audit_tamper, untouched, monkeypatch) -> None:
    monkeypatch.setattr(audit_tamper, "MAX_TRIES", 2)
    with pytest.raises(RuntimeError, match="no last row matched"):
        audit_tamper._matching_prefix(untouched.read_text(encoding="utf-8").splitlines(), "not-hex")


@pytest.mark.parametrize("exit_code", [0, 2, 3])
def test_a_head_check_without_json_that_stops_exiting_1_fails_the_measurement(audit_tamper, untouched, rule,
                                                                              exit_code) -> None:
    with pytest.raises(RuntimeError, match="without --json"):
        audit_tamper._measure(untouched, rule.verify, rule.check_head, lambda log, head: exit_code)


@pytest.mark.parametrize("rows", [9, 11])
def test_a_chain_that_verifies_with_the_wrong_row_count_fails_the_check(audit_tamper, rows) -> None:
    verdict = audit_tamper.Verdict(0, rows, "h", True, ())
    with pytest.raises(RuntimeError, match=f"read {rows} rows, expected 10"):
        audit_tamper._passes("a log", verdict, rows=10)
    audit_tamper._passes("a log", verdict, rows=rows)


@pytest.mark.parametrize("exit_code, ok, breaks", [(0, False, ()), (2, True, ()), (0, True, ((1, "hash"),))])
def test_a_chain_that_half_verifies_fails_the_check(audit_tamper, exit_code, ok, breaks) -> None:
    with pytest.raises(RuntimeError, match="expected the chain to verify"):
        audit_tamper._passes("a log", audit_tamper.Verdict(exit_code, 10, "h", ok, breaks), rows=10)


@pytest.mark.parametrize("exit_code, chain_ok", [(0, False), (2, True), (2, False), (1, True), (1, False)])
def test_a_head_check_must_both_exit_0_and_say_ok_to_match(audit_tamper, exit_code, chain_ok) -> None:
    with pytest.raises(RuntimeError, match="expected the head to match"):
        audit_tamper._head_matches("a log", audit_tamper.HeadVerdict(exit_code, chain_ok))
    audit_tamper._head_matches("a log", audit_tamper.HeadVerdict(0, True))


@pytest.mark.parametrize("exit_code, chain_ok", [(2, True), (0, False), (0, True), (1, False), (1, True)])
def test_a_head_check_must_both_exit_2_and_say_not_ok_to_refuse(audit_tamper, exit_code, chain_ok) -> None:
    with pytest.raises(RuntimeError, match="not detected with the saved head"):
        audit_tamper._head_refused("a log", audit_tamper.HeadVerdict(exit_code, chain_ok))
    audit_tamper._head_refused("a log", audit_tamper.HeadVerdict(2, False))


def test_the_real_commands_answer_as_the_measurement_reads_them(audit_tamper, untouched) -> None:
    """No engine and no stubs: the installed CLI on a scripted log, so the exit codes and rows the
    measurement relies on have a witness in this suite as well as in the CI run."""
    lines = untouched.read_text(encoding="utf-8").splitlines()
    scratch = untouched.parent

    good = audit_tamper._verify(untouched)
    assert (good.exit_code, good.rows, good.ok, good.breaks) == (0, 10, True, ())
    assert good.head == json.loads(lines[-1])["hash"]

    changed = audit_tamper._verify(audit_tamper._write_altered(scratch, "changed", audit_tamper._changed(lines, 5), lines))
    assert (changed.exit_code, changed.rows, changed.ok, changed.breaks) == (2, 10, False, ((5, "hash"),))

    cut = audit_tamper._write_altered(scratch, "truncated", lines[:-3], lines)
    passed = audit_tamper._verify(cut)
    assert (passed.exit_code, passed.rows, passed.ok, passed.breaks) == (0, 7, True, ())

    assert audit_tamper._check_head(untouched, good.head) == audit_tamper.HeadVerdict(0, True)
    assert audit_tamper._check_head(cut, good.head) == audit_tamper.HeadVerdict(2, False)
    assert audit_tamper._head_exit_without_json(untouched, good.head) == 1


def test_an_alteration_that_changes_nothing_fails_the_measurement(audit_tamper, tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="changed nothing"):
        audit_tamper._write_altered(tmp_path, "changed", ["a"], ["a"])


@pytest.mark.parametrize("rows, refused", [
    ([{"action": "leg.finished"}] * 6, "no approval"),
    ([_gate_row("review"), *[{"action": "leg.finished"}] * 5], "not in the middle"),
    ([*[{"action": "leg.finished"}] * 5, _gate_row("review")], "not in the middle"),
    ([*[{"action": "leg.finished"}] * 4, _gate_row("review"), {"action": "leg.finished"}], "not in the middle"),
])
def test_a_log_with_no_approval_in_its_middle_fails_the_measurement(audit_tamper, rows, refused) -> None:
    with pytest.raises(RuntimeError, match=refused):
        audit_tamper._approval_position([json.dumps(row) for row in rows])


def test_a_rewrite_changes_the_row_and_keeps_every_later_row(audit_tamper, untouched) -> None:
    lines = untouched.read_text(encoding="utf-8").splitlines()
    rewritten = audit_tamper._rewritten(lines, 5)
    assert rewritten[:4] == lines[:4]
    before, after = [[json.loads(line) for line in side[4:]] for side in (lines, rewritten)]
    assert after[0]["actor"] == audit_tamper.FORGED_ACTOR != before[0]["actor"]
    assert [row.get("target") for row in after] == [row.get("target") for row in before]
    assert all(old["hash"] != new["hash"] for old, new in zip(before, after))
    assert json.loads(audit_tamper._rewritten(lines, 1)[0])["actor"] == audit_tamper.FORGED_ACTOR


@pytest.mark.parametrize("returncode, stdout, refused", [
    (1, "", "exited 1"),
    (3, "{}", "exited 3"),
    (0, "not json", "did not print JSON"),
    (2, "", "did not print JSON"),
])
def test_a_command_that_does_not_answer_fails_the_measurement(audit_tamper, tmp_path: Path,
                                                              monkeypatch: pytest.MonkeyPatch, returncode, stdout,
                                                              refused) -> None:
    done = SimpleNamespace(returncode=returncode, stdout=stdout, stderr="")
    monkeypatch.setattr(audit_tamper.subprocess, "run", lambda *a, **k: done)
    with pytest.raises(RuntimeError, match=refused):
        audit_tamper._verify(tmp_path / "audit.jsonl")


def test_a_last_row_that_never_comes_fails_the_measurement(audit_tamper, untouched) -> None:
    engine = SimpleNamespace(audit_log=untouched)
    audit_tamper._wait_for_last_row(engine, "pl_x")
    with pytest.raises(RuntimeError, match="the audit row that ends pl_other"):
        audit_tamper._wait_for_last_row(engine, "pl_other", timeout=0.0)


class _Engine:
    """Stands in for the engine in ``main``: the log it would have written is already there."""
    url = "http://nowhere"

    def __init__(self, log: Path) -> None:
        self.audit_log = log

    def __enter__(self) -> "_Engine":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def log_tail(self) -> str:
        return ""


def _main(audit_tamper, untouched: Path, out: Path, monkeypatch: pytest.MonkeyPatch, verify, check_head) -> int:
    monkeypatch.setattr(audit_tamper, "LocalEngine", lambda profile: _Engine(untouched))
    monkeypatch.setattr(audit_tamper, "_one_run", lambda client: (0.0, 0.0, "pl_x"))
    monkeypatch.setattr("cairn_client.CairnClient", lambda url: SimpleNamespace(login=lambda *a: None))
    monkeypatch.setattr(audit_tamper, "_verify", verify)
    monkeypatch.setattr(audit_tamper, "_check_head", check_head)
    monkeypatch.setattr("sys.argv", ["audit_tamper.py", "--out", str(out)])
    return audit_tamper.main()


def test_a_failed_check_writes_no_result(audit_tamper, untouched, rule, tmp_path: Path,
                                         monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out"
    passes = lambda v: type(v)(0, v.rows, v.head, True, ())  # noqa: E731
    with pytest.raises(RuntimeError, match="not detected"):
        _main(audit_tamper, untouched, out, monkeypatch, _when("changed", passes, rule.verify), rule.check_head)
    assert not out.exists()


def test_a_run_where_every_check_holds_publishes_its_limits(audit_tamper, untouched, rule, tmp_path: Path,
                                                            monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out"
    assert _main(audit_tamper, untouched, out, monkeypatch, rule.verify, rule.check_head) == 0
    result = json.loads((out / "audit_tamper.json").read_text(encoding="utf-8"))
    assert result["script"] == "benchmark/audit_tamper.py"
    assert sum(row["result"].startswith("NOT detected") for row in result["results"]) == 8
