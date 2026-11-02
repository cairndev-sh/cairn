"""The crash matrix publishes only cells that passed in that run (#326). Every way a cell can
fail to count fails the generator, shows the cell as it was, and writes nothing."""
from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import pytest
import yaml

from benchmark import render
from tests.e2e.conftest import CELLS
from tests.e2e.hello import test_crash
from tests.e2e.hello._receipts import CRASH_TESTS, FEATURES, _load_junit, passed_cases

MODULE = "tests.e2e.hello.test_crash"
#: 8 setups for each of the two kills every setup runs, and 4 DBOS setups for the third
ALL_CASES = 8 + 8 + 4


@pytest.fixture
def matrix(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(render.REPO / "benchmark"))
    module = importlib.import_module("crash_matrix")
    monkeypatch.setattr(module, "_uncommitted_cells", lambda: "")
    monkeypatch.setattr(sys.modules["_common"], "provenance", lambda: {
        "cairndev_version": "1.2.3", "uv_lock_sha256": "a" * 64, "git_sha": "b" * 40, "code_clean": True,
        "machine": {"os": "Linux", "arch": "x86_64", "cpus": 4, "python": "3.13.1"},
        "measured_at": "2026-11-02", "ci_run": None,
    })
    return module


def _all_names(matrix) -> list[tuple[str, str, str]]:
    return [(feature, cell.id, f"{CRASH_TESTS[feature]}[{cell.id}]")
            for cell in CELLS for feature in CRASH_TESTS if matrix._applies(feature, cell)]


def _write_junit(path: Path, cases: list[tuple[str, str, str]], *, ran_at: Optional[str] = "now") -> Path:
    stamp = datetime.now(timezone.utc).isoformat() if ran_at == "now" else ran_at
    body = []
    for name, outcome, message in cases:
        inner = {"failed": f'<failure message="{message}"/>', "skipped": f'<skipped message="{message}"/>',
                 "error": f'<error message="{message}"/>'}.get(outcome, "")
        body.append(f'<testcase classname="{MODULE}" name="{name}">{inner}</testcase>')
    attr = f' timestamp="{stamp}"' if stamp else ""
    path.write_text(f'<testsuites><testsuite name="pytest"{attr}>{"".join(body)}</testsuite></testsuites>')
    return path


def _write_receipts(path: Path, rows: list[dict]) -> Path:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return path


def _receipt(feature: str, cell: str, name: str, **changes) -> dict:
    return {"feature": feature, "cell": cell, "pipeline": "p", "evidence": f"evidence of {name}", "proved": True,
            "test": name, **changes}


class _Run:
    """A run where every cell passed, for one test to spoil."""

    def __init__(self, tmp_path: Path, matrix) -> None:
        self.names = _all_names(matrix)
        self.cases = [(name, "passed", "") for _, _, name in self.names]
        self.rows = [_receipt(feature, cell, name) for feature, cell, name in self.names]
        self.junit, self.receipts, self.out = tmp_path / "junit.xml", tmp_path / "receipts.jsonl", tmp_path / "out"
        self.ran_at: Optional[str] = "now"
        self.matrix = matrix

    def set_outcome(self, name: str, outcome: str, message: str) -> None:
        self.cases = [(n, outcome, message) if n == name else (n, o, m) for n, o, m in self.cases]

    def report(self) -> int:
        _write_junit(self.junit, self.cases, ran_at=self.ran_at)
        _write_receipts(self.receipts, self.rows)
        return self.matrix._report(self.junit, self.receipts, self.out)

    @property
    def written(self) -> bool:
        return (self.out / "crash_matrix.json").exists()


@pytest.fixture
def run(tmp_path: Path, matrix) -> _Run:
    return _Run(tmp_path, matrix)


GATE_SQLITE = "test_killed_at_the_person_gate[sqlite-inprocess-inprocess]"
RESUME_PG = "test_dbos_resumes_a_template_run_after_a_crash[postgres-dbos-postgres]"


def test_a_run_where_every_cell_passed_is_published(run: _Run) -> None:
    assert len(run.names) == ALL_CASES
    assert run.report() == 0
    result = json.loads((run.out / "crash_matrix.json").read_text())
    assert result["script"] == "benchmark/crash_matrix.py" and result["inputs"]["cells_sha256"]
    assert len(result["results"]) == len(CELLS) == 8
    assert {c["state"] for c in result["cells"]} == {"passed"} and len(result["cells"]) == ALL_CASES
    assert any(c["evidence"] == f"evidence of {GATE_SQLITE}" for c in result["cells"])


def test_the_published_result_renders_as_a_table_with_its_words(run: _Run) -> None:
    assert run.report() == 0
    block = render.render_block(render.load_results(run.out, render.REPO))
    assert "| Storage | Runtime | Bus | Killed at a person gate | Killed inside a flow leg |" in block
    assert "|---|---|---|---|---|---|" in block
    assert "| SQLite | in-process | in-process | passed | passed | n/a |" in block
    assert "| Postgres | DBOS | Postgres | passed | passed | passed |" in block
    assert block.count("| n/a |") == 4, "only the setups with no DBOS have no DBOS cell"
    assert "**Not covered.**" in block and "The Temporal runtime." in block
    for scenario in ("Killed at a person gate.", "Killed inside a flow leg.", "Killed inside a template leg on DBOS."):
        assert f"**{scenario}**" in block


@pytest.mark.parametrize("outcome, shown", [("failed", "failed"), ("error", "error"), ("skipped", "skipped")])
def test_a_cell_that_did_not_pass_fails_and_writes_nothing(run: _Run, capsys, outcome: str, shown: str) -> None:
    run.set_outcome(GATE_SQLITE, outcome, "the reason it did not pass")
    assert run.report() == 1
    assert not run.written
    out = capsys.readouterr()
    assert f"| SQLite | in-process | in-process | {shown} | passed | n/a |" in out.out
    assert GATE_SQLITE in out.err and "the reason it did not pass" in out.err


def test_a_cell_that_did_not_run_shows_as_not_run(run: _Run, capsys) -> None:
    run.cases = [c for c in run.cases if c[0] != RESUME_PG]
    assert run.report() == 1
    assert not run.written
    out = capsys.readouterr()
    assert "| Postgres | DBOS | Postgres | passed | passed | not run |" in out.out
    assert RESUME_PG in out.err


def test_receipts_without_a_junit_prove_nothing(run: _Run, matrix, capsys) -> None:
    _write_receipts(run.receipts, run.rows)
    assert matrix._report(run.junit, run.receipts, run.out) == 1
    assert not run.written
    assert "passed" not in capsys.readouterr().out


def test_a_passed_case_with_no_receipt_fails(run: _Run) -> None:
    run.rows = [r for r in run.rows if r["test"] != GATE_SQLITE]
    assert run.report() == 1
    assert not run.written


def test_a_receipt_for_another_cell_or_feature_does_not_count(run: _Run, matrix) -> None:
    run.rows = [_receipt("crash.mid-leg", "sqlite-inprocess-inprocess", GATE_SQLITE) if r["test"] == GATE_SQLITE else r
                for r in run.rows]
    assert run.report() == 1
    run.rows = [_receipt("crash.gate", "postgres-dbos-postgres", GATE_SQLITE) if r["test"] == GATE_SQLITE else r
                for r in run.rows]
    assert run.report() == 1
    assert not run.written


def test_a_receipt_that_says_it_proved_nothing_does_not_count(run: _Run) -> None:
    run.rows = [{**r, "proved": False} if r["test"] == GATE_SQLITE else r for r in run.rows]
    assert run.report() == 1
    assert not run.written


def test_receipts_from_two_runs_fail(run: _Run, capsys) -> None:
    run.rows = [*run.rows, _receipt("crash.gate", "sqlite-inprocess-inprocess", GATE_SQLITE)]
    assert run.report() == 1
    assert not run.written
    assert "more than one run" in capsys.readouterr().err


def test_a_case_that_passed_and_then_errored_at_teardown_did_not_pass(run: _Run, capsys) -> None:
    """junit can list one case twice; the receipt was recorded before the teardown failed."""
    run.cases = [*run.cases, (GATE_SQLITE, "error", "the server would not stop")]
    assert run.report() == 1
    assert not run.written
    assert "the server would not stop" in capsys.readouterr().err


def test_the_receipt_rule_reads_that_case_as_not_passed(tmp_path: Path) -> None:
    junit = _write_junit(tmp_path / "junit.xml", [(GATE_SQLITE, "passed", ""), (GATE_SQLITE, "error", "teardown"),
                                                  (RESUME_PG, "passed", "")])
    assert passed_cases(_load_junit(junit)) == {RESUME_PG}


def test_a_junit_from_another_run_fails(run: _Run, capsys) -> None:
    run.ran_at = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    assert run.report() == 1
    assert not run.written
    assert "too old" in capsys.readouterr().err


def test_a_junit_that_does_not_say_when_it_ran_fails(run: _Run, capsys) -> None:
    run.ran_at = None
    assert run.report() == 1
    assert not run.written
    assert "when it ran" in capsys.readouterr().err


def test_a_crash_case_this_script_does_not_describe_fails(run: _Run, capsys) -> None:
    run.cases = [*run.cases, ("test_killed_somewhere_new[sqlite-inprocess-inprocess]", "failed", "boom")]
    assert run.report() == 1
    assert not run.written
    assert "test_killed_somewhere_new" in capsys.readouterr().err


def test_edited_cells_fail(run: _Run, matrix, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(matrix, "_uncommitted_cells", lambda: " M tests/e2e/hello/test_crash.py")
    assert run.report() == 1
    assert not run.written
    assert "uncommitted" in capsys.readouterr().err


def test_a_failed_run_leaves_an_earlier_result_alone(run: _Run) -> None:
    assert run.report() == 0
    before = (run.out / "crash_matrix.json").read_text()
    run.set_outcome(GATE_SQLITE, "failed", "boom")
    assert run.report() == 1
    assert (run.out / "crash_matrix.json").read_text() == before


class _Pytest:
    def __init__(self, code: int, write: Optional[Callable[[list[str], dict], None]] = None) -> None:
        self.code, self.write = code, write
        self.call: Optional[tuple[list[str], dict]] = None

    def __call__(self, cmd: list[str], **kwargs):
        self.call = (cmd, kwargs)
        if self.write:
            self.write(cmd, kwargs["env"])
        return type("Done", (), {"returncode": self.code})()


def test_running_the_cells_itself_scrubs_the_env_and_asks_for_receipts(
        matrix, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _Pytest(0)
    monkeypatch.setattr(matrix.subprocess, "run", fake)
    monkeypatch.setenv("CAIRN_STORAGE", "postgres://elsewhere")
    monkeypatch.setenv("CAIRN_PROOF_PG_DSN", "postgres://proof")
    monkeypatch.setattr("sys.argv", ["crash_matrix.py", "--out", str(tmp_path / "out")])
    assert matrix.main() == 1, "pytest wrote no junit, so no cell ran"
    assert not (tmp_path / "out").exists()
    cmd, kwargs = fake.call
    assert "-m" in cmd and "crash" in cmd and "tests/e2e/hello/test_crash.py" in cmd
    env = kwargs["env"]
    assert env["CAIRN_E2E"] == "1" and env["CAIRN_PROOF_RECEIPTS"] and env["CAIRN_PROOF_PG_DSN"] == "postgres://proof"
    assert "CAIRN_STORAGE" not in env


def test_a_pytest_that_exits_non_zero_fails_even_if_every_case_reads_passed(
        matrix, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    names = _all_names(matrix)

    def write(cmd: list[str], env: dict) -> None:
        junit = next(a for a in cmd if a.startswith("--junitxml=")).split("=", 1)[1]
        _write_junit(Path(junit), [(n, "passed", "") for _, _, n in names])
        _write_receipts(Path(env["CAIRN_PROOF_RECEIPTS"]), [_receipt(f, c, n) for f, c, n in names])

    monkeypatch.setattr(matrix.subprocess, "run", _Pytest(3, write))
    monkeypatch.setattr("sys.argv", ["crash_matrix.py", "--out", str(tmp_path / "out")])
    assert matrix.main() == 1
    assert not (tmp_path / "out").exists()
    assert "pytest exited 3" in capsys.readouterr().err


def test_junit_and_receipts_go_together(matrix, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["crash_matrix.py", "--junit", "j.xml"])
    with pytest.raises(SystemExit):
        matrix.main()


# ── the words and the wiring can't drift from the cells ───────────────────

def test_the_matrix_expects_exactly_what_the_cells_parametrize(matrix) -> None:
    def ids(fn) -> set[str]:
        mark = next(m for m in fn.pytestmark if m.name == "parametrize")
        return {f"{fn.__name__}[{param.id}]" for param in mark.args[1]}

    collected = set()
    for name in CRASH_TESTS.values():
        collected |= ids(getattr(test_crash, name))
    assert collected == {name for _, _, name in _all_names(matrix)}


def test_every_crash_feature_has_its_words(matrix) -> None:
    assert set(matrix.SCENARIOS) == set(CRASH_TESTS)
    assert set(CRASH_TESTS) <= set(FEATURES)


def test_the_benchmark_readme_says_what_the_script_says(matrix) -> None:
    readme = (render.REPO / "benchmark" / "README.md").read_text(encoding="utf-8")
    assert "[`crash_matrix.py`](./crash_matrix.py)" in readme
    for scenario in matrix.SCENARIOS.values():
        for words in (scenario.column, scenario.crashed, scenario.when, scenario.required):
            assert words in readme, words
    for item in matrix.NOT_COVERED:
        assert item in readme, item


def _e2e_steps() -> list[dict]:
    ci = yaml.safe_load((render.REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    return ci["jobs"]["public-e2e"]["steps"]


def test_public_e2e_records_receipts_where_they_can_be_written() -> None:
    cells = next(s for s in _e2e_steps() if "-m e2e" in str(s.get("run", "")))
    receipts = cells["env"]["CAIRN_PROOF_RECEIPTS"]
    assert receipts == "/tmp/test-results/receipts.jsonl"
    assert cells["run"].startswith("mkdir -p /tmp/test-results"), "pytest makes this directory only when it ends"


def test_public_e2e_publishes_only_after_the_cells_and_uploads_the_result() -> None:
    steps = _e2e_steps()
    names = [str(s.get("run", "")) for s in steps]
    cells = next(i for i, run in enumerate(names) if "-m e2e" in run)
    generator = next(i for i, run in enumerate(names) if "benchmark/crash_matrix.py" in run)
    assert generator > cells
    assert "if" not in steps[generator], "a red cell step skips it, so a failed run writes nothing"
    for part in ("--junit /tmp/test-results/public-e2e.xml", "--receipts /tmp/test-results/receipts.jsonl",
                 "--out /tmp/test-results/benchmark"):
        assert part in names[generator]
    upload = next(s for s in steps if "upload-artifact" in str(s.get("uses", "")))
    assert upload["with"]["path"] == "/tmp/test-results" and upload["if"] == "always()"
