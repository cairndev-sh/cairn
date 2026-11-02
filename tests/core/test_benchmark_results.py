"""The README's benchmark numbers are generated, and each one can be traced (#323)."""
from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark import render


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
        quickstart_path._audit_rows(engine)


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
