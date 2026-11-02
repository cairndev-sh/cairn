"""What happens when the server is killed: the crash cells of ``tests/e2e/hello/test_crash.py`` as a table.

This script proves nothing itself. Each cell SIGKILLs a real server, starts a new one on the same
stores and checks the outcome ADR-0125 requires; this script publishes which cells passed. A cell
counts only when its case passed in that run's junit and it left a receipt
(``tests/e2e/hello/_receipts.py``). Any cell that failed, was skipped or didn't run is shown as
such, exits non-zero and writes no result.

    # run the cells, then write the result; the Postgres cells need CAIRN_PROOF_PG_DSN answering
    .venv/bin/python benchmark/crash_matrix.py [--out benchmark/results]
    # read a run that already happened, as the public-e2e CI job does
    .venv/bin/python benchmark/crash_matrix.py --junit public-e2e.xml --receipts receipts.jsonl --out DIR
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from _common import REPO, RESULTS, _git, scrubbed_env, sha256_file, write_result

sys.path.insert(0, str(REPO))  # the cells and the receipt rule live under tests/

from tests.e2e.conftest import CELLS, Cell  # noqa: E402
from tests.e2e.hello._receipts import (  # noqa: E402
    CELL_SCOPE,
    CRASH_TESTS,
    _load_junit,
    _load_receipts,
    passed_cases,
)

CELLS_FILE = REPO / "tests" / "e2e" / "hello" / "test_crash.py"
CELLS_MODULE = "tests.e2e.hello.test_crash"
#: a job can't outlive this, so an older junit is from another run
MAX_JUNIT_AGE = timedelta(hours=6)
NOT_APPLICABLE = "n/a"
LABELS = {"sqlite": "SQLite", "postgres": "Postgres", "inprocess": "in-process", "dbos": "DBOS"}
PLACES = {"storage": "Storage", "runtime": "Runtime", "bus": "Bus"}


@dataclass(frozen=True)
class Scenario:
    column: str
    crashed: str
    when: str
    required: str


#: One per ``CRASH_TESTS`` key, and ``benchmark/README.md`` repeats these words (a test holds both to it).
SCENARIOS = {
    "crash.gate": Scenario(
        column="Killed at a person gate",
        crashed="The server process, with SIGKILL, running the `hello-crash/slow-publish` pipeline.",
        when="While the `review` person gate waits for a decision, after the three legs before it have finished.",
        required="The gate is still waiting after the restart and no finished leg runs again: a step waiting for "
                 "approval has no process to lose, so ADR-0125 D3 leaves it out of the liveness check. Approving it "
                 "finishes the pipeline with one run per leg and one published artifact.",
    ),
    "crash.mid-leg": Scenario(
        column="Killed inside a flow leg",
        crashed="The server process, with SIGKILL, running the `hello-crash/slow-publish` pipeline. The leg's engine "
                "subprocess isn't killed and outlives the server, as in a real crash.",
        when="About a second into the `slow` flow leg on the builtin engine, after the leg's engine handle is saved.",
        required="Right after the restart the leg is still `running` on the same run: the dead owner's lock has been "
                 "found free only once, which ADR-0125 D3 calls undetermined. A second check more than 15 s later "
                 "says the owner is dead, and the leg is re-attached under a new owner (D4). The builtin engine lost "
                 "its job table with the old process, so the leg settles `failed` and the pipeline fails. The work "
                 "isn't resumed: the leg isn't submitted again, the `stage` and `publish` legs after it never start, "
                 "and no artifact is staged or published.",
    ),
    "crash.dbos-resume": Scenario(
        column="Killed inside a template leg on DBOS",
        crashed="The server process, with SIGKILL, running the `hello-cairn/ask-the-docs` pipeline.",
        when="While the `assistant` template leg, a DBOS workflow, waits on its model call.",
        required="A run on a durable runtime names no in-process owner, so ADR-0125 D1 leaves its recovery to the "
                 "runtime. At the next boot DBOS runs the workflow again from its last checkpoint: the model is "
                 "called again, the same run finishes `done`, and the pipeline reaches its person gate. Approving "
                 "it finishes the pipeline with one run per leg.",
    ),
}

NOT_COVERED = (
    "The Temporal runtime. No cell runs it: the cells run on the in-process runtime and on DBOS.",
    "A kill inside a task leg, or inside a template leg on the in-process runtime.",
    "A kill inside a flow leg before its engine handle is saved.",
    "A flow leg on an engine whose jobs outlive the server, such as Metaflow or SkyPilot. Every cell uses the builtin "
    "engine, so no cell shows a re-attached leg finishing.",
    "A kill while a scheduled or event step fires, while a finalizer runs, after a cancel, or while a pipeline starts. "
    "A cell tests only the moment its column names.",
    "A second kill during recovery.",
    "More than one server: a second replica, or a server on another host.",
    "Anything other than the server process dying. Postgres and the disk stay up in every cell, so a Postgres restart, "
    "a failover or a power loss is not tested.",
    "How fast recovery is. A cell only has a time limit.",
)


def _applies(feature: str, cell: Cell) -> bool:
    scope = CELL_SCOPE.get(feature)
    return scope(cell.id) if scope else True


def _case_name(feature: str, cell: Cell) -> str:
    return f"{CRASH_TESTS[feature]}[{cell.id}]"


def judge(cases: list[dict], receipts: list[dict]) -> list[dict]:
    """One verdict per cell that should exist. Only ``state == "passed"`` proves anything: the
    cell's case passed in this junit and left exactly one receipt."""
    passed = passed_cases(cases)
    verdicts = []
    for cell in CELLS:
        for feature in SCENARIOS:
            if not _applies(feature, cell):
                continue
            name = _case_name(feature, cell)
            mine = [c for c in cases if c["id"] == f"{CELLS_MODULE}::{name}"]
            found = [r for r in receipts if r.get("proved", True) and r.get("test") == name
                     and r.get("feature") == feature and r.get("cell") == cell.id]
            state, detail, evidence = "passed", "", ""
            if not mine:
                state, detail = "not run", "this run's junit has no such case"
            elif name not in passed:
                bad = next(c for c in mine if c["outcome"] != "passed")
                state, detail = bad["outcome"], bad["message"]
            elif not found:
                state, detail = "no receipt", "its case passed but recorded no receipt"
            elif len(found) > 1:
                state, detail = "no receipt", f"{len(found)} receipts for one case: the file spans more than one run"
            else:
                evidence = found[0]["evidence"]
            verdicts.append({"cell": cell.id, "feature": feature, "test": name, "state": state, "detail": detail,
                             "evidence": evidence})
    return verdicts


def table_rows(verdicts: list[dict]) -> list[dict[str, str]]:
    """Storage, runtime, bus and each scenario's real state, whatever it was. ``n/a`` is only
    for a cell that doesn't exist in that setup."""
    states = {(v["cell"], v["feature"]): v["state"] for v in verdicts}
    return [{"storage": LABELS[c.storage], "runtime": LABELS[c.runtime], "bus": LABELS[c.bus],
             **{f: states.get((c.id, f), "not run") if _applies(f, c) else NOT_APPLICABLE for f in SCENARIOS}}
            for c in CELLS]


def columns() -> dict[str, str]:
    return {**PLACES, **{feature: scenario.column for feature, scenario in SCENARIOS.items()}}


def notes() -> list[str]:
    """The words beside the table: what each column did and what it can't say."""
    return [
        "Each row is one setup: where records are stored, which runtime owns a run, and which bus carries events. "
        "Each cell kills the real server, starts a new one on the same stores, and checks what the records say "
        "afterwards. `passed` means that cell's test passed in this run. `n/a` means the cell doesn't exist in that "
        "setup: only DBOS owns a template run this way.",
        *(f"**{s.column}.** {s.when} {s.required}" for s in SCENARIOS.values()),
        "**Not covered.** No cell tests these. Some have tests under `tests/core`, which drive the code in one "
        "process and don't kill a real server.\n\n" + "\n".join(f"- {item}" for item in NOT_COVERED),
    ]


def _junit_ran_at(junit: Path) -> Optional[datetime]:
    root = ET.parse(junit).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    stamp = suite.get("timestamp") if suite is not None else None
    return datetime.fromisoformat(stamp).astimezone(timezone.utc) if stamp else None


def _uncommitted_cells() -> str:
    """``code_clean`` covers what is measured, not the tests doing the measuring. Here they are the measure."""
    return _git("status", "--porcelain", "--", "tests/e2e")


def refusals(junit: Path, cases: list[dict], verdicts: list[dict], *, now: Optional[datetime] = None) -> list[str]:
    """Every reason this run can't be published; empty when it can."""
    problems = [f"{v['test']}: {v['state']}" + (f": {v['detail']}" if v["detail"] else "")
                for v in verdicts if v["state"] != "passed"]
    known = {f"{CELLS_MODULE}::{v['test']}" for v in verdicts}
    problems += [f"{c['id']}: ran, but this script doesn't describe it"
                 for c in cases if c["id"].startswith(f"{CELLS_MODULE}::") and c["id"] not in known]
    if junit.is_file():
        ran_at = _junit_ran_at(junit)
        if ran_at is None:
            problems.append(f"{junit} doesn't say when it ran")
        elif (now or datetime.now(timezone.utc)) - ran_at > MAX_JUNIT_AGE:
            problems.append(f"{junit} is from {ran_at.isoformat()}, too old to be this run's")
    dirty = _uncommitted_cells()
    if dirty:
        problems.append(f"the cells under tests/e2e have uncommitted changes:\n{dirty}")
    return problems


def _run_the_cells(scratch: Path) -> tuple[Path, Path, int]:
    """The crash cells on this machine, four at a time as in CI. A cell that can't reach
    Postgres skips with its reason, so a run without it publishes nothing."""
    junit, receipts = scratch / "junit.xml", scratch / "receipts.jsonl"
    env = {**scrubbed_env(), **{k: v for k, v in os.environ.items() if k.startswith("CAIRN_PROOF_")},
           "CAIRN_E2E": "1", "CAIRN_PROOF_RECEIPTS": str(receipts)}
    done = subprocess.run([sys.executable, "-m", "pytest", "-q", "--no-header", "-p", "no:cacheprovider", "-rs",
                           "-n", "4", "--dist", "load", "-m", "crash", f"--junitxml={junit}",
                           str(CELLS_FILE.relative_to(REPO))], cwd=REPO, env=env, check=False)
    return junit, receipts, done.returncode


def _print_table(rows: list[dict[str, str]]) -> None:
    heads = columns()
    print(f"| {' | '.join(heads.values())} |")
    print(f"|{'---|' * len(heads)}")
    for row in rows:
        print(f"| {' | '.join(row[k] for k in heads)} |")


def _publish(out: Path, verdicts: list[dict]) -> Path:
    inputs: dict[str, Any] = {"cells": str(CELLS_FILE.relative_to(REPO)), "cells_sha256": sha256_file(CELLS_FILE)}
    return write_result(out, "crash_matrix", title="What happens when the server is killed", inputs=inputs,
                        results=table_rows(verdicts), extra={
                            "columns": columns(),
                            "notes": notes(),
                            "scenarios": {f: {"column": s.column, "crashed": s.crashed, "when": s.when,
                                              "required": s.required} for f, s in SCENARIOS.items()},
                            "not_covered": list(NOT_COVERED),
                            "cells": [{k: v[k] for k in ("cell", "feature", "test", "state", "evidence")}
                                      for v in verdicts],
                        })


def _report(junit: Path, receipts: Path, out: Path, *, exit_code: int = 0) -> int:
    cases = _load_junit(junit)
    verdicts = judge(cases, _load_receipts(receipts))
    _print_table(table_rows(verdicts))
    problems = refusals(junit, cases, verdicts)
    if exit_code:
        problems.append(f"pytest exited {exit_code}")
    if problems:
        print(f"\nNo result written, {len(problems)} problem(s):", file=sys.stderr)
        for problem in problems:
            print(f"- {problem}", file=sys.stderr)
        return 1
    path = _publish(out, verdicts)
    print(path.read_text(encoding="utf-8"))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--junit", type=Path, help="the junit of a run that already happened; needs --receipts")
    parser.add_argument("--receipts", type=Path, help="that run's CAIRN_PROOF_RECEIPTS file")
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args()
    if bool(args.junit) != bool(args.receipts):
        parser.error("--junit and --receipts go together")
    if args.junit:
        return _report(args.junit, args.receipts, args.out)
    with tempfile.TemporaryDirectory(prefix="cairn-crash-matrix-") as scratch:
        junit, receipts, exit_code = _run_the_cells(Path(scratch))
        return _report(junit, receipts, args.out, exit_code=exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
