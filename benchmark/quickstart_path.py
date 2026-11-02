"""The quickstart's path on a real local engine: no API key, no spend.

Starts an engine, launches ``hello-cairn/profile-and-publish`` the way the quickstart does,
and answers both gates. It measures how long the machinery takes when nobody is waiting to
decide, and checks what the README claims along the way: the pipeline stops at the person
gate before anything is published, a false data gate stops it before any later step starts,
and the audit chain verifies at the end. Any failed check exits non-zero and writes no result.

    .venv/bin/python benchmark/quickstart_path.py [--repeats 5] [--out benchmark/results]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from _common import REPO, RESULTS, LocalEngine, sha256_file, wait_for, write_result

PIPELINE = "hello-cairn/profile-and-publish"
SAMPLE = REPO / "examples" / "hello-cairn" / "data" / "sample.jsonl"
ENVIRONMENT = "sandbox"
TERMINAL = {"done", "failed", "cancelled"}
#: every step after the data gate, in `pipelines/profile-and-publish.yaml`
AFTER_DATA_GATE = ("redact", "engine-check", "stage", "review", "promote", "publish", "audit")
#: the only statuses of a step that did not start; an unknown status or a missing step fails the check
NOT_STARTED = {"pending", "skipped"}


def _steps(record: dict) -> dict[str, str]:
    return {sid: st.get("status", "") for sid, st in record.get("step_state", {}).items()}


def _awaiting(client: Any, pipeline_id: str, step: str) -> dict:
    """The record once ``step`` waits for a person, refusing a pipeline that ended instead."""
    record = wait_for(lambda: client.pipeline(pipeline_id),
                      lambda r: _steps(r).get(step) == "awaiting_approval" or r.get("status") in TERMINAL,
                      what=f"{pipeline_id} to wait at {step}")
    if _steps(record).get(step) != "awaiting_approval":
        raise RuntimeError(f"{pipeline_id} ended {record.get('status')} before {step}: {_steps(record)}")
    return record


def _one_run(client: Any) -> tuple[float, float]:
    """Launch to the person gate, and the approvals to done, in seconds."""
    start = time.monotonic()
    pipeline_id = client.launch_pipeline(PIPELINE, {"path": str(SAMPLE)}, environment=ENVIRONMENT)["pipeline_id"]
    record = _awaiting(client, pipeline_id, "review")
    to_gate = time.monotonic() - start
    steps = _steps(record)
    if steps.get("publish") == "done" or steps.get("audit") == "done":
        raise RuntimeError(f"{pipeline_id} published before its person gate: {steps}")

    start = time.monotonic()
    client.approve_step(pipeline_id, "review")
    _awaiting(client, pipeline_id, "promote")
    client.approve_step(pipeline_id, "promote")
    record = wait_for(lambda: client.pipeline(pipeline_id), lambda r: r.get("status") in TERMINAL,
                      what=f"{pipeline_id} to finish")
    if record.get("status") != "done":
        raise RuntimeError(f"{pipeline_id} ended {record.get('status')}: {_steps(record)}")
    return to_gate, time.monotonic() - start


def _data_gate_refuses(client: Any) -> None:
    """A false data gate fails the pipeline, and no step after the gate starts."""
    launched = client.launch_pipeline(PIPELINE, {"path": str(SAMPLE), "min_rows": 1000}, environment=ENVIRONMENT)
    pipeline_id = launched["pipeline_id"]
    record = wait_for(lambda: client.pipeline(pipeline_id), lambda r: r.get("status") in TERMINAL,
                      what=f"{pipeline_id} to stop at its data gate")
    steps = _steps(record)
    if record.get("status") != "failed" or any(steps.get(s) not in NOT_STARTED for s in AFTER_DATA_GATE):
        raise RuntimeError(f"the data gate did not stop {pipeline_id}: {record.get('status')} {steps}")


def _audit_rows(engine: LocalEngine) -> int:
    from cairn.core.obs.audit import verify_chain  # noqa: PLC0415 — the rule the writer and `audit verify` share

    report = verify_chain(engine.audit_log.read_text(encoding="utf-8").splitlines())
    if not report.ok or not report.rows:
        raise RuntimeError(f"the audit chain does not verify: rows={report.rows} broken={report.broken}")
    return report.rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args()

    from cairn_client import CairnClient  # noqa: PLC0415 — after --help, which needs no install

    with LocalEngine() as engine:
        client = CairnClient(engine.url)
        client.login("dev", "dev")
        try:
            # the first run warms the engine's imports; it is not one of the repeats
            _one_run(client)
            runs = [_one_run(client) for _ in range(args.repeats)]
            _data_gate_refuses(client)
            rows = _audit_rows(engine)
        except Exception:
            print(engine.log_tail(), file=sys.stderr)
            raise
        boot = engine.boot_seconds

    to_gate = statistics.median(r[0] for r in runs)
    to_done = statistics.median(r[1] for r in runs)
    of = f"median of {args.repeats}, after one warm-up run"
    path = write_result(args.out, "quickstart_path", title="The quickstart's path on a local engine", inputs={
        "pipeline": PIPELINE, "environment": ENVIRONMENT, "sample_sha256": sha256_file(SAMPLE),
        "repeats": args.repeats,
    }, results=[
        {"what": "Engine start to answering", "result": f"{boot:.1f} s"},
        {"what": "Launch to a pipeline paused at its person gate", "result": f"{to_gate:.1f} s ({of})"},
        {"what": "Both approvals to a finished pipeline, nobody waiting", "result": f"{to_done:.1f} s ({of})"},
        {"what": "Nothing published before the person gate", "result": f"held on all {args.repeats + 1} runs"},
        {"what": "A false data gate stops the pipeline before any later step starts", "result": "held"},
        {"what": "Audit chain verifies at the end", "result": f"held ({rows} rows)"},
    ])
    print(path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
