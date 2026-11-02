"""What the audit log detects when someone alters it, and what it can't: no API key, no spend.

Runs the quickstart's pipeline on a real local engine, so the log is one the engine wrote, then
alters copies of it and asks the real commands about each copy. Changing a field, deleting a row,
inserting a forged row and swapping two rows must each exit 2 and name the row they should. A hash
chain has nothing outside the file to compare with, so cutting the tail or recomputing the hashes
is not detected; those cases must pass ``cairn admin audit verify`` and fail against a head saved
earlier (``cairn admin lineage --expect-head``), and the result says so. If any of this differs,
the published claim would be wrong: the script exits non-zero and writes no result.

The engine is driven through ``cairn-client``, like every measurement. The two checks go through
the CLI because those commands are what is being measured.

    .venv/bin/python benchmark/audit_tamper.py [--out benchmark/results]
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from _common import (RESULTS, LocalEngine, cairn_command, scrubbed_env, sha256_file, wait_for,
                     write_result)
from quickstart_path import PIPELINE, PROFILE, _one_run

FORGED_ACTOR = "not-the-approver"
#: how many rows the truncation cuts
CUT = 3
BROKEN_EXIT = 2


@dataclass(frozen=True)
class Verdict:
    """What ``cairn admin audit verify --json`` said about one file."""
    exit_code: int
    rows: int
    head: str
    ok: bool
    breaks: tuple[tuple[int, str], ...]


@dataclass(frozen=True)
class HeadVerdict:
    """What ``cairn admin lineage --json --expect-head`` said about one file."""
    exit_code: int
    chain_ok: bool


def _cli(log: Path, *args: str) -> tuple[int, dict]:
    """The exit code and JSON report of a read-only command. Exit 1 means the file could not be
    read, which is neither answer this measurement is after."""
    done = subprocess.run([cairn_command(), *args, "--json", "--audit-log", str(log)], cwd=log.parent,
                          env={**scrubbed_env(), "HOME": str(log.parent)}, capture_output=True, text=True,
                          timeout=120)
    command = f"cairn {' '.join(args)}"
    if done.returncode not in (0, BROKEN_EXIT):
        raise RuntimeError(f"`{command}` exited {done.returncode} on {log.name}:\n{done.stderr}")
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError as e:
        raise RuntimeError(f"`{command}` did not print JSON on {log.name}: {done.stdout!r}") from e


def _verify(log: Path) -> Verdict:
    code, report = _cli(log, "admin", "audit", "verify")
    return Verdict(code, report["rows"], report["head_hash"], report["ok"],
                   tuple((b["row"], b["reason"]) for b in report["broken"]))


def _check_head(log: Path, head: str) -> HeadVerdict:
    # --json: without it a log with no lineage records exits 1 whether or not the head matches
    code, report = _cli(log, "admin", "lineage", "--expect-head", head)
    return HeadVerdict(code, report["chain_ok"])


def _approval_position(lines: list[str]) -> int:
    """The 1-based position of the first approval a person gave, which must have a row before it
    and two after it, so that every alteration below lands in the middle of the log."""
    actions = [json.loads(line).get("action") for line in lines]
    if "pipeline.gate_approved" not in actions:
        raise RuntimeError(f"the log has no approval to alter: {actions}")
    at = actions.index("pipeline.gate_approved") + 1
    if not 2 <= at <= len(lines) - 2:
        raise RuntimeError(f"the approval is at row {at} of {len(lines)}, not in the middle of the log")
    return at


def _changed(lines: list[str], at: int) -> list[str]:
    """Row ``at`` names someone else; its hashes stay as they were."""
    row = json.loads(lines[at - 1])
    return [*lines[:at - 1], json.dumps({**row, "actor": FORGED_ACTOR}), *lines[at:]]


def _deleted(lines: list[str], at: int) -> list[str]:
    return [*lines[:at - 1], *lines[at:]]


def _inserted(lines: list[str], at: int) -> list[str]:
    """A forged copy of row ``at`` goes before it, as well forged as the file allows: it links to
    the row before it and its own hash is right, so the first row that can't hold is the next."""
    from cairn.core.obs.audit import chain_hash  # noqa: PLC0415 — the rule the writer and `audit verify` share

    row = _payload(lines[at - 1])
    prev = json.loads(lines[at - 2])["hash"]
    forged = {**row, "actor": FORGED_ACTOR}
    return [*lines[:at - 1], json.dumps({**forged, "prev_hash": prev, "hash": chain_hash(prev, forged)}),
            *lines[at - 1:]]


def _swapped(lines: list[str], at: int) -> list[str]:
    """Row ``at`` trades places with the row after it."""
    return [*lines[:at - 1], lines[at], lines[at - 1], *lines[at + 1:]]


def _payload(line: str) -> dict:
    return {k: v for k, v in json.loads(line).items() if k not in ("prev_hash", "hash")}


def _rewritten(lines: list[str], at: int) -> list[str]:
    """Row ``at`` names someone else and every hash from it on is recomputed, which anyone who can
    write the file can do: the rule is public and takes no key."""
    from cairn.core.obs.audit import GENESIS_HASH, chain_hash  # noqa: PLC0415

    out = lines[:at - 1]
    prev = json.loads(out[-1])["hash"] if out else GENESIS_HASH
    for line in lines[at - 1:]:
        row = _payload(line)
        if len(out) == at - 1:
            row["actor"] = FORGED_ACTOR
        digest = chain_hash(prev, row)
        out.append(json.dumps({**row, "prev_hash": prev, "hash": digest}))
        prev = digest
    return out


def _caught(what: str, verdict: Verdict, *, rows: int, breaks: tuple[tuple[int, str], ...]) -> None:
    """The documented exit code, and exactly the breaks this alteration must cause."""
    if verdict.exit_code != BROKEN_EXIT or verdict.ok:
        raise RuntimeError(f"{what}: not detected: exit {verdict.exit_code}, ok={verdict.ok}")
    if verdict.breaks != breaks:
        raise RuntimeError(f"{what}: reported {verdict.breaks}, expected {breaks}")
    if verdict.rows != rows:
        raise RuntimeError(f"{what}: read {verdict.rows} rows, expected {rows}")


def _passes(what: str, verdict: Verdict, *, rows: int) -> None:
    """The chain holds. On an altered log this is a limit we publish, so a run where it stops
    being true fails too: the docs and this script then say the wrong thing."""
    if verdict.exit_code != 0 or not verdict.ok or verdict.breaks:
        raise RuntimeError(f"{what}: expected the chain to verify: exit {verdict.exit_code}, "
                           f"ok={verdict.ok}, breaks={verdict.breaks}")
    if verdict.rows != rows:
        raise RuntimeError(f"{what}: read {verdict.rows} rows, expected {rows}")


def _head_matches(what: str, verdict: HeadVerdict) -> None:
    if verdict.exit_code != 0 or not verdict.chain_ok:
        raise RuntimeError(f"{what}: expected the head to match: exit {verdict.exit_code}, "
                           f"chain_ok={verdict.chain_ok}")


def _head_refused(what: str, verdict: HeadVerdict) -> None:
    if verdict.exit_code != BROKEN_EXIT or verdict.chain_ok:
        raise RuntimeError(f"{what}: not detected with the saved head: exit {verdict.exit_code}, "
                           f"chain_ok={verdict.chain_ok}")


def _write_altered(scratch: Path, name: str, altered: list[str], original: list[str]) -> Path:
    if altered == original:
        raise RuntimeError(f"the alteration {name!r} changed nothing")
    path = scratch / f"{name}.jsonl"
    path.write_text("".join(f"{line}\n" for line in altered), encoding="utf-8")
    return path


def _detected(breaks: tuple[tuple[int, str], ...]) -> str:
    rows = ", ".join(str(row) for row, _ in breaks)
    reasons = sorted({reason for _, reason in breaks})
    return f"detected: exit {BROKEN_EXIT}, row{'s' if len(breaks) > 1 else ''} {rows} ({', '.join(reasons)})"


def _measure(untouched: Path, verify: Callable[[Path], Verdict],
             check_head: Callable[[Path, str], HeadVerdict]) -> list[dict[str, str]]:
    """Every check, on copies of ``untouched``; the result's rows once all of them held."""
    scratch = untouched.parent
    lines = untouched.read_text(encoding="utf-8").splitlines()
    total, at = len(lines), _approval_position(lines)

    baseline = verify(untouched)
    _passes("the untouched log", baseline, rows=total)
    head = baseline.head
    _head_matches("the untouched log", check_head(untouched, head))
    results = [
        {"what": f"The untouched log, {total} rows, with `cairn admin audit verify`", "result": "verifies: exit 0"},
        {"what": "The untouched log against its own head, with `cairn admin lineage --expect-head`",
         "result": "matches: exit 0"},
    ]

    for name, what, altered, rows, breaks in (
        ("changed", f"Changed who gave an approval, row {at}", _changed(lines, at), total, ((at, "hash"),)),
        ("deleted", "Deleted that row", _deleted(lines, at), total - 1, ((at, "prev_hash"),)),
        ("inserted", "Inserted a forged approval before it, correctly linked, so the row after the forgery breaks",
         _inserted(lines, at), total + 1, ((at + 1, "prev_hash"),)),
        ("swapped", "Swapped it with the next row", _swapped(lines, at), total,
         ((at, "prev_hash"), (at + 1, "prev_hash"), (at + 2, "prev_hash"))),
    ):
        _caught(what, verify(_write_altered(scratch, name, altered, lines)), rows=rows, breaks=breaks)
        results.append({"what": what, "result": _detected(breaks)})

    for name, what, altered, rows in (
        ("truncated", f"Limit: cut the last {CUT} rows", lines[:-CUT], total - CUT),
        ("emptied", "Limit: emptied the file", [], 0),
        ("rewritten", f"Limit: changed row {at} and recomputed every hash after it", _rewritten(lines, at), total),
        ("rewritten-all", "Limit: changed row 1 and recomputed every hash in the file", _rewritten(lines, 1), total),
    ):
        path = _write_altered(scratch, name, altered, lines)
        _passes(what, verify(path), rows=rows)
        _head_refused(what, check_head(path, head))
        results.append({"what": what, "result": f"NOT detected by the chain: exit 0. Detected only against a head "
                                                f"saved outside the log (`--expect-head`): exit {BROKEN_EXIT}"})

    # a head saved when the log was shorter: it says nothing about the rows written since
    older = scratch / "truncated.jsonl"
    older_head = verify(older).head
    what = "Limit: rows written after a head was saved, then cut"
    _head_matches(what, check_head(older, older_head))
    results.append({"what": what, "result": "NOT detected, even with that head: exit 0"})
    what = "Limit: a head saved earlier, on a log that has grown since"
    _head_refused(what, check_head(untouched, older_head))
    results.append({"what": what, "result": f"reported as a mismatch: exit {BROKEN_EXIT}. "
                                            "The saved head has to be the latest one"})
    return results


def _wait_for_last_row(engine: Any, pipeline_id: str, timeout: float = 60.0) -> None:
    """Until the row that ends this pipeline is in the log, which comes after the record says done."""
    wait_for(lambda: engine.audit_log.read_text(encoding="utf-8"),
             lambda text: any("pipeline.finished" in line and pipeline_id in line for line in text.splitlines()),
             what=f"the audit row that ends {pipeline_id}", timeout=timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args()

    from cairn_client import CairnClient  # noqa: PLC0415 — after --help, which needs no install

    with tempfile.TemporaryDirectory(prefix="cairn-benchmark-") as scratch:
        untouched = Path(scratch) / "audit.jsonl"
        with LocalEngine(PROFILE) as engine:
            client = CairnClient(engine.url)
            client.login("dev", "dev")
            try:
                _, _, pipeline_id = _one_run(client)
                _wait_for_last_row(engine, pipeline_id)
                shutil.copyfile(engine.audit_log, untouched)
            except Exception:
                print(engine.log_tail(), file=sys.stderr)
                raise
        results = _measure(untouched, _verify, _check_head)

    path = write_result(args.out, "audit_tamper", title="What the audit log detects, and what it can't", inputs={
        "pipeline": PIPELINE, "profile": PROFILE.name, "profile_sha256": sha256_file(PROFILE),
        "rows_cut": CUT,
    }, results=results)
    print(path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
