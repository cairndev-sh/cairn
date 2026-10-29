"""``cairn lineage`` — show the content-addressed provenance record(s) for a subject (#146).

Domain-neutral. It reads the hash-chained audit log (the same ``CAIRN_AUDIT_LOG``
that any ``…-lineage-audit`` node writes to), finds the ``lineage.record`` rows for
a subject, VERIFIES the chain as it reads (recomputing each row's hash from the
previous — tamper-evidence you can see), and renders whatever the record carries
plus the ``run_id`` to reproduce it (``cairn replay <run_id>``).

The model-training spine is the first writer (subject = the model name; keys =
dataset/config/adapter/eval/cost/promotion), but the reader assumes none of that:
an incident RCA, a Terraform plan review, or any run that writes a
``lineage.record`` row gets the same listing, chain verification and replay for
free. Read-only: it never mutates the log.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import argparse
import json as _json
import os
import sys
from pathlib import Path
from typing import Any, Optional

import typer

_ACTION = "lineage.record"
# Header keys rendered specially; everything else in the record body is rendered
# generically (so the reader needs no knowledge of any domain's record shape).
_HEADER_KEYS = {"subject", "model_name", "run_id"}


#: The capability whose provider writes ``lineage.record`` rows — the platform's lineage
#: operator, absent from a public install (#2536, ADR-0127 D7). The reader names the
#: capability, never the operator: core hard-codes no ref of a non-public pack (ADR-0120 D3).
_CAPABILITY = "lineage.record"


def _writer_installed() -> bool:
    """Does any installed operator provide the ``lineage.record`` capability? Read from the
    operator packs' manifests (``pack.toml``), without importing any operator pack."""
    from cairn.core.operator.loader import discover_operator_pack_manifests  # noqa: PLC0415 — off the CLI's import path

    return any(_CAPABILITY in (e.provides_capabilities or ())
               for e in discover_operator_pack_manifests().values())


def _no_writer_lines() -> list[str]:
    return [
        "no lineage records: they are written by the platform's lineage operator, which "
        "this install does not have.",
        "  For a run's graph (the runs it came from and the runs derived from it) ask the "
        "server: GET /runs/{id}/lineage; `cairn remote status <run>` shows the run itself.",
    ]


def _audit_path(override: Optional[str]) -> Path:
    """The audit log path: ``--audit-log`` wins, else ``CAIRN_AUDIT_LOG``, else CWD."""
    return Path(override or getenv("CAIRN_AUDIT_LOG", "audit.jsonl"))


def _read_chain(path: Path) -> tuple[list[dict], list[int]]:
    """Read every row + verify the hash-chain. Returns (rows, broken_indices).

    The rule lives in ONE place, ``cairn.core.obs.audit`` (ADR-0156 D3): this reader
    parses the rows for display and asks ``verify_chain`` which links are broken
    (0-based indices into ``rows``; a corrupt line is a break and a placeholder row).
    """
    from cairn.core.obs.audit import verify_chain  # noqa: PLC0415 — keep obs off the CLI import path

    text = path.read_text(encoding="utf-8")
    rows: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = _json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("not an object")
        except Exception:  # noqa: BLE001
            row = {"_corrupt": line}
        rows.append(row)
    report = verify_chain(text.splitlines())
    return rows, [b.row - 1 for b in report.broken]


def _subject_of(row: dict) -> str:
    """The thing a record is about — generic ``subject``, else the model name, else
    the audit event ``target``. Lets one reader serve every domain's records."""
    meta = row.get("metadata") or {}
    return meta.get("subject") or meta.get("model_name") or row.get("target") or "?"


def _matches(row: dict, subject: Optional[str]) -> bool:
    if row.get("action") != _ACTION:
        return False
    if not subject:
        return True
    meta = row.get("metadata") or {}
    return subject in (row.get("target"), meta.get("model_name"), meta.get("subject"))


def _scalar(v: Any) -> str:
    return _json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)


def _fmt_value(v: Any) -> str:
    """Render a record field generically: a dict → ``k=v`` pairs, a list → CSV,
    a scalar → itself. One level of structure is enough for a provenance record."""
    if isinstance(v, dict):
        return "  ".join(f"{k}={_scalar(sub)}" for k, sub in v.items())
    if isinstance(v, list):
        return ", ".join(_scalar(x) for x in v)
    return str(v)


def _field(label: str, value: str) -> str:
    key = label + ":"
    pad = " " * max(1, 11 - len(key))  # ≥1 space even when the key overflows the column
    return f"  {key}{pad}{value}"


def _render_record(row: dict) -> str:
    """Render any lineage record: the subject heading + run + every body key it
    carries + the audit-chain receipt + the replay command. No domain assumptions."""
    rec = row.get("metadata") or {}
    run_id = rec.get("run_id") or row.get("run_id") or "?"
    lines = [
        _subject_of(row),
        _field("run", run_id),
        _field("ts", row.get("ts") or "(stamped at write)"),
    ]
    for k, v in rec.items():
        if k in _HEADER_KEYS:
            continue
        lines.append(_field(k, _fmt_value(v)))
    lines.append(_field("receipt", f"{(row.get('hash') or '?')[:19]}…  (audit-chain hash)"))
    lines.append(_field("replay", f"cairn replay {run_id}"))
    return "\n".join(lines)


def _render_line(row: dict) -> str:
    rec = row.get("metadata") or {}
    run_id = rec.get("run_id") or row.get("run_id") or "?"
    receipt = (row.get("hash") or "")[:16]
    return f"{_subject_of(row):24}  {run_id:28}  {receipt}…"


def _head_mismatch(rows: list[dict], expect_head: Optional[str]) -> bool:
    """True when ``expect_head`` (a full hash or a prefix of one, as quoted from an
    earlier receipt) is not the hash of the log's last row. This is how a log that
    lost rows from its tail is told apart from one that did not: the chain alone
    verifies the same either way."""
    if not expect_head:
        return False
    last = str(rows[-1].get("hash") or "") if rows else ""
    return not (last and last.startswith(expect_head.strip()))


def _run(subject: Optional[str], *, json: bool, audit_log: Optional[str],
         expect_head: Optional[str] = None) -> int:
    path = _audit_path(audit_log)
    if not path.exists() and not _writer_installed():
        print("\n".join(_no_writer_lines()), file=sys.stderr)
        return 1
    if not path.exists():
        print(f"no audit log at {path}", file=sys.stderr)
        print("  (set CAIRN_AUDIT_LOG or pass --audit-log; a governed run writes its "
              "lineage.record row there)", file=sys.stderr)
        return 1

    rows, broken = _read_chain(path)
    matches = [r for r in rows if _matches(r, subject)]
    head_mismatch = _head_mismatch(rows, expect_head)

    if json:
        print(_json.dumps(
            {"records": [r.get("metadata") or r for r in matches],
             "chain_ok": not broken and not head_mismatch},
            indent=2, default=str,
        ))
    elif not matches:
        where = f" for {subject!r}" if subject else ""
        print(f"no lineage records{where} in {path}")
        if not _writer_installed():
            print("\n".join(_no_writer_lines()), file=sys.stderr)
        if not broken and not head_mismatch:
            return 1
    elif subject:
        for i, r in enumerate(matches):
            if i:
                print()
            print(_render_record(r))
    else:
        print(f"lineage records in {path} ({len(matches)}):\n")
        print(f"  {'SUBJECT':24}  {'RUN':28}  RECEIPT")
        for r in matches:
            print(f"  {_render_line(r)}")

    if broken:
        print(f"\n⚠ audit chain broken at row(s) {broken} — the log was altered; "
              f"provenance is NOT trustworthy.", file=sys.stderr)
        return 2
    if head_mismatch:
        print(f"\n⚠ the log's last row is not the expected head {expect_head!r} — rows "
              f"are missing from its end or it is a different log; provenance is NOT trustworthy.",
              file=sys.stderr)
        return 2
    return 0


# ─── entry points ───────────────────────────────────────────────────


def cmd(
    subject: Optional[str] = typer.Argument(
        None,
        help="Subject of the record — a model name, incident id, plan id, … "
        "(matched on the audit target / model_name / subject). Omit to list all.",
    ),
    json: bool = typer.Option(False, "--json", help="Emit the matching records as JSON."),
    audit_log: Optional[str] = typer.Option(
        None, "--audit-log", help="Audit log path (default: $CAIRN_AUDIT_LOG or ./audit.jsonl)."
    ),
    expect_head: Optional[str] = typer.Option(
        None, "--expect-head",
        help="Hash of the last row you expect (a receipt quoted earlier, full or a prefix). "
        "The chain alone cannot show rows missing from its end; this does.",
    ),
) -> None:
    """Show the content-addressed provenance/lineage record(s) for a subject."""
    raise typer.Exit(code=_run(subject, json=json, audit_log=audit_log, expect_head=expect_head))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cairn lineage",
        description="Show the content-addressed provenance record(s) for a subject.",
    )
    parser.add_argument("subject", nargs="?", default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--audit-log", default=None)
    parser.add_argument("--expect-head", default=None)
    args = parser.parse_args(argv)
    return _run(args.subject, json=args.json, audit_log=args.audit_log, expect_head=args.expect_head)


if __name__ == "__main__":
    raise SystemExit(main())
