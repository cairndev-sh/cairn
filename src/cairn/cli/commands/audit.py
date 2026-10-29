"""``cairn audit`` — read the audit trail (ADR-0156 D2).

``audit verify`` recomputes the hash chain of the audit log that ``audit/store`` writes
(``hash = sha256(prev_hash + canonical(row))``, ADR-0028) and reports whether it holds. It
needs no lineage writer and never writes: the rule is the one in ``cairn.core.obs.audit``
(ADR-0156 D3), shared with the writer and with ``cairn lineage``.

Exit codes: 0 the chain verifies (an empty log verifies, with zero rows); 2 it is broken,
a corrupt line included; 1 there is no log, or it cannot be read.
"""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Optional

import typer

from cairn.core.settings import getenv

app = typer.Typer(help="Read the audit trail.", no_args_is_help=True)

EXIT_OK = 0
EXIT_NO_LOG = 1
EXIT_BROKEN = 2


def _audit_path(override: Optional[str]) -> Path:
    """``--audit-log`` wins, else ``CAIRN_AUDIT_LOG``, else ``./audit.jsonl`` (as ``cairn lineage``)."""
    return Path(override or getenv("CAIRN_AUDIT_LOG", "audit.jsonl"))


def _verify(audit_log: Optional[str], *, as_json: bool) -> int:
    from cairn.core.obs.audit import verify_chain  # noqa: PLC0415 — keep obs off the CLI import path

    path = _audit_path(audit_log)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        typer.echo(f"no audit log at {path}", err=True)
        typer.echo("  (set CAIRN_AUDIT_LOG or pass --audit-log)", err=True)
        return EXIT_NO_LOG
    except (OSError, UnicodeDecodeError) as e:
        typer.echo(f"cannot read the audit log at {path}: {e}", err=True)
        return EXIT_NO_LOG

    report = verify_chain(text.splitlines())
    if as_json:
        typer.echo(_json.dumps({
            "path": str(path),
            "rows": report.rows,
            "first_ts": report.first_ts,
            "last_ts": report.last_ts,
            "head_hash": report.head_hash,
            "ok": report.ok,
            "broken": [{"row": b.row, "reason": b.reason} for b in report.broken],
        }, indent=2))
    else:
        typer.echo(f"audit log: {path}")
        typer.echo(f"  rows:      {report.rows}")
        typer.echo(f"  first ts:  {report.first_ts or '-'}")
        typer.echo(f"  last ts:   {report.last_ts or '-'}")
        typer.echo(f"  head hash: {report.head_hash}")
        if report.ok:
            typer.echo("chain OK")
        else:
            first = report.broken[0]
            typer.echo(f"chain BROKEN at row {first.row} ({first.reason}); "
                       f"{len(report.broken)} broken row(s) in all — the log was altered "
                       f"or truncated.", err=True)
    return EXIT_OK if report.ok else EXIT_BROKEN


@app.command("verify")
def verify_cmd(
    audit_log: Optional[str] = typer.Option(
        None, "--audit-log", help="Audit log path (default: $CAIRN_AUDIT_LOG or ./audit.jsonl)."
    ),
    json: bool = typer.Option(False, "--json", help="Emit the report as JSON."),
) -> None:
    """Verify the audit log's hash chain (read-only). Exit 0 verified, 2 broken or corrupt, 1 no log."""
    raise typer.Exit(code=_verify(audit_log, as_json=json))
