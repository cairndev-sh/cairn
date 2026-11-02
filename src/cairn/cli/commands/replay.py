"""``cairn status <run> --replay`` (``cairn replay`` is the old name) — print what a run recorded:
its inputs, the operators it names, and its events. Nothing runs again.

Sources (``--source``, default ``auto``):
- ``file``: the on-disk ``.cairn/runs/<run_id>/{meta.json, events.jsonl}`` trace that
  :mod:`cairn.cli.runs` writes, and
- ``db``: the **DB run store** this process is configured for (sqlite/postgres, #76).

``auto`` tries the local files, then the configured store, then the engine ``cairn status`` talks to
(#369: a run on ``cairn dev`` lives in the engine's store, which this process doesn't name). When no
engine answers it reads the local dev store itself. It never boots the ephemeral engine the other
``status`` forms use: an engine re-adopts interrupted work at boot, and this command runs nothing.

``--json`` dumps the raw form for piping into other tools. Read-only.
"""

from __future__ import annotations

import argparse
import asyncio
import json as _json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import typer

from cairn.cli.commands import remote
from cairn.cli.commands.dev import dev_env
from cairn.cli.runs import read_events, read_meta, resolve_run_dir
from cairn.cli.runs.reader import run_dir_exists


# ─── rendering ──────────────────────────────────────────────────────


def _short_ts(iso: str) -> str:
    """Strip the date + tz for the per-line timestamp prefix.

    ``meta.json`` already carries the run's anchor time; per-event the user
    cares about elapsed wall-clock vs absolute date.
    """
    # ISO is "YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00"
    if "T" in iso:
        return iso.split("T", 1)[1].split("+", 1)[0][:12]
    return iso


def _summarize_event(type: str, data: dict) -> str:
    """One-line summary per event type. Unknown types fall through to a
    truncated JSON dump so future event kinds still render."""
    if type == "skill_started":
        return f"started skill={data.get('skill_name','?')}"
    if type == "findings":
        rc = (data.get("root_cause") or {}).get("description", "")
        n_ev = len(data.get("evidence") or [])
        return (f"findings skill={data.get('skill_name','?')} evidence={n_ev}"
                + (f" root_cause={rc!r}" if rc else ""))
    if type == "action_decision":
        return (f"action_decision id={data.get('candidate_id','?')} "
                f"decision={data.get('decision','?')}")
    if type == "action_outcome":
        return (f"action_outcome id={data.get('candidate_id','?')} "
                f"status={data.get('status','?')}")
    if type == "skill_completed":
        dur = data.get("duration_seconds")
        # §24.11.3b: include the template's content-addressed short-hash
        # so replay shows *which version* of the template ran. Helps with
        # "this run looks weird, what prompt did it use?" debugging.
        hash_str = ""
        h = data.get("template_hash") or ""
        if h.startswith("sha256:"):
            hash_str = f" template={data.get('template_name','?')}@{h[7:19]}"
        return (f"completed skill={data.get('skill_name','?')}"
                + hash_str
                + (f" duration={dur:.1f}s" if isinstance(dur, (int, float)) else ""))
    # Fallback for unknown types.
    blob = _json.dumps(data, default=str)
    if len(blob) > 80:
        blob = blob[:77] + "..."
    return f"{type} {blob}"


def _operators(record: dict, events: list[dict]) -> list[str]:
    """The operators a run's record names, in order. A task leg's ref is its operator; a template's
    runtime writes one per node in its `parameters` events; `resolved_operators` is the record's own
    field. A flow leg's ref names the flow, and its stages' operators aren't on the record."""
    named = [str(o.get("operator")) for o in record.get("resolved_operators") or [] if o.get("operator")]
    ref = str(record.get("template_ref") or "")
    if ref.startswith("task:"):
        named.append(ref.removeprefix("task:"))
    for ev in events:
        op = str((ev.get("data") or {}).get("operator") or "") if ev.get("type") == "parameters" else ""
        if op and not op.startswith("flow:"):
            named.append(op)
    return list(dict.fromkeys(named))


def _render_inputs(inputs: Any) -> list[str]:
    if not inputs:
        return ["  inputs:      (none recorded)"]
    lines = ["  inputs:"]
    for key, value in inputs.items():
        blob = value if isinstance(value, str) else _json.dumps(value, default=str)
        lines.append(f"    {key} = {blob if len(blob) <= 200 else blob[:197] + '...'}")
    return lines


def _render_text(run_id: str, meta: dict, events: list) -> str:
    lines: list[str] = []
    lines.append(f"run {run_id}")
    lines.append(f"  status:      {meta.get('status','?')}")
    lines.append(f"  environment: {meta.get('environment','?')}")
    if meta.get("template_ref"):
        lines.append(f"  ran:         {meta['template_ref']}")
    if "operators" in meta:  # a local file trace has neither this nor inputs
        lines.append(f"  operators:   {', '.join(meta['operators']) or '(none recorded)'}")
        lines.extend(_render_inputs(meta.get("inputs")))
    trig = meta.get("trigger") or {}
    lines.append(
        f"  trigger:     source={trig.get('source','?')} "
        f"service={trig.get('service','?')}"
    )
    if meta.get("matched_skills") is not None:
        lines.append(f"  matched:     {meta['matched_skills']} skill(s)")
    if meta.get("error"):
        lines.append(f"  error:       {meta['error']}")
    lines.append("")

    if not events:
        lines.append("(no events recorded)")
    else:
        lines.append(f"timeline ({len(events)} event(s)):")
        for ev in events:
            lines.append(f"  {_short_ts(ev.ts)}  {_summarize_event(ev.type, ev.data)}")
    return "\n".join(lines) + "\n"


# ─── shared impl ────────────────────────────────────────────────────


def _read_store(store: Any, run_id: str) -> Optional[tuple[dict, list[dict]]]:
    """A run's record and events from a store, in the shape the engine's API returns them."""
    async def _read():
        run = await store.get(run_id)
        return run, (await store.get_events(run_id) if run is not None else [])

    try:
        run, events = asyncio.run(_read())
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
    if run is None:
        return None
    return run.model_dump(mode="json"), [e.model_dump(mode="json") for e in events]


def _read_configured_store(run_id: str) -> Optional[tuple[dict, list[dict]]]:
    # Deferred import: keep the storage/DB stack (asyncpg etc.) off the hot path
    # of every `cairn` invocation (Rule 4 — measured CLI-startup deferral).
    from cairn.core.storage import default_store

    return _read_store(default_store(), run_id)


def _print_record(run_id: str, record: dict, events: list[dict], *, json: bool) -> int:
    meta = {
        "status": record.get("status"),
        "environment": record.get("environment") or "?",
        "trigger": record.get("trigger") or {},
        "matched_skills": record.get("matched_skills"),
        "error": record.get("error"),
        "template_ref": record.get("template_ref"),
        "operators": _operators(record, events),
        "inputs": record.get("inputs"),
    }
    results = record.get("result_payloads")
    if json:
        print(_json.dumps({
            "meta": meta,
            "events": [{"ts": e.get("ts"), "type": e.get("type"), "data": e.get("data")} for e in events],
            "result_payloads": results,  # full structured result(s) (#76)
        }, indent=2, default=str))
    else:
        print(_render_text(run_id, meta, [SimpleNamespace(ts=str(e.get("ts")), type=str(e.get("type")),
                                                          data=e.get("data") or {}) for e in events]))
        if results:
            print(f"result: {len(results)} structured result(s) "
                  f"recorded (see --json or GET /runs/{run_id})")
    return 0


def _run_from_db(run_id: str, *, json: bool) -> int:
    """Render a run from the configured DB run store (#76). The in-memory store (nothing configured)
    has no durable trace to read."""
    found = _read_configured_store(run_id)
    if found is None:
        print(f"no such run in the DB store: {run_id}", file=sys.stderr)
        print("  (CAIRN_STORAGE / CAIRN_RUN_DB name this store. Without --source db, "
              "the engine is asked too)", file=sys.stderr)
        return 1
    return _print_record(run_id, *found, json=json)


def _run_anywhere(run_id: str, *, json: bool, endpoint: Optional[str], token: Optional[str],
                  profile: Optional[str]) -> int:
    """No local trace: the configured store, then the engine, then the local dev store (#369)."""
    found = _read_configured_store(run_id)
    if found is not None:
        return _print_record(run_id, *found, json=json)
    ep, tok = remote._resolve(endpoint, token, profile)
    if remote._reachable(ep):
        with remote._api(ep, tok) as c:
            record = remote._call(c.run, run_id) or {}
            events = (remote._call(c.run_events, run_id) or {}).get("events", [])
        return _print_record(run_id, record, events, json=json)
    db = Path(dev_env(0)["CAIRN_STORAGE"].removeprefix("sqlite:///"))
    if db.exists():
        from cairn.core.storage import SqliteRunStore  # noqa: PLC0415 — as in _read_configured_store

        found = _read_store(SqliteRunStore(str(db)), run_id)
    if found is None:
        print(f"no such run: {run_id}", file=sys.stderr)
        print(f"  no engine answers at {ep}, and the local store {db} doesn't have it", file=sys.stderr)
        print("  start the engine it ran on (cairn setup), or name it with --endpoint", file=sys.stderr)
        return 1
    print(f"no engine answers at {ep}; read from {db}", file=sys.stderr)
    return _print_record(run_id, *found, json=json)


def _run(run_id: str, *, json: bool, source: str = "auto", endpoint: Optional[str] = None,
         token: Optional[str] = None, profile: Optional[str] = None) -> int:
    """Dispatch on ``source``.

    - ``auto`` (default): local trace files if present, else :func:`_run_anywhere`.
    - ``file``: only the on-disk ``.cairn/runs/<id>/`` trace (original).
    - ``db``: only the configured run store.
    """
    if source == "db":
        return _run_from_db(run_id, json=json)

    have_files = run_dir_exists(run_id)
    if source == "file" and not have_files:
        print(f"no such run: {run_id}", file=sys.stderr)
        print(f"  (looked in: {resolve_run_dir(run_id)})", file=sys.stderr)
        return 1
    if not have_files:
        return _run_anywhere(run_id, json=json, endpoint=endpoint, token=token, profile=profile)

    meta = read_meta(run_id) or {}
    events = list(read_events(run_id))
    if json:
        payload = {
            "meta": meta,
            "events": [{"ts": e.ts, "type": e.type, "data": e.data} for e in events],
        }
        print(_json.dumps(payload, indent=2, default=str))
    else:
        print(_render_text(run_id, meta, events))
    return 0


# ─── entry points ───────────────────────────────────────────────────


def cmd(
    run_id: str = typer.Argument(..., help="Run id from a prior `cairn run`."),
    json: bool = typer.Option(
        False, "--json",
        help="Emit raw meta+events JSON instead of the human-readable timeline.",
    ),
    source: str = typer.Option(
        "auto", "--source",
        help="Where to read it: auto (local files, the configured store, then the engine) | file | db.",
    ),
) -> None:
    """Print what a past run recorded: its inputs, operators and events. Nothing runs again."""
    raise typer.Exit(code=_run(run_id, json=json, source=source))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cairn replay",
        description="Print what a past run recorded: its inputs, operators and events. Nothing runs again.",
    )
    parser.add_argument("run_id")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--source", choices=["auto", "file", "db"], default="auto")
    args = parser.parse_args(argv)
    return _run(args.run_id, json=args.json, source=args.source)


if __name__ == "__main__":
    raise SystemExit(main())
