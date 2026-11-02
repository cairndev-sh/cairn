"""``cairn status`` and ``cairn decide``: one door to look, one door to decide (ADR-0165 D4, D5).

Thin over the same ``cairn-client`` calls the ``remote`` commands use: no new route or status. A
``pl_…`` id is a pipeline (the store's id prefix); anything else is a run.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Optional

import typer

from cairn.cli.commands import remote
from cairn.cli.commands.remote import _PROF, _TOK, _call, _fail

#: ADR-0165 D9 — long form only: on these commands `-e` is never the endpoint.
_ENDPOINT = typer.Option(None, "--endpoint", help="server URL (overrides profile/env)")

_HUMAN_WAITS = ("awaiting_approval", "awaiting_advance")
_ACTIVE = ("queued", "running", "paused")
_GLYPH = {"done": "✓", "failed": "✗", "cancelled": "✗", "skipped": "–", "running": "▶", "queued": "·",
          "pending": "·"}
_ANSWERS = ("approve", "reject", "cancel")


def _is_pipeline(target: str) -> bool:
    return target.startswith("pl_")


# ── what's running ────────────────────────────────────────────────────────────


def _overview_rows(c: Any, status: Optional[str], limit: int) -> tuple[list[dict], list[dict]]:
    """Pipelines newest first, and the runs that belong to no pipeline."""
    pipes = (_call(c.pipelines, status=status) or {}).get("pipelines", [])
    pipes = sorted(pipes, key=lambda p: str(p.get("created_at") or ""), reverse=True)[:limit]
    runs = _call(c.runs, status=status, limit=limit) or []
    return pipes, [r for r in runs if not r.get("bundle_pipeline_id")]


def _print_overview(pipes: list[dict], runs: list[dict]) -> None:
    active = [p for p in pipes if p.get("status") in _ACTIVE]
    finished = [p for p in pipes if p.get("status") not in _ACTIVE]
    for title, rows in (("RUNNING", active), ("FINISHED", finished)):
        if rows:
            typer.secho(title, bold=True)
        for p in rows:
            waiting = [s for s, st in (p.get("steps") or {}).items() if str(st) in _HUMAN_WAITS]
            typer.echo(f"  {remote._pipeline_line(p)}" + (f"  waiting: {' '.join(waiting)}" if waiting else ""))
    if runs:
        typer.secho("RUNS", bold=True)
    for r in runs:
        typer.echo(f"  {r.get('id', ''):22s} {r.get('status', ''):10s} {r.get('template_ref') or ''}")
    if pipes or runs:
        typer.echo("\ncairn status <id> for details")
    else:
        typer.echo("(nothing has run yet)")


# ── one pipeline, one run ─────────────────────────────────────────────────────


def _print_pipeline(pipeline_id: str, p: dict) -> None:
    typer.secho(f"{p.get('pipeline_name') or ''}  {pipeline_id}  {p.get('status', '')}", bold=True)
    trigger = p.get("trigger") or {}
    if trigger.get("publisher"):  # who started it (#289)
        by = str(trigger["publisher"]).removeprefix("pipeline:")
        typer.echo(f"Started by  {by}" + (f"  ({trigger['topic']})" if trigger.get("topic") else ""))
    typer.echo("")
    errors = {sid: str((run or {}).get("error") or "") for sid, run in (p.get("step_runs") or {}).items()}
    order = p.get("step_order") or list((p.get("step_state") or {}).keys())
    states = p.get("step_state") or {}
    for sid in order:
        st = states.get(sid) or {}
        status = str(st.get("status", ""))
        note = f"approved by {st['approver']}" if st.get("approver") else status
        typer.echo(f"  {_GLYPH.get(status, '⏸')} {sid:18s} {note}")
        if errors.get(sid):
            typer.secho(f"      {errors[sid][:200]}", fg="red")
    typer.echo("")
    remote._print_pipeline_next(pipeline_id, p)
    typer.echo("More: --results   --logs   --follow")


def _print_run(run_id: str, run: dict) -> None:
    typer.secho(f"{run.get('template_ref') or ''}  {run_id}  {run.get('status', '')}", bold=True)
    if run.get("bundle_pipeline_id"):
        typer.echo(f"Pipeline    {run['bundle_pipeline_id']}  step {run.get('step_id') or '-'}")
    total = (run.get("cost") or {}).get("total_usd") if isinstance(run.get("cost"), dict) else None
    if total:
        typer.echo(f"Cost        ${float(total):.2f}")
    if run.get("error"):
        typer.secho(f"Error       {str(run['error'])[:300]}", fg="red")
    if run.get("status") == "awaiting_approval":
        typer.echo(f"Decide      cairn decide {run_id} approve   (or: reject)")
    typer.echo("More: --results   --logs   --follow   --replay")


def _print_logs(c: Any, target: str, *, after_seq: int, events_only: bool, as_json: bool) -> None:
    """A run's event log, or each leg's in turn for a pipeline."""
    if _is_pipeline(target):
        p = _call(c.pipeline, target) or {}
        legs = [(sid, st.get("run_id")) for sid, st in (p.get("step_state") or {}).items() if st.get("run_id")]
    else:
        legs = [("", target)]
    collected: dict[str, list] = {}
    for sid, run_id in legs:
        events = (_call(c.run_events, run_id, after_seq=after_seq) or {}).get("events", [])
        if events_only:
            events = [ev for ev in events if ev.get("type") in remote._LIFECYCLE]
        collected[run_id] = events
        if as_json:
            continue
        if sid:
            typer.secho(f"{sid}  {run_id}", fg="cyan")
        for ev in events:
            remote._print_event(ev.get("seq"), ev.get("type"), ev.get("data"))
    if as_json:
        typer.echo(json.dumps(collected, indent=2))


# ── following a pipeline ──────────────────────────────────────────────────────


def follow_pipeline(c: Any, pipeline_id: str, *, decision: Optional[str], native: bool,
                    timeout: Optional[float], on_timeout: str, dump: Optional[str] = None,
                    interval: float = 2.0, quiet: bool = False) -> int:
    """Stay with a pipeline to the end, asking at each human wait. Returns the exit code: 0 done,
    1 failed or cancelled, 2 waiting on a person with no way to ask (it says the command).
    ``quiet`` keeps stdout for the caller's ``--json``: it prints nothing and never prompts."""
    approver = os.environ.get("USER", "cli-user")
    last = ""
    told = False
    answered: set[tuple[str, Any]] = set()  # a pause is answered once, however slowly the bridge takes it up
    while True:
        p = remote._poll_pipeline(c, pipeline_id)
        render = json.dumps(p.get("step_state"), sort_keys=True)
        if render != last and not quiet:
            typer.echo(f"  {p.get('status')}")
            remote._print_steps(p)
        last = render
        status = p.get("status")
        if status in remote._TERMINAL:
            if status != "done" and not quiet:  # done: the results say it, and the chain may go on
                remote._print_pipeline_next(pipeline_id, p)
            return 0 if status == "done" else 1
        waiting = [s for s, st in (p.get("step_state") or {}).items()
                   if st.get("status") in _HUMAN_WAITS and (s, st.get("awaiting_since")) not in answered]
        if status == "paused" and waiting:
            if quiet and not decision:
                return 2
            if not quiet:
                remote._print_pipeline_next(pipeline_id, p, approver=approver)
            if dump:
                with open(dump, "w", encoding="utf-8") as fh:
                    json.dump(p, fh, indent=2)
            for sid in waiting:
                answer = remote._decide(decision_opt=decision, native=native, timeout=timeout, on_timeout=on_timeout)
                if answer is None:
                    typer.secho(f"detached: {pipeline_id} is still waiting at {sid}.", fg="yellow")
                    return 2
                if answer == "abort":
                    _call(c.cancel_pipeline, pipeline_id)
                    typer.secho(f"aborted: {pipeline_id} cancelled", fg="yellow")
                    return 1
                if answer == "reject":
                    reason = typer.prompt("Reason") if remote._is_interactive() else "rejected with --decision"
                    _call(c.reject_step, pipeline_id, sid, reason=reason, approver=approver)
                    typer.secho(f"rejected {sid}", fg="yellow")
                    break  # the pipeline is ending: any other waiting gate no longer applies
                if p["step_state"][sid].get("status") == "awaiting_approval":
                    _call(c.approve_step, pipeline_id, sid, approver=approver)
                else:
                    _call(c.advance_step, pipeline_id, sid, approver=approver)
                answered.add((sid, p["step_state"][sid].get("awaiting_since")))
                typer.secho(f"{answer}d {sid}", fg="green")
        elif status == "paused" and not told and not answered and not quiet:
            typer.echo("  waiting on a schedule or an event, not a person. Ctrl-C to detach; the pipeline keeps going.")
            told = True
        time.sleep(max(0.5, interval))  # also lets the bridge take a decision up before the next read


#: How long to look for a pipeline an `emits:` started: the bus delivers at once, so this is short.
_START_WAIT_S = 10.0


def _pipelines_started_by(c: Any, rec: dict, seen: set[str]) -> list[str]:
    """Pipelines the finished ``rec`` started through its events (#289 names the publisher on the
    started record's ``trigger``). Only looks when the plan emits something, and gives up after
    :data:`_START_WAIT_S`: an event with no enabled trigger on it starts nothing, which is normal."""
    plan = rec.get("plan") or {}
    if not plan.get("emits") and not any(s.get("emits") for s in plan.get("steps") or []):
        return []
    pipeline_id = rec.get("pipeline_id")
    deadline = time.monotonic() + _START_WAIT_S
    while True:
        started = []
        rows = (_call(c.pipelines) or {}).get("pipelines", [])
        for row in rows:
            other = row.get("pipeline_id")
            if other in seen or str(row.get("created_at") or "") < str(rec.get("created_at") or ""):
                continue
            full = _call(c.pipeline, other) or {}
            if (full.get("trigger") or {}).get("correlation_id") == pipeline_id:
                started.append(other)
        if started or time.monotonic() >= deadline:
            return started
        time.sleep(1.0)


def _print_results(rec: dict) -> None:
    """A finished pipeline's declared ``outputs:`` (ADR-0161)."""
    rows = [(k, v) for k, v in (rec.get("outputs") or {}).items() if v is not None]
    if not rows:
        return
    typer.secho(f"  Result of {rec.get('pipeline_name') or rec.get('pipeline_id')}", bold=True)
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        typer.echo(f"    {remote._safe_line(key):<{width}}  {remote._safe_line(value)}")


def follow_chain(c: Any, pipeline_id: str, *, quiet: bool = False, **kw: Any) -> tuple[int, list[dict]]:
    """Follow a pipeline, then any pipeline its events start, and those they start. Stops at the
    first that doesn't end ``done``. Returns the exit code and the records, in order."""
    queue, seen, records = [pipeline_id], {pipeline_id}, []
    while queue:
        current = queue.pop(0)
        code = follow_pipeline(c, current, quiet=quiet, **kw)
        rec = _call(c.pipeline, current) or {}
        records.append(rec)
        if code:
            return code, records
        if not quiet:
            _print_results(rec)
        started = _pipelines_started_by(c, rec, seen)
        for other in started:
            seen.add(other)
            queue.append(other)
            if not quiet:
                typer.secho(f"  → {current} started {other}", fg="cyan")
    if not quiet:
        typer.secho(f"  ✓ done: {' → '.join(r.get('pipeline_id', '') for r in records)}", fg="green")
    return 0, records


def launch_and_follow(ref: str, inputs: dict, *, environment: Optional[str], preset: Optional[str],
                      save_preset: Optional[str], detach: bool, interval: float, timeout: Optional[float],
                      on_timeout: str, native: bool, dump: Optional[str], json_out: bool,
                      endpoint: Optional[str], token: Optional[str], profile: Optional[str]) -> int:
    """``cairn run <pipeline>``: through the launch door, then stay with it (ADR-0165 D3)."""
    with remote._session(endpoint, token, profile) as (ep, tok, local):
        if local and detach:
            typer.secho("ephemeral engine: it stops with this command, so --detach is ignored.", fg="yellow", err=True)
            detach = False
        # generous: the in-process engine can stall on a leg's cold start, and a launch is not retryable
        with remote._api(ep, tok, timeout=300.0) as c:
            out = _call(c.launch_pipeline, ref, inputs, environment=environment or None, preset=preset) or {}
            pipeline_id = out.get("pipeline_id")
            typer.secho(f"pipeline_id={pipeline_id} status={out.get('status')}", fg="green", err=json_out)
            if save_preset:
                remote._save_preset(c, ref, preset, inputs, save_preset)
            if detach:
                if json_out:
                    typer.echo(json.dumps(out, indent=2))
                else:
                    remote._next("watch", f"cairn status {pipeline_id} --follow")
                return 0
            code, records = follow_chain(
                c, pipeline_id, quiet=json_out, decision=None, native=native, timeout=timeout,
                on_timeout=on_timeout, dump=dump, interval=interval)
            if json_out:
                typer.echo(json.dumps({"pipelines": records}, indent=2))
            return code


# ── the commands ──────────────────────────────────────────────────────────────


def status_cmd(
    target: Optional[str] = typer.Argument(None, help="a pipeline (pl_…) or run id; none: what's running and what just finished"),
    status: Optional[str] = typer.Option(None, "--status", help="no id: filter by status (running, paused, done, failed)"),
    limit: int = typer.Option(20, "--limit", help="no id: how many"),
    watch: bool = typer.Option(False, "--watch", help="no id: keep refreshing (Ctrl-C to stop)"),
    interval: float = typer.Option(2.0, "--interval", "-n", help="with --watch: seconds between refreshes"),
    results: bool = typer.Option(False, "--results", help="every step's findings"),
    logs: bool = typer.Option(False, "--logs", help="the event log"),
    after_seq: int = typer.Option(0, "--after-seq", help="with --logs: resume after this event"),
    events_only: bool = typer.Option(False, "--events-only", help="with --logs or --follow: only lifecycle and transition events"),
    follow: bool = typer.Option(False, "--follow", "-f", help="stay attached and ask at each gate"),
    replay: bool = typer.Option(False, "--replay", help="replay the run's persisted trace"),
    source: str = typer.Option("auto", "--source", help="with --replay: auto, file or db"),
    decision: Optional[str] = typer.Option(None, "--decision", help="with --follow: answer each pause without asking (approve or reject)"),
    timeout: Optional[float] = typer.Option(None, "--timeout", help="with --follow: seconds to wait at a prompt before --on-timeout"),
    on_timeout: str = typer.Option("detach", "--on-timeout", help="with --follow: at a prompt timeout, detach, reject or abort"),
    native: bool = typer.Option(False, "--native", help="with --follow: prompt with an OS dialog"),
    dump: Optional[str] = typer.Option(None, "--dump", help="with --follow: write a pause's context to this file"),
    json_out: bool = typer.Option(False, "--json", help="the full record, or the raw form of what was asked for"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """See what's running, or one pipeline's or run's state and results."""
    if on_timeout not in ("detach", "reject", "abort"):
        _fail("--on-timeout must be detach, reject or abort")
    if target is None:
        if results or logs or follow or replay:
            _fail("--results, --logs, --follow and --replay need an id: cairn status <id> ...")
        with remote._session(endpoint, token, profile) as (ep, tok, _local):
            with remote._api(ep, tok) as c:
                while True:
                    pipes, runs = _overview_rows(c, status, limit)
                    if json_out:
                        typer.echo(json.dumps({"pipelines": pipes, "runs": runs}, indent=2))
                    else:
                        _print_overview(pipes, runs)
                    if not watch:
                        return
                    time.sleep(max(0.5, interval))
                    typer.echo("")
    if replay:
        from cairn.cli.commands import replay as replay_cmd  # noqa: PLC0415 — it pulls the run store

        replay_cmd.cmd(run_id=target, json=json_out, source=source)
        return
    if results:
        remote._show_findings(target, json_out, endpoint, token, profile)
        return
    pipeline = _is_pipeline(target)
    if logs and follow:
        if pipeline:
            _fail(f"--logs --follow takes a run. To stay with a pipeline: cairn status {target} --follow")
        ep, tok = remote._resolve(endpoint, token, profile)
        remote._tail(ep, tok, target, after_seq=after_seq, flow=events_only)
        return
    if follow and not pipeline:
        ep, tok = remote._resolve(endpoint, token, profile)
        raise typer.Exit(remote._watch_hitl(
            ep, tok, target, approver=os.environ.get("USER", "cli-user"), decision_opt=decision, native=native,
            timeout=timeout, on_timeout=on_timeout, flow=events_only, dump=dump))
    with remote._session(endpoint, token, profile) as (ep, tok, _local):
        with remote._api(ep, tok) as c:
            if logs:
                _print_logs(c, target, after_seq=after_seq, events_only=events_only, as_json=json_out)
            elif follow:
                code, _records = follow_chain(c, target, decision=decision, native=native, timeout=timeout,
                                              on_timeout=on_timeout, dump=dump, interval=interval)
                raise typer.Exit(code)
            elif pipeline:
                p = _call(c.pipeline, target) or {}
                if json_out:
                    typer.echo(json.dumps(p, indent=2))
                else:
                    _print_pipeline(target, p)
            else:
                run = _call(c.run, target) or {}
                if json_out:
                    typer.echo(json.dumps(run, indent=2))
                else:
                    _print_run(target, run)


def decide_cmd(
    target: str = typer.Argument(..., help="a pipeline (pl_…) or run id"),
    answer: str = typer.Argument(..., help="approve, reject or cancel"),
    step: Optional[str] = typer.Option(None, "--step", help="which gate, when a pipeline waits at more than one"),
    reason: Optional[str] = typer.Option(None, "--reason", help="why; required to reject a pipeline gate"),
    approver: Optional[str] = typer.Option(None, "--approver", help="recorded when sign-in is off; under sign-in the server records your identity"),
    endpoint: Optional[str] = _ENDPOINT,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
) -> None:
    """Approve, reject or cancel. The id picks the behaviour."""
    answer = answer.strip().lower()
    if answer not in _ANSWERS:
        _fail(f"the answer is one of {', '.join(_ANSWERS)}, not {answer!r}")
    if answer == "cancel":
        if _is_pipeline(target):
            remote._cancel_pipeline(target, endpoint, token, profile)
        else:
            remote._cancel_run(target, endpoint, token, profile)
        return
    if not _is_pipeline(target):
        if step:
            _fail(f"--step names a pipeline's gate; {target} is a run")
        remote._resume(target, answer, approver or "cli-user", endpoint, token, profile, note=reason)
        return
    if answer == "approve" and reason:
        _fail("--reason goes with reject: a pipeline gate's approval records who, not why")
    step = step or _only_waiting_step(target, endpoint, token, profile)
    if answer == "approve":
        remote._advance_step(target, step, approver, endpoint, token, profile)
    else:
        remote._reject_gate(target, step, reason, approver or "cli-user", endpoint, token, profile)


def _only_waiting_step(pipeline_id: str, endpoint: Optional[str], token: Optional[str], profile: Optional[str]) -> str:
    """The one step a person can answer. None or several is a refusal that says what to do."""
    with remote._session(endpoint, token, profile) as (ep, tok, _local):
        with remote._api(ep, tok) as c:
            states = (_call(c.pipeline, pipeline_id) or {}).get("step_state") or {}
    waiting = [s for s, st in states.items() if st.get("status") in _HUMAN_WAITS]
    if not waiting:
        _fail(f"{pipeline_id} is not waiting on anyone")
    if len(waiting) > 1:
        _fail(f"{pipeline_id} is waiting at {', '.join(waiting)}: name one with --step")
    return waiting[0]
