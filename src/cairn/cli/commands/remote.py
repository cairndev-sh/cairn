"""``cairn remote`` — drive a running cairn-server from the terminal.

Submit runs, list/inspect them, stream their logs live, cancel them, and
resolve HITL approvals — against a local or hosted server. Talks the server's
existing HTTP + SSE control plane (an internal issue); the server protects
these routes with JWT auth when ``CAIRN_JWT_SECRET`` is set.

Config lives at ``~/.cairn/config.toml``::

    default_profile = "default"
    [profiles.default]
    endpoint = "http://localhost:9090"
    token = "eyJ..."        # written by `remote login`

Resolution precedence for endpoint/token: explicit ``--endpoint``/``--token`` >
env (``CAIRN_REMOTE_ENDPOINT`` / ``CAIRN_REMOTE_TOKEN``) > the named/default profile
> ``http://localhost:9090`` (no token).
"""
from __future__ import annotations
from cairn.core.settings import getenv

import contextlib
import json
import os
import select
import shutil
import socket
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any, Optional

import typer

from cairn.cli.commands.dev import dev_env

app = typer.Typer(
    name="remote",
    help="Drive a remote cairn-server: login / submit / ps / status / logs / cancel / approve / reject.",
    no_args_is_help=True,
    add_completion=False,
)

_TERMINAL = {"done", "failed", "cancelled"}
_LIFECYCLE = {"queued", "running", "awaiting_approval", "resumed", *_TERMINAL}
_DEFAULT_ENDPOINT = "http://localhost:9090"


# ─── config (~/.cairn/config.toml) ─────────────────────────────


def _config_path() -> Path:
    home = getenv("CAIRN_CONFIG_HOME") or str(Path.home() / ".cairn")
    return Path(home) / "config.toml"


def _load_config() -> dict:
    p = _config_path()
    if not p.exists():
        return {}
    try:
        return tomllib.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _dump_config(cfg: dict) -> str:
    """Minimal TOML emitter for our flat profile schema (string values only)."""
    lines: list[str] = []
    dp = cfg.get("default_profile")
    if dp:
        lines.append(f'default_profile = "{dp}"')
        lines.append("")
    for name, prof in (cfg.get("profiles") or {}).items():
        lines.append(f"[profiles.{name}]")
        for k, v in prof.items():
            if v is None:
                continue
            esc = str(v).replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'{k} = "{esc}"')
        lines.append("")
    return "\n".join(lines) + "\n"


def _save_config(cfg: dict) -> None:
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_dump_config(cfg), encoding="utf-8")
    try:
        p.chmod(0o600)  # the token is a credential
    except OSError:
        pass


def _resolve(
    endpoint: Optional[str], token: Optional[str], profile: Optional[str],
) -> tuple[str, Optional[str]]:
    cfg = _load_config()
    prof_name = profile or cfg.get("default_profile") or "default"
    prof = (cfg.get("profiles") or {}).get(prof_name, {})
    ep = (
        endpoint
        or getenv("CAIRN_REMOTE_ENDPOINT")
        or prof.get("endpoint")
        or _DEFAULT_ENDPOINT
    ).rstrip("/")
    tok = token or getenv("CAIRN_REMOTE_TOKEN") or prof.get("token")
    return ep, tok


# ─── http helpers ──────────────────────────────────────────────────
# #1195 (ADR-0085 D7): request/response calls go through cairn-client — the
# same typed, spec-anchored surface the console and future SDK consumers use.
# Only the SSE streams (`/runs/{id}/stream`) still use raw httpx, until the
# client grows a stream() door.


def _api(endpoint: str, token: Optional[str], *, timeout: Optional[float] = 30.0):
    from cairn_client import CairnClient  # noqa: PLC0415 — lazy like the old httpx import

    return CairnClient(endpoint, token=token, timeout=timeout)


def _sse_client(endpoint: str, token: Optional[str], *, timeout: Optional[float] = 30.0):
    import httpx  # noqa: PLC0415 — SSE-only raw client
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.Client(base_url=endpoint, headers=headers, timeout=timeout)


def _fail(msg: str) -> None:
    typer.secho(msg, fg="red", err=True)
    raise typer.Exit(1)


# ─── ephemeral local engine (#1195 tail, ADR-0085 D7 — ephemeral-server attach) ──
# `cairn remote launch …` must work on a fresh laptop with NOTHING running:
# when no endpoint is explicitly configured and the default one is dead, boot
# the pinned dev engine (same sqlite store `cairn dev` uses, so state persists
# across invocations) for the duration of the command, then tear it down.
# An EXPLICITLY configured endpoint that is down still fails loudly — silently
# substituting a local engine for someone's prod URL would be worse than the
# error.


def _explicit_endpoint(endpoint: Optional[str], profile: Optional[str]) -> bool:
    if endpoint or getenv("CAIRN_REMOTE_ENDPOINT"):
        return True
    cfg = _load_config()
    prof_name = profile or cfg.get("default_profile") or "default"
    return bool(((cfg.get("profiles") or {}).get(prof_name) or {}).get("endpoint"))


def _reachable(ep: str) -> bool:
    with contextlib.suppress(Exception):
        with _api(ep, None, timeout=2.0) as c:
            c.healthz()
        return True
    return False


@contextlib.contextmanager
def _ephemeral():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = dev_env(port)
    db = env["CAIRN_STORAGE"].removeprefix("sqlite:///")
    typer.secho(
        f"no engine reachable — starting an ephemeral local one (sqlite {db})",
        fg="yellow", err=True,
    )
    proc = subprocess.Popen(
        [sys.executable, "-P", "-m", "cairn.core.triggers.main"], env=env,  # -P: never the cwd's cairn/
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    ep = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30.0
        while not _reachable(ep):
            if proc.poll() is not None:
                _fail("ephemeral engine exited during startup — "
                      "run `cairn dev --engine-only` to see why.")
            if time.monotonic() > deadline:
                _fail("ephemeral engine did not become healthy within 30s.")
            time.sleep(0.3)
        with _api(ep, None) as c:
            tok = c.login("dev", "dev")  # dev_env pins dev/dev
        yield ep, tok
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


@contextlib.contextmanager
def _session(endpoint: Optional[str], token: Optional[str], profile: Optional[str]):
    """Yield ``(endpoint, token, ephemeral)`` — a reachable control plane,
    booting an ephemeral local engine only when nothing was configured."""
    ep, tok = _resolve(endpoint, token, profile)
    if _explicit_endpoint(endpoint, profile) or _reachable(ep):
        yield ep, tok, False
        return
    with _ephemeral() as (eph_ep, eph_tok):
        yield eph_ep, eph_tok, True


def _call(fn, *args, **kwargs):
    """Run one client call, translating API errors to the CLI's exit style."""
    import httpx  # noqa: PLC0415 — lazy with the client
    from cairn_client import CairnAPIError  # noqa: PLC0415

    try:
        return fn(*args, **kwargs)
    except CairnAPIError as e:
        if e.status_code == 401:
            _fail("401 Unauthorized — run `cairn remote login` (or set CAIRN_REMOTE_TOKEN).")
        _fail(f"{e.status_code}: {e.detail[:300]}")
    except httpx.HTTPError as e:
        _fail(f"transport error talking to the engine: {type(e).__name__}: {e}")


def _poll_pipeline(c, pipeline_id: str) -> dict:
    """One watch poll. The inprocess engine can stall its event loop for
    longer than the client timeout while a leg does CPU-heavy work (found
    live: presidio cold-start blocked /pipelines for >30s) — a transient
    timeout retries instead of crashing the watch (which, under an ephemeral
    engine, would tear it down and kill the in-flight leg)."""
    import httpx  # noqa: PLC0415 — lazy with the client
    from cairn_client import CairnAPIError  # noqa: PLC0415

    for _ in range(5):
        try:
            return c.pipeline(pipeline_id)
        except httpx.TimeoutException:
            time.sleep(2.0)
        except CairnAPIError as e:
            if e.status_code == 401:
                # #1234 — the token outlived by a long pipeline (default TTL
                # ~60 min). The pipeline keeps running server-side; tell the
                # user how to re-attach instead of printing a bare 401.
                _fail(
                    "token expired mid-watch (the pipeline is still running "
                    "server-side). Refresh it (`cairn remote login`, or set "
                    f"CAIRN_REMOTE_TOKEN) and re-attach: `cairn remote status {pipeline_id}`."
                )
            _fail(f"{e.status_code}: {e.detail[:300]}")
    _fail("engine stopped responding (5 consecutive poll timeouts).")


def _parse_kv(pairs: list[str]) -> dict:
    out: dict = {}
    for item in pairs:
        if "=" not in item:
            typer.secho(f"--input must be key=value, got {item!r}", fg="red", err=True)
            raise typer.Exit(2)
        k, v = item.split("=", 1)
        out[k.strip()] = v
    return out


def _print_event(seq, ev_type, data) -> None:
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            pass
    summary = ""
    if isinstance(data, dict) and data:
        summary = ", ".join(f"{k}={str(v)[:60]}" for k, v in list(data.items())[:4])
    color = {"failed": "red", "cancelled": "yellow", "done": "green"}.get(ev_type or "")
    line = f"[{seq}] {ev_type}" + (f" — {summary}" if summary else "")
    typer.secho(line, fg=color or None)


def _tail(endpoint: str, token: Optional[str], run_id: str, *, after_seq: int = 0) -> None:
    """Follow a run's SSE event stream, resuming with ``after_seq`` if the
    connection drops before the run reaches a terminal state."""
    import httpx
    last = after_seq
    while True:
        ev_type = data = seq = None
        saw_terminal = False
        try:
            with _sse_client(endpoint, token, timeout=None) as c:
                with c.stream("GET", f"/runs/{run_id}/stream", params={"after_seq": last}) as r:
                    if r.status_code == 401:
                        _fail("401 Unauthorized — run `cairn remote login`.")
                    if r.status_code == 404:
                        _fail(f"run {run_id} not found")
                    r.raise_for_status()
                    for line in r.iter_lines():
                        if line == "":
                            if ev_type is not None:
                                _print_event(seq, ev_type, data)
                                if seq is not None:
                                    last = seq
                                if ev_type in _TERMINAL:
                                    saw_terminal = True
                                    break
                            ev_type = data = seq = None
                            continue
                        if line.startswith("id:"):
                            try:
                                seq = int(line[3:].strip())
                            except ValueError:
                                seq = None
                        elif line.startswith("event:"):
                            ev_type = line[6:].strip()
                        elif line.startswith("data:"):
                            data = line[5:].strip()
        except typer.Exit:
            raise
        except httpx.HTTPError:
            pass  # dropped — fall through to a status check, then reconnect
        if saw_terminal:
            return
        # Stream closed without a terminal frame: check status; stop if the run
        # actually finished, otherwise reconnect from the last seq we saw.
        try:
            with _api(endpoint, token) as c:
                if c.run(run_id).get("status") in _TERMINAL:
                    return
        except Exception:
            return


# ─── commands ──────────────────────────────────────────────────────

_EP = typer.Option(None, "--endpoint", "-e", help="server URL (overrides profile/env)")
_TOK = typer.Option(None, "--token", help="bearer token (overrides profile/env)")
_PROF = typer.Option(None, "--profile", help="config profile name")


@app.command()
def login(
    username: Optional[str] = typer.Option(None, "--username", "-u", help="admin username"),
    password: Optional[str] = typer.Option(None, "--password", "-p", help="prompted if omitted"),
    endpoint: Optional[str] = _EP,
    profile: Optional[str] = _PROF,
):
    """Authenticate to the server (POST /token) and store the JWT in the profile."""
    ep, _ = _resolve(endpoint, None, profile)
    username = username or typer.prompt("Username", default="admin")
    password = password or typer.prompt("Password", hide_input=True)
    with _api(ep, None) as c:
        token = _call(c.login, username, password)
    cfg = _load_config()
    prof_name = profile or cfg.get("default_profile") or "default"
    cfg.setdefault("profiles", {}).setdefault(prof_name, {})
    cfg["profiles"][prof_name]["endpoint"] = ep
    cfg["profiles"][prof_name]["token"] = token
    cfg.setdefault("default_profile", prof_name)
    _save_config(cfg)
    typer.secho(f"logged in to {ep} (profile '{prof_name}'); token saved to {_config_path()}", fg="green")


@app.command()
def submit(
    skill: str = typer.Argument(..., help="skill name to fire"),
    input_: list[str] = typer.Option([], "--input", "-i", help="payload key=value (repeatable)"),
    follow: bool = typer.Option(False, "--follow", "-f", help="stream logs until the run ends"),
    wait: bool = typer.Option(False, "--wait", help="block server-side until done (realtime lane) instead of async"),
    service: Optional[str] = typer.Option(None, "--service"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Submit a run by skill name. Async by default (returns a run_id immediately)."""
    ep, tok = _resolve(endpoint, token, profile)
    body = {"skill_name": skill, "service": service, "payload": _parse_kv(input_), "wait": wait}
    with _api(ep, tok, timeout=None if wait else 30.0) as c:
        out = _call(c.trigger_manual, body)
    run_id = out.get("run_id")
    typer.secho(f"run_id={run_id} status={out.get('status')}", fg="green")
    if follow and run_id:
        _tail(ep, tok, run_id)


def _runs_table(invs: list[dict]):
    """Build a Rich table of runs — shared by `ps` (one-shot) and `watch` (live)."""
    from rich.table import Table

    _style = {"running": "yellow", "done": "green", "failed": "red",
              "cancelled": "red", "queued": "dim"}
    table = Table(show_header=True, header_style="bold")
    for col in ("run_id", "status", "lane", "skills", "template", "created"):
        table.add_column(col)
    for inv in invs:
        results = inv.get("results") or []
        tmpl = results[0].get("template_name") if results else ""
        st = inv.get("status", "")
        st_cell = f"[{_style[st]}]{st}[/]" if st in _style else st
        table.add_row(
            inv.get("id", ""), st_cell, inv.get("lane", ""),
            str(inv.get("matched_skills", "")), tmpl or "", inv.get("created_at", ""),
        )
    return table


@app.command()
def ps(
    status: Optional[str] = typer.Option(None, "--status", help="queued|running|done|failed|cancelled"),
    limit: int = typer.Option(50, "--limit", "-n"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """List runs on the server (one-shot)."""
    from rich.console import Console
    ep, tok = _resolve(endpoint, token, profile)
    with _api(ep, tok) as c:
        invs = _call(c.runs, status=status, limit=limit)
    Console().print(_runs_table(invs))
    if not invs:
        typer.echo("(no runs)")


@app.command()
def watch(
    interval: float = typer.Option(2.0, "--interval", "-n", help="refresh interval (seconds)"),
    status: Optional[str] = typer.Option(None, "--status", help="filter by status"),
    limit: int = typer.Option(50, "--limit"),
    once: bool = typer.Option(False, "--once", help="render one frame and exit (scripts/CI)"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Live-refreshing dashboard of runs on the server (Ctrl-C to stop)."""
    from rich.console import Console
    from rich.live import Live

    ep, tok = _resolve(endpoint, token, profile)

    def _frame():
        with _api(ep, tok) as c:
            return _runs_table(_call(c.runs, status=status, limit=limit))

    console = Console()
    if once:
        console.print(_frame())
        return
    with Live(_frame(), console=console, refresh_per_second=4) as live:
        try:
            while True:
                time.sleep(max(0.5, interval))
                live.update(_frame())
        except KeyboardInterrupt:
            pass


@app.command()
def status(
    run_id: str = typer.Argument(...),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Show one run's lifecycle record."""
    ep, tok = _resolve(endpoint, token, profile)
    with _api(ep, tok) as c:
        typer.echo(json.dumps(_call(c.run, run_id), indent=2))


@app.command()
def logs(
    run_id: str = typer.Argument(...),
    follow: bool = typer.Option(False, "--follow", "-f", help="tail live via SSE until terminal"),
    after_seq: int = typer.Option(0, "--after-seq", help="resume after this event seq"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Print a run's event log (snapshot), or tail it live with --follow."""
    ep, tok = _resolve(endpoint, token, profile)
    if follow:
        _tail(ep, tok, run_id, after_seq=after_seq)
        return
    with _api(ep, tok) as c:
        out = _call(c.run_events, run_id, after_seq=after_seq)
    for ev in (out or {}).get("events", []):
        _print_event(ev.get("seq"), ev.get("type"), ev.get("data"))


@app.command()
def cancel(
    run_id: str = typer.Argument(...),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Cancel a running run (best-effort)."""
    ep, tok = _resolve(endpoint, token, profile)
    with _api(ep, tok) as c:
        out = _call(c.cancel_run, run_id)
    typer.secho(f"status={(out or {}).get('status')}", fg="yellow")


# ─── interactive HITL watch loop (an internal issue) ──────────────


def _is_interactive() -> bool:
    """True only when a human can actually be prompted — stdin AND stdout are
    TTYs. False in CI / piped contexts, so we never block on stdin there."""
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:
        return False


def _native_confirm(question: str) -> Optional[bool]:
    """OS confirm/deny dialog — macOS ``osascript``, Linux ``zenity``. Returns the
    choice, or None when no native dialog is available (caller falls back to the
    terminal). Opt-in only; terminal accept/reject stays the default."""
    try:
        if sys.platform == "darwin":
            script = (
                f"display dialog {json.dumps(question)} "
                'buttons {"Reject", "Approve"} default button "Approve"'
            )
            out = subprocess.run(
                ["osascript", "-e", script], capture_output=True, text=True, timeout=300
            )
            return out.returncode == 0 and "Approve" in out.stdout
        if shutil.which("zenity"):
            rc = subprocess.run(["zenity", "--question", "--text", question], timeout=300)
            return rc.returncode == 0
    except Exception:
        return None
    return None


def _prompt_decision(timeout: Optional[float], on_timeout: str) -> Optional[str]:
    """Terminal accept/reject, optionally bounded by ``timeout`` seconds. On
    expiry apply ``on_timeout``: ``detach`` (None) | ``reject`` | ``abort``."""
    prompt = "Approve this step? [a]pprove / [r]eject / [s]kip (detach)"
    if timeout:
        prompt += f"  — auto-{on_timeout} in {int(timeout)}s"
    typer.secho(prompt, fg="cyan")
    if timeout:
        try:
            ready, _, _ = select.select([sys.stdin], [], [], timeout)
        except Exception:
            ready = [sys.stdin]  # select-on-stdin unsupported → fall back to blocking
        if not ready:
            typer.secho(f"timed out → {on_timeout}", fg="yellow", err=True)
            return None if on_timeout == "detach" else on_timeout
        ans = sys.stdin.readline().strip().lower()
    else:
        ans = input("> ").strip().lower()
    if ans in ("a", "approve", "y", "yes"):
        return "approve"
    if ans in ("r", "reject", "n", "no"):
        return "reject"
    return None  # skip / unrecognized → detach (leave the run paused)


def _decide(*, decision_opt, native, timeout, on_timeout) -> Optional[str]:
    """Resolve a HITL decision at a pause. Returns ``approve`` | ``reject`` |
    ``abort`` | None (None = detach: leave the run paused, resume later).

      * explicit ``--decision`` wins (works headless / CI);
      * else interactive TTY → prompt (optional native dialog), bounded by
        ``--timeout`` → ``--on-timeout``;
      * else headless with no decision → detach (never block on stdin in CI).
    """
    if decision_opt:
        d = decision_opt.strip().lower()
        if d not in ("approve", "reject"):
            _fail("--decision must be 'approve' or 'reject'")
        return d
    if not _is_interactive():
        typer.secho(
            "non-interactive (no TTY) — not prompting. Decide with "
            "`cairn remote approve|reject <run_id>`, or pass --decision.",
            fg="yellow", err=True,
        )
        return None
    if native:
        choice = _native_confirm("Approve this step?")
        if choice is not None:
            return "approve" if choice else "reject"
    return _prompt_decision(timeout, on_timeout)


def _as_obj(data):
    if isinstance(data, str):
        try:
            return json.loads(data)
        except Exception:
            return data
    return data


def _render_pause(endpoint, token, run_id: str, data, dump: Optional[str]) -> None:
    """Show WHAT the paused run is asking to approve (#55): the ApprovalRequest
    (operator/risk/sources/reason/allowed_decisions) from the awaiting_approval
    event or `/runs/{id}/pending-approval`, plus the run summary + recent events."""
    typer.secho(f"\n⏸  run {run_id} paused for approval (awaiting_approval)", fg="yellow", bold=True)
    ctx: dict = {"run_id": run_id, "event_data": _as_obj(data)}
    ev_obj = _as_obj(data)
    approval = ev_obj.get("approval") if isinstance(ev_obj, dict) else None
    try:
        with _api(endpoint, token) as c:
            if approval is None:  # missed the event frame — ask the API (#55)
                try:
                    approval = (c.pending_approval(run_id) or {}).get("approval")
                except Exception:
                    pass
            try:
                ctx["run"] = c.run(run_id)
            except Exception:
                pass
            try:
                ctx["events"] = (c.run_events(run_id) or {}).get("events", [])
            except Exception:
                pass
    except Exception:
        pass
    ctx["approval"] = approval
    if isinstance(approval, dict):
        typer.echo(f"    operator : {approval.get('operator')}")
        if approval.get("risk"):
            typer.secho(f"    risk     : {approval.get('risk')}", fg="red")
        if approval.get("reason"):
            typer.echo(f"    reason   : {approval.get('reason')}")
        if approval.get("sources"):
            typer.echo(f"    sources  : {', '.join(approval.get('sources') or [])}")
        if approval.get("allowed_decisions"):
            typer.echo(f"    decisions: {', '.join(approval.get('allowed_decisions') or [])}")
    else:
        for r in (ctx.get("run") or {}).get("results") or []:
            typer.echo(f"    skill={r.get('skill_name')}  template={r.get('template_name')}")
    if dump:
        Path(dump).write_text(json.dumps(ctx, indent=2), encoding="utf-8")
        typer.secho(f"    pause context → {dump}", fg="blue")


def _watch_hitl(endpoint, token, run_id: str, *, approver, decision_opt,
                native, timeout, on_timeout, flow, dump) -> int:
    """Stream a run; at each HITL pause render context, resolve a decision, and
    resume — until terminal. Reconnects by ``after_seq`` like :func:`_tail`."""
    import httpx  # noqa: PLC0415 — lazy like _tail (heavy http client)

    last = 0
    while True:
        ev_type = data = seq = None
        paused = saw_terminal = False
        try:
            with _sse_client(endpoint, token, timeout=None) as c:
                with c.stream("GET", f"/runs/{run_id}/stream", params={"after_seq": last}) as r:
                    if r.status_code == 401:
                        _fail("401 Unauthorized — run `cairn remote login`.")
                    if r.status_code == 404:
                        _fail(f"run {run_id} not found")
                    r.raise_for_status()
                    for line in r.iter_lines():
                        if line == "":
                            if ev_type is not None:
                                if not flow or ev_type in _LIFECYCLE:
                                    _print_event(seq, ev_type, data)
                                if seq is not None:
                                    last = seq
                                if ev_type == "awaiting_approval":
                                    paused = True
                                    break
                                if ev_type in _TERMINAL:
                                    saw_terminal = True
                                    break
                            ev_type = data = seq = None
                            continue
                        if line.startswith("id:"):
                            try:
                                seq = int(line[3:].strip())
                            except ValueError:
                                seq = None
                        elif line.startswith("event:"):
                            ev_type = line[6:].strip()
                        elif line.startswith("data:"):
                            data = line[5:].strip()
        except typer.Exit:
            raise
        except httpx.HTTPError:
            pass  # dropped — fall through to a status check, then reconnect
        if saw_terminal:
            return 0
        if not paused:
            try:
                with _api(endpoint, token) as c:
                    st = c.run(run_id).get("status")
            except Exception:
                return 0
            if st in _TERMINAL:
                return 0
            if st != "awaiting_approval":
                continue  # transient close — reconnect from `last`
            paused, data = True, None  # missed the event but the run is parked

        _render_pause(endpoint, token, run_id, data, dump)
        decision = _decide(decision_opt=decision_opt, native=native,
                           timeout=timeout, on_timeout=on_timeout)
        if decision is None:
            typer.secho(
                f"detached — run {run_id} is still paused. Resume later:\n"
                f"    cairn remote approve {run_id}   (or: reject {run_id})",
                fg="yellow",
            )
            return 2
        if decision == "abort":
            with _api(endpoint, token) as c:
                _call(c.cancel_run, run_id)
            typer.secho(f"aborted — run {run_id} cancelled", fg="yellow")
            return 1
        with _api(endpoint, token) as c:
            out = _call(c.resume_run, run_id, decision=decision, approver=approver)
        still = (out or {}).get("still_paused")
        typer.secho(f"{decision}d — {'advanced to the next gate' if still else 'resuming'}",
                    fg="green")
        # loop: re-subscribe from `last` for post-resume events (next gate / terminal)


def _resume(run_id: str, decision: str, approver: str, endpoint, token, profile) -> None:
    """Deliver a HITL decision to a run paused at an operation gate (ADR-0034).

    Targets ``POST /runs/{id}/resume`` (an internal issue) — the run-level
    gate (status ``awaiting_approval``) the server routes to the deployment
    runtime. NOT the action-saga ``POST /triggers/approve`` callback, which needs
    a DurableActionHandler and 501s on a default deploy (issue #42).
    """
    ep, tok = _resolve(endpoint, token, profile)
    with _api(ep, tok) as c:
        out = _call(c.resume_run, run_id, decision=decision, approver=approver)
    typer.echo(json.dumps(out, indent=2))


@app.command()
def approve(
    run_id: str = typer.Argument(..., help="the paused run id (status awaiting_approval)"),
    watch: bool = typer.Option(False, "--watch", "-w", help="stream the run and prompt at each HITL pause"),
    decision: Optional[str] = typer.Option(None, "--decision", help="non-interactive decision for --watch (approve|reject)"),
    timeout: Optional[float] = typer.Option(None, "--timeout", help="seconds to wait at the prompt before --on-timeout"),
    on_timeout: str = typer.Option("detach", "--on-timeout", help="on prompt timeout: detach | reject | abort"),
    native: bool = typer.Option(False, "--native", help="use an OS confirm dialog (terminal fallback)"),
    flow: bool = typer.Option(False, "--flow", help="show only lifecycle/transition events"),
    dump: Optional[str] = typer.Option(None, "--dump", help="write the pause context to this file"),
    approver: str = typer.Option("cli-user", "--approver"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Approve a run paused at an operation-gate HITL interrupt (ADR-0034).

    With ``--watch``, stream the run and prompt (or auto-decide) at each pause —
    the interactive HITL loop (#53). Without it, post an immediate approval.
    """
    if on_timeout not in ("detach", "reject", "abort"):
        _fail("--on-timeout must be detach | reject | abort")
    if watch:
        ep, tok = _resolve(endpoint, token, profile)
        raise typer.Exit(_watch_hitl(
            ep, tok, run_id, approver=approver, decision_opt=decision, native=native,
            timeout=timeout, on_timeout=on_timeout, flow=flow, dump=dump,
        ))
    _resume(run_id, "approve", approver, endpoint, token, profile)


@app.command()
def reject(
    run_id: str = typer.Argument(..., help="the paused run id (status awaiting_approval)"),
    approver: str = typer.Option("cli-user", "--approver"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Reject a run paused at an operation-gate HITL interrupt — skips the gated op."""
    _resume(run_id, "reject", approver, endpoint, token, profile)


@app.command()
def console(
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Launch the interactive Textual console (Toad-style) over the server — a
    live runs view, a command bar with autocomplete, and clickable HITL
    approve/reject. Needs textual and textual-autocomplete, which cairndev does not
    install (there is no 'tui' extra): the error says how to add them."""
    ep, tok = _resolve(endpoint, token, profile)
    try:
        from cairn.cli.tui.app import run_console
    except ModuleNotFoundError as e:
        _fail(
            f"the terminal console needs textual and textual-autocomplete ({e.name} is "
            "missing), which cairndev does not install (there is no 'tui' extra). Add "
            "them to the environment cairn runs in:\n"
            f"  uv pip install --python {sys.executable} textual textual-autocomplete"
        )
    run_console(ep, tok)


@app.command(name="hash-password")
def hash_password_cmd(
    password: Optional[str] = typer.Option(None, "--password", "-p", help="prompted if omitted"),
):
    """Print an argon2 hash for CAIRN_API_PASSWORD_HASH (needs the `auth` extra)."""
    password = password or typer.prompt("Password", hide_input=True, confirmation_prompt=True)
    try:
        from cairn.core.triggers.auth import hash_password
        typer.echo(hash_password(password))
    except RuntimeError as e:
        _fail(str(e))


# ─── pipelines (#1195 slice 2 — the multi-leg surface, client-backed) ──────
# The ADR-0084 runnable surface driven remotely: launch a pack pipeline,
# watch its legs chain, resolve the HITL gate — the same loop the prod E2E
# runs, as three commands.


def _step_status(s: Any) -> str:
    """A step's status: ``GET /pipelines`` gives the bare string, a full record a dict."""
    return str(s.get("status", "") if isinstance(s, dict) else s or "")


def _pipeline_line(p: dict) -> str:
    # `GET /pipelines` rows carry `steps` ({step: status}) and `name`; a full
    # record (`GET /pipelines/{id}`) carries `step_state` and `pipeline_name`.
    steps = p.get("steps") or p.get("step_state") or {}
    done = sum(1 for s in steps.values() if _step_status(s) in ("done", "skipped"))
    name = p.get("name") or p.get("pipeline_name") or ""
    return (f"{p.get('pipeline_id', ''):22s} {p.get('status', ''):10s} "
            f"{done}/{len(steps)} steps  {name}")


@app.command()
def pipelines(
    status: Optional[str] = typer.Option(None, "--status", help="filter: running|paused|done|failed"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """List pipelines on the server."""
    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            out = _call(c.pipelines, status=status)
    rows = (out or {}).get("pipelines", [])
    for p in rows:
        typer.echo(_pipeline_line(p))
    if not rows:
        typer.echo("(no pipelines)")


@app.command()
def pipeline(
    pipeline_id: str = typer.Argument(...),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Show one pipeline's full durable record (plan + step_state)."""
    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            typer.echo(json.dumps(_call(c.pipeline, pipeline_id), indent=2))


def _print_steps(p: dict) -> None:
    for sid, st in (p.get("step_state") or {}).items():
        typer.echo(f"    {sid:18s} {st.get('status', ''):20s} run={st.get('run_id')}")


# ─── next-command discipline (#1792) ──────────────────────────────────────
# `cairn remote` had launch/advance/logs/findings the whole time, and the
# 2026-08-28 flagship sessions drove the control plane with raw curl and a bash
# poll loop — because nothing in the output ever pointed at them. A surface the
# operator has to already know is a surface that does not exist. So every state
# this CLI leaves the user in now names the verb that acts on it: a launch ends
# with the watch line, a pause prints its own advance line (per waiting step,
# with the approver flag filled in), a failed leg prints the logs line for the
# run that failed. Printed, never executed — the point is to teach the verb.


def _next(label: str, cmd: str) -> None:
    typer.secho(f"→ {label:9s} {cmd}", fg="cyan")


def _waiting_steps(p: dict) -> list[str]:
    """Step ids parked on a human — the bridge's own `awaiting_*` vocabulary
    (`awaiting_approval` for a gate or a leg whose gate fired, `awaiting_event`
    / `awaiting_schedule` for the trigger kinds)."""
    return [sid for sid, st in (p.get("step_state") or {}).items()
            if str(st.get("status", "")).startswith("awaiting")]


def _failed_steps(p: dict) -> list[tuple[str, str]]:
    """(step_id, run_id) for every failed leg — the run id IS the logs argument."""
    return [(sid, str(st.get("run_id") or ""))
            for sid, st in (p.get("step_state") or {}).items()
            if str(st.get("status", "")) == "failed"]


def _print_pipeline_next(pipeline_id: str, p: dict, *, approver: str = "") -> None:
    """The next command for whatever state this pipeline just reached."""
    status = str(p.get("status") or "")
    who = f" --approver {approver}" if approver else ""
    if status == "paused":
        for sid in _waiting_steps(p) or ["<step>"]:
            _next("approve", f"cairn remote advance {pipeline_id} {sid}{who}")
    for sid, run_id in _failed_steps(p):
        _next("logs", f"cairn remote logs {run_id}"
                      + (f"   # leg {sid}" if sid else ""))
    if status in ("failed", "done", "paused"):
        _next("findings", f"cairn remote findings {pipeline_id}")
    if status not in ("done", "failed", "cancelled"):
        # last line, deliberately: the one command that keeps you attached.
        _next("watch", f"cairn remote pipeline {pipeline_id}"
                       "    # or re-run launch with --watch to follow it live")


# ─── launch presets (#1789, ADR-0071 §6) ──────────────────────────────────
# A named input set, saved server-side and expanded server-side. The CLI only
# ever passes the NAME: resolving it here would be a second implementation of
# the rule the launch door already owns, and a preset that means one thing from
# a terminal and another from the console is worse than no preset at all.


@app.command(name="presets")
def presets_cmd(
    ref: str = typer.Argument(..., help="installed pipeline ref, e.g. training/retrain-cycle"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """List the launch presets for a pipeline (saved + pack-shipped)."""
    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            out = _call(c.pipeline_presets, ref)
    rows = (out or {}).get("presets", [])
    for p in rows:
        used = p.get("last_used_at") or "never"
        typer.echo(f"{p.get('name', ''):24s} {p.get('input_count', 0):3d} inputs  "
                   f"{p.get('source', ''):6s} last used {used}")
        if p.get("description"):
            typer.echo(f"  {p['description']}")
    if not rows:
        typer.echo(f"(no presets for {(out or {}).get('ref', ref)})")


@app.command(name="delete-preset")
def delete_preset_cmd(
    ref: str = typer.Argument(...),
    name: str = typer.Argument(...),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Delete a SAVED preset (a pack-shipped one of the same name reappears)."""
    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            out = _call(c.delete_pipeline_preset, ref, name)
    typer.secho(f"deleted={(out or {}).get('deleted')}",
                fg="green" if (out or {}).get("deleted") else "yellow")


@app.command()
def launch(
    ref: str = typer.Argument(..., help="installed pipeline ref, e.g. training/e2e-finetune"),
    input_: list[str] = typer.Option([], "--input", "-i", help="pipeline input key=value (repeatable)"),
    preset: Optional[str] = typer.Option(
        None, "--preset", "-p",
        help="launch from a saved/pack-shipped input set; --input overrides it per key"),
    save_preset: Optional[str] = typer.Option(
        None, "--save-preset",
        help="after a successful launch, save this launch's inputs under this name"),
    environment: Optional[str] = typer.Option(None, "--environment"),
    watch: bool = typer.Option(False, "--watch", "-w", help="poll step states until paused/terminal"),
    interval: float = typer.Option(3.0, "--interval", help="poll interval for --watch (seconds)"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Launch an installed pipeline (each leg becomes its own governed run)."""
    with _session(endpoint, token, profile) as (ep, tok, local):
        if local and not watch:
            # the engine dies with this command — returning early would kill
            # the in-flight legs, so an ephemeral launch always watches
            typer.secho("ephemeral engine — watching until the pipeline parks "
                        "(gate or terminal).", fg="yellow", err=True)
            watch = True
        # generous timeout: the inprocess engine can stall the event loop with
        # a leg's CPU-heavy cold start, and the launch POST is NOT retryable
        # (it lands even when the response can't flush — retry = double-launch)
        with _api(ep, tok, timeout=300.0) as c:
            overrides = _parse_kv(input_)
            out = _call(c.launch_pipeline, ref, overrides,
                        environment=environment, preset=preset)
            pid = (out or {}).get("pipeline_id")
            typer.secho(f"pipeline_id={pid} status={(out or {}).get('status')}", fg="green")
            if save_preset:
                # #1789 criterion 3 — save from a launch that WORKED, so a preset
                # can only ever be a set of inputs the door already accepted.
                # Saved AFTER the launch for exactly that reason. The saved set is
                # the merge the server used: the chosen preset's inputs with these
                # --input keys on top (read back from the server rather than
                # rebuilt here, so the CLI is not re-deriving the merge rule).
                inputs = dict(overrides)
                if preset:
                    listed = _call(c.pipeline_presets, ref) or {}
                    base = next((p for p in listed.get("presets", [])
                                 if p.get("name") == preset), None)
                    if base:
                        inputs = {**(base.get("inputs") or {}), **overrides}
                saved = _call(c.save_pipeline_preset, ref, save_preset, inputs)
                typer.secho(f"saved preset {save_preset!r} "
                            f"({(saved or {}).get('input_count', 0)} inputs)", fg="green")
            if not (watch and pid):
                # #1792 — a launch that returns immediately must still say how
                # to follow it; this is the line whose absence sent the flagship
                # sessions to curl. The launch response carries no step_state
                # (nothing has run), so these are the two reads that work at
                # any state rather than a per-step advance line.
                _next("findings", f"cairn remote findings {pid}")
                _next("watch", f"cairn remote pipeline {pid}"
                               "    # or re-run launch with --watch to follow it live")
                return
            last_render = ""
            while True:
                p = _poll_pipeline(c, pid)
                render = json.dumps(p.get("step_state"), sort_keys=True)
                if render != last_render:
                    typer.echo(f"  {p.get('status')}")
                    _print_steps(p)
                    last_render = render
                if p.get("status") in ("done", "failed", "cancelled"):
                    # #1792 — a failed leg names the run whose logs explain it.
                    _print_pipeline_next(pid, p)
                    raise typer.Exit(0 if p.get("status") == "done" else 1)
                if p.get("status") == "paused":
                    # #1792 — the partial line finished: EVERY waiting step, and
                    # the --approver flag that lands the decision in the audit
                    # trail (a gate released by "who?" is a gate half-recorded).
                    typer.secho(f"⏸ paused — {len(_waiting_steps(p)) or 1} step(s) "
                                f"waiting on a human", fg="yellow")
                    _print_pipeline_next(pid, p, approver=os.environ.get("USER", "cli-user"))
                    raise typer.Exit(2)
                time.sleep(max(0.5, interval))


@app.command()
def advance(
    pipeline_id: str = typer.Argument(...),
    step_id: str = typer.Argument(..., help="the step waiting on a human (gate / manual)"),
    approver: Optional[str] = typer.Option(
        None, "--approver",
        help="who decided a gate — recorded when auth is off; under auth the server "
             "records the signed-in identity"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Approve/advance a pipeline step that is waiting on a human.

    A gate (``awaiting_approval``) is decided through ``POST …/approve``, which
    records the approver; any other waiting step (``awaiting_advance``) is
    released through ``POST …/advance``."""
    with _session(endpoint, token, profile) as (ep, tok, local):
        with _api(ep, tok, timeout=300.0) as c:  # resume can stall the loop too
            # #2388 — `/advance` records no approver and refuses a gate; a gate goes
            # to `/approve` so the audit trail and the review name who decided.
            st = ((_call(c.pipeline, pipeline_id) or {}).get("step_state") or {}).get(step_id) or {}
            if st.get("status") == "awaiting_approval":
                out = _call(c.approve_step, pipeline_id, step_id, approver=approver)
                typer.secho(f"approved — pipeline status={(out or {}).get('status')}", fg="green")
            else:
                out = _call(c.advance_step, pipeline_id, step_id, approver=approver)
                typer.secho(f"advanced — pipeline status={(out or {}).get('status')}", fg="green")
            if not local:
                # #1792 — the released pipeline runs on the server now; say how
                # to keep watching it, and at the NEXT gate this same line comes
                # back naming that step.
                _print_pipeline_next(
                    pipeline_id, _call(c.pipeline, pipeline_id) or {},
                    approver=approver or os.environ.get("USER", "cli-user"))
            else:
                # the resumed legs run in THIS ephemeral engine — hold it open
                # until the pipeline parks again (next gate or terminal)
                typer.secho("ephemeral engine — watching until the pipeline parks.",
                            fg="yellow", err=True)
                time.sleep(2.0)  # let the bridge pick the approval up
                while True:
                    p = _poll_pipeline(c, pipeline_id)
                    if p.get("status") in ("paused", "done", "failed", "cancelled"):
                        typer.echo(f"  {p.get('status')}")
                        _print_steps(p)
                        # #1792 — the next gate names itself, and a failed leg
                        # names the run whose logs explain it.
                        _print_pipeline_next(
                            pipeline_id, p,
                            approver=approver or os.environ.get("USER", "cli-user"))
                        break
                    time.sleep(2.0)


@app.command(name="cancel-pipeline")
def cancel_pipeline_cmd(
    pipeline_id: str = typer.Argument(...),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Cancel a whole pipeline (best-effort across its in-flight legs)."""
    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            out = _call(c.cancel_pipeline, pipeline_id)
    typer.secho(f"status={(out or {}).get('status')}", fg="yellow")


# ─── findings (#1790) ─────────────────────────────────────────────────
# The read that used to require `docker exec` + an in-container python call
# into `reconstruct_step_output`. One verb over the two doors: a run's own
# findings, or every non-pending step of a pipeline.


def _scalar(v) -> bool:
    """Flat enough for a table cell? Containers get a shape summary instead —
    a truncated JSON blob in a 2-column table is worse than saying `{3 keys}`
    and pointing at --json."""
    return v is None or isinstance(v, (str, int, float, bool))


def _fmt(v) -> str:
    if isinstance(v, dict):
        return f"{{{len(v)} keys}}"
    if isinstance(v, list):
        return f"[{len(v)} items]"
    if isinstance(v, float):
        return f"{v:g}"
    return "-" if v is None else str(v)


def _findings_table(found: dict, indent: str = "") -> None:
    """Flat scalars as an aligned key/value table; containers as a shape."""
    if not found:
        typer.secho(f"{indent}(no findings)", fg="yellow")
        return
    width = max(len(str(k)) for k in found)
    for k in sorted(found):
        v = found[k]
        typer.echo(f"{indent}{str(k):<{width}}  {_fmt(v)}"
                   + ("" if _scalar(v) else "   (--json for the full value)"))


@app.command()
def findings(
    target_id: str = typer.Argument(..., help="a run id, or a pipeline id (pl_…)"),
    json_out: bool = typer.Option(False, "--json", help="print the raw JSON response"),
    endpoint: Optional[str] = _EP,
    token: Optional[str] = _TOK,
    profile: Optional[str] = _PROF,
):
    """Show a run's findings, or every step's findings for a pipeline.

    The id kind is auto-detected: `pl_…` is a pipeline (the store's id prefix),
    anything else is a run — and either way a 404 falls back to the other door,
    so a hand-typed id never needs a flag.
    """
    from cairn_client import CairnAPIError  # noqa: PLC0415 — lazy with the client

    with _session(endpoint, token, profile) as (ep, tok, _local):
        with _api(ep, tok) as c:
            # (kind, door) pairs, best guess first — the kind is carried
            # alongside rather than recovered from the callable, because a bound
            # method is a fresh object per attribute access (`door is
            # c.run_findings` is False even for the door we just called).
            doors = [("pipeline", c.pipeline_findings), ("run", c.run_findings)]
            if not target_id.startswith("pl_"):  # the pipeline store's id prefix
                doors.reverse()
            out, kind, last = None, "", None
            for door_kind, door in doors:
                try:
                    out = door(target_id)
                    kind = door_kind
                    break
                except CairnAPIError as e:
                    if e.status_code == 401:
                        _fail("401 Unauthorized — run `cairn remote login` "
                              "(or set CAIRN_REMOTE_TOKEN).")
                    if e.status_code != 404:
                        _fail(f"{e.status_code}: {e.detail[:300]}")
                    last = e
            if out is None:
                _fail(f"no run or pipeline {target_id!r} "
                      f"({last.status_code if last else 404})")

    if json_out:
        typer.echo(json.dumps(out, indent=2))
        return
    if kind == "run":
        typer.secho(f"run {out.get('run_id', target_id)}  "
                    f"status={out.get('status', '')}", fg="green")
        _findings_table(out.get("findings") or {}, indent="  ")
        return
    typer.secho(f"pipeline {out.get('pipeline_id', target_id)}  "
                f"status={out.get('status', '')}", fg="green")
    steps = out.get("steps") or {}
    if not steps:
        typer.secho("  (no step has run yet)", fg="yellow")
    for sid, st in steps.items():
        typer.secho(f"  {sid}  [{st.get('status', '')}] run={st.get('run_id') or '-'}",
                    fg="cyan")
        _findings_table(st.get("findings") or {}, indent="    ")
