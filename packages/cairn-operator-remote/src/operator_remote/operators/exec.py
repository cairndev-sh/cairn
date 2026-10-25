"""remote/exec — run a command (and ship files in/out) on any SSH box you own.

A generic raw-compute primitive (#70 Axis-2): a ``RemoteJobOperator`` over
asyncssh. ``submit`` launches the command **detached** on the remote host and
records its pid; ``poll`` reads the captured exit code, corroborated against
pid liveness for the not-yet-written case (#337), and pulls outputs + any
``metrics.json``; ``cancel`` kills it. Lets you run a workflow locally but
dispatch CPU/GPU work to a box you have SSH access to — the foundation for the
NATS remote-worker channel (Slice 2, which reuses the JetStream substrate).

Connection: a ``compute.ssh`` (``cairn connect ssh``) resolved into ``ctx.compute``
— key-based auth (Ed25519 recommended) with the private key from the SecretStore,
host-key verified via ``known_hosts`` (NEVER disabled). asyncssh is the optional
``[ssh]`` extra. The SSH calls live behind ``_connect`` so tests monkeypatch them.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shlex
import tempfile
import time
import uuid
from types import SimpleNamespace

from cairn.core.operator import as_ssh_params, current_workspace
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator
from cairn.core.operator.jobs import JobHandle, JobStatus

from operator_remote.schemas.exec import RemoteExecInput, RemoteExecOutput

_JOBROOT = ".cairn-jobs"

logger = logging.getLogger("operator_remote.exec")

#: #2606 — the detached launch writes its pid file asynchronously, AFTER the launch
#: command has returned, so a poll in the first moments can find neither ``pid`` nor
#: ``exit_code``. Within this window after launch that means "starting", not "died";
#: past it, a job with no pid never started (a launch the box could not detach).
_START_GRACE_S = 30.0

#: The job runs as `sh runner.sh` in its OWN session (so it survives the SSH channel
#: closing) and as its own process-group leader (so cancel's ``kill -<pid>`` reaches
#: the whole job). ``setsid`` (util-linux) does both on Linux; a box without it (macOS)
#: gets the same through ``perl``'s ``POSIX::setsid`` (#2606). `echo $$` before the
#: ``exec`` keeps the pid file naming the job.
_JOB = "sh -c 'echo $$ > pid; exec sh runner.sh'"
_DETACH = "< /dev/null > /dev/null 2>&1 &"
_LAUNCH = (
    "if command -v setsid >/dev/null 2>&1; "
    f"then setsid {_JOB} {_DETACH} "
    f"else perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' {_JOB} {_DETACH} fi"
)


def _require_ssh():
    # optional dependency: the [ssh] extra (asyncssh). Lazy so importing the pack
    # doesn't require it.
    try:
        import asyncssh  # noqa: F401, PLC0415
    except ImportError as e:  # pragma: no cover - exercised by the no-dep test
        raise RuntimeError(
            "remote/exec needs asyncssh: pip install 'cairndev[ssh]'"
        ) from e


def _conn_params(ctx: OperationContext) -> dict:
    """host/user/port/known_hosts + private key from the resolved compute.ssh
    connection — the SHARED core helper (#1544), not a local copy."""
    return as_ssh_params(ctx, operator="remote/exec")


# #337: a half-open TCP connection (e.g. the peer vanished mid-session, or an
# sshd per-source penalty is silently dropping packets) can otherwise hang an
# asyncssh.connect() indefinitely. connect_timeout/login_timeout bound the
# handshake+auth; the keepalive makes a REUSED connection (see _get_conn below)
# notice a dead peer and raise instead of hanging the next .run() forever.
_CONNECT_TIMEOUT = 15
_LOGIN_TIMEOUT = 15
_KEEPALIVE_INTERVAL = 15
_KEEPALIVE_COUNT_MAX = 3


async def _connect(params: dict):
    """Open an asyncssh connection. Seam: tests monkeypatch this."""
    _require_ssh()
    import asyncssh  # noqa: PLC0415 - optional dep (see _require_ssh)

    client_keys = [asyncssh.import_private_key(params["private_key"])] if params.get("private_key") else None
    return await asyncssh.connect(
        params["host"], port=params["port"], username=params["user"],
        client_keys=client_keys, known_hosts=params["known_hosts"],
        connect_timeout=_CONNECT_TIMEOUT, login_timeout=_LOGIN_TIMEOUT,
        keepalive_interval=_KEEPALIVE_INTERVAL, keepalive_count_max=_KEEPALIVE_COUNT_MAX,
    )


# #337: one live connection per (host, user, port), reused across submit/poll/
# cancel. Opening a FRESH connection per call is what trips sshd's per-source
# penalties (rapid successive auths → intermittent hangs). Reuse checks
# ``is_closed`` — a dropped connection reconnects transparently; test fakes
# (which lack ``is_closed``) are never cached, keeping hermetic tests isolated.
_conn_cache: dict[tuple, object] = {}


async def _get_conn(params: dict):
    key = (params.get("host"), params.get("user"), params.get("port"))
    cached = _conn_cache.get(key)
    if cached is not None and not getattr(cached, "is_closed", True):
        return cached
    conn = await _connect(params)
    _conn_cache[key] = conn
    return conn


def _reset_conn_cache() -> None:
    """Test/shutdown hook — drop cached connections (they are NOT closed here;
    asyncssh closes on GC/process exit)."""
    _conn_cache.clear()


def _create_owner_only() -> tuple:
    """``(pflags, attrs)`` for an SFTP open that CREATES a new file with mode 0600
    and fails if the file already exists (``FXF_EXCL``): an existing file is never
    truncated and left with its old mode. asyncssh is loaded: a connection exists."""
    import asyncssh  # noqa: PLC0415 - optional dep (see _require_ssh)

    return (asyncssh.FXF_WRITE | asyncssh.FXF_CREAT | asyncssh.FXF_EXCL,
            asyncssh.SFTPAttrs(permissions=0o600))


def _runner(command: str, env: dict) -> str:
    # capture the exit code + stream stdout/stderr to a log. The command is the
    # workflow author's (trusted) and is written to a file — not interpolated.
    exports = "".join(f"export {k}={shlex.quote(str(v))}\n" for k, v in (env or {}).items())
    return f"#!/bin/sh\n{exports}{{ {command} ; echo $? > exit_code ; }} > stdout.log 2>&1\n"


def _default_workdir(ctx: OperationContext) -> str:
    """#551: a STABLE per-node workdir (run_id + node) so a re-dispatch of the same
    node lands in the same directory and can be de-duplicated. Falls back to a
    random dir only when there's no run/node identity to key on."""
    run = str(getattr(ctx, "pipeline_id", "") or "").strip()
    node = str(getattr(ctx, "node_name", "") or "").strip()
    if run:
        key = re.sub(r"[^A-Za-z0-9_.-]+", "-", f"{run}-{node or 'node'}").strip("-")
        if key:
            return f"{_JOBROOT}/{key}"
    return f"{_JOBROOT}/{uuid.uuid4().hex[:12]}"


async def _live_job(conn, q: str) -> bool:
    """True if a prior submit to this workdir is still RUNNING (a live pid and no
    exit_code yet) — mirrors _poll's liveness check."""
    if (await conn.run(f"cat {q}/exit_code 2>/dev/null", check=False)).stdout.strip():
        return False  # already terminal
    pid = (await conn.run(f"cat {q}/pid 2>/dev/null", check=False)).stdout.strip()
    return bool(pid) and (
        await conn.run(f"kill -0 {shlex.quote(pid)} 2>/dev/null && echo up", check=False)
    ).stdout.strip() == "up"


async def _submit(inp: RemoteExecInput, ctx: OperationContext) -> JobHandle:
    p = _conn_params(ctx)
    conn = await _get_conn(p)  # #337: reused across submit/poll/cancel
    workdir = inp.workdir or _default_workdir(ctx)
    q = shlex.quote(workdir)
    await conn.run(f"mkdir -p {q}", check=True)
    handle = JobHandle(
        job_id=f"{p['host']}:{workdir}", backend="ssh",
        meta={"host": p["host"], "user": p["user"], "port": p["port"], "workdir": workdir,
              "known_hosts": p["known_hosts"]},  # #601: cancel needs it (host-key verify)
    )
    # Cancel runs later with no ctx, possibly outside any workspace scope: the
    # connection's NAME and the ACCOUNT it resolved in ride in the handle, so it
    # reconnects with the same credential this submit used (ADR-0149 D4). Names
    # only — never a value. The workspace is recorded for the reader.
    connection = str(getattr(ctx.compute, "name", "") or "").strip()
    if connection:
        handle.meta["connection"] = connection
        account = str(getattr(ctx.compute, "account_id", "") or "").strip()
        if account:
            handle.meta["account_id"] = account
        handle.meta["workspace_id"] = current_workspace()
    # #551: idempotent submit — if a prior dispatch to this (stable) workdir is
    # still running, ADOPT it instead of clearing state + launching a second
    # process. Closes the crash-between-submit-and-register double-spend window and
    # makes a false-positive relaunch a no-op (no two trainers in one dir).
    if await _live_job(conn, q):
        return handle
    # #708: we are NOT adopting (no LIVE job above), yet a pid file may still name a
    # prior process that died EXTERNALLY (box killed / OOM) and isn't fully reaped.
    # A relaunch that overlaps a not-yet-dead ~GB trainer stacks two on the box and
    # OOMs it (observed live 2026-07-11). Kill that stale process GROUP before
    # launching so a relaunch can never stack a second trainer. No-op when the pid
    # is already gone; the kill precedes the state-file rm below (which needs the pid).
    await conn.run(
        f'p=$(cat {q}/pid 2>/dev/null); [ -n "$p" ] && kill -KILL -"$p" 2>/dev/null; true',
        check=False,
    )
    # #337: a caller MAY pass an explicit workdir (schema allows it) and reuse it
    # across submissions (e.g. a retry, or a human re-running a live e2e probe).
    # _poll trusts exit_code the instant it exists, so a leftover exit_code/pid/
    # stdout.log/metrics.json from a PRIOR run in that directory reports a brand
    # new job "succeeded" (or failed) before it has even started. Clear all
    # per-run state files up front so poll can never observe stale state.
    await conn.run(
        f"rm -f {q}/exit_code {q}/pid {q}/stdout.log {q}/metrics.json {q}/runner.sh", check=False,
    )
    # #552: expose the run_id on the box so per-run artifacts (e.g. S3 checkpoint
    # keys, used by cairn-train-local) are stable across relaunches and never
    # collide across runs. Never override an explicit binding value.
    env = dict(inp.env or {})
    if getattr(ctx, "pipeline_id", None):
        env.setdefault("CAIRN_RUN_ID", ctx.pipeline_id)
    async with conn.start_sftp_client() as sftp:
        for local in inp.upload:
            await sftp.put(local, f"{workdir}/")
        # runner.sh carries the job's env (the spec, the job-channel token, forwarded
        # settings): it is CREATED, exclusively, owner-only (0600), so another account
        # on the box never reads it (ADR-0149 D2). The copy a previous submit left was
        # removed above; if it is still there, refuse rather than reuse its mode.
        pflags, attrs = _create_owner_only()
        try:
            f = await sftp.open(f"{workdir}/runner.sh", pflags, attrs)
        except Exception as e:  # noqa: BLE001 — re-raised with the reason
            raise RuntimeError(
                f"remote/exec: could not create a fresh runner.sh in {workdir} "
                f"({type(e).__name__}); an earlier copy may not have been removed — not launching"
            ) from e
        async with f:
            await f.write(_runner(inp.command, env))
    # launch detached; record the pid for poll/cancel.
    # #601: the launch MUST be backgrounded INSIDE a group, not with a trailing
    # `&` on the whole `cd … && …` and-list. `A && B &` backgrounds the ENTIRE
    # and-list as one subshell, and that subshell inherits the SSH channel's
    # fds and WAITS for B (the trainer) — so it holds the channel open until the
    # trainer EXITS. asyncssh's conn.run() returns only on channel close, so it
    # blocked for the whole training duration (the `>/dev/null` redirect binds
    # to B, not to the waiting subshell). Verified live: `< /dev/null` and even
    # `setsid` on B did NOT help — the culprit was the and-list backgrounding.
    # Under Temporal that made the submit activity outlive its start-to-close
    # timeout on any non-trivial job → CancelledError → retry; and since
    # store.register() runs only after submit returns (it never did), the retry's
    # #551 _live_job adopt saw no record → it RE-SUBMITTED a duplicate.
    # `cd {q} && { … & }` backgrounds the trainer WITHIN the group, so the launch
    # command returns immediately → conn.run() returns fast → record registered →
    # interrupt fires → control loop polls. `setsid` + all-std-streams-to-/dev/null
    # keep the trainer off the channel and in its own session so it survives the
    # close and a retry can adopt it; the pid is captured from inside that session
    # (`echo $$` before `exec sh runner.sh`) so the pid file names the trainer.
    # #2606: `_LAUNCH` keeps that shape and falls back to perl's setsid on a box
    # without `setsid`; each branch backgrounds its own simple command inside the group.
    await conn.run(f"cd {q} && {{ {_LAUNCH}; }}", check=True)
    # #2606: the pid file lands after this returns; poll reads `launched_at` to tell
    # "starting" (no pid yet, inside _START_GRACE_S) from "never started".
    handle.meta["launched_at"] = time.time()
    return handle


async def _read_metrics(conn, q: str) -> dict:
    """Read the job's ``metrics.json`` from the workdir (empty if absent/unparseable).
    A job that writes it incrementally (e.g. cairn-train-local's progress callback)
    surfaces live progress + the learning-gate ``eval_losses`` WHILE still running,
    not just at completion."""
    m = (await conn.run(f"cat {q}/metrics.json 2>/dev/null", check=False)).stdout.strip()
    if m:
        try:
            return json.loads(m)
        except Exception:  # noqa: BLE001 — a half-written file just means "not yet"
            pass
    return {}


async def _read_log_tail(conn, q: str, n: int = 2000) -> str:
    """Last ~n bytes of the job's stdout.log — recent output surfaced on every poll
    so the control plane / console isn't blind while a remote job runs (#553)."""
    return (await conn.run(f"tail -c {n} {q}/stdout.log 2>/dev/null", check=False)).stdout


async def _poll(handle: JobHandle, inp: RemoteExecInput, ctx: OperationContext) -> JobStatus:
    # #337: liveness, not just exit_code presence, decides "not done yet" vs
    # terminal. code_out empty covers BOTH "job hasn't finished" and "exit_code
    # file exists but the write (echo $? > exit_code) hasn't landed yet" — a
    # bare `cat` on a not-yet-flushed file reads as empty either way, so the
    # empty-string branch below is the single guard for both. In that branch we
    # don't assume "still running": we corroborate against the recorded pid via
    # `kill -0` so a process that died WITHOUT ever writing exit_code (killed,
    # crashed before the trap) reports failed instead of hanging as "running"
    # forever. _submit clears any stale exit_code/pid from a prior run in this
    # workdir, so a non-empty code_out here is trustworthy — never leftover state.
    workdir = handle.meta.get("workdir")
    q = shlex.quote(workdir)
    conn = await _get_conn(_conn_params(ctx))  # #337: reused connection
    code_out = (await conn.run(f"cat {q}/exit_code 2>/dev/null", check=False)).stdout.strip()
    if not code_out:
        pid = (await conn.run(f"cat {q}/pid 2>/dev/null", check=False)).stdout.strip()
        launched_at = handle.meta.get("launched_at")
        if not pid and launched_at is not None and time.time() - float(launched_at) < _START_GRACE_S:
            # #2606: the detached launch has not written its pid file yet — starting.
            return JobStatus(state="running")
        alive = bool(pid) and (
            await conn.run(f"kill -0 {shlex.quote(pid)} 2>/dev/null && echo up", check=False)
        ).stdout.strip() == "up"
        if alive:
            # #553: surface live progress + eval_losses + a recent stdout tail while
            # the job runs, so the console isn't blind and the learning-gate
            # early-abort can fire.
            return JobStatus(
                state="running", metrics=await _read_metrics(conn, q),
                log_tail=await _read_log_tail(conn, q),
            )
        # RACE GUARD: the process may have written exit_code AND exited in the gap
        # between our exit_code read and the kill -0 (two separate SSH round-trips;
        # a CPU-saturated box widens the window). Re-read exit_code ONCE before
        # declaring an abnormal death — a job that finished cleanly in that window
        # has a valid exit_code now. Caught live on a ~85s 10k dataset-fix job whose
        # box was pegged by Presidio: exit_code=0 on disk, yet the run failed
        # "exited without an exit code". If it's STILL empty, the death is real.
        code_out = (await conn.run(f"cat {q}/exit_code 2>/dev/null", check=False)).stdout.strip()
        if not code_out:
            if not pid and launched_at is not None:
                # #2606: no pid file long after launch — the job never started (the box
                # could not detach it). Not a recoverable death: a relaunch would fail
                # the same way.
                return JobStatus(
                    state="failed", retriable=False,
                    error=(f"remote job never started: no pid file within "
                           f"{int(_START_GRACE_S)}s of launch — the box needs `setsid` "
                           f"(util-linux) or `perl` to detach the job"),
                )
            # #552: pid gone with NO exit code = the process died without running its
            # trap (killed / OOM / box reclaimed) — a RECOVERABLE death, so the control
            # loop may relaunch it from a durable checkpoint. A clean non-zero exit
            # (below) is a genuine error and is NOT retriable.
            return JobStatus(
                state="failed", retriable=True,
                error="remote process exited without an exit code",
                metrics=await _read_metrics(conn, q),
            )
        # else: the re-read found it — fall through to terminal exit_code handling.
    code = int(code_out.split()[0])
    if code != 0:
        tail = (await conn.run(f"tail -c 1500 {q}/stdout.log 2>/dev/null", check=False)).stdout
        return JobStatus(state="failed", error=f"remote command exit {code}: ...{tail[-500:]}")
    # success → fetch outputs + any metrics.json
    fetch_dir = os.path.join(tempfile.gettempdir(), f"cairn-remote-{workdir.rsplit('/', 1)[-1]}")
    os.makedirs(fetch_dir, exist_ok=True)
    fetched: list[str] = []
    if inp.fetch:
        async with conn.start_sftp_client() as sftp:
            for pat in inp.fetch:
                try:
                    await sftp.get(f"{workdir}/{pat}", fetch_dir, recurse=True)
                    fetched.append(os.path.join(fetch_dir, pat))
                except Exception:  # noqa: BLE001 - best-effort per pattern
                    pass
    metrics = await _read_metrics(conn, q)
    result = RemoteExecOutput(
        status="succeeded", exit_code=0, host=handle.meta.get("host"),
        workdir=workdir, fetched=fetched, metrics=metrics,
    )
    return JobStatus(state="succeeded", output=result.model_dump(), metrics=metrics)


async def _job_ssh_params(handle: JobHandle, connection: str = "") -> dict:
    """Reconnect params for a job submitted earlier, outside any ctx (cancel, the
    checkpoint relay): WHERE the job is comes from the handle; the KEY is the
    ``PRIVATE_KEY`` of the compute.ssh connection that submitted it — the one
    credential submit used, resolved through the same helper (#1544).

    The connection the HANDLE names wins; ``connection`` (the engine's target)
    is used only for a handle saved before names were recorded, and a handle
    that names none and has no target keeps the old ambient ``SSH_PRIVATE_KEY``.
    The account recorded at submit scopes the lookup, so it needs no workspace
    in scope (a learning-gate abort, a run cancel); without one, the lookup is
    the caller's scope. A named connection that does not resolve raises:
    guessing another credential is not a fallback."""
    name = (str(handle.meta.get("connection") or "") or connection).strip()
    if name:
        from cairn.core.operator import resolve_connection  # noqa: PLC0415 - lazy: core is absent on a data-plane box

        account = str(handle.meta.get("account_id") or "").strip()
        compute = await (resolve_connection(name, account_id=account) if account
                         else resolve_connection(name))
        if compute is None:
            raise LookupError(f"compute.ssh connection {name!r} is unknown or disabled")
        key = as_ssh_params(SimpleNamespace(compute=compute), operator="remote/exec")["private_key"]
    else:
        key = os.environ.get("SSH_PRIVATE_KEY")
    return {
        "host": handle.meta.get("host"), "user": handle.meta.get("user"),
        "port": int(handle.meta.get("port") or 22),
        # #601: use the connection's known_hosts (carried in the handle at submit).
        # `None` made asyncssh REJECT the host, so a cancel from a process that
        # never SSH'd here (e.g. the server, when the job ran on the remote worker)
        # silently failed and orphaned the remote process.
        "known_hosts": handle.meta.get("known_hosts"),
        "private_key": key,
    }


def _unreachable(what: str, handle: JobHandle, connection: str, e: Exception) -> None:
    """Say that a reconnect failed — where, through which connection, and the
    error's TYPE (never its text: an SSH or key-import error may quote input)."""
    name = (str(handle.meta.get("connection") or "") or connection).strip() or "SSH_PRIVATE_KEY"
    detail = str(e) if isinstance(e, LookupError) else type(e).__name__
    logger.warning("remote/exec: %s for %s:%s failed via %s (%s)", what,
                   handle.meta.get("host"), handle.meta.get("workdir"), name, detail)


async def _cancel(handle: JobHandle, *, connection: str = "") -> bool:
    """Kill the job's process group. Best-effort (a cancel failure must never crash
    the loop — #127) but never silent: ``False`` plus a warning when the box could
    not be reached, since the job may still be running and billing."""
    workdir = handle.meta.get("workdir")
    if not workdir:
        return True  # nothing was launched, so nothing is left running
    try:
        conn = await _get_conn(await _job_ssh_params(handle, connection))  # #337: reused connection
        # #603: kill the whole PROCESS GROUP, not just the pid file's pid. The pid
        # file names runner.sh (the wrapper), which `setsid` makes a group leader;
        # the real trainer (cairn-train-local) is its CHILD in that group. Plain
        # `kill <pid>` reaped only the shell and ORPHANED the trainer (cost leak —
        # the run showed cancelled but the GPU/CPU job kept running). `kill -<pid>`
        # signals the group (shell + trainer); SIGKILL after a grace beat in case
        # the trainer ignores SIGTERM.
        q2 = shlex.quote(workdir)
        await conn.run(
            f'p=$(cat {q2}/pid 2>/dev/null); [ -n "$p" ] && '
            f'{{ kill -TERM -"$p" 2>/dev/null; sleep 1; kill -KILL -"$p" 2>/dev/null; }}',
            check=False,
        )
    except Exception as e:  # noqa: BLE001
        _unreachable("cancel (the remote process may still be running)", handle, connection, e)
        return False
    return True


@register_operator(name="remote/exec", version="0.1.0", capabilities=["remote.exec"],
                   implements=["contract://remote.exec/1.0.0"])  # #2243 V2 — cairn-contracts-remote
class RemoteExecOp(Operation[RemoteExecInput, RemoteExecOutput]):
    input_schema = RemoteExecInput
    output_schema = RemoteExecOutput
    # #1539 — governance declarations; the [operator.*] table is generated from these
    long_running = True
    risk = 'external_write'
    executors = ['remote_job', 'in_process']
    supports_mock = True
    supports_cancel = True
    # ADR-0103 (#2035) — a remote_job command has no structured metric
    # stream (stdout is text, not a series), so there is nothing this
    # class can declare beyond the submit/poll state the runtime already
    # surfaces.
    reports = ReportSpec.NONE

    async def submit(self, inp: RemoteExecInput, ctx: OperationContext) -> JobHandle:
        return await _submit(inp, ctx)

    async def poll(self, handle: JobHandle, inp: RemoteExecInput, ctx: OperationContext) -> JobStatus:
        return await _poll(handle, inp, ctx)

    async def cancel(self, handle: JobHandle) -> None:
        if not await _cancel(handle):
            # reported to the caller (the control worker logs it) instead of "sent"
            raise RuntimeError(
                f"remote/exec could not cancel {handle.job_id}; the remote process may still be running")

    # in-process fallback (when the remote_job executor isn't selected): submit + poll once.
    async def run(self, inp: RemoteExecInput, ctx: OperationContext) -> RemoteExecOutput:
        handle = await _submit(inp, ctx)
        st = await _poll(handle, inp, ctx)
        return RemoteExecOutput(
            status=st.state, host=handle.meta.get("host"), workdir=handle.meta.get("workdir"),
            metrics=st.metrics, error=st.error,
        )

    def mock(self, inp: RemoteExecInput, ctx: OperationContext) -> RemoteExecOutput:
        return RemoteExecOutput(status="succeeded", host="mock", workdir="/tmp/mock", metrics={"mock": True})
