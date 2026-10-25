"""Hermetic tests for remote/exec — the SSH calls are behind `_connect`, which we
monkeypatch with a fake connection, so no real SSH/asyncssh is needed."""
from __future__ import annotations

import importlib.util as u
import shlex
import time

import pytest

from cairn.core.operator import OperationContext, default_registry, load_all_operators
from cairn.core.operator.jobs import JobHandle, is_remote_job_operator
from operator_remote.operators import exec as remote_exec
from operator_remote.operators.exec import RemoteExecOp
from operator_remote.schemas.exec import RemoteExecInput


def _ctx() -> OperationContext:
    ctx = OperationContext(pipeline_id="t", node_name="exec")
    ctx.compute = type("C", (), {
        "config": {"host": "box", "user": "ubuntu", "port": "22", "known_hosts": None},
        "secrets": {"PRIVATE_KEY": "PEMKEY"},
    })()
    return ctx


def _inp(**kw) -> RemoteExecInput:
    return RemoteExecInput(command="echo hi", **kw)


class _SFTP:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def put(self, local, remote): pass
    async def chmod(self, path, mode): pass
    async def get(self, remote, local, recurse=False): pass
    async def open(self, path, mode, attrs=None):
        class _F:
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
            async def write(s, data): pass
        return _F()


class _Run:
    def __init__(self, stdout=""): self.stdout = stdout


class _Conn:
    """Fake asyncssh connection: maps command substrings → canned stdout."""
    def __init__(self, responses): self._r = responses
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def run(self, cmd, check=False):
        for key, val in self._r.items():
            if key in cmd:
                return _Run(val)
        return _Run("")
    def start_sftp_client(self): return _SFTP()


def _patch_conn(monkeypatch, responses):
    async def fake_connect(params): return _Conn(responses)
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)


def test_registered_and_is_remote_job():
    load_all_operators()
    assert "remote/exec" in default_registry.list_names()
    assert "remote.exec" in RemoteExecOp.capabilities
    assert is_remote_job_operator(RemoteExecOp) is True  # submit + poll


def test_mock_needs_no_ssh():
    out = RemoteExecOp().mock(_inp(), _ctx())
    assert out.status == "succeeded" and out.metrics == {"mock": True}


async def test_submit_returns_handle(monkeypatch):
    _patch_conn(monkeypatch, {})
    h = await RemoteExecOp().submit(_inp(), _ctx())
    assert h.backend == "ssh" and h.meta["host"] == "box" and h.meta["workdir"]


async def test_poll_running_then_success(monkeypatch):
    # running: no exit_code, pid alive
    _patch_conn(monkeypatch, {"exit_code": "", "pid": "123", "kill -0": "up"})
    st = await RemoteExecOp().poll(JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx())
    assert st.state == "running"
    # success: exit_code 0 + a metrics.json
    _patch_conn(monkeypatch, {"exit_code": "0", "metrics.json": '{"eval_loss": 0.4}'})
    st = await RemoteExecOp().poll(JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx())
    assert st.state == "succeeded" and st.metrics == {"eval_loss": 0.4}


async def test_poll_running_surfaces_live_metrics(monkeypatch):
    # #553: a still-running job that writes metrics.json incrementally must surface
    # those metrics (progress + eval_losses) on the RUNNING poll — not only at
    # completion — so the console isn't blind and the learning gate can fire mid-run.
    _patch_conn(monkeypatch, {
        "exit_code": "", "pid": "123", "kill -0": "up",
        "metrics.json": '{"step": 5, "eval_losses": [0.9, 0.7]}',
    })
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx())
    assert st.state == "running"
    assert st.metrics == {"step": 5, "eval_losses": [0.9, 0.7]}


async def test_poll_failure(monkeypatch):
    _patch_conn(monkeypatch, {"exit_code": "1", "tail": "boom"})
    st = await RemoteExecOp().poll(JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx())
    assert st.state == "failed" and "exit 1" in st.error


# ── #337: liveness — a job isn't terminal just because exit_code is absent ──


async def test_poll_no_exit_code_pid_alive_is_running(monkeypatch):
    """A `sleep 120`-style long job polled before it finishes: exit_code hasn't
    been written yet, but the recorded pid is still alive on the remote — must
    report `running`, never a premature `succeeded`/`failed`."""
    _patch_conn(monkeypatch, {"exit_code": "", "pid": "4242", "kill -0": "up"})
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "running"


async def test_poll_no_exit_code_pid_dead_is_failed(monkeypatch):
    """pid recorded, but `kill -0` finds nothing and exit_code was never
    written: the process died without going through the exit-code trap
    (killed, crashed before `{ ... ; echo $? > exit_code ; }` could run)."""
    _patch_conn(monkeypatch, {"exit_code": "", "pid": "4242"})  # kill -0 -> "" (not "up")
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "failed"
    assert "without an exit code" in st.error


async def test_poll_race_reread_exit_code_after_pid_gone(monkeypatch):
    """RACE GUARD: the process wrote exit_code AND exited in the window between our
    exit_code read and the kill -0 (two SSH round-trips; a CPU-saturated box widens
    it). First exit_code read is empty + pid is gone, but a re-read finds exit_code=0
    → the poll must report SUCCEEDED, not a false 'exited without an exit code'.
    Caught live on a ~85s 10k dataset-fix job whose box was pegged by Presidio."""
    calls = {"exit": 0}

    class _RaceConn(_Conn):
        async def run(self, cmd, check=False):
            if "exit_code" in cmd:
                calls["exit"] += 1
                return _Run("" if calls["exit"] == 1 else "0")  # empty first, 0 on re-read
            if "kill -0" in cmd:
                return _Run("")       # pid gone
            if "/pid" in cmd:
                return _Run("999")
            return _Run("")

    async def fake_connect(params):
        return _RaceConn({})
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "succeeded", st.error
    assert calls["exit"] >= 2  # proves the re-read happened


async def test_poll_no_exit_code_no_pid_is_failed(monkeypatch):
    """Neither exit_code nor pid present at all (e.g. submit's launch line
    never ran) — nothing to corroborate liveness against, so failed."""
    _patch_conn(monkeypatch, {})  # everything reads back empty
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "failed"


async def test_poll_exit_code_present_succeeds(monkeypatch):
    """exit_code already on disk (job genuinely finished) → succeeded, even
    though a pid file might still be lying around."""
    _patch_conn(monkeypatch, {"exit_code": "0", "pid": "4242"})
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "succeeded"


async def test_poll_exit_code_present_nonzero_fails(monkeypatch):
    _patch_conn(monkeypatch, {"exit_code": "2", "tail": "oops"})
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx(),
    )
    assert st.state == "failed" and "exit 2" in st.error


async def test_submit_clears_stale_state_files(monkeypatch):
    """#337: a reused workdir must not leak a PRIOR run's exit_code/pid — submit
    clears them before launching so poll can never observe stale terminal state."""
    seen: list[str] = []

    class _RecordingConn(_Conn):
        async def run(self, cmd, check=False):
            seen.append(cmd)
            return await super().run(cmd, check=check)

    async def fake_connect(params):
        return _RecordingConn({})

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    await RemoteExecOp().submit(_inp(workdir="fixed/workdir"), _ctx())
    cleanup_cmds = [c for c in seen if c.startswith("rm -f")]
    assert len(cleanup_cmds) == 1
    assert "exit_code" in cleanup_cmds[0] and "pid" in cleanup_cmds[0]
    assert "stdout.log" in cleanup_cmds[0] and "metrics.json" in cleanup_cmds[0]
    # cleanup must run BEFORE the detached launch line, not after.
    launch_idx = next(i for i, c in enumerate(seen) if "exec sh runner.sh" in c)
    cleanup_idx = seen.index(cleanup_cmds[0])
    assert cleanup_idx < launch_idx
    remote_exec._reset_conn_cache()


async def test_submit_launch_backgrounds_inside_group(monkeypatch):
    """#601: the trainer must be backgrounded INSIDE a `{ … & }` group, NOT with a
    trailing `&` on the whole `cd && …` and-list. `A && B &` backgrounds the entire
    and-list as one subshell that inherits the SSH channel's fds and WAITS for B, so
    asyncssh conn.run() blocks until the trainer EXITS → under Temporal the submit
    activity outlives its timeout → CancelledError → duplicate re-submit. Verified
    live that `< /dev/null` / `setsid` alone did NOT fix it — the and-list
    backgrounding was the culprit. setsid + all-streams-off-the-channel keep the
    trainer detached so it survives the close and a retry adopts; the pid is captured
    from inside the session (`echo $$` before `exec sh runner.sh`)."""
    seen: list[str] = []

    class _RecordingConn(_Conn):
        async def run(self, cmd, check=False):
            seen.append(cmd)
            return await super().run(cmd, check=check)

    async def fake_connect(params):
        return _RecordingConn({})

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    await RemoteExecOp().submit(_inp(), _ctx())
    launch = next(c for c in seen if "exec sh runner.sh" in c)
    assert "&& {" in launch and launch.rstrip().endswith("}")  # #601 THE FIX — backgrounded inside a group, not the whole and-list
    # the `&` backgrounds the trainer INSIDE the group — in both launch branches (#2606)
    assert "& else" in launch and "& fi" in launch
    assert "setsid" in launch                 # new session — off the channel + survives close
    assert "< /dev/null" in launch            # stdin off the channel
    assert "> /dev/null 2>&1" in launch       # stdout/stderr off the channel
    assert "exec sh runner.sh" in launch      # pid-preserving exec (so echo $$ names the trainer)
    assert "echo $$ > pid" in launch          # pid captured from inside the session
    remote_exec._reset_conn_cache()


async def test_cancel_kills_process_group_not_just_pid(monkeypatch):
    """#603: cancel must signal the process GROUP (`kill -<pid>`), not just the pid.
    The pid file names the wrapper shell (setsid group leader); the real trainer
    (cairn-train-local) is its CHILD in that group, so `kill <pid>` reaped only the
    shell and ORPHANED the trainer (the run showed cancelled but the job kept
    running). SIGTERM the group, then SIGKILL in case the trainer ignores SIGTERM."""
    from cairn.core.operator.jobs import JobHandle
    seen: list[str] = []

    class _RecordingConn(_Conn):
        async def run(self, cmd, check=False):
            seen.append(cmd)
            return await super().run(cmd, check=check)

    async def fake_connect(params):
        return _RecordingConn({})

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    monkeypatch.setenv("SSH_PRIVATE_KEY", "key")
    remote_exec._reset_conn_cache()
    h = JobHandle(job_id="h:/w/dir", backend="ssh",
                  meta={"host": "h", "user": "u", "port": 22, "workdir": "/w/dir", "known_hosts": None})
    await RemoteExecOp().cancel(h)
    kill = next(c for c in seen if "kill" in c)
    assert 'kill -TERM -"$p"' in kill          # #603 — SIGTERM the GROUP (negative pid), not `kill <pid>`
    assert 'kill -KILL -"$p"' in kill          # SIGKILL the group as fallback
    remote_exec._reset_conn_cache()


async def test_no_ssh_dep_raises(monkeypatch):
    if u.find_spec("asyncssh"):
        pytest.skip("asyncssh installed — skipping the no-dep path")
    with pytest.raises(RuntimeError, match="asyncssh"):
        await RemoteExecOp().submit(_inp(), _ctx())


# ── #337: connect/login timeouts + keepalive so a half-open connection fails
# fast instead of hanging ───────────────────────────────────────────────────


async def test_connect_passes_timeouts_and_keepalive(monkeypatch):
    """`_connect` must bound the handshake/auth (connect_timeout, login_timeout)
    and enable keepalive so a REUSED connection (see _get_conn) notices a dead
    peer instead of hanging the next `.run()` forever. asyncssh need not be
    installed for this test — we inject a fake module."""
    import sys
    import types

    calls: list[dict] = []

    async def fake_asyncssh_connect(host, **kw):
        calls.append(kw)
        return object()

    fake_asyncssh = types.ModuleType("asyncssh")
    fake_asyncssh.connect = fake_asyncssh_connect
    fake_asyncssh.import_private_key = lambda k: k
    monkeypatch.setitem(sys.modules, "asyncssh", fake_asyncssh)

    params = {"host": "box", "port": 22, "user": "ubuntu", "known_hosts": None, "private_key": None}
    await remote_exec._connect(params)

    assert len(calls) == 1
    kw = calls[0]
    assert kw["connect_timeout"] == remote_exec._CONNECT_TIMEOUT
    assert kw["login_timeout"] == remote_exec._LOGIN_TIMEOUT
    assert kw["keepalive_interval"] == remote_exec._KEEPALIVE_INTERVAL
    assert kw["keepalive_count_max"] == remote_exec._KEEPALIVE_COUNT_MAX
    # sanity: these are non-hanging, positive, finite bounds — not disabled (None/0).
    assert kw["connect_timeout"] > 0 and kw["login_timeout"] > 0
    assert kw["keepalive_interval"] > 0 and kw["keepalive_count_max"] > 0


# ── #337: connection reuse across submit/poll/cancel ────────────────────────


async def test_connection_reused_when_open(monkeypatch):
    """A conn that reports is_closed=False is cached and reused — rapid
    poll loops must NOT re-auth per call (sshd per-source penalties)."""
    from operator_remote.operators import exec as remote_exec

    remote_exec._reset_conn_cache()
    calls = {"n": 0}

    class _ReusableConn(_Conn):
        is_closed = False

    async def fake_connect(params):
        calls["n"] += 1
        return _ReusableConn({})

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    params = {"host": "h", "user": "u", "port": 22, "known_hosts": None, "private_key": None}
    c1 = await remote_exec._get_conn(params)
    c2 = await remote_exec._get_conn(params)
    assert c1 is c2 and calls["n"] == 1          # reused, single auth

    # dropped connection → transparent reconnect
    c1.is_closed = True
    c3 = await remote_exec._get_conn(params)
    assert c3 is not c1 and calls["n"] == 2
    remote_exec._reset_conn_cache()


async def test_fakes_without_is_closed_never_cached(monkeypatch):
    """Objects lacking is_closed (test fakes) are treated as closed — every
    call reconnects, keeping hermetic tests isolated from the cache."""
    from operator_remote.operators import exec as remote_exec

    remote_exec._reset_conn_cache()
    calls = {"n": 0}

    async def fake_connect(params):
        calls["n"] += 1
        return _Conn({})                          # no is_closed attribute

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    params = {"host": "h", "user": "u", "port": 22, "known_hosts": None, "private_key": None}
    await remote_exec._get_conn(params)
    await remote_exec._get_conn(params)
    assert calls["n"] == 2
    remote_exec._reset_conn_cache()


async def test_poll_running_surfaces_log_tail(monkeypatch):
    # #553: a running poll surfaces a recent stdout tail so the console isn't blind.
    _patch_conn(monkeypatch, {"exit_code": "", "pid": "1", "kill -0": "up", "stdout.log": "hello logs"})
    st = await RemoteExecOp().poll(
        JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w"}), _inp(), _ctx())
    assert st.state == "running" and st.log_tail == "hello logs"


# ── #551: idempotent submit — stable workdir + adopt-live-pid ──────────────


async def test_submit_workdir_is_stable_per_run_and_node(monkeypatch):
    # A re-dispatch of the same node (same run_id + node) lands in the SAME dir,
    # so a duplicate submit can be de-duplicated instead of double-spending.
    _patch_conn(monkeypatch, {})
    h1 = await RemoteExecOp().submit(_inp(), _ctx())
    h2 = await RemoteExecOp().submit(_inp(), _ctx())
    assert h1.meta["workdir"] == h2.meta["workdir"]
    assert h1.meta["workdir"].endswith("t-exec")  # <run_id>-<node>


async def test_submit_adopts_live_job_and_does_not_relaunch(monkeypatch):
    launched = []

    class _AdoptConn(_Conn):
        async def run(self, cmd, check=False):
            if "nohup" in cmd:
                launched.append(cmd)
            return await super().run(cmd, check=check)

    async def fake_connect(params):
        return _AdoptConn({"exit_code": "", "pid": "999", "kill -0": "up"})

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    h = await RemoteExecOp().submit(_inp(), _ctx())
    assert h.meta["workdir"].endswith("t-exec")
    assert launched == []  # adopted the live pid — did NOT launch a second process
    remote_exec._reset_conn_cache()


async def test_submit_launches_when_no_live_job(monkeypatch):
    launched = []

    class _C(_Conn):
        async def run(self, cmd, check=False):
            if "exec sh runner.sh" in cmd:  # the detached launch line (#601: setsid, was nohup)
                launched.append(cmd)
            return await super().run(cmd, check=check)

    async def fake_connect(params):
        return _C({})  # nothing alive → normal launch

    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    remote_exec._reset_conn_cache()
    await RemoteExecOp().submit(_inp(), _ctx())
    assert len(launched) == 1
    remote_exec._reset_conn_cache()


# ── #708: kill a stale process group before relaunch (OOM-stacking guard) ──
class _RecordingConn(_Conn):
    def __init__(self, responses):
        super().__init__(responses)
        self.cmds: list[str] = []

    async def run(self, cmd, check=False):
        self.cmds.append(cmd)
        return await super().run(cmd, check=check)


async def test_submit_kills_stale_group_before_relaunch(monkeypatch):
    # a pid file exists but the process is DEAD (kill -0 → not "up") → not adopted;
    # _submit must KILL the stale group before launching so a relaunch never stacks
    # a second trainer (the live OOM we hit 2026-07-11).
    conn = _RecordingConn({"exit_code": "", "/pid": "4242", "kill -0": ""})
    async def fake_connect(params): return conn
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)

    await RemoteExecOp().submit(_inp(), _ctx())
    assert any("kill -KILL" in c for c in conn.cmds), "no stale-group kill before relaunch"
    # and it precedes the detached launch (setsid runner.sh)
    kill_i = next(i for i, c in enumerate(conn.cmds) if "kill -KILL" in c)
    launch_i = next(i for i, c in enumerate(conn.cmds) if "runner.sh" in c and "setsid" in c)
    assert kill_i < launch_i


async def test_submit_adopts_live_job_no_kill(monkeypatch):
    # a LIVE job (pid alive) → adopt, do NOT kill or relaunch
    conn = _RecordingConn({"exit_code": "", "/pid": "4242", "kill -0": "up"})
    async def fake_connect(params): return conn
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)

    await RemoteExecOp().submit(_inp(), _ctx())
    assert not any("kill -KILL" in c for c in conn.cmds)
    assert not any("setsid" in c for c in conn.cmds)  # no relaunch


# ── #2606: the start window and the launch on a box without `setsid` ─────────

def _handle(**meta) -> JobHandle:
    return JobHandle(job_id="box:/w", meta={"host": "box", "workdir": "/w", **meta})


async def test_submit_stamps_launched_at(monkeypatch):
    """The pid file lands AFTER the launch command returns; poll needs the launch
    time to tell 'starting' from 'never started'."""
    _patch_conn(monkeypatch, {})
    remote_exec._reset_conn_cache()
    h = await RemoteExecOp().submit(_inp(), _ctx())
    assert isinstance(h.meta.get("launched_at"), float)
    remote_exec._reset_conn_cache()


async def test_poll_no_pid_yet_right_after_launch_is_running(monkeypatch):
    """#2606 THE RACE: the first poll after a launch finds neither pid nor exit_code
    — that is a job starting, not one that died."""
    _patch_conn(monkeypatch, {})
    remote_exec._reset_conn_cache()
    st = await RemoteExecOp().poll(_handle(launched_at=time.time()), _inp(), _ctx())
    assert st.state == "running", st.error
    remote_exec._reset_conn_cache()


async def test_poll_no_pid_long_after_launch_never_started(monkeypatch):
    """Past the start window with still no pid: the box could not detach the job.
    Failed, NOT retriable (a relaunch fails the same way), with what the box needs."""
    _patch_conn(monkeypatch, {})
    remote_exec._reset_conn_cache()
    st = await RemoteExecOp().poll(
        _handle(launched_at=time.time() - remote_exec._START_GRACE_S - 1), _inp(), _ctx())
    assert st.state == "failed" and st.retriable is False
    assert "never started" in st.error and "setsid" in st.error and "perl" in st.error
    remote_exec._reset_conn_cache()


async def test_poll_dead_pid_inside_start_window_is_still_a_death(monkeypatch):
    """The start window covers only 'no pid yet'. A pid that is gone with no exit
    code is the #552 recoverable death, even moments after launch."""
    _patch_conn(monkeypatch, {"/pid": "4242"})  # kill -0 reads back empty → dead
    remote_exec._reset_conn_cache()
    st = await RemoteExecOp().poll(_handle(launched_at=time.time()), _inp(), _ctx())
    assert st.state == "failed" and st.retriable is True
    assert "exited without an exit code" in st.error
    remote_exec._reset_conn_cache()


def _launch_for_real(tmp_path, path_env: str) -> tuple[int, int]:
    """Run the operator's real launch line through /bin/sh in a scratch workdir, with
    a 1-second job; return (pid, pgid) once the job has written its exit code."""
    import os
    import subprocess

    (tmp_path / "runner.sh").write_text(remote_exec._runner("sleep 1", {}))
    line = f"cd {shlex.quote(str(tmp_path))} && {{ {remote_exec._LAUNCH}; }}"
    rc = subprocess.run(["/bin/sh", "-c", line], env={"PATH": path_env}, timeout=10).returncode
    assert rc == 0  # the launch line returns at once (#601), whichever branch ran
    deadline = time.time() + 10
    pid_file = tmp_path / "pid"
    while not (pid_file.exists() and pid_file.read_text().strip()) and time.time() < deadline:
        time.sleep(0.05)
    pid = int(pid_file.read_text())
    pgid = os.getpgid(pid)
    while not (tmp_path / "exit_code").exists() and time.time() < deadline:
        time.sleep(0.05)
    assert (tmp_path / "exit_code").read_text().strip() == "0"
    return pid, pgid


def test_launch_without_setsid_uses_perl_and_leads_its_own_group(tmp_path):
    """#2606: on a box without `setsid` (macOS) the launch must still detach the job
    into its own session, as the leader of its own process group — cancel signals
    `kill -<pid>`, which reaches the job only if pid == pgid. PATH holds only sh and
    perl, so `command -v setsid` fails and the perl branch runs."""
    import os
    import shutil

    perl, sh = shutil.which("perl"), shutil.which("sh")
    if not perl or not sh:
        pytest.skip("perl and sh are needed to exercise the setsid-less branch")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for tool in (perl, sh, shutil.which("sleep")):
        os.symlink(tool, bindir / os.path.basename(tool))
    work = tmp_path / "w"
    work.mkdir()
    pid, pgid = _launch_for_real(work, str(bindir))
    assert pid == pgid


def test_launch_with_setsid_leads_its_own_group(tmp_path):
    """The Linux branch: `setsid` present → the same detached, group-leading job."""
    import os
    import shutil

    if not shutil.which("setsid"):
        pytest.skip("no setsid on this host (macOS) — the perl branch test covers it")
    pid, pgid = _launch_for_real(tmp_path, os.environ.get("PATH", "/usr/bin:/bin"))
    assert pid == pgid
