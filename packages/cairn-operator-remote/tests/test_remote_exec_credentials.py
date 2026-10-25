"""remote/exec: the job's env file and the credential cancel reconnects with.

* ``runner.sh`` exports the job's environment (the spec, the job-channel token,
  forwarded settings). A stale copy is removed first, and the file is CREATED
  with mode 0600 (SFTP open attrs), so it is never readable by another account
  on the box, assuming the SFTP server honours the create mode (OpenSSH does).
* Submit authenticates with the compute.ssh connection's ``PRIVATE_KEY``. Cancel
  runs later with no operation context, and used to reconnect with
  ``SSH_PRIVATE_KEY`` from the control plane's env instead: with the documented
  setup it could not authenticate, said nothing, and the job kept running. The
  handle now names the connection, and cancel resolves the same key.

Hermetic: ``_connect`` and the connection lookup are monkeypatched.
"""
from __future__ import annotations

import logging

import pytest

import cairn.core.connections as connections
from cairn.core.operator import OperationContext
from cairn.core.operator.jobs import JobHandle
from operator_remote.operators import exec as remote_exec
from operator_remote.operators.exec import RemoteExecOp
from operator_remote.schemas.exec import RemoteExecInput


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.delenv("SSH_PRIVATE_KEY", raising=False)
    remote_exec._reset_conn_cache()
    yield
    remote_exec._reset_conn_cache()


def _ctx() -> OperationContext:
    ctx = OperationContext(pipeline_id="t", node_name="exec")
    ctx.compute = type("C", (), {
        "name": "gpu",
        "config": {"host": "box", "user": "ubuntu", "port": "22", "known_hosts": "/kh"},
        "secrets": {"PRIVATE_KEY": "CONN-KEY"},
    })()
    return ctx


class _Run:
    def __init__(self, stdout=""):
        self.stdout = stdout


class _Conn:
    def __init__(self, events: list, existing: frozenset = frozenset()):
        self.events = events
        self.existing = existing

    async def run(self, cmd, check=False):
        self.events.append(("run", cmd))
        return _Run("")

    def start_sftp_client(self):
        events, existing = self.events, self.existing

        class _SFTP:
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
            async def put(s, local, remote): events.append(("put", local))
            async def open(s, path, mode, attrs=None):
                if path in existing:
                    raise OSError("file exists")   # what FXF_EXCL does on a real server
                events.append(("open", path, mode, getattr(attrs, "permissions", None)))

                class _F:
                    async def __aenter__(f): return f
                    async def __aexit__(f, *a): return False
                    async def write(f, data): events.append(("write", path, data))
                return _F()
        return _SFTP()


def _wire(monkeypatch, existing: frozenset = frozenset()) -> tuple[list, list]:
    events: list = []
    params_seen: list = []

    async def fake_connect(params):
        params_seen.append(params)
        return _Conn(events, existing)
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    return events, params_seen


def _bind_connection(monkeypatch, key="CONN-KEY", *, exists=True):
    seen: list = []

    async def fake_resolve(name, **_kw):
        seen.append(name)
        if not exists:
            return None
        return type("RC", (), {
            "name": name,
            "config": {"host": "box", "user": "ubuntu", "port": 22, "known_hosts": "/kh"},
            "secrets": {"PRIVATE_KEY": key},
        })()
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)
    return seen


async def test_runner_is_created_owner_only(monkeypatch):
    events, _ = _wire(monkeypatch)
    await RemoteExecOp().submit(RemoteExecInput(command="run", env={"CAIRN_ENGINE_TOKEN": "tok"}), _ctx())
    runner = [e for e in events if e[0] != "run" and str(e[1]).endswith("runner.sh")]
    assert [e[0] for e in runner] == ["open", "write"], runner
    assert runner[0][3] == 0o600                      # the mode is set when it is created
    import asyncssh

    assert runner[0][2] & asyncssh.FXF_EXCL           # created exclusively: never truncated
    # a stale runner.sh is removed first, so the create mode always applies
    rm = next(e[1] for e in events if e[0] == "run" and e[1].startswith("rm -f"))
    assert "runner.sh" in rm
    # the value is in the file, not on any command line
    assert not any("tok" in e[1] for e in events if e[0] == "run")


async def test_submit_records_the_connection_it_used(monkeypatch):
    _wire(monkeypatch)
    h = await RemoteExecOp().submit(RemoteExecInput(command="run"), _ctx())
    assert h.meta["connection"] == "gpu"


async def test_cancel_reconnects_with_the_submitting_connections_key(monkeypatch):
    events, params_seen = _wire(monkeypatch)
    seen = _bind_connection(monkeypatch, key="CONN-KEY")
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w",
        "known_hosts": "/kh", "connection": "gpu"})
    await RemoteExecOp().cancel(h)
    assert seen == ["gpu"]
    assert params_seen[0]["private_key"] == "CONN-KEY"
    assert params_seen[0]["known_hosts"] == "/kh"
    assert any(e[0] == "run" and 'kill -TERM -"$p"' in e[1] for e in events)


async def test_cancel_of_a_handle_from_before_the_connection_was_recorded(monkeypatch):
    """A handle persisted by an earlier version names no connection: cancel keeps
    the old behaviour (the control plane's ``SSH_PRIVATE_KEY``)."""
    _, params_seen = _wire(monkeypatch)
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh",
                  meta={"host": "box", "user": "ubuntu", "port": 22, "workdir": "/w"})
    await RemoteExecOp().cancel(h)
    assert params_seen[0]["private_key"] == "AMBIENT-KEY"


async def test_cancel_with_an_unresolvable_connection_says_so(monkeypatch, caplog):
    _, params_seen = _wire(monkeypatch)
    _bind_connection(monkeypatch, exists=False)
    monkeypatch.setenv("SSH_PRIVATE_KEY", "AMBIENT-KEY")
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w", "connection": "gpu"})
    with caplog.at_level(logging.WARNING):
        assert await remote_exec._cancel(h) is False
    assert params_seen == []                 # never fell back to another credential
    assert "gpu" in caplog.text and "/w" in caplog.text
    assert "AMBIENT-KEY" not in caplog.text


async def test_cancel_that_cannot_authenticate_says_so(monkeypatch, caplog):
    _bind_connection(monkeypatch, key="CONN-KEY")

    async def fake_connect(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_connect", fake_connect)
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w", "connection": "gpu"})
    with caplog.at_level(logging.WARNING):
        assert await remote_exec._cancel(h) is False
    assert "PermissionError" in caplog.text and "box" in caplog.text
    assert "CONN-KEY" not in caplog.text


# ── the job's account: cancel works with no workspace in scope ──────────────


async def test_submit_records_the_account_and_workspace(monkeypatch):
    _wire(monkeypatch)
    ctx = _ctx()
    ctx.compute.account_id = "acct-1"
    h = await RemoteExecOp().submit(RemoteExecInput(command="run"), ctx)
    assert h.meta["account_id"] == "acct-1"
    assert h.meta["workspace_id"]


async def test_cancel_resolves_in_the_jobs_recorded_account(monkeypatch):
    _, params_seen = _wire(monkeypatch)
    seen_kw: list = []

    async def fake_resolve(name, **kw):
        seen_kw.append(kw)
        return type("RC", (), {"name": name, "config": {"host": "box"},
                               "secrets": {"PRIVATE_KEY": "CONN-KEY"}})()
    monkeypatch.setattr(connections, "resolve_connection", fake_resolve)
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w",
        "connection": "gpu", "account_id": "acct-1"})
    assert await remote_exec._cancel(h) is True
    assert seen_kw == [{"account_id": "acct-1"}]
    assert params_seen[0]["private_key"] == "CONN-KEY"


def _real_connection(monkeypatch, tmp_path):
    """A real sqlite connection store holding the deployment's `gpu` connection,
    with auth ON and no workspace in scope (the learning-gate abort, a run
    cancel outside its scope). Nothing about the account is stubbed."""
    from cairn.core.connections import Connection, SqliteConnectionStore

    db = str(tmp_path / "connections.db")
    monkeypatch.setenv("CAIRN_CONNECTIONS_DB", db)
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    monkeypatch.delenv("CAIRN_WORKSPACE", raising=False)
    monkeypatch.setenv("GPU_SSH_KEY", "CONN-KEY")
    store = SqliteConnectionStore(db)
    return store, Connection(name="gpu", kind="compute.ssh", account_id="default",
                             config={"host": "box", "user": "ubuntu", "port": "22"},
                             secrets={"PRIVATE_KEY": "GPU_SSH_KEY"})


async def test_cancel_with_auth_on_and_no_workspace_in_scope(monkeypatch, tmp_path):
    store, conn = _real_connection(monkeypatch, tmp_path)
    await store.create(conn)
    store.close()
    monkeypatch.setenv("CAIRN_JWT_SECRET", "s" * 40)
    _, params_seen = _wire(monkeypatch)
    h = JobHandle(job_id="box:/w", backend="ssh", meta={
        "host": "box", "user": "ubuntu", "port": 22, "workdir": "/w",
        "connection": "gpu", "account_id": "default"})
    assert await remote_exec._cancel(h) is True
    assert params_seen[0]["private_key"] == "CONN-KEY"


class _Registry:
    def get(self, name):
        return RemoteExecOp


async def test_a_run_cancel_counts_only_jobs_it_reached(monkeypatch, tmp_path):
    """dispatcher.cancel → cancel_remote_jobs_for_run, with auth on and no scope:
    the job is reached with the recorded account; a box that cannot be reached is
    not counted as cancelled (the record is still closed)."""
    from cairn.core.operator import job_store as js
    from cairn.core.operator.job_control import cancel_remote_jobs_for_run
    from cairn.core.operator.job_store import RUNNING, InMemoryRemoteJobStore, RemoteJobRecord

    store, conn = _real_connection(monkeypatch, tmp_path)
    await store.create(conn)
    store.close()
    monkeypatch.setenv("CAIRN_JWT_SECRET", "s" * 40)
    jobs = InMemoryRemoteJobStore()
    monkeypatch.setattr(js, "get_remote_job_store", lambda: jobs)
    meta = {"host": "box", "user": "ubuntu", "port": 22, "workdir": "/w",
            "connection": "gpu", "account_id": "default"}
    await jobs.register(RemoteJobRecord(run_id="r-1", node_name="n1", provider="remote/exec",
                                        job_id="box:/w", backend="ssh", status=RUNNING,
                                        job_meta=meta, workspace_id="ws-a"))
    _, params_seen = _wire(monkeypatch)
    assert await cancel_remote_jobs_for_run("r-1", registry=_Registry()) == 1
    assert params_seen[0]["private_key"] == "CONN-KEY"

    async def unreachable(params):
        raise PermissionError("Permission denied")
    monkeypatch.setattr(remote_exec, "_connect", unreachable)
    remote_exec._reset_conn_cache()
    await jobs.register(RemoteJobRecord(run_id="r-2", node_name="n1", provider="remote/exec",
                                        job_id="box:/w2", backend="ssh", status=RUNNING,
                                        job_meta={**meta, "workdir": "/w2"}, workspace_id="ws-a"))
    assert await cancel_remote_jobs_for_run("r-2", registry=_Registry()) == 0
    assert (await jobs.get("r-2:n1")).status == "cancelled"


async def test_an_existing_runner_is_never_reused(monkeypatch):
    """If the earlier runner.sh survived the pre-launch rm (e.g. it is not ours to
    remove), the exclusive create fails and nothing is written or launched: an
    existing file is never truncated and left with its old mode."""
    events, _ = _wire(monkeypatch, existing=frozenset({".cairn-jobs/t-exec/runner.sh"}))
    with pytest.raises(RuntimeError, match="fresh runner.sh"):
        await RemoteExecOp().submit(RemoteExecInput(command="run", env={"K": "v"}), _ctx())
    assert not any(e[0] == "write" for e in events)
    assert not any(e[0] == "run" and "exec sh runner.sh" in e[1] for e in events)
