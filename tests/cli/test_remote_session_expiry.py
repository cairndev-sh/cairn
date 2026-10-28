"""#230 — a `cairn remote` session the server refuses ends in one line and exit 77.

The qualification run's failure: the CLI token expired mid-run and a watch went quiet.
Now any verb that meets a 401 with a session prints `session expired at HH:MM — run
cairn remote login` (or `session refused by the server (401)` for an unexpired token)
and exits 77, watches and the end-of-stream status check included. The CLI does not
renew its sign-in (ADR-0142 D8). The profile file is written 0600.

The end-to-end tests drive a real app with auth on through the CLI's HTTP seams, as
`test_remote.py` does; the watch test uses a fake client.
"""
from __future__ import annotations

import os
import stat
import time
import types
from datetime import timedelta

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

from cairn.cli.commands import remote  # noqa: E402

cli = CliRunner()
SECRET = "test-secret-long-enough-for-hs256-abc"


@pytest.fixture()
def app(tmp_path, monkeypatch):
    """A real app, auth on, env admin `admin`/`pw`, every store in tmp; the CLI's
    two HTTP seams pointed at it and its config in tmp."""
    import httpx
    from cairn_client import CairnClient
    from fastapi.testclient import TestClient

    from cairn.core import account as A
    from cairn.core import refresh_tokens as RT
    from cairn.core import workspace as _ws
    from cairn.core.triggers import auth
    from cairn.core.triggers.server import build_app

    for name in ("JWT_SECRET", "ACCESS_TOKEN_EXPIRE_MINUTES", "API_USER", "API_PASSWORD",
                 "API_PASSWORD_HASH", "PUBLIC_URL", "REMOTE_ENDPOINT", "REMOTE_TOKEN"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_API_USER", "admin")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "pw")
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path / "cfg"))
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    monkeypatch.setattr(RT, "_default_store", RT.SqliteRefreshTokenStore(path=str(tmp_path / "rt.db")))
    monkeypatch.setattr(auth, "_login_failures", {})

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    application = build_app(FakeRunner(), enable_scheduler=False)

    def _api(endpoint, token, *, timeout=30.0):
        return CairnClient("http://testserver", token=token, timeout=timeout,
                           transport=TestClient(application)._transport)

    def _sse(endpoint, token, *, timeout=30.0):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.Client(base_url="http://testserver", headers=headers, timeout=timeout,
                            transport=TestClient(application)._transport)

    monkeypatch.setattr(remote, "_api", _api)
    monkeypatch.setattr(remote, "_sse_client", _sse)
    return application


def _expired_token(minutes_ago: int = 5) -> str:
    from cairn.core.auth_tokens import create_access_token

    return create_access_token({"sub": "admin", "account_id": "default"},
                               expires_delta=timedelta(minutes=-minutes_ago))


def _login() -> dict:
    res = cli.invoke(remote.app, ["login", "-u", "admin", "-p", "pw", "-e", "http://testserver"])
    assert res.exit_code == 0, res.output
    return remote._load_config()["profiles"]["default"]


def _set_token(token: str) -> None:
    cfg = remote._load_config()
    cfg["profiles"]["default"]["token"] = token
    remote._save_config(cfg)


def _expired_line(token: str) -> str:
    exp = remote._token_exp(token)
    return f"session expired at {time.strftime('%H:%M', time.localtime(exp))} — run `cairn remote login`"


def test_login_writes_only_the_access_token_to_a_private_file(app):
    prof = _login()
    assert prof["token"] and set(prof) == {"endpoint", "token"}  # no refresh credential
    assert stat.S_IMODE(os.stat(remote._config_path()).st_mode) == 0o600
    assert cli.invoke(remote.app, ["ps"]).exit_code == 0


def test_an_expired_session_says_so_and_exits_77(app):
    _login()
    stale = _expired_token(minutes_ago=7)
    _set_token(stale)
    res = cli.invoke(remote.app, ["ps"])
    assert res.exit_code == remote.EXIT_SESSION_EXPIRED == 77
    assert _expired_line(stale) in res.output
    assert stale not in res.output


def test_findings_ends_the_same_way(app):
    _login()
    _set_token(_expired_token())
    res = cli.invoke(remote.app, ["findings", "pl_x"])
    assert res.exit_code == 77, res.output
    assert "run `cairn remote login`" in res.output


def test_an_explicit_expired_token_exits_77(app):
    _login()
    res = cli.invoke(remote.app, ["ps", "--token", _expired_token()])
    assert res.exit_code == 77, res.output


def test_a_refused_unexpired_token_is_named_as_refused(app):
    _login()
    res = cli.invoke(remote.app, ["ps", "--token", "not.a-real.token"])
    assert res.exit_code == 77, res.output
    assert "session refused by the server (401)" in res.output


def test_no_session_at_all_keeps_the_plain_401_and_exit_1(app):
    res = cli.invoke(remote.app, ["ps", "-e", "http://testserver"])
    assert res.exit_code == 1, res.output
    assert "401 Unauthorized — run `cairn remote login`" in res.output


def test_logs_follow_ends_loudly_when_the_stream_is_refused(app):
    _login()
    stale = _expired_token()
    _set_token(stale)
    res = cli.invoke(remote.app, ["logs", "run_x", "--follow"])
    assert res.exit_code == 77, res.output
    assert _expired_line(stale) in res.output


def test_launch_watch_ends_loudly_mid_poll_with_how_to_reattach(tmp_path, monkeypatch):
    """`launch --watch` polls `GET /pipelines/{id}`; a 401 mid-watch prints the expiry
    and how to re-attach (the pipeline keeps running) and exits 77, never silently."""
    from cairn_client import CairnAPIError

    for name in ("REMOTE_ENDPOINT", "REMOTE_TOKEN"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    stale = _expired_token()

    class _Client:
        token = stale

        def pipeline(self, pid):
            raise CairnAPIError(401, "token expired", "get", f"/pipelines/{pid}")

    import typer

    with pytest.raises(typer.Exit) as exc:
        remote._poll_pipeline(_Client(), "pl_1")
    assert exc.value.exit_code == 77


class _Stream:
    """One SSE response: a status code and no frames (the stream closes at once)."""

    def __init__(self, status_code):
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def raise_for_status(self):
        return None

    def iter_lines(self):
        return iter(())


class _Sse:
    def __init__(self, status_code):
        self._code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def stream(self, *a, **k):
        return _Stream(self._code)


class _RunClient:
    """`GET /runs/{id}` answering from a script: an exception to raise, or a status."""

    def __init__(self, script):
        self._script = script

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def run(self, run_id):
        step = self._script.pop(0)
        if isinstance(step, Exception):
            raise step
        return {"status": step}


def _offline(monkeypatch, sse_code, script):
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setattr(remote, "_sse_client", lambda ep, tok, timeout=None: _Sse(sse_code))
    monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: _RunClient(script))


def test_approve_watch_ends_loudly_when_the_stream_is_refused(monkeypatch):
    import typer

    _offline(monkeypatch, 401, [])
    with pytest.raises(typer.Exit) as exc:
        remote._watch_hitl("http://e", _expired_token(), "run_x", approver=None,
                           decision_opt=None, native=False, timeout=None,
                           on_timeout=None, flow=None, dump=None)
    assert exc.value.exit_code == 77


@pytest.mark.parametrize("follow", ["tail", "watch_hitl"])
def test_a_refused_status_check_at_the_end_of_a_stream_exits_77_not_0(monkeypatch, follow):
    """The stream closes without a terminal frame and the status check meets a 401:
    exit 77, never the silent exit 0 that looks like a quiet run."""
    import typer
    from cairn_client import CairnAPIError

    _offline(monkeypatch, 200, [CairnAPIError(401, "expired", "get", "/runs/run_x")])
    with pytest.raises(typer.Exit) as exc:
        if follow == "tail":
            remote._tail("http://e", _expired_token(), "run_x")
        else:
            remote._watch_hitl("http://e", _expired_token(), "run_x", approver=None,
                               decision_opt=None, native=False, timeout=None,
                               on_timeout=None, flow=None, dump=None)
    assert exc.value.exit_code == 77


def test_a_transport_error_at_the_end_of_a_stream_reconnects(monkeypatch):
    """An engine restarting between frames is not an ending: reconnect, then stop
    when the run is terminal."""
    import httpx

    script = [httpx.ConnectError("engine restarting"), "done"]
    _offline(monkeypatch, 200, script)
    remote._tail("http://e", "tok", "run_x")
    assert script == []
