"""`cairn remote` CLI (an internal issue).

Pure helpers (kv parse, config round-trip, resolution precedence) plus
end-to-end command coverage against an in-process server: the CLI's two HTTP
seams — ``remote._api`` (the typed cairn-client) and ``remote._sse_client``
(the raw httpx stream) — are pointed at a real ``build_app`` instance through
Starlette's sync test transport, so the CLI exercises the actual HTTP control
plane, routes and JWT middleware included.

#1560: these patched ``remote._client`` until 2026-08-24, an attribute
removed by #1216 when ``cairn remote`` was ported onto cairn-client. Nine
tests had been failing on ``AttributeError`` ever since — asserting against a
surface that no longer existed, and hiding whatever the CLI actually does.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
import typer
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from cairn.cli.commands import remote
from cairn.core.engine.runner import Runner
from cairn.core.triggers.server import build_app

cli = CliRunner()


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("OBS_CONFIG_HOME", str(tmp_path / "cfg"))
    for k in (
        "OBS_REMOTE_ENDPOINT", "OBS_REMOTE_TOKEN",
        "OBS_JWT_SECRET", "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH",
    ):
        monkeypatch.delenv(k, raising=False)
    yield


def _patch_client(monkeypatch, app):
    """Point the CLI's HTTP seams at an in-process ``app``.

    #1560 — this used to patch ``remote._client`` and hand back a
    ``TestClient``. PR #1216 (#1195 slice 1) ported ``cairn remote`` onto
    cairn-client: the factory is now ``_api`` and returns a typed
    ``CairnClient``, so the old patch silently missed and every
    server-touching test in this file failed with ``AttributeError: module
    … has no attribute '_client'``. The tests were asserting against a
    surface that had not existed for months.

    Both seams are patched, because the CLI has two: ``_api`` for
    request/response (typed client) and ``_sse_client`` for the raw httpx
    stream the client has no door for yet. Both drive the SAME in-process
    app through an ASGI transport, so the CLI still exercises the real
    control plane — routes, dependencies and the JWT middleware included —
    rather than a mock of it, which is the whole point of this file.
    """
    import httpx
    from cairn_client import CairnClient

    def _transport():
        # Starlette's TestClient transport: a SYNC httpx transport that drives
        # the real ASGI app in-process (no socket, no server thread). NOT
        # httpx.ASGITransport — that one is async-only, and a sync client
        # calling transport.close() on it dies with AttributeError.
        return TestClient(app)._transport

    def _api_factory(endpoint, token, *, timeout=30.0):
        return CairnClient("http://testserver", token=token, timeout=timeout,
                           transport=_transport())

    def _sse_factory(endpoint, token, *, timeout=30.0):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return httpx.Client(base_url="http://testserver", headers=headers,
                            timeout=timeout, transport=_transport())

    monkeypatch.setattr(remote, "_api", _api_factory)
    monkeypatch.setattr(remote, "_sse_client", _sse_factory)


# ─── pure helpers ──────────────────────────────────────────────────


def test_parse_kv():
    assert remote._parse_kv(["a=1", "b=two"]) == {"a": "1", "b": "two"}


def test_parse_kv_rejects_bare():
    with pytest.raises(typer.Exit):
        remote._parse_kv(["bad"])


def test_config_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("OBS_CONFIG_HOME", str(tmp_path))
    remote._save_config({
        "default_profile": "prod",
        "profiles": {"prod": {"endpoint": "https://x", "token": "t0k"}},
    })
    got = remote._load_config()
    assert got["default_profile"] == "prod"
    assert got["profiles"]["prod"]["endpoint"] == "https://x"
    assert got["profiles"]["prod"]["token"] == "t0k"


def test_resolve_precedence(tmp_path, monkeypatch):
    monkeypatch.setenv("OBS_CONFIG_HOME", str(tmp_path))
    remote._save_config({
        "default_profile": "default",
        "profiles": {"default": {"endpoint": "http://profile", "token": "ptok"}},
    })
    assert remote._resolve(None, None, None) == ("http://profile", "ptok")
    monkeypatch.setenv("OBS_REMOTE_ENDPOINT", "http://env")
    ep, _ = remote._resolve(None, None, None)
    assert ep == "http://env"
    ep, tok = remote._resolve("http://flag", "ftok", None)
    assert ep == "http://flag" and tok == "ftok"


# ─── e2e against an in-process server ──────────────────────────────


def test_ps_empty(monkeypatch):
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, ["ps"])
    assert res.exit_code == 0, res.output
    assert "no runs" in res.output


def test_submit_returns_run_id(monkeypatch):
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, ["submit", "nonexistent-skill"])
    assert res.exit_code == 0, res.output
    assert "run_id=" in res.output


def test_submit_401_when_auth_enabled(monkeypatch):
    monkeypatch.setenv("OBS_JWT_SECRET", "s")
    monkeypatch.setenv("OBS_API_PASSWORD", "p")
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    # no token configured → middleware rejects
    res = cli.invoke(remote.app, ["submit", "x"])
    assert res.exit_code == 1
    assert "401" in res.output or "Unauthorized" in res.output


def test_runs_table_renders_rows():
    import io

    from rich.console import Console

    t = remote._runs_table([{
        "id": "r1", "status": "running", "lane": "realtime",
        "matched_skills": 1, "results": [{"template_name": "finetune"}], "created_at": "t0",
    }])
    buf = io.StringIO()
    Console(file=buf, width=200).print(t)
    out = buf.getvalue()
    assert "r1" in out and "running" in out and "finetune" in out


def test_watch_once_renders(monkeypatch):
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, ["watch", "--once"])
    assert res.exit_code == 0, res.output
    assert "run_id" in res.output  # table header rendered (empty server)


@pytest.mark.parametrize("cmd", ["approve", "reject"])
def test_hitl_targets_resume_endpoint_not_action_saga(monkeypatch, cmd):
    # an internal issue: approve/reject must hit POST /runs/{id}/resume (the
    # operation gate, #38), NOT the action-saga GET /triggers/approve. An unknown
    # run on the resume endpoint returns 404; the old action-saga path returned
    # 501 (no DurableActionHandler). So 404-not-501 proves the routing fix.
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, [cmd, "no-such-run"])
    assert res.exit_code == 1, res.output
    assert "404" in res.output
    assert "501" not in res.output


# ─── interactive HITL watch loop (#53) ─────────────────────────────


def test_decide_explicit_decision_wins(monkeypatch):
    monkeypatch.setattr(remote, "_is_interactive", lambda: False)  # even headless
    assert remote._decide(decision_opt="approve", native=False, timeout=None, on_timeout="detach") == "approve"
    assert remote._decide(decision_opt="reject", native=False, timeout=None, on_timeout="detach") == "reject"


def test_decide_invalid_decision_exits():
    with pytest.raises(typer.Exit):
        remote._decide(decision_opt="maybe", native=False, timeout=None, on_timeout="detach")


def test_decide_headless_detaches(monkeypatch):
    # Rule 3 corrected: no TTY + no --decision → never block on stdin, detach.
    monkeypatch.setattr(remote, "_is_interactive", lambda: False)
    assert remote._decide(decision_opt=None, native=False, timeout=None, on_timeout="detach") is None


@pytest.mark.parametrize("on_timeout,expected", [("detach", None), ("reject", "reject"), ("abort", "abort")])
def test_decide_timeout_applies_on_timeout(monkeypatch, on_timeout, expected):
    # Rule 1 corrected: a prompt timeout applies the explicit --on-timeout policy.
    monkeypatch.setattr(remote, "_is_interactive", lambda: True)
    monkeypatch.setattr(remote.select, "select", lambda *a, **k: ([], [], []))  # always times out
    assert remote._decide(decision_opt=None, native=False, timeout=0.01, on_timeout=on_timeout) == expected


def test_decide_tty_prompt(monkeypatch):
    monkeypatch.setattr(remote, "_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *a: "a")
    assert remote._decide(decision_opt=None, native=False, timeout=None, on_timeout="detach") == "approve"
    monkeypatch.setattr("builtins.input", lambda *a: "r")
    assert remote._decide(decision_opt=None, native=False, timeout=None, on_timeout="detach") == "reject"


# ── watch-loop orchestration against a scripted fake server ──

class _FakeResp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class _FakeStream:
    def __init__(self, lines):
        self.status_code = 200
        self._lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_lines(self):
        yield from self._lines


class _FakeClient:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def stream(self, method, path, params=None):
        return _FakeStream(self.state["streams"].pop(0) if self.state["streams"] else [])

    def get(self, path, params=None):
        if path.endswith("/events"):
            return _FakeResp(200, {"events": []})
        return _FakeResp(200, {"status": "awaiting_approval",
                               "results": [{"skill_name": "s", "template_name": "t"}]})

    def post(self, path, params=None):
        self.state["posts"].append((path, params))
        return _FakeResp(200, {"resumed": True, "still_paused": False})


def _sse(seq, ev_type, data="{}"):
    return [f"id: {seq}", f"event: {ev_type}", f"data: {data}", ""]


def _patch_fake(monkeypatch, state):
    """Drive the watch loop off scripted SSE, through the REAL client (#1560).

    Previously this patched ``remote._client`` with a hand-rolled fake whose
    ``post()`` recorded the raw path — the attribute has not existed since
    #1216. Rather than re-fake the typed client's methods, both seams now run
    the genuine objects over an ``httpx.MockTransport``:

      * ``_api``        → a real ``CairnClient``; ``c.resume_run(...)`` really
        builds its request, so ``state["posts"]`` still records the true URL
        and body. That keeps the invariant these tests exist for
        (``/runs/{id}/resume``, not an action-saga endpoint) AND newly proves
        cairn-client maps the typed call onto it.
      * ``_sse_client`` → a real ``httpx.Client`` serving the scripted event
        lines, so the loop's own parsing is exercised, not stubbed.
    """
    import json

    import httpx
    from cairn_client import CairnClient

    def _handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST":
            body = json.loads(request.content.decode() or "{}")
            state["posts"].append((path, body))
            return httpx.Response(200, json={"resumed": True, "still_paused": False})
        if path.endswith("/stream"):
            lines = state["streams"].pop(0) if state["streams"] else []
            return httpx.Response(200, text="\n".join(lines) + "\n")
        if path.endswith("/events"):
            return httpx.Response(200, json={"events": []})
        return httpx.Response(200, json={
            "status": "awaiting_approval",
            "results": [{"skill_name": "s", "template_name": "t"}],
        })

    monkeypatch.setattr(remote, "_api", lambda ep, tok, *, timeout=30.0: CairnClient(
        "http://testserver", token=tok, timeout=timeout,
        transport=httpx.MockTransport(_handler)))
    monkeypatch.setattr(remote, "_sse_client", lambda ep, tok, *, timeout=30.0: httpx.Client(
        base_url="http://testserver", timeout=timeout,
        transport=httpx.MockTransport(_handler)))


def test_watch_resumes_at_pause(monkeypatch):
    # stream pauses at awaiting_approval; with --decision approve the loop posts
    # the decision to /runs/{id}/resume, then the run streams to done → exit 0.
    state = {"streams": [
        _sse(1, "running") + _sse(2, "awaiting_approval", '{"matched_skills": 1}'),
        _sse(3, "resumed", '{"still_paused": false}') + _sse(4, "done"),
    ], "posts": []}
    _patch_fake(monkeypatch, state)
    rc = remote._watch_hitl("http://x", None, "run-1", approver="cli-user", decision_opt="approve",
                            native=False, timeout=None, on_timeout="detach", flow=False, dump=None)
    assert rc == 0
    assert state["posts"] == [("/runs/run-1/resume", {"decision": "approve", "approver": "cli-user"})]


def test_watch_detaches_headless(monkeypatch):
    state = {"streams": [_sse(1, "running") + _sse(2, "awaiting_approval")], "posts": []}
    _patch_fake(monkeypatch, state)
    monkeypatch.setattr(remote, "_is_interactive", lambda: False)
    rc = remote._watch_hitl("http://x", None, "run-1", approver="cli-user", decision_opt=None,
                            native=False, timeout=None, on_timeout="detach", flow=False, dump=None)
    assert rc == 2                 # detached
    assert state["posts"] == []    # never resumed — run stays paused


def test_watch_command_unknown_run_against_real_server(monkeypatch):
    # `approve --watch` end-to-end through CliRunner against the real build_app:
    # an unknown run's SSE stream returns 404, so the watch loop fails cleanly.
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, ["approve", "no-such-run", "--watch", "--decision", "approve"])
    assert res.exit_code == 1
    assert "not found" in res.output or "404" in res.output


def test_pipelines_lists_name_and_step_progress(monkeypatch):
    """0.1.2 printed ``0/0 steps`` and no name for every pipeline: the CLI read
    ``step_state``/``pipeline_name``, but ``GET /pipelines`` answers ``steps``/``name``.
    Driven through the real route, so the row shape is the server's own."""
    import asyncio

    from cairn.core.storage import pipeline_store as ps

    store = ps.InMemoryPipelineStore()
    monkeypatch.setattr(ps, "_default_store", store)
    rec = ps.PipelineRecord(
        pipeline_name="profile-and-publish", status="running",
        step_state={"profile": ps.StepState(status="done"),
                    "data-ok": ps.StepState(status="done"),
                    "review": ps.StepState(status="awaiting_approval"),
                    "publish": ps.StepState(status="pending")})
    asyncio.run(store.put(rec))
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    res = cli.invoke(remote.app, ["pipelines"])
    assert res.exit_code == 0, res.output
    line = next(ln for ln in res.output.splitlines() if rec.pipeline_id in ln)
    assert "2/4 steps" in line, line
    assert line.rstrip().endswith("profile-and-publish"), line
