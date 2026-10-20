"""The server's access log never records a query-string bearer.

The run stream accepts ``?access_token=<jwt>`` (#58: a browser's EventSource
cannot set a header), and uvicorn's access log records the full path with its
query string. The 0.1.2 verification found the JWT in the engine log in full.

Driven through a real uvicorn server (its own logging config, its own access
logger) in front of the real app with auth on.
"""
from __future__ import annotations

import logging
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
import uvicorn

from cairn.core.engine.runner import Runner
from cairn.core.triggers import auth
from cairn.core.triggers.server import build_app


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def served(monkeypatch):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "CAIRN_JWT_ALGORITHM", "CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES", "CAIRN_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret-test-secret-test-secret")
    app = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    port = _free_port()
    # uvicorn configures its loggers here (dictConfig), after build_app ran.
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                           log_level="info", access_log=True, lifespan="off"))
    cap = _Capture()
    access = logging.getLogger("uvicorn.access")
    access.addHandler(cap)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.02)
    assert server.started, "uvicorn did not start"
    try:
        yield f"http://127.0.0.1:{port}", cap
    finally:
        server.should_exit = True
        t.join(timeout=10)
        access.removeHandler(cap)


def _get(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def test_stream_query_token_is_redacted_in_the_access_log(served):
    base, cap = served
    token = auth.create_access_token({"sub": "alice"})
    # 404: the guard accepted the token and the route answered (no such run).
    assert _get(f"{base}/runs/no-such-run/stream?access_token={token}") == 404
    for _ in range(100):
        if any("/runs/no-such-run/stream" in ln for ln in cap.lines):
            break
        time.sleep(0.02)
    lines = [ln for ln in cap.lines if "/runs/no-such-run/stream" in ln]
    assert lines, cap.lines
    assert all(token not in ln for ln in lines), lines
    assert any("access_token=***" in ln for ln in lines), lines


def test_redaction_keeps_the_rest_of_the_query():
    assert auth.redact_query_credentials(
        "/runs/r/stream?since=3&access_token=eyJ.a.b&x=1"
    ) == "/runs/r/stream?since=3&access_token=***&x=1"
    assert auth.redact_query_credentials("/runs?limit=5") == "/runs?limit=5"
