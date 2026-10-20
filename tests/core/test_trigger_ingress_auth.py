"""Which trigger ingress paths skip the JWT guard (#59).

A webhook receiver that REQUIRES a valid signature authenticates by it, so it is
reachable without a bearer token (#686). A receiver whose signature is optional
(``signature.required: false``) has no authentication of its own: it used to be
registered public anyway, so anyone who could reach the port started runs with an
unsigned payload. It now keeps the JWT guard when auth is on.

Driven through the real app with auth on: ``build_app`` + the real
``_mount_trigger_routes`` + the real signature check.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.engine.runner import Runner
from cairn.core.engine.triggers_spec import load_trigger_spec_text
from cairn.core.triggers import auth
from cairn.core.triggers.router import TriggerRouter
from cairn.core.triggers.server import _mount_trigger_routes, build_app

SECRET = "hook-secret"


def _spec(path: str, *, required: bool):
    return load_trigger_spec_text(
        "source: custom\n"
        f"ingress: {{transport: http, path: {path}, method: POST, signature: "
        f"{{header: X-Signature, secret: '${{env:HOOK_SECRET}}', required: {str(required).lower()}}}}}\n"
        "normalize: {event_type: type}\n"
        "run: {template: pipeline:p/x}\n",
        name=path,
    )


class _Started:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def __call__(self, ref, inputs, *, environment=None):
        self.calls.append((ref, dict(inputs)))
        return type("Run", (), {"id": f"run-{len(self.calls)}", "status": "queued"})()


@pytest.fixture
def started() -> _Started:
    return _Started()


def _client(monkeypatch, started, *, auth_on: bool) -> TestClient:
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    if auth_on:
        monkeypatch.setenv("OBS_JWT_SECRET", "test-secret")
    else:
        monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    monkeypatch.setenv("HOOK_SECRET", SECRET)
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())  # module state: start clean
    app = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    _mount_trigger_routes(app, TriggerRouter(
        [_spec("/triggers/optional", required=False), _spec("/triggers/signed", required=True)],
        run_template=started,
    ))
    return TestClient(app)


BODY = json.dumps({"type": "push"}).encode()


def _sig(body: bytes = BODY) -> dict[str, str]:
    return {"X-Signature": hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest(),
            "content-type": "application/json"}


# ─── the defect: an optional-signature receiver was public ─────────────


def test_optional_signature_receiver_refuses_an_anonymous_unsigned_payload(monkeypatch, started):
    c = _client(monkeypatch, started, auth_on=True)
    r = c.post("/triggers/optional", content=BODY, headers={"content-type": "application/json"})
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []


def test_optional_signature_path_is_not_registered_public(monkeypatch, started):
    _client(monkeypatch, started, auth_on=True)
    assert not auth._is_public_path("/triggers/optional", "POST")
    assert auth._is_public_path("/triggers/signed", "POST")


# ─── the legitimate paths keep working ────────────────────────────────


def test_signed_webhook_still_starts_a_run_without_a_token(monkeypatch, started):
    c = _client(monkeypatch, started, auth_on=True)
    r = c.post("/triggers/signed", content=BODY, headers=_sig())
    assert r.status_code == 200, r.text
    assert r.json()["matched"] == 1 and len(started.calls) == 1


def test_signed_webhook_with_a_bad_signature_is_still_refused(monkeypatch, started):
    c = _client(monkeypatch, started, auth_on=True)
    r = c.post("/triggers/signed", content=BODY, headers={**_sig(), "X-Signature": "0" * 64})
    assert r.status_code == 401 and started.calls == []


def test_optional_signature_receiver_accepts_an_authenticated_caller(monkeypatch, started):
    c = _client(monkeypatch, started, auth_on=True)
    tok = auth.create_access_token({"sub": "admin"})
    r = c.post("/triggers/optional", content=BODY,
               headers={"Authorization": f"Bearer {tok}", "content-type": "application/json"})
    assert r.status_code == 200, r.text
    assert len(started.calls) == 1


def test_keyless_dev_receiver_is_unchanged(monkeypatch, started):
    c = _client(monkeypatch, started, auth_on=False)
    r = c.post("/triggers/optional", content=BODY, headers={"content-type": "application/json"})
    assert r.status_code == 200, r.text
    assert len(started.calls) == 1
