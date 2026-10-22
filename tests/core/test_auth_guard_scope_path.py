"""The bearer guard, the webhook signature check and the jobs router agree with routing.

Three places where an auth decision was made on something other than what the
request actually reaches:

* **The guard's path (#207, #214).** The guard matched its allowlists (public paths,
  the query-token route, the per-run worker routes) against ``request.url.path``.
  Starlette builds that from the Host header plus ``scope['path']`` and splits it
  with ``urlsplit``, so a decoded ``?`` or ``#`` in the path (sent as ``%3F`` /
  ``%23``), or a Host header older Starlette did not reject, made it differ from
  ``scope['path']``, which is what routing and the workspace middleware use. The
  guard now keys every decision on ``scope['path']``.
* **A non-ASCII signature (#214).** ``hmac.compare_digest`` raises ``TypeError`` on a
  non-ASCII ``str``, so such a header answered 500 instead of 401. It is compared
  as bytes now.
* **The jobs router's run scope (#208).** The guard treats a token as run-scoped when
  it carries ``scope=run-ingest`` OR a ``worker:<run>`` subject (``token_run_scope``);
  the jobs router looked at the subject only, so a ``run-ingest`` token with another
  subject was an operator there. The router now uses ``token_run_scope`` on the
  claims the guard authenticated.

Driven through the real app (``build_app``) with auth on.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import time

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.engine.runner import Runner
from cairn.core.engine.triggers_grammar import WebhookTrigger, webhook_spec_yaml
from cairn.core.engine.triggers_spec import load_trigger_spec_text
from cairn.core.operator.job_store import InMemoryRemoteJobStore, RemoteJobRecord
from cairn.core.operator.jobs import live_channel_env_for
from cairn.core.triggers import auth
from cairn.core.triggers.router import TriggerRouter
from cairn.core.triggers.routers import jobs as jobs_router
from cairn.core.triggers.server import _mount_trigger_routes, build_app
from cairn.core.workspace import current_workspace

SECRET = "hook-secret"
BODY = json.dumps({"type": "push"}).encode()


class _Started:
    """The launch-door stub: records each start."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, str]] = []

    async def __call__(self, ref, inputs, *, environment=None):
        self.calls.append((ref, dict(inputs), current_workspace()))
        return type("Run", (), {"id": f"run-{len(self.calls)}", "status": "queued"})()


def _env(monkeypatch) -> None:
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_JWT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("HOOK_SECRET", SECRET)
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())  # module state: start clean


def _spec(path: str):
    return load_trigger_spec_text(
        "source: custom\n"
        f"ingress: {{transport: http, path: '{path}', method: POST, signature: "
        "{header: X-Signature, secret: '${env:HOOK_SECRET}', required: true}}\n"
        "normalize: {event_type: type}\n"
        "run: {template: pipeline:p/x}\n", name=path)


def _sig(secret: str = SECRET, body: bytes = BODY) -> dict[str, str]:
    return {"X-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
            "content-type": "application/json"}


def _bearer(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture
def started() -> _Started:
    return _Started()


@pytest.fixture
def job_store(monkeypatch):
    store = InMemoryRemoteJobStore()
    monkeypatch.setattr(jobs_router, "get_remote_job_store", lambda: store)
    return store


@pytest.fixture
def app(monkeypatch, started, job_store):
    """The real app, auth on, with a signed receiver at ``/triggers/signed`` (public
    for POST) and, mounted AFTER it, a test route that only the guard protects (no
    ``require_auth``) whose parameter can hold a ``?``: the shape that turns a
    guard/router path disagreement into a bypass."""
    _env(monkeypatch)
    # the per-run worker token is minted for the http job channel
    monkeypatch.setenv("OBS_JOB_CHANNEL", "http")
    monkeypatch.setenv("CAIRN_ENGINE_URL", "http://engine.test")
    a = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    _mount_trigger_routes(a, TriggerRouter([_spec("/triggers/signed")], run_template=started))

    @a.post("/triggers/{name}")
    async def _protected(name: str) -> dict[str, str]:
        return {"served": name}

    return a


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


# ═══ #214 (1) / #207 — the guard decides on scope['path'] ═══════════════════


@pytest.mark.parametrize("encoded", ["%3F", "%23"])
def test_an_encoded_delimiter_does_not_borrow_a_public_paths_pass(client, encoded):
    """``/triggers/signed%3Fx`` routes to the protected ``/triggers/{name}`` (name =
    ``signed?x``); the guard must judge that path, not the ``/triggers/signed`` that
    ``request.url.path`` cut it down to."""
    r = client.post(f"/triggers/signed{encoded}x", content=BODY,
                    headers={"content-type": "application/json"})
    assert r.status_code == 401, (r.status_code, r.text)


def test_a_crafted_host_header_does_not_make_a_guarded_route_public(client, monkeypatch):
    """Starlette < 1.0.1 put any Host header into ``request.url`` (1.0.1 ignores a
    malformed one), so ``Host: x/healthz?`` made ``url.path`` read ``/healthz`` for a
    request routed elsewhere. Emulate that URL builder: the guard must not care."""
    import starlette.datastructures as sd

    monkeypatch.setattr(sd, "_HOST_RE", re.compile(r".*", re.DOTALL))
    r = client.post("/triggers/other", headers={"host": "x/healthz?"})
    assert r.status_code == 401, (r.status_code, r.text)


def test_the_locked_starlette_floor_has_the_host_check():
    """pyproject pins ``starlette>=1.0.1``, the first release that ignores a
    malformed Host when building ``request.url``."""
    import starlette.datastructures as sd

    assert hasattr(sd, "_HOST_RE")


# ═══ #214 (2) — a non-ASCII signature is a 401, not a 500 ══════════════════


def _preset_spec(source: str):
    text = webhook_spec_yaml(WebhookTrigger(kind="webhook", source=source),
                             workflow_ref="pipeline:p/x")
    return load_trigger_spec_text(text, name=source)


@pytest.mark.parametrize("source,env,header,extra", [
    ("github", "GITHUB_WEBHOOK_SECRET", "X-Hub-Signature-256", {}),
    ("slack", "SLACK_SIGNING_SECRET", "X-Slack-Signature",
     {"X-Slack-Request-Timestamp": str(int(time.time()))}),
    ("pagerduty", "PAGERDUTY_WEBHOOK_SECRET", "X-PagerDuty-Signature", {}),
])
def test_a_non_ascii_signature_is_refused_with_401(monkeypatch, started, source, env, header, extra):
    _env(monkeypatch)
    monkeypatch.setenv(env, "a-real-vendor-secret")
    spec = _preset_spec(source)
    a = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    _mount_trigger_routes(a, TriggerRouter([spec], run_template=started))
    c = TestClient(a, raise_server_exceptions=False)
    headers = {header: "sha256=café".encode("utf-8"), "content-type": "application/json",
               **extra}
    r = c.post(spec.ingress.path, content=BODY, headers=headers)
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []


# ═══ #208 — the jobs router scopes a run-ingest token like the guard does ═══


def _register(job_store, job_id: str, run_id: str) -> None:
    asyncio.run(job_store.register(RemoteJobRecord(id=job_id, run_id=run_id, node_name="train")))


def test_a_run_ingest_token_with_another_subject_cannot_heartbeat_another_runs_job(
        client, job_store):
    _register(job_store, "run-x:train", "run-x")
    tok = auth.create_access_token({"sub": "alice", "scope": "run-ingest", "run_id": "run-y"})
    r = client.post("/jobs/run-x:train/heartbeat", json={"worker_id": "w1"}, headers=_bearer(tok))
    assert r.status_code == 403, (r.status_code, r.text)


def test_a_run_ingest_token_with_another_subject_cannot_claim_work(client):
    tok = auth.create_access_token({"sub": "alice", "scope": "run-ingest", "run_id": "run-y"})
    r = client.post("/jobs/claim", json={"worker_id": "w1"}, headers=_bearer(tok))
    assert r.status_code == 403, (r.status_code, r.text)


# ═══ the legitimate paths keep working ═════════════════════════════════════


def test_healthz_stays_public(client):
    assert client.get("/healthz").status_code == 200


def test_a_protected_route_still_answers_a_token(client):
    tok = auth.create_access_token({"sub": "admin"})
    r = client.post("/triggers/other", headers=_bearer(tok))
    assert r.status_code == 200 and r.json() == {"served": "other"}, r.text


def test_sse_query_token_still_authenticates_the_stream(client):
    tok = auth.create_access_token({"sub": "alice"})
    # 404 (no such run) is the route answering: the guard let the query token through.
    assert client.get(f"/runs/no-such-run/stream?access_token={tok}").status_code == 404
    assert client.get(f"/workspaces/default/runs/no-such-run/stream?access_token={tok}").status_code == 404
    assert client.get("/runs/no-such-run/stream").status_code == 401


def test_a_correctly_signed_webhook_still_starts_a_run(client, started):
    r = client.post("/triggers/signed", content=BODY, headers=_sig())
    assert r.status_code == 200, (r.status_code, r.text)
    assert len(started.calls) == 1
    assert client.post("/triggers/signed", content=BODY, headers=_sig("wrong")).status_code == 401
    assert len(started.calls) == 1


def test_a_worker_still_heartbeats_its_own_job(client, job_store):
    _register(job_store, "run-x:train", "run-x")
    tok = live_channel_env_for("run-x", "train")["CAIRN_ENGINE_TOKEN"]
    r = client.post("/jobs/run-x:train/heartbeat", json={"worker_id": "w1"}, headers=_bearer(tok))
    assert r.status_code == 200, (r.status_code, r.text)


def test_a_run_ingest_token_heartbeats_its_own_runs_job(client, job_store):
    _register(job_store, "run-y:train", "run-y")
    tok = auth.create_access_token({"sub": "alice", "scope": "run-ingest", "run_id": "run-y"})
    r = client.post("/jobs/run-y:train/heartbeat", json={"worker_id": "w1"}, headers=_bearer(tok))
    assert r.status_code == 200, (r.status_code, r.text)


def test_a_worker_pool_token_still_heartbeats_any_job(client, job_store):
    """cairn-platform#224: a worker credential heartbeats any job; a user's token,
    which the old rule called an operator, no longer does."""
    _register(job_store, "run-x:train", "run-x")
    tok = auth.mint_worker_pool_token("gpu-a")
    r = client.post("/jobs/run-x:train/heartbeat", json={"worker_id": "w1"}, headers=_bearer(tok))
    assert r.status_code == 200, (r.status_code, r.text)
    user = auth.create_access_token({"sub": "alice"})
    r = client.post("/jobs/run-x:train/heartbeat", json={"worker_id": "w1"}, headers=_bearer(user))
    assert r.status_code == 403, (r.status_code, r.text)
