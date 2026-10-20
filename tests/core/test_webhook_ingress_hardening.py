"""Webhook ingress: an empty secret, an unchecked path, a header-chosen workspace.

Three ways a signed webhook receiver could be more than a receiver:

* **An empty signing secret.** A receiver whose signature is required but whose secret
  env var is set to ``""`` verified against an HMAC keyed with the empty string —
  anyone can compute that — and was registered public. An empty secret is now unset:
  the receiver is not public and refuses every request.
* **An unchecked ingress path.** A declared ``path:`` was never validated, and a
  required-signature receiver registered its PATH public for every method. A receiver
  declared at a control-plane path made the real route at that path answer without a
  token. Ingress paths now live under ``/triggers/``, a path an app route already
  answers is refused at mount, and only the receiver's own (method, path) is public.
* **A header-chosen workspace.** On a public route the bearer guard authenticates
  nobody, so the workspace middleware took ``X-Cairn-Workspace`` unchecked: a sender
  holding a receiver's HMAC secret chose which workspace the run landed in. Public
  routes now ignore the header, and a receiver's runs land in the workspace that
  declared it. The guard also hands the middleware the claims it decoded instead of
  the raw token, so a token that expires between two decodes cannot skip the
  workspace check.

Driven through the real app (``build_app``) with auth on unless a test says otherwise.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.engine.runner import Runner
from cairn.core.engine.triggers_grammar import (
    DeclaredTriggerError,
    WebhookTrigger,
    parse_declared_triggers,
    webhook_spec_yaml,
)
from cairn.core.engine.triggers_spec import (
    IngressSpec,
    RunSpec,
    SignatureSpec,
    TriggerSpec,
    TriggerSpecError,
    load_trigger_spec_text,
)
from cairn.core.triggers import auth
from cairn.core.triggers.router import TriggerRouter, resolve_secret
from cairn.core.triggers.server import _mount_trigger_routes, build_app
from cairn.core.workspace import current_workspace

SECRET = "hook-secret"
BODY = json.dumps({"type": "push"}).encode()


def _spec_text(path: str, *, required: bool = True, method: str = "POST",
               secret_ref: str = "${env:HOOK_SECRET}") -> str:
    return (
        "source: custom\n"
        f"ingress: {{transport: http, path: '{path}', method: {method}, signature: "
        f"{{header: X-Signature, secret: '{secret_ref}', required: {str(required).lower()}}}}}\n"
        "normalize: {event_type: type}\n"
        "run: {template: pipeline:p/x}\n"
    )


def _spec(path: str, **kw):
    return load_trigger_spec_text(_spec_text(path, **kw), name=path)


def _raw_spec(path: str, *, method: str = "POST") -> TriggerSpec:
    """A spec built WITHOUT the loader — what a pack handing ``trigger_specs`` objects
    could do. The mount must hold the line on its own."""
    return TriggerSpec(
        source="custom",
        ingress=IngressSpec(transport="http", path=path, method=method,
                            signature=SignatureSpec(header="X-Signature",
                                                    secret="${env:HOOK_SECRET}", required=True)),
        normalize={"event_type": "type"},
        run=RunSpec(template="pipeline:p/x"),
    )


class _Started:
    """The launch door stub: records each start and the workspace it ran in."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, str]] = []

    async def __call__(self, ref, inputs, *, environment=None):
        self.calls.append((ref, dict(inputs), current_workspace()))
        return type("Run", (), {"id": f"run-{len(self.calls)}", "status": "queued"})()


@pytest.fixture
def started() -> _Started:
    return _Started()


def _env(monkeypatch, *, auth_on: bool, secret: str = SECRET) -> None:
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE"):
        monkeypatch.delenv(k, raising=False)
    if auth_on:
        monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    else:
        monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
        monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.setenv("HOOK_SECRET", secret)
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())  # module state: start clean


def _client(monkeypatch, started, specs, *, auth_on: bool = True, secret: str = SECRET,
            workspaces: dict | None = None) -> TestClient:
    _env(monkeypatch, auth_on=auth_on, secret=secret)
    app = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    kw = {"workspaces": workspaces} if workspaces is not None else {}
    _mount_trigger_routes(app, TriggerRouter(specs, run_template=started, **kw))
    return TestClient(app)


def _sig(secret: str = SECRET, body: bytes = BODY) -> dict[str, str]:
    return {"X-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
            "content-type": "application/json"}


def _bearer(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


# ═══ #206 (1) — an empty secret is no secret ═════════════════════════════


def test_empty_secret_env_resolves_to_unset(monkeypatch):
    monkeypatch.setenv("HOOK_SECRET", "")
    assert resolve_secret("${env:HOOK_SECRET}") is None
    monkeypatch.setenv("HOOK_SECRET", "   ")
    assert resolve_secret("env:HOOK_SECRET") is None
    assert resolve_secret("") is None
    monkeypatch.setenv("HOOK_SECRET", SECRET)
    assert resolve_secret("${env:HOOK_SECRET}") == SECRET


def test_empty_secret_receiver_refuses_a_signature_keyed_with_the_empty_string(monkeypatch, started):
    c = _client(monkeypatch, started, [_spec("/triggers/signed")], secret="")
    r = c.post("/triggers/signed", content=BODY, headers=_sig(secret=""))
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []


def test_empty_secret_receiver_is_not_public(monkeypatch, started):
    _client(monkeypatch, started, [_spec("/triggers/signed")], secret="")
    assert not auth._is_public_path("/triggers/signed", "POST")


def test_empty_secret_receiver_refuses_even_an_authenticated_caller(monkeypatch, started):
    """Fail closed: a token gets past the guard, the signature check still refuses."""
    c = _client(monkeypatch, started, [_spec("/triggers/signed")], secret="")
    tok = auth.create_access_token({"sub": "admin"})
    r = c.post("/triggers/signed", content=BODY, headers={**_sig(secret=""), **_bearer(tok)})
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []


def test_empty_secret_receiver_refuses_in_keyless_dev(monkeypatch, started):
    c = _client(monkeypatch, started, [_spec("/triggers/signed")], auth_on=False, secret="")
    r = c.post("/triggers/signed", content=BODY, headers=_sig(secret=""))
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []


def test_empty_secret_is_named_at_mount(monkeypatch, started, caplog):
    with caplog.at_level(logging.ERROR, logger="cairn.trigger_server"):
        _client(monkeypatch, started, [_spec("/triggers/signed")], secret="")
    assert any("/triggers/signed" in r.getMessage() and "secret" in r.getMessage()
               for r in caplog.records if r.levelno >= logging.ERROR), caplog.text


# ═══ #206 (2) — an ingress path lives under /triggers/ ═══════════════════


@pytest.mark.parametrize("path", [
    "/runs", "/runs/abc/cancel", "/token", "/workspaces/acme/runs", "/healthz",
    "/triggers", "/triggers/", "/triggers//x", "/triggers/../runs", "/triggers/./x",
    "/triggers/{run_id}", "/triggers/x?y=1", "/triggers/x/", "triggers/x", "/triggers/a b",
])
def test_ingress_path_outside_triggers_is_refused_by_the_loader(path):
    with pytest.raises(TriggerSpecError, match="/triggers/"):
        load_trigger_spec_text(_spec_text(path), name="t")


@pytest.mark.parametrize("path", ["/runs", "/runs/abc/cancel", "/triggers/../runs"])
def test_declared_webhook_path_outside_triggers_is_refused_at_parse(path):
    with pytest.raises(DeclaredTriggerError, match="/triggers/"):
        parse_declared_triggers([{"kind": "webhook", "source": "github", "path": path}])


@pytest.mark.parametrize("path", ["/runs", "/runs/abc/cancel"])
def test_a_control_plane_route_is_never_made_public_by_a_receiver(monkeypatch, started, path):
    """The original shape: a required-signature receiver declared AT a control-plane
    path. Built without the loader so only the mount stands in the way."""
    c = _client(monkeypatch, started, [_raw_spec(path)])
    assert not auth._is_public_path(path, "GET")
    assert not auth._is_public_path(path, "POST")
    assert c.get(path).status_code == 401
    assert c.post(path, content=BODY, headers=_sig()).status_code == 401
    assert started.calls == []


@pytest.mark.parametrize("path", ["/triggers/manual", "/triggers/approve/wf-1",
                                  "/triggers/inbox/replay", "/triggers/scheduled"])
def test_a_receiver_colliding_with_an_app_route_is_refused_at_mount(monkeypatch, started, path, caplog):
    """Inside the prefix too: a receiver at a path an app route already answers
    would hand that route the receiver's public pass."""
    with caplog.at_level(logging.ERROR, logger="cairn.trigger_server"):
        c = _client(monkeypatch, started, [_raw_spec(path)])
    assert not auth._is_public_path(path, "POST")
    r = c.post(path, content=BODY, headers=_sig())
    assert r.status_code == 401, (r.status_code, r.text)
    assert started.calls == []
    assert any(path in r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)


def test_build_time_receivers_are_checked_against_every_app_route(monkeypatch, started):
    """``build_app`` mounts the declared receivers; the inbox and the routers after it
    must already be there to be collided with."""
    _env(monkeypatch, auth_on=True)
    monkeypatch.setattr(TriggerRouter, "from_installed", classmethod(
        lambda cls, **kw: TriggerRouter([_raw_spec("/triggers/inbox/replay"),
                                         _spec("/triggers/signed")], run_template=started)))
    c = TestClient(build_app(Runner(), enable_scheduler=False))
    assert c.post("/triggers/inbox/replay", content=BODY, headers=_sig()).status_code == 401
    assert c.post("/triggers/signed", content=BODY, headers=_sig()).status_code == 200
    assert len(started.calls) == 1


def test_only_the_receivers_own_method_is_public(monkeypatch, started):
    c = _client(monkeypatch, started, [_spec("/triggers/signed")])
    assert auth._is_public_path("/triggers/signed", "POST")
    assert not auth._is_public_path("/triggers/signed", "GET")
    assert not auth._is_public_path("/triggers/signed", "PUT")
    assert not auth._is_public_path("/triggers/signed")
    # GET is not the receiver's method: the guard answers before the router does.
    assert c.get("/triggers/signed").status_code == 401


# ═══ #209 — a public route does not take its workspace from the caller ════


def test_signed_webhook_ignores_a_workspace_header(monkeypatch, started):
    """A pack receiver declares no workspace: its run lands in the default one,
    whatever the sender names."""
    c = _client(monkeypatch, started, [_spec("/triggers/signed")])
    r = c.post("/triggers/signed", content=BODY,
               headers={**_sig(), "X-Cairn-Workspace": "globex"})
    assert r.status_code == 200, r.text
    assert [ws for *_, ws in started.calls] == ["default"]


def test_signed_webhook_lands_in_the_workspace_that_declared_it(monkeypatch, started):
    c = _client(monkeypatch, started, [_spec("/triggers/signed")],
                workspaces={"/triggers/signed": "acme-ops"})
    r = c.post("/triggers/signed", content=BODY,
               headers={**_sig(), "X-Cairn-Workspace": "globex"})
    assert r.status_code == 200, r.text
    assert [ws for *_, ws in started.calls] == ["acme-ops"]


def test_stored_receivers_carry_their_workspace(monkeypatch):
    """Console- and pipeline-declared receivers live in the trigger store with the
    workspace they were created in; the router keeps it."""
    from cairn.core.storage.trigger_store import StoredTrigger
    from cairn.core.triggers import router as router_mod

    rows = [StoredTrigger(id="t1", workspace_id="acme-ops", name="t1", source="custom",
                          yaml=_spec_text("/triggers/acme"), enabled=True),
            StoredTrigger(id="t2", workspace_id="default", name="t2", source="custom",
                          yaml=_spec_text("/triggers/dflt"), enabled=True)]
    store = types.SimpleNamespace(list_sync=lambda *a, **k: rows, close=lambda: None)
    import cairn.core.storage.trigger_store as ts
    monkeypatch.setattr(ts, "default_trigger_store", lambda: store)
    monkeypatch.setattr(router_mod, "load_trigger_specs", lambda *_a, **_k: [])
    r = TriggerRouter.from_installed()
    assert r.workspace_for(r.spec_for_path("/triggers/acme")) == "acme-ops"
    assert r.workspace_for(r.spec_for_path("/triggers/dflt")) == "default"


# ─── two tenants: the JWT-gated receiver and the double decode ──────────

ACME_WS_RUN, GLOBEX_WS, GLOBEX_RUN = "run-acme-1", "globex", "run-globex-1"


def _seed_runs(path: str) -> None:
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.storage import Run, SqliteRunStore

    async def _write():
        s = SqliteRunStore(path)
        trig = TriggerEvent(source="manual", matched_pattern="p", service="svc")
        await s.create(Run(id=GLOBEX_RUN, trigger=trig, fingerprint=GLOBEX_RUN,
                           workspace_id=GLOBEX_WS))
        await s.append_event(GLOBEX_RUN, "running", {"secret": "globex-evidence"})
        await s.append_event(GLOBEX_RUN, "done", {})
        s.close()

    asyncio.run(_write())


@pytest.fixture()
def two_tenants(tmp_path, monkeypatch, started):
    _env(monkeypatch, auth_on=True)
    monkeypatch.setenv("OBS_SIGNUP", "open")
    monkeypatch.setenv("OBS_ACCOUNT_DB", str(tmp_path / "a.db"))
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setenv("OBS_WORKSPACE_DB", str(tmp_path / "w.db"))
    for k in ("OBS_API_USER", "OBS_API_PASSWORD", "OBS_STORAGE"):
        monkeypatch.delenv(k, raising=False)
    from cairn.core import account as _acct
    from cairn.core import workspace as _ws
    monkeypatch.setattr(_acct, "_store", _acct.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    _seed_runs(str(tmp_path / "r.db"))

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    app = build_app(FakeRunner(), enable_scheduler=False, enable_spec_routes=False)
    # a JWT-gated receiver (optional signature) declared in Globex's workspace
    _mount_trigger_routes(app, TriggerRouter(
        [_spec("/triggers/globex-hook", required=False)], run_template=started,
        workspaces={"/triggers/globex-hook": GLOBEX_WS}))
    c = TestClient(app)

    def token(company: str, email: str) -> str:
        c.post("/auth/signup", json={"company": company, "email": email, "password": "pw12345"})
        return c.post("/token", data={"username": email, "password": "pw12345"}).json()["access_token"]

    acme, globex = token("Acme Inc", "a@acme.com"), token("Globex", "b@globex.com")
    ws = {w["id"] for w in c.get("/workspaces", headers=_bearer(globex)).json()["workspaces"]}
    assert GLOBEX_WS in ws, ws
    return c, acme, globex


def test_jwt_gated_receiver_refuses_a_caller_outside_its_workspace(two_tenants, started):
    c, acme, _ = two_tenants
    r = c.post("/triggers/globex-hook", content=BODY,
               headers={**_bearer(acme), "content-type": "application/json"})
    assert r.status_code == 404, (r.status_code, r.text)
    assert started.calls == []


def test_jwt_gated_receiver_starts_for_a_member_in_its_workspace(two_tenants, started):
    c, acme, globex = two_tenants
    r = c.post("/triggers/globex-hook", content=BODY,
               headers={**_bearer(globex), "content-type": "application/json"})
    assert r.status_code == 200, (r.status_code, r.text)
    assert [ws for *_, ws in started.calls] == [GLOBEX_WS]


def test_a_token_expiring_between_decodes_does_not_skip_the_workspace_check(
        two_tenants, monkeypatch):
    """The guard accepts the token; any later decode sees it expired. The tenant check
    must judge the identity the guard accepted, not skip because a second decode failed."""
    c, acme, _ = two_tenants
    real = auth.decode_claims
    seen = {"n": 0}

    def decode_once(token):
        seen["n"] += 1
        return real(token) if seen["n"] == 1 else None

    monkeypatch.setattr(auth, "decode_claims", decode_once)
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream", headers=_bearer(acme))
    assert r.status_code == 404, (r.status_code, r.text)
    assert "globex-evidence" not in r.text


def test_guard_hands_the_middleware_claims_not_a_token(two_tenants):
    c, _, globex = two_tenants
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream", headers=_bearer(globex))
    assert r.status_code == 200, (r.status_code, r.text)
    assert "globex-evidence" in r.text


# ═══ the legitimate receivers keep working ═══════════════════════════════


def _preset_spec(source: str):
    text = webhook_spec_yaml(WebhookTrigger(kind="webhook", source=source),
                             workflow_ref="pipeline:p/x")
    return load_trigger_spec_text(text, name=source)


def _slack_headers(secret: str, body: bytes) -> dict[str, str]:
    ts = str(int(time.time()))
    sig = "v0=" + hmac.new(secret.encode(), b"v0:" + ts.encode() + b":" + body,
                           hashlib.sha256).hexdigest()
    return {"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": ts,
            "content-type": "application/json"}


def _pagerduty_headers(secret: str, body: bytes) -> dict[str, str]:
    sig = "v1=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {"X-PagerDuty-Signature": f"v1=deadbeef,{sig}", "content-type": "application/json"}


def _github_headers(secret: str, body: bytes) -> dict[str, str]:
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {"X-Hub-Signature-256": sig, "content-type": "application/json"}


@pytest.mark.parametrize("source,env,headers", [
    ("slack", "SLACK_SIGNING_SECRET", _slack_headers),
    ("pagerduty", "PAGERDUTY_WEBHOOK_SECRET", _pagerduty_headers),
    ("github", "GITHUB_WEBHOOK_SECRET", _github_headers),
])
def test_signed_vendor_presets_start_runs_without_a_token(monkeypatch, started, source, env, headers):
    monkeypatch.setenv(env, "a-real-vendor-secret")
    spec = _preset_spec(source)
    assert spec.ingress.path == f"/triggers/{source}"
    c = _client(monkeypatch, started, [spec])
    assert auth._is_public_path(spec.ingress.path, "POST")
    r = c.post(spec.ingress.path, content=BODY, headers=headers("a-real-vendor-secret", BODY))
    assert r.status_code == 200, (r.status_code, r.text)
    assert r.json()["matched"] == 1 and len(started.calls) == 1
    bad = c.post(spec.ingress.path, content=BODY, headers=headers("wrong-secret", BODY))
    assert bad.status_code == 401 and len(started.calls) == 1


def test_every_preset_path_is_a_valid_ingress_path():
    from cairn.core.engine.triggers_presets import PRESET_SOURCES

    for source in PRESET_SOURCES:
        assert _preset_spec(source).ingress.path.startswith("/triggers/")


def test_hello_cairn_webhook_declaration_still_validates():
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    doc = yaml.safe_load((root / "examples/hello-cairn/pipelines/profile-and-publish.yaml")
                         .read_text(encoding="utf-8"))
    hooks = [d for d in parse_declared_triggers(doc["triggers"]) if d.kind == "webhook"]
    assert [h.path for h in hooks] == ["/triggers/hello-sample"]
    spec = load_trigger_spec_text(webhook_spec_yaml(hooks[0], workflow_ref="pipeline:hello/x"))
    assert spec.ingress.path == "/triggers/hello-sample"


def test_keyless_dev_receiver_is_unchanged(monkeypatch, started):
    c = _client(monkeypatch, started, [_spec("/triggers/hello-sample", required=False)],
                auth_on=False)
    r = c.post("/triggers/hello-sample", content=BODY, headers={"content-type": "application/json"})
    assert r.status_code == 200, r.text
    assert [ws for *_, ws in started.calls] == ["default"]
