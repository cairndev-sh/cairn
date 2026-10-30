"""cairn-client — the typed Python client for the cairn control plane (#1194).

Design (ADR-0085 D7):

- **Spec-anchored, not memory-anchored**: every typed method carries its
  ``(VERB, /path/template)`` in ``_SURFACE``; a repo test diffs that table
  against the committed ``console/openapi.json`` so client/contract drift
  fails CI, exactly like the console's freshness gate.
- **No cairn.core imports** — this package is the dependency boundary the
  CLI port (#1195) and ask port (#1196) build on. CI enforces the rule.
- **Thin by intent**: httpx + dicts. Full codegen (models for 164 operations)
  can replace the internals later without changing this surface.
"""
from __future__ import annotations

from typing import Any, Optional

import httpx

__all__ = ["CairnClient", "AsyncCairnClient", "CairnAPIError", "AskUnavailableError"]

#: (method, path-template) for every typed method — the contract table the
#: spec-anchoring test verifies. Keep in lockstep with the methods below.
_SURFACE: dict[str, tuple[str, str]] = {
    "healthz": ("get", "/healthz"),
    "login": ("post", "/token"),
    "runs": ("get", "/runs"),
    "run": ("get", "/runs/{run_id}"),
    "resume_run": ("post", "/runs/{run_id}/resume"),
    "cancel_run": ("post", "/runs/{run_id}/cancel"),
    "run_artifacts": ("get", "/runs/{run_id}/artifacts"),
    "run_events": ("get", "/runs/{run_id}/events"),
    # #1790 findings doors — added in PR #1795, which missed this table; the
    # contract test went red on main until the next PR's review caught it.
    "run_findings": ("get", "/runs/{run_id}/findings"),
    "pipeline_findings": ("get", "/pipelines/{pipeline_id}/findings"),
    "stored_triggers": ("get", "/triggers/stored"),
    "set_trigger_enabled": ("patch", "/triggers/stored/{trigger_id}"),
    "pending_approval": ("get", "/runs/{run_id}/pending-approval"),
    "trigger_manual": ("post", "/triggers/manual"),
    "packs": ("get", "/packs"),
    "run_template": ("post", "/packs/{pack}/{template}/run"),
    "preflight": ("post", "/packs/{pack}/{template}/preflight"),
    "pipelines": ("get", "/pipelines"),
    "installed_pipelines": ("get", "/pipelines/installed"),
    "pipeline": ("get", "/pipelines/{pipeline_id}"),
    # #1792 — the dry-run "how will each step be triggered" projection
    "explain_pipeline": ("get", "/pipelines/{pack}/{name}/explain"),
    "launch_pipeline": ("post", "/pipelines/launch"),
    # #1789 — named launch presets for a pipeline (ADR-0071 §6)
    "pipeline_presets": ("get", "/pipelines/presets"),
    "save_pipeline_preset": ("post", "/pipelines/presets"),
    "delete_pipeline_preset": ("delete", "/pipelines/presets"),
    "cancel_pipeline": ("post", "/pipelines/{pipeline_id}/cancel"),
    "advance_step": ("post", "/pipelines/{pipeline_id}/steps/{step_id}/advance"),
    "approve_step": ("post", "/pipelines/{pipeline_id}/steps/{step_id}/approve"),
    # ADR-0156 D4 — an attributable "no" at a pipeline gate
    "reject_step": ("post", "/pipelines/{pipeline_id}/steps/{step_id}/reject"),
    "ask": ("post", "/ask"),  # platform-only: the public server has no /ask (ADR-0123 D1)
    "installed_workflows": ("get", "/registry/installed-workflows"),
    # #1629 (ADR-0095) — production signal capture. `capture_trace` and
    # `capture_feedback` are the two calls an application makes; the rest is the
    # operator's side (read, stats, consent, export).
    "capture_trace": ("post", "/capture/events"),
    "capture_feedback": ("post", "/capture/events"),
    "capture_batch": ("post", "/capture/events"),
    "capture_events": ("get", "/capture/events"),
    "capture_stats": ("get", "/capture/stats"),
    "capture_consent": ("get", "/capture/consent"),
    "set_capture_consent": ("put", "/capture/consent"),
    "export_capture": ("post", "/capture/export"),
}


def _split_pipeline_ref(ref: str) -> tuple[str, str]:
    """``[pipeline:]<pack>/<name>`` → ``(pack, name)`` for the path-templated
    pipeline doors. A module function, not a method, so the sync and async
    twins split identically (the parity test compares signatures, which would
    not catch one of them mishandling the prefix)."""
    raw = (ref or "").strip()
    if raw.startswith("pipeline:"):
        raw = raw[len("pipeline:"):]
    pack, _, name = raw.partition("/")
    return pack, name


def _ws_header(workspace: Optional[str]) -> Optional[dict]:
    return {"X-Cairn-Workspace": workspace} if workspace else None


# The capture bodies are built by module functions, not methods, so the sync and
# async twins send byte-identical requests: the parity test compares signatures,
# which would not catch one of them quietly dropping `adapter_uri`.
def _capture_trace_body(trace_id, prompt, completion, model, adapter_uri, run_id,
                        workflow_ref, source, payload) -> dict:
    return {"kind": "trace", "trace_id": trace_id, "prompt": prompt,
            "completion": completion, "model": model, "adapter_uri": adapter_uri,
            "run_id": run_id, "workflow_ref": workflow_ref, "source": source,
            "payload": payload or {}}


def _capture_feedback_body(kind, trace_id, run_id, edited_text, score, score_scale,
                           note, model, adapter_uri, prompt, completion, source,
                           payload) -> dict:
    body: dict[str, Any] = {
        "kind": kind, "trace_id": trace_id, "run_id": run_id,
        "edited_text": edited_text, "score_scale": score_scale, "note": note,
        "model": model, "adapter_uri": adapter_uri, "prompt": prompt,
        "completion": completion, "source": source, "payload": payload or {},
    }
    if score is not None:
        body["score"] = score
    return body


def _capture_export_body(kinds, since, holdout_fraction) -> dict:
    body: dict[str, Any] = {}
    if kinds is not None:
        body["kinds"] = kinds
    if since is not None:
        body["since"] = since
    if holdout_fraction is not None:
        body["holdout_fraction"] = holdout_fraction
    return body


class CairnAPIError(RuntimeError):
    """A non-2xx response, carrying the status and the server's detail."""

    def __init__(self, status_code: int, detail: str, method: str, path: str) -> None:
        super().__init__(f"{method.upper()} {path} → {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class AskUnavailableError(CairnAPIError):
    """``POST /ask`` answered 404: Cairn Ask is part of cairn-platform, and this deployment
    does not serve it (ADR-0127 D7, #2573). A :class:`CairnAPIError`, so existing handlers
    still catch it."""

    def __init__(self, err: CairnAPIError) -> None:
        super().__init__(err.status_code, err.detail, "post", "/ask")
        self.args = ("Cairn Ask is part of cairn-platform; this deployment does not serve "
                     "POST /ask (the public engine never does)",)


def _ask_or_explain(err: CairnAPIError) -> CairnAPIError:
    return AskUnavailableError(err) if err.status_code == 404 else err


class CairnClient:
    """A synchronous client for one cairn deployment.

    ``token`` may be passed directly, obtained via :meth:`login`, or omitted
    entirely against an auth-disabled dev engine."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: Optional[float] = 30.0,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        """``transport`` injects a custom httpx transport — the seam that lets
        an engine-side factory hand in an in-process transport (#1196) without
        this package ever importing cairn. Mock transports for tests ride the
        same door."""
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._http = httpx.Client(
            base_url=self.base_url, timeout=timeout, transport=transport
        )

    # ── plumbing ─────────────────────────────────────────────────────────
    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """The generic door — any route, same auth/error handling as the
        typed methods. Returns the parsed JSON body (``None`` for empty)."""
        headers = dict(kwargs.pop("headers", {}) or {})
        if self.token:
            headers.setdefault("Authorization", f"Bearer {self.token}")
        r = self._http.request(method, path, headers=headers, **kwargs)
        if r.status_code >= 400:
            try:
                detail = str(r.json().get("detail", r.text))
            except Exception:  # noqa: BLE001 — a non-JSON error body is still an error
                detail = r.text
            raise CairnAPIError(r.status_code, detail, method, path)
        if not r.content:
            return None
        return r.json()

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "CairnClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ── auth ─────────────────────────────────────────────────────────────
    def login(self, username: str, password: str) -> str:
        """POST /token (form-encoded, the FastAPI OAuth2 shape) and hold the JWT."""
        out = self.request("post", "/token", data={"username": username, "password": password})
        self.token = out["access_token"]
        return self.token

    # ── health ───────────────────────────────────────────────────────────
    def healthz(self) -> dict:
        return self.request("get", "/healthz")

    # ── runs ─────────────────────────────────────────────────────────────
    def runs(self, *, status: Optional[str] = None, limit: Optional[int] = None) -> list[dict]:
        params = {k: v for k, v in {"status": status, "limit": limit}.items() if v is not None}
        out = self.request("get", "/runs", params=params)
        # the list rides under the legacy `investigations` key
        return out.get("investigations", out.get("runs", []))

    def run(self, run_id: str) -> dict:
        return self.request("get", f"/runs/{run_id}")

    def resume_run(
        self,
        run_id: str,
        *,
        decision: str,
        approver: str = "api-user",
        note: Optional[str] = None,
    ) -> dict:
        """Deliver a HITL decision (``'approve'`` / ``'reject'``). ``decision``
        is REQUIRED by the server (#628: no fail-open default — an omitted
        decision is a 422, never a silent approve)."""
        body: dict[str, Any] = {"decision": decision, "approver": approver}
        if note is not None:
            body["note"] = note
        return self.request("post", f"/runs/{run_id}/resume", json=body)

    def run_events(self, run_id: str, *, after_seq: int = 0) -> Any:
        return self.request("get", f"/runs/{run_id}/events", params={"after_seq": after_seq})

    def pending_approval(self, run_id: str) -> Any:
        return self.request("get", f"/runs/{run_id}/pending-approval")

    def cancel_run(self, run_id: str) -> dict:
        return self.request("post", f"/runs/{run_id}/cancel")

    def run_artifacts(self, run_id: str) -> Any:
        return self.request("get", f"/runs/{run_id}/artifacts")

    def run_findings(self, run_id: str) -> dict:
        """GET /runs/{id}/findings — the normalized output the gates read (#1790)."""
        return self.request("get", f"/runs/{run_id}/findings")

    # ── triggers ─────────────────────────────────────────────────────────
    def trigger_manual(self, body: dict) -> dict:
        """Submit a manual trigger event (the `cairn remote submit` door)."""
        return self.request("post", "/triggers/manual", json=body)

    # ── packs / templates ────────────────────────────────────────────────
    def packs(self) -> dict:
        return self.request("get", "/packs")

    def installed_workflows(self, *, workspace: Optional[str] = None) -> dict:
        """GET /registry/installed-workflows — the workspace's runnable set
        (ADR-0049: the DB is the runnable set)."""
        headers = {"X-Cairn-Workspace": workspace} if workspace else None
        return self.request("get", "/registry/installed-workflows", headers=headers)

    def run_template(self, pack: str, template: str, inputs: dict, **params: Any) -> dict:
        return self.request("post", f"/packs/{pack}/{template}/run", json=inputs, params=params or None)

    def preflight(self, pack: str, template: str, inputs: dict) -> dict:
        return self.request("post", f"/packs/{pack}/{template}/preflight", json=inputs)

    # ── pipelines ────────────────────────────────────────────────────────
    def pipelines(self, *, status: Optional[str] = None) -> dict:
        return self.request("get", "/pipelines", params={"status": status} if status else None)

    def installed_pipelines(self) -> Any:
        return self.request("get", "/pipelines/installed")

    def pipeline(self, pipeline_id: str) -> dict:
        return self.request("get", f"/pipelines/{pipeline_id}")

    # ── stored triggers (ADR-0160) ───────────────────────────────────────
    def stored_triggers(self) -> dict:
        """GET /triggers/stored — this workspace's trigger-store rows and their switch."""
        return self.request("get", "/triggers/stored")

    def set_trigger_enabled(self, trigger_id: str, enabled: bool) -> dict:
        """PATCH /triggers/stored/{id} — enable or pause one trigger-store row."""
        return self.request("patch", f"/triggers/stored/{trigger_id}", json={"enabled": enabled})

    def pipeline_findings(self, pipeline_id: str) -> dict:
        """GET /pipelines/{id}/findings — every non-pending step's findings,
        readable mid-run (#1790)."""
        return self.request("get", f"/pipelines/{pipeline_id}/findings")

    def explain_pipeline(
        self, ref: str, *, environment: Optional[str] = None,
        preset: Optional[str] = None,
    ) -> dict:
        """GET /pipelines/{pack}/{name}/explain — the dry-run projection of how
        every step will be triggered (#1792). ``ref`` is ``<pack>/<name>`` with
        or without the ``pipeline:`` prefix."""
        pack, name = _split_pipeline_ref(ref)
        params = {k: v for k, v in
                  {"environment": environment, "preset": preset}.items() if v}
        return self.request("get", f"/pipelines/{pack}/{name}/explain",
                            params=params or None)

    def launch_pipeline(
        self, ref: str, inputs: dict, *, environment: Optional[str] = None,
        expected_hash: Optional[str] = None, preset: Optional[str] = None,
    ) -> dict:
        body: dict[str, Any] = {"ref": ref, "inputs": inputs}
        if environment:
            body["environment"] = environment
        if expected_hash:
            # #1255 — pin the exact installed record; the server 409s on drift
            body["expected_hash"] = expected_hash
        if preset:
            # #1789 — the NAME travels; the server expands it. `inputs` is then
            # only what overrides the preset, per key.
            body["preset"] = preset
        return self.request("post", "/pipelines/launch", json=body)

    # ── pipeline launch presets (#1789) ──────────────────────────────────
    def pipeline_presets(self, ref: str) -> dict:
        return self.request("get", "/pipelines/presets", params={"ref": ref})

    def save_pipeline_preset(
        self, ref: str, name: str, inputs: dict, *, description: str = "",
    ) -> dict:
        body: dict[str, Any] = {"ref": ref, "name": name, "inputs": inputs}
        if description:
            body["description"] = description
        return self.request("post", "/pipelines/presets", json=body)

    def delete_pipeline_preset(self, ref: str, name: str) -> dict:
        return self.request("delete", "/pipelines/presets", params={"ref": ref, "name": name})

    def cancel_pipeline(self, pipeline_id: str) -> dict:
        return self.request("post", f"/pipelines/{pipeline_id}/cancel")

    def advance_step(
        self, pipeline_id: str, step_id: str, *, approver: Optional[str] = None
    ) -> dict:
        body = {"approver": approver} if approver else {}
        return self.request("post", f"/pipelines/{pipeline_id}/steps/{step_id}/advance", json=body)

    def approve_step(
        self, pipeline_id: str, step_id: str, *, approver: Optional[str] = None
    ) -> dict:
        body = {"approver": approver} if approver else {}
        return self.request("post", f"/pipelines/{pipeline_id}/steps/{step_id}/approve", json=body)

    def reject_step(
        self, pipeline_id: str, step_id: str, *, reason: str, approver: Optional[str] = None
    ) -> dict:
        """Reject a pipeline gate (ADR-0156): the pipeline ends failed with the step's
        verdict ``rejected``. ``reason`` is required; under auth the server records the
        token's identity and ignores ``approver``."""
        body: dict[str, Any] = {"reason": reason}
        if approver:
            body["approver"] = approver
        return self.request("post", f"/pipelines/{pipeline_id}/steps/{step_id}/reject", json=body)


    # ── ask ──────────────────────────────────────────────────────────────
    def ask(
        self,
        query: str,
        *,
        thread_id: Optional[str] = None,
        workspace: Optional[str] = None,
    ) -> dict:
        """POST /ask — the ONE governed read door for the ask agent (#621).
        ``workspace`` scopes the read via the X-Cairn-Workspace header.

        Platform-only: the public server answers 404 unless the platform's ask router is
        installed (ADR-0123 D1, #2427); that 404 is raised as :class:`AskUnavailableError`."""
        body: dict[str, Any] = {"query": query}
        if thread_id is not None:
            body["thread_id"] = thread_id
        headers = {"X-Cairn-Workspace": workspace} if workspace else None
        try:
            return self.request("post", "/ask", json=body, headers=headers)
        except CairnAPIError as e:
            raise _ask_or_explain(e) from e

    # ── capture (#1629) ──────────────────────────────────────────────────
    # The thin SDK helper the loop's fuel arrives through. `model` /
    # `adapter_uri` are named parameters rather than payload keys on purpose:
    # they say WHICH version a signal is evidence about, and without them cycle
    # N trains on complaints about cycle N−3 (ADR-0095, capture decision 1).
    def capture_trace(
        self, trace_id: str, *, prompt: str = "", completion: str = "",
        model: str = "", adapter_uri: str = "", run_id: str = "",
        workflow_ref: str = "", source: str = "sdk",
        payload: Optional[dict] = None, workspace: Optional[str] = None,
    ) -> dict:
        """Record what the agent did. Idempotent per ``trace_id``: the same
        trace seen again — including via a gateway/Langfuse tap — dedups."""
        return self.request("post", "/capture/events", json=_capture_trace_body(
            trace_id, prompt, completion, model, adapter_uri, run_id,
            workflow_ref, source, payload), headers=_ws_header(workspace))

    def capture_feedback(
        self, kind: str, *, trace_id: str = "", run_id: str = "",
        edited_text: str = "", score: Optional[float] = None, score_scale: str = "",
        note: str = "", model: str = "", adapter_uri: str = "", prompt: str = "",
        completion: str = "", source: str = "sdk", payload: Optional[dict] = None,
        workspace: Optional[str] = None,
    ) -> dict:
        """Record what the user did with it: ``accept | reject | edit | retry |
        score``. At least one of ``trace_id`` / ``run_id`` is required — the
        server 422s otherwise, because feedback about nothing trains nothing."""
        return self.request("post", "/capture/events", json=_capture_feedback_body(
            kind, trace_id, run_id, edited_text, score, score_scale, note, model,
            adapter_uri, prompt, completion, source, payload),
            headers=_ws_header(workspace))

    def capture_batch(self, events: list[dict], *, workspace: Optional[str] = None) -> dict:
        """Send many events in one POST (the server caps the batch at 500)."""
        return self.request("post", "/capture/events", json={"events": events},
                            headers=_ws_header(workspace))

    def capture_events(
        self, *, kind: Optional[str] = None, since: Optional[str] = None,
        limit: Optional[int] = None, workspace: Optional[str] = None,
    ) -> dict:
        params = {k: v for k, v in {"kind": kind, "since": since, "limit": limit}.items()
                  if v is not None}
        return self.request("get", "/capture/events", params=params or None,
                            headers=_ws_header(workspace))

    def capture_stats(self, *, workspace: Optional[str] = None) -> dict:
        """Counts by kind — the cheap "is there enough new signal?" read."""
        return self.request("get", "/capture/stats", headers=_ws_header(workspace))

    def capture_consent(self, *, workspace: Optional[str] = None) -> dict:
        return self.request("get", "/capture/consent", headers=_ws_header(workspace))

    def set_capture_consent(self, allow: bool, *, workspace: Optional[str] = None) -> dict:
        """Flip training consent for a workspace. Fail-closed by default: the
        export refuses with a 409 until this is true."""
        return self.request("put", "/capture/consent", json={"allow": bool(allow)},
                            headers=_ws_header(workspace))

    def export_capture(
        self, *, kinds: Optional[list[str]] = None, since: Optional[str] = None,
        holdout_fraction: Optional[float] = None, workspace: Optional[str] = None,
    ) -> dict:
        """Materialize the window as a governed dataset version — returns
        ``{ref: 'dataset:capture-signal@N', rows, holdout_rows, holdout_ref?}``."""
        return self.request("post", "/capture/export",
                            json=_capture_export_body(kinds, since, holdout_fraction),
                            headers=_ws_header(workspace))


class AsyncCairnClient:
    """The async twin of :class:`CairnClient` — same ``_SURFACE``, same error
    shape, for async consumers (the ask agent and REPL, #1196). A repo test
    enforces method parity with the sync client, so the two cannot drift.

    ``transport`` accepts an ``httpx.AsyncBaseTransport`` — the engine-side
    factory injects an in-process ASGI transport here for the single-binary,
    no-server path."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: Optional[float] = 30.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._http = httpx.AsyncClient(
            base_url=self.base_url, timeout=timeout, transport=transport
        )

    # ── plumbing ─────────────────────────────────────────────────────────
    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        if self.token:
            headers.setdefault("Authorization", f"Bearer {self.token}")
        r = await self._http.request(method, path, headers=headers, **kwargs)
        if r.status_code >= 400:
            try:
                detail = str(r.json().get("detail", r.text))
            except Exception:  # noqa: BLE001 — a non-JSON error body is still an error
                detail = r.text
            raise CairnAPIError(r.status_code, detail, method, path)
        if not r.content:
            return None
        return r.json()

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncCairnClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── auth ─────────────────────────────────────────────────────────────
    async def login(self, username: str, password: str) -> str:
        out = await self.request("post", "/token", data={"username": username, "password": password})
        self.token = out["access_token"]
        return self.token

    # ── health ───────────────────────────────────────────────────────────
    async def healthz(self) -> dict:
        return await self.request("get", "/healthz")

    # ── runs ─────────────────────────────────────────────────────────────
    async def runs(self, *, status: Optional[str] = None, limit: Optional[int] = None) -> list[dict]:
        params = {k: v for k, v in {"status": status, "limit": limit}.items() if v is not None}
        out = await self.request("get", "/runs", params=params)
        return out.get("investigations", out.get("runs", []))

    async def run(self, run_id: str) -> dict:
        return await self.request("get", f"/runs/{run_id}")

    async def resume_run(
        self,
        run_id: str,
        *,
        decision: str,
        approver: str = "api-user",
        note: Optional[str] = None,
    ) -> dict:
        body: dict[str, Any] = {"decision": decision, "approver": approver}
        if note is not None:
            body["note"] = note
        return await self.request("post", f"/runs/{run_id}/resume", json=body)

    async def run_events(self, run_id: str, *, after_seq: int = 0) -> Any:
        return await self.request("get", f"/runs/{run_id}/events", params={"after_seq": after_seq})

    async def pending_approval(self, run_id: str) -> Any:
        return await self.request("get", f"/runs/{run_id}/pending-approval")

    async def cancel_run(self, run_id: str) -> dict:
        return await self.request("post", f"/runs/{run_id}/cancel")

    async def run_artifacts(self, run_id: str) -> Any:
        return await self.request("get", f"/runs/{run_id}/artifacts")

    async def run_findings(self, run_id: str) -> dict:
        """GET /runs/{id}/findings — the normalized output the gates read (#1790)."""
        return await self.request("get", f"/runs/{run_id}/findings")

    # ── triggers ─────────────────────────────────────────────────────────
    async def trigger_manual(self, body: dict) -> dict:
        return await self.request("post", "/triggers/manual", json=body)

    # ── packs / templates ────────────────────────────────────────────────
    async def packs(self) -> dict:
        return await self.request("get", "/packs")

    async def installed_workflows(self, *, workspace: Optional[str] = None) -> dict:
        headers = {"X-Cairn-Workspace": workspace} if workspace else None
        return await self.request("get", "/registry/installed-workflows", headers=headers)

    async def run_template(self, pack: str, template: str, inputs: dict, **params: Any) -> dict:
        return await self.request(
            "post", f"/packs/{pack}/{template}/run", json=inputs, params=params or None
        )

    async def preflight(self, pack: str, template: str, inputs: dict) -> dict:
        return await self.request("post", f"/packs/{pack}/{template}/preflight", json=inputs)

    # ── pipelines ────────────────────────────────────────────────────────
    async def pipelines(self, *, status: Optional[str] = None) -> dict:
        return await self.request("get", "/pipelines", params={"status": status} if status else None)

    async def installed_pipelines(self) -> Any:
        return await self.request("get", "/pipelines/installed")

    async def pipeline(self, pipeline_id: str) -> dict:
        return await self.request("get", f"/pipelines/{pipeline_id}")

    # ── stored triggers (ADR-0160) ───────────────────────────────────────
    async def stored_triggers(self) -> dict:
        """GET /triggers/stored — see the sync twin."""
        return await self.request("get", "/triggers/stored")

    async def set_trigger_enabled(self, trigger_id: str, enabled: bool) -> dict:
        """PATCH /triggers/stored/{id} — see the sync twin."""
        return await self.request("patch", f"/triggers/stored/{trigger_id}", json={"enabled": enabled})

    async def pipeline_findings(self, pipeline_id: str) -> dict:
        """GET /pipelines/{id}/findings — every non-pending step's findings,
        readable mid-run (#1790)."""
        return await self.request("get", f"/pipelines/{pipeline_id}/findings")

    async def explain_pipeline(
        self, ref: str, *, environment: Optional[str] = None,
        preset: Optional[str] = None,
    ) -> dict:
        """GET /pipelines/{pack}/{name}/explain — see the sync twin (#1792)."""
        pack, name = _split_pipeline_ref(ref)
        params = {k: v for k, v in
                  {"environment": environment, "preset": preset}.items() if v}
        return await self.request("get", f"/pipelines/{pack}/{name}/explain",
                                  params=params or None)

    async def launch_pipeline(
        self, ref: str, inputs: dict, *, environment: Optional[str] = None,
        expected_hash: Optional[str] = None, preset: Optional[str] = None,
    ) -> dict:
        body: dict[str, Any] = {"ref": ref, "inputs": inputs}
        if environment:
            body["environment"] = environment
        if expected_hash:
            # #1255 — pin the exact installed record; the server 409s on drift
            body["expected_hash"] = expected_hash
        if preset:
            # #1789 — the NAME travels; the server expands it (see the sync twin)
            body["preset"] = preset
        return await self.request("post", "/pipelines/launch", json=body)

    # ── pipeline launch presets (#1789) ──────────────────────────────────
    async def pipeline_presets(self, ref: str) -> dict:
        return await self.request("get", "/pipelines/presets", params={"ref": ref})

    async def save_pipeline_preset(
        self, ref: str, name: str, inputs: dict, *, description: str = "",
    ) -> dict:
        body: dict[str, Any] = {"ref": ref, "name": name, "inputs": inputs}
        if description:
            body["description"] = description
        return await self.request("post", "/pipelines/presets", json=body)

    async def delete_pipeline_preset(self, ref: str, name: str) -> dict:
        return await self.request(
            "delete", "/pipelines/presets", params={"ref": ref, "name": name})

    async def cancel_pipeline(self, pipeline_id: str) -> dict:
        return await self.request("post", f"/pipelines/{pipeline_id}/cancel")

    async def advance_step(
        self, pipeline_id: str, step_id: str, *, approver: Optional[str] = None
    ) -> dict:
        body = {"approver": approver} if approver else {}
        return await self.request(
            "post", f"/pipelines/{pipeline_id}/steps/{step_id}/advance", json=body
        )

    async def approve_step(
        self, pipeline_id: str, step_id: str, *, approver: Optional[str] = None
    ) -> dict:
        body = {"approver": approver} if approver else {}
        return await self.request(
            "post", f"/pipelines/{pipeline_id}/steps/{step_id}/approve", json=body
        )

    async def reject_step(
        self, pipeline_id: str, step_id: str, *, reason: str, approver: Optional[str] = None
    ) -> dict:
        """See :meth:`CairnClient.reject_step`."""
        body: dict[str, Any] = {"reason": reason}
        if approver:
            body["approver"] = approver
        return await self.request(
            "post", f"/pipelines/{pipeline_id}/steps/{step_id}/reject", json=body
        )


    # ── ask ──────────────────────────────────────────────────────────────
    async def ask(
        self,
        query: str,
        *,
        thread_id: Optional[str] = None,
        workspace: Optional[str] = None,
    ) -> dict:
        """POST /ask (see :meth:`CairnClient.ask`). Platform-only: the public server answers
        404 unless the platform's ask router is installed (ADR-0123 D1, #2427)."""
        body: dict[str, Any] = {"query": query}
        if thread_id is not None:
            body["thread_id"] = thread_id
        headers = {"X-Cairn-Workspace": workspace} if workspace else None
        try:
            return await self.request("post", "/ask", json=body, headers=headers)
        except CairnAPIError as e:
            raise _ask_or_explain(e) from e

    # ── capture (#1629) ──────────────────────────────────────────────────
    async def capture_trace(
        self, trace_id: str, *, prompt: str = "", completion: str = "",
        model: str = "", adapter_uri: str = "", run_id: str = "",
        workflow_ref: str = "", source: str = "sdk",
        payload: Optional[dict] = None, workspace: Optional[str] = None,
    ) -> dict:
        return await self.request("post", "/capture/events", json=_capture_trace_body(
            trace_id, prompt, completion, model, adapter_uri, run_id,
            workflow_ref, source, payload), headers=_ws_header(workspace))

    async def capture_feedback(
        self, kind: str, *, trace_id: str = "", run_id: str = "",
        edited_text: str = "", score: Optional[float] = None, score_scale: str = "",
        note: str = "", model: str = "", adapter_uri: str = "", prompt: str = "",
        completion: str = "", source: str = "sdk", payload: Optional[dict] = None,
        workspace: Optional[str] = None,
    ) -> dict:
        return await self.request("post", "/capture/events", json=_capture_feedback_body(
            kind, trace_id, run_id, edited_text, score, score_scale, note, model,
            adapter_uri, prompt, completion, source, payload),
            headers=_ws_header(workspace))

    async def capture_batch(self, events: list[dict], *, workspace: Optional[str] = None) -> dict:
        return await self.request("post", "/capture/events", json={"events": events},
                                  headers=_ws_header(workspace))

    async def capture_events(
        self, *, kind: Optional[str] = None, since: Optional[str] = None,
        limit: Optional[int] = None, workspace: Optional[str] = None,
    ) -> dict:
        params = {k: v for k, v in {"kind": kind, "since": since, "limit": limit}.items()
                  if v is not None}
        return await self.request("get", "/capture/events", params=params or None,
                                  headers=_ws_header(workspace))

    async def capture_stats(self, *, workspace: Optional[str] = None) -> dict:
        return await self.request("get", "/capture/stats", headers=_ws_header(workspace))

    async def capture_consent(self, *, workspace: Optional[str] = None) -> dict:
        return await self.request("get", "/capture/consent", headers=_ws_header(workspace))

    async def set_capture_consent(self, allow: bool, *, workspace: Optional[str] = None) -> dict:
        return await self.request("put", "/capture/consent", json={"allow": bool(allow)},
                                  headers=_ws_header(workspace))

    async def export_capture(
        self, *, kinds: Optional[list[str]] = None, since: Optional[str] = None,
        holdout_fraction: Optional[float] = None, workspace: Optional[str] = None,
    ) -> dict:
        return await self.request("post", "/capture/export",
                                  json=_capture_export_body(kinds, since, holdout_fraction),
                                  headers=_ws_header(workspace))
