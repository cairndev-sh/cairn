"""Trigger router — the core seam between a verified external event and a
template run (endpoint architecture §10.3 / §18).

The flow this module owns:

    raw HTTP payload
      → provider.verify(headers, body)      (signature check)
      → provider.normalize(headers, body)   (→ list of normalized events)
      → for each event:
          event_matches(event, spec.match)  (filter)
          map_inputs(event, spec.run.input) (build template input)
          start a run of spec.run.template
      → one RouteResult per event

Two provider paths feed step 1+2:

  * **Vendor provider** — an operator pack ships a :class:`TriggerProvider`
    subclass registered in-process (the entry-point group was retired, ADR-0120).
    Used when a source needs bespoke signature schemes or payload parsing.
  * **Generic YAML provider** — :class:`GenericYamlProvider`, a builtin,
    spec-bound default that needs *no per-vendor Python*. It applies the
    domain pack's ``TriggerSpec.normalize`` JSONPath-lite mapping +
    ``fan_out`` and verifies an optional HMAC signature. This is what the
    pagerduty / prometheus demo runs on today — the trigger YAML drives
    everything.

The router never imports an operator implementation and never reaches an
external system itself — it resolves a template ref and hands off. With a
dispatcher wired (the server), that hand-off is the ONE launch door and the
flow becomes a one-leg pipeline (ADR-0105 D4, #2115); with none, it is the bare
in-process compile→run of :func:`start_template_run`.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import hashlib
import hmac
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from cairn.core.storage.trigger_inbox import TriggerInbox

from cairn.core.triggers.provider import (
    TriggerProvider,
    _ProviderRegistry,
    default_provider_registry,
)
from cairn.core.engine.triggers_spec import map_inputs, resolve_path  # noqa: F401 — moved to the engine (ADR-0131 D5); re-exported here
from cairn.core.engine.triggers_spec import (
    TriggerSpec,
    load_trigger_spec_text,
    load_trigger_specs,
)

logger = logging.getLogger("cairn.core.triggers.router")


def _user_trigger_overlay_dir() -> Path:
    """Layer ② — the user-overlay file dir (hand / git authored). Override with
    ``CAIRN_TRIGGERS_DIR``; defaults to ``~/.cairn/triggers``."""
    return Path(os.path.expanduser(getenv("CAIRN_TRIGGERS_DIR", "").strip() or "~/.cairn/triggers"))


def _store_trigger_specs() -> list[TriggerSpec]:
    """Layer ③ — console-authored triggers from the DB store, parsed through the
    same strict loader the file path uses. Best-effort: one bad stored spec is
    logged + skipped, never blanks the router."""
    return [spec for spec, _ws in _store_trigger_rows()]


def _store_trigger_rows() -> list[tuple[TriggerSpec, str]]:
    """:func:`_store_trigger_specs` with the workspace each row was created in —
    the workspace that declared the receiver, where its runs land (#209)."""
    try:
        from cairn.core.storage.trigger_store import default_trigger_store  # lazy: store pulls sqlite/env
    except Exception:
        return []
    store = default_trigger_store()
    try:
        stored = store.list_sync()
    except Exception:
        return []
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()
    out: list[tuple[TriggerSpec, str]] = []
    for st in stored:
        if not st.enabled:
            continue
        try:
            out.append((load_trigger_spec_text(st.yaml, name=st.name), st.workspace_id))
        except Exception as e:  # noqa: BLE001 — skip a broken stored trigger
            logger.warning("stored trigger %r failed to load: %s", st.name, e)
    return out


def _dedupe_specs_by_path(specs: list[TriggerSpec]) -> list[TriggerSpec]:
    """Collapse same-ingress-path specs keeping the LAST (overlay/store win over
    a pack default sharing the path)."""
    by_path: dict[str, TriggerSpec] = {}
    for s in specs:
        by_path[s.ingress.path] = s
    return list(by_path.values())


# ─── JSONPath-lite ───────────────────────────────────────────────────
#
# The ``normalize`` mapping and ``run.input`` references use a deliberately
# tiny path grammar: dotted keys into nested dicts, ``.`` for "the whole
# object". No wildcards, no indexing — fan-out handles the one list case.



def project(payload: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    """Apply a ``normalize`` mapping: ``{field: jsonpath}`` → ``{field: value}``."""
    return {field_: resolve_path(payload, expr) for field_, expr in mapping.items()}


def apply_fan_out(payload: dict[str, Any], iterate: Optional[str]) -> list[dict[str, Any]]:
    """Split one payload into N sub-payloads via the ``fan_out.iterate`` path.

    With no fan-out, the payload is a single event. With fan-out, the path
    must point at a list; each item becomes its own event payload. A
    non-list (or missing) target degrades to the whole payload so a
    misconfigured iterate path fails loud at match time, not here."""
    if not iterate:
        return [payload]
    target = resolve_path(payload, iterate)
    if isinstance(target, list):
        return [item if isinstance(item, dict) else {"value": item} for item in target]
    logger.warning("fan_out.iterate %r did not resolve to a list; not splitting", iterate)
    return [payload]


# ─── signature verification ──────────────────────────────────────────


_ENV_REF = re.compile(r"^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$|^env:([A-Za-z_][A-Za-z0-9_]*)$")


def resolve_secret(ref: str) -> Optional[str]:
    """Resolve a secret reference. ``${env:NAME}`` / ``env:NAME`` read the
    environment; anything else is treated as a literal secret value.

    An empty (or whitespace-only) value is NO secret and resolves to ``None``,
    exactly like an unset variable (#206): an HMAC keyed with ``""`` is one
    anyone can compute, so a required signature with an empty secret must refuse
    every request rather than verify a forgery."""
    m = _ENV_REF.match(ref or "")
    value = os.environ.get(m.group(1) or m.group(2)) if m else ref
    if not value or not value.strip():
        return None
    return value


def _header_get(headers: dict[str, str], name: str) -> Optional[str]:
    """Case-insensitive header lookup."""
    if name in headers:
        return headers[name]
    lname = name.lower()
    for k, v in headers.items():
        if k.lower() == lname:
            return v
    return None


def verify_signature(spec: TriggerSpec, headers: dict[str, str], body: bytes) -> bool:
    """Scheme-aware signature verification against ``ingress.signature`` (#309).

    No signature block → always valid. ``required: false`` with a missing
    header or unresolvable secret → valid (dev / unsigned senders). A
    present header is always checked when the secret resolves.

    Schemes: ``hmac_sha256`` (default) = HMAC-SHA256 hex of the raw body,
    bare or ``prefix=<hex>`` (GitHub style); ``slack_v0`` = Slack's
    ``v0:{timestamp}:{body}`` framing with X-Slack-Request-Timestamp;
    ``pagerduty_v1`` = one-or-more comma-separated ``v1=<hex>`` values."""
    sig = spec.ingress.signature
    if sig is None:
        return True
    provided = _header_get(headers, sig.header)
    secret = resolve_secret(sig.secret)
    if not sig.required and (provided is None or secret is None):
        return True
    if provided is None or secret is None:
        return False
    scheme = getattr(sig, "scheme", "hmac_sha256") or "hmac_sha256"

    if scheme == "slack_v0":
        # https://api.slack.com/authentication/verifying-requests-from-slack
        ts = _header_get(headers, "X-Slack-Request-Timestamp")
        if ts is None:
            return False
        base = b"v0:" + ts.encode("utf-8") + b":" + body
        expected = "v0=" + hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, provided)

    if scheme == "pagerduty_v1":
        # X-PagerDuty-Signature: "v1=<hex>[,v1=<hex>...]" — valid if ANY matches.
        expected = "v1=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return any(
            hmac.compare_digest(expected, candidate.strip())
            for candidate in provided.split(",")
        )

    # hmac_sha256 — accept "sha256=<hex>" (GitHub style) and bare hex.
    provided_hex = provided.split("=", 1)[1] if "=" in provided else provided
    expected_hex = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_hex, provided_hex)


# ─── generic YAML-driven provider ────────────────────────────────────


class GenericYamlProvider(TriggerProvider):
    """Spec-bound builtin provider. Verifies the optional HMAC signature and
    normalizes purely from the domain pack's ``TriggerSpec`` — no per-vendor
    Python. Bound to one spec at construction (so it can read ``normalize`` /
    ``fan_out`` / ``ingress.signature``); not entry-point discovered."""

    def __init__(self, spec: TriggerSpec) -> None:
        self.spec = spec
        self.source = spec.source
        self.signature_required = bool(
            spec.ingress.signature and spec.ingress.signature.required
        )

    def verify(self, headers: dict[str, str], body: bytes) -> bool:
        return verify_signature(self.spec, headers, body)

    def normalize(self, headers: dict[str, str], body: bytes) -> list[dict[str, Any]]:
        try:
            payload = json.loads(body) if body else {}
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"trigger {self.spec.source!r}: body is not JSON: {e}") from e
        if not isinstance(payload, dict):
            payload = {"value": payload}
        iterate = self.spec.fan_out.iterate if self.spec.fan_out else None
        return [project(sub, self.spec.normalize) for sub in apply_fan_out(payload, iterate)]


# ─── match + input mapping ───────────────────────────────────────────


def event_matches(event: dict[str, Any], match: dict[str, list[str]]) -> bool:
    """An event clears the filter iff every ``match`` key's normalized value
    (stringified) is in its allowed list. Empty filter matches everything."""
    for field_, allowed in match.items():
        if str(event.get(field_)) not in allowed:
            return False
    return True



# ─── run starter ─────────────────────────────────────────────────────


async def start_template_run(template_ref: str, input_data: dict[str, Any]) -> dict[str, Any]:
    """Compile a ``<pack>/<template>`` ref and run it IN THIS PROCESS — the
    fallback for an ingress with no dispatcher behind it, and the same bare
    compile→run the CLI's dev loop takes (run.py ``_run_template_async``, the
    ``--interactive``/``--mock`` path). It produces no run record and no
    pipeline; the governed door is :func:`cairn.core.dispatch.start.launch_ref`,
    which is what a wired router uses."""
    from cairn.core.engine.compiler import compile_template
    from cairn.core.engine.runner import run_within_budget
    from cairn.core.engine.skill_engine import resolve_template_path

    template_path = resolve_template_path(template_ref)
    compiled = compile_template(template_path)
    return await run_within_budget(compiled, input_data)


# ─── router ──────────────────────────────────────────────────────────


@dataclass
class RouteResult:
    """One normalized event's outcome through the router."""

    source: str
    matched: bool
    template: Optional[str] = None
    run_id: Optional[str] = None
    #: #2115 (ADR-0105 D4) — the pipeline the matched event started. A flow is
    #: started as its ONE-LEG pipeline, so the event now has two handles: the
    #: governed run (``run_id``, the leg) and the pipeline that owns it. Empty on
    #: the legacy in-process path and on a dispatcher with no bridge.
    pipeline_id: Optional[str] = None
    status: str = "skipped"        # skipped | done | failed
    error: Optional[str] = None
    output: Optional[dict[str, Any]] = None
    event: dict[str, Any] = field(default_factory=dict)


class TriggerRouter:
    """Holds the installed packs' trigger specs and routes verified events.

    Construct with an explicit spec list (tests) or via
    :meth:`from_installed`, which loads packs + vendor providers from
    entry-points. Index is by ``ingress.path`` (the HTTP route) and by
    ``source`` (the routing key), so the ingress can look a spec up either
    way."""

    def __init__(
        self,
        specs: list[TriggerSpec],
        *,
        provider_registry: Optional[_ProviderRegistry] = None,
        inbox: "Optional[TriggerInbox]" = None,
        run_template: Optional[Callable[..., Awaitable[Any]]] = None,
        run_environment: Optional[str] = None,
        workspaces: Optional[dict[str, str]] = None,
    ) -> None:
        # ADR-0131 D4 — a `bus` row (a declared `kind: event`) is never an HTTP route;
        # the bus subscriber (dispatch/event_triggers.py) fires it.
        self.specs = [s for s in specs if getattr(s.ingress, "transport", "http") != "bus"]
        self._by_path = {s.ingress.path: s for s in self.specs}
        self._by_source = {s.source: s for s in self.specs}
        self._providers = provider_registry or default_provider_registry
        # Durable ingress (opt-in, CAIRN_TRIGGER_DURABLE): a matched trigger is
        # persisted here and dispatched with retry/backoff by a drain loop,
        # instead of inline — so an inbound webhook survives a crash. See inbox.py.
        self.inbox = inbox
        # #687: when wired, a matched event runs through the RunDispatcher — so
        # the run is recorded in the run store and is first-class in /runs, audit
        # + replay. Without it (tests / bare loader) we fall back to an
        # in-process ``start_template_run``.
        #
        # #2115 (ADR-0105 D4): the server wires
        # :func:`cairn.core.dispatch.start.unattended_starter`, the ONE launch
        # door, NOT the bare ``dispatcher.run_template`` it used to pass — a
        # matched event starts a flow the way ``/start`` does, as its one-leg
        # pipeline. The parameter keeps its name and its ``(ref, inputs, *,
        # environment)`` shape, so this class is agnostic about which door it
        # holds and a test can still pass a plain stub.
        self._run_template = run_template
        self._run_environment = run_environment
        #: #209 — ingress path → the workspace that declared the receiver (a stored
        #: row's ``workspace_id``). A receiver's runs land THERE, never in a workspace
        #: the sender names. A pack- or overlay-file receiver has no row and no
        #: entry: it runs in the process default (``current_workspace()`` with no
        #: request override — ``CAIRN_WORKSPACE`` or ``default``).
        self._workspace_by_path = dict(workspaces or {})

    @classmethod
    def from_installed(
        cls,
        *,
        run_template: Optional[Callable[..., Awaitable[Any]]] = None,
        run_environment: Optional[str] = None,
    ) -> "TriggerRouter":
        """Collect trigger specs from every installed pack + load vendor
        providers registered in-process (no entry-point discovery, ADR-0120).

        ``run_template`` (the server passes the one launch door,
        :func:`cairn.core.dispatch.start.unattended_starter`) routes matched
        events through the RunDispatcher so webhook runs land in the run store
        (#687) as the flow's one-leg pipeline (#2115); ``run_environment`` is the
        profile those runs bind to."""
        from cairn.core.pack import ensure_loaded
        from cairn.core.pack.registry import get_packs

        ensure_loaded()
        # ① pack-bundled defaults, then ② the user-overlay dir, then ③ the
        # console DB store — last-wins by ingress path (store > overlay > pack).
        specs: list[TriggerSpec] = []
        for pack in get_packs():
            specs.extend(getattr(pack, "trigger_specs", []) or [])
        specs.extend(load_trigger_specs(_user_trigger_overlay_dir()))
        # #209 — a stored row knows the workspace it was created in; last-wins by
        # path like the specs themselves, so the workspace follows the winning spec.
        workspaces: dict[str, str] = {}
        for spec, ws in _store_trigger_rows():
            specs.append(spec)
            workspaces[spec.ingress.path] = ws
        specs = _dedupe_specs_by_path([s for s in specs if s.ingress.transport != "bus"])  # ADR-0131 D4
        router = cls(specs, run_template=run_template, run_environment=run_environment,
                     workspaces=workspaces)
        if getenv("CAIRN_TRIGGER_DURABLE", "").strip().lower() in ("1", "true", "yes", "on"):
            from cairn.core.storage.trigger_inbox import get_trigger_inbox
            router.inbox = get_trigger_inbox()
        return router

    def paths(self) -> list[str]:
        """Distinct ingress paths to mount on the HTTP app."""
        return sorted(self._by_path.keys())

    def spec_for_path(self, path: str) -> Optional[TriggerSpec]:
        return self._by_path.get(path)

    def workspace_for(self, spec: TriggerSpec) -> Optional[str]:
        """The workspace that declared ``spec``'s receiver, or ``None`` for a
        pack/overlay receiver (the process default applies) — #209."""
        return self._workspace_by_path.get(spec.ingress.path) or None

    def provider_for(self, spec: TriggerSpec) -> TriggerProvider:
        """A registered vendor provider for the source, else the generic
        YAML-driven one bound to this spec."""
        cls_ = self._providers.get(spec.source)
        if cls_ is not None:
            return cls_()
        return GenericYamlProvider(spec)

    async def route(
        self,
        spec: TriggerSpec,
        headers: dict[str, str],
        body: bytes,
    ) -> list[RouteResult]:
        """Verify, normalize, match, and start a run per matching event.

        Raises :class:`PermissionError` when verification fails (the ingress
        maps it to HTTP 401)."""
        provider = self.provider_for(spec)
        if not provider.verify(headers, body):
            raise PermissionError(f"signature verification failed for source {spec.source!r}")

        events = provider.normalize(headers, body)
        results: list[RouteResult] = []
        for event in events:
            if not event_matches(event, spec.match):
                results.append(RouteResult(source=spec.source, matched=False, event=event))
                continue
            if spec.run is None:
                logger.warning(
                    "trigger %r matched an event but declares no run target", spec.source,
                )
                results.append(RouteResult(source=spec.source, matched=True, event=event))
                continue
            run_id = uuid.uuid4().hex[:12]
            input_data = map_inputs(event, spec.run.input)
            res = RouteResult(
                source=spec.source,
                matched=True,
                template=spec.run.template,
                run_id=run_id,
                event=event,
            )
            if self.inbox is not None:
                # Durable path: persist + return; the drain loop dispatches with retry.
                rec = await self.inbox.enqueue(spec.source, spec.run.template, input_data)
                res.run_id = rec.id
                res.status = "queued"
            elif self._run_template is not None:
                # #687: dispatch through the RunDispatcher so the run is RECORDED
                # in the run store — first-class in /runs, audit + replay, not a
                # fire-and-forget uuid. Returns the handle immediately; the run
                # completes on the dispatcher lane (caller polls /runs/{id}).
                try:
                    run = await self._run_template(
                        spec.run.template, input_data, environment=self._run_environment,
                    )
                    res.run_id = getattr(run, "id", res.run_id)
                    res.status = getattr(run, "status", "queued")
                    # #2115 — the one door answers with both ids; a bare
                    # `Run` (no bridge behind the dispatcher) has neither the
                    # attribute nor a pipeline, and reports none.
                    res.pipeline_id = str(getattr(run, "pipeline_id", "") or "") or None
                except Exception as e:  # one bad event must not sink the batch
                    logger.exception("trigger %r run failed for event", spec.source)
                    res.status = "failed"
                    res.error = f"{type(e).__name__}: {e}"
            else:
                # Legacy in-process path (tests / no dispatcher wired): run to
                # completion and return the output inline.
                try:
                    res.output = await start_template_run(spec.run.template, input_data)
                    res.status = "done"
                except Exception as e:  # one bad event must not sink the batch
                    logger.exception("trigger %r run failed for event", spec.source)
                    res.status = "failed"
                    res.error = f"{type(e).__name__}: {e}"
            results.append(res)
        return results


__all__ = [
    "GenericYamlProvider",
    "RouteResult",
    "TriggerRouter",
    "apply_fan_out",
    "event_matches",
    "map_inputs",
    "project",
    "resolve_path",
    "resolve_secret",
    "start_template_run",
    "verify_signature",
]
