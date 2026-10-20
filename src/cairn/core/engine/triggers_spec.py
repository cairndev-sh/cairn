"""Parsed shape of a domain-pack trigger YAML.

A domain pack ships ``triggers/<source>.yaml`` files declaring *what
external event it wants to react to* — never *how* to receive or
verify the payload. The receiver code lives in operator packs
(per ``architecture_obs_agent_locked.md`` §6, trigger split):

  Domain pack:     declares intent (source, normalize mapping, example)
  Operator pack:   ships TriggerProvider (verify + normalize impl)
  Core router:     matches normalized events against domain triggers

This module owns the *intent* side — the loader that walks each pack's
``triggers/`` directory, parses each YAML into a strict
:class:`TriggerSpec`, and surfaces validation errors with file paths
attached for doctor display.

Phase 2.8 ships the spec + loader + composition validation. The
runtime side (FastAPI routes, signature verification, fan-out
execution) lands in Phase 2.9+ with per-source operator packs.

Closed schema: unknown top-level keys raise :class:`TriggerSpecError`.
The schema is permissive on the *values* of ``normalize`` (any string →
any JSONPath-lite expression) because the path grammar is enforced
at runtime by the projection engine, not at load time.

Moved here from ``cairn.core.triggers.spec`` (#2416, ADR-0120 D6): the spec is a
grammar, so it lives in the engine, below the HTTP layer; the
old path is a deprecated alias for one release.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


class TriggerSpecError(ValueError):
    """A trigger YAML failed to parse or violated the schema."""


# Closed set of top-level keys in a trigger YAML. New shapes (e.g.
# ``schedule``, ``queue``) extend this set with the corresponding
# parser branch.
_TRIGGER_TOP_KEYS = {
    "source",          # required: identifier matching a TriggerProvider
    "description",     # optional free-form
    "ingress",         # required: how the substrate accepts payloads
    "normalize",       # required: vendor → normalized event mapping
    "fan_out",         # optional: split one webhook into N events
    "match",           # optional: filter normalized events before running
    "run",             # optional: which template to start + input mapping
    "example",         # optional: path to a sample payload
}

# Closed set of keys inside the ``ingress`` sub-table. ``http`` is the
# only transport implemented; future ``cli`` / ``schedule`` / ``queue``
# transports extend this.
_INGRESS_KEYS = {"transport", "path", "method", "signature", "topic"}

_SIGNATURE_KEYS = {"header", "secret", "required", "scheme"}

# Vendor signature schemes (#309, ADR-0052 §7). ``hmac_sha256`` is the generic
# default (bare hex or ``prefix=<hex>`` of the raw body — GitHub style).
# ``slack_v0`` signs ``v0:{timestamp}:{body}`` with the X-Slack-Request-Timestamp
# header; ``pagerduty_v1`` sends one or more comma-separated ``v1=<hex>`` values.
_VALID_SIGNATURE_SCHEMES = {"hmac_sha256", "slack_v0", "pagerduty_v1"}

_FAN_OUT_KEYS = {"iterate"}

_RUN_KEYS = {"template", "input"}

#: ``bus`` (ADR-0131 D4): a declared ``kind: event`` trigger — the row fires when its
#: ``topic`` is published on the bus, never as an HTTP route.
_VALID_TRANSPORTS = {"http", "bus"}
_BUS_TOPIC = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*")
_VALID_METHODS = {"POST", "PUT"}

#: #206 — every HTTP receiver lives under this prefix. A receiver that requires a
#: signature answers without a bearer token, so one declared at a control-plane
#: path (``/runs``, ``/runs/<id>/cancel``) would hand that route its public pass.
#: The path is also a literal: no ``{param}`` (it would match paths the author never
#: named), no ``.``/``..`` segment, no empty segment, no trailing slash.
INGRESS_PATH_PREFIX = "/triggers/"
_INGRESS_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_~-][A-Za-z0-9._~-]*")


def ingress_path_problem(path: Any) -> str:
    """Why ``path`` cannot be a webhook receiver's ingress path, or ``""`` if it can.

    The ONE rule, read by the spec loader, the ``triggers:`` grammar and the HTTP
    mount, so a path cannot pass one and fail another."""
    p = path if isinstance(path, str) else ""
    rest = p[len(INGRESS_PATH_PREFIX):] if p.startswith(INGRESS_PATH_PREFIX) else ""
    if rest and all(_INGRESS_PATH_SEGMENT.fullmatch(seg) for seg in rest.split("/")):
        return ""
    return (
        f"ingress path {path!r} must be a literal path under {INGRESS_PATH_PREFIX} "
        f"(e.g. {INGRESS_PATH_PREFIX}my-source): each segment letters, digits or "
        f"`._~-`, no parameters, no empty or dot segments, no trailing slash. A "
        f"receiver anywhere else would sit on a control-plane route"
    )


@dataclass(frozen=True)
class SignatureSpec:
    """How payloads are signed by the upstream sender.

    ``required`` defaults True — most production webhooks sign. Some
    vendors (Prometheus Alertmanager) leave signing optional and rely
    on network-level controls; those YAMLs set ``required: false``.
    """

    header: str
    #: Secret reference, typically ``${env:NAME}`` or ``vault://path``.
    #: Resolution happens at runtime by the substrate's secret resolver,
    #: not at load time.
    secret: str
    required: bool = True
    #: Vendor signature scheme (#309) — one of ``_VALID_SIGNATURE_SCHEMES``.
    #: ``hmac_sha256`` (default) = HMAC-SHA256 hex of the raw body;
    #: ``slack_v0`` / ``pagerduty_v1`` implement those vendors' framing.
    scheme: str = "hmac_sha256"


@dataclass(frozen=True)
class IngressSpec:
    """How a payload arrives at the substrate."""

    transport: str    # one of _VALID_TRANSPORTS
    path: str         # e.g. "/triggers/pagerduty"; "" for the bus transport
    method: str = "POST"
    signature: Optional[SignatureSpec] = None
    #: ADR-0131 D4 — the bus topic a ``bus`` ingress fires on ("" for http).
    topic: str = ""


@dataclass(frozen=True)
class FanOutSpec:
    """One webhook → N events. ``iterate`` names a JSONPath-lite into
    the vendor payload pointing at a list (e.g. Alertmanager's
    ``alerts`` array)."""

    iterate: str


@dataclass(frozen=True)
class RunSpec:
    """What to run when a normalized event clears the ``match`` filter.

    ``template`` is a ``<pack>/<template>`` namespaced reference resolved
    the same way ``cairn run`` resolves a direct-template ref. ``input``
    maps each template input field to a ``$event.<field>`` reference into
    the *normalized* event dict (so ``service: $event.service`` pulls the
    normalized ``service`` the ``normalize`` mapping produced)."""

    template: str
    #: template-input field → ``$event.<path>`` reference (or a literal).
    input: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TriggerSpec:
    """One parsed trigger YAML, ready for routing + validation."""

    #: Source identifier — must match a registered :class:`TriggerProvider`.
    source: str
    ingress: IngressSpec
    #: vendor → normalized field mapping. Keys are normalized fields
    #: (event_type, severity, ...); values are JSONPath-lite into vendor
    #: payload.
    normalize: dict[str, str]
    description: str = ""
    fan_out: Optional[FanOutSpec] = None
    #: normalized-field → allowed-values filter. Empty = match everything.
    #: An event clears the filter only if every key's normalized value is
    #: in its allowed list. Values are compared as strings.
    match: dict[str, list[str]] = field(default_factory=dict)
    #: Which template to start + how to build its input. ``None`` means
    #: the trigger declares intent but isn't routable yet (the router
    #: skips it — doctor can surface that as a gap later).
    run: Optional[RunSpec] = None
    example: Optional[str] = None

    #: Originating file, recorded for doctor diagnostics.
    source_file: Optional[Path] = None


# ─── JSONPath-lite and the input mapping (moved from triggers/router.py) ──
#
# Pure, and shared by the HTTP receiver (``triggers/router.py``) and the bus
# subscriber for ``kind: event`` rows (``dispatch/event_triggers.py``, ADR-0131
# D5), which must not import the HTTP layer. Dotted keys into nested dicts, ``.``
# for the whole object; no wildcards, no indexing.


def resolve_path(obj: Any, path: str) -> Any:
    """Resolve a dotted ``a.b.c`` path into ``obj``. ``"."`` returns ``obj``
    whole. A missing key (or indexing into a non-dict) returns ``None``."""
    if path == "." or path == "":
        return obj
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def map_inputs(event: dict[str, Any], input_map: dict[str, str]) -> dict[str, Any]:
    """Build a run's input dict. ``$event`` → whole event; ``$event.<path>``
    → a resolved field; anything else is a literal.

    A ``$event.<path>`` reference that resolves to ``None`` (the event didn't
    carry that field) is **omitted** rather than injected as null — so the
    target's own input defaults apply and strict validation isn't tripped by a
    stray null."""
    out: dict[str, Any] = {}
    for field_, ref in input_map.items():
        if ref == "$event":
            out[field_] = event
        elif ref.startswith("$event."):
            val = resolve_path(event, ref[len("$event."):])
            if val is not None:
                out[field_] = val
        else:
            out[field_] = ref
    return out


# ─── parser ──────────────────────────────────────────────────────────


def load_trigger_specs(triggers_dir: Path) -> list[TriggerSpec]:
    """Walk ``triggers_dir`` and return every parsed trigger spec.

    Broken YAML logs a warning and skips — one bad trigger must not
    take the whole pack down at load time. Doctor surfaces the gap
    as a fail row separately.

    Returns an empty list when the directory doesn't exist (a pack
    that doesn't ship any triggers is well-formed).
    """
    out: list[TriggerSpec] = []
    if not triggers_dir.is_dir():
        return out

    for path in sorted(triggers_dir.glob("*.yaml")):
        try:
            spec = load_trigger_spec(path)
        except TriggerSpecError as e:
            logger.warning("trigger spec %s failed to load: %s", path, e)
            continue
        out.append(spec)
    return out


def load_trigger_spec(path: Path) -> TriggerSpec:
    """Parse one trigger YAML file. Strict — raises on schema violations."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise TriggerSpecError(f"{path}: invalid YAML — {e}") from e
    except OSError as e:
        raise TriggerSpecError(f"{path}: cannot read — {e}") from e
    return _parse_trigger_raw(raw, path)


def load_trigger_spec_text(text: str, *, name: str = "store") -> TriggerSpec:
    """Parse a trigger spec from YAML *text* (the DB-store path) through the same
    strict validation as the file loader. ``name`` only labels error messages /
    the synthetic ``source_file``."""
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise TriggerSpecError(f"<store:{name}>: invalid YAML — {e}") from e
    return _parse_trigger_raw(raw, Path(f"<store:{name}>"))


def _parse_trigger_raw(raw: Any, path: Path) -> TriggerSpec:
    """Validate an already-loaded trigger mapping. Shared by the file loader and
    the store loader; ``path`` is only used for error messages + ``source_file``."""
    if not isinstance(raw, dict):
        raise TriggerSpecError(
            f"{path}: top-level must be a mapping, got {type(raw).__name__}"
        )

    bad = sorted(set(raw.keys()) - _TRIGGER_TOP_KEYS)
    if bad:
        raise TriggerSpecError(
            f"{path}: unknown top-level key(s): {bad!r}. "
            f"Allowed: {sorted(_TRIGGER_TOP_KEYS)!r}."
        )

    source = _required_str(raw, "source", path)
    description = _optional_str(raw, "description", path, default="")
    example = _optional_str(raw, "example", path, default=None)

    ingress = _parse_ingress(raw.get("ingress"), path)

    normalize_raw = raw.get("normalize")
    if normalize_raw is None:
        raise TriggerSpecError(f"{path}: missing required key 'normalize'")
    if not isinstance(normalize_raw, dict):
        raise TriggerSpecError(f"{path}: 'normalize' must be a mapping")
    normalize: dict[str, str] = {}
    for k, v in normalize_raw.items():
        if not isinstance(v, str):
            raise TriggerSpecError(
                f"{path}: normalize.{k} must be a string (JSONPath-lite), "
                f"got {type(v).__name__}"
            )
        normalize[k] = v

    fan_out = _parse_fan_out(raw.get("fan_out"), path)
    match = _parse_match(raw.get("match"), path)
    run = _parse_run(raw.get("run"), path)

    return TriggerSpec(
        source=source,
        ingress=ingress,
        normalize=normalize,
        description=description,
        fan_out=fan_out,
        match=match,
        run=run,
        example=example,
        source_file=path,
    )


# ─── helpers ─────────────────────────────────────────────────────────


def _parse_ingress(raw: Any, path: Path) -> IngressSpec:
    if not isinstance(raw, dict):
        raise TriggerSpecError(
            f"{path}: 'ingress' is required and must be a mapping"
        )
    bad = sorted(set(raw.keys()) - _INGRESS_KEYS)
    if bad:
        raise TriggerSpecError(
            f"{path}: unknown key(s) in ingress: {bad!r}. "
            f"Allowed: {sorted(_INGRESS_KEYS)!r}."
        )
    transport = _required_str(raw, "transport", path, dotted="ingress.transport")
    if transport not in _VALID_TRANSPORTS:
        raise TriggerSpecError(
            f"{path}: ingress.transport must be one of "
            f"{sorted(_VALID_TRANSPORTS)!r}, got {transport!r}"
        )
    if transport == "bus":
        # ADR-0131 D4 — a bus ingress names a topic and nothing an HTTP route needs.
        stray = sorted({"path", "method", "signature"} & set(raw.keys()))
        if stray:
            raise TriggerSpecError(f"{path}: a bus ingress takes `topic`, not {stray!r}")
        topic = _required_str(raw, "topic", path, dotted="ingress.topic")
        if not _BUS_TOPIC.fullmatch(topic) or topic.startswith("run."):
            raise TriggerSpecError(
                f"{path}: ingress.topic {topic!r} must be a domain topic (letters, digits, "
                f"`._:-`); `run.*` is the engine's own lifecycle firehose")
        return IngressSpec(transport="bus", path="", method="", topic=topic)
    if "topic" in raw:
        raise TriggerSpecError(f"{path}: `topic` belongs to a bus ingress, not {transport!r}")
    ingress_path = _required_str(raw, "path", path, dotted="ingress.path")
    problem = ingress_path_problem(ingress_path)
    if problem:
        raise TriggerSpecError(f"{path}: {problem}")
    method = _optional_str(raw, "method", path, default="POST",
                           dotted="ingress.method")
    if method not in _VALID_METHODS:
        raise TriggerSpecError(
            f"{path}: ingress.method must be one of "
            f"{sorted(_VALID_METHODS)!r}, got {method!r}"
        )

    signature = _parse_signature(raw.get("signature"), path)

    return IngressSpec(
        transport=transport,
        path=ingress_path,
        method=method,
        signature=signature,
    )


def _parse_signature(raw: Any, path: Path) -> Optional[SignatureSpec]:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise TriggerSpecError(
            f"{path}: ingress.signature must be a mapping when present"
        )
    bad = sorted(set(raw.keys()) - _SIGNATURE_KEYS)
    if bad:
        raise TriggerSpecError(
            f"{path}: unknown key(s) in ingress.signature: {bad!r}. "
            f"Allowed: {sorted(_SIGNATURE_KEYS)!r}."
        )
    header = _required_str(raw, "header", path,
                           dotted="ingress.signature.header")
    secret = _required_str(raw, "secret", path,
                           dotted="ingress.signature.secret")
    required = raw.get("required", True)
    if not isinstance(required, bool):
        raise TriggerSpecError(
            f"{path}: ingress.signature.required must be a boolean"
        )
    scheme = raw.get("scheme", "hmac_sha256")
    if scheme not in _VALID_SIGNATURE_SCHEMES:
        raise TriggerSpecError(
            f"{path}: ingress.signature.scheme {scheme!r} is not one of "
            f"{sorted(_VALID_SIGNATURE_SCHEMES)!r}"
        )
    return SignatureSpec(header=header, secret=secret, required=required, scheme=scheme)


def _parse_fan_out(raw: Any, path: Path) -> Optional[FanOutSpec]:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise TriggerSpecError(f"{path}: fan_out must be a mapping when present")
    bad = sorted(set(raw.keys()) - _FAN_OUT_KEYS)
    if bad:
        raise TriggerSpecError(
            f"{path}: unknown key(s) in fan_out: {bad!r}. "
            f"Allowed: {sorted(_FAN_OUT_KEYS)!r}."
        )
    iterate = _required_str(raw, "iterate", path, dotted="fan_out.iterate")
    return FanOutSpec(iterate=iterate)


def _parse_match(raw: Any, path: Path) -> dict[str, list[str]]:
    """``match`` is ``{normalized_field: [allowed, ...]}``. A bare scalar
    value is sugar for a single-element list."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise TriggerSpecError(f"{path}: 'match' must be a mapping when present")
    out: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, list):
            out[str(k)] = [str(item) for item in v]
        elif isinstance(v, (str, int, float, bool)):
            out[str(k)] = [str(v)]
        else:
            raise TriggerSpecError(
                f"{path}: match.{k} must be a scalar or list of scalars, "
                f"got {type(v).__name__}"
            )
    return out


def _parse_run(raw: Any, path: Path) -> Optional["RunSpec"]:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise TriggerSpecError(f"{path}: 'run' must be a mapping when present")
    bad = sorted(set(raw.keys()) - _RUN_KEYS)
    if bad:
        raise TriggerSpecError(
            f"{path}: unknown key(s) in run: {bad!r}. "
            f"Allowed: {sorted(_RUN_KEYS)!r}."
        )
    template = _required_str(raw, "template", path, dotted="run.template")
    input_raw = raw.get("input")
    inp: dict[str, str] = {}
    if input_raw is not None:
        if not isinstance(input_raw, dict):
            raise TriggerSpecError(f"{path}: run.input must be a mapping")
        for k, v in input_raw.items():
            if not isinstance(v, str):
                raise TriggerSpecError(
                    f"{path}: run.input.{k} must be a string "
                    f"($event.<path> or literal), got {type(v).__name__}"
                )
            inp[str(k)] = v
    return RunSpec(template=template, input=inp)


def _required_str(
    tbl: dict[str, Any], key: str, path: Path, *, dotted: Optional[str] = None,
) -> str:
    label = dotted or key
    if key not in tbl:
        raise TriggerSpecError(f"{path}: missing required key {label!r}")
    val = tbl[key]
    if not isinstance(val, str) or not val:
        raise TriggerSpecError(f"{path}: {label!r} must be a non-empty string")
    return val


def _optional_str(
    tbl: dict[str, Any],
    key: str,
    path: Path,
    *,
    default: Optional[str],
    dotted: Optional[str] = None,
) -> Optional[str]:
    label = dotted or key
    if key not in tbl:
        return default
    val = tbl[key]
    if val is None:
        return default
    if not isinstance(val, str):
        raise TriggerSpecError(f"{path}: {label!r} must be a string")
    return val


__all__ = [
    "TriggerSpec",
    "IngressSpec",
    "SignatureSpec",
    "FanOutSpec",
    "RunSpec",
    "TriggerSpecError",
    "INGRESS_PATH_PREFIX",
    "ingress_path_problem",
    "load_trigger_specs",
    "load_trigger_spec",
]
