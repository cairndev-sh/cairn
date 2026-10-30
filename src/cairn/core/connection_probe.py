"""Authenticated connection health probe (connector-probe).

A :class:`~cairn.core.connections.Connection` names a provider (via the catalog)
plus resolved config + secret values. :func:`probe_connection` runs the cheapest
**authenticated** request that proves the credential actually works — a single
HTTP round-trip for the api_key/bearer/basic/http_no_auth shapes, driven by the
provider's :class:`~cairn.core.connections.ProbeSpec`, or a clearly-typed
structural handler (ssh_key / mtls / oauth2 / public_private_key) for the rest.

Design rules:

* **Best-effort — never raises.** Any failure (missing secret, DNS, timeout,
  4xx/5xx) returns ``ProbeResult(ok=False, …)`` with a human ``detail``.
* **Authenticated, not liveness.** We hit endpoints that 401 without a valid
  credential, so a 200 means the creds are good — not merely that the host is up
  (the Langfuse ``/api/public/health`` vs ``/api/public/projects`` distinction).
* **No false "connected".** Providers with no cheap authed check
  (``ProbeSpec.supported=False``: modal, jarvislabs, ray, opentelemetry, W&B,
  gcs/bigquery, snowflake) return ``ok=None`` with ``status=0`` and a documented
  reason — degraded/unsupported, never a green tick.

The prober takes an optional injected ``httpx.AsyncClient`` so tests drive it
with ``httpx.MockTransport`` (mirrors :mod:`cairn.core.gateway`)."""
from __future__ import annotations

import ipaddress
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx

from cairn.core.connections import (
    APPLY_BASIC,
    APPLY_HEADER,
    APPLY_QUERY,
    Connection,
    HealthState,
    ProbeSpec,
    ProviderSpec,
    PROVIDER_CATALOG,
    ResolvedConnection,
    default_connection_store,
    resolve_connection_obj,
    resolve_provider_key,
)

_TIMEOUT = 10.0


@dataclass
class ProbeResult:
    """The outcome of one probe.

    ``ok``: True (authenticated + reachable), False (failed / creds rejected),
    or None (unsupported/degraded — no cheap authed check exists). ``status`` is
    the HTTP status (or 0 for structural/degraded). ``detail`` is human text
    (never a secret). ``latency_ms`` is the round-trip time when measured."""

    ok: Optional[bool]
    status: int = 0
    detail: str = ""
    latency_ms: Optional[float] = None

    def to_health(self) -> HealthState:
        return HealthState(
            ok=self.ok,
            status=self.status,
            detail=self.detail,
            latency_ms=self.latency_ms,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )


def _spec_for(conn: Connection) -> Optional[ProviderSpec]:
    """Resolve the ProviderSpec for a connection.

    Prefers the connection name (``cairn connect add openai`` names it "openai"),
    then falls back to matching on ``kind`` — so a renamed connection still finds
    its provider when the name is a clean provider key/alias."""
    key = resolve_provider_key(conn.name)
    if key is not None:
        return PROVIDER_CATALOG[key]
    # fall back: unique provider for this kind (skips the generic multi-entry kinds)
    same = [s for s in PROVIDER_CATALOG.values() if s.kind == conn.kind]
    return same[0] if len(same) == 1 else None


def _base_url(spec: ProbeSpec, resolved: ResolvedConnection) -> str:
    """Resolve the probe's base URL: the literal ``base_url`` when set, else the
    config/secret value named by ``base_url_key`` (config first, then secrets so
    DSN-style keys like ``TRACKING_URI`` that live as secret refs still resolve)."""
    if spec.base_url:
        return spec.base_url.rstrip("/")
    key = spec.base_url_key
    if not key:
        return ""
    val = resolved.config.get(key) or resolved.secrets.get(key) or ""
    return str(val).rstrip("/")


def _apply_path_params(path: str, resolved: ResolvedConnection) -> str:
    """Fill ``{name}`` placeholders in a probe path from config (e.g. Sentry's
    ``/api/0/organizations/{org}/``). Missing keys are left as-is so the caller
    surfaces a clear 404/400 rather than a crash."""
    if "{" not in path:
        return path
    out = path
    for k, v in resolved.config.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _build_request(
    provider: ProviderSpec, resolved: ResolvedConnection
) -> tuple[httpx.Request, Optional[str]]:
    """Build the authenticated probe request from the ProviderSpec + resolved
    creds. Returns ``(request, error)``; ``error`` is a human reason the request
    couldn't be built (missing base URL / secret), else None."""
    spec = provider.probe
    assert spec is not None  # callers guard supported HTTP probes
    base = _base_url(spec, resolved)
    if not base:
        return _EMPTY_REQUEST, "no base URL configured (set the provider's url/host in config)"
    url = base + _apply_path_params(spec.path, resolved)

    headers: dict[str, str] = {}
    params: dict[str, str] = {}
    auth: Optional[tuple[str, str]] = None

    if spec.apply == APPLY_HEADER:
        val = resolved.secrets.get(spec.secret_key, "")
        if not val:
            return _EMPTY_REQUEST, f"secret {spec.secret_key} is unset"
        headers[spec.header_name] = f"{spec.prefix}{val}"
        # Anthropic pins an API version header alongside the key.
        if provider.provider == "anthropic":
            headers["anthropic-version"] = "2023-06-01"
    elif spec.apply == APPLY_QUERY:
        val = resolved.secrets.get(spec.secret_key, "")
        if not val:
            return _EMPTY_REQUEST, f"secret {spec.secret_key} is unset"
        params[spec.query_param] = val
    elif spec.apply == APPLY_BASIC:
        # username may live in config (jira email, clickhouse user) or secrets.
        user = (
            resolved.secrets.get(spec.secret_key)
            or resolved.config.get(spec.secret_key)
            or ""
        )
        pwd = ""
        if spec.basic_password_key:
            pwd = resolved.secrets.get(spec.basic_password_key) or resolved.config.get(
                spec.basic_password_key, ""
            )
        if not user and not pwd:
            return _EMPTY_REQUEST, "no credential to apply (Basic auth needs a key)"
        auth = (str(user), str(pwd))

    req = httpx.Request(spec.method, url, headers=headers, params=params, json=spec.json_body)
    if auth is not None:
        # httpx.Request doesn't take auth=; encode Basic ourselves.
        import base64  # stdlib, but only needed on the Basic path

        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.headers["Authorization"] = f"Basic {token}"
    return req, None


# A placeholder request returned alongside a build error (never actually sent).
_EMPTY_REQUEST = httpx.Request("GET", "http://invalid.local/")


# ─── SSRF guard (#1506) ────────────────────────────────────────────────
#
# The probe sends an authenticated request to a URL the USER supplies (a custom
# connection's `base_url` lands in `resolved.config`), from a host that carries
# an IAM instance role. Unguarded, that is a credential-reading primitive:
# point a connection at http://169.254.169.254/… and read the probe's reply.
#
# Two different rules, because the probe kinds have different legitimate uses:
#
#   HTTP  — talks to third-party vendor APIs. Loopback / link-local / reserved
#           is NEVER legitimate. RFC-1918 usually isn't either, but a
#           self-hoster with an internal gateway is a real case, so private
#           ranges are blocked by DEFAULT and re-openable with
#           CAIRN_PROBE_ALLOW_PRIVATE=1.
#   SSH / DSN — reaching a private host IS the point (your own GPU box, your
#           own Postgres). Only the cloud metadata addresses are blocked there,
#           since nothing legitimate speaks SSH or Postgres to them.

#: Cloud instance-metadata addresses. Blocked for every probe kind, always.
_METADATA_HOSTS = frozenset({
    "169.254.169.254",       # AWS / Azure / GCP / OpenStack IMDS
    "fd00:ec2::254",         # AWS IMDSv6
    "metadata.google.internal",
    "metadata.goog",
})


def _ip_literal(value: str) -> Optional[ipaddress._BaseAddress]:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


#: The metadata addresses as networks: all of IPv4 link-local (IMDS, and ECS task
#: metadata at 169.254.170.2) plus every address literal in ``_METADATA_HOSTS``.
_METADATA_NETS = (
    ipaddress.ip_network("169.254.0.0/16"),
    *(ipaddress.ip_network(ip) for ip in map(_ip_literal, sorted(_METADATA_HOSTS)) if ip),
)
_NAT64 = (ipaddress.ip_network("64:ff9b::/96"), ipaddress.ip_network("64:ff9b:1::/48"))


def _is_metadata_ip(ip: ipaddress._BaseAddress) -> bool:
    """``ip``, or the IPv4 address it embeds (IPv4-mapped, 6to4, Teredo client, NAT64,
    IPv4-compatible), is a metadata address."""
    if ip.version == 6:
        if ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        elif ip.sixtofour:
            ip = ip.sixtofour
        elif ip.teredo:
            ip = ip.teredo[1]
        elif any(ip in net for net in _NAT64) or (int(ip) >> 32 == 0 and int(ip) > 1):
            ip = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return any(ip in net for net in _METADATA_NETS)


_ALLOW_PRIVATE_ENV = "CAIRN_PROBE_ALLOW_PRIVATE"


def _allow_private() -> bool:
    return os.environ.get(_ALLOW_PRIVATE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _ip_block_reason(ip: ipaddress._BaseAddress, *, allow_private: bool) -> Optional[str]:
    """Why this address must not be probed, or None if it is fine."""
    if ip.is_loopback:
        return "loopback address"
    if ip.is_link_local:            # 169.254.0.0/16, fe80::/10 — IMDS lives here
        return "link-local address"
    if ip.is_multicast:
        return "multicast address"
    if ip.is_unspecified:
        return "unspecified address"
    if ip.is_reserved:
        return "reserved address"
    if getattr(ip, "is_site_local", False):   # fec0::/10
        return "site-local address"
    if ip.is_private and not allow_private:
        return "private address"
    return None


def _resolved_ips(host: str, port: int) -> tuple[list, Optional[str]]:
    """Every address `host` resolves to, or (``[]``, reason) when it resolves to
    nothing. An unresolvable host is NOT blocked — there is nothing to connect
    to, so no SSRF is possible, and the request will fail on its own with a
    clearer error. This also keeps the guard usable offline."""
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except (OSError, UnicodeError):  # UnicodeError: a name IDNA cannot encode (`a..b`)
        return [], "unresolvable"
    out = []
    for info in infos:
        try:
            out.append(ipaddress.ip_address(info[4][0]))
        except ValueError:  # pragma: no cover — getaddrinfo returned a non-IP
            continue
    return out, None


def probe_target_block_reason(
    url: str, *, allow_private: Optional[bool] = None, metadata_only: bool = False
) -> Optional[str]:
    """Why this target must not be probed, or None if it may be.

    Returns a string rather than raising: a probe never raises (module docstring),
    it reports. ``metadata_only`` applies the looser SSH/DSN rule.

    Known limitation, deliberately not papered over: this resolves the name and
    checks the answer, so a DNS-rebinding attacker who flips the record between
    this check and the connect can still slip through. Closing that needs
    connect-to-pinned-IP with the Host header preserved, which is a bigger
    change to how the client is built; the realistic attack this blocks is a
    direct IMDS/internal-host probe.
    """
    if allow_private is None:
        allow_private = _allow_private()
    try:
        parsed = urlparse(url if "://" in url else f"//{url}", scheme="https")
    except ValueError:
        return "unparseable URL"
    if parsed.scheme not in ("http", "https"):
        return f"scheme {parsed.scheme!r} is not allowed (http/https only)"
    host = (parsed.hostname or "").strip().rstrip(".")
    if not host:
        return "no host in URL"
    if host.lower() in _METADATA_HOSTS:
        return "cloud instance-metadata endpoint"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return _addr_block_reason(host, port, allow_private=allow_private, metadata_only=metadata_only)


def host_block_reason(host: str) -> Optional[str]:
    """Why this bare host must not be probed, or None: the SSH/DSN rule
    (``metadata_only``) of :func:`probe_target_block_reason`, for the kinds whose
    target is a host, not a URL (#77). It is not routed through URL parsing, where
    ``//host`` lost its hostname and an unbracketed IPv6 literal does not parse."""
    host = host.strip().rstrip(".").strip("[]").partition("%")[0]  # no IPv6 zone id
    if not host:
        return "no host"
    if host.lower() in _METADATA_HOSTS:
        return "cloud instance-metadata endpoint"
    return _addr_block_reason(host, None, allow_private=True, metadata_only=True)


def _addr_block_reason(
    host: str, port: Optional[int], *, allow_private: bool, metadata_only: bool
) -> Optional[str]:
    """The address rules shared by both guards, for a host that is not a metadata name."""
    try:  # an IP literal needs no resolution
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        if _is_metadata_ip(literal):
            return "cloud instance-metadata endpoint"
        if metadata_only:
            return None
        return _ip_block_reason(literal, allow_private=allow_private)

    ips, unresolvable = _resolved_ips(host, port)
    if unresolvable:
        return None  # nothing to connect to — let the request fail naturally
    for ip in ips:
        if _is_metadata_ip(ip):
            return "resolves to a cloud instance-metadata endpoint"
        if metadata_only:
            continue
        reason = _ip_block_reason(ip, allow_private=allow_private)
        if reason:
            return f"resolves to a {reason}"
    return None


async def _http_probe(
    provider: ProviderSpec,
    resolved: ResolvedConnection,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> ProbeResult:
    """Run the generic authenticated HTTP probe for the HTTP-shaped auth types."""
    req, err = _build_request(provider, resolved)
    if err is not None:
        return ProbeResult(ok=False, status=0, detail=err)

    spec = provider.probe
    assert spec is not None

    blocked = probe_target_block_reason(str(req.url))
    if blocked:
        return ProbeResult(ok=False, status=0, detail=f"blocked: {blocked}")

    owns = client is None
    # follow_redirects=False (#1506): a 3xx to 169.254.169.254 would otherwise
    # walk straight past the check above, which only ever sees the FIRST URL.
    # A redirect is now reported as its status instead of chased.
    cl = client or httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False)
    t0 = time.monotonic()
    try:
        resp = await cl.send(req)
    except httpx.HTTPError as exc:  # DNS, timeout, connect, TLS — all httpx errors
        return ProbeResult(ok=False, status=0, detail=f"{type(exc).__name__}: {exc}")
    except Exception as exc:  # noqa: BLE001 — best-effort: a probe never raises
        return ProbeResult(ok=False, status=0, detail=f"{type(exc).__name__}: {exc}")
    finally:
        if owns:
            await cl.aclose()
    latency = (time.monotonic() - t0) * 1000.0

    ok = resp.status_code in spec.ok_status
    if ok:
        detail = "authenticated"
    elif resp.status_code in (401, 403):
        detail = f"credential rejected (HTTP {resp.status_code})"
    else:
        detail = f"HTTP {resp.status_code}"
    return ProbeResult(ok=ok, status=resp.status_code, detail=detail, latency_ms=round(latency, 1))


def _degraded(provider: ProviderSpec) -> ProbeResult:
    """The result for a provider with no cheap authenticated liveness check."""
    reason = (provider.probe.unsupported_reason if provider.probe else "") or (
        "no cheap authenticated probe for this provider"
    )
    return ProbeResult(ok=None, status=0, detail=f"unsupported: {reason}")


async def _ssh_probe(resolved: ResolvedConnection) -> ProbeResult:
    """SSH key auth probe — open a connection + authenticate with the private key.

    Structural, not HTTP. Uses the (optional) ``asyncssh`` dep; if it's absent we
    return a clear NotImplemented-style degraded result rather than faking OK."""
    host = str(resolved.config.get("host") or "")
    if not host:
        return ProbeResult(ok=False, status=0, detail="no host configured")
    # Reaching a PRIVATE host over SSH is the point (#1506) — that is the GPU
    # box. Only the metadata endpoints are off-limits; nothing legitimate
    # speaks SSH to them.
    blocked = host_block_reason(host)
    if blocked:
        return ProbeResult(ok=False, status=0, detail=f"blocked: {blocked}")
    try:
        import asyncssh  # noqa: PLC0415 — optional dep (remote/ssh); degrade if absent
    except ImportError:
        return ProbeResult(
            ok=None, status=0,
            detail="unsupported: SSH probe needs the 'remote' extra (asyncssh) — install to enable open+auth check",
        )
    user = str(resolved.config.get("user") or "")
    port = int(str(resolved.config.get("port") or "22") or "22")
    key = resolved.secrets.get("PRIVATE_KEY") or None
    try:
        client_key = asyncssh.import_private_key(key) if key else None
        async with asyncssh.connect(
            host, port=port, username=user or None,
            client_keys=[client_key] if client_key else None,
            known_hosts=None,
        ):
            return ProbeResult(ok=True, status=0, detail=f"ssh auth ok ({user}@{host}:{port})")
    except Exception as exc:  # noqa: BLE001 — best-effort
        return ProbeResult(ok=False, status=0, detail=f"ssh: {type(exc).__name__}: {exc}")


def _dsn_hosts(dsn: str) -> list[str]:
    """Every host asyncpg dials from ``dsn``'s netloc, parsed as asyncpg parses it:
    after the first ``@``, comma-separated, ``[v6]:port`` or ``host:port``, unquoted."""
    netloc = urlparse(dsn).netloc
    spec = netloc.partition("@")[2] if "@" in netloc else netloc
    hosts = []
    for part in spec.split(",") if spec else ():
        part = part[1:].partition("]")[0] if part.startswith("[") else part.partition(":")[0]
        hosts.append(unquote(part))
    return [h for h in hosts if h]


async def _dsn_probe(resolved: ResolvedConnection, provider: ProviderSpec) -> ProbeResult:
    """Postgres/pgvector DSN probe — connect + ``SELECT 1``. Optional asyncpg;
    degrade cleanly when it's missing rather than a false OK."""
    spec = provider.probe
    dsn = ""
    if spec is not None:
        dsn = (
            resolved.secrets.get(spec.secret_key)
            or resolved.config.get(spec.base_url_key or spec.secret_key)
            or ""
        )
    dsn = dsn or resolved.config.get("dsn") or resolved.secrets.get("DSN") or ""
    if not dsn:
        return ProbeResult(ok=False, status=0, detail="no DSN configured")
    # A private Postgres IS the normal case (#1506) — metadata endpoints only.
    for _dsn_host in _dsn_hosts(dsn):
        blocked = host_block_reason(_dsn_host)
        if blocked:
            return ProbeResult(ok=False, status=0, detail=f"blocked: {blocked}")
    try:
        import asyncpg  # noqa: PLC0415 — optional dep (storage-postgres); degrade if absent
    except ImportError:
        return ProbeResult(
            ok=None, status=0,
            detail="unsupported: DSN probe needs asyncpg (storage-postgres extra) — install to enable SELECT 1",
        )
    try:
        c = await asyncpg.connect(dsn, timeout=_TIMEOUT)
        try:
            await c.fetchval("SELECT 1")
        finally:
            await c.close()
        return ProbeResult(ok=True, status=0, detail="SELECT 1 ok")
    except Exception as exc:  # noqa: BLE001 — best-effort
        return ProbeResult(ok=False, status=0, detail=f"dsn: {type(exc).__name__}: {exc}")


async def probe_connection(
    conn: Connection, *, client: Optional[httpx.AsyncClient] = None
) -> ProbeResult:
    """Run the authenticated probe for a connection. Best-effort: never raises;
    a failure returns ``ok=False`` with a human ``detail``.

    Resolves the connection's ProviderSpec + its live config/secrets, then
    dispatches by ``auth_type``. The injectable ``client`` (httpx MockTransport)
    keeps HTTP probes hermetic in tests."""
    if not conn.enabled:
        return ProbeResult(ok=False, status=0, detail="connection is disabled")

    provider = _spec_for(conn)
    if provider is None:
        return ProbeResult(
            ok=None, status=0,
            detail=f"unsupported: no catalog provider for connection kind={conn.kind!r}",
        )

    try:
        resolved = await resolve_connection_obj(conn)
    except Exception as exc:  # noqa: BLE001 — e.g. oauth broker lookup can raise; degrade
        return ProbeResult(ok=False, status=0, detail=f"resolve failed: {type(exc).__name__}: {exc}")
    if resolved is None:
        return ProbeResult(ok=False, status=0, detail="connection not found or disabled")

    return await _dispatch_probe(provider, resolved, client=client)


async def _dispatch_probe(
    provider: ProviderSpec, resolved: ResolvedConnection, *, client: Optional[httpx.AsyncClient] = None
) -> ProbeResult:
    """Run the right probe for a (provider spec, resolved creds) pair. Shared by
    :func:`probe_connection` (saved connection) and :func:`probe_credentials`
    (the console's test-before-save button — ephemeral, entered creds)."""
    auth = provider.auth_type

    # Structural (non-HTTP) auth types get dedicated handlers.
    if auth == "ssh_key":
        return await _ssh_probe(resolved)
    if auth == "oauth2":
        # A broker owns the token dance; resolve_connection already introspected
        # by handing back a fresh ACCESS_TOKEN. Presence == usable-for-now.
        tok = resolved.secrets.get("ACCESS_TOKEN")
        if tok:
            return ProbeResult(ok=True, status=0, detail="oauth token resolved (broker introspect)")
        return ProbeResult(ok=False, status=0, detail="oauth: no access token (authorize the connection)")
    if auth in ("mtls", "none"):
        return _degraded(provider)

    # DSN-shaped stores (postgres/pgvector) — Basic auth_type but a DB connect.
    if provider.provider in ("postgres", "pgvector"):
        return await _dsn_probe(resolved, provider)

    # public_private_key (S3/R2/GCS/BigQuery): SigV4/JWT signing — SDK territory.
    if auth == "public_private_key":
        return _degraded(provider)

    # HTTP-shaped: api_key / bearer_token / basic / http_no_auth.
    if provider.probe is None or not provider.probe.supported:
        return _degraded(provider)
    return await _http_probe(provider, resolved, client=client)


async def probe_credentials(
    provider_key: str,
    *,
    config: Optional[dict] = None,
    secret_values: Optional[dict] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> ProbeResult:
    """Test-before-save: probe ENTERED credentials for a provider without
    persisting anything (the console "Test connection" button). Builds an
    ephemeral :class:`ResolvedConnection` from the values the user typed — keyed
    by the provider's own secret keys, exactly as the create path stores them —
    and runs the same authenticated probe a saved connection would.

    Best-effort: never raises; an unknown provider degrades to ``ok=None``."""
    from cairn.core.connections import PROVIDER_CATALOG, resolve_provider_key

    key = resolve_provider_key(provider_key) or provider_key
    provider = PROVIDER_CATALOG.get(key)
    if provider is None:
        return ProbeResult(ok=None, status=0, detail=f"unknown provider {provider_key!r}")
    # Layer entered overrides on the provider's config defaults (base_url, host,
    # …) exactly as connection_from_provider does — the prober needs the base URL.
    entered = {str(k): v for k, v in (config or {}).items() if v not in (None, "")}
    cfg = {**dict(provider.config), **entered}
    secrets = {str(k): str(v).strip() for k, v in (secret_values or {}).items() if str(v or "").strip()}
    resolved = ResolvedConnection(name=key, kind=provider.kind, config=cfg, secrets=secrets)
    return await _dispatch_probe(provider, resolved, client=client)


async def test_connection(
    conn_id: str, *, store=None, client: Optional[httpx.AsyncClient] = None
) -> tuple[Optional[Connection], ProbeResult]:
    """Service entrypoint: load a connection by id, run the authenticated probe,
    **persist** the result on the connection's ``health`` field, and return both.

    Used by the ``cairn connect test`` CLI, the admin create/edit on-add hook,
    and (follow-up) the console "Test" button. Returns ``(None, result)`` when
    the connection id is unknown. Best-effort throughout — the probe never
    raises; a persist failure is swallowed so a probe result is always returned."""
    own_store = store is None
    store = store or default_connection_store()
    try:
        conn = await store.get(conn_id)
        if conn is None:
            return None, ProbeResult(ok=False, status=0, detail=f"no connection {conn_id!r}")
        result = await probe_connection(conn, client=client)
        try:
            conn = await store.update(conn_id, health=result.to_health())
        except Exception:  # noqa: BLE001 — a persist failure must not lose the probe result
            conn = conn.model_copy(update={"health": result.to_health()})
        return conn, result
    finally:
        if own_store:
            close = getattr(store, "close", None)
            if callable(close):
                close()


async def probe_and_attach(
    conn: Connection, *, client: Optional[httpx.AsyncClient] = None
) -> Connection:
    """Probe a connection in-hand and return a copy with ``health`` attached —
    used by the create/edit write hooks *before* the row is first persisted, so
    the stored record carries its initial health without a second round-trip."""
    result = await probe_connection(conn, client=client)
    return conn.model_copy(update={"health": result.to_health()})
