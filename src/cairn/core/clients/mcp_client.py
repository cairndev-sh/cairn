"""Core-side MCP client facade.

The *only* place in ``core/`` that reaches into ``services/mcp``. Today this
is a thin re-export over the in-process implementation; tomorrow the
implementation can move behind an MCP-wire client without touching any
caller in core.

Public surface:
  - ``tool_adapter`` — submodule with capability→LangChain-tool adapters
  - ``slack_post(text, channel=None)`` — fire-and-forget chat notify
  - ``is_configured(server)`` — check whether a named MCP server is reachable
  - ``mcp_server_spec(config, secrets, *, allow_stdio=False,
    allow_private_network=False)`` — kind=mcp Connection config → a
    MultiServerMCPClient connection spec (ValueError when not MCP-shaped, or
    when it would start a process or reach an internal address the caller
    did not allow)
  - ``no_redirect_http_client(headers, timeout, auth)`` — the
    ``httpx_client_factory`` a tenant (``allow_private_network=False``) spec
    carries: the SDK's client with redirects off
  - ``discover_tools(name, spec, timeout=…)`` — short-lived session →
    ``tools/list`` → read-only ``[{name, description, inputs, input_schema}]``
    (#355; the schema itself since ADR-0101 D5)
  - ``discover_tools_cached(key, …)`` — the same, memoized for
    ``TOOLS_CACHE_TTL`` seconds (ADR-0101 D5: the builder catalog reads
    discovery on every palette load)
  - ``tool_risk(config, tool)`` / ``tool_risk_map(config)`` — the
    per-connection ``read``/``write`` allowlist, unknown tools ``write``
  - ``leaf_error(exc)`` — human message from (possibly grouped) MCP failures
"""
from __future__ import annotations

import asyncio
import json
import shlex
import time
from typing import Any

from cairn.services.mcp import tool_adapter
from cairn.services.mcp.client import slack_post, is_configured

__all__ = [
    "tool_adapter",
    "slack_post",
    "is_configured",
    "mcp_server_spec",
    "no_redirect_http_client",
    "discover_tools",
    "discover_tools_cached",
    "reset_tools_cache",
    "tool_risk",
    "tool_risk_map",
    "TOOL_RISKS",
    "TOOL_RISK_DEFAULT",
    "TOOLS_CACHE_TTL",
    "leaf_error",
]


# ─── kind=mcp connection → server spec (#355) ────────────────────────────
#
# A kind=mcp Connection (cairn.core.connections) stores what the connect
# drawer collected: {"transport": stdio|streamable_http|sse|json, "command",
# "args", "url", ("raw_json")}. Normalize that — plus the connection's
# *resolved* secrets — into the transport spec MultiServerMCPClient takes
# (the same client cairn.services.mcp.client is built on).


def mcp_server_spec(
    config: dict[str, Any],
    secrets: dict[str, str] | None = None,
    *,
    allow_stdio: bool = False,
    allow_private_network: bool = False,
) -> dict[str, Any]:
    """Normalize a kind=mcp connection ``config`` (+ resolved ``secrets``) into
    a ``MultiServerMCPClient`` connection spec.

    Raises ``ValueError`` with a precise message when the config is not
    MCP-shaped — no ``url`` for an http/sse transport, no ``command`` for
    stdio, or an unparseable ``raw_json``.

    Fails closed on where the spec reaches: a ``stdio`` spec runs a command on
    this server, so it is built only with ``allow_stdio=True``; an http/sse
    ``url`` must pass the connection probe's address guard
    (:func:`cairn.core.connection_probe.probe_target_block_reason`: no loopback,
    link-local, cloud metadata or, unless ``CAIRN_PROBE_ALLOW_PRIVATE``, private
    address) unless ``allow_private_network=True``. Both are the deployment's
    call to make, not a tenant connection's.

    Without ``allow_private_network`` the http/sse spec also carries
    ``httpx_client_factory=``:func:`no_redirect_http_client`, so the client
    never follows a redirect past the check. Such a spec therefore holds a
    callable and is not JSON-serialisable: pass it to ``MultiServerMCPClient``
    (or :func:`discover_tools`) as is, rather than storing or logging it as
    JSON. With ``allow_private_network`` the spec is unchanged
    (the SDK's default client, which follows redirects): the deployment already
    trusts where it points. Known limits of the address check: it resolves the
    host once, at spec-build time, so it does not defend against DNS
    rebinding, and a host that does not resolve then passes it.
    ``CAIRN_PROBE_ALLOW_PRIVATE`` relaxes its private-address rule here too.
    """
    cfg = dict(config or {})
    live = {str(k): str(v) for k, v in (secrets or {}).items() if v}
    transport = str(cfg.get("transport") or "").strip().lower()

    # transport=json — the drawer's "paste config" mode: one server object,
    # optionally under an {"mcpServers": {...}} wrapper (Claude-style config).
    if transport == "json":
        raw = cfg.get("raw_json") or ""
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except Exception as e:  # noqa: BLE001 — bad paste → precise 400 upstream
            raise ValueError(f"invalid mcpServers JSON: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("mcpServers JSON must be an object")
        servers = parsed.get("mcpServers")
        if isinstance(servers, dict):
            if not servers:
                raise ValueError("mcpServers JSON declares no servers")
            parsed = next(iter(servers.values()))
            if not isinstance(parsed, dict):
                raise ValueError("mcpServers entries must be objects")
        cfg = dict(parsed)
        transport = str(cfg.get("transport") or "").strip().lower()

    url = str(cfg.get("url") or "").strip()
    command = str(cfg.get("command") or "").strip()
    if not transport:  # infer from shape when unset
        transport = "streamable_http" if url else ("stdio" if command else "")

    if transport in ("streamable_http", "http", "sse"):
        if not url:
            raise ValueError(f"transport={transport} needs a 'url' in the connection config")
        if not allow_private_network:
            from cairn.core.connection_probe import probe_target_block_reason  # lazy: httpx off import path

            blocked = probe_target_block_reason(url)
            if blocked:
                raise ValueError(f"MCP server url is not allowed: {blocked}")
        headers = {str(k): str(v) for k, v in (cfg.get("headers") or {}).items()}
        token = live.get("TOKEN") or live.get("BEARER_TOKEN") or live.get("API_KEY")
        if token and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {token}"
        spec: dict[str, Any] = {
            "transport": "sse" if transport == "sse" else "streamable_http",
            "url": url,
        }
        if headers:
            spec["headers"] = headers
        if not allow_private_network:
            # The address check above runs once; the MCP SDK's default client
            # follows redirects, so a checked URL could 30x somewhere the check
            # would refuse. A tenant spec's client never follows one.
            spec["httpx_client_factory"] = no_redirect_http_client
        return spec

    if transport == "stdio":
        if not allow_stdio:
            raise ValueError("transport=stdio runs a command on this server and is not allowed for this caller")
        if not command:
            raise ValueError("transport=stdio needs a 'command' in the connection config")
        args = cfg.get("args") or []
        if isinstance(args, str):
            args = shlex.split(args)
        env = {str(k): str(v) for k, v in (cfg.get("env") or {}).items()}
        env.update(live)  # logical secret keys ride as env vars (e.g. TOKEN)
        spec = {"transport": "stdio", "command": command, "args": [str(a) for a in args]}
        if env:
            spec["env"] = env
        return spec

    raise ValueError(
        "no MCP endpoint in the connection config — set a 'url' (streamable_http/sse) "
        "or a 'command' (stdio)"
    )


def no_redirect_http_client(
    headers: dict[str, str] | None = None,
    timeout: Any = None,
    auth: Any = None,
) -> Any:
    """``httpx_client_factory`` for a tenant MCP spec: the MCP SDK's own
    ``create_mcp_http_client`` (same defaults and signature —
    ``McpHttpClientFactory`` in ``mcp.shared._httpx_utils``) except that it
    never follows a redirect, so the address check in :func:`mcp_server_spec`
    is the only hop the client takes. A 30x surfaces as a failed request."""
    import httpx  # lazy: httpx off import path

    kwargs: dict[str, Any] = {
        "follow_redirects": False,
        # the SDK's MCP_DEFAULT_TIMEOUT / MCP_DEFAULT_SSE_READ_TIMEOUT (mcp 1.27);
        # a test pins this against create_mcp_http_client so drift shows up
        "timeout": timeout if timeout is not None else httpx.Timeout(30.0, read=300.0),
    }
    if headers is not None:
        kwargs["headers"] = headers
    if auth is not None:
        kwargs["auth"] = auth
    return httpx.AsyncClient(**kwargs)


def _first_line(text: Any, cap: int = 240) -> str:
    s = str(text or "").strip()
    return s.splitlines()[0][:cap] if s else ""


def _inputs_summary(schema: Any) -> str:
    """A short human summary of a tool's input schema: required params with
    types, plus an optional-count — e.g. ``path (string), +2 optional``."""
    if not isinstance(schema, dict):
        return ""
    props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
    required = [str(r) for r in (schema.get("required") or [])]
    if not props and not required:
        return "no inputs"

    def _typ(key: str) -> str:
        p = props.get(key)
        t = p.get("type") if isinstance(p, dict) else None
        return str(t) if t else "any"

    parts = [f"{k} ({_typ(k)})" for k in required]
    optional = [k for k in props if k not in required]
    if optional:
        parts.append(f"+{len(optional)} optional")
    return ", ".join(parts)


async def discover_tools(name: str, spec: dict[str, Any], *, timeout: float = 5.0) -> list[dict[str, Any]]:
    """Read-only tool discovery (#355): open a short-lived session against
    ``spec`` (from :func:`mcp_server_spec`), call ``tools/list`` (following
    pagination), hang up. Returns
    ``[{name, description, inputs, input_schema}]``.

    ``input_schema`` (ADR-0101 D5) is the tool's own ``inputSchema``, verbatim
    — the JSON Schema the builder generates the step's argument form from
    (the same trick the operator palette plays with an operator's pydantic
    schema). ``inputs`` stays the one-line human summary #355 shipped; both
    are cheap and they answer different questions.

    Time-boxed by ``timeout`` — raises ``TimeoutError`` on expiry; transport /
    handshake failures propagate for the caller to map (502 in the console API).
    """
    async def _list() -> list[Any]:
        # lazy: the langchain adapter stack is heavy — keep it off import paths
        from langchain_mcp_adapters.client import MultiServerMCPClient

        client = MultiServerMCPClient({name: spec})
        async with client.session(name) as session:
            out: list[Any] = []
            cursor: str | None = None
            while True:
                res = await session.list_tools(cursor=cursor)
                out.extend(res.tools)
                cursor = getattr(res, "nextCursor", None)
                if not cursor:
                    break
            return out

    tools = await asyncio.wait_for(_list(), timeout)
    out: list[dict[str, Any]] = []
    for t in tools:
        schema = getattr(t, "inputSchema", None)
        out.append({
            "name": t.name,
            "description": _first_line(getattr(t, "description", "")),
            "inputs": _inputs_summary(schema),
            "input_schema": schema if isinstance(schema, dict) else {},
        })
    return out


# ─── discovery cache (ADR-0101 D5) ───────────────────────────────────────
#
# The builder catalog resolves every enabled kind=mcp connection's tools on
# each palette load, so an uncached discovery would open a session per
# connection per keystroke-scale request. One short TTL, keyed by whatever
# the caller considers one server (the console passes account + connection
# name), keeps a stale entry bounded to a minute — long enough to make the
# catalog cheap, short enough that a tool added on the server shows up
# without a restart. `refresh=True` is the console's "re-probe now".

#: How long a discovered tool list is reused (seconds).
TOOLS_CACHE_TTL = 60.0

_tools_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


async def discover_tools_cached(
    key: str,
    name: str,
    spec: dict[str, Any],
    *,
    timeout: float = 5.0,
    ttl: float = TOOLS_CACHE_TTL,
    refresh: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    """:func:`discover_tools`, memoized under ``key`` for ``ttl`` seconds.

    Returns ``(tools, cached)`` — ``cached`` is what lets the console say
    whether it is looking at a live probe. A failure is NOT cached: the next
    caller retries rather than being told for a minute that a server which
    has since come back is unreachable.
    """
    hit = _tools_cache.get(key)
    if hit is not None and not refresh and hit[0] > time.monotonic():
        return hit[1], True
    tools = await discover_tools(name, spec, timeout=timeout)
    _tools_cache[key] = (time.monotonic() + max(0.0, ttl), tools)
    return tools, False


def reset_tools_cache() -> None:
    """Drop every memoized tool list (tests; a connection's config changing)."""
    _tools_cache.clear()


# ─── per-connection risk allowlist (ADR-0101 D5) ─────────────────────────
#
# Every MCP tool is `read` or `write`. The classification is a DEPLOYMENT
# decision, so it lives on the connection's own config (`tools: {<tool>:
# read|write}`), set through the existing connection update route — not on
# the pack, not in the pipeline record. `write` is the default for anything
# unlisted: a tool nobody has classified is treated as consequential, which
# is the fail-closed direction (an unclassified write tool that skipped the
# gate would be the expensive mistake).

#: The two risk classes a connection may pin a tool to.
TOOL_RISKS = ("read", "write")

#: What an unlisted tool is: fail-closed, so a new tool on the server gets a
#: gate until someone says otherwise.
TOOL_RISK_DEFAULT = "write"


def tool_risk_map(config: dict[str, Any] | None) -> dict[str, str]:
    """The connection's ``config["tools"]`` allowlist, normalized.

    Junk is dropped rather than raised on — the map is user-typed config, and
    an unparseable entry must not take the tool list down with it. A dropped
    entry simply falls back to :data:`TOOL_RISK_DEFAULT`.
    """
    raw = (config or {}).get("tools")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for tool, risk in raw.items():
        cls = str(risk or "").strip().lower()
        if str(tool).strip() and cls in TOOL_RISKS:
            out[str(tool).strip()] = cls
    return out


def tool_risk(config: dict[str, Any] | None, tool: str) -> str:
    """``read`` or ``write`` for ``tool`` on a connection with ``config``."""
    return tool_risk_map(config).get(tool, TOOL_RISK_DEFAULT)


def leaf_error(exc: BaseException) -> str:
    """Human-readable message from an MCP client failure — anyio task groups
    wrap the real cause in (nested) ExceptionGroups; unwrap to the first leaf."""
    while isinstance(exc, BaseExceptionGroup) and exc.exceptions:
        exc = exc.exceptions[0]
    return str(exc).strip() or type(exc).__name__
