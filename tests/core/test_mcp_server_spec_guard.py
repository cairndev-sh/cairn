"""``mcp_server_spec`` fails closed on where a kind=mcp connection may reach.

A ``stdio`` spec starts a process on the server, so it is built only when the
caller passes ``allow_stdio=True`` (the deployment). An http/sse spec goes
through the same address guard as the connection probe (no loopback,
link-local, cloud metadata or private address) unless the caller passes
``allow_private_network=True``. A tenant http/sse spec also carries a client
factory that never follows a redirect, so the one address check is the only hop.
Hermetic: IP literals, a stubbed resolver and an in-memory httpx transport; no
network.
"""
from __future__ import annotations

import ipaddress
import json

import pytest

from cairn.core import connection_probe
from cairn.core.clients.mcp_client import discover_tools, mcp_server_spec, no_redirect_http_client


@pytest.fixture(autouse=True)
def _no_private_override(monkeypatch):
    monkeypatch.delenv("CAIRN_PROBE_ALLOW_PRIVATE", raising=False)


STDIO = {"transport": "stdio", "command": "some-mcp-server", "args": ["--flag"]}


def test_stdio_is_refused_by_default():
    with pytest.raises(ValueError, match="stdio"):
        mcp_server_spec(STDIO)


def test_stdio_inferred_from_a_command_is_refused():
    with pytest.raises(ValueError, match="stdio"):
        mcp_server_spec({"command": "some-mcp-server"})


def test_stdio_inside_pasted_json_is_refused():
    raw = json.dumps({"mcpServers": {"x": {"command": "some-mcp-server"}}})
    with pytest.raises(ValueError, match="stdio"):
        mcp_server_spec({"transport": "json", "raw_json": raw})


def test_stdio_is_built_when_the_deployment_allows_it():
    spec = mcp_server_spec(STDIO, {"TOKEN": "t"}, allow_stdio=True)
    assert spec == {"transport": "stdio", "command": "some-mcp-server", "args": ["--flag"], "env": {"TOKEN": "t"}}


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:8080/mcp",
    "http://169.254.169.254/latest/meta-data/",
    "http://metadata.google.internal/",
    "http://10.0.0.5/mcp",
    "http://[::1]/sse",
])
def test_http_to_an_internal_address_is_refused(url):
    with pytest.raises(ValueError, match="not allowed"):
        mcp_server_spec({"transport": "streamable_http", "url": url}, {"TOKEN": "t"})


def test_sse_to_a_name_resolving_inside_is_refused(monkeypatch):
    monkeypatch.setattr(connection_probe, "_resolved_ips", lambda host, port: ([ipaddress.ip_address("10.1.2.3")], None))
    with pytest.raises(ValueError, match="not allowed"):
        mcp_server_spec({"transport": "sse", "url": "https://mcp.internal.example/sse"})


def test_http_to_a_public_address_is_built():
    spec = mcp_server_spec({"transport": "streamable_http", "url": "https://93.184.215.14/mcp"}, {"TOKEN": "t"})
    assert spec == {"transport": "streamable_http", "url": "https://93.184.215.14/mcp",
                    "headers": {"Authorization": "Bearer t"},
                    "httpx_client_factory": no_redirect_http_client}


def test_the_deployment_may_reach_an_internal_server():
    spec = mcp_server_spec({"transport": "streamable_http", "url": "http://127.0.0.1:8080/mcp"},
                           allow_private_network=True)
    assert spec == {"transport": "streamable_http", "url": "http://127.0.0.1:8080/mcp"}


# ─── redirects: the address check is the only hop ────────────────────────

PUBLIC = "https://93.184.215.14/mcp"


@pytest.mark.parametrize("transport", ["streamable_http", "http", "sse"])
def test_a_tenant_http_spec_carries_the_no_redirect_factory(transport):
    spec = mcp_server_spec({"transport": transport, "url": PUBLIC})
    assert spec["httpx_client_factory"] is no_redirect_http_client


@pytest.mark.asyncio
async def test_the_factory_builds_the_sdk_client_without_redirects():
    httpx = pytest.importorskip("httpx")
    from mcp.shared._httpx_utils import create_mcp_http_client

    sdk = create_mcp_http_client(headers={"X": "1"})
    ours = no_redirect_http_client(headers={"X": "1"})
    async with sdk, ours:
        assert sdk.follow_redirects is True  # what the SDK default would do
        assert ours.follow_redirects is False
        assert ours.timeout == sdk.timeout  # same defaults otherwise
        assert ours.headers["X"] == "1"
    t = httpx.Timeout(5.0, read=7.0)
    async with no_redirect_http_client(timeout=t) as c:
        assert c.timeout == t


def test_the_deployment_spec_keeps_the_sdk_default_client():
    """Decision: with ``allow_private_network=True`` the deployment already
    trusts where the URL points, so the spec is unchanged (no factory; the
    SDK's default client)."""
    spec = mcp_server_spec({"transport": "sse", "url": PUBLIC}, allow_private_network=True)
    assert "httpx_client_factory" not in spec


@pytest.fixture
def redirecting_transport(monkeypatch):
    """Every client the factory builds talks to an in-memory server that
    answers the checked URL with a redirect to loopback; records each request."""
    httpx = pytest.importorskip("httpx")
    seen: list[str] = []

    def _handler(request):
        seen.append(str(request.url))
        if request.url.host == "93.184.215.14":
            return httpx.Response(307, headers={"Location": "http://127.0.0.1:9/mcp"})
        return httpx.Response(200, json={})

    real = httpx.AsyncClient

    class _Mocked(real):
        def __init__(self, *a, **kw):
            kw["transport"] = httpx.MockTransport(_handler)
            super().__init__(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", _Mocked)
    return seen


@pytest.mark.asyncio
async def test_a_redirect_to_loopback_is_not_followed(redirecting_transport):
    async with no_redirect_http_client() as client:
        resp = await client.get(PUBLIC)
    assert resp.status_code == 307
    assert redirecting_transport == [PUBLIC]


@pytest.mark.asyncio
async def test_discover_tools_uses_the_spec_factory(redirecting_transport):
    spec = mcp_server_spec({"transport": "streamable_http", "url": PUBLIC})
    with pytest.raises(BaseException):  # the handshake fails on the 307
        await discover_tools("x", spec, timeout=3.0)
    assert redirecting_transport, "discovery never reached the transport"
    assert all("127.0.0.1" not in u for u in redirecting_transport)
