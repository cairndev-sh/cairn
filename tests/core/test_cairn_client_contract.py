"""cairn-client contract tests (#1194, ADR-0085 D7).

Three guarantees, each CI-enforced so they can never silently rot:

1. **Spec-anchored** — every (verb, path) the client's typed surface claims
   exists in the committed contract (`console/openapi.json`). That case lives in
   `test_openapi_contract.py` beside the spec's other tests: the committed spec is the
   full app's, so it is off the public list (#2424).
2. **Complete** — every typed public method IS in the surface table (no
   untracked endpoints sneaking in).
3. **Boundary** — the package imports nothing from `cairn`/`cairn.core`:
   this is the import-linter rule that makes the CLI (#1195) and ask (#1196)
   ports real instead of cosmetic.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import cairn_client
from cairn_client import CairnClient, _SURFACE


def test_every_typed_method_is_in_the_surface_table():
    infra = {"request", "close", "login"}  # login IS in the table; keep it checked
    methods = {
        n for n, m in inspect.getmembers(CairnClient, predicate=inspect.isfunction)
        if not n.startswith("_")
    }
    untracked = methods - set(_SURFACE) - (infra - {"login"})
    assert not untracked, f"typed methods missing from _SURFACE: {sorted(untracked)}"


def test_client_imports_nothing_from_cairn_core():
    """The dependency boundary (ADR-0085 D7). Same enforcement pattern as the
    test asserting leg_executor imports no langgraph."""
    pkg_dir = Path(inspect.getfile(cairn_client)).parent
    offenders: list[str] = []
    for py in pkg_dir.rglob("*.py"):
        tree = ast.parse(py.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                offenders += [f"{py.name}: import {a.name}" for a in node.names
                              if a.name == "cairn" or a.name.startswith("cairn.")]
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                if mod == "cairn" or mod.startswith("cairn."):
                    offenders.append(f"{py.name}: from {mod} import …")
    assert not offenders, f"cairn-client must not import engine internals: {offenders}"


def test_error_shape_carries_status_and_detail():
    from cairn_client import CairnAPIError

    e = CairnAPIError(404, "unknown pipeline", "get", "/pipelines/x")
    assert e.status_code == 404 and "unknown pipeline" in str(e)


def test_async_client_has_full_method_parity_with_sync():
    """#1196: AsyncCairnClient is a hand-written twin — this is the guard that
    keeps the two surfaces in lockstep (method names AND signatures)."""
    from cairn_client import AsyncCairnClient

    def surface(cls):
        return {
            n: str(inspect.signature(m))
            for n, m in inspect.getmembers(cls, predicate=inspect.isfunction)
            if not n.startswith("_")
        }

    sync_s, async_s = surface(CairnClient), surface(AsyncCairnClient)
    assert set(sync_s) == set(async_s), (
        f"method drift: sync-only={sorted(set(sync_s) - set(async_s))} "
        f"async-only={sorted(set(async_s) - set(sync_s))}"
    )
    for name in sync_s:
        assert sync_s[name] == async_s[name], f"{name}: signature drift\n  sync : {sync_s[name]}\n  async: {async_s[name]}"


def test_async_client_speaks_the_same_wire_shape():
    """One request through a mock async transport proves auth header + error
    mapping match the sync client's semantics."""
    import asyncio

    import httpx

    from cairn_client import AsyncCairnClient, CairnAPIError

    seen: dict = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["path"] = request.url.path
        if request.url.path == "/pipelines/missing":
            return httpx.Response(404, json={"detail": "unknown pipeline"})
        return httpx.Response(200, json={"ok": True})

    async def main():
        c = AsyncCairnClient(
            "http://cairn.local", token="T", transport=httpx.MockTransport(handler)
        )
        assert await c.healthz() == {"ok": True}
        assert seen["auth"] == "Bearer T"
        try:
            await c.pipeline("missing")
            raise AssertionError("expected CairnAPIError")
        except CairnAPIError as e:
            assert e.status_code == 404 and "unknown pipeline" in e.detail
        await c.close()

    asyncio.run(main())


def test_reject_step_posts_the_reason_on_both_clients():
    """ADR-0156 D4/D6 — `reject_step` beside `approve_step`: the reason is required, the
    approver is sent only when given, and a 404/403/409 maps to CairnAPIError."""
    import asyncio
    import json

    import httpx
    import pytest

    from cairn_client import AsyncCairnClient, CairnAPIError

    seen: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, json.loads(request.content)))
        code = {"pl_403": 403, "pl_404": 404, "pl_409": 409}.get(request.url.path.split("/")[2], 200)
        return httpx.Response(code, json={"detail": f"d{code}"} if code != 200 else {"status": "failed"})

    sync = CairnClient("http://c", transport=httpx.MockTransport(handler))
    assert sync.reject_step("pl_1", "gate", reason="no") == {"status": "failed"}
    assert sync.reject_step("pl_1", "gate", reason="no", approver="dana") == {"status": "failed"}
    assert seen[0] == ("POST", "/pipelines/pl_1/steps/gate/reject", {"reason": "no"})
    assert seen[1][2] == {"reason": "no", "approver": "dana"}
    with pytest.raises(TypeError):
        sync.reject_step("pl_1", "gate")                      # the reason is required
    for pid, code in (("pl_403", 403), ("pl_404", 404), ("pl_409", 409)):
        with pytest.raises(CairnAPIError) as ei:
            sync.reject_step(pid, "gate", reason="no")
        assert ei.value.status_code == code

    async def main():
        c = AsyncCairnClient("http://c", transport=httpx.MockTransport(handler))
        assert await c.reject_step("pl_2", "g", reason="r", approver="x") == {"status": "failed"}
        with pytest.raises(CairnAPIError):
            await c.reject_step("pl_404", "g", reason="r")
        await c.close()

    asyncio.run(main())
    assert seen[-2][2] == {"reason": "r", "approver": "x"}


# ── #2573: Ask is platform-only — the client says so on the public server's 404 ──────────────
def _server(status: int):
    import httpx

    def handler(request):
        return httpx.Response(status, json={"detail": "Not Found" if status == 404 else "boom"})
    return httpx.MockTransport(handler)


def test_ask_on_a_server_without_it_raises_ask_unavailable():
    import pytest
    from cairn_client import AskUnavailableError, CairnAPIError, CairnClient

    c = CairnClient("http://cairn.local", token="T", transport=_server(404))
    with pytest.raises(AskUnavailableError, match="part of cairn-platform") as e:
        c.ask("why did the run fail?")
    assert isinstance(e.value, CairnAPIError) and e.value.status_code == 404   # old handlers still catch it


def test_any_other_ask_error_is_the_plain_api_error():
    import pytest
    from cairn_client import AskUnavailableError, CairnAPIError, CairnClient

    c = CairnClient("http://cairn.local", token="T", transport=_server(500))
    with pytest.raises(CairnAPIError) as e:
        c.ask("q")
    assert not isinstance(e.value, AskUnavailableError) and e.value.status_code == 500


async def test_the_async_client_explains_the_404_too():
    import pytest
    from cairn_client import AskUnavailableError, AsyncCairnClient

    c = AsyncCairnClient("http://cairn.local", token="T", transport=_server(404))
    with pytest.raises(AskUnavailableError):
        await c.ask("q")
