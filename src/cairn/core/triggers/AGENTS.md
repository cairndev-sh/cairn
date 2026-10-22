# cairn/core/triggers — the HTTP control plane (FastAPI), auth, ingress, declared triggers

**Purpose.** `server.py:build_app` composes the D2 routers (`routers/`, plus `accounts` —
`/auth/*`, `/workspaces`, `/variables`) and mounts the `cairn.routers` providers; the
console's routers, `webui.py` and the ops-MCP mount left for `packages/cairn-platform`
(#2427), and so did `ask`. `jobs` (the pull lane `cairn worker --pull` calls) and `oauth`
(`/oauth/callback`, the redirect of `cairn connect --via oauth`) are D2, mounted by `build_app`;
the root registers no `cairn.routers` provider. Here too: the workspace
middleware, JWT auth (`auth.py`, enforced iff `OBS_JWT_SECRET`), webhook ingress
(`router.py`; the inbox is `storage/trigger_inbox.py`, the spec and presets are
`engine/triggers_{spec,presets}.py`), stored schedules/triggers (`storage/schedule_store.py`,
`storage/trigger_store.py`); the `triggers:` front-matter grammar and materialisation are
`engine/triggers_grammar.py` (#2416 — `declared.py`, `spec.py`, `presets.py`,
`schedule_store.py`, `store.py`, `inbox.py` here are deprecated aliases), feature gates (`feature_gate.py`), the ops MCP server
(`mcp.py`), console support. Console script: `cairn-server` (`main.py`).
**Owns.** Route shapes and status codes; auth/workspace enforcement at the edge; the
committed contract `console/openapi.json`.
**Does not own.** Business logic — routes must delegate to `dispatch/` doors and stores
(`start.py`, `pipeline_launch.py`, `dispatcher`), never re-implement admission. The trigger
*grammar* in `declared.py` is consumed by `engine/schema.py` (a layering inversion — do not
deepen it; see `docs/architecture/02-dependency-model.md`).

**Contracts.** Any route change → `uv run python scripts/dump_openapi.py` (CI fails stale
specs; oasdiff fails breaking changes without the `api-break` label) → console `npm run
gen:api` → vite `API_PATHS`. New surfaces behind `require_feature(...)` when optional.
`POST /start` (`routers/start.py`) is the one launch verb; older routes delegate to the same
functions.

**Allowed deps.** everything in core it must expose (it is the top layer). **Forbidden.**
operator packs, `cairn.packs.*`; putting stores or grammars here that lower layers then
import back.

**Testing.** `tests/core/test_openapi_contract.py`, `test_cairn_client_contract.py`,
`test_trigger_server_dispatch.py`, `test_workspace_boundary.py`, `test_pack_contract_api.py`,
`test_knowledge_api.py`,
`test_pending_approvals_api.py`, `test_server_lifespan_boot.py`. AUTH-ON is a different app
than keyless — test with `CAIRN_JWT_SECRET` set when touching auth or admin. The capture,
datasets and judges routers left for `packages/cairn-platform` (#2421); their tests went with them.

**Common mistakes.** Handlers as 300-line closures inside `build_*_router` (existing debt;
put logic in a module); reading stores directly for a mutation that has a door; a
workspace write or execute route without `Depends(console_support.writable_workspace)` (#221;
`tests/core/test_write_route_workspace_guard.py` fails it unless `EXEMPT` says why); a
governance-config write without `require_admin`.

**Read.** `docs/architecture/01-system-map.md`, ADR-0085 D1/D4, ADR-0094, ADR-0101 D1.
