"""Ops-MCP surface (ADR-0067, #642) — the cairn HTTP app's READ routes, exposed
as MCP tools, auto-derived.

The FastAPI app IS the governed operations layer: every route already runs
policy + audit + workspace scoping. ``FastMCP.from_fastapi(app)`` turns the
allow-listed routes into agent tools, so ``cairn ask`` (and any MCP client)
reasons over the *same* endpoints the console + CLI use. #640's read route
``POST /ui/api/knowledge/query`` becomes the ``answer_knowledge`` tool with zero
bespoke tool code — that's the whole point of the seam.

This is cairn's MCP server: it mirrors the OPERATIONS API. (The pack-capability
MCP server ADR-0067 kept beside it, ``services/mcp/server.py``, was retired in #2425.)

**Reads-first, fail-closed.** ``route_maps`` is an explicit TOOL allow-list of
the curated read endpoints followed by a final EXCLUDE catch-all — so a route
not named here is off the agent belt by default. A newly-added *mutating* route
can never silently leak onto an agent (this is what enforces the ask RED LINE by
capability: no run / resume / cancel / install / admin tool exists to bind). Note
one sharp edge this closed: ``GET /triggers/approve/{id}`` used to be a mutation
behind a GET (a magic-link approval); the decision is a POST now (#63), and the
catch-all still excludes the route because we do NOT blanket-allow GETs.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastmcp import FastMCP

_ENV_FLAG = "CAIRN_OPS_MCP"


def ops_mcp_enabled() -> bool:
    """True when the ops-MCP surface is opted in (``CAIRN_OPS_MCP`` truthy). Off by
    default until the reads-first surface is proven — existing deployments and
    tests see no change."""
    return getenv(_ENV_FLAG, "").strip().lower() in ("1", "true", "yes", "on")


def _route_maps():
    """The reads-first allow-list. First match wins; the trailing EXCLUDE makes it
    fail-closed. Patterns match the OpenAPI path template (``/runs/{run_id}``)."""
    # optional dependency lives one import deeper; RouteMap/MCPType are light.
    from fastmcp.server.providers.openapi import MCPType, RouteMap

    TOOL = MCPType.TOOL
    return [
        # ⭐ grounded-knowledge read (#640) → the `answer_knowledge` tool. POST, but
        # read-only by construction (no synthesis, no mutation).
        RouteMap(methods=["POST"], pattern=r"^/ui/api/knowledge/query$", mcp_type=TOOL),
        # run state + observability — reads only (NOT /stream, /events SSE, nor the
        # binary /artifacts/download; those fall through to EXCLUDE).
        RouteMap(methods=["GET"], pattern=r"^/runs$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/runs/[^/]+$", mcp_type=TOOL),
        RouteMap(
            methods=["GET"],
            pattern=r"^/runs/[^/]+/(report|diagnosis|pending-approval|pilot|artifacts)$",
            mcp_type=TOOL,
        ),
        RouteMap(methods=["GET"], pattern=r"^/insights(/.*)?$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/suggestions(/[^/]+)?$", mcp_type=TOOL),
        # audit trail + remote-job status + operator catalog (#620 read surface):
        # "audit for run X", "did my training job finish", "what operators exist".
        RouteMap(methods=["GET"], pattern=r"^/console/api/(audit|jobs|operators)$", mcp_type=TOOL),
        # schedules + installed skills/triggers (read-only; the /triggers POSTs that
        # RUN or mutate schedules stay excluded — only these two GETs are allowed).
        RouteMap(methods=["GET"], pattern=r"^/triggers/(scheduled|skills)$", mcp_type=TOOL),
        # discovery: packs (contract/graph = plan preview + render-run-UI), registry
        RouteMap(methods=["GET"], pattern=r"^/packs(/.*)?$", mcp_type=TOOL),
        RouteMap(
            methods=["GET"],
            pattern=r"^/registry/(catalog|installed-workflows|installs)$",
            mcp_type=TOOL,
        ),
        # governed config reads
        RouteMap(methods=["GET"], pattern=r"^/ui/api/policies(/.*)?$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/ui/api/models/(catalog|roles)$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/ui/api/catalog$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/ui/api/connections$", mcp_type=TOOL),
        RouteMap(methods=["GET"], pattern=r"^/ui/api/knowledge(/corpora)?$", mcp_type=TOOL),
        # ── fail closed ──────────────────────────────────────────────────────
        # everything else off the wire: every mutation (run/resume/cancel/install/
        # enable/disable/sync, all /triggers POST, admin, token, GET-magic-link
        # approve), plus SSE, downloads, /ui HTML, /healthz, oauth.
        RouteMap(mcp_type=MCPType.EXCLUDE),
    ]


# Clean, agent-legible tool names (#645). Keyed by FastAPI operationId — the
# from_fastapi default derives ugly names (`get_run_runs`, `list_investigations_
# runs_get`). Only the reads-first allow-list is mapped; keys for excluded routes
# are simply ignored. Keep in sync with _route_maps.
_TOOL_NAMES = {
    # ⭐ grounded knowledge (#640)
    "ui_api_knowledge_query_ui_api_knowledge_query_post": "answer_knowledge",
    # run state + observability
    "list_investigations_runs_get": "query_runs",
    "get_run_runs__run_id__get": "get_run",
    "get_run_report_runs__run_id__report_get": "get_run_report",
    "get_run_diagnosis_runs__run_id__diagnosis_get": "get_run_diagnosis",
    "get_pending_approval_runs__run_id__pending_approval_get": "get_run_pending_approval",
    "pending_approvals_runs_pending_approvals_get": "count_pending_approvals",  # #1880 badge count
    "get_run_pilot_runs__run_id__pilot_get": "get_run_pilot",
    "get_run_artifacts_runs__run_id__artifacts_get": "get_run_artifacts",
    # audit + remote jobs + operators + schedules + skills (#620 read surface)
    "list_audit_console_api_audit_get": "get_audit",
    "list_jobs_console_api_jobs_get": "list_remote_jobs",
    "list_operators_console_api_operators_get": "list_operators",
    "list_scheduled_triggers_scheduled_get": "list_schedules",
    "list_skills_triggers_skills_get": "list_skills",
    "get_insights_summary_insights_summary_get": "get_insights_summary",
    "get_insights_pipelines_insights_pipelines_get": "get_insights_pipelines",  # #2346
    "get_insights_cost_insights_cost_get": "get_cost",
    "get_insights_failures_insights_failures_get": "get_insights_failures",
    "get_insights_health_insights_health_get": "get_insights_health",
    "get_workflow_insights_insights_workflow_get": "get_workflow_insights",
    "get_workflow_clusters_insights_workflow_clusters_get": "get_workflow_clusters",
    "get_workflow_report_insights_workflow_report_get": "get_workflow_report",
    "get_workflow_runs_insights_workflow_runs_get": "get_workflow_runs",
    "list_suggestions_endpoint_suggestions_get": "list_suggestions",
    "get_suggestion_endpoint_suggestions__suggestion_id__get": "get_suggestion",
    # discovery: packs (contract = render-run-UI, graph = plan preview), registry
    "list_packs_packs_get": "list_packs",
    "pack_contract_packs__pack___template__contract_get": "get_pack_contract",
    "pack_graph_packs__pack___template__graph_get": "get_pack_graph",
    "pack_slack_modal_packs__pack___template__slack_modal_get": "get_pack_slack_modal",
    "registry_catalog_registry_catalog_get": "get_registry_catalog",
    "installed_workflows_registry_installed_workflows_get": "list_installed_workflows",
    "registry_installs_registry_installs_get": "list_registry_installs",
    # governed config reads
    "ui_api_policies_ui_api_policies_get": "list_policies",
    "ui_api_policy_get_ui_api_policies__policy_id__get": "get_policy",
    "ui_api_policy_schema_ui_api_policies_schema_get": "get_policy_schema",
    "ui_api_model_roles_ui_api_models_roles_get": "get_model_roles",
    "ui_api_model_catalog_ui_api_models_catalog_get": "get_model_catalog",
    "ui_api_catalog_ui_api_catalog_get": "get_connector_catalog",
    "ui_api_connections_ui_api_connections_get": "list_connections",
    "ui_api_corpora_list_ui_api_knowledge_corpora_get": "list_knowledge_corpora",
    "ui_api_knowledge_get_ui_api_knowledge_get": "list_knowledge_packs",
}


def build_ops_mcp(
    app: "FastAPI", *, workspace: Optional[str] = None, token: Optional[str] = None,
) -> "FastMCP":
    """Derive the ops-MCP server from a fully-routed cairn app. Call AFTER every
    route is registered so the tool set is complete. Tool invocations route back
    into ``app`` in-process (from_fastapi's ASGI-backed httpx client), so the same
    policy/audit/workspace path runs — the agent is just another client.

    ``workspace`` scopes EVERY tool call to that workspace by stamping the
    ``X-Cairn-Workspace`` header on the internal client — the same header the
    console/CLI send everywhere else (#228). Omit it and the tools read the
    default workspace (workspace-blind), which leaks across tenants; the caller
    (ask endpoint / CLI) must pass the active workspace.

    ``token`` stamps ``Authorization: Bearer <token>`` on the internal client so
    the tools can reach the app's auth-gated routes on an auth-enabled deploy
    (#663). Without it the agent reads 401 everywhere in prod (dev disables auth,
    which is why this was invisible until the box). The agent reads AS the caller,
    so it never exceeds the caller's own scope."""
    from fastmcp import FastMCP  # optional dep at the leaf; kept off import path

    headers = {}
    if workspace:
        headers["X-Cairn-Workspace"] = workspace
    if token:
        headers["Authorization"] = f"Bearer {token}"
    client_kwargs = {"headers": headers} if headers else None
    return FastMCP.from_fastapi(
        app, name="cairn-ops", route_maps=_route_maps(), mcp_names=_TOOL_NAMES,
        httpx_client_kwargs=client_kwargs,
    )


__all__ = ["ops_mcp_enabled", "build_ops_mcp"]
