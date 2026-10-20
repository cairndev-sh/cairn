"""Settings — environment + YAML config loading.

Two layers:
- `Settings` (pydantic-settings): runtime env vars (secrets, endpoints, feature flags)
- `get_app_config()`: structural config from `config/app.yaml` (agents, prompts, routing rules)

Env vars override YAML where they overlap.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import AliasChoices, ConfigDict, Field
from pydantic_settings import BaseSettings


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_env() -> None:
    """Load ``.env`` into ``os.environ`` (idempotent; ``override=False`` keeps an
    explicitly-set shell var winning). Call once at the top of every entrypoint
    — CLI, worker, server — before importing modules that build LLM clients.

    Required because litellm / the OpenAI SDK read ``OPENAI_API_KEY`` from
    ``os.environ`` directly, while pydantic-settings reads ``.env`` into the
    Settings object, NOT into ``os.environ``.
    """
    load_dotenv(PROJECT_ROOT / ".env")


class Settings(BaseSettings):
    """Runtime env vars. Loaded from .env or process env."""

    model_config = ConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ─── LLM providers ────────────────────────────────────────────────
    openai_api_key: str = ""

    # ─── OpenTelemetry ────────────────────────────────────────────────
    otel_exporter_otlp_endpoint: str = ""
    otel_service_name: str = "cairn"
    otel_console: bool = False

    # ─── Observability deep-links (#248) ──────────────────────────────
    # Base URL of the Langfuse instance holding the LLM traces cairn exports
    # (integrate-don't-rebuild: cairn links OUT, never renders traces itself).
    # The console builds the per-run trace URL from this + ``langfuse_project_id``
    # (see below). Empty → the console hides the "View in Langfuse" link.
    # Trailing slash tolerated. Env: ``LANGFUSE_BASE`` (field name == env var).
    langfuse_base: str = ""
    # Langfuse project id (from the Langfuse URL / GET /api/public/projects). The
    # canonical trace URL is project-scoped — ``{base}/project/{id}/traces/{tid}``;
    # the bare ``{base}/trace/{tid}`` form times out on a cross-project lookup. When
    # set, the console builds the project-scoped link; empty → it falls back to the
    # short form. Env: ``LANGFUSE_PROJECT_ID``.
    langfuse_project_id: str = ""
    # Base URL of the Grafana instance holding the #242 fleet dashboards, for a
    # future "open dashboard" link. Empty → hidden. Env: ``GRAFANA_BASE``.
    grafana_base: str = ""

    # ─── MCP server (SSE transport) ───────────────────────────────────
    # When set, app talks to MCP server over SSE. Otherwise spawns stdio subprocess.
    mcp_sse_url: str = ""

    # ─── Telemetry backend (data source for MCP tools) ────────────────
    # "mock"  → reads data/*.json fixtures (default; what tests + evals use)
    # "otel"  → live queries against Jaeger / Prometheus / (optional) Loki
    #
    # Switch with env: CAIRN_BACKEND=otel. Each backend URL is overridable
    # too; defaults match the upstream OTel demo's docker-compose ports.
    # Logs are degraded to [] when OTEL_LOKI_URL is empty so the minimal
    # demo (no Loki) still works end-to-end.
    # ADR-0122: CAIRN_BACKEND, else the former OBS_BACKEND (read until 1.0). Pydantic
    # resolves the .env file too, which the process-env lookup (settings.getenv) cannot.
    obs_backend: str = Field("mock", validation_alias=AliasChoices("CAIRN_BACKEND", "OBS_BACKEND"))
    otel_jaeger_url: str = "http://localhost:16686"
    otel_prometheus_url: str = "http://localhost:9090"
    otel_loki_url: str = ""

    # ─── ClickStack backend (ClickHouse + HyperDX + OTel Collector) ───
    # Recommended OSS substrate per Roadmap §3.3. The adapter speaks
    # ClickHouse SQL directly against the otel_logs / otel_traces /
    # otel_metrics_* tables that HyperDX provisions on first boot.
    clickstack_clickhouse_url: str = "http://localhost:8123"
    clickstack_clickhouse_user: str = "obs_agent"
    clickstack_clickhouse_password: str = ""
    clickstack_clickhouse_database: str = "default"
    clickstack_hyperdx_url: str = "http://localhost:8081"

    # ─── Self-critique (IncidentAgent) ────────────────────────────────
    # When synthesis confidence < threshold, run a second LLM call to audit
    # the analysis and append findings. No extra cost on high-confidence cases.
    enable_self_critique: bool = True
    self_critique_threshold: float = 0.7

    # ─── Per-session budget cap (USD) ─────────────────────────────────
    # src/obs/cost.py raises BudgetExceeded when the running total for the
    # current request scope crosses this number. Set to 0 to disable.
    max_cost_usd: float = 1.0

    # ─── Per-agent-node latency budget (ms) ───────────────────────────
    # src/graph/builder.py wraps each agent's handle() in asyncio.wait_for.
    # On timeout the request degrades gracefully (returns a partial answer
    # instead of hanging the SSE). Override per agent via
    # `node_timeout_ms:` in config/app.yaml. Set to 0 to disable.
    node_timeout_ms: int = 30000

    # ─── RAG / Runbook search ───────────────────────────────────────────
    # Default: sqlite_vec (in-built, dev/local). pgvector for prod. qdrant /
    # pinecone are external providers (#76) — set explicitly to use them.
    rag_vector_store: str = "sqlite_vec"
    rag_embed_provider: str = "gemini"
    gemini_api_key: str = ""
    llama_cloud_api_key: str = ""
    qdrant_url: str = "http://localhost:6333"
    pinecone_api_key: str = ""
    pinecone_index: str = "obs-runbooks"
    pgvector_dsn: str = ""
    rag_collection_name: str = "obs_runbooks"
    memory_collection_name: str = "obs_memories"
    rag_embed_model: str = "gemini-embedding-001"
    rag_embed_dim: int = 3072
    # #513 — cross-corpus re-ranker: identity (default, local score sort) |
    # cross_encoder (local model) | cohere (API). identity adds no LLM/API cost.
    rag_reranker: str = "identity"

    # ─── LLM gateway (proxy for cost governance, routing, audit) ─────
    llm_gateway: str = "direct"
    llm_gateway_url: str = ""
    llm_gateway_api_key: str = ""
    llm_gateway_job_tag_header: str = "x-job-tag"
    llm_gateway_add_provider_prefix: bool = True

    # ─── LLM-based query planner ──────────────────────────────────────
    # Off by default: keyword planner is free and good enough for the demo.
    # Turn on for production where query phrasing varies wildly. Adds one
    # LLM call (~$0.0002, ~300ms) before each deterministic agent run.
    # Failures silently fall back to the keyword planner.
    llm_planner_enabled: bool = False
    llm_planner_model: str = "gpt-4o-mini"

    @property
    def tracing_enabled(self) -> bool:
        return bool(self.otel_exporter_otlp_endpoint) or self.otel_console


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_app_config() -> dict[str, Any]:
    """Load structural config from config/app.yaml."""
    config_path = PROJECT_ROOT / "config" / "app.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
