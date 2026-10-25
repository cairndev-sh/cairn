"""Typed infra connections — access (MCP / API / HTTP) + compute targets.

A :class:`Connection` is one reusable, typed binding to external infra,
discriminated by ``kind`` (the two-axis model of an internal issue / #70). It
carries non-secret ``config`` plus ``secrets`` that are **references** into the
SecretStore (ADR-0033) — never raw secret values. cairn already owns the secret
half (``get_secret_provider`` / ``cairn-operator-secrets-env``), so connections point
at it rather than building a second encrypted vault (the divergence from
Langflow's Fernet-encrypted Variables).

Storage mirrors :class:`~cairn.core.storage.SqliteRunStore`: the full Connection
serializes to a JSON ``data`` blob, with ``id``/``name``/``kind`` mirrored into
indexed columns. Backend follows the env (sqlite today; Postgres is the same
mirror as ``PostgresRunStore`` — a follow-up).

``resolve_connection(name)`` is the runtime wiring: it returns the config with
the secret refs resolved through the bound SecretStore — what an operator /
runtime calls to actually use a connection.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json as _json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from pydantic import BaseModel, Field, field_validator
from cairn.core._time import utc_now_iso
from cairn.core.pg_schema_init import PgSchemaInit

logger = logging.getLogger(__name__)

#: The implicit account every single-node / OSS deployment runs in (ADR-0054,
#: epic #392 phase 2). A connection with no explicit tenant belongs here;
#: pre-#392 rows migrate to it. Mirrors :data:`cairn.core.account.DEFAULT_ACCOUNT`
#: (kept as a local literal so connections.py stays import-light and free of an
#: account→connections import cycle).
DEFAULT_ACCOUNT = "default"

# Known connection kinds (#68 Axis-1 access + #70 Axis-2 compute). Not a hard
# enum — providers may add kinds; this is the curated set the UI + docs know.
KNOWN_KINDS = (
    "mcp", "api_key", "http", "oauth",
    # #1815 — a DB connection is a DRIVER connection (libpq / MySQL wire), not
    # HTTP. It was riding `http` because no better kind existed, which made the
    # kind read as a promise the transport never kept; ingestion (dlt) resolves
    # these by name and builds a SQLAlchemy URI from the DSN.
    "database",
    "compute.docker", "compute.kube", "compute.runpod", "compute.ray", "compute.ssh",
)


def slugify(name: str) -> str:
    """Stable, URL-safe id from a connection name (admin pk + lookup key)."""
    s = re.sub(r"[^a-zA-Z0-9._-]+", "-", name.strip().lower()).strip("-")
    return s or "connection"


class HealthState(BaseModel):
    """The last authenticated-probe result stored on a :class:`Connection`.

    Companion written by ``test_connection`` / the on-add hook so the CLI + the
    console can show a health dot without re-probing. ``ok=None`` means "never
    probed"; ``status`` is the transport code (HTTP status, or 0 for
    structural/degraded probes)."""

    ok: Optional[bool] = None
    status: int = 0
    detail: str = ""
    latency_ms: Optional[float] = None
    checked_at: Optional[str] = None


class Connection(BaseModel):
    """One typed infra connection. ``secrets`` maps a logical key to a
    SecretStore reference (e.g. ``{"API_KEY": "OPENAI_API_KEY"}``) — never a raw
    secret value."""

    id: str = ""
    name: str
    kind: str
    #: The owning account/tenant (ADR-0054, #392 phase 2). The isolation boundary:
    #: every store read/write is scoped to it so one account never sees another's
    #: connectors or credential refs. Mirrored into an indexed column out of the
    #: JSON blob; defaults to the implicit ``default`` account (OSS single-tenant).
    account_id: str = DEFAULT_ACCOUNT
    config: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    enabled: bool = True
    #: ADR-0086 H5 (#1247) — how many legs may be IN FLIGHT on this compute
    #: target at once. ``None`` (the default) = unlimited, the honest zero-change
    #: default: nothing counts, nothing queues, the submit path is byte-identical
    #: to before the cap existed. A positive value makes the control plane admit
    #: at most N concurrent legs bound to this connection; the rest WAIT in
    #: ``queued`` (a visible status) rather than being refused — one GPU box is
    #: protected from five pipelines launching at once without any of them
    #: failing. Also settable as ``config["max_in_flight"]`` so
    #: ``cairn connect … --set max_in_flight=2`` works with no CLI change; the
    #: field wins when both are present. Values ``<= 0`` mean "unset".
    max_in_flight: Optional[int] = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: Optional[str] = None
    #: last authenticated-probe result (health dot). None until first probed.
    health: Optional[HealthState] = None

    @field_validator("name")
    @classmethod
    def _name_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("connection name must be non-empty")
        return v.strip()

    @field_validator("kind")
    @classmethod
    def _kind_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("connection kind must be non-empty")
        return v.strip()

    def model_post_init(self, _ctx: Any) -> None:
        if not self.id:
            object.__setattr__(self, "id", slugify(self.name))

    @property
    def secret_keys(self) -> list[str]:
        """Logical secret names this connection needs — safe to display
        (the values/refs are NOT exposed here)."""
        return sorted(self.secrets.keys())


# ─── provider catalog ───────────────────────────────────────────────────
#
# The catalog is what makes ``cairn connect <provider>`` zero-config: each
# entry knows a provider's connection ``kind``, the logical secret keys it
# needs (mapped to their conventional SecretStore/env ref), and any config
# defaults. ``cairn connect openai`` therefore produces a working api_key
# connection pointed at ``OPENAI_API_KEY`` with no flags. Overrides via
# ``--set``/``--secret`` layer on top. Reusable by admin + ``cairn ask``.


# ── authentication taxonomy (connector-probe) ───────────────────────────
#
# ``auth_type`` is HOW a provider's credential is presented — orthogonal to the
# connection ``kind`` (which is the runtime mechanism). It drives the prober's
# dispatch. HTTP-shaped types (api_key/bearer_token/basic/http_no_auth) run the
# generic HTTP probe; the structural ones (ssh_key/mtls/oauth2/…) have their own
# (stubbed / clearly-NotImplemented) handlers.
AUTH_TYPES = (
    "api_key",           # a key/token applied as a custom header or query param
    "bearer_token",      # Authorization: Bearer <key>
    "basic",             # HTTP Basic (user:pass, often key-as-user)
    "public_private_key",  # keypair signing (e.g. SigV4 for S3/R2)
    "oauth2",            # broker-owned auth-code + refresh (introspect to probe)
    "mtls",              # mutual-TLS client cert
    "ssh_key",           # SSH private key (open + auth)
    "http_no_auth",      # HTTP reachable, no credential (dev/local servers)
    "none",              # nothing to probe (structural placeholder)
)

# How the resolved credential is applied to the generic HTTP probe request.
APPLY_HEADER = "header"        # via a named request header
APPLY_QUERY = "query"          # via a URL query parameter
APPLY_BASIC = "basic"          # HTTP Basic auth (username[:password])
APPLY_NONE = "none"            # no credential applied


@dataclass(frozen=True)
class ProbeSpec:
    """Descriptor that drives the generic prober for HTTP-shaped auth types.

    A probe is a single cheap authenticated request against the provider's base
    URL. ``path`` is joined onto the resolved base (``base_url_key`` → the config
    key holding it, else ``base_url`` literal). The credential named by
    ``secret_key`` is applied per ``apply``:

    * ``header`` → set header ``header_name`` to ``prefix + value``
      (e.g. ``Bearer `` prefix for bearer tokens; empty for api-key headers);
    * ``query``  → set query param ``query_param`` to the value;
    * ``basic``  → HTTP Basic; the value fills the username, ``basic_password_key``
      (another secret logical key) fills the password when set, else the format
      is ``value:`` (key-as-user).

    ``ok_status`` is the set of HTTP codes counted as a healthy authenticated
    response. ``supported=False`` marks providers with no cheap authenticated
    liveness check (SDK-only or unauth-only health) — the prober returns a
    documented "reachable/unsupported" result instead of a false "connected"."""

    method: str = "GET"
    path: str = ""
    #: literal base URL when the provider has a fixed host (else use base_url_key).
    base_url: str = ""
    #: config key holding the base URL / host (e.g. "base_url", "url", "host").
    base_url_key: str = ""
    apply: str = APPLY_NONE
    header_name: str = ""
    prefix: str = ""
    query_param: str = ""
    #: logical secret key to read the credential from (default "API_KEY").
    secret_key: str = "API_KEY"
    #: second secret key for the Basic password half (else key-as-user / value:).
    basic_password_key: str = ""
    #: JSON body to send (for POST probes, e.g. GraphQL liveness queries).
    json_body: Optional[dict[str, Any]] = None
    ok_status: tuple[int, ...] = (200,)
    #: False → no cheap authed probe exists; prober degrades, never fakes "ok".
    supported: bool = True
    #: one-liner shown when supported=False (why + what's checked instead).
    unsupported_reason: str = ""


@dataclass(frozen=True)
class ProviderSpec:
    """The shape of a connectable provider. ``secrets`` maps a logical key to
    its default SecretStore *reference* (an env-var name by convention) — not a
    value. ``config`` holds non-secret defaults the user can override."""

    provider: str
    kind: str
    description: str
    secrets: dict[str, str] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    # ── catalog display metadata (#195) — drives the backend-driven gallery ──
    #: human display name (falls back to the provider key, title-cased).
    label: str = ""
    #: simple-icons slug; the SPA renders it from a CDN, monogram fallback.
    icon: str = ""
    #: display grouping for the gallery — NOT the connection ``kind``. One of:
    #: models · gateway · compute · ssh · mcp · tracking · observability ·
    #: storage · vectordb · database · saas.
    category: str = ""
    docs_url: str = ""
    #: MCP transports the server speaks (kind=mcp only): stdio · streamable_http · sse.
    transports: list[str] = field(default_factory=list)
    # ── authenticated health probe (connector-probe) ──
    #: how the credential is presented — one of AUTH_TYPES; drives prober dispatch.
    auth_type: str = "none"
    #: the HTTP-probe descriptor (used for api_key/bearer/basic/http_no_auth).
    probe: Optional[ProbeSpec] = None

    @property
    def display_label(self) -> str:
        return self.label or self.provider.replace("-", " ").replace("_", " ").title()


# The connectable-provider catalog (#195). ``kind`` is the connection mechanism
# (api_key/http/oauth/mcp/compute.*/tracking.*) the runtime resolves; ``category``
# is the human grouping the gallery shows. Secrets are env-var *references* by
# convention; entered values go to the writable secret store (#200).
PROVIDER_CATALOG: dict[str, ProviderSpec] = {
    # ── models (kind=api_key / http) ──
    # OpenAI-compatible: GET /v1/models · Authorization: Bearer $KEY → 200.
    "openai": ProviderSpec("openai", "api_key", "OpenAI — GPT-4o, embeddings", {"API_KEY": "OPENAI_API_KEY"}, {"base_url": "https://api.openai.com"}, label="OpenAI", icon="openai", category="models", docs_url="https://platform.openai.com/docs", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    # Anthropic: GET /v1/models · x-api-key + anthropic-version (NOT bearer).
    "anthropic": ProviderSpec("anthropic", "api_key", "Anthropic — Claude", {"API_KEY": "ANTHROPIC_API_KEY"}, {"base_url": "https://api.anthropic.com"}, label="Anthropic", icon="anthropic", category="models", docs_url="https://docs.anthropic.com", auth_type="api_key", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="x-api-key", secret_key="API_KEY")),
    # Gemini: GET /v1beta/models?key=$KEY (key is a query param).
    "gemini": ProviderSpec("gemini", "api_key", "Google Gemini", {"API_KEY": "GEMINI_API_KEY"}, {"base_url": "https://generativelanguage.googleapis.com"}, label="Google Gemini", icon="googlegemini", category="models", auth_type="api_key", probe=ProbeSpec(path="/v1beta/models", base_url_key="base_url", apply=APPLY_QUERY, query_param="key", secret_key="API_KEY")),
    "mistral": ProviderSpec("mistral", "api_key", "Mistral AI", {"API_KEY": "MISTRAL_API_KEY"}, {"base_url": "https://api.mistral.ai"}, label="Mistral", icon="mistralai", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "cohere": ProviderSpec("cohere", "api_key", "Cohere — embed + rerank", {"API_KEY": "COHERE_API_KEY"}, {"base_url": "https://api.cohere.com"}, label="Cohere", icon="cohere", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "groq": ProviderSpec("groq", "api_key", "Groq — low-latency inference", {"API_KEY": "GROQ_API_KEY"}, {"base_url": "https://api.groq.com/openai"}, label="Groq", icon="groq", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "together": ProviderSpec("together", "api_key", "Together AI — inference + fine-tuning", {"API_KEY": "TOGETHER_API_KEY"}, {"base_url": "https://api.together.xyz"}, label="Together AI", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "fireworks": ProviderSpec("fireworks", "api_key", "Fireworks AI", {"API_KEY": "FIREWORKS_API_KEY"}, {"base_url": "https://api.fireworks.ai/inference"}, label="Fireworks AI", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "deepinfra": ProviderSpec("deepinfra", "api_key", "DeepInfra inference", {"API_KEY": "DEEPINFRA_API_KEY"}, {"base_url": "https://api.deepinfra.com"}, label="DeepInfra", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/v1/openai/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    # DeepSeek: OpenAI-compatible · GET /models · Authorization: Bearer $KEY.
    "deepseek": ProviderSpec("deepseek", "api_key", "DeepSeek — chat + reasoner", {"API_KEY": "DEEPSEEK_API_KEY"}, {"base_url": "https://api.deepseek.com"}, label="DeepSeek", icon="deepseek", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "ollama": ProviderSpec("ollama", "http", "Ollama — local inference", config={"base_url": "http://localhost:11434"}, label="Ollama", icon="ollama", category="models", auth_type="http_no_auth", probe=ProbeSpec(path="/api/tags", base_url_key="base_url", apply=APPLY_NONE)),
    "vllm": ProviderSpec("vllm", "http", "vLLM — self-hosted OpenAI-compatible server", config={"base_url": "http://localhost:8000/v1"}, label="vLLM", category="models", auth_type="http_no_auth", probe=ProbeSpec(path="/models", base_url_key="base_url", apply=APPLY_NONE)),
    # ── LLM gateways (kind=api_key) — the console Gateway view sources spend/routing here (#182) ──
    "litellm": ProviderSpec("litellm", "api_key", "LiteLLM gateway — OpenAI-compatible proxy, 100+ providers", {"MASTER_KEY": "LITELLM_MASTER_KEY"}, {"base_url": ""}, label="LiteLLM", category="gateway", docs_url="https://docs.litellm.ai", auth_type="bearer_token", probe=ProbeSpec(path="/v1/models", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="MASTER_KEY")),
    # OpenRouter true-auth check = GET /api/v1/key (NOT /models, which is public).
    "openrouter": ProviderSpec("openrouter", "api_key", "OpenRouter — model marketplace router", {"API_KEY": "OPENROUTER_API_KEY"}, {"base_url": "https://openrouter.ai"}, label="OpenRouter", category="gateway", auth_type="bearer_token", probe=ProbeSpec(path="/api/v1/key", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    # ── model / data hubs ──
    # HF: GET /api/whoami-v2 · Authorization: Bearer $TOKEN.
    "huggingface": ProviderSpec("huggingface", "api_key", "Hugging Face Hub — models + datasets", {"TOKEN": "HF_TOKEN"}, {"base_url": "https://huggingface.co"}, label="Hugging Face", icon="huggingface", category="models", auth_type="bearer_token", probe=ProbeSpec(path="/api/whoami-v2", base_url_key="base_url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="TOKEN")),
    # Kaggle: Basic ($USERNAME:$KEY).
    "kaggle": ProviderSpec("kaggle", "api_key", "Kaggle kernels/datasets", {"USERNAME": "KAGGLE_USERNAME", "KEY": "KAGGLE_KEY"}, {"base_url": "https://www.kaggle.com"}, label="Kaggle", icon="kaggle", category="storage", auth_type="basic", probe=ProbeSpec(path="/api/v1/datasets/list", base_url_key="base_url", apply=APPLY_BASIC, secret_key="USERNAME", basic_password_key="KEY")),
    # ── compute / GPU (kind=compute.*) ──
    # Docker: structural — engine socket, not an HTTP-auth path.
    "docker": ProviderSpec("docker", "compute.docker", "Local/remote Docker engine", config={"host": ""}, label="Docker", icon="docker", category="compute", auth_type="none", probe=ProbeSpec(supported=False, unsupported_reason="Docker engine liveness is a socket ping (docker version), not an HTTP-auth probe — checked by the docker executor.")),
    "kube": ProviderSpec("kube", "compute.kube", "Kubernetes cluster", config={"context": "", "namespace": "default"}, label="Kubernetes", icon="kubernetes", category="compute", auth_type="mtls", probe=ProbeSpec(supported=False, unsupported_reason="Kube apiserver auth is kubeconfig/mTLS — checked by the kube executor, not a generic probe.")),
    # RunPod: POST https://api.runpod.io/graphql?api_key=$KEY body { myself { id } }.
    "runpod": ProviderSpec("runpod", "compute.runpod", "RunPod GPU cloud", {"API_KEY": "RUNPOD_API_KEY"}, label="RunPod", category="compute", auth_type="api_key", probe=ProbeSpec(method="POST", path="/graphql", base_url="https://api.runpod.io", apply=APPLY_QUERY, query_param="api_key", secret_key="API_KEY", json_body={"query": "{ myself { id } }"})),
    # Vast: GET /api/v0/users/current/ · Bearer.
    "vast": ProviderSpec("vast", "compute.vast", "Vast.ai GPU marketplace", {"API_KEY": "VAST_API_KEY"}, config={"gpu_name": ""}, label="Vast.ai", category="compute", auth_type="bearer_token", probe=ProbeSpec(path="/api/v0/users/current/", base_url="https://console.vast.ai", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    "ray": ProviderSpec("ray", "compute.ray", "Ray cluster", config={"address": "auto"}, label="Ray", icon="ray", category="compute", auth_type="http_no_auth", probe=ProbeSpec(supported=False, unsupported_reason="Ray dashboard exposes only an unauthenticated health endpoint — reachable-only, no credential to verify.")),
    # Modal: SDK-only, no cheap authenticated REST.
    "modal": ProviderSpec("modal", "api_key", "Modal — serverless GPU", {"TOKEN_ID": "MODAL_TOKEN_ID", "TOKEN_SECRET": "MODAL_TOKEN_SECRET"}, label="Modal", category="compute", auth_type="api_key", probe=ProbeSpec(supported=False, unsupported_reason="Modal auth is SDK-only (gRPC) — no cheap REST liveness; verify via the modal client.")),
    # Lambda Labs: GET /api/v1/instance-types · HTTP Basic ($KEY:).
    "lambda_labs": ProviderSpec("lambda_labs", "compute.lambda", "Lambda Labs — GPU cloud", {"API_KEY": "LAMBDA_API_KEY"}, label="Lambda Labs", category="compute", auth_type="basic", probe=ProbeSpec(path="/api/v1/instance-types", base_url="https://cloud.lambdalabs.com", apply=APPLY_BASIC, secret_key="API_KEY")),
    # JarvisLabs: SDK/CLI-only, no documented cheap authenticated REST.
    "jarvislabs": ProviderSpec("jarvislabs", "compute.jarvislabs", "JarvisLabs — GPU cloud", {"API_KEY": "JARVISLABS_API_KEY"}, label="JarvisLabs", category="compute", auth_type="api_key", probe=ProbeSpec(supported=False, unsupported_reason="JarvisLabs auth is SDK-only (jlclient) — no cheap public REST liveness; verify via the SDK.")),
    # e2b: GET https://api.e2b.app/sandboxes · X-API-Key.
    "e2b": ProviderSpec("e2b", "compute.e2b", "E2B — code-interpreter sandboxes", {"API_KEY": "E2B_API_KEY"}, label="E2B", category="compute", auth_type="api_key", probe=ProbeSpec(path="/sandboxes", base_url="https://api.e2b.app", apply=APPLY_HEADER, header_name="X-API-Key", secret_key="API_KEY")),
    # ── SSH host (kind=compute.ssh; #70 Axis-2). Private key is a SecretStore secret. ──
    # #1787 adds three OPTIONAL lifecycle keys (all default ""), so a compute.ssh
    # connection can describe not just how to REACH a box but how to turn it on
    # and what that costs: `instance_id`/`region` are what `compute/acquire` and
    # `compute/release` hand boto3, and `hourly_usd` is the number an
    # authorization gate shows a human before it spends. Absent them the
    # connection behaves exactly as before (SSH transport only) and the lifecycle
    # operators refuse with a cited reason rather than guessing an instance.
    "ssh": ProviderSpec("ssh", "compute.ssh", "Remote host via SSH (raw CPU/GPU compute)", {"PRIVATE_KEY": "SSH_PRIVATE_KEY"}, config={"host": "", "user": "", "port": "22", "known_hosts": "", "instance_id": "", "region": "", "hourly_usd": ""}, label="SSH host", category="ssh", auth_type="ssh_key"),
    # ── experiment tracking (kind=tracking.*) — #136 ──
    "mlflow": ProviderSpec("mlflow", "tracking.mlflow", "MLflow experiment tracking", {"TRACKING_URI": "MLFLOW_TRACKING_URI"}, config={"experiment": ""}, label="MLflow", icon="mlflow", category="tracking", auth_type="http_no_auth", probe=ProbeSpec(path="/health", base_url_key="TRACKING_URI", apply=APPLY_NONE)),
    "wandb": ProviderSpec("wandb", "tracking.wandb", "Weights & Biases", {"API_KEY": "WANDB_API_KEY"}, config={"project": ""}, label="Weights & Biases", icon="weightsandbiases", category="tracking", auth_type="api_key", probe=ProbeSpec(supported=False, unsupported_reason="W&B auth is verified via its GraphQL viewer over the SDK — no cheap public REST liveness.")),
    # ── observability (kind=api_key / http) ──
    "prometheus": ProviderSpec("prometheus", "http", "Prometheus — metrics query", config={"url": "http://localhost:9090"}, label="Prometheus", icon="prometheus", category="observability", auth_type="http_no_auth", probe=ProbeSpec(path="/-/healthy", base_url_key="url", apply=APPLY_NONE)),
    # Grafana: GET /api/org · Bearer (NOT /api/health / /api/user).
    "grafana": ProviderSpec("grafana", "api_key", "Grafana — dashboards + alerts", {"API_TOKEN": "GRAFANA_API_TOKEN"}, config={"url": ""}, label="Grafana", icon="grafana", category="observability", auth_type="bearer_token", probe=ProbeSpec(path="/api/org", base_url_key="url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_TOKEN")),
    "loki": ProviderSpec("loki", "http", "Loki — logs query", config={"url": ""}, label="Loki", category="observability", auth_type="http_no_auth", probe=ProbeSpec(path="/ready", base_url_key="url", apply=APPLY_NONE)),
    # Datadog: GET /api/v1/validate · DD-API-KEY.
    "datadog": ProviderSpec("datadog", "api_key", "Datadog — metrics/logs/traces", {"API_KEY": "DD_API_KEY", "APP_KEY": "DD_APP_KEY"}, config={"site": "datadoghq.com"}, label="Datadog", icon="datadog", category="observability", auth_type="api_key", probe=ProbeSpec(path="/api/v1/validate", base_url="https://api.datadoghq.com", apply=APPLY_HEADER, header_name="DD-API-KEY", secret_key="API_KEY")),
    # Sentry: GET /api/0/organizations/{org}/ · Bearer.
    "sentry": ProviderSpec("sentry", "api_key", "Sentry — errors + releases", {"AUTH_TOKEN": "SENTRY_AUTH_TOKEN"}, config={"org": "", "project": ""}, label="Sentry", icon="sentry", category="observability", auth_type="bearer_token", probe=ProbeSpec(path="/api/0/organizations/{org}/", base_url="https://sentry.io", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="AUTH_TOKEN")),
    "opentelemetry": ProviderSpec("opentelemetry", "http", "OpenTelemetry Collector (OTLP)", config={"endpoint": "http://localhost:4318"}, label="OpenTelemetry", icon="opentelemetry", category="observability", auth_type="http_no_auth", probe=ProbeSpec(supported=False, unsupported_reason="OTLP collectors expose no standard liveness endpoint — reachable-only, no credential to verify.")),
    # ── object storage (kind=api_key / http) ──
    # S3/R2: ListObjectsV2 max-keys=1 (SigV4 signing) — public_private_key.
    "s3": ProviderSpec("s3", "api_key", "AWS S3 / MinIO — artifacts + datasets", {"ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID", "SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY"}, config={"bucket": "", "endpoint_url": "", "region": ""}, label="S3 / MinIO", icon="amazons3", category="storage", auth_type="public_private_key"),
    "gcs": ProviderSpec("gcs", "api_key", "Google Cloud Storage", {"SERVICE_ACCOUNT_JSON": "GOOGLE_APPLICATION_CREDENTIALS"}, config={"bucket": ""}, label="GCS", icon="googlecloud", category="storage", auth_type="public_private_key", probe=ProbeSpec(supported=False, unsupported_reason="GCS auth is service-account-JWT signed — verified via the storage client, not a generic HTTP probe.")),
    "r2": ProviderSpec("r2", "api_key", "Cloudflare R2", {"ACCESS_KEY_ID": "R2_ACCESS_KEY_ID", "SECRET_ACCESS_KEY": "R2_SECRET_ACCESS_KEY"}, config={"account_id": "", "bucket": ""}, label="Cloudflare R2", icon="cloudflare", category="storage", auth_type="public_private_key"),
    # ── vector DB (kind=api_key / http) — RAG ──
    # pgvector/postgres: connect DSN → SELECT 1.
    "pgvector": ProviderSpec("pgvector", "http", "pgvector — Postgres vector store", config={"dsn": ""}, label="pgvector", icon="postgresql", category="vectordb", auth_type="basic", probe=ProbeSpec(base_url_key="dsn")),
    # Qdrant: GET /collections · api-key header.
    "qdrant": ProviderSpec("qdrant", "api_key", "Qdrant — vector DB", {"API_KEY": "QDRANT_API_KEY"}, config={"url": "http://localhost:6333"}, label="Qdrant", icon="qdrant", category="vectordb", auth_type="api_key", probe=ProbeSpec(path="/collections", base_url_key="url", apply=APPLY_HEADER, header_name="api-key", secret_key="API_KEY")),
    "chroma": ProviderSpec("chroma", "http", "Chroma — local/dev vector DB", config={"url": "http://localhost:8000"}, label="Chroma", category="vectordb", auth_type="http_no_auth", probe=ProbeSpec(path="/api/v2/heartbeat", base_url_key="url", apply=APPLY_NONE)),
    # Pinecone: GET https://api.pinecone.io/indexes · Api-Key.
    "pinecone": ProviderSpec("pinecone", "api_key", "Pinecone — managed vector DB", {"API_KEY": "PINECONE_API_KEY"}, config={"environment": ""}, label="Pinecone", icon="pinecone", category="vectordb", auth_type="api_key", probe=ProbeSpec(path="/indexes", base_url="https://api.pinecone.io", apply=APPLY_HEADER, header_name="Api-Key", secret_key="API_KEY")),
    # Weaviate: GET /v1/meta · Bearer.
    "weaviate": ProviderSpec("weaviate", "api_key", "Weaviate — vector DB", {"API_KEY": "WEAVIATE_API_KEY"}, config={"url": ""}, label="Weaviate", category="vectordb", auth_type="bearer_token", probe=ProbeSpec(path="/v1/meta", base_url_key="url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    # Milvus/Zilliz: POST {cluster}/v2/vectordb/collections/list · Bearer(user:pass).
    "milvus": ProviderSpec("milvus", "api_key", "Milvus / Zilliz — vector DB", {"TOKEN": "MILVUS_TOKEN"}, config={"url": ""}, label="Milvus / Zilliz", icon="milvus", category="vectordb", auth_type="bearer_token", probe=ProbeSpec(method="POST", path="/v2/vectordb/collections/list", base_url_key="url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="TOKEN", json_body={})),
    # ── database / warehouse (external query operators) ──
    # #1815 — `database` kind (was `http`: a DSN is not an HTTP base URL, and the
    # generic prober answered "no base URL configured (set the provider's
    # url/host)", which is advice a Postgres DSN can never take). Probe stance
    # now matches snowflake/bigquery: degrade honestly, never a false tick.
    # Existing rows keep whatever kind they were stored with — nothing dispatches
    # on `kind` for these, and the ingest operator resolves BY NAME.
    "postgres": ProviderSpec("postgres", "database", "Postgres — query/schema + ingestion source", {"DSN": "POSTGRES_DSN"}, label="Postgres", icon="postgresql", category="database", auth_type="basic", probe=ProbeSpec(supported=False, unsupported_reason="Postgres auth is a driver connect (SELECT 1) over libpq, not a generic HTTP probe — verified by the operator that uses it.")),
    "mysql": ProviderSpec("mysql", "database", "MySQL / MariaDB — query + ingestion source", {"DSN": "MYSQL_DSN"}, label="MySQL", icon="mysql", category="database", auth_type="basic", probe=ProbeSpec(supported=False, unsupported_reason="MySQL auth is a driver connect (SELECT 1) over the MySQL wire protocol, not a generic HTTP probe — verified by the operator that uses it.")),
    # #1823 — DSN alongside PASSWORD so `ingest/pull` can reach it: the
    # snowflake-sqlalchemy URL carries account/warehouse/role, which no
    # combination of the config keys below can express unambiguously.
    "snowflake": ProviderSpec("snowflake", "database", "Snowflake — warehouse query + ingestion source", {"PASSWORD": "SNOWFLAKE_PASSWORD", "DSN": "SNOWFLAKE_DSN"}, config={"account": "", "user": "", "warehouse": ""}, label="Snowflake", icon="snowflake", category="database", auth_type="basic", probe=ProbeSpec(supported=False, unsupported_reason="Snowflake auth is a warehouse connect (SELECT 1) over the connector, not a generic HTTP probe — verified by the operator that uses it.")),
    # #1823 — BigQuery is the one warehouse whose URL holds NO credential:
    # `bigquery://project/dataset` authenticates through the ambient service
    # account (GOOGLE_APPLICATION_CREDENTIALS, the secret ref below). So its
    # URL is CONFIG, and resolve_dsn reads it from there — refusing any
    # config URL that does carry userinfo, since config is plain JSON on the
    # row rather than the SecretStore.
    "bigquery": ProviderSpec("bigquery", "database", "BigQuery — warehouse query + ingestion source", {"SERVICE_ACCOUNT_JSON": "GOOGLE_APPLICATION_CREDENTIALS"}, config={"project": "", "dsn": ""}, label="BigQuery", icon="googlebigquery", category="database", auth_type="public_private_key", probe=ProbeSpec(supported=False, unsupported_reason="BigQuery auth is service-account-JWT signed — verified via the BigQuery client, not a generic HTTP probe.")),
    # #1823 — the probe STAYS an HTTP one: unlike its neighbours, ClickHouse
    # genuinely answers `SELECT 1` over :8123, which is exactly how the
    # clickstack observability backend already reads it. DSN is additive, for
    # the ingestion path (clickhouse+http:// via clickhouse-sqlalchemy).
    "clickhouse": ProviderSpec("clickhouse", "database", "ClickHouse — analytics/logs + ingestion source", {"PASSWORD": "CLICKHOUSE_PASSWORD", "DSN": "CLICKHOUSE_DSN"}, config={"url": "", "user": "default"}, label="ClickHouse", icon="clickhouse", category="database", auth_type="basic", probe=ProbeSpec(method="GET", path="/?query=SELECT%201", base_url_key="url", apply=APPLY_BASIC, secret_key="user", basic_password_key="PASSWORD")),
    # ── SaaS (kind=oauth / api_key) ──
    "github": ProviderSpec("github", "oauth", "GitHub — repo, PRs, Actions", config={"broker": ""}, label="GitHub", icon="github", category="saas", auth_type="oauth2"),
    "gitlab": ProviderSpec("gitlab", "api_key", "GitLab — repo, MRs, pipelines", {"TOKEN": "GITLAB_TOKEN"}, config={"url": "https://gitlab.com"}, label="GitLab", icon="gitlab", category="saas", auth_type="bearer_token", probe=ProbeSpec(path="/api/v4/user", base_url_key="url", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="TOKEN")),
    "slack": ProviderSpec("slack", "api_key", "Slack — notify + approvals", {"BOT_TOKEN": "SLACK_BOT_TOKEN"}, label="Slack", icon="slack", category="saas", auth_type="bearer_token", probe=ProbeSpec(path="/api/auth.test", base_url="https://slack.com", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="BOT_TOKEN")),
    "linear": ProviderSpec("linear", "api_key", "Linear — issues", {"API_KEY": "LINEAR_API_KEY"}, label="Linear", icon="linear", category="saas", auth_type="api_key", probe=ProbeSpec(method="POST", path="/graphql", base_url="https://api.linear.app", apply=APPLY_HEADER, header_name="Authorization", secret_key="API_KEY", json_body={"query": "{ viewer { id } }"})),
    "jira": ProviderSpec("jira", "api_key", "Jira — issues", {"API_TOKEN": "JIRA_API_TOKEN"}, config={"url": "", "email": ""}, label="Jira", icon="jira", category="saas", auth_type="basic", probe=ProbeSpec(path="/rest/api/3/myself", base_url_key="url", apply=APPLY_BASIC, secret_key="email", basic_password_key="API_TOKEN")),
    "notion": ProviderSpec("notion", "api_key", "Notion — docs/runbooks", {"TOKEN": "NOTION_TOKEN"}, label="Notion", icon="notion", category="saas", auth_type="bearer_token", probe=ProbeSpec(method="POST", path="/v1/search", base_url="https://api.notion.com", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="TOKEN", json_body={"page_size": 1})),
    "pagerduty": ProviderSpec("pagerduty", "api_key", "PagerDuty — incidents", {"API_KEY": "PAGERDUTY_API_KEY"}, label="PagerDuty", icon="pagerduty", category="saas", auth_type="api_key", probe=ProbeSpec(path="/users?limit=1", base_url="https://api.pagerduty.com", apply=APPLY_HEADER, header_name="Authorization", prefix="Token token=", secret_key="API_KEY")),
    # ── tracing / eval (LLM observability) ──
    # Langfuse: GET {host}/api/public/projects · Basic ($PUBLIC:$SECRET) — NOT /health.
    "langfuse": ProviderSpec("langfuse", "api_key", "Langfuse — LLM tracing + eval (MIT)", {"PUBLIC_KEY": "LANGFUSE_PUBLIC_KEY", "SECRET_KEY": "LANGFUSE_SECRET_KEY"}, config={"host": "https://cloud.langfuse.com"}, label="Langfuse", category="observability", auth_type="basic", probe=ProbeSpec(path="/api/public/projects", base_url_key="host", apply=APPLY_BASIC, secret_key="PUBLIC_KEY", basic_password_key="SECRET_KEY")),
    # LangSmith: GET /api/v1/sessions?limit=1 · x-api-key.
    "langsmith": ProviderSpec("langsmith", "api_key", "LangSmith — LangChain tracing + eval", {"API_KEY": "LANGSMITH_API_KEY"}, config={"host": "https://api.smith.langchain.com"}, label="LangSmith", category="observability", auth_type="api_key", probe=ProbeSpec(path="/api/v1/sessions?limit=1", base_url_key="host", apply=APPLY_HEADER, header_name="x-api-key", secret_key="API_KEY")),
    # Arize Phoenix: GET /v1/projects · Bearer.
    "arize_phoenix": ProviderSpec("arize_phoenix", "api_key", "Arize Phoenix — open-source LLM tracing", {"API_KEY": "PHOENIX_API_KEY"}, config={"host": "http://localhost:6006"}, label="Arize Phoenix", category="observability", auth_type="bearer_token", probe=ProbeSpec(path="/v1/projects", base_url_key="host", apply=APPLY_HEADER, header_name="Authorization", prefix="Bearer ", secret_key="API_KEY")),
    # Supabase: GET {ref}.supabase.co/rest/v1/ · apikey.
    # #1823 — Supabase needs NO new ingestion code: it is Postgres, so its DSN
    # works with `ingest/pull` exactly as a self-hosted Postgres does. The
    # API_KEY + REST probe stay, because they are how its OTHER half is reached.
    "supabase": ProviderSpec("supabase", "api_key", "Supabase — Postgres + auth + storage", {"API_KEY": "SUPABASE_KEY", "DSN": "SUPABASE_DSN"}, config={"url": ""}, label="Supabase", icon="supabase", category="database", auth_type="api_key", probe=ProbeSpec(path="/rest/v1/", base_url_key="url", apply=APPLY_HEADER, header_name="apikey", secret_key="API_KEY")),
    # ── MCP servers (kind=mcp; transports: stdio · streamable_http · sse) — behind the policy gateway (#69) ──
    "mcp": ProviderSpec("mcp", "mcp", "Custom MCP server (any transport)", config={"transport": "stdio", "command": "", "args": "", "url": ""}, label="Custom MCP server", category="mcp", transports=["stdio", "streamable_http", "sse"]),
    "github-mcp": ProviderSpec("github-mcp", "mcp", "GitHub MCP — repo/PR/issues", config={"transport": "streamable_http", "url": "https://api.githubcopilot.com/mcp/"}, label="GitHub MCP", icon="github", category="mcp", transports=["streamable_http", "stdio"]),
    "filesystem-mcp": ProviderSpec("filesystem-mcp", "mcp", "Filesystem MCP — local files", config={"transport": "stdio", "command": "npx", "args": "-y @modelcontextprotocol/server-filesystem"}, label="Filesystem MCP", category="mcp", transports=["stdio"]),
    "git-mcp": ProviderSpec("git-mcp", "mcp", "Git MCP — diff/history/changed-files", config={"transport": "stdio", "command": "uvx", "args": "mcp-server-git"}, label="Git MCP", icon="git", category="mcp", transports=["stdio"]),
    "postgres-mcp": ProviderSpec("postgres-mcp", "mcp", "Postgres MCP — query/schema", config={"transport": "stdio", "command": "npx", "args": "-y @modelcontextprotocol/server-postgres"}, label="Postgres MCP", icon="postgresql", category="mcp", transports=["stdio"]),
    "playwright-mcp": ProviderSpec("playwright-mcp", "mcp", "Playwright MCP — browser/web", config={"transport": "stdio", "command": "npx", "args": "@playwright/mcp@latest"}, label="Playwright MCP", icon="playwright", category="mcp", transports=["stdio"]),
}

#: Gallery category order + labels (the filter pills the SPA renders).
CATALOG_CATEGORIES: list[tuple[str, str]] = [
    ("models", "Models"), ("gateway", "Gateways"), ("compute", "Compute / GPU"),
    ("ssh", "SSH"), ("mcp", "MCP servers"), ("tracking", "Tracking"),
    ("observability", "Observability"), ("storage", "Storage"),
    ("vectordb", "Vector DB"), ("database", "Database"), ("saas", "SaaS"),
]

# Friendly aliases → canonical provider key.
_PROVIDER_ALIASES = {
    "hf": "huggingface", "claude": "anthropic", "google": "gemini",
    "k8s": "kube", "kubernetes": "kube", "openai-api": "openai",
    "w&b": "wandb", "weights-and-biases": "wandb",
}


def resolve_provider_key(provider: str) -> Optional[str]:
    """Map a user-typed provider name (case-insensitive, with aliases) to a
    catalog key, or ``None`` if unknown."""
    key = provider.strip().lower()
    key = _PROVIDER_ALIASES.get(key, key)
    return key if key in PROVIDER_CATALOG else None


def connection_from_provider(
    provider: str,
    *,
    name: Optional[str] = None,
    config: Optional[dict[str, Any]] = None,
    secrets: Optional[dict[str, str]] = None,
    description: Optional[str] = None,
    enabled: bool = True,
    account_id: Optional[str] = None,
) -> Connection:
    """Build a :class:`Connection` from a catalog provider, layering caller
    overrides on top of the spec defaults. Raises ``KeyError`` if the provider
    is not in the catalog (after alias resolution).

    ``account_id`` stamps the owning account. For a tenant account (not the
    deployment's ``default``) the catalog's secret defaults, which name server
    environment variables, are not applied, and a caller-supplied secret ref must
    be one minted for that account (``ValueError`` otherwise)."""
    from cairn.core.secrets_store import ref_in_account, scoped_account  # lazy: crypto off import path

    key = resolve_provider_key(provider)
    if key is None:
        raise KeyError(provider)
    spec = PROVIDER_CATALOG[key]
    tenant = scoped_account(account_id) is not None
    for logical, ref in (secrets or {}).items():
        if not ref_in_account(str(ref), account_id):
            raise ValueError(f"secret {logical!r}: only a reference stored for this account is accepted")
    return Connection(
        name=name or spec.provider,
        kind=spec.kind,
        account_id=account_id or DEFAULT_ACCOUNT,
        config={**spec.config, **(config or {})},
        secrets={**({} if tenant else spec.secrets), **(secrets or {})},
        description=spec.description if description is None else description,
        enabled=enabled,
    )


class ResolvedConnection(BaseModel):
    """A connection with its secret refs resolved to values. Handle at the
    integration boundary only — do not log; ``repr`` masks the secret values."""

    name: str
    kind: str
    config: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    #: The account the row belongs to. A job that reconnects later, outside any
    #: workspace scope (a remote job's cancel), resolves the same row with it
    #: (``resolve_connection(name, account_id=...)``, ADR-0149 D4).
    account_id: str = ""

    def __repr__(self) -> str:  # never echo resolved secret values
        masked = {k: "••••••" for k in self.secrets}
        return f"ResolvedConnection(name={self.name!r}, kind={self.kind!r}, secrets={masked})"


# ─── compute.ssh → asyncssh params (ADR-0092 principle 3, #1544) ────────


def as_ssh_params(ctx: Any, *, operator: str) -> dict[str, Any]:
    """The asyncssh connect kwargs for the ``compute.ssh`` connection on ``ctx``.

    THE one place the compute.ssh → connect-params mapping lives. It was copied
    verbatim into ``remote/exec`` and ``serve/vllm`` (``_conn_params``), so the
    ``known_hosts or None`` MITM guard below existed in two files and one SSH
    host-key fix meant two edits (#1544 / ADR-0092 principle 3).

    ``operator`` names the caller in the "no connection bound" error, which is
    the only part the two copies differed in. ``ctx.compute`` is duck-typed
    (``.config`` / ``.secrets``) rather than typed to
    :class:`ResolvedConnection`, because that is the contract operators already
    rely on — the runtime binds a real ResolvedConnection, pack tests bind a
    stand-in.
    """
    compute = getattr(ctx, "compute", None)
    if compute is None:
        raise RuntimeError(
            f"{operator} needs a compute.ssh connection — run `cairn connect ssh` "
            "and bind it via execution.compute_target"
        )
    cfg = getattr(compute, "config", None) or {}
    host = cfg.get("host")
    if not host:
        raise RuntimeError("compute.ssh connection has no host")
    key = compute.secrets.get("PRIVATE_KEY") if getattr(compute, "secrets", None) else None
    return {
        "host": host, "user": cfg.get("user"), "port": int(cfg.get("port") or 22),
        # None → asyncssh default (~/.ssh/known_hosts), which REJECTS unknown hosts.
        # Never () — that disables host-key checking (MITM risk).
        "known_hosts": cfg.get("known_hosts") or None,
        "private_key": key,
    }


# ─── stores ─────────────────────────────────────────────────────────────


class InMemoryConnectionStore:
    """Volatile store (tests / no-config default).

    Every read/delete takes an optional ``account_id`` (ADR-0054, #392 phase 2):
    when set, the op is scoped to that tenant and a row in another account is
    invisible (``get``/``get_by_name`` → ``None``, ``list`` excludes it,
    ``delete`` is a no-op). ``account_id=None`` means "unscoped" — an internal
    caller that already trusts the ref (e.g. runtime ``resolve_connection``); the
    API layer always passes a concrete account so callers can never leak across.
    """

    def __init__(self) -> None:
        self._by_id: dict[str, Connection] = {}

    async def create(self, conn: Connection) -> Connection:
        self._by_id[conn.id] = conn
        return conn

    async def get(self, conn_id: str, account_id: Optional[str] = None) -> Optional[Connection]:
        c = self._by_id.get(conn_id)
        if c is None or (account_id is not None and c.account_id != account_id):
            return None
        return c

    async def get_by_name(self, name: str, account_id: Optional[str] = None) -> Optional[Connection]:
        return next(
            (c for c in self._by_id.values() if c.name == name
             and (account_id is None or c.account_id == account_id)),
            None,
        )

    async def list(self, account_id: Optional[str] = None) -> list[Connection]:
        conns = self._by_id.values()
        if account_id is not None:
            conns = [c for c in conns if c.account_id == account_id]
        return sorted(conns, key=lambda c: c.name)

    async def update(self, conn_id: str, **fields: Any) -> Connection:
        cur = self._by_id.get(conn_id)
        if cur is None:
            raise KeyError(conn_id)
        updated = cur.model_copy(update={**fields, "updated_at": utc_now_iso()})
        self._by_id[conn_id] = updated
        return updated

    async def delete(self, conn_id: str, account_id: Optional[str] = None) -> bool:
        c = self._by_id.get(conn_id)
        if c is None or (account_id is not None and c.account_id != account_id):
            return False
        return self._by_id.pop(conn_id, None) is not None

    def close(self) -> None:  # parity with the sqlite store
        pass


class SqliteConnectionStore:
    """Embedded-SQLite store — restart-safe, single file. Mirrors
    :class:`SqliteRunStore`: full Connection in a JSON ``data`` blob, sqlite work
    via ``asyncio.to_thread`` under a lock so it never blocks the event loop."""

    def __init__(self, path: str | None = None) -> None:
        import sqlite3

        if path is None:
            from cairn.core import settings

            # single shared cairn.db (#427/ADR-0058); CAIRN_*_DB overrides
            path = getenv("CAIRN_CONNECTIONS_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id    TEXT PRIMARY KEY,
                name  TEXT UNIQUE,
                kind  TEXT,
                account_id TEXT NOT NULL DEFAULT 'default',
                data  TEXT NOT NULL
            )
            """
        )
        self._migrate()
        self._conn.commit()
        self._lock = asyncio.Lock()

    def _migrate(self) -> None:
        """Bring a pre-#392 ``connections`` table (no ``account_id`` column) up to
        the account-scoped schema (ADR-0054 phase 2). Adds the indexed
        ``account_id`` column via the shared migration helper and back-fills every
        existing row to the ``default`` account — so a running deploy's connectors
        stay reachable to their (single) tenant. Idempotent: a no-op once current,
        safe on every connect. Without it, ``SELECT … WHERE account_id=?`` raised
        ``no such column: account_id`` after the upgrade."""
        # lazy: shared sqlite-migration primitives (ADR-0050); import-cheap stdlib-
        # only module — kept local to sit next to the one call site it serves.
        from cairn.core.storage import migrations

        added = migrations.add_missing_columns(
            self._conn, "connections",
            {"account_id": f"TEXT NOT NULL DEFAULT '{DEFAULT_ACCOUNT}'"},
        )
        if "account_id" in added:
            # existing rows also carry account_id inside their JSON `data` blob; the
            # column default fixed the column, but the blob still reads pre-#392 (the
            # model defaults it to `default` on validate, so it agrees) — nothing more
            # to rewrite. New writes stamp both.
            self._conn.execute(
                f"UPDATE connections SET account_id='{DEFAULT_ACCOUNT}' WHERE account_id IS NULL"
            )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_connections_account ON connections (account_id)"
        )

    def _create_sync(self, conn: Connection) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO connections (id, name, kind, account_id, data) VALUES (?,?,?,?,?)",
            (conn.id, conn.name, conn.kind, conn.account_id, conn.model_dump_json()),
        )
        self._conn.commit()

    async def create(self, conn: Connection) -> Connection:
        async with self._lock:
            await asyncio.to_thread(self._create_sync, conn)
        return conn

    def _get_sync(self, sql: str, params: tuple) -> Optional[Connection]:
        row = self._conn.execute(sql, params).fetchone()
        return Connection.model_validate_json(row[0]) if row else None

    async def get(self, conn_id: str, account_id: Optional[str] = None) -> Optional[Connection]:
        # account_id filter is applied in SQL so another tenant's row never even
        # loads (defence at the store, not just the API). None → unscoped (trusted
        # internal caller, e.g. runtime resolve); a value → deny cross-account.
        if account_id is None:
            sql, params = "SELECT data FROM connections WHERE id=?", (conn_id,)
        else:
            sql, params = "SELECT data FROM connections WHERE id=? AND account_id=?", (conn_id, account_id)
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, sql, params)

    async def get_by_name(self, name: str, account_id: Optional[str] = None) -> Optional[Connection]:
        if account_id is None:
            sql, params = "SELECT data FROM connections WHERE name=?", (name,)
        else:
            sql, params = "SELECT data FROM connections WHERE name=? AND account_id=?", (name, account_id)
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, sql, params)

    def _list_sync(self, account_id: Optional[str]) -> list[Connection]:
        if account_id is None:
            rows = self._conn.execute(
                "SELECT data FROM connections ORDER BY name ASC"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT data FROM connections WHERE account_id=? ORDER BY name ASC", (account_id,)
            ).fetchall()
        return [Connection.model_validate_json(r[0]) for r in rows]

    async def list(self, account_id: Optional[str] = None) -> list[Connection]:
        async with self._lock:
            return await asyncio.to_thread(self._list_sync, account_id)

    def _update_sync(self, conn_id: str, fields: dict) -> Connection:
        row = self._conn.execute(
            "SELECT data FROM connections WHERE id=?", (conn_id,)
        ).fetchone()
        if row is None:
            raise KeyError(conn_id)
        updated = Connection.model_validate_json(row[0]).model_copy(
            update={**fields, "updated_at": utc_now_iso()}
        )
        self._conn.execute(
            "UPDATE connections SET name=?, kind=?, data=? WHERE id=?",
            (updated.name, updated.kind, updated.model_dump_json(), conn_id),
        )
        self._conn.commit()
        return updated

    async def update(self, conn_id: str, **fields: Any) -> Connection:
        async with self._lock:
            return await asyncio.to_thread(self._update_sync, conn_id, fields)

    def _delete_sync(self, conn_id: str, account_id: Optional[str]) -> bool:
        # Scope the DELETE itself so a cross-account delete removes nothing and
        # returns False — the endpoint maps that to a 404 (no existence leak).
        if account_id is None:
            cur = self._conn.execute("DELETE FROM connections WHERE id=?", (conn_id,))
        else:
            cur = self._conn.execute(
                "DELETE FROM connections WHERE id=? AND account_id=?", (conn_id, account_id)
            )
        self._conn.commit()
        return cur.rowcount > 0

    async def delete(self, conn_id: str, account_id: Optional[str] = None) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._delete_sync, conn_id, account_id)

    def close(self) -> None:
        self._conn.close()


# ── Postgres backend (mirrors storage/postgres/runs.py) — #427 ─────────────
# account_id is a first-class indexed column (ADR-0054 #392 phase 2), parity with
# the sqlite store: every read/delete is scoped by it so one tenant never sees
# another's connectors. `ADD COLUMN IF NOT EXISTS` + the index migrate an existing
# prod table in place (Postgres back-fills the NOT NULL DEFAULT for old rows).
_CREATE_CONNS_PG_SQL = """
    CREATE TABLE IF NOT EXISTS connections (
        id    TEXT PRIMARY KEY,
        name  TEXT UNIQUE,
        kind  TEXT,
        account_id TEXT NOT NULL DEFAULT 'default',
        data  JSONB NOT NULL
    )
"""
_MIGRATE_CONNS_PG_SQL = (
    "ALTER TABLE connections ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT 'default'",
    "CREATE INDEX IF NOT EXISTS idx_connections_account ON connections (account_id)",
)
_INSERT_CONN_PG_SQL = """
    INSERT INTO connections (id, name, kind, account_id, data)
    VALUES ($1, $2, $3, $4, $5::jsonb)
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name, kind = EXCLUDED.kind,
        account_id = EXCLUDED.account_id, data = EXCLUDED.data
"""
_SELECT_CONN_PG_BY_ID = "SELECT data::text FROM connections WHERE id = $1"
_SELECT_CONN_PG_BY_ID_ACCT = "SELECT data::text FROM connections WHERE id = $1 AND account_id = $2"
_SELECT_CONN_PG_BY_NAME = "SELECT data::text FROM connections WHERE name = $1"
_SELECT_CONN_PG_BY_NAME_ACCT = "SELECT data::text FROM connections WHERE name = $1 AND account_id = $2"
_LIST_CONNS_PG_SQL = "SELECT data::text FROM connections ORDER BY name ASC"
_LIST_CONNS_PG_ACCT_SQL = "SELECT data::text FROM connections WHERE account_id = $1 ORDER BY name ASC"
_UPDATE_CONN_PG_SQL = "UPDATE connections SET name = $2, kind = $3, data = $4::jsonb WHERE id = $1"
_DELETE_CONN_PG_SQL = "DELETE FROM connections WHERE id = $1"
_DELETE_CONN_PG_ACCT_SQL = "DELETE FROM connections WHERE id = $1 AND account_id = $2"


class PostgresConnectionStore(PgSchemaInit):
    """Postgres-backed connection store — mirrors :class:`SqliteConnectionStore`
    (full Connection in a JSONB ``data`` column) and :class:`PostgresRunStore`
    (lazy asyncpg pool, created on first call). Selected by
    :func:`default_connection_store` when ``CAIRN_STORAGE=postgres://…`` so
    connections live in the one prod Postgres instead of a per-container SQLite
    file — the storage-unification move for this store (#427)."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn
        self._pool = None
        self._schema_ready = False

    async def _open_pool(self):
        try:
            import asyncpg  # noqa: PLC0415 — optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "Postgres connection store requires the 'storage-postgres' optional "
                "dep group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "PostgresConnectionStore needs a DSN. Pass dsn= or set CAIRN_STORAGE=postgres://…"
            )
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        # #614: one pool per DSN, shared across every Postgres store in this process.
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as c:
            await c.execute(_CREATE_CONNS_PG_SQL)
            for stmt in _MIGRATE_CONNS_PG_SQL:  # add account_id + index to a pre-#392 table
                await c.execute(stmt)

    async def create(self, conn: Connection) -> Connection:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            await c.execute(
                _INSERT_CONN_PG_SQL, conn.id, conn.name, conn.kind, conn.account_id, conn.model_dump_json()
            )
        return conn

    async def get(self, conn_id: str, account_id: Optional[str] = None) -> Optional[Connection]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = (
                await c.fetchrow(_SELECT_CONN_PG_BY_ID, conn_id) if account_id is None
                else await c.fetchrow(_SELECT_CONN_PG_BY_ID_ACCT, conn_id, account_id)
            )
        return Connection.model_validate_json(row["data"]) if row else None

    async def get_by_name(self, name: str, account_id: Optional[str] = None) -> Optional[Connection]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = (
                await c.fetchrow(_SELECT_CONN_PG_BY_NAME, name) if account_id is None
                else await c.fetchrow(_SELECT_CONN_PG_BY_NAME_ACCT, name, account_id)
            )
        return Connection.model_validate_json(row["data"]) if row else None

    async def list(self, account_id: Optional[str] = None) -> list[Connection]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            rows = (
                await c.fetch(_LIST_CONNS_PG_SQL) if account_id is None
                else await c.fetch(_LIST_CONNS_PG_ACCT_SQL, account_id)
            )
        return [Connection.model_validate_json(r["data"]) for r in rows]

    async def update(self, conn_id: str, **fields: Any) -> Connection:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            async with c.transaction():
                row = await c.fetchrow(_SELECT_CONN_PG_BY_ID, conn_id)
                if row is None:
                    raise KeyError(conn_id)
                updated = Connection.model_validate_json(row["data"]).model_copy(
                    update={**fields, "updated_at": utc_now_iso()}
                )
                await c.execute(_UPDATE_CONN_PG_SQL, conn_id, updated.name, updated.kind, updated.model_dump_json())
        return updated

    async def delete(self, conn_id: str, account_id: Optional[str] = None) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = (
                await c.execute(_DELETE_CONN_PG_SQL, conn_id) if account_id is None
                else await c.execute(_DELETE_CONN_PG_ACCT_SQL, conn_id, account_id)
            )
        return status.split()[-1] != "0"  # asyncpg returns e.g. "DELETE 1"

    def close(self) -> None:
        # The pool lives for the process (parity with SqliteConnectionStore.close,
        # which closes its single conn); pool teardown happens with the event loop.
        pass


def default_connection_store():
    """Pick a connection store from the env. **`CAIRN_STORAGE` is the selector**
    (#427): ``postgres://…`` → :class:`PostgresConnectionStore` (the one prod
    Postgres). Otherwise durable-by-default sqlite at ``~/.cairn/connections.db``
    (like the run store) so connections added via ``cairn connect`` / the admin
    panel persist and are visible to ``resolve_connection`` at runtime — the
    three must agree on where connections live. ``CAIRN_CONNECTIONS_DB`` overrides
    the sqlite path; ``CAIRN_CONNECTIONS_DB=:memory:`` opts into the volatile
    in-memory store (tests otherwise construct stores explicitly)."""
    url = getenv("CAIRN_STORAGE", "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresConnectionStore(dsn=url)
    if getenv("CAIRN_CONNECTIONS_DB", "").strip() == ":memory:":
        return InMemoryConnectionStore()
    return SqliteConnectionStore()


async def resolve_connection_obj(conn: Connection) -> Optional[ResolvedConnection]:
    """Resolve an in-hand :class:`Connection`'s secret refs to values via the
    bound SecretStore (ADR-0033), falling back to ``os.environ``. Returns
    ``None`` if the connection is disabled. Split out from
    :func:`resolve_connection` so callers that already hold the row (e.g. the
    health prober) don't re-hit the store.

    The row's ``account_id`` scopes resolution: a tenant account's connection
    resolves only references minted for that account (no bound SecretStore, no
    ``os.environ`` fallback, no other account's), and its oauth token is looked up
    under its own id. The deployment's (``default``) connections are unchanged."""
    if not conn.enabled:
        return None
    from cairn.core.secrets_store import scoped_account  # lazy: crypto off import path

    tenant = scoped_account(conn.account_id) is not None

    # kind=oauth: a broker (#100) owns the auth-code dance + token refresh.
    # Hand back a FRESH access token instead of resolving static secret refs, so
    # downstream operators consume it identically to an api_key connection — the
    # broker is invisible below this call. Replaceable: the broker name comes
    # from config["broker"] (default CAIRN_OAUTH_BROKER), never a vendor import here.
    if conn.kind == "oauth":
        # lazy: keep connections.py import-light + avoid an import cycle.
        from cairn.core.oauth import get_oauth_broker

        broker = get_oauth_broker(conn.config.get("broker"))
        token = await broker.get_access_token(
            provider=str(conn.config.get("provider", "")),
            # a tenant's connection reads its own token, never one another row names
            connection_id=conn.name if tenant else str(conn.config.get("connection_id") or conn.name),
        )
        return ResolvedConnection(
            name=conn.name,
            kind=conn.kind,
            config=dict(conn.config),
            secrets={"ACCESS_TOKEN": token},
            account_id=conn.account_id,
        )

    # Lazy: avoid import-time coupling to the operator/secret stack.
    from cairn.core.operator.secrets import get_secret_provider
    from cairn.core.secrets_store import resolve_secret  # writable backends (#200)

    if tenant:
        resolved = {
            key: resolve_secret(ref, account_id=conn.account_id) or "" for key, ref in conn.secrets.items()
        }
        return ResolvedConnection(name=conn.name, kind=conn.kind, config=dict(conn.config), secrets=resolved,
                                  account_id=conn.account_id)

    provider = get_secret_provider()
    resolved: dict[str, str] = {}
    for key, ref in conn.secrets.items():
        val = None
        if provider is not None:
            try:
                val = provider.get(ref)
            except Exception:  # noqa: BLE001 — undeclared/missing → store/env fallback
                val = None
        if not val:
            # writable backends (cairn:// local-encrypted-file, infisical://) +
            # env / bare-name refs (resolve_secret handles all of these), #200.
            val = resolve_secret(ref) or os.environ.get(ref, "")
        resolved[key] = val
    return ResolvedConnection(
        name=conn.name, kind=conn.kind, config=dict(conn.config), secrets=resolved,
        account_id=conn.account_id,
    )


async def run_account() -> str:
    """The account the current run resolves connections in: the owner of
    :func:`~cairn.core.workspace.current_workspace` (the dispatch layer scopes it
    from the run's record). With auth off, ``default`` — the deployment — for any
    context with no workspace in scope. With auth on, no workspace in scope is
    refused (:class:`~cairn.core.workspace.WorkspaceScopeRequired`), as is a
    workspace no store holds; the deployment's own tools pass ``account_id``."""
    from cairn.core.workspace import workspace_account  # noqa: PLC0415 — lazy: keep this module import-light

    return await workspace_account()


def lookup_account(account_id: Optional[str]) -> str:
    """The ``account_id`` a store read is filtered on: the tenant's own id, or
    ``default`` for the deployment (no account, empty, or ``default``)."""
    from cairn.core.secrets_store import scoped_account  # noqa: PLC0415 — lazy: crypto off import path

    return DEFAULT_ACCOUNT if scoped_account(account_id) is None else str(account_id).strip()


async def resolve_connection(
    name: str, *, store=None, account_id: Optional[str] = None
) -> Optional[ResolvedConnection]:
    """Resolve a connection by name: load it from the store, then resolve its
    secret refs (see :func:`resolve_connection_obj`). Returns ``None`` if the
    connection is unknown or disabled. The runtime/operator wiring.

    The lookup is scoped to one account: ``account_id``, or when omitted the
    current run's (:func:`run_account`). A tenant account resolves only its own
    rows; the deployment (``default``) only the deployment's. A single-node or
    auth-off deployment, where every row is ``default``, is unchanged."""
    acct = lookup_account(account_id if account_id is not None else await run_account())
    store = store or default_connection_store()
    close = getattr(store, "close", None)
    try:
        conn = await store.get_by_name(name, account_id=acct)
        if conn is None:
            return None
        return await resolve_connection_obj(conn)
    finally:
        if callable(close):
            close()


def _declared_targets(conn: Connection, wanted: set[str], *, by_catalog: bool) -> dict[str, str]:
    """``{logical key: declared target}`` for the keys of ``conn`` an operator's
    ``secrets:`` block asks for. A key matches when its ref IS the target (the
    catalog convention, ``{"KEY": "KAGGLE_KEY"}``). With ``by_catalog`` (a
    tenant's connection, whose references are minted for its account and are
    never an env name) it also matches when the key itself is the target
    (``{"HF_TOKEN": <ref>}``) or when the provider catalog entry named by the
    connection maps that key to the target."""
    out: dict[str, str] = {}
    defaults: dict[str, str] = {}
    if by_catalog:
        spec_key = resolve_provider_key(conn.name) or resolve_provider_key(str(conn.config.get("provider") or ""))
        defaults = dict(PROVIDER_CATALOG[spec_key].secrets) if spec_key else {}
    for logical, ref in (conn.secrets or {}).items():
        if ref in wanted:
            out[logical] = ref
        elif by_catalog and logical in wanted:
            out[logical] = logical
        elif defaults.get(logical) in wanted:
            out[logical] = defaults[logical]
    return out


async def declared_secret_values(
    declared: Iterable[str], *, account_id: Optional[str] = None, store=None
) -> dict[str, str]:
    """The values of an operator's DECLARED secrets, from ``account_id``'s own
    enabled connections (when omitted, the current run's account). Nothing is
    written anywhere; a key no connection supplies is simply absent. Never
    raises — a missing or broken connection supplies nothing."""
    wanted = {d for d in declared if d}
    if not wanted:
        return {}
    own = store is None
    try:
        acct = lookup_account(account_id if account_id is not None else await run_account())
        store = store or default_connection_store()
        try:
            conns = await store.list(acct)
        finally:
            close = getattr(store, "close", None)
            if own and callable(close):
                close()
    except Exception:  # noqa: BLE001 — cred bridging must never break dispatch
        logger.warning("[secrets] could not list connections for declared secrets", exc_info=True)
        return {}

    tenant = acct != DEFAULT_ACCOUNT
    values: dict[str, str] = {}
    for conn in conns:
        if not wanted:
            break
        if not getattr(conn, "enabled", True) or lookup_account(conn.account_id) != acct:
            continue
        targets = _declared_targets(conn, wanted, by_catalog=tenant)
        if not targets:
            continue
        try:
            resolved = await resolve_connection_obj(conn)
        except Exception:  # noqa: BLE001 — one broken connection must not hide the others
            logger.warning("[secrets] could not resolve connection %r", conn.name, exc_info=True)
            continue
        if resolved is None:
            continue
        for logical, target in targets.items():
            value = resolved.secrets.get(logical)
            if value and target in wanted:
                values[target] = str(value)
                wanted.discard(target)
    return values


async def inject_declared_secrets(
    declared: Iterable[str], *, account_id: Optional[str] = None
) -> list[str]:
    """Bridge the connection catalog → ``os.environ`` for an operator's DECLARED
    secrets (#532), for a DEPLOYMENT run only.

    An operator that needs a credential declares it in its binding
    (``secrets: [KAGGLE_USERNAME, KAGGLE_KEY, HF_TOKEN]``). Those keys are env-var
    *targets*. A stored connection maps each logical key to a SecretStore *ref*
    that is, by catalog convention, that same env-var name
    (``kaggle: {"USERNAME": "KAGGLE_USERNAME", "KEY": "KAGGLE_KEY"}``). So for
    each declared target we find the connection whose ref matches it, resolve the
    stored value, and set ``os.environ[target]`` — so both operators that read
    ``os.environ`` directly (``train/kaggle``, ``~/.kaggle``) and the env-backed
    SecretStore see the credential. Without this the catalog holds creds the
    operator can't see.

    Account scope: ``account_id`` (when omitted, the current run's account). The
    deployment's (``default``) connections are the only candidates; a tenant
    account's run writes NOTHING to the process environment, which every other
    run shares — its values reach the operator through ``ctx.secrets`` instead
    (:func:`declared_secret_values` + ``build_scoped_secrets``).

    Declared-scoped: only the exact targets the binding asks for are injected, so
    an unrelated step never gets another provider's secret. Existing env is NOT
    overridden (an explicit box env / test stub wins). Never raises — a missing or
    broken connection leaves the env exactly as it was."""
    from cairn.core.secrets_store import scoped_account  # noqa: PLC0415 — lazy: crypto off import path

    try:
        acct = account_id if account_id is not None else await run_account()
    except Exception:  # noqa: BLE001 — an unknown account injects nothing (never the deployment's by default)
        logger.warning("[secrets] could not determine the run's account; nothing injected", exc_info=True)
        return []
    if scoped_account(acct) is not None:
        return []
    wanted = [d for d in declared if d and not os.environ.get(d)]
    if not wanted:
        return []
    values = await declared_secret_values(wanted, account_id=DEFAULT_ACCOUNT)
    injected: list[str] = []
    for target in wanted:
        value = values.get(target)
        if value and not os.environ.get(target):
            os.environ[target] = value
            injected.append(target)
    if injected:
        logger.info("[secrets] injected from connections: %s", ", ".join(injected))
    return injected
