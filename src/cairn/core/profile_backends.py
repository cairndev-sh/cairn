"""Self-describing schema for a deployment's runtime backend capabilities (ADR-0052 §1).

One place declares every configurable backend the runtime exposes — its control
type (``toggle`` / ``select`` / ``text``), choices, default, help text, and the
environment variable it currently falls back to. Three consumers read this one
schema:

  - the console renders an Environment form from it (#303) — ``toggle`` → checkbox,
    ``select`` → dropdown (default pre-selected), ``text`` → input, ``description``
    → help text;
  - ``doctor`` validates a pack's declared ``requires`` against what the
    environment provisions (#304);
  - :func:`effective_backends` computes the value the runtime should use, with the
    precedence **profile record → environment variable → schema default**.

This module *describes* the existing knobs so they become configurable from one
surface; it does not itself rewire any consumer (they still read their env vars
today). Adding a backend = one entry in :data:`BACKENDS`; the form, the resolver,
and the docs all follow. Every capability keeps a zero-config safe default
(sqlite / inprocess / off), so a fresh checkout runs with an empty profile.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
from typing import Any, Literal, Mapping, Optional

from pydantic import BaseModel, Field

# Environment values counted as "on" for toggle backends (mirrors the ad-hoc
# checks in triggers/server.py, operator/runtime.py, bus/__init__.py).
_TRUTHY = {"1", "true", "yes", "on"}

# Form section order (the console groups fields by ``group`` in this order).
GROUPS: list[str] = ["State & storage", "RAG", "Memory", "Eventing", "Execution", "Data plane", "Models"]


class BackendOption(BaseModel):
    """One choice in a ``select`` backend field."""

    value: str
    label: str
    # #305 (ADR-0052 §3): the connection-catalog provider this choice binds its
    # credentials to (e.g. rag_vector_store="qdrant" → the "qdrant" provider). The
    # form offers a connection dropdown of that provider's connections; the profile
    # stores only the connection *name* (`<field>_connection`), never a secret.
    # ``None`` = a zero-config/local choice that needs no connection.
    connection_provider: Optional[str] = None


class BackendField(BaseModel):
    """One configurable backend capability — the self-describing unit the form
    renders and the resolver resolves."""

    key: str
    label: str
    group: str
    control: Literal["toggle", "select", "text"]
    default: Any = ""                       # bool for toggle; str for select/text
    description: str = ""
    options: list[BackendOption] = Field(default_factory=list)
    env: Optional[str] = None               # env var this falls back to (None = profile-only)
    env_is_url_kind: bool = False           # env holds a URL; derive the backend *kind* (storage)
    needs_connection: bool = False          # non-default choice binds a stored connection (§3 / #305)
    optional_on: Optional[str] = None       # only meaningful when this toggle key is on (UI reveal)


def _opts(*pairs: tuple) -> list[BackendOption]:
    """Build options from (value, label) or (value, label, connection_provider)."""
    out: list[BackendOption] = []
    for p in pairs:
        value, label = p[0], p[1]
        provider = p[2] if len(p) > 2 else None
        out.append(BackendOption(value=value, label=label, connection_provider=provider))
    return out


# ── The canonical inventory (ADR-0052 §1) ────────────────────────────────
# Ordered within groups; keys are the `profile.backends` keys the resolver reads.
BACKENDS: list[BackendField] = [
    # ── State & storage ──
    BackendField(
        key="storage", label="Run / event store", group="State & storage", control="select",
        options=_opts(("sqlite", "SQLite (local, single-node)"), ("postgres", "Postgres (multi-node)", "postgres")),
        default="sqlite", env="CAIRN_STORAGE", env_is_url_kind=True, needs_connection=True,
        description="Where runs, events, and the ledger persist. SQLite for local/single-node; Postgres for multi-node deployments (binds a Postgres connection).",
    ),
    BackendField(
        key="durable_runtime", label="Durable runtime", group="State & storage", control="select",
        options=_opts(("in_process", "In-process (dev)"), ("temporal", "Temporal (durable)"), ("dbos", "DBOS (batch)")),
        default="in_process", env="CAIRN_RUNTIME",
        description="Execution durability tier. In-process for dev/single-node; Temporal for cross-machine durable workflows; DBOS for Postgres-backed batch fan-out.",
    ),
    # ── Data plane (ADR-0081 D4) ──
    BackendField(
        key="dataflow_engine", label="Dataflow engine", group="Data plane", control="select",
        options=_opts(("builtin", "Builtin (one box / subprocess)"),
                      ("metaflow", "Metaflow (bridge deployment)"), ("ray", "Ray on KubeRay"),
                      ("skypilot", "SkyPilot (provisions the machine)")),
        default="builtin", env="CAIRN_DATAFLOW_ENGINE",
        description="Which engine executes dataflows submitted by dataflow/run (ADR-0081). Builtin runs cairn-dataflow-run on one machine; Metaflow/SkyPilot/Ray are adapter engines (#1054) — selecting an uninstalled adapter fails loudly at submit.",
    ),
    # #1442 — where a flow's EXPORT lands, i.e. how one leg's product reaches
    # the next: local (a path in the leg's workdir — one box only) or s3 (the
    # s3/put contract: CAIRN_ARTIFACT_S3_BUCKET/_PREFIX/_REGION, IAM/env creds).
    # The environment decides; a flow's export.* stage reads this when its own
    # `store` config is unset.
    BackendField(
        key="artifact_store", label="Artifact store", group="Data plane", control="select",
        options=_opts(("local", "Local (leg workdir — one box)"), ("s3", "S3 (CAIRN_ARTIFACT_S3_BUCKET)")),
        default="local", env="CAIRN_ARTIFACT_STORE",
        description="Where flow exports (the leg-to-leg data products) are written and registered: local path in the leg's workdir, or S3 under CAIRN_ARTIFACT_S3_BUCKET/CAIRN_ARTIFACT_S3_PREFIX so a downstream leg on ANY box can read them (#1442).",
    ),
    BackendField(
        key="dataflow_target", label="Dataflow compute target", group="Data plane", control="text",
        default="", env="CAIRN_DATAFLOW_TARGET", needs_connection=True,
        description="Optional compute.ssh connection the BUILTIN engine dispatches to (#1050 slice 3): set = cairn-dataflow-run launches on that box via the proven remote/exec machinery; empty = local subprocess. The box installs cairndev[dataflow-engine] so the binary + kernels exist there (the fix-remote deployment contract).",
    ),
    # ── ML plane (ADR-0082 D7, #1074) — kind: ml flows behind ml/run ──
    BackendField(
        key="ml_engine", label="ML flow engine", group="Data plane", control="select",
        options=_opts(("builtin", "Builtin (one box / subprocess)"), ("metaflow", "Metaflow (bridge deployment)"), ("skypilot", "SkyPilot (provisions the machine)")),
        default="builtin", env="CAIRN_ML_ENGINE",
        description="Which engine executes kind: ml flows submitted by ml/run (ADR-0082 D7). Engines are kind-blind stage interpreters over the shared kernel registry — builtin runs sequentially on one machine; Metaflow/other bridges (#1078) map per-stage resources natively. Selecting an uninstalled engine fails loudly at submit.",
    ),
    BackendField(
        key="ml_target", label="ML compute target", group="Data plane", control="text",
        default="", env="CAIRN_ML_TARGET", needs_connection=True,
        description="Optional compute.ssh connection the BUILTIN ML engine dispatches to (the #1050 pattern): set = the flow runs on that box (which installs cairn-operator-training[local] for real kernels); empty = local subprocess.",
    ),
    # ── Remote execution unit (ADR-0104 D1, #2071). A box carries drivers +
    # a container runtime and NOTHING of cairn's: the hand-installed venv on
    # the GPU box drifted since August and stopped carrying the flow kernel the
    # fine-tune flow needs (#2069), which is a deployment failure wearing a
    # training failure's clothes. These two keys are what an environment sets
    # to move the SSH path onto the CI-built image.
    BackendField(
        key="remote_exec_mode", label="Remote execution mode", group="Data plane", control="select",
        options=_opts(("auto", "Auto (image when one is named)"), ("image", "Container image"), ("venv", "Box virtualenv")),
        default="auto", env="CAIRN_REMOTE_EXEC_MODE",
        description="How the builtin SSH engine runs a flow on a compute.ssh box (ADR-0104 D1): 'image' runs the named image via docker run --gpus all with the workdir bind-mounted and the live-channel env forwarded; 'venv' is the box's virtualenv; 'auto' picks image when remote_image or the connection names one, else venv. A compute.ssh connection's own 'mode' and 'image' override this per box. Image mode with no image named anywhere is refused at submit.",
    ),
    BackendField(
        key="remote_image", label="Remote execution image", group="Data plane", control="text",
        default="", env="CAIRN_TRAIN_IMAGE",
        description="The container image the SSH engine runs on the box. There is no default: with no image named here or on the connection, 'auto' runs the box's virtualenv and image mode is refused at submit. PIN A TAG, never :latest — the run record has to name the exact build that produced the artifacts; pin an immutable tag such as the image's :<sha>. A compute.ssh connection's own 'image' key overrides this per box.",
    ),
    BackendField(
        key="remote_env", label="Env forwarded to the remote job", group="Data plane", control="text",
        default="", env="CAIRN_REMOTE_ENV",
        description="Comma- or space-separated NAMES of control-plane environment variables the builtin SSH engine passes to the job on the compute.ssh box (e.g. CAIRN_CHECKPOINT_BUCKET, CAIRN_CHECKPOINT_PREFIX, AWS_DEFAULT_REGION), in addition to the spec, the live channel and MLFLOW_*. Set ones only; values go in the job's owner-only runner.sh (or docker run -e NAME), never on a command line. A name that looks like a credential (KEY, SECRET, TOKEN, PASSWORD) or that the engine sets itself is refused and the flow does not start: credentials stay on the box (an instance role).",
    ),
    # ── Metaflow substrate (ADR-0089, #1345) — identity/compute split. Before
    # this block, `<kind>_target` was OVERLOADED as the Metaflow metadata root,
    # so "Metaflow AND a compute target" was unsayable. These keys give each
    # concern its own field; the engine maps them to Metaflow natively.
    BackendField(
        key="metaflow_metadata_root", label="Metaflow metadata root", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_ROOT",
        description="Directory Metaflow's local metadata lives in — submit and re-attach must share it (ADR-0089). Empty = ~/.cairn/metaflow. Legacy fallback: a bare dataflow_target/ml_target still works as the root when this is unset.",
    ),
    BackendField(
        key="metaflow_datastore", label="Metaflow datastore", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_DATASTORE",
        description="Metaflow datastore backend: 's3' for cross-box artifacts + remote @kubernetes/@batch tasks; empty/'local' = files under the metadata root (one box only).",
    ),
    BackendField(
        key="metaflow_datastore_root", label="Metaflow datastore root", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_DATASTORE_ROOT",
        description="S3 URI the datastore writes under, e.g. s3://bucket/cairn-metaflow (ADR-0089 #1349). Required when the datastore is s3 — that is what makes artifacts readable from another box. AWS credentials come from a declared connection (metaflow_secrets), never from this field.",
    ),
    BackendField(
        key="metaflow_metadata_service", label="Metaflow metadata service", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_SERVICE_URL",
        description="URL of a Metaflow metadata service (#1349). Local metadata is CWD-scoped, so a run submitted on box A cannot be resolved from box B; a service makes pathspec re-attach and `resume` work across boxes. Empty = local metadata (single box).",
    ),
    BackendField(
        key="metaflow_compute", label="Metaflow compute", group="Data plane", control="select",
        options=_opts(("local", "Local subprocess"), ("kubernetes", "Kubernetes (@kubernetes)"), ("batch", "AWS Batch (@batch)")),
        default="local", env="CAIRN_METAFLOW_COMPUTE",
        description="Where Metaflow schedules the flow's execute step (ADR-0089 #1346): local subprocess, or remote via the @kubernetes/@batch decorators attached at run time (--with). Remote kinds require an image and a Metaflow-configured cluster.",
    ),
    BackendField(
        key="metaflow_compute_image", label="Metaflow task image", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_IMAGE",
        description="Container image for remote Metaflow tasks (required when compute is kubernetes/batch). Must ship cairndev[dataflow-engine] + the flow's kernels — the same deployment contract as the SSH box.",
    ),
    BackendField(
        key="metaflow_compute_queue", label="Metaflow batch queue", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_QUEUE",
        description="AWS Batch job queue for compute=batch (ignored for kubernetes/local).",
    ),
    BackendField(
        key="metaflow_retry_times", label="Metaflow retry times", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_RETRY",
        description="Substrate-level @retry count for the execute step (transient-infra retries, distinct from cairn's H2 leg retry). Empty/0 = no Metaflow retry.",
    ),
    BackendField(
        key="metaflow_secrets", label="Metaflow flow connections", group="Data plane", control="text",
        default="", env="CAIRN_METAFLOW_SECRETS", needs_connection=True,
        description="Comma-separated connection names whose credentials this engine's flows may use (ADR-0089 #1344). Resolved control-side and injected as env; nothing else from the control plane is passed under strict isolation. Empty = the flow gets no credentials.",
    ),
    BackendField(
        key="metaflow_env_isolation", label="Metaflow env isolation", group="Data plane", control="select",
        options=_opts(("auto", "Auto (strict when remote)"), ("inherit", "Inherit control-plane env"), ("strict", "Strict (declared only)")),
        default="auto", env="CAIRN_METAFLOW_ISOLATION",
        description="What environment a flow task receives. 'strict' passes only an essential allowlist + declared connections; 'inherit' passes the whole control-plane env (the pre-ADR-0089 behavior). 'auto' inherits for local compute (back-compat) and is strict for kubernetes/batch, where shipping every control-plane secret into a container is indefensible.",
    ),
    # ── SkyPilot provisioning engine (ADR-0104 D5, #2073) — the adapter's own
    # knobs. The MACHINE comes from `<kind>_target` (the compute connection);
    # these two say what runs on it and how it is bought.
    BackendField(
        key="skypilot_image", label="SkyPilot task image", group="Data plane", control="text",
        default="", env="CAIRN_SKYPILOT_IMAGE",
        description="Container image the provisioned machine runs (ADR-0104 D1: the image is the unit of remote work, so nothing on the machine can drift). Empty = the task installs cairndev[dataflow-engine] in its setup step, which is slower and driftable — set this in production.",
    ),
    BackendField(
        key="skypilot_accelerator", label="SkyPilot accelerator type", group="Data plane", control="text",
        default="", env="CAIRN_SKYPILOT_ACCELERATOR",
        description="The accelerator TYPE this target offers (A100, T4, L4…). SkyPilot asks for <type>:<count>: the COUNT comes from the flow's per-stage resource hint, but WHICH accelerator is a property of the target, not the flow. Required whenever a flow requests a GPU — a bare count is read as a device literally named that number (#2073), which no cloud can satisfy.",
    ),
    BackendField(
        key="skypilot_use_spot", label="SkyPilot spot instances", group="Data plane", control="toggle",
        default=False, env="CAIRN_SKYPILOT_USE_SPOT",
        description="Ask SkyPilot for pre-emptible capacity. Cheaper, and a reclaimed machine surfaces as a RETRIABLE failure the control loop may relaunch — only turn it on for a flow whose stages checkpoint (ADR-0088).",
    ),
    # ── RAG ──
    BackendField(
        key="rag_vector_store", label="Vector store", group="RAG", control="select",
        options=_opts(("sqlite_vec", "sqlite-vec (local)"), ("pgvector", "pgvector", "postgres"), ("qdrant", "Qdrant", "qdrant"), ("pinecone", "Pinecone", "pinecone")),
        default="sqlite_vec", env="RAG_VECTOR_STORE", needs_connection=True,
        description="Backend for RAG vector search. sqlite-vec runs locally with no service; pgvector/Qdrant/Pinecone bind a stored connection.",
    ),
    BackendField(
        key="rag_autoindex", label="Auto-index corpora on startup", group="RAG", control="toggle",
        default=False, env="CAIRN_RAG_AUTOINDEX",
        description="When on, packs that declare a RAG corpus are indexed on startup and refreshed on change. Off by default so a keyless checkout pays nothing.",
    ),
    BackendField(
        key="rag_parser", label="Document parser", group="RAG", control="select",
        options=_opts(("default", "Built-in (pypdf / readers)"), ("docling", "Docling (PDF layout/tables)"), ("markitdown", "MarkItDown (office → markdown)")),
        default="default",
        description="Parser backend for RAG ingestion. Built-in is the deterministic fallback; Docling/MarkItDown are optional operator backends (#307).",
    ),
    BackendField(
        key="embedder", label="Embedding model", group="RAG", control="text",
        default="text-embedding-3-small", env="RAG_EMBED_MODEL",
        description="Embedding model used for RAG + memory (e.g. text-embedding-3-small, or a local fastembed model). Free-form to allow any provider's model id.",
    ),
    BackendField(
        key="rag_reranker", label="Re-ranker", group="RAG", control="select",
        options=_opts(("identity", "Score sort (no model, free)"), ("cross_encoder", "Cross-encoder (local)"), ("cohere", "Cohere Rerank (API)")),
        default="identity", env="RAG_RERANKER",
        description="Cross-corpus re-ranker for composed knowledge (#513). identity = deterministic score sort (default, no cost); cross_encoder = local model; cohere = API. Only matters when composing many corpora.",
    ),
    # ── Memory ──
    BackendField(
        key="memory", label="Episodic memory", group="Memory", control="toggle",
        default=False,
        description="Enable episodic memory for this environment. When on, runs capture episodic memory as a lifecycle guarantee and memory operators are available (ADR-0052 §5, #306).",
    ),
    BackendField(
        key="memory_vector_store", label="Memory store", group="Memory", control="select",
        options=_opts(("inherit", "Inherit from RAG vector store"), ("sqlite_vec", "sqlite-vec (local)"), ("pgvector", "pgvector", "postgres"), ("qdrant", "Qdrant", "qdrant")),
        default="inherit", needs_connection=True, optional_on="memory",
        description="Backend for episodic memory. Defaults to the RAG vector store; override to isolate memory on its own store.",
    ),
    # ── Eventing ──
    BackendField(
        key="event_bus", label="Notification bus", group="Eventing", control="select",
        # ADR-0127 D5 — the redis option is gone: CAIRN_BUS=redis://… is refused.
        options=_opts(("inprocess", "In-process"), ("postgres", "Postgres (needs a DSN in CAIRN_BUS)")),
        default="inprocess", env="CAIRN_BUS",
        description="Outbound notification/replay bus. In-process is the default. The Postgres bus (LISTEN/NOTIFY + replay, #477) is selected by a postgres:// DSN in CAIRN_BUS; this field carries no DSN, so choosing Postgres here without one runs in-process.",
    ),
    BackendField(
        key="trigger_inbox", label="Trigger inbox", group="Eventing", control="select",
        options=_opts(("sqlite", "SQLite (queryable)"), ("postgres", "Postgres (queryable)"), ("nats", "NATS JetStream (multi-node)")),
        default="sqlite", env="CAIRN_TRIGGER_INBOX", env_is_url_kind=True, needs_connection=True,
        description="Durable inbound-trigger queue. SQLite/Postgres are queryable + replayable; NATS JetStream scales across nodes (binds a NATS connection).",
    ),
    BackendField(
        key="job_channel", label="Remote-job channel", group="Eventing", control="select",
        options=_opts(("inmemory", "In-memory (single-process)"), ("nats", "NATS JetStream")),
        default="inmemory", env="CAIRN_JOB_CHANNEL", env_is_url_kind=True, needs_connection=True,
        description="Events-up / control-down channel for remote jobs (heartbeat, abort). In-memory for single-process; NATS for a real worker on another box.",
    ),
    # ── Execution ──
    BackendField(
        key="remote_execution", label="Remote execution", group="Execution", control="toggle",
        default=False, env="CAIRN_REMOTE_JOB_DURABLE",
        description="Enable the durable remote-job control loop so operators can run on external compute (SSH / Docker / K8s / Ray).",
    ),
    BackendField(
        key="remote_executor", label="Executor driver", group="Execution", control="select",
        options=_opts(("ssh", "SSH (remote/exec)", "ssh"), ("docker", "Docker"), ("kube", "Kubernetes Job"), ("ray", "Ray")),
        default="ssh", needs_connection=True, optional_on="remote_execution",
        description="How remote operators execute. Each driver binds a compute connection (compute.ssh / compute.docker / compute.kube / compute.ray).",
    ),
    # ── Models ──
    BackendField(
        key="llm_gateway", label="LLM gateway", group="Models", control="text",
        default="", needs_connection=True,
        description="Gateway/endpoint governed LLM calls route through (e.g. a Bifrost/LiteLLM URL, or an API-key connection ref). Empty uses the provider default.",
    ),
]

_BY_KEY = {f.key: f for f in BACKENDS}


def get_field(key: str) -> Optional[BackendField]:
    return _BY_KEY.get(key)


def backends_schema() -> list[dict[str, Any]]:
    """The full inventory as JSON-serializable dicts (the form's data source, #303)."""
    return [f.model_dump() for f in BACKENDS]


def backends_schema_grouped() -> list[dict[str, Any]]:
    """The inventory grouped for the form: ``[{group, fields:[...]}]`` in GROUPS order."""
    return [
        {"group": g, "fields": [f.model_dump() for f in BACKENDS if f.group == g]}
        for g in GROUPS
    ]


def _kind_from_url(raw: str) -> str:
    """Derive a backend *kind* from a URL-style env value (e.g. CAIRN_STORAGE).
    ``postgres://…`` / ``postgresql://…`` → ``postgres``; ``sqlite…`` → ``sqlite``;
    ``nats://…`` → ``nats``; otherwise the scheme, else the raw value."""
    low = raw.strip().lower()
    if low.startswith(("postgres://", "postgresql://")):
        return "postgres"
    if low.startswith("sqlite"):
        return "sqlite"
    if "://" in low:
        return low.split("://", 1)[0]
    return low


def effective_backends(
    profile_backends: Optional[Mapping[str, Any]] = None,
    env: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """Resolve every backend field to the value the runtime should use.

    Precedence per field: **profile record → environment variable → schema
    default**. Toggles return ``bool``; select/text return ``str``. For
    ``env_is_url_kind`` fields the env URL is mapped to its kind (so a
    ``postgres://…`` ``CAIRN_STORAGE`` resolves the ``storage`` field to
    ``"postgres"``). Unknown/blank profile values fall through to env, then
    default — never raise.
    """
    pb: Mapping[str, Any] = profile_backends or {}
    ev: Mapping[str, str] = env if env is not None else os.environ
    out: dict[str, Any] = {}
    for f in BACKENDS:
        pv = pb.get(f.key)
        if f.control == "toggle":
            if pv is not None:
                out[f.key] = _coerce_bool(pv)
            elif f.env and getenv(f.env, environ=ev) is not None:
                out[f.key] = str(getenv(f.env, "", environ=ev)).strip().lower() in _TRUTHY
            else:
                out[f.key] = bool(f.default)
            continue
        # select / text
        if pv is not None and str(pv).strip() != "":
            out[f.key] = str(pv)
        elif f.env and str(getenv(f.env, "", environ=ev)).strip():
            raw = str(getenv(f.env, "", environ=ev)).strip()
            out[f.key] = _kind_from_url(raw) if f.env_is_url_kind else raw
        else:
            out[f.key] = str(f.default)
    return out


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in _TRUTHY
