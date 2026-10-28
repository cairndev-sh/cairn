"""Preflight validation for the unified deployment stack (issue #390).

``cairn deploy doctor`` reads the deploy ``.env`` + the enabled compose profiles
and checks that they AGREE before you ``docker compose up`` — catching the class
of mistake where a service is configured but its profile isn't enabled (so it
silently never starts) or vice-versa. The brief's canonical example:
``CAIRN_RUNTIME=temporal`` without ``--profile temporal`` → fail loud.

Pure data-in/data-out (``check_deploy(env, profiles) -> list[CheckResult]``) so
it's unit-testable without a live Docker host; the CLI in
``cairn.cli.commands.deploy`` is a thin wrapper. Reuses the ``CheckResult`` type
that ``cairn doctor`` already renders.
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml
from dotenv import dotenv_values

from cairn.core import settings as _settings
from cairn.core.pack import CheckResult

#: What ``env_from_values`` writes for a secret that lives in a Kubernetes
#: Secret this process cannot read (``secrets.existingSecret``). Present, so the
#: REQUIRED check is satisfied; unmistakable, so the argon2 parse check reports
#: "not verifiable here" instead of failing a hash it was never given.
EXTERNAL_SECRET = "<supplied by existingSecret>"

# Vars the unified compose declares with `${VAR:?...}` — the stack refuses to
# start without them, so surface them as hard failures here first.
REQUIRED: dict[str, str] = {
    "CADDY_DOMAIN": "TLS domain for Caddy (app.example.com or <ip-dashes>.sslip.io)",
    "POSTGRES_PASSWORD": "app Postgres password (openssl rand -hex 16)",
    "CAIRN_JWT_SECRET": "control-plane JWT secret — auth is ON in prod (#2)",
    "CAIRN_API_PASSWORD_HASH": "argon2 hash for the API user (cairn remote hash-password)",
    "CAIRN_PROFILE": "the deployment profile — the engine refuses to start without one "
                   "(docs/runbooks/deployment-profile.md)",
}

# Profiles the unified docker-compose.yml actually defines, tagged by deployment
# tier (#408). Tiers are informational only — compose has no native "profile
# group", so this dict is the one place that maps a profile to its tier for the
# report. See docker-compose.yml's header comment for the full rationale.
#
#   "core" — OSS core: cairn can't run/be governed without it (or it's a
#            governed-access/secrets primitive). Bundled, no external accounts.
#   "saas" — internal SaaS: watches/scores/records what cairn did. Optional;
#            self-host (this box or another) OR a paid cloud tier.
PROFILE_TIERS: dict[str, str] = {
    "temporal": "core",
    "dedicated-temporal-db": "core",
    "nats": "core",
    "infisical": "core",
    "backup": "core",
    "otel": "saas",
    "observability": "saas",
    "mlflow": "saas",
}

KNOWN_PROFILES: frozenset[str] = frozenset(PROFILE_TIERS)

_TRUTHY = {"1", "true", "yes", "on"}


def parse_profiles(value: str | None) -> set[str]:
    """Split a ``COMPOSE_PROFILES`` value (comma-separated) into a set."""
    return {p.strip() for p in (value or "").split(",") if p.strip()}


def parse_env_file(path: str | Path) -> dict[str, str]:
    """Load a deploy ``.env`` into a dict (empty values kept as ``""``).

    Missing file → empty dict, so a caller that checks it anyway sees every
    required var as unset. ``cairn deploy doctor`` does not: it reports the
    missing file as the one finding (#2536). Uses python-dotenv so
    quoting/escaping matches what docker-compose reads.
    """
    p = Path(path)
    if not p.exists():
        return {}
    return {k: (v or "") for k, v in dotenv_values(p).items()}


def _env(env: dict[str, str], name: str) -> str:
    """``name`` from a deploy env — a cairn variable through the one lookup, so its
    ``OBS_*`` spelling still counts (ADR-0122 D2)."""
    if name.startswith(_settings.ENV_PREFIX):
        return _settings.getenv(name, "", environ=env) or ""
    return env.get(name, "") or ""


def check_deploy(env: dict[str, str], profiles: set[str]) -> list[CheckResult]:
    """Validate a deploy env + enabled profiles. Returns one CheckResult per
    finding; ``fail`` means the stack would misbehave as configured.
    """
    out: list[CheckResult] = []

    # ── ADR-0122 D4: an OBS_* name still works until 1.0 — say what replaces it ──
    for old in sorted(k for k in env if k.startswith(_settings.LEGACY_ENV_PREFIX)):
        new = _settings.ENV_PREFIX + old[len(_settings.LEGACY_ENV_PREFIX):]
        out.append(CheckResult(
            old, "warn", f"deprecated name — rename it to {new} (read until 1.0, ADR-0122)",
            category="deprecated"))

    # ── required vars ────────────────────────────────────────────────────
    for var, desc in REQUIRED.items():
        val = _env(env, var).strip()
        out.append(
            CheckResult(var, "ok" if val else "fail",
                        "set" if val else f"REQUIRED — {desc}", category="required")
        )

    # ── password hash must PARSE, not just be set (#1853) ────────────────
    # "set" was the whole check, and prod passed it while every login failed:
    # compose had interpolated the `$` segments out of the argon2 hash. The
    # login path reports that as "incorrect password", so the doctor is the
    # one place that can say what actually happened before anyone tries.
    h = _env(env, "CAIRN_API_PASSWORD_HASH").strip()
    if h == EXTERNAL_SECRET:
        # A chart deploy that points at `secrets.existingSecret`: the hash is in
        # the cluster, not in the values file. Its presence is the cluster's
        # problem; the doctor says so rather than failing a hash it never saw.
        out.append(CheckResult(
            "password_hash", "warn",
            "supplied by an existing Kubernetes Secret — cannot be verified from here; "
            "confirm it parses with `kubectl get secret … -o jsonpath` before you trust a login",
            category="required"))
    elif h:
        try:
            from pwdlib import PasswordHash  # noqa: PLC0415 — a base dependency since 0.1.2; guarded for older installs
        except ImportError:
            out.append(CheckResult(
                "password_hash", "warn",
                "cannot verify the hash format — pwdlib[argon2] is not installed here (a base dependency since 0.1.2)",
                category="required"))
        else:
            try:
                PasswordHash.recommended().verify("cairn-hash-format-probe", h)
                out.append(CheckResult("password_hash", "ok", "parses as argon2", category="required"))
            except Exception as e:  # noqa: BLE001 — UnknownHashError or malformed payload
                out.append(CheckResult(
                    "password_hash", "fail",
                    f"CAIRN_API_PASSWORD_HASH does not parse ({type(e).__name__}; starts {h[:12]!r}) — "
                    "every login will fail as 'incorrect password'. Compose strips `$` from "
                    "the hash: write it as `$$argon2id$$…` (compose un-escapes to `$`), or "
                    "move it out of .env. (#1853)",
                    category="required"))

    # ── unknown profiles (typo guard) ────────────────────────────────────
    for p in sorted(profiles - set(KNOWN_PROFILES)):
        out.append(CheckResult(
            f"profile:{p}", "warn",
            f"unknown profile — the compose file defines: {', '.join(sorted(KNOWN_PROFILES))}",
            category="profiles",
        ))

    # ── tier summary (#408) — informational: which tier(s) this deploy runs ──
    known_on = sorted(p for p in profiles if p in PROFILE_TIERS)
    core_on = [p for p in known_on if PROFILE_TIERS[p] == "core"]
    saas_on = [p for p in known_on if PROFILE_TIERS[p] == "saas"]
    tier_desc = (
        f"OSS core: {', '.join(core_on) or '(none enabled)'} · "
        f"internal SaaS: {', '.join(saas_on) or '(none enabled)'}"
    )
    out.append(CheckResult("tiers", "ok", tier_desc, category="profiles"))

    # ── infisical profile ↔ real secret values ───────────────────────────
    # The compose falls back to `change-me`/blank defaults so the base config
    # always parses (profile-off deploys must not need these vars at all);
    # doctor is what actually enforces they're real before the profile ships.
    if "infisical" in profiles:
        infisical_vars = {
            "INFISICAL_ENCRYPTION_KEY": "openssl rand -hex 16",
            "INFISICAL_AUTH_SECRET": "openssl rand -hex 32",
            "INFISICAL_DB_PASSWORD": "openssl rand -hex 16",
        }
        missing = [
            v for v, hint in infisical_vars.items()
            if not env.get(v, "").strip() or env.get(v, "").strip().startswith("change-me")
        ]
        if missing:
            out.append(CheckResult(
                "infisical", "fail",
                "'infisical' profile is on but these vars are unset/default: "
                + ", ".join(missing) + " — generate real secrets before deploying",
                category="secrets"))
        else:
            out.append(CheckResult("infisical", "ok", "secrets configured", category="secrets"))

        # ── infisical service ↔ engine secret-provider binding (#408) ────────
        # Standing up the Infisical service is only half of it: the engine must
        # be told to READ from it via a cairn profile that binds
        # `secrets.store = secrets/infisical` (CAIRN_PROFILE). Without that the
        # service runs but every `secrets:` read still hits the plaintext env —
        # same silent-mismatch class as CAIRN_RUNTIME=temporal without the profile.
        obs_profile = _env(env, "CAIRN_PROFILE").strip()
        if not obs_profile:
            out.append(CheckResult(
                "secrets_binding", "warn",
                "'infisical' profile is on but CAIRN_PROFILE is unset — the engine "
                "won't read from Infisical unless a profile binds "
                "secrets.store=secrets/infisical (ship CAIRN_PROFILE=.../prod-infisical.toml)",
                category="secrets"))
        else:
            # Best-effort: if the profile file is reachable from here, confirm it
            # actually binds Infisical (a plain text scan — no profile-loader
            # import, and container-only paths just skip the deep check).
            pf = Path(obs_profile)
            binds_infisical = pf.exists() and "secrets/infisical" in pf.read_text()
            if pf.exists() and not binds_infisical:
                out.append(CheckResult(
                    "secrets_binding", "warn",
                    f"CAIRN_PROFILE={obs_profile} doesn't bind secrets.store=secrets/infisical "
                    "— the Infisical service is running but the engine reads secrets "
                    "from the plaintext env",
                    category="secrets"))
            else:
                out.append(CheckResult(
                    "secrets_binding", "ok",
                    f"engine secret-provider via CAIRN_PROFILE={obs_profile}",
                    category="secrets"))

    # ── runtime ↔ temporal profile ───────────────────────────────────────
    # #1852 — the profile can pin the runtime; judge what the app will actually
    # run, not just the env var. Container-only profile paths are unreachable
    # from the host and simply fall back to CAIRN_RUNTIME (same best-effort rule
    # the infisical binding check below uses). An INVALID profile value is a
    # hard failure — that is exactly the accepted-and-ignored defect.
    from cairn.core import settings as _rt_settings  # noqa: PLC0415 — settings pulls the pack stack; doctor stays light

    try:
        _pinned = _rt_settings.profile_runtime(_env(env, "CAIRN_PROFILE"))
    except ValueError as e:
        out.append(CheckResult("runtime", "fail", str(e), category="runtime"))
        _pinned = None
    runtime = _pinned or (_env(env, "CAIRN_RUNTIME") or "inprocess").strip()
    temporal_on = "temporal" in profiles
    if runtime == "temporal" and not temporal_on:
        out.append(CheckResult(
            "runtime", "fail",
            "CAIRN_RUNTIME=temporal but the 'temporal' profile is off — Temporal + "
            "worker won't start; add 'temporal' to COMPOSE_PROFILES",
            category="runtime"))
    elif temporal_on and runtime != "temporal":
        out.append(CheckResult(
            "runtime", "warn",
            f"'temporal' profile is on but CAIRN_RUNTIME={runtime} — the server won't "
            "submit to Temporal (set CAIRN_RUNTIME=temporal)",
            category="runtime"))
    else:
        out.append(CheckResult(
            "runtime", "ok",
            runtime + (" (+ temporal profile)" if temporal_on else ""),
            category="runtime"))

    # ── durable runtime ↔ checkpointer (#532) ────────────────────────────
    # A durable runtime (Temporal, or a worker separate from the server) survives
    # restarts, but a HITL-gated run only resumes if its LangGraph checkpoint is
    # in a SHARED, durable store. With the default in-process MemorySaver the
    # interrupt lives in one process's memory: a run paused for approval — and
    # every composed-subgraph gate (training/finetune → dataset-readiness) — is
    # unresumable across the server/worker boundary or a restart. This is the
    # exact silent footgun behind #532 Problem 2. Applies whenever Temporal is on
    # (server submits, a separate worker executes → they must share the store).
    #
    # The SAME footgun applies to durable remote jobs even under in_process (#551
    # seam 6): a remote job (train/remote, train/runpod, …) pauses via interrupt()
    # and its graph state lives in the checkpointer. With a MemorySaver a control-
    # plane restart DURING the (hours-long) job orphans the paused run — the job
    # finishes on the box but its result can never be re-attached. So durable
    # remote jobs REQUIRE a shared, durable checkpointer regardless of runtime.
    #
    # DBOS (#1847) is the same footgun reached by a different road. There is no
    # server↔worker split — DBOS runs the workflow in THIS process — so Temporal's
    # wording does not fit, but the outcome is worse: `DBOS.launch()` re-runs
    # PENDING workflows on restart, `run_template` asks the checkpointer whether a
    # thread already exists, and a MemorySaver died with the process. So it
    # re-seeds `initial_state` and replays the WHOLE graph — every operator and
    # every LLM call, paid a second time — while reporting itself as a durable
    # runtime. `tests/core/test_dbos_runtime.py` passes only because it wires a
    # SqliteSaver explicitly; a deployment that sets CAIRN_RUNTIME=dbos and nothing
    # else gets none of that.
    checkpointer = (_env(env, "CAIRN_CHECKPOINTER") or "memory").strip().lower()
    remote_job_durable = _env(env, "CAIRN_REMOTE_JOB_DURABLE").strip().lower() in _TRUTHY
    if runtime == "dbos" and checkpointer == "memory":
        out.append(CheckResult(
            "checkpointer", "fail",
            "CAIRN_RUNTIME=dbos but CAIRN_CHECKPOINTER=memory — DBOS recovers PENDING "
            "workflows on restart, but the LangGraph checkpoint they resume from "
            "died with the process, so recovery re-runs the whole graph from zero "
            "(every operator and LLM call charged again) instead of continuing. "
            "Set CAIRN_CHECKPOINTER=sqlite (single node) or postgres "
            "(+ CAIRN_CHECKPOINTER_DSN).",
            category="runtime"))
    elif runtime == "temporal" and checkpointer == "memory":
        out.append(CheckResult(
            "checkpointer", "fail",
            "CAIRN_RUNTIME=temporal but CAIRN_CHECKPOINTER=memory — HITL-gated runs "
            "(and composed-subgraph gates) are held in one process's memory and "
            "cannot resume across the server↔worker boundary or a restart. Set "
            "CAIRN_CHECKPOINTER=postgres (+ CAIRN_CHECKPOINTER_DSN) on BOTH the server "
            "and the worker.",
            category="runtime"))
    elif remote_job_durable and checkpointer == "memory":
        out.append(CheckResult(
            "checkpointer", "fail",
            "CAIRN_REMOTE_JOB_DURABLE=1 but CAIRN_CHECKPOINTER=memory — a durable remote "
            "job pauses via interrupt() with its state in the checkpointer, so a "
            "control-plane restart during the (long) job orphans the run and its "
            "result can never be re-attached. Set CAIRN_CHECKPOINTER=postgres "
            "(+ CAIRN_CHECKPOINTER_DSN).",
            category="runtime"))
    elif checkpointer == "postgres" and not _env(env, "CAIRN_CHECKPOINTER_DSN").strip():
        out.append(CheckResult(
            "checkpointer", "fail",
            "CAIRN_CHECKPOINTER=postgres but CAIRN_CHECKPOINTER_DSN is unset — the "
            "checkpointer will fail to start.",
            category="runtime"))
    else:
        out.append(CheckResult(
            "checkpointer", "ok", checkpointer, category="runtime"))

    # ── durable remote-job ↔ SHARED job store (#564) ─────────────────────
    # Distinct from the checkpointer above: the remote-job STORE (CAIRN_JOB_STORE /
    # CAIRN_STORAGE) holds the RemoteJobRecord the control loop polls + resumes on.
    # The record is written by the NODE (under Temporal, that's the *worker*
    # process) and read by the control loop (the *server* process). An in-memory
    # store is per-process, so under Temporal the server's control loop never sees
    # the worker's record → the job "completes on the box" but the run is never
    # resumed (the #564 "record not tracked / run failed" symptom). Even under
    # in_process a memory store loses records on a control-plane restart mid-job.
    # Shared/durable = CAIRN_STORAGE=postgres://… OR CAIRN_JOB_STORE=sqlite (default).
    if remote_job_durable:
        storage_url = (_env(env, "CAIRN_STORAGE") or "").strip().lower()
        job_store = (_env(env, "CAIRN_JOB_STORE") or "").strip().lower()
        store_is_memory = not storage_url.startswith(("postgres://", "postgresql://")) and job_store in (
            "memory", ":memory:",
        )
        if store_is_memory:
            fix = "unset CAIRN_JOB_STORE (sqlite default) or set CAIRN_STORAGE=postgres://…"
            out.append(CheckResult(
                "remote_job_store", "fail",
                "CAIRN_REMOTE_JOB_DURABLE=1 but the remote-job store is in-memory "
                "(CAIRN_JOB_STORE=memory) — the RemoteJobRecord is written by the node "
                "and read by the server's control loop; in-memory doesn't share across "
                f"the Temporal worker↔server boundary or survive a restart, so the run "
                f"never resumes (#564). Use a shared store: {fix}.",
                category="runtime"))
        else:
            out.append(CheckResult(
                "remote_job_store", "ok",
                "postgres" if storage_url.startswith(("postgres://", "postgresql://")) else (job_store or "sqlite"),
                category="runtime"))

    # ── dedicated Temporal DB profile ↔ seeds ────────────────────────────
    dedicated = "dedicated-temporal-db" in profiles
    seeds = env.get("TEMPORAL_DB_SEEDS", "").strip()
    if seeds == "temporal-db" and not dedicated:
        out.append(CheckResult(
            "temporal_db", "fail",
            "TEMPORAL_DB_SEEDS=temporal-db but the 'dedicated-temporal-db' profile "
            "is off — that database won't start",
            category="runtime"))
    elif dedicated and seeds != "temporal-db":
        out.append(CheckResult(
            "temporal_db", "warn",
            "'dedicated-temporal-db' profile is on but TEMPORAL_DB_SEEDS != temporal-db "
            "— Temporal will still use the shared postgres",
            category="runtime"))

    # ── nats profile ↔ durable trigger inbox ─────────────────────────────
    inbox = _env(env, "CAIRN_TRIGGER_INBOX").strip()
    durable = _env(env, "CAIRN_TRIGGER_DURABLE").strip().lower() in _TRUTHY
    nats_on = "nats" in profiles
    wants_nats = inbox.startswith("nats://") or durable
    if wants_nats and not nats_on:
        out.append(CheckResult(
            "trigger_inbox", "fail",
            "durable NATS inbox configured (CAIRN_TRIGGER_DURABLE/CAIRN_TRIGGER_INBOX) "
            "but the 'nats' profile is off — NATS won't start",
            category="triggers"))
    elif nats_on and not inbox:
        out.append(CheckResult(
            "trigger_inbox", "warn",
            "'nats' profile is on but CAIRN_TRIGGER_INBOX is unset "
            "(set CAIRN_TRIGGER_INBOX=nats://nats:4222)",
            category="triggers"))
    elif nats_on:
        out.append(CheckResult("trigger_inbox", "ok", inbox, category="triggers"))

    # ── otel profile ↔ collector endpoint ────────────────────────────────
    otel_ep = env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    otel_on = "otel" in profiles
    targets_collector = "otel-collector" in otel_ep
    if targets_collector and not otel_on:
        out.append(CheckResult(
            "otel", "fail",
            "OTEL_EXPORTER_OTLP_ENDPOINT points at otel-collector but the 'otel' "
            "profile is off — the collector won't start",
            category="observability"))
    elif otel_on and not targets_collector:
        out.append(CheckResult(
            "otel", "warn",
            "'otel' profile is on but OTEL_EXPORTER_OTLP_ENDPOINT doesn't target "
            "the bundled collector (http://otel-collector:4318)",
            category="observability"))

    # ── mlflow profile ↔ tracking URI (#408) ─────────────────────────────
    mlflow_uri = env.get("MLFLOW_TRACKING_URI", "").strip()
    mlflow_on = "mlflow" in profiles
    targets_self_hosted_mlflow = "mlflow:5000" in mlflow_uri
    if targets_self_hosted_mlflow and not mlflow_on:
        out.append(CheckResult(
            "mlflow", "fail",
            "MLFLOW_TRACKING_URI points at the self-hosted mlflow service but "
            "the 'mlflow' profile is off — it won't start",
            category="observability"))
    elif mlflow_on and not mlflow_uri:
        out.append(CheckResult(
            "mlflow", "warn",
            "'mlflow' profile is on but MLFLOW_TRACKING_URI is unset on the worker "
            "(set MLFLOW_TRACKING_URI=http://mlflow:5000)",
            category="observability"))
    elif mlflow_on:
        out.append(CheckResult("mlflow", "ok", mlflow_uri, category="observability"))

    # ── observability profile ↔ otel (needs the collector's Prometheus export) ──
    if "observability" in profiles and not otel_on:
        out.append(CheckResult(
            "observability", "warn",
            "'observability' profile (Prometheus+Grafana) is on but 'otel' is off "
            "— Prometheus has nothing to scrape (enable --profile otel too)",
            category="observability"))

    # ── auth hardening (#391) ────────────────────────────────────────────
    # The compose defaults CAIRN_REQUIRE_AUTH=1; warn only if it's explicitly off.
    if _env(env, "CAIRN_REQUIRE_AUTH").strip().lower() in {"0", "false", "no", "off"}:
        out.append(CheckResult(
            "require_auth", "warn",
            "CAIRN_REQUIRE_AUTH is disabled — the engine will run OPEN if the JWT "
            "secret is ever missing/blank (leave it on for prod)",
            category="required"))

    # ── openai backend needs a key ───────────────────────────────────────
    if _env(env, "CAIRN_BACKEND").strip() == "openai" and not env.get("OPENAI_API_KEY", "").strip():
        out.append(CheckResult(
            "llm_backend", "fail",
            "CAIRN_BACKEND=openai but OPENAI_API_KEY is unset",
            category="llm"))

    return out



# ══ Helm values (ADR-0104 D4, #26) ═══════════════════════════════════════
#
# A chart deploy carries the same information a compose deploy carries in a
# ``.env`` + ``COMPOSE_PROFILES``, in a different shape. So the doctor
# TRANSLATES rather than forks: values → (env, profiles) → the very same
# ``check_deploy`` above. A rule added for compose applies to the chart for
# free, and the two can never disagree about, say, temporal-without-the-profile.
#
# Only the rules that have no compose counterpart live below (``check_values``):
# the bundled-vs-external Postgres pairing, an unpinned image tag, a pull worker
# with no command, a secret the doctor cannot read.


def parse_values_file(path: str | Path) -> dict:
    """Load a Helm values file. Missing/empty file → ``{}``."""
    p = Path(path)
    if not p.is_file():
        return {}
    loaded = yaml.safe_load(p.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _get(values: dict, *path, default=None):
    """``_get(v, "postgres", "external", "dsn")`` without the None-guards."""
    cur = values
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def values_dsn(values: dict) -> str:
    """The DSN the chart's ConfigMap would render (``cairn.dsn`` in _helpers.tpl)."""
    if _get(values, "postgres", "external", "dsn"):
        return str(_get(values, "postgres", "external", "dsn"))
    if _get(values, "postgres", "enabled", default=True):
        user = _get(values, "postgres", "user", default="cairn")
        pw = _get(values, "postgres", "password", default="")
        port = _get(values, "postgres", "port", default=5432)
        db = _get(values, "postgres", "database", default="cairn")
        # The release name is not known here (it is a `helm install` argument),
        # so the host is the chart's default shape. Only the credentials and the
        # scheme matter to the checks.
        return f"postgresql://{user}:{pw}@cairn-postgres:{port}/{db}"
    return ""


def profiles_from_values(values: dict) -> set[str]:
    """The enabled ``profiles.*`` — the values twin of ``COMPOSE_PROFILES``."""
    profiles = values.get("profiles") or {}
    return {
        name for name, cfg in profiles.items()
        if isinstance(cfg, dict) and cfg.get("enabled")
    }


def env_from_values(values: dict) -> dict[str, str]:
    """Render a values file into the environment the containers would get.

    Mirrors ``templates/configmap.yaml`` + ``templates/secret.yaml``. The one
    translation that is not a rename: the chart has no Caddy, so the INGRESS
    HOST plays ``CADDY_DOMAIN``'s role — it is the public name TLS is issued
    for, which is exactly what that check is about.

    The chart also renders the DSN family's ``OBS_*`` twin until 1.0 (#2429) so an
    ``existingSecret`` in either spelling wins; this env carries the ``CAIRN_*`` name
    only — it resolves to the same value, and the twin is the chart's, not a
    deprecated name the operator wrote (``tests/deploy/test_helm_chart.py`` holds
    the two in step).
    """
    dsn = values_dsn(values)
    existing = _get(values, "secrets", "existingSecret", default="")
    runtime = str(_get(values, "runtime", default="dbos"))

    def _secret(field: str) -> str:
        val = _get(values, "secrets", field, default="")
        if val:
            return str(val)
        return EXTERNAL_SECRET if existing else ""

    env: dict[str, str] = {
        "CADDY_DOMAIN": str(_get(values, "ingress", "host", default="")),
        "CAIRN_TRIGGER_PORT": str(_get(values, "server", "port", default=9090)),
        "CAIRN_BACKEND": str(_get(values, "env", "backend", default="mock")),
        "CAIRN_RUNTIME": runtime,
        "CAIRN_STORAGE": dsn,
        "CAIRN_CHECKPOINTER": str(_get(values, "checkpointer", default="postgres")),
        "CAIRN_CHECKPOINTER_DSN": dsn,
        "CAIRN_PROFILE": str(_get(values, "env", "profile", default="")),
        "CAIRN_API_USER": str(_get(values, "env", "apiUser", default="admin")),
        "CAIRN_REQUIRE_AUTH": "1" if _get(values, "env", "requireAuth", default=True) else "0",
        "CAIRN_JWT_SECRET": _secret("obsJwtSecret"),
        "CAIRN_API_PASSWORD_HASH": _secret("obsApiPasswordHash"),
        "OPENAI_API_KEY": _secret("openaiApiKey"),
        "OTEL_EXPORTER_OTLP_ENDPOINT": str(_get(values, "observability", "otlp", "endpoint", default="")),
        "OTEL_EXPORTER_OTLP_HEADERS": str(_get(values, "observability", "otlp", "headers", default="")),
        "MLFLOW_TRACKING_URI": str(_get(values, "profiles", "mlflow", "trackingUri", default="")),
    }
    if runtime == "dbos":
        env["CAIRN_DBOS_DSN"] = dsn
    if "nats" in profiles_from_values(values):
        env["CAIRN_TRIGGER_DURABLE"] = "1"
        env["CAIRN_TRIGGER_INBOX"] = "nats://cairn-nats:4222"
    if _get(values, "profiles", "mlflow", "enabled") and not env["MLFLOW_TRACKING_URI"]:
        env["MLFLOW_TRACKING_URI"] = "http://cairn-mlflow:5000"

    # POSTGRES_PASSWORD is a REQUIRED var because compose interpolates it into
    # every DSN. The chart does the same for the bundled database; with an
    # external DSN the password is IN the DSN, so read it back out rather than
    # asking the operator to repeat it (a repeated secret is a secret that drifts).
    pw = str(_get(values, "postgres", "password", default=""))
    if not pw and dsn:
        pw = unquote(urlsplit(dsn).password or "")
    env["POSTGRES_PASSWORD"] = pw or (EXTERNAL_SECRET if existing else "")

    for k, v in (_get(values, "env", "extra", default={}) or {}).items():
        env[str(k)] = str(v)
    return env


#: A pullWorker.extraEnv name the chart refuses (_helpers.tpl cairn.pullWorkerUnsafeName):
#: extraEnv is rendered into a ConfigMap, so credentials and database settings go in the
#: worker's own Secret instead. A marker counts as an underscore-separated segment or a
#: segment suffix, case-insensitively: HF_TOKEN and APITOKEN are refused,
#: TOKENIZERS_PARALLELISM is not.
_PULL_WORKER_UNSAFE_SEGMENT = re.compile(r"(KEY|SECRET|TOKEN|PASSWORD|DSN)S?\Z")
#: A value carrying a password in a URL (cairn.pullWorkerUnsafeValue).
_URL_WITH_PASSWORD = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^/?#@\s]*:[^/?#@\s]+@")
_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def pull_worker_unsafe_name(name: str) -> bool:
    """The chart's rule for a pullWorker.extraEnv name (see the constants above)."""
    u = str(name).upper()
    return (any(_PULL_WORKER_UNSAFE_SEGMENT.search(seg) for seg in u.split("_"))
            or u.endswith("STORAGE") or u == "OTEL_EXPORTER_OTLP_HEADERS"
            or "CONNECTION_STRING" in u or u.startswith("CAIRN_CONN_"))


def _pull_worker_extra_env_checks(values: dict) -> list[CheckResult]:
    """The chart's pullWorker.extraEnv refusals, before a render: names that look like a
    secret or a database setting, and values with a password in a URL. Names and URL
    credentials only — any other credential belongs in the worker's Secret."""
    extra = _get(values, "pullWorker", "extraEnv", default={}) or {}
    if not isinstance(extra, dict):
        return [CheckResult("pull_worker_extra_env", "fail",
                            "pullWorker.extraEnv must be a map of NAME: value.", category="secrets")]
    out: list[CheckResult] = []
    for name, value in extra.items():
        if not _ENV_NAME_RE.match(str(name)):
            out.append(CheckResult("pull_worker_extra_env", "fail",
                                   f"pullWorker.extraEnv key {name!r} is not an environment "
                                   "variable name.", category="secrets"))
        elif pull_worker_unsafe_name(name):
            out.append(CheckResult("pull_worker_extra_env", "fail",
                                   f"pullWorker.extraEnv.{name} looks like a secret or a database "
                                   "setting — extraEnv is rendered into a ConfigMap; put it in the "
                                   "worker's own Secret (pullWorker.existingSecret).",
                                   category="secrets"))
        elif _URL_WITH_PASSWORD.search(str(value)):
            out.append(CheckResult("pull_worker_extra_env", "fail",
                                   f"pullWorker.extraEnv.{name} has a URL with a password in it — "
                                   "put the credential in the worker's own Secret.",
                                   category="secrets"))
    return out


def _control_plane_service_account(values: dict) -> str:
    """The control plane's ServiceAccount name, when the values alone decide it
    (cairn.serviceAccountName); ``""`` when it depends on the release name."""
    name = str(_get(values, "serviceAccount", "name", default="") or "").strip()
    if name:
        return name
    if not _get(values, "serviceAccount", "create", default=True):
        return "default"
    return str(_get(values, "fullnameOverride", default="") or "").strip()


def _pull_worker_service_account_checks(values: dict) -> list[CheckResult]:
    """The chart's pull-worker ServiceAccount refusals: create=false with no name, and
    the control plane's ServiceAccount (when the values name it)."""
    create = _get(values, "pullWorker", "serviceAccount", "create", default=True)
    name = str(_get(values, "pullWorker", "serviceAccount", "name", default="") or "").strip()
    if not create and not name:
        return [CheckResult("pull_worker_service_account", "fail",
                            "pullWorker.serviceAccount.create=false needs "
                            "pullWorker.serviceAccount.name (the pre-provisioned ServiceAccount).",
                            category="secrets")]
    override = str(_get(values, "fullnameOverride", default="") or "").strip()
    worker = name or (f"{override}-pull-worker" if override else "")
    control = _control_plane_service_account(values)
    if worker and control and worker == control:
        return [CheckResult("pull_worker_service_account", "fail",
                            f"the pull worker's ServiceAccount {worker!r} is the control plane's — "
                            "name another one (pullWorker.serviceAccount.name).",
                            category="secrets")]
    return []


def _pull_worker_token_checks(values: dict) -> list[CheckResult]:
    """The chart's pull-worker token refusals (_helpers.tpl), before a render: no token,
    both values, a non-string token, the JWT secret, and the control plane's Secret.

    The release name is a `helm install` argument, so the control plane's Secret is
    known here only when `secrets.existingSecret` or `fullnameOverride` names it; a
    pull-worker Secret that merely looks like the chart's default name is a warning."""
    def fail(msg: str) -> CheckResult:
        return CheckResult("pull_worker_token", "fail", msg, category="secrets")

    raw = _get(values, "pullWorker", "token", default="")
    if raw and not isinstance(raw, str):
        return [fail(f"pullWorker.token must be a string, not {type(raw).__name__} — quote it.")]
    token = (raw or "").strip()
    existing = str(_get(values, "pullWorker", "existingSecret", default="") or "").strip()
    if token and existing:
        return [fail("pullWorker.token AND pullWorker.existingSecret are both set — pick one.")]
    if not token and not existing:
        return [fail("pullWorker.enabled=true but neither pullWorker.token nor "
                     "pullWorker.existingSecret is set (a whitespace-only token counts as "
                     "unset) — the pull worker reads a worker-pool token (`cairn remote "
                     "worker-token --name <pool>`) from a Secret of its own.")]
    jwt = str(_get(values, "secrets", "obsJwtSecret", default="") or "").strip()
    if token and jwt and token == jwt:
        return [fail("pullWorker.token equals secrets.obsJwtSecret — the worker must never "
                     "hold the control plane's JWT secret.")]
    if existing:
        cp = str(_get(values, "secrets", "existingSecret", default="") or "").strip()
        override = str(_get(values, "fullnameOverride", default="") or "").strip()
        known = {n for n in (cp, f"{override}-secrets" if override and not cp else "") if n}
        if existing in known:
            return [fail(f"pullWorker.existingSecret is {existing!r}, the control plane's "
                         "Secret — give the worker a Secret of its own.")]
        if not cp and existing.endswith("-secrets"):
            return [CheckResult(
                "pull_worker_token", "warn",
                f"pullWorker.existingSecret {existing!r} looks like the chart's control-plane "
                "Secret name (<release>-secrets); the chart refuses it if it is.",
                category="secrets")]
    return []


def check_values(values: dict, strict: bool = False) -> list[CheckResult]:
    """The chart-only rules — everything else comes from ``check_deploy``."""
    out: list[CheckResult] = []

    # ── database: bundled XOR external (the chart refuses to render otherwise) ──
    bundled = bool(_get(values, "postgres", "enabled", default=True))
    external = str(_get(values, "postgres", "external", "dsn", default=""))
    if bundled and external:
        out.append(CheckResult(
            "postgres", "fail",
            "postgres.enabled=true AND postgres.external.dsn is set — the chart would "
            "stand up a StatefulSet nothing connects to. Set postgres.enabled=false.",
            category="required"))
    elif not bundled and not external:
        out.append(CheckResult(
            "postgres", "fail",
            "no database: postgres.enabled=false and postgres.external.dsn is empty. "
            "The run store, the LangGraph checkpointer and the DBOS system schema all "
            "live in Postgres.",
            category="required"))
    elif bundled and not _get(values, "postgres", "password"):
        out.append(CheckResult(
            "postgres", "fail",
            "postgres.enabled=true but postgres.password is empty (openssl rand -hex 16)",
            category="required"))
    else:
        out.append(CheckResult(
            "postgres", "ok",
            "bundled StatefulSet" if bundled else "external DSN", category="required"))

    # ── the image tag ────────────────────────────────────────────────────
    # A rolling tag makes "what is deployed" unanswerable by SHA, which is the
    # property the platform's ship script exists to guarantee on the compose side.
    tag = str(_get(values, "image", "tag", default="")).strip()
    if not tag or tag == "latest":
        out.append(CheckResult(
            "image_tag", "fail" if strict else "warn",
            f"image.tag={tag or '(unset → Chart.appVersion)'} — pin a version tag or a "
            "digest so a rollback and an audit can name the exact image "
            "(engine-image.yml publishes :<sha> for every commit on main)",
            category="required"))
    else:
        out.append(CheckResult("image_tag", "ok", tag, category="required"))

    # ── secrets the doctor cannot read ───────────────────────────────────
    existing = str(_get(values, "secrets", "existingSecret", default=""))
    if existing:
        out.append(CheckResult(
            "existing_secret", "warn",
            f"secrets come from the existing Secret {existing!r} — this check cannot see "
            "its contents. Confirm it carries CAIRN_JWT_SECRET, CAIRN_API_PASSWORD_HASH and "
            "(with a bundled Postgres) POSTGRES_PASSWORD.",
            category="secrets"))

    # ── the pull worker (D2, #565) ───────────────────────────────────────
    if _get(values, "pullWorker", "enabled") and not _get(values, "pullWorker", "command"):
        out.append(CheckResult(
            "pull_worker", "fail",
            "pullWorker.enabled=true but pullWorker.command is empty — the chart does not "
            "hard-code D2's command (#565); supply it explicitly.",
            category="runtime"))
    if _get(values, "pullWorker", "enabled"):
        out.extend(_pull_worker_token_checks(values))
        out.extend(_pull_worker_extra_env_checks(values))
        out.extend(_pull_worker_service_account_checks(values))

    # ── ingress ──────────────────────────────────────────────────────────
    if _get(values, "ingress", "enabled", default=True):
        if not _get(values, "ingress", "tls", "enabled", default=True):
            out.append(CheckResult(
                "ingress_tls", "warn",
                "ingress.tls.enabled=false — the control plane would serve the console "
                "and its JWT over plaintext HTTP", category="ingress"))
        elif not _get(values, "ingress", "certManager", "clusterIssuer"):
            out.append(CheckResult(
                "ingress_tls", "warn",
                "TLS is on but ingress.certManager.clusterIssuer is empty — nothing will "
                f"issue the certificate; create {_get(values, 'ingress', 'tls', 'secretName', default='the TLS secret')} yourself",
                category="ingress"))
        else:
            out.append(CheckResult(
                "ingress_tls", "ok",
                f"cert-manager issuer {_get(values, 'ingress', 'certManager', 'clusterIssuer')}",
                category="ingress"))

    # ── profiles this chart delegates to an upstream chart ───────────────
    delegated = {
        "observability": "prometheus-community/kube-prometheus-stack",
        "otel": "open-telemetry/opentelemetry-collector",
        "infisical": "infisical/infisical-standalone",
        "llm-gateway": "berriai/litellm-helm",
        "metaflow-service": "outerbounds/metaflow-tools",
        "dedicated-temporal-db": "the official temporal chart (or a managed Postgres)",
    }
    for name in sorted(profiles_from_values(values) & set(delegated)):
        out.append(CheckResult(
            f"profile:{name}", "fail",
            f"profiles.{name} is not implemented by this chart — install {delegated[name]} "
            "instead (the chart refuses to render with it enabled)",
            category="profiles"))

    return out


#: Site-address lines in a Caddyfile: an address (or comma list) then ``{``.
#: Deliberately simple — this is a duplicate-detector, not a Caddy parser; the
#: authoritative check is ``caddy validate``, which only runs where caddy does.
_CADDY_SITE_RE = re.compile(r"^\s*([^\s#{}][^{}]*?)\s*\{\s*$")


def check_caddyfile(path: str | Path) -> list[CheckResult]:
    """Catch a Caddyfile that Caddy will refuse to load (#1565).

    Prod was down on every boot because the Caddyfile had TWO
    ``mlflow.cairndev.sh`` site blocks: Caddy exits with ``ambiguous site
    definition`` and crash-loops, so the whole ingress dies — the app, not just
    the duplicated vhost. Nothing preflighted it, and the box copy is
    rsync-excluded (gotcha #10), so a hand-edit was the only thing standing
    between a deploy and an outage.

    A duplicate address is a hard ``fail``: it is never intentional, and its
    blast radius is the entire site.
    """
    p = Path(path)
    if not p.is_file():
        return [CheckResult("caddyfile", "warn", f"not found: {p}", category="ingress")]

    text = p.read_text(encoding="utf-8")
    seen: dict[str, int] = {}
    for i, line in enumerate(text.splitlines(), 1):
        m = _CADDY_SITE_RE.match(line)
        if not m:
            continue
        head = m.group(1).strip()
        if head.startswith(("handle", "route", "@", "basic_auth", "reverse_proxy",
                            "header", "tls", "log", "encode", "file_server")):
            continue          # a directive block, not a site address
        for addr in (a.strip() for a in head.split(",")):
            if not addr or addr.startswith("$") or "{$" in addr:
                continue      # env-substituted address — value unknown here
            if addr in seen:
                return [CheckResult(
                    "caddyfile", "fail",
                    f"duplicate site {addr!r} (lines {seen[addr]} and {i}) — Caddy "
                    f"refuses the whole config with 'ambiguous site definition' and "
                    f"crash-loops, taking the ENTIRE ingress down, not just this "
                    f"vhost (#1565). Merge the blocks; basic_auth accepts multiple "
                    f"users if you are keeping two credentials.",
                    category="ingress")]
            seen[addr] = i
    out = [CheckResult("caddyfile", "ok",
                       f"{len(seen)} site block(s), no duplicates",
                       category="ingress")]
    out.extend(_check_caddy_sse(text))
    return out


#: The console's live-run graph reads these; both are text/event-stream.
_SSE_ROUTES = ("/runs/{id}/events", "/runs/{id}/stream")


def _check_caddy_sse(text: str) -> list[CheckResult]:
    """Warn when a Caddyfile proxies the app but never disables buffering (#774).

    Caddy buffers reverse-proxy responses by default, so the SSE run stream
    arrives only when the buffer flushes: the console sits on "Connected —
    waiting for events…" while the run completes invisibly. Direct to the
    engine it streams fine, which is exactly why this read as an application
    bug for months — it is a proxy setting.

    A WARN, not a fail: an ingress that fronts no SSE (or terminates it
    elsewhere) is legitimate, and the box copy of this file is hand-maintained
    (deploy gotcha #10), so this is the thing that notices when an edit drops
    the flush.
    """
    if "reverse_proxy" not in text:
        return []
    if "flush_interval" in text:
        return [CheckResult("caddy_sse", "ok",
                            "flush_interval set — SSE is not buffered",
                            category="ingress")]
    return [CheckResult(
        "caddy_sse", "warn",
        "reverse_proxy with no `flush_interval` — Caddy BUFFERS responses by "
        "default, so the console's live-run stream (" + ", ".join(_SSE_ROUTES) +
        ") shows 'Connected — waiting for events…' and never updates while the "
        "run completes invisibly (#774). Add a matcher for the SSE routes with "
        "`flush_interval -1`.",
        category="ingress")]


# ─── compute.ssh targets (ADR-0104 D1, #2071) ────────────────────────────

#: What a ``compute.ssh`` connection MUST carry for a dispatch to reach the box.
#: Mirrors :func:`cairn.core.connections.as_ssh_params`, which is where each of
#: these is read at submit time — the point of this check is to ask the same
#: questions BEFORE a run exists rather than after one has been gated and
#: dispatched.
_SSH_REQUIRED = {
    "host": "no host — `as_ssh_params` raises 'compute.ssh connection has no "
            "host' at SUBMIT, after the run is created, gated and dispatched",
    "user": "no user — asyncssh would authenticate as whoever the control "
            "plane's process runs as, which is not the box's account",
}


def _exec_unit(conn_mode: str, conn_image: str, env_mode: str,
               env_image: str) -> tuple[str, str, bool]:
    """What the builtin SSH engine will run on a box: ``(kind, image,
    from_connection)`` with ``kind`` one of ``venv`` / ``image`` / ``refused``.

    A TWIN of ``operator_dataflow.engines_ssh.resolve_exec_mode`` — core may
    not import an operator pack, so the precedence is restated here and
    ``tests/core/test_exec_unit_parity.py`` runs one matrix through both. The
    connection's ``mode`` beats the environment's ``remote_exec_mode``; the
    connection's ``image`` beats ``remote_image``; ``auto`` (or anything else)
    is image when an image is named, else venv; image mode with no image
    anywhere is refused at submit (ADR-0150 D1). ``from_connection`` says
    whether the connection's own ``mode`` chose it, which decides the remedy."""
    mode = (conn_mode or "").strip().lower()
    from_connection = bool(mode)
    if not mode:
        mode = (env_mode or "").strip().lower() or "auto"
    named = (conn_image or "").strip() or (env_image or "").strip()
    if mode not in ("image", "venv"):
        mode = "image" if named else "venv"
    if mode == "venv":
        return "venv", "", from_connection
    if not named:
        return "refused", "", from_connection
    return "image", named, from_connection


def check_compute_ssh(
    key: str,
    target: str,
    config: dict | None,
    *,
    has_key: bool | None = None,
    env_mode: str | None = None,
    env_image: str | None = None,
) -> list[CheckResult]:
    """Validate the ``compute.ssh`` connection an environment names as its
    compute target. Pure: the caller resolves the connection.

    ``key`` is the backend key that named it (``ml_target`` /
    ``dataflow_target``) so the report says which selector to fix; ``target``
    is the connection name; ``config`` is the resolved connection's config
    (``None`` = the connection does not resolve). ``has_key`` says whether a
    ``PRIVATE_KEY`` secret resolved (``None`` = not checked).

    Empty ``target`` returns no rows: an environment with no compute target
    runs ML in-process, which is a legal dev shape.

    A second row reports the EXECUTION UNIT (ADR-0104 D1, ADR-0150 D1) with
    the engine's own precedence (:func:`_exec_unit`): ``env_mode`` /
    ``env_image`` are the environment's selected ``remote_exec_mode`` /
    ``remote_image``. Venv is OK; image mode with no image anywhere is a FAIL,
    because the engine refuses it at submit; an image pinned to ``:latest``
    (or to no tag at all) warns, because the run record then cannot name the
    build that produced its artifacts. A caller that cannot read the
    environment passes neither, and a row whose answer depends on it is a
    conditional warn that says so.
    """
    name = f"compute_target:{key}"
    if not (target or "").strip():
        return []
    target = target.strip()
    if config is None:
        return [CheckResult(
            name, "fail",
            f"{key} names connection {target!r}, which does not resolve — create "
            f"it with `cairn connect add` (`cairn connect ls` shows what exists, "
            f"`cairn connect test {target}` probes it). Every dispatch to this "
            f"environment fails at submit.",
            category="runtime")]

    missing = [f"{field}: {why}" for field, why in _SSH_REQUIRED.items()
               if not str(config.get(field) or "").strip()]
    if has_key is False:
        missing.append(
            "PRIVATE_KEY: no private key resolved for the connection — the SSH "
            "handshake cannot authenticate")
    if missing:
        return [CheckResult(
            name, "fail",
            f"{key} → {target!r} is incomplete · " + " · ".join(missing),
            category="runtime")]

    port = str(config.get("port") or "22")
    out = [CheckResult(
        name, "ok",
        f"{key} → {target!r} = {config.get('user')}@{config.get('host')}:{port}",
        category="runtime")]

    conn_image = str(config.get("image") or "").strip()
    conn_mode = str(config.get("mode") or "").strip()
    mode_name = f"{name}:mode"
    if env_mode is None and env_image is None:
        # The environment was not read: decide only what holds for every
        # environment, and say the rest depends on it.
        outcomes = {_exec_unit(conn_mode, conn_image, m, i)
                    for m in ("auto", "venv", "image") for i in ("", "<environment image>")}
        if len(outcomes) != 1:
            out.append(CheckResult(
                mode_name, "warn",
                f"{target!r}: what runs depends on the environment's "
                f"`remote_exec_mode` / `remote_image`, which were not read here — "
                f"the box's virtualenv when no image is named; image mode with no "
                f"image on the connection or in `remote_image` / CAIRN_TRAIN_IMAGE "
                f"is refused at submit",
                category="runtime"))
            return out
        kind, image, from_connection = outcomes.pop()
    else:
        kind, image, from_connection = _exec_unit(
            conn_mode, conn_image, env_mode or "", env_image or "")
    if kind == "venv":
        out.append(CheckResult(
            mode_name, "ok", f"{target!r} runs from the box's virtualenv (venv mode)",
            category="runtime"))
        return out
    if kind == "refused":
        if from_connection:
            detail = (f"{target!r} sets mode = image but no image is named — every "
                      f"dispatch is refused at submit. Set the connection's `image` "
                      f"to a pinned tag, or change its `mode` (the environment's "
                      f"`remote_exec_mode` does not override a connection's mode).")
        else:
            detail = (f"{target!r} runs in image mode (`remote_exec_mode`) but no "
                      f"image is named — every dispatch is refused at submit. Set "
                      f"`remote_image` / CAIRN_TRAIN_IMAGE or the connection's "
                      f"`image` to a pinned tag, or set `remote_exec_mode` to auto "
                      f"or venv.")
        out.append(CheckResult(mode_name, "fail", detail, category="runtime"))
        return out
    tag = image.rsplit("/", 1)[-1].partition(":")[2]
    if not tag or tag == "latest":
        out.append(CheckResult(
            mode_name, "warn",
            f"{target!r} runs the image {image!r} — a moving tag. The run record "
            f"then cannot name the build that produced its artifacts (the "
            f"ambiguity behind #1776). Pin an immutable tag, e.g. the image "
            f"workflow's :<sha>.",
            category="runtime"))
    else:
        out.append(CheckResult(
            mode_name, "ok", f"{target!r} runs the image {image}", category="runtime"))
    return out


_CATEGORY_ORDER = ("required", "runtime", "ingress", "triggers", "observability", "llm", "secrets", "profiles")


def format_deploy_report(results: list[CheckResult]) -> str:
    """Render grouped ``✓/⚠/✗`` lines + a summary, matching ``cairn doctor``."""
    by_cat: dict[str, list[CheckResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)
    extra = [c for c in by_cat if c not in _CATEGORY_ORDER]
    lines: list[str] = ["cairn deploy doctor", ""]
    for cat in (*_CATEGORY_ORDER, *sorted(extra)):
        rows = by_cat.get(cat)
        if not rows:
            continue
        lines.append(f"[{cat}]")
        lines.extend(f"  {r.symbol} {r.name}: {r.detail}" for r in rows)
        lines.append("")
    n_ok = sum(1 for r in results if r.status == "ok")
    n_warn = sum(1 for r in results if r.status == "warn")
    n_fail = sum(1 for r in results if r.status == "fail")
    lines.append(f"Summary: {n_ok} ok, {n_warn} warn, {n_fail} fail")
    return "\n".join(lines)


def exit_code(results: list[CheckResult], strict: bool = False) -> int:
    """1 if any fail (or, in ``strict``, any warn); else 0."""
    if any(r.status == "fail" for r in results):
        return 1
    if strict and any(r.status == "warn" for r in results):
        return 1
    return 0
