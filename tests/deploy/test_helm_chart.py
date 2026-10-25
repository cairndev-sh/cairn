"""Tests for the Helm chart at ``deploy/helm/cairn`` (ADR-0104 D4, #26).

The chart is a translation of ``docker-compose.yml`` onto Kubernetes:
Caddy becomes an Ingress, the compose profiles become value toggles, the ``.env``
becomes a ConfigMap + Secret. A translation is only correct if nothing is lost,
so the load-bearing test here is ``test_server_env_covers_every_compose_cairn_key``:
it parses the compose file, collects every ``CAIRN_*`` variable compose sets on the
``server`` service, renders the chart, and asserts the set difference is empty.
Adding a ``CAIRN_*`` to compose without adding it to the chart fails this suite.

``helm`` is a local dev tool, not a project dependency: when it is absent the
helm-dependent tests skip and only the pure-YAML checks run. CI installs it
(``.github/workflows/helm-lint.yml``), so the gate is real there.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "cairn"
COMPOSE = REPO / "deploy" / "prod" / "docker-compose.yml"
COMPOSE_DBOS = REPO / "deploy" / "prod" / "docker-compose.dbos-postgres.yml"

HELM = shutil.which("helm")
needs_helm = pytest.mark.skipif(HELM is None, reason="helm is not installed on this machine")

#: The chart is checked against files only the platform repository carries: the production
#: compose stack it translates, and the workflows that build its image and lint it. The
#: public tree has the chart but not those files (#2428), so these cases run where the files
#: are, and `test_the_platform_cases_run_wherever_the_platform_is` keeps the skip honest.
ENGINE_IMAGE_WF = REPO / ".github" / "workflows" / "engine-image.yml"
HELM_LINT_WF = REPO / ".github" / "workflows" / "helm-lint.yml"
PLATFORM_FILES = (COMPOSE, COMPOSE_DBOS, ENGINE_IMAGE_WF, HELM_LINT_WF)
needs_platform_files = pytest.mark.skipif(
    not all(f.is_file() for f in PLATFORM_FILES),
    reason="the platform's compose stack and image workflows are not in this tree")


def test_the_platform_cases_run_wherever_the_platform_is():
    """A skip that can fire in the monorepo would silently drop the compose parity
    checks. The console is the platform's too: where it is, the files must be."""
    if (REPO / "console" / "package.json").is_file():
        assert [f.name for f in PLATFORM_FILES if not f.is_file()] == []


# ── helpers ──────────────────────────────────────────────────────────────


def _values(name: str) -> Path:
    return CHART / name


def render(*values_files: str, sets: dict[str, str] | None = None) -> list[dict]:
    """``helm template`` the chart and return the parsed manifests."""
    cmd = [HELM, "template", "cairn", str(CHART)]
    for v in values_files:
        cmd += ["-f", str(_values(v))]
    for k, val in (sets or {}).items():
        cmd += ["--set", f"{k}={val}"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr}"
    return [d for d in yaml.safe_load_all(proc.stdout) if d]


def render_error(*values_files: str, sets: dict[str, str] | None = None) -> str:
    """``helm template`` expected to FAIL; returns stderr."""
    cmd = [HELM, "template", "cairn", str(CHART)]
    for v in values_files:
        cmd += ["-f", str(_values(v))]
    for k, val in (sets or {}).items():
        cmd += ["--set", f"{k}={val}"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode != 0, f"expected helm template to fail, it succeeded:\n{proc.stdout[:2000]}"
    return proc.stderr


def _kinds(docs: list[dict], kind: str) -> list[dict]:
    return [d for d in docs if d.get("kind") == kind]


def _by_name(docs: list[dict], kind: str, suffix: str) -> dict:
    matches = [
        d for d in _kinds(docs, kind)
        if d["metadata"]["name"].endswith(suffix)
    ]
    assert matches, f"no {kind} ending in {suffix!r}; got " + ", ".join(
        f'{d["kind"]}/{d["metadata"]["name"]}' for d in docs
    )
    return matches[0]


def _pod_spec(workload: dict) -> dict:
    return workload["spec"]["template"]["spec"]


def _main_container(workload: dict) -> dict:
    return _pod_spec(workload)["containers"][0]


def effective_env(docs: list[dict], workload: dict) -> dict[str, str | None]:
    """Resolve a container's env: inline ``env`` + every ``envFrom`` source.

    ConfigMap/Secret references are resolved against the manifests rendered in
    the same pass, so this is what the container actually sees — a key that the
    chart names but never defines cannot pass.
    """
    container = _main_container(workload)
    out: dict[str, str | None] = {}
    for ref in container.get("envFrom", []) or []:
        if "configMapRef" in ref:
            src = {d["metadata"]["name"]: d for d in _kinds(docs, "ConfigMap")}
            cm = src[ref["configMapRef"]["name"]]
            out.update({k: str(v) for k, v in (cm.get("data") or {}).items()})
        if "secretRef" in ref:
            src = {d["metadata"]["name"]: d for d in _kinds(docs, "Secret")}
            sec = src.get(ref["secretRef"]["name"])
            if sec is not None:
                out.update({k: None for k in (sec.get("stringData") or sec.get("data") or {})})
    for item in container.get("env", []) or []:
        out[item["name"]] = item.get("value")
    return out


def compose_cairn_keys(path: Path, service: str) -> set[str]:
    """Every ``CAIRN_*`` key the compose file sets on ``service`` (ADR-0122)."""
    doc = yaml.safe_load(path.read_text())
    env = doc["services"][service].get("environment") or {}
    keys = env.keys() if isinstance(env, dict) else {e.split("=", 1)[0] for e in env}
    return {k for k in keys if k.startswith("CAIRN_")}


# ── the chart exists and is well-formed ──────────────────────────────────


def test_chart_directory_exists():
    assert (CHART / "Chart.yaml").is_file(), f"no chart at {CHART}"
    assert (CHART / "values.yaml").is_file()
    assert (CHART / "README.md").is_file()


def test_example_values_files_exist():
    names = sorted(p.name for p in CHART.glob("values-*.example.yaml"))
    assert names == [
        "values-external-dsn.example.yaml",
        "values-postgres.example.yaml",
        "values-temporal.example.yaml",
    ], names


def test_chart_metadata_is_sane():
    meta = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert meta["name"] == "cairn"
    assert meta["apiVersion"] == "v2"
    assert meta["type"] == "application"
    # No sub-charts: the chart owns its Postgres StatefulSet or takes an
    # external DSN (ADR-0104 D4 — no bitnami dependency to vendor).
    assert not meta.get("dependencies")


@needs_helm
def test_helm_lint_is_clean():
    proc = subprocess.run([HELM, "lint", str(CHART)], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "0 chart(s) failed" in proc.stdout


# ── the three supported shapes render ────────────────────────────────────


@needs_helm
def test_renders_with_bundled_postgres():
    docs = render("values-postgres.example.yaml")
    sts = _by_name(docs, "StatefulSet", "postgres")
    assert _main_container(sts)["image"].startswith("pgvector/pgvector:")
    server = _by_name(docs, "Deployment", "server")
    env = effective_env(docs, server)
    assert "@cairn-postgres:5432/" in (env["CAIRN_STORAGE"] or "")


@needs_helm
def test_renders_with_external_dsn_and_no_bundled_postgres():
    docs = render("values-external-dsn.example.yaml")
    assert not _kinds(docs, "StatefulSet"), "external DSN must not bundle Postgres"
    server = _by_name(docs, "Deployment", "server")
    env = effective_env(docs, server)
    assert "db.example.com" in (env["CAIRN_STORAGE"] or "")


@needs_helm
def test_renders_temporal_runtime_with_worker():
    docs = render("values-temporal.example.yaml")
    names = {d["metadata"]["name"] for d in _kinds(docs, "Deployment")}
    assert any(n.endswith("-worker") for n in names), names
    assert any(n.endswith("-temporal") for n in names), names
    worker = _by_name(docs, "Deployment", "-worker")
    assert _main_container(worker)["command"] == ["cairn-worker"]
    server = _by_name(docs, "Deployment", "server")
    assert _main_container(server)["command"] == ["cairn-server"]
    # Same image for both — one build, two commands (as compose does).
    assert _main_container(worker)["image"] == _main_container(server)["image"]


@needs_helm
def test_dbos_runtime_starts_no_worker_and_no_temporal():
    docs = render("values-postgres.example.yaml")
    names = {d["metadata"]["name"] for d in _kinds(docs, "Deployment")}
    assert not any(n.endswith(("-worker", "-temporal")) for n in names), names


# ── the translation loses nothing (the load-bearing check) ───────────────


@needs_platform_files
@needs_helm
def test_server_env_covers_every_compose_cairn_key():
    """Every ``CAIRN_*`` compose sets on `server` must reach the chart's server."""
    wanted = compose_cairn_keys(COMPOSE, "server")
    assert wanted, "parsed no CAIRN_* keys out of compose — the parser is wrong"
    docs = render("values-temporal.example.yaml")
    got = set(effective_env(docs, _by_name(docs, "Deployment", "server")))
    assert wanted - got == set(), f"chart drops compose CAIRN_* keys: {sorted(wanted - got)}"


@needs_platform_files
@needs_helm
def test_worker_env_covers_every_compose_cairn_key():
    wanted = compose_cairn_keys(COMPOSE, "worker")
    docs = render("values-temporal.example.yaml")
    got = set(effective_env(docs, _by_name(docs, "Deployment", "-worker")))
    assert wanted - got == set(), f"chart drops compose CAIRN_* keys: {sorted(wanted - got)}"


@needs_platform_files
@needs_helm
def test_server_env_covers_the_dbos_tier_cairn_keys():
    """Prod runs the DBOS tier (docker-compose.dbos-postgres.yml), not Temporal."""
    wanted = compose_cairn_keys(COMPOSE_DBOS, "server")
    docs = render("values-postgres.example.yaml")
    got = set(effective_env(docs, _by_name(docs, "Deployment", "server")))
    assert wanted - got == set(), f"chart drops dbos-tier CAIRN_* keys: {sorted(wanted - got)}"


# ── the Postgres XOR, validated in _helpers.tpl ──────────────────────────


@needs_helm
def test_bundled_postgres_and_external_dsn_together_is_refused():
    err = render_error("values-postgres.example.yaml",
                       sets={"postgres.external.dsn": "postgresql://a:b@c/d"})
    assert "postgres.enabled" in err and "postgres.external.dsn" in err


@needs_helm
def test_neither_bundled_postgres_nor_external_dsn_is_refused():
    err = render_error("values-postgres.example.yaml", sets={"postgres.enabled": "false"})
    assert "postgres.external.dsn" in err


@needs_helm
@pytest.mark.parametrize("value", ["", "  ", "null"])
def test_a_chart_without_a_deployment_profile_is_refused(value):
    """#2296 — no silent default: the engine refuses to start without CAIRN_PROFILE."""
    err = render_error("values-postgres.example.yaml", sets={"env.profile": value})
    assert "env.profile is empty" in err and "deployment-profile.md" in err


@needs_helm
def test_the_profile_reaches_the_engine_env():
    docs = render("values-postgres.example.yaml")
    cm = next(d for d in _kinds(docs, "ConfigMap") if "CAIRN_PROFILE" in d.get("data", {}))
    assert cm["data"]["CAIRN_PROFILE"].endswith("/cairn/profiles/secure.toml")  # a public profile (ADR-0134 D5)


# ── ingress replaces Caddy ───────────────────────────────────────────────


@needs_helm
def test_ingress_carries_host_tls_and_cert_manager_annotation():
    docs = render("values-postgres.example.yaml")
    ing = _kinds(docs, "Ingress")
    assert len(ing) == 1, ing
    spec = ing[0]["spec"]
    assert spec["rules"][0]["host"] == "cairn.example.com"
    assert spec["tls"][0]["hosts"] == ["cairn.example.com"]
    ann = ing[0]["metadata"].get("annotations") or {}
    assert ann.get("cert-manager.io/cluster-issuer") == "letsencrypt-prod"


@needs_helm
def test_no_caddy_anywhere():
    docs = render("values-postgres.example.yaml")
    blob = json.dumps(docs)
    assert "caddy" not in blob.lower(), "the chart must not ship Caddy — Ingress replaces it"


@needs_helm
def test_ingress_routes_sse_without_proxy_buffering():
    """#774 — Caddy needed `flush_interval -1`; nginx needs proxy-buffering off."""
    docs = render("values-postgres.example.yaml")
    ann = _kinds(docs, "Ingress")[0]["metadata"].get("annotations") or {}
    assert ann.get("nginx.ingress.kubernetes.io/proxy-buffering") == "off"


# ── probes, secrets, image pull ──────────────────────────────────────────


@needs_helm
def test_readiness_and_liveness_probe_the_real_health_route():
    docs = render("values-postgres.example.yaml")
    c = _main_container(_by_name(docs, "Deployment", "server"))
    for probe in ("readinessProbe", "livenessProbe"):
        assert c[probe]["httpGet"]["path"] == "/healthz", probe
        assert c[probe]["httpGet"]["port"] == 9090, probe


def test_probe_route_is_public_in_the_app():
    """/healthz must stay in the app's unauthenticated set or probes 401."""
    auth = (REPO / "src" / "cairn" / "core" / "triggers" / "auth.py").read_text()
    assert '"/healthz"' in auth.split("_OPEN_PATHS")[1][:400]


@needs_helm
def test_secret_is_rendered_from_values():
    docs = render("values-postgres.example.yaml")
    sec = _by_name(docs, "Secret", "-secrets")
    keys = set(sec.get("stringData") or {})
    assert {"CAIRN_JWT_SECRET", "CAIRN_API_PASSWORD_HASH", "POSTGRES_PASSWORD"} <= keys, keys


@needs_helm
def test_existing_secret_replaces_the_rendered_one():
    docs = render("values-postgres.example.yaml", sets={"secrets.existingSecret": "my-secret"})
    assert not [d for d in _kinds(docs, "Secret") if d["metadata"]["name"].endswith("-secrets")]
    server = _by_name(docs, "Deployment", "server")
    refs = [r["secretRef"]["name"] for r in _main_container(server)["envFrom"] if "secretRef" in r]
    assert refs == ["my-secret"], refs


@needs_helm
def test_image_pull_secrets_are_wired_for_the_private_ghcr_period():
    """Until the owner flips the GHCR package public, the chart needs a pull secret."""
    docs = render("values-postgres.example.yaml",
                  sets={"imagePullSecrets[0].name": "ghcr-pull"})
    server = _by_name(docs, "Deployment", "server")
    assert _pod_spec(server)["imagePullSecrets"] == [{"name": "ghcr-pull"}]


@needs_helm
def test_default_image_is_the_public_engine_repo():
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["image"]["repository"] == "ghcr.io/cairndev-sh/cairn-engine"


@needs_helm
def test_resources_and_requests_are_set():
    docs = render("values-postgres.example.yaml")
    res = _main_container(_by_name(docs, "Deployment", "server"))["resources"]
    assert res["requests"]["memory"] and res["limits"]["memory"]


# ── the pull worker (D2, #565) is declared but OFF ───────────────────────


@needs_helm
def test_pull_worker_is_off_by_default():
    docs = render("values-postgres.example.yaml")
    assert not [d for d in _kinds(docs, "Deployment")
                if d["metadata"]["name"].endswith("-pull-worker")]


_PULL = {
    "pullWorker.enabled": "true",
    "pullWorker.command[0]": "cairn",
    "pullWorker.command[1]": "worker",
    "pullWorker.command[2]": "--pull",
}

#: What the control plane's Secret carries; none of it may reach the pull worker.
_CONTROL_PLANE_KEYS = {"CAIRN_JWT_SECRET", "CAIRN_API_PASSWORD_HASH", "POSTGRES_PASSWORD",
                       "OPENAI_API_KEY", "SSH_PRIVATE_KEY"}


@needs_helm
def test_pull_worker_renders_with_the_operator_supplied_command():
    docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": "wp-token"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    assert _main_container(dep)["command"] == ["cairn", "worker", "--pull"]


@needs_helm
def test_pull_worker_without_a_command_is_refused():
    err = render_error("values-postgres.example.yaml",
                       sets={"pullWorker.enabled": "true", "pullWorker.token": "wp-token"})
    assert "pullWorker.command" in err


@needs_helm
def test_pull_worker_reads_its_token_from_its_own_secret():
    """The worker gets CAIRN_ENGINE_TOKEN from a Secret of its own and none of the
    control plane's Secret keys (values: test_no_worker_env_value_carries_a_control_plane_secret)."""
    docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": "wp-token"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    secret = _by_name(docs, "Secret", "-pull-worker")
    assert secret["stringData"] == {"CAIRN_ENGINE_TOKEN": "wp-token"}
    refs = [r["secretRef"]["name"] for r in _main_container(dep)["envFrom"] if "secretRef" in r]
    assert refs == [secret["metadata"]["name"]]
    env = effective_env(docs, dep)
    assert "CAIRN_ENGINE_TOKEN" in env
    assert not (_CONTROL_PLANE_KEYS & set(env)), _CONTROL_PLANE_KEYS & set(env)
    # the server still reads the control plane's Secret, and not the worker's
    server = effective_env(docs, _by_name(docs, "Deployment", "-server"))
    assert "CAIRN_JWT_SECRET" in server and "CAIRN_ENGINE_TOKEN" not in server


@needs_helm
def test_pull_worker_takes_an_existing_secret():
    docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.existingSecret": "pool-gpu"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    assert not [d for d in _kinds(docs, "Secret") if d["metadata"]["name"].endswith("-pull-worker")]
    refs = [r["secretRef"]["name"] for r in _main_container(dep)["envFrom"] if "secretRef" in r]
    assert refs == ["pool-gpu"]


@needs_helm
def test_pull_worker_renders_no_secret_when_disabled():
    docs = render("values-postgres.example.yaml")
    assert not [d for d in docs if d["metadata"]["name"].endswith("-pull-worker")]


@needs_helm
@pytest.mark.parametrize("extra,needle", [
    ({}, "needs the worker's own token"),
    ({"pullWorker.token": "a", "pullWorker.existingSecret": "b"}, "pick one"),
    ({"pullWorker.existingSecret": "cairn-secrets"}, "the control plane's Secret"),
])
def test_pull_worker_token_is_refused_unless_its_own(extra, needle):
    err = render_error("values-postgres.example.yaml", sets={**_PULL, **extra})
    assert needle in err, err


@needs_helm
def test_pull_worker_token_must_not_be_the_jwt_secret():
    jwt = yaml.safe_load(_values("values-postgres.example.yaml").read_text())["secrets"]["obsJwtSecret"]
    err = render_error("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": jwt})
    assert "JWT secret" in err, err


@needs_helm
def test_pull_worker_token_is_trimmed():
    docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": " wp-token "})
    assert _by_name(docs, "Secret", "-pull-worker")["stringData"] == {"CAIRN_ENGINE_TOKEN": "wp-token"}


@needs_helm
@pytest.mark.parametrize("blank", ["   ", "\n", " \t\n "])
def test_a_whitespace_only_token_is_refused(blank):
    err = _render_error_with_file({**_PULL_VALUES, "token": blank})
    assert "needs the worker's own token" in err, err


@needs_helm
def test_a_jwt_secret_with_surrounding_whitespace_is_still_refused():
    jwt = yaml.safe_load(_values("values-postgres.example.yaml").read_text())["secrets"]["obsJwtSecret"]
    err = _render_error_with_file({**_PULL_VALUES, "token": f"  {jwt}\n"})
    assert "JWT secret" in err, err


@needs_helm
def test_a_non_string_token_is_refused():
    err = render_error("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": "12345"})
    assert "must be a string" in err and "--set-string" in err, err


@needs_helm
def test_a_token_change_rolls_the_worker():
    def checksum(tok: str) -> str:
        docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": tok})
        dep = _by_name(docs, "Deployment", "-pull-worker")
        return dep["spec"]["template"]["metadata"]["annotations"]["checksum/token"]

    assert checksum("wp-a") != checksum("wp-b")
    assert checksum("wp-a") == checksum(" wp-a ")          # trimmed before hashing
    docs = render("values-postgres.example.yaml", sets={**_PULL, "pullWorker.existingSecret": "pool"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    assert "checksum/token" not in dep["spec"]["template"]["metadata"]["annotations"]


@needs_helm
def test_an_existing_secret_equal_to_a_custom_control_plane_secret_is_refused():
    err = render_error("values-postgres.example.yaml", sets={
        **_PULL, "secrets.existingSecret": "my-control-plane",
        "pullWorker.existingSecret": "my-control-plane"})
    assert "the control plane's Secret" in err, err


_PULL_VALUES = {"enabled": True, "command": ["cairn", "worker", "--pull"]}


def _render_error_with_file(pull: dict) -> str:
    """A values file, so whitespace and newlines reach the chart verbatim."""
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump({"pullWorker": pull}, f)
    cmd = [HELM, "template", "cairn", str(CHART), "-f", str(_values("values-postgres.example.yaml")),
           "-f", f.name]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode != 0, f"expected helm template to fail:\n{proc.stdout[:2000]}"
    return proc.stderr


def _doctor(pull: dict, **top) -> dict[str, str]:
    from cairn.core.deploy_check import check_values

    values = yaml.safe_load(_values("values-postgres.example.yaml").read_text())
    values["pullWorker"] = {"enabled": True, "command": ["cairn", "worker", "--pull"], **pull}
    for k, v in top.items():
        values[k] = v
    return {r.name: r.status for r in check_values(values) if r.name == "pull_worker_token"}


# ── the pull worker sees no control-plane secret (option A, 2026-10-24) ──

_OTEL_HEADERS = "x-honeycomb-team=hc-otel-header-value"


def _value_env(docs: list[dict], workload: dict, *, external: str = "") -> dict[str, str]:
    """Like :func:`effective_env`, but with VALUES: ConfigMap data, Secret stringData
    and inline env, resolved against the manifests rendered in the same pass.

    Every ``envFrom`` source must resolve to a rendered manifest, except ``external``
    (the worker's own ``existingSecret``), which may be the ONLY one that does not: an
    unresolvable reference would otherwise hide whatever it carries."""
    container = _main_container(workload)
    out: dict[str, str] = {}
    unresolved: list[str] = []
    for ref in container.get("envFrom", []) or []:
        if "configMapRef" in ref:
            name = ref["configMapRef"]["name"]
            cm = {d["metadata"]["name"]: d for d in _kinds(docs, "ConfigMap")}.get(name)
            if cm is None:
                unresolved.append(f"ConfigMap/{name}")
                continue
            out.update({k: str(v) for k, v in (cm.get("data") or {}).items()})
        if "secretRef" in ref:
            name = ref["secretRef"]["name"]
            sec = {d["metadata"]["name"]: d for d in _kinds(docs, "Secret")}.get(name)
            if sec is None:
                unresolved.append(f"Secret/{name}")
                continue
            out.update({k: str(v) for k, v in (sec.get("stringData") or {}).items()})
    for item in container.get("env", []) or []:
        out[item["name"]] = str(item.get("value"))
    assert unresolved == ([f"Secret/{external}"] if external else []), unresolved
    return out


def _control_plane_secrets(example: str, sets: dict[str, str]) -> list[str]:
    """The control-plane secret values this render carries: the database password
    (bundled) or the whole external DSN, the JWT secret, the OTEL headers."""
    vals = yaml.safe_load(_values(example).read_text())
    found = [vals["postgres"].get("password") or "", (vals["postgres"].get("external") or {}).get("dsn") or "",
             vals["secrets"].get("obsJwtSecret") or "", sets.get("observability.otlp.headers", "")]
    return [v for v in found if v]


@needs_helm
@pytest.mark.parametrize("example,extra", [
    ("values-postgres.example.yaml", {}),                                  # bundled Postgres
    ("values-postgres.example.yaml", {"secrets.existingSecret": "my-control-plane"}),
    ("values-postgres.example.yaml", {"artifacts.s3.enabled": "true"}),
    ("values-external-dsn.example.yaml", {}),                              # an external DSN
    ("values-external-dsn.example.yaml", {"artifacts.s3.enabled": "true"}),
    ("values-temporal.example.yaml", {}),
    ("values-postgres.example.yaml", {"pullWorker.existingSecret": "pool-gpu"}),   # the worker's own
])
def test_no_worker_env_value_carries_a_control_plane_secret(example, extra):
    token = {} if "pullWorker.existingSecret" in extra else {"pullWorker.token": "wp-token"}
    sets = {**_PULL, **token, "observability.otlp.headers": _OTEL_HEADERS,
            "env.extra.SOME_VENDOR_KEY": "vendor-extra-value", **extra}
    secrets = _control_plane_secrets(example, sets)
    assert len(secrets) >= 3, secrets                     # the database, the JWT, the headers
    docs = render(example, sets=sets)
    env = _value_env(docs, _by_name(docs, "Deployment", "-pull-worker"),
                     external=extra.get("pullWorker.existingSecret", ""))
    leaks = sorted(k for k, v in env.items() if any(s in v for s in secrets))
    assert not leaks, leaks
    assert "vendor-extra-value" not in env.values()       # env.extra is not the worker's
    if token:
        assert env["CAIRN_ENGINE_TOKEN"] == "wp-token"


@needs_helm
def test_the_worker_configmap_is_an_allowlist():
    docs = render("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token", "pullWorker.engineUrl": "https://cp.example",
        "artifacts.s3.enabled": "true", "observability.otlp.endpoint": "https://otel.example",
        "observability.otlp.headers": _OTEL_HEADERS, "env.extra.SOME_SETTING": "x",
        "pullWorker.extraEnv.HF_HUB_OFFLINE": "1"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    refs = [r.get("configMapRef", {}).get("name") for r in _main_container(dep)["envFrom"]]
    cm = _by_name(docs, "ConfigMap", "-pull-worker")
    assert refs[0] == cm["metadata"]["name"] and "cairn-config" not in refs
    assert set(cm["data"]) == {
        "CAIRN_ENGINE_URL", "CAIRN_BACKEND", "CAIRN_PROFILE", "MLFLOW_TRACKING_URI",
        "OTEL_EXPORTER_OTLP_ENDPOINT", "CAIRN_ARTIFACT_S3_ENDPOINT", "CAIRN_ARTIFACT_S3_BUCKET",
        "CAIRN_ARTIFACT_S3_REGION", "HF_HUB_OFFLINE"}
    assert cm["data"]["CAIRN_ENGINE_URL"] == "https://cp.example"
    assert cm["data"]["HF_HUB_OFFLINE"] == "1"


@needs_helm
def test_extra_env_changes_roll_the_worker():
    def checksum(v: str) -> str:
        docs = render("values-postgres.example.yaml", sets={
            **_PULL, "pullWorker.token": "wp-token", "pullWorker.extraEnv.HF_HUB_OFFLINE": v})
        dep = _by_name(docs, "Deployment", "-pull-worker")
        return dep["spec"]["template"]["metadata"]["annotations"]["checksum/config"]

    assert checksum("0") != checksum("1")


_UNSAFE_NAMES = ["HF_TOKEN", "APITOKEN", "hf_token", "MY_API_KEY", "MY_API_KEYS", "DB_PASSWORD",
                 "aws_secret", "AWS_SECRET_ACCESS_KEY", "CAIRN_STORAGE", "OBS_STORAGE", "PG_DSN",
                 "CAIRN_DBOS_DSN", "OTEL_EXPORTER_OTLP_HEADERS", "DB_CONNECTION_STRING",
                 "CAIRN_CONN_HF_ENDPOINT"]
_SAFE_NAMES = ["TOKENIZERS_PARALLELISM", "KEYBOARD_LAYOUT", "HF_HUB_OFFLINE", "AWS_ENDPOINT_URL",
               "CAIRN_ENGINE_URL", "CAIRN_CONNECTOR"]


@needs_helm
@pytest.mark.parametrize("name", _UNSAFE_NAMES)
def test_a_secret_looking_extra_env_name_is_refused(name):
    err = render_error("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token", f"pullWorker.extraEnv.{name}": "v"})
    assert f"pullWorker.extraEnv.{name}" in err and "own Secret" in err, err


@needs_helm
def test_ordinary_extra_env_names_render():
    docs = render("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token",
        **{f"pullWorker.extraEnv.{n}": "v" for n in _SAFE_NAMES}})
    data = _by_name(docs, "ConfigMap", "-pull-worker")["data"]
    assert set(_SAFE_NAMES) <= set(data)


@needs_helm
@pytest.mark.parametrize("value", ["https://user:pass@ml.example:5000", "postgresql://u:p@db/cairn",
                                   "redis://:s3cret@cache:6379/0"])
def test_an_extra_env_value_with_a_password_in_a_url_is_refused(value):
    err = render_error("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token", "pullWorker.extraEnv.MLFLOW_TRACKING_URI": value})
    assert "password in it" in err, err
    assert value not in err                               # the value is never echoed


@needs_helm
@pytest.mark.parametrize("value", ["https://ml.example:5000", "https://user@ml.example", "s3://bucket/a:b"])
def test_an_extra_env_value_without_a_url_password_renders(value):
    docs = render("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token", "pullWorker.extraEnv.MLFLOW_TRACKING_URI": value})
    assert _by_name(docs, "ConfigMap", "-pull-worker")["data"]["MLFLOW_TRACKING_URI"] == value


@needs_helm
def test_the_engine_url_falls_back_to_either_spelling():
    for key in ("CAIRN_ENGINE_URL", "OBS_ENGINE_URL"):
        docs = render("values-postgres.example.yaml", sets={
            **_PULL, "pullWorker.token": "wp-token", f"env.extra.{key}": "https://cp.example"})
        assert _by_name(docs, "ConfigMap", "-pull-worker")["data"]["CAIRN_ENGINE_URL"] == "https://cp.example"


@needs_helm
def test_the_worker_has_its_own_service_account():
    docs = render("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token",
        "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn": "arn:aws:iam::1:role/control-plane",
        "pullWorker.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn": "arn:aws:iam::1:role/worker",
        "imagePullSecrets[0].name": "ghcr-pull"})
    dep = _by_name(docs, "Deployment", "-pull-worker")
    pod = _pod_spec(dep)
    sa = _by_name(docs, "ServiceAccount", "-pull-worker")
    assert pod["serviceAccountName"] == sa["metadata"]["name"]
    assert pod["automountServiceAccountToken"] is False and sa["automountServiceAccountToken"] is False
    assert sa["metadata"]["annotations"] == {"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/worker"}
    assert pod["imagePullSecrets"] == [{"name": "ghcr-pull"}]
    server = _pod_spec(_by_name(docs, "Deployment", "-server"))
    assert server["serviceAccountName"] != pod["serviceAccountName"]


@needs_helm
def test_a_pre_provisioned_service_account_can_be_named():
    docs = render("values-postgres.example.yaml", sets={
        **_PULL, "pullWorker.token": "wp-token", "pullWorker.serviceAccount.create": "false",
        "pullWorker.serviceAccount.name": "gpu-pool-worker"})
    pod = _pod_spec(_by_name(docs, "Deployment", "-pull-worker"))
    assert pod["serviceAccountName"] == "gpu-pool-worker"
    assert pod["automountServiceAccountToken"] is False
    assert not [d for d in _kinds(docs, "ServiceAccount") if d["metadata"]["name"] == "gpu-pool-worker"]
    assert not [d for d in _kinds(docs, "ServiceAccount") if d["metadata"]["name"].endswith("-pull-worker")]


@needs_helm
@pytest.mark.parametrize("sets,needle", [
    ({"pullWorker.serviceAccount.create": "false"}, "needs pullWorker.serviceAccount.name"),
    ({"pullWorker.serviceAccount.create": "false", "pullWorker.serviceAccount.name": "cairn"},
     "is the control plane's"),
    ({"pullWorker.serviceAccount.name": "cp-sa", "serviceAccount.name": "cp-sa"}, "is the control plane's"),
])
def test_the_worker_service_account_is_never_the_control_planes(sets, needle):
    err = render_error("values-postgres.example.yaml", sets={**_PULL, "pullWorker.token": "wp-token", **sets})
    assert needle in err, err


def _doctor_named(name: str, pull: dict, **top) -> dict[str, str]:
    from cairn.core.deploy_check import check_values

    values = yaml.safe_load(_values("values-postgres.example.yaml").read_text())
    values["pullWorker"] = {"enabled": True, "command": ["cairn"], "token": "wp", **pull}
    values.update(top)
    return {r.name: r.status for r in check_values(values) if r.name == name}


def test_deploy_doctor_refuses_a_secret_looking_extra_env_name():
    for name in (*_UNSAFE_NAMES, "BAD-NAME"):
        assert _doctor_named("pull_worker_extra_env", {"extraEnv": {name: "v"}}) == {
            "pull_worker_extra_env": "fail"}, name
    for name in _SAFE_NAMES:
        assert _doctor_named("pull_worker_extra_env", {"extraEnv": {name: "v"}}) == {}, name


def test_deploy_doctor_refuses_an_extra_env_value_with_a_url_password():
    bad = {"extraEnv": {"MLFLOW_TRACKING_URI": "https://user:pass@ml.example"}}
    assert _doctor_named("pull_worker_extra_env", bad) == {"pull_worker_extra_env": "fail"}
    ok = {"extraEnv": {"MLFLOW_TRACKING_URI": "https://user@ml.example"}}
    assert _doctor_named("pull_worker_extra_env", ok) == {}


def test_deploy_doctor_checks_the_worker_service_account():
    sa = "pull_worker_service_account"
    assert _doctor_named(sa, {"serviceAccount": {"create": False}}) == {sa: "fail"}
    assert _doctor_named(sa, {"serviceAccount": {"create": False, "name": "cp"}},
                         serviceAccount={"create": True, "name": "cp"}) == {sa: "fail"}
    assert _doctor_named(sa, {"serviceAccount": {"name": "x-pull-worker"}},
                         fullnameOverride="x-pull-worker") == {sa: "fail"}   # control plane SA = fullname
    assert _doctor_named(sa, {"serviceAccount": {"create": False, "name": "gpu-pool-worker"}}) == {}
    assert _doctor_named(sa, {}) == {}


def test_deploy_doctor_matches_the_chart_token_refusals():
    jwt = yaml.safe_load(_values("values-postgres.example.yaml").read_text())["secrets"]["obsJwtSecret"]
    assert _doctor({}) == {"pull_worker_token": "fail"}                       # no token
    assert _doctor({"token": "  \n"}) == {"pull_worker_token": "fail"}       # whitespace only
    assert _doctor({"token": "a", "existingSecret": "b"}) == {"pull_worker_token": "fail"}
    assert _doctor({"token": f" {jwt} "}) == {"pull_worker_token": "fail"}     # the JWT secret
    assert _doctor({"token": 12345}) == {"pull_worker_token": "fail"}          # not a string
    cp = {"existingSecret": "my-control-plane", "obsJwtSecret": jwt}
    assert _doctor({"existingSecret": "my-control-plane"}, secrets=cp) == {"pull_worker_token": "fail"}
    assert _doctor({"existingSecret": "x-cairn-secrets"}, fullnameOverride="x-cairn") == {
        "pull_worker_token": "fail"}
    assert _doctor({"existingSecret": "rel-cairn-secrets"}) == {"pull_worker_token": "warn"}
    assert _doctor({"token": "wp-token"}) == {}
    assert _doctor({"existingSecret": "pool-gpu"}) == {}


# ── the compose profiles are mirrored, not invented ──────────────────────


@needs_platform_files
def test_values_declare_exactly_the_compose_profiles():
    """`profiles:` in values.yaml mirrors the compose profiles 1:1 (no invention)."""
    compose = yaml.safe_load(COMPOSE.read_text())
    from_compose = {
        p for svc in compose["services"].values() for p in (svc.get("profiles") or [])
    }
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert set(values["profiles"]) == from_compose, {
        "only in compose": sorted(from_compose - set(values["profiles"])),
        "only in values": sorted(set(values["profiles"]) - from_compose),
    }


@needs_helm
def test_a_delegated_profile_fails_loudly_rather_than_pretending():
    """Profiles the chart delegates to an upstream chart must refuse, not no-op."""
    err = render_error("values-postgres.example.yaml", sets={"profiles.observability.enabled": "true"})
    assert "kube-prometheus-stack" in err


@needs_helm
def test_nats_profile_sets_the_durable_trigger_inbox():
    docs = render("values-postgres.example.yaml", sets={"profiles.nats.enabled": "true"})
    env = effective_env(docs, _by_name(docs, "Deployment", "server"))
    assert env["CAIRN_TRIGGER_INBOX"].startswith("nats://")
    assert env["CAIRN_TRIGGER_DURABLE"] == "1"
    assert _kinds(docs, "StatefulSet")


@needs_helm
def test_backup_profile_renders_a_cronjob():
    docs = render("values-postgres.example.yaml", sets={"profiles.backup.enabled": "true"})
    assert _kinds(docs, "CronJob")


@needs_helm
def test_temporal_runtime_without_the_temporal_profile_is_refused():
    """The compose-doctor rule (#390), enforced at render time."""
    err = render_error("values-temporal.example.yaml", sets={"profiles.temporal.enabled": "false"})
    assert "runtime" in err.lower() and "temporal" in err.lower()


# ── console: served by the server container, never a second service ──────


@needs_helm
def test_console_has_no_service_of_its_own():
    docs = render("values-postgres.example.yaml")
    svc_names = {d["metadata"]["name"] for d in _kinds(docs, "Service")}
    assert not any("console" in n for n in svc_names), svc_names


# ── the image workflow ───────────────────────────────────────────────────


@needs_platform_files
def test_engine_image_workflow_exists_and_parses():
    wf = ENGINE_IMAGE_WF
    assert wf.is_file(), "no engine-image workflow"
    doc = yaml.safe_load(wf.read_text())
    job = doc["jobs"]["build-and-push"]
    assert job["permissions"]["packages"] == "write"
    assert doc["env"]["IMAGE"] == "ghcr.io/cairndev-sh/cairn-engine"
    tags = [s for s in job["steps"] if "docker/build-push-action" in str(s.get("uses", ""))]
    assert tags, "no build-push step"
    tag_block = tags[0]["with"]["tags"]
    assert "github.sha" in tag_block, "the commit sha must be a pinnable tag"
    # :latest on main, the version on a tag — via docker/metadata-action.
    text = wf.read_text()
    assert "value=latest" in text and "type=semver" in text


@needs_platform_files
def test_engine_image_workflow_builds_the_console_into_the_image():
    """#1339 f2 — without a console build the image ships an API and no UI."""
    wf = ENGINE_IMAGE_WF.read_text()
    assert "npm ci --prefix console" in wf and "npm run build --prefix console" in wf


@needs_platform_files
def test_helm_lint_workflow_gates_every_example_values_file():
    wf = HELM_LINT_WF
    assert wf.is_file()
    text = wf.read_text()
    assert "helm lint" in text and "helm template" in text
    # the OCI push must be tag-gated, never on a branch push
    doc = yaml.safe_load(text)
    push = [s for s in doc["jobs"]["helm-lint"]["steps"] if "helm push" in str(s.get("run", ""))]
    assert push, "no helm push step"
    assert "refs/tags/" in push[0]["if"]


# ── #2073: SkyPilot's state must outlive the pod ─────────────────────────
# The skypilot engine keeps its cluster state in a SQLite database under
# ~/.sky, and cairn's own adapter writes the SSH node pool and the key it
# materialises there too. The chart mounted only /root/.cairn, so a pod restart
# lost all of it — and a cluster SkyPilot can no longer find is a rented machine
# that bills with nothing able to `sky down` it. A cost leak that survives the
# thing that caused it.


@needs_helm
def test_the_server_persists_the_skypilot_state_dir():
    docs = render("values-postgres.example.yaml")
    dep = _by_name(docs, "Deployment", "server")
    container = dep["spec"]["template"]["spec"]["containers"][0]
    mounts = {m["mountPath"]: m for m in container.get("volumeMounts", [])}
    assert "/root/.sky" in mounts, (
        "the server must persist /root/.sky — SkyPilot's cluster state lives "
        "there, and losing it orphans paid clusters (#2073)")
    vols = {v["name"]: v for v in dep["spec"]["template"]["spec"].get("volumes", [])}
    sky = mounts["/root/.sky"]
    assert "persistentVolumeClaim" in vols[sky["name"]], (
        "/root/.sky must come from a PVC, not an emptyDir")


@needs_helm
@pytest.mark.parametrize("name", ["CAIRN_ML_ENGINE", "OBS_ML_ENGINE"])
def test_skypilot_without_persistence_is_refused(name):
    """Ephemeral state + a provisioning engine = orphaned paid machines (either spelling)."""
    err = render_error("values-postgres.example.yaml",
                       sets={"server.persistence.enabled": "false",
                             f"env.extra.{name}": "skypilot"})
    assert "skypilot" in err.lower()
    assert "persistence" in err.lower()


# ── #2429: CAIRN_* keys; the DSN family keeps an OBS_ twin until 1.0 ─────
# cairn reads CAIRN_<NAME> first and OBS_<NAME> only when it is unset, so a CAIRN_
# DSN rendered into the ConfigMap would shadow an OBS_-spelled existingSecret key.
# With no existingSecret both names render; with one, only the OBS_ name does.

_DSN_FAMILY = ("STORAGE", "CHECKPOINTER_DSN", "DBOS_DSN")


def _config(docs: list[dict]) -> dict[str, str]:
    return {k: str(v) for d in _kinds(docs, "ConfigMap") for k, v in (d.get("data") or {}).items()}


@needs_helm
def test_the_dsn_family_renders_both_names_and_every_other_key_is_cairn():
    data = _config(render("values-postgres.example.yaml"))
    assert {k for k in data if k.startswith("OBS_")} == {f"OBS_{n}" for n in _DSN_FAMILY}
    for n in _DSN_FAMILY:
        assert data[f"CAIRN_{n}"] == data[f"OBS_{n}"] and "@cairn-postgres:5432/" in data[f"CAIRN_{n}"]


@needs_helm
@pytest.mark.parametrize("secret_key", ["CAIRN_STORAGE", "OBS_STORAGE", None])
def test_an_existing_secret_in_either_spelling_wins(secret_key):
    from cairn.core.settings import getenv

    docs = render("values-postgres.example.yaml", sets={"secrets.existingSecret": "mine"})
    data = _config(docs)
    assert {k for k in data if k.startswith(("CAIRN_STORAGE", "CAIRN_CHECKPOINTER_DSN", "CAIRN_DBOS_DSN"))} == set()
    env = {**data, **({secret_key: "postgres://mine"} if secret_key else {})}  # the Secret is envFrom'd last
    want = "postgres://mine" if secret_key else data["OBS_STORAGE"]
    assert getenv("CAIRN_STORAGE", environ=env) == want


@needs_helm
@pytest.mark.parametrize("example", ["values-postgres.example.yaml", "values-external-dsn.example.yaml",
                                     "values-temporal.example.yaml"])
def test_deploy_doctor_sees_what_the_chart_renders(example):
    """``deploy_check.env_from_values`` agrees with the rendered ConfigMap + Secret: every
    cairn name it produces resolves to the same value from the chart's env."""
    from cairn.core.deploy_check import env_from_values
    from cairn.core.settings import getenv

    docs = render(example)
    chart = {**_config(docs), **{k: str(v) for d in _kinds(docs, "Secret")
                                 for k, v in (d.get("stringData") or {}).items()}}
    doctor = env_from_values(yaml.safe_load(_values(example).read_text()))
    cairn_keys = sorted(k for k in doctor if k.startswith("CAIRN_"))
    assert cairn_keys and not [k for k in doctor if k.startswith("OBS_")]
    assert {k: getenv(k, environ=chart) for k in cairn_keys} == {k: doctor[k] for k in cairn_keys}
