"""``cairn deploy doctor --values`` — the chart half of the preflight (#26, D4).

Compose deploys are validated from a ``.env`` + enabled profiles
(``test_deploy_check.py``). A chart deploy carries the same information in a
Helm values file, so the doctor learns to read one: values → (env, profiles) →
the SAME ``check_deploy`` rules. No forked checks — a rule added for compose
applies to the chart for free, which is the whole point of translating instead
of reimplementing.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from cairn.core.deploy_check import (
    check_deploy,
    check_values,
    env_from_values,
    exit_code,
    parse_values_file,
    profiles_from_values,
)

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "cairn"

# argon2 hash of "s3cret" — the doctor verifies the hash PARSES (#1853).
_HASH = "$argon2id$v=19$m=65536,t=3,p=4$hdL7wUwAlItMM+v3pVfHxA$R1VstcN4/3bgfIjAOEmkCALYtjAqE4bjysDdB7zSit8"


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "values.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def _fails(results):
    return {r.name for r in results if r.status == "fail"}


def _full(values: dict, strict: bool = False):
    env, profiles = env_from_values(values), profiles_from_values(values)
    return check_deploy(env, profiles) + check_values(values, strict=strict)


# ── loading ──────────────────────────────────────────────────────────────


def test_parse_values_file_reads_yaml(tmp_path):
    p = _write(tmp_path, """
        image:
          tag: v1.2.3
        runtime: dbos
    """)
    assert parse_values_file(p)["image"]["tag"] == "v1.2.3"


def test_parse_missing_values_file_is_empty(tmp_path):
    assert parse_values_file(tmp_path / "nope.yaml") == {}


# ── values → the env the containers actually get ─────────────────────────


def test_ingress_host_becomes_the_public_domain():
    """The chart's Ingress host plays Caddy's CADDY_DOMAIN role."""
    values = {"ingress": {"host": "cairn.example.com"}}
    assert env_from_values(values)["CADDY_DOMAIN"] == "cairn.example.com"


def test_bundled_postgres_password_and_dsn_are_derived():
    values = {"postgres": {"enabled": True, "password": "s3cret"}}
    env = env_from_values(values)
    assert env["POSTGRES_PASSWORD"] == "s3cret"
    assert env["CAIRN_STORAGE"].startswith("postgresql://cairn:s3cret@")


def test_external_dsn_supplies_the_postgres_password():
    values = {"postgres": {"enabled": False,
                           "external": {"dsn": "postgresql://u:pw@db.example.com:5432/cairn"}}}
    env = env_from_values(values)
    assert env["CAIRN_STORAGE"] == "postgresql://u:pw@db.example.com:5432/cairn"
    assert env["POSTGRES_PASSWORD"] == "pw"


def test_runtime_and_checkpointer_carry_through():
    values = {"runtime": "temporal", "profiles": {"temporal": {"enabled": True}},
              "postgres": {"enabled": True, "password": "p"}}
    env = env_from_values(values)
    assert env["CAIRN_RUNTIME"] == "temporal"
    assert env["CAIRN_CHECKPOINTER"] == "postgres"
    assert env["CAIRN_CHECKPOINTER_DSN"]


def test_profiles_from_values_lists_the_enabled_ones():
    values = {"profiles": {"temporal": {"enabled": True}, "nats": {"enabled": False},
                           "backup": {"enabled": True}}}
    assert profiles_from_values(values) == {"temporal", "backup"}


# ── the same rules compose gets ──────────────────────────────────────────


def _good() -> dict:
    return {
        "image": {"repository": "ghcr.io/example/cairn-engine", "tag": "v0.1.0"},
        "runtime": "dbos",
        "ingress": {"host": "cairn.example.com",
                    "certManager": {"clusterIssuer": "letsencrypt-prod"}},
        "postgres": {"enabled": True, "password": "s3cret"},
        "secrets": {"obsJwtSecret": "jwt", "obsApiPasswordHash": _HASH},
        "env": {"profile": "/app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml"},  # #2296
    }


def test_a_complete_values_file_passes():
    results = _full(_good())
    assert _fails(results) == set(), [(r.name, r.detail) for r in results if r.status == "fail"]
    assert exit_code(results) == 0


def test_missing_jwt_secret_fails():
    values = _good()
    values["secrets"].pop("obsJwtSecret")
    assert "CAIRN_JWT_SECRET" in _fails(_full(values))
    assert exit_code(_full(values)) == 1


def test_missing_password_hash_fails():
    values = _good()
    values["secrets"].pop("obsApiPasswordHash")
    assert "CAIRN_API_PASSWORD_HASH" in _fails(_full(values))


def test_missing_ingress_host_fails_like_a_missing_caddy_domain():
    values = _good()
    values.pop("ingress")
    assert "CADDY_DOMAIN" in _fails(_full(values))


def test_temporal_runtime_without_the_temporal_profile_fails():
    """The canonical compose rule, reached through values."""
    values = dict(_good(), runtime="temporal")
    results = _full(values)
    assert "runtime" in _fails(results)


def test_temporal_runtime_with_the_profile_passes():
    values = dict(_good(), runtime="temporal", profiles={"temporal": {"enabled": True}})
    assert _fails(_full(values)) == set()


def test_dbos_runtime_with_a_memory_checkpointer_fails():
    values = dict(_good(), checkpointer="memory")
    assert "checkpointer" in _fails(_full(values))


# ── values-only rules ────────────────────────────────────────────────────


def test_existing_secret_is_accepted_but_reported_as_unverifiable():
    values = _good()
    values["secrets"] = {"existingSecret": "cairn-prod"}
    results = _full(values)
    assert _fails(results) == set()
    warn = next(r for r in results if r.name == "existing_secret")
    assert warn.status == "warn" and "cairn-prod" in warn.detail


def test_latest_tag_warns_and_fails_under_strict():
    values = dict(_good(), image={"repository": "ghcr.io/example/cairn-engine", "tag": "latest"})
    results = _full(values)
    assert _fails(results) == set()
    assert next(r for r in results if r.name == "image_tag").status == "warn"
    assert exit_code(results, strict=True) == 1


def test_pinned_tag_is_ok_under_strict():
    results = _full(_good())
    assert next(r for r in results if r.name == "image_tag").status == "ok"
    assert exit_code(results, strict=True) == 0


def test_bundled_postgres_and_external_dsn_together_fails():
    values = _good()
    values["postgres"]["external"] = {"dsn": "postgresql://u:p@h/d"}
    assert "postgres" in _fails(_full(values))


def test_neither_bundled_postgres_nor_external_dsn_fails():
    values = _good()
    values["postgres"] = {"enabled": False}
    assert "postgres" in _fails(_full(values))


def test_pull_worker_enabled_without_a_command_fails():
    values = dict(_good(), pullWorker={"enabled": True})
    assert "pull_worker" in _fails(_full(values))


# ── the shipped example files must pass their own doctor ─────────────────


@pytest.mark.parametrize("name", [
    "values-postgres.example.yaml",
    "values-external-dsn.example.yaml",
    "values-temporal.example.yaml",
])
def test_shipped_example_values_pass(name):
    values = parse_values_file(CHART / name)
    assert values, f"{name} did not load"
    results = _full(values)
    assert _fails(results) == set(), [(r.name, r.detail) for r in results if r.status == "fail"]


def test_examples_are_valid_yaml_mappings():
    for p in CHART.glob("values-*.example.yaml"):
        assert isinstance(yaml.safe_load(p.read_text()), dict), p
