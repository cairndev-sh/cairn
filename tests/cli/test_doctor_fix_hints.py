"""Every failing or actionable row ``cairn doctor`` builds names what fixes it.

``cairn setup`` prints only the lines that start with ✗, so the hint rides at the end of the
row's ``detail``, on the same line: ``... — fix: <what to do>``. ``CheckResult`` (a public
contract) gains no field.

Each scenario below drives the real check function into the failing state and asserts the
row's status and that its hint names the right command or setting. Three guards sit beside
them: a hint never prints a value from the environment, a ``cairn ...`` command or
``cairndev[extra]`` a hint names exists, and the rows that read ``unset (optional)`` (about a
hundred, none of them a problem) carry no hint.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import inspect
import os
import re
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest
import typer

from cairn.cli.commands import doctor as doctor_mod
from cairn.core.pack import CheckResult
from cairn.core.pack.drift import DriftFinding

ROOT = Path(__file__).resolve().parents[2]
_ENV = ("CAIRN_STORAGE", "OBS_STORAGE", "CAIRN_RUN_DB", "OBS_RUN_DB", "CAIRN_PROFILE", "OBS_PROFILE",
        "CAIRN_RUNTIME", "OBS_RUNTIME", "CAIRN_TEMPORAL_ADDRESS", "CAIRN_ARTIFACT_STORE",
        "CAIRN_MCP_SERVERS", "SLACK_WEBHOOK_URL", "MLFLOW_TRACKING_URI", "CAIRN_WORKSPACE")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ENV:
        monkeypatch.delenv(var, raising=False)


def _hint(row: CheckResult) -> str:
    """What follows ``fix:`` in the row, or ``""``."""
    return row.detail.split("fix:", 1)[1].strip() if "fix:" in row.detail else ""


def _one(rows: list[CheckResult], name: str | None = None) -> CheckResult:
    picked = [r for r in rows if name is None or r.name == name or r.name.startswith(name)]
    assert len(picked) == 1, [(r.name, r.status) for r in rows]
    return picked[0]


# ── scenarios: (id, how to reach the failing state, row name prefix, status, words the hint names) ──
# Each ``arrange`` returns the rows the real check produced.

Arrange = Callable[[pytest.MonkeyPatch, Path], list[CheckResult]]
SCENARIOS: list[tuple[str, Arrange, str, str, tuple[str, ...]]] = []


def scenario(sid: str, row: str, status: str, *names: str):
    def deco(fn: Arrange) -> Arrange:
        SCENARIOS.append((sid, fn, row, status, names))
        return fn
    return deco


def _packs(monkeypatch, *packs):
    """The packs the registry reports, both ways the checks reach it."""
    import cairn.core.pack as pack_pkg
    import cairn.core.pack.registry as registry

    monkeypatch.setattr(pack_pkg, "get_packs", lambda: list(packs))
    monkeypatch.setattr(registry, "get_packs", lambda: list(packs))


def _pack(name="acme", **manifest) -> SimpleNamespace:
    base = dict(kind="", trust_level="", publisher="", cairn_engine=None, required_capabilities=[],
                required_memory=None, required_rag=None, required_artifacts=None)
    return SimpleNamespace(name=name, manifest=SimpleNamespace(**{**base, **manifest}))


def _profile(tmp_path: Path, body: str = "") -> str:
    path = tmp_path / "profile.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[profile]\n" + body)
    return str(path)


# settings ------------------------------------------------------------------------------------

@scenario("settings-required-unset", "ZZ_REQUIRED", "fail", "ZZ_REQUIRED")
def _(monkeypatch, tmp_path):
    from cairn.core.settings import SettingSpec

    monkeypatch.setattr(doctor_mod._settings, "get_inventory", lambda: [
        SettingSpec("zz", "ZZ_REQUIRED", None, "dispatcher", "a knob a pack needs", required=True)])
    return doctor_mod.check_settings()


@scenario("settings-legacy-name", "CAIRN_RUN_DB", "warn", "OBS_RUN_DB", "CAIRN_RUN_DB")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "old.db"))
    return doctor_mod.check_settings()


# the run store -------------------------------------------------------------------------------

@scenario("run-db-unknown-scheme", "run_db", "fail", "CAIRN_STORAGE", "sqlite:///path.db")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_STORAGE", "mysql2://user:hunter2@db.invalid/cairn")
    return [doctor_mod.check_run_db()]


@scenario("run-db-in-memory", "run_db", "warn", "CAIRN_STORAGE", "CAIRN_RUN_DB")
def _(monkeypatch, tmp_path):
    return [doctor_mod.check_run_db()]


@scenario("run-db-cannot-create-parent", "run_db", "fail", "CAIRN_RUN_DB")
def _(monkeypatch, tmp_path):
    (tmp_path / "afile").write_text("")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "afile" / "sub" / "runs.db"))
    return [doctor_mod.check_run_db()]


@scenario("run-db-parent-not-writable", "run_db", "fail", "CAIRN_STORAGE")
def _(monkeypatch, tmp_path):
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root can write anywhere")
    ro = tmp_path / "ro"
    ro.mkdir()
    ro.chmod(0o500)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite://{ro / 'runs.db'}")
    try:
        return [doctor_mod.check_run_db()]
    finally:
        ro.chmod(0o700)


@scenario("engine-store-not-reported", "engine_store", "warn", "CAIRN_STORAGE")
def _(monkeypatch, tmp_path):
    import cairn.cli.commands.remote as remote

    monkeypatch.setattr(remote, "store_check", lambda: ("sqlite mine", "http://engine.invalid", None))
    return [doctor_mod.check_engine_store()]


@scenario("engine-store-differs", "engine_store", "warn", "CAIRN_STORAGE")
def _(monkeypatch, tmp_path):
    import cairn.cli.commands.remote as remote

    monkeypatch.setattr(remote, "store_check", lambda: ("sqlite mine", "http://engine.invalid", False))
    return [doctor_mod.check_engine_store()]


# runtime and reachability ----------------------------------------------------------------------

@scenario("runtime-temporal-sdk-missing", "runtime", "fail", "cairndev[runtime-temporal]")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    monkeypatch.setitem(sys.modules, "temporalio", None)  # an import of it now raises ImportError
    return [asyncio.run(doctor_mod.check_runtime())]


@scenario("runtime-temporal-unreachable", "runtime", "warn", "CAIRN_TEMPORAL_ADDRESS")
def _(monkeypatch, tmp_path):
    pytest.importorskip("temporalio.contrib.langgraph")
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    monkeypatch.setenv("CAIRN_TEMPORAL_ADDRESS", "127.0.0.1:1")
    return [asyncio.run(doctor_mod.check_runtime())]


def _unreachable(monkeypatch, why="ConnectError"):
    async def probe(url):
        return False, why
    monkeypatch.setattr(doctor_mod, "_probe_url", probe)


@scenario("slack-unreachable", "slack_webhook", "warn", "SLACK_WEBHOOK_URL")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.invalid/services/T000/B000/xoxb-token")
    _unreachable(monkeypatch)
    return [asyncio.run(doctor_mod.check_slack_webhook())]


@scenario("slack-httpx-missing", "slack_webhook", "warn", "pip install httpx")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.invalid/services/T000/B000/xoxb-token")
    _unreachable(monkeypatch, "httpx not installed")
    return [asyncio.run(doctor_mod.check_slack_webhook())]


@scenario("metaflow-service-unreachable", "metaflow_service", "fail", "CAIRN_METAFLOW_SERVICE_URL",
          "metaflow_metadata_service")
def _(monkeypatch, tmp_path):
    import cairn.core.backend_selection as bs

    monkeypatch.setattr(bs, "selected", lambda key, default="": "http://mf.invalid:8080")
    _unreachable(monkeypatch)
    return [asyncio.run(doctor_mod.check_metaflow_service())]


@scenario("engine-client-missing", "engine_client:dataflow_engine", "fail", "cairndev[metaflow]")
def _(monkeypatch, tmp_path):
    import cairn.core.backend_selection as bs

    monkeypatch.setattr(bs, "selected", lambda key, default="": "metaflow" if key == "dataflow_engine" else "")
    real = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: None if name == "metaflow" else real(name, *a))
    return doctor_mod.check_engine_clients()


@scenario("stage-kernel-does-not-import", "stage_kernel[broken]", "warn", "dependencies")
def _(monkeypatch, tmp_path):
    import cairn.core.dataflow.defs as defs

    class Entry:
        def load(self):
            raise ImportError("No module named 'numpyish'")

    monkeypatch.setattr(defs, "_stage_entry_points", lambda: {"broken": Entry()})
    return doctor_mod.check_stage_kernels()


# extension points ----------------------------------------------------------------------------

_FAILURE = {"name": "bad", "target": "bad_pkg:provider", "error": "ImportError: No module named 'x'"}


@scenario("seed-provider-does-not-load", "seed_provider[bad]", "warn", "uninstall")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.seeds as seeds

    monkeypatch.setattr(seeds, "seed_load_failures", lambda: [_FAILURE])
    return doctor_mod.check_seed_providers()


@scenario("seed-provider-none", "seed_providers", "warn", "cairndev")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.seeds as seeds

    monkeypatch.setattr(seeds, "load_seed_providers", lambda: {})
    monkeypatch.setattr(seeds, "seed_load_failures", lambda: [])
    return doctor_mod.check_seed_providers()


@scenario("model-catalog-does-not-load", "model_catalog[bad]", "warn", "uninstall")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.model_catalogs as mc

    monkeypatch.setattr(mc, "catalog_load_failures", lambda: [_FAILURE])
    return doctor_mod.check_model_catalog_providers()


@scenario("model-catalog-none", "model_catalogs", "warn", "cairn.model_catalogs")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.model_catalogs as mc

    monkeypatch.setattr(mc, "load_catalog_providers", lambda: {})
    monkeypatch.setattr(mc, "catalog_load_failures", lambda: [])
    return doctor_mod.check_model_catalog_providers()


@scenario("run-hook-does-not-load", "run_hook[bad]", "warn", "uninstall")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.run_hooks as hooks

    monkeypatch.setattr(hooks, "run_hook_load_failures", lambda: [_FAILURE])
    return doctor_mod.check_run_hooks()


@scenario("router-provider-does-not-load", "router_provider[bad]", "warn", "uninstall")
def _(monkeypatch, tmp_path):
    import cairn.core.triggers.router_providers as rp

    monkeypatch.setattr(rp, "discover_router_providers", lambda: [
        rp.RouterProvider("bad", "bad_pkg:routers", fn=None, error="ImportError: x")])
    return doctor_mod.check_router_providers()


@scenario("cli-provider-does-not-load", "cli_provider[bad]", "warn", "uninstall")
def _(monkeypatch, tmp_path):
    import cairn.cli.providers as providers

    monkeypatch.setattr(providers, "load_cli_providers", lambda: [
        providers.CliProvider("bad", "bad_pkg:cli", error="ImportError: x")])
    monkeypatch.setattr(providers, "cli_mount_failures", lambda: [])
    return doctor_mod.check_cli_providers()


@scenario("cli-provider-command-refused", "cli_provider[run]", "warn", "rename")
def _(monkeypatch, tmp_path):
    import cairn.cli.providers as providers

    monkeypatch.setattr(providers, "load_cli_providers", lambda: [])
    monkeypatch.setattr(providers, "cli_mount_failures", lambda: [
        {"name": "run", "target": "bad_pkg:cli", "error": "the name is taken by a public command"}])
    return doctor_mod.check_cli_providers()


# packs and composition -----------------------------------------------------------------------

@scenario("no-packs", "packs_loaded", "warn", "cairn pack add --dev", "CAIRN_PACK_PATH")
def _(monkeypatch, tmp_path):
    _packs(monkeypatch)
    return doctor_mod.check_composition()


@pytest.mark.parametrize("declared, installed, words", [
    (">=99", "0.3.0", ("cairndev",)),
    ("not a range", "0.3.0", ("PEP 440",)),
    (">=0.1", None, ("pip install cairndev",)),
], ids=["outside-range", "invalid-range", "engine-not-installed"])
def test_pack_compat_hint(monkeypatch, declared, installed, words):
    import cairn.core.pack.loader as loader

    monkeypatch.setattr(loader, "installed_engine_version", lambda: installed)
    _packs(monkeypatch, _pack(cairn_engine=declared))
    row = _one(doctor_mod.check_composition(), "pack_compat:acme")
    assert row.status == "warn"
    assert all(w in _hint(row) for w in words), row.detail


def _composition(monkeypatch, *, conflicts=(), warnings=()):
    import cairn.core.pack as pack_pkg

    _packs(monkeypatch, _pack())
    monkeypatch.setattr(pack_pkg, "get_composition_report", lambda: SimpleNamespace(
        provider_map={}, conflicts=list(conflicts), warnings=list(warnings)))
    return doctor_mod.check_composition()


@pytest.mark.parametrize("reason, words", [
    ("profile picks 'x' for capability 'c' but no pack named 'x' is installed", ("[profile.capabilities]",)),
    ("profile picks 'x' for capability 'c' but that pack doesn't provide it; actual providers: none",
     ("[profile.capabilities]",)),
    ("capability 'c' provided by multiple packs ['a', 'b'] — pick one in [profile.capabilities]",
     ("[profile.capabilities]", "CAIRN_PROFILE")),
    ("pack 'a' requires capability 'c' but no installed pack provides it", ("install a pack",)),
    ("pack 'a' [requires.mcp].tools entry 'x' must be '<server>:<tool>'", ("<server>:<tool>",)),
], ids=["pick-not-installed", "pick-not-provider", "duplicate-providers", "unmet-requirement", "tool-shape"])
def test_composition_conflict_hint(monkeypatch, reason, words):
    row = _one(_composition(monkeypatch, conflicts=[reason]), "composition_conflict")
    assert row.status == "fail"
    assert all(w in _hint(row) for w in words), row.detail


def test_composition_conflict_that_names_its_own_fix_keeps_it(monkeypatch):
    reason = "pack 'a' requires 'x' but no binding; fix: add it under [profile.bindings]"
    row = _one(_composition(monkeypatch, conflicts=[reason]), "composition_conflict")
    assert row.detail == reason


def test_composition_warning_hint(monkeypatch):
    reason = "pack 'a' optional capability 'c' unprovided; richer behavior will not activate"
    row = _one(_composition(monkeypatch, warnings=[reason]), "composition_warning")
    assert row.status == "warn" and "install a pack" in _hint(row)


def test_composition_phrases_are_the_ones_the_loader_writes():
    """Each phrase a hint is keyed on must still occur in the module that writes the reason."""
    import cairn.core.pack.composition as composition

    source = inspect.getsource(composition).replace("\n", " ")
    source = re.sub(r"\"\s+f?\"", "", source)  # adjacent string literals read as one
    for phrase, _ in doctor_mod._COMPOSITION_FIXES:
        assert phrase in source, phrase


def _operators(monkeypatch, *, metadata, picks):
    from cairn.core.operator import executor
    from cairn.core.operator.registry import default_registry

    _packs(monkeypatch, _pack())
    monkeypatch.setattr(default_registry, "list_names", lambda: ["ghost/op"])
    monkeypatch.setattr(default_registry, "get_metadata", lambda name: metadata)
    monkeypatch.setattr(default_registry, "get", lambda name: object)
    monkeypatch.setattr(executor, "pick_executor", picks)
    return _one(doctor_mod.check_composition(), "operator:ghost/op")


def test_operator_without_manifest_metadata_hint(monkeypatch):
    row = _operators(monkeypatch, metadata=None, picks=lambda cls: "in_process")
    assert row.status == "warn" and "cairn pack sync" in _hint(row)


def test_operator_with_no_implemented_executor_hint(monkeypatch):
    from cairn.core.operator.executor import UnsupportedExecutorError

    md = SimpleNamespace(risk="low", executors=["k8s_job"], requires_approval=False,
                         supports_dry_run=False, supports_verify=False, supports_compensate=False)

    def picks(cls):
        raise UnsupportedExecutorError("none implemented")

    row = _operators(monkeypatch, metadata=md, picks=picks)
    assert row.status == "warn" and "UNSUPPORTED" in row.detail
    assert "executors" in _hint(row) and "cairn pack sync" in _hint(row)


@pytest.mark.parametrize("kind", ["declared_not_registered", "registered_not_declared", "version_mismatch",
                                  "capability_mismatch", "schema_mismatch", "governance_mismatch"])
def test_manifest_drift_hint(monkeypatch, kind):
    monkeypatch.setattr(doctor_mod, "check_installed_operator_drift",
                        lambda: [DriftFinding(kind, "acme/op", "acme/op disagrees with its manifest")])
    row = _one(doctor_mod.check_manifest_drift())
    assert row.status == "fail"
    assert "cairn pack sync <pack-dir>" in _hint(row)
    assert ("pip install -e" in _hint(row)) == (kind == "declared_not_registered")


def test_manifest_drift_pack_not_installed_hint_keeps_its_warn(monkeypatch):
    monkeypatch.setattr(doctor_mod, "check_installed_operator_drift",
                        lambda: [DriftFinding("pack_not_installed", "acme/op", "declared by a pack not installed")])
    row = _one(doctor_mod.check_manifest_drift())
    assert row.status == "warn" and "pip install -e" in _hint(row)


@scenario("domain-pack-without-pipeline", "pipeline_mandate[acme]", "fail", "pipelines/<name>.yaml")
def _(monkeypatch, tmp_path):
    monkeypatch.setattr(doctor_mod, "_domain_packs_without_pipelines", lambda: ["acme"])
    return doctor_mod.check_pipeline_mandate()


@scenario("legacy-flow-layout", "pack_layout[acme]", "warn", "flows/")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.loader as loader

    _packs(monkeypatch, _pack())
    monkeypatch.setattr(loader, "legacy_layout_dirs", lambda pack: ["templates_dir"])
    return doctor_mod.check_pack_layout()


@scenario("flow-stem-collision", "flow_stem_collision[acme]", "warn", "rename")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.loader as loader

    _packs(monkeypatch, _pack())
    monkeypatch.setattr(loader, "flow_ref_collisions", lambda pack: ["acme/x: two flows at one ref"])
    return doctor_mod.check_flow_stem_collisions()


# profile -----------------------------------------------------------------------------------

@scenario("profile-does-not-load", "profile", "fail", "CAIRN_PROFILE", ".toml")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_PROFILE", str(tmp_path / "missing.toml"))
    return doctor_mod.check_profile()


@scenario("binding-to-unregistered-operator", "binding:data.sink", "fail", "[profile.bindings]", "ghost/op")
def _(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_PROFILE", _profile(tmp_path, '[profile.bindings]\n"data.sink" = "ghost/op"\n'))
    return doctor_mod.check_profile()


@pytest.mark.parametrize("capability, binding", [("audit.sink", "audit/store"), ("redact.text", "redact/text")])
def test_unbound_governance_capability_hint(monkeypatch, tmp_path, capability, binding):
    monkeypatch.setenv("CAIRN_PROFILE", _profile(tmp_path))
    row = _one(doctor_mod.check_profile(), f"binding:{capability}")
    assert row.status == "warn"
    assert f'"{capability}" = "{binding}"' in _hint(row) and "[profile.bindings]" in _hint(row)


def test_governance_binding_hints_name_what_the_secure_profile_binds():
    secure = tomllib.loads((ROOT / "src/cairn/profiles/secure.toml").read_text())["profile"]["bindings"]
    assert {k: secure[k] for k in doctor_mod._GOVERNANCE_BINDING} == doctor_mod._GOVERNANCE_BINDING


@scenario("pack-requires-unbound-capability", "requires:acme:audit.sink", "fail", "[profile.bindings]")
def _(monkeypatch, tmp_path):
    _packs(monkeypatch, _pack(required_capabilities=["audit.sink"]))
    return doctor_mod.check_required_services()


@pytest.mark.parametrize("kind, words", [
    ("memory", ("memory = true", "[profile.backends]")),
    ("rag", ("rag_vector_store", "RAG_VECTOR_STORE")),
])
def test_unprovisioned_backend_hint(monkeypatch, kind, words):
    import cairn.core.profile_backends as pb

    _packs(monkeypatch, _pack(**{f"required_{kind}": {"store": {}}}))
    monkeypatch.setattr(pb, "effective_backends", lambda declared: {})
    row = _one(doctor_mod.check_provisioned_backends(), f"provisioned:acme:{kind}")
    assert row.status == "fail"
    assert all(w in _hint(row) for w in words), row.detail


def _profile_config(monkeypatch, tmp_path, op_name, *, has, schema):
    from cairn.core.operator.registry import default_registry

    monkeypatch.setenv("CAIRN_PROFILE", _profile(tmp_path, f'[profile.config."{op_name}"]\nknob = 1\n'))
    monkeypatch.setattr(default_registry, "has", lambda name: has)
    monkeypatch.setattr(default_registry, "get", lambda name: SimpleNamespace(config_schema=schema))
    return _one(doctor_mod.check_operator_config(), f"config:{op_name}")


def test_config_for_an_operator_that_is_not_installed_hint(monkeypatch, tmp_path):
    row = _profile_config(monkeypatch, tmp_path, "ghost/op", has=False, schema=None)
    assert row.status == "fail" and '[profile.config."ghost/op"]' in _hint(row)


def test_config_for_an_operator_with_no_knobs_hint(monkeypatch, tmp_path):
    row = _profile_config(monkeypatch, tmp_path, "plain/op", has=True, schema=None)
    assert row.status == "fail" and 'remove [profile.config."plain/op"]' in _hint(row)


def test_invalid_operator_config_hint(monkeypatch, tmp_path):
    import pydantic

    class Schema(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(extra="forbid")
        limit: int = 1

    row = _profile_config(monkeypatch, tmp_path, "tuned/op", has=True, schema=Schema)
    assert row.status == "fail" and "accepted fields" in _hint(row)


# production readiness ---------------------------------------------------------------------

def _production(monkeypatch, tmp_path, body, missing):
    monkeypatch.setenv("CAIRN_PROFILE", _profile(tmp_path, body))
    real = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: None if name in missing else real(name, *a))
    return doctor_mod.check_production_readiness()


def test_presidio_missing_hint(monkeypatch, tmp_path):
    rows = _production(monkeypatch, tmp_path, '[profile.config."dataset/pii_scan"]\nbackend = "presidio"\n',
                       {"presidio_analyzer"})
    row = _one(rows, "pii-backend")
    assert row.status == "fail" and "pip install 'cairndev[pii]'" in _hint(row)


def test_lm_eval_missing_hint(monkeypatch, tmp_path):
    rows = _production(monkeypatch, tmp_path,
                       '[profile.config."model/gate".thresholds]\nrequire_benchmark = true\n', {"lm_eval"})
    row = _one(rows, "benchmark-harness")
    assert row.status == "fail" and "cairn-operator-eval-lm[harness]" in _hint(row)


def test_temporalio_missing_under_a_profile_hint(monkeypatch, tmp_path):
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    row = _one(_production(monkeypatch, tmp_path, "", {"temporalio"}), "runtime-temporal")
    assert row.status == "fail" and "cairndev[runtime-temporal]" in _hint(row)


def test_mlflow_missing_hint(monkeypatch, tmp_path):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow.invalid")
    row = _one(_production(monkeypatch, tmp_path, "", {"mlflow"}), "registry-mlflow")
    assert row.status == "warn" and "MLFLOW_TRACKING_URI" in _hint(row) and "mlflow.invalid" not in _hint(row)


# model roles ----------------------------------------------------------------------------------

def _roles(monkeypatch, provider):
    import cairn.core.llm.models as models

    monkeypatch.setattr(models, "connected_llm_providers",
                        lambda: {"openai": False, "anthropic": False, "gemini": False, "gateway": False})
    monkeypatch.setattr(models, "list_roles", lambda: [
        SimpleNamespace(role="fast", provider=provider, name="m", source="models.yaml")])
    return _one(doctor_mod.check_model_roles(), "model_role:fast")


@pytest.mark.parametrize("provider, setting", [
    ("openai", "OPENAI_API_KEY"), ("anthropic", "ANTHROPIC_API_KEY"), ("gemini", "GEMINI_API_KEY"),
    ("gateway", "LLM_GATEWAY_URL"),
])
def test_role_bound_to_a_keyless_provider_names_the_key(monkeypatch, provider, setting):
    row = _roles(monkeypatch, provider)
    assert row.status == "warn" and setting in _hint(row)


def test_role_bound_to_a_provider_cairn_cannot_call_says_which_it_can(monkeypatch):
    row = _roles(monkeypatch, "ollama")
    assert row.status == "warn"
    assert all(p in _hint(row) for p in ("openai", "anthropic", "gemini", "gateway"))


def test_provider_hints_cover_the_providers_the_check_knows():
    import cairn.core.llm.models as models

    assert set(doctor_mod._PROVIDER_FIX) == set(models.connected_llm_providers())


# the artifact registry -------------------------------------------------------------------------

def _artifacts(monkeypatch, tmp_path, health=None, boom=None):
    import cairn.core.storage.artifact_bytes as ab

    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite://{tmp_path / 'cairn.db'}")
    if boom is not None or health is not None:
        async def registry_health(store, workspace_id):
            if boom is not None:
                raise boom
            return health
        monkeypatch.setattr(ab, "registry_health", registry_health)
    return asyncio.run(doctor_mod.check_artifact_bytes())


def test_no_durable_artifact_store_hint(monkeypatch, tmp_path):
    row = _one(_artifacts(monkeypatch, tmp_path), "artifact_bytes")
    assert row.status == "warn" and "CAIRN_ARTIFACT_STORE to s3://<bucket>" in _hint(row)


def test_no_artifact_registry_hint():
    row = _one(asyncio.run(doctor_mod.check_artifact_bytes()), "artifact_bytes")
    assert row.status == "warn" and "CAIRN_STORAGE" in _hint(row)


def test_unreadable_artifact_registry_hint(monkeypatch, tmp_path):
    row = _one(_artifacts(monkeypatch, tmp_path, boom=RuntimeError("down")), "artifact_registry_bytes")
    assert row.status == "warn" and "CAIRN_STORAGE" in _hint(row)


def test_dangling_artifacts_hint(monkeypatch, tmp_path):
    health = {"total": 3, "dangling": 1, "durable": 1, "local": 1,
              "dangling_sample": [{"id": "a" * 32, "uri": "/gone/box/out.bin"}]}
    row = _one(_artifacts(monkeypatch, tmp_path, health=health), "artifact_registry_bytes")
    assert row.status == "warn" and "CAIRN_ARTIFACT_STORE" in _hint(row)


# MCP and compute targets ------------------------------------------------------------------------

@scenario("mcp-server-unreachable", "mcp:ghost", "warn", "CAIRN_MCP_SERVERS", "CAIRN_MCP_GHOST_URL")
def _(monkeypatch, tmp_path):
    import cairn.services.mcp.client as client

    class Down:
        async def list_tools(self, name):
            raise ConnectionError("refused")

    monkeypatch.setenv("CAIRN_MCP_SERVERS", '{"ghost": {"transport": "streamable_http", "url": "http://x.invalid"}}')
    monkeypatch.setattr(client, "get_mcp_client", lambda: Down())
    return asyncio.run(doctor_mod.check_mcp_servers())


@scenario("mcp-server-declared-in-profile-only", "mcp:slackish", "warn", "CAIRN_MCP_SERVERS", "'slackish'")
def _(monkeypatch, tmp_path):
    import cairn.core.pack.profile as profile

    monkeypatch.setattr(profile, "load_profile_from_env",
                        lambda: SimpleNamespace(mcp_servers={"slackish": {"url": "http://x.invalid"}}))
    return asyncio.run(doctor_mod.check_mcp_servers())


@scenario("compute-target-unresolvable", "compute_target:ml_target", "warn", "CAIRN_STORAGE",
          "cairn admin connect ls")
def _(monkeypatch, tmp_path):
    import cairn.core.backend_selection as bs
    import cairn.core.connections as connections

    async def resolve(target, account_id):
        raise RuntimeError("store down")

    monkeypatch.setattr(bs, "selected", lambda key, default="": "gpu-box" if key == "ml_target" else default)
    monkeypatch.setattr(connections, "resolve_connection", resolve)
    return asyncio.run(doctor_mod.check_compute_targets())


# ── the scenarios, run ────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("arrange, row, status, names", [s[1:] for s in SCENARIOS], ids=[s[0] for s in SCENARIOS])
def test_row_names_its_fix(monkeypatch, tmp_path, arrange, row, status, names):
    before = dict(os.environ)
    rows = [r for r in arrange(monkeypatch, tmp_path) if r is not None]
    set_here = [v for k, v in os.environ.items() if before.get(k) != v]
    found = _one(rows, row)
    assert found.status == status, found
    hint = _hint(found)
    assert hint, f"no `fix:` on {found.name}: {found.detail}"
    assert "\n" not in found.detail, "the hint must stay on the row's one line"
    for word in names:
        assert word in hint, (word, found.detail)
    assert not re.search(r"ADR-\d+|#\d+", hint), f"a hint carries no ADR or issue number: {hint}"
    leaked = [v for v in set_here if len(v) >= 12 and v in hint]
    assert not leaked, "a hint never holds a value from the environment"


# ── guards ────────────────────────────────────────────────────────────────────────────────────

def test_no_hint_prints_a_secret_set_in_the_environment(monkeypatch, tmp_path):
    secrets = {
        "CAIRN_STORAGE": "mysql2://alice:SECRET-pw-1@db.invalid/cairn",
        "SLACK_WEBHOOK_URL": "https://hooks.invalid/services/T000/B000/SECRET-token-2",
        "CAIRN_PROFILE": _profile(tmp_path / "SECRET-profile-3", '[profile.bindings]\n"data.sink" = "ghost/op"\n'),
        "CAIRN_TEMPORAL_ADDRESS": "SECRET-host-4:1",
        "CAIRN_RUNTIME": "temporal",
        "CAIRN_MCP_SERVERS": '{"ghost": {"transport": "streamable_http", "url": "http://SECRET-mcp-5.invalid"}}',
        "MLFLOW_TRACKING_URI": "http://SECRET-mlflow-6.invalid",
    }
    for key, value in secrets.items():
        monkeypatch.setenv(key, value)
    _unreachable(monkeypatch)
    monkeypatch.setitem(sys.modules, "temporalio", None)
    rows = (doctor_mod.check_settings() + [doctor_mod.check_run_db()] + doctor_mod.check_profile()
            + doctor_mod.check_production_readiness())
    rows += [asyncio.run(doctor_mod.check_runtime()), asyncio.run(doctor_mod.check_slack_webhook())]
    hinted = [r for r in rows if r is not None and "fix:" in r.detail]
    assert {r.name for r in hinted} >= {"run_db", "binding:data.sink", "runtime-temporal", "runtime", "slack_webhook",
                                       "registry-mlflow"}
    for r in hinted:
        assert "SECRET" not in _hint(r), (r.name, r.detail)
    # the rows that name a masked secret stay masked, hinted or not
    for r in rows:
        if r is not None and r.name in {"run_db", "slack_webhook"}:
            assert "SECRET" not in r.detail, (r.name, r.detail)


def test_a_required_setting_names_the_setting_never_a_value(monkeypatch):
    from cairn.core.settings import SettingSpec

    monkeypatch.setattr(doctor_mod._settings, "get_inventory", lambda: [
        SettingSpec("zz", "ZZ_REQUIRED", "a-default-value", "dispatcher", "needed", required=True)])
    row = _one(doctor_mod.check_settings())
    assert row.status == "fail" and "set ZZ_REQUIRED" in _hint(row) and "a-default-value" not in row.detail


def test_unset_optional_rows_carry_no_hint():
    optional = [r for r in doctor_mod.check_settings() if r.detail == "unset (optional)"]
    assert len(optional) > 20, "the inventory should still hold many optional knobs"
    assert not [r for r in optional if "fix:" in r.detail]


def test_the_detail_still_reads_as_one_line_per_row():
    for r in doctor_mod.check_settings():
        assert "\n" not in r.detail


def test_checkresult_gains_no_field():
    import dataclasses

    assert [f.name for f in dataclasses.fields(CheckResult)] == ["name", "status", "detail", "category"]


# the `cairn ...` commands and `cairndev[...]` extras a hint names are real

def _strings_in_doctor() -> list[str]:
    """Every string literal doctor builds rows from (docstrings left out)."""
    tree = ast.parse(Path(doctor_mod.__file__).read_text())
    docstrings = {id(node.body[0].value) for node in ast.walk(tree)
                  if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                  and node.body and isinstance(node.body[0], ast.Expr)
                  and isinstance(getattr(node.body[0], "value", None), ast.Constant)}
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]


def _command_tree() -> dict[str, Any]:
    import cairn.cli.main as cli_main

    out: dict[str, Any] = {}

    def walk(cmd, path):
        out[path] = cmd
        for sub in getattr(cmd, "commands", {}).values():
            walk(sub, f"{path} {sub.name}")

    walk(typer.main.get_command(cli_main.app), "cairn")
    return out


def _named_commands() -> list[str]:
    """`cairn ...` spans in backticks, from the literals and from what the scenarios print."""
    text = "\n".join(_strings_in_doctor())
    return sorted(set(re.findall(r"`(cairn [^`]+)`", text)))


def test_the_walk_finds_the_commands_the_hints_name():
    named = _named_commands()
    for expected in ("cairn pack sync <pack-dir>", "cairn admin connect ls", "cairn pack add --dev <pack-dir>"):
        assert expected in named


@pytest.mark.parametrize("command", _named_commands())
def test_every_cairn_command_doctor_names_exists(command):
    tree = _command_tree()
    words = command.split()
    path = ["cairn"]
    flags = []
    for word in words[1:]:
        if word.startswith("-"):
            flags.append(word.split("=")[0])
        elif word.startswith(("<", "[")):
            continue
        elif not flags:
            here = tree[" ".join(path)]
            if hasattr(here, "commands"):  # a group: the next word must be one of its commands
                assert f"{' '.join(path)} {word}" in tree, f"`{command}`: {' '.join(path)} has no {word!r}"
                path.append(word)
    target = tree[" ".join(path)]
    assert len(path) > 1, f"`{command}` names no command"
    for flag in flags:
        known = {opt for param in target.params for opt in (*param.opts, *param.secondary_opts)}
        assert flag in known, f"`{command}`: {' '.join(path)} has no {flag}"


def test_every_cairndev_extra_doctor_names_exists():
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    named = set(re.findall(r"cairndev\[([a-z0-9-]+)\]", "\n".join(_strings_in_doctor())))
    assert {"runtime-temporal", "pii", "metaflow"} <= named
    assert named <= set(extras), named - set(extras)


def test_the_metaflow_row_never_prints_a_login_or_a_query_in_its_url(monkeypatch):
    async def down(url):
        return False, "ConnectError"

    url = "https://bob:s3cret@mf.invalid:8080/path?token=abc123"
    monkeypatch.setattr("cairn.core.backend_selection.selected", lambda key, default="": url)
    monkeypatch.setattr(doctor_mod, "_probe_url", down)
    detail = asyncio.run(doctor_mod.check_metaflow_service()).detail
    assert "s3cret" not in detail and "abc123" not in detail and "bob" not in detail
    assert "mf.invalid:8080" in detail


def test_a_config_error_names_the_field_and_never_quotes_its_value():
    from pydantic import BaseModel, ValidationError

    class _Config(BaseModel):
        retries: int

    with pytest.raises(ValidationError) as caught:
        _Config(retries="s3cret-value")
    text = doctor_mod._config_errors(caught.value)
    assert "retries" in text and "s3cret-value" not in text
    assert doctor_mod._config_errors(ValueError("s3cret-value")) == "ValueError"
