"""The open-source boundary, the half an import graph cannot see (ADR-0120 D8, #2424).

ADR-0120 draws the public core (D1) and says the boundary is a test (D8). The import
edges — core/cli → ``cairn.packs``, core/cli/services → an operator pack, lower layers →
``cairn.core.triggers`` — are rules in ``test_architecture_boundaries.py``. This file holds
the rest:

* operator-pack module names written as strings (a generated import, a ``python -m``
  entry point) in core, cli and services;
* hard-coded refs (``<pack>/<name>``) of an operator or domain pack that is not public;
* the router set the server mounts against the D2 public API.

(The product-modules inventory, rule 6, is closed: #2497 moved ``actions/`` and the
incident observability integration to the platform and kept the rest in core by decision —
the justifications are in ``cairn/core/CLAUDE.md``.)

Each rule follows the boundaries-test discipline: an explicit allowlist of the sites that
violate it today, every entry naming the Phase 1 issue that removes it, a test that FAILS
when an allowlisted site no longer violates (the list only shrinks), and a ``*_can_fail``
test on synthetic input. Literals are read with ``ast`` once per session; docstrings and
bare string statements are prose, not references, and are skipped.
"""
from __future__ import annotations

import ast
import functools
import re
import tomllib
from pathlib import Path

import pytest

from tests import _layout

SCANNED = ("core", "cli", "services")
ISSUE_RE = re.compile(r"#\d{2,6}")

Literal = tuple[str, int, str]  # (relative file, line, value)


# --------------------------------------------------------------------------- shared tree read
def _literals_of_source(src: str, rel: str) -> list[Literal]:
    """Every ``str`` constant in ``src`` except docstrings and bare string statements.
    f-string parts are constants too, so a generated import cannot hide in one."""
    tree = ast.parse(src)
    prose = {id(n.value) for n in ast.walk(tree)
             if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
    return [(rel, n.lineno, n.value) for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in prose]


@functools.cache
def _tree_literals() -> tuple[Literal, ...]:
    out: list[Literal] = []
    for root in SCANNED:
        for path in sorted((_layout.ENGINE / root).rglob("*.py")):
            rel = _layout.rel(path)
            if "/tests/" in rel:
                continue
            out.extend(_literals_of_source(path.read_text(), rel))
    return tuple(out)


def _check(hits: list[Literal], allow: dict[tuple[str, str], str]) -> tuple[list[Literal], list[tuple[str, str]]]:
    """(new violations, allowlisted entries no longer present)."""
    new = sorted(h for h in hits if (h[0], h[2]) not in allow)
    present = {(h[0], h[2]) for h in hits}
    return new, sorted(k for k in allow if k not in present)


def _fmt(hits: list[Literal]) -> str:
    return "\n".join(f"  {rel}:{line}: {value!r}" for rel, line, value in hits)


# --------------------------------------------------------------------------- rule 2: operator module names as strings
#: The private vocabulary as recorded, for a tree without the platform's packs (the public
#: tree, #2424): the import names of the official operator packs, and the ref prefixes of the
#: official operators and the domain packs that no builtin shares. Where the packs are present
#: the rules derive both and ``test_recorded_private_vocabulary_is_current`` holds this to them.
PRIVATE_OPERATOR_MODULES: frozenset[str] = frozenset({
    "operator_blast_radius", "operator_clickstack", "operator_compress", "operator_compute",
    "operator_data_expectations", "operator_dataset_checks", "operator_dataset_synth", "operator_eval",
    "operator_eval_judged", "operator_eval_lm", "operator_git", "operator_ingest", "operator_lineage",
    "operator_llm", "operator_logs_analytics", "operator_model_registry", "operator_notify_slack",
    "operator_postmortem", "operator_redteam", "operator_report", "operator_s3", "operator_serving",
    "operator_training"})
PRIVATE_REF_PREFIXES: frozenset[str] = frozenset({
    "blast-radius", "clickstack", "compress", "cost", "dataset", "dataset-readiness",
    "dataset_readiness", "eval", "git", "incident", "infer", "ingest", "lineage", "llm", "logs", "model",
    "notify", "postmortem", "redteam", "report", "s3", "security-kb", "security-triage", "security_kb",
    "security_triage", "serve", "serving", "traces", "training"})
#: Official operators under a prefix a builtin shares (``data/profile`` and ``rag/search`` are builtins).
#: `train/` is shared since the public train-local builtin (`train/lora`, `train/eval-loss`, ADR-0127 D4),
#: so the platform training pack's `train/*` operators are named one by one. `compute/` is shared since
#: `compute/acquire` and `compute/release` moved into the public remote builtin (ADR-0151 D6, owner
#: decision D-D 2026-10-24): exactly those two are public, the platform's other three stay private.
PRIVATE_SHARED_PREFIX_REFS: frozenset[str] = frozenset({
    "compute/actual", "compute/estimate", "compute/quote",
    "data/expectations", "rag/eval",
    "train/kaggle", "train/local", "train/modal", "train/proxy-trial", "train/runpod",
    "train/stage-dataset", "train/stub", "train/teardown", "train/vast"})


def _pack_modules(*tiers: str) -> frozenset[str]:
    return frozenset(m.name for d in _layout.operator_packs(*tiers) for m in (d / "src").glob("operator_*") if m.is_dir())


@functools.cache
def _operator_modules() -> frozenset[str]:
    """The import names of every operator pack (``src/operator_*``), the private ones as
    recorded where their packs are absent."""
    return _pack_modules() | PRIVATE_OPERATOR_MODULES


_IMPORT_STMT = re.compile(r"^\s*(?:from|import)\s+(operator_\w+)", re.M)
_DOTTED = re.compile(r"(operator_\w+)(?:\.\w+)*(?::\w+)?")


def _names_operator_module(value: str, modules: frozenset[str]) -> bool:
    """A module reference, not prose: an import statement (``from operator_x …``) or a
    whole-string dotted path / entry-point spec (``operator_x.mod``, ``operator_x.mod:fn``).
    Install hints (``cairn-operator-dataflow[engine]``) and descriptions are not references."""
    if _IMPORT_STMT.search(value):
        return True
    m = _DOTTED.fullmatch(value.strip())
    return bool(m and m.group(1) in modules)


#: (file, literal) → the issue that removes it.
OPERATOR_MODULE_STRINGS: dict[tuple[str, str], str] = {}


def _operator_module_hits(literals) -> list[Literal]:
    mods = _operator_modules()
    return [lit for lit in literals if _names_operator_module(lit[2], mods)]


def test_core_names_no_operator_module_as_a_string():
    """ADR-0120 D5/D8: core, cli and services never name an operator pack's module in a
    string — the remote entry point and generated imports come from manifest metadata."""
    new, _ = _check(_operator_module_hits(_tree_literals()), OPERATOR_MODULE_STRINGS)
    assert not new, "operator module named as a string (ADR-0120 D5):\n" + _fmt(new)


def test_operator_module_string_debt_only_shrinks():
    """ADR-0120 D8: an allowlisted site that no longer names an operator module leaves the list."""
    _, gone = _check(_operator_module_hits(_tree_literals()), OPERATOR_MODULE_STRINGS)
    assert not gone, "recorded debt is gone — delete these entries:\n" + "\n".join(
        f"  {k} ({OPERATOR_MODULE_STRINGS[k]})" for k in gone)


@pytest.mark.parametrize("src, flagged", [
    ('ENTRY = ["python", "-m", "operator_eval.run_entry"]\n', True),
    ('CODE = "\\n".join(["import os", "from operator_training.flow_stages import load"])\n', True),
    ('def f(x):\n    return f"from operator_dataflow.stages import {x}"\n', True),
    ('EP = "operator_mcp.operators:CallOp"\n', True),
    ('HINT = "pip install cairn-operator-dataflow[engine]"\n', False),
    ('KEY = "operator_id"\n', False),
    ('DESC = "Empty = the pinned default (operator_dataflow.engines_ssh.DEFAULT_TRAIN_IMAGE)."\n', False),
    ('def f():\n    "Generated code imports operator_dataflow.stages."\n', False),
])
def test_operator_module_string_rule_can_fail(src, flagged):
    hits = _operator_module_hits(_literals_of_source(src, "cairn/core/new.py"))
    new, _ = _check(hits, OPERATOR_MODULE_STRINGS)
    assert bool(new) is flagged, (src, hits)


# --------------------------------------------------------------------------- rule 3: hard-coded non-public refs
#: Domain packs that ship with the public core (ADR-0120 D1: one example pack, #2423).
PUBLIC_DOMAIN_PACKS: frozenset[str] = frozenset()

#: Ref-shaped literals that are not refs, with the reason.
NOT_A_REF: dict[str, str] = {
    "eval/loss": "a training-metric series key (HF Trainer / TensorBoard), not an operator",
    "train/loss": "a training-metric series key (HF Trainer / TensorBoard), not an operator",
}


def _manifest_refs(kind: str) -> set[str]:
    refs: set[str] = set()
    for d in _layout.operator_packs(kind):
        refs.update(tomllib.loads((d / "pack.toml").read_text()).get("operator", {}))
    return refs


def _derived_private_vocabulary() -> tuple[frozenset[str], frozenset[str]]:
    """(prefixes, exact refs) read from the packs this tree has. Prefixes: the official
    operator packs' operator prefixes minus every builtin prefix (builtins ship with core;
    ``data`` and ``rag`` are shared, ``secrets`` is builtin-only since ADR-0127 D3), plus every
    non-public domain pack name in both spellings. Exact refs: official operators under a shared prefix (``data/expectations``)."""
    builtin, official = _manifest_refs("builtins"), _manifest_refs("official")
    builtin_prefixes = {r.split("/")[0] for r in builtin}
    prefixes = {r.split("/")[0] for r in official} - builtin_prefixes
    for p in sorted(_layout.DOMAIN_PACKS.glob("*/pack.toml")) if _layout.DOMAIN_PACKS else ():
        names = {p.parent.name, p.parent.name.replace("_", "-"),
                 str(tomllib.loads(p.read_text()).get("pack", {}).get("name") or p.parent.name)}
        if not names & PUBLIC_DOMAIN_PACKS:
            prefixes |= names
    return frozenset(prefixes), frozenset(r for r in official - builtin if r.split("/")[0] in builtin_prefixes)


@functools.cache
def _nonpublic_ref_shape() -> tuple[re.Pattern[str], frozenset[str]]:
    """(prefix pattern, exact refs): the derived vocabulary plus the recorded one, so the
    pattern is never empty (an empty alternation would flag ``/hr``)."""
    prefixes, exact = _derived_private_vocabulary()
    prefixes |= PRIVATE_REF_PREFIXES
    pattern = re.compile("(?:" + "|".join(sorted(map(re.escape, prefixes))) + r")/[A-Za-z0-9_.\-]+")
    return pattern, exact | PRIVATE_SHARED_PREFIX_REFS


def _nonpublic_ref_hits(literals) -> list[Literal]:
    pattern, exact = _nonpublic_ref_shape()
    return [lit for lit in literals
            if lit[2] not in NOT_A_REF and (pattern.fullmatch(lit[2]) or lit[2] in exact)]


#: (file, literal) → the issue that removes it. Keyed by literal, not line, so a
#: refactor does not churn it; a second copy of an allowlisted literal in the same file
#: is not caught (the file is already debt).
NONPUBLIC_REFS: dict[tuple[str, str], str] = {
    # profile probes naming official operators: they leave with the platform's packs
    # (owner decision 2026-10-12 on #2415: re-tagged to the move, #2427)
    ("cairn/cli/commands/doctor.py", "dataset/pii_scan"): "#2427",
    ("cairn/cli/commands/doctor.py", "model/gate"): "#2427",
    # the eval ledger's recipe label for a contract aggregate — persisted run data,
    # not an operator; it leaves core with cairn/core/eval (#2421)
    ("cairn/core/eval/auto_eval.py", "eval/contract"): "#2421",
}


def test_core_hard_codes_no_nonpublic_ref():
    """ADR-0120 D3/D5/D8: core, cli and services name no operator of an official pack and
    no pipeline of a non-public domain pack; builtin refs (``mcp/call``, ``dataflow/run``)
    are allowed because builtins ship with core."""
    new, _ = _check(_nonpublic_ref_hits(_tree_literals()), NONPUBLIC_REFS)
    assert not new, "hard-coded ref of a non-public pack (ADR-0120 D3/D5):\n" + _fmt(new)


def test_nonpublic_ref_debt_only_shrinks():
    """ADR-0120 D8: an allowlisted ref that is no longer in its file leaves the list."""
    _, gone = _check(_nonpublic_ref_hits(_tree_literals()), NONPUBLIC_REFS)
    assert not gone, "recorded debt is gone — delete these entries:\n" + "\n".join(
        f"  {k} ({NONPUBLIC_REFS[k]})" for k in gone)


@pytest.mark.parametrize("src, flagged", [
    ('OP = "eval/judge"\n', True),                      # official operator
    ('P = ("training/finetune",)\n', True),              # domain pack pipeline
    ('P = "dataset-readiness/import"\n', True),          # domain pack, dashed spelling
    ('S = "data/expectations"\n', True),                # official op under a builtin prefix
    ('OP = "train/kaggle"\n', True),                    # official op under a prefix shared with train-local
    ('OP = "train/lora"\n', False),                     # builtin train-local (ADR-0127 D4)
    ('OP = "compute/estimate"\n', True),               # official op under a prefix shared with remote (ADR-0151 D6)
    ('OP = "compute/actual"\n', True),
    ('OP = "compute/quote"\n', True),
    ('OP = "cost/gate"\n', True),                      # `cost` stays a private prefix
    ('OP = "compute/acquire"\n', False),               # builtin remote (ADR-0151 D6)
    ('OP = "compute/release"\n', False),
    ('OP = "mcp/call"\n', False),                        # builtin
    ('OP = "data/profile"\n', False),                    # builtin under a shared prefix
    ('OP = "secrets/infisical"\n', False),               # builtin (ADR-0127 D3); `secrets` is builtin-only
    ('K = "eval/loss"\n', False),                        # NOT_A_REF
    ('def f():\n    "Runs eval/judge after training/finetune."\n', False),  # docstring
])
def test_nonpublic_ref_rule_can_fail(src, flagged):
    new, _ = _check(_nonpublic_ref_hits(_literals_of_source(src, "cairn/core/new.py")), NONPUBLIC_REFS)
    assert bool(new) is flagged, src


def test_a_shared_prefix_is_shared_with_a_builtin():
    """An exact private ref is recorded only under a prefix a builtin really uses; otherwise the
    prefix itself belongs in ``PRIVATE_REF_PREFIXES`` and the move to exact refs dropped a name."""
    builtin_prefixes = {r.split("/")[0] for r in _manifest_refs("builtins")}
    assert {r.split("/")[0] for r in PRIVATE_SHARED_PREFIX_REFS} <= builtin_prefixes


def test_exactly_two_compute_refs_are_public():
    """ADR-0151 D6 (owner decision D-D, 2026-10-24): the `compute` prefix is public for
    `compute/acquire` and `compute/release` only; the platform's cost operators stay private."""
    assert {r for r in _manifest_refs("builtins") if r.startswith("compute/")} == {
        "compute/acquire", "compute/release"}
    assert "compute" not in PRIVATE_REF_PREFIXES and "operator_compute" in PRIVATE_OPERATOR_MODULES
    new, _ = _check(_nonpublic_ref_hits(_literals_of_source(
        'A = "compute/acquire"\nR = "compute/release"\nE = "compute/estimate"\n', "cairn/core/new.py")),
        NONPUBLIC_REFS)
    assert [v for _, _, v in new] == ["compute/estimate"]


@pytest.mark.skipif(not _layout.PRIVATE_TREES, reason="the platform's packs are not in this tree")
def test_recorded_private_vocabulary_is_current():
    """The recorded vocabulary is exactly what the platform's packs declare: a pack added or
    gone updates it here, so the public tree's rules know every private name."""
    prefixes, exact = _derived_private_vocabulary()
    assert (PRIVATE_REF_PREFIXES, PRIVATE_SHARED_PREFIX_REFS) == (prefixes, exact)
    assert PRIVATE_OPERATOR_MODULES == _pack_modules("official")


@pytest.mark.parametrize("allow", [OPERATOR_MODULE_STRINGS, NONPUBLIC_REFS])
def test_string_debt_names_its_issue(allow):
    assert all(ISSUE_RE.fullmatch(i) for i in allow.values())


# --------------------------------------------------------------------------- rule 5: the public server's routers
#: ADR-0120 D2, as the endpoint modules that serve it — grown by the accounts router (owner
#: decision on #2427: `/auth/*`, `/workspaces` and `/variables` stay public; the CLI logs in
#: and scopes by workspace), by `jobs` (owner decision on #2427: the pull lane is core) and by
#: `oauth` (owner decision on #2427: the public CLI's `cairn connect --via oauth` redirects to
#: `/oauth/callback`; ADR-0123 D1).
PUBLIC_SURFACES: frozenset[str] = frozenset({
    "cairn.core.triggers.auth",              # /token (+ /auth/status)
    "cairn.core.triggers.server",            # /healthz and spec-declared trigger ingress
    "cairn.core.triggers.routers.start",
    "cairn.core.triggers.routers.pipelines",
    "cairn.core.triggers.routers.runs",
    "cairn.core.triggers.routers.run_control",
    "cairn.core.triggers.routers.reviews",
    "cairn.core.triggers.routers.templates",
    "cairn.core.triggers.routers.registry",
    "cairn.core.triggers.routers.schedules",
    "cairn.core.triggers.routers.ingress",   # ingress and inbox
    "cairn.core.triggers.routers.accounts",  # /workspaces, /variables, /auth/* (#2427: the CLI logs in)
    "cairn.core.triggers.routers.jobs",      # /jobs/claim, /jobs/{id}/heartbeat (#2427: `cairn worker --pull`)
    "cairn.core.triggers.routers.oauth",     # /oauth/callback (#2427: `cairn connect --via oauth`)
    "fastapi.applications",                  # /openapi.json, /docs
})

#: Mounted only when a build artefact or opt-in env is present (the console SPA when
#: ``console/dist`` is built; ops-MCP under ``OBS_OPS_MCP``). Console-only either way.
CONDITIONAL_SURFACES: frozenset[str] = frozenset({"mount:/console", "cairn_platform.console.static", "mount:/mcp"})

#: What the ``cairn.routers`` providers registered in the ROOT pyproject mount beyond D2
#: (#2422). Proven empty by #2427: the console's surfaces and ``ask`` moved to the platform,
#: ``jobs`` and ``/oauth/callback`` joined D2, and the root registers no provider. Kept so a
#: console-only router added to the root has to be recorded here (and fails review).
CONSOLE_ONLY_SURFACES: dict[str, str] = {}

#: What the private platform's ``cairn.routers`` providers mount (packages/cairn-platform,
#: #2421) — expected only where ``cairn_platform`` is installed (not in the public set).
PLATFORM_SURFACES: frozenset[str] = frozenset({
    # the console's surfaces (#2427)
    "cairn_platform.chat.api",
    "cairn_platform.console.api",
    "cairn_platform.console.artifacts_api",
    "cairn_platform.routers.admin",  # /admin/users, /admin/invites, /admin/account/domain, /demo/*
    "cairn_platform.routers.ask",  # /ask, /ask/thread/{id} (needs the private cairn-ask extra)
    "cairn_platform.routers.backends",
    "cairn_platform.routers.builder",
    "cairn_platform.routers.connections",
    "cairn_platform.routers.console_triggers",
    "cairn_platform.routers.features",
    "cairn_platform.routers.knowledge",
    "cairn_platform.routers.models",
    "cairn_platform.routers.policies",
    "cairn_platform.routers.profiles",
    "cairn_platform.routers.setup",
    # the product routers (#2421)
    "cairn_platform.routers.capture",
    "cairn_platform.routers.datasets",
    "cairn_platform.routers.judges",
    "cairn_platform.routers.pilot",  # #2421 wave 2c: /series, /runs/{id}/pilot, /runs/{id}/adapter-chain
    "cairn_platform.routers.mid_training",  # /runs/{id}/proposals*, /runs/pending-proposals
    "cairn_platform.routers.eval",  # /runs/{id}/evaluate, /runs/{id}/diagnosis, /packs/{p}/{t}/metric-contract
    "cairn_platform.routers.recommender",  # #2421 wave 2d: POST /pipelines/suggest
    "cairn_platform.routers.insights",  # the console-only routers that import product modules
    "cairn_platform.routers.model_catalog",
    "cairn_platform.routers.playground",
    "cairn_platform.routers.training_runs",
})


def _platform_installed() -> bool:
    import importlib.util

    return importlib.util.find_spec("cairn_platform") is not None


def _surfaces(app) -> frozenset[str]:
    """Each route as the module that serves it; a sub-app mount as ``mount:<path>``."""
    out: set[str] = set()
    for route in app.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is not None:
            out.add(endpoint.__module__)
        else:
            out.add(f"mount:{route.path}")
    return frozenset(out)


def _build(*, providers: bool) -> frozenset[str]:
    from cairn.core.triggers import router_providers
    from cairn.core.triggers.server import build_app
    from tests.core.test_trigger_server_dispatch import FakeRunner

    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("OBS_OPS_MCP", raising=False)
        if not providers:
            mp.setattr(router_providers, "load_router_providers", lambda: [])
        return _surfaces(build_app(FakeRunner(), enable_scheduler=False))


@pytest.fixture(scope="module")
def mounted() -> frozenset[str]:
    """The app as this repo installs it: the public API plus every registered provider."""
    return _build(providers=True)


@pytest.fixture(scope="module")
def public_mounted() -> frozenset[str]:
    """The app with no ``cairn.routers`` provider installed — the public core alone."""
    return _build(providers=False)


def test_server_mounts_every_public_router(mounted):
    """ADR-0120 D2: the routers the CLI and cairn-client call are all mounted."""
    assert PUBLIC_SURFACES <= mounted, sorted(PUBLIC_SURFACES - mounted)


def test_server_extra_routers_are_exactly_the_recorded_console_set(mounted):
    """ADR-0120 D2/D4: beyond the public API the registered providers mount exactly the
    recorded console-only set — a new console-only router fails here, and one that leaves
    must leave the list. (A stale editable install without the ``cairn.routers`` entry
    points mounts none of them: reinstall the root project.)"""
    extra = mounted - PUBLIC_SURFACES - CONDITIONAL_SURFACES
    recorded = set(CONSOLE_ONLY_SURFACES) | (PLATFORM_SURFACES if _platform_installed() else set())
    assert extra == recorded, (
        f"new (not D2, not recorded): {sorted(extra - recorded)}\n"
        f"gone (delete from CONSOLE_ONLY_SURFACES / PLATFORM_SURFACES): {sorted(recorded - extra)}")


def test_public_server_mounts_only_d2(public_mounted):
    """ADR-0120 D2/D4: with no router provider installed the server mounts the D2 routers
    and nothing else — every other surface reaches the app only through ``cairn.routers``."""
    assert public_mounted == PUBLIC_SURFACES, (
        f"beyond D2: {sorted(public_mounted - PUBLIC_SURFACES)}\n"
        f"missing: {sorted(PUBLIC_SURFACES - public_mounted)}")


def test_router_rule_can_fail():
    from fastapi import APIRouter, FastAPI

    app = FastAPI()
    r = APIRouter()
    r.add_api_route("/x", lambda: None)
    app.include_router(r)
    assert __name__ in _surfaces(app) - PUBLIC_SURFACES  # a router defined anywhere new is seen
