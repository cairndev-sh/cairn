"""`cairn pack new pack` must scaffold a pack that obeys the pack contract (#1511).

The scaffolder is a compounding teacher: whatever shape it emits is the shape
every external author starts from. It was emitting the **retired**
code-carrying shape — a `code/` package with `capabilities.py` and
`tool_descriptors()` — so the first contributor would ship a rule-violating
pack with the tool's blessing.

It was also, separately, **broken**: the generated `pack.toml` declared a
`memory` asset, which is not an allowed key, so `load_manifest` raised and the
pack could not be imported at all. Nobody had run the output end to end.

These tests drive the real CLI and then load what it produced, because a
scaffolder that is only checked by reading its templates is checked by nobody.
"""
from __future__ import annotations

import os
import subprocess
import sys

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

CAIRN = os.path.join(os.path.dirname(sys.executable), "cairn")


@pytest.fixture(scope="module")
def scaffolded(tmp_path_factory):
    """Run the real `cairn pack new pack` once and hand back the pack root."""
    root = tmp_path_factory.mktemp("scaffold")
    proc = subprocess.run(
        [CAIRN, "pack", "new", "pack", "demopack"],
        cwd=root, capture_output=True, text=True,
    )
    assert proc.returncode == 0, f"pack new pack failed:\n{proc.stdout}\n{proc.stderr}"
    return root / "demopack"


# ─── packs are DATA ────────────────────────────────────────────────────


def test_the_only_python_file_is_the_pack_shim(scaffolded):
    """HARD RULE: a domain pack ships md + YAML + assets + one __init__ shim.

    Python belongs in an operator pack, which is its own wheel.
    """
    py = sorted(p.relative_to(scaffolded).as_posix()
                for p in scaffolded.rglob("*.py"))
    assert py == ["__init__.py"], f"a domain pack must ship no domain Python, found: {py}"


def test_no_code_directory_is_scaffolded(scaffolded):
    assert not (scaffolded / "code").exists(), "code/ is the retired pack shape"


def test_the_retired_vocabulary_is_gone(scaffolded):
    """`capabilities.py` / CAPABILITY_BINDINGS / tool_descriptors / templates_dir
    are all the pre-§24.20 shape."""
    blob = "\n".join(
        p.read_text(encoding="utf-8", errors="ignore")
        for p in scaffolded.rglob("*")
        if p.is_file()
    )
    for retired in ("CAPABILITY_BINDINGS", "tool_descriptors", "templates_dir",
                    "code.capabilities"):
        assert retired not in blob, f"scaffold still teaches {retired!r}"


# ─── it ships a runnable surface ───────────────────────────────────────


def test_it_scaffolds_flows_and_pipelines(scaffolded):
    """ADR-0084 D4: the pipeline is what an operator launches. The old scaffold
    emitted neither, so a fresh pack had nothing runnable."""
    assert (scaffolded / "flows" / "hello.md").is_file()
    assert (scaffolded / "pipelines" / "hello.yaml").is_file()
    assert not (scaffolded / "templates").exists(), "templates/ is the retired alias"


# ─── and it actually WORKS ─────────────────────────────────────────────


def test_the_generated_manifest_loads(scaffolded):
    """The regression that made every generated pack unusable: `pack.toml`
    declared a `memory` asset, which is not an allowed key, so load_manifest
    raised and the pack could not be imported."""
    from cairn.core.pack import find_manifest, load_manifest

    path = find_manifest(scaffolded)
    assert path is not None
    manifest = load_manifest(path)          # raised ManifestError before #1511
    assert manifest.name == "demopack"


def test_the_generated_pack_imports_and_builds_a_Pack(scaffolded):
    """Drive the whole thing: import the module the entry-point targets and
    confirm the Pack it builds is data-only with a working doctor check."""
    proc = subprocess.run(
        [sys.executable, "-c",
         "import demopack; p = demopack.pack; "
         "print(p.name, bool(p.pipelines_dir), bool(p.flows_dir), "
         "p.doctor_checks[0]().status, "
         "len(list(getattr(p, 'tools', []) or [])), "
         "len(list(getattr(p, 'capability_bindings', []) or [])))"],
        cwd=scaffolded.parent,
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(scaffolded.parent)},
    )
    assert proc.returncode == 0, f"generated pack does not import:\n{proc.stderr}"
    name, has_pipelines, has_flows, status, n_tools, n_bindings = proc.stdout.split()
    assert name == "demopack"
    assert has_pipelines == "True" and has_flows == "True"
    assert status == "ok", "the scaffolded doctor check must pass on a fresh pack"
    assert n_tools == "0" and n_bindings == "0", "a domain pack contributes no Python"


# ── #1541 — the scaffolded pipeline must be RUNNABLE, not just present ───────
#
# The #1511 fix made the pack load; it left a pipeline that could never launch:
# `_render_pipeline` wired the scaffold's agentic `.md` flow as a `flow:` leg,
# and `flow:` routes to the data/ML engine executor, which resolves refs by
# globbing `flows/*.yaml` ONLY. Nothing planned or launched the generated
# pipeline, so the defect shipped. These tests drive the real plan path.


def test_scaffolded_pipeline_plans_and_its_leg_resolves(scaffolded):
    """The generated pipeline parses with the real loader, plans with the real
    planner, and its one executable leg is a `template:` leg whose ref resolves
    against the pack's own flows/ — the convention every shipped pack and the
    SDK compiler follow (`.md` → `template:`, `.yaml` → `flow:`)."""
    from cairn.core.engine.pipeline import load_pipeline_def, plan_pipeline
    from cairn.core.engine.skill_engine import resolve_template_path

    d = load_pipeline_def(scaffolded / "pipelines" / "hello.yaml")
    plan = plan_pipeline(d)                      # raises on an unplannable def
    assert plan.steps, "the scaffolded pipeline plans to zero steps"

    legs = [s for s in d.steps if s.leg_kind in ("flow", "template", "task")]
    assert legs, "the scaffolded pipeline has no executable leg"
    for leg in legs:
        # the parser pack-qualifies refs (`hello` → `demopack/hello`); the
        # namespaced form needs the pack INSTALLED, so resolve the local name
        # against the scaffold's own flows/ — the invariant under guard is
        # "the leg keyword matches the format of the file the scaffold wrote".
        local = leg.ref.rsplit("/", 1)[-1]
        if leg.leg_kind == "template":
            p = resolve_template_path(local, search_paths=[scaffolded / "flows"])
            assert p.is_file() and p.suffix == ".md"
        elif leg.leg_kind == "flow":
            # a flow: leg may only reference a data/ML flow the engine executor
            # can actually find (flows/*.yaml) — never the agentic .md.
            assert (scaffolded / "flows" / f"{local}.yaml").is_file(), (
                f"leg '{leg.id}' wires `flow: {local}` but flows/{local}.yaml "
                f"does not exist — the engine executor globs *.yaml only, so this "
                f"leg can never launch (#1541). If the ref is an .md agentic "
                f"flow, the leg keyword must be `template:`."
            )


def test_scaffolded_pack_has_changelog(scaffolded):
    """Parity with `cairn pack new operator` (#1541): a CHANGELOG from day one."""
    assert (scaffolded / "CHANGELOG.md").is_file()
