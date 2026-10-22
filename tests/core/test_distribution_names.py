"""ADR-0121 D3/D5 (#2429): the engine DISTRIBUTION is ``cairndev`` (import name ``cairn``
and the ``cairn`` command unchanged); every place that names the distribution says so; the
engine image variable is ``CAIRN_IMAGE``."""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

from cairn.core.pack.loader import ENGINE_DISTRIBUTION
from tests import _layout

#: Runs in both trees (#2524): the engine and the operator packs come from the layout helper,
#: and the platform's packs are checked only where they sit beside the engine.
REPO = _layout.REPO
ENGINE = _layout.ENGINE


def test_the_engine_distribution_is_cairndev_and_points_at_the_public_repo():
    proj = tomllib.loads((REPO / "pyproject.toml").read_text())["project"]
    assert proj["name"] == "cairndev"
    assert proj["urls"]["Source"] == "https://github.com/cairndev-sh/cairn"
    assert (ENGINE / "py.typed").is_file()


def test_version_lookups_and_install_hints_name_the_distribution():
    bad = []
    for f in ENGINE.rglob("*.py"):
        s = f.read_text(errors="ignore")
        if 'version("cairn")' in s or "pip install 'cairn[" in s or "pip install cairn[" in s:
            bad.append(f.relative_to(REPO).as_posix())
    assert not bad, bad
    assert 'version("cairndev")' in (ENGINE / "core/settings.py").read_text()


#: An install command naming the bare ``cairn`` distribution: on PyPI that name belongs to an
#: unrelated project (ADR-0029), so a user following such a line installs someone else's code.
_BARE_CAIRN_INSTALL = re.compile(
    r"""(?:(?:pip|pipx|uv tool|uv pip) install(?:\s+-[-\w=]+)*|uv add)\s+["']?cairn(?:[\[\s"'=<>~!]|$)""", re.M)


def test_install_lines_never_name_the_bare_cairn_distribution():
    roots = [REPO / "README.md", REPO / "CONTRIBUTING.md", REPO / "examples", REPO / "docs/site/src",
             REPO / "docs/site/public", REPO / "src", REPO / "tests", REPO / "packages"]
    files = [p for r in roots if r.exists() for p in ([r] if r.is_file() else r.rglob("*"))
             if p.is_file() and p.suffix in {".md", ".mdx", ".yml", ".yaml", ".sh", ".toml", ".py"}
             and p.resolve() != Path(__file__).resolve()]  # this file names the patterns it forbids
    bad = [f"{f.relative_to(REPO).as_posix()}: {m.group(0).strip()}"
           for f in files for m in _BARE_CAIRN_INSTALL.finditer(f.read_text(errors="ignore"))]
    assert not bad, bad


def test_the_pack_loader_checks_compat_against_the_engine_distribution():
    """#2512: ``[compat] cairn_engine`` is checked against ``ENGINE_DISTRIBUTION``; naming a
    distribution that is not the engine's makes every pack's check "cannot verify"."""
    assert ENGINE_DISTRIBUTION == tomllib.loads((REPO / "pyproject.toml").read_text())["project"]["name"]


def test_the_example_pack_and_the_scaffold_depend_on_cairndev():
    assert "cairndev>=" in (REPO / "examples/hello-cairn/pyproject.toml").read_text()
    assert '"cairndev>=' in (ENGINE / "cli/commands/init.py").read_text()


def test_the_image_variable_is_cairn_image():
    stale = []
    for pattern in ("deploy/**/*.yml", "deploy/**/*.example", "deploy/**/README.md", ".github/workflows/*.yml", "docs/deploy-ec2.md"):
        for f in REPO.glob(pattern):
            if re.search(r"\bOBSAGENT_IMAGE\b(?! until)", f.read_text(errors="ignore")):
                stale.append(f.relative_to(REPO).as_posix())
    assert not stale, stale
    assert "${CAIRN_IMAGE:-" in (REPO / "deploy/light/docker-compose.yml").read_text()


#: ADR-0121 D3 (#2429): the operator packs ship as ``cairn-operator-<name>``; import names
#: (``operator_<name>``), operator refs and entry-point names do not change.
_OPERATOR_PYPROJECTS = sorted(p / "pyproject.toml" for p in _layout.operator_packs())
#: 23 official + 14 builtin + 2 optional packs in the monorepo; the public tree has the 14
#: builtins (`train_local` joined, ADR-0127 D4) and the optional Metaflow and SkyPilot engines
#: (ADR-0134 D2/D3).
_OPERATOR_PACKS = 39 if _layout.PRIVATE_TREES else 16


def test_every_operator_pack_distribution_is_cairn_operator_and_matches_its_directory():
    names = {_layout.pack_name(p.parent): tomllib.loads(p.read_text())["project"]["name"] for p in _OPERATOR_PYPROJECTS}
    assert len(names) == _OPERATOR_PACKS, sorted(names)
    wrong = {d: n for d, n in names.items() if n != "cairn-operator-" + d.replace("_", "-")}
    assert not wrong, wrong


def test_every_shipped_pack_manifest_names_cairn_as_publisher():
    domain = sorted(_layout.DOMAIN_PACKS.glob("*/pack.toml")) if _layout.DOMAIN_PACKS else []
    manifests = [*(p / "pack.toml" for p in _layout.operator_packs()), *domain]
    wrong = [m.relative_to(REPO).as_posix() for m in manifests
             if tomllib.loads(m.read_text())["pack"].get("publisher") != "cairn"]
    assert len(manifests) == (48 if _layout.PRIVATE_TREES else _OPERATOR_PACKS) and not wrong, wrong


def test_no_old_operator_distribution_name_remains():
    old = "|".join(sorted((_layout.pack_name(p.parent).replace("_", "-") for p in _OPERATOR_PYPROJECTS), key=len, reverse=True))
    stale = re.compile(rf"(?<![A-Za-z0-9_./-])operator-(?:{old})(?![A-Za-z0-9_-])")
    files = [REPO / n for n in ("pyproject.toml", ".env.example", ".pre-commit-config.yaml",
                                "Dockerfile.engine", "Dockerfile.dataflow-task", "THIRD_PARTY_NOTICES.md")]
    files = [f for f in files if f.is_file()]
    for top in (ENGINE, *(REPO / t for t in ("tests", "scripts", "examples", "packages", "contracts", "deploy"))):
        for pattern in ("*.py", "*.toml", "*.yaml", "*.yml", "Dockerfile*", "*.example"):
            files += [f for f in top.rglob(pattern) if "__pycache__" not in f.parts]
    hits = sorted({f.relative_to(REPO).as_posix() for f in files
                   if stale.search(f.read_text(errors="ignore"))})
    assert not hits, hits
