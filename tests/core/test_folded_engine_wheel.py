"""The `cairndev` wheel carries the builtin operator packs and the core contracts (ADR-0137 D2).

A folded directory under ``packages/`` keeps its sources, tests and its own ``pyproject.toml``,
but that file has no build-system table: nothing builds it, and the root ``pyproject.toml``
carries the copy of what it declares. These tests hold the copy equal to the original:

- the root's entry points and scripts are the union of the folded packs' tables;
- each pack extra is flattened onto exactly one root extra (``FOLDED_EXTRAS``), with a sibling
  pack's extra replaced by that pack's own requirements;
- every folded import root is a wheel package, every ``pack.toml`` and contract
  ``definitions`` directory is force-included where its own wheel installed it;
- no folded directory is a workspace member, a source, a dependency or has a build-system;
- the sdist keeps every path the wheel takes (uv builds the wheel from the sdist).

``scripts/check_folded_wheels.py`` proves the same on built wheels.
"""
from __future__ import annotations

import fnmatch
import re
import tomllib
from pathlib import Path, PurePosixPath

import pytest

from tests import _layout

REPO = _layout.REPO
ROOT = tomllib.loads((REPO / "pyproject.toml").read_text())
PROJECT = ROOT["project"]
HATCH = ROOT["tool"]["hatch"]["build"]["targets"]

#: (pack, pack extra) → the root extra it is flattened onto (ADR-0137 D5). A pack extra missing
#: here fails: a new extra is never dropped silently.
FOLDED_EXTRAS = {
    ("remote", "ssh"): "ssh",
    ("dataflow", "ssh"): "ssh",
    ("dataflow", "engine"): "dataflow-engine",
    ("train_local", "train"): "train",
    ("secrets_infisical", "infisical"): "infisical",
    ("rag_rerank", "cross_encoder"): "rerank-cross-encoder",
    ("rag_rerank", "cohere"): "rerank-cohere",
}


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _req_name(req: str) -> str:
    return _norm(re.split(r"[\[<>=!~;\s]", req.strip(), maxsplit=1)[0])


def _folded() -> dict[Path, dict]:
    found = {}
    for path in sorted(REPO.glob("packages/*/pyproject.toml")):
        doc = tomllib.loads(path.read_text())
        if "build-system" not in doc:
            found[path.parent] = doc
    return found


FOLDED = _folded()
PACKS = {d: doc for d, doc in FOLDED.items() if (d / "pack.toml").is_file()}
CONTRACTS = {d: doc for d, doc in FOLDED.items() if (d / "definitions").is_dir()}
NAMES = {_norm(doc["project"]["name"]): d for d, doc in FOLDED.items()}


def _pack(d: Path) -> str:
    return _layout.pack_name(d)


def _import_root(doc: dict) -> str:
    (package,) = doc["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    return package


def test_the_fold_is_the_fourteen_builtins_and_the_thirteen_core_contracts():
    assert len(PACKS) == 14 and len(CONTRACTS) == 13 and len(FOLDED) == 27
    assert set(PACKS) == set(_layout.operator_packs("builtins"))
    assert all(d.name.startswith("cairn-contracts-") for d in CONTRACTS)


@pytest.mark.parametrize("table", ["entry-points", "scripts"])
def test_root_entry_points_and_scripts_are_the_union_of_the_folded_packs(table):
    union: dict = {}
    for d, doc in PACKS.items():
        groups = doc["project"].get(table, {})
        if table == "scripts":
            groups = {"scripts": groups}
        for group, entries in groups.items():
            for name, target in entries.items():
                assert union.setdefault(group, {}).setdefault(name, target) == target, (
                    f"{d.name}: {group} {name} clashes with another folded pack")
    root = PROJECT.get(table, {})
    if table == "scripts":
        root = {"scripts": root}
    for group, entries in union.items():
        for name, target in entries.items():
            assert root.get(group, {}).get(name) == target, f"the root lacks {group} {name} = {target}"
    # the groups only builtins declare carry nothing but theirs
    for group in ("cairn.operators", "cairn.dataflow_engines", "cairn.dataflow_stages"):
        assert root.get(group, {}) == union.get(group, {}), group


def _flatten(pack_dir: Path, extra: str, seen: frozenset = frozenset()) -> set[str]:
    """A pack extra's requirements with every sibling folded pack resolved to its own."""
    assert (pack_dir, extra) not in seen, f"cyclic pack extras at {pack_dir.name}[{extra}]"
    out = set()
    for req in FOLDED[pack_dir]["project"]["optional-dependencies"][extra]:
        name = _req_name(req)
        if name in NAMES:
            inner = re.search(r"\[([^\]]+)\]", req)
            assert inner, f"{pack_dir.name}[{extra}] names the folded {name} without an extra"
            for e in inner.group(1).split(","):
                out |= _flatten(NAMES[name], e.strip(), seen | {(pack_dir, extra)})
        else:
            out.add(req)
    return out


def test_every_pack_extra_is_flattened_onto_its_root_extra():
    by_root: dict[str, set[str]] = {}
    for d, doc in PACKS.items():
        for extra in doc["project"].get("optional-dependencies", {}):
            key = (_pack(d), extra)
            assert key in FOLDED_EXTRAS, f"pack extra {d.name}[{extra}] is not mapped onto a root extra"
            by_root.setdefault(FOLDED_EXTRAS[key], set()).update(_flatten(d, extra))
    extras = PROJECT["optional-dependencies"]
    for root_extra, reqs in by_root.items():
        assert set(extras.get(root_extra, ())) == reqs, root_extra


def test_the_root_names_no_folded_distribution():
    named = [*PROJECT["dependencies"], *(r for reqs in PROJECT["optional-dependencies"].values() for r in reqs)]
    assert not {_req_name(r) for r in named} & set(NAMES)
    uv = ROOT["tool"]["uv"]
    assert not {_norm(s) for s in uv["sources"]} & set(NAMES)
    members = {(REPO / m).resolve() for m in uv["workspace"]["members"]}
    assert not members & {d.resolve() for d in FOLDED}


def test_every_folded_import_root_manifest_and_contract_is_in_the_wheel():
    packages = set(HATCH["wheel"]["packages"])
    force = HATCH["wheel"]["force-include"]
    for d, doc in PACKS.items():
        rel = d.relative_to(REPO).as_posix()
        package = _import_root(doc)
        assert f"{rel}/{package}" in packages, f"{rel}/{package} is not a wheel package"
        target = doc["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]["pack.toml"]
        assert force.get(f"{rel}/pack.toml") == target == f"{PurePosixPath(package).name}/pack.toml"
    for d, doc in CONTRACTS.items():
        rel = d.relative_to(REPO).as_posix()
        target = doc["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]["definitions"]
        assert force.get(f"{rel}/definitions") == target == f"cairn_contracts/{d.name}"
    folded_sources = {k for k in force if k.startswith("packages/")}
    assert len(folded_sources) == len(PACKS) + len(CONTRACTS), "a force-include names no folded directory"


def _excluded(path: str, patterns: list[str]) -> bool:
    """gitignore-style, as hatch reads ``exclude``: an anchored pattern (``/x/*/y``) matches the
    path or one of its parent directories segment by segment; a bare one matches any segment."""
    parts = PurePosixPath(path).parts
    for pattern in patterns:
        if pattern.startswith("/"):
            pat = PurePosixPath(pattern[1:]).parts
            if len(pat) <= len(parts) and all(fnmatch.fnmatchcase(p, q) for p, q in zip(parts, pat)):
                return True
        elif any(fnmatch.fnmatchcase(p, pattern) for p in parts):
            return True
    return False


def test_the_sdist_keeps_every_path_the_wheel_takes():
    """uv builds the wheel FROM the sdist: a force-include source or wheel package the sdist
    excludes fails the release build (ADR-0137 Context 8)."""
    excludes = HATCH["sdist"]["exclude"]
    wanted = [*HATCH["wheel"]["packages"], *HATCH["wheel"]["force-include"]]
    assert [p for p in wanted if _excluded(p, excludes)] == []
    # and the matcher is not vacuous: the folded packs' tests and the client stay out
    assert _excluded("packages/cairn-operator-dataflow/tests/test_x.py", excludes)
    assert _excluded("packages/cairn-client/src/cairn_client/__init__.py", excludes)


def test_no_folded_directory_builds():
    """Independent of ``FOLDED`` (which IS the no-build-system predicate): the directories the
    root wheel takes sources from (its ``packages`` and ``force-include``) are the folded ones,
    and none of them builds on its own."""
    taken = {REPO.joinpath(*PurePosixPath(p).parts[:2])
             for p in [*HATCH["wheel"]["packages"], *HATCH["wheel"]["force-include"]]
             if PurePosixPath(p).parts[0] == "packages"}
    assert len(taken) == 27, sorted(d.name for d in taken)
    assert taken == set(FOLDED), sorted(d.name for d in taken ^ set(FOLDED))
    for d in taken:
        text = (d / "pyproject.toml").read_text()
        assert "build-system" not in tomllib.loads(text), d.name
        assert "FOLDED INTO cairndev (ADR-0137" in text, f"{d.name}: no header saying where it went"
