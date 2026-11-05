"""Every shipped operator pack.toml must parse (regression for #513).

The rag/rerank pack.toml declared ``capabilities = ["rag.rerank"]`` (an array)
where the loader requires ``[operator."x".capabilities]`` with a ``provides``
list — so the operator failed to load at runtime with 6 warning lines, but no
UNIT test caught it (operator tests exercise the operator function, not manifest
parsing). This test loads EVERY operator manifest so a malformed one fails CI.
"""
from __future__ import annotations

import os
import tomllib

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from pathlib import Path

import pytest

from cairn.core.pack.manifest import load_manifest
from tests import _layout

_MANIFESTS = [d / "pack.toml" for d in _layout.operator_packs()]


def test_operator_manifests_present():
    assert _MANIFESTS, "no operator pack.toml files discovered — layout changed?"


@pytest.mark.parametrize("manifest", _MANIFESTS, ids=lambda p: _layout.pack_name(p.parent))
def test_operator_manifest_parses(manifest: Path):
    """Loads clean — a schema violation (e.g. capabilities as an array) raises."""
    m = load_manifest(manifest)
    if not getattr(m, "operators", None):
        # ADR-0134 D2: an engine-only pack (the Metaflow engine's wheel) registers a dataflow
        # engine and no operator. That is the ONLY exemption: its pyproject must say so.
        with (manifest.parent / "pyproject.toml").open("rb") as fh:
            eps = tomllib.load(fh).get("project", {}).get("entry-points", {})
        assert eps.get("cairn.dataflow_engines") and not eps.get("cairn.operators"), \
            f"{manifest} declares no operators and registers no dataflow engine"


