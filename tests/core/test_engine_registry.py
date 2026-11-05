"""#1512 — core selects a flow engine without importing any operator pack.

The load-bearing test here is :func:`test_core_never_imports_an_operator_pack`.
The rest pin the selection semantics that moved out of
``operator_dataflow.engines.resolve_engine``; the structural one is what stops
the coupling coming back, because it fails on the *next* ``from operator_...``
anywhere in core rather than on this one call site.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from cairn.core.dataflow.engine import EngineError
from cairn.core.dataflow import engine_registry as reg

CORE = Path(__file__).resolve().parents[2] / "src" / "cairn" / "core"


def test_core_never_imports_an_operator_pack():
    """No module under ``cairn/core`` may import an operator pack.

    ADR-0084 D1: runtimes are adapters behind a core-owned waist, so the
    dependency runs pack -> core. Two call sites had inverted it
    (``pipeline_bridge`` and the Temporal pipeline activities), which made the
    dataflow pack a hard requirement for dispatching ANY flow leg.

    This walks the AST rather than grepping: an import node is an import; a
    string is data. Operator-pack module names written as strings are the
    separate rule in ``test_public_core_boundary.py`` (#2415 removed the last
    generated ``operator_dataflow`` import from ``dataflow/export.py``).
    """
    offenders: list[str] = []
    for path in sorted(CORE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".")[0].startswith("operator_"):
                    offenders.append(f"{path.relative_to(CORE.parent.parent)}:{node.lineno} -> {name}")
    assert not offenders, (
        "core imports an operator pack — resolve it through an entry-point "
        "group instead (see cairn/core/dataflow/engine_registry.py):\n  "
        + "\n  ".join(offenders)
    )


def test_selected_engine_defaults_to_builtin():
    """No profile, no env → the builtin engine and no target."""
    assert reg.selected_engine("data") == ("builtin", "")
    assert reg.selected_engine("ml") == ("builtin", "")


def test_selected_engine_reads_the_kind_axis(monkeypatch):
    """`kind: ml` reads ml_engine/ml_target, `kind: data` reads dataflow_*.

    #1180 — an ml leg handed the data engine was a real bug; keep the axis
    pinned. An unknown kind falls back to the data keys rather than raising:
    selection is not the place to validate a flow def.
    """
    monkeypatch.setenv("OBS_ML_ENGINE", "metaflow")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "skypilot")
    assert reg.selected_engine("ml")[0] == "metaflow"
    assert reg.selected_engine("data")[0] == "skypilot"
    assert reg.selected_engine("nonsense")[0] == "skypilot"


def test_resolve_engine_calls_the_registered_factory(monkeypatch):
    """Core hands (kind, target) to the factory and returns whatever it built —
    core itself constructs nothing."""
    seen: dict[str, object] = {}

    def factory(*, kind: str, target: str) -> str:
        seen.update(kind=kind, target=target)
        return "an-engine"

    monkeypatch.setattr(reg, "selected_engine", lambda kind="data": ("stub", "box-1"))
    monkeypatch.setattr(reg, "load_engine_factory", lambda name: factory if name == "stub" else None)
    assert reg.resolve_engine(None, kind="ml") == "an-engine"
    assert seen == {"kind": "ml", "target": "box-1"}


def test_uninstalled_engine_fails_loudly_and_names_what_is_available(monkeypatch):
    """#568 discipline: a selected-but-missing engine raises rather than quietly
    running on the builtin one, and the message lists what IS installed (the old
    hardcoded "builtin, metaflow, prefect" advertised engines a deploy might
    never have installed)."""
    monkeypatch.setattr(reg, "selected_engine", lambda kind="data": ("dagster", ""))
    monkeypatch.setattr(reg, "load_engine_factory", lambda name: None)
    monkeypatch.setattr(reg, "installed_engines", lambda: ["builtin", "metaflow"])
    with pytest.raises(EngineError) as excinfo:
        reg.resolve_engine(None, kind="data")
    msg = str(excinfo.value)
    assert "dagster" in msg and "builtin, metaflow" in msg
    assert "dataflow_engine" in msg  # names the knob to turn, for the data kind


def test_dataflow_pack_registers_its_engines():
    """The installed dataflow pack advertises its adapters through the group —
    the other half of the seam. Skipped where the pack isn't installed, since
    core is meant to work without it."""
    installed = reg.installed_engines()
    if not installed:
        pytest.skip("no cairn.dataflow_engines provider installed")
    assert "builtin" in installed
    factory = reg.load_engine_factory("builtin")
    assert callable(factory)


def test_builtin_factory_builds_the_local_engine_and_refuses_a_target():
    """The `builtin` engine runs on this machine; a target box belongs to another engine."""
    pytest.importorskip("operator_dataflow")
    from operator_dataflow.engines import BuiltinLocalEngine, builtin_engine

    assert isinstance(builtin_engine(kind="data", target=""), BuiltinLocalEngine)
    with pytest.raises(EngineError, match="box-1"):
        builtin_engine(kind="data", target="box-1")
