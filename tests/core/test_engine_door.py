"""The flow-engine door (ADR-0134 D3c; was the SkyPilot door of ADR-0104 D5, #2073).

A requirement DERIVED from configuration — the environment selected a flow engine — enforced
at the launch door (preflight, the dispatcher, the pipeline surface) before any record exists,
naming the remedy. The control plane refuses; it never installs an extra and never provisions
a machine to make the check pass.

The door names no engine. It asks only about the flow kinds a launch reaches, never refuses
for ``builtin``, and asks an installed engine's optional ``check_ready()`` for its reasons.

Everything here drives fakes: asking a real provisioning engine whether a cloud is enabled is
a network call, and asking it to launch is a paid one.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cairn.core.dataflow.engine_door import (
    ENGINE_OPERATORS,
    check_flow_engines,
    engines_selected,
    kinds_reached,
)


class _Engine:
    """An installed engine that offers ``check_ready()``."""

    def __init__(self, reasons=()):
        self._reasons = list(reasons)

    async def check_ready(self):
        return list(self._reasons)


class _PlainEngine:
    """An installed engine without the optional capability — ready once installed."""


def _factory(engine):
    """The registry LOOKUP the door injects: name -> the pack's factory."""
    def _lookup(name):
        def _make(*, kind="data", target=""):
            return engine
        return _make
    return _lookup


@pytest.fixture(autouse=True)
def _no_ambient_selection(monkeypatch):
    for var in ("ML_ENGINE", "DATAFLOW_ENGINE", "ML_TARGET", "DATAFLOW_TARGET"):
        for prefix in ("OBS_", "CAIRN_"):
            monkeypatch.delenv(prefix + var, raising=False)


# ── scope ───────────────────────────────────────────────────────────────────


def test_only_the_engine_operators_reach_a_kind():
    assert kinds_reached(["ml/run", "rag/search", "dataflow/run"]) == ["data", "ml"]
    assert kinds_reached(["mcp/call", "memory/write"]) == []
    assert ENGINE_OPERATORS == {"data": "dataflow/run", "ml": "ml/run"}


async def test_builtin_is_never_refused_and_never_looked_up(monkeypatch):
    """An install without the builtin dataflow pack is legitimate; the door is not where
    its absence is reported."""
    monkeypatch.setenv("OBS_ML_ENGINE", "builtin")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "builtin")

    def _lookup(name):
        raise AssertionError(f"the door must not look up {name!r}")

    v = await check_flow_engines(factory=_lookup)
    assert v.ok and not v.reasons and not v.required


async def test_a_kind_the_launch_does_not_reach_is_not_asked(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "metaflow")
    assert (await check_flow_engines(kinds=["data"], factory=lambda name: None)).ok
    assert (await check_flow_engines(kinds=[], factory=lambda name: None)).ok


def test_selection_is_read_from_either_flow_kind(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    assert engines_selected()["ml"] == "skypilot"
    monkeypatch.delenv("OBS_ML_ENGINE")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "metaflow")
    assert engines_selected() == {"data": "metaflow", "ml": "builtin"}


# ── refusals ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("engine", ["skypilot", "metaflow"])
async def test_an_engine_no_installed_package_provides_is_refused(monkeypatch, engine):
    """Any engine, not only SkyPilot: a Metaflow-selected environment without the wheel is
    refused here instead of failing at submit after the record exists."""
    monkeypatch.setenv("OBS_ML_ENGINE", engine)
    v = await check_flow_engines(factory=lambda name: None)
    assert not v.ok and v.required == [engine]
    reason = v.reasons[0]
    assert "no installed package provides it" in reason
    assert "install the package that does" in reason and "ml_engine" in reason
    assert "installing" not in reason  # it names the fix; it never installs


async def test_the_engines_own_reasons_are_the_refusal(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_ML_TARGET", "gpubox")
    v = await check_flow_engines(
        factory=_factory(_Engine(["sky check: infra 'ssh/gpubox' is not enabled (enabled: aws)"])))
    assert not v.ok
    assert v.reasons == ["ml flows are set to the 'skypilot' engine: "
                         "sky check: infra 'ssh/gpubox' is not enabled (enabled: aws)"]


async def test_a_registration_that_does_not_load_is_a_refusal_not_a_crash(monkeypatch):
    """A stale or broken entry point (the module moved, the attribute is gone) raises on
    load. The door must refuse with the remedy, not raise into the launch."""
    monkeypatch.setenv("OBS_ML_ENGINE", "metaflow")

    def _broken(name):
        raise AttributeError("module 'operator_dataflow.engines' has no attribute 'metaflow_engine'")

    v = await check_flow_engines(factory=_broken)
    assert not v.ok and "does not load" in v.reasons[0] and "cairndev[metaflow]" in v.reasons[0]


async def test_an_engine_whose_check_raises_is_a_refusal_not_a_crash(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")

    class _Boom:
        async def check_ready(self):
            raise RuntimeError("sky check exploded")

    v = await check_flow_engines(factory=_factory(_Boom()))
    assert not v.ok and "sky check exploded" in v.reasons[0]


# ── admissions ──────────────────────────────────────────────────────────────


async def test_a_ready_engine_and_an_engine_without_check_ready_both_pass(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "metaflow")
    engines = {"skypilot": _Engine(), "metaflow": _PlainEngine()}
    v = await check_flow_engines(factory=lambda name: _factory(engines[name])(name))
    assert v.ok and not v.reasons and sorted(v.required) == ["metaflow", "skypilot"]


async def test_the_verdict_shape_is_the_one_the_doors_already_speak(monkeypatch):
    from cairn.core.job_channel_door import ServiceVerdict

    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    assert isinstance(await check_flow_engines(factory=_factory(_Engine())), ServiceVerdict)


def test_building_a_registered_engine_for_the_check_has_no_side_effect(tmp_path, monkeypatch):
    """The door builds the selected engine with the factory call the leg will make, with the
    real target. That is only safe if construction is free of side effects: every installed
    engine is built here in an empty working directory and an empty HOME, which must both stay empty."""
    from importlib.metadata import entry_points

    from cairn.core.dataflow.engine_registry import ENTRY_POINT_GROUP

    cwd, home = tmp_path / "cwd", tmp_path / "home"
    cwd.mkdir(), home.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("HOME", str(home))
    built = []
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        try:
            make = ep.load()
        except (ImportError, AttributeError):  # a registration that does not load: the door refuses it unbuilt
            continue
        for kind in ("data", "ml"):
            make(kind=kind, target="")
            built.append((ep.name, kind))
    assert ("builtin", "data") in built or not built  # the builtin engine is exercised when installed
    assert list(cwd.iterdir()) == [] and list(home.iterdir()) == []


# ── the door at the launch surfaces ─────────────────────────────────────────


def _dispatcher(monkeypatch, operators):
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.storage import build_storage

    async def _run(compiled, input_data):
        return {"summary": "ran"}

    monkeypatch.setattr("cairn.core.engine.skill_engine.resolve_template_path", lambda ref: "X")
    monkeypatch.setattr("cairn.core.engine.compiler.compile_template", lambda path: SimpleNamespace())
    monkeypatch.setattr("cairn.core.engine.runner.run_within_budget", _run)
    monkeypatch.setattr("cairn.core.operator.load_all_operators", lambda **k: [])
    monkeypatch.setattr("cairn.core.pack.loader.load_all_packs", lambda **k: None)
    monkeypatch.setattr("cairn.core.engine.workflow_config.collect_template_operators",
                        lambda ref: list(operators))
    monkeypatch.setattr("cairn.core.dataflow.engine_door.load_engine_factory", lambda name: None)
    monkeypatch.setenv("OBS_STORAGE", "")

    class FakeRunner:
        pass

    return RunDispatcher(FakeRunner(), backend=build_storage())


async def test_the_dispatcher_refuses_a_template_that_reaches_the_engine_before_a_record(monkeypatch):
    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setenv("OBS_ML_TARGET", "gpubox")
    d = _dispatcher(monkeypatch, ["ml/run"])
    with pytest.raises(ValueError, match="flow-engine door"):
        await d.run_template("demo/train", {})
    assert d._tasks == {}


async def test_the_dispatcher_never_asks_the_door_for_an_agentic_template(monkeypatch):
    """ADR-0134 D3c: before it, every template run was judged by ml_engine, so an
    environment set to an uninstalled engine refused agentic runs that reach none."""
    monkeypatch.setenv("OBS_ML_ENGINE", "metaflow")
    d = _dispatcher(monkeypatch, ["rag/search", "mcp/call"])
    asked: list = []

    async def _spy(**kw):
        asked.append(kw)
        raise AssertionError("the door must not be asked for an agentic template")

    monkeypatch.setattr("cairn.core.dataflow.engine_door.check_flow_engines", _spy)
    try:
        await d.run_template("demo/assistant", {})
    except ValueError as e:  # another door may still answer; the engine door must not
        assert "flow-engine door" not in str(e)
    assert asked == []


async def test_preflight_reports_the_engine_blocker_only_when_the_template_reaches_it(monkeypatch):
    from cairn.core import preflight as pf

    monkeypatch.setenv("OBS_ML_ENGINE", "skypilot")
    monkeypatch.setattr("cairn.core.dataflow.engine_door.load_engine_factory", lambda name: None)

    from cairn.core.ui_contract import UIContract

    def _contract(operators):
        plan = {"template_found": True, "operators": [{"operator": o} for o in operators]}
        return lambda ref, profile=None: UIContract(template_ref=ref, plan=plan)

    monkeypatch.setattr(pf, "build_ui_contract", _contract(["ml/run"]))
    v = await pf.build_preflight("demo/train", {}, check_connections=False)
    blockers = [b for b in v.blockers if b.kind == "engine-not-ready"]
    assert blockers and "no installed package provides it" in blockers[0].what
    assert "cairndev[<engine>]" in blockers[0].fix and v.ready is False

    monkeypatch.setattr(pf, "build_ui_contract", _contract(["rag/search"]))
    v = await pf.build_preflight("demo/assistant", {}, check_connections=False)
    assert not [b for b in v.blockers if b.kind == "engine-not-ready"]
