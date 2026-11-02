"""`cairn run <pack>/<flow>` — ADR-0106 D1 (#2134) closed the governed half.

#2118 made this command start a bare flow as its one-leg pipeline, so a flow
started from a terminal and one started from the console were the same governed
object. ADR-0106 D1 keeps the goal and changes the mechanism: only a DECLARED
pipeline runs, so the synthesis is gone from every door and this one refuses,
naming the wrapper to add.

What survives is the half that was never a governed run at all — the dev loop.
That is now the command's whole job for a flow ref, which is why the refusal
names ``--interactive`` in the same breath as the wrapper.

These tests pin the door, not the printer:

* ``cairn run <flow> --input …`` refuses, with BOTH ways forward, and leaves no
  pipeline record behind;
* a ref that names a pipeline goes to the launch door (ADR-0165 D3);
* ``--interactive``, ``--mock`` and ``--dry-run`` keep the in-process dev path
  and make no pipeline record at all — they are the local loop, and `--help`
  says so.

Everything below the door is real (real ``PipelineBridge``, real
``RunDispatcher``); only the flow's BODY is stubbed, the way
``tests/core/test_pipeline_bridge_e2e.py`` stubs it.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")
os.environ.setdefault("OBS_CONNECTIONS_DB", ":memory:")

import pytest
import typer
from typer.testing import CliRunner

from cairn.cli.commands import run as run_mod
from cairn.core.storage.pipeline_store import InMemoryPipelineStore

#: `scratch-dev/echo`: an agentic flow no pipeline is named after, in a scratch pack whose
#: directory (`scratch_dev`) and `pack.toml` name differ by separator — so the refusal below
#: is for being unwrapped, not unreachable (#2126). A scratch pack, not a platform one, so
#: the door is pinned where only the public core is installed (#2428); the platform's
#: `security-triage/triage-alert` has the same shape.
FLOW = "scratch-dev/echo"
FLOW_INPUTS = {"indicator": "203.0.113.7"}
#: A declared pipeline every install has: the example pack's.
PIPELINE = "hello-cairn/profile-and-publish"

# Mounted on a LOCAL Typer app (not `cairn.cli.main`, which calls `load_env()` at
# import and repopulates the storage keys conftest neutralizes — #1199). The
# second command keeps Typer in GROUP shape, or `run` would be parsed as the
# positional ref. Same pattern as tests/cli/test_ls_runnable_surface.py.
app = typer.Typer()
app.command(name="run")(run_mod.cmd)


@app.command(name="_noop")
def _noop() -> None:
    """Keeps the Typer group shape."""


runner = CliRunner()


@pytest.fixture(autouse=True)
def _packs_loaded(dev_packs, scratch_pack, example_pack):
    from cairn.core.operator import load_all_operators
    load_all_operators(strict=False)
    dev_packs(example_pack, scratch_pack("scratch_dev", "scratch-dev").parent)


@pytest.fixture
def inputs_file(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("OBS_RUNS_DIR", str(tmp_path / "runs"))  # never write ./.cairn/runs
    p = tmp_path / "in.json"
    p.write_text(json.dumps(FLOW_INPUTS))
    return p


@pytest.fixture
def bridges(monkeypatch) -> list:
    """Every bridge the command builds, each on its own in-memory pipeline store.

    The store is injected rather than defaulted so the assertions read the
    records this run made, not whatever the shared sqlite file holds.
    """
    import cairn.core.dispatch.pipeline_bridge as bridge_mod

    made: list = []
    real = bridge_mod.PipelineBridge

    def _factory(dispatcher, **kw):
        kw.setdefault("store", InMemoryPipelineStore())
        b = real(dispatcher, **kw)
        made.append(b)
        return b

    monkeypatch.setattr(bridge_mod, "PipelineBridge", _factory)
    return made


@pytest.fixture
def flow_body(monkeypatch) -> list[dict]:
    """Stub the flow's BODY (the dispatcher imports it lazily at call time), so
    the leg completes, publishes its terminal, and the bridge settles."""
    seen: list[dict] = []

    async def _run(_compiled, input_data):
        seen.append(dict(input_data))
        return {"verdict": "pass", "rows": 3}

    monkeypatch.setattr("cairn.core.engine.runner.run_within_budget", _run)
    monkeypatch.setattr("cairn.core.pack.provisioning.unprovisioned_requires", lambda p: [])
    return seen


@pytest.fixture(autouse=True)
def _fast_polls(monkeypatch):
    """The contract is ownership and ordering, not cadence (2s in production)."""
    import cairn.cli.commands.pipeline as pipeline_cli

    orig = pipeline_cli._drive_until_terminal

    async def _fast(bridge, pid, *, poll_s=0.02):
        return await orig(bridge, pid, poll_s=0.02)

    monkeypatch.setattr(pipeline_cli, "_drive_until_terminal", _fast)


def _records(bridges: list) -> list:
    return [r for b in bridges for r in b.store._by_id.values()]


# ── the governed door, now a refusal ─────────────────────────────────


def test_a_bare_flow_ref_is_refused_with_both_ways_forward(inputs_file, bridges, flow_body):
    """THE change: `cairn run` refuses through the same resolver `POST /start`
    refuses through, so a terminal and the console cannot disagree about what
    is runnable."""
    r = runner.invoke(app, ["run", FLOW, "--input", str(inputs_file)])
    assert r.exit_code == 2, r.output
    out = " ".join(r.output.split())
    assert FLOW in out
    assert "$inherit" in out and f"pipeline:{FLOW}" in out  # the wrapper
    assert "--interactive" in out                                           # the dev loop
    assert _records(bridges) == [], "a refused start must leave no record"
    assert flow_body == [], "and must run nothing"


def test_a_pipeline_ref_goes_to_the_launch_door(inputs_file, bridges, monkeypatch):
    """`cairn run` is the one door (ADR-0165 D3): a pipeline starts, it is not redirected."""
    started = []
    monkeypatch.setattr(
        "cairn.cli.commands.status.launch_and_follow",
        lambda ref, inputs, **kw: started.append((ref, inputs)) or 0,
    )
    r = runner.invoke(app, ["run", PIPELINE, "--input", str(inputs_file)])
    assert r.exit_code == 0, r.output
    assert started == [(PIPELINE, FLOW_INPUTS)]
    assert _records(bridges) == []


def test_an_unknown_ref_is_one_refusal_from_the_shared_resolver(inputs_file, bridges, monkeypatch, tmp_path):
    """`resolve_start_ref` is the resolver here too, so the refusal is the same
    one `POST /start` gives. No server is configured: a named one may have the ref installed."""
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path))
    monkeypatch.delenv("CAIRN_REMOTE_ENDPOINT", raising=False)
    r = runner.invoke(app, ["run", "nope/nope", "--input", str(inputs_file)])
    assert r.exit_code != 0
    assert "no pipeline 'nope/nope' is installed" in r.output
    assert _records(bridges) == []


# ── the local dev path, kept ─────────────────────────────────────────


def test_interactive_stays_in_process_and_makes_no_pipeline(inputs_file, bridges, flow_body):
    """`--interactive` is HITL at the terminal — it cannot be a governed launch,
    so it keeps the in-process path and creates NO pipeline record."""
    r = runner.invoke(app, ["run", FLOW, "--input", str(inputs_file), "--interactive"])
    assert r.exit_code == 0, r.output
    assert _records(bridges) == [], "the dev loop must not manufacture a pipeline"
    assert flow_body, "it still RAN — in this process"


def test_mock_stays_in_process_and_makes_no_pipeline(inputs_file, bridges, flow_body):
    r = runner.invoke(app, ["run", FLOW, "--input", str(inputs_file), "--mock"])
    assert r.exit_code == 0, r.output
    assert _records(bridges) == []
    assert flow_body


def test_dry_run_starts_nothing_at_all(inputs_file, bridges):
    runner.invoke(app, ["run", FLOW, "--input", str(inputs_file), "--dry-run"])
    assert _records(bridges) == []


def test_help_says_plainly_which_flags_are_not_governed():
    """A user reading `--help` has to be able to tell the two apart without the
    ADR: the command starts a one-leg pipeline, and three named flags do not."""
    r = runner.invoke(app, ["run", "--help"])
    assert r.exit_code == 0, r.output
    plain = re.sub(r"\x1b\[[0-9;]*m", "", r.output)  # typer forces colour under GITHUB_ACTIONS
    out = " ".join(plain.split())             # rich wraps the help columns
    assert "not a governed pipeline" in out or "not governed pipelines" in out
    for flag in ("--interactive", "--mock", "--dry-run"):
        assert flag in out
    assert "IN-PROCESS" in out or "in-process" in out
