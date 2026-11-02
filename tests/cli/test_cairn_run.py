"""ADR-0165 D3, D12 — `cairn run` is the one door that starts work, and the old names still work.

The HTTP seam is a stub: what's under test is which door each form takes, what it follows and what
it prints. The door itself is pinned in ``test_run_one_leg_pipeline.py``.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from cairn.cli.commands import pipeline as pipeline_cli
from cairn.cli.commands import remote
from cairn.cli.commands import run as run_mod
from cairn.cli.commands import status as status_cli
from cairn.cli.main import app

cli = CliRunner()
PIPELINE = "hello-cairn/profile-and-publish"
GATE = {"status": "awaiting_approval", "run_id": None}
DONE = {"status": "done", "run_id": "run-1", "approver": None}


def _rec(pipeline_id: str, status: str = "done", steps=None, **extra) -> dict:
    return {"pipeline_id": pipeline_id, "pipeline_name": pipeline_id, "status": status, "created_at": "2",
            "step_state": steps if steps is not None else {"a": DONE}, **extra}


class _Server:
    """Each id reads through its list of records and stays on the last."""
    token = "tok"

    def __init__(self, **records) -> None:
        self.records = records
        self.calls: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def __getattr__(self, name):
        def call(*args, **kw):
            self.calls.append((name, args, kw))
            return {"status": "running"}
        return call

    def launch_pipeline(self, ref, inputs, **kw):
        self.calls.append(("launch_pipeline", (ref, inputs), kw))
        return {"pipeline_id": "pl_a", "status": "running"}

    def pipeline(self, pipeline_id):
        states = self.records[pipeline_id]
        return states[0] if len(states) == 1 else states.pop(0)

    def pipelines(self, status=None):
        return {"pipelines": [{"pipeline_id": k, "created_at": "2"} for k in self.records]}


@pytest.fixture
def wire(monkeypatch, example_pack):
    def _wire(**records) -> _Server:
        c = _Server(**records)

        @contextlib.contextmanager
        def _session(endpoint, token, profile):
            yield "http://testserver", "tok", False

        monkeypatch.setattr(remote, "_session", _session)
        monkeypatch.setattr(remote, "_resolve", lambda *a: ("http://testserver", "tok"))
        monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: c)
        monkeypatch.setattr(remote.time, "sleep", lambda *_a: None)
        monkeypatch.setattr(status_cli.time, "sleep", lambda *_a: None)
        return c
    return _wire


def _names(c: _Server) -> list[str]:
    return [name for name, _a, _k in c.calls]


# ── starting a pipeline ──────────────────────────────────────────────────────────────────────

def test_a_pipeline_is_launched_with_its_inputs_and_ends_with_its_results(wire):
    c = wire(pl_a=[_rec("pl_a", outputs={"model": "m@v1", "empty": None})])
    r = cli.invoke(app, ["run", PIPELINE, "-i", "rows=3", "-i", "name=x", "--set", "rows=4", "-e", "prod", "-p", "small"])
    assert r.exit_code == 0, r.output
    _, (ref, inputs), kw = c.calls[0]
    assert (ref, inputs) == (PIPELINE, {"rows": 4, "name": "x"})
    assert kw == {"environment": "prod", "preset": "small"}
    assert "m@v1" in r.stdout and "empty" not in r.stdout
    assert "✓ done: pl_a" in r.stdout


def test_inputs_come_from_a_file_with_or_without_the_at(wire, tmp_path):
    f = tmp_path / "in.json"
    f.write_text('{"rows": 1, "name": "file"}')
    for spelled in (f"@{f}", str(f)):
        c = wire(pl_a=[_rec("pl_a")])
        r = cli.invoke(app, ["run", PIPELINE, "-i", spelled, "-i", "name=later"])
        assert r.exit_code == 0, r.output
        assert c.calls[0][1][1] == {"rows": 1, "name": "later"}


@pytest.mark.parametrize("bad", ["no-equals", "@/no/such/file.json"])
def test_a_bad_input_exits_2_before_anything_starts(wire, bad):
    c = wire(pl_a=[_rec("pl_a")])
    r = cli.invoke(app, ["run", PIPELINE, "-i", bad])
    assert r.exit_code == 2, r.output
    assert c.calls == []


def test_a_pipeline_its_event_starts_is_followed_to_the_end(wire, monkeypatch):
    plan = {"steps": [{"id": "a", "emits": "data.ready"}]}
    c = wire(pl_a=[_rec("pl_a", plan=plan, outputs={"version": "v1"})],
             pl_other=[_rec("pl_other", trigger={"correlation_id": "somebody-else"})],
             # read once to see who started it, once to find the gate, then it has been answered
             pl_b=[_rec("pl_b", "paused", {"gate": GATE}, trigger={"correlation_id": "pl_a"}),
                   _rec("pl_b", "paused", {"gate": GATE}, trigger={"correlation_id": "pl_a"}),
                   _rec("pl_b", outputs={"model": "m@v2"}, trigger={"correlation_id": "pl_a"})])
    monkeypatch.setattr(remote, "_is_interactive", lambda: True)
    monkeypatch.setattr(remote, "_prompt_decision", lambda timeout, on_timeout: "approve")
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert "pl_a started pl_b" in r.stdout and "v1" in r.stdout and "m@v2" in r.stdout
    assert "✓ done: pl_a → pl_b" in r.stdout
    assert "pl_other started" not in r.stdout
    assert [(n, a) for n, a, _k in c.calls[1:]] == [("approve_step", ("pl_b", "gate"))]


def test_a_decision_answers_the_pipeline_it_names_not_one_an_event_started(wire):
    """Nobody who typed `--decision approve` on pl_a has seen pl_b's gate."""
    c = wire(pl_a=[_rec("pl_a", "paused", {"gate": GATE}),
                   _rec("pl_a", steps={"gate": DONE}, plan={"emits": "e"}, outputs={})],
             pl_b=[_rec("pl_b", "paused", {"gate": GATE}, trigger={"correlation_id": "pl_a"})])
    r = cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "approve"])
    assert r.exit_code == 2, r.output
    assert [(n, a) for n, a, _k in c.calls] == [("approve_step", ("pl_a", "gate"))]
    assert "cairn decide pl_b approve --step gate" in r.output


def test_one_event_can_start_two_and_both_are_followed(wire):
    c = wire(pl_a=[_rec("pl_a", plan={"emits": "e"}, outputs={})],
             pl_b=[_rec("pl_b", trigger={"correlation_id": "pl_a"})])

    def _late_sibling(status=None, _rows=[]):  # noqa: B006 — the second one shows up on the next look
        _rows.append(1)
        ids = ["pl_b"] if len(_rows) == 1 else ["pl_b", "pl_c"]
        return {"pipelines": [{"pipeline_id": k, "created_at": "2"} for k in ids]}

    c.pipelines = _late_sibling
    c.records["pl_c"] = [_rec("pl_c", trigger={"correlation_id": "pl_a"})]
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert "✓ done: pl_a → pl_b → pl_c" in r.stdout


def test_a_chain_that_stops_says_what_else_is_still_going(wire):
    wire(pl_a=[_rec("pl_a", plan={"emits": "e"}, outputs={})],
         pl_b=[_rec("pl_b", "failed", trigger={"correlation_id": "pl_a"})],
         pl_c=[_rec("pl_c", trigger={"correlation_id": "pl_a"})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 1, r.output
    assert "also started, still going: pl_c" in r.stdout


def test_results_written_just_after_done_are_still_shown(wire):
    """The bridge writes `done`, then `outputs`: the read that sees `done` can come between them."""
    plan = {"outputs": {"model": "train.model"}}
    wire(pl_a=[_rec("pl_a", plan=plan), _rec("pl_a", plan=plan), _rec("pl_a", plan=plan, outputs={"model": "m@v1"})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert "m@v1" in r.stdout


def test_a_pipeline_level_emits_is_followed_too(wire):
    c = wire(pl_a=[_rec("pl_a", plan={"emits": "data.ready", "steps": []}, outputs={})],
             pl_b=[_rec("pl_b", trigger={"correlation_id": "pl_a"})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert "✓ done: pl_a → pl_b" in r.stdout
    assert _names(c) == ["launch_pipeline"]


def test_a_failed_pipeline_stops_the_chain_and_exits_1(wire):
    wire(pl_a=[_rec("pl_a", "failed", plan={"emits": "data.ready"})],
         pl_b=[_rec("pl_b", trigger={"correlation_id": "pl_a"})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 1, r.output
    assert "started pl_b" not in r.stdout


def test_an_event_nobody_listens_to_gives_up_and_still_ends_done(wire, monkeypatch):
    monkeypatch.setattr(status_cli, "_START_WAIT_S", 0.0)
    wire(pl_a=[_rec("pl_a", plan={"emits": "data.ready"}, outputs={})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert "✓ done: pl_a" in r.stdout


def test_a_gate_with_a_person_at_the_terminal_is_asked(wire, monkeypatch):
    c = wire(pl_a=[_rec("pl_a", "paused", {"gate": GATE}), _rec("pl_a", steps={"gate": DONE})])
    monkeypatch.setattr(remote, "_is_interactive", lambda: True)
    monkeypatch.setattr(remote, "_prompt_decision", lambda timeout, on_timeout: "approve")
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 0, r.output
    assert [(n, a) for n, a, _k in c.calls[1:]] == [("approve_step", ("pl_a", "gate"))]


def test_a_gate_with_no_terminal_exits_2_and_says_the_command(wire):
    c = wire(pl_a=[_rec("pl_a", "paused", {"gate": GATE})])
    r = cli.invoke(app, ["run", PIPELINE])
    assert r.exit_code == 2, r.output
    assert "cairn decide pl_a approve --step gate" in r.output
    assert _names(c) == ["launch_pipeline"]


def test_detach_starts_it_and_says_how_to_watch(wire):
    c = wire()
    r = cli.invoke(app, ["run", PIPELINE, "--detach"])
    assert r.exit_code == 0, r.output
    assert "cairn status pl_a --follow" in r.output
    assert _names(c) == ["launch_pipeline"]


def test_json_is_one_document_on_stdout_and_never_prompts(wire, monkeypatch):
    def _no_prompt(*a, **k):
        raise AssertionError("--json asked a question")

    monkeypatch.setattr(remote, "_decide", _no_prompt)
    wire(pl_a=[_rec("pl_a", plan={"emits": "e"}, outputs={"model": "m"})],
         pl_b=[_rec("pl_b", "paused", {"gate": GATE}, trigger={"correlation_id": "pl_a"})])
    r = cli.invoke(app, ["run", PIPELINE, "--json"])
    assert r.exit_code == 2, r.output
    doc = json.loads(r.stdout)
    assert [p["pipeline_id"] for p in doc["pipelines"]] == ["pl_a", "pl_b"]
    assert doc["pipelines"][0]["outputs"] == {"model": "m"}


@pytest.mark.parametrize("flags", [("--series", "s"), ("--from", "run-1"), ("--workspace", "w"), ("--mode", "sandbox")])
def test_a_flag_that_does_not_apply_to_a_pipeline_start_exits_2(wire, flags):
    c = wire()
    r = cli.invoke(app, ["run", PIPELINE, *flags])
    assert r.exit_code == 2, r.output
    assert flags[0] in r.output
    assert c.calls == []


def test_save_preset_stores_what_was_launched(wire, monkeypatch):
    saved = []
    monkeypatch.setattr(remote, "_save_preset",
                        lambda c, ref, preset, inputs, name, err=False: saved.append((ref, inputs, name, err)))
    wire(pl_a=[_rec("pl_a")])
    r = cli.invoke(app, ["run", PIPELINE, "-i", "rows=3", "--save-preset", "mine"])
    assert r.exit_code == 0, r.output
    assert saved == [(PIPELINE, {"rows": 3}, "mine", False)]


def test_json_stays_one_document_when_a_preset_is_saved(wire):
    c = wire(pl_a=[_rec("pl_a")])
    c.save_pipeline_preset = lambda ref, name, inputs: {"input_count": 1}
    r = cli.invoke(app, ["run", PIPELINE, "-i", "rows=3", "--save-preset", "mine", "--json"])
    assert r.exit_code == 0, r.output
    assert [p["pipeline_id"] for p in json.loads(r.stdout)["pipelines"]] == ["pl_a"]
    assert "saved preset" in r.stderr


def test_a_key_value_is_never_read_as_a_file_that_happens_to_have_that_name(wire, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "rows=100").write_text("{}")
    c = wire(pl_a=[_rec("pl_a")])
    r = cli.invoke(app, ["run", PIPELINE, "-i", "rows=100"])
    assert r.exit_code == 0, r.output
    assert c.calls[0][1][1] == {"rows": 100}


@pytest.mark.parametrize("flags, shown", [(("-e", "staging"), "staging  [--environment]"), ((), "  [file]")])
def test_a_pipeline_file_takes_the_environment_and_the_inputs(flags, shown, tmp_path, monkeypatch):
    from cairn import pipeline_file

    seen = {}

    original = pipeline_file.resolve_run_args

    def _spy(cls, *, env, inputs):
        seen.update(env=env, inputs=inputs)
        return original(cls, env=env, inputs=inputs)

    monkeypatch.setattr(pipeline_file, "resolve_run_args", _spy)
    f = tmp_path / "p.py"
    f.write_text("from cairn.pipeline_file import Pipeline\n\n"
                 "class P(Pipeline):\n    workflow = 'nope/nope'\n    env = 'from-file'\n")
    r = cli.invoke(app, ["run", str(f), "--dry-run", "-i", "rows=3", *flags])
    assert seen == {"env": flags[1] if flags else None, "inputs": {"rows": 3}}, r.output
    assert shown in r.output


# ── the other forms of the door ──────────────────────────────────────────────────────────────

def test_no_target_lists_what_can_run(monkeypatch):
    listed = []
    monkeypatch.setattr(pipeline_cli, "ls_cmd", lambda: listed.append(True))
    r = cli.invoke(app, ["run"])
    assert r.exit_code == 0, r.output
    assert listed and "cairn run <pipeline>" in r.stdout


def test_dry_run_of_a_pipeline_plans_it_and_starts_nothing(wire):
    c = wire()
    r = cli.invoke(app, ["run", PIPELINE, "--dry-run", "--json", "-i", "name=plain"])
    assert r.exit_code == 0, r.output
    assert json.loads(r.stdout)["ref"] == f"pipeline:{PIPELINE}"
    assert c.calls == []


def test_dry_run_of_a_flow_is_the_template_preflight(example_pack, monkeypatch):
    seen = {}

    def _dry(ref, path, check_connections=True):
        seen.update(ref=ref, checks=check_connections, inputs=json.loads(open(path, encoding="utf-8").read()))
        return 0

    monkeypatch.setattr(run_mod, "_dry_run_template", _dry)
    r = cli.invoke(app, ["run", "hello-cairn/engine-check", "--dry-run", "--no-connections", "-i", "a=1"])
    assert r.exit_code == 0, r.output
    assert seen == {"ref": "hello-cairn/engine-check", "checks": False, "inputs": {"a": 1}}


@pytest.mark.parametrize("code", [0, 1])
def test_check_exits_on_the_verdict(monkeypatch, code):
    import typer

    from cairn.cli.commands import gate as gate_cli

    seen = {}

    def _gate(**kw):
        seen.update(kw)
        raise typer.Exit(code)

    monkeypatch.setattr(gate_cli, "cmd", _gate)
    r = cli.invoke(app, ["run", "some/check", "--check", "--fail-on", "warn", "-i", "a=1"])
    assert r.exit_code == code, r.output
    assert seen["template_ref"] == "some/check" and seen["fail_on"] == "warn"


def test_skill_goes_to_the_skill_door(monkeypatch):
    seen = {}
    monkeypatch.setattr(remote, "_submit_skill", lambda skill, payload, **kw: seen.update(skill=skill, payload=payload, **kw))
    r = cli.invoke(app, ["run", "triage", "--skill", "-i", "a=1", "--detach", "--service", "api"])
    assert r.exit_code == 0, r.output
    assert seen["skill"] == "triage" and seen["payload"] == {"a": 1}
    assert seen["follow"] is False and seen["service"] == "api"


# ── the old names (ADR-0165 D12) ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("old, new", [
    (["pipeline", "run"], "cairn run"),
    (["pipeline", "explain"], "cairn run --dry-run"),
    (["remote", "launch"], "cairn run"),
    (["remote", "submit"], "cairn run --skill"),
    (["preflight"], "cairn run --dry-run"),
    (["gate"], "cairn run --check"),
])
def test_an_old_name_warns_on_stderr_and_is_hidden(old, new):
    command = get_command(app)
    for name in old:
        command = command.commands[name]
    assert command.hidden
    # any exit is fine (these have no server or no such ref): the warning comes first and on stderr
    r = cli.invoke(app, [*old, "nope/nope"], env={"CAIRN_REMOTE_ENDPOINT": "http://127.0.0.1:9"})
    assert f"is now `{new}" in " ".join(r.stderr.split()), r.output
    assert "is now" not in r.stdout


def test_a_gated_pipeline_run_stops_at_the_pause_instead_of_polling_forever():
    """#260: `cairn pipeline run` has no prompt and no server, so a person's wait ends the command."""
    reads = []

    async def get(pipeline_id):
        reads.append(pipeline_id)
        assert len(reads) < 3, "polled past a pause nobody here can answer"
        return SimpleNamespace(status="paused", step_state={"gate": SimpleNamespace(status="awaiting_approval", run_id=None)})

    bridge = SimpleNamespace(store=SimpleNamespace(get=get))
    rec = asyncio.run(pipeline_cli._drive_until_terminal(bridge, "pl_a", poll_s=0))
    assert rec.status == "paused" and reads == ["pl_a"]
