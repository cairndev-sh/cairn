"""ADR-0165 D4, D5, D12 — one door to look, one door to decide, and the old names still work.

The HTTP seam is a stub: what's under test is which client call each form makes and what it prints.
"""
from __future__ import annotations

import contextlib
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import remote
from cairn.cli.main import app

cli = CliRunner()

GATE = {"status": "awaiting_approval", "run_id": None}
DONE = {"status": "done", "run_id": "run-1", "approver": None}


def _record(status="paused", steps=None, **extra) -> dict:
    return {"pipeline_id": "pl_a", "pipeline_name": "prep", "status": status,
            "step_state": steps if steps is not None else {"measure": DONE, "review": GATE}, **extra}


class _Client:
    token = "tok"

    def __init__(self, records: list[dict] | None = None) -> None:
        self.records = records or [_record()]
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

    def pipeline(self, pipeline_id):
        return self.records[0] if len(self.records) == 1 else self.records.pop(0)

    def pipelines(self, status=None):
        return {"pipelines": [
            {"pipeline_id": "pl_old", "name": "prep", "status": "done", "steps": {"a": "done"}, "created_at": "1"},
            {"pipeline_id": "pl_new", "name": "sft", "status": "paused", "created_at": "2",
             "steps": {"a": "done", "review": "awaiting_approval"}}]}

    def runs(self, status=None, limit=50):
        return [{"id": "run-loose", "status": "done", "template_ref": "skill:x"},
                {"id": "run-leg", "status": "done", "bundle_pipeline_id": "pl_old"}]

    def run(self, run_id):
        return {"id": run_id, "status": "failed", "template_ref": "task:x", "error": "it broke",
                "cost": {"total_usd": 1.5}}

    def run_events(self, run_id, after_seq=0):
        self.calls.append(("run_events", (run_id,), {"after_seq": after_seq}))
        return {"events": [{"seq": 1, "type": "running", "data": "{}"}, {"seq": 2, "type": "node", "data": "{}"}]}


@pytest.fixture
def wire(monkeypatch):
    def _wire(records=None) -> _Client:
        c = _Client(records)

        @contextlib.contextmanager
        def _session(endpoint, token, profile):
            yield "http://testserver", "tok", False

        monkeypatch.setattr(remote, "_session", _session)
        monkeypatch.setattr(remote, "_resolve", lambda *a: ("http://testserver", "tok"))
        monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: c)
        monkeypatch.setattr(remote.time, "sleep", lambda *_a: None)
        return c
    return _wire


def _names(c: _Client) -> list[str]:
    return [name for name, _a, _k in c.calls]


# ── cairn status ──────────────────────────────────────────────────────────────


def test_status_lists_pipelines_newest_first_and_only_the_runs_with_no_pipeline(wire):
    wire()
    out = cli.invoke(app, ["status"]).stdout
    assert out.index("RUNNING") < out.index("pl_new") < out.index("FINISHED") < out.index("pl_old")
    assert "waiting: review" in out
    assert "run-loose" in out and "run-leg" not in out


def test_status_of_a_pipeline_says_who_started_it_and_the_next_command(wire):
    wire([_record(trigger={"source": "event", "topic": "dataset.prepared", "publisher": "pipeline:pl_prep"},
                  steps={"measure": DONE, "review": GATE})])
    out = cli.invoke(app, ["status", "pl_a"]).stdout
    assert "Started by  pl_prep  (dataset.prepared)" in out
    assert "✓ measure" in out and "⏸ review" in out
    assert "cairn decide pl_a approve --step review" in out


def test_status_of_a_pipeline_shows_a_waiting_gates_evidence(wire):
    gate = {**GATE, "gate_context": {"fields": {"measure.score": 0.97}, "evidence": {"cost_usd": 1.5}}}
    wire([_record(steps={"measure": DONE, "review": gate})])
    out = cli.invoke(app, ["status", "pl_a"]).stdout
    assert "measure.score" in out and "$1.50" in out


def test_status_of_a_run_shows_its_error_and_cost(wire):
    wire()
    out = cli.invoke(app, ["status", "run-1"]).stdout
    assert "it broke" in out and "$1.50" in out


def test_status_logs_of_a_pipeline_reads_each_leg_and_events_only_filters(wire):
    c = wire()
    all_ = cli.invoke(app, ["status", "pl_a", "--logs"]).stdout
    assert ("run_events", ("run-1",), {"after_seq": 0}) in c.calls
    assert "node" in all_
    assert "node" not in cli.invoke(app, ["status", "pl_a", "--logs", "--events-only"]).stdout


@pytest.mark.parametrize("flag", ["--results", "--logs", "--follow", "--replay"])
def test_status_refuses_a_flag_that_needs_an_id(wire, flag):
    wire()
    r = cli.invoke(app, ["status", flag])
    assert r.exit_code != 0 and "need an id" in r.output


def test_following_a_pipeline_answers_a_gate_and_ends_with_its_status(wire):
    c = wire([_record(), _record(status="done", steps={"measure": DONE, "review": DONE})])
    r = cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "approve"])
    assert r.exit_code == 0, r.output
    assert _names(c) == ["approve_step"]


def test_following_with_no_terminal_and_no_decision_exits_2_and_says_the_command(wire):
    c = wire()
    r = cli.invoke(app, ["status", "pl_a", "--follow"])
    assert r.exit_code == 2, r.output
    assert "cairn decide pl_a approve --step review" in r.output and _names(c) == []


def test_following_a_failed_pipeline_says_why_its_leg_failed(wire):
    """The reason is the run's own error, in the output of the command that followed it."""
    wire([_record(status="failed", steps={"clean": {"status": "failed", "run_id": "run-1"}, "next": {"status": "pending"}},
                  step_runs={"clean": {"error": "this run's estimate $5.00 would exceed the $1.00 cap"}})])
    r = cli.invoke(app, ["status", "pl_a", "--follow"])
    assert r.exit_code == 1
    assert "clean: this run's estimate $5.00 would exceed the $1.00 cap" in r.output


def test_following_a_failed_pipeline_exits_1(wire):
    wire([_record(status="failed", steps={"measure": {"status": "failed", "run_id": "run-1"}})])
    assert cli.invoke(app, ["status", "pl_a", "--follow"]).exit_code == 1


def test_a_reject_stops_answering_the_other_waiting_gates(wire):
    c = wire([_record(steps={"a": GATE, "b": GATE}),
              _record(status="failed", steps={"a": {"status": "skipped"}, "b": {"status": "pending"}})])
    r = cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "reject"])
    assert r.exit_code == 1, r.output
    assert _names(c) == ["reject_step"], "the first reject ends the pipeline; the second gate is not answered"


def test_a_decision_waits_before_the_next_read(wire, monkeypatch):
    c = wire([_record(), _record(status="done", steps={"review": DONE})])
    naps: list = []
    monkeypatch.setattr(remote.time, "sleep", naps.append)
    assert cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "approve"]).exit_code == 0
    assert naps == [2.0] and _names(c) == ["approve_step"]


def test_a_wait_on_a_schedule_asks_nobody_and_says_so_once(wire):
    waiting = _record(steps={"tick": {"status": "awaiting_schedule"}})
    c = wire([waiting, waiting, _record(status="done", steps={"tick": DONE})])
    r = cli.invoke(app, ["status", "pl_a", "--follow"])
    assert r.exit_code == 0, r.output
    assert r.output.count("not a person") == 1 and _names(c) == []


def test_a_slow_bridge_does_not_get_the_same_pause_answered_twice(wire):
    c = wire([_record(), _record(), _record(status="done", steps={"review": DONE})])
    assert cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "approve"]).exit_code == 0
    assert _names(c) == ["approve_step"]


def test_a_new_pause_of_the_same_step_is_answered_again(wire):
    first, second = {**GATE, "awaiting_since": "t1"}, {**GATE, "awaiting_since": "t2"}
    c = wire([_record(steps={"review": first}), _record(steps={"review": second}),
              _record(status="done", steps={"review": DONE})])
    assert cli.invoke(app, ["status", "pl_a", "--follow", "--decision", "approve"]).exit_code == 0
    assert _names(c) == ["approve_step", "approve_step"]


# ── cairn decide ──────────────────────────────────────────────────────────────


def test_decide_approves_the_only_waiting_gate(wire):
    c = wire()
    r = cli.invoke(app, ["decide", "pl_a", "approve", "--approver", "vibhu"])
    assert r.exit_code == 0, r.output
    assert ("approve_step", ("pl_a", "review"), {"approver": "vibhu"}) in c.calls


def test_decide_releases_a_manual_wait_through_advance(wire):
    c = wire([_record(steps={"go": {"status": "awaiting_advance"}})])
    assert cli.invoke(app, ["decide", "pl_a", "approve"]).exit_code == 0
    assert "advance_step" in _names(c) and "approve_step" not in _names(c)


def test_decide_refuses_two_waiting_gates_without_a_step(wire):
    c = wire([_record(steps={"a": GATE, "b": GATE})])
    r = cli.invoke(app, ["decide", "pl_a", "approve"])
    assert r.exit_code != 0 and "--step" in r.output and "a, b" in r.output
    assert _names(c) == []


def test_decide_refuses_a_pipeline_nobody_is_waiting_on(wire):
    c = wire([_record(steps={"a": DONE})])
    r = cli.invoke(app, ["decide", "pl_a", "approve"])
    assert r.exit_code != 0 and "not waiting" in r.output and _names(c) == []


def test_rejecting_a_pipeline_gate_needs_a_reason(wire):
    c = wire()
    r = cli.invoke(app, ["decide", "pl_a", "reject"])
    assert r.exit_code != 0 and "--reason" in r.output and _names(c) == []
    assert cli.invoke(app, ["decide", "pl_a", "reject", "--reason", "wrong source"]).exit_code == 0
    assert c.calls[-1][0] == "reject_step" and c.calls[-1][2]["reason"] == "wrong source"


@pytest.mark.parametrize("target, answer, call", [
    ("pl_a", "cancel", "cancel_pipeline"),
    ("run-1", "cancel", "cancel_run"),
    ("run-1", "approve", "resume_run"),
    ("run-1", "reject", "resume_run"),
])
def test_the_id_picks_the_route(wire, target, answer, call):
    c = wire()
    assert cli.invoke(app, ["decide", target, answer]).exit_code == 0
    assert _names(c) == [call]


def test_a_run_rejection_carries_its_reason_as_the_note(wire):
    c = wire()
    assert cli.invoke(app, ["decide", "run-1", "reject", "--reason", "no"]).exit_code == 0
    assert c.calls[0][2] == {"decision": "reject", "approver": "cli-user", "note": "no"}


def test_decide_refuses_an_unknown_answer_and_a_step_on_a_run(wire):
    c = wire()
    assert "approve, reject, cancel" in cli.invoke(app, ["decide", "pl_a", "yes"]).output
    assert "--step" in cli.invoke(app, ["decide", "run-1", "approve", "--step", "x"]).output
    assert _names(c) == []


def test_a_reason_on_a_pipeline_approval_is_refused_not_dropped(wire):
    c = wire()
    r = cli.invoke(app, ["decide", "pl_a", "approve", "--reason", "looks fine"])
    assert r.exit_code != 0 and "--reason goes with reject" in r.output and _names(c) == []


@pytest.mark.parametrize("command", ["status", "decide"])
def test_dash_e_is_not_the_endpoint_on_the_new_commands(command):
    """ADR-0165 D9: `-e` is always the environment; the endpoint is long form only."""
    from typer.main import get_command

    params = get_command(app).commands[command].params
    opts = {o for p in params for o in p.opts}
    assert "--endpoint" in opts and "--token" in opts and "--profile" in opts
    assert "-e" not in opts


# ── the old names (ADR-0165 D12) ──────────────────────────────────────────────


@pytest.mark.parametrize("argv, new", [
    (["remote", "pipelines"], "cairn status"),
    (["remote", "pipeline", "pl_a"], "cairn status <id> --json"),
    (["remote", "ps"], "cairn status"),
    (["remote", "watch", "--once"], "cairn status --watch"),
    (["remote", "status", "run-1"], "cairn status <run>"),
    (["remote", "logs", "run-1"], "cairn status <run> --logs"),
    (["remote", "findings", "pl_a"], "cairn status <id> --results"),
    (["remote", "advance", "pl_a", "review"], "cairn decide <id> approve --step <step>"),
    (["remote", "approve", "run-1"], "cairn decide <run> approve"),
    (["remote", "reject", "run-1"], "cairn decide <id> reject"),
    (["remote", "cancel", "run-1"], "cairn decide <run> cancel"),
    (["remote", "cancel-pipeline", "pl_a"], "cairn decide <id> cancel"),
])
def test_an_old_name_still_works_and_says_the_new_one_on_stderr(wire, argv, new):
    wire()
    r = cli.invoke(app, argv)
    assert r.exit_code == 0, r.output
    assert f"is now `{new}`" in r.stderr
    assert "is now" not in r.stdout, "stdout stays what a script reads"


def test_replay_takes_a_run_not_a_pipeline():
    r = cli.invoke(app, ["status", "pl_a", "--replay"])
    assert r.exit_code == 1 and "cairn status pl_a --logs" in r.stderr


def test_replay_still_works_as_an_alias(wire):
    r = cli.invoke(app, ["replay", "no-such-run", "--source", "file"])
    assert "is now `cairn status <run> --replay`" in r.stderr


@pytest.mark.parametrize("name", ["pipelines", "ps", "findings", "advance", "cancel-pipeline"])
def test_an_old_name_leaves_help(name):
    assert f" {name} " not in cli.invoke(app, ["remote", "--help"]).stdout


def test_the_new_commands_are_in_help():
    out = cli.invoke(app, ["--help"]).stdout
    assert " status " in out and " decide " in out and " replay " not in out
