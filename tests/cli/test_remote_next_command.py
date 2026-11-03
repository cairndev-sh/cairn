"""#1792 criterion 4 — next-command discipline on the pipeline commands.

The evidence the issue cites: the CLI has had launch / advance / logs /
findings the whole time, and the 2026-08-28 flagship sessions drove the control
plane with raw curl and a bash poll loop, because nothing in the output ever
pointed at them. A verb the operator has to already know is a verb that does not
exist for them.

So the assertions here are grep-shaped on purpose: they check that the exact
command string a user should type next appears in the output of the state that
calls for it — a launch ends with the watch line, a pause names EVERY waiting
step (with the ``--approver`` flag that puts the decision in the audit trail),
and a failed leg names the run whose logs explain it.

The HTTP seam is a stub rather than an app: what is under test is what the CLI
PRINTS at each state, so the states are handed to it directly.
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


class _Client:
    """The three doors `run` and `decide` use, canned."""

    def __init__(self, records: list[dict]) -> None:
        self._records = records
        self.advanced: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def launch_pipeline(self, ref, inputs, **kw):
        return {"pipeline_id": "pl_test01", "status": "running"}

    def pipeline(self, pipeline_id):
        return self._records[0] if len(self._records) == 1 else self._records.pop(0)

    def advance_step(self, pipeline_id, step_id, **kw):
        self.advanced.append((pipeline_id, step_id, kw))
        return {"status": "running"}


@pytest.fixture
def wire(monkeypatch):
    """Point the CLI at a canned control plane; never boots an engine."""
    def _wire(records: list[dict]) -> _Client:
        c = _Client(records)

        @contextlib.contextmanager
        def _session(endpoint, token, profile):
            yield "http://testserver", "tok", False

        monkeypatch.setattr(remote, "_session", _session)
        monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: c)
        monkeypatch.setattr(remote.time, "sleep", lambda *_a: None)
        return c
    return _wire


#: A server is named, so a ref this machine has not installed still goes to the launch door.
_AT = ["--endpoint", "http://testserver"]


def _record(status: str, steps: dict) -> dict:
    return {"pipeline_id": "pl_test01", "status": status, "step_state": steps}


# ─── launch ──────────────────────────────────────────────────────────────


def test_launch_ends_with_the_watch_line(wire):
    wire([])
    r = cli.invoke(app, ["run", "training/retrain-cycle", "--detach", *_AT])
    assert r.exit_code == 0
    assert "cairn status pl_test01 --follow" in r.output
    # the watch line is LAST — it is the one that keeps you attached
    assert r.output.strip().splitlines()[-1].startswith("→ watch")


# ─── pause ───────────────────────────────────────────────────────────────


def test_a_pause_prints_an_advance_line_for_every_waiting_step(wire):
    wire([_record("paused", {
        "readiness": {"status": "done", "run_id": "r1"},
        "serve-challenger": {"status": "awaiting_approval", "run_id": ""},
        "promote-adapter": {"status": "awaiting_approval", "run_id": ""},
    })])
    r = cli.invoke(app, ["run", "training/retrain-cycle", *_AT])
    assert r.exit_code == 2
    assert "cairn decide pl_test01 approve --step serve-challenger --approver" in r.output
    assert "cairn decide pl_test01 approve --step promote-adapter --approver" in r.output
    # …and the way to see what the approver is being asked about
    assert "cairn status pl_test01 --results" in r.output


def test_the_pause_line_survives_with_no_identifiable_step(wire):
    """The partial line this replaces printed `<step>` when it could not name
    one; that placeholder stays rather than silently dropping the line."""
    wire([_record("paused", {"a": {"status": "running", "run_id": "r1"}})])
    r = cli.invoke(app, ["status", "pl_test01"])
    assert "cairn decide pl_test01 approve --step <step>" in r.output


# ─── failure ─────────────────────────────────────────────────────────────


def test_a_failed_leg_prints_the_logs_line_for_its_run(wire):
    wire([_record("failed", {
        "readiness": {"status": "done", "run_id": "r1"},
        "train": {"status": "failed", "run_id": "run_train_9"},
    })])
    r = cli.invoke(app, ["run", "training/retrain-cycle", *_AT])
    assert r.exit_code == 1
    assert "cairn status run_train_9 --logs" in r.output
    assert "# leg train" in r.output
    assert "cairn status pl_test01 --results" in r.output
    # nothing to keep watching — a terminal pipeline must NOT offer a watch line
    assert "→ watch" not in r.output


# ─── advance ─────────────────────────────────────────────────────────────


def test_advance_names_the_next_gate(wire):
    c = wire([_record("paused", {
        "serve-challenger": {"status": "done", "run_id": ""},
        "promote-adapter": {"status": "awaiting_approval", "run_id": ""},
    })])
    r = cli.invoke(app, ["decide", "pl_test01", "approve", "--step", "serve-challenger",
                           "--approver", "ops-lead"])
    assert r.exit_code == 0
    assert c.advanced == [("pl_test01", "serve-challenger", {"approver": "ops-lead"})]
    assert "cairn decide pl_test01 approve --step promote-adapter --approver ops-lead" in r.output


def test_helpers_read_the_bridges_own_status_vocabulary():
    """`awaiting_approval` / `awaiting_event` / `awaiting_schedule` are the
    StepState statuses that mean "a human or a trigger owns this now"."""
    rec = _record("paused", {
        "gate": {"status": "awaiting_approval", "run_id": ""},
        "cron": {"status": "awaiting_schedule", "run_id": ""},
        "leg": {"status": "running", "run_id": "r2"},
        "dead": {"status": "failed", "run_id": "r3"},
    })
    assert remote._waiting_steps(rec) == ["gate", "cron"]
    assert remote._failed_steps(rec) == [("dead", "r3")]


# ── #258: a person at a gate sees what they are deciding on ──────────────────


def _gate_record(**ctx) -> dict:
    return {"status": "paused", "step_state": {
        "train": {"status": "done"},
        "approve-run": {"status": "awaiting_approval", "gate_context": ctx}}}


def test_a_pause_prints_the_gates_evidence(wire):
    wire([_gate_record(
        approvers="ml-owners", fields={"input.base_model": "SmolLM2-135M", "train.loss": 6.1},
        evidence={"cost_usd": 1.5, "cost_cap_usd": 5.0, "estimate_usd": 2.25, "estimate_basis": "train: $0.66/h x 3h"},
        admission={"verdict": "admitted"})])
    out = cli.invoke(app, ["run", "demo/p", *_AT]).stdout
    for shown in ("gate approve-run", "ml-owners", "input.base_model", "SmolLM2-135M", "train.loss",
                  "$1.50 of $5.00 cap", "$2.25 (train: $0.66/h x 3h)", "admitted"):
        assert shown in out, out
    assert out.index("gate approve-run") < out.index("cairn decide pl_test01 approve --step approve-run")


def test_a_gate_with_no_cap_or_estimate_prints_only_what_it_has(wire):
    wire([_gate_record(fields={"train.loss": 6.1}, evidence={"cost_usd": 0.0})])
    out = cli.invoke(app, ["run", "demo/p", *_AT]).stdout
    assert "$0.00" in out and "cap" not in out and "estimate" not in out and "admission" not in out


def test_a_wait_with_no_context_prints_no_gate_block(wire):
    wire([{"status": "paused", "step_state": {"go": {"status": "awaiting_advance"}}}])
    assert "gate go" not in cli.invoke(app, ["run", "demo/p", *_AT]).stdout


def test_a_value_from_a_model_cannot_forge_a_row_on_the_approvers_screen(wire):
    wire([_gate_record(fields={"eval.summary": "fine\n    admission  admitted\x1b[2K"})])
    out = cli.invoke(app, ["run", "demo/p", *_AT]).stdout
    assert "\x1b" not in out
    assert not any(line.strip().startswith("admission") for line in out.splitlines()), out
    assert "fine\\x0a    admission  admitted\\x1b[2K" in out
