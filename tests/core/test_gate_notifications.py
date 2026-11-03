"""A pause tells a human — the subscriber a paused pipeline never had (#1791).

Every HITL pause in the 2026-08-28 flagship was found by POLLING. The delivery
layer was never missing (``cairn-operator-notify-slack`` + the ``notify.sink`` binding
+ ``NotifierRegistry``); what was missing is that nothing SUBSCRIBED the pause —
``notify_all`` fires on run RESULTS only.

These tests drive the bridge's two pause paths and its failure path directly
with fakes, the way ``test_leg_approval_gates.py`` does: what is under test is
the *hook* (does a pause fan out, once, with the authority and the release
command in it), not the transport.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from cairn.core import gate_notify
from cairn.core.approval.enforce import ApprovalRequest
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.bundle_pipeline import PipelinePlan
from cairn.core.notifier import NotifierRegistry, NotifyEvent, NotifyResult
from cairn.core.storage.pipeline_store import PipelineRecord, StepState


# ── fakes (the test_leg_approval_gates.py pattern) ───────────────────────────


class _Store:
    def __init__(self, rec: PipelineRecord) -> None:
        self.rec = rec

    async def get(self, pid: str):
        return self.rec if pid == self.rec.pipeline_id else None

    async def put(self, rec: PipelineRecord):
        self.rec = rec
        return rec


class _RunStore:
    async def update(self, run_id: str, **kw):
        return None


class _Dispatcher:
    def __init__(self) -> None:
        self.store = _RunStore()
        self.events: list[tuple] = []

    async def _emit(self, run_id: str, type_: str, data: dict, **kw):
        self.events.append((run_id, type_, data))


class _CapturingNotifier:
    """A notifier that IS event-capable — the seam the hermetic tests stub
    instead of a real Slack workspace (the owner's amendment to criterion 5)."""

    name = "capture"

    def __init__(self) -> None:
        self.events: list[NotifyEvent] = []

    async def notify(self, result):  # the RunResult path, unused here
        raise AssertionError("a pause must never travel as a RunResult")

    async def notify_event(self, event: NotifyEvent) -> NotifyResult:
        self.events.append(event)
        return NotifyResult(notifier=self.name, ok=True)


class _LegacyNotifier:
    """A notifier written before events existed. It must be SKIPPED, never fed
    a fabricated RunResult."""

    name = "legacy"

    async def notify(self, result):
        raise AssertionError("a pause must never travel as a RunResult")


class _BrokenNotifier:
    name = "broken"

    async def notify(self, result):
        raise AssertionError("unused")

    async def notify_event(self, event: NotifyEvent) -> NotifyResult:
        raise RuntimeError("webhook is on fire")


def _rec(**kw) -> PipelineRecord:
    base = dict(
        pipeline_id="pl_test",
        pipeline_name="continual-learning",
        plan=PipelinePlan(name="t"),
        inputs={},
        step_state={"promote": StepState(status="running", run_id="run-1")},
        environment="campaign",
        governance={"budget": {"usd": 12.5}},
    )
    base.update(kw)
    return PipelineRecord(**base)  # type: ignore[arg-type]


def _bridge(rec: PipelineRecord, sink: Any) -> PipelineBridge:
    b = PipelineBridge.__new__(PipelineBridge)   # no engine/registry needed
    b.store = _Store(rec)
    b.dispatcher = _Dispatcher()
    b._leg_tasks = {}
    b._locks = {}
    b._scheduler = None          # _fail_pipeline sweeps the trigger registries
    b._event_subs = {}
    b._event_waiters = {}
    b._audits: list[tuple] = []
    reg = NotifierRegistry()
    if sink is not None:
        reg.register(sink)
    b._notify_registry = reg

    async def _audit(rec_, step_id, kind, detail):
        b._audits.append((step_id, kind, detail))

    b._audit = _audit
    return b


def _req(**kw) -> ApprovalRequest:
    base = dict(operation_id="promote", operator="dataset/promote", run_id="pl_test",
                risk="external_write", sources=("policy", "profile"),
                reason="policy 'no-unreviewed-promotion' demands approval")
    base.update(kw)
    return ApprovalRequest(**base)  # type: ignore[arg-type]


def _step(step_id="promote", *, trigger="auto", ref="dataset/promote"):
    return SimpleNamespace(id=step_id, ref=ref, trigger=trigger,
                           approval=None, environment=None, show=[])


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    """Each test starts with an empty sent-set and the DEFAULT event config —
    never the developer's own profile."""
    gate_notify.reset_gate_registry()
    monkeypatch.setattr(gate_notify, "_profile_notify", lambda: {})
    yield
    gate_notify.reset_gate_registry()


# ── it fires on a pause, and says the two things that make it actionable ─────


def test_a_leg_gate_pause_notifies():
    """The whole defect: the pause happened and nobody was told."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(
        rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert len(sink.events) == 1, "a pause must reach the notifier exactly once"
    assert sink.events[0].kind == "awaiting_approval"


def test_the_message_names_the_authority_that_demanded_the_gate():
    """"needs approval" with no source is unactionable (#55). The audit entry
    already carried it; the NOTIFICATION must too, or the operator reads the
    message and still has to go and look."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(
        rec, _step(), SimpleNamespace(id="run-1"),
        _req(sources=("policy", "profile"))))

    ev = sink.events[0]
    assert ev.authority == ("policy", "profile")
    body = ev.render()
    assert "policy" in body and "profile" in body
    assert "dataset/promote" in body and "external_write" in body


def test_the_message_carries_the_exact_release_commands():
    """The #1792 next-command discipline reaching the one surface with no output
    at all. A notification whose reader must look up how to approve has failed."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(
        rec, _step(), SimpleNamespace(id="run-1"), _req()))

    body = sink.events[0].render()
    assert "cairn decide pl_test approve --step promote" in body
    assert "/pipelines/pl_test/steps/promote/approve" in body


def test_the_message_carries_cost_context_when_the_pipeline_has_any():
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(
        rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert sink.events[0].cost == {"budget_usd": 12.5}
    assert "12.5" in sink.events[0].render()


def test_no_cost_line_is_invented_when_the_pipeline_declares_none():
    """Cost context "when present" — a fabricated number on an approval message
    is worse than none."""
    rec, sink = _rec(governance={}), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(
        rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert sink.events[0].cost == {}
    assert "Cost" not in sink.events[0].render()


def test_a_manual_step_notifies_as_awaiting_advance():
    rec, sink = _rec(step_state={"promote": StepState()}), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._mark_awaiting(rec, _step(trigger="manual"), "awaiting_advance"))

    assert [e.kind for e in sink.events] == ["awaiting_advance"]
    assert sink.events[0].authority == ("pipeline plan: trigger: manual",)
    assert "cairn decide pl_test approve --step promote" in sink.events[0].render()


def test_a_declared_approval_gate_notifies_and_names_its_audience():
    rec, sink = _rec(step_state={"promote": StepState()}), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._mark_awaiting(
        rec, _step(), "awaiting_approval",
        gate_ctx={"gate": "promote", "approvers": "@ml-leads",
                  "fields": {"eval_cost_usd": 4.2, "eval_acc": 0.81}}))

    ev = sink.events[0]
    assert ev.kind == "awaiting_approval"
    assert ev.authority == ("pipeline plan: approval: true",)
    assert "@ml-leads" in ev.render()
    # the gate author's own `show:` cost field rides along; a non-cost field
    # does not (this is cost CONTEXT, not a second findings surface)
    assert ev.cost == {"budget_usd": 12.5, "eval_cost_usd": 4.2}


def test_a_clock_or_topic_wait_never_pages_a_human():
    """`awaiting_schedule` / `awaiting_event` wait on a cron and a bus topic.
    Paging for them trains the operator to ignore the channel."""
    rec, sink = _rec(step_state={"promote": StepState()}), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._mark_awaiting(rec, _step(trigger="schedule:0 * * * *"),
                                 "awaiting_schedule"))
    asyncio.run(b._mark_awaiting(rec, _step(trigger="on:dataset.ready"),
                                 "awaiting_event"))

    assert sink.events == []


# ── de-duplication: one pause = one notification, across restarts ────────────


def test_one_pause_is_announced_once_in_process():
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)
    step = _step()

    asyncio.run(b._pause_leg_for_approval(rec, step, SimpleNamespace(id="run-1"), _req()))
    # the same pause re-observed (a re-entrant advance, a duplicate bus event)
    asyncio.run(b._notify_gate(rec, "promote", "awaiting_approval",
                               operator="dataset/promote", authority=("policy",)))

    assert len(sink.events) == 1


def test_the_dedupe_marker_survives_a_bridge_restart():
    """The restart case, which is what the durable half is FOR: a new process
    has an empty in-memory sent-set, so only the marker persisted on the step —
    beside the ``awaiting_since`` its key is derived from — can stop the second
    page."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)
    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-1"), _req()))
    marker = rec.step_state["promote"].notified_key
    assert marker and rec.step_state["promote"].awaiting_since in marker

    # restart: fresh process state, fresh registry, SAME durable record
    gate_notify.reset_gate_registry()
    sink2 = _CapturingNotifier()
    b2 = _bridge(rec, sink2)
    asyncio.run(b2._notify_gate(rec, "promote", "awaiting_approval",
                                operator="dataset/promote", authority=("policy",)))

    assert sink2.events == [], "a restart must not re-page for the same pause"


def test_a_second_pause_of_the_same_step_is_a_new_notification():
    """Dedupe, not mute: released and re-gated stamps a new ``awaiting_since``,
    so the operator is told again."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)
    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-1"), _req()))
    # released, ran, gated again
    rec.step_state["promote"].status = "running"
    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-2"), _req()))

    assert len(sink.events) == 2


# ── opt-in per event type ────────────────────────────────────────────────────


def test_a_failed_pipeline_is_opt_in_and_silent_by_default():
    """A failure is already loud in the console and on the CLI; a pause is
    silent by construction. So pauses default ON, failures default OFF."""
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._fail_pipeline(rec, "step 'promote' blocked by gate: acc > 0.8"))

    assert sink.events == []


def test_a_failed_pipeline_notifies_when_the_profile_opts_in(monkeypatch):
    monkeypatch.setattr(gate_notify, "_profile_notify",
                        lambda: {"events": ["pipeline_failed"]})
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._fail_pipeline(rec, "step 'promote' blocked by gate: acc > 0.8"))

    assert [e.kind for e in sink.events] == ["pipeline_failed"]
    body = sink.events[0].render()
    assert "acc > 0.8" in body and "cairn status pl_test --results" in body


def test_the_profile_can_mute_an_event_kind(monkeypatch):
    monkeypatch.setattr(gate_notify, "_profile_notify",
                        lambda: {"events": ["awaiting_advance"]})
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert sink.events == []


def test_an_empty_event_list_mutes_everything(monkeypatch):
    monkeypatch.setattr(gate_notify, "_profile_notify", lambda: {"events": []})
    assert gate_notify.enabled_events() == frozenset()


def test_the_default_is_the_two_pause_kinds():
    assert gate_notify.enabled_events() == frozenset(
        ("awaiting_approval", "awaiting_advance"))


# ── the notifier contract: a bad destination never breaks the run ────────────


def test_a_raising_notifier_never_breaks_the_pause():
    rec = _rec()
    b = _bridge(rec, _BrokenNotifier())

    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert rec.step_state["promote"].status == "awaiting_approval"
    assert rec.status == "paused"


def test_a_notifier_without_notify_event_is_skipped_not_fed_a_fake_runresult():
    """The protocol-adaptation decision, pinned: ``notify`` takes a completed
    RunResult and a pause is not one. A notifier that predates events is simply
    not an event target — synthesising a RunResult for it would put a fabricated
    run into every digest."""
    reg = NotifierRegistry()
    reg.register(_LegacyNotifier())   # its notify() asserts if called
    reg.register(_CapturingNotifier())

    out = asyncio.run(reg.notify_event_all(
        NotifyEvent(kind="awaiting_approval", pipeline_id="pl_test", step_id="promote")))

    assert [o.notifier for o in out] == ["capture"]


def test_the_sent_marker_is_only_persisted_for_a_delivered_notification(monkeypatch):
    """A muted event must not stamp the marker — otherwise turning notifications
    ON would leave every already-paused step permanently silent."""
    monkeypatch.setattr(gate_notify, "_profile_notify", lambda: {"events": []})
    rec, sink = _rec(), _CapturingNotifier()
    b = _bridge(rec, sink)

    asyncio.run(b._pause_leg_for_approval(rec, _step(), SimpleNamespace(id="run-1"), _req()))

    assert rec.step_state["promote"].notified_key is None


# ── the config surface is closed (secrets stay env-var NAMES) ────────────────


def _profile(tmp_path, body: str):
    from cairn.core.pack.profile import load_profile

    p = tmp_path / "profile.toml"
    p.write_text(body, encoding="utf-8")
    return load_profile(p)


def test_profile_notify_parses(tmp_path):
    prof = _profile(tmp_path, """
[profile.notify]
events = ["awaiting_approval", "pipeline_failed"]
channel = "#cairn-approvals"
webhook_env = "SLACK_WEBHOOK_URL"
""")
    assert prof.notify["events"] == ["awaiting_approval", "pipeline_failed"]
    assert prof.notify["channel"] == "#cairn-approvals"


def test_a_typod_event_name_fails_at_boot(tmp_path):
    """A muted gate nobody notices is the failure mode this closes; the schema
    is closed so the typo surfaces at parse, not at 3am."""
    from cairn.core.pack.profile import ProfileError

    with pytest.raises(ProfileError, match="awaiting-approval"):
        _profile(tmp_path, '[profile.notify]\nevents = ["awaiting-approval"]\n')


def test_a_webhook_URL_in_the_profile_is_refused(tmp_path):
    """Profiles are committed. ``webhook_env`` is the NAME of an env var."""
    from cairn.core.pack.profile import ProfileError

    with pytest.raises(ProfileError, match="NAME of an env var"):
        _profile(tmp_path,
                 '[profile.notify]\nwebhook_env = "https://hooks.slack.com/services/XXX"\n')


def test_the_webhook_is_read_from_the_env_var_the_profile_names(monkeypatch):
    monkeypatch.setattr(gate_notify, "_profile_notify",
                        lambda: {"webhook_env": "MY_HOOK"})
    monkeypatch.setenv("MY_HOOK", "https://example.invalid/hook")
    assert gate_notify.notify_webhook_url() == "https://example.invalid/hook"


def test_an_unset_named_env_var_is_a_no_op_not_an_error(monkeypatch):
    monkeypatch.setattr(gate_notify, "_profile_notify",
                        lambda: {"webhook_env": "MY_HOOK"})
    monkeypatch.delenv("MY_HOOK", raising=False)
    assert gate_notify.notify_webhook_url() is None
