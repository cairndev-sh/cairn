"""Approval notifications — the subscriber a paused pipeline never had (#1791).

Every HITL pause in the 2026-08-28 flagship was discovered by POLLING. The
delivery layer was never the gap: ``cairn-operator-notify-slack`` posts, the
``notify.sink`` profile binding resolves it (ADR-0032), and ``NotifierRegistry``
fans out. What was missing is that **nothing subscribed the pause** —
``notify_all`` fires on run RESULTS only, and a pause is not a result.

This module is that subscriber, and only that:

  - **policy** — which event kinds are enabled, read from ``[profile.notify]``
    (pause kinds on by default; ``pipeline_failed`` is opt-in, because a
    pipeline that failed is already loud in the console and on the CLI, while a
    pause is silent by construction);
  - **de-duplication** — one pause = one notification, keyed on
    ``(pipeline, step, awaiting_since)`` and durable across a bridge restart;
  - **fan-out** — through the EXISTING registry, via the optional
    ``notify_event`` method (see :class:`~cairn.core.notifier.NotifyEvent` for
    why a pause does not travel as a RunResult).

The message CONTENT lives in ``NotifyEvent.render`` (core), not in any channel:
the authority that demanded the gate and the exact release command must be in
the message on every surface, and a per-channel renderer is how one of them
silently loses the half that makes it actionable.

Owner scope (2026-08-30): Slack + console + CLI. No apprise, no multi-channel
layer — this fans out to whatever ``notify.sink`` is bound, and that is the
whole delivery story.
"""
from __future__ import annotations

import logging
import os
from collections import OrderedDict
from typing import Any, Optional

from cairn.core import settings as _settings
from cairn.core.notifier import (
    GATE_EVENT_KINDS,
    NotifierRegistry,
    NotifyEvent,
    default_notifiers,
)

logger = logging.getLogger("cairn.gate_notify")

#: Enabled when the profile says nothing. The two PAUSE kinds only: a pause is
#: the silent state, and silence is the defect this closes. A failure already
#: reaches the operator through the run's own error path, so paging on it is a
#: choice the deployment makes, not one we make for it (issue criterion 2).
DEFAULT_GATE_EVENTS = ("awaiting_approval", "awaiting_advance")

#: In-process memory of what we already sent. The DURABLE half is the
#: ``StepState.notified_key`` marker the bridge persists next to
#: ``awaiting_since``; this is the same-process fast path (and the only guard
#: the pipeline-level ``pipeline_failed`` event needs, since a terminal
#: pipeline cannot re-enter ``failed`` — ``guard_pipeline_put`` refuses it).
#: Bounded so a long-lived control plane cannot grow it without limit.
_SENT: "OrderedDict[str, bool]" = OrderedDict()
_SENT_MAX = 4096

_registry: Optional[NotifierRegistry] = None


def _profile_notify() -> dict[str, Any]:
    """``[profile.notify]`` or an empty table. Fail-open, like every other
    notifier resolution: a broken profile must not wedge the pipeline."""
    try:
        from cairn.core.pack.profile import load_profile_from_env  # noqa: PLC0415 - cycle: pack.profile → notifier

        return dict(load_profile_from_env().notify or {})
    except Exception as e:  # noqa: BLE001 — no profile = defaults
        logger.debug("no [profile.notify] resolved: %s", e)
        return {}


def enabled_events() -> frozenset[str]:
    """Which event kinds this deployment wants. ``events = []`` mutes them all —
    a legitimate choice, and distinct from omitting the key."""
    cfg = _profile_notify()
    raw = cfg.get("events")
    if raw is None:
        return frozenset(DEFAULT_GATE_EVENTS)
    return frozenset(str(e) for e in raw if str(e) in GATE_EVENT_KINDS)


def notify_channel() -> str:
    """The destination override from ``[profile.notify].channel``, if any."""
    return str(_profile_notify().get("channel") or "")


def notify_webhook_url() -> Optional[str]:
    """The webhook, resolved from the env var the profile NAMES.

    ``webhook_env = "SLACK_WEBHOOK_URL"`` — the name is committed, the value
    never is. Returns None when the profile names nothing (the delivery layer
    then falls back to its own default env var), or when the named var is
    unset — an unset destination is a no-op, not an error.
    """
    name = str(_profile_notify().get("webhook_env") or "")
    return (os.environ.get(name) or None) if name else None


def default_gate_registry() -> NotifierRegistry:
    """Process-wide registry for gate events — the SAME notifier set the runner
    uses (``default_notifiers()``: stdout + inbox file + the bound
    ``notify.sink``). Built once; a deployment with no sink bound simply gets
    the local two, which is still better than silence."""
    global _registry
    if _registry is None:
        reg = NotifierRegistry()
        reg.register_many(list(default_notifiers()))
        _registry = reg
    return _registry


def reset_gate_registry() -> None:
    """Test seam — drop the cached registry and the sent-set."""
    global _registry
    _registry = None
    _SENT.clear()


def pause_dedupe_key(pipeline_id: str, step_id: str, awaiting_since: str) -> str:
    """One pause = one notification. ``awaiting_since`` is what makes a SECOND
    pause of the same step (released, re-gated) a new event rather than a
    suppressed duplicate — and it is already persisted, so the key survives a
    restart without a new table."""
    return f"{pipeline_id}:{step_id}:{awaiting_since}"


def failure_dedupe_key(pipeline_id: str, ended_at: str) -> str:
    return f"{pipeline_id}::failed:{ended_at}"


def _seen(key: str) -> bool:
    return bool(key) and key in _SENT


def _mark(key: str) -> None:
    if not key:
        return
    _SENT[key] = True
    while len(_SENT) > _SENT_MAX:
        _SENT.popitem(last=False)


def release_commands(pipeline_id: str, step_id: str, *, approver: str = "") -> tuple[str, ...]:
    """The two lines that actually release the gate — the CLI verb and the API
    call. Printed, never executed. This is the #1792 next-command discipline
    reaching the one surface that had no output at all: a notification whose
    reader must go and *look up* how to approve is a notification that failed."""
    who = f" --approver {approver}" if approver else " --approver $USER"
    base = _settings.approval_base_url() or "$CAIRN_ENDPOINT"
    body = f'{{"approver": "{approver or "you"}"}}'
    return (
        f"cairn decide {pipeline_id} approve --step {step_id}{who}",
        f"curl -sX POST {base}/pipelines/{pipeline_id}/steps/{step_id}/approve "
        f"-H 'content-type: application/json' -d '{body}'",
    )


async def emit_gate_event(
    event: NotifyEvent, *, registry: Optional[NotifierRegistry] = None,
) -> bool:
    """Fan one gate event out. Returns True when it was actually delivered, so
    the caller can persist the sent-marker only for a notification that happened.

    Never raises: a misconfigured destination logs and the pipeline carries on
    (the notifier contract, kept).
    """
    if event.kind not in enabled_events():
        logger.debug("gate event %s not enabled by [profile.notify]", event.kind)
        return False
    if _seen(event.dedupe_key):
        logger.debug("gate event %s already sent", event.dedupe_key)
        return False
    reg = registry if registry is not None else default_gate_registry()
    # #2453 (B): the profile's destination rides on the event — the notifier pack
    # reads no profile and imports no settings.
    from dataclasses import replace  # noqa: PLC0415 — stdlib, local to keep the module head unchanged

    event = replace(event, channel=event.channel or notify_channel(),
                    webhook_url=event.webhook_url or notify_webhook_url())
    try:
        outcomes = await reg.notify_event_all(event)
    except Exception:  # noqa: BLE001 — belt: notify_event_all already isolates each notifier
        logger.exception("gate notification failed for %s", event.dedupe_key)
        return False
    _mark(event.dedupe_key)
    logger.info(
        "gate notification %s → %s", event.dedupe_key,
        [f"{o.notifier}={'ok' if o.ok else o.detail}" for o in outcomes],
    )
    return True


__all__ = [
    "DEFAULT_GATE_EVENTS",
    "default_gate_registry",
    "emit_gate_event",
    "enabled_events",
    "failure_dedupe_key",
    "notify_channel",
    "notify_webhook_url",
    "pause_dedupe_key",
    "release_commands",
    "reset_gate_registry",
]
