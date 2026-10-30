"""Webhook-receiver **presets** — the vendor starter kit the trigger form seeds
from (#309, ADR-0052 §7), and now also what a flow's declared ``kind: webhook``
trigger names in its ``source:`` (ADR-0101 D1, #1973).

Lifted out of ``routers/console_triggers`` unchanged: the engine-side
front-matter validator has to resolve a preset by name, and importing a FastAPI
router module to read a list of dicts would drag the whole web surface onto the
compiler's import path. ``console_triggers`` re-imports both names, so its
``/ui/api/triggers/schema`` response is byte-identical to before.

Each preset carries the vendor's ingress path, signature scheme (verified by
``router.verify_signature``) and a starter normalize mapping into the vendor's
payload shape. The ``GenericYamlProvider`` mounts + fires these with NO
per-vendor Python; secrets stay env references.

Moved here from ``cairn.core.triggers.presets`` (#2416, ADR-0120 D6): the trigger
grammar validates against these, so they live beside it, below the HTTP layer; the
old path is a deprecated alias for one release.
"""
from __future__ import annotations

from typing import Any, Optional

# Vendor presets the form seeds from — each carries the vendor's ingress path,
# signature scheme (verified by router.verify_signature), and a starter
# normalize mapping into the vendor's payload shape. The GenericYamlProvider
# mounts + fires these with NO per-vendor Python; secrets stay env references.
SIGNATURE_SCHEMES: list[dict[str, str]] = [
    {"value": "hmac_sha256", "label": "HMAC-SHA256 of body (GitHub-style)",
     "description": "bare hex or prefix=<hex> of the raw body"},
    {"value": "slack_v0", "label": "Slack v0",
     "description": "signs v0:{timestamp}:{body} with X-Slack-Request-Timestamp"},
    {"value": "pagerduty_v1", "label": "PagerDuty v1",
     "description": "one or more comma-separated v1=<hex> values"},
]

TRIGGER_PRESETS: list[dict[str, Any]] = [
    {
        "source": "slack", "label": "Slack (Events API)",
        "description": "Slack Events API webhook — mentions, messages, reactions.",
        "path": "/triggers/slack", "method": "POST",
        "signature": {"header": "X-Slack-Signature", "secret": "${env:SLACK_SIGNING_SECRET}",
                      "scheme": "slack_v0", "required": True},
        "normalize": {"event_type": "event.type", "channel": "event.channel",
                      "user": "event.user", "text": "event.text", "team": "team_id"},
        "note": "Point the Slack app's Event Subscriptions Request URL at this path; set SLACK_SIGNING_SECRET from the app's Basic Information page.",
    },
    {
        "source": "pagerduty", "label": "PagerDuty (V3 webhooks)",
        "description": "PagerDuty V3 webhook — incident lifecycle events.",
        "path": "/triggers/pagerduty", "method": "POST",
        "signature": {"header": "X-PagerDuty-Signature", "secret": "${env:PAGERDUTY_WEBHOOK_SECRET}",
                      "scheme": "pagerduty_v1", "required": True},
        "normalize": {"event_type": "event.event_type", "severity": "event.data.priority.summary",
                      "service": "event.data.service.summary", "title": "event.data.title",
                      "incident_id": "event.data.id"},
        "note": "Create a V3 webhook subscription in PagerDuty; the signing secret is shown once at creation.",
    },
    {
        "source": "gmail", "label": "Gmail (Pub/Sub push)",
        "description": "Gmail watch notifications delivered via Google Cloud Pub/Sub push.",
        "path": "/triggers/gmail", "method": "POST",
        # Google push auth is an OIDC bearer token, not an HMAC header — verify at
        # the network layer / a future google_oidc scheme. Signature optional here.
        "signature": {"header": "Authorization", "secret": "${env:GMAIL_PUSH_TOKEN}",
                      "scheme": "hmac_sha256", "required": False},
        "normalize": {"message_id": "message.messageId", "data": "message.data",
                      "subscription": "subscription", "published_at": "message.publishTime"},
        "note": "Gmail push = users.watch → a Pub/Sub topic with a push subscription at this path. message.data is base64-encoded JSON ({emailAddress, historyId}); Google authenticates pushes with an OIDC token (not HMAC) — validate it upstream or leave required=false behind a private network.",
    },
    {
        "source": "github", "label": "GitHub (webhooks)",
        "description": "GitHub repository/org webhooks — pushes, PRs, issues, releases.",
        "path": "/triggers/github", "method": "POST",
        "signature": {"header": "X-Hub-Signature-256", "secret": "${env:GITHUB_WEBHOOK_SECRET}",
                      "scheme": "hmac_sha256", "required": True},
        "normalize": {"event_type": "action", "repo": "repository.full_name",
                      "sender": "sender.login", "title": "pull_request.title"},
        "note": "GitHub sends sha256=<hex> in X-Hub-Signature-256 — the generic HMAC scheme verifies it as-is.",
    },
    {
        "source": "custom", "label": "Custom webhook",
        "description": "Any JSON webhook — bring your own path + normalize mapping.",
        "path": "/triggers/my-source", "method": "POST",
        "signature": {"header": "X-Signature", "secret": "${env:WEBHOOK_SECRET}",
                      "scheme": "hmac_sha256", "required": False},
        "normalize": {"event_type": "type"},
        "note": "The generic provider verifies an optional HMAC and normalizes via JSONPath-lite — no code needed.",
    },
]


# The earlier private names (#297); the same list objects.
_SIGNATURE_SCHEME_OPTS = SIGNATURE_SCHEMES
_TRIGGER_PRESETS = TRIGGER_PRESETS

#: Every preset's ``source`` — what a declared webhook trigger may name.
PRESET_SOURCES: tuple[str, ...] = tuple(p["source"] for p in TRIGGER_PRESETS)


def preset_for(source: str) -> Optional[dict[str, Any]]:
    """The preset whose ``source`` is *source*, or ``None`` when unknown.

    The ONE lookup: the console form picks a preset by this key and so does the
    declared-trigger validator, so "what is a known receiver" cannot drift
    between the two surfaces.
    """
    key = (source or "").strip()
    return next((p for p in TRIGGER_PRESETS if p["source"] == key), None)


__all__ = [
    "PRESET_SOURCES", "SIGNATURE_SCHEMES", "TRIGGER_PRESETS", "preset_for",
    "_SIGNATURE_SCHEME_OPTS", "_TRIGGER_PRESETS",
]
