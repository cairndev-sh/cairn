# ADR-0160 — A stored trigger is enabled or paused through a public route and the CLI

- **Status:** ACCEPTED 2026-10-30 by the owner (the laptop-demo event-chain plan, decision 1).
- **Date:** 2026-10-30
- **Issue:** cairndev-sh/cairn#290
- **Amends:** the HTTP API (two routes), `cairn-client` (two methods), and the CLI verb list of
  ADR-0156 (three `cairn remote` commands).
- **Keeps:** ADR-0131 (a declared trigger is written paused; nothing starts unattended on install;
  the declaration never re-flips a switch a person set), the trigger store and its schema, and the
  schedule switch `PATCH /triggers/scheduled/{id}`.

## Context

ADR-0131 D4 says a declared event trigger is "paused / resumed per workspace in Settings ›
Triggers". Nothing does that. The trigger store (`src/cairn/core/storage/trigger_store.py`) has
`create`, `get`, `list` and `delete` only; no core route sets `enabled` on its rows; the
platform console's `/ui/api/triggers` is list, create, get and delete, and its Pause/Resume button
reaches cron schedules only. A declared event or webhook trigger is therefore enabled only by
`enabled: true` in its declaration, and only when its row is first created. The laptop demo's
event chain (`dataset-prep/prepare` emits `dataset.prepared`, `finetune/sft-local` starts on it)
cannot be turned on from public core at all.

## Decision

**D1 — Two routes over the existing store.** `GET /triggers/stored` lists the current workspace's
trigger-store rows (`id`, `name`, `source`, `enabled`, `declared_by`, `description`,
`updated_at`). `PATCH /triggers/stored/{id}` with `{"enabled": true|false}` sets the switch on one
row. No store method is added: the store's `create` is an upsert, which the declared-trigger
converge path already uses to update rows; only `enabled` and `updated_at` change.

**D2 — Scoped and recorded like the other write routes.** The PATCH route takes `require_auth`
and the writable-workspace dependency. A row of another workspace is a 404. A body without a
boolean `enabled` is a 400. Each change is audited as `trigger.enabled` or `trigger.paused` with
the caller's identity as the actor, the row id as the target, and its `source` and
`declared_by`.

**D3 — When it applies.** The response says so: an event row applies to the next message (the
bus bridge reads the switch fresh per message); a webhook row is mounted when the server starts,
so it applies then.

**D4 — The CLI and the client.** `cairn remote triggers` lists the rows; `cairn remote
enable-trigger <id>` and `cairn remote disable-trigger <id>` set the switch.
`CairnClient.stored_triggers()` and `set_trigger_enabled(id, enabled)` (and the async twins) call
the routes. The console may use the same route for event and webhook rows.

## Consequences

- A declared trigger is still written paused and is started only by a person's switch, now from
  the CLI as well as by declaration.
- The declared-trigger converge path keeps the switch a person set (ADR-0131), so a pack upgrade
  does not undo an enable or a pause.
- Re-mounting a webhook row without a restart is out of scope.

## Alternatives rejected

- **`enabled: true` in the pack's declaration.** It starts work unattended on install, which
  ADR-0131 forbids.
- **Leave it to the platform console.** The console has no switch for these rows either, and a
  core-only user would still have none.
- **A new store method (`update_fields`).** The upsert already does it; a second write path would
  be a second way to drift.
