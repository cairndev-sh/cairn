"""The declared triggers of the hello-cairn example, armed and fired (ADR-0131, hello-cairn WP7).

Every trigger the example declares ships ``enabled: false``. A user turns one on by setting
``enabled: true`` on the declaration (and, for a schedule, the cron they want). Each test
does exactly that on a COPY of the pack, starts a real ``cairn-server`` on it, and checks
that the trigger started the declaring pipeline through the launch door, with the start
source on the record:

* ``trigger.webhook`` — ``POST /triggers/hello-sample`` starts check-and-publish;
* ``trigger.schedule`` — the Monday cron, set to every minute, starts check-and-publish;
* ``event.emit`` / ``event.on`` — check-and-publish's ``publish`` leg emits
  ``dataset.ready`` and ask-the-docs' ``kind: event`` trigger starts ask-the-docs on it;
* ``trigger.watch`` — a run's ``engine-check`` leg registers the sample as a version of
  ``hello-sample`` (#2632), and the every-minute watch then starts check-and-publish.

One cell (SQLite, in-process runtime and bus): what is proved here is the trigger path, not
the store or bus behind it — those are the matrix's cells.
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path

import pytest

from tests.e2e.conftest import HELLO, Api, cell_params
from tests.e2e.hello._receipts import record

pytestmark = pytest.mark.e2e

CELL = cell_params()[0]          # sqlite-inprocess-inprocess, no services
_CID = CELL.values[0].id


#: The copy's pack name. Installed packs win every name collision with a dev pack
#: (``cairn/core/pack/loader.py``), and the example is installed (the ``hello`` extra), so
#: an armed copy under the same name would be shadowed and its triggers never seen.
ARMED = "hello-armed"


def _pack_with(tmp: Path, edits: dict[str, list[tuple[str, str]]]) -> str:
    """A copy of the example pack, renamed :data:`ARMED`, with each file's (old, new) text
    replacements applied. Every replacement must match exactly once: a drifted example
    fails here, loudly. Returns the CAIRN_PACK_PATH entry (the dir CONTAINING the pack)."""
    dst = tmp / "packs" / ARMED
    shutil.copytree(HELLO, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    edits = {"pack.toml": [('name        = "hello-cairn"', f'name        = "{ARMED}"')], **edits}
    for rel, subs in edits.items():
        p = dst / rel
        text = p.read_text(encoding="utf-8")
        for old, new in subs:
            assert text.count(old) == 1, f"{rel}: expected one {old!r}"
            text = text.replace(old, new)
        p.write_text(text, encoding="utf-8")
    return str(dst.parent)


def _pipelines(api: Api, name: str) -> list[dict]:
    out = api._ok(api.c.get("/pipelines"))
    return [p for p in out["pipelines"] if p.get("name") == name]


def _wait_started(api: Api, name: str, source: str, *, timeout: float, seen: frozenset = frozenset()) -> dict:
    """The first pipeline ``name`` not in ``seen`` whose record names ``source`` as its start."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for row in _pipelines(api, name):
            if row["pipeline_id"] in seen:
                continue
            rec = api.pipeline(row["pipeline_id"])
            if (rec.get("trigger") or {}).get("source") == source and str(rec.get("bundle_ref", "")).find(ARMED) >= 0:
                return rec
        time.sleep(1.0)
    found = [(p["pipeline_id"], (api.pipeline(p["pipeline_id"]).get("trigger") or {}).get("source"))
             for p in _pipelines(api, name)]
    raise AssertionError(f"no {name} pipeline started by {source!r} within {timeout:.0f}s; found {found}")


PNP = "pipelines/check-and-publish.yaml"
WEBHOOK_ON = (
    "    path: /triggers/hello-sample\n    enabled: false\n",
    "    path: /triggers/hello-sample\n    enabled: true\n",
)
SCHEDULE_EVERY_MINUTE = (
    '    cron: "0 6 * * 1"\n    enabled: false\n',
    '    cron: "* * * * *"\n    enabled: true\n',
)
EVENT_ON = ("    topic: dataset.ready\n    enabled: false\n", "    topic: dataset.ready\n    enabled: true\n")


@pytest.mark.parametrize("cell", [CELL])
def test_a_webhook_starts_its_pipeline(cell, server_factory, tmp_path) -> None:
    pack = _pack_with(tmp_path, {PNP: [WEBHOOK_ON]})
    srv = server_factory(cell, "webhook", pack_path=pack)
    api = srv.api()
    r = api.c.post("/triggers/hello-sample", json={"note": "feature proof"})
    assert r.status_code < 400, (r.status_code, r.text[:300])
    rec = _wait_started(api, "check-and-publish", "webhook", timeout=60)
    record("trigger.webhook", cell.id, "check-and-publish",
           f"POST /triggers/hello-sample → pipeline {rec['pipeline_id']} (trigger.source='webhook')")


@pytest.mark.parametrize("cell", [CELL])
def test_a_schedule_fires_its_pipeline(cell, server_factory, tmp_path) -> None:
    pack = _pack_with(tmp_path, {PNP: [SCHEDULE_EVERY_MINUTE]})
    srv = server_factory(cell, "schedule", pack_path=pack)
    api = srv.api()
    rec = _wait_started(api, "check-and-publish", "schedule", timeout=150)
    cron = (rec.get("trigger") or {}).get("cron")
    assert cron == "* * * * *", rec.get("trigger")
    record("trigger.schedule", cell.id, "check-and-publish",
           f"cron {cron!r} fired pipeline {rec['pipeline_id']} (trigger.source='schedule')")


@pytest.mark.parametrize("cell", [CELL])
def test_a_published_event_starts_ask_the_docs(cell, server_factory, tmp_path) -> None:
    pack = _pack_with(tmp_path, {"pipelines/ask-the-docs.yaml": [EVENT_ON]})
    srv = server_factory(cell, "event", pack_path=pack)
    api = srv.api()
    before = frozenset(p["pipeline_id"] for p in _pipelines(api, "ask-the-docs"))
    pid = api.start(f"{ARMED}/check-and-publish")
    for gate in ("review", "promote"):
        api.wait_step(pid, gate, "awaiting_approval", timeout=180)
        api.approve(pid, gate, approver="proof", rationale="feature proof")
    done = api.wait_terminal(pid, timeout=300)
    assert done["status"] == "done", done.get("trigger")
    record("event.emit", cell.id, "check-and-publish",
           f"pipeline {pid}: the `publish` leg (emits: dataset.ready) completed")
    rec = _wait_started(api, "ask-the-docs", "event", timeout=60, seen=before)
    trig = rec.get("trigger") or {}
    assert trig.get("topic") == "dataset.ready" and trig.get("trigger_id"), trig
    record("event.on", cell.id, "ask-the-docs",
           f"dataset.ready from {pid} started ask-the-docs {rec['pipeline_id']} "
           f"(kind: event row {trig['trigger_id']})")


WATCH_EVERY_MINUTE = (
    '    every: "*/15 * * * *"\n    on_change: start\n    enabled: false\n',
    '    every: "* * * * *"\n    on_change: start\n    enabled: true\n',
)


@pytest.mark.parametrize("cell", [CELL])
def test_a_dataset_watch_starts_its_pipeline_on_a_new_version(cell, server_factory, tmp_path) -> None:
    """The watch on `hello-sample` (every minute here) starts check-and-publish only once
    the dataset gained a version: a manual run's `engine-check` leg registers the sample
    (#2632), and the next tick sees the change (ADR-0131 D3)."""
    pack = _pack_with(tmp_path, {PNP: [WATCH_EVERY_MINUTE]})
    srv = server_factory(cell, "watch", pack_path=pack)
    api = srv.api()
    pid = api.start(f"{ARMED}/check-and-publish")
    api.wait(pid, lambda r: r["step_state"].get("engine-check", {}).get("status") == "done",
             timeout=180, what="engine-check done (the sample registered)")
    rec = _wait_started(api, "check-and-publish", "watch", timeout=150, seen=frozenset({pid}))
    # the watch's own run registers the SAME content: no new version, so no further start
    # (#2632 review) — two more ticks, still only one watch-started run
    time.sleep(130)
    watch_runs = [p["pipeline_id"] for p in _pipelines(api, "check-and-publish")
                  if (api.pipeline(p["pipeline_id"]).get("trigger") or {}).get("source") == "watch"]
    assert watch_runs == [rec["pipeline_id"]], watch_runs
    record("trigger.watch", cell.id, "check-and-publish",
           f"hello-sample gained a version in {pid}; the watch started pipeline {rec['pipeline_id']} "
           f"(trigger.source='watch')")
