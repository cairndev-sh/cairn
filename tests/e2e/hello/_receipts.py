"""Feature receipts for the hello-cairn proof (ADR-0127 D1) and the FEATURE_PROOF.md report.

A cell that proves a feature calls :func:`record`; the proof script turns the receipts and
pytest's junit into ``FEATURE_PROOF.md``: one row per advertised feature naming the cell
that proved it, or why nothing did (a skip reason, a failure, or "no receipt").

    python -m tests.e2e.hello._receipts report --junit junit.xml --receipts receipts.jsonl --out FEATURE_PROOF.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

#: The advertised features (the plan's feature matrix), in report order:
#: key → (what it is, the hello-cairn pipeline whose row proves it). The test that proves a
#: row is ``test_pipeline[<cell>-<pipeline>]``, except the crash rows (:data:`CRASH_TESTS`).
FEATURES: dict[str, tuple[str, str]] = {
    "pipeline.data": ("a data pipeline from the CLI/API", "profile-and-publish"),
    "pipeline.agentic": ("an agentic (template) pipeline", "ask-the-docs"),
    "pipeline.ml": ("an ml pipeline", "tiny-finetune"),
    "pipeline.flow": ("a flow-kind pipeline", "nightly-ops"),
    "gate.data": ("a data gate (predicate on a leg's output)", "profile-and-publish"),
    "gate.person": ("a person gate, decided through the API/CLI", "profile-and-publish"),
    "gate.operator": ("an operator-declared gate (requires_approval) decided on the leg's one Run", "profile-and-publish"),
    "flow.builtin-engine": ("a flow leg on the builtin dataflow engine", "profile-and-publish"),
    "artifact.store": ("an artifact written by storage/put (artifact.store)", "profile-and-publish"),
    "artifact.promote": ("artifact/promote moves a governed tag to the flow leg's artifact", "profile-and-publish"),
    "audit": ("audit rows in the hash-chained sink (audit/store leg)", "profile-and-publish"),
    "budget": ("a declared budget on the record", "profile-and-publish"),
    "redaction": ("the redact.text stage", "profile-and-publish"),
    "trigger.schedule": ("a cron schedule trigger (enabled) starting its pipeline", "profile-and-publish"),
    "trigger.watch": ("a dataset watch starting its pipeline on a new version", "profile-and-publish"),
    "trigger.webhook": ("a declared webhook starting its pipeline", "profile-and-publish"),
    "event.emit": ("a leg's `emits:` published on the bus", "profile-and-publish"),
    "event.on": ("a `kind: event` trigger starting a pipeline on that topic", "ask-the-docs"),
    "hooks": ("a cairn.run_hooks listener's receipts", "profile-and-publish"),
    "llm": ("a model call through core's build_chat_model (the keyless stub model, #2607)", "ask-the-docs"),
    "mcp": ("mcp/call against a stdio server", "ask-the-docs"),
    "rag": ("rag index + query on a vector store", "ask-the-docs"),
    "memory": ("memory/write to a local store", "ask-the-docs"),
    "monitor": ("monitor/threshold or monitor/propose", "ask-the-docs"),
    "train": ("a real LoRA fine-tune (train.finetune)", "tiny-finetune"),
    "secrets.env": ("secrets.store bound to secrets/env (nightly-ops' secret check)", "nightly-ops"),
    "secrets.infisical": ("secrets.store bound to secrets/infisical (nightly-ops on Infisical)", "nightly-ops"),
    "ssh": ("remote/exec over SSH on a compute.ssh connection", "nightly-ops"),
    "worker.pull": ("cairn worker --pull --once serving a leg", "tiny-finetune"),
    "storage.postgres": ("every store on Postgres", "profile-and-publish"),
    "bus.postgres": ("the Postgres bus", "profile-and-publish"),
    "runtime.dbos": ("the DBOS runtime running a template leg", "ask-the-docs"),
    "crash.gate": ("a server killed at a gate; restart; the gate is decided after", "crash-pack"),
    "crash.mid-leg": ("a server killed mid-leg; restart; the ADR-0125 outcome", "crash-pack"),
    "crash.dbos-resume": ("a template run resumed by DBOS after a crash", "ask-the-docs"),
}

#: crash feature → the test whose cells prove it (the crash pack, not a hello pipeline)
CRASH_TESTS = {
    "crash.gate": "test_killed_at_the_person_gate",
    "crash.mid-leg": "test_killed_mid_flow_leg",
    "crash.dbos-resume": "test_dbos_resumes_a_template_run_after_a_crash",
}

#: trigger feature → the test in ``test_triggers.py`` whose cell proves it (ADR-0131)
TRIGGER_TESTS = {
    "trigger.webhook": "test_a_webhook_starts_its_pipeline",
    "trigger.schedule": "test_a_schedule_fires_its_pipeline",
    "event.emit": "test_a_published_event_starts_ask_the_docs",
    "event.on": "test_a_published_event_starts_ask_the_docs",
    "trigger.watch": "test_a_dataset_watch_starts_its_pipeline_on_a_new_version",
}

#: features that only one value of a cell dimension can prove: a failure in another cell
#: (say, ask-the-docs on the in-process runtime) is not a failure of this feature
CELL_SCOPE = {
    "storage.postgres": lambda cell: cell.startswith("postgres-"),
    "bus.postgres": lambda cell: cell.endswith("-postgres"),
    "runtime.dbos": lambda cell: "-dbos-" in cell,
    "crash.dbos-resume": lambda cell: "-dbos-" in cell,
}

#: what answers the model calls (the engine has no built-in mock model, #2607)
MODEL_PROVIDER = ("`tests/e2e/model_stub.py` — a deterministic, keyless OpenAI-compatible stub "
                  "(chat completions + embeddings) the harness starts and points every server at "
                  "with `OPENAI_BASE_URL` (#2607)")

RECEIPTS_ENV = "CAIRN_PROOF_RECEIPTS"
_SESSION: list[dict] = []


def record(feature: str, cell: str, pipeline: str, evidence: str, *, proved: bool = True) -> None:
    """One receipt: ``feature`` was proved in ``cell`` by ``pipeline``; ``evidence`` is the
    one line a reader can check (a run id, a path, a count). ``proved=False`` records why
    a cell that ran could NOT prove it (the pipeline on this tree lacks the leg), so the
    report names the debt instead of "no receipt"."""
    assert feature in FEATURES, f"unknown feature {feature!r} — add it to FEATURES"
    row = {"feature": feature, "cell": cell, "pipeline": pipeline, "evidence": evidence, "proved": proved,
           "test": current_test()}
    _SESSION.append(row)
    path = os.environ.get(RECEIPTS_ENV, "")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")


def current_test() -> str:
    """The running test's case name (``test_pipeline[<cell>-<row>]``), from pytest's
    ``PYTEST_CURRENT_TEST`` ("<file>::<name> (call)"). A receipt carries it so the report
    counts the receipt only when THAT case passed: a cell that records a receipt and then
    fails a later assertion proves nothing (PR #2656 review)."""
    current = os.environ.get("PYTEST_CURRENT_TEST", "")
    return current.split(" (", 1)[0].rsplit("::", 1)[-1]


def _load_receipts(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def _load_junit(path: Path) -> list[dict]:
    """Every testcase as ``{id, outcome, message}``; outcome ∈ passed/failed/skipped/error."""
    if not path.is_file():
        return []
    out = []
    for tc in ET.parse(path).getroot().iter("testcase"):
        tid = f"{tc.get('classname', '')}::{tc.get('name', '')}"
        outcome, message = "passed", ""
        for tag, name in (("skipped", "skipped"), ("failure", "failed"), ("error", "error")):
            el = tc.find(tag)
            if el is not None:
                outcome, message = name, (el.get("message") or (el.text or "")).strip()
                break
        out.append({"id": tid, "outcome": outcome, "message": message.splitlines()[0] if message else ""})
    return out


def _cases_for(key: str, pipeline: str, cases: list[dict]) -> list[dict]:
    """The junit cases that prove ``key``: the crash test, or ``test_pipeline`` for its
    pipeline — only in the cells that can prove it (:data:`CELL_SCOPE`)."""
    test = CRASH_TESTS.get(key) or TRIGGER_TESTS.get(key)
    if test:
        mine = [c for c in cases if f"{test}[" in c["id"]]
    else:
        mine = [c for c in cases if "test_pipeline[" in c["id"] and c["id"].endswith(f"-{pipeline}]")]
    scope = CELL_SCOPE.get(key)
    return [c for c in mine if scope(_cell_of(c["id"]))] if scope else mine


def _cell_of(case_id: str) -> str:
    """``…[sqlite-dbos-inprocess-profile-and-publish]`` → ``sqlite-dbos-inprocess``."""
    inner = case_id.rsplit("[", 1)[-1].rstrip("]")
    return "-".join(inner.split("-")[:3])


def feature_status(junit: Path, receipts: Path) -> dict[str, dict]:
    """Each FEATURES key's verdict from one run: ``{state, cells, evidence, pipeline}`` with
    state PROVED | FAILED | NOT PROVED — the rule FEATURE_PROOF.md and the generated feature
    matrix (scripts/oss/feature_matrix.py, #2667) both read."""
    cases = _load_junit(junit)
    rows = _load_receipts(receipts)
    # a receipt proves its feature only when the case that recorded it PASSED in this
    # junit: no junit (the run died before writing it), a stale one, or a case that
    # failed after recording all leave the row unproved (PR #2656 review)
    passed = {c["id"].rsplit("::", 1)[-1] for c in cases if c["outcome"] == "passed"}
    by_feature: dict[str, list[dict]] = {}
    owed: dict[str, set[str]] = {}
    for r in rows:
        if r.get("proved", True):
            if r.get("test") not in passed:
                continue
            by_feature.setdefault(r["feature"], []).append(r)
        else:
            owed.setdefault(r["feature"], set()).add(r["evidence"])
    out: dict[str, dict] = {}
    for key, (_what, pipeline) in FEATURES.items():
        mine = _cases_for(key, pipeline, cases)
        failed = [c for c in mine if c["outcome"] in ("failed", "error")]
        got = by_feature.get(key, [])
        if failed:
            out[key] = {"state": "FAILED", "pipeline": pipeline,
                        "cells": sorted({_cell_of(c["id"]) for c in failed}),
                        "evidence": "; ".join(sorted({c["message"] or c["id"] for c in failed}))[:160]}
        elif got:
            out[key] = {"state": "PROVED", "pipeline": got[0]["pipeline"],
                        "cells": sorted({g["cell"] for g in got}), "evidence": got[0]["evidence"]}
        else:
            skips = {f"{c['outcome']}: {c['message'] or c['id']}" for c in mine if c["outcome"] != "passed"}
            out[key] = {"state": "NOT PROVED", "pipeline": pipeline, "cells": [],
                        "evidence": "; ".join(sorted(owed.get(key) or skips or {"no receipt recorded"}))}
    return out


def build_report(junit: Path, receipts: Path) -> str:
    cases = _load_junit(junit)
    status = feature_status(junit, receipts)
    lines = [
        "# FEATURE_PROOF — hello-cairn (ADR-0127 D1)",
        "",
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} by "
        "`scripts/oss/feature_proof.sh` from `tests/e2e/hello`. A row is PROVED when a matrix "
        "cell recorded a receipt for it and no cell of its test failed; a failed cell makes the "
        "row FAILED whatever the other cells said; otherwise the row says why nothing proved it.",
        "",
        f"Model provider: {MODEL_PROVIDER}.",
        "",
        "| Feature | What | Proved by | Cell(s) | Evidence |",
        "|---|---|---|---|---|",
    ]
    for key, (what, _pipeline) in FEATURES.items():
        s = status[key]
        cells = ", ".join(s["cells"]) or "—"
        if s["state"] == "FAILED":
            lines.append(f"| `{key}` | {what} | `{s['pipeline']}` | {cells} | FAILED — {s['evidence']} |")
        elif s["state"] == "PROVED":
            lines.append(f"| `{key}` | {what} | `{s['pipeline']}` | {cells} | {s['evidence']} |")
        else:
            lines.append(f"| `{key}` | {what} | `{s['pipeline']}` | — | NOT PROVED — {s['evidence']} |")
    counts: dict[str, int] = {}
    for c in cases:
        counts[c["outcome"]] = counts.get(c["outcome"], 0) + 1
    lines += ["", "## Cells", "", "| Outcome | Count |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    skipped = [c for c in cases if c["outcome"] == "skipped"]
    if skipped:
        lines += ["", "### Skipped", ""]
        lines += [f"- `{c['id'].rsplit('::', 1)[-1]}` — {c['message']}" for c in skipped]
    failed = [c for c in cases if c["outcome"] in ("failed", "error")]
    if failed:
        lines += ["", "### Failed", ""]
        lines += [f"- `{c['id'].rsplit('::', 1)[-1]}` — {c['message']}" for c in failed]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    rep = sub.add_parser("report")
    rep.add_argument("--junit", type=Path, required=True)
    rep.add_argument("--receipts", type=Path, required=True)
    rep.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    args.out.write_text(build_report(args.junit, args.receipts), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
