"""The public names over a run (#297, ``docs/contracts/pipelines-and-runs.md`` §Public Python
names): each exists, the earlier private spelling is the same object, and the contract doc
lists every one."""
from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: (module, public name, earlier private name)
PROMOTED = [
    ("cairn.core.run_progress", "duration_ms", "_duration_ms"),
    ("cairn.core.run_progress", "trace_from_events", "_trace_from_events"),
    ("cairn.core.eval.actuation", "proposal_markdown", "_proposal_markdown"),
    ("cairn.core.demo_seed", "demo_inputs_for", "_demo_inputs_for"),
]


@pytest.mark.parametrize("module,public,private", PROMOTED)
def test_the_private_spelling_is_the_public_object(module, public, private):
    mod = importlib.import_module(module)
    assert getattr(mod, private) is getattr(mod, public)


@pytest.mark.parametrize("name", [p[1] for p in PROMOTED] + ["run_template_ref"])
def test_the_contract_doc_lists_each_public_name(name):
    doc = (REPO / "docs/contracts/pipelines-and-runs.md").read_text()
    assert re.search(rf"\b{name}\b", doc)


def test_trace_from_events_times_each_step_to_the_next_lifecycle_event():
    from cairn.core.run_progress import duration_ms, trace_from_events

    events = [
        {"type": "node", "seq": 1, "ts": "2026-10-30T10:00:00", "data": {"node": "a"}},
        {"type": "node", "seq": 2, "ts": "2026-10-30T10:00:02", "data": {"node": "b"}},
        {"type": "done", "seq": 3, "ts": "2026-10-30T10:00:03"},
    ]
    assert [(s["step"], s["ms"]) for s in trace_from_events(events)] == [("a", 2000), ("b", 1000)]
    assert duration_ms(None, "2026-10-30T10:00:00") is None
