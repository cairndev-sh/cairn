"""A template leg over its cost cap, on a real server with a keyless stub model (#395).

The model calls stop on the one that crosses ``budget.max_cost_usd``, the leg fails, and the
pipeline stops with the leg after it never started. The pack is the harness's own
``hello-budget/capped`` (``budget-pack/``), and each call costs what ``model_stub`` reports.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.e2e.conftest import HELLO, Cell, cell_params

pytestmark = [pytest.mark.e2e]

PACK_PATH = os.pathsep.join([str(HELLO), str(Path(__file__).parent / "budget-pack")])
REF = "hello-budget/capped"
CHAT = "/chat/completions"
#: six calls are declared; the cap sits between the second call's spend and the third's
CALLS_TO_THE_CAP = 3


@pytest.mark.parametrize("cell", cell_params())
def test_a_capped_template_leg_stops_its_pipeline(cell: Cell, server_factory, model_stub) -> None:
    srv = server_factory(cell, "budget-cap", pack_path=PACK_PATH)
    api = srv.api()
    before = model_stub.calls[CHAT]
    pid = api.start(REF, {"question": "q"})
    rec = api.wait_terminal(pid, timeout=180)

    assert rec["status"] == "failed", (rec["status"], rec.get("trigger", {}).get("failure_reason"))
    assert model_stub.calls[CHAT] - before == CALLS_TO_THE_CAP
    steps = rec["step_state"]
    assert steps["calls"]["status"] == "failed", steps["calls"]
    assert not steps["after"].get("run_id"), steps["after"]

    run = api.run(steps["calls"]["run_id"])
    assert run["status"] == "failed" and "over budget" in run["error"], run.get("error")
    assert run["cost"]["llm_usd"] > 0.00007, run["cost"]
    assert run["result_payloads"][0]["output"]["budget_exceeded"] is True
