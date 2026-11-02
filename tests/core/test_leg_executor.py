"""A flow leg runs with no graph runtime (ADR-0084 D1): the claim, checked.

Runs a real leg on the builtin engine in a fresh interpreter, because any test that ran
earlier in this process would already have loaded LangGraph. Both processes report what
they loaded when they exit: the one calling `execute_leg`, and the job the engine spawns.

What this covers is `execute_leg` and the job, not the process around them.
`engine/compiler.py` imports LangGraph at module level, and `dispatch/pipeline_bridge.py`
and the CLI both reach it, so a control plane has it loaded whatever kind of leg it runs.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

_REPORT_AT_EXIT = """
import atexit, json, os, sys

def _report():
    with open(os.environ["LEG_TEST_REPORT"], "a") as f:
        f.write(json.dumps({
            "argv": sys.argv[0],
            "loaded": sorted({m.split(".")[0] for m in sys.modules} & {"langgraph", "temporalio"}),
        }) + "\\n")

atexit.register(_report)
"""
_PACK = """
[pack]
name = "leg-test"
version = "0.0.1"
description = "Two flows on the builtin engine."
kind = "domain"
publisher = "test"
trust_level = "builtin"

[compat]
cairn_engine = ">=0.1.0"
"""
_PACK_INIT = "from cairn.core.pack import Pack\npack = Pack.from_manifest(__file__)\n"
_FLOW = "name: {kind}-check\nkind: {kind}\ndescription: One noop stage.\nstages:\n  - stage: noop\n"
_RUN_A_LEG = """
import asyncio, sys
from cairn.core.dataflow.engine_registry import engine_for_name
from cairn.core.dispatch.leg_executor import execute_leg
from cairn.core.pack.loader import load_all_packs

load_all_packs()
kind = sys.argv[1]
out = asyncio.run(execute_leg(f"flow:leg-test/{kind}-check", inputs={}, kind=kind, poll_budget_s=60,
                              engine=engine_for_name("builtin", kind=kind), run_id="r1"))
print(out.state)
"""


@pytest.mark.parametrize("kind", ["data", "ml"])
def test_a_data_leg_never_imports_langgraph(tmp_path, kind):
    (tmp_path / "sitecustomize.py").write_text(_REPORT_AT_EXIT)
    pack = tmp_path / "pack"
    (pack / "flows").mkdir(parents=True)
    (pack / "pack.toml").write_text(_PACK)
    (pack / "__init__.py").write_text(_PACK_INIT)
    (pack / "flows" / f"{kind}-check.yaml").write_text(_FLOW.format(kind=kind))
    report = tmp_path / "report.jsonl"

    done = subprocess.run(
        [sys.executable, "-c", _RUN_A_LEG, kind], cwd=tmp_path, capture_output=True, text=True, timeout=120,
        env={**os.environ, "CAIRN_PACK_PATH": str(pack), "LEG_TEST_REPORT": str(report),
             # this code, not an installed copy; the first entry reports what each process loaded
             "PYTHONPATH": os.pathsep.join([str(tmp_path), *(p for p in sys.path if p)])},
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.split()[-1] == "succeeded", done.stdout + done.stderr

    reports = [json.loads(line) for line in report.read_text().splitlines()]
    # the caller and the job: a job that never reported proves nothing
    assert len(reports) == 2 and reports[0]["argv"].endswith("entry.py"), reports
    assert [r for r in reports if r["loaded"]] == []
