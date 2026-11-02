"""#369 — a run on the local engine can be read back with `cairn status <run> --replay`, with no
extra environment variable, and doing so runs nothing.

The real boundary: `cairn dev --engine-only` on SQLite in a scratch HOME, the quickstart's sign-in
and launch, and the CLI in a process of its own whose environment names no store. This harness's
`Server` pins CAIRN_STORAGE into the CLI's environment, which is the very thing under test, so this
file starts its own engine.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from cairn_client import CairnClient

from tests.e2e.conftest import HELLO, PROFILE, free_port

pytestmark = pytest.mark.e2e


def _cairn(home: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", "from cairn.cli.main import app; app()", *args],
                          cwd=home, env=_env(home), capture_output=True, text=True, timeout=120)


def _env(home: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("CAIRN_", "OBS_"))}
    env.update({"HOME": str(home), "CAIRN_PROFILE": str(PROFILE), "CAIRN_PACK_PATH": str(HELLO)})
    return env


def _wait_for_the_gate(url: str, pipeline_id: str) -> dict:
    with CairnClient(url) as c:
        c.login("dev", "dev")
        deadline = time.monotonic() + 120
        while True:
            record = c.pipeline(pipeline_id)
            if record["status"] == "paused":
                return record
            assert record["status"] == "running" and time.monotonic() < deadline, record
            time.sleep(0.5)


def test_a_local_run_reads_back_with_no_environment_variable_and_nothing_runs(tmp_path):
    # One test, one engine: each CLI process and the engine's boot cost seconds.
    home = tmp_path
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    sample = str(HELLO / "data" / "sample.jsonl")
    log = open(home / "engine.log", "wb")
    proc = subprocess.Popen(
        [sys.executable, "-c", "from cairn.cli.main import app; app()", "dev", "--engine-only", "--port", str(port)],
        cwd=home, env=_env(home), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                if httpx.get(f"{url}/healthz", timeout=2.0).status_code < 500:
                    break
            except httpx.HTTPError:
                pass
            assert proc.poll() is None, (home / "engine.log").read_text(errors="replace")[-2000:]
            assert time.monotonic() < deadline, "the engine did not answer"
            time.sleep(0.2)
        signed_in = _cairn(home, "setup", "--server", url, "-u", "dev", "--password", "dev")
        assert signed_in.returncode == 0, signed_in.stderr
        launched = _cairn(home, "run", "hello-cairn/profile-and-publish", "-i", f"path={sample}",
                          "--environment", "sandbox", "--detach")
        assert launched.returncode == 0, launched.stderr
        pipeline_id = launched.stdout.split("pipeline_id=", 1)[1].split()[0]
        before = _wait_for_the_gate(url, pipeline_id)
        run_id = before["step_state"]["profile"]["run_id"]

        # The documented command, with no store in its environment.
        shown = _cairn(home, "status", run_id, "--replay")
        assert shown.returncode == 0, shown.stderr
        assert "status:      done" in shown.stdout
        assert "environment: sandbox" in shown.stdout
        assert "ran:         task:data/profile" in shown.stdout
        assert "operators:   data/profile" in shown.stdout
        assert sample in shown.stdout, "the recorded inputs"
        assert "timeline (1 event(s)):" in shown.stdout and '"leg": "task:data/profile"' in shown.stdout

        raw = _cairn(home, "status", run_id, "--replay", "--json")
        assert raw.returncode == 0, raw.stderr
        body = json.loads(raw.stdout)
        assert body["meta"]["inputs"]["source"] == sample
        assert body["meta"]["operators"] == ["data/profile"]
        assert [e["type"] for e in body["events"]] == ["done"]
        assert body["meta"]["source"] == "engine"
        assert url in raw.stderr, "it says where it read from"

        alias = _cairn(home, "replay", run_id)
        assert alias.returncode == 0, alias.stderr
        assert alias.stdout == shown.stdout
        assert "is now `cairn status <run> --replay`" in alias.stderr

        # A flow leg, on the same engine: the record names the flow, and has no stage operators to show.
        flow_run = before["step_state"]["engine-check"]["run_id"]
        flow = _cairn(home, "status", flow_run, "--replay")
        assert flow.returncode == 0, flow.stderr
        assert "ran:         flow:hello-cairn/engine-check" in flow.stdout
        assert "operators:   (none recorded)" in flow.stdout
        assert "register = hello-sample" in flow.stdout, "the recorded inputs"
        assert "timeline (2 event(s)):" in flow.stdout and '"leg": "flow:hello-cairn/engine-check"' in flow.stdout

        # A pipeline id says where its runs are.
        refused = _cairn(home, "status", pipeline_id, "--replay")
        assert refused.returncode == 1
        assert f"cairn status {pipeline_id} --logs" in refused.stderr

        # Nothing ran again: the paused pipeline is what it was, and so are the leg's record and events.
        after = _wait_for_the_gate(url, pipeline_id)
        assert after["step_state"] == before["step_state"]
        again = json.loads(_cairn(home, "status", run_id, "--replay", "--json").stdout)
        assert again == body

        # With the engine gone the record still reads back, from the store it wrote, and it says so.
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        stopped = _cairn(home, "status", run_id, "--replay")
        assert stopped.returncode == 0, stopped.stderr
        assert "operators:   data/profile" in stopped.stdout
        assert str(home / ".cairn" / "dev.db") in stopped.stderr
        stopped_raw = json.loads(_cairn(home, "status", run_id, "--replay", "--json").stdout)
        assert stopped_raw["meta"]["source"] == "dev-db"

        # Naming the engine that stopped is a failure, though its record is on this machine.
        named = _cairn(home, "status", run_id, "--replay", "--endpoint", url)
        assert named.returncode == 1 and named.stdout == "" and url in named.stderr
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        proc.wait(timeout=10)
