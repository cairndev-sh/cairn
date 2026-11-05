"""Start the Compose stack from a clean checkout and run a pipeline through it (#371).

``deploy/light`` is the documented way to run cairn on one box, and nothing else starts it. This
brings up its default services as ``install.md`` says to, with no ``.env`` and no API key, and goes
through the published port, so Caddy is on the path too. Then, twice:

  * with no login, as the stack starts;
  * with ``CAIRN_JWT_SECRET`` and ``CAIRN_API_PASSWORD`` set, as ``docker-compose.yml`` documents, where
    a call with no token must be refused and a sign-in must work;

it runs ``hello-cairn/check-and-publish`` through both of its gates, and checks that the engine's
own ``cairn admin audit verify`` passes on the log and that the log holds that pipeline's decisions
and its end. Exit 0 only if all of it held. It always takes the stack and its volumes down.

Helm and the add-on profiles aren't covered.

    .venv/bin/python scripts/check_compose.py [--port 8080]
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

REPO = Path(__file__).resolve().parents[1]
LIGHT = REPO / "deploy" / "light"
SAMPLE = REPO / "examples" / "hello-cairn" / "data" / "sample.jsonl"
PIPELINE = "hello-cairn/check-and-publish"
GATES = ("review", "promote")
TERMINAL = {"done", "failed", "cancelled"}
PROJECT = "cairn-compose-check"
#: where docker-compose.yml sets CAIRN_AUDIT_LOG
AUDIT_LOG = "/root/.cairn/audit.jsonl"
IN_CONTAINER_SAMPLE = "/tmp/sample.jsonl"
USER = "admin"
#: the first build compiles the engine image
UP_TIMEOUT_SECONDS = 1800
WAIT_SECONDS = 180.0


class ComposeError(RuntimeError):
    """The stack did not do what the docs say it does."""


def clean_env(**extra: str) -> dict[str, str]:
    """This environment with nothing that would change what the stack does: no key, no cairn setting."""
    dropped = ("CAIRN_", "OBS_", "OPENAI_", "POSTGRES_", "LLM_GATEWAY", "COMPOSE_")
    return {**{k: v for k, v in os.environ.items() if not k.startswith(dropped)}, **extra}


def check_clean_checkout(light: Path = LIGHT) -> None:
    """The docs promise no ``.env``, so one here would make the proof about something else."""
    if (light / ".env").exists():
        raise ComposeError(f"{light / '.env'} exists: this check proves the stack with none")


def check_audit(report: dict[str, Any], rows: list[dict[str, Any]], pipeline_id: str,
                actor: Optional[str] = None) -> None:
    """The chain verifies, and the log holds this pipeline's decisions, by this actor, and its end."""
    if report.get("ok") is not True or not report.get("rows"):
        raise ComposeError(f"`cairn admin audit verify` didn't pass on a log with rows: {report}")
    decisions = {row.get("target"): row for row in rows if row.get("action") == "pipeline.gate_approved"}
    for gate in GATES:
        row = decisions.get(f"{pipeline_id}/{gate}")
        if row is None:
            raise ComposeError(f"no pipeline.gate_approved audit row for {pipeline_id}/{gate}")
        if actor and row.get("actor") != actor:
            raise ComposeError(f"{pipeline_id}/{gate} was logged with actor {row.get('actor')!r}, not {actor!r}")
    if not any(row.get("action") == "pipeline.finished" and row.get("target") == pipeline_id for row in rows):
        raise ComposeError(f"no pipeline.finished audit row for {pipeline_id}")


def check_refused_without_a_token(status: Optional[int]) -> None:
    """With auth on, a call with no token gets a 401, not an answer."""
    if status != 401:
        raise ComposeError(f"with auth on, a call with no token answered {status}, not 401")


def _wait(read: Callable[[], Any], done: Callable[[Any], bool], *, what: str, timeout: float = WAIT_SECONDS) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = read()
            if done(last):
                return last
        except Exception as exc:  # noqa: BLE001 — not up yet looks like any other failure
            last = exc
        time.sleep(0.5)
    raise ComposeError(f"timed out waiting for {what}; last saw: {last!r}")


class Stack:
    def __init__(self, port: int) -> None:
        self.port = port
        self.url = f"http://localhost:{port}"
        self.env = clean_env(CAIRN_PORT=str(port))

    def compose(self, *args: str, env: Optional[dict[str, str]] = None,
                timeout: float = 300.0) -> subprocess.CompletedProcess:
        """A ``docker compose`` command from ``deploy/light``, as the docs run it. Non-zero raises."""
        argv = ["docker", "compose", "-p", PROJECT, *args]
        done = subprocess.run(argv, cwd=LIGHT, env={**self.env, **(env or {})}, capture_output=True, text=True,
                              timeout=timeout)
        if done.returncode != 0:
            raise ComposeError(f"{' '.join(argv)} exited {done.returncode}:\n{done.stdout}\n{done.stderr}")
        return done

    def up(self, env: Optional[dict[str, str]] = None) -> None:
        self.compose("up", "-d", "--build", "--wait", "--wait-timeout", str(UP_TIMEOUT_SECONDS), env=env,
                     timeout=UP_TIMEOUT_SECONDS + 60)
        import httpx  # noqa: PLC0415 — after --help, which needs no install

        _wait(lambda: httpx.get(f"{self.url}/healthz", timeout=5.0).status_code, lambda code: code == 200,
              what=f"{self.url}/healthz through Caddy")

    def audit(self, env: Optional[dict[str, str]] = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """What the image's own CLI says about the log, and the log's rows."""
        verified = self.compose("exec", "-T", "server", "cairn", "admin", "audit", "verify",
                                "--audit-log", AUDIT_LOG, "--json", env=env)
        text = self.compose("exec", "-T", "server", "cat", AUDIT_LOG, env=env).stdout
        return json.loads(verified.stdout), [json.loads(line) for line in text.splitlines() if line.strip()]


def run_pipeline(client: Any) -> str:
    """Launch, answer both gates, and require ``done``. The pipeline's id."""
    pipeline_id = client.launch_pipeline(PIPELINE, {"path": IN_CONTAINER_SAMPLE}, environment="sandbox")["pipeline_id"]

    def steps(record: dict) -> dict[str, str]:
        return {sid: st.get("status", "") for sid, st in record.get("step_state", {}).items()}

    for gate in GATES:
        record = _wait(lambda: client.pipeline(pipeline_id),
                       lambda r: steps(r).get(gate) == "awaiting_approval" or r.get("status") in TERMINAL,
                       what=f"{pipeline_id} to wait at {gate}")
        if steps(record).get(gate) != "awaiting_approval":
            raise ComposeError(f"{pipeline_id} ended {record.get('status')} before {gate}: {steps(record)}")
        client.approve_step(pipeline_id, gate)
    record = _wait(lambda: client.pipeline(pipeline_id), lambda r: r.get("status") in TERMINAL,
                   what=f"{pipeline_id} to finish")
    if record.get("status") != "done":
        raise ComposeError(f"{pipeline_id} ended {record.get('status')}: {steps(record)}")
    return pipeline_id


def prove(stack: Stack) -> None:
    from cairn_client import CairnAPIError, CairnClient  # noqa: PLC0415 — after --help, which needs no install

    check_clean_checkout()
    print("the default services, no .env, no key", flush=True)
    stack.up()
    stack.compose("cp", str(SAMPLE), f"server:{IN_CONTAINER_SAMPLE}")
    pipeline_id = run_pipeline(CairnClient(stack.url))
    report, rows = stack.audit()
    check_audit(report, rows, pipeline_id)
    print(f"  {pipeline_id} done, chain OK over {report['rows']} rows", flush=True)

    print("the same stack with auth on", flush=True)
    password = secrets.token_urlsafe(24)
    auth = {"CAIRN_JWT_SECRET": secrets.token_urlsafe(48), "CAIRN_API_PASSWORD": password}
    stack.up(env=auth)
    stack.compose("cp", str(SAMPLE), f"server:{IN_CONTAINER_SAMPLE}", env=auth)
    status: Optional[int] = None
    try:
        CairnClient(stack.url).pipelines()
    except CairnAPIError as exc:
        status = exc.status_code
    check_refused_without_a_token(status)
    client = CairnClient(stack.url)
    client.login(USER, password)
    pipeline_id = run_pipeline(client)
    report, rows = stack.audit(env=auth)
    check_audit(report, rows, pipeline_id, actor=USER)
    print(f"  {pipeline_id} done as {USER}, chain OK over {report['rows']} rows", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    stack = Stack(args.port)
    try:
        prove(stack)
    except BaseException:
        logs = subprocess.run(["docker", "compose", "-p", PROJECT, "logs", "--no-color", "--tail", "120"], cwd=LIGHT,
                              env=stack.env, capture_output=True, text=True, timeout=120)
        print(f"--- docker compose logs ---\n{logs.stdout}\n{logs.stderr}", file=sys.stderr)
        raise
    finally:
        subprocess.run(["docker", "compose", "-p", PROJECT, "down", "-v"], cwd=LIGHT, env=stack.env,
                       capture_output=True, timeout=180)
    print("compose check passed: the stack came up clean, and a pipeline ran through it with and without auth")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
