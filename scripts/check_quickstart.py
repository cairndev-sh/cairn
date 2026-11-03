"""Run the README's Quickstart, command by command, and fail if any of it does not work (#330).

The commands are read out of the README's ``## Quickstart`` section, not copied here, so the
README cannot drift from what this runs: a line this script does not know how to run is an error,
not a skip. In a scratch directory and HOME, with no API key in the environment, it:

  * runs the ``export`` lines and the ``cairn setup`` and ``cairn run`` commands of the first code
    block as written, and the ``cairn decide`` and ``cairn status`` commands of the second;
  * answers both gates through ``cairn decide`` and checks the pipeline ends ``done``;
  * runs the ``cairn run`` again with the ``-i min_rows=1000`` the README suggests, and checks the
    data gate fails it before any later step starts;
  * runs ``cairn admin audit verify`` on ``./audit.jsonl``, and checks it holds a row for each
    decision a person made;
  * stops the engine it started, and checks that its process is gone.

What it does not run: the install line (the check installs the checkout instead), and the two
``curl`` lines, which fetch the example's files from ``main`` on GitHub; the same files are copied
from this checkout. Exit 0 only if every step held.

    .venv/bin/python scripts/check_quickstart.py [--cairn .venv/bin/cairn]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
RAW = "https://raw.githubusercontent.com/cairndev-sh/cairn/main/"
INSTALL = "curl -LsSf https://docs.cairndev.sh/install.sh"
PIPELINE = "hello-cairn/profile-and-publish"
GATES = ("review", "promote")
#: every step after the data gate, in `examples/hello-cairn/pipelines/profile-and-publish.yaml`
AFTER_DATA_GATE = ("redact", "engine-check", "stage", "review", "promote", "publish", "audit")
NOT_STARTED = {"pending", "skipped"}
#: `cairn run` exits 2 when the pipeline waits at a gate and there is no terminal to ask on
PAUSED_EXIT = 2


class QuickstartError(Exception):
    """The README says something this check cannot run, or what it says does not hold."""


def section(readme: str) -> str:
    match = re.search(r"^## Quickstart\n(.*?)(?=^## |\Z)", readme, re.S | re.M)
    if not match:
        raise QuickstartError("README.md has no '## Quickstart' section")
    return match.group(1)


def blocks(text: str) -> list[list[str]]:
    """The commands of each ```bash block of ``text``, continuation lines joined, comments dropped."""
    out = []
    for body in re.findall(r"^```bash\n(.*?)^```", text, re.S | re.M):
        commands: list[str] = []
        for line in re.sub(r"\\\n\s*", "", body).splitlines():
            line = re.sub(r"\s+#.*$", "", line).strip()
            if line and not line.startswith("#"):
                commands.append(line)
        out.append(commands)
    return out


def plan(readme: str) -> tuple[list[str], list[str]]:
    """The first block (set up and launch) and the second (decide and read), checked to be what this
    script can run. Raises :class:`QuickstartError` naming whatever it can't."""
    found = blocks(section(readme))
    if len(found) < 2:
        raise QuickstartError(f"the Quickstart has {len(found)} bash block(s); this check reads two")
    launch, decide = found[0], found[1]
    port = _port(launch)
    if not any(c.startswith("cairn run ") and PIPELINE in c for c in launch):
        raise QuickstartError(f"the first block does not run {PIPELINE}")
    if sum(c.startswith("cairn setup") for c in launch) != 1 or not port:
        raise QuickstartError("the first block must have one `cairn setup --port N`")
    for c in launch:
        if not (c.startswith(("cairn setup", "cairn run ", "mkdir ", "export ", "curl -LsSfO " + RAW))
                or c.startswith(INSTALL)):
            raise QuickstartError(f"the first block has a line this check can't run: {c!r}")
    for gate in GATES:
        if f"cairn decide <pipeline_id> approve --step {gate}" not in decide:
            raise QuickstartError(f"the second block has no `cairn decide <pipeline_id> approve --step {gate}`")
    if "cairn status <pipeline_id> --results" not in decide or len(decide) != 3:
        raise QuickstartError("the second block must be the two `cairn decide` lines and `cairn status --results`")
    for line in decide:
        if not line.startswith(("cairn decide <pipeline_id> ", "cairn status <pipeline_id> ")):
            raise QuickstartError(f"the second block has a line this check can't run: {line!r}")
    prose = section(readme)
    for claim in ("`cairn admin audit verify`", "`-i min_rows=1000`", "./audit.jsonl"):
        if claim not in prose:
            raise QuickstartError(f"the Quickstart no longer mentions {claim}; this check proves it")
    return launch, decide


def _port(launch: list[str]) -> Optional[int]:
    for c in launch:
        match = re.fullmatch(r"cairn setup --port (\d+)", c)
        if match:
            return int(match.group(1))
    return None


def sanitized_env(home: Path) -> dict[str, str]:
    """No setting that changes what the commands do, and no model key: the Quickstart needs none."""
    drop = re.compile(r"^(CAIRN_|OBS_|OPENAI_|ANTHROPIC_|AZURE_|MISTRAL_|GROQ_)|(_API_KEY|_TOKEN|_SECRET)$")
    env = {k: v for k, v in os.environ.items() if not drop.search(k)}
    env["HOME"] = str(home)
    return env


class Quickstart:
    def __init__(self, cairn: str, work: Path, env: dict[str, str]) -> None:
        self.cairn, self.work, self.env = cairn, work, env
        self.cwd = work

    def cairn_run(self, line: str, *, ok: tuple[int, ...] = (0,), timeout: int = 300) -> subprocess.CompletedProcess:
        print(f"$ {line}", flush=True)
        argv = shlex.split(line.replace("$PWD", str(self.cwd)))  # the shell would expand it
        argv[0] = self.cairn
        done = subprocess.run(argv, cwd=self.cwd, env=self.env, capture_output=True, text=True,
                              stdin=subprocess.DEVNULL, timeout=timeout)
        print(done.stdout + done.stderr, end="", flush=True)
        if done.returncode not in ok:
            raise QuickstartError(f"`{line}` exited {done.returncode}, expected {' or '.join(map(str, ok))}")
        return done

    def shell_line(self, line: str) -> None:
        """The non-`cairn` lines of the first block."""
        if line.startswith(INSTALL):
            print(f"(skipped: {line.split('|')[0].strip()} | ...; this check installed the checkout)", flush=True)
        elif line.startswith("curl -LsSfO "):
            url = line.split()[-1]
            if not url.startswith(RAW):
                raise QuickstartError(f"a curl line outside {RAW}: {line!r}")
            source = REPO / url.removeprefix(RAW)
            if not source.is_file():
                raise QuickstartError(f"the README downloads {url}, which this checkout does not have")
            shutil.copy(source, self.cwd / source.name)
            print(f"(copied {source.relative_to(REPO)} from this checkout instead of {url})", flush=True)
        elif line.startswith("mkdir "):
            match = re.fullmatch(r"mkdir (\S+) && cd (\S+)", line)
            if not match or match.group(1) != match.group(2):
                raise QuickstartError(f"a mkdir line this check can't run: {line!r}")
            self.cwd = self.work / match.group(1)
            self.cwd.mkdir()
        elif line.startswith("export "):
            match = re.fullmatch(r"export (\w+)=(\S+)", line)
            if not match:
                raise QuickstartError(f"an export line this check can't run: {line!r}")
            self.env[match.group(1)] = match.group(2).replace("$PWD", str(self.cwd))
        else:
            raise QuickstartError(f"a line this check can't run: {line!r}")

    def record(self, pipeline_id: str) -> dict:
        done = subprocess.run([self.cairn, "status", pipeline_id, "--json"], cwd=self.cwd, env=self.env,
                              capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)
        if done.returncode:
            raise QuickstartError(f"cairn status {pipeline_id} --json exited {done.returncode}: {done.stderr}")
        return json.loads(done.stdout)

    def wait(self, pipeline_id: str, status: str, *, timeout: float = 120.0) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            record = self.record(pipeline_id)
            if record.get("status") == status:
                return record
            if time.monotonic() > deadline or record.get("status") in {"done", "failed", "cancelled"}:
                raise QuickstartError(f"{pipeline_id} is {record.get('status')}, not {status}: "
                                      f"{ {s: v.get('status') for s, v in record.get('step_state', {}).items()} }")
            time.sleep(1)

    def decide(self, line: str, pipeline_id: str, *, timeout: float = 120.0) -> None:
        """A gate takes a decision only once the step before it has finished; try until it does."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.cairn_run(line.replace("<pipeline_id>", pipeline_id))
                return
            except QuickstartError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(2)


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _stopped(q: Quickstart, home: Path) -> None:
    """Stop the engine this check started and prove its process is gone."""
    state_file = home / ".cairn" / "engine.json"
    pid = None
    if state_file.exists():
        pid = json.loads(state_file.read_text(encoding="utf-8")).get("pid")
    try:
        q.cairn_run("cairn setup --stop")
    finally:
        if isinstance(pid, int):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return
                time.sleep(0.2)
            os.kill(pid, 9)
            raise QuickstartError(f"the engine (pid {pid}) was still running after `cairn setup --stop`; killed")


def run(readme: str, cairn: str) -> None:
    launch, decide = plan(readme)
    port = _port(launch)
    assert port is not None
    if port_in_use(port):
        raise QuickstartError(f"port {port} is already in use: `cairn setup` would use that engine, not start one")
    with tempfile.TemporaryDirectory(prefix="cairn-quickstart-") as scratch:
        home = Path(scratch) / "home"
        home.mkdir()
        q = Quickstart(cairn, Path(scratch) / "work", sanitized_env(home))
        q.work.mkdir()
        try:
            started = False
            pipeline_id = ""
            run_line = ""
            for line in launch:
                if line.startswith("cairn setup"):
                    started = True
                    q.cairn_run(line)
                elif line.startswith("cairn run "):
                    run_line = line
                    done = q.cairn_run(line, ok=(PAUSED_EXIT,))
                    found = re.search(r"pipeline_id=(\S+)", done.stdout)
                    if not found or "still waiting at review" not in done.stdout + done.stderr:
                        raise QuickstartError("the run did not stop at the `review` person gate")
                    pipeline_id = found.group(1)
                else:
                    q.shell_line(line)
            q.wait(pipeline_id, "paused")
            for line in decide:
                if line.startswith("cairn decide"):
                    q.decide(line, pipeline_id)
                else:
                    q.wait(pipeline_id, "done")
                    shown = q.cairn_run(line.replace("<pipeline_id>", pipeline_id))
                    if "status=done" not in shown.stdout:
                        raise QuickstartError(f"`{line}` does not show status=done")

            # the data gate, as the README suggests
            failed_line = f"{run_line} -i min_rows=1000"
            failed = q.cairn_run(failed_line, ok=(1,))
            failed_id = re.search(r"pipeline_id=(\S+)", failed.stdout)
            if not failed_id:
                raise QuickstartError("the min_rows=1000 run printed no pipeline id")
            record = q.record(failed_id.group(1))
            steps = {s: v.get("status", "") for s, v in record.get("step_state", {}).items()}
            if record.get("status") != "failed" or any(steps.get(s) not in NOT_STARTED for s in AFTER_DATA_GATE):
                raise QuickstartError(f"the data gate did not stop the pipeline before a later step: {steps}")

            # the audit trail the README points at
            log = q.cwd / "audit.jsonl"
            if not log.is_file():
                raise QuickstartError(f"no {log.name} in the Quickstart directory")
            verified = q.cairn_run("cairn admin audit verify")
            if "chain OK" not in verified.stdout:
                raise QuickstartError("`cairn admin audit verify` did not print 'chain OK'")
            rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            decided = {row.get("target") for row in rows if row.get("action") == "pipeline.gate_approved"}
            missing = [g for g in GATES if f"{pipeline_id}/{g}" not in decided]
            if missing:
                raise QuickstartError(f"no pipeline.gate_approved audit row for {pipeline_id}/{missing}")
            if not started:
                raise QuickstartError("the Quickstart never ran `cairn setup`")
        except BaseException:
            engine_log = home / ".cairn" / "engine.log"
            if engine_log.exists():
                print("--- engine log (tail) ---\n" + "\n".join(engine_log.read_text(errors="replace").splitlines()[-60:]),
                      file=sys.stderr)
            raise
        finally:
            if (home / ".cairn" / "engine.json").exists():
                _stopped(q, home)


def absolute_command(cairn: str) -> str:
    """The commands run from a scratch directory, so a relative path has to be made absolute first."""
    return os.path.abspath(cairn) if os.sep in cairn else cairn


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    default = REPO / ".venv" / "bin" / "cairn"
    parser.add_argument("--cairn", default=str(default) if default.exists() else (shutil.which("cairn") or "cairn"),
                        help="the cairn command that stands for `cairn` in the README")
    parser.add_argument("--readme", type=Path, default=REPO / "README.md")
    args = parser.parse_args()
    try:
        run(args.readme.read_text(encoding="utf-8"), absolute_command(args.cairn))
    except (QuickstartError, subprocess.TimeoutExpired) as exc:
        print(f"quickstart check FAILED: {exc}", file=sys.stderr)
        return 1
    print("quickstart check passed: every Quickstart command ran and held")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
