"""What every measurement shares: where a result comes from, and a real local engine.

A measurement drives the engine through ``cairn-client`` and the HTTP API, which stay stable
when CLI commands are renamed. Only the engine's start goes through the CLI, because that is
what a user types."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "benchmark" / "results"
#: what decides the behaviour being measured, and the measurement itself; a change here makes
#: `code_clean` false
_MEASURED_PATHS = ("src", "packages", "examples", "uv.lock", "pyproject.toml", "benchmark",
                   ":(exclude)benchmark/results")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()


def provenance() -> dict[str, Any]:
    """The cairndev version, lock hash, machine and date a result was measured on. No host
    name or user: results are published."""
    run_id = os.environ.get("GITHUB_RUN_ID")
    return {
        "cairndev_version": importlib.metadata.version("cairndev"),
        "uv_lock_sha256": sha256_file(REPO / "uv.lock"),
        "git_sha": _git("rev-parse", "HEAD"),
        "code_clean": not _git("status", "--porcelain", "--", *_MEASURED_PATHS),
        "machine": {
            "os": platform.system(),
            "arch": platform.machine(),
            "cpus": os.cpu_count(),
            "python": platform.python_version(),
        },
        "measured_at": datetime.now(timezone.utc).date().isoformat(),
        "ci_run": (f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{run_id}"
                   if run_id else None),
    }


def write_result(out: Path, measurement: str, *, title: str, inputs: dict[str, Any],
                 results: list[dict[str, str]], extra: Optional[dict[str, Any]] = None) -> Path:
    """One result file. A runner calls this only after every check held, so a failed run
    publishes nothing. ``extra`` adds fields, such as the ``columns`` and ``notes`` ``render.py`` reads."""
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{measurement}.json"
    path.write_text(json.dumps({
        "measurement": measurement,
        "title": title,
        "script": f"benchmark/{measurement}.py",
        "provenance": provenance(),
        "inputs": inputs,
        "results": results,
        **(extra or {}),
    }, indent=2) + "\n", encoding="utf-8")
    return path


def cairn_command() -> str:
    """The ``cairn`` beside this Python, else the one on the PATH."""
    cairn = Path(sys.executable).with_name("cairn")
    return str(cairn) if cairn.exists() else shutil.which("cairn") or "cairn"


def scrubbed_env() -> dict[str, str]:
    """This environment without any setting that would change what a command under measurement does."""
    return {k: v for k, v in os.environ.items() if not k.startswith(("CAIRN_", "OBS_"))}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LocalEngine:
    """``cairn dev --engine-only`` in a scratch HOME on a free port, with the example pack, under
    ``profile`` when one is given."""

    def __init__(self, profile: Optional[Path] = None) -> None:
        self.profile = profile
        self._scratch = tempfile.TemporaryDirectory(prefix="cairn-benchmark-")
        self.home = Path(self._scratch.name)
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.audit_log = self.home / "audit.jsonl"
        self.boot_seconds: Optional[float] = None
        self._proc: Optional[subprocess.Popen] = None

    def __enter__(self) -> "LocalEngine":
        import httpx  # noqa: PLC0415 — only a real run needs it

        cmd = [cairn_command(), "dev", "--engine-only",
               "--port", str(self.port), "--db", str(self.home / "dev.db")]
        env = scrubbed_env()
        env.update({"HOME": str(self.home), "CAIRN_AUDIT_LOG": str(self.audit_log),
                    "CAIRN_PACK_PATH": str(REPO / "examples" / "hello-cairn")})
        if self.profile is not None:
            env["CAIRN_PROFILE"] = str(self.profile)
        log = open(self.home / "engine.log", "wb")
        start = time.monotonic()
        self._proc = subprocess.Popen(cmd, cwd=self.home, env=env, stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
        log.close()
        try:
            while time.monotonic() - start < 300:
                if self._proc.poll() is not None:
                    raise RuntimeError(f"the engine exited {self._proc.returncode} at boot:\n{self.log_tail()}")
                try:
                    if httpx.get(f"{self.url}/healthz", timeout=2.0).status_code < 500:
                        self.boot_seconds = time.monotonic() - start
                        return self
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            raise RuntimeError(f"the engine did not answer /healthz in 300s:\n{self.log_tail()}")
        except BaseException:
            # Python skips __exit__ when __enter__ raises, and Ctrl-C can't reach the engine's own session
            self.__exit__()
            raise

    def __exit__(self, *exc: object) -> None:
        if self._proc is not None:
            try:
                os.killpg(self._proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            self._proc.wait(timeout=10)
        self._scratch.cleanup()

    def log_tail(self, n: int = 40) -> str:
        return "\n".join((self.home / "engine.log").read_text(errors="replace").splitlines()[-n:])


def wait_for(read: Any, done: Any, *, what: str, timeout: float = 300.0, every: float = 0.1) -> Any:
    """Poll ``read()`` until ``done(value)``; the value, or a clear failure with the last one."""
    deadline = time.monotonic() + timeout
    value = read()
    while not done(value):
        if time.monotonic() > deadline:
            raise RuntimeError(f"timed out waiting for {what}; last value: {value!r}")
        time.sleep(every)
        value = read()
    return value
