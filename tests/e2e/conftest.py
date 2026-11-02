"""The end-to-end harness (ADR-0127 D1, hello-cairn WP7): a REAL ``cairn-server`` subprocess
per cell, driven over HTTP and the CLI exactly as a user would, against the light stack's
``proof`` profile when it answers.

The matrix cell
---------------
A cell is ``storage × runtime × bus`` (:class:`Cell`), all 2 × 2 × 2 = 8 (ADR-0127 D1). The
three selectors are independent: ``CAIRN_STORAGE`` (``cairn/core/storage/__init__.py``:
``sqlite://`` or ``postgres://``), ``CAIRN_BUS`` (``cairn/core/bus/__init__.py``
``bus_backend``: ``inprocess`` or a Postgres DSN; ``redis`` refused) and ``CAIRN_RUNTIME``
(``cairn/core/runtime/__init__.py`` ``select_runtime``: ``inprocess`` or ``dbos``, whose
system database ``CAIRN_DBOS_DSN`` is SQLite or Postgres). The light and prod stacks ship
``CAIRN_STORAGE=postgres`` with ``CAIRN_BUS`` unset (an in-process bus), so no cell is
pruned; a cell whose store or bus is Postgres is marked ``services`` and skips, with the
reason, when the proof stack does not answer.

Opt-in: every test here spawns real servers, so the directory is skipped unless
``CAIRN_E2E=1`` (set by ``scripts/oss/feature_proof.sh`` and the ``public-e2e`` job).

Fixtures
--------
``server_factory(cell, name)`` starts a server for a cell in a scratch HOME on a free port
(skipping, with the reason, when the cell needs services that do not answer) and tears it
down with SIGKILL on the whole process group, so an orphaned builtin-engine subprocess
never outlives the test. ``Server.kill()`` + ``Server.start()`` is the crash cell.
``Api`` is the thin HTTP driver; ``cli()`` runs the ``cairn`` CLI with the server as its
endpoint. ``model_stub`` (session) is the keyless OpenAI-compatible model every server is
pointed at (``OPENAI_BASE_URL``; ``tests/e2e/model_stub.py``, #2607): the engine has no
built-in mock model, and ask-the-docs makes a real model call and embeds. Nothing here duplicates a fixture under ``tests/core`` — the in-process server
there is ``build_app`` + a TestClient; this harness needs a separate process to kill.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import httpx
import pytest

from tests.e2e.model_stub import ModelStub

REPO = Path(__file__).resolve().parents[2]
HELLO = REPO / "examples" / "hello-cairn"
#: the example's own deployment profile (WP6): builtin bindings, ``secrets.store`` →
#: secrets/env and the ``hello-calc`` MCP server the pack declares
PROFILE = HELLO / "profiles" / "hello.toml"
#: how the engine starts the pack's stdio MCP server (the README's CAIRN_MCP_SERVERS)
MCP_SERVERS = json.dumps({"hello-calc": {"transport": "stdio", "command": sys.executable,
                                         "args": [str(HELLO / "tools" / "calc_server.py")]}})

PG_DSN = os.environ.get("CAIRN_PROOF_PG_DSN", "postgres://cairn:cairn@127.0.0.1:55432/cairn")
SSH = os.environ.get("CAIRN_PROOF_SSH", "127.0.0.1:2222")
SSH_USER = os.environ.get("PROOF_SSH_USER", "cairn")
SSH_KEY = os.environ.get("CAIRN_PROOF_SSH_KEY", str(REPO / "deploy/light/.proof/ssh/proof_ed25519"))

PIPELINE_TERMINAL = {"done", "failed", "cancelled"}


@dataclass(frozen=True)
class Cell:
    storage: str   # sqlite | postgres
    runtime: str   # inprocess | dbos
    bus: str       # inprocess | postgres

    @property
    def id(self) -> str:
        return f"{self.storage}-{self.runtime}-{self.bus}"

    @property
    def needs_services(self) -> bool:
        return self.storage == "postgres" or self.bus == "postgres"


#: Every cell (the module docstring says why none is pruned).
CELLS: tuple[Cell, ...] = tuple(
    Cell(storage, runtime, bus)
    for storage in ("sqlite", "postgres")
    for runtime in ("inprocess", "dbos")
    for bus in ("inprocess", "postgres")
)
assert len(CELLS) == 8, CELLS


def cell_params() -> list:
    """``pytest.param`` per cell, marked ``services`` when it needs the proof stack."""
    return [pytest.param(c, id=c.id, marks=[pytest.mark.services] if c.needs_services else [])
            for c in CELLS]


# ── what answers ──────────────────────────────────────────────────────────

def pg_reachable(dsn: str = PG_DSN) -> Optional[str]:
    """None when Postgres answers; else the reason it does not."""
    try:
        import psycopg  # noqa: PLC0415
    except ModuleNotFoundError:
        return "psycopg is not installed (the dev extra)"
    try:
        with psycopg.connect(dsn, connect_timeout=2):
            return None
    except Exception as e:  # noqa: BLE001 — any failure is one reason
        return f"Postgres at {dsn.rsplit('@', 1)[-1]} does not answer ({type(e).__name__}); " \
               "bring up `docker compose --profile proof` in deploy/light (scripts/oss/feature_proof.sh)"


def tcp_reachable(hostport: str) -> bool:
    host, _, port = hostport.rpartition(":")
    try:
        with socket.create_connection((host or "127.0.0.1", int(port)), timeout=1.5):
            return True
    except OSError:
        return False


def train_available() -> Optional[str]:
    """None when the ``[train]`` extra imports; else the reason the train rows skip."""
    for mod in ("torch", "transformers", "peft"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001 — a broken import is the same as a missing one
            return f"the [train] extra is not installed ({mod} does not import)"
    return None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── a scratch Postgres database per server ─────────────────────────────────

def _pg_admin(dsn: str, sql: str) -> None:
    import psycopg  # noqa: PLC0415

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        conn.execute(sql)


def create_database(name: str) -> str:
    """A fresh database on the proof Postgres; returns its DSN."""
    _pg_admin(PG_DSN, f'CREATE DATABASE "{name}"')
    base, _, _ = PG_DSN.rpartition("/")
    return f"{base}/{name}"


def drop_database(name: str) -> None:
    try:
        _pg_admin(PG_DSN, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                          f"WHERE datname = '{name}' AND pid <> pg_backend_pid()")
        _pg_admin(PG_DSN, f'DROP DATABASE IF EXISTS "{name}"')
    except Exception:  # noqa: BLE001 — a leftover scratch database is not a test failure
        pass


# ── the server subprocess ─────────────────────────────────────────────────

class Server:
    """One ``cairn-server`` process for one cell, in a scratch HOME, on a free port."""

    def __init__(self, cell: Cell, home: Path, *, pack_path: str, model_url: str, dsn: str = "",
                 extra_env: Optional[dict[str, str]] = None) -> None:
        self.cell = cell
        self.home = home
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.dsn = dsn
        self.proc: Optional[subprocess.Popen] = None
        self._groups: list[int] = []      # every process group this server ever started
        self._clients: list[Api] = []
        self.log = home / "server.log"
        self.artifacts = home / "artifacts"
        self.audit_log = home / "audit.jsonl"
        self.env = self._env(pack_path, model_url, extra_env or {})

    def _env(self, pack_path: str, model_url: str, extra: dict[str, str]) -> dict[str, str]:
        home = self.home
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("CAIRN_", "OBS_"))}   # the cell's env, nothing inherited
        storage = self.dsn if self.cell.storage == "postgres" else f"sqlite:///{home / 'runs.db'}"
        env.update({
            "HOME": str(home),
            "PYTHONPATH": str(REPO),
            "PYTHONUNBUFFERED": "1",
            # the model and the embedder: the session's keyless stub (tests/e2e/model_stub.py,
            # #2607) behind the same OpenAI client a hosted provider would get
            "OPENAI_API_KEY": "sk-proof-dummy",
            "OPENAI_BASE_URL": model_url,
            "RAG_EMBED_PROVIDER": "openai",
            "CAIRN_RAG_AUTOINDEX": "1",      # index the pack's rag/docs at boot
            "CAIRN_MCP_SERVERS": MCP_SERVERS,
            "CAIRN_PROFILE": str(PROFILE),
            "CAIRN_BACKEND": "mock",
            "CAIRN_PACK_PATH": pack_path,
            "CAIRN_TRIGGER_HOST": "127.0.0.1",
            "CAIRN_TRIGGER_PORT": str(self.port),
            "CAIRN_JWT_SECRET": "",          # keyless, loopback
            "CAIRN_STORAGE": storage,
            "CAIRN_RUNTIME": self.cell.runtime,
            "CAIRN_BUS": self.dsn if self.cell.bus == "postgres" else "inprocess",
            "CAIRN_ARTIFACTS_DIR": str(self.artifacts),
            "OBS_ARTIFACTS_DIR": str(self.artifacts),   # ADR-0122: a pack built before it reads OBS_
            "CAIRN_AUDIT_LOG": str(self.audit_log),
            "OBS_AUDIT_LOG": str(self.audit_log),
            # the crash cells wait for the owner verdict (ADR-0125 D3): sweep often, no grace
            "CAIRN_RECONCILE_SWEEP_SECONDS": "2",
            "CAIRN_RECONCILE_GRACE_SECONDS": "0",
        })
        if self.cell.runtime == "dbos":
            if self.cell.storage == "postgres":
                env.update({"CAIRN_DBOS_DSN": self.dsn, "CAIRN_CHECKPOINTER": "postgres",
                            "CAIRN_CHECKPOINTER_DSN": self.dsn})
            else:
                env.update({"CAIRN_DBOS_DSN": f"sqlite:///{home / 'dbos.sqlite'}",
                            "CAIRN_CHECKPOINTER": "sqlite",
                            "CAIRN_CHECKPOINTER_PATH": str(home / "ckpt.sqlite")})
        elif self.cell.storage == "postgres":
            env.update({"CAIRN_CHECKPOINTER": "postgres", "CAIRN_CHECKPOINTER_DSN": self.dsn})
        env.update(extra)
        return env

    def start(self, timeout: float = 120.0) -> "Server":
        assert self.proc is None or self.proc.poll() is not None, "already running"
        self.home.mkdir(parents=True, exist_ok=True)
        with open(self.log, "ab") as log:
            log.write(b"\n=== cairn-server start ===\n")
            self.proc = subprocess.Popen(
                [sys.executable, "-c",
                 "from cairn.core.triggers.main import main; raise SystemExit(main())"],
                cwd=str(REPO), env=self.env, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,   # its own group: SIGKILL reaches engine subprocesses
            )
        self._groups.append(self.proc.pid)   # the session leader's pid is the group id
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"cairn-server exited {self.proc.returncode} at boot:\n{self.tail()}")
            try:
                if httpx.get(f"{self.url}/healthz", timeout=2.0).status_code < 500:
                    return self
            except httpx.HTTPError:
                pass
            time.sleep(0.25)
        raise RuntimeError(f"cairn-server did not answer /healthz in {timeout}s:\n{self.tail()}")

    def kill(self) -> None:
        """SIGKILL the server process only — the crash. Its engine subprocesses are left
        to the kernel (they outlive it, exactly as in a real crash)."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)

    def stop(self) -> None:
        """Teardown: EVERY process group this server started (a crash cell's first boot
        too, whose engine subprocess outlived the SIGKILL), and its HTTP clients."""
        for client in self._clients:
            client.close()
        self._clients.clear()
        for pgid in self._groups:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        self._groups.clear()
        if self.proc is not None and self.proc.poll() is None:
            self.proc.wait(timeout=10)

    def tail(self, n: int = 40) -> str:
        try:
            return "\n".join(self.log.read_text(errors="replace").splitlines()[-n:])
        except OSError:
            return ""

    def api(self) -> "Api":
        client = Api(self.url)
        self._clients.append(client)
        return client


class Api:
    """The HTTP driver — the same doors the console and the ``cairn`` CLI use."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.c = httpx.Client(base_url=url, timeout=60.0)

    def close(self) -> None:
        self.c.close()

    def _ok(self, r: httpx.Response) -> Any:
        if r.status_code >= 400:
            raise AssertionError(f"{r.request.method} {r.request.url.path} → {r.status_code}: {r.text[:500]}")
        return r.json()

    def start(self, ref: str, inputs: Optional[dict] = None) -> str:
        out = self._ok(self.c.post("/start", json={"ref": ref, "inputs": inputs or {}}))
        return out["pipeline_id"]

    def pipeline(self, pid: str) -> dict:
        return self._ok(self.c.get(f"/pipelines/{pid}"))

    def wait(self, pid: str, until: Callable[[dict], bool], *, timeout: float = 120.0,
             what: str = "condition") -> dict:
        deadline = time.monotonic() + timeout
        rec = self.pipeline(pid)
        while time.monotonic() < deadline:
            rec = self.pipeline(pid)
            if until(rec):
                return rec
            if rec["status"] in PIPELINE_TERMINAL:
                break
            time.sleep(0.5)
        steps = {k: v.get("status") for k, v in rec.get("step_state", {}).items()}
        raise AssertionError(f"pipeline {pid} never reached {what}: status={rec['status']} steps={steps} "
                             f"failure={rec.get('trigger', {}).get('failure_reason', '')!r}")

    def wait_terminal(self, pid: str, **kw) -> dict:
        return self.wait(pid, lambda r: r["status"] in PIPELINE_TERMINAL, what="a terminal state", **kw)

    def wait_step(self, pid: str, step: str, status: str, **kw) -> dict:
        return self.wait(pid, lambda r: r["step_state"].get(step, {}).get("status") == status,
                         what=f"step {step} at {status}", **kw)

    def approve(self, pid: str, step: str, *, approver: str = "proof", rationale: str = "") -> dict:
        return self._ok(self.c.post(f"/pipelines/{pid}/steps/{step}/approve",
                                    json={"approver": approver, "rationale": rationale}))

    def advance(self, pid: str, step: str) -> dict:
        return self._ok(self.c.post(f"/pipelines/{pid}/steps/{step}/advance", json={}))

    def run(self, run_id: str) -> dict:
        return self._ok(self.c.get(f"/runs/{run_id}"))

    def output(self, run_id: str) -> dict:
        """The leg's output as its run recorded it (``result_payloads[0].output``)."""
        payloads = self.run(run_id).get("result_payloads") or [{}]
        return (payloads[0] or {}).get("output") or {}

    def runs(self, **params: Any) -> list[dict]:
        out = self._ok(self.c.get("/runs", params={"limit": 500, **params}))
        return out["investigations"]   # the listing's historical key

    def resume_run(self, run_id: str, decision: str = "approve", approver: str = "proof") -> dict:
        return self._ok(self.c.post(f"/runs/{run_id}/resume",
                                    json={"decision": decision, "approver": approver}))


def dbos_workflow(server: Server, workflow_id: str) -> Optional[tuple[str, str]]:
    """``(status, name)`` of the DBOS workflow ``workflow_id`` in the cell's DBOS system
    database (``CAIRN_DBOS_DSN``), or None. A template run's workflow id IS its run id
    (``dispatcher.py``, #38), so this row is the runtime's own record that DBOS ran it."""
    if server.cell.storage == "postgres":
        import psycopg  # noqa: PLC0415

        with psycopg.connect(server.dsn, connect_timeout=5) as conn:
            row = conn.execute("SELECT status, name FROM dbos.workflow_status WHERE workflow_uuid = %s",
                               (workflow_id,)).fetchone()
    else:
        import sqlite3  # noqa: PLC0415

        conn = sqlite3.connect(str(server.home / "dbos.sqlite"))
        try:
            row = conn.execute("SELECT status, name FROM workflow_status WHERE workflow_uuid = ?",
                               (workflow_id,)).fetchone()
        finally:
            conn.close()
    return (row[0], row[1]) if row else None


def cli(server: Server, *args: str, timeout: float = 120.0) -> subprocess.CompletedProcess:
    """Run the ``cairn`` CLI against ``server`` (``CAIRN_REMOTE_ENDPOINT``), as a user would."""
    env = {**server.env, "CAIRN_REMOTE_ENDPOINT": server.url}
    env.pop("CAIRN_TRIGGER_PORT", None)
    return subprocess.run(
        [sys.executable, "-c", "from cairn.cli.main import app; app()", *args],
        cwd=str(REPO), env=env, capture_output=True, text=True, timeout=timeout,
    )


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def model_stub():
    """The session's keyless OpenAI-compatible model (#2607): every server points at it."""
    stub = ModelStub().start()
    yield stub
    stub.stop()


@pytest.fixture
def server_factory(request, tmp_path_factory, model_stub):
    """``make(cell, name, pack_path=..., extra_env=...) -> Server`` (started). Skips when
    the cell's services do not answer; stops every server it made at teardown."""
    made: list[Server] = []
    dbs: list[str] = []

    def make(cell: Cell, name: str, *, pack_path: str = str(HELLO),
             extra_env: Optional[dict[str, str]] = None) -> Server:
        dsn = ""
        if cell.needs_services:
            why = pg_reachable()
            if why:
                pytest.skip(f"services cell {cell.id}: {why}")
            db = f"proof_{name}_{uuid.uuid4().hex[:8]}".replace("-", "_")
            dsn = create_database(db)
            dbs.append(db)
        if cell.storage == "postgres":
            # the cell's CAIRN_CHECKPOINTER=postgres, as the light and prod stacks set it
            pytest.importorskip("langgraph.checkpoint.postgres",
                                reason="the runtime-postgres extra is not installed (the Postgres checkpointer)")
            pytest.importorskip("psycopg_pool", reason="the runtime-postgres extra is not installed (psycopg_pool)")
        if cell.runtime == "dbos":
            pytest.importorskip("dbos", reason="the runtime-dbos extra is not installed")
            import sqlite3  # noqa: PLC0415

            if cell.storage == "sqlite" and sqlite3.sqlite_version_info < (3, 42, 0):
                # the engine refuses this loudly (#2655: DBOS's SQLite system DB uses
                # unixepoch('subsec'), NULL before 3.42); the cell names it instead of failing
                pytest.skip(f"cell {cell.id}: this Python links SQLite {sqlite3.sqlite_version}; "
                            "DBOS's SQLite system DB needs >= 3.42 (use a uv-managed Python)")
        home = tmp_path_factory.mktemp(f"home-{cell.id}-{name}")
        srv = Server(cell, home, pack_path=pack_path, model_url=model_stub.base_url, dsn=dsn,
                     extra_env=extra_env)
        made.append(srv)
        return srv.start()

    yield make
    for srv in made:
        srv.stop()
        if getattr(request.node, "rep_failed", False):
            print(f"\n--- {srv.cell.id} server log tail ---\n{srv.tail(60)}")
    for db in dbs:
        drop_database(db)
    for srv in made:
        shutil.rmtree(srv.home, ignore_errors=True)


def pytest_collection_modifyitems(config, items):
    """The opt-in: a bare ``pytest`` never spawns servers (tests/CLAUDE.md)."""
    if os.environ.get("CAIRN_E2E") == "1":
        return
    skip = pytest.mark.skip(reason="tests/e2e spawns real cairn-server processes: set CAIRN_E2E=1 "
                                   "(scripts/oss/feature_proof.sh and the public-e2e job do)")
    for item in items:
        if item.get_closest_marker("e2e"):
            item.add_marker(skip)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Mark the item failed so the server log tail is printed at teardown."""
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call" and rep.failed:
        item.rep_failed = True
