"""DBOS runtime adapter (ADR-0035, an internal issue).

Subprocess-driven because DBOS is a process singleton and its async checkpointer
binds to one event loop — each scenario gets a fresh process (see
``_dbos_scenarios.py``). Marked ``dbos`` so they can be deselected
(``-m "not dbos"``) since they spawn processes and are slower than unit tests.
"""
import getpass
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dbos

_HELPER = Path(__file__).parent / "_dbos_scenarios.py"


def _run(scenario: str, workdir: Path, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(_HELPER), scenario, str(workdir)],
        capture_output=True, text=True, timeout=120, env=env,
    )


def _pg_admin_dsn() -> str:
    return f"postgres://{getpass.getuser()}@localhost:5432/postgres"


def _pg_reachable() -> bool:
    try:
        import psycopg
        with psycopg.connect(_pg_admin_dsn(), connect_timeout=2):
            return True
    except Exception:
        return False


def test_dbos_non_gated(tmp_path):
    """A non-gated graph completes under DBOSRuntime (checkpointed since #1850, no interrupt loop)."""
    r = _run("non_gated", tmp_path / "ng")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_gated_resume(tmp_path):
    """A gated graph (sync SqliteSaver, compiler-style) parks at the approval
    interrupt and resumes via DBOSRuntime.resume → recv → Command(resume),
    exercising the sync→async checkpointer recompile."""
    r = _run("gated_resume", tmp_path / "gr")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_parallel_gated_resume(tmp_path):
    """an internal issue regression: a gated graph with PARALLEL fan-out
    (fan→{a,b,c}→gate) parks at the interrupt under DBOSRuntime and resumes to
    completion — the durable runtime handles parallel graphs, not just
    sequential. Also exercises the session-scoped checkpointer (#51)."""
    r = _run("parallel_gated", tmp_path / "par")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


@pytest.mark.skipif(not _pg_reachable(), reason="no local Postgres on :5432")
def test_dbos_parallel_gated_resume_postgres(tmp_path):
    """The #41 regression on the Postgres tier — exercises the pooled,
    session-scoped AsyncPostgresSaver (a connection per run, none held across
    the HITL wait). Fresh DB so the run-id can't collide; dropped after."""
    import psycopg

    db = f"dbos_pgpar_{os.getpid()}"
    admin = _pg_admin_dsn()
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {db}")
        c.execute(f"CREATE DATABASE {db}")
    dsn = f"postgresql://{getpass.getuser()}@localhost:5432/{db}"
    env = {**os.environ, "OBS_DBOS_TEST_PG_DSN": dsn}
    try:
        r = _run("parallel_gated", tmp_path / "parpg", env=env)
        assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr
    finally:
        with psycopg.connect(admin, autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {db}")


def test_dbos_rebuild(tmp_path):
    """rebuild() recompiles installed templates into the registry — the
    boot-time repopulation a fresh process needs to recover PENDING runs."""
    r = _run("rebuild", tmp_path / "rb")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_crash_recovery(tmp_path):
    """Acceptance #1: a gated run killed while PENDING is recovered by
    DBOS.launch() in a fresh process, the SqliteSaver checkpointer resumes graph
    state (step1 NOT re-run), and the run completes after the decision."""
    workdir = tmp_path / "crash"
    submit = _run("crash_submit", workdir)  # parks, then os._exit(0) while PENDING
    assert submit.returncode == 0, submit.stdout + submit.stderr
    recover = _run("crash_recover", workdir)
    assert "RESULT: PASS" in recover.stdout, recover.stdout + recover.stderr


@pytest.mark.skipif(not _pg_reachable(), reason="no local Postgres on :5432")
def test_dbos_crash_recovery_postgres(tmp_path):
    """Same crash-recovery acceptance on the Postgres tier: DBOS system DB +
    AsyncPostgresSaver checkpointer survive a process restart. Creates a fresh
    DB (so the run-id can't collide with a prior run) and drops it after."""
    import psycopg

    db = f"dbos_pgtest_{os.getpid()}"
    admin = _pg_admin_dsn()
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {db}")
        c.execute(f"CREATE DATABASE {db}")
    dsn = f"postgresql://{getpass.getuser()}@localhost:5432/{db}"
    env = {**os.environ, "OBS_DBOS_TEST_PG_DSN": dsn}
    try:
        workdir = tmp_path / "pgcrash"
        submit = _run("crash_submit", workdir, env=env)
        assert submit.returncode == 0, submit.stdout + submit.stderr
        recover = _run("crash_recover", workdir, env=env)
        assert "RESULT: PASS" in recover.stdout, recover.stdout + recover.stderr
    finally:
        with psycopg.connect(admin, autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {db}")


def test_dbos_ungated_crash_recovery(tmp_path):
    """#1850 — an UNGATED graph (no compiler checkpointer) killed mid-run is
    recovered by DBOS.launch() and RESUMES at the next super-step: the completed
    node is not re-run. Before the fix, ungated graphs got no checkpointer at
    all, so recovery restarted at zero and the side log read [n1, n1, n2]."""
    workdir = tmp_path / "ucrash"
    submit = _run("ungated_crash_submit", workdir)
    assert submit.returncode == 0, submit.stdout + submit.stderr
    recover = _run("ungated_crash_recover", workdir)
    assert "RESULT: PASS" in recover.stdout, recover.stdout + recover.stderr


def test_dbos_composed_ungated_subgraph(tmp_path):
    """#1864 — a composed template (subgraph_node with an UNGATED child) completes
    under DBOS. Before the fix the child re-dispatched to DBOSRuntime with the
    parent's run id; DBOS deduplicated it to the running parent and the node
    awaited its own workflow — PENDING forever (observed on prod 2026-09-02).
    The scenario runs prod's exact dispatch path under a 30s deadline."""
    r = _run("composed_ungated", tmp_path / "comp")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_node_events_and_heartbeat(tmp_path):
    """#1865 — a DBOS run emits per-node events + a heartbeat (it had neither:
    the workflow ran on DBOS's background loop where the dispatcher's contextvar
    sink is invisible)."""
    r = _run("node_events", tmp_path / "ne")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_node_events_capture_llm_cost(tmp_path):
    r = _run("node_events_cost", tmp_path / "ne-cost")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr


def test_dbos_graceful_shutdown_leaves_the_workflow_pending(tmp_path):
    """#1872 — cancelling every task on the loop (what uvicorn's asyncio.run does
    on SIGTERM) must leave a parked workflow PENDING, not ERROR. The sync
    start_workflow drove it from a thread-pool worker whose cancelled future
    became a persisted ERROR; invoke() now uses start_workflow_async."""
    r = _run("graceful_cancel", tmp_path / "gc")
    assert "RESULT: PASS" in r.stdout, r.stdout + r.stderr
