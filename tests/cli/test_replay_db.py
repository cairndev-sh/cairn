"""DB-backed `cairn status --replay` (#76 slice 2).

The local-file replay path stays the default; this exercises `--source db`,
which renders a run's trace + full result from the configured run store —
for runs that only live in the DB (e.g. dispatched via the server).
"""
from __future__ import annotations

import asyncio
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import remote, replay
from cairn.cli.main import app
from cairn.core.engine.schema import TriggerEvent
import cairn.core.storage as storage_mod
from cairn.core.storage import Run, SqliteRunStore


def _seed(db: str) -> None:
    async def _go() -> None:
        # Fresh store per loop (sqlite conn is sync; avoids cross-loop reuse).
        s = SqliteRunStore(db)
        await s.create(
            Run(id="run-x", trigger=TriggerEvent(source="manual", matched_pattern="t", service="svc")),
        )
        await s.append_event("run-x", "skill_started", {"skill_name": "t"})
        await s.append_event("run-x", "done", {"matched_skills": 1})
        await s.update(
            "run-x", status="done",
            result_payloads=[{"skill_name": "t", "rca": {"summary": "root cause"}}],
        )
        s.close()
    asyncio.run(_go())


def test_db_replay_renders_run_events_and_result(tmp_path, monkeypatch, capsys):
    db = str(tmp_path / "runs.db")
    _seed(db)
    # _run_from_db builds the store via default_store(); point it at our file.
    monkeypatch.setattr(storage_mod, "default_store", lambda: SqliteRunStore(db))

    rc = replay._run("run-x", json=True, source="db")
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["meta"]["status"] == "done"
    assert [e["type"] for e in out["events"]] == ["skill_started", "done"]
    # slice-1 full result rides through DB replay
    assert out["result_payloads"][0]["rca"]["summary"] == "root cause"


def test_db_replay_missing_run_is_clean_error(tmp_path, monkeypatch, capsys):
    db = str(tmp_path / "empty.db")
    monkeypatch.setattr(storage_mod, "default_store", lambda: SqliteRunStore(db))
    rc = replay._run("nope", json=True, source="db")
    assert rc == 1
    err = capsys.readouterr().err
    assert "no such run in the DB store" in err
    assert "the engine is asked too" in err  # #2536, #369 — a server's run is read over the API


# ─── end-to-end via the real env-driven path (no default_store monkeypatch) ──
# The tests above patch default_store; these prove the WHOLE chain on sqlite:
# OBS_STORAGE=sqlite://<file> → default_store() → SqliteRunStore → replay. That
# env→store URL resolution is what was actually broken (#76) — patching around
# default_store hid it.


def _seed_via_env(db, run_id: str, *, with_result: bool) -> None:
    async def _go() -> None:
        from cairn.core.storage import default_store
        s = default_store()                          # resolves from OBS_STORAGE
        assert getattr(s, "path", None) == str(db)   # the URL mapped to our file
        await s.create(
            Run(id=run_id, trigger=TriggerEvent(source="manual", matched_pattern="t", service="svc")),
        )
        await s.append_event(run_id, "skill_started", {"skill_name": "t"})
        await s.append_event(run_id, "done", {"matched_skills": 1})
        fields = {"status": "done"}
        if with_result:
            fields["result_payloads"] = [{"skill_name": "t", "rca": {"summary": "root cause"}}]
        await s.update(run_id, **fields)
        s.close()
    asyncio.run(_go())


def test_db_replay_end_to_end_via_obs_storage_url(tmp_path, monkeypatch, capsys):
    db = tmp_path / "runs.db"
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{db}")   # sqlite:// + abs path
    monkeypatch.delenv("OBS_RUN_DB", raising=False)
    _seed_via_env(db, "run-e2e", with_result=True)

    rc = replay._run("run-e2e", json=True, source="db")
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["meta"]["status"] == "done"
    assert [e["type"] for e in out["events"]] == ["skill_started", "done"]
    assert out["result_payloads"][0]["rca"]["summary"] == "root cause"


def test_db_replay_auto_falls_back_to_db_store_via_env(tmp_path, monkeypatch, capsys):
    """`--source auto` with no on-disk trace falls back to the env-resolved DB
    store — the path a server-dispatched run takes."""
    db = tmp_path / "runs.db"
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{db}")
    monkeypatch.delenv("OBS_RUN_DB", raising=False)
    # force "no local files" deterministically so auto must reach the DB store
    monkeypatch.setattr(replay, "run_dir_exists", lambda rid: False)
    _seed_via_env(db, "run-auto", with_result=False)

    rc = replay._run("run-auto", json=False, source="auto")
    assert rc == 0
    assert "status:      done" in capsys.readouterr().out


# ─── #369: a run the CLI's own environment doesn't name a store for ──
# The real engine and CLI are in tests/e2e/hello/test_status_replay_local.py; these pin the choices
# between the sources, which that test can only take one way each.


class _Engine:
    """What `cairn-client` returns for a task leg, as the engine does."""

    def __init__(self):
        self.seen = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def run(self, run_id):
        self.seen.append(run_id)
        return {"id": run_id, "status": "done", "environment": "sandbox", "template_ref": "task:data/profile",
                "inputs": {"source": "/data.jsonl"}, "resolved_operators": []}

    def run_events(self, run_id):
        return {"events": [{"seq": 1, "ts": "2026-11-02T10:00:00+00:00", "type": "done", "data": {"leg": "x"}}]}


@pytest.fixture
def no_store(tmp_path, monkeypatch):
    """A HOME with nothing in it and a process with no store configured; booting an engine fails."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path / ".cairn"))
    monkeypatch.chdir(tmp_path)
    for name in ("STORAGE", "RUN_DB", "REMOTE_ENDPOINT", "REMOTE_TOKEN"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)
    monkeypatch.setattr(remote.subprocess, "Popen", lambda *a, **k: pytest.fail("this command boots no engine"))
    return tmp_path


def test_a_run_only_the_engine_has_is_read_from_the_engine(no_store, monkeypatch, capsys):
    engine = _Engine()
    monkeypatch.setattr(remote, "_reachable", lambda ep: True)
    monkeypatch.setattr(remote, "_api", lambda ep, tok: engine)
    assert replay._run("run-1", json=False) == 0
    out = capsys.readouterr().out
    assert engine.seen == ["run-1"]
    assert "operators:   data/profile" in out and "source = /data.jsonl" in out and "environment: sandbox" in out


def test_the_endpoint_named_on_the_command_is_the_one_asked(no_store, monkeypatch, capsys):
    asked = []
    monkeypatch.setattr(remote, "_reachable", lambda ep: asked.append(ep) or True)
    monkeypatch.setattr(remote, "_api", lambda ep, tok: _Engine())
    assert replay._run("run-1", json=True, endpoint="http://there:1") == 0
    assert asked == ["http://there:1"]


def test_no_engine_reads_the_local_store_and_says_so_and_boots_nothing(no_store, monkeypatch, capsys):
    (no_store / ".cairn").mkdir()
    db = no_store / ".cairn" / "dev.db"
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{db}")
    _seed_via_env(db, "run-1", with_result=False)
    monkeypatch.delenv("CAIRN_STORAGE")
    monkeypatch.setattr(remote, "_reachable", lambda ep: False)
    assert replay._run("run-1", json=True) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["meta"]["status"] == "done"
    assert str(db) in captured.err


def test_nowhere_says_where_it_looked(no_store, monkeypatch, capsys):
    monkeypatch.setattr(remote, "_reachable", lambda ep: False)
    assert replay._run("run-1", json=False) == 1
    err = capsys.readouterr().err
    assert "no such run: run-1" in err and "no engine answers at" in err
    assert not (no_store / ".cairn" / "dev.db").exists(), "looking creates no store"


def test_the_operators_are_what_the_record_names():
    events = [{"type": "parameters", "data": {"operator": "llm/complete"}},
              {"type": "parameters", "data": {"operator": "llm/complete"}},
              {"type": "parameters", "data": {"operator": "flow:p/f"}},
              {"type": "done", "data": {"operator": "not-this"}}]
    assert replay._operators({"template_ref": "t", "resolved_operators": [{"operator": "a/b"}]}, events) == \
        ["a/b", "llm/complete"]
    assert replay._operators({"template_ref": "task:data/profile"}, []) == ["data/profile"]
    assert replay._operators({"template_ref": "flow:p/f"}, []) == []


# ─── a named engine is never silently replaced by local data (the rule above remote._explicit_endpoint) ──

DEAD_REMOTE = "http://prod.example.invalid:1"
DEAD_LOCAL = "http://127.0.0.1:1"  # port 1: nothing listens there


def _seed_dev_db(home, monkeypatch, run_id="run-x"):
    (home / ".cairn").mkdir(exist_ok=True)
    db = home / ".cairn" / "dev.db"
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{db}")
    _seed_via_env(db, run_id, with_result=False)
    monkeypatch.delenv("CAIRN_STORAGE")
    return db


def _save_endpoint(endpoint):
    remote._save_config({"default_profile": "default", "profiles": {"default": {"endpoint": endpoint, "token": "t"}}})


@pytest.mark.parametrize("name_it", ["flag", "env", "saved"])
def test_a_dead_remote_engine_fails_and_local_data_never_stands_in(no_store, monkeypatch, name_it):
    _seed_dev_db(no_store, monkeypatch)
    args = ["status", "run-x", "--replay", "--json"]
    if name_it == "flag":
        args += ["--endpoint", DEAD_REMOTE]
    elif name_it == "env":
        monkeypatch.setenv("CAIRN_REMOTE_ENDPOINT", DEAD_REMOTE)
    else:
        _save_endpoint(DEAD_REMOTE)
    r = CliRunner().invoke(app, args)
    assert r.exit_code != 0, r.stdout
    assert r.stdout == ""
    assert DEAD_REMOTE in r.stderr


def test_a_named_engine_that_is_down_fails_even_when_the_configured_store_has_the_run(no_store, monkeypatch):
    db = _seed_dev_db(no_store, monkeypatch)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{db}")
    r = CliRunner().invoke(app, ["status", "run-x", "--replay", "--endpoint", DEAD_REMOTE])
    assert r.exit_code != 0, r.stdout
    assert r.stdout == "" and DEAD_REMOTE in r.stderr


def test_a_named_engine_is_asked_before_the_configured_store_and_a_local_trace(no_store, monkeypatch):
    db = _seed_dev_db(no_store, monkeypatch)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{db}")
    monkeypatch.setattr(replay, "run_dir_exists", lambda rid: True)
    engine = _Engine()
    monkeypatch.setattr(remote, "_reachable", lambda ep: True)
    monkeypatch.setattr(remote, "_api", lambda ep, tok: engine)
    r = CliRunner().invoke(app, ["status", "run-x", "--replay", "--json", "--endpoint", "http://there:1"])
    assert r.exit_code == 0, r.stderr
    assert engine.seen == ["run-x"]
    assert json.loads(r.stdout)["meta"]["source"] == "engine"


def test_naming_a_local_engine_that_is_down_fails_too(no_store, monkeypatch):
    _seed_dev_db(no_store, monkeypatch)
    r = CliRunner().invoke(app, ["status", "run-x", "--replay", "--endpoint", DEAD_LOCAL])
    assert r.exit_code != 0, r.stdout
    assert r.stdout == "" and DEAD_LOCAL in r.stderr


@pytest.mark.parametrize("how", ["default", "env", "saved-127", "saved-localhost", "saved-ipv6"])
def test_a_loopback_engine_that_is_down_falls_back_to_the_local_store(no_store, monkeypatch, how):
    """Not named on this command: `cairn setup --server` saves the endpoint in the profile, and a
    local engine that has stopped still leaves its record on this machine."""
    db = _seed_dev_db(no_store, monkeypatch)
    if how == "env":
        monkeypatch.setenv("CAIRN_REMOTE_ENDPOINT", DEAD_LOCAL)
    elif how != "default":
        _save_endpoint({"saved-127": DEAD_LOCAL, "saved-localhost": "http://localhost:1",
                        "saved-ipv6": "http://[::1]:1"}[how])
    r = CliRunner().invoke(app, ["status", "run-x", "--replay", "--json"])
    assert r.exit_code == 0, r.stderr
    assert json.loads(r.stdout)["meta"]["source"] == "dev-db"
    assert str(db) in r.stderr


def test_every_source_is_named_in_the_json_and_on_stderr(no_store, monkeypatch, capsys):
    db = _seed_dev_db(no_store, monkeypatch)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite://{db}?secret=hunter2")
    monkeypatch.setattr(storage_mod, "default_store", lambda: SqliteRunStore(str(db)))
    assert replay._run("run-x", json=True) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["meta"]["source"] == "db"
    assert "configured store" in captured.err and "hunter2" not in captured.err, "a DSN can hold a password"

    assert replay._run("run-x", json=True, source="db") == 0
    assert json.loads(capsys.readouterr().out)["meta"]["source"] == "db"

    monkeypatch.setattr(storage_mod, "default_store", lambda: SqliteRunStore(str(no_store / "empty.db")))
    monkeypatch.setattr(remote, "_reachable", lambda ep: True)
    monkeypatch.setattr(remote, "_api", lambda ep, tok: _Engine())
    assert replay._run("run-x", json=True) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["meta"]["source"] == "engine"
    assert "the engine at" in captured.err


def test_a_local_trace_is_named_as_the_source(no_store, monkeypatch, capsys):
    from cairn.cli.runs import write_meta  # noqa: PLC0415

    write_meta("run-t", {"status": "done"})
    assert replay._run("run-t", json=True) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["meta"]["source"] == "file"
    assert "local trace" in captured.err


@pytest.mark.parametrize("source", ["file", "db"])
def test_a_named_engine_does_not_go_with_a_source_that_would_ignore_it(no_store, source):
    r = CliRunner().invoke(app, ["status", "run-x", "--replay", "--source", source, "--endpoint", DEAD_LOCAL])
    assert r.exit_code != 0 and "--endpoint" in r.stderr


def test_nowhere_offers_no_option_the_command_does_not_have(no_store, monkeypatch):
    monkeypatch.setattr(remote, "_reachable", lambda ep: False)
    r = CliRunner().invoke(app, ["status", "run-x", "--replay"])
    assert r.exit_code == 1 and "--endpoint" not in r.stderr


def test_a_pipeline_is_pointed_at_its_runs(no_store):
    r = CliRunner().invoke(app, ["status", "pl_abc", "--replay"])
    assert r.exit_code == 1 and "cairn status pl_abc --logs" in r.stderr


@pytest.mark.parametrize("endpoint, loopback", [
    ("http://localhost:8000", True),
    ("http://LOCALHOST:8000", True),
    ("http://127.0.0.1:8000", True),
    ("http://127.1.2.3:8000", True),
    ("http://[::1]:8000", True),
    ("http://evil.example@localhost:8000", True),   # the host really is localhost
    ("http://localhost.evil.example:8000", False),
    ("http://127.0.0.1.evil.example:8000", False),
    ("http://localhost@evil.example:8000", False),
    ("http://evil.example/localhost", False),
    ("http://evil.example#@localhost", False),
    ("http://0.0.0.0:8000", False),
    ("http://[::1", False),                          # malformed: fails closed, no traceback
    ("localhost:8000", False),
])
def test_only_a_host_that_is_this_machine_counts_as_loopback(endpoint, loopback):
    """The local store stands in only for a local engine, so a look-alike must not pass (#369)."""
    assert replay._is_loopback(endpoint) is loopback


def test_a_password_in_the_endpoint_is_never_printed(no_store, monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    r = CliRunner().invoke(
        app, ["status", "run-x", "--replay", "--endpoint", "http://user:hunter2@prod.example.invalid:1"])
    assert r.exit_code == 1
    assert "hunter2" not in r.output and "user:" not in r.output
    assert "prod.example.invalid" in r.output
