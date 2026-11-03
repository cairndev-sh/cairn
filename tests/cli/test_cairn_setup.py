"""ADR-0165 D2 — `cairn setup`: start a local engine or connect to a server, and check that it works.

The engine process and the HTTP seam are stubs: what's under test is what this command starts, what it
signs in to, what it turns on and what it leaves behind. A real engine is what the live proof drives.
"""
from __future__ import annotations

import json
import os
import signal
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import dev as dev_cli
from cairn.cli.commands import doctor, remote, setup
from cairn.cli.main import app

cli = CliRunner()
SFT = "pipeline:finetune/sft-local"


class _Client:
    def __init__(self, triggers=()):
        self.triggers = list(triggers)
        self.enabled: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def login(self, username, password):
        return f"tok-{username}"

    def stored_triggers(self):
        return {"triggers": self.triggers}

    def set_trigger_enabled(self, trigger_id, enabled):
        assert enabled is True
        self.enabled.append(trigger_id)
        return {"id": trigger_id, "enabled": True}


class _Proc:
    pid = 4242
    returncode = None

    def __init__(self):
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True


@pytest.fixture
def world(tmp_path, monkeypatch):
    """No engine anywhere, a config home of its own, and a recording process seam."""
    monkeypatch.setenv("CAIRN_CONFIG_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("CAIRN_REMOTE_ENDPOINT", raising=False)
    monkeypatch.delenv("CAIRN_REMOTE_TOKEN", raising=False)
    monkeypatch.delenv("CAIRN_PROFILE", raising=False)
    monkeypatch.delenv("CAIRN_TRIGGER_HOST", raising=False)
    w = type("World", (), {})()
    w.client = _Client([{"id": "t-sft", "declared_by": SFT}, {"id": "t-other", "declared_by": "pipeline:a/b"}])
    w.started, w.answers, w.proc, w.checks, w.ours = [], [False, False, True], _Proc(), [0], set()

    def popen(argv, **kw):
        w.started.append((argv, kw))
        w.ours.add(w.proc.pid)
        return w.proc

    monkeypatch.setattr(setup.subprocess, "Popen", popen)
    monkeypatch.setattr(setup, "_is_ours", lambda pid: pid in w.ours)
    monkeypatch.setattr(remote, "_reachable",
                        lambda ep: (w.answers.pop(0) if len(w.answers) > 1 else w.answers[0]))
    monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: w.client)
    monkeypatch.setattr(setup, "_run_checks", lambda env: w.checks.__setitem__(0, w.checks[0]) or w.checks[0])
    monkeypatch.setattr(setup.time, "sleep", lambda *_a: None)
    return w


def _config() -> dict:
    return remote._load_config()


def test_it_starts_the_engine_waits_signs_in_runs_the_checks_and_returns(world):
    r = cli.invoke(app, ["setup"])
    assert r.exit_code == 0, r.output
    (argv, kw), = world.started
    assert argv[-2:] == ["-m", "cairn.core.triggers.main"]
    assert kw["start_new_session"] is True, "it has to outlive this command"
    assert kw["env"]["CAIRN_TRIGGER_PORT"] == "9090"
    assert kw["env"]["CAIRN_TRIGGER_HOST"] == "127.0.0.1"
    assert world.answers == [True], "returned only once the engine answered"
    cfg = _config()
    assert cfg["default_profile"] == setup._LOCAL_PROFILE
    assert cfg["profiles"][setup._LOCAL_PROFILE] == {"endpoint": "http://127.0.0.1:9090", "token": "tok-dev"}
    assert json.loads(setup._state_path().read_text())["pid"] == 4242
    assert "cairn run" in r.stdout


def test_a_second_setup_reports_the_engine_and_starts_no_other(world):
    assert cli.invoke(app, ["setup"]).exit_code == 0
    world.started.clear()
    r = cli.invoke(app, ["setup"])
    assert r.exit_code == 0, r.output
    assert world.started == []
    assert "already running" in r.stdout and "4242" in r.stdout


@pytest.mark.parametrize("flags", [("--port", "9999"), ("--db", "/tmp/other.db")])
def test_asking_a_running_engine_for_something_else_says_to_stop_it_first(world, flags):
    assert cli.invoke(app, ["setup"]).exit_code == 0
    world.started.clear()
    r = cli.invoke(app, ["setup", *flags])
    assert r.exit_code == 1, r.output
    assert "cairn setup --stop" in r.output and world.started == []


def test_an_engine_this_command_did_not_start_is_used_and_never_stopped(world, monkeypatch):
    remote._save_config({"default_profile": "prod", "profiles": {"prod": {"endpoint": "https://x"}}})
    world.answers[:] = [True]
    r = cli.invoke(app, ["setup"])
    assert r.exit_code == 0, r.output
    assert world.started == [] and "didn't start it" in r.stdout
    monkeypatch.setattr(setup.os, "kill", lambda *a: pytest.fail("stopped an engine it didn't start"))
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0 and "keeps running" in r.stdout
    assert _config()["default_profile"] == "prod" and setup._LOCAL_PROFILE not in _config()["profiles"]


def test_a_stale_record_is_replaced_and_still_remembers_the_default(world):
    remote._save_config({"default_profile": "prod", "profiles": {"prod": {"endpoint": "https://x"}}})
    assert cli.invoke(app, ["setup"]).exit_code == 0
    world.ours.clear()  # the engine died; the default is still `local`
    world.answers[:] = [False, True, True]
    assert cli.invoke(app, ["setup"]).exit_code == 0
    assert len(world.started) == 2
    assert json.loads(setup._state_path().read_text())["previous_default"] == "prod"


def test_an_engine_that_dies_while_starting_says_where_the_log_is(world):
    world.answers[:] = [False]
    world.proc.returncode = 3
    r = cli.invoke(app, ["setup"])
    assert r.exit_code == 1, r.output
    assert "code 3" in r.output and "engine.log" in r.output
    assert not setup._state_path().exists()


def test_a_profile_of_a_persons_own_called_local_survives(world, monkeypatch):
    mine = {"default_profile": "local", "profiles": {"local": {"endpoint": "https://mine", "token": "t"}}}
    remote._save_config(json.loads(json.dumps(mine)))
    assert cli.invoke(app, ["setup"]).exit_code == 0
    assert _config()["profiles"]["local"] == mine["profiles"]["local"]
    world.ours.clear()
    assert cli.invoke(app, ["setup", "--stop"]).exit_code == 0
    assert _config() == mine


def test_a_restored_default_that_no_longer_exists_is_not_restored(world):
    remote._save_config({"default_profile": "prod", "profiles": {"prod": {"endpoint": "https://x"}}})
    assert cli.invoke(app, ["setup"]).exit_code == 0
    cfg = _config()
    del cfg["profiles"]["prod"]
    remote._save_config(cfg)
    world.ours.clear()
    assert cli.invoke(app, ["setup", "--stop"]).exit_code == 0
    assert "default_profile" not in _config()


@pytest.mark.parametrize("junk", ["not json", "[1]", "3"])
def test_a_damaged_record_is_no_record(world, junk):
    setup._state_path().parent.mkdir(parents=True, exist_ok=True)
    setup._state_path().write_text(junk)
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0 and "no background engine" in r.stdout


def test_the_engine_is_on_record_before_it_answers(world, monkeypatch):
    """If the sign-in then fails, or this command is killed, `--stop` can still reach it."""
    monkeypatch.setattr(remote, "_sign_in", lambda *a, **k: pytest.fail("signed in"))
    world.answers[:] = [False, True]
    seen = []
    real = setup._write_state
    monkeypatch.setattr(setup, "_write_state", lambda s: (seen.append(dict(s)), real(s)))
    monkeypatch.setattr(setup, "_sign_in_locally", lambda *a: (_ for _ in ()).throw(RuntimeError("no")))
    r = cli.invoke(app, ["setup"])
    assert r.exit_code != 0
    assert seen[0]["pid"] == 4242 and json.loads(setup._state_path().read_text())["pid"] == 4242


def test_ctrl_c_while_waiting_stops_the_engine_and_leaves_no_record(world, monkeypatch):
    def interrupted(ep):
        raise KeyboardInterrupt

    answers = [False]
    monkeypatch.setattr(remote, "_reachable", lambda ep: answers.pop(0) if answers else interrupted(ep))
    r = cli.invoke(app, ["setup"])
    assert r.exit_code != 0
    assert world.proc.terminated and not setup._state_path().exists()


def test_an_engine_that_never_answers_is_stopped_with_a_clear_message(world, monkeypatch):
    world.answers[:] = [False]
    monkeypatch.setattr(setup, "_READY_TIMEOUT_S", 0.0)
    r = cli.invoke(app, ["setup"])
    assert r.exit_code == 1, r.output
    assert "didn't answer" in r.output and world.proc.terminated


# ── triggers ─────────────────────────────────────────────────────────────────────────────────

def test_enable_turns_on_only_what_that_pipeline_declares(world):
    r = cli.invoke(app, ["setup", "--enable", "finetune/sft-local"])
    assert r.exit_code == 0, r.output
    assert world.client.enabled == ["t-sft"]


def test_nothing_is_turned_on_unless_named(world, tmp_path):
    """ADR-0131: nothing starts unattended on install, whatever the profile."""
    prof = tmp_path / "p.toml"
    prof.write_text("")
    r = cli.invoke(app, ["setup", "--profile", str(prof)])
    assert r.exit_code == 0, r.output
    assert world.client.enabled == []
    assert world.started[0][1]["env"]["CAIRN_PROFILE"] == str(prof.resolve())


def test_enabling_a_pipeline_that_declares_nothing_fails_and_names_the_ones_that_do(world):
    r = cli.invoke(app, ["setup", "--enable", "no/such"])
    assert r.exit_code == 1, r.output
    assert "no/such" in r.output and SFT in r.output
    assert world.client.enabled == []


def test_a_typo_in_one_enable_turns_on_none_of_them(world):
    r = cli.invoke(app, ["setup", "--enable", "finetune/sft-local", "--enable", "no/such"])
    assert r.exit_code == 1, r.output
    assert world.client.enabled == []


def test_failed_checks_leave_the_engine_up_and_turn_nothing_on(world):
    world.checks[0] = 1
    r = cli.invoke(app, ["setup", "--enable", "finetune/sft-local"])
    assert r.exit_code == 1, r.output
    assert world.client.enabled == [] and "cairn setup --stop" in r.output
    assert setup._state_path().exists()


def test_the_checks_show_what_failed_and_the_count_not_the_whole_report(world, monkeypatch, capsys):
    monkeypatch.undo()
    seen = {}

    def run(argv, **kw):
        seen.update(env=kw["env"], argv=argv)
        return SimpleNamespace(returncode=1, stderr="", stdout=(
            "cairn setup --check\n\n[store]\n  ✓ A: fine\n  ⚠ B: unset (optional)\n  ✗ C: broken\n\nSummary: 1 ok, 1 warn, 1 fail"))

    monkeypatch.setattr(setup.subprocess, "run", run)
    assert setup._run_checks({"K": "the engine's"}) == 1
    out = capsys.readouterr().out
    assert "✗ C: broken" in out and "1 ok, 1 warn, 1 fail" in out and "cairn setup --check" in out
    assert "A: fine" not in out and "B: unset" not in out
    assert seen["env"] == {"K": "the engine's"} and seen["argv"][-1] == "cairn.cli.commands.doctor"


def test_only_seed_brings_the_checkouts_example_pack(world, monkeypatch):
    example = "/checkout/examples/hello-cairn"
    monkeypatch.setattr(dev_cli, "_example_pack_dir", lambda: example)
    monkeypatch.setattr(dev_cli, "_seed", lambda env: None)
    assert cli.invoke(app, ["setup"]).exit_code == 0
    assert example not in world.started[0][1]["env"].get("CAIRN_PACK_PATH", "")
    world.ours.clear()  # gone, so the next setup starts a new one
    world.started.clear()
    world.answers[:] = [False, True, True]
    assert cli.invoke(app, ["setup", "--seed"]).exit_code == 0
    assert example in world.started[0][1]["env"]["CAIRN_PACK_PATH"]


# ── stopping ─────────────────────────────────────────────────────────────────────────────────

def test_stop_stops_it_and_gives_the_default_back(world, monkeypatch):
    remote._save_config({"default_profile": "prod", "profiles": {"prod": {"endpoint": "https://x"}}})
    assert cli.invoke(app, ["setup"]).exit_code == 0
    assert _config()["default_profile"] == setup._LOCAL_PROFILE
    killed = []

    def kill(pid, sig):
        killed.append((pid, sig))
        world.ours.discard(pid)

    monkeypatch.setattr(setup.os, "kill", kill)
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0, r.output
    assert killed == [(4242, signal.SIGTERM)]
    assert not setup._state_path().exists()
    assert _config() == {"default_profile": "prod", "profiles": {"prod": {"endpoint": "https://x"}}}


def test_stop_never_kills_a_process_that_took_over_the_pid(world, monkeypatch):
    assert cli.invoke(app, ["setup"]).exit_code == 0
    world.ours.clear()
    monkeypatch.setattr(setup.os, "kill", lambda *a: pytest.fail("killed a process that isn't the engine"))
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0, r.output
    assert "already stopped" in r.stdout and not setup._state_path().exists()


def test_an_engine_that_exits_as_it_is_stopped_is_stopped(world, monkeypatch):
    assert cli.invoke(app, ["setup"]).exit_code == 0

    def gone(pid, sig):
        world.ours.discard(pid)
        raise ProcessLookupError

    monkeypatch.setattr(setup.os, "kill", gone)
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0, r.output
    assert not setup._state_path().exists()


def test_stop_with_nothing_running_says_so(world):
    r = cli.invoke(app, ["setup", "--stop"])
    assert r.exit_code == 0 and "no background engine" in r.stdout


def test_is_ours_reads_the_command_line(monkeypatch):
    """The real check, unstubbed: this process is alive, and it isn't an engine."""
    assert setup._is_ours(os.getpid()) is False
    assert setup._is_ours(None) is False and setup._is_ours(-1) is False


# ── the other forms ──────────────────────────────────────────────────────────────────────────

def test_server_signs_in_and_starts_nothing(world):
    world.answers[:] = [True]
    r = cli.invoke(app, ["setup", "--server", "https://cairn.example/", "-u", "me", "--password", "pw"])
    assert r.exit_code == 0, r.output
    assert world.started == [] and not setup._state_path().exists()
    assert _config()["profiles"]["default"] == {"endpoint": "https://cairn.example", "token": "tok-me"}


def test_server_prompts_for_what_is_missing(world):
    world.answers[:] = [True]
    r = cli.invoke(app, ["setup", "--server", "https://x"], input="me\npw\n")
    assert r.exit_code == 0, r.output
    assert _config()["profiles"]["default"]["token"] == "tok-me"


def test_a_server_that_does_not_answer_fails_before_asking_for_a_password(world):
    world.answers[:] = [False]
    r = cli.invoke(app, ["setup", "--server", "https://x", "-u", "me"])
    assert r.exit_code == 1 and "nothing answers" in r.output and "Password" not in r.output


def test_server_can_enable(world):
    world.answers[:] = [True]
    r = cli.invoke(app, ["setup", "--server", "https://x", "-u", "m", "--password", "p", "--enable", "finetune/sft-local"])
    assert r.exit_code == 0, r.output
    assert world.client.enabled == ["t-sft"]


@pytest.mark.parametrize("flags, strict", [([], False), (["--strict"], True)])
def test_check_runs_the_checks_and_starts_nothing(world, monkeypatch, flags, strict):
    seen = []
    monkeypatch.setattr(doctor, "_run_and_exit_code", lambda strict: seen.append(strict) or 3)
    r = cli.invoke(app, ["setup", "--check", *flags])
    assert r.exit_code == 3, r.output
    assert seen == [strict] and world.started == []


@pytest.mark.parametrize("flags, kw", [
    (["--foreground"], {"engine_only": False, "web_only": False}),
    (["--web-only"], {"engine_only": False, "web_only": True}),
    (["--foreground", "--engine-only", "--port", "8000", "--db", "/x", "--seed"],
     {"engine_only": True, "port": 8000, "db": "/x", "seed": True}),
])
def test_attached_is_the_dev_loop(world, monkeypatch, flags, kw):
    seen = {}
    monkeypatch.setattr(dev_cli, "run_attached", lambda **k: seen.update(k))
    r = cli.invoke(app, ["setup", *flags])
    assert r.exit_code == 0, r.output
    assert world.started == [] and {k: seen[k] for k in kw} == kw


@pytest.mark.parametrize("flags", [
    ["--engine-only", "--web-only"], ["--strict"], ["-u", "me"], ["--password", "x"],
    ["--stop", "--check"], ["--stop", "--server", "http://x"], ["--stop", "--port", "1"],
    ["--check", "--enable", "a/b"], ["--server", "http://x", "--port", "1"], ["--server", "http://x", "--profile", "p"],
    ["--foreground", "--enable", "a/b"], ["--web-only", "--enable", "a/b"],
])
def test_options_that_do_not_go_together_exit_2_before_anything_starts(world, flags):
    r = cli.invoke(app, ["setup", *flags])
    assert r.exit_code == 2, r.output
    assert world.started == []


# ── the profile ──────────────────────────────────────────────────────────────────────────────

def test_a_profile_is_a_name_cairn_ships(world):
    assert setup._profile_file("secure").name == "secure.toml"


def test_a_profile_is_a_name_an_installed_pack_ships(world, tmp_path, monkeypatch):
    (tmp_path / "pk" / "pipelines").mkdir(parents=True)
    (tmp_path / "pk" / "profiles").mkdir()
    (tmp_path / "pk" / "profiles" / "mine.toml").write_text("")
    monkeypatch.setattr("cairn.core.pack.registry.all_pipeline_dirs", lambda: [str(tmp_path / "pk" / "pipelines")])
    assert setup._profile_file("mine") == tmp_path / "pk" / "profiles" / "mine.toml"


def test_an_unknown_profile_fails_before_anything_starts(world):
    r = cli.invoke(app, ["setup", "--profile", "nope-no-such"])
    assert r.exit_code == 1 and "nope-no-such" in r.output
    assert world.started == []


# ── where the engine listens ─────────────────────────────────────────────────────────────────

def test_an_explicit_host_wins(world, monkeypatch):
    monkeypatch.setenv("CAIRN_TRIGGER_HOST", "0.0.0.0")
    assert cli.invoke(app, ["setup"]).exit_code == 0
    assert world.started[0][1]["env"]["CAIRN_TRIGGER_HOST"] == "0.0.0.0"


def test_every_local_engine_listens_on_this_machine_by_default(monkeypatch):
    """`cairn setup --foreground`, `cairn setup` and the temporary engine `cairn run` starts all come through here."""
    monkeypatch.delenv("CAIRN_TRIGGER_HOST", raising=False)
    assert dev_cli.dev_env(1)["CAIRN_TRIGGER_HOST"] == "127.0.0.1"
    monkeypatch.setenv("CAIRN_TRIGGER_HOST", "")
    assert dev_cli.dev_env(1)["CAIRN_TRIGGER_HOST"] == "127.0.0.1"
    monkeypatch.setenv("CAIRN_TRIGGER_HOST", "0.0.0.0")
    assert dev_cli.dev_env(1)["CAIRN_TRIGGER_HOST"] == "0.0.0.0"
