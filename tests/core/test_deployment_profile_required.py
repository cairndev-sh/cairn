"""#2296 — every deployment names its profile: `cairn-server` and `cairn-worker` refuse to
start when `OBS_PROFILE` is unset, blank, missing or malformed; the dev launchers pass
`local-dev.toml`; compose, the chart and `cairn deploy doctor` require it."""
from __future__ import annotations

import pathlib

import pytest

import cairn.profiles as profiles_pkg
from cairn.core.pack.profile import Profile, ProfileError, require_deployment_profile

LOCAL_DEV = pathlib.Path(profiles_pkg.__file__).parent / "local-dev.toml"


def _bad(tmp_path, kind):
    if kind == "missing":
        return str(tmp_path / "nope.toml")
    p = tmp_path / "bad.toml"
    p.write_bytes(b"\xff\xfe[profile]" if kind == "not-utf8" else b"[profile\nextends = ")
    return str(p)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_an_unset_or_blank_profile_refuses(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("OBS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("OBS_PROFILE", value)
    with pytest.raises(ProfileError, match=r"CAIRN_PROFILE is unset.*profiles/secure\.toml.*deployment-profile\.md"):
        require_deployment_profile()


@pytest.mark.parametrize("kind", ["missing", "malformed", "not-utf8"])
def test_an_unloadable_profile_refuses(monkeypatch, tmp_path, kind):
    monkeypatch.setenv("OBS_PROFILE", _bad(tmp_path, kind))
    with pytest.raises(ProfileError, match=r"cannot be loaded.*deployment-profile\.md"):
        require_deployment_profile()


def test_a_shipped_profile_loads(monkeypatch):
    monkeypatch.setenv("OBS_PROFILE", str(pathlib.Path(profiles_pkg.__file__).parent / "secure.toml"))
    assert isinstance(require_deployment_profile(), Profile)


def _server(monkeypatch):
    import uvicorn

    from cairn.core import provider_settings
    from cairn.core.engine.runner import Runner
    from cairn.core.triggers import main as main_mod

    ran = []
    monkeypatch.setattr(provider_settings, "load_env", lambda *a, **k: None)
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: ran.append(True))
    monkeypatch.setattr(main_mod, "build_default_runner", lambda: Runner())
    return main_mod.main, ran


@pytest.mark.parametrize("kind", ["unset", "missing", "malformed"])
def test_the_server_refuses_to_start_without_a_loadable_profile(monkeypatch, tmp_path, kind):
    if kind == "unset":
        monkeypatch.delenv("OBS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("OBS_PROFILE", _bad(tmp_path, kind))
    main, ran = _server(monkeypatch)
    assert main() == 2 and ran == []


def test_the_server_starts_with_a_profile(monkeypatch):
    monkeypatch.setenv("OBS_PROFILE", str(LOCAL_DEV))
    main, ran = _server(monkeypatch)
    assert main() == 0 and ran == [True]


def _worker(monkeypatch):
    worker = pytest.importorskip("cairn.core.runtime.temporal.worker")
    ran = []

    async def _run(task_queue=None):
        ran.append(task_queue)
    monkeypatch.setattr(worker, "load_env", lambda *a, **k: None)
    monkeypatch.setattr(worker, "init_tracing", lambda **k: None)
    monkeypatch.setattr(worker, "run_worker", _run)
    return worker.main, ran


@pytest.mark.parametrize("kind", ["unset", "missing", "malformed"])
def test_the_worker_refuses_to_start_without_a_profile(monkeypatch, tmp_path, kind):
    if kind == "unset":
        monkeypatch.delenv("OBS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("OBS_PROFILE", _bad(tmp_path, kind))
    main, ran = _worker(monkeypatch)
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 2 and ran == []


def test_the_worker_starts_with_a_profile(monkeypatch):
    monkeypatch.setenv("OBS_PROFILE", str(LOCAL_DEV))
    main, ran = _worker(monkeypatch)
    main(task_queue="q")
    assert ran == ["q"]


def test_the_dev_launcher_passes_local_dev_and_keeps_a_callers_profile(monkeypatch):
    from cairn.cli.commands.dev import dev_env

    monkeypatch.delenv("OBS_PROFILE", raising=False)
    monkeypatch.delenv("CAIRN_PROFILE", raising=False)
    assert pathlib.Path(dev_env(port=8000, example_pack=False)["CAIRN_PROFILE"]) == LOCAL_DEV and LOCAL_DEV.exists()
    monkeypatch.setenv("CAIRN_PROFILE", "/srv/mine.toml")
    assert dev_env(port=8000)["CAIRN_PROFILE"] == "/srv/mine.toml"
    monkeypatch.delenv("CAIRN_PROFILE")
    monkeypatch.setenv("OBS_PROFILE", "/srv/old.toml")  # the former name is kept, not overridden (ADR-0122)
    assert "CAIRN_PROFILE" not in dev_env(port=8000)


def test_deploy_doctor_requires_the_profile():
    from cairn.core.deploy_check import check_deploy

    def status(env):
        return next(r.status for r in check_deploy(env, set()) if r.name == "CAIRN_PROFILE")
    assert status({}) == "fail"
    assert status({"CAIRN_PROFILE": "/app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml"}) == "ok"


@pytest.mark.parametrize("runtime", ["inprocess", "dbos", "temporal"])
def test_a_deployment_profile_follows_the_deployments_runtime(monkeypatch, runtime):
    """#2296 — the local-dev base layer pins no runtime, so secure.toml (the public deployment
    profile, ADR-0134 D5) honours OBS_RUNTIME; a profile that pins still wins (#1852)."""
    from cairn.core import settings

    shipped = pathlib.Path(profiles_pkg.__file__).parent
    monkeypatch.setenv("OBS_RUNTIME", runtime)
    monkeypatch.setenv("OBS_PROFILE", str(shipped / "secure.toml"))
    assert settings.runtime() == runtime
    monkeypatch.setenv("OBS_PROFILE", str(shipped / "sandbox.toml"))
    assert settings.runtime() == "inprocess"  # sandbox pins in_process
