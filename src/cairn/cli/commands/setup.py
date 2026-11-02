"""``cairn setup`` — start a local engine or connect to a server, and check it works (ADR-0165 D2).

By default the engine runs in the background, tracked by ``engine.json`` beside the CLI's config, and
this command returns once the engine answers, this CLI is signed in to it and the checks have run.
``--enable`` is the only thing here that turns a trigger on: nothing starts unattended because a
profile or a pack was installed (ADR-0131).
"""
from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

import typer

from cairn.cli.commands import dev as dev_cli
from cairn.cli.commands import remote
from cairn.core import settings

#: The port the CLI looks at when nothing is configured, so a bare `cairn setup` needs no flag.
_DEFAULT_PORT = 9090
#: A cold start imports the whole engine: 13-15 s measured on a laptop, more on a small box.
_READY_TIMEOUT_S = 120.0
_STOP_TIMEOUT_S = 15.0
#: The config profile this command signs in under, so the one a person made for a server survives.
_LOCAL_PROFILE = "local-engine"
_ENGINE_MODULE = "cairn.core.triggers.main"


def _state_path() -> Path:
    return remote._config_path().parent / "engine.json"


def _log_path() -> Path:
    return remote._config_path().parent / "engine.log"


def _load_state() -> dict[str, Any]:
    try:
        state = json.loads(_state_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return state if isinstance(state, dict) else {}


def _is_ours(pid: Any) -> bool:
    """Is this pid still the engine we started? A pid is reused, so its command line has to say so."""
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        out = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return False
    return _ENGINE_MODULE in out.stdout


def _profile_file(name: str) -> Path:
    """A profile by path, by the name of one cairn ships, or by the name of one in an installed pack."""
    from cairn.core.pack import ensure_loaded  # noqa: PLC0415 — keeps --help light
    from cairn.core.pack.registry import all_pipeline_dirs  # noqa: PLC0415
    from cairn.core.ui_contract import list_profiles  # noqa: PLC0415

    path = Path(name).expanduser()
    if path.is_file():
        return path.resolve()
    shipped = {p["name"]: Path(p["path"]) for p in list_profiles()}
    if name in shipped:
        return shipped[name]
    ensure_loaded()
    for pipelines in all_pipeline_dirs():
        cand = Path(pipelines).parent / "profiles" / f"{name}.toml"
        if cand.is_file():
            return cand
    typer.secho(f"no profile {name!r}: not a file, and not one cairn or an installed pack ships. "
                f"cairn ships: {', '.join(sorted(shipped)) or '(none)'}.", fg="red", err=True)
    raise typer.Exit(1)


def _sign_in_locally(endpoint: str, state: dict[str, Any]) -> Optional[str]:
    """Sign in to the local engine and make it the CLI's default until ``--stop``, remembering what
    the default was. ``dev_env`` pins the login."""
    cfg = remote._load_config()
    if cfg.get("default_profile") != _LOCAL_PROFILE:
        state["previous_default"] = cfg.get("default_profile")
    return remote._sign_in(endpoint, "dev", "dev", _LOCAL_PROFILE, make_default=True)


def _forget_sign_in(state: dict[str, Any]) -> None:
    """Undo :func:`_sign_in_locally`, so a stopped engine leaves no dead default behind."""
    cfg = remote._load_config()
    (cfg.get("profiles") or {}).pop(_LOCAL_PROFILE, None)
    if cfg.get("default_profile") == _LOCAL_PROFILE:
        cfg.pop("default_profile")
        if state.get("previous_default") in (cfg.get("profiles") or {}):
            cfg["default_profile"] = state["previous_default"]
    remote._save_config(cfg)


def _on_this_machine(environ: Any) -> None:
    """A setup engine is for this machine, unless ``CAIRN_TRIGGER_HOST`` says otherwise."""
    if not (settings.getenv("CAIRN_TRIGGER_HOST", "", environ=environ) or "").strip():
        environ["CAIRN_TRIGGER_HOST"] = "127.0.0.1"


def _write_state(state: dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _start_engine(env: dict[str, str], endpoint: str, state: dict[str, Any]) -> None:
    """Start the engine detached from this terminal and wait until it answers. It is on record from
    the moment it exists, so whatever happens next, `--stop` can still reach it."""
    log = _log_path()
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("ab") as out:
        proc = subprocess.Popen(
            [sys.executable, "-P", "-m", _ENGINE_MODULE], env=env,  # -P: never the cwd's cairn/
            stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
        )
    state["pid"] = proc.pid
    _write_state(state)
    typer.echo(f"starting the engine at {endpoint} (pid {proc.pid}, log {log}); a first start can take a minute ...")
    deadline = time.monotonic() + _READY_TIMEOUT_S
    try:
        while not remote._reachable(endpoint):
            if proc.poll() is not None:
                typer.secho(f"the engine exited with code {proc.returncode} while starting. See {log}.",
                            fg="red", err=True)
                raise typer.Exit(1)
            if time.monotonic() >= deadline:
                typer.secho(f"the engine didn't answer at {endpoint} within {_READY_TIMEOUT_S:.0f}s, so it was "
                            f"stopped. See {log}.", fg="red", err=True)
                raise typer.Exit(1)
            time.sleep(0.5)
    except (typer.Exit, KeyboardInterrupt):
        if proc.poll() is None:
            proc.terminate()
        _state_path().unlink(missing_ok=True)
        raise


def _stop() -> None:
    state = _load_state()
    pid = state.get("pid")
    if not state:
        typer.echo("no background engine.")
        return
    if pid is None:
        typer.echo("this command didn't start the engine it signed in to, so it keeps running. Signed out of it.")
    elif _is_ours(pid):
        with contextlib.suppress(ProcessLookupError):  # it can exit between the check and the signal
            os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + _STOP_TIMEOUT_S
            while _is_ours(pid) and time.monotonic() < deadline:
                time.sleep(0.2)
            if _is_ours(pid):
                os.kill(pid, signal.SIGKILL)
        typer.secho(f"stopped the engine (pid {pid}).", fg="green")
    else:
        typer.echo("the background engine had already stopped.")
    _forget_sign_in(state)
    _state_path().unlink(missing_ok=True)


def _run_checks(env: dict[str, str]) -> int:
    """The doctor, in the engine's environment: its report is about what the engine runs under. Only
    what failed and the count are shown here: most of the report is settings left at their default."""
    done = subprocess.run([sys.executable, "-P", "-m", "cairn.cli.commands.doctor"], env=env,
                          capture_output=True, text=True)
    lines = done.stdout.splitlines()
    for line in lines:
        if line.lstrip().startswith("✗"):
            typer.secho(line, fg="red")
    typer.echo(f"checks: {lines[-1] if lines else 'no report'} (`cairn setup --check` shows every one)")
    if not lines:
        typer.secho(done.stderr, fg="red", err=True)
    return done.returncode


def _enable(endpoint: str, refs: list[str], *, token: Optional[str]) -> None:
    """Turn on what each pipeline declares (ADR-0160). A ref that declares nothing is an error: the
    person asked for something to start, and it never will."""
    from cairn.core.refs import same_ref  # noqa: PLC0415 — keeps --help light

    with remote._api(endpoint, token) as c:
        rows = (remote._call(c.stored_triggers) or {}).get("triggers", [])
        wanted: list[str] = []
        for ref in refs:  # all of them first: a typo turns nothing on
            mine = [t["id"] for t in rows
                    if same_ref(str(t.get("declared_by") or ""), f"pipeline:{ref.removeprefix('pipeline:')}")]
            if not mine:
                declared = sorted({str(t.get("declared_by")) for t in rows if t.get("declared_by")})
                typer.secho(f"--enable {ref}: it declares no event or webhook trigger here. Pipelines that "
                            f"do: {', '.join(declared) or '(none)'}.", fg="red", err=True)
                raise typer.Exit(1)
            wanted += mine
        for trigger_id in wanted:
            out = remote._call(c.set_trigger_enabled, trigger_id, True) or {}
            typer.secho(f"enabled {out.get('id', trigger_id)}; applies {out.get('applies', 'now')}", fg="green")


def _connect(server: str, username: Optional[str], password: Optional[str], enable: list[str]) -> None:
    endpoint = server.rstrip("/")
    if not remote._reachable(endpoint):
        typer.secho(f"nothing answers at {endpoint}.", fg="red", err=True)
        raise typer.Exit(1)
    username = username or typer.prompt("Username", default="admin")
    password = password or typer.prompt("Password", hide_input=True)
    cfg = remote._load_config()
    current = cfg.get("default_profile") or "default"
    token = remote._sign_in(endpoint, username, password, "default" if current == _LOCAL_PROFILE else current,
                            make_default=True)
    if enable:
        _enable(endpoint, enable, token=token)


def _refuse_mixed(**given: Any) -> None:
    """Exit 2 on options that can't go together, before anything starts."""
    server, stop, check, foreground, web_only = (
        given["server"], given["stop"], given["check"], given["foreground"], given["web_only"])
    local = {"--port": given["port"], "--db": given["db"], "--seed": given["seed"], "--profile": given["profile"],
             "--foreground": foreground, "--engine-only": given["engine_only"], "--web-only": web_only}
    wrong: Optional[str] = None
    if given["engine_only"] and web_only:
        wrong = "--engine-only and --web-only can't go together."
    elif given["strict"] and not check:
        wrong = "--strict goes with --check."
    elif (given["username"] or given["password"]) and not server:
        wrong = "--username and --password go with --server: a local engine's login is dev/dev."
    elif sum(map(bool, (server, stop, check))) > 1:
        wrong = "--server, --stop and --check each do one thing: pick one."
    elif (stop or check) and (given["enable"] or any(local.values())):
        wrong = f"{'--stop' if stop else '--check'} takes no other options" + (" but --strict." if check else ".")
    elif server and any(local.values()):
        wrong = f"--server connects to a server, so {next(k for k, v in local.items() if v)} doesn't apply."
    elif given["enable"] and (foreground or web_only):
        wrong = "--enable needs the engine up before this command returns: drop --foreground."
    if wrong:
        typer.secho(wrong, fg="red", err=True)
        raise typer.Exit(2)


def cmd(
    profile: Optional[str] = typer.Option(
        None, "--profile", help="The deployment profile the engine runs under: a file, or a name cairn or a pack ships."),
    enable: list[str] = typer.Option(
        [], "--enable", help="Turn on this pipeline's declared triggers. Repeatable. Nothing else turns them on."),
    server: Optional[str] = typer.Option(
        None, "--server", help="Connect to this server instead of starting an engine, and sign in."),
    username: Optional[str] = typer.Option(None, "--username", "-u", help="With --server. Prompted when absent."),
    password: Optional[str] = typer.Option(None, "--password", help="With --server. Prompted when absent."),
    foreground: bool = typer.Option(False, "--foreground", help="Stay attached; Ctrl-C stops it."),
    stop: bool = typer.Option(False, "--stop", help="Stop the background engine."),
    port: Optional[int] = typer.Option(None, "--port", help=f"Engine port (default {_DEFAULT_PORT})."),
    db: Optional[str] = typer.Option(None, "--db", help="Sqlite path (default ~/.cairn/dev.db)."),
    seed: bool = typer.Option(False, "--seed", help="Seed deterministic demo data first."),
    engine_only: bool = typer.Option(False, "--engine-only", help="Start only the engine."),
    web_only: bool = typer.Option(False, "--web-only", help="Start only the console dev server."),
    check: bool = typer.Option(False, "--check", help="Only check the environment and its services; start nothing."),
    strict: bool = typer.Option(False, "--strict", help="With --check: warnings fail."),
) -> None:
    """Start a local engine, or connect to a server, and check that it works.

    The engine runs in the background until `cairn setup --stop`. A second `cairn setup` reports the
    running engine and starts no other. The login is dev/dev and the engine listens on this machine
    only. Triggers stay paused unless you name them with --enable.
    """
    _refuse_mixed(profile=profile, enable=enable, server=server, username=username, password=password,
                  foreground=foreground, stop=stop, port=port, db=db, seed=seed, engine_only=engine_only,
                  web_only=web_only, check=check, strict=strict)
    if stop:
        _stop()
        return
    if check:
        from cairn.cli.commands import doctor  # noqa: PLC0415 — keeps --help light

        raise typer.Exit(doctor._run_and_exit_code(strict=strict))
    if server:
        _connect(server, username, password, enable)
        return

    profile_file = _profile_file(profile) if profile else None
    if foreground or web_only:
        if profile_file:
            os.environ["CAIRN_PROFILE"] = str(profile_file)  # dev_env keeps a profile that is set
        _on_this_machine(os.environ)
        dev_cli.run_attached(port=port or _DEFAULT_PORT, engine_only=engine_only, web_only=web_only, db=db, seed=seed,
                             example_pack=seed)
        return

    state = _load_state()
    running = _is_ours(state.get("pid"))
    wanted_port = port or (state.get("port") if running else None) or _DEFAULT_PORT
    wanted_db = db or (state.get("db") if running else None)
    wanted_profile = str(profile_file) if profile_file else (state.get("profile") if running else None)
    if running and (wanted_port != state.get("port") or wanted_profile != state.get("profile") or wanted_db != state.get("db")):
        typer.secho(f"an engine is already running at http://127.0.0.1:{state.get('port')} under profile "
                    f"{state.get('profile') or 'local-dev'}. To change it, run `cairn setup --stop` first.",
                    fg="red", err=True)
        raise typer.Exit(1)

    # Only --seed needs the checkout's example pack: its requirements aren't what was asked to run.
    env = dev_cli.dev_env(wanted_port, wanted_db, example_pack=seed)
    _on_this_machine(env)
    if wanted_profile:
        env["CAIRN_PROFILE"] = wanted_profile
    endpoint = f"http://127.0.0.1:{wanted_port}"
    kept = {k: v for k, v in state.items() if k == "previous_default"}  # past a stale record, too
    if running:
        typer.secho(f"an engine is already running at {endpoint} (pid {state.get('pid')}).", fg="green")
    elif remote._reachable(endpoint):
        # Not ours to stop or restart: a `cairn dev` in another terminal, say.
        typer.secho(f"an engine already answers at {endpoint}, and this command didn't start it; using it."
                    + (" --profile doesn't apply to it." if profile_file else ""), fg="yellow")
        state = kept
    else:
        if seed:
            dev_cli._seed(env)
        state = {**kept, "port": wanted_port, "db": wanted_db, "profile": wanted_profile}
        _start_engine(env, endpoint, state)
    token = _sign_in_locally(endpoint, state)
    _write_state(state)
    code = _run_checks(env)
    if code:
        typer.secho("the engine is running, but the checks above failed, so no trigger was turned on. "
                    "`cairn setup --stop` stops it.", fg="red", err=True)
        raise typer.Exit(code)
    if enable:
        _enable(endpoint, enable, token=token)
    typer.secho(f"ready: {endpoint}, signed in as dev. Next: cairn run", fg="green")
