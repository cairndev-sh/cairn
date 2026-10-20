"""``cairn dev`` — the one-command sqlite dev environment (#1188, ADR-0085 D5).

The 15-minute-quickstart entrypoint: starts the engine on a pinned, hermetic
dev configuration and (unless ``--engine-only``) the console's vite dev server
next to it. The pinning is the point — every knob a stale repo ``.env`` could
poison is set EXPLICITLY in the child process env, so ``load_dotenv``
(``override=False``) can never redirect a newcomer's first run to somebody's
postgres DSN or pgvector selection. The foot-guns this retires are the ones
that cost real debugging time: the .env DSN, CAIRN_STORAGE-unset-means-in-memory,
and auth silently off.

Auth is seeded ``dev``/``dev`` rather than disabled — the console's login flow
is part of what a frontend contributor works on, so it must exist (and a
printed password removes the "why 401" dead end).

Keyless by default: ``CAIRN_BACKEND`` stays ``mock`` unless the shell/.env
carries a real key — same behavior the server entrypoint already has.

``--seed`` (deterministic demo data, #1189) is the next slice.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer

from cairn.core import settings as _settings

app = typer.Typer(help="One-command sqlite dev environment (ADR-0085 D5).", no_args_is_help=False)

#: Every knob the repo .env could otherwise inject into a dev session. Pinned
#: explicitly (set-but-empty = "use the built-in default"; every getter treats
#: "" as unset) so the dev env is identical on every machine.
_PINNED_EMPTY = ("CAIRN_BUS", "CAIRN_CHECKPOINTER", "CAIRN_CONNECTIONS_DB", "RAG_VECTOR_STORE")


def dev_env(port: int, db_path: Optional[str] = None) -> dict[str, str]:
    """The pinned child-process environment — pure, unit-testable.

    Starts from the CURRENT process env (a real OPENAI_API_KEY etc. passes
    through) and overrides the storage/runtime/auth family unconditionally."""
    db = db_path or str(Path.home() / ".cairn" / "dev.db")
    env = dict(os.environ)
    env.update({
        "CAIRN_STORAGE": f"sqlite:///{db}",
        "CAIRN_RUNTIME": "inprocess",
        "CAIRN_TRIGGER_PORT": str(port),
        # dev/dev login — auth exists (the console login page must work) but
        # never blocks a newcomer. Pinned, deterministic, dev-only.
        "CAIRN_JWT_SECRET": "cairn-dev-secret",
        "CAIRN_API_USER": "dev",
        "CAIRN_API_PASSWORD": "dev",
    })
    for k in _PINNED_EMPTY:
        env[k] = ""
        if k.startswith(_settings.ENV_PREFIX):  # an OBS_* spelling would otherwise fill it (ADR-0122)
            env[_settings.legacy_name(k)] = ""
    if not (_settings.getenv("CAIRN_PROFILE", "", environ=env) or "").strip():  # the server refuses without one (#2296)
        import cairn.profiles as profiles_pkg

        env["CAIRN_PROFILE"] = str(Path(profiles_pkg.__file__).parent / "local-dev.toml")
    example = _example_pack_dir()
    if example is not None:  # #2418 — the seed and the engine both see the example pack
        from cairn.core.pack.loader import DEV_PACK_PATH_ENV  # noqa: PLC0415 - keep `cairn --help` light

        dirs = [d for d in env.get(DEV_PACK_PATH_ENV, "").split(os.pathsep) if d.strip()]
        if str(example) not in dirs:
            env[DEV_PACK_PATH_ENV] = os.pathsep.join([*dirs, str(example)])
    return env


def _example_pack_dir() -> Optional[Path]:
    """``examples/hello-cairn`` when running from a repo checkout (#2418): the
    example pack is its own distribution, so an installed engine finds it only
    when ``cairn-pack-hello`` is installed. ``cairn dev`` adds the checkout's copy
    to ``CAIRN_PACK_PATH`` (an installed copy still wins the name, as for every
    dev pack) so ``--seed`` has the example to seed and the engine can run it."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "examples" / "hello-cairn"
        if (cand / "pack.toml").is_file():
            return cand
    return None


def _web_dir() -> Optional[Path]:
    """The console source dir, when running from a repo checkout (an installed
    wheel ships only the built dist — vite dev needs the checkout). Top-level
    ``console/`` since #1190 (ADR-0085 D3)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "console" / "package.json"
        if cand.exists():
            return cand.parent
    return None


def _seed(env: dict[str, str]) -> None:
    """#1189 — deterministic demo data into the DEV store, so every console
    surface is non-empty on first paint. Delegates to the existing opt-in
    seeder (`cairn demo`, #394 — every installed `cairn.seeds` provider, #2418:
    the example pack from a checkout, plus the platform's scenarios when it is
    installed; each leaves one pipeline waiting at a person gate) run under the
    PINNED env — the data lands in dev.db, never in whatever the shell's .env
    points at. ONE code path: `cairn demo` → `seed_demo_workspace`. Idempotent."""
    cairn_bin = Path(sys.executable).with_name("cairn")
    cmd = [str(cairn_bin), "demo"] if cairn_bin.exists() else [
        sys.executable, "-P", "-m", "cairn.cli.main", "demo"]
    typer.secho("seeding demo data into the dev store…", fg=typer.colors.GREEN)
    r = subprocess.run(cmd, env=env)
    if r.returncode:
        typer.secho("seed failed — continuing without demo data.", fg=typer.colors.YELLOW)


@app.callback(invoke_without_command=True)
def dev_cmd(
    port: int = typer.Option(8000, "--port", help="Engine port (vite proxies to it)."),
    engine_only: bool = typer.Option(False, "--engine-only", help="Start only the engine."),
    web_only: bool = typer.Option(False, "--web-only", help="Start only the console dev server."),
    db: Optional[str] = typer.Option(None, "--db", help="Sqlite path (default ~/.cairn/dev.db)."),
    seed: bool = typer.Option(
        False, "--seed",
        help="Seed deterministic demo data first (#1189) — every surface non-empty on first paint.",
    ),
) -> None:
    """Start the engine + console on a pinned sqlite dev config. Ctrl-C stops both."""
    if engine_only and web_only:
        typer.secho("--engine-only and --web-only are mutually exclusive.", fg=typer.colors.RED)
        raise typer.Exit(2)

    env = dev_env(port, db)
    if seed:
        _seed(env)
    procs: list[subprocess.Popen] = []

    if not web_only:
        typer.secho(
            f"engine  → http://localhost:{port}  (sqlite {env['CAIRN_STORAGE'].removeprefix('sqlite:///')}, "
            "login dev/dev)",
            fg=typer.colors.GREEN,
        )
        procs.append(subprocess.Popen(
            [sys.executable, "-P", "-m", "cairn.core.triggers.main"], env=env,  # -P: never the cwd's cairn/
        ))

    if not engine_only:
        web = _web_dir()
        if web is None:
            # The web console is the platform's (#2427): the public engine serves no
            # /console, so do not point anyone at one.
            typer.secho(
                "no web console here: it is not part of the public core (the cairn "
                "platform ships it). Running the engine only; use the CLI "
                "(`cairn remote login`, `cairn remote ps`) or the HTTP API. Pass "
                "--engine-only to skip this check.",
                fg=typer.colors.YELLOW,
            )
        else:
            typer.secho(f"console → vite dev server ({web})", fg=typer.colors.GREEN)
            web_env = dict(env)
            web_env["CAIRN_ENGINE"] = f"http://localhost:{port}"  # vite proxy target
            procs.append(subprocess.Popen(["npm", "run", "dev"], cwd=str(web), env=web_env))

    if not procs:
        raise typer.Exit(2)

    # Ctrl-C → terminate both children, wait, exit clean. If either child dies
    # on its own, stop the other too — half a dev env is worse than none.
    def _shutdown(*_a: object) -> None:
        for p in procs:
            if p.poll() is None:
                p.terminate()

    signal.signal(signal.SIGTERM, _shutdown)
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    _shutdown()
    for p in procs:
        p.wait()
    code = max((p.returncode or 0) for p in procs)
    if code:
        raise typer.Exit(code)
