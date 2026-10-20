"""``cairn-server`` — the trigger server, runnable from an installed wheel.

Console-script entrypoint (ADR-0029). Pack-neutral by construction: the
``Runner`` self-assembles from entry-point pack discovery (skills, policies,
templates come from whatever packs are installed) and the default notifiers
attach (stdout + inbox; Slack if ``SLACK_WEBHOOK_URL`` is set). The public core
wires no action handler (#2497): actions are policy-evaluated, and executed only
where a ``cairn.routers`` provider injects a handler into the runner.

Run::

    OPENAI_API_KEY=sk-... cairn-server          # installed wheel
    OPENAI_API_KEY=sk-... .venv/bin/cairn-server  # repo checkout

Then::

    curl http://localhost:9090/healthz
    curl -X POST http://localhost:9090/triggers/manual -d '{...}'

Knobs: ``CAIRN_TRIGGER_PORT`` (default 9090), ``CAIRN_BACKEND`` (default ``mock``
so the server boots without external infra), ``CAIRN_ENVIRONMENT``, plus
everything in :mod:`cairn.core.settings`.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import logging
import os
import sys


def build_default_runner():
    """A pack-neutral Runner: discovery-driven skills/policies and default
    notifiers. No action handler — the core executes no actions by itself (#2497)."""
    from cairn.core.engine.runner import Runner
    from cairn.core.notifier import NotifierRegistry, default_notifiers
    from cairn.core.operator import load_all_operators
    from cairn.core.pack.loader import ensure_loaded

    ensure_loaded(strict=False)
    # Operators register from the `cairn.operators` entry-point group — a
    # SEPARATE group from packs. The server must load them too (the CLI and the
    # dbos/temporal runtimes already do), else server-dispatched in_process runs
    # compile against an empty operator registry and fail with
    # "No operator registered ... Known: (none)".
    load_all_operators(strict=False)

    notifiers = NotifierRegistry()
    notifiers.register_many(default_notifiers())

    return Runner(
        notifiers=notifiers,
        default_environment=getenv("CAIRN_ENVIRONMENT", "production"),  # type: ignore[arg-type]
    )


def main() -> int:
    # Load .env first (real OPENAI_API_KEY etc. into os.environ) before building
    # the runner / LLM clients — imported lazily to match this module's startup
    # deferral. See provider_settings.load_env.
    from cairn.core.provider_settings import load_env

    load_env()
    # Mock backend by default so a bare install boots without external infra.
    if getenv("CAIRN_BACKEND") is None:  # either spelling counts (ADR-0122)
        os.environ["CAIRN_BACKEND"] = "mock"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )
    from cairn.core.pack.profile import ProfileError, require_deployment_profile

    try:
        require_deployment_profile()
    except ProfileError as e:
        logging.getLogger("cairn.server").error("refusing to start: %s", e)
        return 2

    import uvicorn

    from cairn.core.triggers.server import build_app

    runner = build_default_runner()
    app = build_app(runner)

    # server-side in-process clients (the /ask flow's resolve reads, #1196)
    # must hit THIS app, not build a second one
    from cairn.core.app_client import set_local_app
    set_local_app(app)

    port = int(getenv("CAIRN_TRIGGER_PORT", "9090"))
    log = logging.getLogger("cairn.server")

    # Bind address (#1507). The host was hardcoded to 0.0.0.0, so a keyless
    # `cairn-server` published an OPEN control plane on every interface — the
    # fail-open default and a public bind compounding into the worst case.
    #
    # Being unauthenticated is survivable on loopback (that IS `cairn dev`).
    # Being unauthenticated AND reachable from the network is not. So the
    # default now follows the auth state, and an operator who genuinely wants
    # the old behaviour has to say so.
    from cairn.core.triggers.auth import auth_enabled  # noqa: PLC0415 - cycle: main → server → auth

    host = getenv("CAIRN_TRIGGER_HOST", "").strip()
    if not host:
        host = "0.0.0.0" if auth_enabled() else "127.0.0.1"  # noqa: S104
        if not auth_enabled():
            log.warning(
                "AUTH IS OFF — binding to 127.0.0.1 only. An unauthenticated "
                "control plane must not be reachable from the network. Set "
                "CAIRN_JWT_SECRET to enable auth (then the default bind is "
                "0.0.0.0), or set CAIRN_TRIGGER_HOST to override this "
                "deliberately. Inside a container 127.0.0.1 is the container's "
                "own loopback, so a published port answers nothing: turn auth "
                "on, or for a keyless trial set CAIRN_TRIGGER_HOST=0.0.0.0 and "
                "publish on the host's loopback (-p 127.0.0.1:9090:9090)."
            )

    log.info("trigger server starting on %s:%d", host, port)
    log.info("skills loaded: %s", runner.skill_engine.list_skills())
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
