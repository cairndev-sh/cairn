"""``cairn deploy`` — operate the unified deployment stack (issue #390).

Slice 1: ``deploy doctor`` — preflight that the deploy ``.env`` and the enabled
compose profiles agree before ``docker compose up`` (e.g. CAIRN_RUNTIME=temporal
without the 'temporal' profile → fail loud). The validation logic lives in
``cairn.core.deploy_check`` (pure, unit-testable); this is the thin CLI wrapper.
"""
from __future__ import annotations

from pathlib import Path

import typer

from cairn.core.deploy_check import (
    REQUIRED,
    check_caddyfile,
    check_deploy,
    check_values,
    env_from_values,
    exit_code,
    format_deploy_report,
    parse_env_file,
    parse_profiles,
    parse_values_file,
    profiles_from_values,
)

app = typer.Typer(
    name="deploy",
    help="Operate the unified deployment stack (compose profiles + one .env).",
    no_args_is_help=True,
    add_completion=False,
)


@app.command()
def doctor(
    env: Path = typer.Option(
        Path(".env"), "--env", "-e",
        help="Path to the deploy .env file to validate.",
    ),
    profile: list[str] = typer.Option(
        None, "--profile", "-p",
        help="Enabled compose profile (repeatable). Merged with COMPOSE_PROFILES "
             "from the .env.",
    ),
    values: Path = typer.Option(
        None, "--values", "-f",
        # chart deploy: ADR-0104 D4
        help="Validate a HELM VALUES file instead of a .env. The values "
             "are rendered into the effective environment and run through the SAME "
             "checks; --env and --profile are ignored.",
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Treat warnings as failures (exit non-zero).",
    ),
) -> None:
    """Validate that the deploy .env and enabled profiles are consistent."""
    # ── chart deploy (ADR-0104 D4, #26) ──────────────────────────────────
    # A values file carries the same information in a different shape, so it is
    # TRANSLATED into (env, profiles) and handed to the same check_deploy — a
    # rule added for compose applies to the chart for free. No Caddyfile check:
    # the chart has an Ingress, and its render-time door (_helpers.tpl) already
    # refuses the configurations check_caddyfile exists to catch.
    if values is not None:
        loaded = parse_values_file(values)
        if not loaded:
            typer.echo(f"no values loaded from {values}", err=True)
            raise typer.Exit(code=1)
        results = check_deploy(env_from_values(loaded), profiles_from_values(loaded))
        results += check_values(loaded, strict=strict)
        print(format_deploy_report(results))
        raise typer.Exit(code=exit_code(results, strict=strict))

    # #2536 — no .env at all is one finding, not one FAIL per variable it would hold:
    # a fresh install is not a deployment, and the doctor has nothing to check yet.
    # A Caddyfile already beside it is still checked (a box whose .env went missing).
    if not env.exists():
        typer.echo(f"no deploy .env at {env} — `cairn deploy doctor` preflights a "
                   "deployment's .env before `docker compose up`; a local install needs "
                   "none. Pass --env <file>, or --values <helm values> for the chart.",
                   err=True)
        caddyfile = env.parent / "Caddyfile"
        if caddyfile.exists():
            print(format_deploy_report(check_caddyfile(caddyfile)))
        raise typer.Exit(code=1)
    env_values = parse_env_file(env)
    profiles = set(profile or []) | parse_profiles(env_values.get("COMPOSE_PROFILES"))
    results = check_deploy(env_values, profiles)
    # #1565 — the Caddyfile sits beside the .env and is rsync-EXCLUDED (gotcha
    # #10), so it is hand-edited on the box and nothing ever preflighted it. A
    # duplicate site block crash-loops Caddy and takes the whole ingress down.
    results += check_caddyfile(env.parent / "Caddyfile")
    print(format_deploy_report(results))
    # The required variables stay FAIL: the compose stack declares each `${VAR:?}`
    # and refuses to start without it. Say whose requirement that is (#2536).
    if any(r.status == "fail" and r.name in REQUIRED for r in results):
        print("\nrequired = what the compose stack refuses to start without; a local "
              "`cairn` install (CLI, `cairn dev`) needs none of them.")
    raise typer.Exit(code=exit_code(results, strict=strict))
