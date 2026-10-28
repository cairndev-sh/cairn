"""cairn connect — manage typed infra connections (#68 / #98).

A thin, governed CLI over the :class:`ConnectionStore`. ``cairn connect add
<provider>`` is the headline verb: the provider catalog knows each provider's
connection ``kind`` + the secret refs it needs, so ``cairn connect add openai``
produces a working api_key connection pointed at ``OPENAI_API_KEY`` with no
flags. Secrets are stored as *references* into the SecretStore (ADR-0033),
never as raw values — this command never reads, prints, or persists a secret
value.

Subcommands::

    cairn connect add <provider> [--name] [--set k=v] [--secret KEY=REF]
    cairn connect add --preset <name> [--name] [--set k=v] [--secret KEY=REF]
    cairn connect ls
    cairn connect test <name>
    cairn connect rm <name>
    cairn connect providers
    cairn connect presets

Before ``add`` or ``rm`` writes, the CLI checks that the engine it is configured for
(the endpoint ``cairn remote`` uses) reads the same store (#229): the engine reports a
fingerprint of its store on ``/healthz``. When they differ the write is refused with
exit 3, unless ``--force``.

``--preset`` builds a kind=mcp connection from a shipped connector preset
(``cairn/connectors/<name>.toml``, read only by ``cairn.services.mcp.presets``)
through the same catalog path as ``cairn connect add mcp``; flags override the
preset's values and the preset's required env vars become secret *references*.
"""
from __future__ import annotations

import asyncio
import shlex

import typer
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from cairn.core.connection_probe import ProbeResult, test_connection
from cairn.core.connections import (
    DEFAULT_ACCOUNT,
    PROVIDER_CATALOG,
    Connection,
    connection_from_provider,
    default_connection_store,
    resolve_connection,
    resolve_provider_key,
)
from cairn.core.oauth import AuthSession, OAuthBrokerError, get_oauth_broker
from cairn.services.mcp import presets

app = typer.Typer(
    help="Manage typed infra connections (model APIs, compute, MCP) (#68). "
    "`cairn connect presets` lists the shipped MCP connector presets "
    "(github, slack, notion); add one with `cairn connect add --preset <name>`.",
    no_args_is_help=True,
)
console = Console()

#: #229 — the exit code of a write refused because the running engine reads another store.
EXIT_STORE_MISMATCH = 3


def _guard_store(force: bool) -> None:
    """#229 — refuse a write the running engine would never see.

    The qualification run's failure: an engine on ``--db ~/.cairn/train.db``, a shell
    without ``CAIRN_STORAGE``, and ``connect add`` reported success into
    ``~/.cairn/cairn.db``. Only a store with no credentials in its name is printed.
    """
    from cairn.cli.commands import remote  # noqa: PLC0415 — the engine endpoint is `cairn remote`'s

    checked = remote.store_check()
    if checked is None:
        return  # an in-memory store is nobody else's
    mine, ep, same = checked
    if same:
        return
    if same is None:
        why = (f"the engine at {ep} does not report its store" if ep
               else "no engine answered to compare stores with")
        console.print(f"[dim]writing to {escape(mine)} — {why}[/]")
        return
    if force:
        console.print(f"[yellow]writing to {escape(mine)} anyway (--force): the engine at "
                      f"{ep} reads another store and will not see this.[/]")
        return
    console.print(
        f"[red]✗ not written:[/] this CLI writes {escape(mine)}, but the engine at {ep} "
        "reads a different store, so it would never see this. Run the CLI on the "
        "engine's store (set CAIRN_STORAGE to what the engine was started with; "
        "`cairn dev --db PATH` is sqlite:///PATH), or pass --force to write here anyway.")
    raise typer.Exit(EXIT_STORE_MISMATCH)


def _kv(pairs: list[str] | None) -> dict[str, str]:
    """Parse repeatable ``key=value`` options into a dict (ignores blank keys)."""
    out: dict[str, str] = {}
    for item in pairs or []:
        k, _, v = item.partition("=")
        k = k.strip()
        if k:
            out[k] = v
    return out


def _persist(conn: Connection, force: bool) -> bool:
    """Upsert a connection; return False if it exists and ``force`` is off."""
    _guard_store(force)

    async def _save() -> bool:
        store = default_connection_store()
        try:
            existing = await store.get_by_name(conn.name)
            if existing is not None and not force:
                return False
            await store.create(conn)  # INSERT OR REPLACE → upsert by id
            return True
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()

    return asyncio.run(_save())


def _initiate_oauth(conn: Connection) -> AuthSession:
    """Start the broker's auth-code flow for a ``kind=oauth`` connection."""
    broker = get_oauth_broker(conn.config.get("broker"))
    return asyncio.run(
        broker.start_authorization(
            provider=str(conn.config.get("provider", "")),
            connection_id=str(conn.config.get("connection_id") or conn.name),
            scopes=conn.config.get("scopes") or None,
        )
    )


def _dup_exit(conn: Connection) -> None:
    console.print(
        f"[red]connection [bold]{conn.name}[/] already exists[/] "
        "— pass [bold]--force[/] to overwrite."
    )
    raise typer.Exit(1)


def _render_saved(conn: Connection, *, extra: str = "") -> None:
    secret_keys = ", ".join(f"{k}→{v}" for k, v in sorted(conn.secrets.items())) or "—"
    cfg = ", ".join(f"{k}={v}" for k, v in sorted(conn.config.items()) if v) or "—"
    console.print(
        Panel(
            f"[bold]{conn.name}[/]  ·  kind [bold]{conn.kind}[/]"
            f"{extra}"
            f"{'  ·  [yellow]disabled[/]' if not conn.enabled else ''}\n"
            f"secret refs: {secret_keys}\n"
            f"config: {cfg}",
            title="[green]✓ connection saved[/]",
            border_style="green",
        )
    )


@app.command("add")
def add_cmd(
    provider: str = typer.Argument(
        None, help="provider name, e.g. openai / anthropic / docker / runpod / mcp; "
        "with --via oauth this is the OAuth provider key (github, slack, …); "
        "omit it with --preset"
    ),
    name: str = typer.Option(
        None, "--name", help="connection name (default: the provider or preset name)"
    ),
    set_config: list[str] = typer.Option(
        None, "--set", help="config override key=value (repeatable)"
    ),
    secret: list[str] = typer.Option(
        None, "--secret", help="secret-ref override KEY=ENV_REF (repeatable)"
    ),
    description: str = typer.Option(None, "--description", help="override description"),
    disabled: bool = typer.Option(False, "--disabled", help="create it disabled"),
    force: bool = typer.Option(
        False, "--force", help="overwrite an existing connection of the same name; "
        "also write when the running engine reads another store"
    ),
    via: str = typer.Option(
        None, "--via", help="broker an OAuth connection: --via oauth (provider can be "
        "any OAuth provider, not just the catalog)"
    ),
    broker: str = typer.Option(
        None, "--broker", help="oauth broker name (default: CAIRN_OAUTH_BROKER → authlib)"
    ),
    connection_id: str = typer.Option(
        None, "--connection-id", help="broker connection id (default: the name)"
    ),
    scope: list[str] = typer.Option(None, "--scope", help="oauth scope (repeatable)"),
    preset: str = typer.Option(
        None, "--preset", help="build an MCP connection from a shipped connector "
        "preset (see `cairn connect presets`); --name/--set/--secret/--description "
        "override its values"
    ),
) -> None:
    """Add (or --force replace) a connection — from a catalog provider, an MCP
    connector preset (``--preset``), or an OAuth connection via a broker
    (``--via oauth``)."""
    if preset is not None:
        if provider is not None or via is not None:
            console.print("[red]--preset takes the place of the provider argument and --via[/] "
                          "— pass one or the other.")
            raise typer.Exit(2)
        conn = _from_preset(
            preset, name=name, set_config=set_config, secret=secret,
            description=description, enabled=not disabled,
        )
        _save_and_probe(conn, force)
        return
    if provider is None:
        console.print("[red]missing provider[/] — `cairn connect add <provider>` "
                      "(see [bold]cairn connect providers[/]) or `--preset <name>`.")
        raise typer.Exit(2)
    if via is not None:
        _add_oauth(
            provider, name=name, broker=broker, connection_id=connection_id,
            scopes=scope, set_config=set_config, secret=secret,
            description=description, disabled=disabled, force=force, via=via,
        )
        return

    key = resolve_provider_key(provider)
    if key is None:
        known = ", ".join(sorted(PROVIDER_CATALOG))
        console.print(
            Panel(
                f"unknown provider [red]{provider}[/]\n\nknown: {known}\n\n"
                "(MCP/custom: use [bold]cairn connect add mcp --set url=…[/]; "
                "OAuth: [bold]cairn connect add github --via oauth[/])",
                title="[red]no such provider[/]",
                border_style="red",
            )
        )
        raise typer.Exit(2)

    conn = connection_from_provider(
        key,
        name=name,
        config=_kv(set_config),
        secrets=_kv(secret),
        description=description,
        enabled=not disabled,
    )
    _save_and_probe(conn, force)


def _save_and_probe(conn: Connection, force: bool) -> None:
    """Persist a catalog-built connection, render it, run the on-add probe."""
    if not _persist(conn, force):
        _dup_exit(conn)
    _render_saved(conn)
    # On-add hook: run the authenticated probe once and persist its health so the
    # row shows a health dot immediately (best-effort — never blocks the add).
    _, result = asyncio.run(_probe_by_name(conn.name))
    _render_probe(result)
    console.print(f"  [dim]re-check anytime with[/] [bold]cairn connect test {conn.name}[/]")


def _from_preset(
    preset: str,
    *,
    name: str | None,
    set_config: list[str] | None,
    secret: list[str] | None,
    description: str | None,
    enabled: bool,
) -> Connection:
    """A kind=mcp connection from a shipped preset, via the ``mcp`` catalog
    provider. The preset's [server] fills the config, its required env vars
    become secret refs (``VAR → VAR``, resolved at use); flags win."""
    if preset not in presets.list_presets():
        console.print(
            Panel(
                f"unknown preset [red]{preset}[/]\n\nknown: {', '.join(presets.list_presets())}",
                title="[red]no such preset[/]",
                border_style="red",
            )
        )
        raise typer.Exit(1)
    spec = presets.load_preset(preset)
    server = spec.get("server", {})
    command = list(server.get("command") or [])
    config = {
        "transport": server.get("transport", "stdio"),
        "command": command[0] if command else "",
        "args": shlex.join(command[1:]),
        "url": server.get("url", ""),
    }
    refs = {k: k for k in presets.required_env(preset)}
    return connection_from_provider(
        "mcp",
        name=name or preset,
        config={**config, **_kv(set_config)},
        secrets={**refs, **_kv(secret)},
        description=spec.get("connector", {}).get("description", "") if description is None else description,
        enabled=enabled,
    )


def _add_oauth(
    provider: str,
    *,
    name: str | None,
    broker: str | None,
    connection_id: str | None,
    scopes: list[str] | None,
    set_config: list[str] | None,
    secret: list[str] | None,
    description: str | None,
    disabled: bool,
    force: bool,
    via: str,
) -> None:
    """`--via oauth` path: persist a kind=oauth connection, then start the
    broker's auth-code flow and print the redirect URL to open."""
    if via.lower() != "oauth":
        console.print(f"[red]unknown --via {via!r}[/] — only 'oauth' is supported.")
        raise typer.Exit(2)

    # Validate the broker name before persisting (don't save a dangling broker).
    try:
        get_oauth_broker(broker)
    except KeyError as exc:
        console.print(Panel(str(exc), title="[red]unknown oauth broker[/]",
                            border_style="red"))
        raise typer.Exit(2)

    conn_name = name or provider
    config: dict = {
        **_kv(set_config),
        "provider": provider,
        "connection_id": connection_id or conn_name,
    }
    if broker:
        config["broker"] = broker            # pin per-connection; else global default
    if scopes:
        config["scopes"] = list(scopes)
    conn = Connection(
        name=conn_name,
        kind="oauth",
        config=config,
        secrets=_kv(secret),
        description=description or f"OAuth ({broker or 'default broker'}): {provider}",
        enabled=not disabled,
    )

    if not _persist(conn, force):
        _dup_exit(conn)
    _render_saved(conn, extra=f"  ·  broker [bold]{broker or 'default'}[/]")

    try:
        session = _initiate_oauth(conn)
    except OAuthBrokerError as exc:
        console.print(
            f"[yellow]saved, but couldn't start authorization:[/] {exc}\n"
            f"  [dim]set the provider client creds (CAIRN_OAUTH_<P>_CLIENT_ID/SECRET) and retry:[/] "
            f"[bold]cairn connect authorize {conn.name}[/]"
        )
        return
    console.print(
        Panel(f"[bold]{session.redirect_url}[/]",
              title="authorize — open this URL", border_style="cyan")
    )
    console.print(f"  [dim]then verify with[/] [bold]cairn connect test {conn.name}[/]")


@app.command("authorize")
def authorize_cmd(
    name: str = typer.Argument(..., help="oauth connection name to (re)authorize"),
) -> None:
    """(Re)start the OAuth flow for an existing kind=oauth connection."""

    async def _get():
        store = default_connection_store()
        try:
            return await store.get_by_name(name)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()

    conn = asyncio.run(_get())
    if conn is None:
        console.print(f"[red]no connection named [bold]{name}[/][/]")
        raise typer.Exit(1)
    if conn.kind != "oauth":
        console.print(
            f"[red]connection [bold]{name}[/] is kind={conn.kind}, not oauth[/]"
        )
        raise typer.Exit(1)
    try:
        session = _initiate_oauth(conn)
    except (KeyError, OAuthBrokerError) as exc:
        console.print(Panel(str(exc), title="[red]authorization failed[/]",
                            border_style="red"))
        raise typer.Exit(1)
    console.print(
        Panel(f"[bold]{session.redirect_url}[/]",
              title=f"authorize {name} — open this URL", border_style="cyan")
    )
    console.print(f"  [dim]then verify with[/] [bold]cairn connect test {name}[/]")


@app.command("ls")
def ls_cmd() -> None:
    """List configured connections (names + kinds + secret keys — never values)."""

    async def _list():
        store = default_connection_store()
        try:
            return await store.list()
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()

    conns = asyncio.run(_list())
    if not conns:
        console.print(
            "[dim]no connections yet — add one with[/] "
            "[bold]cairn connect add <provider>[/] "
            "[dim](see[/] [bold]cairn connect providers[/][dim])[/]"
        )
        return
    t = Table("name", "kind", "enabled", "secret keys", "description",
              title="connections")
    for c in conns:
        t.add_row(
            c.name,
            c.kind,
            "[green]yes[/]" if c.enabled else "[red]no[/]",
            ", ".join(c.secret_keys) or "—",
            c.description or "",
        )
    console.print(t)


def _render_probe(result: ProbeResult) -> None:
    """Print the authenticated-probe verdict panel (health dot + latency)."""
    if result.ok is True:
        lat = f"  ·  {result.latency_ms:.0f} ms" if result.latency_ms is not None else ""
        console.print(Panel(f"[green]● connected[/] — {result.detail}{lat}", border_style="green"))
    elif result.ok is None:
        console.print(Panel(f"[yellow]● {result.detail}[/]", title="degraded", border_style="yellow"))
    else:
        code = f"  ·  HTTP {result.status}" if result.status else ""
        console.print(Panel(f"[red]● failed[/] — {result.detail}{code}", border_style="red"))


@app.command("test")
def test_cmd(
    name: str = typer.Argument(..., help="connection name (from `cairn connect ls`)"),
) -> None:
    """Resolve a connection's secret refs via the SecretStore, then run the
    provider's **authenticated** health probe (the creds actually work) and
    persist the result. Secret values are checked for presence only — never
    printed. Exit 0 = connected/degraded-but-reachable, 1 = missing/failed."""
    # the CLI is the deployment's own tool: it tests the deployment's connections
    resolved = asyncio.run(resolve_connection(name, account_id=DEFAULT_ACCOUNT))
    if resolved is None:
        console.print(
            Panel(
                f"connection [bold]{name}[/] not found or disabled",
                title="[red]✗ unusable[/]",
                border_style="red",
            )
        )
        raise typer.Exit(1)

    t = Table("secret key", "resolves?", title=f"{name} · {resolved.kind}")
    all_ok = True
    for k in sorted(resolved.secrets):
        ok = bool(resolved.secrets[k])
        all_ok = all_ok and ok
        t.add_row(k, "[green]✓[/]" if ok else "[red]✗ (unset)[/]")
    if not resolved.secrets:
        t.add_row("[dim](no secrets)[/]", "[green]✓[/]")
    console.print(t)

    cfg = ", ".join(f"{k}={v}" for k, v in sorted(resolved.config.items()) if v)
    if cfg:
        console.print(f"  [dim]config:[/] {cfg}")

    if not all_ok:
        console.print(Panel("[red]✗ missing secret(s)[/]", border_style="red"))
        raise typer.Exit(1)

    # Authenticated probe (the creds actually work) + persist health on the row.
    conn, result = asyncio.run(_probe_by_name(name))
    _render_probe(result)
    # ok is True/None (reachable-or-unsupported) → 0; explicit failure → 1.
    raise typer.Exit(1 if result.ok is False else 0)


async def _probe_by_name(name: str):
    """Load a connection by name and run + persist the authenticated probe."""
    store = default_connection_store()
    try:
        conn = await store.get_by_name(name)
        if conn is None:
            return None, ProbeResult(ok=False, status=0, detail=f"no connection {name!r}")
        return await test_connection(conn.id, store=store)
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()


@app.command("rm")
def rm_cmd(
    name: str = typer.Argument(..., help="connection name to remove"),
    force: bool = typer.Option(
        False, "--force", help="remove even when the running engine reads another store"),
) -> None:
    """Delete a connection. Removes only the reference — never the secret value."""
    _guard_store(force)

    async def _rm() -> bool:
        store = default_connection_store()
        try:
            conn = await store.get_by_name(name)
            if conn is None:
                return False
            return await store.delete(conn.id)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()

    if asyncio.run(_rm()):
        console.print(f"[green]removed[/] [bold]{name}[/]")
    else:
        console.print(f"[red]no connection named [bold]{name}[/][/]")
        raise typer.Exit(1)


@app.command("providers")
def providers_cmd() -> None:
    """List the provider catalog — what you can `cairn connect add`."""
    t = Table("provider", "kind", "secret keys", "description",
              title="connectable providers")
    for key in sorted(PROVIDER_CATALOG):
        spec = PROVIDER_CATALOG[key]
        t.add_row(
            key,
            spec.kind,
            ", ".join(sorted(spec.secrets)) or "—",
            spec.description,
        )
    console.print(t)
    console.print(
        "  [dim]add with[/] [bold]cairn connect add <provider>[/]"
        "  [dim]· aliases: hf, claude, google, k8s[/]"
    )


@app.command("presets")
def presets_cmd() -> None:
    """List the shipped MCP connector presets — what you can `cairn connect add --preset`."""
    t = Table("preset", "provider", "transport", "required env", "description",
              title="MCP connector presets")
    for key in presets.list_presets():
        spec = presets.load_preset(key)
        t.add_row(
            key,
            "mcp",
            spec.get("server", {}).get("transport", "stdio"),
            ", ".join(presets.required_env(key)) or "—",
            spec.get("connector", {}).get("description", ""),
        )
    console.print(t)
    console.print("  [dim]add with[/] [bold]cairn connect add --preset <name>[/]")
