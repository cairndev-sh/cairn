"""
``cairn doctor`` — pre-flight check that tells an operator what's missing.

Reads :data:`~cairn.core.settings.INVENTORY` as the source of truth for
"what knobs exist," and probes the services that are actually configured (no
point probing a Temporal cluster when the runtime is inprocess). The
output is a per-category report with one of three statuses per check:

  ✓ ok    — set / reachable / writable
  ⚠ warn  — unset-but-optional, unreachable, defaulted in a way operators often
            want to override (e.g. unset CAIRN_STORAGE and CAIRN_RUN_DB →
            in-memory store that loses state on restart)
  ✗ fail  — required-but-missing, unwritable path, or invalid value

Pairs with ``cairn validate`` (artifact validation) and ``cairn ls``
(pack contributions): small focused CLIs, one per concern.

Designed to be cheap, offline-tolerant, and CI-runnable: every probe has a
short timeout and any network failure degrades to a ``warn``, not a crash.

The :class:`CheckResult` type lives in :mod:`cairn.core.pack.protocol`
because packs produce CheckResults — they must not depend on this CLI module.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import typer

from cairn.core import settings as _settings
from cairn.core.pack import CheckResult
from cairn.core.pack.drift import check_installed_operator_drift
from cairn.core.pack.provisioning import REQUIRES_BACKEND

_log = logging.getLogger("cairn.cli.commands.doctor")

_PROBE_TIMEOUT_SECONDS = 2.0


# ─── settings inventory pass ────────────────────────────────────────


def _mask(value: str) -> str:
    """Mask a secret like 'xoxb-1234567' → 'xoxb-12******'. A URL, or a DSN missing
    its scheme, goes through ``redacted_url`` instead: its first characters are the
    user and password (GHSA-gw2v-3922-g7gm)."""
    if not value:
        return ""
    if _settings.url_scheme(value) or "@" in value:
        return _settings.redacted_url(value)
    keep = min(8, max(2, len(value) // 4))
    return value[:keep] + "*" * max(4, len(value) - keep)


def check_settings() -> list[CheckResult]:
    """One CheckResult per declared knob in the inventory."""
    out: list[CheckResult] = []
    for spec in _settings.get_inventory():
        legacy = spec.legacy_env_var
        raw = _settings.getenv(spec.env_var) if legacy else os.environ.get(spec.env_var)
        if raw is None or raw == "":
            if spec.required:
                out.append(CheckResult(spec.env_var, "fail",
                                       f"required but unset ({spec.description})",
                                       category=spec.category))
            elif spec.default:
                out.append(CheckResult(spec.env_var, "ok",
                                       f"using default '{spec.default}'",
                                       category=spec.category))
            else:
                out.append(CheckResult(spec.env_var, "warn",
                                       "unset (optional)",
                                       category=spec.category))
            continue
        # A knob not marked secret can still hold a DSN (CAIRN_BUS, CAIRN_JOB_CHANNEL)
        shown = _mask(raw) if spec.secret else _settings.redacted_url(raw)
        if legacy and not os.environ.get(spec.env_var):  # ADR-0122 D4: read until 1.0, then ignored
            out.append(CheckResult(spec.env_var, "warn",
                                   f"set: {shown} via {legacy} — deprecated, rename it to {spec.env_var}",
                                   category=spec.category))
            continue
        out.append(CheckResult(spec.env_var, "ok", f"set: {shown}", category=spec.category))
    return out


# ─── filesystem: investigation DB ───────────────────────────────────


def check_run_db() -> CheckResult:
    """Where the run store persists, chosen as ``storage.default_store`` chooses it:
    ``CAIRN_STORAGE`` (Postgres or a ``sqlite://`` file), else ``CAIRN_RUN_DB``, else
    in-memory (#61). A SQLite file's parent dir must be writable."""
    url = getenv("CAIRN_STORAGE", "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        label = _settings.store_identity()[0]  # host:port/db, never the user or password
        return CheckResult("run_db", "ok", f"{label} (CAIRN_STORAGE)", category="store")
    if url.startswith("sqlite://"):
        path, knob = _settings.sqlite_path_from_url(url) or ":memory:", "CAIRN_STORAGE"
    elif url:
        try:  # only the scheme: the value may carry a user and password
            scheme = urlsplit(url).scheme
        except ValueError:
            scheme = ""
        if not url.lower().startswith(f"{scheme}://"):
            scheme = ""
        named = f"has an unrecognized scheme {scheme!r}" if scheme else "is not a storage URL"
        return CheckResult("run_db", "fail",
                           f"CAIRN_STORAGE {named}; the run store refuses it "
                           "(postgres://…, sqlite:///path.db, sqlite://:memory:)",
                           category="store")
    else:
        path, knob = _settings.run_db_path(), "CAIRN_RUN_DB"
    if not path:
        return CheckResult(
            "run_db", "warn",
            "CAIRN_STORAGE and CAIRN_RUN_DB unset → in-memory store (state lost on restart)",
            category="store",
        )
    if path == ":memory:":
        return CheckResult("run_db", "ok", "in-memory (:memory:)", category="store")
    parent = Path(path).expanduser().resolve().parent
    if not parent.exists():
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            return CheckResult("run_db", "fail",
                               f"cannot create parent {parent}: {e}", category="store")
    if not os.access(parent, os.W_OK):
        return CheckResult("run_db", "fail",
                           f"parent dir {parent} is not writable", category="store")
    return CheckResult("run_db", "ok",
                       f"{path} ({knob}); parent {parent} is writable", category="store")


def check_engine_store() -> Optional[CheckResult]:
    """#229 — does the engine this CLI is configured for read the store this CLI
    writes? ``cairn connect add`` refuses a write when it does not; this says so
    before anyone writes. ``None`` (no row) when no engine answers."""
    from cairn.cli.commands.remote import store_check  # noqa: PLC0415 — cli → cli, lazy like the rest

    checked = store_check()
    if checked is None or checked[1] is None:
        return None
    mine, ep, same = checked
    if same is None:
        return CheckResult("engine_store", "warn",
                           f"the engine at {ep} does not report its store; "
                           f"cannot compare it with {mine}", category="store")
    if same:
        return CheckResult("engine_store", "ok", f"{mine} — the store the engine at {ep} reads",
                           category="store")
    return CheckResult("engine_store", "warn",
                       f"this CLI writes {mine}, but the engine at {ep} reads a different "
                       "store: `cairn connect add` refuses until CAIRN_STORAGE names the "
                       "engine's store (or --force)", category="store")


# ─── network reachability probes ────────────────────────────────────


async def _probe_url(url: str) -> tuple[bool, str]:
    """HEAD with a tight timeout. ANY HTTP response = reachable; only TCP/DNS
    errors or timeouts count as unreachable."""
    try:
        import httpx
    except ImportError:
        return False, "httpx not installed"
    try:
        async with httpx.AsyncClient(timeout=_PROBE_TIMEOUT_SECONDS) as c:
            resp = await c.request("HEAD", url)
            return True, f"HTTP {resp.status_code}"
    except Exception as e:  # ConnectError, TimeoutException, etc.
        return False, type(e).__name__


async def check_runtime() -> CheckResult:
    """Temporal Step 3 — surface the configured StateGraph runtime.

    ``inprocess`` is always ok. ``temporal`` verifies the SDK import and
    probes the cluster address (gRPC port, plain TCP connect) — missing
    SDK is a fail (runs would crash at submit); unreachable cluster is a
    warn (it may simply not be up yet)."""
    rt = _settings.runtime()
    if rt != "temporal":
        return CheckResult("runtime", "ok", f"{rt} (default)", category="runtime")
    try:
        import temporalio  # noqa: F401
        from temporalio.contrib import langgraph as _lg  # noqa: F401
    except ImportError as e:
        return CheckResult(
            "runtime", "fail",
            f"CAIRN_RUNTIME=temporal but the SDK is missing ({e}); "
            "pip install 'temporalio[langgraph]'",
            category="runtime",
        )
    address = _settings.temporal_address()
    host, _, port = address.partition(":")
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port or 7233)), timeout=2.0,
        )
        writer.close()
        await writer.wait_closed()
        detail = f"temporal — {address} reachable"
        status = "ok"
    except Exception as e:  # noqa: BLE001 — any connect failure is a warn
        detail = f"temporal — {address} unreachable ({type(e).__name__})"
        status = "warn"
    return CheckResult("runtime", status, detail, category="runtime")


async def check_slack_webhook() -> Optional[CheckResult]:
    url = _settings.slack_webhook_url()
    if not url:
        return None
    ok, detail = await _probe_url(url)
    return CheckResult(
        "slack_webhook",
        "ok" if ok else "warn",
        f"{_mask(url)} — {detail}",
        category="reachability",
    )


async def check_metaflow_service() -> Optional[CheckResult]:
    """#1365 — a configured-but-unreachable Metaflow metadata service is a
    named preflight FAILURE, not a run-time surprise.

    The service is what removes Metaflow's box affinity: without it the client
    resolves runs via CWD-scoped local metadata, so a process that didn't
    submit the run gets ``MetaflowNotFound`` (#1358, live-verified). A wrong
    URL fails in exactly the same silent way — hence "fail", not "warn":
    every cross-box/detached feature the deployment configured this FOR is
    broken while it's unreachable."""
    from cairn.core.backend_selection import selected  # noqa: PLC0415 — cycle: backend_selection → pack.profile

    url = selected("metaflow_metadata_service").strip()
    if not url:
        return None
    ok, detail = await _probe_url(url.rstrip("/") + "/ping")
    return CheckResult(
        "metaflow_service",
        "ok" if ok else "fail",
        f"{url} — {detail}"
        + ("" if ok else " (run resolution falls back to CWD-scoped local "
                         "metadata: cross-box resume and detached runs break, #1358)"),
        category="reachability",
    )


# selected engine → (importable module, the extra that provides it)
_ENGINE_CLIENT = {
    "metaflow": ("metaflow", "cairndev[metaflow]"),   # ADR-0134 D2: its own optional wheel
}


def check_engine_clients() -> list[CheckResult]:
    """An engine the environment SELECTS (``dataflow_engine`` / ``ml_engine``)
    must have its client library importable in THIS process, or every flow leg
    fails at submit — after the run has been admitted, gated and recorded.

    Found live 2026-08-18: the prod image shipped with no engine libraries at
    all (``metaflow`` and ``duckdb`` both missing); doctor said
    ``ml_engine=metaflow`` in green because it checked the *service* URL, never
    the *client*. Every engine run in history had ridden a hand-patched
    container. This is the reachability lesson: probe the path the run takes."""
    import importlib.util  # noqa: PLC0415 - stdlib, kept local to this cheap check

    from cairn.core.backend_selection import selected  # noqa: PLC0415 — cycle: backend_selection → pack.profile

    out: list[CheckResult] = []
    for field in ("dataflow_engine", "ml_engine"):
        choice = selected(field).strip().lower()
        if choice not in _ENGINE_CLIENT:
            continue                                   # builtin / unset: nothing to import
        module, extra = _ENGINE_CLIENT[choice]
        present = importlib.util.find_spec(module) is not None
        out.append(CheckResult(
            f"engine_client:{field}",
            "ok" if present else "fail",
            f"{field}={choice} → `{module}` "
            + ("importable" if present else
               f"NOT importable — every {field.split('_')[0]} flow leg will fail at "
               f"submit; install `{extra}` (the image's `engines` extra bakes it)"),
            category="reachability",
        ))
    return out


def check_stage_kernels() -> list[CheckResult]:
    """#2363 (ADR-0118 D1) — every registered stage kernel must load HERE, on the control
    plane. The launch door reads a kernel's ``implements`` through the same entry point
    (``stage_implements``), and an unloadable kernel reads as legacy: no contract entry is
    recorded and its spec goes out unstamped, so an opted-in kernel would run unchecked. A
    kernel that does not load cannot say whether it opts in, so each one is reported."""
    from cairn.core.dataflow.defs import _stage_entry_points  # noqa: PLC0415 - the door's own loader; cycle-free but heavy

    broken: list[CheckResult] = []
    opted_in = 0
    eps = _stage_entry_points()
    for name, ep in sorted(eps.items()):
        try:
            opted_in += bool(getattr(ep.load(), "implements", ()))
        except Exception as e:  # noqa: BLE001 — a diagnostic must never crash the doctor
            broken.append(CheckResult(
                f"stage_kernel[{name}]", "warn",
                f"kernel {name!r} cannot be imported on the control plane ({type(e).__name__}: {e}) — "
                "its `implements` cannot be read here, so if it opts in to a stage contract the "
                "launch door records nothing and the contract is NOT checked (#2363). Install its "
                "dependencies on the control plane.", category="packs"))
    return broken or [CheckResult("stage_kernels", "ok",
                                  f"all {len(eps)} registered stage kernels load on the control plane "
                                  f"({opted_in} opted in to a stage contract)", category="packs")]


def check_seed_providers() -> list[CheckResult]:
    """ADR-0120 D4 (#2418) — the ``cairn.seeds`` providers ``cairn demo`` runs. One row
    naming them; a ``warn`` per entry point that does not load; a ``warn`` when none is
    installed, because ``cairn demo`` then seeds an empty workspace."""
    from cairn.core.pack.seeds import load_seed_providers, seed_load_failures  # noqa: PLC0415 - cli → core

    found = load_seed_providers()
    out = [CheckResult(f"seed_provider[{f['name']}]", "warn",
                       f"{f['target']} does not load ({f['error']}) — `cairn demo` skips it",
                       category="packs") for f in seed_load_failures()]
    if found:
        out.append(CheckResult("seed_providers", "ok", ", ".join(found), category="packs"))
    else:
        out.append(CheckResult("seed_providers", "warn",
                               "no `cairn.seeds` provider is installed — `cairn demo` seeds an empty "
                               "workspace (reinstall cairn, or install a pack that ships one)",
                               category="packs"))
    return out


def check_model_catalog_providers() -> list[CheckResult]:
    """ADR-0120 D4 (#2414) — the ``cairn.model_catalogs`` providers the licence door
    asks. One row naming them; a ``warn`` per entry point that does not load; a ``warn``
    when none is installed, because a ``base_model`` input is then not licence-checked."""
    from cairn.core.pack.model_catalogs import catalog_load_failures, load_catalog_providers  # noqa: PLC0415 - cli → core

    found = load_catalog_providers()
    out = [CheckResult(f"model_catalog[{f['name']}]", "warn",
                       f"{f['target']} does not load ({f['error']}) — the licence door skips it",
                       category="packs") for f in catalog_load_failures()]
    if found:
        out.append(CheckResult("model_catalogs", "ok", ", ".join(found), category="packs"))
    else:
        out.append(CheckResult("model_catalogs", "warn",
                               "no `cairn.model_catalogs` provider is installed — a `base_model` input "
                               "is not checked against a catalogue (install the platform, or a pack "
                               "that ships one)", category="packs"))
    return out


def check_run_hooks() -> list[CheckResult]:
    """ADR-0120 D4 (#2421) — the ``cairn.run_hooks`` providers the dispatch layer fires on
    run and leg events. One row naming them; a ``warn`` per entry point that does not load;
    ``ok`` when none is installed (a finished run is then scored only by its own evals)."""
    from cairn.core.pack.run_hooks import load_run_hooks, run_hook_load_failures  # noqa: PLC0415 - cli → core

    found = load_run_hooks()
    out = [CheckResult(f"run_hook[{f['name']}]", "warn",
                       f"{f['target']} does not load ({f['error']}) — the dispatcher skips it",
                       category="packs") for f in run_hook_load_failures()]
    out.append(CheckResult("run_hooks", "ok",
                           ", ".join(found) if found else
                           "no `cairn.run_hooks` provider installed — runs are recorded and scored by their own evals only",
                           category="packs"))
    return out


def check_router_providers() -> list[CheckResult]:
    """ADR-0120 D4 (#2422) — the ``cairn.routers`` providers the server mounts beyond the
    public API. None is a valid install (the server serves D2 only); one that does not load
    is skipped at boot with a warning, so it is reported here too."""
    from cairn.core.triggers.router_providers import discover_router_providers  # noqa: PLC0415 — CLI is a surface (ADR-0120 D6)

    providers = discover_router_providers()
    out = [CheckResult(f"router_provider[{p.name}]", "warn",
                       f"router provider {p.name!r} ({p.value}) does not load: {p.error} — "
                       "the server boots without it", category="packs")
           for p in providers if p.fn is None]
    loaded = [p.name for p in providers if p.fn is not None]
    out.append(CheckResult("router_providers", "ok",
                           f"{len(loaded)} router provider(s) registered beyond the public API: "
                           f"{', '.join(loaded)}" if loaded else
                           "no router providers installed — the server mounts the public API only",
                           category="packs"))
    return out


def check_cli_providers() -> list[CheckResult]:
    """ADR-0120 D4 (#2421) — the ``cairn.cli`` providers the public ``cairn`` mounts after
    its own commands. None is a valid install (the public commands only); a provider that
    does not load, or a command refused because a public command holds its name, is
    skipped at startup with a warning, so it is reported here too."""
    from cairn.cli.providers import cli_mount_failures, load_cli_providers

    providers = load_cli_providers()
    out = [CheckResult(f"cli_provider[{p.name}]", "warn",
                       f"cli provider {p.name!r} ({p.value}) does not load: {p.error} — "
                       "`cairn` starts without it", category="packs")
           for p in providers if p.error]
    out += [CheckResult(f"cli_provider[{f['name']}]", "warn",
                        f"command {f['name']!r} from {f['target']} refused: {f['error']}",
                        category="packs") for f in cli_mount_failures()]
    loaded = [f"{p.name} ({', '.join(n for n, _ in p.commands)})" for p in providers if not p.error]
    out.append(CheckResult("cli_providers", "ok",
                           f"{len(loaded)} cli provider(s) mounted beyond the public commands: "
                           f"{', '.join(loaded)}" if loaded else
                           "no cli providers installed — `cairn` lists the public commands only",
                           category="packs"))
    return out


# ─── orchestrator ───────────────────────────────────────────────────


def check_packs() -> list[CheckResult]:
    """Run every installed pack's own doctor checks."""
    from cairn.core.pack import all_doctor_checks, ensure_loaded

    ensure_loaded()
    out: list[CheckResult] = []
    for check in all_doctor_checks():
        try:
            out.append(check())
        except Exception as e:  # a pack check must never crash the preflight
            out.append(CheckResult(getattr(check, "__name__", "pack_check"),
                                   "warn", f"check raised {type(e).__name__}: {e}",
                                   category="incident"))
    return out


# ─── §24.3 composition (P2.c) ───────────────────────────────────────


def check_composition() -> list[CheckResult]:
    """Render the §24.3 composition report under the ``[packs]`` category.

    Reads the cached :class:`CompositionReport` from the pack registry —
    populated by the loader's validation pass — and produces:

      - One ``packs_loaded`` row listing every installed pack name.
      - One ``capabilities_provided`` row summarising the contribution
        registry.
      - One ``fail`` row per conflict (duplicate providers, unresolved
        ``[requires.capabilities]``, profile-pick typos).
      - One ``warn`` row per unmet ``[optional.capabilities]``.

    A pure-permissive deployment (no pack with ``[provides]``) renders
    as two ok rows + no conflicts — matches today's behavior so existing
    setups don't regress the doctor exit code.

    Lenient mode is used here on purpose: a misconfigured pack should
    surface in doctor *as a diagnostic*, not crash the CLI on import.
    """
    from cairn.core.pack import (
        engine_compat_status,
        ensure_loaded,
        get_composition_report,
        get_packs,
    )

    ensure_loaded()  # lenient — populates the report even on conflicts
    report = get_composition_report()
    packs = get_packs()
    out: list[CheckResult] = []

    if not packs:
        out.append(CheckResult(
            "packs_loaded", "warn",
            "no packs installed — install at least one via "
            "the 'cairn.packs' entry-point group",
            category="packs",
        ))
        return out

    out.append(CheckResult(
        "packs_loaded", "ok",
        f"{len(packs)}: {', '.join(sorted(p.name for p in packs))}",
        category="packs",
    ))

    # §Phase-2.1 — tier/trust metadata per loaded Pack instance.
    # Records-only; no fail rows. Phase 3+ wires policy gates here.
    for p in sorted(packs, key=lambda x: x.name):
        m = p.manifest
        if m is None:
            continue
        tier_bits: list[str] = []
        if m.kind:
            tier_bits.append(f"kind={m.kind}")
        if m.trust_level:
            tier_bits.append(f"trust={m.trust_level}")
        if m.publisher:
            tier_bits.append(f"publisher={m.publisher}")
        if tier_bits:
            out.append(CheckResult(
                f"pack_tier:{p.name}", "ok",
                ", ".join(tier_bits), category="packs",
            ))

        # #1545 — [compat] cairn_engine. Parsed since the manifest existed,
        # enforced nowhere: a pack could declare `cairn_engine = ">=9.0"` and
        # load silently on 0.1.0. Rendered here (never a fail — see
        # engine_compat_status for why a hard reject would be wrong) so the
        # mismatch is visible in the same place the rest of the pack's metadata is.
        compat = engine_compat_status(p)
        if compat is not None:
            out.append(CheckResult(
                f"pack_compat:{p.name}", compat[0], compat[1], category="packs",
            ))

    # §Phase-2.8 — declared trigger sources per pack + which provider
    # would handle each. A source with no vendor TriggerProvider is
    # still generically servable (GenericYamlProvider, #309) and
    # surfaces as a composition_warning row, not a fail; this loop
    # renders the positive case (matched vendor provider) and the
    # generic-fallback bookkeeping so an env owner can audit what's
    # wired.
    from cairn.core.triggers import default_provider_registry

    known_provider_sources = set(default_provider_registry.list_sources())
    for p in sorted(packs, key=lambda x: x.name):
        for spec in getattr(p, "trigger_specs", []) or []:
            source = getattr(spec, "source", "?")
            path = getattr(getattr(spec, "ingress", None), "path", "?")
            if source in known_provider_sources:
                provider_cls = default_provider_registry.get(source)
                detail = (
                    f"{p.name} declares {source!r} → "
                    f"provider {provider_cls.__module__}.{provider_cls.__name__} "
                    f"(POST {path})"
                )
                status = "ok"
            else:
                detail = (
                    f"{p.name} declares {source!r} → served by the generic "
                    f"YAML provider (POST {path})"
                )
                status = "ok"
            out.append(CheckResult(
                f"trigger:{p.name}:{source}", status,
                detail, category="packs",
            ))

    # §Phase-2.2 — operator-pack metadata, surfaced from the registry.
    # The registry carries metadata for every operator the entry-point
    # loader discovered AND whose pack.toml the manifest walker visited.
    # Operator packs aren't Pack instances (they expose Operations via
    # entry-points), so this loop is the canonical surface for them.
    from cairn.core.operator.loader import ensure_loaded as ensure_ops_loaded
    from cairn.core.operator.registry import default_registry as op_registry

    ensure_ops_loaded()
    # §Phase-2.7 — surface the picked executor per operator so the env
    # owner sees which transport `agentops run` would dispatch through.
    # An operator that declares only unimplemented executors raises in
    # pick_executor; we render it as a warn row rather than crashing
    # the whole doctor pass.
    from cairn.core.operator.executor import (
        UnsupportedExecutorError,
        pick_executor,
    )

    for op_name in sorted(op_registry.list_names()):
        md = op_registry.get_metadata(op_name)
        if md is None:
            out.append(CheckResult(
                f"operator:{op_name}", "warn",
                "no pack.toml metadata found "
                "(operator class registered without a matching "
                "[operator.\"<name>\"] table)",
                category="packs",
            ))
            continue
        bits = [f"risk={md.risk or '∅'}"]
        if md.executors:
            bits.append(f"executors=[{','.join(md.executors)}]")
        # Pick the executor that would actually dispatch.
        op_cls = op_registry.get(op_name)
        try:
            picked = pick_executor(op_cls)
            bits.append(f"→ {picked}")
            status = "ok"
        except UnsupportedExecutorError:
            bits.append("→ UNSUPPORTED")
            status = "warn"
        if md.requires_approval:
            bits.append("approval=required")
        if md.supports_dry_run:
            bits.append("dry_run")
        if md.supports_verify:
            bits.append("verify")
        if md.supports_compensate:
            bits.append("compensate")
        out.append(CheckResult(
            f"operator:{op_name}", status,
            " | ".join(bits), category="packs",
        ))

    if report is None:
        # Should not happen — ensure_loaded() runs validation — but be
        # defensive so a registry quirk doesn't shadow the rest of the
        # report.
        out.append(CheckResult(
            "composition", "warn",
            "no composition report cached (loader did not validate?)",
            category="packs",
        ))
        return out

    # n_caps counts post-override capabilities. The "underlying
    # competition" between providers is already surfaced separately —
    # either as a fail row (no override) or implicit in [profile.toml]
    # (operator chose), so we don't double-render it here.
    n_caps = len(report.provider_map)
    out.append(CheckResult(
        "capabilities_provided", "ok",
        f"{n_caps} capability(ies)", category="packs",
    ))

    for reason in report.conflicts:
        out.append(CheckResult(
            "composition_conflict", "fail", reason, category="packs",
        ))
    for reason in report.warnings:
        out.append(CheckResult(
            "composition_warning", "warn", reason, category="packs",
        ))
    return out


def check_profile() -> list[CheckResult]:
    """Profile category (ADR-0028 + housekeeping wave): does the active
    profile load, and does every ``[profile.bindings]`` entry resolve to a
    registered operator? A bound-but-unresolvable service would otherwise
    surface only at runtime as a fail-open no-op — doctor fails it loud."""
    import os as _os

    out: list[CheckResult] = []
    raw = (getenv("CAIRN_PROFILE") or "").strip()
    if not raw:
        out.append(CheckResult(
            "profile", "ok",
            "CAIRN_PROFILE unset — defaults active (no bindings; redact/audit off)",
            category="profile",
        ))
        return out

    try:
        from cairn.core.pack.profile import load_profile_from_env
        profile = load_profile_from_env()
    except Exception as e:  # noqa: BLE001 — a broken profile is exactly what doctor reports
        out.append(CheckResult(
            "profile", "fail", f"CAIRN_PROFILE={raw!r} failed to load: {e}",
            category="profile",
        ))
        return out

    out.append(CheckResult(
        "profile", "ok",
        f"loaded {raw} ({len(profile.bindings)} binding(s))", category="profile",
    ))

    from cairn.core.operator import default_registry, load_all_operators
    load_all_operators(strict=False)
    for capability, op_name in sorted(profile.bindings.items()):
        try:
            default_registry.get(op_name)
            out.append(CheckResult(
                f"binding:{capability}", "ok", f"→ {op_name} (registered)",
                category="profile",
            ))
        except Exception:  # noqa: BLE001
            out.append(CheckResult(
                f"binding:{capability}", "fail",
                f"→ {op_name!r} is not a registered operator — the service "
                f"would silently no-op at runtime (fail-open)",
                category="profile",
            ))

    # #1396 — a loaded profile with governance capabilities UNBOUND is legal
    # (ADR-0028: the null sink / no-op redactor are the correct defaults) but
    # must be LOUD: an operator reading a green doctor otherwise believes the
    # audit trail and redaction are on when every event is being dropped.
    _GOVERNANCE_UNBOUND = {
        "audit.sink": "audit events are DROPPED (NullAuditSink) — no "
                      "hash-chained trail exists for this environment",
        "redact.text": "LLM egress is NOT redacted — prompts leave for the "
                       "vendor verbatim",
    }
    for capability, consequence in _GOVERNANCE_UNBOUND.items():
        if capability not in profile.bindings:
            out.append(CheckResult(
                f"binding:{capability}", "warn",
                f"unbound under {raw!r} — {consequence}",
                category="profile",
            ))
    return out


def check_production_readiness() -> list[CheckResult]:
    """#898 — cross-check the active profile's DECLARED governance intent against
    what's actually installed, so a governed run can't silently degrade to a weaker
    backend than the profile promised (the "you think you're in prod but you're on
    stub / regex / sample" footgun). FAILS loud when the profile demands a real
    backend this deployment can't provide. No-op when CAIRN_PROFILE is unset (dev)."""
    import importlib.util as _u

    out: list[CheckResult] = []
    if not getenv("CAIRN_PROFILE", "").strip():
        return out  # dev — no governance intent declared, nothing to enforce
    try:
        from cairn.core.pack.profile import load_profile_from_env
        cfg = load_profile_from_env().config or {}
    except Exception:  # noqa: BLE001 — check_profile already reports a broken profile
        return out

    def _have(mod: str) -> bool:
        return bool(_u.find_spec(mod))

    cat = "production"
    # PII: a profile that demands presidio must have it — else the governed PII gate
    # (#889) degrades to regex and can't CERTIFY a clean scrub, so finetune fail-closes.
    pii = cfg.get("dataset/pii_scan") or {}
    if str(pii.get("backend", "")).lower() == "presidio" and not _have("presidio_analyzer"):
        out.append(CheckResult(
            "pii-backend", "fail",
            "profile requires PII backend=presidio but presidio isn't installed "
            "(pip install 'cairndev[production]') — the governed PII gate degrades to regex "
            "and can't certify a clean scrub (#889)", category=cat))
    # Benchmark: require_benchmark demands the lm-eval harness so the promote can
    # actually gate on task quality (#702/#898) — else no run can satisfy the gate.
    mg = (cfg.get("model/gate") or {}).get("thresholds") or {}
    if mg.get("require_benchmark") and not _have("lm_eval"):
        out.append(CheckResult(
            "benchmark-harness", "fail",
            "profile requires benchmark scores to promote but lm-eval isn't installed "
            "(pip install 'cairndev[production]') — no run can satisfy the gate (#898)", category=cat))
    # Temporal: CAIRN_RUNTIME=temporal needs temporalio, else the durable runtime can't load.
    if _settings.runtime() == "temporal" and not _have("temporalio"):
        out.append(CheckResult(
            "runtime-temporal", "fail",
            "CAIRN_RUNTIME=temporal but temporalio isn't installed (pip install 'cairndev[production]')",
            category=cat))
    # MLflow: a tracking URI set without mlflow means model/register silently falls
    # back to the LOCAL registry — no real MLflow promotion / lineage.
    if os.environ.get("MLFLOW_TRACKING_URI") and not _have("mlflow"):
        out.append(CheckResult(
            "registry-mlflow", "warn",
            "MLFLOW_TRACKING_URI is set but mlflow isn't installed — model/register falls "
            "back to the local registry (no MLflow lineage)", category=cat))
    if not out:
        out.append(CheckResult(
            "production", "ok",
            "profile's governance backends resolve (presidio / lm-eval / temporal as declared)",
            category=cat))
    return out


def check_model_roles() -> list[CheckResult]:
    """#352 — every model role (fast/strong/router/…) must resolve to a
    provider this deployment can call; a role bound to a keyless provider
    only fails at call time, so surface it here."""
    from cairn.core.llm.models import connected_llm_providers, list_roles

    connected = connected_llm_providers()
    out: list[CheckResult] = []
    for cfg in list_roles():
        if connected.get(cfg.provider, False):
            out.append(CheckResult(
                f"model_role:{cfg.role}", "ok",
                f"{cfg.provider}/{cfg.name} ({cfg.source})", category="settings",
            ))
        else:
            out.append(CheckResult(
                f"model_role:{cfg.role}", "warn",
                f"bound to '{cfg.provider}/{cfg.name}' but provider "
                f"'{cfg.provider}' has no key configured — calls will fail; "
                f"reassign on the LLM gateway page or set the key",
                category="settings",
            ))
    return out


def check_required_services() -> list[CheckResult]:
    """ADR-0027's doctor half: a pack that *requires* a capability must be
    deployed with a provider bound for it — fail loud, not no-op."""
    from cairn.core.pack import ensure_loaded
    from cairn.core.pack.profile import load_profile_from_env
    from cairn.core.pack.registry import get_packs

    ensure_loaded()
    try:
        bindings = load_profile_from_env().bindings
    except Exception:  # noqa: BLE001 — profile errors already reported by check_profile
        bindings = {}

    out: list[CheckResult] = []
    any_required = False
    for pack in get_packs():
        manifest = getattr(pack, "manifest", None)
        for cap in getattr(manifest, "required_capabilities", None) or []:
            any_required = True
            if cap in bindings:
                out.append(CheckResult(
                    f"requires:{pack.name}:{cap}", "ok",
                    f"bound → {bindings[cap]}", category="profile",
                ))
            else:
                out.append(CheckResult(
                    f"requires:{pack.name}:{cap}", "fail",
                    f"pack '{pack.name}' requires '{cap}' but no provider is "
                    f"bound in [profile.bindings]", category="profile",
                ))
    if not any_required:
        out.append(CheckResult(
            "required_services", "ok",
            "no pack declares [requires].capabilities", category="profile",
        ))
    return out


# The requires→backend mapping lives in cairn.core.pack.provisioning (#304) —
# shared with the runtime fail-closed gate in the dispatcher so doctor and the
# run path can never disagree about what counts as provisioned.
_REQUIRES_BACKEND = REQUIRES_BACKEND


def check_manifest_drift() -> list[CheckResult]:
    """Manifest ↔ code drift for operator packs (#1105, ADR-0083 D4).

    The manifest is what governance reads (``risk`` decides whether a gate
    opens), so a drifted manifest is a governance-relevant inconsistency — not
    cosmetics. Reported as ``fail`` because the two known cases both produced a
    warning nobody acted on: an orphaned manifest entry (``rag/parse``) and an
    operator whose entry point was stale until the pack was reinstalled.
    """
    try:
        findings = check_installed_operator_drift()
    except Exception as e:  # noqa: BLE001 — a diagnostic must never crash the doctor
        return [CheckResult("manifest_drift", "warn",
                            f"could not check manifest drift: {type(e).__name__}: {e}",
                            category="packs")]
    if not findings:
        return [CheckResult("manifest_drift", "ok",
                            "every operator manifest matches its registered class",
                            category="packs")]
    return [
        # `pack_not_installed` is an environment fact (a dev machine or slim
        # deployment legitimately omits packs) — a warn, never a fail.
        CheckResult(f"drift[{f.kind}]", "fail" if f.is_drift else "warn",
                    f.detail, category="packs")
        for f in findings
    ]


def _domain_packs_without_pipelines() -> list[str]:
    """Domain packs missing their mandatory `pipelines/` surface (#1131).

    Knowledge and operator packs are EXEMPT: they are attachable resources, not
    runnable domains (ADR-0084 ruling 10), so a mandatory pipeline there would
    be an empty ritual.
    """
    from cairn.core.pack import ensure_loaded
    from cairn.core.pack.registry import get_packs

    ensure_loaded()
    out: list[str] = []
    for pack in get_packs():
        m = getattr(pack, "manifest", None)
        if str(getattr(m, "kind", "") or "") != "domain":
            continue
        if not getattr(pack, "pipelines_dir", None):
            out.append(str(getattr(m, "name", "") or "?"))
    return out


def check_pipeline_mandate() -> list[CheckResult]:
    """Every domain pack ships at least one pipeline (#1131, ADR-0084 D4).

    The pipeline is the pack's runnable surface and the console's primary noun —
    a domain pack without one has no way to be run or shown.
    """
    missing = _domain_packs_without_pipelines()
    if not missing:
        return [CheckResult("pipeline_mandate", "ok",
                            "every domain pack ships a pipeline", category="packs")]
    return [
        CheckResult(f"pipeline_mandate[{name}]", "fail",
                    f"domain pack {name!r} has no pipelines/ — a domain pack's "
                    f"runnable surface is mandatory (ADR-0084 D4). Add "
                    f"pipelines/<name>.yaml composing its own flows.",
                    category="packs")
        for name in missing
    ]


def check_pack_layout() -> list[CheckResult]:
    """Which packs still keep flows under a superseded dir (#1164, ADR-0084 D2).

    WARN, never fail: `templates/` and `dataflows/` still load, and third-party
    packs depend on that. But #1133 promised a warning and emitted none, so six
    of seven first-party packs sat on the old layout for a full phase with
    nothing signalling it. A deprecation nobody can see is not a deprecation.
    """
    from cairn.core.pack import ensure_loaded  # noqa: PLC0415 - cycle
    from cairn.core.pack.loader import legacy_layout_dirs  # noqa: PLC0415 - cycle

    ensure_loaded()
    from cairn.core.pack.registry import get_packs  # noqa: PLC0415 - cycle

    stale = {p.name: legacy_layout_dirs(p) for p in get_packs() if legacy_layout_dirs(p)}
    if not stale:
        return [CheckResult("pack_layout", "ok",
                            "every pack keeps its flows in flows/", category="packs")]
    return [
        CheckResult(f"pack_layout[{name}]", "warn",
                    f"pack {name!r} keeps flows in "
                    f"{', '.join(a.replace('_dir', '/') for a in dirs)} — superseded "
                    f"by flows/ (ADR-0084 D2). The alias still loads; move the "
                    f"files when convenient (#1163).",
                    category="packs")
        for name, dirs in sorted(stale.items())
    ]


def check_flow_stem_collisions() -> list[CheckResult]:
    """Which packs ship ``flows/<stem>.md`` AND ``flows/<stem>.yaml`` (#1543).

    ADR-0106 D9 (#2134) made the pair ILLEGAL: a ref addresses ONE flow, and
    :func:`cairn.core.pack.loader.assert_flow_assets` refuses a second one
    at the install door. The two that shipped (``training/finetune``,
    ``dataset-readiness/clean``) were renamed to ``training/sft-train`` and
    ``dataset-readiness/intake``, so this reports ``ok`` on the shipped packs.

    Still WARN, never fail, and deliberately not a second implementation of the
    rule: the lint is the door that refuses, doctor is the surface that tells an
    operator what a lenient boot let through (a dev pack under
    ``CAIRN_PACK_PATH``, a pack registered before the lint ran). ``--strict``
    promotes the warn for anyone who wants doctor to be that door too.
    """
    from cairn.core.pack import ensure_loaded  # noqa: PLC0415 - cycle
    from cairn.core.pack.loader import flow_ref_collisions  # noqa: PLC0415 - cycle
    from cairn.core.pack.registry import get_packs  # noqa: PLC0415 - cycle

    ensure_loaded()
    # ONE implementation of the rule (#1402): the lint decides what a collision
    # is, doctor decides how to say it. A second scan here is how the door and
    # the report end up disagreeing about which packs are legal.
    rows = [(p.name, reason) for p in get_packs() for reason in flow_ref_collisions(p)]
    if not rows:
        return [CheckResult("flow_stem_collisions", "ok",
                            "no flow ref is claimed by both a .md and a .yaml",
                            category="packs")]
    return [
        CheckResult(f"flow_stem_collision[{pack}]", "warn",
                    f"{reason} ADR-0106 D9 forbids it and the install door "
                    f"refuses this pack until it is fixed (#1543, #2134).",
                    category="packs")
        for pack, reason in rows
    ]


def _backend_provenance_checks() -> list[CheckResult]:
    """One line per backend selector whose value is NOT the schema default (#1093).

    Shows the effective value and which plane set it (``profile`` / ``env``), so a
    surprising engine or target is one ``cairn doctor`` away instead of a grep
    through call sites. Defaults are omitted to keep the output short.
    """
    from cairn.core.backend_selection import effective_report

    try:
        rows = [r for r in effective_report() if r["provenance"] != "default"]
    except Exception as e:  # noqa: BLE001 — diagnostics must never fail the doctor
        return [CheckResult("backend_provenance", "warn",
                            f"could not resolve backend provenance: {e}",
                            category="profile")]
    if not rows:
        return [CheckResult("backend_provenance", "ok",
                            "all backends at schema defaults", category="profile")]
    return [
        # NB name shape `config[<key>]`, NOT `…:<key>` — consumers filter
        # provisioning rows by a `:<kind>` suffix (`provisioned:<pack>:memory`),
        # so a colon here would make a diagnostic row masquerade as a check.
        CheckResult(
            f"config[{r['key']}]", "ok",
            f"{r['key']}={_settings.redacted_url(str(r['value']))} (from {r['provenance']}"
            + (f": {r['env']}" if r["provenance"] == "env" and r["env"] else "")
            + ")",
            category="profile",
        )
        for r in rows
    ]


def check_provisioned_backends() -> list[CheckResult]:
    """ADR-0052 §4: a pack that declares a stateful ``[requires.memory|rag]`` must
    run in an environment that *provisions* the backing capability — fail closed
    with a remediation, never a silent skip. Distinct from
    :func:`check_required_services` (operator-binding deps): this validates the
    environment's ``[profile.backends]`` provisioning."""
    from cairn.core.pack import ensure_loaded
    from cairn.core.pack.profile import load_profile_from_env
    from cairn.core.pack.registry import get_packs
    from cairn.core.profile_backends import effective_backends

    ensure_loaded()
    try:
        backends = effective_backends(load_profile_from_env().backends)
    except Exception:  # noqa: BLE001 — profile errors already surfaced by check_profile
        backends = effective_backends({})

    out: list[CheckResult] = []
    any_required = False
    for pack in get_packs():
        manifest = getattr(pack, "manifest", None)
        checks = (
            ("memory", getattr(manifest, "required_memory", None)),
            ("rag", getattr(manifest, "required_rag", None)),
        )
        for kind, reqs in checks:
            if not reqs:
                continue
            any_required = True
            field = _REQUIRES_BACKEND[kind]
            if backends.get(field):
                out.append(CheckResult(
                    f"provisioned:{pack.name}:{kind}", "ok",
                    f"{kind} provisioned ({field}={backends.get(field)})",
                    category="profile",
                ))
            else:
                out.append(CheckResult(
                    f"provisioned:{pack.name}:{kind}", "fail",
                    f"pack '{pack.name}' requires {kind} but no {kind} backend is "
                    f"enabled here — enable it in Environments → Backends",
                    category="profile",
                ))
    if not any_required:
        out.append(CheckResult(
            "provisioned_backends", "ok",
            "no pack declares a stateful [requires.memory|rag]", category="profile",
        ))

    # #1093 — report WHICH plane decided each non-default selector. P9's failure
    # mode was that "profile vs env" was per-call-site folklore; the fix is only
    # complete once an operator can SEE the answer.
    out.extend(_backend_provenance_checks())
    return out


def check_operator_config() -> list[CheckResult]:
    """ADR-0068 (#742): every ``[profile.config."<op>"]`` table must validate
    against that operator's declared ``config_schema``. An unknown key, wrong
    type, or a config table for an operator that exposes no knobs fails preflight
    with the operator's field list — so a typo surfaces at boot, never on the box
    mid-run (matching the profile's fail-at-boot ethos)."""
    from cairn.core.operator import load_all_operators
    from cairn.core.operator.registry import default_registry
    from cairn.core.pack.profile import load_profile_from_env

    try:
        cfg = load_profile_from_env().config
    except Exception:  # noqa: BLE001 — profile parse errors reported by check_profile
        return []
    if not cfg:
        return [CheckResult(
            "operator_config", "ok", "no [profile.config] overrides", category="profile",
        )]

    load_all_operators(strict=False)
    out: list[CheckResult] = []
    for op_name, values in cfg.items():
        if not default_registry.has(op_name):
            out.append(CheckResult(
                f"config:{op_name}", "fail",
                f"[profile.config.\"{op_name}\"] names an operator that isn't installed",
                category="profile",
            ))
            continue
        schema = getattr(default_registry.get(op_name), "config_schema", None)
        if schema is None:
            out.append(CheckResult(
                f"config:{op_name}", "fail",
                f"operator '{op_name}' declares no config_schema (no tunable knobs) — "
                f"remove [profile.config.\"{op_name}\"]",
                category="profile",
            ))
            continue
        try:
            schema.model_validate(values)
        except Exception as e:  # noqa: BLE001 — the validation error IS the finding
            fields = ", ".join(sorted(schema.model_fields.keys())) or "(none)"
            out.append(CheckResult(
                f"config:{op_name}", "fail",
                f"[profile.config.\"{op_name}\"] invalid: {e}. Accepts: {fields}",
                category="profile",
            ))
        else:
            out.append(CheckResult(
                f"config:{op_name}", "ok",
                f"config valid ({len(values)} override group(s))", category="profile",
            ))
    return out


async def check_artifact_bytes() -> list[CheckResult]:
    """#1395 — is the artifact registry's BYTES story intact?

    Two questions, deliberately separate:

    ``artifact_bytes``
        is a durable byte backend configured at all? Without one, every
        registered artifact whose URI is a workdir path dies with its box —
        a warn, not a fail: a single-host install is a legitimate deployment.
    ``artifact_registry_bytes``
        of what IS registered, how much can still be fetched? ``dangling``
        means lineage cites bytes nobody can get, and the first offenders are
        named because a bare count says nothing about where to look.

    Both degrade to a warn on any registry error — doctor reports, it never
    crashes on a database that happens to be down.
    """
    from cairn.core.storage.artifact_bytes import durable_target, registry_health  # noqa: PLC0415 - CLI startup latency: keeps storage off `cairn --help`
    from cairn.core.storage.artifacts import _NullArtifactStore, build_artifact_store  # noqa: PLC0415 - same

    target = durable_target()
    backend = CheckResult(
        "artifact_bytes", "ok", f"relaying registered artifacts to s3://{target[0]}/{target[1]}".rstrip("/"),
        category="store") if target else CheckResult(
        "artifact_bytes", "warn",
        "no durable artifact byte store (CAIRN_ARTIFACT_STORE unset/local) — a registered "
        "artifact whose URI is a box workdir is unfetchable once that box is gone",
        category="store")

    store = build_artifact_store()
    if isinstance(store, _NullArtifactStore):
        return [CheckResult("artifact_bytes", "warn",
                            "no artifact registry configured (CAIRN_STORAGE unset) — nothing to check",
                            category="store")]
    # The registry answers per workspace (ADR-0148); the CLI checks the process's
    # own workspace (CAIRN_WORKSPACE, else `default`), which owns every artifact
    # registered before workspace scoping.
    from cairn.core.workspace import current_workspace  # noqa: PLC0415 - CLI startup latency

    ws = current_workspace()
    try:
        health = await registry_health(store, workspace_id=ws)
    except Exception as e:  # noqa: BLE001 - a doctor probe reports, it never crashes
        return [backend, CheckResult("artifact_registry_bytes", "warn",
                                     f"could not read the artifact registry: {e}", category="store")]
    finally:
        try:
            await store.close()
        except Exception:  # noqa: BLE001 - closing a probe's store is never the operator's problem
            _log.debug("doctor: artifact store close failed", exc_info=True)

    if health["dangling"]:
        names = ", ".join(f"{a['id'][:12]} ({a['uri']})" for a in health["dangling_sample"])
        detail = (f"{health['dangling']} dangling of {health['total']} registered in workspace {ws!r} "
                  f"({health['durable']} durable, {health['local']} local-only) — first: {names}")
        status = "warn"
    else:
        detail = (f"{health['total']} registered artifacts in workspace {ws!r} resolve "
                  f"({health['durable']} durable, {health['local']} local-only)")
        status = "ok"
    return [backend, CheckResult("artifact_registry_bytes", status, detail, category="store")]


async def check_mcp_servers() -> list[CheckResult]:
    """MCP reachability: every declared server must answer a tool listing.
    Also flags profile-declared servers the env-based client can't see
    (the [profile.mcp_servers] → client wiring is env-only today)."""
    from cairn.services.mcp.client import get_mcp_client, server_connections

    out: list[CheckResult] = []
    conns = server_connections()
    if not conns:
        out.append(CheckResult(
            "mcp_servers", "ok", "none declared (CAIRN_MCP_SERVERS / CAIRN_MCP_<NAME>_*)",
            category="mcp",
        ))
    else:
        client = get_mcp_client()
        for name in sorted(conns):
            try:
                tools = await asyncio.wait_for(client.list_tools(name), timeout=3.0)
                out.append(CheckResult(
                    f"mcp:{name}", "ok", f"reachable — {len(tools)} tool(s)",
                    category="mcp",
                ))
            except Exception as e:  # noqa: BLE001 — unreachable is the finding
                out.append(CheckResult(
                    f"mcp:{name}", "warn",
                    f"unreachable ({type(e).__name__}: {e})", category="mcp",
                ))

    try:
        from cairn.core.pack.profile import load_profile_from_env
        for name in sorted(load_profile_from_env().mcp_servers):
            if name not in conns:
                out.append(CheckResult(
                    f"mcp:{name}", "warn",
                    "declared in [profile.mcp_servers] but the MCP client reads "
                    "env only — set CAIRN_MCP_SERVERS / CAIRN_MCP_<NAME>_* too",
                    category="mcp",
                ))
    except Exception:  # noqa: BLE001 — profile errors reported elsewhere
        pass
    return out


async def check_compute_targets() -> list[CheckResult]:
    """ADR-0104 D1 (#2071) — the environment's ``compute.ssh`` targets resolve
    and are complete.

    On 2026-09-07 a GPU run died on ``compute.ssh connection has no host``,
    raised by ``as_ssh_params`` at SUBMIT — after the run had been created,
    gated and dispatched — because the ``gpu`` connection carried no host.
    Nothing looked at that connection between "somebody saved it" and "a run
    died on it", and doctor called the deployment healthy. This asks the same
    questions the dispatch will, before there is a run to lose.

    Reads the SAME selection plane the engine does (profile record → env →
    default), so a profile-configured target is checked, not just an env one.
    """
    from cairn.core.backend_selection import selected  # noqa: PLC0415 - cycle: cli → core
    from cairn.core.deploy_check import check_compute_ssh  # noqa: PLC0415 - cli → core

    out: list[CheckResult] = []
    # ADR-0150 D1 — the execution unit is the connection's mode/image over the
    # environment's; read the environment's half here so the row matches what
    # the engine will run. Unreadable → None, and the row says it depends.
    try:
        env_mode: str | None = selected("remote_exec_mode", default="auto") or "auto"
        env_image: str | None = selected("remote_image", default="") or ""
    except Exception:  # noqa: BLE001 — a broken profile is reported by check_profile
        env_mode = env_image = None
    for key in ("dataflow_target", "ml_target"):
        try:
            target = (selected(key, default="") or "").strip()
        except Exception:  # noqa: BLE001 — a broken profile is reported by check_profile
            continue
        if not target:
            continue
        try:
            import cairn.core.connections as _connections  # noqa: PLC0415 - cli → core
            # the profile's targets are the deployment's own connections
            resolved = await _connections.resolve_connection(target, account_id=_connections.DEFAULT_ACCOUNT)
        except Exception as e:  # noqa: BLE001 — an unreachable store is a warn, not a crash
            out.append(CheckResult(
                f"compute_target:{key}", "warn",
                f"{key} → {target!r} could not be resolved ({type(e).__name__}: {e}) "
                f"— doctor cannot vouch for this target from here",
                category="runtime"))
            continue
        cfg = dict(getattr(resolved, "config", None) or {}) if resolved is not None else None
        secrets = getattr(resolved, "secrets", None) or {} if resolved is not None else {}
        out.extend(check_compute_ssh(
            key, target, cfg, has_key=bool(secrets.get("PRIVATE_KEY")),
            env_mode=env_mode, env_image=env_image))
    return out


async def run_checks() -> list[CheckResult]:
    """Run every check. Network probes happen concurrently with a tight timeout."""
    results: list[CheckResult] = []
    results.extend(check_settings())
    results.extend(check_model_roles())  # #352 — roles resolve to callable providers
    results.append(check_run_db())
    results.extend(check_profile())            # ADR-0028 — bindings resolve
    results.extend(check_production_readiness())  # #898 — profile intent vs installed backends
    results.extend(check_required_services())  # ADR-0027 — required caps bound
    results.extend(check_provisioned_backends())  # ADR-0052 §4 — stateful backends provisioned
    results.extend(check_manifest_drift())  # #1105 — manifest ↔ code drift is loud
    results.extend(check_pipeline_mandate())  # #1131 — domain packs ship a pipeline
    results.extend(check_pack_layout())       # #1164 — flows/ vs the legacy alias
    results.extend(check_flow_stem_collisions())  # #1543 — one stem, two runtimes
    results.extend(check_operator_config())  # ADR-0068 — [profile.config] validates vs config_schema
    results.extend(check_composition())  # §24.3 / P2.c — packs category
    results.extend(check_packs())
    results.extend(check_engine_clients())  # 2026-08-18 — a SELECTED engine's client must import
    results.extend(check_stage_kernels())  # #2363 — the door reads stage `implements` here
    results.extend(check_seed_providers())  # #2418 — what `cairn demo` seeds
    results.extend(check_model_catalog_providers())  # #2414 — what the licence door asks
    results.extend(check_router_providers())  # #2422 — the HTTP surfaces beyond D2
    results.extend(check_run_hooks())  # #2421 — who listens to run and leg events
    results.extend(check_cli_providers())  # #2421 — the commands beyond the public CLI
    probes = await asyncio.gather(
        check_artifact_bytes(),  # #1395 — registered artifacts that can't be fetched
        check_slack_webhook(),
        check_metaflow_service(),  # #1365 — configured service must be reachable
        check_runtime(),
        check_compute_targets(),  # ADR-0104 D1 (#2071) — the box the run lands on
        check_mcp_servers(),
        asyncio.to_thread(check_engine_store),  # #229 — the CLI's store is the engine's
        return_exceptions=False,
    )
    for p in probes:
        if p is None:
            continue
        if isinstance(p, list):
            results.extend(p)
        else:
            results.append(p)
    return results


# ─── output ─────────────────────────────────────────────────────────

# `packs` lands between the core engine categories and reachability so the
# composition report is visible before the noisier reachability probes.
_CATEGORY_ORDER = ("dispatcher", "store", "actions", "notifier", "profile",
                   "packs", "slack", "mcp", "runtime", "reachability")


def format_report(results: list[CheckResult]) -> str:
    by_cat: dict[str, list[CheckResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)

    # Known core categories first, then any pack-contributed categories.
    extra = [c for c in by_cat if c not in _CATEGORY_ORDER]
    lines: list[str] = ["cairn doctor", ""]
    for cat in (*_CATEGORY_ORDER, *sorted(extra)):
        rows = by_cat.get(cat)
        if not rows:
            continue
        lines.append(f"[{cat}]")
        for r in rows:
            lines.append(f"  {r.symbol} {r.name}: {r.detail}")
        lines.append("")

    n_ok = sum(1 for r in results if r.status == "ok")
    n_warn = sum(1 for r in results if r.status == "warn")
    n_fail = sum(1 for r in results if r.status == "fail")
    lines.append(f"Summary: {n_ok} ok, {n_warn} warn, {n_fail} fail")
    return "\n".join(lines)


def exit_code(results: list[CheckResult]) -> int:
    return 1 if any(r.status == "fail" for r in results) else 0


# ─── entry points ───────────────────────────────────────────────────
#
# Two entry points share one implementation:
#   - ``cmd(strict)``     — Typer command, wired in cairn.cli.main.
#   - ``main(argv)``      — argv-style, kept for tests + ``python -m`` use.
# Both delegate to ``_run_and_exit_code``.


def _run_and_exit_code(strict: bool) -> int:
    results = asyncio.run(run_checks())
    print(format_report(results))
    rc = exit_code(results)
    if strict and any(r.status == "warn" for r in results):
        rc = 1
    return rc


def cmd(
    strict: bool = typer.Option(
        False, "--strict",
        help="Treat warnings as failures (exit non-zero).",
    ),
) -> None:
    """Preflight: report on env config + reachability of configured services."""
    rc = _run_and_exit_code(strict=strict)
    raise typer.Exit(code=rc)


def main(argv: Optional[list[str]] = None) -> int:
    """argv-style entry point. Kept for backward-compatible test use.

    The Typer command (``cmd``) is the primary entry; this just wraps
    argparse for ``python -m cairn.cli.commands.doctor`` and the test
    suite, both of which pre-date Typer.
    """
    parser = argparse.ArgumentParser(
        prog="cairn doctor",
        description="Preflight: report on env config + reachability of configured services.",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Treat warnings as failures (exit non-zero).",
    )
    args = parser.parse_args(argv)
    return _run_and_exit_code(strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
