"""Resolve a backend field's bound connection into the env the factories read
(#336, ADR-0052 §3 — the runtime half of #305).

The profile binds a credentialed backend to a stored connection by NAME
(``[profile.backends] rag_vector_store_connection = "prod-qdrant"``, written by
the #303/#305 form). The stateful store factories (``build_vector_store``,
pgvector, …) are synchronous and read env/settings — so the seam is: at each
service's ASYNC entry point, resolve the bound connection ONCE and inject its
config + secret values into the env names those factories already read
(``Settings()`` is rebuilt per call, so injection is picked up immediately).
Mirrors the compute seam (``execution.compute_target`` → ``ctx.compute``).

Secrets stay references in the profile/connection; values only ever transit
process env here, resolved via the SecretStore/env by ``resolve_connection``.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from cairn.core.provider_settings import get_settings

logger = logging.getLogger("cairn.core.backend_connections")

#: backend VALUE → [(source, key, target-env)] injection map. ``source`` is
#: "config" (Connection.config[key]) or "secret" (resolved secret value by
#: logical key). Targets are the env names the existing factories/settings read
#: (Settings.qdrant_url ← QDRANT_URL, Settings.pgvector_dsn ← PGVECTOR_DSN, …).
_INJECTIONS: dict[str, list[tuple[str, str, str]]] = {
    "qdrant": [
        ("config", "url", "QDRANT_URL"),
        ("secret", "API_KEY", "QDRANT_API_KEY"),
    ],
    "pgvector": [
        # the postgres catalog provider carries the DSN as a secret ref
        ("secret", "DSN", "PGVECTOR_DSN"),
        ("config", "dsn", "PGVECTOR_DSN"),
    ],
    "pinecone": [
        ("secret", "API_KEY", "PINECONE_API_KEY"),
        ("config", "index", "PINECONE_INDEX"),
    ],
}

#: fields already applied this process (field → connection name), so hot paths
#: don't re-resolve on every save/recall. A changed binding needs a restart —
#: same contract as every other env-sourced backend knob.
_applied: dict[str, str] = {}


async def apply_backend_connection(field: str) -> Optional[str]:
    """If the active profile binds ``<field>_connection``, resolve that
    connection and inject its values into the target env names for the chosen
    backend. Returns the connection name when an injection happened (idempotent
    per process), ``None`` otherwise. Never raises — a missing/broken binding
    logs and leaves the env-fallback behavior exactly as before."""
    try:
        # lazy: profile + connections pull config/store stacks; keep this module import-light
        from cairn.core.pack.profile import load_profile_from_env
        from cairn.core.profile_backends import effective_backends

        backends = load_profile_from_env().backends
        name = backends.get(f"{field}_connection")
        if not name or not isinstance(name, str):
            return None
        if _applied.get(field) == name:
            return name

        chosen = str(effective_backends(backends).get(field) or "")
        spec = _INJECTIONS.get(chosen)
        if not spec:
            logger.info("backend %s=%r has a bound connection %r but no injection map — ignored", field, chosen, name)
            return None

        from cairn.core.connections import DEFAULT_ACCOUNT, resolve_connection

        # A profile binding is the deployment's: it resolves the deployment's own
        # connection and writes the process environment, never a tenant's row.
        resolved = await resolve_connection(name, account_id=DEFAULT_ACCOUNT)
        if resolved is None:
            logger.warning("backend %s binds connection %r but it is unknown/disabled — env fallback", field, name)
            return None

        injected = []
        for source, key, target in spec:
            value = resolved.config.get(key) if source == "config" else resolved.secrets.get(key)
            if value:
                os.environ[target] = str(value)
                injected.append(target)
        _applied[field] = name
        if injected:
            # get_settings() is lru-cached — any earlier caller froze a Settings
            # snapshot that predates this injection, so every consumer reading
            # get_settings().qdrant_url etc. would silently keep the stale value
            # (#359). Drop the cache so the injected env is actually seen.
            get_settings.cache_clear()
        logger.info("backend %s ← connection %r (injected: %s)", field, name, ", ".join(injected) or "nothing")
        return name
    except Exception:  # noqa: BLE001 — cred injection must never take a run down
        logger.warning("apply_backend_connection(%s) failed — env fallback", field, exc_info=True)
        return None


def reset_applied() -> None:
    """Test hook: forget per-process injections + the Settings snapshot that
    may hold them (#359)."""
    _applied.clear()
    get_settings.cache_clear()
