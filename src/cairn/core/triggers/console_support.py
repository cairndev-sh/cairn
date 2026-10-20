"""Shared helpers behind the console API (``/ui/api/*``).

These were module-level functions in :mod:`cairn.core.triggers.webui` when every
console route lived in one 1160-line ``register_web_ui`` closure (#1513). The
routes now live in :mod:`cairn.core.triggers.routers`, one module per resource,
and several of these helpers are needed by more than one of them —
``profile_path`` by profiles/knowledge/backends, ``require_delete_confirm`` by
profiles/policies, ``invalidate_policy_cache`` by profiles/policies — so they
live here rather than in whichever router happened to be extracted first.

Routers reach them as ``support.<name>()`` (module attribute, not a ``from``
import) on purpose: a test that patches ``console_support.profile_path`` then
affects *every* router, which is how the profile-editor tests point the API at a
tmp file instead of a shipped profile.
"""
from __future__ import annotations

import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import tomlkit  # comment-preserving TOML read/write for the profile form (base dep)
from fastapi import HTTPException

from cairn.core.profile_backends import get_field  # Environment backends configurator (#303)
from cairn.core.triggers.auth import caller_identity, identity_from_claims  # account-scope the API (#392 p2)


# ── profiles: edit the shipped execution profiles (cairn/profiles/*.toml) ──
#
# Only names that ``list_profiles()`` exposes are editable — this both scopes
# the editor to real profiles and prevents path traversal via the {name} param.


def profile_path(name: str) -> Optional[Path]:
    # lazy: keep cairn.ask off the trigger import path
    from cairn.core.ui_contract import list_profiles

    for p in list_profiles():
        if p["name"] == name:
            return Path(p["path"])
    return None


def profiles_dir() -> Path:
    """The directory shipped environment (profile) TOMLs live in."""
    from cairn.core.ui_contract import list_profiles

    profs = list_profiles()
    if profs:
        return Path(profs[0]["path"]).parent
    import cairn.core.ui_contract as _c  # fallback when none exist yet

    return Path(_c.__file__).resolve().parents[1] / "profiles"


def require_delete_confirm(confirm: str) -> None:
    """Backend-enforced typed-delete guard (#323). Every destructive endpoint
    requires ``?confirm=delete`` — a raw API call without it is rejected, so the
    typed-confirmation can't be bypassed by talking to the API directly."""
    if confirm != "delete":
        raise HTTPException(
            status_code=400,
            detail="destructive action requires confirm=delete (type 'delete' to confirm)",
        )


def is_table(v: Any) -> bool:
    """A TOML table (vs a scalar/array) — dict-like and not a string."""
    return hasattr(v, "items") and not isinstance(v, str)


def scalar_type(v: Any) -> Optional[str]:
    if isinstance(v, bool):  # bool before int (bool is an int subclass)
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    return None  # arrays / inline tables → edit via Raw mode


def profile_sections(content: str) -> list[dict[str, Any]]:
    """Parse a profile's TOML into editable sections — one per table, listing its
    scalar entries. Nested tables (e.g. ``[profile.bindings]``) become their own
    sections (``path`` is the table path). Non-scalar values are skipped (the Raw
    editor handles those)."""
    doc = tomlkit.parse(content)
    sections: list[dict[str, Any]] = []

    def walk(table: Any, path: list[str]) -> None:
        entries: list[dict[str, Any]] = []
        subtables: list[tuple[str, Any]] = []
        for key, value in table.items():
            if is_table(value):
                subtables.append((str(key), value))
                continue
            t = scalar_type(value)
            if t is not None:
                entries.append({"key": str(key), "value": value, "type": t})
        if path:  # skip the synthetic root; surface every named table
            sections.append({"path": list(path), "title": ".".join(path), "entries": entries})
        for key, value in subtables:
            walk(value, path + [key])

    walk(doc, [])
    return sections


def apply_profile_values(content: str, values: list[dict[str, Any]]) -> str:
    """Re-apply edited scalar values onto the on-disk TOML via tomlkit, preserving
    comments/structure, and return the new text."""
    doc = tomlkit.parse(content)
    for item in values:
        path = item.get("path") or []
        key = item.get("key")
        if key is None:
            continue
        node: Any = doc
        for seg in path:
            node = node[seg]
        raw = item.get("value")
        typ = item.get("type", "string")
        if typ == "boolean":
            val: Any = raw in (True, "true", "True", "1", 1)
        elif typ == "number":
            try:
                s = str(raw).strip()
                val = int(s) if s.lstrip("-").isdigit() else float(s)
            except (TypeError, ValueError):
                val = raw
        else:
            val = "" if raw is None else str(raw)
        node[key] = val
    return tomlkit.dumps(doc)


def invalidate_policy_cache() -> None:
    """Drop the shared runtime PolicyEngine after a policy write (#1095).

    The per-operation gate consults ONE cached engine
    (:mod:`cairn.core.approval.policy_source`); without this a policy created,
    deleted, or attached in the console would not gate until the next restart.
    Best-effort: a failure here must never fail the write that already landed.
    """
    try:
        from cairn.core.approval.policy_source import (  # noqa: PLC0415 - lazy: off the trigger import path
            reset_runtime_policy_engine,
        )
        reset_runtime_policy_engine()
    except Exception as e:  # noqa: BLE001 — cache invalidation is never fatal
        logging.getLogger("cairn.triggers.webui").warning(
            "could not invalidate the runtime policy cache: %s", e
        )


def apply_policies(content: str, names: Optional[list[str]]) -> str:
    """Set (or clear) the ``[profile].policies`` attach-list via tomlkit,
    preserving comments/structure (#317, ADR-0053).

    ``names`` is a list → write it (incl. ``[]``, which is strict "pack policies
    only"). ``None`` → remove the key entirely, reverting to the back-compat
    "all store policies apply" default.
    """
    doc = tomlkit.parse(content)
    prof = doc.get("profile")
    if prof is None:
        prof = tomlkit.table()
        doc["profile"] = prof
    if names is None:
        if "policies" in prof:
            del prof["policies"]
    else:
        # de-dupe preserving order; store-policy names are plain strings
        seen: list[str] = []
        for n in names:
            if isinstance(n, str) and n and n not in seen:
                seen.append(n)
        prof["policies"] = seen
    return tomlkit.dumps(doc)


def apply_knowledge(content: str, names: Optional[list[str]]) -> str:
    """Set (or clear) the ``[profile].knowledge`` role attach-list via tomlkit,
    preserving comments/structure (#505, ADR-0061 — mirrors ``apply_policies``).

    ``names`` is a list → write it (incl. ``[]`` = no ambient knowledge in this
    env). ``None`` → remove the key, reverting to "all roles active" (back-compat).
    """
    doc = tomlkit.parse(content)
    prof = doc.get("profile")
    if prof is None:
        prof = tomlkit.table()
        doc["profile"] = prof
    if names is None:
        if "knowledge" in prof:
            del prof["knowledge"]
    else:
        seen: list[str] = []
        for n in names:
            if isinstance(n, str) and n and n not in seen:
                seen.append(n)
        prof["knowledge"] = seen
    return tomlkit.dumps(doc)


def apply_backends(content: str, values: dict[str, Any]) -> str:
    """Write backend-configurator choices into ``[profile.backends]`` via tomlkit,
    creating the table if absent and preserving the rest of the file. Only keys in
    the backends schema are written; toggle fields are stored as booleans, the rest
    as strings. Unknown keys are ignored (forward-compatible)."""
    doc = tomlkit.parse(content)
    profile = doc.get("profile")
    if not is_table(profile):
        profile = tomlkit.table()
        doc["profile"] = profile
    backends = profile.get("backends")
    if not is_table(backends):
        backends = tomlkit.table()
        profile["backends"] = backends
    for key, raw in values.items():
        # #305: `<field>_connection` companion keys bind a stored connection NAME
        # (a reference, never a secret) to a credentialed backend. Empty → unbind.
        if key.endswith("_connection") and get_field(key[: -len("_connection")]) is not None:
            if raw:
                backends[key] = str(raw)
            elif key in backends:
                del backends[key]
            continue
        field = get_field(key)
        if field is None:  # not a known backend → skip
            continue
        if field.control == "toggle":
            backends[key] = raw in (True, "true", "True", "1", 1)
        else:
            backends[key] = "" if raw is None else str(raw)
    return tomlkit.dumps(doc)


#: How long a staged upload may sit before a later staging call sweeps it.
STAGE_TTL_SECONDS = 24 * 3600


def staging_dir(name: str) -> str:
    """``<tmp>/<name>``, created on demand: where an upload route stages bytes
    before registering them (dataset import, capture import). Each call first
    sweeps files older than :data:`STAGE_TTL_SECONDS`, best-effort."""
    d = os.path.join(tempfile.gettempdir(), name)
    os.makedirs(d, exist_ok=True)
    cutoff = time.time() - STAGE_TTL_SECONDS
    try:
        for entry in os.listdir(d):
            p = os.path.join(d, entry)
            try:
                if os.path.isfile(p) and os.path.getmtime(p) < cutoff:
                    os.remove(p)
            except OSError:
                pass
    except OSError:
        pass
    return d


def policy_capabilities() -> list[str]:
    """Declared capability names across loaded operators — the form's `capabilities`
    option list. Best-effort: empty when the registry isn't loaded."""
    try:
        from cairn.core.operator.registry import default_registry
        caps: set[str] = set()
        for _name, op_cls in default_registry.all().items():
            for c in (getattr(op_cls, "capabilities", None) or []):
                caps.add(c)
        return sorted(caps)
    except Exception:  # noqa: BLE001 — schema must never crash the dashboard
        return []


async def caller_account(token: Optional[str]) -> str:
    """The account/tenant the caller acts in (ADR-0054, #392 phase 2).

    Delegates to :func:`caller_identity` so every connection route resolves the
    SAME tenant boundary the admin API uses. Auth-off (keyless dev) → the implicit
    ``default`` account, so single-tenant OSS is unchanged and never breaks; auth-on
    with a bad/absent token → ``caller_identity`` raises 401. The returned id is
    what the store call is scoped to — the API-side half of the two-layer isolation
    (the store filter is the other half)."""
    ident = await caller_identity(token)
    return str(ident.get("account_id") or "default")


async def account_from_claims(claims: dict[str, Any]) -> str:
    """:func:`caller_account` for the claims the bearer guard already verified
    (#209) — the same tenant resolution, no second decode of the token."""
    ident = await identity_from_claims(claims)
    return str(ident.get("account_id") or "default")


async def assert_can_write_workspace(token: Optional[str]) -> str:
    """The workspace a write lands in, once the caller is allowed to act in it.

    ``require_auth`` already answered "is this caller authenticated"; this answers
    "…and may they act in THIS workspace". ``_WorkspaceMiddleware`` makes the same
    assertion for the path form, but a write is worth the second check: the
    ``X-Cairn-Workspace`` header fallback reaches the flat route too, and a
    fail-open tenancy check on a write is the #1610 shape of bug. Auth-off
    (keyless dev) resolves to no account, which ``assert_workspace_allowed``
    treats as "no boundary to enforce" — single-node OSS is unchanged.

    Lives here rather than in one router because more than one needs it: the
    workspace-variable writes (#1974) and ``POST /start`` (#1975), which is the
    sharpest write cairn has — it spends money in the named tenant.
    """
    # lazy: the routers ↔ server cycle runs through cairn.core.workspace, which
    # this module is imported by; the admin router deferred these for the same
    # reason before the helper moved here.
    from cairn.core.workspace import (  # noqa: PLC0415
        WorkspaceForbidden,
        assert_workspace_allowed,
        current_workspace,
    )
    ws = current_workspace()
    try:
        await assert_workspace_allowed(ws, await caller_account(token))
    except WorkspaceForbidden:
        raise HTTPException(status_code=403, detail=f"not a member of workspace {ws!r}") from None
    return ws
