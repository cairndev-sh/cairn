"""Read-only admin UI over cairn's run store (starlette-admin).

Optional inspection surface. It needs ``starlette-admin`` and ``itsdangerous``,
which ``cairndev`` does not install (there is no ``admin`` extra): add them to the
environment cairn runs in. It binds to the live :class:`RunStore` via a
**custom data layer** (no ORM), so it works identically whether the store is
SQLite or Postgres (``CAIRN_STORAGE`` / ``CAIRN_RUN_DB``). Unlike Datasette (SQLite
only) this follows the configured backend.

**Read-only by design** (``can_create/edit/delete`` all False): it inspects runs
+ their full structured results, never mutates — so the run state and the
hash-chained audit trail stay intact. Bind it to localhost / behind auth; it
exposes every run's payload.

Mount on the trigger server (or any Starlette/FastAPI app)::

    from cairn.core.admin import mount_admin
    mount_admin(app)            # adds /admin

Or run standalone::

    cairn-admin                 # serves /admin on CAIRN_ADMIN_PORT (default 9091)

Why starlette-admin (not sqladmin/fastapi-admin): those bind to an ORM
(SQLAlchemy / Tortoise) and cairn has none — runs are a JSON ``data`` blob in
raw sqlite/asyncpg. starlette-admin's custom-view API lets us back the view with
``RunStore`` directly.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
from types import SimpleNamespace
from typing import Any, Optional, Sequence

try:
    from starlette_admin import (
        BaseAdmin,
        BaseModelView,
        BooleanField,
        IntegerField,
        JSONField,
        StringField,
    )
    from starlette_admin.auth import AdminUser, AuthProvider, LoginFailed
except ImportError as e:  # optional deps, not an extra of cairndev
    import sys as _sys

    raise ImportError(
        "the admin UI needs starlette-admin and itsdangerous, which cairndev does not "
        "install (there is no 'admin' extra). Add them to the environment cairn runs in:\n"
        f"  uv pip install --python {_sys.executable} starlette-admin itsdangerous"
    ) from e

import json
import logging

from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import Response

from cairn.core.storage import Run, RunStore, default_store

logger = logging.getLogger("cairn.admin")


def _row(run: Run) -> SimpleNamespace:
    """Flatten a Run into an attribute object starlette-admin can read by field
    name (it serializes via ``getattr(obj, field.name)``). The nested trigger is
    lifted to ``service``/``source`` columns; trigger + result keep a JSON view."""
    d = run.model_dump(mode="json")
    trig = d.get("trigger") or {}
    return SimpleNamespace(
        id=d.get("id"),
        status=d.get("status"),
        lane=d.get("lane"),
        fingerprint=d.get("fingerprint"),
        created_at=d.get("created_at"),
        service=trig.get("service"),
        source=trig.get("source"),
        matched_skills=d.get("matched_skills"),
        error=d.get("error"),
        trigger=trig,
        result_payloads=d.get("result_payloads") or [],
    )


class RunView(BaseModelView):
    """Read-only browse of the run store: list runs + drill into the full
    structured result (``result_payloads``) of any one."""

    identity = "run"
    name = "Run"
    label = "Runs"
    icon = "fa fa-play"
    pk_attr = "id"
    page_size = 25
    fields = [
        StringField("id"),
        StringField("status"),
        StringField("service"),
        StringField("source"),
        StringField("lane"),
        IntegerField("matched_skills", label="skills"),
        StringField("created_at"),
        StringField("error"),
        JSONField("trigger"),
        JSONField("result_payloads", label="result"),
    ]

    # ── read-only: never mutate runs or the audit chain ────────────────
    def can_create(self, request: Request) -> bool:
        return False

    def can_edit(self, request: Request) -> bool:
        return False

    def can_delete(self, request: Request) -> bool:
        return False

    # ── custom data layer over RunStore ────────────────────────────────
    _store: Optional[RunStore] = None

    @classmethod
    def store(cls) -> RunStore:
        # One store for the view's lifetime (it holds a connection); resolved
        # from the env-configured backend, same as the engine uses.
        if cls._store is None:
            cls._store = default_store()
        return cls._store

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: Any = None,
        order_by: Optional[list[str]] = None,
    ) -> Sequence[Any]:
        # RunStore.list is newest-first with a limit (no offset) — fetch the
        # window we need and slice. Fine at admin/dev volumes.
        runs = await self.store().list(limit=skip + limit)
        return [_row(r) for r in runs[skip: skip + limit]]

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        run = await self.store().get(pk)
        return _row(run) if run else None

    async def count(self, request: Request, where: Any = None) -> int:
        # No native COUNT on the store; bounded list is enough for an admin.
        return len(await self.store().list(limit=10_000))


class _RegistryView(BaseModelView):
    """Base for read-only views over an in-memory registry (packs, operators,
    MCP servers). Subclass sets identity/name/label/pk_attr/fields + ``rows()``;
    pagination + lookup are derived from the materialized row list."""

    def can_create(self, request: Request) -> bool:
        return False

    def can_edit(self, request: Request) -> bool:
        return False

    def can_delete(self, request: Request) -> bool:
        return False

    def can_view_details(self, request: Request) -> bool:
        # Registry rows are flat — every field shows in the list, so no detail
        # page is needed. It also avoids building a `/detail/{pk}` URL, which the
        # default str path-convertor rejects for operator pks like "model/register".
        return False

    def rows(self) -> list[SimpleNamespace]:  # override
        raise NotImplementedError

    async def find_all(
        self, request: Request, skip: int = 0, limit: int = 100,
        where: Any = None, order_by: Optional[list[str]] = None,
    ) -> Sequence[Any]:
        return self.rows()[skip: skip + limit]

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        return next((r for r in self.rows() if getattr(r, self.pk_attr) == pk), None)

    async def count(self, request: Request, where: Any = None) -> int:
        return len(self.rows())


class PackView(_RegistryView):
    """Registered packs (the pack registry) — what's loaded + what each ships."""

    identity = "pack"
    name = "Pack"
    label = "Packs"
    icon = "fa fa-cube"
    pk_attr = "name"
    page_size = 50
    fields = [
        StringField("name"),
        StringField("version"),
        StringField("description"),
        JSONField("tags"),
        IntegerField("tools"),
        IntegerField("triggers"),
        JSONField("assets", label="asset dirs"),
    ]

    _ASSET_KINDS = (
        "templates", "prompts", "skills", "policies", "schemas",
        "triggers", "evals", "rag", "memory", "examples", "data",
    )

    def rows(self) -> list[SimpleNamespace]:
        from cairn.core.pack.registry import get_packs

        out = []
        for p in get_packs():
            assets = [k for k in self._ASSET_KINDS if getattr(p, f"{k}_dir", None)]
            out.append(SimpleNamespace(
                name=p.name, version=p.version, description=p.description,
                tags=list(p.tags or []), tools=len(p.tools or []),
                triggers=len(getattr(p, "trigger_specs", []) or []), assets=assets,
            ))
        return sorted(out, key=lambda r: r.name)


class OperatorView(_RegistryView):
    """The operator registry — every operator + its governance metadata
    (risk, requires_approval, executors, capabilities). The reason an op pauses
    for HITL is visible here."""

    identity = "operator"
    name = "Operator"
    label = "Operators"
    icon = "fa fa-gears"
    # pk is a /-free token (operator names like "model/register" contain "/",
    # which starlette-admin's str path-convertor rejects when it builds the
    # always-present detailUrl). The real name shows in the `name` column.
    pk_attr = "id"
    page_size = 100
    fields = [
        StringField("name"),
        StringField("version"),
        StringField("risk"),
        BooleanField("requires_approval"),
        JSONField("executors"),
        JSONField("capabilities"),
        BooleanField("supports_mock"),
        StringField("mcp_server"),
    ]

    def rows(self) -> list[SimpleNamespace]:
        from cairn.core.operator.loader import discover_operator_pack_manifests

        out = []
        for name, e in sorted(discover_operator_pack_manifests().items()):
            out.append(SimpleNamespace(
                id=e.name.replace("/", "."),   # /-free routing pk
                name=e.name, version=e.version or "", risk=e.risk or "",
                requires_approval=bool(e.requires_approval),
                executors=list(e.executors or []),
                capabilities=list(e.provides_capabilities or []),
                supports_mock=bool(e.supports_mock),
                mcp_server=e.mcp_server or "",
            ))
        return out


def _as_dict(v: Any) -> dict:
    """Coerce a form/JSON field value to a dict (JSONField may hand back a dict
    or a JSON string; empty → {})."""
    if not v:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "on", "yes")


class ConnectionView(BaseModelView):
    """Infra connections (#68/#70) — access + compute bindings. **Full CRUD.**

    Backed by the ConnectionStore (sqlite/postgres via env). Secrets are stored
    as **references** into the SecretStore — the create/edit form takes
    ``{key: ref}`` (e.g. ``{"API_KEY": "OPENAI_API_KEY"}``), never raw secret
    values; the list shows only the secret KEYS (values live in the SecretStore).
    """

    identity = "connection"
    name = "Connection"
    label = "Connections"
    icon = "fa fa-link"
    pk_attr = "id"
    page_size = 50
    fields = [
        StringField("id", exclude_from_create=True, exclude_from_edit=True, read_only=True),
        StringField("name", required=True),
        StringField("kind", required=True,
                    help_text="mcp | api_key | http | compute.docker|kube|runpod|ray"),
        BooleanField("enabled"),
        JSONField("config", help_text='non-secret params, e.g. {"url": "..."}'),
        JSONField("secrets", exclude_from_list=True,
                  help_text='logical key → SecretStore ref, e.g. {"API_KEY": "OPENAI_API_KEY"}'),
        JSONField("secret_keys", label="secrets",
                  exclude_from_create=True, exclude_from_edit=True),
        StringField("description"),
    ]

    def can_create(self, request: Request) -> bool:
        return True

    def can_edit(self, request: Request) -> bool:
        return True

    def can_delete(self, request: Request) -> bool:
        return True

    _store: Any = None

    @classmethod
    def store(cls):
        if cls._store is None:
            from cairn.core.connections import default_connection_store
            cls._store = default_connection_store()
        return cls._store

    @staticmethod
    def _row(c: Any) -> SimpleNamespace:
        # `secrets` (refs) is present for the edit form but excluded from the
        # list; `secret_keys` is the masked list display. `health` is the last
        # authenticated-probe result (health dot) — None until first probed.
        return SimpleNamespace(
            id=c.id, name=c.name, kind=c.kind, enabled=c.enabled,
            config=dict(c.config), secrets=dict(c.secrets),
            secret_keys=c.secret_keys, description=c.description,
            health=(c.health.model_dump() if getattr(c, "health", None) else None),
        )

    @staticmethod
    async def _probe(conn: Any) -> Any:
        """On add/edit: run the authenticated probe and attach its health to the
        connection before persisting (best-effort — a probe never blocks a save).
        The console "Test" button is a follow-up; it reuses ``test`` below."""
        from cairn.core.connection_probe import probe_and_attach

        try:
            return await probe_and_attach(conn)
        except Exception:  # noqa: BLE001 — best-effort; save the row regardless
            return conn

    async def test(self, pk: Any) -> Any:
        """Console seam (follow-up "Test" button): re-run the authenticated probe
        for an existing connection by id, persist + return the fresh health row."""
        from cairn.core.connection_probe import test_connection

        conn, _ = await test_connection(pk, store=self.store())
        return self._row(conn) if conn is not None else None

    async def find_all(
        self, request: Request, skip: int = 0, limit: int = 100,
        where: Any = None, order_by: Optional[list[str]] = None,
    ) -> Sequence[Any]:
        conns = await self.store().list()
        return [self._row(c) for c in conns[skip: skip + limit]]

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        c = await self.store().get(pk)
        return self._row(c) if c else None

    async def count(self, request: Request, where: Any = None) -> int:
        return len(await self.store().list())

    # ── write hooks ────────────────────────────────────────────────────
    def _kind_default(self) -> str:
        return "api_key"

    async def create(self, request: Request, data: dict) -> Any:
        from cairn.core.connections import Connection

        conn = Connection(
            name=data["name"],
            kind=(data.get("kind") or self._kind_default()),
            config=_as_dict(data.get("config")),
            secrets=_as_dict(data.get("secrets")),
            description=data.get("description") or "",
            enabled=_as_bool(data.get("enabled", True)),
        )
        conn = await self._probe(conn)  # on-add hook: probe + attach health
        await self.store().create(conn)
        return self._row(conn)

    async def edit(self, request: Request, pk: Any, data: dict) -> Any:
        fields: dict[str, Any] = {}
        if data.get("name"):
            fields["name"] = data["name"]
        if data.get("kind"):
            fields["kind"] = data["kind"]
        if "config" in data:
            fields["config"] = _as_dict(data.get("config"))
        if "secrets" in data:
            fields["secrets"] = _as_dict(data.get("secrets"))
        if "description" in data:
            fields["description"] = data.get("description") or ""
        if "enabled" in data:
            fields["enabled"] = _as_bool(data.get("enabled"))
        updated = await self.store().update(pk, **fields)
        # on-edit hook: re-probe with the new config/secrets and persist health.
        probed = await self._probe(updated)
        if probed.health is not None:
            updated = await self.store().update(pk, health=probed.health)
        return self._row(updated)

    async def delete(self, request: Request, pks: list) -> Optional[int]:
        n = 0
        for pk in pks:
            if await self.store().delete(pk):
                n += 1
        return n


class McpServerView(ConnectionView):
    """MCP servers — connections of ``kind=mcp`` (#68). Full CRUD; a focused lens
    over the same ConnectionStore that forces ``kind=mcp`` on create. (The active
    profile's TOML ``mcp_servers`` is a separate runtime source; this is the
    store-backed, admin-managed surface.)"""

    identity = "mcp_server"
    name = "MCP server"
    label = "MCP servers"
    icon = "fa fa-plug"

    def _kind_default(self) -> str:
        return "mcp"

    async def find_all(
        self, request: Request, skip: int = 0, limit: int = 100,
        where: Any = None, order_by: Optional[list[str]] = None,
    ) -> Sequence[Any]:
        conns = [c for c in await self.store().list() if c.kind == "mcp"]
        return [self._row(c) for c in conns[skip: skip + limit]]

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        c = await self.store().get(pk)
        return self._row(c) if c and c.kind == "mcp" else None

    async def count(self, request: Request, where: Any = None) -> int:
        return len([c for c in await self.store().list() if c.kind == "mcp"])

    async def create(self, request: Request, data: dict) -> Any:
        return await super().create(request, {**data, "kind": "mcp"})


class ProfileView(BaseModelView):
    """Deployment profiles (#11) — capability bindings + disambiguation +
    backends. **Full CRUD** over the ProfileStore. (Distinct from the shipped
    TOML profiles; runtime-loading a store-backed profile is a follow-up.)"""

    identity = "profile"
    name = "Profile"
    label = "Profiles"
    icon = "fa fa-sliders"
    pk_attr = "id"
    page_size = 50
    fields = [
        StringField("id", exclude_from_create=True, exclude_from_edit=True, read_only=True),
        StringField("name", required=True),
        StringField("extends", help_text="parent profile name (layering)"),
        BooleanField("enabled"),
        JSONField("bindings", help_text='capability → operator, e.g. {"rag.search": "rag/search"}'),
        JSONField("capabilities", help_text="capability → winning pack (disambiguation)"),
        JSONField("backends", help_text='e.g. {"llm_gateway": "env:OPENAI_API_KEY"}'),
        StringField("description"),
    ]

    def can_create(self, request: Request) -> bool:
        return True

    def can_edit(self, request: Request) -> bool:
        return True

    def can_delete(self, request: Request) -> bool:
        return True

    _store: Any = None

    @classmethod
    def store(cls):
        if cls._store is None:
            from cairn.core.profile_store import default_profile_store
            cls._store = default_profile_store()
        return cls._store

    @staticmethod
    def _row(p: Any) -> SimpleNamespace:
        return SimpleNamespace(
            id=p.id, name=p.name, extends=p.extends, enabled=p.enabled,
            bindings=dict(p.bindings), capabilities=dict(p.capabilities),
            backends=dict(p.backends), description=p.description,
        )

    async def find_all(
        self, request: Request, skip: int = 0, limit: int = 100,
        where: Any = None, order_by: Optional[list[str]] = None,
    ) -> Sequence[Any]:
        rows = await self.store().list()
        return [self._row(p) for p in rows[skip: skip + limit]]

    async def find_by_pk(self, request: Request, pk: Any) -> Any:
        p = await self.store().get(pk)
        return self._row(p) if p else None

    async def count(self, request: Request, where: Any = None) -> int:
        return len(await self.store().list())

    async def create(self, request: Request, data: dict) -> Any:
        from cairn.core.profile_store import ProfileRecord

        p = ProfileRecord(
            name=data["name"], extends=data.get("extends") or "",
            bindings=_as_dict(data.get("bindings")),
            capabilities=_as_dict(data.get("capabilities")),
            backends=_as_dict(data.get("backends")),
            description=data.get("description") or "",
            enabled=_as_bool(data.get("enabled", True)),
        )
        await self.store().create(p)
        return self._row(p)

    async def edit(self, request: Request, pk: Any, data: dict) -> Any:
        fields: dict[str, Any] = {}
        if data.get("name"):
            fields["name"] = data["name"]
        if "extends" in data:
            fields["extends"] = data.get("extends") or ""
        for k in ("bindings", "capabilities", "backends"):
            if k in data:
                fields[k] = _as_dict(data.get(k))
        if "description" in data:
            fields["description"] = data.get("description") or ""
        if "enabled" in data:
            fields["enabled"] = _as_bool(data.get("enabled"))
        return self._row(await self.store().update(pk, **fields))

    async def delete(self, request: Request, pks: list) -> Optional[int]:
        n = 0
        for pk in pks:
            if await self.store().delete(pk):
                n += 1
        return n


class AdminAuth(AuthProvider):
    """Username/password login for the admin panel.

    Credentials come from ``CAIRN_ADMIN_USER`` / ``CAIRN_ADMIN_PASSWORD`` (dev
    default ``admin`` / ``admin``). The signed-in username is kept in the
    signed session cookie (SessionMiddleware). This is a thin gate for a
    local/inspection surface — for a shared deployment, set a strong password
    (and a real ``CAIRN_ADMIN_SECRET``) or front it with your own SSO.
    """

    @staticmethod
    def _expected() -> tuple[str, str]:
        return (
            getenv("CAIRN_ADMIN_USER", "admin"),
            getenv("CAIRN_ADMIN_PASSWORD", "admin"),
        )

    async def login(
        self, username: str, password: str, remember_me: bool,
        request: Request, response: Response,
    ) -> Response:
        exp_user, exp_pwd = self._expected()
        if username == exp_user and password == exp_pwd:
            request.session.update({"admin_user": username})
            return response
        raise LoginFailed("invalid username or password")

    async def is_authenticated(self, request: Request) -> bool:
        user = request.session.get("admin_user")
        if user:
            request.state.user = user
            return True
        return False

    def get_admin_user(self, request: Request) -> AdminUser:
        return AdminUser(username=request.session.get("admin_user", "admin"))

    async def logout(self, request: Request, response: Response) -> Response:
        request.session.clear()
        return response


def build_admin(title: str = "cairn admin") -> BaseAdmin:
    """Construct the Admin: read-only run view + username/password auth."""
    secret = getenv("CAIRN_ADMIN_SECRET")
    if not secret:
        secret = "cairn-admin-dev-secret-change-me"
        logger.warning(
            "CAIRN_ADMIN_SECRET unset — using a dev default; set it (and "
            "CAIRN_ADMIN_PASSWORD) for any shared deployment."
        )
    admin = BaseAdmin(
        title=title,
        base_url="/admin",
        auth_provider=AdminAuth(),
        middlewares=[Middleware(SessionMiddleware, secret_key=secret)],
    )
    admin.add_view(RunView)
    admin.add_view(PackView)
    admin.add_view(OperatorView)
    admin.add_view(ConnectionView)
    admin.add_view(McpServerView)
    admin.add_view(ProfileView)
    return admin


def mount_admin(app: Any, *, title: str = "cairn admin") -> None:
    """Mount the read-only admin at ``/admin`` on a Starlette/FastAPI app."""
    build_admin(title=title).mount_to(app)


def main() -> None:
    """Standalone server: ``cairn-admin`` → /admin on CAIRN_ADMIN_PORT (9091)."""
    import uvicorn  # base dep (fastapi[standard]); CLI-startup deferral
    from starlette.applications import Starlette

    from cairn.core.operator import load_all_operators
    from cairn.core.pack.loader import load_all_packs
    from cairn.core.provider_settings import load_env

    load_env()  # pick up .env (CAIRN_STORAGE / CAIRN_RUN_DB) like other entrypoints
    # Populate the registries the Packs/Operators views read (best-effort).
    load_all_operators(strict=False)
    load_all_packs(strict=False)
    app = Starlette()
    mount_admin(app)
    host = getenv("CAIRN_ADMIN_HOST", "127.0.0.1")
    port = int(getenv("CAIRN_ADMIN_PORT", "9091"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
