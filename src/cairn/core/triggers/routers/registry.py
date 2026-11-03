"""Pack UI contract + registry surface (#109, ADR-0045/0047/0049, #258) —
extracted from server.py in #1191. Handler bodies are verbatim; only the
decorator target changed (``@app.…`` → ``@r.…``) and the ``installed_workflows``
loop variable was renamed ``r`` → ``r_`` (it would shadow the router).

What CLI/TUI/web/Slack all render from: the pack picker, the registry
catalog/install lifecycle, and the versioned installed-workflow records.
"""
# NO `from __future__ import annotations` here: InstallRequest is lazily
# imported inside the factory (cycle rule), so PEP 563 string annotations
# would be unresolvable in this module's globals when FastAPI builds the
# OpenAPI schema. Without it, handler annotations bind at def time.

from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from cairn.core.refs import ref_key  # ONE ref spelling (#2126)
from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import require_admin, require_auth
from cairn.core.triggers.routers.pipelines import _off_loop  # #2317 listing helper


def build_registry_router() -> APIRouter:
    """The /packs and /registry/* routes (uninstall excluded — it lives in a
    later region of server.py). Captures no build_app state."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # this is the module-level request model the inline block used.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        InstallRequest,
    )

    r = APIRouter()

    # ── Pack UI contract (#109) — what CLI/TUI/web/Slack all render from ──
    @r.get("/packs")
    async def list_packs() -> dict[str, Any]:
        """Registered templates — the UI's pack picker."""
        # lazy: keep cairn.ask out of the core.triggers import path
        from cairn.core.pack.discovery import candidate_templates
        return {"packs": candidate_templates()}

    @r.get("/registry/catalog")
    async def registry_catalog() -> dict[str, Any]:
        """The cairn-registry catalog — available packs/bundles, each annotated
        installed-vs-available against live discovery. Read-only (the install
        lifecycle is #211/#185); the console's "Browse registry" reads this."""
        # lazy: catalog loader pulls httpx + the discovery registries
        from cairn.core.registry_catalog import load_registry_catalog
        return load_registry_catalog(annotate=True)

    @r.post("/registry/install")
    async def registry_install(
        body: InstallRequest,
        admin: Annotated[Optional[str], Depends(require_admin)],
    ) -> dict[str, Any]:
        """Install a pack from the registry — **501, by posture** (#1545 D2).

        Kept as a door that refuses HONESTLY rather than removed outright: a
        501 naming the supported path tells an operator what to do instead,
        where a 404 would tell them nothing, and the decision is explicitly
        provisional (see #1545). Packs reach production through the image:
        ``cairn pack add`` → CI → rebuild → redeploy.
        """
        # A pack is process-wide, so the door is the deployment admin's alone (an
        # internal issue): with auth on, anyone else gets 403 before any install
        # record is written. (A comment, not the docstring: that is the OpenAPI text.)
        support.require_deployment_admin(admin)
        # lazy: keep the installer + catalog off the import path
        from cairn.core.registry_install import (
            PackInstallUnsupportedError,
            install_pack,
        )
        try:
            rec = await install_pack(body.ref.strip(), actor="console")
        except PackInstallUnsupportedError as e:
            raise HTTPException(status_code=501, detail=str(e)) from e
        return rec.model_dump(mode="json")

    @r.get("/registry/installs", dependencies=[Depends(require_auth)])
    async def registry_installs() -> dict[str, Any]:
        """Console-initiated install records + lifecycle (reconciled against live
        discovery by the reader). What this deployment installed via the console."""
        from cairn.core.registry_install import get_install_store
        store = get_install_store()
        return {"installs": [r_.model_dump(mode="json") for r_ in await store.list()]}

    @r.get("/registry/installed-workflows")
    async def installed_workflows() -> dict[str, Any]:
        """Versioned installed-workflow records (#220, ADR-0047) — what's installed,
        at which version + schema (by hash), with required capabilities, composed
        operator packs, and config. Read-only run provenance; the schema docs live
        in the content-addressed store, referenced by ``schema_hash``."""
        from cairn.core.workflow_registry import get_workflow_registry, sync_discovered_workflows
        from cairn.core.workspace import DEFAULT_WORKSPACE, current_workspace
        ws = current_workspace()
        store = get_workflow_registry()
        recs = await store.list_installed(ws)
        # Only `default` auto-syncs from discovery (#258): a fresh single-node
        # deployment shows its discoverable workflows there. Other workspaces start
        # EMPTY and are populated by the explicit per-workspace enable — so a new
        # workspace doesn't silently inherit the whole catalog. Active records only.
        if not recs and ws == DEFAULT_WORKSPACE:
            try:
                await sync_discovered_workflows(workspace_id=ws, store=store)
                recs = await store.list_installed(ws)
            except Exception:  # noqa: BLE001
                pass
        # Playground eligibility (ADR-0073 §1a) — attach per record so the
        # Playground picker filters in one call. Best-effort + memoised by ref
        # (a ref can appear at multiple versions); never fail the list on it.
        # Descriptive metadata (title/description/domain) for the picker (ADR-0074
        # follow-up) — read cheaply from the template frontmatter, memoised by ref.
        # #2360 — that per-record body is synchronous (it plans every ref) and
        # took 2–3 s warm ON the event loop, stalling every other request: it
        # re-walked every operator `pack.toml` once per ref. The manifests are
        # read ONCE per listing now, and the body runs on a worker thread via
        # the #2317 listing helper (same lock, same context copy).
        return {"workflows": await _off_loop(_decorate_installed, recs)}

    def _decorate_installed(recs: list[Any]) -> list[dict[str, Any]]:
        from cairn.core.eligibility import workflow_eligibility, workflow_meta
        from cairn.core.operator.loader import discover_operator_pack_manifests
        from cairn.core.pack.curation import DEFAULT_VISIBILITY
        try:
            manifests = discover_operator_pack_manifests()
        except Exception:  # noqa: BLE001 — best-effort, as per ref below
            manifests = None
        elig_cache: dict[str, dict[str, Any]] = {}
        meta_cache: dict[str, dict[str, Any]] = {}
        out: list[dict[str, Any]] = []
        for r_ in recs:
            d = r_.model_dump(mode="json")
            ref = r_.workflow_ref
            if ref not in elig_cache:
                try:
                    elig_cache[ref] = workflow_eligibility(
                        ref, manifests=manifests).model_dump(mode="json")
                except Exception:  # noqa: BLE001
                    elig_cache[ref] = {"ref": ref, "eligible": True, "reason": ""}
            if ref not in meta_cache:
                try:
                    m = workflow_meta(ref).model_dump(mode="json")
                except Exception:  # noqa: BLE001
                    m = {"ref": ref, "title": "", "description": "",
                         "domain": ref.split("/")[0], "visibility": DEFAULT_VISIBILITY}
                meta_cache[ref] = m
            m = meta_cache[ref]
            d["playground"] = elig_cache[ref]
            d["title"] = m.get("title", "")
            d["description"] = m.get("description", "")
            d["domain"] = m.get("domain", "")
            # #1566 — the curation tier travels WITH the record so the gallery
            # curates from data. The list stays complete (internal rows are
            # returned, just tagged): an operator asking the API what is
            # installed must get the true answer.
            d["visibility"] = m.get("visibility", DEFAULT_VISIBILITY)
            d["composes"] = list(getattr(r_, "composes", []) or [])
            # #2126 — the canonical ref key, stamped SERVER-side, so the
            # Catalog and the Playground join this row against a pipeline leg
            # or a `/flows` row without re-deriving the `dataset_readiness` vs
            # `dataset-readiness` spelling for themselves.
            d["flow_key"] = ref_key(ref)
            out.append(d)
        return out

    @r.post("/registry/sync-workflows", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def sync_workflows() -> dict[str, Any]:
        """Populate the installed-workflow registry from discovered templates
        (#225, ADR-0049 — populate-on-discovery): the DB becomes the system of
        record, discovery syncs into it. Idempotent. Auth-gated (it reads/compiles
        every template). Returns the count synced into the current workspace."""
        from cairn.core.workflow_registry import sync_discovered_workflows
        from cairn.core.workspace import current_workspace
        return {"synced": await sync_discovered_workflows(workspace_id=current_workspace())}

    @r.post("/registry/enable", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def registry_enable(body: InstallRequest) -> dict[str, Any]:
        """Enable one discovered workflow into the current workspace (#258): record
        its provenance so it becomes runnable here, without auto-inheriting the rest
        of the catalog. Workspace comes from the path (`/workspaces/{id}/registry/
        enable`, #261). Idempotent (content-addressed). 404 if the ref isn't found."""
        from cairn.core.workflow_registry import enable_workflow
        from cairn.core.workspace import current_workspace
        ws = current_workspace()
        ok = await enable_workflow(body.ref.strip(), workspace_id=ws)
        if not ok:
            raise HTTPException(status_code=404, detail=f"workflow {body.ref!r} not found / not enableable")
        return {"enabled": body.ref.strip(), "workspace_id": ws}

    @r.post("/registry/disable", dependencies=[Depends(require_auth), Depends(support.writable_workspace)])
    async def registry_disable(body: InstallRequest) -> dict[str, Any]:
        """Disable one workflow in the current workspace — the inverse of
        ``/registry/enable``. Soft: flips the workflow's record to ``disabled`` so it
        drops out of the active listing, never deleting it (runs/events/audit are
        untouched). Reversible by enabling again. Workspace comes from the path
        (`/workspaces/{id}/registry/disable`). 404 if no active record is found."""
        from cairn.core.workflow_registry import disable_workflow
        from cairn.core.workspace import current_workspace
        ws = current_workspace()
        ok = await disable_workflow(body.ref.strip(), workspace_id=ws)
        if not ok:
            raise HTTPException(status_code=404, detail=f"workflow {body.ref!r} not enabled in this workspace")
        return {"disabled": body.ref.strip(), "workspace_id": ws}

    return r
