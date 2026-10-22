"""Pack templates — contract/run/validate/preflight and friends (#109-#113,
ADR-0072), plus the console-installed pack uninstall (ADR-0045).

Extracted verbatim from server.py (#1191).
"""
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from cairn.core.triggers import console_support as support
from cairn.core.triggers.auth import require_admin
from cairn.core.workspace import current_workspace


def build_templates_router(dispatcher=None) -> APIRouter:
    """The /packs/{pack}/{template}/* surface + /registry/uninstall.

    ``dispatcher`` is no longer read: the only handler that dispatched anything
    was ``/packs/{pack}/{template}/run``, which ADR-0106 D1 (#2134) turned into
    a refusal. The parameter is kept (optional) so ``build_app``'s wiring line
    is unchanged and a future handler here has the dispatcher in reach."""
    # lazy: server.py is fully imported by the time build_app calls this —
    # request models the inline block shared.
    from cairn.core.triggers.server import (  # noqa: PLC0415 — routers ↔ server cycle at import time
        InstallRequest,
    )

    r = APIRouter()

    @r.post("/registry/uninstall")
    async def registry_uninstall(
        body: InstallRequest,
        admin: Annotated[Optional[str], Depends(require_admin)],
    ) -> dict[str, Any]:
        """Uninstall a *console-installed* pack (v1, ADR-0045): guard (must have an
        install record) → uv uninstall → record removed. RCE-class + destructive,
        so auth-gated; engine-bundled/editable packs (no record) are refused with
        409. Restart-to-deactivate — it stays imported until the next start."""
        # cairn-platform#223 — a pack is process-wide, so the deployment admin's
        # alone: with auth on, anyone else (an account owner too) gets 403.
        support.require_deployment_admin(admin)
        # lazy: keep the uninstaller + catalog off the import path
        from cairn.core.registry_install import (
            PackInstallUnsupportedError,
            uninstall_pack,
        )
        try:
            rec = await uninstall_pack(body.ref.strip(), actor="console")
        except PermissionError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except PackInstallUnsupportedError as e:  # #1545 D2 — same posture as install
            raise HTTPException(status_code=501, detail=str(e)) from e
        return rec.model_dump(mode="json")

    @r.get("/packs/{pack}/{template}/contract")
    async def pack_contract(pack: str, template: str, profile: Optional[str] = None) -> dict[str, Any]:
        """UI contract for a template: input/output JSON Schema + plan + steps
        + integrations + remediation + selectable profiles (the payload `cairn
        pack schema --json` emits). ``profile`` re-resolves the plan/can_run/
        remediation under that execution profile. 404 if unregistered."""
        # lazy: avoid a core.triggers → cairn.ask import at module load
        from cairn.core.ui_contract import build_ui_contract
        from cairn.core.connections import lookup_account, default_connection_store, run_account
        ref = f"{pack}/{template}"
        try:
            conns = await default_connection_store().list(lookup_account(await run_account()))
            connected = {c.name for c in conns if getattr(c, "kind", "") == "mcp"}
        except Exception:  # noqa: BLE001 — remediation is best-effort
            connected = set()
        contract = build_ui_contract(ref, connected_names=connected, profile=profile)
        if not contract.plan.get("template_found"):
            raise HTTPException(status_code=404, detail=f"template not found: {ref}")
        return contract.model_dump(mode="json")

    @r.post("/packs/{pack}/{template}/run", dependencies=[Depends(support.writable_workspace)])
    async def pack_run(
        pack: str, template: str, values: dict[str, Any],
        profile: Optional[str] = None, config_preset: Optional[str] = None,
        sandbox: bool = False,
        from_run: Optional[str] = Query(None, alias="from"),  # #989: run lineage
        series: Optional[str] = None,  # #993: attach to a budgeted series
        max_cost_usd: Optional[float] = None,  # #1655
    ) -> dict[str, Any]:
        """REFUSED since ADR-0106 D1 (#2134) — a flow does not run on its own.

        This was cairn's original run door (#113): a bare flow ref in, a ``Run``
        out. #2113 made it produce a one-leg pipeline instead, which fixed the
        RECORD but left the runnable surface implicit — nothing in git said
        which flows were meant to be started alone. ADR-0106 D1 settles it: only
        a declared pipeline runs, so this door answers 400 with the wrapper to
        add and the dev-loop alternative.

        The route is KEPT rather than deleted so an old client gets the rule
        instead of a 404 it will read as a deployment problem. Its parameters
        are kept for the same reason — a caller that still sends ``series`` or
        ``sandbox`` should see the refusal, not a 422 about an unexpected query
        argument.
        """
        from cairn.core.dispatch.start import (  # noqa: PLC0415 — engine off the router import path
            FlowNotRunnableError, flow_kind_installed,
        )
        ref = f"{pack}/{template}"
        raise HTTPException(
            status_code=400,
            detail=str(FlowNotRunnableError(ref, flow_kind=flow_kind_installed(ref))),
        )

    @r.post("/packs/{pack}/{template}/validate")
    async def pack_validate(pack: str, template: str, values: dict[str, Any]) -> dict[str, Any]:
        """Validate + coerce submitted form values against the template's input
        schema (#109) — the SAME `coerce_and_validate` the CLI/TUI use. Returns
        the typed inputs to save as `in.json` + the run command. No execution."""
        # lazy: avoid a core.triggers → cairn.ask import at module load
        from cairn.core.ui_contract import build_ui_contract
        from cairn.core.ui_render import coerce_and_validate
        ref = f"{pack}/{template}"
        contract = build_ui_contract(ref)
        if not contract.plan.get("template_found"):
            raise HTTPException(status_code=404, detail=f"template not found: {ref}")
        typed, errors = coerce_and_validate(contract, values or {})
        return {
            "valid": not errors, "errors": errors, "typed": typed,
            "command": f"cairn run {ref} --input in.json",
        }

    @r.post("/packs/{pack}/{template}/preflight")
    async def pack_preflight(pack: str, template: str, body: dict[str, Any]) -> dict[str, Any]:
        """Dry-run PREFLIGHT (ADR-0072): a side-effect-free "can this run?" verdict.

        Composes the existing side-effect-free checks — input validation, operator
        resolution, the #744 config preflight (fail-closed), gates/cost from the
        plan, advisory connection health — into ``{ready, blockers, warnings, cost,
        gates, resolved_config, inputs_echoed, command}``. NO run row, NO spend, NO
        external writes. Body: ``{values?, profile?}``. 404 if the template isn't
        registered (so the console can distinguish "blocked" from "no such workflow").

        ADR-0101 D3 (#1975): the verdict is composed by
        :func:`cairn.core.dispatch.start.preflight_workflow`, which ``POST
        /start`` with ``dry_run: true`` also calls — one dry run, two doors."""
        # lazy: avoid a core.triggers → cairn.ask import at module load
        from cairn.core.dispatch.start import preflight_workflow
        ref = f"{pack}/{template}"
        verdict = await preflight_workflow(
            ref, (body or {}).get("values") or {},
            environment=(body or {}).get("profile"),
            config_preset=(body or {}).get("config_preset"),
            workspace_id=current_workspace(),
        )
        if not verdict.found:
            raise HTTPException(status_code=404, detail=f"template not found: {ref}")
        return verdict.model_dump(mode="json")

    @r.get("/packs/{pack}/{template}/eligibility")
    async def pack_eligibility(pack: str, template: str) -> dict[str, Any]:
        """Playground eligibility (ADR-0073 §1a): can this workflow run in the
        sandbox? A workflow qualifies iff none of its operators declares
        ``requires_gpu``. Side-effect-free; GPU workflows come back
        ``eligible=false`` with a ``reason`` so the picker can disable-with-reason."""
        # lazy: avoid a core.triggers → cairn.ask import at module load
        from cairn.core.eligibility import workflow_eligibility
        return workflow_eligibility(f"{pack}/{template}").model_dump(mode="json")

    @r.get("/packs/{pack}/{template}/slack-modal")
    async def pack_slack_modal(pack: str, template: str) -> dict[str, Any]:
        """The template's input schema as a Slack Block Kit modal view (#109) —
        what a slash-command handler `views.open`s. Pure JSON; the live Slack app
        wiring (slash + signing + view_submission→run) is a follow-up."""
        # lazy: avoid a core.triggers → cairn.ask import at module load
        from cairn.core.ui_contract import build_ui_contract
        from cairn.core.ui_render import contract_to_slack_modal
        ref = f"{pack}/{template}"
        contract = build_ui_contract(ref)
        if not contract.plan.get("template_found"):
            raise HTTPException(status_code=404, detail=f"template not found: {ref}")
        return contract_to_slack_modal(contract)

    @r.get("/packs/{pack}/{template}/graph")
    async def pack_graph(pack: str, template: str) -> dict[str, Any]:
        """The compiled workflow graph: Mermaid flowchart text + nodes/edges JSON
        (from LangGraph's ``get_graph()`` — the real executable structure incl.
        gates/fan-out/subgraphs). The web renders the Mermaid; nodes/edges are
        React-Flow-ready. 404 if the template isn't registered."""
        # lazy: avoid a core.triggers → cairn.ask/engine import at module load
        from cairn.core.graph_view import build_graph_view
        ref = f"{pack}/{template}"
        view = build_graph_view(ref)
        if view is None:
            raise HTTPException(status_code=404, detail=f"template not found: {ref}")
        return view.model_dump(mode="json")

    return r
