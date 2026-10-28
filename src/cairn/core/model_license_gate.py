"""The LICENSE DOOR (#2027, ADR-0102 D5) — a gated catalog model may not
launch without the run's own ``accept_license: true`` input AND a connected
Hugging Face token (``cairn.core.connections``, provider ``"huggingface"``).
Refused otherwise, naming the model id and its licence — never a bare 500.

Same shape as the holdout/intake doors (``cairn.core.holdout`` /
``cairn.core.intake``): :func:`check_license_for_inputs` is side-effect-free
so preflight (``cairn.core.preflight``) and the dispatcher
(``cairn.core.dispatch.dispatcher``) both call the SAME check — a dry run's
blocker names exactly what a real launch refuses. :func:`record_license_acceptance`
writes the audit row (``model.license.accepted``) and is called ONLY by the
dispatcher, only once the run has cleared the gate — never at preflight, and
never with the token value itself (only the model id + licence name).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from cairn.core.pack.model_catalogs import lookup as _catalog_lookup


@dataclass
class LicenseVerdict:
    """What the door found. ``ok`` is the answer; the rest is the record."""

    #: True when there is nothing to refuse: no catalog model in this run's
    #: inputs, an ungated catalog model, or a gated model correctly accepted.
    ok: bool = True
    #: False when ``base_model`` did not name a KNOWN catalog entry at all —
    #: the free-text "Any Hugging Face id" escape hatch, which this door has
    #: nothing to say about.
    checked: bool = False
    model_id: Optional[str] = None
    license: Optional[str] = None
    gated: bool = False
    has_token: bool = False
    accepted: bool = False

    def reasons(self) -> list[str]:
        if self.ok or not self.checked or not self.gated:
            return []
        missing = []
        if not self.accepted:
            missing.append("accept_license was not set to true")
        if not self.has_token:
            missing.append("no connected Hugging Face token (add one under Connections)")
        return [f"{self.model_id} is gated under {self.license!r} — " + "; ".join(missing)]

    def as_record(self) -> dict[str, Any]:
        return {
            "checked": self.checked, "ok": self.ok, "model_id": self.model_id,
            "license": self.license, "gated": self.gated,
            "has_token": self.has_token, "accepted": self.accepted,
        }


def _base_model_in(inputs: dict[str, Any]) -> Optional[str]:
    v = inputs.get("base_model")
    return v.strip() if isinstance(v, str) and v.strip() else None


def _accept_license_in(inputs: dict[str, Any]) -> bool:
    return bool(inputs.get("accept_license", False))


async def _has_hf_token() -> bool:
    """Best-effort: is there an ENABLED ``huggingface`` Connection whose token
    secret actually resolves to a non-empty value? Returns only a boolean —
    the token value itself is never read into a log, an audit row, or the
    caller's control flow beyond this truthiness check."""
    from cairn.core.connections import (  # noqa: PLC0415 — off the hot import path
        lookup_account,
        default_connection_store,
        resolve_connection_obj,
        run_account,
    )

    try:
        # the launching run's own account: a tenant's token, never the deployment's
        account = lookup_account(await run_account())
        store = default_connection_store()
        try:
            conn = await store.get_by_name("huggingface", account_id=account)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()
        if conn is None or not conn.enabled:
            return False
        resolved = await resolve_connection_obj(conn)
        if resolved is None:
            return False
        return bool(resolved.secrets.get("TOKEN"))
    except Exception:  # noqa: BLE001 — a store hiccup fails the gate CLOSED, never breaks the request
        return False


async def check_license_for_inputs(inputs: dict[str, Any]) -> LicenseVerdict:
    """The launch-path entry point: read ``base_model`` (+ ``accept_license``)
    out of a run's inputs and evaluate the gate. ``inputs`` may be the typed
    OR the raw form — same both-values reasoning preflight already applies to
    the holdout/intake doors is the caller's job, not this function's."""
    model_id = _base_model_in(inputs)
    if not model_id:
        return LicenseVerdict(ok=True, checked=False)

    entry = _catalog_lookup(model_id)  # the installed `cairn.model_catalogs` providers (ADR-0120 D4)
    if entry is None:
        # Not a catalog id — or no catalogue is installed (then nothing is gated). — the "Any Hugging Face id" escape hatch. Nothing
        # this door can check; existing free-text launches are unaffected.
        return LicenseVerdict(ok=True, checked=False, model_id=model_id)

    if not entry.gated:
        return LicenseVerdict(ok=True, checked=True, model_id=model_id,
                               license=entry.license, gated=False)

    accepted = _accept_license_in(inputs)
    has_token = await _has_hf_token()
    ok = accepted and has_token
    return LicenseVerdict(ok=ok, checked=True, model_id=model_id, license=entry.license,
                           gated=True, has_token=has_token, accepted=accepted)


async def record_license_acceptance(
    verdict: LicenseVerdict, *, actor: Optional[str] = None, run_id: Optional[str] = None,
) -> None:
    """Write the ``model.license.accepted`` audit row for a gated model's
    admitted launch. The dispatcher calls this ONCE, right after the gate
    passes and the run id exists — never at preflight (side-effect free) and
    never for an ungated model or a free-text id (nothing was accepted)."""
    if not (verdict.checked and verdict.gated and verdict.ok):
        return
    from cairn.core.obs.audit import audit_emit  # noqa: PLC0415 — keep this module import-light

    await audit_emit(
        "model.license.accepted",
        actor=actor,
        target=verdict.model_id,
        run_id=run_id,
        license=verdict.license,
    )
