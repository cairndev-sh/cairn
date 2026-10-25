"""Knowledge role composition (#496, ADR-0061) — the cross-pack composer.

A corpus declares the GLOBAL role it provides into via
``[requires.rag.<name>].role``. This module resolves a role to the UNION of
every corpus, across every installed pack, that provides it — so one workflow
composes knowledge from many independently-owned packs into one slot.

Binding SCOPE (the ADR-0053 pattern, per the #471 binding decision):
  - **global** — a role is available wherever it's declared; ``resolve_role``
    with ``active_roles=None`` returns every provider.
  - **env-scoped** — a profile/environment's ``Profile.knowledge`` attach-list
    names which roles are LIVE for that deployment. Passing that set as
    ``active_roles`` gates the resolution: a role not in the set resolves empty.
    ``None`` (no attach-list) ⇒ all roles active, matching how absent
    ``Profile.policies`` leaves all store policies active (back-compat).

This slice resolves the composed corpus SET; ambient retrieval + injection +
citation over it is Slice 3 (#497), policy admissibility Slice 4 (#498).
"""
from __future__ import annotations

from typing import Optional

from cairn.services.rag.lifecycle import CorpusTarget, discover_corpora


def list_roles(packs=None, *, workspace: Optional[str] = None) -> dict[str, list[CorpusTarget]]:
    """Map every declared knowledge role → the corpora that provide into it, for
    ONE workspace: every installed pack's corpora (the deployment's packs, so
    listed in every workspace; retrieval still reads the caller's workspace
    collection, as before) plus that workspace's own BYO corpora (ADR-0062
    "Isolation"); another workspace's BYO corpora are never listed.
    ``workspace`` defaults to :func:`~cairn.core.workspace.current_workspace`.
    Corpora with no ``role`` are omitted (legacy explicit-node corpora don't
    participate in composition)."""
    from cairn.core.workspace import current_workspace
    from cairn.services.rag.byo import workspace_corpus_targets_sync  # BYO union (#516)

    ws = workspace or current_workspace()
    roles: dict[str, list[CorpusTarget]] = {}
    # pack corpora + this workspace's (BYO) corpora compose into the same roles
    for t in list(discover_corpora(packs)) + workspace_corpus_targets_sync(ws):
        if not t.role:
            continue
        roles.setdefault(t.role, []).append(t)
    # deterministic order — by pack-qualified corpus id
    for members in roles.values():
        members.sort(key=lambda t: t.corpus_id)
    return roles


def resolve_role(
    role: str,
    *,
    active_roles: Optional[set[str]] = None,
    packs=None,
) -> list[CorpusTarget]:
    """The composed corpus set for ``role`` — the union across all providing
    packs, in deterministic order.

    ``active_roles`` gates by the environment's attach-list: when a set is
    given and ``role`` is not in it, the role is inactive for this deployment
    and resolves to ``[]``. ``None`` ⇒ every role is active (back-compat).
    BYO corpora are the current workspace's only (see :func:`list_roles`).
    """
    if active_roles is not None and role not in active_roles:
        return []
    return list_roles(packs).get(role, [])


def active_knowledge_roles(profile) -> Optional[set[str]]:
    """The set of roles a profile activates (its ``knowledge`` attach-list), or
    ``None`` when the profile declares none — meaning all roles are active, the
    same back-compat default ``Profile.policies`` uses for store policies."""
    names = getattr(profile, "knowledge", None) if profile is not None else None
    return set(names) if names else None


__all__ = ["list_roles", "resolve_role", "active_knowledge_roles"]
