"""H6b/H6c (#1248, ADR-0088) — checkpoint relay + resume selection.

H6a made the RUNNER snapshot declared stages into ``checkpoints/stage-NN/``
next to the workdir. The two halves here make that survive the box and get
USED:

- **relay** (H6b): copy newly observed checkpoint dirs into durable
  control-plane storage and register each as an artifact (descriptor →
  registry, bytes → the durable root; the ADR-0080 control/data split).
  Local engines relay by direct copy; a remote engine supplies the same two
  functions over its own channel (SSH sftp — the H6d box session wires it).
- **selection** (H6c): given a spec's key, find the newest registered
  checkpoint and build the ``resume_from`` block the H6a runner consumes.

The spec key formula here MUST equal the runner's (blake2b-8 over the spec
JSON excluding ``resume_from``) — a cross test pins the mirror.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from cairn.core.dataflow.contract import DataflowSpec, ResumeFrom
from cairn.core.storage.artifacts import ArtifactDescriptor, ArtifactStore

_log = logging.getLogger("cairn.core.dataflow.checkpoints")

#: durable root for relayed checkpoint bytes when the environment binds no
#: object store — control-plane local disk is sufficient for the failure H6
#: insures (BOX death); an s3:// backend rides the same uri field later.
_DEFAULT_ROOT = Path(os.environ.get(
    "CAIRN_CHECKPOINT_DIR", str(Path.home() / ".cairn" / "checkpoints")))


def spec_key(spec: DataflowSpec) -> str:
    """The spec's checkpoint identity. A carried ``checkpoint_key`` wins
    (#1314 — the control plane owns identity; engine-side rewrites like the
    SSH path's ``_localize_inputs`` must not fork it). Otherwise the hash —
    resume_from excluded (a resumed submission would self-invalidate its own
    checkpoints) and checkpoint_key excluded (stamping must not change the
    key, and unstamped specs keep their pre-#1314 hashes)."""
    if getattr(spec, "checkpoint_key", None):
        return str(spec.checkpoint_key)
    return hashlib.blake2b(
        spec.model_dump_json(exclude={"resume_from", "checkpoint_key"}).encode("utf-8"),
        digest_size=8,
    ).hexdigest()


async def relay_local_checkpoints(
    workdir: Path,
    *,
    run_id: str,
    store: ArtifactStore,
    dest_root: Optional[Path] = None,
    workspace_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Relay every not-yet-relayed checkpoint under ``workdir`` (H6b, local
    form): copy ``checkpoints/stage-NN/`` to the durable root and register a
    descriptor, owned by ``workspace_id`` (else the workspace in scope; ADR-0148).
    Idempotent per (run, stage) — safe on every heartbeat tick.
    Best-effort by contract: relay failure must never disturb the leg.
    """
    out: list[dict[str, Any]] = []
    src_root = Path(workdir) / "checkpoints"
    if not src_root.is_dir():
        return out
    root = Path(dest_root) if dest_root else _DEFAULT_ROOT
    for src in sorted(src_root.iterdir()):
        manifest_path = src / "manifest.json"
        if not src.is_dir() or not manifest_path.is_file():
            continue
        dest = root / run_id / src.name
        if dest.exists():
            continue                     # already relayed on an earlier tick
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            tmp = dest.with_suffix(".partial")
            if tmp.exists():
                shutil.rmtree(tmp)
            shutil.copytree(src, tmp)
            tmp.rename(dest)             # atomic-enough: visible only complete
            content_hash = "sha256:" + hashlib.sha256(
                manifest_path.read_bytes()).hexdigest()[:16]
            await store.register(
                ArtifactDescriptor(
                    uri=f"file://{dest}",
                    kind="dataset",
                    content_hash=content_hash,
                    stats={
                        "checkpoint": True,
                        "spec_key": str(manifest.get("spec_key", "")),
                        "stage_index": int(manifest.get("stage_index", -1)),
                        "stage": str(manifest.get("stage", "")),
                    },
                ),
                producer_run_id=run_id,
                producer_job="checkpoint-relay",
                workspace_id=workspace_id,
            )
            manifest["_relayed_uri"] = f"file://{dest}"
            out.append(manifest)
        except Exception:  # noqa: BLE001 — insurance must not disturb the leg
            _log.warning("checkpoint relay failed for %s", src, exc_info=True)
    return out


async def find_resume_checkpoint(
    store: ArtifactStore, key: str, *,
    workspace_id: Optional[str] = None,
    run_workspace: Optional[Callable[[str], Awaitable[Optional[str]]]] = None,
) -> Optional[ResumeFrom]:
    """H6c — the newest registered, still-readable checkpoint for ``key``,
    as the ``resume_from`` block the runner consumes. ``None`` when there is
    nothing to resume from (fresh legs cost one registry query).

    ``workspace_id`` keeps the search inside one workspace: a checkpoint counts
    only when ``run_workspace(producer_run_id)`` (the workspace of the run that
    relayed it) is that workspace. The spec key is a hash of the spec, so two
    workspaces running the same spec share it; without this a leg could resume
    from another workspace's bytes. A producer run that cannot be read does not
    count — the leg starts fresh instead. The registry read itself is in
    ``workspace_id`` too (ADR-0148): another workspace's checkpoints are not
    listed at all."""
    try:
        records = await store.list_artifacts(kind="dataset", limit=200, workspace_id=workspace_id)
    except Exception:  # noqa: BLE001 — a registry hiccup means "no resume"
        _log.debug("checkpoint lookup failed", exc_info=True)
        return None
    best = None
    for r in records:
        s = dict(getattr(r, "stats", {}) or {})
        if not s.get("checkpoint") or str(s.get("spec_key", "")) != key:
            continue
        if workspace_id is not None and not await _produced_in(r, workspace_id, run_workspace):
            continue
        if best is None or int(s.get("stage_index", -1)) > int(
                dict(best.stats).get("stage_index", -1)):
            best = r
    if best is None:
        return None
    dest = Path(str(best.uri).removeprefix("file://"))
    manifest_path = dest / "manifest.json"
    if not manifest_path.is_file():
        _log.warning("checkpoint %s registered but bytes missing at %s",
                     best.id, dest)
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    idx = int(manifest["stage_index"])
    return ResumeFrom(
        spec_key=key,
        stage_index=idx,
        artifacts=[{
            "uri": f"file://{dest / f['name']}",
            "content_hash": f.get("content_hash", ""),
            "dest": f["name"],
        } for f in manifest.get("files", [])],
        # frozen stats for the checkpointed stage; earlier un-checkpointed
        # stages get empty-stat synthetic markers (skip is what matters)
        completed=[{"index": i, "stage": "", "stats": {}} for i in range(idx)]
        + [{"index": idx, "stage": manifest.get("stage", ""),
            "stats": dict(manifest.get("stats") or {})}],
    )


async def _produced_in(record: Any, workspace_id: str,
                       run_workspace: Optional[Callable[[str], Awaitable[Optional[str]]]]) -> bool:
    """Whether ``record`` was relayed by a run of ``workspace_id``. Unknown is no."""
    producer = str(getattr(record, "producer_run_id", "") or "")
    if not producer or run_workspace is None:
        return False
    try:
        return await run_workspace(producer) == workspace_id
    except Exception:  # noqa: BLE001 — an unreadable producer is not this workspace's
        _log.debug("checkpoint producer %s unreadable", producer, exc_info=True)
        return False


__all__ = ["find_resume_checkpoint", "relay_local_checkpoints", "spec_key"]
