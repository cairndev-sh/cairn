"""Durable BYTES behind the artifact registry (#1395, epic #2019 T17).

:mod:`cairn.core.storage.artifacts` is content-addressed, wired into lineage,
and stores a **reference**. That was the gap: only CHECKPOINTS were relayed off
a box (``operator_training.checkpoint_sync``), so any other registered artifact
whose URI is a box workdir (``file:///…/workdir/eval.jsonl``) dangled the moment
the box died or the workdir was cleaned — and lineage went on citing an artifact
nobody could fetch. Eval results (#2031), reports and the export bundle (T13)
all land as exactly that shape.

**What this module does.** When an object store is configured, a registration
whose URI is LOCAL gets its bytes copied to
``s3://<bucket>/<prefix>/artifacts/<content-hash>/<name>`` and the registry row
keeps **both** URIs::

    uri          file:///data/run-42/eval.jsonl    ← where it was produced
    durable_uri  s3://cairn-artifacts/…/eval.jsonl ← where it can still be read

The original is never rewritten. Where bytes were produced is provenance; an
overwrite would erase it, and a run's logs would then point at a path that never
existed on that host.

**Configuration** reuses the plane that is already there rather than inventing a
second one (``cairn.core.profile_backends``'s ``artifact_store`` knob and the
``CAIRN_ARTIFACT_S3_*`` contract the export kernels + ``dataflow.capture`` share):

``CAIRN_ARTIFACT_STORE``
    ``s3`` (with ``CAIRN_ARTIFACT_S3_BUCKET`` / ``_PREFIX``) — the spelling the
    deploy compose files already set — or, equivalently, a literal
    ``s3://bucket/prefix``. Anything else (``local``, unset) means no relay and
    every helper here is a no-op: local-only installs are unchanged.

The boto3 client is :func:`cairn.core.dataflow.capture.s3_client` — the same
one, so region/endpoint (MinIO, R2) can never be resolved two different ways.

**Posture.** The relay is BEST-EFFORT and never raises into a registration: a
dead bucket must not make the control plane forget what a run produced. What it
must not do is *lie* — a failed relay leaves ``durable_uri`` empty, the artifact
reads ``local`` or ``dangling``, and :func:`registry_health` (the doctor check
and the console's badge) says so.

**Scope, deliberately.** Registration is control-plane-only (ADR-0081 D3), so
the relay can only copy bytes the CONTROL PLANE can read — the co-located case
(engine and leg on one host, the local/dev/light default) and anything an
operator staged back. A file that only ever existed on a remote GPU box is not
reachable from here; it registers with no durable copy and shows as dangling
once the box is gone, which is the honest answer and the one the health check
exists to surface. Directory URIs (an adapter dir) are likewise not relayed —
``checkpoint_sync`` owns those.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import logging
import os
from typing import Optional
from urllib.parse import unquote, urlparse

logger = logging.getLogger("cairn.storage.artifact_bytes")

#: Key segment every relayed artifact lands under, so a bucket shared with the
#: capture/export keys stays legible.
KEY_ROOT = "artifacts"

#: Files larger than this are NOT relayed inline at registration — a multi-GB
#: copy inside a registration call would stall the control plane. Skipped, not
#: silently truncated: the row keeps no durable copy and the health check counts
#: it. Override with ``CAIRN_ARTIFACT_RELAY_MAX_BYTES``.
DEFAULT_MAX_BYTES = 512 * 1024 * 1024

#: The three honest answers about one registered artifact.
DURABLE = "durable"
LOCAL = "local"
DANGLING = "dangling"

#: Schemes that are off-box by construction — already durable, nothing to relay.
_REMOTE_SCHEMES = ("s3", "gs", "az", "abfs", "abfss", "http", "https", "hf", "mlflow")


def durable_target() -> tuple[str, str] | None:
    """``(bucket, prefix)`` for the artifact object store, or ``None``.

    ``None`` is the default and means "no relay" — every caller must no-op, not
    guess a bucket.
    """
    raw = getenv("CAIRN_ARTIFACT_STORE", "").strip()
    if raw.startswith("s3://"):
        bucket, _, prefix = raw[len("s3://"):].partition("/")
        return (bucket, prefix.strip("/")) if bucket else None
    if raw.lower() != "s3":
        return None
    bucket = getenv("CAIRN_ARTIFACT_S3_BUCKET", "").strip()
    if not bucket:
        return None
    return bucket, getenv("CAIRN_ARTIFACT_S3_PREFIX", "").strip("/")


def local_path(uri: str) -> str | None:
    """The on-disk path a URI names, or ``None`` if it names something remote.

    ``file://`` URIs and bare absolute paths are local; every scheme in
    :data:`_REMOTE_SCHEMES` (and any other ``scheme://``) is not. Percent-escapes
    are decoded — a workdir with a space in it is still a path.
    """
    if not uri:
        return None
    if uri.startswith("file://"):
        return unquote(urlparse(uri).path) or None
    if "://" in uri:
        return None
    return uri if uri.startswith("/") else None


def durable_uri_for(content_hash: str, name: str, target: tuple[str, str] | None = None) -> str:
    """Where ``content_hash``'s bytes live in the object store. Content-addressed,
    so re-relaying identical bytes is the same key — idempotent by construction,
    exactly like the registration it rides along with."""
    target = target or durable_target()
    if target is None:
        return ""
    bucket, prefix = target
    head = f"s3://{bucket}" + (f"/{prefix}" if prefix else "")
    safe = os.path.basename(name.rstrip("/")) or "bytes"
    return f"{head}/{KEY_ROOT}/{content_hash}/{safe}"


def _max_bytes() -> int:
    raw = os.environ.get("CAIRN_ARTIFACT_RELAY_MAX_BYTES", "").strip()
    try:
        return int(raw) if raw else DEFAULT_MAX_BYTES
    except ValueError:
        return DEFAULT_MAX_BYTES


def _put(src: str, dest: str) -> None:
    """Blocking upload — run off the event loop by :func:`relay`."""
    from cairn.core.dataflow.capture import (  # noqa: PLC0415 - cycle: dataflow imports storage on export paths
        s3_client,
        split_s3_uri,
    )

    bucket, key = split_s3_uri(dest)
    client = s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return  # same content hash, same bytes — already there
    except Exception:  # noqa: BLE001 - any "not there / can't tell" means upload
        pass
    client.upload_file(src, bucket, key)


async def relay(uri: str, content_hash: str) -> str:
    """Copy a LOCAL artifact's bytes to the object store; return its durable URI.

    ``""`` — never an exception — whenever the relay does not happen: no store
    configured, a remote URI (already durable), a path this host cannot read (the
    remote-box case), a directory, an oversized file, or an S3 error. The caller
    stores that empty string as "no durable copy", which is what
    :func:`resolution_state` then reports.
    """
    target = durable_target()
    if target is None or not content_hash:
        return ""
    path = local_path(uri)
    if not path or not os.path.isfile(path):
        return ""
    try:
        size = os.path.getsize(path)
    except OSError:
        return ""
    cap = _max_bytes()
    if size > cap:
        logger.warning(
            "artifact %s not relayed: %d bytes exceeds CAIRN_ARTIFACT_RELAY_MAX_BYTES=%d "
            "— it stays reachable only on this host", content_hash, size, cap)
        return ""
    dest = durable_uri_for(content_hash, path, target)
    try:
        await asyncio.to_thread(_put, path, dest)
    except Exception:  # noqa: BLE001 - the relay is best-effort; a dead bucket must not fail a registration
        logger.warning("artifact byte relay failed for %s → %s", uri, dest, exc_info=True)
        return ""
    return dest


def resolution_state(uri: str, durable_uri: str = "") -> str:
    """Can this artifact still be fetched, and for how long?

    :data:`DURABLE`
        a durable copy exists, or the URI is already off-box. Survives the box.
    :data:`LOCAL`
        the file is here, on THIS host, and only here. Readable now; gone with
        the box. Deliberately not "ok": the difference is the whole ticket.
    :data:`DANGLING`
        nothing resolves. Lineage cites bytes that cannot be fetched.
    """
    if durable_uri:
        return DURABLE
    path = local_path(uri)
    if path is None:
        return DURABLE if uri else DANGLING
    return LOCAL if os.path.exists(path) else DANGLING


#: How many artifacts one health pass reads before it stops and says so. A
#: registry is unbounded; a doctor check is not allowed to be.
SCAN_LIMIT = 2000
_PAGE = 200


async def registry_health(store, *, scan: int = SCAN_LIMIT, sample: int = 5,
                          workspace_id: Optional[str] = None) -> dict:
    """Count the registry by :func:`resolution_state`, newest first.

    Counts the artifacts ``workspace_id`` owns (else the workspace in scope;
    ADR-0148), the same answer the registry gives that workspace's reads.

    The ONE query behind both readers — ``cairn doctor`` and the console's
    lineage/data-products surface — so the CLI and the UI can never disagree
    about how many artifacts are unfetchable.

    ``dangling_sample`` carries the first few offenders (id/kind/uri/producer
    run), because a bare count tells an operator nothing about where to look.
    ``truncated`` is true when the registry is larger than ``scan``.
    """
    counts = {DURABLE: 0, LOCAL: 0, DANGLING: 0}
    offenders: list[dict] = []
    seen = 0
    offset = 0
    while seen < scan:
        page = await store.list_artifacts(limit=min(_PAGE, scan - seen), offset=offset,
                                          workspace_id=workspace_id)
        if not page:
            break
        for rec in page:
            seen += 1
            state = resolution_state(rec.uri, getattr(rec, "durable_uri", "") or "")
            counts[state] += 1
            if state == DANGLING and len(offenders) < sample:
                offenders.append({"id": rec.id, "kind": rec.kind, "uri": rec.uri,
                                  "producer_run_id": rec.producer_run_id})
        offset += len(page)
        if len(page) < _PAGE:
            break
    target = durable_target()
    return {
        "total": seen,
        "durable": counts[DURABLE],
        "local": counts[LOCAL],
        "dangling": counts[DANGLING],
        "dangling_sample": offenders,
        "scanned": seen,
        "truncated": seen >= scan,
        "backend": f"s3://{target[0]}/{target[1]}".rstrip("/") if target else "",
    }
