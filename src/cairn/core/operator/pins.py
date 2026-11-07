"""Reproducibility pins at train-submit time (ADR-0082 P0, #1072).

The reproducibility floor for the training leg: at SUBMIT time the run
record is stamped with what was actually submitted — the dataset reference
AND its content hash, the container image AND its resolved digest (a
``:latest`` tag is mutable; the digest is not), and the full resolved spec.
Pins survive a failed or cancelled job — that is the point: the SUBMISSION
stays reproducible even when the run isn't.

Contract: best-effort, never in the way. ``record_train_pins`` never raises
and the digest lookup is bounded by a short timeout — a registry outage must
not delay or fail a GPU submit. Disable digest resolution entirely with
``CAIRN_PIN_IMAGE_DIGEST=0`` (air-gapped/offline).

``dataset_hash`` matches ``lineage/record`` semantics (local file → sha256
of the BYTES; remote ref (hf://…, s3://…) → sha256 of the ref STRING), so
submit-time and promote-time pins agree on the same dataset.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from urllib.request import url2pathname

import httpx

_log = logging.getLogger("cairn.core.operator.pins")

_DIGEST_TIMEOUT_S = 3.0

_MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
    )
)


_SHA256_FRAGMENT = re.compile(r"#sha256=([0-9a-fA-F]{64})$")


def dataset_hash(ref: str) -> str:
    """``sha256:`` content hash of a dataset ref.

    - ``<ref>#sha256=<hex>`` (the launch door's pinned form) → that hash,
      taken as stated without reading the data;
    - a local path or ``file://`` URI → sha256 of the file's BYTES;
    - a genuinely remote ref (``hf://``, ``s3://``…) → sha256 of the ref
      STRING (same semantics as ``lineage/record``).
    """
    m = _SHA256_FRAGMENT.search(ref)
    if m:
        return f"sha256:{m.group(1).lower()}"
    try:
        local = ref
        if ref.startswith("file:"):
            local = url2pathname(urlparse(ref).path)
        p = Path(local)
        if p.is_file():
            h = hashlib.sha256()
            with p.open("rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            return f"sha256:{h.hexdigest()}"
    except OSError:
        pass
    return f"sha256:{hashlib.sha256(ref.encode('utf-8')).hexdigest()}"


def _parse_image(image: str) -> Optional[tuple[str, str, str]]:
    """``[host/]repo[:tag]`` → ``(registry_host, repo, tag)``; None if the
    reference is unparseable. Hostless refs resolve Docker-Hub style."""
    name = image.strip()
    if not name:
        return None
    tag = "latest"
    base, sep, maybe_tag = name.rpartition(":")
    if sep and "/" not in maybe_tag:
        name, tag = base, maybe_tag
    parts = name.split("/")
    if len(parts) > 1 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        host, repo = parts[0], "/".join(parts[1:])
    else:
        host = "registry-1.docker.io"
        repo = name if "/" in name else f"library/{name}"
    if not repo:
        return None
    return host, repo, tag


async def resolve_image_digest(image: str) -> str:
    """Best-effort tag → ``sha256:…`` digest via the registry v2 API
    (anonymous pull token). Empty string on ANY failure — never raises,
    bounded by ``_DIGEST_TIMEOUT_S``."""
    if os.environ.get("CAIRN_PIN_IMAGE_DIGEST", "").strip().lower() in ("0", "false", "off", "no"):
        return ""
    if "@sha256:" in image:
        return "sha256:" + image.rsplit("@sha256:", 1)[1]
    parsed = _parse_image(image)
    if parsed is None:
        return ""
    host, repo, tag = parsed
    if host == "registry-1.docker.io":
        token_url, service = "https://auth.docker.io/token", "registry.docker.io"
    else:
        token_url, service = f"https://{host}/token", host
    try:
        async with httpx.AsyncClient(timeout=_DIGEST_TIMEOUT_S, follow_redirects=True) as client:
            headers = {"Accept": _MANIFEST_ACCEPT}
            params = {"service": service, "scope": f"repository:{repo}:pull"}
            tok = await client.get(token_url, params=params)
            if tok.status_code != 200:
                # private package (GHCR defaults private): retry the token
                # exchange with a registry credential when one is around.
                pat = (os.environ.get("GHCR_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
                if pat:
                    tok = await client.get(token_url, params=params, auth=("token", pat))
            if tok.status_code == 200:
                payload = tok.json()
                token = payload.get("token") or payload.get("access_token") or ""
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            resp = await client.head(f"https://{host}/v2/{repo}/manifests/{tag}", headers=headers)
            if resp.status_code == 200:
                return resp.headers.get("docker-content-digest", "")
    except Exception:  # noqa: BLE001 — pinning is best-effort; a registry outage must not fail a submit
        pass
    return ""


def _find_dataset_uri(resolved_input: dict[str, Any]) -> str:
    """The dataset reference in a resolved operator input: a TrainSpec-shaped
    field directly, or the ``remote/exec`` env convention."""
    for key in ("dataset_uri", "dataset_path"):
        v = resolved_input.get(key)
        if isinstance(v, str) and v:
            return v
    # ml/run nests the spec's values under config (#1074)
    cfg = resolved_input.get("config")
    if isinstance(cfg, dict):
        for key in ("dataset_uri", "dataset_path"):
            v = cfg.get(key)
            if isinstance(v, str) and v:
                return v
    env = resolved_input.get("env")
    if isinstance(env, dict):
        v = env.get("CAIRN_DATASET_URI") or env.get("CAIRN_DATASET_PATH")
        if isinstance(v, str) and v:
            return v
    return ""


async def record_train_pins(
    run_id: str,
    node_name: str,
    *,
    operator: str,
    resolved_input: dict[str, Any],
    image: str = "",
) -> Optional[dict[str, Any]]:
    """Stamp reproducibility pins onto the run record at submit time.

    Returns the pins dict (or None when the input carries no dataset
    reference — nothing to reproduce). One entry per node: a re-submit of
    the same node REPLACES its entry, so retries don't accumulate. NEVER
    raises — a pin failure logs and the submit proceeds.
    """
    try:
        resolved_input = dict(resolved_input or {})
        ds = _find_dataset_uri(resolved_input)
        if not ds or not run_id:
            return None
        pins: dict[str, Any] = {
            "node": node_name,
            "operator": operator,
            "pinned_at": datetime.now(timezone.utc).isoformat(),
            "dataset_uri": ds,
            "dataset_hash": dataset_hash(ds),
            "image": image or "",
            "image_digest": (await resolve_image_digest(image)) if image else "",
            "spec": resolved_input,
        }
        from cairn.core.storage import get_storage  # noqa: PLC0415 — keep the storage stack off the operator import path (runtime.py convention)

        store = get_storage().runs
        run = await store.get(run_id)
        if run is None:
            _log.warning("train pins: run %r not in the run store — pins not recorded", run_id)
            return pins
        existing = [
            p for p in (getattr(run, "train_pins", []) or [])
            if not (isinstance(p, dict) and p.get("node") == node_name)
        ]
        await store.update(run_id, train_pins=[*existing, pins])
        return pins
    except Exception:  # noqa: BLE001 — pins must NEVER fail or delay a submit
        _log.warning("train pins for run %r failed (submit unaffected)", run_id, exc_info=True)
        return None
