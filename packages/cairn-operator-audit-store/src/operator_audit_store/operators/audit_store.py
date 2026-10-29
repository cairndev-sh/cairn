"""audit/store — zero-dep hash-chained append-only audit sink (ADR-0028 builtin).

The storage lives HERE, in the operator pack — core holds only the ``AuditSink``
interface + ``AuditEvent`` + the emission seams (`cairn.core.obs.audit`). This
class is dual-purpose:

  - ``record(event)`` — the service path. ``get_audit_sink()`` resolves this
    operator by name (``audit/store``), instantiates it once, and the seams
    (LLM gateway / MCP boundary / action handler) call ``record`` per event.
  - ``run(inp, ctx)``  — the node path, so a template ``operation`` can write an
    audit row explicitly.

Tamper-evidence by hash-chain: each row carries ``prev_hash`` + ``hash`` where
``hash = sha256(prev_hash + canonical(event))``. Altering any row breaks every
later link. Every append re-reads the head from the file under a file lock, so
one log has one chain however many writers share it. We reuse the ``events.jsonl`` file convention (ADR-0028: do NOT
build a new audit store); richer sinks (`operator-audit-otel`, `-immudb`,
`-siem`) ship later and swap the binding.
"""
from __future__ import annotations
from cairn.core.operator import getenv

import asyncio
import contextlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from cairn.core.operator import AuditEvent, GENESIS_HASH, chain_hash
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator

from operator_audit_store.schemas.audit_store import (
    AuditWriteInput,
    AuditWriteOutput,
)

logger = logging.getLogger(__name__)


def _default_path() -> Path:
    """Override with ``CAIRN_AUDIT_LOG``; defaults to ``audit.jsonl`` (CWD)."""
    return Path(getenv("CAIRN_AUDIT_LOG", "audit.jsonl"))


_TAIL_CHUNK = 8192
_LOCK_WAIT_S = 30.0

try:  # POSIX
    import fcntl as _fcntl
except ImportError:  # pragma: no cover — Windows: fall back to a sidecar lock file
    _fcntl = None


@contextlib.contextmanager
def _exclusive(f, path: Path):
    """Hold an exclusive cross-process lock for the duration of one append.

    POSIX: ``flock`` on the log's own descriptor. Elsewhere: a ``<log>.lock``
    sidecar created with ``O_EXCL``. A file system that refuses ``flock``
    (some network mounts) is logged and the append proceeds unlocked, as before.
    """
    if _fcntl is not None:
        try:
            _fcntl.flock(f.fileno(), _fcntl.LOCK_EX)
        except OSError as exc:
            logger.warning("audit log %s: file locking unavailable (%s); appends from "
                           "several processes may not share one chain", path, exc)
            yield
            return
        try:
            yield
        finally:
            _fcntl.flock(f.fileno(), _fcntl.LOCK_UN)
        return
    lock = path.with_name(path.name + ".lock")  # pragma: no cover
    deadline = time.monotonic() + _LOCK_WAIT_S  # pragma: no cover
    while True:  # pragma: no cover
        try:
            os.close(os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
            break
        except FileExistsError:
            if time.monotonic() > deadline:
                raise TimeoutError(f"audit log lock {lock} held for over {_LOCK_WAIT_S:.0f}s")
            time.sleep(0.02)
    try:  # pragma: no cover
        yield
    finally:  # pragma: no cover
        with contextlib.suppress(OSError):
            os.unlink(lock)


def _tail_state(f) -> tuple[str, bool]:
    """Read the chain head from the END of the open log without scanning it.

    Returns ``(last_hash, ends_with_newline)``. Walks backwards in chunks to the
    last parseable row; a corrupt line is skipped (the chain continues from the
    last good row), an empty file starts at genesis.
    """
    size = f.seek(0, os.SEEK_END)
    if size == 0:
        return GENESIS_HASH, True
    f.seek(size - 1)
    ends_nl = f.read(1) == b"\n"
    pos, buf = size, b""
    while pos > 0:
        step = min(_TAIL_CHUNK, pos)
        pos -= step
        f.seek(pos)
        buf = f.read(step) + buf
        lines = buf.split(b"\n")
        # lines[0] may be a partial line unless we reached the start of the file
        complete = lines if pos == 0 else lines[1:]
        for raw in reversed(complete):
            raw = raw.strip()
            if not raw:
                continue
            try:
                h = json.loads(raw).get("hash")
            except Exception:  # noqa: BLE001 — skip a corrupt row, keep going
                continue
            if isinstance(h, str) and h:
                return h, ends_nl
    logger.warning("audit log has no readable chain head; starting a new chain")
    return GENESIS_HASH, ends_nl


class _HashChainLog:
    """Append-only JSONL with a sha256 hash-chain over canonicalized rows.

    One chain head across writers: every append takes an exclusive file lock and
    reads the last row's hash from the file itself, so any number of instances,
    in any number of processes, extend the same chain. Within an instance an
    ``asyncio.Lock`` keeps concurrent seams from queueing threads needlessly.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()

    def _append_row(self, payload: dict) -> str:
        """Blocking locked append — run via asyncio.to_thread so the audit write
        (fired on every governed op) never stalls the event loop (#86)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a+b") as f:
            with _exclusive(f, self._path):
                prev, ends_nl = _tail_state(f)
                h = chain_hash(prev, payload)
                row = {**payload, "prev_hash": prev, "hash": h}
                data = ("" if ends_nl else "\n") + json.dumps(row, default=str) + "\n"
                f.write(data.encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
        return h

    async def append(self, payload: dict) -> str:
        async with self._lock:
            return await asyncio.to_thread(self._append_row, payload)


@register_operator(
    name="audit/store",
    version="0.1.1",
    capabilities=["audit.sink"],
    implements=["contract://audit.sink/1.0.0"],  # #2242 V1-B — cairn-contracts-audit
)
class AuditStoreOp(Operation[AuditWriteInput, AuditWriteOutput]):
    """Hash-chained append-only audit sink. Backs the substrate ``audit.sink``."""

    input_schema = AuditWriteInput
    output_schema = AuditWriteOutput
    supports_mock = True
    # ADR-0103: one append per call, nothing long-running to report. Stated,
    # never left out — the worked example for the rest of the packs.
    reports = ReportSpec.NONE

    def __init__(self) -> None:
        # The head lives in the file, not here: any number of instances share one chain.
        self._log = _HashChainLog(_default_path())

    # ── service path (called by obs.audit.get_audit_sink) ──────────────
    async def record(self, event: AuditEvent) -> None:
        await self._log.append(event.to_dict())

    # ── node path (usable as a template `operation`) ──────────────────
    async def run(self, inp: AuditWriteInput, ctx: OperationContext) -> AuditWriteOutput:
        # stamp the active workspace so node-authored rows are workspace-scoped
        # too (#288) — the service path already stamps it via audit_emit.
        from cairn.core.operator import current_workspace

        event = AuditEvent(
            action=inp.action,
            outcome=inp.outcome,
            actor=inp.actor,
            target=inp.target,
            run_id=inp.run_id,
            workspace_id=current_workspace(),
            # #1233 — honor the schema's contract ("filled by the sink if
            # blank"): templates never pass ts, so node-authored rows landed
            # with ts="" and the console audit WHEN column rendered "—". The
            # service path (audit_emit) already stamps at emission.
            ts=inp.ts or datetime.now(timezone.utc).isoformat(),
            metadata=inp.metadata,
        )
        h = await self._log.append(event.to_dict())
        return AuditWriteOutput(ok=True, hash=h)

    def mock(self, inp: AuditWriteInput, ctx: OperationContext) -> AuditWriteOutput:
        return AuditWriteOutput(ok=True, hash="")
