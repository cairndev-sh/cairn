"""The SSH primitives a leg needs on a box it does not run on (#1788, ADR-0098).

**Why this module exists, and why it is in core.** ``asyncssh.connect(...)`` with
the compute.ssh key/known_hosts mapping was already written out FOUR times —
``remote/exec``, ``serve/vllm``, the dataflow SSH engine, and
``connection_probe`` — which is exactly the shape #1544 lifted ``as_ssh_params``
out of: one host-key fix meant four edits. #1788 needs a fifth caller
(``serve/start``, which must exec, ship a file, and forward a port), so the
choice was "write the fifth copy" or "lift the primitive". This lifts it.

It is deliberately SMALL — open a connection, run a command, put a file, forward
a port. It is not a job protocol: ``remote/exec``'s submit/poll/adopt semantics
(#337 / #551 / #601 / #603 / #708) stay in that operator, which owns them. What
moved here is only the part every caller had verbatim, plus the two primitives
nobody had yet.

**The named seam is :func:`connect`.** Every function below reaches SSH through
it, so a test (or a bounded local drive) replaces one callable and the whole
module is hermetic. ``CAIRN_SSH_CONNECT_FACTORY='module:callable'`` does the same
without patching — the mirror of ``CAIRN_EC2_CLIENT_FACTORY`` (#1787), a
dev/test seam that production leaves unset.

**Host-key verification is never disabled here.** ``known_hosts=None`` means
asyncssh's default (``~/.ssh/known_hosts``), which REJECTS an unknown host;
``()`` — which would disable checking — is never passed.
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import os
import shlex
from types import SimpleNamespace
from typing import Any, Optional

logger = logging.getLogger("cairn.core.ssh_exec")

# #337: a half-open TCP connection can otherwise hang connect() indefinitely.
# These are the values remote/exec arrived at against a real sshd with per-source
# penalties enabled; they are the reason this is a lift and not a rewrite.
CONNECT_TIMEOUT = 15
LOGIN_TIMEOUT = 15
KEEPALIVE_INTERVAL = 15
KEEPALIVE_COUNT_MAX = 3


class SshExecError(RuntimeError):
    """An SSH primitive could not do what the leg asked."""


def require_ssh() -> None:
    """Fail with an install line rather than an ImportError traceback."""
    try:
        import asyncssh  # noqa: F401, PLC0415 — optional [ssh] extra
    except ImportError as e:  # pragma: no cover — exercised by the no-dep test
        raise SshExecError(
            "this leg needs asyncssh: pip install 'cairndev[ssh]'"
        ) from e


def _factory_from_env():
    """``CAIRN_SSH_CONNECT_FACTORY='pkg.mod:callable'`` → the callable, or None.

    A dev/test seam, not a configuration knob (the #1787 precedent): it exists so
    a local drive of a pipeline that SERVES a model can prove the leg wiring —
    health probe, endpoint output, finalizer teardown — with no box, no GPU and
    no network. Unset in production, where it costs one absent env read.
    """
    raw = os.environ.get("CAIRN_SSH_CONNECT_FACTORY", "").strip()
    if not raw:
        return None
    mod_name, _, attr = raw.partition(":")
    if not mod_name or not attr:
        raise SshExecError(
            f"CAIRN_SSH_CONNECT_FACTORY={raw!r} is not 'module:callable'")
    return getattr(importlib.import_module(mod_name), attr)


async def connect(params: dict) -> Any:
    """Open an asyncssh connection from ``as_ssh_params``-shaped kwargs.

    THE seam. Everything else in this module calls it; nothing else in cairn
    should call ``asyncssh.connect`` for a ``compute.ssh`` box.
    """
    factory = _factory_from_env()
    if factory is not None:
        conn = factory(dict(params))
        return await conn if asyncio.iscoroutine(conn) else conn
    require_ssh()
    import asyncssh  # noqa: PLC0415 — optional [ssh] extra (see require_ssh)

    client_keys = (
        [asyncssh.import_private_key(params["private_key"])]
        if params.get("private_key") else None
    )
    return await asyncssh.connect(
        params["host"], port=int(params.get("port") or 22),
        username=params.get("user"), client_keys=client_keys,
        # None → asyncssh's ~/.ssh/known_hosts, which REJECTS unknown hosts.
        # Never () — that disables host-key checking (MITM risk).
        known_hosts=params.get("known_hosts"),
        connect_timeout=CONNECT_TIMEOUT, login_timeout=LOGIN_TIMEOUT,
        keepalive_interval=KEEPALIVE_INTERVAL,
        keepalive_count_max=KEEPALIVE_COUNT_MAX,
    )


async def ssh_params_for(connection_name: str, *, operator: str) -> dict:
    """Resolve a connection NAME to asyncssh kwargs.

    By NAME, not off ``ctx.compute``, for the #1787 reason: a pipeline ``task:``
    leg carries no compute binding (#1459), so an operator that must reach a box
    takes the box's name as an INPUT — which is also the thing a human approves
    at a gate, and the thing ``$steps.acquire.target`` hands downstream.

    The config→kwargs mapping itself is ``as_ssh_params`` (#1544), not a copy:
    the resolved connection is presented to it the way ``ctx`` would be.
    """
    # lazy: keep the connections/secret stack off this module's import path.
    from cairn.core.connections import (  # noqa: PLC0415
        as_ssh_params, resolve_connection,
    )

    conn = await resolve_connection(connection_name)
    if conn is None:
        raise SshExecError(
            f"{operator}: compute connection {connection_name!r} did not resolve "
            f"to an enabled connection — add it with "
            f"`cairn admin connect add ssh --name {connection_name}`")
    if conn.kind != "compute.ssh":
        raise SshExecError(
            f"{operator}: connection {connection_name!r} is kind {conn.kind!r}; "
            f"this leg runs on a `compute.ssh` box")
    return as_ssh_params(SimpleNamespace(compute=conn), operator=operator)


class ExecResult:
    """What a remote command did. Deliberately not a pydantic model — it never
    crosses an operator boundary, it is read three lines after it is made."""

    __slots__ = ("exit_status", "stdout", "stderr")

    def __init__(self, exit_status: int, stdout: str, stderr: str = "") -> None:
        self.exit_status = int(exit_status)
        self.stdout = stdout or ""
        self.stderr = stderr or ""

    @property
    def ok(self) -> bool:
        return self.exit_status == 0

    def __repr__(self) -> str:
        return (f"ExecResult(exit_status={self.exit_status}, "
                f"stdout={self.stdout[:80]!r})")


async def run(conn: Any, command: str, *, check: bool = False) -> ExecResult:
    """Run one command on an open connection.

    Normalizes asyncssh's result object (and any test fake shaped like it) into
    :class:`ExecResult`, so callers never branch on ``getattr(r, 'exit_status',
    1)`` — the idiom that was copied into every SSH caller in the tree.
    """
    r = await conn.run(command, check=False)
    out = ExecResult(
        int(getattr(r, "exit_status", 0) or 0),
        str(getattr(r, "stdout", "") or ""),
        str(getattr(r, "stderr", "") or ""),
    )
    if check and not out.ok:
        raise SshExecError(
            f"remote command failed (exit {out.exit_status}): {command}\n"
            f"{(out.stderr or out.stdout)[-500:]}")
    return out


async def write_file(conn: Any, remote_path: str, content: str, *,
                     mode: Optional[int] = None) -> None:
    """Write ``content`` to ``remote_path`` over SFTP, creating its directory.

    Used to SHIP a script rather than interpolate it into a command line: the
    hf-shim is ~200 lines of Python, and putting it through ``sh -c`` would make
    every quote in it a remote-execution hazard.
    """
    parent = remote_path.rsplit("/", 1)[0] if "/" in remote_path else "."
    await run(conn, f"mkdir -p {shlex.quote(parent)}", check=True)
    async with conn.start_sftp_client() as sftp:
        async with sftp.open(remote_path, "w") as f:
            await f.write(content)
    if mode is not None:
        await run(conn, f"chmod {mode:o} {shlex.quote(remote_path)}", check=False)


async def forward_local_port(conn: Any, *, listen_host: str, listen_port: int,
                             remote_host: str, remote_port: int) -> Any:
    """``ssh -L`` as an object whose lifetime the caller owns.

    This is the primitive #1788 exists to stop being a human procedure: every
    flagship measurement session had an operator run ``ssh -L`` in a spare
    terminal so the eval legs on the control plane could reach the endpoint on
    the GPU box. The returned listener is closed by :func:`close_listener`.

    ``listen_port=0`` asks the OS for a free port; read it back off the returned
    listener with :func:`listener_port`, because publishing an endpoint URL that
    names a port nothing listens on is worse than not publishing one.
    """
    return await conn.forward_local_port(
        listen_host, int(listen_port), remote_host, int(remote_port))


def listener_port(listener: Any, requested: int) -> int:
    """The port a forwarding listener actually bound.

    asyncssh exposes ``get_port()``; a fake may not. Falling back to what was
    REQUESTED is correct for every non-zero request and honest about the one
    case it cannot answer (an ephemeral bind on a fake), which the caller then
    reports as 0 rather than inventing a number.
    """
    getter = getattr(listener, "get_port", None)
    if callable(getter):
        try:
            return int(getter())
        except Exception:  # noqa: BLE001 — a fake that has the attr but not the answer
            logger.debug("listener.get_port() failed", exc_info=True)
    return int(requested)


def close_listener(listener: Any) -> None:
    """Close a port-forward listener. Never raises — this runs in teardown."""
    if listener is None:
        return
    try:
        closer = getattr(listener, "close", None)
        if callable(closer):
            closer()
    except Exception:  # noqa: BLE001 — teardown must not fail the leg
        logger.debug("closing a port-forward listener failed", exc_info=True)


def close_conn(conn: Any) -> None:
    """Close an SSH connection. Never raises — this runs in teardown."""
    if conn is None:
        return
    try:
        closer = getattr(conn, "close", None)
        if callable(closer):
            closer()
    except Exception:  # noqa: BLE001 — teardown must not fail the leg
        logger.debug("closing an SSH connection failed", exc_info=True)


async def wait_remote_port(conn: Any, port: int, *, timeout_s: float,
                           interval_s: float = 2.0) -> bool:
    """Bounded wait until ``port`` accepts on the REMOTE box's loopback.

    Deliberately not a health check — it only says a socket is listening, which
    is the precondition for opening the tunnel. What makes the leg `done` is the
    real completion probe the operator sends THROUGH that tunnel (#1788
    criterion 2); a port that accepts proves nothing about a model being loaded.
    """
    probe = (f"python3 -c \"import socket,sys; s=socket.socket(); "
             f"s.settimeout(2); sys.exit(s.connect_ex(('127.0.0.1',{int(port)})))\"")
    waited = 0.0
    while waited < timeout_s:
        if (await run(conn, probe)).ok:
            return True
        await asyncio.sleep(interval_s)
        waited += interval_s
    return False
