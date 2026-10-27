"""The compute LIFECYCLE seam: turn a declared box on, turn it off, sweep the
ones whose lease expired (#1787, ADR-0097).

Until this module existed, every GPU session in the 08-26→30 campaign started and
stopped its box by hand with a shell script, and "GPU only with
authorization" was a rule that lived in the operator's memory rather than in the
platform. The 40 lines that script proves (``aws ec2 start-instances`` /
``stop-instances`` / ``describe-instances``) move here as boto3 — a *base*
dependency, so nothing new is installed — and everything else the issue asks for
is composition over machinery that already shipped: the gate is an ordinary
``gate:`` leg, the release is an ordinary task leg the bridge runs as a
``finalizer:``.

**One module, three callers.** ``compute/acquire`` and ``compute/release`` (the
builtin ``cairn-operator-remote`` pack, ``operator_remote.operators``, since
ADR-0151 D6; they reach these functions through the operator contract's
re-exports in :mod:`cairn.core.operator`, ADR-0120 D7) and ``cairn compute sweep``
(the CLI) all call the functions here. That is deliberate — #1376's rule: a
chokepoint is a FUNCTION others call, not a
convention. If the CLI carried its own copy of the stop logic, the sweep and the
release could disagree about what "stopped" means, and the disagreement would
only surface as a bill.

**The seam.** :func:`ec2_client` is the ONE place a boto3 client is built, and
the one thing tests replace. It honours the same env the rest of the codebase
does (``AWS_ENDPOINT_URL`` / ``AWS_DEFAULT_REGION``), exactly like
``operator_training.checkpoint_sync.s3_client``. ``CAIRN_EC2_CLIENT_FACTORY``
("module:callable") lets a *developer* point it at a fake without patching — how
the #1787 live drive proved the gate/finalizer wiring with no AWS call at all.
It is a test/dev seam and is documented as one: production leaves it unset.

**What this module deliberately does NOT do**: provision, terminate, resize, or
create anything. It starts and stops instances that already exist and that a
``compute.ssh`` connection already names. Provision-per-run (cattle) is served by
the ``train/runpod|vast|modal`` remote-job operators that already shipped, and
SkyPilot stays available as a later rung (ADR-0097 §Buy-vs-build) — this is the
thin pet-box half, and it is thin on purpose.
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import os
from typing import Any, Optional

logger = logging.getLogger("cairn.core.compute_lifecycle")

#: The deadline tag stamped at acquire — an ISO-8601 UTC instant after which the
#: box must not still be running. It rides on the INSTANCE rather than in our
#: database on purpose (#1787 criterion 5): the guard has to survive the control
#: plane, so the fact that the lease exists must live where AWS keeps it, not
#: where our process keeps it.
TAG_RELEASE_AFTER = "cairn:release-after"
#: Who/what acquired it — the pipeline id when a pipeline did, else the caller.
TAG_ACQUIRED_BY = "cairn:acquired-by"
#: Marks the instance as under cairn's lifecycle management, so the sweep can
#: scope itself to boxes cairn turned on and never touch a neighbour's.
TAG_MANAGED = "cairn:managed"

#: EC2 states that mean "this instance is costing money right now".
BILLING_STATES = frozenset({"pending", "running", "rebooting"})
#: EC2 states that mean "already off" — releasing one of these is a no-op success.
STOPPED_STATES = frozenset({"stopping", "stopped", "shutting-down", "terminated"})


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def iso(ts: _dt.datetime) -> str:
    """The ONE timestamp spelling used in tags and operator output."""
    return ts.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(raw: str) -> Optional[_dt.datetime]:
    """Parse a tag timestamp, tolerating the ``Z`` suffix; ``None`` if unusable.

    Unparseable is deliberately NOT treated as expired: a tag we cannot read is
    a tag someone else may own, and the sweep must never stop a box on a guess.
    """
    text = (raw or "").strip()
    if not text:
        return None
    try:
        dt = _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=_dt.timezone.utc)


class ComputeLifecycleError(RuntimeError):
    """A lifecycle action that cannot proceed (no instance id, no creds, …)."""


# ─── the boto3 seam ──────────────────────────────────────────────────────


def _factory_from_env():
    """``CAIRN_EC2_CLIENT_FACTORY='pkg.mod:callable'`` → the callable, or None.

    A dev/test seam, not a configuration knob: it exists so a local drive of a
    pipeline that acquires compute can prove the GOVERNANCE (gate pauses,
    finalizer fires) without an AWS call. Unset in production, where it costs
    one absent env read.
    """
    raw = os.environ.get("CAIRN_EC2_CLIENT_FACTORY", "").strip()
    if not raw:
        return None
    mod_name, _, attr = raw.partition(":")
    if not mod_name or not attr:
        raise ComputeLifecycleError(
            f"CAIRN_EC2_CLIENT_FACTORY={raw!r} is not 'module:callable'")
    import importlib  # noqa: PLC0415 - only reached when the dev seam is set

    return getattr(importlib.import_module(mod_name), attr)


def ec2_client(region: str = "") -> Any:
    """A boto3 EC2 client for ``region`` (else ``AWS_DEFAULT_REGION``).

    THE named seam. Mirrors ``operator_training.checkpoint_sync.s3_client``:
    endpoint + region come from the environment the operator injection already
    sets, so a fake endpoint (moto/localstack) works with no code change.
    """
    factory = _factory_from_env()
    if factory is not None:
        return factory(region or os.environ.get("AWS_DEFAULT_REGION") or "")
    import boto3  # noqa: PLC0415 - base dep; deferred so the seam above never needs it

    return boto3.client(
        "ec2",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,
        region_name=region or os.environ.get("AWS_DEFAULT_REGION") or None,
    )


# ─── connection → lifecycle facts ────────────────────────────────────────


class ComputeBox:
    """The lifecycle facts a ``compute.ssh`` connection carries (#1787).

    Deliberately a small value object rather than another pydantic model: it is
    read from ``Connection.config``, which is ``dict[str, Any]`` by design, and
    every field here is optional on the connection.
    """

    __slots__ = ("name", "host", "port", "instance_id", "region", "hourly_usd")

    def __init__(self, name: str, config: dict) -> None:
        self.name = name
        self.host = str(config.get("host") or "")
        self.port = int(config.get("port") or 22)
        self.instance_id = str(config.get("instance_id") or "").strip()
        self.region = str(config.get("region") or "").strip()
        raw = str(config.get("hourly_usd") or "").strip()
        try:
            self.hourly_usd = float(raw) if raw else 0.0
        except ValueError:
            self.hourly_usd = 0.0

    def require_instance(self) -> str:
        if not self.instance_id:
            raise ComputeLifecycleError(
                f"compute connection {self.name!r} has no `instance_id` — the "
                f"lifecycle operators start and stop an instance that already "
                f"exists, so they need its id. Set it with: "
                f"`cairn connect ssh --name {self.name} --set instance_id=i-…"
                f" --set region=<aws-region>`"
            )
        return self.instance_id


async def load_box(name: str, *, account_id: Optional[str] = None) -> ComputeBox:
    """Resolve a connection NAME to its lifecycle facts, in ``account_id`` (when
    omitted, the current run's account — see ``resolve_connection``).

    By name rather than off ``ctx.compute`` because a pipeline TASK leg carries
    no compute binding (#1459) — the acquire leg's whole job is to name the box
    it is being asked to turn on.
    """
    # lazy: keep the connections/secret stack off this module's import path,
    # which the CLI loads for `cairn compute sweep`.
    from cairn.core.connections import resolve_connection  # noqa: PLC0415

    conn = await resolve_connection(name, account_id=account_id)
    if conn is None:
        raise ComputeLifecycleError(
            f"compute connection {name!r} did not resolve to an enabled "
            f"connection — add it with `cairn connect ssh --name {name}`")
    if conn.kind != "compute.ssh":
        raise ComputeLifecycleError(
            f"connection {name!r} is kind {conn.kind!r}; the lifecycle "
            f"operators manage `compute.ssh` boxes (per-job cloud GPUs are "
            f"served by train/runpod|vast|modal)")
    return ComputeBox(name, dict(conn.config or {}))


# ─── EC2 actions (each one call, each wrapped off the event loop) ────────


async def describe_state(instance_id: str, *, region: str = "",
                         client: Any = None) -> str:
    """The instance's current EC2 state name, or ``""`` when it cannot be read."""
    c = client or ec2_client(region)

    def _call() -> str:
        resp = c.describe_instances(InstanceIds=[instance_id])
        for res in resp.get("Reservations") or []:
            for inst in res.get("Instances") or []:
                return str((inst.get("State") or {}).get("Name") or "")
        return ""

    return await asyncio.to_thread(_call)


async def wait_port_open(host: str, port: int, *, timeout_s: float,
                         interval_s: float = 5.0) -> bool:
    """Bounded wait until ``host:port`` accepts a TCP connection.

    The readiness signal is the SSH port accepting, which is what
    that script's ``wait_ready`` loop measured too (it shelled out to
    ssh; this opens the socket). It is deliberately not an authenticated SSH
    handshake: the private key lives in the SecretStore and belongs to the SSH
    engine, and a lifecycle operator that needed it would be a second place
    credentials are read. A box whose port is open but whose auth is broken is a
    connection problem, and it fails on the leg that actually uses it, with that
    leg's error.
    """
    if not host:
        return False
    deadline = asyncio.get_running_loop().time() + max(0.0, timeout_s)
    while True:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=min(10.0, max(1.0, interval_s)))
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 — the probe succeeded; closing is best-effort
                pass
            del reader
            return True
        except Exception:  # noqa: BLE001 — not up yet is the normal case here
            pass
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(interval_s)


async def stamp_lease(instance_id: str, *, region: str = "", client: Any = None,
                      release_after: Optional[str] = None,
                      acquired_by: str = "") -> None:
    """Write the lease tags onto the instance.

    This is the ``max_hours`` guard's durable half (#1787 criterion 5): the
    deadline lives on the instance, so a control plane that dies leaves the
    evidence behind for ``cairn compute sweep`` (or a human reading the console)
    to act on. It is NOT cloud-side ENFORCEMENT on its own — see
    :func:`sweep_expired` and the PR/ADR, which say so plainly rather than
    claiming a guard that was not built.
    """
    c = client or ec2_client(region)
    tags = [{"Key": TAG_MANAGED, "Value": "cairn"}]
    if release_after:
        tags.append({"Key": TAG_RELEASE_AFTER, "Value": release_after})
    if acquired_by:
        tags.append({"Key": TAG_ACQUIRED_BY, "Value": acquired_by})
    await asyncio.to_thread(c.create_tags, Resources=[instance_id], Tags=tags)


async def start_instance(instance_id: str, *, region: str = "",
                         client: Any = None) -> str:
    """Start the instance; returns the state EC2 reports for it afterwards.

    Idempotent by EC2's own contract — ``StartInstances`` on a running instance
    is a no-op that returns its current state.
    """
    c = client or ec2_client(region)
    resp = await asyncio.to_thread(c.start_instances, InstanceIds=[instance_id])
    for inst in resp.get("StartingInstances") or []:
        return str((inst.get("CurrentState") or {}).get("Name") or "")
    return ""


async def stop_instance(instance_id: str, *, region: str = "",
                        client: Any = None) -> str:
    """Stop the instance; returns the state EC2 reports for it afterwards.

    Idempotent the same way: stopping an already-stopped instance succeeds and
    reports ``stopped``. That property is load-bearing — the bridge may run a
    finalizer more than once across a restart, and a release that raised on the
    second call would turn a successful teardown into a failed pipeline.
    """
    c = client or ec2_client(region)
    resp = await asyncio.to_thread(c.stop_instances, InstanceIds=[instance_id])
    for inst in resp.get("StoppingInstances") or []:
        return str((inst.get("CurrentState") or {}).get("Name") or "")
    return ""


# ─── the watchdog: one sweep over expired leases ─────────────────────────


async def sweep_expired(*, region: str = "", apply: bool = False,
                        client: Any = None, now: Optional[_dt.datetime] = None,
                        ) -> list[dict[str, Any]]:
    """Every cairn-managed instance whose lease has expired, optionally stopped.

    Scoped by the ``cairn:managed`` tag *and* by a readable ``cairn:release-after``
    in the past, so it can never stop a box cairn did not turn on and never acts
    on a tag it could not parse. ``apply=False`` (the default) REPORTS and
    changes nothing — the shape a human runs first and a cron runs with
    ``--apply``.

    Honest scope: this is a *watchdog*, not cloud-side enforcement. It runs when
    something runs it. What it buys over the status quo is that the guard no
    longer depends on the pipeline's own control plane surviving — any host with
    the credentials can enforce every lease. True instance-side enforcement (an
    EventBridge one-shot schedule targeting ``ec2:StopInstances``, or an
    ``arn:aws:automate:…:ec2:stop`` alarm action) needs an IAM role created in
    the owner's account, which is exactly the kind of thing this PR must not do
    on its own — it is stated as the next rung, not shipped as done.
    """
    c = client or ec2_client(region)
    at = now or _now()

    def _describe() -> list[dict]:
        resp = c.describe_instances(Filters=[
            {"Name": f"tag:{TAG_MANAGED}", "Values": ["cairn"]},
            {"Name": "instance-state-name", "Values": sorted(BILLING_STATES)},
        ])
        out: list[dict] = []
        for res in resp.get("Reservations") or []:
            out.extend(res.get("Instances") or [])
        return out

    rows: list[dict[str, Any]] = []
    for inst in await asyncio.to_thread(_describe):
        iid = str(inst.get("InstanceId") or "")
        tags = {str(t.get("Key")): str(t.get("Value"))
                for t in (inst.get("Tags") or [])}
        deadline = parse_iso(tags.get(TAG_RELEASE_AFTER, ""))
        state = str((inst.get("State") or {}).get("Name") or "")
        if deadline is None:
            rows.append({"instance_id": iid, "state": state, "release_after": "",
                         "expired": False, "stopped": False,
                         "note": "no readable cairn:release-after — left alone"})
            continue
        if deadline > at:
            rows.append({"instance_id": iid, "state": state,
                         "release_after": iso(deadline), "expired": False,
                         "stopped": False,
                         "note": f"lease valid for another "
                                 f"{int((deadline - at).total_seconds() // 60)} min"})
            continue
        stopped = False
        note = "EXPIRED — run with --apply to stop it"
        if apply:
            try:
                state = await stop_instance(iid, region=region, client=c) or state
                stopped = True
                note = "expired — stop requested"
            except Exception as e:  # noqa: BLE001 — one bad instance must not end the sweep
                logger.exception("sweep: stopping %s failed", iid)
                note = f"expired — stop FAILED: {type(e).__name__}: {e}"
        rows.append({"instance_id": iid, "state": state,
                     "release_after": iso(deadline), "expired": True,
                     "stopped": stopped, "note": note,
                     "acquired_by": tags.get(TAG_ACQUIRED_BY, "")})
    return rows


__all__ = [
    "BILLING_STATES", "STOPPED_STATES", "TAG_ACQUIRED_BY", "TAG_MANAGED",
    "TAG_RELEASE_AFTER", "ComputeBox", "ComputeLifecycleError", "describe_state",
    "ec2_client", "iso", "load_box", "parse_iso", "stamp_lease", "start_instance",
    "stop_instance", "sweep_expired", "wait_port_open",
]
