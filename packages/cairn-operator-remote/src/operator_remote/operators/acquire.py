"""compute/acquire — turn a declared box ON, under governance (#1787, ADR-0097).

`risk = paid_compute`. This is the operator that spends money, and the whole
point of #1787 is that the spend now happens *behind a leg* instead of behind a
shell prompt: an ordinary ``gate:`` step in front of it is the authorization, and
the approver + the cost shown land on the pipeline record like any other gate.

No new lifecycle logic lives here. Every EC2 call goes through
:mod:`cairn.core.compute_lifecycle`, which is also what ``compute/release`` and
``cairn compute sweep`` call — one function three callers, so "what starting a
box means" cannot drift between them (#1376). The pack reaches it through the
operator contract's re-exports (``cairn.core.operator``, ADR-0120 D7), never by
importing the core module.

Public since ADR-0151 D6 (#201): moved from the platform's ``cairn-operator-compute``
0.1.0 with the same ref, schemas, risk and capabilities.

It takes a CONNECTION NAME, not an instance id, and resolves the connection
itself rather than reading ``ctx.compute``: a pipeline task leg carries no
compute binding (#1459), and naming the box you are asking to power on is the
input a human approves at the gate anyway.
"""
from __future__ import annotations

import datetime as _dt

from cairn.core.operator import (
    BILLING_STATES,
    describe_state,
    ec2_client,
    iso,
    load_box,
    stamp_lease,
    start_instance,
    wait_port_open,
)
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator

from operator_remote.schemas.lifecycle import AcquireInput, AcquireOutput


def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


@register_operator(name="compute/acquire", version="0.1.0",
                   capabilities=["compute.acquire"])
class ComputeAcquireOp(Operation[AcquireInput, AcquireOutput]):
    """Start the instance a ``compute.ssh`` connection names; wait for its SSH
    port; stamp the lease. Returns the target downstream legs bind."""

    input_schema = AcquireInput
    output_schema = AcquireOutput
    supports_mock = True
    #: paid_compute — this is the call that starts billing. Declared on the class
    #: AND in pack.toml (CLAUDE.md rule 7): the class default is what the
    #: bridge's restart policy reads, and a `read_only` default there would mean
    #: a restarted control plane silently re-started a paid box.
    risk = "paid_compute"
    # ADR-0103 (#2035) — the two waits worth showing: powering the instance
    # on, then waiting for SSH to answer.
    reports = ReportSpec(stages=("start", "wait_ssh"), scalars=("ssh_ready", "already_running"))

    async def run(self, inp: AcquireInput, ctx: OperationContext) -> AcquireOutput:
        box = await load_box(inp.connection)
        instance_id = box.require_instance()
        client = ec2_client(box.region)

        ctx.report.stage("start", percent=0.0)
        before = await describe_state(instance_id, region=box.region, client=client)
        already = before in BILLING_STATES
        started_at = _utcnow()

        release_after = ""
        if inp.max_hours > 0:
            release_after = iso(started_at + _dt.timedelta(hours=inp.max_hours))
        # ADR-0151 D8 — the lease is stamped BEFORE the start. `cairn compute
        # sweep` finds boxes by the `cairn:managed` tag, so a control plane that
        # died between a start and a later stamp would leave a billing box nothing
        # can see. Tag first: a crash between the two calls leaves a tagged box
        # that is not running, which costs nothing. Stamped even when max_hours
        # is 0 — the managed tag scopes the sweep, and a box cairn turned on
        # should say so regardless of whether it carries a deadline.
        await stamp_lease(instance_id, region=box.region, client=client,
                          release_after=release_after or None,
                          acquired_by=inp.acquired_by)
        state = await start_instance(instance_id, region=box.region, client=client)
        ctx.report.stage("start", percent=100.0)

        ctx.report.stage("wait_ssh", percent=0.0)
        ready = await wait_port_open(box.host, box.port, timeout_s=inp.wait_ready_s)
        ctx.report.stage("wait_ssh", percent=100.0)
        if not ready and inp.wait_ready_s > 0:
            note = (f"started, but {box.host}:{box.port} did not accept within "
                    f"{inp.wait_ready_s:.0f}s — the box may still be booting")
        elif already:
            note = "already running — start was a no-op"
        else:
            note = "started and reachable"

        ctx.report.scalars(ssh_ready=ready, already_running=already)
        return AcquireOutput(
            target=inp.connection,
            instance_id=instance_id,
            host=box.host,
            state=state or before,
            hourly_usd=box.hourly_usd,
            started_at=iso(started_at),
            release_after=release_after,
            ssh_ready=ready,
            already_running=already,
            note=note,
        )

    async def mock(self, inp: AcquireInput, ctx: OperationContext) -> AcquireOutput:
        """Mock mode never touches AWS — the acquire is the one call in this pack
        that costs money, so `--mock` has to be a hard stop, not a fast path."""
        now = _utcnow()
        return AcquireOutput(
            target=inp.connection, instance_id="i-mock", host="mock.invalid",
            state="running", hourly_usd=0.0, started_at=iso(now),
            release_after=(iso(now + _dt.timedelta(hours=inp.max_hours))
                           if inp.max_hours > 0 else ""),
            ssh_ready=True, already_running=False,
            note="mocked — no EC2 call was made",
        )
