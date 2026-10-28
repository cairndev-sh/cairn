"""compute/release — turn the box OFF, idempotently (#1787, ADR-0097).

The half that has actually been failing: a box left billing after a run has
happened repeatedly and was caught only by human discipline. In a managed
pipeline this operator is declared ``finalizer: true``, so the bridge runs it
when the pipeline reaches ANY terminal state — done, failed, or cancelled — the
way a ``finally:`` block runs. That is the engine behaviour ADR-0097 adds; this
operator is the ordinary task leg it runs.

**Risk class: `external_write`, and `requires_approval = False` — deliberately.**
The taxonomy's higher classes (`production_mutation`, `paid_compute`) both route
to `high` in the approval resolver, which is where the environment's HITL
policies bite. Sending the release down that path would mean the teardown can be
*paused behind an unanswered gate* — a box billing while it waits for a human is
precisely the failure this issue exists to end. So: the class records honestly
that this mutates external cloud state (`external_write`), and it is never
approval-gated, because the direction of the mutation is *stop spending*.

**Idempotent by contract.** A finalizer may run more than once — a control plane
that dies mid-release re-submits it on adoption. Stopping an instance that is
already stopped is reported as `already_stopped: true` and is a SUCCESS. If it
raised, a successful teardown would come back as a failed pipeline.

Public since ADR-0151 D6 (#201): moved from the platform's ``cairn-operator-compute``
0.1.0 unchanged; the lifecycle calls come through the operator contract's
re-exports (``cairn.core.operator``, ADR-0120 D7).
"""
from __future__ import annotations

import datetime as _dt

from cairn.core.operator import (
    STOPPED_STATES,
    describe_state,
    ec2_client,
    iso,
    load_box,
    stop_instance,
)
from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator

from operator_remote.schemas.lifecycle import ReleaseInput, ReleaseOutput


@register_operator(name="compute/release", version="0.1.0",
                   capabilities=["compute.release"])
class ComputeReleaseOp(Operation[ReleaseInput, ReleaseOutput]):
    """Stop the instance a ``compute.ssh`` connection names. No-op success when
    it is already stopped."""

    input_schema = ReleaseInput
    output_schema = ReleaseOutput
    supports_mock = True
    risk = "external_write"
    requires_approval = False
    reports = ReportSpec.NONE  # ADR-0103: one idempotent teardown call, nothing to report

    async def run(self, inp: ReleaseInput, ctx: OperationContext) -> ReleaseOutput:
        box = await load_box(inp.connection)
        instance_id = (inp.instance_id or "").strip() or box.require_instance()
        client = ec2_client(box.region)
        now = iso(_dt.datetime.now(_dt.timezone.utc))

        state = await describe_state(instance_id, region=box.region, client=client)
        if state in STOPPED_STATES:
            return ReleaseOutput(
                instance_id=instance_id, state=state, stopped=False,
                already_stopped=True, stopped_at=now,
                note=f"already {state} — nothing to release",
            )
        after = await stop_instance(instance_id, region=box.region, client=client)
        return ReleaseOutput(
            instance_id=instance_id, state=after or state, stopped=True,
            already_stopped=False, stopped_at=now,
            note="stop requested (Elastic IP + EBS persist)",
        )

    async def mock(self, inp: ReleaseInput, ctx: OperationContext) -> ReleaseOutput:
        return ReleaseOutput(
            instance_id="i-mock", state="stopped", stopped=False,
            already_stopped=True,
            stopped_at=iso(_dt.datetime.now(_dt.timezone.utc)),
            note="mocked — no EC2 call was made",
        )
