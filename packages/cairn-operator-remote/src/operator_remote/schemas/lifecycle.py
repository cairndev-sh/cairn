"""Schemas for the compute LIFECYCLE pair — compute/acquire + compute/release
(#1787, ADR-0097; public in this pack since ADR-0151 D6, #201).

``remote/exec`` runs work on a ``compute.ssh`` box; these two turn the box on and
off. The fields are the platform's ``cairn-operator-compute`` 0.1.0 contract,
unchanged. Both take a CONNECTION NAME rather than an instance id: the instance
id, region and hourly price live on the ``compute.ssh`` connection, so a pipeline
says *which box* and the connection says *how to reach and bill it* — the same
split every other cairn leg uses (packs say what, connections/environments say
where).
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class AcquireInput(BaseModel):
    connection: str = Field(
        ...,
        description="Name of the `compute.ssh` connection to power on. Its "
                    "`instance_id`/`region`/`hourly_usd` config carries the rest.",
    )
    max_hours: float = Field(
        0.0, ge=0,
        description="Lease length. >0 stamps a `cairn:release-after` deadline tag on "
                    "the instance, which `cairn compute sweep` enforces from ANY host "
                    "with credentials — so the box does not outlive a crashed control "
                    "plane. 0 = no lease stamped (the box is then only released by the "
                    "pipeline's own release finalizer).",
    )
    wait_ready_s: float = Field(
        600.0, ge=0,
        description="Bounded wait for the SSH port to accept after start. Expiry is "
                    "reported (`ssh_ready: false`), not raised: a box that booted "
                    "slowly is a fact the next leg will hit with its own error.",
    )
    acquired_by: str = Field(
        "", description="Recorded in the `cairn:acquired-by` tag — the pipeline id, "
                        "so an unexpected running box names what turned it on.",
    )


class AcquireOutput(BaseModel):
    target: str = Field(
        "", description="The compute target downstream legs bind: the connection name.",
    )
    instance_id: str = Field("", description="The EC2 instance that was started.")
    host: str = Field("", description="The connection's host (stable across stop/start "
                                      "when it carries an Elastic IP).")
    state: str = Field("", description="EC2 state after the start call.")
    hourly_usd: float = Field(
        0.0, description="$/hr this box bills while up, from the connection's "
                         "`hourly_usd`. 0.0 = not declared — an authorization gate "
                         "then shows the estimate's price rather than a number this "
                         "operator invented.",
    )
    started_at: str = Field("", description="When the start was issued (ISO-8601 UTC).")
    release_after: str = Field(
        "", description="The lease deadline stamped on the instance; empty when "
                        "`max_hours` was 0.",
    )
    ssh_ready: bool = Field(
        False, description="Did the SSH port accept within `wait_ready_s`?")
    already_running: bool = Field(
        False, description="The box was already up — start was a no-op.")
    note: str = Field("", description="Human-readable detail for the audit trail.")


class ReleaseInput(BaseModel):
    connection: str = Field(
        ..., description="Name of the `compute.ssh` connection to power off.",
    )
    instance_id: str = Field(
        "", description="Override the connection's instance id (rarely needed).",
    )


class ReleaseOutput(BaseModel):
    instance_id: str = Field("", description="The instance the stop applied to.")
    state: str = Field("", description="EC2 state after the call.")
    stopped: bool = Field(False, description="A stop was issued by this call.")
    already_stopped: bool = Field(
        False, description="Nothing to do — the instance was already stopped/stopping. "
                           "This is a SUCCESS: a finalizer may run more than once "
                           "across a control-plane restart, so release is idempotent "
                           "by contract (ADR-0097).",
    )
    stopped_at: str = Field("", description="When the stop was issued (ISO-8601 UTC).")
    note: str = Field("", description="Human-readable detail for the audit trail.")
