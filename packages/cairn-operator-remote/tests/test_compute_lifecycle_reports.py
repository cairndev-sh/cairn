"""ADR-0103 (#2035) — compute/acquire's ``reports`` declaration must not be
decorative. Runs the REAL engine path with a strict reporter (via
``assert_reports_conform``), so an undeclared name would raise. Reuses this
pack's fake-EC2 seam (``tests/test_compute_lifecycle.py``) — no AWS call is made.
"""
from __future__ import annotations

import pytest

from cairn.core import compute_lifecycle as cl
from cairn.core.connections import ResolvedConnection
from cairn.core.operator.testing import assert_reports_conform
from operator_remote.operators.acquire import ComputeAcquireOp

BOX = {"host": "10.0.0.9", "port": "22", "instance_id": "i-abc123",
       "region": "eu-central-1", "hourly_usd": "0.66"}


class _FakeEC2:
    def __init__(self, state: str = "stopped") -> None:
        self.state = state
        self.tags: dict[str, str] = {}

    def describe_instances(self, **kw):
        return {"Reservations": [{"Instances": [
            {"InstanceId": "i-abc123", "State": {"Name": self.state},
             "Tags": [{"Key": k, "Value": v} for k, v in self.tags.items()]}]}]}

    def start_instances(self, **kw):
        self.state = "running"
        return {"StartingInstances": [{"CurrentState": {"Name": "running"}}]}

    def create_tags(self, **kw):
        for t in kw.get("Tags") or []:
            self.tags[t["Key"]] = t["Value"]


@pytest.fixture(autouse=True)
def _seams(monkeypatch):
    fake = _FakeEC2()
    monkeypatch.setattr("operator_remote.operators.acquire.ec2_client", lambda region="": fake)
    monkeypatch.setattr(cl, "ec2_client", lambda region="": fake)

    async def _resolve(name, **kw):
        return ResolvedConnection(name=name, kind="compute.ssh", config=dict(BOX))

    monkeypatch.setattr("cairn.core.connections.resolve_connection", _resolve)

    async def _ok(host, port, *, timeout_s, interval_s=5.0):
        return True

    monkeypatch.setattr(cl, "wait_port_open", _ok)
    monkeypatch.setattr("operator_remote.operators.acquire.wait_port_open", _ok)


async def test_acquire_emits_every_declared_name():
    await assert_reports_conform(
        ComputeAcquireOp, {"connection": "gpu", "max_hours": 2}, mocked=False,
    )
