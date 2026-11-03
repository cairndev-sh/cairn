"""Job-executor lifecycle + drivers (an internal issue).

Fully hermetic — a fake driver/operator proves dispatch → poll → succeed,
**resume (re-poll, never re-dispatch)**, cancel, failure, and timeout, with no
Docker daemon or provider account. The docker driver's argv is unit-tested; its
actual daemon calls are reference impls validated on a host with Docker.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.operator import executor as ex
from cairn.core.operator.jobs import (
    JobFailed,
    JobHandle,
    JobStatus,
    JobTimeout,
    DockerJobDriver,
    RemoteJobDriver,
    build_docker_run_argv,
    is_remote_job_operator,
    run_job_lifecycle,
)


class _FakeDriver:
    """In-memory job: 'running' for `running_polls` polls, then 'succeeded'."""

    backend = "fake"

    def __init__(self, running_polls: int = 1, fail: bool = False) -> None:
        self.running_polls = running_polls
        self.fail = fail
        self.dispatched = 0
        self.cancelled = False
        self._polls = 0

    async def dispatch(self) -> JobHandle:
        self.dispatched += 1
        return JobHandle(job_id="job-1", backend="fake")

    async def poll(self, handle: JobHandle) -> JobStatus:
        if self.fail:
            return JobStatus(state="failed", error="boom")
        self._polls += 1
        if self._polls > self.running_polls:
            return JobStatus(state="succeeded", output={"ok": True})
        return JobStatus(state="running")

    async def cancel(self, handle: JobHandle) -> None:
        self.cancelled = True


# ─── lifecycle ───────────────────────────────────────────────────────


async def test_lifecycle_dispatch_poll_succeed():
    d = _FakeDriver(running_polls=1)
    out = await run_job_lifecycle(d, poll_interval=0)
    assert out == {"ok": True}
    assert d.dispatched == 1


async def test_lifecycle_resume_skips_dispatch():
    """The whole point of durability: a restarted worker re-polls an existing
    handle and NEVER re-dispatches."""
    d = _FakeDriver(running_polls=0)
    existing = JobHandle(job_id="prior", backend="fake")
    out = await run_job_lifecycle(d, poll_interval=0, load_handle=lambda: existing)
    assert out == {"ok": True}
    assert d.dispatched == 0


async def test_lifecycle_persist_records_handle():
    d = _FakeDriver(running_polls=0)
    saved: list[JobHandle] = []
    await run_job_lifecycle(d, poll_interval=0, persist=saved.append)
    assert len(saved) == 1 and saved[0].job_id == "job-1"


async def test_lifecycle_failed_raises():
    with pytest.raises(JobFailed):
        await run_job_lifecycle(_FakeDriver(fail=True), poll_interval=0)


async def test_lifecycle_timeout():
    with pytest.raises(JobTimeout):
        await run_job_lifecycle(_FakeDriver(running_polls=99), poll_interval=0, max_polls=3)


# ─── remote_job driver over a RemoteJobOperator ──────────────────────


class _FakeRemoteOp:
    def __init__(self) -> None:
        self._polls = 0

    async def submit(self, inp, ctx) -> JobHandle:
        return JobHandle(job_id="r-1", backend="remote_job")

    async def poll(self, handle, inp, ctx) -> JobStatus:
        self._polls += 1
        if self._polls > 1:
            return JobStatus(state="succeeded", output={"adapter_uri": "hf://x"})
        return JobStatus(state="running")

    async def cancel(self, handle) -> None:
        pass


class _PlainOp:
    async def run(self, inp, ctx):
        return {}


def test_is_remote_job_operator():
    assert is_remote_job_operator(_FakeRemoteOp) is True
    assert is_remote_job_operator(_PlainOp) is False


async def test_remote_job_driver_delegates():
    out = await run_job_lifecycle(RemoteJobDriver(_FakeRemoteOp(), inp=None, ctx=None), poll_interval=0)
    assert out == {"adapter_uri": "hf://x"}


# ─── docker driver argv (pure) + executor enum ───────────────────────


def test_build_docker_run_argv():
    argv = build_docker_run_argv("img:tag", "/tmp/io", ["python", "-m", "entry"])
    assert argv[:2] == ["docker", "run"]
    assert "--volume" in argv and "/tmp/io:/io" in argv
    assert "img:tag" in argv
    assert argv[-2:] == ["/io/in.json", "/io/out.json"]


def test_docker_driver_defaults_to_the_images_own_entrypoint():
    """#2415: with no declared ``container_entrypoint`` core supplies no operator
    pack's module — the image's ENTRYPOINT receives the io paths."""
    d = DockerJobDriver(image="img:tag", io_dir="/tmp/io")
    argv = build_docker_run_argv(d.image, d.io_dir, d.entrypoint)
    assert argv[-3:] == ["img:tag", "/io/in.json", "/io/out.json"]
    declared = DockerJobDriver(image="img:tag", io_dir="/tmp/io", entrypoint=["run-op"])
    assert declared.entrypoint == ["run-op"]


def test_docker_and_remote_job_implemented():
    impl = ex.implemented_executors()
    assert "docker" in impl and "remote_job" in impl
    assert ex.is_implemented("remote_job") and ex.is_implemented("docker")


async def test_lifecycle_cancelled_ends_without_spinning():
    """ADR-0151 D9: `cancelled` is terminal — the lifecycle ends (JobCancelled, a
    JobFailed) on the first poll instead of polling until max_polls."""
    class _Cancelled(_FakeDriver):
        async def poll(self, handle):
            self._polls += 1
            return JobStatus(state="cancelled", error="remote job cancelled")

    d = _Cancelled()
    try:
        await run_job_lifecycle(d, poll_interval=0, max_polls=50)
        err = None
    except Exception as e:  # noqa: BLE001 — the terminal raise is what is checked
        err = e
    assert d._polls == 1, f"spun {d._polls} polls on a cancelled job ({type(err).__name__})"
    from cairn.core.operator.jobs import JobCancelled

    assert isinstance(err, JobCancelled) and "cancelled" in str(err)
    assert issubclass(JobCancelled, JobFailed)
