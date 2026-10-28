"""#1248 H6d — BuiltinSshEngine.fetch_checkpoints, hermetically.

A fake sftp client plays the box. Pins the pull contract the bridge relies
on: manifest.json is the completeness marker on both sides (a dir pulled
mid-snapshot is dropped and retried), pulls are idempotent per stage, and
every failure path returns False instead of raising (best-effort — the next
relay beat retries). The live half is the H6d box acceptance.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from cairn.core.dataflow import EngineJobHandle

import operator_dataflow.engines_ssh as mod
from operator_dataflow.engines_ssh import BuiltinSshEngine


def _handle(workdir: str = ".cairn-jobs/j1") -> EngineJobHandle:
    return EngineJobHandle(job_id="host:j1", engine="builtin-ssh", meta={
        "job_handle": {"job_id": "host:j1", "backend": "ssh",
                       "meta": {"workdir": workdir, "host": "h", "user": "u",
                                "port": 22, "known_hosts": None}},
        "exec_input": {"command": "cairn-dataflow-run"},
    })


class _FakeSftp:
    """Remote FS = a dict of {relpath: content}; get() materializes files."""

    def __init__(self, remote: dict[str, str]) -> None:
        self.remote = remote
        self.get_calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def listdir(self, path: str):
        names = {k[len(path) + 1:].split("/", 1)[0]
                 for k in self.remote if k.startswith(path + "/")}
        if not names:
            raise FileNotFoundError(path)
        return sorted(names)

    async def get(self, remotepath: str, localpath: str, recurse: bool = False):
        self.get_calls.append(remotepath)
        name = remotepath.rsplit("/", 1)[-1]
        for k, v in self.remote.items():
            if k.startswith(remotepath + "/"):
                out = Path(localpath) / name / k[len(remotepath) + 1:]
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(v, encoding="utf-8")


class _FakeConn:
    def __init__(self, sftp: _FakeSftp) -> None:
        self._sftp = sftp

    def start_sftp_client(self):
        return self._sftp


async def _params(jh, connection=""):
    # the real one resolves the target connection's key; host/user come from the handle
    return {"host": jh.meta["host"], "user": jh.meta["user"]}


def _wire(monkeypatch, sftp: _FakeSftp) -> None:
    class _Exec:
        _job_ssh_params = staticmethod(_params)

        @staticmethod
        async def _get_conn(params):
            assert params["host"] == "h" and params["user"] == "u"
            return _FakeConn(sftp)

    monkeypatch.setattr(mod, "_remote_exec", lambda: _Exec)


def _manifest(i: int = 0) -> str:
    return json.dumps({"spec_key": "k", "stage_index": i, "stage": "s",
                       "files": [], "stats": {}})


async def test_pull_complete_stage_then_idempotent(monkeypatch, tmp_path):
    wd = ".cairn-jobs/j1"
    sftp = _FakeSftp({
        f"{wd}/checkpoints/stage-00/manifest.json": _manifest(0),
        f"{wd}/checkpoints/stage-00/current.jsonl": '{"r": 1}\n',
    })
    _wire(monkeypatch, sftp)
    eng = BuiltinSshEngine("box")

    assert await eng.fetch_checkpoints(_handle(), tmp_path) is True
    got = tmp_path / "checkpoints" / "stage-00"
    assert (got / "manifest.json").is_file()
    assert (got / "current.jsonl").read_text() == '{"r": 1}\n'
    assert len(sftp.get_calls) == 1

    # second beat: still True, but NO second transfer
    assert await eng.fetch_checkpoints(_handle(), tmp_path) is True
    assert len(sftp.get_calls) == 1


async def test_mid_snapshot_stage_is_dropped_then_retried(monkeypatch, tmp_path):
    """The runner writes manifest.json LAST — a pull that raced it must not
    poison the staging dir (the relay would skip a manifest-less dir forever
    but WE must re-pull once the manifest lands)."""
    wd = ".cairn-jobs/j1"
    sftp = _FakeSftp({f"{wd}/checkpoints/stage-00/current.jsonl": "partial\n"})
    _wire(monkeypatch, sftp)
    eng = BuiltinSshEngine("box")

    assert await eng.fetch_checkpoints(_handle(), tmp_path) is False
    assert not (tmp_path / "checkpoints" / "stage-00").exists()

    # the box finishes the snapshot; the next beat completes the pull
    sftp.remote[f"{wd}/checkpoints/stage-00/manifest.json"] = _manifest(0)
    assert await eng.fetch_checkpoints(_handle(), tmp_path) is True
    assert (tmp_path / "checkpoints" / "stage-00" / "manifest.json").is_file()


async def test_no_remote_checkpoints_and_no_workdir(monkeypatch, tmp_path):
    sftp = _FakeSftp({})
    _wire(monkeypatch, sftp)
    eng = BuiltinSshEngine("box")
    assert await eng.fetch_checkpoints(_handle(), tmp_path) is False
    assert await eng.fetch_checkpoints(_handle(workdir=""), tmp_path) is False


def test_localize_resume_ships_bytes_and_drops_seeded_artifacts(tmp_path):
    """H6d submit half: locally-readable resume artifacts ride the sftp
    upload and leave the block (the channel materialized them); http(s) and
    unreadable file:// artifacts stay, so the box fetches or fails LOUDLY."""
    from cairn.core.dataflow import Budget, DataflowSpec, ResumeFrom, StageRef

    ck = tmp_path / "durable" / "r1" / "stage-00"
    ck.mkdir(parents=True)
    (ck / "current.jsonl").write_text('{"r": 1}\n', encoding="utf-8")

    spec = DataflowSpec(
        stages=[StageRef(stage="noop", checkpoint=True), StageRef(stage="noop.slow")],
        budget=Budget(),
        resume_from=ResumeFrom(
            spec_key="k", stage_index=0,
            artifacts=[
                {"uri": f"file://{ck}/current.jsonl", "content_hash": "sha256:aa",
                 "dest": "current.jsonl"},
                {"uri": "https://cp.example/ck/other.jsonl", "content_hash": "",
                 "dest": "other.jsonl"},
                {"uri": "file:///gone/lost.jsonl", "content_hash": "",
                 "dest": "lost.jsonl"},
            ],
            completed=[{"index": 0, "stage": "noop", "stats": {}}],
        ),
    )
    out, uploads = BuiltinSshEngine._localize_resume(spec)
    assert uploads == [f"{ck}/current.jsonl"]
    kept = [a["uri"] for a in out.resume_from.artifacts]
    assert kept == ["https://cp.example/ck/other.jsonl", "file:///gone/lost.jsonl"]
    # markers + resume index untouched; the ORIGINAL spec untouched
    assert out.resume_from.stage_index == 0 and out.resume_from.completed
    assert len(spec.resume_from.artifacts) == 3

    # no resume block → identity, no uploads
    bare = spec.model_copy(update={"resume_from": None})
    out2, up2 = BuiltinSshEngine._localize_resume(bare)
    assert out2 is bare and up2 == []


async def test_connection_failure_returns_false(monkeypatch, tmp_path):
    class _Exec:
        _job_ssh_params = staticmethod(_params)

        @staticmethod
        async def _get_conn(params):
            raise ConnectionError("box unreachable")

    monkeypatch.setattr(mod, "_remote_exec", lambda: _Exec)
    eng = BuiltinSshEngine("box")
    assert await eng.fetch_checkpoints(_handle(), tmp_path) is False
