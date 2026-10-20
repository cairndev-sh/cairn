"""``cairn dev`` and ``cairn demo`` point at what the public core has (0.1.2
verification, bug 12): ``cairn dev`` said "the engine serves the built console at
/console" (a 404 in the public core), and the seed told people to "open the console".
"""
from __future__ import annotations

from types import SimpleNamespace

from typer.testing import CliRunner

from cairn.cli.commands import demo, dev


class _Exited:
    """A child process that has already ended cleanly."""

    returncode = 0

    def __init__(self, *a, **kw) -> None:
        pass

    def poll(self) -> int:
        return 0

    def wait(self) -> int:
        return 0

    def terminate(self) -> None:
        pass


def test_dev_without_a_console_does_not_promise_one(monkeypatch, tmp_path):
    monkeypatch.setattr(dev, "_web_dir", lambda: None)
    monkeypatch.setattr(dev.subprocess, "Popen", _Exited)
    res = CliRunner().invoke(dev.app, ["--db", str(tmp_path / "dev.db")])
    assert res.exit_code == 0, res.output
    assert "/console" not in res.output
    assert "not part of the public core" in res.output
    assert "--engine-only" in res.output


def test_the_seed_says_how_to_see_the_demo_workspace(monkeypatch):
    async def _seed(workspace_id):
        return SimpleNamespace(workspace_id=workspace_id, enabled=[], pipelines=[],
                               completed_runs=[], pending_gates=[], providers=[],
                               skipped=[])
    monkeypatch.setattr(demo, "seed_demo_workspace", _seed)
    res = CliRunner().invoke(demo.app, [])
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    assert "Open the console" not in out
    assert "X-Cairn-Workspace: demo" in out
