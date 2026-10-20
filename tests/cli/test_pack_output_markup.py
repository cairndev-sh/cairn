"""``cairn pack sync`` / ``cairn pack add`` render their colours instead of printing
Rich markup as text (0.1.2 verification, bug 10: ``[yellow]import[/] …`` and
``[red]✗ no uv workspace root found[/]`` on the terminal). Dynamic text that holds
square brackets (``[operator."x"]``, ``[tool.uv.workspace]``) still prints as written.
"""
from __future__ import annotations

import typer
from typer.testing import CliRunner

from cairn.cli.commands import pack as pack_mod
from cairn.core.operator import manifest_sync

app = typer.Typer()
app.command(name="add")(pack_mod.add_cmd)
app.command(name="sync")(pack_mod.sync_cmd)
runner = CliRunner()

_MARKUP = ("[yellow]", "[red]", "[green]", "[/]", "[bold]")


def test_pack_sync_renders_its_markup(tmp_path, monkeypatch):
    (tmp_path / "pack.toml").write_text('[pack]\nname = "p"\nversion = "0.1.0"\n')
    report = manifest_sync.SyncReport(
        pack_dir=tmp_path,
        import_problems=["p.ops: No module named 'x[y]'"],
        missing_tables=["verify/echo"],
        drifted={"verify/echo": ["risk: low != high"]},
        missing_entry_points=["verify/echo = p.ops:Echo"],
        orphan_tables=["old/op"])
    monkeypatch.setattr(manifest_sync, "check_pack", lambda target: report)
    res = runner.invoke(app, ["sync", str(tmp_path), "--check"])
    assert res.exit_code == 1, res.output
    assert not [m for m in _MARKUP if m in res.output], res.output
    assert "import p.ops: No module named 'x[y]'" in res.output
    assert 'missing table [operator."verify/echo"]' in res.output
    assert "drift verify/echo: risk: low != high" in res.output


def test_pack_add_outside_a_checkout_renders_its_markup(tmp_path, monkeypatch):
    pack = tmp_path / "vp"
    pack.mkdir()
    (pack / "pack.toml").write_text('[pack]\nname = "vp"\nversion = "0.1.0"\n')
    (pack / "pyproject.toml").write_text('[project]\nname = "vp"\nversion = "0.1.0"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pack_mod, "_workspace_root", lambda: None)
    res = runner.invoke(app, ["add", str(pack)])
    assert res.exit_code == 2, res.output
    assert not [m for m in _MARKUP if m in res.output], res.output
    assert "✗ no uv workspace root found" in res.output
    assert "[tool.uv.workspace]" in res.output
