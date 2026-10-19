"""`cairn pack add` — making a scaffolded pack permanent (#1545).

The manual procedure was three hand-edits of the root `pyproject.toml`
(`[tool.uv.workspace] members`, `[project] dependencies`, `[tool.uv.sources]`)
plus a workspace reinstall, documented nowhere. These tests drive the real
command against a real copy of the real root pyproject, because the edit has to
survive ~200 lines of load-bearing comments — a fixture toml would prove
nothing about the file the command actually rewrites.

NB the command is mounted on a LOCAL Typer app rather than imported from
`cairn.cli.main` — the pattern tests/cli/test_ls_operators.py documents: main.py
calls load_env() at module scope, which repopulates the storage keys
tests/conftest.py deliberately neutralizes.
"""
from __future__ import annotations

import os
import textwrap
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
import typer
from typer.testing import CliRunner

from cairn.cli.commands import pack as pack_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
ROOT_PYPROJECT = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")

app = typer.Typer()
app.command(name="add")(pack_mod.add_cmd)


@app.command(name="_noop")
def _noop() -> None:
    """Second command so Typer keeps the group shape."""


runner = CliRunner()


@pytest.fixture
def workspace(tmp_path, monkeypatch) -> Path:
    """A scratch workspace root holding a byte-for-byte copy of the real one."""
    (tmp_path / "pyproject.toml").write_text(ROOT_PYPROJECT, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _make_pack(root: Path, name: str = "mypack") -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "pack.toml").write_text(f'[pack]\nname = "{name}"\nversion = "0.1.0"\n')
    (d / "__init__.py").write_text("from cairn.core.pack import Pack\n")
    (d / "pyproject.toml").write_text(textwrap.dedent(f"""\
        [project]
        name = "{name}"
        version = "0.1.0"
        """))
    return d


# ─── the pure editor ───────────────────────────────────────────────────


def test_all_three_wiring_points_are_written():
    """Mirrors how the ~13 first-party workspace packs are wired: a member
    path, a dependency name, and a `{ workspace = true }` source."""
    new, changes = pack_mod.wire_pack_into_pyproject(
        ROOT_PYPROJECT, member="packs/mypack", dist="mypack")

    assert len(changes) == 3
    added = [line for line in new.splitlines() if line not in ROOT_PYPROJECT.splitlines()]
    assert '    "packs/mypack",' in new
    assert "mypack" in new and "mypack" in "\n".join(added)
    assert any(line.startswith("mypack") and "{ workspace = true }" in line
               for line in new.splitlines()), "no [tool.uv.sources] line"

    # exactly three lines added, nothing removed, nothing reflowed
    old_lines, new_lines = ROOT_PYPROJECT.splitlines(), new.splitlines()
    assert len(new_lines) == len(old_lines) + 3
    assert set(old_lines) <= set(new_lines), "an existing line was changed or dropped"


def test_the_new_lines_land_inside_the_right_sections():
    import tomllib

    new, _ = pack_mod.wire_pack_into_pyproject(
        ROOT_PYPROJECT, member="packs/mypack", dist="mypack")
    data = tomllib.loads(new)             # still valid TOML
    assert "packs/mypack" in data["tool"]["uv"]["workspace"]["members"]
    assert "mypack" in data["project"]["dependencies"]
    assert data["tool"]["uv"]["sources"]["mypack"] == {"workspace": True}
    # and it is wired the same way the shipped members are (the client: every tree builds it
    # beside the engine; the builtin packs are inside the engine wheel, ADR-0137)
    assert data["tool"]["uv"]["sources"]["cairn-client"] == {"workspace": True}


def test_comments_survive():
    """Line-based on purpose: a tomlkit/tomllib round-trip reflows the ~200
    lines of comments explaining every dependency in this file."""
    new, _ = pack_mod.wire_pack_into_pyproject(
        ROOT_PYPROJECT, member="packs/mypack", dist="mypack")
    comments = [line for line in ROOT_PYPROJECT.splitlines() if line.lstrip().startswith("#")]
    assert len(comments) > 20, "the root pyproject lost its comments; this test measures nothing"
    kept = set(new.splitlines())
    assert [c for c in comments if c not in kept] == []


def test_editing_twice_is_a_no_op():
    once, _ = pack_mod.wire_pack_into_pyproject(
        ROOT_PYPROJECT, member="packs/mypack", dist="mypack")
    twice, changes = pack_mod.wire_pack_into_pyproject(
        once, member="packs/mypack", dist="mypack")
    assert changes == [] and twice == once


def test_an_already_shipped_pack_is_recognised_as_wired():
    _, changes = pack_mod.wire_pack_into_pyproject(
        ROOT_PYPROJECT, member="packages/cairn-client", dist="cairn-client")
    assert changes == []


def test_an_inline_members_array_is_refused_not_written_into_the_next_array():
    """The array is edited line by line, so its closing bracket must be found
    inside [tool.uv.workspace]. Scanning past the section once put the member
    into the next multi-line array in the file (pytest's `markers`, #2428)."""
    text = textwrap.dedent("""\
        [project]
        dependencies = [
            "a",
        ]

        [tool.uv.workspace]
        members = ["packages/a"]

        [tool.uv.sources]
        a = { workspace = true }

        [tool.pytest.ini_options]
        markers = [
            "slow: slow",
        ]
        """)
    with pytest.raises(ValueError, match="multi-line `members = \\[` array"):
        pack_mod.wire_pack_into_pyproject(text, member="packs/mypack", dist="mypack")


# ─── the command ───────────────────────────────────────────────────────


def test_add_wires_the_pack_and_prints_the_restore_safe_sync(workspace):
    _make_pack(workspace)
    res = runner.invoke(app, ["add", "mypack"])
    assert res.exit_code == 0, res.output

    written = (workspace / "pyproject.toml").read_text(encoding="utf-8")
    assert '    "mypack",' in written
    assert any(line.startswith("mypack") and "workspace = true" in line
               for line in written.splitlines())
    # it must NOT sync for us — a bare `uv sync` strips extras — but it must say
    # exactly what to run
    assert "--extra dev" in res.output and "--extra auth" in res.output
    assert "does not sync for you" in res.output


def test_the_printed_sync_matches_contributing_md():
    """The command's whole value is that the sync it prints is the restore-safe
    one. If CONTRIBUTING.md's quickstart gains an extra and this doesn't, the
    command starts handing out a venv-stripping command."""
    contributing = (REPO_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    printed_extras = {tok for tok in pack_mod.RESTORE_SAFE_SYNC.split()
                      if not tok.startswith("-") and tok != "\\"}
    printed_extras.discard("uv")
    printed_extras.discard("sync")
    for extra in printed_extras:
        assert f"--extra {extra}" in contributing, (
            f"`cairn pack add` prints --extra {extra}, which CONTRIBUTING.md's "
            "quickstart does not — the two have drifted")
    # ...and every extra the quickstart names is printed
    quickstart = contributing.split("```bash", 1)[1].split("```", 1)[0]
    for line in quickstart.splitlines():
        for i, tok in enumerate(line.split()):
            if tok == "--extra":
                assert line.split()[i + 1] in printed_extras


def test_add_is_idempotent(workspace):
    _make_pack(workspace)
    first = runner.invoke(app, ["add", "mypack"])
    after_first = (workspace / "pyproject.toml").read_text(encoding="utf-8")
    second = runner.invoke(app, ["add", "mypack"])
    assert first.exit_code == 0 and second.exit_code == 0
    assert (workspace / "pyproject.toml").read_text(encoding="utf-8") == after_first
    assert "already wired" in second.output


def test_dev_prints_the_export_and_edits_nothing(workspace):
    pack = _make_pack(workspace)
    before = (workspace / "pyproject.toml").read_text(encoding="utf-8")
    res = runner.invoke(app, ["add", "--dev", str(pack)])
    assert res.exit_code == 0, res.output
    assert "CAIRN_PACK_PATH" in res.output
    assert (workspace / "pyproject.toml").read_text(encoding="utf-8") == before


def test_a_dir_that_is_not_a_pack_is_refused(workspace):
    (workspace / "notapack").mkdir()
    res = runner.invoke(app, ["add", "notapack"])
    assert res.exit_code == 2
    assert "pack.toml" in res.output


def test_a_pack_outside_the_workspace_is_refused_with_the_dev_hint(
    workspace, tmp_path_factory,
):
    """uv requires members to live inside the workspace root, so there is no
    honest edit to make — say so rather than writing a line uv rejects."""
    outside = _make_pack(tmp_path_factory.mktemp("elsewhere"), "outsidepack")
    before = (workspace / "pyproject.toml").read_text(encoding="utf-8")
    res = runner.invoke(app, ["add", str(outside)])
    assert res.exit_code == 2
    assert "--dev" in res.output
    assert (workspace / "pyproject.toml").read_text(encoding="utf-8") == before


def test_a_pack_without_a_pyproject_is_refused(workspace):
    pack = _make_pack(workspace)
    (pack / "pyproject.toml").unlink()
    res = runner.invoke(app, ["add", "mypack"])
    assert res.exit_code == 2
    assert "--dev" in res.output


def test_the_real_root_pyproject_is_untouched():
    """Every test above runs against a copy; this pins that nothing leaked."""
    assert (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8") == ROOT_PYPROJECT
