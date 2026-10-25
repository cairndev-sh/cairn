"""#196 — `cairn ls <pack>` lists the deployment profiles a pack ships.

A pack may ship `profiles/*.toml` beside its pack.toml. Pointing `CAIRN_PROFILE` at
one needed its installed path, found by hand in site-packages. The listing now names
each profile with its absolute installed path and its `[profile] visibility`, in the
text report and in `--json`.
"""
from __future__ import annotations

import json

import typer
from typer.testing import CliRunner

from cairn.cli.commands import ls as ls_mod

app = typer.Typer()
app.command(name="ls")(ls_mod.cmd)


@app.command(name="_noop")
def _noop() -> None:
    """Keeps Typer's group shape (see test_ls_runnable_surface.py)."""


def _out(*args: str) -> str:
    r = CliRunner().invoke(app, ["ls", *args])
    assert r.exit_code == 0, r.output
    return r.output


def _pack_with_profiles(dev_packs, scratch_pack):
    d = scratch_pack("profiled_pack", "profiled-pack")
    (d / "profiles").mkdir()
    (d / "profiles" / "gpu-ssh.toml").write_text(
        '[profile]\nvisibility = "internal"\n\n[profile.bindings]\n"storage.put" = "storage/put"\n')
    (d / "profiles" / "local.toml").write_text('[profile.bindings]\n"storage.put" = "storage/put"\n')
    (d / "profiles" / "README.md").write_text("not a profile\n")
    dev_packs(d.parent)
    return d


def test_the_text_report_lists_each_profile_with_its_installed_path(dev_packs, scratch_pack):
    d = _pack_with_profiles(dev_packs, scratch_pack)
    out = _out("profiled-pack")
    assert "profiles (export CAIRN_PROFILE=<installed path>):" in out
    assert f"profiles/gpu-ssh.toml  [internal]  {(d / 'profiles' / 'gpu-ssh.toml').resolve()}" in out
    assert f"profiles/local.toml  [public]  {(d / 'profiles' / 'local.toml').resolve()}" in out
    assert "README" not in out


def test_the_json_shape_carries_the_same(dev_packs, scratch_pack):
    d = _pack_with_profiles(dev_packs, scratch_pack)
    (pack,) = json.loads(_out("profiled-pack", "--json"))
    assert pack["profiles"] == [
        {"name": "gpu-ssh", "path": "profiles/gpu-ssh.toml",
         "absolute_path": str((d / "profiles" / "gpu-ssh.toml").resolve()), "visibility": "internal"},
        {"name": "local", "path": "profiles/local.toml",
         "absolute_path": str((d / "profiles" / "local.toml").resolve()), "visibility": "public"},
    ]


def test_a_pack_with_no_profiles_says_nothing_about_them(dev_packs, scratch_pack):
    dev_packs(scratch_pack("plain_pack", "plain-pack").parent)
    assert "profiles" not in _out("plain-pack")
    (pack,) = json.loads(_out("plain-pack", "--json"))
    assert pack["profiles"] == []


def test_the_example_pack_lists_hello_toml(example_pack):
    out = _out("hello-cairn")
    assert f"profiles/hello.toml  [public]  {(example_pack / 'profiles' / 'hello.toml').resolve()}" in out


def test_a_pack_whose_asset_dir_is_a_symlink_out_of_it_still_lists_its_profiles(
        dev_packs, scratch_pack, tmp_path):
    """The asset dirs the registry holds are resolved, so a `flows/` symlinked out
    of the pack points outside its root; the root then comes from the pack's own
    module, unresolved."""
    d = scratch_pack("linked_pack", "linked-pack")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (d / "flows").rename(elsewhere / "flows")
    (d / "flows").symlink_to(elsewhere / "flows", target_is_directory=True)
    (d / "profiles").mkdir()
    (d / "profiles" / "box.toml").write_text('[profile]\nvisibility = "public"\n')
    dev_packs(d.parent)
    (pack,) = json.loads(_out("linked-pack", "--json"))
    assert [(p["path"], p["absolute_path"]) for p in pack["profiles"]] == [
        ("profiles/box.toml", str(d / "profiles" / "box.toml"))]


def test_an_unreadable_profiles_dir_does_not_break_the_listing(dev_packs, scratch_pack, monkeypatch):
    _pack_with_profiles(dev_packs, scratch_pack)

    def _boom(p):
        raise PermissionError("profiles: permission denied")

    monkeypatch.setattr(ls_mod, "_pack_root", _boom)
    out = _out("profiled-pack")
    assert "profiled-pack/echo" in out and "profiles (" not in out
    (pack,) = json.loads(_out("profiled-pack", "--json"))
    assert pack["profiles"] == []
