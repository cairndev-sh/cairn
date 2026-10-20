"""The one-line installer, docs/site/public/install.sh (#2430): `curl … | sh` installs cairndev as a
uv tool. Driven against stub `uv`, `curl` and `wget` binaries on PATH, so nothing is installed and
nothing is fetched — what is checked is exactly what the script would run."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "docs/site/public/install.sh"
SH = shutil.which("sh")
pytestmark = pytest.mark.skipif(SH is None, reason="needs a POSIX sh")
LINKS = "https://github.com/cairndev-sh/cairn/releases/expanded_assets/v{}"
INSTALL = (f"tool install --upgrade --force --python 3.13 --find-links {LINKS.format('0.1.2')} "
           "--with cairn-client==0.1.2 cairndev==0.1.2")


def _stub(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o755)


@pytest.fixture
def env(tmp_path):
    """A PATH with a recording `uv` and the system basics, an empty HOME, and a recording uv ready
    for a stub installer to drop into ~/.local/bin."""
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    log = tmp_path / "uv.log"
    _stub(bin_ / "uv", f'echo "$@" >> "{log}"\n')
    fake_uv = tmp_path / "fake-uv"
    _stub(fake_uv, f'echo "$@" >> "{log}"\n')
    home = tmp_path / "home"
    home.mkdir()
    return {"PATH": f"{bin_}:/usr/bin:/bin", "HOME": str(home)}, log, bin_, fake_uv


def _run(env: dict, **extra) -> subprocess.CompletedProcess:
    return subprocess.run([SH, str(SCRIPT)], env={**env, **extra}, capture_output=True, text=True, timeout=30)


def _fetcher(bin_: Path, name: str, fake_uv: Path, fetched: Path, *, installer: str | None = None,
             fail: bool = False) -> None:
    """A stub curl (`-o FILE`) or wget (`-qO FILE`) that records its arguments and writes `installer`
    (default: a stand-in for astral's that copies a recording uv into ~/.local/bin)."""
    body = installer if installer is not None else f'mkdir -p "$HOME/.local/bin" && cp "{fake_uv}" "$HOME/.local/bin/uv"'
    flag = "-o" if name == "curl" else "-qO"
    _stub(bin_ / name, f'echo "$@" >> "{fetched}"\n'
                       + ("exit 22\n" if fail else
                          f'while [ $# -gt 0 ]; do [ "$1" = "{flag}" ] && out="$2"; shift; done\n'
                          f"cat > \"$out\" <<'INSTALLER'\n{body}\nINSTALLER\n"))


def test_it_installs_or_upgrades_cairndev_as_a_uv_tool(env):
    e, log, _, _ = env
    r = _run(e)
    assert r.returncode == 0, r.stderr
    assert log.read_text().splitlines() == [INSTALL]
    assert "cairn --help" in r.stdout


def test_a_rerun_is_an_upgrade_not_a_no_op(env):
    """`--upgrade`: re-running the installer moves an unpinned tool to the latest release."""
    e, log, _, _ = env
    assert _run(e).returncode == 0 and _run(e).returncode == 0
    assert log.read_text().splitlines() == [INSTALL, INSTALL]


def test_extras_and_a_pinned_version_reach_the_spec(env):
    e, log, _, _ = env
    r = _run(e, CAIRN_EXTRAS="hello,metaflow", CAIRN_VERSION="0.1.0", CAIRN_PYTHON="3.12")
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == ("tool install --upgrade --force --python 3.12 "
                                       f"--find-links {LINKS.format('0.1.0')} --with cairn-client==0.1.0 "
                                       "cairndev[hello,metaflow]==0.1.0")


def test_an_empty_release_installs_from_pypi_alone(env):
    """CAIRN_RELEASE= switches the GitHub Release fallback off: the plain PyPI install."""
    e, log, _, _ = env
    r = _run(e, CAIRN_RELEASE="")
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == "tool install --upgrade --force --python 3.13 cairndev"


def test_the_github_fallback_pins_the_engine_and_the_client_to_the_release(env):
    """While the wheels come from GitHub, a higher version of cairndev or cairn-client published on
    PyPI by someone else must not win: both are pinned to the release."""
    e, log, _, _ = env
    r = _run(e, CAIRN_RELEASE="0.1.2", CAIRN_EXTRAS="hello")
    assert r.returncode == 0, r.stderr
    line = log.read_text().strip()
    assert line.endswith("--with cairn-client==0.1.2 cairndev[hello]==0.1.2"), line


def test_a_release_that_is_not_a_version_is_refused(env):
    e, log, _, _ = env
    r = _run(e, CAIRN_RELEASE="0.1.2/../../evil")
    assert r.returncode != 0 and "CAIRN_RELEASE is not a version" in r.stderr
    assert not log.exists()


@pytest.mark.parametrize("var, value", [
    ("CAIRN_EXTRAS", "hello;rm -rf ~"),
    ("CAIRN_EXTRAS", "Hello"),
    ("CAIRN_EXTRAS", "hello\nrag"),
    ("CAIRN_VERSION", "0.1.0 && curl evil"),
    ("CAIRN_VERSION", "0.1\n0"),
    ("CAIRN_PYTHON", "3.13; id"),
    ("CAIRN_PYTHON", "-3"),
])
def test_a_value_that_is_not_a_spec_is_refused_before_anything_runs(env, var, value):
    """The values are spliced into a package spec: anything a spec cannot hold is refused."""
    e, log, _, _ = env
    r = _run(e, **{var: value})
    assert r.returncode != 0 and var in r.stderr
    assert not log.exists()


def test_a_leading_dash_cannot_become_a_uv_option(env):
    """Extras and version sit inside `cairndev[...]==...`, so `-e`-like values stay part of the spec."""
    e, log, _, _ = env
    assert _run(e, CAIRN_EXTRAS="-e", CAIRN_VERSION="-e").returncode == 0
    assert log.read_text().strip().endswith(" cairndev[-e]==-e")


@pytest.mark.parametrize("fetcher", ["curl", "wget"])
def test_a_missing_uv_is_installed_with_astrals_installer(env, tmp_path, fetcher):
    e, log, bin_, fake_uv = env
    (bin_ / "uv").unlink()
    if fetcher == "wget":   # a PATH with the basics the script uses, and no curl
        tools = tmp_path / "tools"
        tools.mkdir()
        for t in ("mktemp", "mkdir", "cp", "cat", "rm", "sh", "true"):
            (tools / t).symlink_to(shutil.which(t))
        e = {**e, "PATH": f"{bin_}:{tools}"}
    fetched = tmp_path / "fetch.log"
    _fetcher(bin_, fetcher, fake_uv, fetched)
    r = _run(e)
    assert r.returncode == 0, r.stderr
    assert "https://astral.sh/uv/install.sh" in fetched.read_text()
    assert log.read_text().strip() == INSTALL
    assert (Path(e["HOME"]) / ".local/bin/uv").exists()


def test_uv_installed_under_uv_install_dir_is_found(env, tmp_path):
    e, log, bin_, fake_uv = env
    (bin_ / "uv").unlink()
    custom = tmp_path / "custom-bin"
    _fetcher(bin_, "curl", fake_uv, tmp_path / "fetch.log",
             installer=f'mkdir -p "$UV_INSTALL_DIR" && cp "{fake_uv}" "$UV_INSTALL_DIR/uv"')
    r = _run(e, UV_INSTALL_DIR=str(custom))
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == INSTALL


def test_a_failed_download_says_so(env, tmp_path):
    """A failed fetch stops with its own message — not the misleading 'not on PATH'."""
    e, log, bin_, fake_uv = env
    (bin_ / "uv").unlink()
    _fetcher(bin_, "curl", fake_uv, tmp_path / "fetch.log", fail=True)
    r = _run(e)
    assert r.returncode != 0 and "could not download uv's installer" in r.stderr
    assert not log.exists()


def test_an_installer_that_leaves_no_uv_is_a_clear_error(env, tmp_path):
    e, log, bin_, fake_uv = env
    (bin_ / "uv").unlink()
    _fetcher(bin_, "curl", fake_uv, tmp_path / "fetch.log", installer="true")
    r = _run(e)
    assert r.returncode != 0 and "not on PATH" in r.stderr


def test_no_fetcher_is_a_clear_error(env):
    e, _, bin_, _ = env
    (bin_ / "uv").unlink()
    r = _run({**e, "PATH": str(bin_)})   # no curl, no wget, no uv
    assert r.returncode != 0 and "neither curl nor wget" in r.stderr


def test_a_download_cut_short_runs_nothing(env):
    """`curl … | sh` that stops mid-file: the body is one function called on the last line, so a
    truncated script defines functions and exits without installing anything."""
    e, log, _, _ = env
    text = SCRIPT.read_text()
    assert text.rstrip().endswith('main "$@"')
    truncated = text[: text.rindex('main "$@"')]
    r = subprocess.run([SH], input=truncated, env=e, capture_output=True, text=True, timeout=30)
    assert r.returncode == 0 and not log.exists()
