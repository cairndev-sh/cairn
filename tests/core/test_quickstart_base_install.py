"""The quickstart works on a plain install (0.1.2): `cairn dev` turns on password sign-in and the
quickstart logs in with it, so password hashing is a base dependency, and the engine process the CLI
starts never imports a `cairn/` folder from the working directory instead of the installed engine."""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_password_hashing_is_in_the_base_install():
    deps = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    assert any(re.match(r"pwdlib\[argon2\]", d) for d in deps), deps


def test_the_cli_starts_the_engine_with_the_working_directory_off_sys_path():
    for name in ("dev.py", "remote.py"):
        text = (ROOT / "src/cairn/cli/commands" / name).read_text()
        launches = re.findall(r"\[sys\.executable, ([^\]]*?)\"cairn\.core\.triggers\.main\"", text)
        assert launches, f"{name} no longer starts the engine with sys.executable"
        assert all('"-P"' in args for args in launches), (name, launches)
