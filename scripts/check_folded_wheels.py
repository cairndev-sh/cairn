"""The built-wheel gate (ADR-0137 D7): the public release is five wheels, and the engine
wheel alone carries the builtin operator packs and the core contracts.

    python scripts/check_folded_wheels.py [--dist DIR] [--keep]

1. ``uv build --all-packages`` (each wheel built FROM its sdist, as a release builds it)
   gives exactly the five public wheels: ``cairndev``, ``cairn-client``, the two optional
   dataflow engines and ``cairn-pack-hello``.
2. A fresh venv gets the three base wheels (``cairndev``, ``cairn-client``,
   ``cairn-pack-hello``) from ``dist`` alone (``--no-index --no-deps``) plus the third-party
   requirements the lock exports for the base install, and nothing else that is cairn's. Not
   ``--extra hello``: the example pack's ``[all]`` brings asyncssh, which the public-set check
   refuses on purpose (the public CI proves the core without it); ``pipeline explain`` needs
   none of it.
3. In that venv, from a directory outside the checkout:
   ``scripts/check_public_set.py --extras hello``; ``cairn pack check``; the 13 core contract
   packages resolve, all hosted by ``cairndev``, with the names, versions and digests of the
   pre-fold snapshot (``tests/core/fixtures/contract_resolution_prefold.json``); the 22
   builtin ``cairn.operators`` entry points all come from ``cairndev``; ``cairn-dataflow-run``
   starts (with no spec it refuses by name); ``cairn pipeline explain
   hello-cairn/profile-and-publish``.

Exit 0 when every step holds; otherwise the failing step and its output, exit 1. CI runs it in
the ``public`` job (a required check).
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
#: The public release (ADR-0137 D6): wheel distribution names, normalised as wheels spell them.
WHEELS = {"cairndev", "cairn_client", "cairn_operator_dataflow_metaflow",
          "cairn_operator_dataflow_skypilot", "cairn_pack_hello"}
#: What a base install holds of cairn's own: the engine, the client and the example pack.
BASE = ("cairndev", "cairn-client", "cairn-pack-hello")
SNAPSHOT = ROOT / "tests" / "core" / "fixtures" / "contract_resolution_prefold.json"
CORE_CONTRACTS = 13
BUILTIN_OPERATORS = 22
#: Inherited settings that would steer the venv or the engine away from a plain install.
SCRUB = ("VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT", "PYTHONPATH", "CAIRN_PACK_PATH", "CAIRN_STORAGE",
         "OBS_STORAGE", "CAIRN_PROFILE")

CHECK = """
import importlib.metadata as md, json, sys
from cairn.core.pack.contracts import discover_contracts, resolve_contracts
from cairn.core.pack.registry import contract_resolution_snapshot
want = json.load(open(sys.argv[1]))
resolved = resolve_contracts()
hosts = {}
for dist in md.distributions():
    for f in dist.files or ():
        parts = str(f).split("/")
        if parts[0] == "cairn_contracts" and len(parts) > 2:
            hosts.setdefault(parts[1], set()).add(dist.metadata["Name"])
names = [d.name for d in resolved]
assert len(names) == int(sys.argv[2]), f"{len(names)} contract packages resolved: {names}"
assert all(hosts.get(n) == {"cairndev"} for n in names), f"contract hosts: {hosts}"
assert contract_resolution_snapshot() == want, "contract identities differ from the pre-fold snapshot"
eps = list(md.entry_points(group="cairn.operators"))
by = sorted({ep.dist.name for ep in eps})
assert len(eps) == int(sys.argv[3]) and by == ["cairndev"], f"{len(eps)} operators from {by}"
installed = sorted(d.metadata["Name"] for d in md.distributions()
                   if d.metadata["Name"].startswith(("cairn-", "cairndev")))
print(f"contracts: {len(names)} packages hosted by cairndev, identities equal to the pre-fold snapshot")
print(f"operators: {len(eps)} cairn.operators entry points, all from cairndev")
print(f"installed: {', '.join(installed)}")
"""


def _env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k not in SCRUB}


def _run(step: str, cmd: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None,
         quiet: bool = False) -> str:
    print(f"[check_folded_wheels] {step}", flush=True)
    done = subprocess.run(cmd, cwd=cwd, env=env or _env(), text=True, capture_output=True)
    if done.returncode != 0:
        sys.stdout.write(done.stdout)
        sys.stderr.write(done.stderr)
        raise SystemExit(f"[check_folded_wheels] FAILED: {step} (exit {done.returncode})")
    if not quiet:
        sys.stdout.write(done.stdout)
    return done.stdout


def wheel_names(dist: Path) -> set[str]:
    return {p.name.split("-")[0] for p in dist.glob("*.whl")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dist", type=Path, default=None, help="build into this directory (default: a temporary one)")
    ap.add_argument("--keep", action="store_true", help="keep the temporary venv and dist")
    ap.add_argument("--python", default=f"{sys.version_info.major}.{sys.version_info.minor}",
                    help="the interpreter the venv is created with (default: this one's version)")
    a = ap.parse_args(argv)
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("check_folded_wheels: uv is not on PATH")
    work = Path(tempfile.mkdtemp(prefix="cairn-folded-"))
    dist = (a.dist or work / "dist").resolve()
    if dist.exists() and any(dist.iterdir()):
        raise SystemExit(f"check_folded_wheels: {dist} is not empty; the gate counts every wheel in it")
    try:
        _run("uv build --all-packages", [uv, "build", "--all-packages", "-o", str(dist)], quiet=True)
        names = wheel_names(dist)
        if names != WHEELS:
            raise SystemExit(f"[check_folded_wheels] FAILED: the wheels are {sorted(names)}, "
                             f"not the public five {sorted(WHEELS)}")
        print(f"[check_folded_wheels] wheels: {', '.join(sorted(p.name for p in dist.glob('*.whl')))}")

        venv = work / "venv"
        python = venv / "bin" / "python"
        _run("a fresh venv", [uv, "venv", "--python", a.python, str(venv)], quiet=True)
        reqs = work / "requirements.txt"
        _run("export the base install's third-party requirements",
             [uv, "export", "--frozen", "--no-dev", "--no-emit-workspace", "--output-file", str(reqs)],
             quiet=True)
        _run("install the three base wheels from dist alone",
             [uv, "pip", "install", "--python", str(python), "--no-index", "--no-deps",
              "--find-links", str(dist), *BASE], quiet=True)
        _run("install the third-party requirements",
             [uv, "pip", "install", "--python", str(python), "-r", str(reqs)], quiet=True)

        bin_ = venv / "bin"
        env = _env() | {"PATH": f"{bin_}{os.pathsep}{os.environ.get('PATH', '')}"}
        here = work  # outside the checkout: nothing is imported from the source tree
        _run("check_public_set --extras hello",
             [str(python), str(ROOT / "scripts" / "check_public_set.py"), "--extras", "hello"], cwd=here, env=env)
        _run("cairn pack check", [str(bin_ / "cairn"), "pack", "check"], cwd=here, env=env, quiet=True)
        _run("contracts, operators and the installed set",
             [str(python), "-c", CHECK, str(SNAPSHOT), str(CORE_CONTRACTS), str(BUILTIN_OPERATORS)],
             cwd=here, env=env)
        # The engine's job-machine script takes no flags: with no spec it refuses by name, which
        # proves the console script resolves into the engine wheel and imports the runner.
        print("[check_folded_wheels] cairn-dataflow-run (no spec: its own refusal)", flush=True)
        ran = subprocess.run([str(bin_ / "cairn-dataflow-run")], cwd=here, env=env, text=True,
                             capture_output=True)
        if "cairn-dataflow-run: no spec" not in ran.stdout + ran.stderr:
            sys.stdout.write(ran.stdout)
            sys.stderr.write(ran.stderr)
            raise SystemExit("[check_folded_wheels] FAILED: cairn-dataflow-run did not start")
        _run("cairn pipeline explain hello-cairn/profile-and-publish",
             [str(bin_ / "cairn"), "pipeline", "explain", "hello-cairn/profile-and-publish"],
             cwd=here, env=env, quiet=True)
        installed = json.loads(_run("the installed set", [uv, "pip", "list", "--python", str(python),
                                                          "--format", "json"], quiet=True))
        ours = sorted(d["name"] for d in installed if d["name"].startswith(("cairn-", "cairndev")))
        if ours != sorted(BASE):
            raise SystemExit(f"[check_folded_wheels] FAILED: cairn distributions installed: {ours}")
    finally:
        if not a.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"[check_folded_wheels] kept {work}")
    print("[check_folded_wheels] OK: five wheels; the engine wheel carries the builtins and the core contracts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
