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
   refuses on purpose (the public CI proves the core without it); nothing below needs it.
3. In that venv, from a directory outside the checkout:
   ``scripts/check_public_set.py --extras hello``; ``cairn pack check``; the 13 core contract
   packages resolve, all hosted by ``cairndev``, with the names, versions and digests of the
   pre-fold snapshot (``tests/core/fixtures/contract_resolution_prefold.json``); the 24
   builtin ``cairn.operators`` entry points all come from ``cairndev``; ``cairn-dataflow-run``
   starts (with no spec it refuses by name); ``cairn pipeline explain
   hello-cairn/profile-and-publish``.
4. D7.4's run: ``hello-cairn/profile-and-publish`` runs IN PROCESS through the one launch
   door, the way ``cairn pipeline run`` drives it (a ``RunDispatcher`` over the engine's own
   ``Runner``, a ``PipelineBridge``, ``launch_pipeline``), on the sample file the
   ``cairn-pack-hello`` wheel installed: no server, no Docker, no network, no provider key,
   ``CAIRN_BACKEND=mock`` and a SQLite store in the work directory. Its two gates that wait
   for a person (``review`` and ``promote``'s own) are approved through
   ``PipelineBridge.advance_step``, the call ``cairn remote advance`` reaches. It must end
   ``done``; every task leg's ``Run`` names ``task:<operator>`` for an operator whose entry
   point is ``cairndev``'s and whose class loads from the venv; every leg's admitted
   invocation names a capability defined by a contract package ``cairndev`` hosts; and the
   record's contract closure equals the pre-fold snapshot.

Exit 0 when every step holds; otherwise the failing step and its output, exit 1. CI runs it in
the ``public`` job (a required check).

The checks themselves are plain functions over plain data (``*_problems``), so a test proves
each one REFUSES a tampered input without building a wheel
(``tests/core/test_check_folded_wheels.py``). The in-venv halves are this same file run with
``--probe``, under ``python -I``: nothing is imported from the checkout.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
#: The public release (ADR-0137 D6): wheel distribution names, normalised as wheels spell them.
WHEELS = {"cairndev", "cairn_client", "cairn_operator_dataflow_metaflow",
          "cairn_operator_dataflow_skypilot", "cairn_pack_hello"}
#: What a base install holds of cairn's own: the engine, the client and the example pack.
BASE = ("cairndev", "cairn-client", "cairn-pack-hello")
ENGINE = "cairndev"
SNAPSHOT = ROOT / "tests" / "core" / "fixtures" / "contract_resolution_prefold.json"
CORE_CONTRACTS = 13
BUILTIN_OPERATORS = 24  # 22, plus compute/acquire and compute/release (ADR-0151 D6)
#: The pipeline D7.4 runs in process, and the gates in it that wait for a person.
PIPELINE = "hello-cairn/profile-and-publish"
PERSON_GATES = ("review", "promote")
#: Inherited settings that would steer the venv or the engine away from a plain install.
SCRUB = ("VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT", "PYTHONPATH", "CAIRN_PACK_PATH", "CAIRN_STORAGE",
         "OBS_STORAGE", "CAIRN_PROFILE", "PYTHONOPTIMIZE")
#: Never reach a provider from the run: no key is inherited, whatever the caller's shell holds.
PROVIDER_KEYS = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY",
                 "AZURE_OPENAI_API_KEY", "COHERE_API_KEY", "MISTRAL_API_KEY", "GROQ_API_KEY")


# ── the checks: plain data in, problems out ─────────────────────────────────────────────────

def wheel_problems(names: Iterable[str]) -> list[str]:
    """D7.2: exactly the public five, nothing more and nothing less."""
    got = set(names)
    if got == WHEELS:
        return []
    return [f"the wheels are {sorted(got)}, not the public five {sorted(WHEELS)}"
            + (f"; extra: {sorted(got - WHEELS)}" if got - WHEELS else "")
            + (f"; missing: {sorted(WHEELS - got)}" if WHEELS - got else "")]


def contract_problems(resolved: list[str], hosts: Mapping[str, Iterable[str]], snapshot: Mapping,
                      want: Mapping, count: int = CORE_CONTRACTS) -> list[str]:
    """D7.4: ``count`` contract packages resolve, each hosted by the engine wheel alone, and
    their closure (names, versions, digests) equals the pre-fold snapshot."""
    out = []
    if len(resolved) != count:
        out.append(f"{len(resolved)} contract packages resolved, not {count}: {resolved}")
    stray = {n: sorted(hosts.get(n, ())) for n in resolved if set(hosts.get(n, ())) != {ENGINE}}
    if stray:
        out.append(f"contract packages not hosted by {ENGINE} alone: {stray}")
    if dict(snapshot) != dict(want):
        out.append("contract identities differ from the pre-fold snapshot: "
                   + _closure_diff(snapshot, want))
    return out


def operator_problems(operators: Mapping[str, str] | Iterable[tuple[str, str]],
                      count: int = BUILTIN_OPERATORS) -> list[str]:
    """D7.4: ``count`` ``cairn.operators`` entry points, every one from the engine wheel.
    ``operators`` is every ``(entry-point name, declaring distribution)`` pair, duplicates kept:
    a second distribution declaring a builtin's name must be refused, not collapsed into it."""
    pairs = list(operators.items()) if isinstance(operators, Mapping) else list(operators)
    names = [name for name, _ in pairs]
    out = []
    if len(pairs) != count:
        out.append(f"{len(pairs)} cairn.operators entry points, not {count}: {sorted(names)}")
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        out.append(f"cairn.operators names declared more than once: "
                   f"{ {n: sorted(d for m, d in pairs if m == n) for n in dupes} }")
    foreign = {op: dist for op, dist in pairs if dist != ENGINE}
    if foreign:
        out.append(f"operators not from {ENGINE}: {foreign}")
    return out


def installed_problems(ours: Iterable[str]) -> list[str]:
    """D7.3: of cairn's own distributions, the venv holds exactly the three base wheels."""
    got = sorted(ours)
    return [] if got == sorted(BASE) else [f"cairn distributions installed: {got}, not {sorted(BASE)}"]


def run_problems(run: Mapping, operators: Mapping[str, str], hosted: Iterable[str],
                 want: Mapping) -> list[str]:
    """D7.4: the in-process run of :data:`PIPELINE`.

    ``run`` is :func:`_probe_run`'s summary: ``status``; ``legs`` (``id``, ``kind``, ``status``,
    ``run_id``, ``template_ref``, ``run_status``, ``op_file``); ``invocations`` (``path``,
    ``operator_ref``, ``capability_ref``, ``distributions``); ``resolution`` (the record's
    closure without its invocations); ``prefix`` (the venv's)."""
    out = []
    hosted = set(hosted)
    if sorted(run.get("approved", ())) != sorted(PERSON_GATES):
        out.append(f"the run approved {sorted(run.get('approved', ()))}, not exactly the person gates "
                   f"{sorted(PERSON_GATES)}")
    if run.get("status") != "done":
        out.append(f"the pipeline ended {run.get('status')!r}, not 'done': "
                   + str({leg["id"]: leg["status"] for leg in run.get("legs", ())}))
    legs = list(run.get("legs", ()))
    tasks = [leg for leg in legs if leg["kind"] == "task"]
    if not tasks:
        out.append("the run recorded no task leg")
    for leg in legs:
        if leg["status"] != "done":
            out.append(f"leg {leg['id']} is {leg['status']!r}, not 'done'")
    for leg in tasks:
        ref = leg.get("template_ref") or ""
        op = ref.removeprefix("task:")
        if not leg.get("run_id") or not ref.startswith("task:"):
            out.append(f"task leg {leg['id']} has no Run naming its operator (run {leg.get('run_id')!r}, "
                       f"template_ref {ref!r})")
            continue
        if leg.get("run_status") != "done":
            out.append(f"task leg {leg['id']}'s Run is {leg.get('run_status')!r}, not 'done'")
        if operators.get(op) != ENGINE:
            out.append(f"task leg {leg['id']} ran {op!r}, which {ENGINE} does not declare "
                       f"(declared by {operators.get(op)!r})")
        prefix, op_file = run.get("prefix"), leg.get("op_file")
        if not prefix or not op_file or not Path(op_file).resolve().is_relative_to(Path(prefix).resolve()):
            out.append(f"task leg {leg['id']}'s operator {op!r} loads from {leg.get('op_file')!r}, "
                       f"not from the venv {prefix!r}")
    invoked = {inv["path"]: inv for inv in run.get("invocations", ())}
    for leg in tasks:
        inv = invoked.get(leg["id"])
        if inv is None:
            out.append(f"task leg {leg['id']} was admitted with no contract invocation")
            continue
        if inv["operator_ref"] != (leg.get("template_ref") or "").removeprefix("task:"):
            out.append(f"task leg {leg['id']}'s invocation names {inv['operator_ref']!r}, "
                       f"its Run {leg.get('template_ref')!r}")
        foreign = sorted(set(inv["distributions"]) - hosted) or (
            [] if inv["distributions"] else ["(no definition)"])
        if foreign:
            out.append(f"task leg {leg['id']}'s capability {inv['capability_ref']} is defined by "
                       f"{foreign}, not by a contract package {ENGINE} hosts")
    if dict(run.get("resolution") or {}) != dict(want):
        out.append("the record's contract closure differs from the pre-fold snapshot: "
                   + _closure_diff(run.get("resolution") or {}, want))
    return out


def _closure_diff(got: Mapping, want: Mapping) -> str:
    def rows(d: Mapping, key: str) -> set[str]:
        return {json.dumps(r, sort_keys=True) for r in d.get(key, ())}
    parts = []
    for key in ("distributions", "definitions"):
        extra, missing = rows(got, key) - rows(want, key), rows(want, key) - rows(got, key)
        if extra or missing:
            parts.append(f"{key}: +{sorted(extra)} -{sorted(missing)}")
    if got.get("format") != want.get("format"):
        parts.append(f"format {got.get('format')!r} != {want.get('format')!r}")
    return "; ".join(parts) or "(key sets differ)"


def _report(problems: list[str]) -> None:
    if problems:
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        raise SystemExit(1)


# ── the in-venv probes (this file under ``python -I --probe``) ─────────────────────────────

def _builtin_operators() -> list[tuple[str, str]]:
    """Every ``cairn.operators`` entry point as ``(name, distribution)``, duplicates kept."""
    import importlib.metadata as md
    return [(ep.name, ep.dist.name) for ep in md.entry_points(group="cairn.operators")]


def _contract_hosts() -> dict[str, set[str]]:
    import importlib.metadata as md
    hosts: dict[str, set[str]] = {}
    for dist in md.distributions():
        for f in dist.files or ():
            parts = str(f).split("/")
            if parts[0] == "cairn_contracts" and len(parts) > 2:
                hosts.setdefault(parts[1], set()).add(dist.metadata["Name"])
    return hosts


def _probe_contracts(want: dict) -> None:
    import importlib.metadata as md

    from cairn.core.pack.contracts import resolve_contracts
    from cairn.core.pack.registry import contract_resolution_snapshot

    names = [d.name for d in resolve_contracts()]
    operators = _builtin_operators()
    _report(contract_problems(names, _contract_hosts(), contract_resolution_snapshot(), want)
            + operator_problems(operators))
    installed = sorted(d.metadata["Name"] for d in md.distributions()
                       if d.metadata["Name"].startswith(("cairn-", "cairndev")))
    print(f"contracts: {len(names)} packages hosted by {ENGINE}, identities equal to the pre-fold snapshot")
    print(f"operators: {len(operators)} cairn.operators entry points, all from {ENGINE}")
    print(f"installed: {', '.join(installed)}")


async def _run_hello(sample: str) -> dict:
    """What ``cairn pipeline run`` does (``cli/commands/pipeline.py``), plus the approvals
    ``cairn remote advance`` gives, in this one process."""
    import asyncio
    import importlib.metadata as md
    import inspect

    from cairn.core import liveness
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.dispatch.pipeline_launch import launch_pipeline
    from cairn.core.engine.runner import Runner
    from cairn.core.storage import build_storage

    dispatcher = RunDispatcher(Runner(), backend=build_storage())
    bridge = PipelineBridge(dispatcher)
    await liveness.start_process_owner(dispatcher.store)
    await bridge.start()
    approved: list[str] = []
    try:
        rec = await launch_pipeline(f"pipeline:{PIPELINE}", inputs={"path": sample}, bridge=bridge,
                                    environment="sandbox", trigger={"source": "check_folded_wheels"})
        deadline = asyncio.get_running_loop().time() + 300
        while True:
            rec = await bridge.store.get(rec.pipeline_id)
            if rec.status in ("done", "failed", "cancelled"):
                break
            waiting = [s for s, st in rec.step_state.items() if st.status == "awaiting_approval"]
            for step in waiting:
                if step not in PERSON_GATES or step in approved:
                    raise SystemExit(f"unexpected wait for a person at {step!r}")
                print(f"  approve {step}", flush=True)
                await bridge.advance_step(rec.pipeline_id, step, approver="check-folded-wheels")
                approved.append(step)
            if asyncio.get_running_loop().time() > deadline:
                raise SystemExit(f"the pipeline is stuck {rec.status!r}: "
                                 + str({k: v.status for k, v in rec.step_state.items()}))
            await asyncio.sleep(0.05)
        kinds = {s.id: s.ref.split(":", 1)[0] for s in rec.plan.steps}
        legs = []
        for sid, st in rec.step_state.items():
            run = await dispatcher.store.get(st.run_id) if st.run_id else None
            ref = getattr(run, "template_ref", None)
            op_file = None
            if kinds.get(sid) == "task" and ref and ref.startswith("task:"):
                (ep,) = md.entry_points(group="cairn.operators", name=ref.removeprefix("task:")) or (None,)
                op_file = inspect.getfile(ep.load()) if ep is not None else None
            legs.append({"id": sid, "kind": kinds.get(sid), "status": st.status, "run_id": st.run_id,
                         "template_ref": ref, "run_status": getattr(run, "status", None),
                         "op_file": op_file})
        res = rec.plan.contract_resolution
        closure = res.model_dump(mode="json")
        invocations = closure.pop("invocations", [])
        return {"pipeline_id": rec.pipeline_id, "status": rec.status, "approved": approved,
                "legs": legs, "resolution": closure, "prefix": sys.prefix,
                "invocations": [{"path": i["path"], "operator_ref": i["operator_ref"],
                                 "capability_ref": i["capability_ref"],
                                 "distributions": sorted({d["distribution"] for d in i["definitions"]})}
                                for i in invocations if i.get("surface", "operation") == "operation"]}
    finally:
        bridge.stop()
        await liveness.stop_process_owner()


def _probe_run(want: dict) -> None:
    import asyncio

    from cairn.core import pack as pack_api
    from cairn.core.pack import loader
    from cairn.profiles import default_dev_profile

    default_dev_profile()  # as ``cairn pipeline run`` does: local compiles get local-dev
    loader.load_all_packs()
    pack = pack_api.get_pack("hello-cairn")
    if pack is None:
        raise SystemExit(f"hello-cairn is not installed: {loader.pack_load_failures()}")
    sample = Path(pack.pipelines_dir).parent / "data" / "sample.jsonl"
    if not sample.is_file() or not sample.resolve().is_relative_to(Path(sys.prefix).resolve()):
        raise SystemExit(f"the sample is not the cairn-pack-hello wheel's: {sample}")
    run = asyncio.run(_run_hello(str(sample)))
    hosted = [n for n, h in _contract_hosts().items() if h == {ENGINE}]
    _report(run_problems(run, dict(_builtin_operators()), hosted, want))
    print(f"run: {PIPELINE} {run['pipeline_id']} ended {run['status']} in process "
          f"(approved {', '.join(run['approved'])})")
    for leg in run["legs"]:
        print(f"  {leg['id']:13s} {leg['kind']:4s} {leg['status']:5s} {leg['template_ref'] or '-'}")
    for inv in run["invocations"]:
        print(f"  {inv['path']:13s} {inv['operator_ref']} -> {inv['capability_ref']} "
              f"({', '.join(inv['distributions'])}, hosted by {ENGINE})")


# ── the gate ────────────────────────────────────────────────────────────────────────────────

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


def _fail(problems: list[str]) -> None:
    if problems:
        raise SystemExit("[check_folded_wheels] FAILED: " + "; ".join(problems))


def wheel_names(dist: Path) -> set[str]:
    return {p.name.split("-")[0] for p in dist.glob("*.whl")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dist", type=Path, default=None, help="build into this directory (default: a temporary one)")
    ap.add_argument("--keep", action="store_true", help="keep the temporary venv and dist")
    ap.add_argument("--python", default=f"{sys.version_info.major}.{sys.version_info.minor}",
                    help="the interpreter the venv is created with (default: this one's version)")
    ap.add_argument("--probe", choices=("contracts", "run"), help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    if a.probe:  # inside the clean venv
        want = json.loads(SNAPSHOT.read_text())
        (_probe_contracts if a.probe == "contracts" else _probe_run)(want)
        return 0
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("check_folded_wheels: uv is not on PATH")
    work = Path(tempfile.mkdtemp(prefix="cairn-folded-"))
    dist = (a.dist or work / "dist").resolve()
    if dist.exists() and any(dist.iterdir()):
        raise SystemExit(f"check_folded_wheels: {dist} is not empty; the gate counts every wheel in it")
    try:
        _run("uv build --all-packages", [uv, "build", "--all-packages", "-o", str(dist)], quiet=True)
        _fail(wheel_problems(wheel_names(dist)))
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
        probe = [str(python), "-I", str(Path(__file__).resolve()), "--probe"]
        _run("check_public_set --extras hello",
             [str(python), str(ROOT / "scripts" / "check_public_set.py"), "--extras", "hello"], cwd=here, env=env)
        _run("cairn pack check", [str(bin_ / "cairn"), "pack", "check"], cwd=here, env=env, quiet=True)
        _run("contracts, operators and the installed set", [*probe, "contracts"], cwd=here, env=env)
        # The engine's job-machine script takes no flags: with no spec it refuses by name, which
        # proves the console script resolves into the engine wheel and imports the runner.
        print("[check_folded_wheels] cairn-dataflow-run (no spec: its own refusal)", flush=True)
        ran = subprocess.run([str(bin_ / "cairn-dataflow-run")], cwd=here, env=env, text=True,
                             capture_output=True)
        if "cairn-dataflow-run: no spec" not in ran.stdout + ran.stderr:
            sys.stdout.write(ran.stdout)
            sys.stderr.write(ran.stderr)
            raise SystemExit("[check_folded_wheels] FAILED: cairn-dataflow-run did not start")
        _run(f"cairn pipeline explain {PIPELINE}",
             [str(bin_ / "cairn"), "pipeline", "explain", PIPELINE], cwd=here, env=env, quiet=True)
        # D7.4: the run. Its state lives in the work directory; no key reaches it.
        state = work / "run"
        state.mkdir()
        run_env = {k: v for k, v in env.items() if k not in PROVIDER_KEYS} | {
            "CAIRN_BACKEND": "mock", "CAIRN_STORAGE": f"sqlite:///{state / 'cairn.db'}",
            "CAIRN_ARTIFACTS_DIR": str(state / "artifacts"), "OBS_ARTIFACTS_DIR": str(state / "artifacts"),
            "CAIRN_AUDIT_LOG": str(state / "audit.jsonl"), "HOME": str(state / "home")}
        _run(f"run {PIPELINE} in process (mock backend, no server, no network)",
             [*probe, "run"], cwd=state, env=run_env)
        installed = json.loads(_run("the installed set", [uv, "pip", "list", "--python", str(python),
                                                          "--format", "json"], quiet=True))
        _fail(installed_problems(d["name"] for d in installed
                                 if d["name"].startswith(("cairn-", "cairndev"))))
    finally:
        if not a.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"[check_folded_wheels] kept {work}")
    print("[check_folded_wheels] OK: five wheels; the engine wheel carries the builtins and the core "
          f"contracts; {PIPELINE} ran in process to done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
