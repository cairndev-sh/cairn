"""Stage registry (ADR-0081 §5, #1048/#1049) — core since #2583 (ADR-0129 D1).

Stages are KERNEL entry-points (groups ``cairn.dataflow_stages`` and
``cairn.flow_stages``) — the builtin runner and the Metaflow and SkyPilot
adapters all resolve the SAME callables, so an adapter is never a
rewrite. A stage is a plain callable::

    stage(workdir: Path, config: dict, inputs: list[str]) -> dict   # scalar stats

Core reads the entry-point GROUPS; it never imports the pack that provides a
kernel (the ``engine_registry`` rule, ADR-0084 D1). Heavy imports live INSIDE
stage impls — the registry and runner stay stdlib-only (D5: the base wheel
carries no data deps). The two conformance stages (``noop`` / ``noop.slow``)
are defined here because the conformance suite that needs them is core's
(:mod:`cairn.core.dataflow.conformance`); the builtin dataflow pack registers
them under its own entry points and re-exports them.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import Callable

_log = logging.getLogger("cairn.dataflow.stages")

StageFn = Callable[[Path, dict, list[str]], dict]

# Legacy group + the engine-neutral alias (ADR-0082 D7, #1074) — ONE merged
# registry serves every flow kind; mirrors core's defs._stage_entry_points.
_GROUPS = ("cairn.dataflow_stages", "cairn.flow_stages")


#: #2069 — why each kernel was dropped, from the last :func:`load_stages`.
#: A stage that is INSTALLED but will not import is not "unknown"; it is broken,
#: and the difference is the whole diagnosis. Keyed by stage name.
_LOAD_FAILURES: dict[str, str] = {}


def stage_load_failures() -> dict[str, str]:
    """``{stage: reason}`` for kernels the last load could not import.

    The registry stays lenient — one broken pack must not hide the rest — but
    the reason is remembered so the runner, the doctor and the console can say
    "installed but failed to import: ModuleNotFoundError: boto3" instead of
    "unknown stage". Read after :func:`load_stages`.
    """
    return dict(_LOAD_FAILURES)


@lru_cache(maxsize=1)
def stage_versions() -> dict[str, str]:
    """``stage name → distribution version`` for every registered kernel
    (#2188, T03): the implementation that will run, read from the entry point's
    own distribution. Best effort — a stage whose distribution cannot be
    resolved simply has no version, never a raised error."""
    out: dict[str, str] = {}
    for group in _GROUPS:
        eps = entry_points(group=group)
        for ep in eps:
            dist = getattr(ep, "dist", None)
            version = getattr(dist, "version", None) if dist is not None else None
            if version:
                out.setdefault(ep.name, str(version))
    return out



def load_stages() -> dict[str, StageFn]:
    """Discover stages from entry-points (mirrors the operator loader).

    Lenient by design, but never SILENT (#2069): a kernel whose module raises
    on import is dropped from the registry AND recorded in
    :func:`stage_load_failures`, with a warning naming the stage and the cause.
    Before this, the drop was a bare ``continue`` — a stage with a missing
    dependency simply was not there, and the run failed with "unknown stages",
    which sent two GPU runs chasing the wrong thing (#2086).
    """
    found: dict[str, StageFn] = {}
    _LOAD_FAILURES.clear()
    for group in _GROUPS:
        for ep in entry_points(group=group):
            if ep.name in found:
                continue
            try:
                found[ep.name] = ep.load()
            except Exception as e:  # noqa: BLE001 — one broken stage pack must not hide the rest
                _LOAD_FAILURES[ep.name] = f"{type(e).__name__}: {e}"
                _log.warning(
                    "stage %r is installed (group %s) but failed to import: %s: %s "
                    "— it will look UNKNOWN to any flow that uses it",
                    ep.name, group, type(e).__name__, e,
                )
                continue
    # editable/dev fallback: the conformance stages are always available even
    # when the dataflow pack's entry-points aren't installed (e.g. a bare
    # checkout on a box).
    found.setdefault("noop", noop)
    found.setdefault("noop.slow", noop_slow)
    return found


def noop(workdir: Path, config: dict, inputs: list[str]) -> dict:
    """The conformance stage: touches nothing, reports zero rows.

    #2632 — with ``register: <name>`` it also names the data it was handed as a dataset
    version, the one way the public core gains one: ``source`` (a local file; a relative
    path resolves against the submitter's cwd, the builtin stages' convention) is hashed,
    and the control plane registers ``<name>`` at that content from the descriptor's
    ``register_as`` (``dispatch/leg_executor._register_dataset_versions``), exactly as it
    does for a platform export stage. Same content, same hash: a re-run adds no version.
    Nothing is written or copied, so the stage stays ``read_only``. ``register_role:`` says
    what the version is for (``training``, ``validation`` or ``test``), so the holdout door
    can see a test set; unset keeps the dataset's default role.
    """
    name = str(config.get("register") or "").strip()
    if not name:
        return {"rows": 0}
    src = str(config.get("source") or "").strip()
    if not src:
        raise ValueError("noop: `register:` needs `source:`, the file whose content is the version")
    path = Path(src)
    submit_cwd = os.environ.get("CAIRN_SUBMIT_CWD", "")
    if not path.is_absolute() and submit_cwd:
        path = Path(submit_cwd) / path
    if not path.is_file():
        raise ValueError(f"noop: the register source {src!r} is not a readable file")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    # `output` is the file itself, so the descriptor's URI names real bytes (the
    # durable relay copies them, and the version and any tag resolve to them); a
    # stage without `output` gets a `stage-NN` placeholder URI holding nothing.
    role = str(config.get("register_role") or "").strip()
    return {"rows": 0, "register_as": name, "content_hash": f"sha256:{digest[:16]}",
            "bytes": path.stat().st_size, "output": str(path.resolve()),
            **({"register_role": role} if role else {})}


def noop_slow(workdir: Path, config: dict, inputs: list[str]) -> dict:
    """Conformance's long-running stage: sleeps in small slices so an external
    kill (cancel) lands promptly. ``config.seconds`` bounds it (default 30)."""
    deadline = time.monotonic() + float(config.get("seconds", 30))
    while time.monotonic() < deadline:
        time.sleep(0.1)
    return {"rows": 0}


# risk classes (ADR-0081 D2, #1051): undeclared = external_write (fail-closed);
# the conformance stages genuinely touch nothing.
noop.risk_class = "read_only"
noop_slow.risk_class = "read_only"
