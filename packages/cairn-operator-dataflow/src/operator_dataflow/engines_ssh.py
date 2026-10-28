"""SSH dispatch for the builtin engine (ADR-0081 P1c slice 3, #1050).

The same ``cairn-dataflow-run`` binary the local subprocess runs, launched on
a box YOU own via the PROVEN ``remote/exec`` machinery — imported, never
re-implemented (the #1048 anti-deviation rule). This module composes exec.py's
battle-tested functions (`_submit`/`_poll`/`_cancel`: connection reuse #337,
idempotent adopt #551, stale-group kill #708, detached launch #601, the poll
race guard, group cancel #603) around the dataflow file contract
(``CAIRN_DATAFLOW_SPEC`` env in → ``result.json``/``metrics.json`` out).

Selected by the environment: ``CAIRN_DATAFLOW_ENGINE=builtin`` +
``CAIRN_DATAFLOW_TARGET=<compute.ssh connection>`` (profile.backends). Stateless
on the control plane — everything poll/cancel need rides in the handle meta,
so it survives restarts like fix-remote did.

Requires the ``cairndev[ssh]`` extra (asyncssh for ``remote/exec``) — imports are lazy
so the base wheel stays dep-free (D5).
"""
from __future__ import annotations
from cairn.core.operator import getenv

import dataclasses
import json
import logging
import os
import re
import shlex
import shutil
from collections.abc import Iterable, Mapping
from urllib.parse import urlparse
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

from cairn.core.dataflow import (
    DataflowResult,
    DataflowSpec,
    EngineError,
    EngineJobHandle,
    EngineJobStatus,
)
from cairn.core.dataflow.resources import ESSENTIAL_ENV_NAMES
from cairn.core.operator.jobs import live_channel_env_for
# The fetch contract (which files come back, where they land, the on-box →
# local path rewrite) is core's since #2583 (ADR-0129 D1); the names stay
# importable from here for one release (ADR-0129 D4).
from cairn.core.dataflow.fetch import (  # noqa: F401 — re-exports
    BOOKKEEPING_OUTPUTS,
    _STAGE_OUTPUTS,
    fetch_dir,
    fetch_list,
    rewrite_fetched,
)

from operator_dataflow.engines import BuiltinLocalEngine, _selected

#: What image mode with no image named is told (#202). There is no built-in
#: image: the deployment names the build it runs, so the run record names it
#: too. PIN A TAG, never ``:latest`` — otherwise the record cannot say which
#: build produced the artifacts. Two triggers, two remedies: the environment's
#: ``remote_exec_mode`` does not override a connection's own ``mode``.
IMAGE_REQUIRED = (
    "remote_exec_mode is 'image' but no image is named: set the remote_image "
    "backend (CAIRN_TRAIN_IMAGE) or the compute.ssh connection's 'image' to a "
    "pinned tag, or set remote_exec_mode to 'auto' or 'venv' to run the box's "
    "virtualenv"
)
CONNECTION_IMAGE_REQUIRED = (
    "the compute.ssh connection sets mode = 'image' but no image is named: set "
    "the connection's 'image' (or the remote_image backend, CAIRN_TRAIN_IMAGE) "
    "to a pinned tag, or change the connection's 'mode' to 'auto' or 'venv' — "
    "remote_exec_mode does not override a connection's own mode"
)

#: Env names the CONTAINER needs that the engine does not put in ``inp.env``
#: itself: ``_submit`` adds CAIRN_RUN_ID after the command is built (#552), so
#: forwarding only the names we know here would drop it and the box's per-run
#: checkpoint keys would collide across runs.
_EXTRA_FORWARDED = ("CAIRN_RUN_ID",)

_log = logging.getLogger("operator_dataflow.engines_ssh")

#: ``remote_env`` — the deployment's list of EXTRA env names the job on the box
#: receives from the control plane's environment (e.g. ``CAIRN_CHECKPOINT_BUCKET``,
#: ``AWS_DEFAULT_REGION``). Declared in the profile (``[profile.backends]``) or
#: ``CAIRN_REMOTE_ENV``, never by a flow or a tenant. Names only: a value rides
#: the job's owner-only ``runner.sh`` (venv) or ``docker run -e NAME`` (image),
#: never a command line. Fails closed on any name below.
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
#: Credentials stay on the box (an instance role, the box's own env): a name that
#: looks like one is refused, whatever its value.
_CREDENTIAL_MARKERS = ("KEY", "SECRET", "TOKEN", "PASSWORD")
#: The engine's own per-submission contract (spec, lineage, live channel): a
#: control-plane value must never replace it.
_ENGINE_OWNED = frozenset(n for n in ESSENTIAL_ENV_NAMES if n.startswith(("CAIRN_", "OBS_")))


def remote_env_names() -> tuple[list[str], list[str]]:
    """``(names to forward, problems)`` for the deployment's ``remote_env``.

    A problem names the variable and the rule, never a value."""
    names: list[str] = []
    problems: list[str] = []
    entries = [e for e in re.split(r"[\s,]+", _selected("remote_env", "").strip()) if e]
    for pos, name in enumerate(entries, start=1):
        if name in names:
            continue
        if not _ENV_NAME.match(name):
            # The entry's position only: any part of a malformed entry may be a
            # pasted secret (an upper-case base32 seed with `=` padding reads exactly
            # like `NAME=`), so none of it is echoed.
            problems.append(f"entry {pos} is not an environment variable name")
        elif any(m in name.upper() for m in _CREDENTIAL_MARKERS):
            problems.append(f"{name} looks like a credential; credentials stay on the box "
                            "(an instance role or the box's own environment), never in the job")
        elif name in _ENGINE_OWNED:
            problems.append(f"{name} is set by the engine for each submission")
        else:
            names.append(name)
    return names, problems


def _control_plane_only(value: Any, where: str = "") -> list[tuple[str, str]]:
    """``(where, location)`` for every launch-resolved dataset version
    (``<uri>#sha256=<hash>``, see ``resolve_dataset_refs_in_config``) in ``value``,
    searched through nested dicts and lists, whose location is a control-plane
    path: absolute, or any ``file:`` URI (``file:///x`` and ``file:/x`` alike, the
    registry's ``local_path`` semantics). A plain path with no pin is an upstream
    leg's hand-off, which :meth:`BuiltinSshEngine._localize_inputs` ships."""
    if isinstance(value, dict):
        return [hit for k, v in value.items()
                for hit in _control_plane_only(v, f"{where}.{k}" if where else str(k))]
    if isinstance(value, list):
        return [hit for i, v in enumerate(value) for hit in _control_plane_only(v, f"{where}[{i}]")]
    if not isinstance(value, str) or "#sha256=" not in value:
        return []
    loc = value.partition("#")[0]
    return [(where, loc)] if loc.startswith("/") or urlparse(loc).scheme == "file" else []


def _remote_command() -> str:
    """The on-box entrypoint. Defaults to the console script on PATH (the
    deployment contract: the box installs the same wheels). ``CAIRN_DATAFLOW_REMOTE_CMD``
    overrides it for boxes whose venv bin isn't on the non-interactive SSH PATH —
    the proven ``fix_command`` lesson (#1031), now a first-class deployment knob
    instead of a per-run input (exit 127 otherwise, #1075)."""
    return getenv("CAIRN_DATAFLOW_REMOTE_CMD", "").strip() or "cairn-dataflow-run"


def _image_command() -> str:
    """The entrypoint INSIDE the image. Deliberately NOT
    ``CAIRN_DATAFLOW_REMOTE_CMD`` — that knob names a venv bin path on the BOX
    (#1075), which does not exist in the container; leaking it in is exit 127.
    ``CAIRN_DATAFLOW_IMAGE_CMD`` is the image-side equivalent."""
    return getenv("CAIRN_DATAFLOW_IMAGE_CMD", "").strip() or "cairn-dataflow-run"


def resolve_exec_mode(config: Mapping[str, Any]) -> tuple[str, str, str]:
    """Decide how this box runs the flow: ``(mode, image, gpus)``.

    ADR-0104 D1. ONE function so the precedence is readable in one place
    instead of scattered across submit:

    * ``mode`` — the connection's ``mode``, else the environment's
      ``remote_exec_mode`` backend (profile record → ``CAIRN_REMOTE_EXEC_MODE``
      → default), else ``auto``. ``auto`` means "image if anything names an
      image, otherwise the box virtualenv" — so a deployment that names no
      image is NOT flipped to containers behind its operator's back.
    * ``image`` — the connection's ``image`` (it names THIS box's build) beats
      the environment's ``remote_image`` backend / ``CAIRN_TRAIN_IMAGE`` (the
      fleet default). There is no built-in default (#202): ``image`` mode with
      neither raises :class:`EngineError` — :data:`CONNECTION_IMAGE_REQUIRED`
      when the connection's own ``mode`` chose image, else
      :data:`IMAGE_REQUIRED`. This runs at submit, where the connection and
      the environment are both known, before anything reaches the box.
    * ``gpus`` — the ``--gpus`` value; empty string disables the flag (a CPU
      box, or a runtime without the NVIDIA container toolkit).

    ``mode == "venv"`` always returns an empty image: the venv path must be
    byte-for-byte what it was, including its handle meta.
    """
    mode = str(config.get("mode") or "").strip().lower()
    from_connection = bool(mode)
    if not mode:
        mode = _selected("remote_exec_mode", "auto").strip().lower() or "auto"
    named = (
        str(config.get("image") or "").strip()
        or _selected("remote_image", "").strip()
    )
    if mode not in ("image", "venv"):
        mode = "image" if named else "venv"
    if mode == "venv":
        return "venv", "", ""
    gpus = str(
        config.get("gpus")
        if config.get("gpus") is not None
        else getenv("CAIRN_DATAFLOW_IMAGE_GPUS", "all")
    ).strip()
    if not named:
        raise EngineError(CONNECTION_IMAGE_REQUIRED if from_connection else IMAGE_REQUIRED)
    return "image", named, gpus


def _docker_command(image: str, inner: str, env_names: Iterable[str], gpus: str) -> str:
    """The remote command line for image mode.

    Shape notes, each of which is load-bearing:

    * **pull once** — ``docker image inspect || docker pull``: a bare
      ``docker pull`` on every dispatch re-downloads multiple GB on a metered
      box. A pinned tag makes the inspect a valid cache check (``:latest``
      would not).
    * **``$PWD``** — ``runner.sh`` runs with cwd = the job workdir (exec.py
      launches it as ``cd <workdir> && … sh runner.sh``), so ``$PWD`` binds
      the workdir without this engine having to predict exec.py's workdir
      naming. Outputs, ``checkpoints/`` and the uploaded inputs therefore land
      exactly where the fetch-back and ``fetch_checkpoints`` already look.
    * **``-e NAME`` with no value** — docker forwards the value from the
      surrounding shell, which ``runner.sh`` has already exported. The per-run
      job-channel token therefore never appears in the container's argv, where
      every ``ps`` on the box could read it.
    * **``--entrypoint sh``** — a training image's own ENTRYPOINT is its
      training runner, so the flow kernel needs an explicit override; going
      through ``sh -c`` keeps ``CAIRN_DATAFLOW_IMAGE_CMD`` free to be any
      command, not just a bare binary.
    """
    img = shlex.quote(image)
    gpu_flag = f" --gpus {shlex.quote(gpus)}" if gpus else ""
    forwarded = "".join(f" -e {shlex.quote(n)}" for n in env_names)
    return (
        f"docker image inspect {img} >/dev/null 2>&1 || docker pull {img}; "
        f"docker run --rm{gpu_flag} -v \"$PWD\":/work -w /work{forwarded} "
        f"--entrypoint sh {img} -c {shlex.quote(inner)}"
    )


#: What an install without the [ssh] extra is told, instead of an ImportError
#: traceback (#2417). The extra is what puts asyncssh (remote/exec's transport) here.
SSH_EXTRA_MISSING = (
    "the builtin engine's SSH dispatch (CAIRN_DATAFLOW_TARGET) needs the ssh extra "
    "(asyncssh): pip install 'cairndev[ssh]'"
)
_SSH_EXTRA_MODULES = frozenset({"operator_remote", "asyncssh"})


def _raise_if_ssh_extra_missing(e: ModuleNotFoundError) -> None:
    """Turn "the [ssh] extra is not installed" into an EngineError naming it.

    Only a module the extra provides is translated; any other missing module is
    a real fault inside an installed pack and propagates unchanged."""
    if (e.name or "").split(".")[0] in _SSH_EXTRA_MODULES:
        raise EngineError(SSH_EXTRA_MISSING) from e


def _remote_exec():
    # optional dependency: the cairndev[ssh] extra (asyncssh) — lazy so the
    # base install imports without it. Known debt: a builtin importing another builtin's
    # module (#2419); the transport (asyncssh) arrives with cairndev[ssh].
    try:
        from operator_remote.operators import exec as remote_exec  # noqa: PLC0415
    except ModuleNotFoundError as e:
        _raise_if_ssh_extra_missing(e)
        raise
    return remote_exec


def _remote_exec_input():
    """The exec input model, behind the same [ssh]-extra seam as :func:`_remote_exec`."""
    try:
        from operator_remote.schemas.exec import RemoteExecInput  # noqa: PLC0415 - [ssh] extra
    except ModuleNotFoundError as e:
        _raise_if_ssh_extra_missing(e)
        raise
    return RemoteExecInput


class BuiltinSshEngine:
    """Run ``cairn-dataflow-run`` on a ``compute.ssh``-connected box."""

    name = "builtin-ssh"
    #: ADR-0149 — the job runs on the box, which cannot read a control-plane path:
    #: the leg executor resolves `dataset:` refs to their durable copy for it.
    needs_durable_inputs = True
    #: ADR-0118 D7 — kernels run through `run_dataflow`, which checks stage contracts.
    carries_stage_contracts = True

    def __init__(self, target: str) -> None:
        if not target.strip():
            raise EngineError("BuiltinSshEngine needs a compute.ssh connection name")
        self._target = target.strip()

    # local capability discovery — the box installs the same wheels (the
    # fix-remote deployment contract), so the local registry is authoritative.
    # Plus what the box could never run with: a refused `remote_env`, and a
    # dataset version that exists only on the control plane.
    def validate(self, spec: DataflowSpec) -> list[str]:
        problems = BuiltinLocalEngine().validate(spec)
        _, env_problems = remote_env_names()
        if env_problems:
            problems.append("remote_env: " + "; ".join(env_problems))
        found = _control_plane_only(spec.config, "spec config")
        for ref in spec.stages:
            found += _control_plane_only(ref.config, f"stage {ref.stage!r} config")
        for where, loc in found:
            problems.append(
                f"{where}: dataset version {loc} has no durable copy and exists only on "
                f"the control plane, which the box behind {self._target!r} cannot read. "
                "Configure the object store on the control plane (CAIRN_ARTIFACT_STORE=s3 "
                "and CAIRN_ARTIFACT_S3_BUCKET) and register the version again while its "
                "file is still there, or re-run the step that produced it; the ref then "
                "resolves to the durable copy")
        return problems

    async def _exec_ctx(self, ctx: Any) -> SimpleNamespace:
        from cairn.core.operator import resolve_connection  # noqa: PLC0415 - lazy: keeps engine importable off the engine path

        return SimpleNamespace(
            compute=await resolve_connection(self._target),
            pipeline_id=getattr(ctx, "pipeline_id", None),
            node_name=getattr(ctx, "node_name", None) or "dataflow",
        )

    @staticmethod
    def _fetch_list(spec: DataflowSpec) -> list[str]:
        return fetch_list(spec)

    @staticmethod
    def _localize_inputs(spec: DataflowSpec) -> tuple[DataflowSpec, list[str]]:
        """Ship LOCAL-FILE inputs to the box (#1180 bug 10 — cross-host leg hand-off).

        A pipeline's upstream leg may run on the control plane and hand this leg
        a control-side path (`dataset_uri=/tmp/cairn-dataflow-…/output.jsonl`);
        submitted verbatim, the box FileNotFounds on a path that only exists
        here. Symmetric with the output fetch-back: every config value that is
        an absolute path to an existing local file is SFTP'd into the job
        workdir (the exec machinery's `upload`) and the config rewritten to the
        workdir-relative basename — on a box, relative = on-box."""
        uploads: list[str] = []
        stages = []
        for ref in spec.stages:
            cfg = dict(ref.config)
            for k, v in list(cfg.items()):
                if isinstance(v, str) and os.path.isabs(v) and os.path.isfile(v):
                    if v not in uploads:
                        uploads.append(v)
                    cfg[k] = os.path.basename(v)
            stages.append(ref.model_copy(update={"config": cfg}))
        if not uploads:
            return spec, []
        return spec.model_copy(update={"stages": stages}), uploads

    @staticmethod
    def _localize_resume(spec: DataflowSpec) -> tuple[DataflowSpec, list[str]]:
        """Ship ``resume_from`` checkpoint bytes to the box (H6d, ADR-0088 D4).

        The control plane builds ``resume_from`` with durable-root ``file://``
        URIs that exist only HERE; the box's ``_seed_resume`` would fail on
        them. Symmetric with ``_localize_inputs``: each locally-readable
        artifact rides the sftp upload (landing at its ``dest`` basename in
        the workdir — checkpoint files are top-level by construction, so the
        basename IS the dest) and is REMOVED from the block — the channel
        already materialized it, so seeding it again is a no-op at best and
        a same-file error at worst. http(s) URIs stay (the box can fetch
        those itself); an unreadable file:// stays too, so the runner fails
        LOUDLY on genuinely lost bytes instead of resuming without them.
        """
        rf = spec.resume_from
        if rf is None:
            return spec, []
        uploads: list[str] = []
        remaining: list[dict] = []
        for art in rf.artifacts:
            uri = str(art.get("uri", ""))
            path = uri[len("file://"):] if uri.startswith("file://") else ""
            dest = str(art.get("dest") or "")
            # only a plain-basename dest lands where the upload puts it
            if path and os.path.isfile(path) and dest == os.path.basename(path):
                uploads.append(path)
            else:
                remaining.append(art)
        if not uploads:
            return spec, []
        return spec.model_copy(update={
            "resume_from": rf.model_copy(update={"artifacts": remaining}),
        }), uploads

    async def submit(self, spec: DataflowSpec, ctx: Any) -> EngineJobHandle:
        problems = self.validate(spec)
        if problems:
            raise EngineError("; ".join(problems))
        remote_exec = _remote_exec()
        RemoteExecInput = _remote_exec_input()  # noqa: N806 - a class, bound locally

        exec_ctx = await self._exec_ctx(ctx)
        spec, uploads = self._localize_inputs(spec)
        spec, resume_uploads = self._localize_resume(spec)
        uploads = uploads + [u for u in resume_uploads if u not in uploads]
        # the deployment's declared names first: the engine's own values below win
        forwarded, _ = remote_env_names()
        env: dict[str, Any] = {n: os.environ[n] for n in forwarded if os.environ.get(n)}
        env["CAIRN_DATAFLOW_SPEC"] = spec.model_dump_json()
        if spec.parent_run:
            env["CAIRN_PARENT_RUN"] = spec.parent_run
            env.update(live_channel_env_for(spec.parent_run, "flow"))  # #2025 — the live channel
        # NOTE: no CAIRN_SUBMIT_CWD on purpose — on a box, relative = on-box.
        # #1235 — experiment-tracking env rides along, so a TrainSpec with
        # report_to=["mlflow"] on the box reaches the control plane's tracker
        # (the direct train/* path already reads these; forwarding closes the
        # flow-path parity gap). Forwarded only when set — no new defaults.
        for _var in ("MLFLOW_TRACKING_URI", "MLFLOW_TRACKING_USERNAME",
                     "MLFLOW_TRACKING_PASSWORD", "MLFLOW_EXPERIMENT_NAME"):
            _val = os.environ.get(_var, "")
            if _val:
                env[_var] = _val
        # ADR-0104 D1 (#2071): the IMAGE is the unit of remote work. In image
        # mode the box runs the image the deployment names and carries nothing
        # of cairn's that can drift (#2069); venv mode is the box install,
        # unchanged. Image mode with no image named raises here, before the
        # exec layer is asked to run anything (#202).
        cfg = dict(getattr(exec_ctx.compute, "config", None) or {})
        mode, image, gpus = resolve_exec_mode(cfg)
        if mode == "image":
            command = _docker_command(
                image, _image_command(), [*env, *_EXTRA_FORWARDED], gpus,
            )
        else:
            command = _remote_command()
        inp = RemoteExecInput(
            command=command, env=env, upload=uploads,
            fetch=self._fetch_list(spec),
        )
        jh = await remote_exec._submit(inp, exec_ctx)
        return EngineJobHandle(
            job_id=jh.job_id, engine=self.name,
            # JobHandle is a dataclass (durable-by-design); the exec input is
            # pydantic — both serialize into the meta so poll/cancel are
            # reconstructable after a control-plane restart. `mode`/`image` ride
            # along as the run's PROVENANCE: "which build produced this adapter?"
            # is otherwise unanswerable after the fact (#1776).
            # ADR-0149 D2: the meta is persisted on the pipeline step and served by
            # the API, so the exec input goes WITHOUT its env (the job-channel
            # token, MLFLOW_TRACKING_PASSWORD, forwarded values); only the NAMES
            # are kept. Nothing after submit needs the values: poll reads `fetch`.
            meta={"job_handle": dataclasses.asdict(jh),
                  "exec_input": inp.model_dump(exclude={"env"}),
                  "env_names": sorted(env), "mode": mode, "image": image},
        )

    def _rebuild(self, handle: EngineJobHandle):
        remote_exec = _remote_exec()
        from cairn.core.operator.jobs import JobHandle  # noqa: PLC0415 - lazy: avoid core import at module load
        RemoteExecInput = _remote_exec_input()  # noqa: N806 - a class, bound locally

        jh = JobHandle(**handle.meta["job_handle"])
        inp = RemoteExecInput.model_validate(handle.meta["exec_input"])
        return remote_exec, jh, inp

    async def poll(self, handle: EngineJobHandle) -> EngineJobStatus:
        remote_exec, jh, inp = self._rebuild(handle)
        ctx = await self._exec_ctx(None)
        status = await remote_exec._poll(jh, inp, ctx)
        if status.state in ("running", "submitted", "queued"):
            return EngineJobStatus(state="running", metrics=dict(status.metrics or {}))
        if status.state == "cancelled":
            # ADR-0151 D9 — a job remote/exec's cancel killed: terminal and never
            # retriable, so execute_leg does not relaunch it and its run settles cancelled.
            return EngineJobStatus(state="cancelled", error=status.error or "remote dataflow cancelled",
                                   metrics=dict(status.metrics or {}))
        if status.state != "succeeded":
            return EngineJobStatus(
                state="failed", error=status.error or f"remote dataflow {status.state}",
                metrics=dict(status.metrics or {}), retriable=bool(status.retriable),
            )
        return EngineJobStatus(state="succeeded", metrics=dict(status.metrics or {}))

    async def cancel(self, handle: EngineJobHandle) -> None:
        remote_exec, jh, _ = self._rebuild(handle)
        # the key of the connection the job was submitted with (the target only
        # for a handle saved before that was recorded). A cancel that did not
        # reach the box is an error to the caller, never "sent" (ADR-0149 D4).
        if not await remote_exec._cancel(jh, connection=self._target):
            raise EngineError(f"could not cancel {handle.job_id}: the box was not reached "
                              "and the job may still be running")

    # ── H6b/d (#1248, ADR-0088 D4): pull declared checkpoints off the box ──
    async def fetch_checkpoints(self, handle: EngineJobHandle, dest_dir: Path) -> bool:
        """Pull the job's ``checkpoints/`` tree into ``dest_dir/checkpoints``.

        The bridge's relay tick calls this for workdirs that don't exist on
        the control plane; the pulled tree then relays through the SAME
        ``relay_local_checkpoints`` path a local workdir uses. Reconnects
        from the handle meta + the target connection's key (the ``_cancel``
        pattern — a relay tick has no OperationContext), rides the cached connection
        (#337), and pulls each ``stage-NN`` dir at most once: get into a
        ``.partial`` staging dir, rename when complete, skip dirs already
        present. Best-effort by contract — any failure returns False and
        the next beat retries; bytes cross ONLY the engine channel, the box
        never holds control-plane credentials (ADR-0081 D3).
        """
        remote_exec, jh, _ = self._rebuild(handle)
        workdir = str(jh.meta.get("workdir", "") or "")
        if not workdir:
            return False
        ckroot = dest_dir / "checkpoints"
        try:
            conn = await remote_exec._get_conn(
                await remote_exec._job_ssh_params(jh, self._target))
            async with conn.start_sftp_client() as sftp:
                try:
                    names = await sftp.listdir(f"{workdir}/checkpoints")
                except Exception:  # noqa: BLE001 — nothing checkpointed yet
                    return ckroot.is_dir() and any(ckroot.iterdir())
                got = False
                ckroot.mkdir(parents=True, exist_ok=True)
                for name in sorted(n for n in names if n.startswith("stage-")):
                    final = ckroot / name
                    # the runner writes manifest.json LAST, so its presence is
                    # the completeness marker on BOTH sides: a pulled dir
                    # without one raced the runner mid-snapshot — drop it and
                    # let the next beat retry.
                    if (final / "manifest.json").is_file():
                        got = True
                        continue
                    if final.is_dir():
                        shutil.rmtree(final)
                    tmp = ckroot / f".{name}.partial"
                    if tmp.exists():
                        shutil.rmtree(tmp)
                    tmp.mkdir(parents=True)
                    await sftp.get(f"{workdir}/checkpoints/{name}", str(tmp),
                                   recurse=True)
                    src = tmp / name            # sftp.get copies INTO the dir
                    if (src / "manifest.json").is_file():
                        src.rename(final)
                        got = True
                    shutil.rmtree(tmp, ignore_errors=True)
                return got
        except Exception as e:  # noqa: BLE001 — best-effort; the next beat retries
            # loud, not silent: an unreachable box means no checkpoints come back.
            # The error's TYPE only — SSH and key errors may quote their input.
            # the connection actually used: the handle's, else the target (ADR-0149 D4)
            used = str(jh.meta.get("connection") or "") or self._target
            _log.warning("checkpoint pull-back from %s:%s via %r failed (%s)",
                         jh.meta.get("host"), workdir, used, type(e).__name__)
            return False

    # ── result: read the FETCHED result.json; point stats at local copies ──
    @staticmethod
    def _fetch_dir(workdir: str) -> Path:
        return fetch_dir(workdir)

    async def result(self, handle: EngineJobHandle) -> DataflowResult:
        _, jh, _inp = self._rebuild(handle)
        workdir = str(jh.meta.get("workdir", ""))
        fetched = self._fetch_dir(workdir)
        result_path = fetched / "result.json"
        if not result_path.exists():
            raise EngineError(
                f"no fetched result.json for {handle.job_id} (expected {result_path})"
            )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        if "error" in payload:
            raise EngineError(f"remote dataflow failed: {payload['error']}")
        payload.pop("lineage_parent_run", None)
        result = DataflowResult.model_validate(payload)
        # remote workdir paths in stats → the fetched local copy where one
        # exists (the hoists read these paths control-side; the artifact URIs
        # keep the on-box location — that's where the data product LIVES).
        rewritten = dict(result.stats)
        for k, v in result.stats.items():
            local = self._rewrite(v, workdir, fetched)
            if local is not None:
                rewritten[k] = local
        # ADR-0104 D1 (#2071) — provenance on the flow's RESULT, which is what
        # the run record and the hoists read. The handle meta already carries
        # it for poll/cancel; this is the copy a human sees on the run.
        image = str((handle.meta or {}).get("image") or "")
        if image:
            rewritten["engine.image"] = image
        return result.model_copy(update={"stats": rewritten})

    @staticmethod
    def _rewrite(value: Any, workdir: str, fetch_dir: Path) -> Optional[str]:
        return rewrite_fetched(value, workdir, fetch_dir)
