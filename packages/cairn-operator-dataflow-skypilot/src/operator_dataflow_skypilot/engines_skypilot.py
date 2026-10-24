"""SkyPilot as the provisioning engine (ADR-0104 D5, #2073).

The engine protocol's three calls map onto three SkyPilot calls and nothing
else: ``submit`` → ``sky.launch``, ``poll`` → ``sky.status``, ``cancel`` →
``sky.down``. Everything cairn already owns stays where it is — the spec still
travels as ``CAIRN_DATAFLOW_SPEC``, the binary on the machine is still
``cairn-dataflow-run``, the live curve still streams over the #2025 job channel
built by :func:`live_channel_env_for`. What SkyPilot adds is the one thing the
SSH engine cannot do: acquire the machine, and give it back.

**Why an adapter and not a second orchestrator** (ADR-0104, #1793): SkyPilot is
a provisioner, so it slots in where the *transport* lives — beside
``BuiltinSshEngine`` — not above the run. The control plane keeps the run
record, the gates, the budget and the audit; SkyPilot keeps a cluster alive for
as long as the job needs it.

**Four properties this module is responsible for**, because nothing above it
can be:

1. *Nothing launches over budget.* The cost estimate runs BEFORE ``sky.launch``
   and refuses; a refused submit has provisioned nothing. There is no
   pre-existing pre-launch cost function that fits a flow spec (``compute/
   estimate`` + ``cost/gate`` are operators shaped around a TRAINING input:
   base model, method, dataset rows), so the hook here is deliberately the
   smallest possible one: a callable seam whose default prices the run from
   the compute connection's own ``hourly_usd`` (the key #1787 already added)
   against ``budget.max_seconds``, and compares it with ``budget.max_cost_usd``
   (the ceiling the contract already carries). A deployment that wants the
   operator pair in this position passes ``estimate=``.
2. *Nothing is left running.* Every terminal state — success, failure,
   cancellation — tears the cluster down, exactly once, and the teardown is
   idempotent because a cluster that costs money must never depend on a poll
   arriving twice or not at all.
3. *The cluster carries its own deadline.* Autostop — as autoDOWN, so the disk
   goes with it — is derived from ``budget.max_seconds`` plus a grace window.
   It is the LAST line of defence, covering only what teardown cannot: a
   control plane that dies mid-run.
4. *Every teardown leaves a receipt.* Cluster name, resources, duration and the
   estimate land as a run event through the ordinary store path, so what a run
   bought is on the run's own timeline instead of in a cloud console.
5. *The result comes home before the machine does not exist any more* (#2102).
   The flow's product — ``result.json``, ``metrics.json`` and each stage's
   declared outputs — is rsync'd off the cluster inside ``_teardown``, BEFORE
   ``sky down``, over the per-cluster SSH config SkyPilot itself generates. The
   names, the destination and the stats rewrite are the SSH engine's, imported
   rather than re-spelled (:func:`~cairn.core.dataflow.fetch.fetch_list`,
   :func:`~cairn.core.dataflow.fetch.fetch_dir`,
   :func:`~cairn.core.dataflow.fetch.rewrite_fetched`), so a pipeline leg
   hands the next leg the same shape whichever transport ran it.

**The seam.** :func:`sky_sdk` is the ONE place the SkyPilot SDK is imported and
the one thing tests replace (``SkyPilotEngine(target, sky=fake)``), mirroring
``cairn.core.compute_lifecycle.ec2_client``. That is not a convenience: a live
``sky launch`` provisions a paid machine, so the adapter has to be provable
without one.

Requires the ``[skypilot]`` extra — imports are lazy so the base wheel stays
dep-free (ADR-0081 D5), like every other engine here.
"""
from __future__ import annotations
from cairn.core.operator import getenv

import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from cairn.core.dataflow import (
    DataflowResult,
    DataflowSpec,
    EngineError,
    EngineJobHandle,
    EngineJobStatus,
)
from cairn.core.operator.jobs import live_channel_env_for

from cairn.core.dataflow import results
from cairn.core.dataflow.resources import aggregate_resources, essential_env, gang_workers
from cairn.core.dataflow.fetch import fetch_dir, fetch_list, rewrite_fetched

#: The environment a provisioned machine gets: core's engine-neutral allowlist only (the spec
#: contract, the live channel, ``CAIRN_CONN_``). The node runs cairn's own runner, not
#: Metaflow, so Metaflow's ``METAFLOW_*`` configuration no longer rides along (ADR-0134 D3b):
#: before #2678 this engine borrowed the Metaflow engine's set wholesale.
_ESSENTIAL_ENV = essential_env()

_log = logging.getLogger("operator_dataflow_skypilot.engines_skypilot")

#: The run-event type a teardown receipt is written under. One name, so the
#: console and any later cost report read the same rows.
RECEIPT_EVENT_TYPE = "skypilot.receipt"

#: Cluster names are DNS-ish and short; SkyPilot rejects the rest.
_CLUSTER_PREFIX = "cairn"
_CLUSTER_MAX = 40

#: The on-machine entrypoint — the SAME console script the builtin and SSH
#: engines run (the #1048 anti-deviation rule: one binary, three transports).
REMOTE_COMMAND = "cairn-dataflow-run"

#: The programs SkyPilot itself shells out to. Not a cairn dependency — a
#: SkyPilot one: it rsyncs the workdir up and ssh's into every node it
#: provisions or adopts, and ``sky ssh up`` cannot register a pool without
#: them. The engine fetches results back over the same two (#2102 item 2).
REQUIRED_BINARIES = ("ssh", "rsync")

#: The per-cluster workdir the machine runs the flow in, under ``$HOME``.
#: RELATIVE on purpose, for two reasons at once: it is what rsync wants on the
#: far side of ``<cluster>:`` (paths there resolve against the remote home),
#: and it is the shape ``rewrite_fetched`` anchors on — the SSH engine records
#: exactly this kind of relative workdir (``.cairn-jobs/<run>``).
REMOTE_WORKDIR_ROOT = ".cairn-jobs"

#: Where SkyPilot writes the SSH config for every cluster it manages. Verified
#: against skypilot 0.13.0: ``cluster_utils.SSHConfigHelper.ssh_cluster_path``
#: is ``constants.SKY_USER_FILE_PATH + '/ssh/{}'`` and ``SKY_USER_FILE_PATH``
#: is ``~/.sky/generated``. The stanza's ``Host`` IS the cluster name, so
#: ``ssh -F <that file> <cluster>`` reaches the head node with the right user,
#: key, port and proxy command already filled in — which is why the fetch goes
#: through it instead of re-deriving any of that from the connection.
SKY_SSH_CONFIG_DIR = (".sky", "generated", "ssh")

#: How long a single fetch may take before the control plane gives up on it. A
#: teardown must not be held hostage by an unreachable machine — an unfetched
#: result is bad, a cluster that bills forever is worse.
FETCH_TIMEOUT_S = 600

#: What a bare VM needs before it can run that binary, when no image is
#: configured. An image is the better answer (ADR-0104 D1) and wins when set.
DEFAULT_SETUP = "pip install 'cairndev[dataflow-engine]'"

#: SkyPilot job states → the engine vocabulary. A cluster can be UP while its
#: job is anything; the JOB is what the run cares about.
_JOB_STATES = {
    "INIT": "queued",
    "PENDING": "queued",
    "SETTING_UP": "queued",
    "RUNNING": "running",
    "SUCCEEDED": "succeeded",
    "FAILED": "failed",
    "FAILED_SETUP": "failed",
    "FAILED_PRECHECKS": "failed",
    "FAILED_DRIVER": "failed",
    "CANCELLED": "cancelled",
}

TERMINAL = frozenset({"succeeded", "failed", "cancelled"})

#: Autostop is the LAST line of defence, not the first: `cancel` and the
#: terminal-state teardown are what normally give a machine back. It exists for
#: the case neither can cover — a control plane that dies mid-run — so it is
#: derived from the run's own wall-clock ceiling plus a grace window, and falls
#: back to a fixed ceiling when the run declared none. A cluster with no
#: autostop at all is the failure mode that shows up as a bill.
AUTOSTOP_GRACE_MIN = 15
AUTOSTOP_DEFAULT_MIN = 120


# ─── the SDK seam ────────────────────────────────────────────────────────────


class _SkySdk:
    """The real SkyPilot SDK, narrowed to the four calls this engine makes.

    Narrow on purpose: the adapter's surface is what a fake has to reproduce,
    and a wide surface is a fake that drifts from the thing it stands in for.
    The SDK is asynchronous by request id — every call returns one and
    ``sky.get`` resolves it — so the blocking wait lives HERE, in one place,
    rather than leaking into the engine's own async methods.
    """

    def __init__(self) -> None:
        # optional dependency: the [skypilot] extra — lazy so this wheel imports and
        # registers without skypilot installed (ADR-0134 D3a).
        try:
            import sky  # noqa: PLC0415
        except ImportError as e:  # pragma: no cover - depends on install shape
            raise EngineError(
                "the skypilot engine needs the [skypilot] extra: "
                "pip install 'cairndev[skypilot]'"
            ) from e
        self._sky = sky

    def launch(self, task: dict, cluster_name: str, idle_minutes: Optional[int] = None) -> dict:
        sky = self._sky
        t = sky.Task.from_yaml_config(task)
        job_id, handle = sky.get(sky.launch(
            t, cluster_name=cluster_name,
            # down=True → autoSTOP becomes autoDOWN: the machine is terminated,
            # not parked. A stopped cluster still holds (and bills) its disk.
            idle_minutes_to_autostop=idle_minutes, down=idle_minutes is not None,
        ))
        return {"job_id": job_id, "cluster_name": cluster_name, "handle": str(handle)}

    def status(self, cluster_name: str) -> dict:
        sky = self._sky
        # `refresh` is a StatusRefreshMode, NOT a bool (verified against
        # skypilot 0.13): FORCE re-queries the cloud, which is the only answer
        # worth having when we are deciding whether a paid machine is still up.
        from sky.utils.common import StatusRefreshMode  # noqa: PLC0415 — optional [skypilot] extra

        records = sky.get(sky.status(cluster_names=[cluster_name],
                                     refresh=StatusRefreshMode.FORCE))
        if not records:
            return {}
        rec = records[0]
        raw = rec.get("status")
        out: dict[str, Any] = {
            "status": getattr(raw, "name", None) or str(raw or ""),
            "resources_str": rec.get("resources_str", ""),
            "launched_at": rec.get("launched_at"),
        }
        # the job queue is what says whether the WORK finished; a cluster that
        # is UP with a SUCCEEDED job is a done run, not a running one.
        try:
            jobs = sky.get(sky.queue(cluster_name))
            if jobs:
                js = jobs[0].get("status")
                out["job_status"] = getattr(js, "name", None) or str(js or "")
        except Exception:  # noqa: BLE001 — an unreachable queue is not a verdict
            out["job_status"] = None
        return out

    def down(self, cluster_name: str) -> None:
        self._sky.get(self._sky.down(cluster_name))

    def enabled_infra(self) -> list[str]:
        """``sky check``-equivalent: the clouds/pools this install can use.

        `sky.enabled_clouds` does NOT exist at the top level — it lives in
        `sky.client.sdk` (verified against skypilot 0.13). Reaching for the
        top-level name raised, and because the door turns any exception here
        into a refusal, D5 would have refused EVERY launch on a perfectly good
        install. The one call nobody had run.
        """
        try:
            from sky.client import sdk  # noqa: PLC0415 — optional [skypilot] extra

            enabled = self._sky.get(sdk.enabled_clouds())
        except Exception as e:  # noqa: BLE001 — report, never crash the door
            raise EngineError(f"sky check failed: {e}") from e
        # 0.13 returns plain strings; older shapes returned Cloud objects.
        return [str(getattr(c, "canonical_name", None) or c).lower() for c in enabled]


def sky_sdk() -> Any:
    """THE seam. ``CAIRN_SKY_SDK_FACTORY='module:callable'`` points it at a
    fake without patching — the dev/test escape hatch
    ``CAIRN_EC2_CLIENT_FACTORY`` already established (#1787). Unset in
    production, where it costs one absent env read."""
    raw = os.environ.get("CAIRN_SKY_SDK_FACTORY", "").strip()
    if raw:
        mod_name, _, attr = raw.partition(":")
        if not mod_name or not attr:
            raise EngineError(f"CAIRN_SKY_SDK_FACTORY={raw!r} is not 'module:callable'")
        import importlib  # noqa: PLC0415 - only reached when the dev seam is set

        return getattr(importlib.import_module(mod_name), attr)()
    return _SkySdk()


def skypilot_importable() -> bool:
    """Is the ``[skypilot]`` extra installed? The door's first question — asked
    without importing the package (import is slow and has side effects)."""
    import importlib.util  # noqa: PLC0415 - a probe, not a use

    return importlib.util.find_spec("sky") is not None


# ─── target resolution: a connection becomes an infra ────────────────────────


@dataclass
class SkyTarget:
    """What a resolved ``ml_target``/``dataflow_target`` gives the engine."""

    infra: str
    """SkyPilot's ``--infra`` value: ``ssh/<pool>``, ``vast``, ``runpod``, ``aws``."""
    kind: str = ""
    hourly_usd: Optional[float] = None
    """Price the receipt and the pre-launch estimate use, when the connection
    declares one (``compute.ssh``'s ``hourly_usd``, added by #1787)."""
    env: dict[str, str] = field(default_factory=dict)
    """Credential env the SDK call needs in the CONTROL PLANE's process (never
    forwarded to the cluster)."""
    files: list[str] = field(default_factory=list)
    """Credential files written because SkyPilot reads that cloud from a file
    and nothing else. Recorded so a reviewer can see every byte that landed."""


#: connection kind → the infra name SkyPilot knows it by.
_KIND_INFRA = {
    "compute.vast": "vast",
    "compute.runpod": "runpod",
    "compute.kube": "kubernetes",
    "compute.lambda": "lambda",
}


def _sky_home(home: Optional[Path]) -> Path:
    return Path(home) if home is not None else Path.home()


def _write_private(path: Path, text: str) -> None:
    """Write owner-only. Credentials land on disk ONLY where SkyPilot reads
    them from a file and accepts nothing else; each such file is named in
    :attr:`SkyTarget.files` and documented in the pack's DESIGN.md."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(mode=0o600, exist_ok=True)
    path.chmod(0o600)
    path.write_text(text, encoding="utf-8")


def write_node_pool(name: str, config: dict, secrets: dict, *,
                    home: Optional[Path] = None) -> str:
    """Register a ``compute.ssh`` box as an SSH node pool and return its name.

    The box cairn already owns joins SkyPilot the way SkyPilot joins existing
    machines: one entry in ``~/.sky/ssh_node_pools.yaml``, then ``sky ssh up``.
    Writing is a MERGE — a pool this function did not create is left untouched,
    and re-resolving the same connection rewrites only its own entry, so the
    file converges instead of growing.

    SkyPilot reads the key as a PATH (``identity_file``), so the connection's
    ``PRIVATE_KEY`` secret is materialized under ``~/.sky/cairn-keys/`` with
    owner-only permissions. That is the one place this module puts a secret on
    disk, and it is the minimum SkyPilot accepts.
    """
    host = str(config.get("host") or "").strip()
    if not host:
        raise EngineError(f"compute.ssh connection {name!r} has no host — set it "
                          "before selecting the skypilot engine")
    pool = _safe_name(name)
    root = _sky_home(home) / ".sky"
    entry: dict[str, Any] = {"hosts": [host]}
    if config.get("user"):
        entry["user"] = str(config["user"])
    key = (secrets or {}).get("PRIVATE_KEY")
    if key:
        key_path = root / "cairn-keys" / f"{pool}.pem"
        _write_private(key_path, key if key.endswith("\n") else key + "\n")
        entry["identity_file"] = str(key_path)

    pools_file = root / "ssh_node_pools.yaml"
    existing: dict[str, Any] = {}
    if pools_file.exists():
        try:
            existing = yaml.safe_load(pools_file.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            raise EngineError(f"{pools_file} is not readable YAML: {e}") from e
        if not isinstance(existing, dict):
            raise EngineError(f"{pools_file} is not a mapping of pools")
    existing[pool] = entry
    root.mkdir(parents=True, exist_ok=True)
    pools_file.write_text(yaml.safe_dump(existing, sort_keys=True), encoding="utf-8")
    return pool


async def resolve_target(name: str, *, home: Optional[Path] = None,
                         materialize: bool = True) -> SkyTarget:
    """Turn the environment's target connection into a SkyPilot infra.

    Resolved through the SAME ``resolve_connection`` seam the SSH engine and the
    Metaflow engine already use, so secret refs stay refs in the profile.

    - ``compute.ssh`` → an SSH node pool entry, ``infra=ssh/<pool>``;
    - ``compute.vast`` → ``~/.config/vastai/vast_api_key`` (the ONLY place
      SkyPilot's Vast provider reads the key), ``infra=vast``;
    - ``compute.runpod`` → ``~/.runpod/config.toml`` (what ``runpod config``
      writes), ``infra=runpod``;
    - anything holding ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY`` → those
      two as process env, no file, ``infra=aws``.

    ``materialize=False`` answers the *name* of the infra without writing
    anything. The door asks in that mode: a dry run must not put a private key
    or an API token on disk, and the question it is answering ("would this
    launch work?") does not need one.

    An unresolvable target raises: an environment that names a box which does
    not exist must not silently launch somewhere else (#568).
    """
    target = (name or "").strip()
    if not target:
        raise EngineError(
            "the skypilot engine needs a compute connection — set [profile.backends] "
            "ml_target (or dataflow_target) to a compute.ssh / compute.vast / "
            "compute.runpod connection"
        )
    from cairn.core.operator import resolve_connection  # noqa: PLC0415 - lazy: core is absent on a data-plane box

    conn = await resolve_connection(target)
    if conn is None:
        raise EngineError(
            f"compute connection {target!r} does not resolve — create it "
            "(cairn connect) or point the target at one that exists"
        )
    cfg = dict(getattr(conn, "config", None) or {})
    secrets = dict(getattr(conn, "secrets", None) or {})
    kind = str(getattr(conn, "kind", "") or "")
    root = _sky_home(home)

    price = _float_or_none(cfg.get("hourly_usd"))

    if kind == "compute.ssh":
        if not materialize:
            if not str(cfg.get("host") or "").strip():
                raise EngineError(f"compute.ssh connection {target!r} has no host")
            return SkyTarget(infra=f"ssh/{_safe_name(target)}", kind=kind, hourly_usd=price)
        pool = write_node_pool(target, cfg, secrets, home=home)
        return SkyTarget(
            infra=f"ssh/{pool}", kind=kind, hourly_usd=price,
            files=[str(root / ".sky" / "ssh_node_pools.yaml")],
        )

    if kind == "compute.vast":
        if not materialize:
            return SkyTarget(infra="vast", kind=kind, hourly_usd=price)
        path = root / ".config" / "vastai" / "vast_api_key"
        _write_private(path, str(secrets.get("API_KEY", "")).strip() + "\n")
        return SkyTarget(infra="vast", kind=kind, hourly_usd=price, files=[str(path)])

    if kind == "compute.runpod":
        if not materialize:
            return SkyTarget(infra="runpod", kind=kind, hourly_usd=price)
        path = root / ".runpod" / "config.toml"
        _write_private(path, f'api_key = "{str(secrets.get("API_KEY", "")).strip()}"\n')
        return SkyTarget(infra="runpod", kind=kind, hourly_usd=price, files=[str(path)])

    if "AWS_ACCESS_KEY_ID" in secrets:
        # boto3 (and so SkyPilot's AWS provider) reads these from the process
        # env — no file, which is why AWS is the shape to prefer.
        return SkyTarget(
            infra="aws", kind=kind, hourly_usd=price,
            env={k: str(secrets[k]) for k in
                 ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
                 if k in secrets},
        )

    infra = _KIND_INFRA.get(kind)
    if infra:
        return SkyTarget(infra=infra, kind=kind, hourly_usd=price)
    raise EngineError(
        f"connection {target!r} has kind {kind!r}, which the skypilot engine does "
        "not know how to turn into an infra — use a compute.ssh, compute.vast, "
        "compute.runpod or AWS-credentialed connection"
    )


def _float_or_none(value: Any) -> Optional[float]:
    try:
        f = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _safe_name(raw: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", str(raw).strip().lower()).strip("-") or "cairn"


def missing_binaries() -> list[str]:
    """Which of :data:`REQUIRED_BINARIES` this process cannot find on PATH.

    A pure PATH probe (``shutil.which``) because that is exactly the question
    SkyPilot's own subprocess calls will ask, and because a door must be cheap
    enough to run on every launch.
    """
    return [b for b in REQUIRED_BINARIES if shutil.which(b) is None]


# ─── results: off the machine before the machine goes away ──────────────────


def remote_workdir(cluster: str) -> str:
    """The workdir the flow runs in on the cluster, relative to ``$HOME``."""
    return f"{REMOTE_WORKDIR_ROOT}/{cluster}"


def ssh_config_path(cluster: str, *, home: Optional[Path] = None) -> Path:
    """SkyPilot's generated SSH config for ``cluster`` (see
    :data:`SKY_SSH_CONFIG_DIR`). Absent until a cluster is UP."""
    return _sky_home(home).joinpath(*SKY_SSH_CONFIG_DIR, cluster)


def fetch_outputs(cluster: str, workdir: str, names: list[str], dest: Path, *,
                  home: Optional[Path] = None,
                  timeout: int = FETCH_TIMEOUT_S) -> int:
    """Pull ``names`` out of the cluster's workdir into ``dest``; return how many.

    **Why rsync over SkyPilot's own generated SSH config, and not the SDK.**
    Checked against skypilot 0.13.0: ``sky.client.sdk`` exposes no generic
    file-download call at all — ``download_logs`` is the only file-shaped one,
    and it returns the job's LOGS, not the workdir the flow wrote its
    ``result.json`` into. What SkyPilot does have, for every cluster it manages,
    is the per-cluster SSH stanza it writes at
    ``~/.sky/generated/ssh/<cluster>`` (``SSHConfigHelper.add_cluster``), which
    already carries the head node's address, user, key, port and any proxy
    command. Rsync over that config is therefore the same transport SkyPilot
    uses itself, with none of it re-derived here — and it is why the door now
    refuses when ``rsync``/``ssh`` are absent.

    Best-effort PER NAME, exactly like the SSH engine's ``sftp.get`` loop: a
    stage that wrote no optional output must not lose the ``result.json`` that
    ran beside it. Never raises — the caller is about to tear a paid cluster
    down, and a fetch failure must not leave it running.
    """
    config = ssh_config_path(cluster, home=home)
    if not config.is_file():
        _log.warning("no SkyPilot ssh config at %s — cannot fetch %s outputs",
                     config, cluster)
        return 0
    rsync = shutil.which("rsync")
    ssh = shutil.which("ssh")
    if not rsync or not ssh:
        _log.warning("cannot fetch %s outputs: %s missing from PATH",
                     cluster, ", ".join(missing_binaries()))
        return 0
    dest.mkdir(parents=True, exist_ok=True)
    # BatchMode: a machine that wants a password must fail, not block the
    # teardown on a prompt nobody is there to answer.
    rsh = (f"{ssh} -F {shlex.quote(str(config))} -o BatchMode=yes "
           "-o ConnectTimeout=15")
    got = 0
    for name in names:
        cmd = [rsync, "-az", "-e", rsh, f"{cluster}:{workdir}/{name}", f"{dest}/"]
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=timeout,
                                  check=False)
        except (OSError, subprocess.SubprocessError) as e:  # noqa: PERF203
            _log.warning("rsync of %s from %s failed: %s", name, cluster, e)
            continue
        if proc.returncode == 0:
            got += 1
        else:
            # code 23 = "some files could not be transferred", which for a
            # per-name fetch means the stage simply wrote nothing. Only worth
            # a debug line; result.json's absence is what `result()` refuses on.
            _log.debug("rsync of %s from %s exited %s: %s", name, cluster,
                       proc.returncode, proc.stderr.decode("utf-8", "replace")[:400])
    return got


# ─── the task ────────────────────────────────────────────────────────────────


def cluster_name(run_id: str, node: str = "") -> str:
    """Deterministic from run id + node, so a control-plane restart finds the
    SAME cluster instead of provisioning a second one (the #551 idempotent-adopt
    property, one layer out). Short and DNS-ish because SkyPilot requires it;
    the hash keeps two long run ids from colliding after truncation."""
    stem = _safe_name(f"{run_id}-{node}" if node else str(run_id))
    digest = hashlib.sha256(f"{run_id}/{node}".encode()).hexdigest()[:8]
    room = _CLUSTER_MAX - len(_CLUSTER_PREFIX) - len(digest) - 2
    return f"{_CLUSTER_PREFIX}-{stem[:room].strip('-')}-{digest}"


def build_task(spec: DataflowSpec, *, name: str, envs: Optional[dict] = None,
               infra: str = "", use_spot: bool = False, image: str = "",
               setup: str = "", command: str = "", accelerator: str = "",
               workdir: str = "") -> dict:
    """The SkyPilot task YAML this spec becomes — a PURE function, so the exact
    submission is unit-testable with nothing installed.

    Resources come from the per-stage :class:`StageResources` hints through the
    SAME aggregation the Metaflow engine uses (core's ``aggregate_resources``): the
    whole dataflow runs in one job, so the machine must fit its heaviest stage.
    ``cpus``/``memory`` are emitted with SkyPilot's ``+`` ("at least") suffix —
    a hint is a floor, not an exact instance type.

    ``workdir`` names the ``$HOME``-relative directory the run command executes
    in, and it is what makes results fetchable (#2102). ``cairn-dataflow-run``
    writes ``result.json``/``metrics.json`` into ``os.getcwd()``
    (``operator_dataflow.engine.entry``), and SkyPilot's generated run script
    only does ``cd ~/sky_workdir`` — a directory that does not exist unless the
    task declared a ``workdir:`` to sync, in which case the ``cd`` fails
    silently and the artifacts land wherever the shell happened to be. Naming
    the directory explicitly here is what lets ``result()`` know where to rsync
    from, and it mirrors the SSH engine's per-job ``.cairn-jobs/<id>`` workdir.
    """
    res: dict[str, Any] = {}
    agg = aggregate_resources(spec)
    if agg.get("gpu"):
        # SkyPilot wants `<type>:<count>`. A bare count is not "N GPUs" — it
        # parses as an accelerator NAMED that number ("1" -> {'1': 1}), which
        # no cloud can satisfy, so the launch fails late or lands a machine
        # with no GPU at all. Verified against skypilot 0.13. The COUNT is the
        # stage's hint; the TYPE is the environment's (`skypilot_accelerator` /
        # CAIRN_SKYPILOT_ACCELERATOR), because which accelerator a target offers
        # is a property of the target, not of the flow.
        if not accelerator:
            raise EngineError(
                f"the flow asks for {agg['gpu']} GPU(s) but no accelerator TYPE is "
                "configured — set `skypilot_accelerator` (CAIRN_SKYPILOT_ACCELERATOR), "
                "e.g. 'A100' or 'T4'. SkyPilot needs <type>:<count>; a bare count "
                "would request an accelerator literally named "
                f"{str(agg['gpu'])!r}."
            )
        res["accelerators"] = f"{accelerator}:{agg['gpu']}"
    if agg.get("cpu"):
        res["cpus"] = f"{agg['cpu']}+"
    if agg.get("memory"):
        # StageResources.memory_mb is MiB; SkyPilot's `memory` is GiB.
        res["memory"] = f"{max(1, int(agg['memory'] // 1024))}+"
    if infra:
        res["infra"] = infra
    if use_spot:
        res["use_spot"] = True
    if image:
        # a bare repo/tag is a container image; an already-qualified value
        # (docker:…, an AMI id, a GCP image path) is passed through untouched.
        qualified = (image.startswith(("docker:", "ami-", "projects/"))
                     or ":" in image.split("/")[0])
        res["image_id"] = image if qualified else f"docker:{image}"

    run = command or REMOTE_COMMAND
    if workdir:
        wd = shlex.quote(workdir)
        # $HOME-anchored, not relative: SkyPilot's run script has already
        # `cd`-ed somewhere by the time this line executes, and where it landed
        # depends on whether ~/sky_workdir exists.
        run = f'mkdir -p "$HOME"/{wd} && cd "$HOME"/{wd} && {run}'
    task: dict[str, Any] = {
        "name": name,
        "resources": res,
        "envs": dict(envs or {}),
        "run": run,
        "num_nodes": gang_workers(spec),
    }
    if not image:
        # an image already carries the install; a setup line on top of one is
        # a second, drifting source of truth (the ADR-0104 D1 failure class).
        task["setup"] = setup or DEFAULT_SETUP
    return task


def autostop_minutes(spec: DataflowSpec) -> int:
    """The cluster's own deadline, from the run's budget (see
    :data:`AUTOSTOP_GRACE_MIN`). Grace is added because autostop measures IDLE
    time and a job that runs right up to its ceiling is not idle — the window
    must outlast the work, or it kills a healthy run."""
    seconds = spec.budget.max_seconds
    if not seconds:
        return AUTOSTOP_DEFAULT_MIN
    return max(1, int(seconds // 60)) + AUTOSTOP_GRACE_MIN


def default_estimate(spec: DataflowSpec, target: SkyTarget) -> Optional[float]:
    """The pre-launch $ estimate, or None when nothing prices this run.

    Deliberately the smallest honest calculation: the hourly price the compute
    connection declares (``hourly_usd``, #1787) — or ``CAIRN_SKYPILOT_HOURLY_USD``
    for a cloud that has no connection-level price — times the wall-clock
    ceiling the spec already declares (``budget.max_seconds``). Wall clock is
    what a cluster bills.

    None means *not priced*, which is NOT the same as free: the gate lets it
    through (refusing every unpriced run would make the engine unusable) and
    the receipt says so, which is the same shape ``cost/gate`` uses when it has
    no estimate to judge.
    """
    price = target.hourly_usd or _float_or_none(getenv("CAIRN_SKYPILOT_HOURLY_USD"))
    seconds = spec.budget.max_seconds
    if not price or not seconds:
        return None
    return round(price * seconds / 3600.0, 2)


# ─── receipts ────────────────────────────────────────────────────────────────


async def write_receipt(run_id: str, payload: dict, *, store: Any) -> None:
    """Append the teardown receipt to the run's event log through ``store`` — the
    submitting context's run store (``ctx.run_store()``, #2453 B), the same store
    stage events and job events use; this engine imports no storage. NEVER raises:
    a receipt that failed to write must not turn a completed run into a failed
    one, and the teardown it records has already happened. No store (the process
    that submitted the job is gone) → the receipt is logged, not persisted."""
    if not run_id:
        return
    if store is None:
        _log.warning("skypilot receipt for run %r not persisted — no run store from the submitting context: %s",
                     run_id, payload)
        return
    try:
        await store.append_event(run_id, RECEIPT_EVENT_TYPE, payload)
    except Exception:  # noqa: BLE001 — see docstring
        _log.warning("skypilot receipt for run %r failed to persist", run_id, exc_info=True)


# ─── the engine ──────────────────────────────────────────────────────────────


class SkyPilotEngine:
    """Provision a cluster, run the dataflow on it, and give it back."""

    name = "skypilot"

    def __init__(self, target: str = "", *, sky: Any = None,
                 estimate: Optional[Callable[[DataflowSpec, SkyTarget], Optional[float]]] = None,
                 use_spot: bool = False, image: str = "", setup: str = "",
                 accelerator: str = "", home: Optional[Path] = None,
                 fetch: Optional[Callable[..., int]] = None) -> None:
        self._target = (target or "").strip()
        self._sky = sky
        self._estimate = estimate or default_estimate
        #: THE fetch seam, mirroring ``sky=``: a live fetch needs a live
        #: cluster, so the ordering property ("results come home BEFORE the
        #: machine is destroyed") has to be provable without one.
        self._fetch = fetch or fetch_outputs
        self._use_spot = use_spot
        self._image = image
        self._setup = setup
        #: The accelerator TYPE this target offers ("A100", "T4"). SkyPilot
        #: needs <type>:<count>; the count comes from the stage's resource hint.
        self._accelerator = accelerator
        self._home = home
        #: clusters this process has already torn down — teardown is
        #: idempotent, but a second `sky.down` is a second API call and a
        #: second receipt, and the receipt must be one per cluster.
        self._down: set[str] = set()
        # #2453 (B): the run-store accessor of the context that submitted each cluster's
        # job — the receipt is written through it at teardown; this engine imports no storage.
        self._run_stores: dict[str, Any] = {}

    # ── capability discovery ────────────────────────────────────────────
    def validate(self, spec: DataflowSpec) -> list[str]:
        """The stage registry is authoritative here for the same reason it is
        for the SSH engine: the machine installs the same wheels."""
        problems = results.validate(spec)
        if not self._target:
            problems.append(
                "the skypilot engine needs ml_target/dataflow_target to name a "
                "compute connection (ssh box, vast, runpod or AWS)"
            )
        return problems

    async def check_ready(self) -> list[str]:
        """The door's question (ADR-0099 §1; asked by core's engine-neutral
        :mod:`cairn.core.dataflow.engine_door`, ADR-0134 D3c): reasons this
        environment cannot launch, empty when it can.

        Five, in the order they cost: is a target set at all, is the extra here,
        are the two BINARIES SkyPilot shells out to on PATH, does the target
        resolve to an infra, is that infra enabled (``sky check``). Asked by core
        through the engine registry so core never imports this pack; a reason is
        a REFUSAL — nothing here installs or provisions to make the answer yes.
        """
        if not self._target:
            # Moved here from core's SkyPilot door (ADR-0134 D3c): the engine owns
            # what it needs, and only it knows a target is where it provisions.
            return [
                "ml_target/dataflow_target is empty — SkyPilot needs a compute "
                "connection to know WHERE to provision (a compute.ssh box becomes an "
                "SSH node pool; compute.vast / compute.runpod / an AWS-credentialed "
                "connection become that cloud)"
            ]
        problems: list[str] = []
        if self._sky is None and not skypilot_importable():
            problems.append(
                "the skypilot extra is not importable here — "
                "pip install 'cairndev[skypilot]'"
            )
            return problems
        missing = missing_binaries()
        if missing:
            # #2102 item 2 — found live in the chart-installed control plane:
            # the pod had the extra pip-installed and NEITHER binary
            # (`command -v ssh` → nothing). Returned ALONE and early because
            # every check below it is moot: SkyPilot cannot reach a node it
            # cannot ssh to, `sky ssh up` cannot register a pool, and this
            # engine cannot fetch the result back. asyncssh does not help —
            # SkyPilot shells out to the real programs.
            return [
                f"skypilot needs {' and '.join(missing)} on PATH and "
                f"{'they are' if len(missing) > 1 else 'it is'} missing — "
                "SkyPilot shells out to ssh/rsync for every node it provisions or "
                "adopts, and `sky ssh up` needs them to register a pool. Install "
                "them in the image that carries the extra "
                "(apt-get install -y --no-install-recommends openssh-client rsync)."
            ]
        try:
            # materialize=False — a door is a question, not a side effect.
            target = await resolve_target(self._target, home=self._home,
                                          materialize=False)
        except EngineError as e:
            return [str(e)]
        try:
            enabled = [str(c).lower() for c in self._sdk().enabled_infra()]
        except Exception as e:  # noqa: BLE001 — an unreadable sky check is a refusal
            return [f"sky check could not run: {e}"]
        cloud = target.infra.split("/")[0].lower()
        # #2102 — an EMPTY `sky check` is the clearest refusal there is: this
        # install can launch on nothing. The guard used to be `if enabled and
        # …`, which SKIPPED the check when the list was empty, so the door said
        # ok and a launch reached `sky.launch` on an un-provisioned pool — after
        # the run record existed. Seen live in the deployed control plane.
        if cloud not in enabled and target.infra.lower() not in enabled:
            how = (f"run `sky ssh up {target.infra.split('/', 1)[1]}` to register the pool"
                   if cloud == "ssh" else f"configure {cloud} credentials")
            problems.append(
                f"sky check: infra {target.infra!r} is not enabled "
                f"(enabled: {', '.join(enabled) or 'none'}) — run `sky check`; {how}, "
                "or point the target at an infra this install can use"
            )
        return problems

    def _sdk(self) -> Any:
        if self._sky is None:
            self._sky = sky_sdk()
        return self._sky

    # ── submit ──────────────────────────────────────────────────────────
    async def submit(self, spec: DataflowSpec, ctx: Any) -> EngineJobHandle:
        problems = self.validate(spec)
        if problems:
            raise EngineError("; ".join(problems))
        target = await resolve_target(self._target, home=self._home)

        # THE COST DOOR — before anything is provisioned. A refused submit has
        # bought nothing, which is the whole point of doing it here rather than
        # in the poll loop.
        estimate_usd = self._estimate(spec, target)
        cap = spec.budget.max_cost_usd
        if cap is not None and estimate_usd is not None and estimate_usd > cap:
            raise EngineError(
                f"skypilot refused to launch: estimated ${estimate_usd:.2f} for "
                f"{spec.budget.max_seconds}s on {target.infra} exceeds the run's "
                f"budget.max_cost_usd of ${cap:.2f} — raise the budget, shorten "
                "budget.max_seconds, or pick cheaper resources. No cluster was created."
            )

        node = str(getattr(ctx, "node_name", "") or "flow")
        cluster = cluster_name(spec.parent_run or "norun", node)
        self._run_stores[cluster] = getattr(ctx, "run_store", None)
        workdir = remote_workdir(cluster)
        task = build_task(
            spec, name=cluster, infra=target.infra, use_spot=self._use_spot,
            image=self._image, setup=self._setup, accelerator=self._accelerator,
            envs=self._task_env(spec, node), workdir=workdir,
        )

        sdk = self._sdk()
        prior = dict(os.environ)
        try:
            # Credential env for the SDK call ONLY — the cluster never sees it
            # (the task's own `envs` is the allowlisted set built above).
            # Process-wide for the duration of the call: SkyPilot's providers
            # read these from the environment and nowhere else, and submit is
            # the dispatcher's serialized path.
            os.environ.update(target.env)
            launched = sdk.launch(task, cluster, idle_minutes=autostop_minutes(spec))
        except EngineError:
            raise
        except Exception as e:  # noqa: BLE001 — a provisioning failure is an engine failure
            raise EngineError(f"sky launch failed on {target.infra}: {e}") from e
        finally:
            for k in target.env:
                if k in prior:
                    os.environ[k] = prior[k]
                else:
                    os.environ.pop(k, None)

        return EngineJobHandle(
            job_id=cluster, engine=self.name,
            # everything poll/cancel need rides in the handle — the SSH engine's
            # stateless-on-the-control-plane property, kept.
            meta={
                "cluster": cluster,
                "run_id": spec.parent_run,
                "node": node,
                "infra": target.infra,
                "resources": _resources_str(task),
                "use_spot": self._use_spot,
                "launched_at": time.time(),
                "estimate_usd": estimate_usd,
                "sky_job_id": (launched or {}).get("job_id"),
                # #2102 — the fetch-back contract, in the handle so poll/cancel
                # and `result` survive a control-plane restart the way the SSH
                # engine's `exec_input` does. `fetch_list` is the SSH engine's
                # own table: both transports hand back the same names, or the
                # hoist and the next pipeline leg see a different shape
                # depending on which engine ran the leg.
                "workdir": workdir,
                "fetch": fetch_list(spec),
                # provenance, and what `result` stamps on the stats.
                "image": self._image,
            },
        )

    def _task_env(self, spec: DataflowSpec, node: str) -> dict[str, str]:
        """The cluster's environment: the spec contract, the live channel, and
        the essential passthrough — core's engine-neutral allowlist
        (``_ESSENTIAL_ENV``), so a cloud credential the control plane
        happens to hold never rides to a machine that did not declare it."""
        env: dict[str, str] = {
            k: v for k, v in os.environ.items() if k in _ESSENTIAL_ENV
        }
        env["CAIRN_DATAFLOW_SPEC"] = spec.model_dump_json()
        # NOTE: no CAIRN_SUBMIT_CWD — on a provisioned machine, relative is
        # on-machine (the SSH engine's rule, for the same reason).
        env.pop("CAIRN_SUBMIT_CWD", None)
        if spec.parent_run:
            env["CAIRN_PARENT_RUN"] = spec.parent_run
            env.update(live_channel_env_for(spec.parent_run, node or "flow"))  # #2025
        return env

    # ── poll ────────────────────────────────────────────────────────────
    async def poll(self, handle: EngineJobHandle) -> EngineJobStatus:
        cluster = str(handle.meta.get("cluster") or handle.job_id)
        try:
            record = self._sdk().status(cluster) or {}
        except Exception as e:  # noqa: BLE001 — an unreadable status is not a verdict
            return EngineJobStatus(state="running", error=f"sky status unavailable: {e}")

        state, retriable = map_status(record, use_spot=bool(handle.meta.get("use_spot")))
        metrics: dict[str, Any] = {}
        if record.get("resources_str"):
            metrics["resources"] = str(record["resources_str"])
        if state in TERMINAL:
            # success, failure and cancellation alike give the machine back
            await self._teardown(handle, reason=state, record=record)
            workdir = str(handle.meta.get("workdir") or "") or remote_workdir(cluster)
            fetched_metrics = results.read_metrics(fetch_dir(workdir))
            partial_cost = fetched_metrics.get("cost_usd") if isinstance(fetched_metrics, dict) else None
            if (isinstance(partial_cost, (int, float)) and not isinstance(partial_cost, bool)
                    and 0 < partial_cost < float("inf")):
                metrics["cost_usd"] = round(float(partial_cost), 6)
        error = ""
        if state == "failed":
            error = (f"skypilot job on {cluster} ended "
                     f"cluster={record.get('status') or 'GONE'} "
                     f"job={record.get('job_status') or 'unknown'}")
        elif state == "cancelled":
            error = "cancelled"
        return EngineJobStatus(state=state, metrics=metrics, error=error, retriable=retriable)

    # ── cancel ──────────────────────────────────────────────────────────
    async def cancel(self, handle: EngineJobHandle) -> None:
        """Cancelling a provisioned run means giving the machine back — an
        orphaned cluster is a cost leak (#603, now with a bill attached).
        Safe to call twice, and safe to call on a job that already ended: the
        teardown fetches first, so a cancel that races a job which already
        SUCCEEDED does not destroy the result along with the disk (#2102)."""
        await self._teardown(handle, reason="cancelled", record={})

    async def _fetch_results(self, handle: EngineJobHandle) -> int:
        """Pull the job's declared outputs into :func:`fetch_dir`. Never raises.

        Stateless like the SSH engine: everything it needs is in the handle
        meta, and the destination is derived from the workdir, so a control
        plane that restarted between the poll and the hoist still finds the
        bytes where ``result`` looks for them.
        """
        cluster = str(handle.meta.get("cluster") or handle.job_id)
        workdir = str(handle.meta.get("workdir") or "") or remote_workdir(cluster)
        names = [str(n) for n in (handle.meta.get("fetch") or [])] or ["result.json"]
        try:
            # Off the event loop: the fetch is rsync in a subprocess, bounded by
            # FETCH_TIMEOUT_S per name, and `poll` runs on the control plane's
            # own loop — a blocking `subprocess.run` here would stall every
            # request and every other run's poll for the whole transfer.
            return int(await asyncio.to_thread(
                self._fetch, cluster, workdir, names, fetch_dir(workdir), home=self._home))
        except Exception:  # noqa: BLE001 — see docstring: a paid cluster is waiting
            _log.warning("fetching results from %s failed", cluster, exc_info=True)
            return 0

    async def _teardown(self, handle: EngineJobHandle, *, reason: str, record: dict) -> None:
        cluster = str(handle.meta.get("cluster") or handle.job_id)
        if cluster in self._down:
            return
        # THE ORDER IS THE WHOLE POINT (#2102): the flow's product lives on the
        # machine this is about to destroy, so it comes home first — on cancel
        # too, because a cancel that arrives after the job already succeeded
        # would otherwise throw the result away with the disk. Best-effort and
        # bounded (BatchMode + ConnectTimeout + FETCH_TIMEOUT_S): an unfetched
        # result is bad, a cluster that keeps billing is worse.
        fetched = await self._fetch_results(handle)
        self._down.add(cluster)
        error = ""
        try:
            self._sdk().down(cluster)
        except Exception as e:  # noqa: BLE001 — a failed teardown must still leave a receipt
            error = str(e)
            _log.error("sky down failed for %s: %s", cluster, e)
        launched_at = handle.meta.get("launched_at")
        duration = max(0.0, time.time() - float(launched_at)) if launched_at else 0.0
        estimate = handle.meta.get("estimate_usd")
        _recorder = self._run_stores.pop(cluster, None)
        await write_receipt(str(handle.meta.get("run_id") or ""), {
            "cluster": cluster,
            "node": handle.meta.get("node") or "",
            "infra": handle.meta.get("infra") or "",
            "resources": record.get("resources_str") or handle.meta.get("resources") or "",
            "reason": reason,
            "duration_s": round(duration, 1),
            "estimated_usd": estimate,
            "priced": estimate is not None,
            "teardown_error": error,
            # what actually came home before the machine went away — the one
            # number that says whether this run's product survived its cluster.
            "fetched": fetched,
        }, store=_recorder() if callable(_recorder) else None)

    # ── result ──────────────────────────────────────────────────────────
    async def result(self, handle: EngineJobHandle) -> DataflowResult:
        """Read the FETCHED ``result.json``; point stats at the local copies.

        Byte-for-byte the SSH engine's contract (#2102), through the SAME three
        functions it uses — :func:`fetch_dir`, :func:`fetch_list`,
        :func:`rewrite_fetched` — so a pipeline hoist and the next leg cannot
        tell which transport ran this one. The fetch itself already happened, in
        ``_teardown``, before the cluster was destroyed; this call only reads
        what came home.

        A missing ``result.json`` is a loud refusal, not an empty result (#568):
        it means the fetch never ran or never landed, and a silently empty
        DataflowResult would read as "the flow produced nothing".
        """
        cluster = str(handle.meta.get("cluster") or handle.job_id)
        workdir = str(handle.meta.get("workdir") or "") or remote_workdir(cluster)
        fetched = fetch_dir(workdir)
        result_path = fetched / "result.json"
        if not result_path.exists():
            raise EngineError(
                f"no fetched result.json for {handle.job_id} (expected {result_path}) "
                f"— nothing came back from cluster {cluster}. Check the "
                f"{RECEIPT_EVENT_TYPE} run event for this cluster: `fetched: 0` "
                "means the machine was unreachable (SkyPilot's generated ssh "
                f"config at {ssh_config_path(cluster, home=self._home)} absent, or "
                "ssh/rsync missing from the control plane) when it was torn down."
            )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        if "error" in payload:
            raise EngineError(f"remote dataflow failed: {payload['error']}")
        payload.pop("lineage_parent_run", None)
        result = DataflowResult.model_validate(payload)
        # remote workdir paths in stats → the fetched local copy where one
        # exists (the hoists read these paths control-side; the artifact URIs
        # keep the on-machine location — that's where the data product LIVED).
        rewritten = dict(result.stats)
        for k, v in result.stats.items():
            local = rewrite_fetched(v, workdir, fetched)
            if local is not None:
                rewritten[k] = local
        # ADR-0104 D1 (#2071) — provenance on the flow's RESULT, the copy a
        # human sees on the run. Same key the SSH engine stamps.
        image = str((handle.meta or {}).get("image") or "")
        if image:
            rewritten["engine.image"] = image
        return result.model_copy(update={"stats": rewritten})


def map_status(record: dict, *, use_spot: bool = False) -> tuple[str, bool]:
    """SkyPilot's (cluster status, job status) → (engine state, retriable).

    - no record at all: the cluster is GONE, which for a job we launched means
      it died — failed, and retriable (something else reclaimed it);
    - ``INIT``: provisioning — queued;
    - ``UP``: the JOB's state decides;
    - ``STOPPED`` with a job still running: the machine was taken away
      mid-flight. That is the recoverable death :class:`EngineJobStatus`
      documents (``retriable``), and it is the normal end of a spot instance.
    """
    cluster = str(record.get("status") or "").upper()
    job = str(record.get("job_status") or "").upper()
    if not cluster:
        return "failed", True
    if cluster == "INIT":
        return "queued", False
    if cluster == "STOPPED":
        mapped = _JOB_STATES.get(job, "")
        if mapped in ("succeeded", "failed", "cancelled"):
            # the job reached its own verdict before the machine went away
            return mapped, False
        # the machine went away with work still on it — recoverable by
        # relaunching, which is what `use_spot` buys and what it costs.
        return "failed", True
    if cluster == "UP":
        if not job:
            return "running", False
        return _JOB_STATES.get(job, "running"), False
    return "failed", False


def _resources_str(task: dict) -> str:
    res = task.get("resources") or {}
    return ", ".join(f"{k}={v}" for k, v in sorted(res.items())) or "default"


__all__ = [
    "DEFAULT_SETUP", "RECEIPT_EVENT_TYPE", "REMOTE_COMMAND", "SkyPilotEngine",
    "AUTOSTOP_DEFAULT_MIN", "AUTOSTOP_GRACE_MIN", "FETCH_TIMEOUT_S",
    "REMOTE_WORKDIR_ROOT", "REQUIRED_BINARIES", "SKY_SSH_CONFIG_DIR",
    "SkyTarget", "autostop_minutes",
    "build_task", "cluster_name", "default_estimate", "fetch_outputs",
    "map_status", "missing_binaries", "remote_workdir",
    "resolve_target", "sky_sdk", "skypilot_importable", "ssh_config_path",
    "write_node_pool", "write_receipt",
]
