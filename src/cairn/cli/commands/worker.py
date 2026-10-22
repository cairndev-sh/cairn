"""``cairn worker`` — the machine that runs the work, in either pull shape (#565).

Two runtimes, one command, because a worker is a worker:

* ``--remote`` (Temporal): polls the dedicated remote/GPU **task queue**
  (``CAIRN_TEMPORAL_REMOTE_TASK_QUEUE``) and drains the remote-job node activities
  the main worker routes there. Unchanged.
* ``--pull`` (ADR-0104 D2): claims from the control plane's **job store** over
  HTTPS (``POST /jobs/claim``), runs the job's image, heartbeats to hold the
  lease, and reports over the job channel. This is the shape that works under
  the DBOS runtime prod actually runs, where no Temporal queue exists.

Both dial OUT — no inbound port, no central SSH key — so the machine can sit on
a GPU box behind NAT. ``--task-queue`` overrides the Temporal queue explicitly.
"""
from __future__ import annotations

import asyncio
from typing import List, Optional

import typer

app = typer.Typer(help="Run a cairn worker — Temporal task queue or HTTP pull (#565).",
                  no_args_is_help=False)


@app.callback(invoke_without_command=True)
def worker_cmd(
    remote: bool = typer.Option(
        False, "--remote", help="Poll the dedicated remote/GPU queue (CAIRN_TEMPORAL_REMOTE_TASK_QUEUE)."
    ),
    pull: bool = typer.Option(
        False, "--pull", help="Claim jobs from the control plane over HTTP (ADR-0104 D2) — no Temporal.",
    ),
    engine_url: Optional[str] = typer.Option(
        None, "--engine-url", envvar="CAIRN_ENGINE_URL", help="--pull: the control plane's base URL.",
    ),
    token: Optional[str] = typer.Option(
        None, "--token", envvar="CAIRN_ENGINE_TOKEN", help="--pull: a worker credential: a worker-pool token (cairn remote worker-token) or the deployment admin's token. A user's token, and a per-run worker token, cannot claim.",
    ),
    capabilities: Optional[List[str]] = typer.Option(
        None, "--capabilities", "-c", help="--pull: what this machine offers, e.g. gpu=T4 (repeatable).",
    ),
    image: Optional[str] = typer.Option(
        None, "--image", help="--pull: pin the container image to run, overriding the job's.",
    ),
    exec_local: bool = typer.Option(
        False, "--exec-local", help="--pull: run the operator IN THIS PROCESS instead of a container.",
    ),
    no_gpus: bool = typer.Option(
        False, "--no-gpus", help="--pull: drop `--gpus all` from the docker run (a CPU worker).",
    ),
    once: bool = typer.Option(
        False, "--once",
        help="--pull: one claim pass — run the job if there is one, exit 0 if not, non-zero "
             "if the control plane refuses or cannot be reached.",
    ),
    lease_seconds: int = typer.Option(
        300, "--lease-seconds", help="--pull: how long a claim is held before the job is re-queued.",
    ),
    poll_seconds: float = typer.Option(
        5.0, "--poll-seconds", help="--pull: how long to wait before asking again on an empty queue.",
    ),
    worker_id: Optional[str] = typer.Option(
        None, "--worker-id", help="--pull: this worker's identity (default: hostname + a random suffix).",
    ),
    dataset: bool = typer.Option(
        False, "--dataset", help="Poll the dedicated dataset-batch queue (CAIRN_TEMPORAL_DATASET_TASK_QUEUE) — #962."
    ),
    task_queue: Optional[str] = typer.Option(
        None, "--task-queue", help="Override the task queue to poll (takes precedence over --remote/--dataset)."
    ),
) -> None:
    """Start a worker. Blocks until interrupted."""
    if pull:
        _run_pull_worker(
            engine_url=engine_url, token=token, capabilities=capabilities, image=image,
            exec_local=exec_local, gpus=not no_gpus, once=once, lease_s=float(lease_seconds),
            poll_s=float(poll_seconds), worker_id=worker_id,
        )
        return

    from cairn.core import settings
    from cairn.core.runtime.temporal import is_available

    if not is_available():
        typer.secho(
            "temporalio[langgraph] is not installed — install the 'runtime-temporal' "
            "extra to run a worker.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(2)

    queue = task_queue
    if queue is None and remote:
        queue = settings.temporal_remote_task_queue()
        if not queue:
            typer.secho(
                "--remote needs CAIRN_TEMPORAL_REMOTE_TASK_QUEUE set (the remote/GPU "
                "queue name). Set it on both the main worker's env (so it routes "
                "remote-job nodes there) and here.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(2)
    if queue is None and dataset:
        queue = settings.temporal_dataset_task_queue()
        if not queue:
            typer.secho(
                "--dataset needs CAIRN_TEMPORAL_DATASET_TASK_QUEUE set (the dataset-batch "
                "queue name). Set it on both the main worker's env (so it routes "
                "Shardable dataset nodes there) and here. Run this worker with "
                "CAIRN_BATCH_RESERVED_CORES=0 on an isolated container (cgroup cpus quota).",
                fg=typer.colors.RED,
            )
            raise typer.Exit(2)

    resolved = queue or settings.temporal_task_queue()
    typer.secho(f"starting temporal worker on task_queue={resolved}", fg=typer.colors.GREEN)

    from cairn.core.runtime.temporal.worker import main as run

    run(task_queue=queue)


def _run_pull_worker(*, engine_url, token, capabilities, image, exec_local, gpus, once,
                     lease_s, poll_s, worker_id) -> None:
    """``cairn worker --pull`` (ADR-0104 D2, #565): claim → run → heartbeat →
    report, against a control plane this machine dials out to."""
    # deliberate CLI-startup deferral (same as the temporal imports above): the
    # worker stack pulls httpx + the operator registry, which `cairn --help` never needs.
    from cairn.core.pull_worker import (
        PullWorker,
        default_worker_id,
        docker_runner,
        local_exec_runner,
        parse_capabilities,
    )

    from cairn.core.settings import getenv

    url = (engine_url or getenv("CAIRN_ENGINE_URL") or "").strip()  # + its OBS_ spelling
    if not url:
        typer.secho(
            "--pull needs --engine-url (or CAIRN_ENGINE_URL): the control plane this "
            "machine claims work from. Nothing dials IN to a worker.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(2)
    caps = parse_capabilities(capabilities)
    wid = worker_id or default_worker_id()
    runner = local_exec_runner() if exec_local else docker_runner(gpus=gpus, image=image)
    worker = PullWorker(url, (token or "").strip(), worker_id=wid, capabilities=caps,
                        runner=runner, lease_s=lease_s, poll_s=poll_s, image=image)
    how = "in-process (--exec-local)" if exec_local else f"docker run{' --gpus all' if gpus else ''}"
    typer.secho(
        f"pull worker {wid} claiming from {url} — capabilities={caps or '{}'}, runs via {how}",
        fg=typer.colors.GREEN,
    )

    async def _go() -> int:
        try:
            if once:  # #2536: one claim pass, never the retry loop
                return 0 if await worker.run_once() is None else 1
            return await worker.run_forever()
        finally:
            await worker.aclose()

    try:
        done = asyncio.run(_go())
    except KeyboardInterrupt:
        typer.secho("stopped", fg=typer.colors.YELLOW)
        return
    except Exception as e:  # noqa: BLE001 — only --once gets here; the loop retries its own errors
        typer.secho(_claim_failure(e, url), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from e
    if once and not done:
        typer.secho("no job to claim — nothing is queued for this worker", fg=typer.colors.YELLOW)
        return
    typer.secho(f"ran {done} job(s)", fg=typer.colors.GREEN)


def _claim_failure(e: Exception, url: str) -> str:
    """Why ``--once`` could not finish its claim pass, in words a person can act on."""
    import httpx  # noqa: PLC0415 — deliberate CLI-startup deferral, as for the worker stack above

    if isinstance(e, httpx.HTTPStatusError):
        code = e.response.status_code
        hint = " — check --token (or CAIRN_ENGINE_TOKEN)" if code in (401, 403) else ""
        return f"the control plane at {url} refused the worker's claim or job report: HTTP {code}{hint}"
    if isinstance(e, httpx.TransportError):
        return f"cannot reach the control plane at {url}: {type(e).__name__}: {e}"
    return f"the claim pass against {url} failed: {type(e).__name__}: {e}"
