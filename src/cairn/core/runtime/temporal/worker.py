"""Temporal worker bootstrap.

Run by ``scripts/temporal_worker.py``. Responsibilities at startup:

  1. Load every installed pack (``load_all_packs()``).
  2. Discover every template file across packs.
  3. Compile each into the :class:`TemplateRegistry`. This is where the LLM
     clients get instantiated — outside Temporal's deterministic sandbox.
  4. Translate per-node ``NodeKind`` / ``RetryPolicy`` stamps into the LangGraph
     plugin's expected metadata (``execute_in`` + ``retry_policy`` +
     ``start_to_close_timeout``). Activity-tagged nodes route through the
     generic ``run_node`` activity at runtime.
  5. Start the worker with :class:`TemplateWorkflow` and the run_node activity.

Idempotent: re-running it on a new template version registers a new sha and
the runtime can fan in-flight workflows out to the appropriate version.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from pathlib import Path
from typing import Any, Callable

from cairn.core import settings
from cairn.core.obs.telemetry import init_tracing
from cairn.core.provider_settings import load_env
from cairn.core.runtime.base import (
    QUEUE_DATASET,
    QUEUE_REMOTE,
    NodeKind,
    get_kind,
    get_queue,
    get_retry,
)
from cairn.core.runtime.retry import default_for
from cairn.core.runtime.temporal import require
from cairn.core.runtime.temporal.registry import (
    TemplateRegistry,
    bind as bind_registry,
)

logger = logging.getLogger("cairn.runtime.temporal.worker")


def build_registry(*, strict: bool = True) -> TemplateRegistry:
    """Load packs, compile every template, bind the process-local registry.

    Compilation instantiates each template's LLM client (outside Temporal's
    deterministic sandbox), so a misconfigured environment — most commonly a
    missing provider key (``OPENAI_API_KEY``) at worker startup — makes *every*
    compile fail. This used to be swallowed per-template as a warning and the
    worker booted with an empty registry: healthy-looking, but answering every
    run with "Graph not found" (#412). Fail loud instead — if templates were
    discovered but none compiled, raise with the collected reasons. Pass
    ``strict=False`` (or set ``CAIRN_TEMPORAL_ALLOW_EMPTY=1``) to allow a
    deliberately empty worker.
    """
    from cairn.core.operator import load_all_operators
    from cairn.core.pack import all_template_dirs, ensure_loaded

    # Operators first: operation BT nodes resolve their operator class at
    # compile time, and entry-point discovery doesn't run implicitly here
    # (same priming the CLI's direct-template path does).
    load_all_operators(strict=False)
    ensure_loaded()
    registry = TemplateRegistry()
    discovered = 0
    failures: list[tuple[Path, Exception]] = []
    for tdir in all_template_dirs():
        for path in Path(tdir).glob("*.md"):
            discovered += 1
            try:
                registry.register(path)
            except Exception as exc:  # noqa: BLE001 — collect, decide after loop
                failures.append((path, exc))
                logger.warning("failed to compile %s: %s", path, exc)
    registered = len(registry.names())
    if failures:
        logger.error(
            "template compilation: %d of %d template(s) failed to compile",
            len(failures), discovered,
        )
    if discovered and not registered and strict:
        reasons = "; ".join(f"{p.name}: {exc}" for p, exc in failures[:5])
        raise RuntimeError(
            f"temporal worker: all {discovered} discovered template(s) failed to "
            "compile — refusing to start a worker that would answer every run "
            "with 'Graph not found'. This is almost always a missing provider "
            "key (e.g. OPENAI_API_KEY) in the worker's environment at startup. "
            f"First reasons: {reasons}. "
            "Set CAIRN_TEMPORAL_ALLOW_EMPTY=1 to boot an empty worker anyway."
        )
    bind_registry(registry)
    return registry


def _installed_pipeline_count() -> int:
    """How many parseable pipelines the installed packs declare."""
    from cairn.core.engine.pipeline_resolve import all_pipeline_defs  # noqa: PLC0415 - only on the 0-graph boot path; keeps pack scanning off the worker import

    return len(all_pipeline_defs())


def ensure_work_to_serve(
    *,
    graph_count: int,
    template_count: int,
    strict: bool,
    pipeline_count: Callable[[], int] = _installed_pipeline_count,
) -> None:
    """Refuse to boot a worker with nothing to serve (#412, #2542).

    The worker serves two things: ``TemplateWorkflow`` over the plugin's
    state_machine graphs, and ``PipelineWorkflow`` (ADR-0069 Phase 4) over the
    installed pipelines. It has work when either exists, so it refuses only when
    there are no state_machine graphs AND no installed pipelines — a registry
    with templates but no state_machine graphs answers every template run with
    'Graph not found', and without pipelines nothing else could reach it.
    ``strict=False`` (``CAIRN_TEMPORAL_ALLOW_EMPTY=1``) skips the check.
    Pipelines are counted only when there are no graphs, so the common path
    never pays for pipeline discovery.
    """
    if graph_count or not strict:
        return
    pipelines = pipeline_count()
    if pipelines:
        logger.info(
            "temporal worker: no state_machine graphs; serving %d installed "
            "pipeline(s) through PipelineWorkflow",
            pipelines,
        )
        return
    raise RuntimeError(
        "temporal worker: no state_machine graphs to serve and no installed "
        f"pipelines ({template_count} template(s) registered, none are "
        "state_machine flows; 0 pipelines installed). Refusing to start — every "
        "template run would fail 'Graph not found' and no pipeline could run "
        "here. Set CAIRN_TEMPORAL_ALLOW_EMPTY=1 to override."
    )


def _wrap_node_for_events(runnable: Any, node_name: str) -> None:
    """Wrap an activity-tagged node's async callable so it emits a per-node event
    to the shared store (+bus) AFTER it runs (#967).

    Under Temporal the langgraph plugin dispatches its OWN `{graph}.{node}`
    activities and never calls cairn's `run_node` — so `_record_node_event` was
    never reached and the Pilot per-node trace / live console progress stayed
    empty. This wrapper runs INSIDE the plugin's activity (worker process), where
    ``activity.info().workflow_id`` (the run_id) and store/bus side effects are
    available. Applied to activity nodes only — PURE nodes run in the workflow
    thread where I/O is forbidden (determinism). Best-effort: emission never
    breaks the node. Preserves the callable's stamped ``__qualname__`` (the
    plugin's task cache keys on it) via ``functools.wraps``."""
    orig = getattr(runnable, "afunc", None)
    if orig is None or not asyncio.iscoroutinefunction(orig):
        return  # sync/PURE glue nodes don't get an async emit wrapper

    from cairn.core.runtime.temporal.activities import _record_node_event  # noqa: PLC0415 — temporal-only, avoids cycle

    @functools.wraps(orig)
    async def _emitting(*args: Any, **kwargs: Any) -> Any:
        from cairn.core import obs  # noqa: PLC0415 — worker node-local accounting

        with obs.cost_scope(f"temporal-node-{node_name}") as cost_ctx:
            out = await orig(*args, **kwargs)
        try:
            await _record_node_event(
                node_name,
                out if isinstance(out, dict) else {},
                llm_cost_usd=cost_ctx.cost_usd,
            )
        except Exception:  # noqa: BLE001 — a node's real work already succeeded
            pass
        return out

    runnable.afunc = _emitting


def adapt_graph_for_temporal(state_graph: Any, graph_name: str = "graph") -> Any:
    """Translate :class:`NodeKind` / :class:`RetryPolicy` tags into the
    LangGraph plugin's per-node metadata format.

    Walks ``state_graph.nodes``; for each node fn previously stamped via
    :func:`cairn.core.runtime.base.tag`:

      - PURE  → ``execute_in='workflow'`` (inline, deterministic)
      - LLM/TOOL/IO → ``execute_in='activity'`` + the resolved retry policy

    Mutates the StateGraph in place and returns it for chaining.

    Note: langgraph's StateGraph stores node metadata internally; the precise
    mutation surface depends on the langgraph version. This function is the
    single seam where that translation lives — if langgraph changes its node
    metadata API, this is the one place to update.
    """
    require()
    from temporalio.common import RetryPolicy as TemporalRetryPolicy  # noqa: PLC0415

    for name, node in state_graph.nodes.items():
        # Unwrap to the original compiler closure: langgraph coerces
        # callables into RunnableCallable specs, and the NodeKind tag
        # lives on the inner fn (.afunc for async, .func for sync).
        runnable = getattr(node, "runnable", node)
        fn = (
            getattr(runnable, "afunc", None)
            or getattr(runnable, "func", None)
            or runnable
        )
        kind = get_kind(fn, default=NodeKind.IO)
        if kind == NodeKind.PURE:
            metadata = {"execute_in": "workflow"}
        else:
            # The plugin's task cache derives identity from
            # ``func.__module__ + __qualname__`` and rejects closures
            # (``<locals>`` in the qualname). Compiler node fns ARE
            # closures by design, so stamp a stable synthetic identity —
            # deterministic across worker restarts (graph + node name,
            # no memory addresses) so cache keys stay consistent.
            qn = getattr(fn, "__qualname__", "") or ""
            if "<locals>" in qn or not qn:
                try:
                    fn.__qualname__ = f"cairn_node__{graph_name}__{name}"
                except (AttributeError, TypeError):
                    logger.warning(
                        "cannot stamp task identity on node %s.%s", graph_name, name,
                    )
            policy = get_retry(fn) or default_for(kind)
            tspec = policy.to_temporal()
            metadata = {
                "execute_in": "activity",
                "start_to_close_timeout": tspec["start_to_close_timeout"],
                "retry_policy": TemporalRetryPolicy(**tspec["retry_policy_kwargs"]),
            }
            # ADR-0080 P0 (#1028) — a declared heartbeat gives Temporal fast
            # stuck-detection; present only when the operator declared one.
            if "heartbeat_timeout" in tspec:
                metadata["heartbeat_timeout"] = tspec["heartbeat_timeout"]
            # #565: route a remote-job node's activity to the dedicated remote
            # queue so a pull-based worker (cairn worker --remote) runs it. The
            # plugin forwards any execute_activity kwarg (incl. task_queue) from
            # node metadata. No remote queue configured → stays on the default
            # queue (unchanged single-worker behavior).
            node_queue = get_queue(fn)
            if node_queue == QUEUE_REMOTE:
                remote_q = settings.temporal_remote_task_queue()
                if remote_q:
                    metadata["task_queue"] = remote_q
            elif node_queue == QUEUE_DATASET:
                # #962 dispatcher A: route a Shardable dataset op's activity to the
                # isolated dataset-batch worker pool. Unset → stays on the default
                # queue (dispatcher C: local budgeted pool), unchanged behavior.
                dataset_q = settings.temporal_dataset_task_queue()
                if dataset_q:
                    metadata["task_queue"] = dataset_q
            # #967 — emit a per-node event from inside the plugin's activity so the
            # Pilot trace + live console get progress (the plugin bypasses cairn's
            # run_node). After the qualname stamp so the wrapper inherits it.
            _wrap_node_for_events(runnable, name)
        # StateNodeSpec.metadata defaults to None on nodes added without
        # explicit metadata — assign a fresh dict rather than skipping, or
        # the plugin (which reads node metadata, not fn attributes) would
        # see no execute_in and reject the graph.
        node.metadata = {**(node.metadata or {}), **metadata}
        logger.debug("temporal metadata name=%s kind=%s -> %s", name, kind, metadata)
    return state_graph


async def run_worker(task_queue: str | None = None) -> None:
    """Boot the worker. Blocks until cancelled.

    ``task_queue`` defaults to the main queue (:func:`settings.temporal_task_queue`).
    Pass :func:`settings.temporal_remote_task_queue` to run a **pull-based remote
    worker** (#565) that drains remote-job (SSH/GPU) node activities the main
    worker routed to that queue — same graphs/activities, different queue.
    """
    require()
    from temporalio.client import Client  # noqa: PLC0415
    from temporalio.contrib.langgraph import LangGraphPlugin  # noqa: PLC0415
    from temporalio.worker import Worker  # noqa: PLC0415
    from temporalio.worker.workflow_sandbox import (  # noqa: PLC0415
        SandboxedWorkflowRunner,
        SandboxRestrictions,
    )

    from cairn.core.runtime.temporal.activities import ACTIVITIES
    from cairn.core.runtime.temporal.pipeline_activities import ACTIVITIES as PIPELINE_ACTIVITIES
    from cairn.core.runtime.temporal.pipeline_workflow import WORKFLOWS as PIPELINE_WORKFLOWS
    from cairn.core.runtime.temporal.workflow import WORKFLOWS

    strict = not settings.temporal_allow_empty()
    registry = build_registry(strict=strict)

    # Translate every registered template's graph metadata for the plugin.
    plugin_graphs: dict[str, Any] = {}
    for name in registry.names():
        entry = registry.get(name)
        # Only state_machine flows have a StateGraph worth plugin-registering.
        graph_obj = getattr(entry.compiled.runnable, "_state_graph", None)
        if graph_obj is None:
            logger.debug("skipping non-state_machine template name=%s", name)
            continue
        plugin_graphs[name] = adapt_graph_for_temporal(graph_obj, graph_name=name)

    ensure_work_to_serve(
        graph_count=len(plugin_graphs),
        template_count=len(registry.names()),
        strict=strict,
    )

    client = await Client.connect(
        settings.temporal_address(),
        namespace=settings.temporal_namespace(),
    )
    plugin = LangGraphPlugin(graphs=plugin_graphs)
    # Temporal's default sandbox re-imports workflow modules to enforce
    # determinism; TemplateWorkflow's import chain pulls in cairn.* (e.g.
    # settings calls Path.home() at module level), which the sandbox rejects
    # (RestrictedWorkflowAccessError, #44). Pass cairn through — it's trusted,
    # and non-deterministic work runs in activities, not the workflow thread.
    # LangGraphPlugin.workflow_runner() extends this runner with its own
    # passthrough (langchain/langgraph/numpy), so the two compose (_plugin.py).
    runner = SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"),
    )
    queue = task_queue or settings.temporal_task_queue()
    worker = Worker(
        client,
        task_queue=queue,
        # TemplateWorkflow (one run) + PipelineWorkflow (the bundle-pipeline owner,
        # ADR-0069 Phase 4) — the same worker serves both tiers.
        workflows=WORKFLOWS + PIPELINE_WORKFLOWS,
        activities=ACTIVITIES + PIPELINE_ACTIVITIES,
        plugins=[plugin],
        workflow_runner=runner,
    )
    logger.info(
        "temporal worker ready task_queue=%s templates=%d",
        queue,
        len(plugin_graphs),
    )
    await worker.run()


def main(task_queue: str | None = None) -> None:
    load_env()  # real OPENAI_API_KEY etc. into os.environ before client construction
    logging.basicConfig(level=logging.INFO)
    from cairn.core.pack.profile import ProfileError, require_deployment_profile

    try:  # the worker runs legs under the same bindings as the server (#2296)
        require_deployment_profile()
    except ProfileError as e:
        logging.getLogger("cairn.worker").error("refusing to start: %s", e)
        raise SystemExit(2) from None
    # #414: initialize the tracer in the WORKER too. Node/LLM spans are emitted
    # from inside the run_node activity (this process), but init_tracing() was
    # only ever called in the FastAPI server lifespan — so under Temporal every
    # span() no-op'd and no GenAI trace reached the collector despite
    # OTEL_EXPORTER_OTLP_ENDPOINT being set. No-op when tracing is unconfigured.
    init_tracing(service_name="cairn-worker")
    asyncio.run(run_worker(task_queue=task_queue))
