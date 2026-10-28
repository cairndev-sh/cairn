"""Temporal runtime — same StateGraph as in-process, durable execution.

The plugin (``temporalio.contrib.langgraph``) reads each node's metadata to
decide whether it runs inline in the workflow body (``execute_in='workflow'``)
or as a separate Activity (``execute_in='activity'``). The :class:`NodeKind`
tags stamped by the compiler (Step 1) drive that translation.

Layout::

  retry.py        # per-NodeKind retry/timeout defaults + per-template override
  temporal/
    __init__.py   # is_available + lazy re-exports
    registry.py   # worker-side compiled-template store (name + sha → graph)
    runtime.py    # TemporalRuntime — submits workflows from the caller side
    workflow.py   # @workflow.defn TemplateWorkflow (imports temporalio)
    activities.py # @activity.defn run_node (imports temporalio)
    worker.py     # bootstrap: load packs → compile all → start Temporal worker

``temporalio`` is an *optional* dep. Importing this package is safe without it;
only :func:`require` raises if missing. The InProcessRuntime path never reaches
into this subpackage at all.
"""

from __future__ import annotations

import importlib.util


#: What the durable pipeline owner (``pipeline_workflow.PipelineWorkflow`` with
#: ``pipeline_activities.pipeline_submit_step``) can run. The launch door reads these
#: (#199) and refuses, before any record exists, a plan this owner would start and
#: not finish. They change with the owner, in the same commit: ``task`` joins the leg
#: kinds when SUBMIT routes task legs (#59; today every non-flow leg goes to
#: ``run_template``), and finalizers turn on when the workflow runs ``finalizer:``
#: steps after a terminal intent (ADR-0151 D10; today it schedules by ``depends_on``
#: only, so a trailing teardown would run on success alone).
PIPELINE_OWNER_LEG_KINDS: frozenset[str] = frozenset({"flow", "template", "gate"})
PIPELINE_OWNER_RUNS_FINALIZERS: bool = False


def is_available() -> bool:
    """True iff ``temporalio[langgraph]`` is importable."""
    return importlib.util.find_spec("temporalio") is not None and (
        importlib.util.find_spec("temporalio.contrib.langgraph") is not None
    )


def require() -> None:
    """Raise a helpful ImportError if the Temporal SDK + plugin aren't installed."""
    if not is_available():
        raise ImportError(
            "cairn Temporal runtime requires the Temporal Python SDK "
            "with the LangGraph plugin. Install with:\n\n"
            '    pip install "temporalio[langgraph]>=1.27.0"\n'
        )


__all__ = [
    "PIPELINE_OWNER_LEG_KINDS", "PIPELINE_OWNER_RUNS_FINALIZERS", "is_available", "require",
]
