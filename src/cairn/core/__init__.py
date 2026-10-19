"""cairn.core — domain-neutral agentic runtime.

The engine. Knows nothing about incidents, telemetry, or runbooks.
Provides protocols (Tool, Executor, Approver, TriggerAdapter, MemoryStore,
RagSource, RunStore, Pack) and the runtime that wires them together.

Packs ship the substance (templates, skills, tools, RAG, memory, executors).
Discovery via Python entry-points: ``[project.entry-points."cairn.packs"]``.
"""
from __future__ import annotations

__version__ = "0.1.1"
