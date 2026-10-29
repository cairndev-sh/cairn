"""Cross-cutting observability layer.

Public surface — import from ``cairn.core.obs`` directly:

    from cairn.core import obs
    obs.start(request_id, session_id, query)
    obs.record_mcp(tool, ms, n_bytes)
    obs.finish(answer)
"""
from cairn.core.obs.base import (
    RequestCtx,
    cost_scope,
    finish,
    get,
    record_mcp,
    record_timeout,
    set_route,
    start,
)
from cairn.core.obs.audit import (
    AuditEvent,
    AuditSink,
    NullAuditSink,
    audit_emit,
    get_audit_sink,
    reset_audit_sink,
)
from cairn.core.obs.cost import BudgetExceeded, merge_cost, record_llm
from cairn.core.obs.guard import sanitize_query
from cairn.core.obs.redact import (
    NoOpRedactor,
    RedactionResult,
    Redactor,
    RedactSpan,
    get_redactor,
    redact_messages,
    redact_text,
    reset_redactor,
)

__all__ = [
    "AuditEvent",
    "AuditSink",
    "BudgetExceeded",
    "NoOpRedactor",
    "NullAuditSink",
    "RedactionResult",
    "Redactor",
    "RedactSpan",
    "RequestCtx",
    "audit_emit",
    "cost_scope",
    "finish",
    "get",
    "get_audit_sink",
    "get_redactor",
    "merge_cost",
    "record_llm",
    "record_mcp",
    "record_timeout",
    "redact_messages",
    "redact_text",
    "reset_audit_sink",
    "reset_redactor",
    "sanitize_query",
    "set_route",
    "start",
]
