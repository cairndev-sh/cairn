"""Pilot observability MVP — Phase 1 (#242): structured cost rollup + the
run/operator trace spans.

Hermetic: cost is a pure function over result payloads; spans are exercised with
a fake tracer (no OTLP/collector). Default (no tracer) must be a zero-overhead
no-op — that's the contract that keeps keyless local-dev free.
"""
from __future__ import annotations

from contextlib import contextmanager

from cairn.core.dispatch.dispatcher import _aggregate_cost, _scan_usd
from cairn.core.obs import telemetry


# ── structured cost rollup ───────────────────────────────────────────

def test_cost_rollup_sums_llm_top_level_and_nested_gpu():
    payloads = [
        {"skill_name": "x", "llm_cost_usd": 0.18},                     # skill path
        {"template_name": "ft", "output": {"lineage": {"actual_usd": 37.80}}},  # nested GPU
    ]
    assert _aggregate_cost(payloads) == {
        "llm_usd": 0.18, "gpu_usd": 37.8, "storage_usd": 0.0, "total_usd": 37.98,
    }


def test_cost_rollup_empty_is_all_zero():
    assert _aggregate_cost([]) == {"llm_usd": 0.0, "gpu_usd": 0.0, "storage_usd": 0.0, "total_usd": 0.0}


def test_cost_rollup_buckets_storage_and_ignores_bools():
    payloads = [{"storage_usd": 0.01, "llm_cost_usd": 0.5, "cached": True}]
    c = _aggregate_cost(payloads)
    assert c["storage_usd"] == 0.01 and c["llm_usd"] == 0.5 and c["total_usd"] == 0.51


def test_scan_usd_recurses_lists_and_dicts():
    obj = [{"a": {"gpu_usd": 1.0}}, {"b": [{"gpu_usd": 2.0}]}]
    assert _scan_usd(obj, ("gpu_usd",)) == 3.0


# ── span behavior ────────────────────────────────────────────────────

def test_span_is_noop_without_tracer(monkeypatch):
    monkeypatch.setattr(telemetry, "_tracer", None)
    with telemetry.span("cairn.run", {"run_id": "r1"}) as s:
        assert s is None  # no tracer → yields None, never raises


def test_span_sets_attributes_when_tracer_present(monkeypatch):
    recorded = {"name": None, "attrs": {}}

    class _FakeSpan:
        def set_attribute(self, k, v): recorded["attrs"][k] = v

    class _FakeTracer:
        @contextmanager
        def start_as_current_span(self, name):
            recorded["name"] = name
            yield _FakeSpan()

    monkeypatch.setattr(telemetry, "_tracer", _FakeTracer())
    with telemetry.span("cairn.operator", {"operator": "dataset_checks/pii", "node": "scan", "skip_none": None}) as s:
        assert s is not None
    assert recorded["name"] == "cairn.operator"
    assert recorded["attrs"]["operator"] == "dataset_checks/pii"
    assert recorded["attrs"]["node"] == "scan"
    assert "skip_none" not in recorded["attrs"]  # None attrs are dropped


def test_current_trace_id_is_empty_without_tracer(monkeypatch):
    # tracing off → no span context to read → empty string (never raises).
    monkeypatch.setattr(telemetry, "_tracer", None)
    assert telemetry.current_trace_id() == ""


def test_current_trace_id_formats_active_span_as_32hex(monkeypatch):
    # active tracer + a recording span → 32-hex trace_id (#248). Uses the real
    # opentelemetry.trace API against a fabricated span context (no exporter).
    from opentelemetry import trace
    tid = 0x0AF7651916CD43DD8448EB211C80319C
    ctx = trace.SpanContext(trace_id=tid, span_id=0x1, is_remote=False)
    span = trace.NonRecordingSpan(ctx)
    monkeypatch.setattr(telemetry, "_tracer", object())  # non-None → "tracing on"
    with trace.use_span(span):
        assert telemetry.current_trace_id() == "0af7651916cd43dd8448eb211c80319c"


def test_current_trace_id_empty_for_invalid_span(monkeypatch):
    # the all-zero (invalid / no active) span context reads as absent.
    from opentelemetry import trace
    monkeypatch.setattr(telemetry, "_tracer", object())
    with trace.use_span(trace.INVALID_SPAN):
        assert telemetry.current_trace_id() == ""


def test_init_tracing_is_noop_without_endpoint(monkeypatch):
    # no OTLP endpoint / console flag configured → stays no-op, never raises
    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_CONSOLE", raising=False)
    telemetry.init_tracing()
    assert telemetry._tracer is None


def test_init_tracing_without_otlp_exporter_warns_and_returns(monkeypatch, caplog):
    # base install: SDK present, `cairndev[telemetry]` (OTLP exporter) absent (#248)
    import logging

    from cairn.core.provider_settings import get_settings

    monkeypatch.setattr(telemetry, "_tracer", None)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    monkeypatch.setitem(
        sys.modules, "opentelemetry.exporter.otlp.proto.http.trace_exporter", None
    )
    get_settings.cache_clear()
    try:
        with caplog.at_level(logging.WARNING, logger=telemetry.logger.name):
            telemetry.init_tracing()  # must not raise
    finally:
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        get_settings.cache_clear()
    assert telemetry._tracer is None
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "cairndev[telemetry]" in warnings[0]


# ── app metrics (#242 Phase 2) ───────────────────────────────────────

from cairn.core.obs import metrics  # noqa: E402


def test_metrics_are_noop_without_meter(monkeypatch):
    monkeypatch.setattr(metrics, "_meter", None)
    # must not raise when the meter isn't initialized
    metrics.record_run("done", workspace="acme", duration_ms=1200.0, cost={"total_usd": 0.19})
    metrics.record_operator_failure("dataset_checks/pii")


def test_record_run_emits_count_duration_and_cost(monkeypatch):
    calls = {"runs": [], "dur": [], "cost": []}

    class _Inst:
        def __init__(self, bucket): self.bucket = bucket
        def add(self, v, attrs): calls[self.bucket].append((v, attrs))
        def record(self, v, attrs): calls[self.bucket].append((v, attrs))

    monkeypatch.setattr(metrics, "_meter", object())  # non-None → active
    monkeypatch.setattr(metrics, "_runs", _Inst("runs"))
    monkeypatch.setattr(metrics, "_run_duration", _Inst("dur"))
    monkeypatch.setattr(metrics, "_run_cost", _Inst("cost"))

    metrics.record_run("done", workspace="acme", duration_ms=1200.0, cost={"total_usd": 37.98})
    assert calls["runs"] == [(1, {"status": "done", "workspace": "acme"})]
    assert calls["dur"] == [(1200.0, {"status": "done", "workspace": "acme"})]
    assert calls["cost"] == [(37.98, {"status": "done", "workspace": "acme"})]

    # zero-cost run records the count but skips the cost counter
    calls["cost"].clear()
    metrics.record_run("failed", workspace="acme", cost={"total_usd": 0.0})
    assert calls["cost"] == []


def test_init_metrics_is_noop_without_endpoint(monkeypatch):
    monkeypatch.setattr(metrics, "_meter", None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_CONSOLE", raising=False)
    metrics.init_metrics()
    assert metrics._meter is None


# ── LLM GenAI-semconv span content (#242 Phase 3 → Langfuse) ─────────

import sys
import cairn.core.llm.client as llm_client  # noqa: E402


def test_gen_ai_system_provider_mapping():
    assert llm_client._gen_ai_system("claude-opus-4-8") == "anthropic"
    assert llm_client._gen_ai_system("gpt-4o-mini") == "openai"
    assert llm_client._gen_ai_system("gemini-1.5-pro") == "google"
    assert llm_client._gen_ai_system("groq/llama-3.1-70b") == "groq"
    assert llm_client._gen_ai_system("???") == "litellm"


async def test_llm_span_carries_full_prompt_and_completion(monkeypatch):
    """#242 Phase 3 — the llm.complete span carries GenAI-semconv metadata AND
    the full prompt + completion. Content now goes through the #247 gate, so
    verify verbatim capture in ``raw`` mode (the gate's redacted/off behavior is
    covered by test_trace_content_gate)."""
    monkeypatch.setenv("OBS_TRACE_CONTENT", "raw")
    recorded: dict = {}

    class _FakeSpan:
        def set_attribute(self, k, v): recorded[k] = v

    class _FakeTracer:
        @contextmanager
        def start_as_current_span(self, name): yield _FakeSpan()

    monkeypatch.setattr(telemetry, "_tracer", _FakeTracer())

    async def _fake_acompletion(**params):
        class _Msg:
            content = "the capital is Paris"
            tool_calls = None
        class _Choice:
            message = _Msg()
        class _Resp:
            choices = [_Choice()]
            usage = type("U", (), {"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16})()
        return _Resp()

    monkeypatch.setattr(llm_client, "acompletion", _fake_acompletion)
    await llm_client.llm_complete(
        messages=[{"role": "user", "content": "capital of France?"}], model="claude-opus-4-8",
    )

    assert recorded["gen_ai.system"] == "anthropic"
    assert recorded["gen_ai.request.model"] == "claude-opus-4-8"
    assert "capital of France?" in recorded["gen_ai.prompt"]      # FULL prompt sent
    assert recorded["gen_ai.completion"] == "the capital is Paris"  # FULL completion sent
    assert recorded["gen_ai.usage.input_tokens"] == 11
    assert recorded["gen_ai.usage.output_tokens"] == 5
