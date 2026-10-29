"""OpenTelemetry metrics for the run loop (#242 Phase 2).

App-level metrics emitted via the OTel Metrics API — the same SDK + OTLP pipe as
tracing (:mod:`cairn.core.obs.telemetry`), so one collector fans out to
Prometheus / ClickStack. Prometheus is a *backend behind the collector*, never
the capture layer here.

Per-run *facts* (a specific run's cost/status) live in the Postgres run ledger
and are queried by SQL — NOT pushed here (high-cardinality ``run_id`` labels are
a Prometheus anti-pattern). These instruments are deliberately low-cardinality
fleet aggregates: count/duration/cost/failures bucketed by status + workspace.

Instruments:
  - ``cairn.runs``            counter   — completed runs, by ``status``/``workspace``
  - ``cairn.run.duration_ms`` histogram — wall-clock per run
  - ``cairn.run.cost_usd``    counter   — total spend (the run's ``cost.total_usd``)
  - ``cairn.operator.failures`` counter — operator-call failures, by ``operator``

Configure via env (same knobs as tracing): ``OTEL_EXPORTER_OTLP_ENDPOINT``
(send to a collector) / ``OTEL_CONSOLE=1`` (print). Neither set → no-op, zero
overhead.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_meter: Any = None
_runs: Any = None
_run_duration: Any = None
_run_cost: Any = None
_operator_failures: Any = None


def init_metrics(service_name: str | None = None) -> None:
    """Initialize the OTel meter + instruments if an exporter is configured.
    Safe to call multiple times (idempotent); no-op when no endpoint is set."""
    global _meter, _runs, _run_duration, _run_cost, _operator_failures

    if _meter is not None:
        return

    from cairn.core.provider_settings import get_settings
    settings = get_settings()
    if not settings.tracing_enabled:  # same gate as tracing: no exporter → stay no-op
        return

    otlp_endpoint = settings.otel_exporter_otlp_endpoint
    service_name = service_name or settings.otel_service_name

    try:
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import (
            ConsoleMetricExporter,
            PeriodicExportingMetricReader,
        )
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    except ImportError:
        logger.warning("OpenTelemetry metrics SDK not installed — metrics disabled")
        return

    readers = []
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            readers.append(PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=f"{otlp_endpoint}/v1/metrics")
            ))
        except ImportError:
            logger.warning("OTLP metric exporter not installed (`pip install 'cairndev[telemetry]'`) — metrics disabled")
            return
    if settings.otel_console:
        readers.append(PeriodicExportingMetricReader(ConsoleMetricExporter()))

    provider = MeterProvider(
        resource=Resource(attributes={SERVICE_NAME: service_name}),
        metric_readers=readers,
    )
    metrics.set_meter_provider(provider)
    _meter = metrics.get_meter(service_name)
    _runs = _meter.create_counter("cairn.runs", description="completed runs by status")
    _run_duration = _meter.create_histogram("cairn.run.duration_ms", unit="ms", description="run wall-clock")
    _run_cost = _meter.create_counter("cairn.run.cost_usd", unit="usd", description="total run spend")
    _operator_failures = _meter.create_counter("cairn.operator.failures", description="operator-call failures")
    logger.info("OpenTelemetry metrics initialized (otlp=%s, console=%s)", bool(otlp_endpoint), settings.otel_console)


def record_run(
    status: str,
    *,
    workspace: str = "default",
    duration_ms: Optional[float] = None,
    cost: Optional[dict] = None,
) -> None:
    """Record one finished run (no-op if metrics aren't initialized)."""
    if _meter is None:
        return
    attrs = {"status": status, "workspace": workspace}
    _runs.add(1, attrs)
    if duration_ms is not None:
        _run_duration.record(float(duration_ms), attrs)
    total = float((cost or {}).get("total_usd", 0) or 0)
    if total:
        _run_cost.add(total, attrs)


def record_operator_failure(operator: str, *, workspace: str = "default") -> None:
    """Record one operator-call failure (no-op if metrics aren't initialized)."""
    if _meter is None:
        return
    _operator_failures.add(1, {"operator": operator, "workspace": workspace})
