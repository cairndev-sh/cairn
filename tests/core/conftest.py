"""Fixtures shared by the trigger-spec and trigger-router tests (#1543).

These two YAMLs used to live in the tree, as
``cairn/packs/incident/triggers/{pagerduty,prometheus-alert}.yaml``, and both
test modules read them from that path. #1543 deleted them: they were not inert
samples — ``TriggerRouter.from_installed()`` mounted them as live HTTP ingress
and a matched alert launched ``incident/incident-rca`` DIRECTLY, around the
``accept-root-cause`` gate that ``pipelines/rca.yaml`` puts in front of the
postmortem.

The router and loader contracts they exercised are still worth pinning, so the
fixtures moved here. They are now what they always claimed to be — canonical
*example* vendor adapters — and no first-party pack ships an ingress that
bypasses its own pipeline.

``_no_real_artifact_bucket`` (autouse, #64) pins artifact and other object-store
settings to a local store for every test under ``tests/core``. Request
``allow_artifact_bucket`` to opt out.
"""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

PAGERDUTY_YAML = dedent("""
    source: pagerduty
    description: PagerDuty incident webhook (v3 events API)

    ingress:
      transport: http
      path: /triggers/pagerduty
      method: POST
      signature:
        header: X-PagerDuty-Signature
        secret: ${env:PAGERDUTY_WEBHOOK_SECRET}

    normalize:
      event_type: event.event_type
      severity:   event.data.severity
      service:    event.data.service.summary
      alert_id:   event.id
      occurred_at: event.occurred_at
      summary:    event.data.title
      raw:        event.data

    match:
      event_type:
        - incident.triggered
      severity:
        - critical
        - high

    run:
      template: incident/incident-rca
      input:
        symptom:    $event.summary
        service:    $event.service
        started_at: $event.occurred_at
        source:     pagerduty

    example: examples/pagerduty-incident.json
""")

#: Alertmanager fires GROUPS, so this one carries the `fan_out:` block — the
#: only fixture in the suite that exercises one-payload-to-N-events.
PROMETHEUS_YAML = dedent("""
    source: prometheus
    description: Prometheus Alertmanager webhook (v4 schema)

    ingress:
      transport: http
      path: /triggers/prometheus
      method: POST
      signature:
        header: X-Webhook-Signature
        secret: ${env:PROMETHEUS_WEBHOOK_SECRET}
        required: false

    fan_out:
      iterate: alerts

    normalize:
      event_type: status
      severity:   labels.severity
      service:    labels.service
      alert_id:   fingerprint
      occurred_at: startsAt
      summary:    annotations.summary
      raw:        .

    match:
      event_type:
        - firing
      severity:
        - critical
        - high

    run:
      template: incident/incident-rca
      input:
        symptom:    $event.summary
        service:    $event.service
        started_at: $event.occurred_at

    example: examples/kubernetes-latency-alert.json
""")


@pytest.fixture
def vendor_trigger_yamls() -> dict[str, str]:
    """``filename → YAML text`` for the two example vendor adapters."""
    return {
        "pagerduty.yaml": PAGERDUTY_YAML,
        "prometheus-alert.yaml": PROMETHEUS_YAML,
    }


@pytest.fixture
def vendor_triggers_dir(tmp_path: Path, vendor_trigger_yamls) -> Path:
    """A populated ``triggers/`` directory — what a pack shipping them looks like."""
    d = tmp_path / "triggers"
    d.mkdir()
    for name, body in vendor_trigger_yamls.items():
        (d / name).write_text(body, encoding="utf-8")
    return d


@pytest.fixture
def vendor_trigger_specs(vendor_triggers_dir: Path) -> dict:
    """``source → TriggerSpec`` parsed from :func:`vendor_triggers_dir`."""
    from cairn.core.engine.triggers_spec import load_trigger_specs

    return {s.source: s for s in load_trigger_specs(vendor_triggers_dir)}


# Object-store knobs that turn a unit test into a network upload. Both spellings:
# ``getenv`` falls through to ``OBS_*`` when ``CAIRN_*`` is empty (ADR-0122).
# Set, not deleted — ``load_dotenv(override=False)`` and a pydantic-settings env
# file refill a missing key from ``.env``, and will not replace one that is set.
_LOCAL_STORE_KEYS = ("CAIRN_ARTIFACT_STORE",)
_BLANK_CLOUD_KEYS = (
    "OBS_ARTIFACT_STORE",
    "CAIRN_ARTIFACT_S3_BUCKET",
    "CAIRN_ARTIFACT_S3_PREFIX",
    "CAIRN_ARTIFACT_S3_REGION",
    "CAIRN_ARTIFACT_S3_ENDPOINT",
    "OBS_ARTIFACT_S3_BUCKET",
    "OBS_ARTIFACT_S3_PREFIX",
    "OBS_ARTIFACT_S3_REGION",
    "OBS_ARTIFACT_S3_ENDPOINT",
    "CAIRN_CHECKPOINT_BUCKET",
    "CAIRN_CHECKPOINT_PREFIX",
    "OBS_CHECKPOINT_BUCKET",
    "OBS_CHECKPOINT_PREFIX",
    "CAIRN_PLAYGROUND_S3_BUCKET",
    "OBS_PLAYGROUND_S3_BUCKET",
    "PLAYGROUND_ARTIFACT_S3_BUCKET",
    "CAIRN_METAFLOW_DATASTORE",
    "CAIRN_METAFLOW_DATASTORE_ROOT",
    "OBS_METAFLOW_DATASTORE",
    "OBS_METAFLOW_DATASTORE_ROOT",
)


@pytest.fixture
def allow_artifact_bucket():
    """Opt out of :func:`_no_real_artifact_bucket`.

    The test then sees ambient artifact and cloud settings (shell and ``.env``)
    and may build a real S3 client. Stub the upload itself — this fixture is
    not permission to reach a network bucket.
    """
    return None


@pytest.fixture(autouse=True)
def _no_real_artifact_bucket(monkeypatch, request):
    """``tests/core`` never touches a network bucket (#64).

    Stage capture uploads whenever ``CAIRN_ARTIFACT_S3_BUCKET`` is set, and
    artifact registration relays local bytes when ``CAIRN_ARTIFACT_STORE`` is
    ``s3`` or ``s3://…``. A developer shell, or a ``.env`` copied into the
    process by ``load_dotenv`` / pydantic-settings, must not turn
    ``run_dataflow`` into that upload. The store is pinned to ``local`` and
    every bucket key is set-but-empty so a later ``load_dotenv(override=False)``
    cannot refill it. The S3 client seam raises if something still builds one.
    """
    if "allow_artifact_bucket" in request.fixturenames:
        yield
        return
    for key in _LOCAL_STORE_KEYS:
        monkeypatch.setenv(key, "local")
    for key in _BLANK_CLOUD_KEYS:
        monkeypatch.setenv(key, "")

    def _refused(*_args, **_kwargs):
        raise AssertionError(
            f"{request.node.nodeid} built an S3 client. tests/core pins artifact "
            "settings to a local store; request allow_artifact_bucket and stub "
            "the upload to opt out (#64)")

    import cairn.core.dataflow.capture as capture

    monkeypatch.setattr(capture, "s3_client", _refused)
    yield
