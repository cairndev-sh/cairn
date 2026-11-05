"""Backend SELECTION resolves through the profile plane (#1093, ADR-0083 D3/R3).

Problem P9/P11: `effective_backends` (profile → env → default, ADR-0052 §1) is
*the* resolver and already declares every selector key — but consumers read the
env vars directly, so a profile that selects a backend is silently ignored. Proven
before the fix: a profile with `dataflow_engine = "metaflow"` and no env var still
resolved to the builtin engine.

Scope note — this covers SELECTOR keys only. Credential/DSN env reads
(`OBS_STORAGE=postgres://…`, `QDRANT_URL`, `PGVECTOR_DSN`, …) are NOT a bypass:
they are the deliberate injection seam of ADR-0052 §3, where
`apply_backend_connection` resolves a bound connection INTO those env names for
the synchronous factories to read. Routing them through `effective_backends`
would lose the DSN (it returns the kind, not the URL).
"""
from __future__ import annotations

from cairn.core import backend_selection as bs


def _profile(backends: dict):
    """A stand-in for the ambient profile object (only `.backends` is read)."""
    return type("_P", (), {"backends": backends})()


def test_profile_wins_over_env(monkeypatch):
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {"dataflow_engine": "metaflow"})
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "builtin")

    assert bs.selected("dataflow_engine") == "metaflow"


def test_env_used_when_profile_is_silent(monkeypatch):
    """Back-compat: today's deployments configure by env and must keep working."""
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {})
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "metaflow")

    assert bs.selected("dataflow_engine") == "metaflow"


def test_schema_default_when_neither_is_set(monkeypatch):
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {})
    monkeypatch.delenv("OBS_DATAFLOW_ENGINE", raising=False)

    assert bs.selected("dataflow_engine") == "builtin"


def test_unknown_key_returns_the_fallback(monkeypatch):
    """A typo'd key must not raise on the dispatch path."""
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {})
    assert bs.selected("not_a_backend", default="x") == "x"


def test_broken_profile_degrades_to_env(monkeypatch):
    """A malformed profile must never break backend selection."""
    def _explode():
        raise RuntimeError("profile is garbage")
    monkeypatch.setattr(bs, "_ambient_backends", _explode)
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "metaflow")

    assert bs.selected("dataflow_engine") == "metaflow"


def test_toggle_keys_return_bools(monkeypatch):
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {"remote_execution": True})
    assert bs.selected_bool("remote_execution") is True

    monkeypatch.setattr(bs, "_ambient_backends", lambda: {})
    monkeypatch.setenv("OBS_REMOTE_JOB_DURABLE", "1")
    assert bs.selected_bool("remote_execution") is True
    monkeypatch.setenv("OBS_REMOTE_JOB_DURABLE", "0")
    assert bs.selected_bool("remote_execution") is False


def test_provenance_reports_which_plane_decided(monkeypatch):
    """Doctor/console need to show WHY a value is in effect (P9's folklore fix)."""
    monkeypatch.setattr(bs, "_ambient_backends", lambda: {"ml_engine": "metaflow"})
    monkeypatch.setenv("OBS_ML_ENGINE", "builtin")
    assert bs.provenance("ml_engine") == "profile"

    monkeypatch.setattr(bs, "_ambient_backends", lambda: {})
    assert bs.provenance("ml_engine") == "env"

    monkeypatch.delenv("OBS_ML_ENGINE", raising=False)
    assert bs.provenance("ml_engine") == "default"


# ── the consumer that proved the bug ─────────────────────────────────────────

def test_resolve_engine_honours_a_profile_selected_engine(monkeypatch):
    """THE regression: before #1093 a profile selecting an engine was ignored —
    `resolve_engine` read only os.environ and returned the builtin engine."""
    from operator_dataflow.engines import resolve_engine

    monkeypatch.setattr(bs, "_ambient_backends",
                        lambda: {"dataflow_engine": "not-installed-engine"})
    monkeypatch.delenv("OBS_DATAFLOW_ENGINE", raising=False)

    # an uninstalled selection must fail LOUDLY (#568) — proving the profile was read
    try:
        resolve_engine(None, kind="data")
    except Exception as e:
        assert "not-installed-engine" in str(e)
    else:
        raise AssertionError("profile-selected engine was ignored")


def test_resolve_engine_reads_profile_target(monkeypatch):
    """The target is a selector too: a profile-bound box reaches the engine, and the builtin
    one, which runs on this machine only, refuses it by name."""
    from cairn.core.dataflow.engine import EngineError
    from operator_dataflow.engines import resolve_engine

    monkeypatch.setattr(bs, "_ambient_backends",
                        lambda: {"dataflow_engine": "builtin", "dataflow_target": "databox"})
    monkeypatch.delenv("OBS_DATAFLOW_ENGINE", raising=False)
    monkeypatch.delenv("OBS_DATAFLOW_TARGET", raising=False)

    with pytest.raises(EngineError, match="databox"):
        resolve_engine(None, kind="data")
