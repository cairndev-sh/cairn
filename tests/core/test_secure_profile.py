"""secure profile + profile doctor checks (ADR-0028 / ADR-0027 doctor half)."""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

SECURE = "src/cairn/profiles/secure.toml"
PROD_INFISICAL = "src/cairn/profiles/prod-infisical.toml"


# ── the profile itself ─────────────────────────────────────────────────

def test_secure_profile_loads_and_extends_local_dev():
    from cairn.core.pack.profile import load_profile
    p = load_profile(Path(SECURE))
    # New safety bindings present…
    assert p.bindings["redact.text"] == "redact/text"
    assert p.bindings["audit.sink"] == "audit/store"
    # …and the inherited local-dev substrate bindings survive the extend.
    assert p.bindings["storage.put"] == "storage/put"
    assert p.bindings["storage.get"] == "storage/get"




@pytest.mark.asyncio
async def test_secure_profile_activates_redaction_and_audit(monkeypatch):
    """OBS_PROFILE=secure → the redactor and audit sink are LIVE, not no-ops."""
    from cairn.core.obs import audit as A
    from cairn.core.obs import redact as R
    from cairn.core.operator import load_all_operators

    load_all_operators(strict=False)
    monkeypatch.setenv("OBS_PROFILE", SECURE)
    R.reset_redactor()
    A.reset_audit_sink()
    try:
        r = R.get_redactor()
        assert not isinstance(r, R.NoOpRedactor)
        res = await r.redact_text("mail bob@example.com")
        assert res.redacted and "[REDACTED:email]" in res.text

        sink = A.get_audit_sink()
        assert not isinstance(sink, A.NullAuditSink)
    finally:
        R.reset_redactor()
        A.reset_audit_sink()


# ── doctor: bindings resolve ───────────────────────────────────────────

def test_doctor_profile_ok_on_secure(monkeypatch):
    from cairn.cli.commands import doctor
    monkeypatch.setenv("OBS_PROFILE", SECURE)
    results = doctor.check_profile()
    by_name = {r.name: r for r in results}
    assert by_name["profile"].status == "ok"
    assert by_name["binding:redact.text"].status == "ok"
    assert by_name["binding:audit.sink"].status == "ok"
    assert all(r.category == "profile" for r in results)


def test_doctor_profile_fails_on_bogus_binding(tmp_path, monkeypatch):
    from cairn.cli.commands import doctor
    bad = tmp_path / "bad.toml"
    bad.write_text('[profile]\n[profile.bindings]\n"redact.text" = "does/not-exist"\n')
    monkeypatch.setenv("OBS_PROFILE", str(bad))
    results = doctor.check_profile()
    by_name = {r.name: r for r in results}
    assert by_name["binding:redact.text"].status == "fail"


def test_doctor_profile_unset_is_ok(monkeypatch):
    from cairn.cli.commands import doctor
    monkeypatch.delenv("OBS_PROFILE", raising=False)
    results = doctor.check_profile()
    assert len(results) == 1 and results[0].status == "ok"


def test_doctor_warns_when_governance_unbound_under_a_loaded_profile(tmp_path, monkeypatch):
    """#1396 — an unbound audit.sink/redact.text under a LOADED profile is
    legal (ADR-0028 null defaults) but must be LOUD: silence here made every
    audit assertion vacuously green."""
    from cairn.cli.commands import doctor
    bare = tmp_path / "bare.toml"
    bare.write_text("[profile]\n[profile.bindings]\n")
    monkeypatch.setenv("OBS_PROFILE", str(bare))
    results = doctor.check_profile()
    by_name = {r.name: r for r in results}
    assert by_name["binding:audit.sink"].status == "warn"
    assert "DROPPED" in by_name["binding:audit.sink"].detail
    assert by_name["binding:redact.text"].status == "warn"


def test_doctor_no_governance_warn_when_bound(monkeypatch):
    """The secure profile binds both — no warn rows appear."""
    from cairn.cli.commands import doctor
    monkeypatch.setenv("OBS_PROFILE", SECURE)
    warns = [r for r in doctor.check_profile() if r.status == "warn"]
    assert warns == []


# ── doctor: required services bound (ADR-0027) ────────────────────────

def _fake_pack(name, required_capabilities):
    manifest = SimpleNamespace(required_capabilities=required_capabilities)
    return SimpleNamespace(name=name, manifest=manifest)


def test_doctor_required_services_fail_when_unbound(monkeypatch):
    from cairn.cli.commands import doctor

    monkeypatch.setattr(
        "cairn.core.pack.registry.get_packs",
        lambda: [_fake_pack("sensitive-pack", ["redact.text"])],
    )
    monkeypatch.delenv("OBS_PROFILE", raising=False)  # nothing bound
    results = doctor.check_required_services()
    fails = [r for r in results if r.status == "fail"]
    assert fails and "redact.text" in fails[0].detail


def test_doctor_required_services_ok_when_bound(monkeypatch):
    from cairn.cli.commands import doctor

    monkeypatch.setattr(
        "cairn.core.pack.registry.get_packs",
        lambda: [_fake_pack("sensitive-pack", ["redact.text"])],
    )
    monkeypatch.setenv("OBS_PROFILE", SECURE)
    results = doctor.check_required_services()
    assert all(r.status == "ok" for r in results)
    assert any("redact/text" in r.detail for r in results)


# ── 2026-08-18: a SELECTED engine's client library must be importable ────────

def test_doctor_engine_client_fails_when_selected_engine_not_importable(monkeypatch):
    """The prod image shipped with no engine libs; doctor read green because it
    probed the metadata SERVICE, never the CLIENT. Selected + not importable
    must be a fail — every flow leg would die at submit."""
    from cairn.cli.commands import doctor
    monkeypatch.setenv("OBS_ML_ENGINE", "metaflow")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "builtin")
    monkeypatch.setattr(doctor, "_ENGINE_CLIENT",
                        {"metaflow": ("definitely_not_a_module_xyz", "cairndev[metaflow]")})
    rows = doctor.check_engine_clients()
    assert [r.name for r in rows] == ["engine_client:ml_engine"]
    assert rows[0].status == "fail"
    assert "cairndev[metaflow]" in rows[0].detail


def test_doctor_engine_client_silent_for_builtin(monkeypatch):
    from cairn.cli.commands import doctor
    monkeypatch.setenv("OBS_ML_ENGINE", "builtin")
    monkeypatch.setenv("OBS_DATAFLOW_ENGINE", "")
    assert doctor.check_engine_clients() == []


# ── #1443: the campaign environment is the platform's (ADR-0134 D5) ──────────
# tests/core/test_platform_profiles.py pins its engines and store.

def test_dataflow_engine_offers_metaflow():
    from cairn.core.profile_backends import BACKENDS
    f = next(b for b in BACKENDS if b.key == "dataflow_engine")
    assert "metaflow" in {o.value for o in f.options}
