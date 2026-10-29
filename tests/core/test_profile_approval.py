"""#256 (ADR-0034) — ``[profile.approval]`` is a real authority.

The resolver read ``profile_approval`` and ``enforce._profile_approval()`` read
``load_profile_from_env().approval``, but the closed profile schema refused the key, so
the coarse switch could never fire. These tests load a real ``profile.toml`` (no
monkeypatched ``_profile_approval``) and follow it to the gate.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from cairn.core.approval import enforce
from cairn.core.approval.resolver import resolve_gate
from cairn.core.pack.profile import ProfileError, load_profile


def _write(tmp_path: Path, body: str, name: str = "profile.toml") -> Path:
    p = tmp_path / name
    p.write_text("[profile]\n" + body, encoding="utf-8")
    return p


def _gate(risk: str, profile_approval):
    return resolve_gate(operator_name="compute/acquire", risk=risk, requires_approval=False,
                        capabilities=[], environment="prod", profile_approval=profile_approval)


def test_the_table_loads(tmp_path):
    p = _write(tmp_path, '[profile.approval]\nmode = "enforce"\ndefault_on_risk = ["paid_compute"]\n')
    assert load_profile(p).approval == {"mode": "enforce", "default_on_risk": ["paid_compute"]}


def test_no_table_is_no_switch(tmp_path):
    assert load_profile(_write(tmp_path, "")).approval == {}


def test_a_profile_from_the_environment_gates_by_risk(tmp_path, monkeypatch):
    p = _write(tmp_path, '[profile.approval]\nmode = "enforce"\ndefault_on_risk = ["paid_compute"]\n')
    monkeypatch.setenv("CAIRN_PROFILE", str(p))
    pa = enforce._profile_approval()
    assert pa == {"mode": "enforce", "default_on_risk": ["paid_compute"]}
    assert "profile" in _gate("paid_compute", pa).sources
    assert "profile" not in _gate("read_only", pa).sources


def test_mode_off_gates_nothing(tmp_path):
    pa = load_profile(_write(tmp_path, '[profile.approval]\nmode = "off"\ndefault_on_risk = ["paid_compute"]\n')).approval
    assert "profile" not in _gate("paid_compute", pa).sources


@pytest.mark.parametrize(("body", "message"), [
    ('[profile.approval]\nmode = "enforce"\nrisks = ["paid_compute"]\n', "unknown key(s) in [profile.approval]"),
    ('[profile.approval]\ndefault_on_risk = ["paid_compute"]\n', "profile.approval.mode must be one of"),
    ('[profile.approval]\nmode = "on"\n', "profile.approval.mode must be one of"),
    ('[profile.approval]\nmode = "enforce"\ndefault_on_risk = ["paid-compute"]\n', "unknown profile.approval.default_on_risk"),
    ('[profile.approval]\nmode = "enforce"\ndefault_on_risk = "paid_compute"\n', "must be a list of strings"),
])
def test_a_typo_fails_at_parse_instead_of_leaving_the_gate_off(tmp_path, body, message):
    with pytest.raises(ProfileError, match=message.replace("[", r"\[").replace("(", r"\(").replace(")", r"\)")):
        load_profile(_write(tmp_path, body))


def test_extends_replaces_per_key(tmp_path):
    _write(tmp_path, '[profile.approval]\nmode = "enforce"\ndefault_on_risk = ["paid_compute"]\n', "base.toml")
    child = _write(tmp_path, 'extends = "base.toml"\n[profile.approval]\nmode = "off"\n', "child.toml")
    assert load_profile(child).approval == {"mode": "off", "default_on_risk": ["paid_compute"]}
