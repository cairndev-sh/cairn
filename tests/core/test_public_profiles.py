"""ADR-0134 D5 — a profile ships in ``cairndev`` only when every binding it makes resolves
against the public set.

A public install has the builtin operator tier and no domain pack. A shipped profile that
binds a platform operator (``eval.delta = eval/delta``), configures one
(``[profile.config."model/gate"]``), names a pack as a capability's winner, or adopts a domain
pack's policies cannot be satisfied there: its templates refuse to compile, or ``cairn doctor``
fails on a fresh install. The builtins are read from their manifests, not from what happens
to be installed, so a development install with the platform's packs proves nothing extra.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from cairn.core.pack.profile import load_profile
from tests._layout import ENGINE, operator_packs

PROFILES = sorted((ENGINE / "profiles").glob("*.toml"))


def _builtins() -> tuple[set[str], set[str]]:
    """(operator names, pack names) the builtin tier's manifests declare."""
    ops: set[str] = set()
    packs: set[str] = set()
    for pack in operator_packs("builtins"):
        manifest = tomllib.loads((pack / "pack.toml").read_text(encoding="utf-8"))
        packs.add(manifest["pack"]["name"])
        ops.update(manifest.get("operator", {}))
    return ops, packs


def outside_the_public_set(path: Path) -> list[str]:
    """What ``path`` (with everything it extends) binds, configures or adopts outside the builtins."""
    ops, packs = _builtins()
    p = load_profile(path)
    bad = [f"binding {cap} = {op}" for cap, op in sorted(p.bindings.items()) if op not in ops]
    bad += [f"config for operator {op}" for op in sorted(p.config) if op not in ops]
    bad += [f"capability {cap} → pack {pk}" for cap, pk in sorted(p.capabilities.items()) if pk not in packs]
    bad += [f"pack policy {pol}" for pol in p.pack_policies or ()]  # a public install ships no domain pack
    return bad


def test_the_public_profile_set():
    """ADR-0134 D5: the platform's production chain, e2e and campaign profiles are the platform's."""
    assert {p.stem for p in PROFILES} == {"local-dev", "sandbox"}


def test_the_builtin_tier_is_read():
    ops, packs = _builtins()
    assert {"train/lora", "dataflow/run", "artifact/promote"} <= ops and packs


@pytest.mark.parametrize("path", PROFILES, ids=lambda p: p.stem)
def test_every_shipped_profile_resolves_against_the_public_builtins(path):
    assert outside_the_public_set(path) == [], path.name


def test_a_profile_binding_a_platform_operator_fails_the_guard(tmp_path):
    """The rejecting case: a profile layered on a public one, binding the platform's eval
    operator, configuring its model gate and adopting a domain pack's policy."""
    (tmp_path / "platform.toml").write_text(
        f'[profile]\nextends = "{(ENGINE / "profiles" / "secure.toml").as_posix()}"\n'
        'pack_policies = ["auto-scale-policy"]\n'
        '[profile.bindings]\n"eval.delta" = "eval/delta"\n'
        '[profile.config."model/gate".thresholds]\nallow_stub = false\n', encoding="utf-8")
    assert outside_the_public_set(tmp_path / "platform.toml") == [
        "binding eval.delta = eval/delta", "config for operator model/gate", "pack policy auto-scale-policy"]
