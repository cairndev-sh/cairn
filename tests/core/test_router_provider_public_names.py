"""The public names a ``cairn.routers`` provider may import (#297,
``docs/contracts/operators-and-packs.md`` §Public Python names): each exists, the earlier
private spelling is the same object, and the contract doc lists every one."""
from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: (module, public name, earlier private name)
PROMOTED = [
    ("cairn.core.triggers.auth", "oauth2_scheme", "_oauth2_scheme"),
    ("cairn.core.triggers.server", "langfuse_trace_url", "_langfuse_trace_url"),
    ("cairn.core.triggers.server", "run_failure_types", "_run_failure_types"),
    ("cairn.core.triggers.router", "user_trigger_overlay_dir", "_user_trigger_overlay_dir"),
    ("cairn.core.engine.triggers_presets", "TRIGGER_PRESETS", "_TRIGGER_PRESETS"),
    ("cairn.core.engine.triggers_presets", "SIGNATURE_SCHEMES", "_SIGNATURE_SCHEME_OPTS"),
]


@pytest.mark.parametrize("module,public,private", PROMOTED)
def test_the_private_spelling_is_the_public_object(module, public, private):
    mod = importlib.import_module(module)
    assert getattr(mod, private) is getattr(mod, public)


@pytest.mark.parametrize("name", [p[1] for p in PROMOTED] + ["env_admin_username"])
def test_the_contract_doc_lists_each_public_name(name):
    doc = (REPO / "docs/contracts/operators-and-packs.md").read_text()
    assert re.search(rf"\b{name}\b", doc)


def test_env_admin_username_names_the_admin_and_never_the_hash(monkeypatch):
    from cairn.core.triggers.auth import env_admin_username

    monkeypatch.delenv("CAIRN_API_PASSWORD_HASH", raising=False)
    monkeypatch.delenv("CAIRN_API_PASSWORD", raising=False)
    monkeypatch.setenv("CAIRN_API_USER", "ops")
    assert env_admin_username() is None  # no password: no configured admin

    monkeypatch.setenv("CAIRN_API_PASSWORD", "s3cret")
    assert env_admin_username() == "ops"


def test_user_trigger_overlay_dir_follows_the_env(monkeypatch, tmp_path):
    from cairn.core.triggers.router import user_trigger_overlay_dir

    monkeypatch.setenv("CAIRN_TRIGGERS_DIR", str(tmp_path))
    assert user_trigger_overlay_dir() == tmp_path
