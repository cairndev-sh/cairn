"""`cairn validate` on the example pack a fresh install ships (#2536; hello-cairn WP6).

Its data flow was validated as a policy, so the first `cairn validate` a new user ran
reported "1 failed" on a pack with nothing wrong in it.

The example validates under its own profile (`profiles/hello.toml`, the README's first
step): nightly-ops' secret check is a `capability: contract://secrets.store/1.0.0` node,
and a capability with no `[profile.bindings]` entry refuses to compile. `flows/lora.yaml`
names the `train.lora` stage of the builtin train-local pack (hello-cairn WP3), which the
public core installs, so the pack validates fully clean."""
from __future__ import annotations

import pytest

from cairn.cli.commands import validate
from tests.cli.conftest import EXAMPLE_PACK

PROFILE = EXAMPLE_PACK / "profiles" / "hello.toml"


@pytest.fixture
def example_profile(monkeypatch):
    monkeypatch.setenv("CAIRN_PROFILE", str(PROFILE))


def test_the_example_pack_validates_clean_but_for_the_training_flow(capsys, example_profile) -> None:
    validate.main([str(EXAMPLE_PACK)])
    out = capsys.readouterr().out
    assert "engine-check.yaml  [flow]" in out
    failed = [line for line in out.splitlines() if line.startswith("✗")]
    assert [f.split()[1].rsplit("/", 1)[-1] for f in failed] in ([], ["lora.yaml"]), failed


def test_the_example_pack_validates_clean(capsys, example_profile) -> None:
    assert validate.main([str(EXAMPLE_PACK)]) == 0
    out = capsys.readouterr().out
    assert "0 failed" in out


