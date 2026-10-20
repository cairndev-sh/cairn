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
    assert "nightly-job.md  [template]" in out and "assistant.md  [template]" in out
    failed = [line for line in out.splitlines() if line.startswith("✗")]
    assert [f.split()[1].rsplit("/", 1)[-1] for f in failed] in ([], ["lora.yaml"]), failed


def test_the_example_pack_validates_clean(capsys, example_profile) -> None:
    assert validate.main([str(EXAMPLE_PACK)]) == 0
    out = capsys.readouterr().out
    assert "0 failed" in out


def test_under_the_default_profile_the_failure_names_the_packs_profile(capsys, monkeypatch) -> None:
    """0.1.2 verification bug 7: `cairn validate <hello pack>` under the default dev
    profile reported nightly-job's unbound `secrets.store` and nothing else. The
    failure stands (local-dev binds no secret store); the output now says which
    profile was active and which profile the pack ships for it."""
    from cairn.profiles import local_dev_path

    monkeypatch.setenv("CAIRN_PROFILE", str(local_dev_path()))  # what an unset profile becomes
    assert validate.main([str(EXAMPLE_PACK)]) == 1
    out = capsys.readouterr().out
    assert "nightly-job.md" in out and "secrets.store" in out
    assert "hint: a capability has no binding in the active profile" in out
    assert "local-dev.toml" in out
    assert str(PROFILE) in out
