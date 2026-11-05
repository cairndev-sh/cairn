"""Which environments a person is offered (#1570, ADR-0093 P1).

The Run tab's picker listed SEVEN environments. Three of them
(`local-dev`, `secure`, `production`) are BASE LAYERS of an inheritance chain
you extend rather than pick; two (`campaign`, `e2e`) are private machinery. And
the Playground banner read "Runs in **campaign**" — the 1M-row training
campaign's private environment — to every new user, because the view looks for
a profile matching /playground|sandbox/i and, finding none, fell back to
`profiles[0]`, which sorted alphabetically to `campaign`.

Since ADR-0134 D5 the engine ships only the profiles a public install can satisfy
(`local-dev`, `sandbox`); `prod` and the rest are the
platform's, and the platform's own tests pin what its merged directory offers.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.pack.curation import is_offerable
from cairn.core.pack.profile import ProfileError, load_profile
from cairn.core.ui_contract import list_profiles

PROFILE_DIR = Path("src/cairn/profiles")


def _by_name():
    return {p["name"]: p for p in list_profiles()}


def test_only_sandbox_is_offered():
    """A public install offers one environment to pick: the sandbox. A deployment adds its own
    (the platform's `prod`, ADR-0134 D5); the base layers and variants stay internal."""
    offered = sorted(p["name"] for p in list_profiles() if is_offerable(p["visibility"]))
    assert offered == ["sandbox"]


def test_the_sandbox_profile_exists_because_the_code_looks_for_it():
    """PlaygroundView searches /playground|sandbox/i. Shipping the profile it
    asks for fixes 'Runs in campaign' at the root instead of relabelling it."""
    assert (PROFILE_DIR / "sandbox.toml").is_file()
    assert _by_name()["sandbox"]["visibility"] == "public"


def test_sandbox_sorts_before_the_private_environments_it_replaced():
    """Belt and braces: even if the name search were removed, the alphabetical
    fallback must not land on `campaign` again."""
    offered = [p["name"] for p in list_profiles() if is_offerable(p["visibility"])]
    assert "campaign" not in offered


@pytest.mark.parametrize("name,why", [
    ("local-dev", "a base layer of the inheritance chain"),
])
def test_the_internal_environments_are_not_offered(name, why):
    p = _by_name()[name]
    assert p["visibility"] == "internal", f"{name} is {why}"
    assert not is_offerable(p["visibility"])


def test_the_api_still_lists_every_environment():
    """Annotated, not filtered: `--profile` and an operator debugging a
    deployment must still see all of them."""
    # every shipped profile, whatever the count — #1868 added prod-dbos.toml and
    # a hard-coded 8 went red on main.
    assert len(list_profiles()) == len(list(PROFILE_DIR.glob("*.toml")))


@pytest.mark.parametrize("name", [
    "sandbox", "local-dev",
])
def test_every_shipped_profile_still_loads(name):
    """`visibility` went into a CLOSED schema — every profile must still parse."""
    assert load_profile(PROFILE_DIR / f"{name}.toml") is not None


def test_a_mistyped_tier_fails_at_parse():
    """Typed, unlike template frontmatter: the closed schema catches it where
    it is cheap, instead of silently publishing a private environment."""
    d = Path(tempfile.mkdtemp())
    (d / "bad.toml").write_text('[profile]\nvisibilty = "internal"\n', encoding="utf-8")
    with pytest.raises(ProfileError, match="unknown key"):
        load_profile(d / "bad.toml")


def test_a_profile_with_no_tier_is_offered():
    """Curation stays opt-in — a new environment is never invisible because its
    author forgot a key."""
    d = Path(tempfile.mkdtemp())
    (d / "plain.toml").write_text("[profile]\n", encoding="utf-8")
    from cairn.core.ui_contract import _profile_visibility
    assert is_offerable(_profile_visibility(d / "plain.toml"))


def test_an_unparseable_profile_does_not_blank_the_selector():
    from cairn.core.ui_contract import _profile_visibility
    d = Path(tempfile.mkdtemp())
    (d / "broken.toml").write_text("this is not toml {{{", encoding="utf-8")
    assert _profile_visibility(d / "broken.toml") == "public"
