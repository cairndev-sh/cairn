"""The chart's example values pin the engine image at the version this repository releases, in the
form the release workflow pushes (`docker/metadata-action` semver: `0.1.2`, no `v`), so a user who
copies an example pulls a tag that exists."""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
EXAMPLES = sorted((ROOT / "deploy/helm/cairn").glob("values-*.example.yaml"))


def test_there_are_example_values_files():
    assert EXAMPLES


def test_the_release_workflow_pushes_semver_tags_without_a_v():
    release = (ROOT / ".github/workflows/release.yml").read_text()
    assert "type=semver,pattern={{version}}" in release


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_each_example_pins_the_released_engine_tag(path):
    tag = str((yaml.safe_load(path.read_text()).get("image") or {}).get("tag", ""))
    assert tag == VERSION, f"{path.name}: image.tag {tag!r}, but the release pushes {VERSION!r}"
    assert not re.match(r"^v\d", tag)


def test_the_released_chart_defaults_to_an_image_tag_that_exists():
    """The chart's image tag defaults to .Chart.AppVersion, so the release must package the chart with
    the bare version (0.1.2), the tag docker/metadata-action pushes, never the git tag (v0.1.2)."""
    release = (ROOT / ".github/workflows/release.yml").read_text()
    helpers = (ROOT / "deploy/helm/cairn/templates/_helpers.tpl").read_text()
    assert ".Chart.AppVersion" in helpers
    assert '--app-version "$VERSION"' in release and '--app-version "$GITHUB_REF_NAME"' not in release
