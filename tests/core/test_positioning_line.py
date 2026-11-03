"""One sentence says what cairn is, everywhere this repository says it (#319)."""
from __future__ import annotations

import re
import tomllib

from typer.testing import CliRunner

from cairn.cli.main import app
from tests import _layout

LINE = "Cairn runs your AI and ML pipelines behind budgets, policy, approvals and an audit trail."
REPO = _layout.REPO


def _text(path: str) -> str:
    return (REPO / path).read_text(encoding="utf-8")


def test_the_readme_leads_with_it():
    assert f"<b>{LINE}</b>" in _text("README.md")


def test_cairn_help_leads_with_it():
    out = CliRunner().invoke(app, ["--help"], env={"COLUMNS": "200", "NO_COLOR": "1"}).output
    assert LINE in re.sub(r"\s+", " ", out)


def test_the_package_description_on_pypi_is_it():
    assert tomllib.loads(_text("pyproject.toml"))["project"]["description"] == LINE


def test_the_docs_index_and_site_description_are_it():
    index = _text("docs/site/src/content/docs/index.md")
    assert f"description: {LINE}\n" in index and f"\n{LINE}\n" in index
    assert f"'{LINE}'" in _text("docs/site/astro.config.mjs")
