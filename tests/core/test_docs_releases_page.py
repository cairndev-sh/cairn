"""The docs site's Releases page is generated from CHANGELOG.md, never hand-written (#353).

The page imports the changelog at build time, so the page cannot drift from it. The site's
Vercel project builds from ``docs/site`` and skips a build when nothing there changed, so
its ignore command must also watch ``CHANGELOG.md``: a release that only edits the
changelog still redeploys the page.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "docs" / "site"


def test_releases_page_is_built_from_the_changelog():
    page = (SITE / "src" / "pages" / "releases.astro").read_text(encoding="utf-8")
    assert "CHANGELOG.md" in page
    assert (SITE / "src" / "pages" / ".." / ".." / ".." / ".." / "CHANGELOG.md").resolve() == ROOT / "CHANGELOG.md"
    assert (ROOT / "CHANGELOG.md").is_file()


def test_sidebar_links_the_releases_page():
    assert "link: '/releases/'" in (SITE / "astro.config.mjs").read_text(encoding="utf-8")


def test_a_changelog_only_commit_redeploys_the_docs():
    ignore = json.loads((SITE / "vercel.json").read_text(encoding="utf-8"))["ignoreCommand"]
    assert "../../CHANGELOG.md" in ignore
