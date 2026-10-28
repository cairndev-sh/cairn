"""Every operator pack carries a design record, and it stays in step with the code.

Owner ruling 2026-09-03: an operator pack is a code module, and a code module
without an architecture + configuration record is not finished. Before this
gate all 35 shipped packs had only a README — a dozen of them under 17 lines,
one of them a single line, and only 11 listing the operators they provide.

The gate has three parts, each enforcing a different half of the rule:

1. **No new pack without a record.** Every pack under ``cairn/operators`` must
   have ``DESIGN.md``. The packs that predate the rule are listed in
   :data:`BACKFILL` with their issue; that list may only SHRINK — a pack added
   to it fails :func:`test_backfill_list_only_shrinks`, so the debt cannot grow
   while it is being paid down (#1997).
2. **A record is a record.** A ``DESIGN.md`` that exists must carry every
   section of ``docs/templates/operator-pack-DESIGN.md``, and must not still
   contain the template's placeholders.
3. **The record follows the code.** Every operator the pack's manifest declares
   must appear in its ``DESIGN.md``. Adding an operator without documenting it
   fails here, in the same PR — which is the whole point (CLAUDE.md rule 8).

Deliberately NOT checked: prose quality, section length, or whether the
architecture described is the architecture implemented. A gate cannot read; a
reviewer can. This gate only makes the omission impossible to miss.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from tests import _layout

TEMPLATE = _layout.REPO / "docs/templates/operator-pack-DESIGN.md"

#: Required top-level sections, matched on the heading text (numbering ignored).
REQUIRED_SECTIONS = (
    "Purpose",
    "Architecture",
    "Operators",
    "Configuration",
    "Governance",
    "Failure modes",
    "Testing",
)

#: Placeholders that mean the template was copied but not filled in.
PLACEHOLDERS = ("<pack-name>", "<pip name>", "<capability.one>", "ADR-xxxx", "<pack>/<op>")

#: Packs that predate the rule (#1997 backfills them). MAY ONLY SHRINK.
#: A pack is removed from this list in the PR that writes its DESIGN.md.
BACKFILL = frozenset({
    "artifact_local", "blast_radius", "clickstack", "compress",
    "compute", "data_expectations", "data_profile",
    "eval", "git", "lineage", "llm", "logs_analytics",
    "mcp", "memory_local", "monitor_threshold", "notify_slack",
    "postmortem", "rag_local", "rag_rerank", "redact_regex",
    "redteam", "report", "s3", "secrets_env", "secrets_infisical",
    "serving",
})


def _packs() -> list[Path]:
    packs = _layout.operator_packs()
    assert packs, "no operator packs found"
    return packs


def _declared_operators(pack_dir: Path) -> list[str]:
    """Operator names from the pack manifest's ``[operator."<name>"]`` tables."""
    with (pack_dir / "pack.toml").open("rb") as fh:
        manifest = tomllib.load(fh)
    return sorted(manifest.get("operator", {}))


def _declared_report_names(pack_dir: Path) -> dict[str, list[str]]:
    """ADR-0103: every series and scalar name each operator's
    ``[operator."<name>".reports]`` table declares — the record must name them."""
    with (pack_dir / "pack.toml").open("rb") as fh:
        manifest = tomllib.load(fh)
    out: dict[str, list[str]] = {}
    for op, tbl in manifest.get("operator", {}).items():
        rep = tbl.get("reports") or {}
        names = [s["name"] if isinstance(s, dict) else str(s) for s in rep.get("series", [])]
        names += list(rep.get("scalars", []))
        if names:
            out[op] = names
    return out


def _headings(text: str) -> list[str]:
    out = []
    for line in text.splitlines():
        if line.startswith("#"):
            title = line.lstrip("#").strip()
            # drop a leading section number ("2. Architecture" -> "Architecture")
            head, _, rest = title.partition(". ")
            out.append(rest.strip() if head.rstrip(".").isdigit() and rest else title)
    return out


def test_the_template_itself_declares_every_required_section():
    """The template is the contract; if a section is renamed there, this fails
    before every pack does."""
    assert TEMPLATE.exists(), f"{TEMPLATE} is missing — the rule has no contract"
    headings = _headings(TEMPLATE.read_text())
    missing = [s for s in REQUIRED_SECTIONS if s not in headings]
    assert not missing, f"template lost required sections: {missing}"


def test_every_operator_pack_has_a_design_record():
    """Part 1. A pack with no record must be named in BACKFILL, nowhere else."""
    undocumented = sorted(
        n for p in _packs() if not (p / "DESIGN.md").exists() and (n := _layout.pack_name(p)) not in BACKFILL
    )
    assert not undocumented, (
        "operator pack(s) without DESIGN.md: "
        f"{undocumented}. Copy {TEMPLATE} into the pack and fill it in "
        "(CLAUDE.md rule 8). A new pack is never added to BACKFILL."
    )


def test_backfill_list_only_shrinks():
    """Part 1b. Every name in BACKFILL must still be a real pack that still
    lacks a record — so a documented pack cannot be left on the list, and the
    list cannot be padded with names to silence part 1."""
    live = {_layout.pack_name(p) for p in _packs()}
    stale = sorted(n for n in BACKFILL if n not in live)
    # the official packs are the platform's: a name of one is provably stale only beside them
    assert not stale or not _layout.PRIVATE_TREES, f"BACKFILL names packs that no longer exist: {stale}"
    already_done = sorted(_layout.pack_name(p) for p in _packs()
                          if _layout.pack_name(p) in BACKFILL and (p / "DESIGN.md").exists())
    assert not already_done, (
        f"these packs now have a DESIGN.md and must be removed from BACKFILL: {already_done}"
    )


@pytest.mark.parametrize("pack_dir", _packs(), ids=_layout.pack_name)
def test_a_design_record_is_complete_and_matches_the_manifest(pack_dir: Path):
    """Parts 2 and 3, for every pack that has a record."""
    doc = pack_dir / "DESIGN.md"
    if not doc.exists():
        pytest.skip(f"{pack_dir.name}: backfill pending (#1997)")
    text = doc.read_text()

    headings = _headings(text)
    missing = [s for s in REQUIRED_SECTIONS if s not in headings]
    assert not missing, f"{pack_dir.name}/DESIGN.md is missing section(s): {missing}"

    left = [ph for ph in PLACEHOLDERS if ph in text]
    assert not left, f"{pack_dir.name}/DESIGN.md still contains template placeholders: {left}"

    declared = _declared_operators(pack_dir)
    undocumented = [op for op in declared if op not in text]
    assert not undocumented, (
        f"{pack_dir.name}/DESIGN.md does not mention operator(s) {undocumented} that "
        "pack.toml declares. Document them in the same PR that added them "
        "(CLAUDE.md rule 8)."
    )

    # Part 4 (ADR-0103). A declared series or scalar the record never names is
    # a metric the console will chart with no explanation of what it means.
    unexplained = {
        op: [n for n in names if n not in text]
        for op, names in _declared_report_names(pack_dir).items()
    }
    unexplained = {op: n for op, n in unexplained.items() if n}
    assert not unexplained, (
        f"{pack_dir.name}/DESIGN.md does not name declared report series/scalars "
        f"{unexplained}. Add them to §3 Operators (Reports column) in the same PR."
    )
