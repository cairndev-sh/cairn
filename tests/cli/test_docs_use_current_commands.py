"""No public page uses a command that ADR-0165 replaced (issue #361).

The replaced commands are read from D10 in the ADR, so there is one copy (the same read as
``test_cli_surface.py``). A stale use is one of them inside inline code or a fenced block of a
markdown page. Lines between ``<!-- old-names -->`` and ``<!-- /old-names -->`` are exempt: they
are the migration note, which has to name the old commands.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
ADR = ROOT / "docs/decisions/ADR-0165-the-cli-is-five-commands.md"

OPEN_MARK = "<!-- old-names -->"
CLOSE_MARK = "<!-- /old-names -->"

# What is scanned. docs/decisions is a record of what was decided, so it is not.
PAGE_ROOTS = (
    "README.md",
    "CONTRIBUTING.md",
    "benchmark",
    "docs/site/src/content/docs",
    "docs/contracts",
    "docs/architecture",
    "docs/golden-paths",
    "docs/runbooks",
    "docs/testing",
    "examples",
    "deploy",
    "packages",
    "src/cairn/connectors",
)
#: Files that are all command text, comments included: every line is read, not only code spans.
RAW_SUFFIXES = {".yaml", ".yml", ".tpl", ".txt", ".toml"}
RAW_ROOTS = ("deploy", "examples", ".github/workflows")


# ── reading the ADR ───────────────────────────────────────────────────────────────────────────

def d10_rows() -> dict[str, str]:
    """The core half of D10: {`cairn <today>`: what its After column says}."""
    table = ADR.read_text().split("### Core", 1)[1].split("### Platform", 1)[0]
    return {
        today: after
        for today, after in re.findall(r"^\| `(cairn [^`]+)` \| (.+) \|$", table, re.MULTILINE)
    }


ROWS = {today: after for today, after in d10_rows().items() if today != "cairn --version"}
REPLACED = {today for today, after in ROWS.items() if not after.startswith("unchanged")}

# Longest match first, so `cairn pipeline run` reports as itself and `cairn pack ls` is never
# read as a shorter command. Every row takes part in the match; only replaced ones are reported.
# A `/` after the name makes it part of a path (`helm upgrade cairn deploy/helm/cairn`), not a command.
_COMMAND = re.compile(
    r"(?<![\w-])("
    + "|".join(re.escape(c).replace(r"\ ", " +") for c in sorted(ROWS, key=len, reverse=True))
    + r")(?![\w/-])"
)


# ── the checker ───────────────────────────────────────────────────────────────────────────────

_INLINE = re.compile(r"(`+)(.+?)\1", re.DOTALL)


def _spans(paragraph: list[tuple[int, str]]):
    """Inline code in one paragraph: a span may wrap across lines, so it is read as one."""
    if paragraph:
        text = "\n".join(line for _, line in paragraph)
        for span in _INLINE.finditer(text):
            yield paragraph[0][0] + text.count("\n", 0, span.start()), " ".join(span.group(2).split())


def code_lines(text: str):
    """Yield (line number, code) for fenced blocks and inline spans.

    Lines inside an ``old-names`` block yield nothing.
    """
    fence = ""
    exempt = False
    paragraph: list[tuple[int, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if OPEN_MARK in line:
            exempt = True
        marker = re.match(r"\s*(`{3,}|~{3,})", line)
        # A table row stands alone: an empty cell's backticks must not pair across the rows.
        if exempt or marker or fence or not line.strip() or line.lstrip().startswith("|"):
            yield from _spans(paragraph)
            paragraph = []
        if exempt:
            if CLOSE_MARK in line:
                exempt = False
            continue
        if marker:
            if not fence:
                fence = marker.group(1)[0]
            elif marker.group(1)[0] == fence:
                fence = ""
            continue
        if fence:
            yield number, line
        elif line.lstrip().startswith("|"):
            yield from _spans([(number, line)])
        elif line.strip():
            paragraph.append((number, line))
    yield from _spans(paragraph)


def stale_uses(text: str) -> list[tuple[int, str]]:
    """[(line number, replaced command)] for every stale use on a page."""
    hits = []
    for number, code in code_lines(text):
        for match in _COMMAND.finditer(code):
            name = " ".join(match.group(1).split())
            if name in REPLACED:
                hits.append((number, name))
    return hits


def raw_files() -> list[Path]:
    return sorted(p for root in RAW_ROOTS for p in (ROOT / root).rglob("*")
                  if p.suffix in RAW_SUFFIXES and p.is_file())


def raw_stale_uses(text: str) -> list[tuple[int, str]]:
    return stale_uses("```\n" + text + "\n```\n")


def pages() -> list[Path]:
    found: list[Path] = []
    for entry in PAGE_ROOTS:
        path = ROOT / entry
        if path.is_file():
            found.append(path)
        else:
            found += [p for p in path.rglob("*") if p.suffix in {".md", ".mdx"} and p.is_file()]
    found += (ROOT / "docs").glob("*.md")
    return sorted(set(found))


# ── tests ─────────────────────────────────────────────────────────────────────────────────────

def test_no_page_uses_a_replaced_command():
    hits = [
        f"{page.relative_to(ROOT)}:{number}: `{name}` is replaced (ADR-0165 D10: {ROWS[name]})"
        for page in pages()
        for number, name in stale_uses(page.read_text())
    ]
    assert not hits, "pages use replaced commands:\n" + "\n".join(hits)


def test_no_config_or_workflow_file_uses_a_replaced_command():
    hits = [
        f"{path.relative_to(ROOT)}:{number - 1}: `{name}` is replaced (ADR-0165 D10: {ROWS[name]})"
        for path in raw_files()
        for number, name in raw_stale_uses(path.read_text())
    ]
    assert not hits, "files use replaced commands:\n" + "\n".join(hits)


def test_the_table_and_every_root_are_really_read():
    assert len(ROWS) > 70 and len(REPLACED) > 60
    for root in PAGE_ROOTS:
        path = ROOT / root
        assert path.is_file() or any(p.is_relative_to(path) for p in pages()), f"{root} holds no page"
    for root in RAW_ROOTS:
        assert any(p.is_relative_to(ROOT / root) for p in raw_files()), f"{root} holds no file"


def test_every_old_names_block_is_closed():
    for page in pages():
        text = page.read_text()
        assert text.count(OPEN_MARK) == text.count(CLOSE_MARK), f"{page.relative_to(ROOT)}: an unclosed block"


def test_a_span_that_wraps_across_lines_fails():
    assert stale_uses("See `cairn deploy\ndoctor --values` here.\n") == [(1, "cairn deploy doctor")]
    assert stale_uses("Not a `span\n\nacross a `cairn validate` paragraph.\n") == [(3, "cairn validate")]


def test_a_table_row_is_read_on_its_own():
    assert stale_uses("| a | `` |\n| b | `cairn validate` |\n") == [(2, "cairn validate")]


def test_the_page_list_covers_what_the_issue_names():
    names = {page.relative_to(ROOT).as_posix() for page in pages()}
    assert {"README.md", "CONTRIBUTING.md"} <= names
    assert any(n.startswith("benchmark/") for n in names)
    assert any(n.startswith("docs/site/src/content/docs/") for n in names)
    assert any(n.startswith("docs/") and n.count("/") == 1 for n in names)
    assert not any(n.startswith("docs/decisions/") for n in names)


def test_a_replaced_command_in_a_fence_fails():
    page = "Run it:\n\n```bash\ncairn pipeline ls\n```\n"
    assert stale_uses(page) == [(4, "cairn pipeline ls")]


def test_a_replaced_command_in_inline_code_fails():
    assert stale_uses("Check it with `cairn validate` first.\n") == [(1, "cairn validate")]


def test_a_path_prefix_still_counts():
    assert stale_uses("```\n.venv/bin/cairn pipeline ls\n```\n") == [(2, "cairn pipeline ls")]


def test_longest_match_names_the_whole_command():
    assert stale_uses("`cairn pipeline run --pipeline x`\n") == [(1, "cairn pipeline run")]


def test_prose_outside_code_does_not_count():
    assert stale_uses("The old cairn validate and cairn pipeline ls are gone.\n") == []


def test_a_marked_block_is_exempt_and_the_page_after_it_is_not():
    page = (
        f"{OPEN_MARK}\n| `cairn validate` | `cairn pack check` |\n{CLOSE_MARK}\n"
        "```\ncairn validate\n```\n"
    )
    assert stale_uses(page) == [(5, "cairn validate")]


def test_current_commands_and_other_programs_are_not_hits():
    page = "```\ncairn pack ls\ncairn run x\ncairn-server --port 1\ncairn-worker\n```\n`cairn pack check`\n"
    assert stale_uses(page) == []
