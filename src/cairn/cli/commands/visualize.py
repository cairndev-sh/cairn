"""``cairn visualize template <name>`` — Mermaid of a compiled template.

§24.16 / ADR-0016: emit Mermaid text (stdout-pasteable into GitHub/GitLab/PRs)
via LangGraph's native ``draw_mermaid`` — no custom renderer. The locked verb
(§24.20.14) is ``visualize template``; ``pack`` / ``skill`` views are future
ADR-0016 scope and report not-yet-implemented rather than guessing.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer


def cmd(
    kind: str = typer.Argument(
        ..., help="What to visualize. Only 'template' (an agentic template) is supported.",
    ),
    name: str = typer.Argument(
        ..., help="Namespaced agentic template ref, e.g. 'incident/orchestrated-incident'.",
    ),
    output: Optional[str] = typer.Option(
        None, "--output", "-o",
        help="Write the Mermaid to this file instead of stdout.",
    ),
) -> None:
    """Render an agentic template's graph as Mermaid (paste into any markdown tool).

    Only `template` is supported: a pipeline's legs and gates are shown by `cairn pack show`,
    a data flow's stages by `cairn pack ls --flows`.
    """
    if kind != "template":
        print(
            f"only 'template' is supported today (got {kind!r}); "
            "pack/skill views are ADR-0016 future scope. A pipeline: "
            "`cairn pack show <ref>`; a data flow: `cairn pack ls --flows`.",
            file=sys.stderr,
        )
        raise typer.Exit(2)

    from cairn.core.engine.compiler import compile_template
    from cairn.core.engine.skill_engine import resolve_template_path
    from cairn.core.engine.visualize import template_mermaid
    from cairn.core.operator import default_registry as op_reg, load_all_operators
    from cairn.core.pack.loader import load_all_packs

    op_reg.clear()
    load_all_operators(strict=False)
    load_all_packs()

    try:
        compiled = compile_template(resolve_template_path(name))
    except Exception as e:  # noqa: BLE001
        print(f"✗ compile failed: {type(e).__name__}: {e}", file=sys.stderr)
        raise typer.Exit(1)

    mermaid = template_mermaid(compiled)
    if output:
        Path(output).write_text(mermaid)
        print(f"wrote {output}")
    else:
        print(mermaid)
