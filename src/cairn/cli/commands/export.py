"""``cairn export <workflow>`` — render an installed workflow as one runnable file.

The forward half of the #1092 conversion slice (ADR-0083 D8 phase 2). It reads
what the platform already knows about a workflow — the UIContract's input schema,
the compiled graph's nodes and gates, the pack's demo tiers — and writes a single
``.py`` a stranger can read, edit, and run with ``cairn run <file>``.

**A projection, not a new source of truth.** The emitted file *references* the
installed workflow; the pack keeps owning the graph, the risk classes, and the
gates. The header says so, because a reader who edits the node list expecting it
to change behavior would otherwise be misled: the nodes are rendered as a comment
for orientation, and editing the pipeline means editing the pack.

Inputs come from the workflow's own JSON-Schema (with the pack's declared demo
tier preferred as the default values, since those are known-good) — so the
rendered file runs as-is wherever the pack ships a demo.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import typer

_PY_KEYWORDS = {"class", "def", "return", "import", "from", "None", "True", "False"}


def _class_name(ref: str) -> str:
    """``training/finetune`` → ``TrainingFinetune`` (a valid, readable identifier)."""
    parts = [p for chunk in ref.split("/") for p in chunk.replace("-", "_").split("_") if p]
    name = "".join(p[:1].upper() + p[1:] for p in parts) or "Exported"
    return name if not name[0].isdigit() else f"W{name}"


def _py_type(spec: dict[str, Any]) -> str:
    """JSON-Schema type → a readable Python annotation."""
    t = spec.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), None)
    return {
        "string": "str", "integer": "int", "number": "float",
        "boolean": "bool", "array": "list", "object": "dict",
    }.get(str(t), "Any")


def _safe_name(name: str) -> Optional[str]:
    """Drop anything that isn't a plain identifier — the file must import."""
    if not name.isidentifier() or name in _PY_KEYWORDS:
        return None
    return name


def _portable(value: Any) -> Any:
    """Shorten an absolute path that points INSIDE the cairn install.

    A demo tier resolves its sample data to an absolute path on the exporting
    machine; pasted into a colleague's file that path does not exist. Render it
    relative to the install root so it reads as what it is — pack sample data —
    and still resolves for the person who exported it.
    """
    if not isinstance(value, str) or not value.startswith("/"):
        return value
    try:
        import cairn as _c
        root = Path(_c.__file__).resolve().parent.parent
        rel = Path(value).resolve().relative_to(root)
        return str(rel)
    except Exception:  # noqa: BLE001 — not under the install, or unresolvable
        return value


def _render_inputs(schema: dict[str, Any], demo: dict[str, Any]) -> list[str]:
    """`class Inputs` body: annotation + default per property.

    Demo values win as defaults (the pack vouches for them), then the schema's
    own default. Two deliberate choices:

    * a REQUIRED property with no default gets an annotation only — the reader
      sees they must supply it, and nothing bogus is sent;
    * an OPTIONAL property with no default is emitted **commented out**, because
      an uncommented ``= None`` would be *sent* as null and fail schema
      validation on a workflow that simply omits the key.
    """
    props = (schema or {}).get("properties") or {}
    required = set((schema or {}).get("required") or [])
    if not props:
        return ["        pass  # this workflow declares no inputs"]

    def _rank(item):
        k, spec = item
        tier = 0 if (k in required and k not in demo and "default" not in (spec or {})) else (
            1 if (k in demo or "default" in (spec or {})) else 2)
        return (tier, k)

    lines: list[str] = []
    for key, spec in sorted(props.items(), key=_rank):
        name = _safe_name(key)
        if name is None:
            lines.append(f"        # {key!r}: skipped — not a valid Python identifier")
            continue
        spec = spec or {}
        ann = _py_type(spec)
        desc = str(spec.get("description") or "").strip().replace("\n", " ")
        comment = f"  # {desc[:78]}" if desc else ""
        if key in demo:
            lines.append(f"        {name}: {ann} = {_portable(demo[key])!r}{comment}")
        elif "default" in spec:
            lines.append(f"        {name}: {ann} = {spec['default']!r}{comment}")
        elif key in required:
            lines.append(f"        {name}: {ann}"
                         f"{comment or '  # REQUIRED'} — set it here or pass --set {name}=…")
        else:
            # optional + no default: keep it discoverable, don't send null.
            lines.append(f"        # {name}: {ann} = ...{comment or '  # optional'}")
    return lines


def _wrap(text: str, width: int = 84, indent: str = "") -> list[str]:
    """Wrap prose for a docstring — one long line is unreadable in an editor."""
    import textwrap

    out: list[str] = []
    for para in [p.strip() for p in text.split("\n\n") if p.strip()]:
        out.extend(textwrap.wrap(para, width=width,
                                 initial_indent=indent, subsequent_indent=indent) or [])
        out.append("")
    return out[:-1] if out and out[-1] == "" else out


def _steps(contract: Any, graph: Any) -> list[tuple[str, bool]]:
    """The user-meaningful steps as ``(label, is_gated)``.

    Compiler internals are filtered out: the graph carries ``__start__``,
    parallel dispatch/join pairs, guard entry/exit pairs and python hoists, all of
    which have no ``ref`` — real steps (operations, the LLM node) always do.
    Showing them would make the file look like the pipeline is 16 steps of
    machinery instead of the 9 things an operator cares about.
    """
    gated = {
        str(getattr(s, "operator", "") or "")
        for s in (getattr(contract, "steps", None) or [])
        if getattr(s, "requires_approval", False)
    }
    out: list[tuple[str, bool]] = []
    for n in (getattr(graph, "nodes", None) or []):
        d = n.__dict__ if hasattr(n, "__dict__") else n
        ref = d.get("ref")
        if not ref:
            continue
        out.append((str(ref), str(d.get("operator") or "") in gated))
    return out


#: `build_ui_contract` reports "no profile resolved" as this sentinel string, not
#: as "". Emitting it verbatim would produce `env = "(none)"` — a value no run
#: can use.
_NO_PROFILE = {"", "(none)", "none", "default"}


def _render(contract: Any, graph: Any, demo_tier: str, demo: dict[str, Any]) -> str:
    ref = contract.template_ref
    cls = _class_name(ref)
    desc = (contract.description or contract.title or "").strip()
    profile = str(contract.execution_profile or "").strip()
    env = profile if profile.lower() not in _NO_PROFILE else "dev"
    steps = _steps(contract, graph)
    file_hint = f"{ref.replace('/', '_').replace('-', '_')}.py"

    head = [
        '"""' + (f"{contract.title or ref} — exported pipeline" if desc else ref),
        "",
    ]
    if desc:
        head += _wrap(desc) + [""]
    head += [
        f"Exported from the installed workflow `{ref}` by `cairn pack export` (#1092).",
        "",
        "This file REFERENCES that workflow: the pack still owns the graph, the",
        "operator risk classes, and the gates, so what runs here is governed the",
        "same way. Edit inputs / env / series below — to change the STEPS, edit",
        "the pack.",
        "",
        f"    cairn run {file_hint} --env {env}",
        '"""',
        "from typing import Any  # noqa: F401 — used by the generated annotations",
        "",
        "from cairn.pipeline_file import Pipeline",
        "",
        "",
        f"class {cls}(Pipeline):",
    ]

    # The class docstring stays SHORT — the module docstring already carries the
    # full description, and repeating 800 characters twice is noise.
    body = [f'    """{ref}"""', ""]
    body += [
        f'    workflow = "{ref}"',
        f'    env      = "{env}"',
        '    series   = ""  # set to group runs under one budget + comparison',
        "",
    ]
    if steps:
        gated_n = sum(1 for _, g in steps if g)
        suffix = f" ({gated_n} human approval{'s' if gated_n != 1 else ''})" if gated_n else ""
        body.append(f"    # What the pack runs — {len(steps)} steps{suffix}. Read-only here:")
        for label, is_gated in steps:
            body.append(f"    #   • {label}" + ("   ⏸ HUMAN APPROVAL" if is_gated else ""))
        body.append("")
    if demo_tier:
        body.append(f"    # Defaults below are the pack's '{demo_tier}' demo tier — known-good,")
        body.append("    # so this file runs as-is.")
    body.append("    class Inputs:")
    body += _render_inputs(contract.inputs or {}, demo)
    return "\n".join(head + body) + "\n"


def cmd(
    workflow: str = typer.Argument(
        ..., help="Installed workflow to export, e.g. 'training/finetune'.",
    ),
    out: Optional[str] = typer.Option(
        None, "--out", "-o",
        help="Write to this path instead of stdout.",
    ),
    demo: Optional[str] = typer.Option(
        None, "--demo",
        help="Demo tier whose inputs become the defaults (default: the first "
             "tier the pack ships; 'none' to use schema defaults only).",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Execution profile the contract resolves under.",
    ),
) -> None:
    """Render an installed workflow as a single runnable pipeline file."""
    # lazy: pack/operator discovery + the contract builder are the expensive
    # imports; `cairn export --help` must stay cheap.
    from cairn.core.ui_contract import build_ui_contract
    from cairn.core.graph_view import build_graph_view
    from cairn.core.operator import default_registry, load_all_operators
    from cairn.core.pack.loader import load_all_packs

    default_registry.clear()
    load_all_operators(strict=False)
    load_all_packs()

    try:
        contract = build_ui_contract(workflow, profile=profile)
    except Exception as e:  # noqa: BLE001 — an unknown ref must be a clear message
        typer.secho(f"cannot export {workflow!r}: {type(e).__name__}: {e}",
                    fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from e

    tier, values = "", {}
    if (demo or "").strip().lower() != "none":
        for d in contract.demos or []:
            name = str(getattr(d, "tier", "") or "")
            if demo and name != demo:
                continue
            tier, values = name, dict(getattr(d, "inputs", {}) or {})
            break
        if demo and not tier:
            typer.secho(f"warning: no demo tier {demo!r} on {workflow} — using "
                        f"schema defaults", fg=typer.colors.YELLOW, err=True)

    try:
        graph = build_graph_view(workflow)
    except Exception as e:  # noqa: BLE001 — the step list is orientation, not correctness
        typer.secho(f"note: could not render the step list ({type(e).__name__}) — "
                    f"exporting without it", fg=typer.colors.YELLOW, err=True)
        graph = None

    text = _render(contract, graph, tier, values)

    if out:
        from pathlib import Path

        p = Path(out).expanduser()
        p.write_text(text, encoding="utf-8")
        typer.secho(f"wrote {p}", fg=typer.colors.GREEN, err=True)
        typer.secho(f"run it:  cairn run {p}", err=True)
    else:
        typer.echo(text)


__all__ = ["cmd"]
