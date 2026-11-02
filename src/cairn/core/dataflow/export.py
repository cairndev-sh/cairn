"""``cairn flow export`` — one-way native projections of a flow record
(ADR-0087 P4, #1256).

The projection tier, outbound half: the SDK compiles author classes INTO
records, this compiles records OUT to a native Metaflow ``FlowSpec``. One IR,
multiple frontends and backends. What crosses is the STAGE LIST — a snapshot,
resolved and stamped; what stays home is everything that makes a cairn run
governed.

Three properties the generator is built around:

* **Per-stage steps.** The runtime bridge (``metaflow_bridge``) is
  deliberately dynamic: ONE deployment interpreting whatever spec arrives,
  never code-generated (ADR-0082 D1). That buys
  engine-neutrality and costs engine-native per-stage features — Metaflow's
  ``resume``/``@retry``/``@resources`` are structurally unreachable from a
  one-step bridge. An EXPORT is not the runtime path, so it can pay the other
  side of that trade: one ``@step`` per stage, resources mapped natively.
* **Stamped derivative.** Header carries the source ref and the def file's
  sha256. Generation is one-way — an export is never re-imported as source of
  truth, so the stamp is how a reader gets back to the record.
* **Governance does not travel.** Gates, budgets, admission, approvals and the
  audit chain are control-plane objects; a generated file has none of them. A
  flow carrying an in-flow gate therefore REFUSES to export (:func:`export_flow`
  raises) rather than shipping a picture with the gate quietly removed — the
  same "a dropped gate marker is a gate that does not gate" rule
  :mod:`cairn.core.dataflow.gate_split` enforces on the run path.

Generated files import only the standard library and the target framework —
never ``cairn.core`` and never a named operator pack (ADR-0120 D5, #2415): each
kernel is resolved by name from the stage entry-point groups, so whichever pack
provides it on the box is the one that runs. The export must run on a box that
has no control plane on it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from cairn.core.dataflow.contract import StageRef
from cairn.core.dataflow.defs import (
    FLOW_STAGE_ENTRY_POINT_GROUP,
    STAGE_ENTRY_POINT_GROUP,
    DataflowDefError,
    flow_def_sha,
    load_flow_def_sources,
    load_flow_defs,
    resolve_flow_ref,
)
from cairn.core.dataflow.gate_split import has_in_flow_gates

# ADR-0134 D4: only an engine cairn runs is an export target — Prefect was
# removed, so no command emits code for it.
EXPORT_TARGETS = ("metaflow",)

_GOVERNANCE_NOTE = (
    "GOVERNANCE DOES NOT TRAVEL. This file is the flow's STAGES, not the",
    "governed pipeline: no approval gates, no budgets, no admission checks, no",
    "audit chain, no lineage registration. Running it runs the work and asks",
    "no one. Keep the governed path (`cairn run`) for anything that needs a",
    "verdict.",
)


class FlowExportError(ValueError):
    """A flow that cannot be projected onto a native orchestrator."""


@dataclass(frozen=True)
class FlowExport:
    """One generated file, plus what it was cut from."""

    ref: str
    target: str
    kind: str
    filename: str
    source: str
    def_sha: str
    stages: list[StageRef]


# ─── naming (deterministic, collision-free) ───────────────────────────────


def _ident(text: str) -> str:
    """A python identifier fragment: ``ingest.normalize`` → ``ingest_normalize``."""
    out = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    return out or "stage"


def _camel(text: str) -> str:
    return "".join(part.capitalize() for part in _ident(text).split("_") if part)


def _step_name(index: int, stage: str) -> str:
    """Stage names repeat inside a flow; the index is what keeps the generated
    function names unique (and the graph readable in order)."""
    return f"stage_{index:02d}_{_ident(stage)}"


# ─── the export ───────────────────────────────────────────────────────────


def export_flow(
    ref: str,
    target: str,
    config: Optional[dict[str, Any]] = None,
) -> FlowExport:
    """Project flow ``ref`` onto ``target``, with ``$config.*`` resolved.

    ``config`` feeds the ordinary resolver, so unset ``$config.<key>`` entries
    are OMITTED exactly as they are on the run path (the kernel's own default
    applies) — a projection that resolved differently from a run would be a
    lie about the flow.
    """
    if target not in EXPORT_TARGETS:
        raise FlowExportError(
            f"unknown export target {target!r} — supported: {', '.join(EXPORT_TARGETS)}"
        )

    defs = load_flow_defs()
    d = defs.get(ref)
    if d is None:
        available = sorted(defs)
        raise FlowExportError(
            f"unknown flow ref {ref!r} — installed packs declare "
            f"{available or '(none)'}"
        )

    if has_in_flow_gates(d):
        gated = [f"{s.stage} (gate: {s.gate})" for s in d.stages if getattr(s, "gate", None)]
        raise FlowExportError(
            f"flow {ref!r} declares in-flow approval gate(s) before "
            f"{', '.join(gated)} — gates don't export. An exported file has no "
            f"approval queue, so the gate would silently become a comment. Run "
            f"it governed (`cairn run`), or split the flow "
            f"(`cairn pack show --split {ref}`) and export a segment."
        )

    try:
        stages = resolve_flow_ref(ref, dict(config or {}))
    except DataflowDefError as e:  # unknown ref is handled above; keep the coordinates
        raise FlowExportError(str(e)) from None

    sources = load_flow_def_sources()
    src = sources.get(ref)
    def_sha = flow_def_sha(src) if src is not None else "sha256:(def file unavailable)"

    slug = _ident(ref)
    generator = (
        f"cairn pack export {ref} --to {target}"
        + "".join(f" --config {k}={v}" for k, v in sorted((config or {}).items()))
    )
    body = _metaflow_source(
        ref=ref, flow=d, stages=stages, slug=slug,
        header=_header(ref=ref, kind=d.kind, target=target, def_sha=def_sha,
                       generator=generator, stages=stages),
    )
    return FlowExport(
        ref=ref, target=target, kind=d.kind,
        filename=f"{slug}_{target}.py", source=body,
        def_sha=def_sha, stages=stages,
    )


def _header(*, ref: str, kind: str, target: str, def_sha: str,
            generator: str, stages: list[StageRef]) -> str:
    """The stamp. No timestamps, no machine paths — an export of an unchanged
    def is byte-identical, which is what makes it diffable in a repo."""
    rule = "# " + "─" * 74
    lines = [
        rule,
        "# GENERATED BY `cairn pack export` — DO NOT EDIT.",
        "#",
        f"#   source flow : {ref}  (kind: {kind}, {len(stages)} stage(s))",
        f"#   def sha256  : {def_sha}",
        f"#   target      : {target}",
        f"#   regenerate  : {generator}",
        "#",
        "# One-way projection: this file is a SNAPSHOT of the resolved stage list",
        "# and is never re-imported as cairn's source of truth. Edits here are lost",
        "# on the next export — change the flow def and re-export instead.",
        "#",
    ]
    lines += [f"# {line}" for line in _GOVERNANCE_NOTE]
    lines.append(rule)
    return "\n".join(lines)


def _description_lines(flow) -> list[str]:
    """The def's description, wrapped — YAML folds it to one long line."""
    text = (flow.description or "").strip()
    if not text:
        return []
    import textwrap  # noqa: PLC0415 - formatting is a leaf concern of this helper

    return textwrap.wrap(" ".join(text.split()), width=76) + [""]


def _stage_comment(index: int, ref: StageRef, total: int) -> str:
    res = ref.resources
    hint = ""
    if res is not None:
        parts = [f"{k}={v}" for k, v in
                 (("cpu", res.cpu), ("memory_mb", res.memory_mb), ("gpu", res.gpu))
                 if v is not None]
        hint = f"  resources: {', '.join(parts)}" if parts else ""
    return f"stage {index} of {total} — kernel {ref.stage!r}.{hint}"


def _config_literal(config: dict[str, Any]) -> str:
    """The resolved config as a python literal. ``repr`` (not json) so
    ``True``/``None`` come out as python, and dict order — hence the bytes —
    follows the def."""
    return repr(dict(config))


_RUNNER = '''
_STAGE_GROUPS = %r


def _kernel(name):
    """Resolve a stage kernel from the SHARED registry — the same entry-point
    groups cairn's own runner resolves, so an exported stage is the identical
    callable, not a copy of its logic."""
    for group in _STAGE_GROUPS:
        for ep in entry_points(group=group):
            if ep.name == name:
                return ep.load()
    raise RuntimeError(
        f"stage kernel {name!r} is not installed (no entry point in "
        f"{list(_STAGE_GROUPS)}) — install the operator pack that provides it"
    )


def _run_stage(name, config, workdir, inputs):
    """Kernel contract: ``stage(workdir, config, inputs) -> scalar stats``.
    Stages chain through the workdir (the working dataset is
    ``workdir/current.jsonl``), so every stage gets the SAME directory."""
    wd = Path(workdir)
    wd.mkdir(parents=True, exist_ok=True)
    return _kernel(name)(wd, dict(config), list(inputs)) or {}
''' % ((STAGE_ENTRY_POINT_GROUP, FLOW_STAGE_ENTRY_POINT_GROUP),)


# ─── metaflow ─────────────────────────────────────────────────────────────


def _metaflow_resources(st: StageRef) -> str:
    """Per-stage hints map NATIVELY here — this is the feature the one-step
    bridge cannot express (#1078/#1243)."""
    res = st.resources
    if res is None:
        return ""
    parts = []
    if res.cpu is not None:
        parts.append(f"cpu={res.cpu}")
    if res.memory_mb is not None:
        parts.append(f"memory={res.memory_mb}")
    if res.gpu is not None:
        parts.append(f"gpu={res.gpu}")
    return f"@resources({', '.join(parts)})" if parts else ""


def _metaflow_source(*, ref: str, flow, stages: list[StageRef], slug: str,
                     header: str) -> str:
    cls = _camel(ref) + "Flow"
    total = len(stages)
    wants_resources = any(_metaflow_resources(st) for st in stages)
    imports = "from metaflow import FlowSpec, Parameter, step"
    if wants_resources:
        imports = "from metaflow import FlowSpec, Parameter, resources, step"

    out: list[str] = [header, '"""', f"Metaflow projection of the cairn flow `{ref}`.", ""]
    out += _description_lines(flow)
    out += [
        "ONE `@step` per stage — which is the point: a per-stage graph is what",
        "makes `resume` (and `@retry`/`@resources`) meaningful. cairn's runtime",
        "bridge runs the whole stage list in a single step because the list is",
        "dynamic; a static export has no such constraint.",
        "",
        "Run it standalone:",
        "",
        f"    python {slug}_metaflow.py run --workdir ./work",
        f"    python {slug}_metaflow.py resume --origin-run-id <id>   # failed step on",
        f"    python {slug}_metaflow.py show                          # per-stage graph",
        "",
        "`resume` clones the completed steps' ARTIFACTS. The working dataset is",
        "ordinary files under `--workdir`, so a resume needs that directory (or a",
        "shared mount) still there for the re-run steps to continue from.",
        '"""',
        "from __future__ import annotations",
        "",
        "import json",
        "from importlib.metadata import entry_points",
        "from pathlib import Path",
        "",
        imports,
        "",
        "",
        _RUNNER.strip(),
        "",
        "",
        f"class {cls}(FlowSpec):",
        f'    """The {total} stage(s) of `{ref}`, one checkpointable step each."""',
        "",
        '    workdir = Parameter("workdir", default="cairn-flow-work",',
        '                        help="Shared stage workdir (holds current.jsonl).")',
        '    input_artifacts = Parameter("input-artifacts", default="[]",',
        '                                help="JSON list of input artifact URIs.")',
        "",
        "    @step",
        "    def start(self):",
        '        """Open the shared workdir; stage stats accumulate from here."""',
        "        self.inputs_list = json.loads(self.input_artifacts or \"[]\")",
        "        self.stage_stats = {}",
        "        Path(self.workdir).mkdir(parents=True, exist_ok=True)",
        f"        self.next(self.{_step_name(0, stages[0].stage)})",
        "",
    ]

    for i, st in enumerate(stages):
        name = _step_name(i, st.stage)
        nxt = _step_name(i + 1, stages[i + 1].stage) if i + 1 < total else "end"
        deco = _metaflow_resources(st)
        if deco:
            out.append(f"    {deco}")
        out += [
            "    @step",
            f"    def {name}(self):",
            f'        """{_stage_comment(i, st, total)}"""',
            f"        config = {_config_literal(st.config)}",
            "        stats = dict(self.stage_stats)",
            f"        stats[{f'{i:02d}:{st.stage}'!r}] = _run_stage(",
            f"            {st.stage!r}, config, self.workdir, self.inputs_list)",
            "        self.stage_stats = stats",
            f"        self.next(self.{nxt})",
            "",
        ]

    out += [
        "    @step",
        "    def end(self):",
        '        """Terminal step: the per-stage stats, as JSON."""',
        "        print(json.dumps(self.stage_stats, indent=2, default=str))",
        "",
        "",
        'if __name__ == "__main__":',
        f"    {cls}()",
        "",
    ]
    return "\n".join(out)


__all__ = ["EXPORT_TARGETS", "FlowExport", "FlowExportError", "export_flow"]
