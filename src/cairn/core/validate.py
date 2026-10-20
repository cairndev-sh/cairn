"""
Offline artifact validation — pure logic surface, no CLI.

Validates a template, skill, or policy WITHOUT running an investigation
(no LLM call, no live backend). It catches the failures that otherwise only
surface mid-incident:

  - **Schema errors** — frontmatter that doesn't match the Pydantic spec.
  - **Unknown capabilities** — a template/skill referencing a tool that no
    backend serves (``registry.validate``).
  - **Unresolved graph deps** — a ``state_machine`` whose ``action`` refs,
    ``guard`` conditions, ``python_node`` functions, or ``subgraph_node``
    templates don't resolve (the compiler raises while building, never runs).
  - **Broken custom_logic** — a skill whose Python module can't import or whose
    named hooks don't exist.
  - **Output schema** — an ``output.schema`` name we don't recognize (warning).

This is the gate the roadmap ties community trust tiers to: an artifact must
``validate`` clean before it can be published/promoted. The reusable functions
here (``validate_path`` etc.) are what a CI conformance test globs over and
what the ``cairn validate`` CLI (:mod:`cairn.cli.commands.validate`)
wraps for human use.

Compiling constructs LLM client objects but never calls them, and
``build_chat_model`` gives a keyless model a compile-only placeholder, so
validation works fully offline. Nothing here touches ``os.environ``: a dummy key
stamped at import leaked into every ``cairn`` process (the CLI imports this
module) and from there into ``cairn dev``'s engine, where it reached the vendor
as a 401 instead of "no model configured".
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

import frontmatter
import yaml

from cairn.core.clients.mcp_client import tool_adapter
from cairn.core.dataflow.defs import DataflowDefError, parse_flow_def, validate_def
from cairn.core.engine.compiler import compile_template
from cairn.core.engine.schema import PolicySpec, SkillSpec
from cairn.core.engine.skill_engine import resolve_template_path
from cairn.core.operator.loader import ensure_loaded as _ensure_operators_loaded

ArtifactType = Literal[
    "template", "skill", "policy", "eval", "pipeline", "trigger", "schema",
    "manifest", "flow", "unknown",
]

# Output schema names the platform ships: the structured Pydantic models (RCA,
# TriageDecision) plus the unstructured passthroughs (free_text). Unknown names
# aren't a hard error (operators may define custom ones), but we flag them so
# typos surface.
KNOWN_OUTPUT_SCHEMAS: frozenset[str] = frozenset(
    {"RCA", "TriageDecision", "free_text", "text", "markdown"}
)

Level = Literal["error", "warning"]


@dataclass
class ValidationIssue:
    level: Level
    message: str


@dataclass
class ValidationReport:
    path: str
    artifact_type: ArtifactType
    issues: list[ValidationIssue] = field(default_factory=list)

    def error(self, msg: str) -> None:
        self.issues.append(ValidationIssue("error", msg))

    def warn(self, msg: str) -> None:
        self.issues.append(ValidationIssue("warning", msg))

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors


# ─── Detection ───────────────────────────────────────────────────────


#: Parent-directory hints, the TIEBREAK when content doesn't decide (#1538).
#: Content wins over location so a policy dropped in the wrong dir still
#: validates as what it IS; the hint only catches shapeless/empty files.
_DIR_HINTS: dict[str, ArtifactType] = {
    "pipelines": "pipeline",
    "triggers": "trigger",
    "policies": "policy",
    "schemas": "schema",
}


def detect_artifact_type(path: Path) -> ArtifactType:
    """Detect by CONTENT first, location second (#1538).

    The old rule — any ``.yaml`` is a policy — validated every pipeline,
    trigger, and schema file against ``PolicySpec`` and produced false errors
    on the scaffolder's own output, the exact opposite of an author-time gate.
    """
    suffix = path.suffix.lower()
    if path.name == "pack.toml":
        return "manifest"
    if suffix in (".yaml", ".yml"):
        if path.name.endswith((".schema.yaml", ".schema.yml")):
            return "schema"
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            return "unknown"  # unparseable YAML → the dispatch reports it
        if isinstance(doc, dict):
            if doc.get("kind") == "pipeline" or "steps" in doc:
                return "pipeline"
            if "ingress" in doc:
                return "trigger"
            if "applies_to" in doc or "rules" in doc:
                return "policy"
            # #2536 — a data/ml flow (a stage list) was validated as a policy, so a
            # fresh install's own example flow reported "1 failed". After the policy
            # markers: a policy with a stray `stages:` key is still a policy.
            if doc.get("kind") in ("data", "ml") or "stages" in doc:
                return "flow"
            if doc.get("$schema") or doc.get("type") == "object" or "properties" in doc:
                return "schema"
        return _DIR_HINTS.get(path.parent.name, "policy")
    if suffix == ".json":
        return "eval"
    if suffix in (".md", ".markdown"):
        try:
            meta = frontmatter.load(path).metadata
        except Exception:
            return "unknown"
        if "flow" in meta:
            return "template"
        if "triggers" in meta and "template" in meta:
            return "skill"
    return "unknown"


# ─── Per-type validators ─────────────────────────────────────────────


def validate_template(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    report = report or ValidationReport(str(path), "template")
    # Operation-driven templates resolve their nodes against the operator
    # registry at compile time; without this, every `operation` node fails with
    # "unregistered operator … Known: (none)". Idempotent — a no-op once loaded.
    _ensure_operators_loaded()
    try:
        compiled = compile_template(path)
    except Exception as exc:  # schema, capability, or graph-resolution failure
        report.error(f"{type(exc).__name__}: {exc}")
        return report

    spec = compiled.spec
    if spec.output and spec.output.schema_name not in KNOWN_OUTPUT_SCHEMAS:
        report.warn(
            f"output.schema '{spec.output.schema_name}' is not a known schema "
            f"({', '.join(sorted(KNOWN_OUTPUT_SCHEMAS))}) — typo or custom?"
        )
    if spec.evals and not Path(spec.evals.dataset).exists():
        report.warn(f"evals.dataset '{spec.evals.dataset}' does not exist on disk")
    return report


def validate_skill(
    path: Path,
    *,
    template_search_paths: Optional[list[Path]] = None,
    report: Optional[ValidationReport] = None,
) -> ValidationReport:
    report = report or ValidationReport(str(path), "skill")
    post = frontmatter.load(path)
    try:
        spec = SkillSpec.model_validate(post.metadata)
    except Exception as exc:
        report.error(f"{type(exc).__name__}: {exc}")
        return report

    # additional_capabilities must be real capability names
    known = set(tool_adapter.list_capabilities())
    for cap in spec.additional_capabilities:
        if cap not in known:
            report.error(f"additional_capabilities: unknown capability '{cap}'")

    # custom_logic: import the module (mirrors SkillEngine.activate order — this
    # also registers any python_node/condition the referenced template needs)
    # then confirm each named hook resolves.
    if spec.custom_logic is not None:
        cl = spec.custom_logic
        try:
            module = importlib.import_module(cl.module)
        except Exception as exc:
            report.error(f"custom_logic.module '{cl.module}' import failed: {exc}")
            module = None
        if module is not None:
            for attr in ("evaluate_trigger", "validate_action", "on_resolve", "on_failure"):
                name = getattr(cl, attr, None)
                if name and getattr(module, name, None) is None:
                    report.error(
                        f"custom_logic.{attr}='{name}' is not defined in '{cl.module}'"
                    )

    # referenced template must resolve and compile
    try:
        tpath = resolve_template_path(spec.template.name, template_search_paths)
    except FileNotFoundError as exc:
        report.error(f"template ref unresolved: {exc}")
        return report
    sub = ValidationReport(str(tpath), "template")
    validate_template(tpath, sub)
    for issue in sub.issues:
        report.issues.append(
            ValidationIssue(issue.level, f"[template {spec.template.name}] {issue.message}")
        )
    return report


def validate_policy(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    report = report or ValidationReport(str(path), "policy")
    try:
        PolicySpec.model_validate(yaml.safe_load(path.read_text()))
    except Exception as exc:
        report.error(f"{type(exc).__name__}: {exc}")
    return report


def validate_flow(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    """A data/ml flow through the parser and stage check the pack loader uses (#2536)."""
    report = report or ValidationReport(str(path), "flow")
    try:
        d = parse_flow_def(path.read_text(encoding="utf-8"), where=path.name)
    except (OSError, DataflowDefError) as exc:
        report.error(str(exc))
        return report
    for problem in validate_def(d, where=path.name):
        report.error(problem)
    return report


def validate_eval(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    report = report or ValidationReport(str(path), "eval")
    import json
    try:
        json.loads(path.read_text())
    except Exception as exc:
        report.error(f"invalid JSON: {exc}")
    return report


def validate_manifest(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    """Run ``pack.toml`` through the REAL manifest loader (#1538).

    ``load_manifest`` has strict, well-worded ``ManifestError`` messages — but
    before this they only fired at entry-point load, where a failure is a
    swallowed log warning and the pack silently vanishes. This makes them fire
    where the author is looking.
    """
    report = report or ValidationReport(str(path), "manifest")
    from cairn.core.pack.manifest import load_manifest  # lazy: pack stack off import path

    try:
        load_manifest(path)
    except Exception as exc:
        report.error(f"{type(exc).__name__}: {exc}")
    return report


def validate_trigger(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    """A ``triggers/*.yaml`` ingress spec, via the same strict loader the
    router and the console store use."""
    report = report or ValidationReport(str(path), "trigger")
    from cairn.core.engine.triggers_spec import load_trigger_spec_text  # lazy: trigger stack

    try:
        load_trigger_spec_text(path.read_text(encoding="utf-8"), name=path.stem)
    except Exception as exc:
        report.error(f"{type(exc).__name__}: {exc}")
    return report


def validate_schema(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    """A ``schemas/*.schema.yaml`` — must parse and be an object schema."""
    report = report or ValidationReport(str(path), "schema")
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        report.error(f"invalid YAML: {exc}")
        return report
    if not isinstance(doc, dict):
        report.error("a schema file must contain a YAML mapping (a JSON-Schema object)")
    return report


def validate_pipeline(path: Path, report: Optional[ValidationReport] = None) -> ValidationReport:
    """A ``pipelines/*.yaml`` — parse with the real loader, then resolve every
    executable leg against the pack's OWN flows/ (#1538).

    This is the author-time half of leg resolution: it works on an
    *uninstalled* pack by checking the files next to the pipeline. The
    runtime half — resolution against installed packs — is
    :func:`cairn.core.engine.pipeline_resolve.assert_runnable`, which gates
    plan/launch. Cross-pack refs are only warnable here (the other pack may
    legitimately not be installed while authoring).
    """
    report = report or ValidationReport(str(path), "pipeline")
    from cairn.core.engine.pipeline import load_pipeline_def, plan_pipeline  # lazy: engine stack

    try:
        d = load_pipeline_def(path)
    except Exception as exc:
        report.error(f"{type(exc).__name__}: {exc}")
        return report

    # #1763 — parse alone is not the whole author-time check: an unknown
    # `depends_on` id (and any other topology defect) only surfaces in
    # plan_pipeline, so without this line `cairn validate` blessed documents
    # that could never plan. Same class of hole as #1742: a validator that
    # skips a layer silently hollows out the gate built on it.
    try:
        plan_pipeline(d)
    except Exception as exc:
        report.error(f"cannot plan: {exc}")
        return report

    # #1792 — the gate-on-path rule, from the ONE function `cairn pipeline
    # explain` renders and the policy injector's own walk backs. It has lived
    # only in the console's client-side builder model since #1771, which meant
    # the server door the builder posts to never applied it. WARN rather than
    # error, measured not assumed: 18 of the 21 first-party pipelines carry a
    # leg it flags today (every `flow:` in the tree aggregates to
    # external_write), so promoting it is a fleet change — `--strict` already
    # fails on it for a CI that wants the stricter bar now.
    from cairn.core.engine.pipeline_explain import gate_path_problems  # noqa: PLC0415 - lazy, as above

    for problem in gate_path_problems(d):
        report.warn(problem)

    pack_root = path.parent.parent
    flows = pack_root / "flows"

    def _walk(steps):
        for s in steps:
            yield s
            yield from _walk(getattr(s, "steps", None) or [])

    # Cross-pack flow/template refs never reach here: the PARSER rejects them
    # (packs are self-sufficient, ADR-0084 D5), so every ref below is this
    # pack's own and checkable against the files on disk.
    for leg in _walk(d.steps):
        kind = leg.leg_kind
        if kind not in ("flow", "template"):
            # task: legs name operators from operator PACKS — installed-registry
            # territory, covered by assert_runnable at plan time.
            continue
        local = leg.ref.rpartition("/")[2]
        if kind == "template":
            if not (flows / f"{local}.md").is_file():
                if (flows / f"{local}.yaml").is_file():
                    report.error(
                        f"step '{leg.name}': `template: {local}` resolves to "
                        f"flows/{local}.yaml — a data/ML flow. Use `flow: {local}`."
                    )
                else:
                    report.error(
                        f"step '{leg.name}': `template: {local}` — flows/{local}.md "
                        f"does not exist in this pack"
                    )
        elif not (flows / f"{local}.yaml").is_file():
            if (flows / f"{local}.md").is_file():
                report.error(
                    f"step '{leg.name}': `flow: {local}` resolves to "
                    f"flows/{local}.md — an agentic flow. Use `template: {local}` "
                    f"(the engine executor only finds flows/*.yaml)."
                )
            else:
                report.error(
                    f"step '{leg.name}': `flow: {local}` — flows/{local}.yaml "
                    f"does not exist in this pack"
                )
    return report


# ─── Dispatch ────────────────────────────────────────────────────────


def _pack_local_search_paths(path: Path) -> list[Path]:
    """The owning pack's ``flows/`` (+ ``templates/`` alias), by walking up to
    ``pack.toml`` (#1538). Without this, validating a scaffolded skill fails
    "template ref unresolved" purely because the pack isn't installed yet —
    the search covered every INSTALLED pack's flows but never the one the
    file actually sits in.
    """
    cur = path.resolve().parent
    while True:
        if (cur / "pack.toml").is_file():
            return [d for d in (cur / "flows", cur / "templates") if d.is_dir()]
        if cur.parent == cur:
            return []
        cur = cur.parent


def validate_path(
    path: Path,
    *,
    template_search_paths: Optional[list[Path]] = None,
) -> ValidationReport:
    atype = detect_artifact_type(path)
    if atype == "template":
        return validate_template(path)
    if atype == "skill":
        search = list(template_search_paths or []) + _pack_local_search_paths(path)
        return validate_skill(path, template_search_paths=search or None)
    if atype == "policy":
        return validate_policy(path)
    if atype == "eval":
        return validate_eval(path)
    if atype == "pipeline":
        return validate_pipeline(path)
    if atype == "trigger":
        return validate_trigger(path)
    if atype == "schema":
        return validate_schema(path)
    if atype == "manifest":
        return validate_manifest(path)
    if atype == "flow":
        return validate_flow(path)
    report = ValidationReport(str(path), "unknown")
    report.error(
        "could not detect artifact type (expected template/skill/policy/eval/"
        "pipeline/trigger/schema/flow/pack.toml)"
    )
    return report


# ─── Expansion helper (shared with the CLI) ──────────────────────────


ARTIFACT_GLOBS: tuple[str, ...] = (
    "*.md", "*.markdown", "*.yaml", "*.yml", "*.json",
    # #1538 — the one file with a strict schema was the one file never
    # validated. Deliberately `pack.toml`, not `*.toml`: pyproject.toml and
    # friends are project metadata, not pack artifacts.
    "pack.toml",
)

# Filenames that match ARTIFACT_GLOBS by extension but are obviously not
# artifacts — README.md, CHANGELOG.md, LICENSE.md, repo metadata, etc.
# Excluded only when discovered through a directory walk; an explicit
# ``cairn validate README.md`` still validates it (and fails, as
# expected — the user asked).
_NON_ARTIFACT_BASENAMES: frozenset[str] = frozenset({
    "README.md", "README.markdown",
    "CHANGELOG.md", "CHANGELOG.markdown",
    "LICENSE.md", "LICENSE", "LICENSE.txt",
    "CONTRIBUTING.md", "CODE_OF_CONDUCT.md",
    "SECURITY.md", "AUTHORS.md", "NOTICE.md",
    # Repo metadata in YAML/JSON that pyproject-style projects ship.
    "pyproject.toml", "package.json", "package-lock.json",
})


def expand_paths(paths: list[Path]) -> list[Path]:
    """Expand dirs to recursively-found artifact files; files pass through.

    Used by the CLI to support both ``cairn validate path/to/file.md`` and
    ``cairn validate path/to/templates/``. Public (no underscore) because
    a CI conformance test can call it directly to walk a contribution dir.

    Skips well-known non-artifact filenames (README.md, LICENSE, …) discovered
    via directory walking — they share extensions with artifacts but aren't
    them. An explicit file path still gets validated (the user asked for it).
    """
    out: list[Path] = []
    for p in paths:
        if p.is_dir():
            for pattern in ARTIFACT_GLOBS:
                for found in sorted(p.rglob(pattern)):
                    if found.name in _NON_ARTIFACT_BASENAMES:
                        continue
                    out.append(found)
        else:
            out.append(p)
    return out
