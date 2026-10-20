"""``pipelines/*.yaml`` — the pack's top orchestrator (#1124, ADR-0084 D4).

The pack-native successor of ``bundle.toml``. A pipeline composes a pack's OWN
flows into one runnable entity: each step is a **leg** that becomes its own
governed run (own admission, budget reservation, policy, approvers), sequenced by
the control plane — never by a graph runtime.

**Evolves, does not parallel.** The authored models here are new because the file
format is new (YAML, leg kinds, loops); the EXECUTION IR is not.
:func:`plan_pipeline` emits the same :class:`~cairn.core.engine.bundle_pipeline.PipelinePlan`
the bridge already runs, reusing its topological sort and dependency validation.
One planner output, one executor.

Two ADR-0084 rules are enforced at PARSE time, because both are cheaper to catch
in a linter than at 3am:

* **D5 self-sufficiency** — a leg referencing another pack is a load error. Packs
  are domain modules that share operators/kernels and knowledge, never each
  other's executables.
* **D7 bounded loops** — ``max_iterations`` is mandatory; ``on_exhausted``
  defaults to ``gate`` so non-convergence escalates to a human rather than
  failing silently or looping on paid compute.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from cairn.core.engine.bundle import check_step_trigger
from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep, _toposort
from cairn.core.pack.manifest import find_manifest, load_manifest
from cairn.core.pack.registry import get_packs

#: Leg kinds. `flow` = data|ml (engine adapter) · `template` = agentic
#: (LangGraph adapter) · `task` = a single operator · `gate` = HITL between legs
#: · `loop` = a bounded iteration block.
LEG_KINDS = ("flow", "template", "task", "gate", "loop")

#: Legs whose ref names a PACK EXECUTABLE and is therefore subject to D5.
#: `task` is exempt: it names an OPERATOR (`domain/verb`) — a shared wheel, which
#: is one of the two layers packs are explicitly allowed to share.
_PACK_SCOPED = ("flow", "template")

#: `on:` trigger kinds the grammar KNOWS (#1543). Declared here — the model
#: layer — so the arming path (``pipeline_triggers``) and the parser share ONE
#: definition instead of two copies that can drift apart.
ON_KINDS = ("cron", "webhook", "watch")

#: `on:` kinds an install can actually ARM. Anything else is dead config: it
#: reads as wired and never fires, so the parser rejects it (#1159, #1543).
ARMABLE_ON_KINDS = ("cron",)

#: #2605 — the shape of a domain event topic a leg may ``emits:``.
_EVENT_TOPIC = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*")


class PipelineDefError(ValueError):
    """A pipeline definition that cannot be loaded (bad shape, cross-pack leg,
    unbounded loop)."""


class PipelineStep(BaseModel):
    """One step: exactly one leg kind, plus wiring.

    ``gate`` and ``loop`` carry a NAME rather than a ref — a gate is audited by
    name, and a loop is identified by one.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str = ""
    # ── exactly one of these ────────────────────────────────────────────────
    flow: Optional[str] = None
    template: Optional[str] = None
    task: Optional[str] = None
    gate: Optional[str] = None
    loop: Optional[str] = None
    # ── wiring (all legs) ───────────────────────────────────────────────────
    inputs: dict[str, Any] = Field(default_factory=dict, alias="with")
    # #1137 — typed artifacts. Optional on purpose: a leg that declares nothing
    # is unchecked, so packs adopt these incrementally instead of all at once.
    # #1251 — a trailing `?` on a CONSUMES kind marks the wire OPTIONAL
    # (`consumes: {report: Report?}`), which is what reading a `when:` leg
    # requires.
    consumes: dict[str, str] = Field(default_factory=dict)
    produces: dict[str, str] = Field(default_factory=dict)
    #: ROUTE (#1251, ADR-0087 D2) — a false `when` SKIPS this leg (verdict
    #: `routed_off`) and the pipeline CONTINUES. It is not a gate: a `gate:` leg
    #: stops the pipeline, `when:` routes around this one.
    when: Optional[str] = None
    #: ADR-0107 D4 (#2145) — a human-readable name for this leg, for the run
    #: page, the graph and the launch preview ("Fine-tuning the model" rather
    #: than `run`). DISPLAY ONLY: the dispatcher never reads it and it never
    #: reaches a step's `inputs`. Absent, every surface falls back to `id`, so
    #: an unlabelled pipeline renders exactly as it does today.
    #:
    #: `exclude=True` for the same reason as `PipelineDef.inherited_inputs`:
    #: `content_hash` is a hash of `model_dump()` (#1255), so a field left in
    #: the dump would move the identity of EVERY installed pipeline the moment
    #: this model changed, breaking every drift pin. A leg gaining a display
    #: name is not a different leg.
    label: str = ""
    depends_on: list[str] = Field(default_factory=list)
    environment: Optional[str] = None
    #: #1787 (ADR-0097) — `finally:`, as a leg. A finalizer step runs when the
    #: pipeline reaches ANY terminal state (done / failed / cancelled), exactly
    #: once, even if its dependency chain never fired. It is NOT dependency-driven
    #: and it is never a gate: the grammar grew one word so that "the box is
    #: released whatever happens" is a property of the plan rather than of the
    #: operator who remembered to stop the box by hand.
    #:
    #: A finalizer must be IDEMPOTENT. The bridge re-submits one that was in
    #: flight when the control plane died, because the alternative — parking it
    #: for a human — is a box billing until someone notices.
    finalizer: bool = False
    #: #2605 (ADR-0069's trigger binding, on a declared pipeline's leg) — WHEN this
    #: leg fires once its dependencies are met: ``auto`` (at once, the default) ·
    #: ``manual`` (waits for a person: ``awaiting_advance``) · ``schedule:<cron>``
    #: (waits for the next tick) · ``on:<event>`` (waits for that bus event). The
    #: bridge has armed all four on the plan since ADR-0069; until #2605 a declared
    #: pipeline could not say so and every leg lowered to ``auto``.
    trigger: str = "auto"
    #: #2605 — a domain event published on the bus when this leg completes, its
    #: payload the leg's output; an ``on:<event>`` leg chains off it.
    emits: Optional[str] = None
    # ── gate only ───────────────────────────────────────────────────────────
    show: list[str] = Field(default_factory=list)
    approvers: Optional[str] = None
    timeout: Optional[str] = None
    #: #1275 (ADR-0087 Amendment 1, G1) — a DATA-answered gate: the ONE
    #: expression grammar over `$input`/`$steps` (same evaluator as `when:`
    #: and the bundle gate), resolved the moment the gate becomes ready.
    #: TRUE → the gate passes instantly with verdict `data` (no human queue);
    #: FALSE → fail-closed STOP (`gate_blocked` → the pipeline fails). This is
    #: the third answerer — data, alongside human (`approvers:`) and policy
    #: (#1252 injected gates) — and it is exclusive with `approvers:`.
    predicate: Optional[str] = None
    # ── loop only ───────────────────────────────────────────────────────────
    until: Optional[str] = None
    max_iterations: Optional[int] = None
    on_exhausted: Literal["gate", "fail"] = "gate"
    steps: list["PipelineStep"] = Field(default_factory=list)

    @field_validator("trigger")
    @classmethod
    def _valid_trigger(cls, v: str) -> str:
        return check_step_trigger(v)

    @field_validator("emits")
    @classmethod
    def _valid_emits(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not _EVENT_TOPIC.fullmatch(v):
            raise ValueError(f"invalid emits {v!r}: an event topic is letters, digits and `._:-`")
        if v.startswith("run."):
            raise ValueError(f"invalid emits {v!r}: `run.*` is the engine's own lifecycle "
                             f"firehose (reserved) — name a domain topic")
        return v

    @property
    def leg_kind(self) -> str:
        for k in LEG_KINDS:
            if getattr(self, k, None):
                return k
        return ""

    @property
    def ref(self) -> str:
        """The pack-qualified executable ref (flow/template) or operator ref (task)."""
        return str(getattr(self, self.leg_kind, "") or "")

    @property
    def name(self) -> str:
        """Step identity: the explicit id, else the gate/loop name."""
        return self.id or str(self.gate or self.loop or "")


class PipelineDef(BaseModel):
    """A parsed ``pipelines/<name>.yaml`` — the pack's runnable surface."""

    model_config = ConfigDict(extra="forbid")

    name: str
    kind: Literal["pipeline"] = "pipeline"
    description: str = ""
    #: #1566 — catalog curation tier. Typed (unlike the template frontmatter
    #: equivalent), so a typo fails at parse instead of silently publishing
    #: campaign machinery into the new-pipeline picker.
    #: See :mod:`cairn.core.pack.curation`.
    visibility: Literal["public", "internal", "deprecated"] = "public"
    pack: str = ""
    #: The declared input contract. Two reserved keys (ADR-0106 D2, #2134):
    #: ``$inherit: <leg id>`` adopts that leg's own contract and wires it into
    #: the leg by name, and ``$hide: [...]`` drops inherited keys. Both are
    #: expanded at PARSE (:mod:`cairn.core.engine.pipeline_inherit`), so every
    #: reader below this line sees a plain ``{key: spec}`` map and an omitted
    #: ``inputs:`` block still means exactly "no inputs".
    inputs: dict[str, Any] = Field(default_factory=dict)
    #: #2134 S4 — provenance of the block above: ``{input key: leg id}`` for
    #: every key ``$inherit`` adopted and the file did not redeclare. Stamped by
    #: :func:`~cairn.core.engine.pipeline_inherit.resolve_inherited_inputs` and
    #: read by the resolved view, so "which document owns this default" is
    #: answered where it is known rather than re-derived by a second reader.
    #:
    #: ``exclude=True`` because it is ABOUT the definition, not part of it: it
    #: must stay out of ``model_dump()`` and therefore out of ``content_hash``,
    #: which is computed over that dump. A field that changed every installed
    #: pipeline's identity to carry a comment would break every drift pin
    #: (#1255) for nothing.
    inherited_inputs: dict[str, str] = Field(default_factory=dict, exclude=True)
    #: ADR-0107 D1/D2 (#2145) — what a person is trying to achieve, in their
    #: words, written by whoever ships the pipeline. The catalog groups on it.
    #: A literal, never inferred: no model sits in the routing path, so what a
    #: search matches is data a human wrote (D2).
    outcome: str = ""
    #: The grouping bucket the outcome belongs to (`model`, `data`, `operate`).
    #: ADR-0100 D1's flow-level `suite:` tag stays valid for the component; this
    #: is the runnable surface's own, per ADR-0107 D1.
    suite: str = ""
    #: ADR-0107 D9 (#2145) — the two legs this pipeline measured, so the
    #: before/after view can show one input through both sides without knowing
    #: the domain: teacher vs student, pre- vs post-hardening, base vs tuned.
    #: `{"before": <leg id>, "after": <leg id>}`; both must name real legs.
    compare: dict[str, str] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    governance: dict[str, Any] = Field(default_factory=dict)
    #: The LEGACY spelling of ``triggers:`` (#1159). Kept, accepted and never
    #: removed; parsed into the same declarations (ADR-0101 S7, #1987). Only
    #: ``cron`` was ever armable here, which is why it stays a raw list of
    #: mappings — the typed grammar lives on ``triggers``.
    on: list[dict[str, Any]] = Field(default_factory=list)
    #: ADR-0101 D1 (#1987) — how this pipeline starts, in the SAME grammar a
    #: flow uses (``cairn.core.engine.triggers_grammar``): manual · schedule ·
    #: webhook · watch. Held as raw mappings for the same reason ``on:`` is —
    #: the record is the YAML, and ``content_hash`` must be a hash of what was
    #: authored — and validated at parse by :func:`_validate_triggers`, which
    #: is where a typo becomes an error instead of a trigger that never fires.
    triggers: list[dict[str, Any]] = Field(default_factory=list)
    steps: list[PipelineStep] = Field(default_factory=list)
    content_hash: str = ""
    #: #1255 (ADR-0087 D6) — sha256 over the RAW record text, stamped at parse.
    #: The drift guard's client-computable identity: an SDK build artifact is
    #: byte-identical to the installed file, so a client can hash its own YAML
    #: without importing this model (the cairn_sdk import boundary), while
    #: ``content_hash`` stays the canonical parsed-content identity.
    source_hash: str = ""

    def all_steps(self) -> list[PipelineStep]:
        """Every step including loop bodies (flattened, for validation)."""
        out: list[PipelineStep] = []
        for s in self.steps:
            out.append(s)
            out.extend(s.steps)
        return out


def parse_pipeline_def(text: str, *, pack: str, source: str = "<string>") -> PipelineDef:
    """Parse + validate a pipeline definition. Raises :class:`PipelineDefError`."""
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        # #1251 — `?` is a YAML indicator, so the optional-wire marker parses
        # only as a BLOCK value (`report: Report?`) or quoted in flow style
        # (`{report: "Report?"}`). Authors hit this the first time they write an
        # optional wire; a bare "expected ',' or '}'" does not help them.
        hint = (
            "\n  hint: the optional-wire marker `?` needs quoting in flow style "
            "— write `consumes: {report: \"Report?\"}`, or use block style:\n"
            "        consumes:\n          report: Report?"
        ) if "'?'" in str(e) else ""
        raise PipelineDefError(f"{source}: invalid YAML: {e}{hint}") from None
    if not isinstance(raw, dict):
        raise PipelineDefError(f"{source}: expected a mapping at the top level")

    # #1159 — YAML 1.1 resolves a bare `on:` key to the BOOLEAN True, so an
    # author writing the trigger block exactly as ADR-0084 specifies gets a
    # `True` key and a "keywords must be strings" error. Normalize it back
    # rather than making every pipeline author quote `"on":`.
    if True in raw:
        raw["on"] = raw.pop(True)

    raw.setdefault("pack", pack)
    try:
        d = PipelineDef(**raw)
    except Exception as e:  # noqa: BLE001 — pydantic detail is the useful message
        raise PipelineDefError(f"{source}: {e}") from None

    if not d.steps:
        raise PipelineDefError(f"{source}: pipeline {d.name!r} needs at least one step")

    _validate_on(d, source=source)
    _validate_triggers(d, source=source)
    _validate_compare(d, source=source)

    for step in d.steps:
        _validate_step(step, pack=pack, source=source, in_loop=False)

    # ADR-0106 D2 (#2134) — `inputs: {$inherit: <leg id>}` adopts that leg's
    # declared contract and wires it in by name. AFTER `_validate_step`, which
    # pack-qualifies a bare leg ref (the ref is what the contract is read from),
    # and BEFORE the content hash, so the hash covers the EFFECTIVE definition.
    from cairn.core.engine.pipeline_inherit import (  # noqa: PLC0415 — reads the ui-contract/dataflow readers; neither belongs on this model's import path
        InheritError,
        resolve_inherited_inputs,
    )

    try:
        resolve_inherited_inputs(d, source=source)
    except InheritError as e:
        raise PipelineDefError(str(e)) from None

    # Identity is the CONTENT, not an authored version — same rule as every other
    # cairn artifact, so a changed pipeline is a different pipeline.
    d.content_hash = "sha256:" + hashlib.sha256(
        json.dumps(identity_dump(d), sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]
    # #1255 — the client-computable identity: hash of the raw text as authored.
    d.source_hash = "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return d


def on_entry_kind(entry: dict) -> str:
    """Which trigger kind an ``on:`` entry declares; ``""`` when none is known.

    The ONE reader of the `on:` grammar — :func:`_validate_on` above and the
    arming path (``pipeline_triggers.plan_triggers``) both call it, so "what
    counts as a trigger" cannot drift between the two.
    """
    for k in ON_KINDS:
        if entry.get(k):
            return k
    return ""


#: The display fields (ADR-0107, #2145) — excluded from a pipeline's IDENTITY,
#: but NOT from its model.
#:
#: ``content_hash`` is a hash of ``model_dump()`` (#1255), so anything left in
#: the dump is part of what a drift pin is keyed on: a field added to this model
#: with a default would move the hash of every installed pipeline at once.
#:
#: They stay in the dump on purpose. The stored ``PipelinePlan`` snapshot is
#: built from it and the run page renders leg labels from that snapshot —
#: marking them ``exclude=True`` on the field would make every historical run
#: show raw leg ids instead.
_DISPLAY_TOP: tuple[str, ...] = ("outcome", "suite", "compare")
_DISPLAY_STEP: tuple[str, ...] = ("label",)
#: #2605 — step fields that join the identity only when an author sets them.
_STEP_DEFAULTS_OUTSIDE_IDENTITY: dict[str, Any] = {"trigger": "auto", "emits": None}
#: …and on an INPUT spec (D10). These needed no model change — `inputs` is an
#: open `dict[str, Any]` — which is exactly how they were missed at first: they
#: ride INSIDE `inputs`, which is part of the identity dump, so annotating a
#: single input moved that pipeline's `content_hash` and would have broken every
#: drift pin (#1255) the moment a pack adopted D10. Caught while building #2155,
#: after the original test only covered the top-level and step-level fields.
_DISPLAY_INPUT: tuple[str, ...] = ("kind", "essential")


def identity_dump(d: "PipelineDef") -> dict[str, Any]:
    """The exact dict ``content_hash`` is taken over — the ONE definition.

    Public because ``test_pipeline_drift_guard`` needs it: that test used to
    restate the formula inline (``model_dump(exclude={"content_hash",
    "source_hash"})``) and so silently stopped describing the implementation the
    moment the canonical one changed. Two copies of a contract drift — the same
    reason :func:`lower_step` has exactly one caller-shared body.
    """
    return _identity_view(d.model_dump(exclude={"content_hash", "source_hash"}))


def _identity_view(dump: dict[str, Any]) -> dict[str, Any]:
    """Strip the display fields from a dumped definition, at any depth.

    A pydantic ``exclude={"steps": {"__all__": {"label"}}}`` reaches only the
    TOP-level legs, and a ``PipelineStep`` can carry its own ``steps`` (a loop
    body). Two nested ``label: ""`` entries survived that form and moved the
    hash of every pipeline with a loop — caught by the ADR-0107 golden baseline
    before it shipped. Recursion has no depth limit, so nesting cannot outrun it.
    """
    out = {k: v for k, v in dump.items() if k not in _DISPLAY_TOP}

    # `inputs` is `{key: spec}`; the display keys sit inside each SPEC.
    ins = out.get("inputs")
    if isinstance(ins, dict):
        out["inputs"] = {
            k: ({kk: vv for kk, vv in spec.items() if kk not in _DISPLAY_INPUT}
                if isinstance(spec, dict) else spec)
            for k, spec in ins.items()
        }

    def _steps(items: Any) -> Any:
        if not isinstance(items, list):
            return items
        cleaned = []
        for st in items:
            if not isinstance(st, dict):
                cleaned.append(st)
                continue
            st = {k: v for k, v in st.items() if k not in _DISPLAY_STEP}
            # #2605 — `trigger`/`emits` at their defaults are the pre-#2605 leg, so
            # they leave the identity untouched: no installed pipeline's
            # content_hash moves (#1255). A leg that SETS one is a different leg.
            for k, default in _STEP_DEFAULTS_OUTSIDE_IDENTITY.items():
                if k in st and st[k] == default:
                    del st[k]
            if "steps" in st:
                st["steps"] = _steps(st["steps"])
            cleaned.append(st)
        return cleaned

    if "steps" in out:
        out["steps"] = _steps(out["steps"])
    return out


def _validate_compare(d: "PipelineDef", *, source: str) -> None:
    """ADR-0107 D9 (#2145) — `compare:` must name real legs.

    The before/after view is the last thing a user sees after a run that cost
    money. A `compare` pointing at a leg that does not exist would render an
    empty panel there, and the author would have no way to know until then —
    so it fails at LOAD, where a typo is cheap.
    """
    if not d.compare:
        return
    ids = {s.id for s in d.steps}
    unknown = sorted(k for k in ("before", "after") if d.compare.get(k) and d.compare[k] not in ids)
    if unknown:
        bad = ", ".join(f"{k}: {d.compare[k]!r}" for k in unknown)
        raise PipelineDefError(
            f"{source}: pipeline {d.name!r} `compare:` names a leg that does not "
            f"exist ({bad}); legs are {sorted(ids)}")
    missing = sorted(k for k in ("before", "after") if not d.compare.get(k))
    if missing:
        raise PipelineDefError(
            f"{source}: pipeline {d.name!r} `compare:` needs both `before` and "
            f"`after`; missing {missing}")
    extra = sorted(set(d.compare) - {"before", "after"})
    if extra:
        raise PipelineDefError(
            f"{source}: pipeline {d.name!r} `compare:` accepts only `before` and "
            f"`after`; got {extra}")


def _validate_on(d: PipelineDef, *, source: str) -> None:
    """Reject dead `on:` config at PARSE, not at arm time (#1543).

    ``plan_triggers`` already refused an unarmable kind — but only when an
    install path reached it, which nothing in `cairn validate` does. So a
    pipeline could ship, lint clean, and declare a `webhook:` that reads as
    wired and never fires. Same rule, moved to the seam every reader crosses.
    """
    # A non-mapping entry never reaches here — `on: list[dict[str, Any]]` makes
    # pydantic reject it first, and that message already names the index.
    for i, entry in enumerate(d.on or []):
        where = f"{source}: pipeline {d.name!r} `on:` entry {i}"
        kind = on_entry_kind(entry)
        if not kind:
            raise PipelineDefError(
                f"{where} declares no known trigger — expected one of "
                f"{', '.join(ON_KINDS)}")
        if kind not in ARMABLE_ON_KINDS:
            raise PipelineDefError(
                f"{where}: `on: {kind}` cannot be armed — only "
                f"{', '.join(ARMABLE_ON_KINDS)} is materialized at install. "
                f"webhook/watch arming is #1159; until it lands, declaring it "
                f"here is dead config: it reads as wired and never fires. "
                f"Remove the entry, or use `on: cron:`.")


def _validate_triggers(d: PipelineDef, *, source: str) -> None:
    """Validate ``triggers:`` on the SAME seam ``on:`` is validated on (#1987).

    ADR-0101 D1 gives a pipeline the flow's trigger grammar, so the grammar is
    the flow's: :func:`cairn.core.engine.triggers_grammar.parse_declared_triggers`,
    not a second parser that would eventually disagree with it about what a
    cron or a receiver preset is.

    ``on:`` and ``triggers:`` are two spellings of ONE thing, so declaring both
    is refused here rather than resolved by a precedence rule. A precedence
    rule is invisible: the most likely way to write both is to convert an
    ``on:`` block and forget to delete it, and whichever way that resolved —
    silently dropping a declared trigger, or arming the same cron twice — is a
    worse answer than a message at parse saying to keep one.
    """
    raw = d.triggers or []
    if raw and d.on:
        raise PipelineDefError(
            f"{source}: pipeline {d.name!r} declares BOTH `on:` and `triggers:` "
            f"— they are two spellings of one grammar. Keep `triggers:` (the "
            f"current one) and move each `on: cron:` entry into it as "
            f"`- kind: schedule`, or keep `on:` alone."
        )
    if not raw:
        return
    # lazy: the trigger grammar lives in the triggers package, which imports the
    # receiver presets and the spec loader — neither belongs on the pipeline
    # model's import path, and `cairn.core.engine.triggers_grammar` reaches back here
    # for a pipeline ref's declarations (a genuine cycle).
    from cairn.core.engine.triggers_grammar import (  # noqa: PLC0415
        DeclaredTriggerError,
        parse_declared_triggers,
    )

    try:
        decls = parse_declared_triggers(raw, where=f"pipeline {d.name!r} triggers")
    except DeclaredTriggerError as e:
        raise PipelineDefError(f"{source}: {e}") from None
    # ADR-0131 D6 — a pipeline that starts on a topic one of its own legs emits would
    # start the next run from every run. Refused here; longer cycles are not detected.
    emitted = {s.emits for s in _all_steps(d.steps) if s.emits}
    for decl in decls:
        if getattr(decl, "kind", "") == "event" and decl.topic in emitted:
            raise PipelineDefError(
                f"{source}: pipeline {d.name!r} starts on `{decl.topic}`, which one of its "
                f"own legs emits — every run would start the next. Emit a different topic, "
                f"or start a different pipeline on it.")


def _all_steps(steps: list["PipelineStep"]) -> list["PipelineStep"]:
    """Every leg, loop bodies included."""
    out: list[PipelineStep] = []
    for s in steps or []:
        out.append(s)
        out.extend(_all_steps(s.steps))
    return out


#: #1288 — the ONE duration grammar for leg `timeout:`: bare seconds or a
#: number with a d/h/m/s suffix ("8h", "30m", "90s", "2d", "3600").
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([dhms]?)\s*$")
_DURATION_UNIT_S = {"d": 86400.0, "h": 3600.0, "m": 60.0, "s": 1.0, "": 1.0}


def timeout_seconds(raw: Optional[str]) -> Optional[float]:
    """Parse a leg ``timeout:`` string to seconds; ``None``/empty → ``None``.

    Raises :class:`PipelineDefError` on an unparseable value — a timeout typo
    must fail at LOAD, not silently become "no limit" at runtime (#1288).
    """
    if raw is None or not str(raw).strip():
        return None
    m = _DURATION_RE.match(str(raw))
    if not m:
        raise PipelineDefError(
            f"unparseable timeout {raw!r} — use seconds or a d/h/m/s suffix "
            f"(\"8h\", \"30m\", \"90s\", \"2d\")."
        )
    return float(m.group(1)) * _DURATION_UNIT_S[m.group(2)]


def _check_expr(raw: Optional[str], *, what: str, source: str, step: str) -> None:
    """Compile a `when:` / `predicate:` / `until:` expression at PARSE time.

    #1825 — these are JMESPath, and nothing used to compile them until the
    moment they ran. An expression like `steps.pull.rows > 0` (a NUMBER
    literal needs backticks in JMESPath) therefore shipped, validated clean,
    and then raised inside the pipeline bridge's terminal handler — where the
    bus's at-most-once contract logs and DROPS the message. The visible result
    was not an error but a pipeline stuck `running` forever with its leg
    already done. Same lesson as #1742: a validator that skips a layer
    silently hollows out the gate built on it.
    """
    if not raw or not str(raw).strip():
        return
    from cairn.core.engine.expr import ExprError, compile_predicate  # noqa: PLC0415 — sibling module, kept local to avoid an import cycle through engine.bundle

    try:
        compile_predicate(str(raw), source=source)
    except ExprError as e:
        raise PipelineDefError(
            f"{source}: step {step!r} has an invalid {what} expression {raw!r}: {e}. "
            f"These are JMESPath: a literal number or string needs backticks — "
            f"write `steps.x.rows > \u00600\u0060`, not `steps.x.rows > 0` — while a bare "
            f"name like `input.max_cost_usd` is read as a field path."
        ) from e


def _validate_step(step: PipelineStep, *, pack: str, source: str, in_loop: bool) -> None:
    # #1288 — a timeout typo fails at LOAD, never silently becomes "no limit"
    if step.timeout and step.leg_kind != "gate":
        timeout_seconds(step.timeout)
    _check_expr(step.when, what="when:", source=source, step=step.name or "<unnamed>")
    kinds = [k for k in LEG_KINDS if getattr(step, k, None)]
    if len(kinds) != 1:
        raise PipelineDefError(
            f"{source}: step {step.name or '<unnamed>'} declares {len(kinds)} leg "
            f"kinds {kinds!r} — a step must have exactly one of {list(LEG_KINDS)}"
        )
    kind = kinds[0]

    # #1787 (ADR-0097) — the three shapes a finalizer must not take, each caught
    # at LOAD rather than discovered on the run that needed the teardown:
    #  * a GATE finalizer would pause a pipeline that has already terminated,
    #    with nothing left to release it — the release would never run;
    #  * a LOOP finalizer has no meaning (a loop is unrolled into body steps);
    #  * `when:` routes a leg OFF, which is the one thing a `finally:` may not be.
    if step.finalizer:
        if kind in ("gate", "loop"):
            raise PipelineDefError(
                f"{source}: step {step.name!r} is `finalizer: true` on a {kind!r} "
                f"leg — a finalizer runs after the pipeline is terminal, where a "
                f"gate can never be answered and a loop can never iterate. Use a "
                f"`task:`/`flow:`/`template:` leg."
            )
        if in_loop:
            raise PipelineDefError(
                f"{source}: step {step.name!r} is `finalizer: true` inside a loop "
                f"body — a finalizer belongs to the PIPELINE, which terminates "
                f"once; a per-iteration teardown is an ordinary body step."
            )
        if step.when:
            raise PipelineDefError(
                f"{source}: step {step.name!r} declares both `finalizer: true` and "
                f"`when:` — a finalizer runs unconditionally, so a route that can "
                f"skip it is a teardown that reads as guaranteed and is not."
            )

    # #2605 — a trigger or an event belongs to a leg that RUNS something. A gate is
    # already a wait (for a person or a predicate); a loop is unrolled into body
    # legs, and a body leg's wait would re-arm on every iteration; a finalizer runs
    # after the pipeline is terminal, when nothing is left to wait for or chain to.
    if step.trigger != "auto" or step.emits:
        what = "trigger:" if step.trigger != "auto" else "emits:"
        if kind in ("gate", "loop"):
            raise PipelineDefError(
                f"{source}: step {step.name!r} declares `{what}` on a {kind!r} leg — "
                f"only a `flow:`/`template:`/`task:` leg can wait on a trigger or emit an event")
        if in_loop:
            raise PipelineDefError(
                f"{source}: step {step.name!r} declares `{what}` inside a loop body — "
                f"put the wait or the event on a leg outside the loop")
        if step.finalizer:
            raise PipelineDefError(
                f"{source}: step {step.name!r} is a finalizer and declares `{what}` — "
                f"a finalizer runs once the pipeline is terminal, unconditionally")

    if kind == "loop":
        if not step.max_iterations or step.max_iterations < 1:
            raise PipelineDefError(
                f"{source}: loop {step.loop!r} has no max_iterations — a loop over "
                f"paid compute MUST be bounded (ADR-0084 D7). Add "
                f"`max_iterations: <n>`; `on_exhausted` defaults to `gate`, so a "
                f"loop that cannot converge asks a human instead of failing silently."
            )
        if not step.until:
            raise PipelineDefError(
                f"{source}: loop {step.loop!r} has no `until:` exit condition")
        # NOT _check_expr: `until:` is evaluated by pipeline_loop.evaluate_until,
        # a different grammar from the JMESPath `when:`/`predicate:` take. Running
        # the JMESPath compiler over it rejected forms the loop evaluator accepts
        # (`eval.rows > 0`) while blessing the backticked form this module's own
        # error message recommends — which evaluate_until reads as a literal
        # string, so the loop never converges and burns every iteration. Until
        # both sides share one compiler, checking it here is worse than not.
        if not step.steps:
            raise PipelineDefError(f"{source}: loop {step.loop!r} has an empty body")
        for inner in step.steps:
            _validate_step(inner, pack=pack, source=source, in_loop=True)
        return

    if kind == "gate":
        bad = [r for r in (step.show or []) if "." not in str(r)]
        if bad:
            raise PipelineDefError(
                f"{source}: gate {step.gate!r} `show:` entries {bad!r} are not "
                f"`<step>.<field>` refs — an approver would see a field that can "
                f"never resolve"
            )
        # #1275 — one gate, one answerer: data XOR human. A predicate gate
        # resolves instantly and never enters the approval queue, so pairing
        # it with `approvers:` would silently drop the human.
        _check_expr(step.predicate, what="predicate:", source=source,
                    step=step.name or "<gate>")
        if step.predicate and step.approvers:
            raise PipelineDefError(
                f"{source}: gate {step.gate!r} declares both `predicate:` and "
                f"`approvers:` — a gate is answered by data OR a human, never "
                f"both. Split into two gates if you need both verdicts."
            )
        return

    if step.predicate:
        raise PipelineDefError(
            f"{source}: step {step.name!r} carries `predicate:` but is a "
            f"{kind} leg — a data predicate belongs on a `gate:` leg (#1275); "
            f"use `when:` to ROUTE around an executable leg."
        )

    if kind in _PACK_SCOPED:
        step_ref = getattr(step, kind)
        owner, _, leaf = str(step_ref).rpartition("/")
        if owner and owner != pack:
            raise PipelineDefError(
                f"{source}: step {step.name!r} references {step_ref!r} from pack "
                f"{owner!r}, but this pipeline belongs to pack {pack!r}. Packs are "
                f"SELF-SUFFICIENT (ADR-0084 D5): a pack composes only its own pack "
                f"flows. Share operators/kernels and knowledge packs; copy the flow "
                f"YAML into {pack!r} if you need it here."
            )
        if not owner:
            # bare `flow: clean` → this pack's clean (the common authoring case)
            setattr(step, kind, f"{pack}/{leaf}")


#: Python-ish names a pipeline's `inputs: {x: {type: ...}}` uses, mapped onto
#: JSON-Schema type names for the field-level wire check (#1540). Artifact
#: kinds (Dataset, Model, …) are NOT here on purpose — they stay the
#: kind-level check's vocabulary and are "unknown" at the field level.
_INPUT_TYPE_NAMES: dict[str, str] = {
    "str": "string", "string": "string",
    "int": "integer", "integer": "integer",
    "float": "number", "number": "number",
    "bool": "boolean", "boolean": "boolean",
    "dict": "object", "object": "object",
    "list": "array", "array": "array",
}


def _leg_io_field_types(
    step: PipelineStep,
) -> tuple[Optional[dict], Optional[dict]]:
    """(input field→types, output field→types) for a task/template leg (#1540).

    ``None`` on a side means "no schema resolvable" and the field-level check
    skips that side — enforcement is exactly as strong as what resolves.
    Resolution failures are deliberately swallowed: a hermetic test's synthetic
    ref or an uninstalled pack must not fail PLANNING here; #1538's
    ``assert_runnable`` owns "does the leg exist" at admission.
    """
    try:
        if step.task:
            from cairn.core.operator.registry import default_registry  # noqa: PLC0415 - cycle: engine → operator
            if not default_registry.has(step.task):
                return None, None
            cls = default_registry.get(step.task)
            from cairn.core.engine.schemas import field_types  # noqa: PLC0415 - local to keep module import light
            return field_types(cls.input_schema), field_types(cls.output_schema)
        if step.template:
            import frontmatter  # noqa: PLC0415 - heavy parse, only on template legs

            from cairn.core.engine.schemas import field_types, resolve_schema  # noqa: PLC0415
            from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415 - cycle
            meta = frontmatter.load(resolve_template_path(step.template)).metadata
            i = resolve_schema(meta.get("input_schema"))
            o = resolve_schema(meta.get("output_schema"))
            return (field_types(i) if i is not None else None,
                    field_types(o) if o is not None else None)
    except Exception:  # noqa: BLE001 — unresolvable = unchecked, never a plan failure
        return None, None
    return None, None


def _type_names(types: frozenset) -> str:
    return "|".join(sorted(types)) or "?"


def _check_wires(d: PipelineDef, steps: list[PipelineStep],
                 by_id: dict[str, PipelineStep],
                 dependencies: dict[str, list[str]]) -> None:
    """Type-check every `<step>.<field>` / `$input.<key>` wire (#1137 + #1540),
    and require an OPTIONAL wire onto any leg that can be routed off (#1251).

    Runs at PLAN time — before the bridge submits leg 1 — because the cost of a
    bad wire is paid in compute, not in a stack trace. Two layers:

    - the KIND check (#1137): coarse artifact kinds from `produces:`/
      `consumes:`; unknown passes (`artifacts.compatible`'s one-sidedness).
    - the FIELD check (#1540): where a leg's real schema resolves (task legs'
      Pydantic models, template legs' frontmatter schemas), each wire is
      checked producer-field-type against consumer-field-type, and a wire
      naming a field a TASK leg provably does not produce is an error — an
      operator's output is Pydantic-validated at runtime, so its schema is the
      complete contract. (Template legs may legitimately emit extra findings
      keys, so absence there is not proof.) Unknown still passes: adoption
      stays incremental, but every resolvable schema is enforced for free.

    The optional-wire rule is NOT one-sided: a leg carrying `when:` may
    legitimately never run, so an undeclared wire onto it would hand the
    reader a null it never asked for — the exact "null surprise at runtime"
    ADR-0087 D2 forbids.
    """
    from cairn.core.engine.artifacts import WireTypeError, check_wire, is_optional  # noqa: PLC0415 - cycle
    from cairn.core.engine.expr import ExprError, compile_expr  # noqa: PLC0415 - local
    from cairn.core.engine.schemas import types_assignable  # noqa: PLC0415 - local

    io_cache: dict[str, tuple[Optional[dict], Optional[dict]]] = {}

    def _io(step: PipelineStep) -> tuple[Optional[dict], Optional[dict]]:
        if step.id not in io_cache:
            io_cache[step.id] = _leg_io_field_types(step)
        return io_cache[step.id]

    def _check_field_types(*, src_label: str, src_field: str,
                           produced_types: Optional[frozenset],
                           dst: PipelineStep, key: str) -> None:
        if produced_types is None:
            return
        dst_in, _ = _io(dst)
        consumed_types = (dst_in or {}).get(key)
        if consumed_types is None:
            return
        if not types_assignable(produced_types, consumed_types):
            raise WireTypeError(
                f"pipeline {d.name!r}: {src_label} produces "
                f"`{src_field}: {_type_names(produced_types)}` but leg "
                f"{dst.name!r} expects `{key}: {_type_names(consumed_types)}` "
                f"— incompatible types. Fix the wire, or the schemas."
            )

    dependency_ancestors: dict[str, set[str]] = {}
    for step in steps:
        ancestors: set[str] = set()
        pending = list(dependencies.get(step.name, []))
        while pending:
            parent = pending.pop()
            if parent in ancestors:
                continue
            ancestors.add(parent)
            pending.extend(dependencies.get(parent, []))
        dependency_ancestors[step.name] = ancestors

    def _check_nested_wires(
        dst: PipelineStep, value: Any, path: tuple[str, ...],
    ) -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                _check_nested_wires(dst, child, path + (f"[{child_key!r}]",))
            return
        if isinstance(value, list):
            for index, child in enumerate(value):
                _check_nested_wires(dst, child, path + (f"[{index}]",))
            return
        if not isinstance(value, str):
            return

        location = "with" + "".join(path)
        if value.startswith("$"):
            try:
                references = compile_expr(value, source=location).references()
            except ExprError as e:
                raise PipelineDefError(
                    f"pipeline {d.name!r}: step {dst.name!r} has an invalid "
                    f"nested wire at `{location}`: {e}"
                ) from e

            for scope, parts in references:
                if scope == "input":
                    input_id = parts[0] if parts else ""
                    if input_id not in (d.inputs or {}):
                        raise PipelineDefError(
                            f"pipeline {d.name!r}: step {dst.name!r} has nested "
                            f"wire {value!r} at `{location}` referencing "
                            f"undeclared pipeline input {input_id!r}."
                        )
                elif scope == "steps":
                    source_id = parts[0] if parts else ""
                    if source_id not in by_id:
                        raise PipelineDefError(
                            f"pipeline {d.name!r}: step {dst.name!r} has nested "
                            f"wire {value!r} at `{location}` referencing "
                            f"unknown step {source_id!r}."
                        )
                    # A $default may intentionally read the prior loop iteration;
                    # its fallback is not a same-iteration dependency.
                    if (value.startswith("$steps.")
                            and source_id not in dependency_ancestors[dst.name]):
                        raise PipelineDefError(
                            f"pipeline {d.name!r}: step {dst.name!r} has nested "
                            f"wire {value!r} at `{location}` but does not depend "
                            f"on producer step {source_id!r}; add it to "
                            "`depends_on`."
                        )
            return

        head, sep, field = value.partition(".")
        if sep and field and head in by_id:
            raise PipelineDefError(
                f"pipeline {d.name!r}: step {dst.name!r} has ambiguous nested "
                f"value {value!r} at `{location}`; write `$steps.{value}` for "
                "a step wire."
            )

    for dst in steps:
        for key, binding in (dst.inputs or {}).items():
            if isinstance(binding, (dict, list)):
                _check_nested_wires(dst, binding, (f"[{key!r}]",))
                continue
            if not isinstance(binding, str):
                continue
            consumed = (dst.consumes or {}).get(key, "")
            raw = binding.strip()
            if raw.startswith("$input."):
                field = raw[len("$input."):]
                spec = (d.inputs or {}).get(field) or {}
                produced = spec.get("type", "") if isinstance(spec, dict) else ""
                check_wire(src_step="$input", src_field=field, produced=produced,
                           dst_step=dst.name, dst_input=key, consumed=consumed)
                declared = _INPUT_TYPE_NAMES.get(str(produced).strip().lower())
                _check_field_types(
                    src_label=f"pipeline input `{field}`", src_field=field,
                    produced_types=frozenset({declared}) if declared else None,
                    dst=dst, key=key,
                )
                continue
            # both wire spellings reach the same step: the #1137 authoring form
            # (`<step>.<field>`) and the runtime resolver's (`$steps.<id>.<f>`).
            head = raw[len("$steps."):] if raw.startswith("$steps.") else raw
            src_id, _, src_field = head.partition(".")
            src = by_id.get(src_id)
            if src is None or not src_field:
                continue          # not a step wire (a literal, or another `$` ref)
            if src.when and not is_optional(consumed):
                raise PipelineDefError(
                    f"pipeline {d.name!r}: step {dst.name!r} consumes "
                    f"{key!r} from {src_id!r}, but {src_id!r} carries "
                    f"`when: {src.when}` — it may be ROUTED OFF and produce "
                    f"nothing. Declare the wire optional "
                    f"(`consumes: {{{key}: {(consumed or 'Kind').strip() or 'Kind'}?}}` "
                    f"on {dst.name!r}) so the key is simply omitted when "
                    f"{src_id!r} is skipped, or drop `when:` from {src_id!r}."
                )
            check_wire(src_step=src_id, src_field=src_field,
                       produced=(src.produces or {}).get(src_field, ""),
                       dst_step=dst.name, dst_input=key, consumed=consumed)
            # `src_field` may be a dotted sub-path (`prep.stats.rows` reads
            # INTO the `stats` field) — existence and typing apply to the HEAD
            # segment only; a sub-path's type is unknown by construction.
            field_head, _, field_rest = src_field.partition(".")
            _, src_out = _io(src)
            # An EMPTY field map is an opaque contract (a passthrough op, a
            # fixture model with no declared fields) — only a schema that
            # declares fields can prove one absent.
            if src_out and field_head not in src_out:
                if src.task and field_head not in (src.produces or {}):
                    raise PipelineDefError(
                        f"pipeline {d.name!r}: step {dst.name!r} wires "
                        f"{key!r} from `{src_id}.{src_field}`, but operator "
                        f"{src.task!r} does not produce a field named "
                        f"{field_head!r} (its output schema declares: "
                        f"{sorted(src_out)}). A renamed output field breaks "
                        f"here, at validate time — not in the consumer's run."
                    )
                continue  # template/flow legs may emit extra findings keys
            _check_field_types(
                src_label=f"leg {src_id!r}", src_field=src_field,
                produced_types=(src_out or {}).get(field_head) if not field_rest else None,
                dst=dst, key=key,
            )


def _optional_inputs(step: PipelineStep) -> list[str]:
    """Input keys this step declared as OPTIONAL wires (`consumes: {x: Report?}`).

    Carried into the plan so the bridge knows which keys to OMIT (rather than
    pass as ``None``) when their producer was routed off (#1251).
    """
    from cairn.core.engine.artifacts import is_optional  # noqa: PLC0415 - cycle

    return sorted(k for k, v in (step.consumes or {}).items() if is_optional(v))


def _lower_wire(raw: Any, known: Any, *, nested: bool = False) -> Any:
    """``<step>.<field>`` (the #1137 authoring grammar) → ``$steps.<step>.<field>``
    (the runtime resolver's grammar). Only rewrites when the head names a REAL
    step — ``$…`` refs and genuine literals pass through untouched. Inside a
    nested ``with:`` value, bare ``<step>.<field>`` strings are ambiguous with
    literals and must use the explicit ``$steps.`` spelling.

    #1180: ``_check_wires`` validated the authoring form at plan time but the
    plan then carried it VERBATIM, so ``resolve_binding`` treated
    "readiness.export" as a literal string and the train leg received a
    ``dataset_uri`` of "readiness.export". Found by the first two-flow-leg live
    run.

    #1675: only STRINGS are wires. A ``with:`` value used to be ``str()``'d on
    the way in, so a map or list reached its leg as a Python repr — a train
    leg's ``retry: {max_attempts: 2}`` stopped being a policy and proxy-rank's
    ``base_inputs`` map failed its operator's schema. Containers now recurse (a
    nested wire string still lowers), JSON scalars keep their YAML type, and
    any other YAML scalar (a date) is ``str()``'d as before so the plan stays
    JSON-native.
    """
    if isinstance(raw, dict):
        return {k: _lower_wire(v, known, nested=True) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_lower_wire(v, known, nested=True) for v in raw]
    if raw is None or isinstance(raw, (bool, int, float)):
        return raw
    raw = str(raw)
    if raw.startswith("$"):
        return raw
    head, sep, field = raw.partition(".")
    if sep and field and head in known:
        if nested:
            raise PipelineDefError(
                f"nested with value {raw!r} is ambiguous with a step wire; "
                f"write `$steps.{raw}` to reference the step."
            )
        return f"$steps.{head}.{field}"
    return raw


def lower_step(step: PipelineStep, *, depends_on: list[str],
               known: Any) -> PipelinePlanStep:
    """Authored step → execution IR. **The one lowering**, and it has exactly
    two callers on purpose: :func:`plan_pipeline` (iteration 1 and everything
    outside a loop) and the bridge's loop-append path (iterations 2..N, #1155).

    That second caller used to carry its own partial COPY of this stamping, and
    every field the copy forgot was silently dropped from iteration 2 onward:

    * ``produces`` — a flow leg's engine stats were then mapped onto no names at
      all, so the next leg's ``$steps.<id>.<name>`` wire resolved to NOTHING;
    * ``gate`` — a DATA gate lost its predicate and passed unconditionally, so a
      loop that blocks on a bad verdict in cycle 1 waved cycle 2 through;
    * ``approval`` — a HITL gate stopped pausing and auto-approved itself;
    * ``optional_inputs`` — a ``when:``-routed producer's omitted key came back
      as a silent ``None``;
    * the wire lowering itself, so an authored ``<step>.<field>`` reached the leg
      as that literal string.

    #1303 already fixed the ``ref`` stamping in that copy the same way — one
    field at a time, as each was found live. Two copies of a contract drift, and
    a copy that drifts INTO fail-open governance is the worst kind, so there is
    now one function and no copy.
    """
    kind = step.leg_kind
    return PipelinePlanStep(
        id=step.name,
        # ADR-0084 D0: a FLOW leg records `flow:<pack>/<name>` so R2's
        # admission-recorded identity holds with no nullable ref. Template legs
        # keep the plain pack/template ref they already use.
        # #1180 — a GATE leg's ref is `gate:<name>` (D0-consistent): a gate has
        # NO executable, and an unprefixed name was dispatched through
        # run_template — "Template 'promote-model' not found".
        # #1278 — a TASK leg records `task:<op>`, same reason.
        ref=(f"flow:{step.ref}" if kind == "flow"
             else f"gate:{step.name}" if kind == "gate"
             else f"task:{step.ref}" if kind == "task"
             else step.ref or step.name),
        depends_on=list(depends_on),
        # #1251 (ADR-0087 D2) — `when` compiles onto the plan's OWN `when`
        # field, not onto `gate`. Folding them made an optional leg ("benchmark
        # only if asked") fail the whole pipeline; the plan's `gate` stays the
        # fail-closed STOP predicate.
        when=step.when,
        # #1275 — a gate leg's DATA predicate rides the fail-closed `gate` field
        # (FALSE → gate_blocked → the pipeline fails); `approval` turns OFF for
        # predicate gates so they resolve instantly instead of queueing.
        gate=step.predicate,
        optional_inputs=_optional_inputs(step),
        inputs={k: _lower_wire(v, known)
                for k, v in (step.inputs or {}).items()},
        environment=step.environment,
        # #1288 — carry the declared wall-clock limit; gate `timeout:` is the
        # approval-expiry surface, not an execution limit.
        timeout_s=timeout_seconds(step.timeout) if kind != "gate" else None,
        approval=(kind == "gate" and not step.predicate),
        produces=sorted((step.produces or {}).keys()),
        # #1128's approver context — gate legs only, and it has to travel WITH
        # the plan: the bridge builds the queue entry from the plan step, so a
        # `show:`/`approvers:`/`timeout:` left behind here is a gate that pauses
        # with nothing to look at.
        show=list(step.show or []) if kind == "gate" else [],
        approvers=step.approvers if kind == "gate" else None,
        timeout=step.timeout if kind == "gate" else None,
        # #1787 — stamped through THE one lowering, so a finalizer inside a
        # bridge-appended loop iteration would carry it too. (It cannot be: the
        # parser refuses `finalizer:` in a loop body. Stamping it here anyway is
        # the point of having one lowering — the field cannot be the next one a
        # second copy forgets.)
        finalizer=bool(step.finalizer),
        # #2605 — the leg's trigger binding and event, onto the plan fields the
        # bridge has armed since ADR-0069.
        trigger=step.trigger,
        emits=step.emits,
    )


def plan_pipeline(d: PipelineDef) -> PipelinePlan:
    """Compile a definition into the EXISTING execution IR.

    Reuses ``bundle_pipeline``'s toposort + dependency validation, so a pipeline
    and a legacy bundle produce the same plan shape and the same executor runs
    both. Loop steps are NOT unrolled here — unrolling happens at runtime
    (#1129), so the plan stays acyclic and the audit trail linear.
    """
    # #1155 — a loop is EXPANDED into iteration 1's body. Later iterations are
    # appended by the bridge as `until` keeps failing, so the plan stays acyclic
    # and every iteration is its own set of governed runs.
    from cairn.core.engine.pipeline_loop import unroll_iteration  # noqa: PLC0415 - cycle

    steps: list[PipelineStep] = []
    loops: dict[str, Any] = {}
    loop_tail: dict[str, list[str]] = {}
    for s in d.steps:
        if s.leg_kind != "loop":
            steps.append(s)
            continue
        body = unroll_iteration(s, 1)
        if not body:
            continue
        # the loop's own deps become the first body step's deps
        body[0].depends_on = list(s.depends_on) or body[0].depends_on
        loops[s.name] = s.model_dump(by_alias=True)
        loop_tail[s.name] = [b.name for b in body]
        steps.extend(body)

    # a step that depended on the LOOP now depends on that iteration's last step
    if loop_tail:
        for s in steps:
            s.depends_on = [loop_tail[dep][-1] if dep in loop_tail else dep
                            for dep in s.depends_on]

    ids = [s.name for s in steps]
    deps: dict[str, list[str]] = {}
    for i, s in enumerate(steps):
        explicit = list(s.depends_on)
        for dep in explicit:
            if dep not in ids:
                raise PipelineDefError(
                    f"pipeline {d.name!r}: step {s.name!r} depends on unknown step "
                    f"{dep!r} — known: {ids!r}"
                )
        # implicit-linear when nothing is declared (the bundle rule, preserved)
        deps[s.name] = explicit or ([steps[i - 1].name] if i else [])

    # #1787 — nothing may DEPEND on a finalizer. A finalizer runs only once the
    # pipeline is terminal, so a step waiting behind one waits forever; caught
    # here, at plan time, rather than as a pipeline that hangs after its last
    # real leg. (The implicit-linear rule makes this easy to write by accident:
    # a plain step declared after the finalizer inherits it as its dependency.)
    finals = {s.name for s in steps if s.finalizer}
    if finals:
        for sid, ds in deps.items():
            if sid in finals:
                continue
            blocked = sorted(set(ds) & finals)
            if blocked:
                raise PipelineDefError(
                    f"pipeline {d.name!r}: step {sid!r} depends on finalizer "
                    f"step(s) {blocked} — a finalizer runs only after the "
                    f"pipeline is terminal, so nothing downstream of one can "
                    f"ever run. Declare finalizers LAST (or give {sid!r} an "
                    f"explicit `depends_on`)."
                )

    order = _toposort(ids, deps)
    by_id = {s.name: s for s in steps}
    _check_wires(d, steps, by_id, deps)

    return PipelinePlan(
        name=d.name,
        # #1688 — governance rides the plan into the durable record; dropping it
        # here is what left `budget:`/`series:`/`approvers:` decorative.
        governance=dict(d.governance or {}),
        loops=loops,
        loop_tail=loop_tail,
        steps=[
            lower_step(by_id[sid], depends_on=deps[sid], known=by_id)
            for sid in order
        ],
    )


def _registered_pack_name(pipelines_dir: Path) -> Optional[str]:
    """The name of the registered pack whose ``pipelines_dir`` is ``pipelines_dir``.

    The registry already holds every installed and ``CAIRN_PACK_PATH`` pack under its
    manifest name, with its asset dirs resolved from ``[assets]`` — so it knows the
    pack even when the pipelines dir is not ``<pack root>/pipelines``.
    """
    try:
        want = pipelines_dir.resolve()
    except OSError:
        return None
    for p in get_packs():
        d = getattr(p, "pipelines_dir", None)
        if d and Path(d).resolve() == want:
            return p.name
    return None


def pipeline_pack_name(path: Path | str) -> str:
    """The pack a ``<pipelines dir>/<name>.yaml`` belongs to (#2513): the registered
    pack that owns that pipelines dir, else the ``[pack] name`` of the ``pack.toml``
    at the pack root.

    The directory is never authoritative: an installed wheel carries the pack under
    its import name (``cairn_pack_training``), and a source checkout under a Python
    package name (``dataset_readiness`` for ``dataset-readiness``), so a ref read off
    the directory depended on how the pack was installed. The manifest name is the
    one spelling in every layout (#2527); rows recorded with the directory spelling
    are rewritten once by ``cairn pipeline respell-packs``. The registry is asked
    first because a manifest may place ``pipelines`` anywhere under the pack
    (``[assets] pipelines = ...``), where the grandparent is not the pack root. Only
    an unregistered, manifest-less directory (a scratch file, a test fixture) still
    names its pack by directory.
    """
    p = Path(path)
    registered = _registered_pack_name(p.parent)
    if registered is not None:
        return registered
    root = p.parent.parent
    manifest = find_manifest(root)
    return root.name if manifest is None else load_manifest(manifest).name


def load_pipeline_def(path: Path | str) -> PipelineDef:
    """Load ``<pack>/pipelines/<name>.yaml``; the pack is named by its ``pack.toml``
    (:func:`pipeline_pack_name`).

    The filename MUST equal the ``name:`` field. Without that rule, copying
    ``e2e.yaml`` to ``nightly.yaml`` and forgetting to edit ``name:`` yields two
    pipelines both called ``e2e`` — one silently shadowing the other, with the
    ref resolving to whichever the directory listed first.
    """
    p = Path(path)
    pack = pipeline_pack_name(p)
    d = parse_pipeline_def(p.read_text(encoding="utf-8"), pack=pack, source=str(p))
    if d.name != p.stem:
        raise PipelineDefError(
            f"{p}: pipeline is named {d.name!r} but the file is {p.stem!r}.yaml — "
            f"they must match, or two pipelines can silently share a ref. Rename "
            f"the file to {d.name}.yaml, or set `name: {p.stem}`."
        )
    return d


PipelineStep.model_rebuild()

__all__ = [
    "LEG_KINDS", "PipelineDef", "PipelineDefError", "PipelineStep",
    "load_pipeline_def", "lower_step", "parse_pipeline_def", "pipeline_pack_name",
    "plan_pipeline",
]
