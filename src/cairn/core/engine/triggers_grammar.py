"""``triggers:`` in a flow's front matter — how a workflow declares its own
starts (ADR-0101 D1, #1973).

A flow says how it runs in the same file that says what it does::

    triggers:
      - kind: manual                      # implicit; declaring it is a no-op
      - kind: schedule
        cron: "0 3 * * *"
        misfire: skip
      - kind: webhook
        source: pagerduty                 # a receiver preset (cairn.core.engine.triggers_presets)
        match: {event_type: [incident.triggered]}
      - kind: watch                       # metric shape: a threshold on a number
        signal: dataset.rows
        when: "> 1000"
        propose: true
      - kind: watch                       # source shape (ADR-0101 D4): ingestion
        source: {kind: s3, uri: "s3://drop-zone/tickets/"}
        every: "*/15 * * * *"
        register_as: incoming-tickets
        on_change: propose                # propose (default) | start | none

This module owns three things and no more:

**The grammar** — one pydantic model per kind, validated where ``input_schema``
is (``TemplateSpec``), so a bad cron or an unknown receiver fails the COMPILE
with a one-line "where + why" instead of surfacing as a trigger that reads as
wired and never fires (the #1543 lesson).

**Materialisation** — the declaration is written into the stores that already
exist and already fire: ``StoredSchedule`` (the apscheduler-backed cron store,
#354) and ``StoredTrigger`` (webhook layer ③, #309). There is NO new store and
NO new scheduler; per ADR-0101 the stores are the *materialisation* of the
declaration and the front matter wins on conflict.

**The declared/hand-made line** — every materialised row carries
``declared_by: <workflow ref>``. A row without it was made by hand in
Settings › Triggers and is NEVER touched here; a row with a *different* ref is
another workflow's and is likewise left alone. Both stores persist a JSON
``data`` blob, so the field costs no migration.

Convergent, not additive — the same shape as ``pipeline_triggers.materialize``
(#1159), whose deterministic-id rule this module reuses: re-enabling an edited
flow updates its rows in place and drops the ones it no longer declares, so a
re-enable can't silently double a firing rate.

Two deliberate limits, both visible rather than silent:

* A ``kind: watch`` with a ``signal:`` (the METRIC shape) is parsed and
  validated but materialises nothing. Its signal names a number, not a place to
  read one from, so there is nothing for a cadence to poll; giving it one here
  would be inventing policy the author did not declare. The console shows it as
  *declared, not armed*. A watch with a ``source:`` and an ``every:`` IS armed —
  see below.
* A materialised schedule is created **paused** unless the flow says
  ``enabled: true``. Committing to unattended spend is its own click (#1568),
  and enabling a pack must not start a nightly bill. After the row exists the
  operator owns the switch: a re-sync updates the cron and inputs but never
  re-flips ``enabled``.

**Ingestion is a trigger kind** (ADR-0101 D4, #1976). A watch with a
``source:`` — a registered dataset, an ``s3://`` prefix, or a table through the
ingestion pack — materialises the same way everything else here does: as a
:class:`StoredSchedule` whose cron is the declared ``every:`` and whose
workflow is the ``dataset-readiness/watch-source`` PIPELINE. No watch store, no
watch poller, no second scheduler: the fire path, the misfire policy, the
catch-up pass, the pause switch and the fire-time input validation are the ones
already shipped, and a watch is visible in Settings › Triggers because it is
literally a schedule row. Its "last seen" marker rides that row's JSON ``data``
blob (``StoredSchedule.watch_state``), so the marker is deleted with the watch
and costs no migration.

Moved here from ``cairn.core.triggers.declared`` (#2416, ADR-0120 D6): the grammar is
compiled with the flow, so it lives in the engine, below the HTTP layer; the
old path is a deprecated alias for one release.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import Annotated, Any, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from cairn.core.engine.triggers_presets import PRESET_SOURCES, preset_for
from cairn.core.engine.triggers_spec import ingress_path_problem, load_trigger_spec_text
from cairn.core.refs import same_ref
from cairn.core.workspace import DEFAULT_WORKSPACE

logger = logging.getLogger(__name__)

#: What ``kind:`` may say. ``manual`` is implicit on every workflow — the
#: "Run now" button — so declaring it is allowed and does nothing.
TRIGGER_KINDS = ("manual", "schedule", "webhook", "watch", "event")

#: Kinds that ALWAYS materialise into a store that actually fires. ``watch`` is
#: conditional — a source watch arms, a metric watch is NAMED as unarmed rather
#: than dropped (see the module docstring) — so ask :func:`is_armed`, which is
#: what the console route does.
MATERIALISED_KINDS = ("schedule", "webhook")

MISFIRE_POLICIES = ("skip", "fire_once", "fire_all")

#: What marks a ref as a PIPELINE rather than a template (ADR-0084 D0). Spelled
#: out here rather than imported from ``pipeline_resolve.PIPELINE_PREFIX``: this
#: module is on the compiler's import path and must not drag the pipeline model
#: onto it. ``tests/core/test_pipeline_start_model.py`` asserts the two agree.
PIPELINE_REF_PREFIX = "pipeline:"

#: ``when: "> 1000"`` → the comparator the watch semantics already use.
_WHEN_RE = re.compile(r"^\s*(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")
_WHEN_COMPARATOR = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}


class DeclaredTriggerError(ValueError):
    """A ``triggers:`` entry is not a valid declaration.

    Raised with a one-line message naming the entry's index and what is wrong,
    so the compiler's error reads like every other front-matter failure.
    """


# ─── the grammar ─────────────────────────────────────────────────────


class ManualTrigger(BaseModel):
    """``kind: manual`` — the Run button. Always present; declaring it is a
    no-op that documents intent (n8n's manual trigger node, made explicit)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["manual"] = "manual"
    description: str = ""


class ScheduleTrigger(BaseModel):
    """``kind: schedule`` — a cron. Mirrors :class:`StoredSchedule` field for
    field so materialisation is a copy, not a translation."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["schedule"]
    #: 5-field crontab expression, validated with the SAME
    #: ``CronTrigger.from_crontab`` the schedule route and the scheduler use.
    cron: str
    #: What to do with fires that fell inside downtime (ADR-0086 H4, #1246).
    misfire: Literal["skip", "fire_once", "fire_all"] = "skip"
    inputs: dict[str, Any] = Field(default_factory=dict)
    environment: str = ""
    config_preset: str = ""
    description: str = ""
    #: Seeds the row's state on FIRST materialisation only. Default False:
    #: enabling a workflow must not start unattended spend (#1568).
    enabled: bool = False


class WebhookTrigger(BaseModel):
    """``kind: webhook`` — an HTTP receiver seeded from a vendor preset.

    ``source`` names a preset in :mod:`cairn.core.engine.triggers_presets` (the same
    list the console's trigger form offers). A source outside that list is a
    custom receiver and must bring its own ``path:``, because the preset is
    what supplies the ingress path, signature scheme and normalize mapping.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["webhook"]
    source: str
    #: Ingress path. Empty ⇒ the preset's. Required for a custom source.
    path: str = ""
    #: normalized-field → allowed value(s); an event runs the flow only if
    #: every key matches. A bare scalar is sugar for a one-element list.
    match: dict[str, list[str]] = Field(default_factory=dict)
    #: flow-input field → ``$event.<normalized field>`` (or a literal), the
    #: same mapping the form's "Input mapping" box writes.
    input: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    enabled: bool = True

    @field_validator("match", mode="before")
    @classmethod
    def _scalar_match_sugar(cls, v: Any) -> Any:
        """``match: {event_type: incident.triggered}`` is sugar for a one-element
        list — the same sugar ``TriggerSpec``'s own ``match`` parser allows, so a
        declaration reads like the YAML it materialises into."""
        if not isinstance(v, dict):
            return v
        return {
            k: ([str(x) for x in val] if isinstance(val, (list, tuple)) else [str(val)])
            for k, val in v.items()
        }


class WatchSource(BaseModel):
    """``source:`` on a watch — WHAT is watched (ADR-0101 D4, #1976).

    Three kinds, each naming a place cairn can cheaply ask "is there anything
    new?" without reading the data itself:

    * ``dataset`` — a registered dataset gained versions. Already admitted
      bytes, so a change proposes/starts downstream work and imports nothing.
    * ``s3`` — objects added or changed under a prefix, compared on
      key + etag + size. Metadata only: a check never downloads.
    * ``table`` — rows past the stored watermark, through ``ingest/pull``'s
      OWN ``cursor_field``/``since`` semantics rather than a second mechanism.

    Fields are per-kind and the unused ones must be absent, so a declaration
    cannot half-say two things (validated in :func:`_check_watch_source`).
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["dataset", "s3", "table"]
    #: ``dataset`` — the registry name being watched.
    ref: str = ""
    #: ``s3`` — ``s3://bucket/prefix/``. A prefix, not an object.
    uri: str = ""
    #: ``table`` — a Connectors entry NAME (its DSN resolves at run time), or
    #: ``dsn_env`` for a deployment that keeps DSNs in the engine's environment.
    connection: str = ""
    dsn_env: str = ""
    #: ``table`` — the table to read, optionally schema-qualified.
    table: str = ""
    #: ``table`` — the monotonic column that makes the watch resumable. This is
    #: ``ingest/pull``'s ``cursor_field``; the watch stores the high-water mark.
    cursor: str = ""


#: What a detected change does. ``propose`` (the default) files the SAME
#: human-gated ``monitor/propose`` card today's watch files. ``start`` is the
#: ONE spelling that authorises unattended spend, and it has to be typed.
#: ``none`` imports and stops — useful when the registration IS the point.
WATCH_ON_CHANGE = ("propose", "start", "none")


class WatchTrigger(BaseModel):
    """``kind: watch`` — something moved (#992, ADR-0076 D4; ADR-0101 D4).

    Two shapes share the kind, and exactly one of them must be declared:

    * **metric** — ``signal:`` + ``when: "> 1000"``. The shipped console watch:
      a threshold on a named number. Still parsed, still NOT armed — a bare
      signal name says nothing about where to read the number, so there is
      nothing for a cadence to poll (see the module docstring).
    * **source** — ``source:`` + ``every:`` (a cron). ADR-0101 D4's ingestion
      trigger, and it IS armed: it materialises into a :class:`StoredSchedule`
      that fires the ``dataset-readiness/watch-source`` pipeline, which detects
      the change, runs the governed import and then proposes or starts.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["watch"]
    #: metric shape — a human name for the number being watched.
    signal: str = ""
    #: metric shape — a comparator and a number, e.g. ``"> 1000"``.
    when: str = ""
    #: True (the default): a crossing FILES a proposal for a human to approve.
    #: False = record the crossing and propose nothing. There is no third
    #: option — a metric crossing never launches a run on its own (#992,
    #: ADR-0076 D4). A SOURCE watch says ``on_change:`` instead.
    propose: bool = True

    # ── the source shape (ADR-0101 D4, #1976) ───────────────────────────────
    #: WHAT is watched. Mutually exclusive with ``signal:``/``when:``.
    source: Optional[WatchSource] = None
    #: How often to look. Required for a source watch and validated as a real
    #: 5-field cron, because the watch materialises into the cron store that
    #: already fires — there is no second scheduler and no poller (ADR-0101:
    #: "a watch is expressible as a cron schedule that fires a watch pipeline").
    every: str = ""
    #: Dataset-registry name the imported version lands under. Required for
    #: ``s3``/``table`` (they bring new bytes); a ``dataset`` source defaults to
    #: the dataset it watches, which is already registered. Named
    #: ``register_as`` rather than ``register`` because a pydantic field called
    #: ``register`` shadows a BaseModel attribute and warns on every import.
    register_as: str = ""
    #: Optional eval set for the import's decontamination leg (empty = skip).
    eval_path: str = ""
    #: What a detected change does AFTER the import. ``propose`` by default —
    #: the never-auto-spend rule. See :data:`WATCH_ON_CHANGE`.
    on_change: Literal["propose", "start", "none"] = "propose"
    #: Extra inputs for the proposed/started run. ``dataset`` is filled in with
    #: the ref of the version this watch just registered unless the author
    #: supplies their own.
    inputs: dict[str, Any] = Field(default_factory=dict)
    #: Shown on the approval card, and stamped on a started run so the
    #: authorisation and the spend can be compared.
    est_cost_usd: float = 0.0
    #: Seeds the row's state on FIRST materialisation only, exactly like a
    #: declared schedule: enabling a pack must not start unattended polling of
    #: someone's production database (#1568).
    enabled: bool = False
    description: str = ""

    @property
    def is_source_watch(self) -> bool:
        return self.source is not None

    @property
    def comparator(self) -> str:
        """``gt`` | ``gte`` | ``lt`` | ``lte`` — the watch form's vocabulary."""
        m = _WHEN_RE.match(self.when)
        return _WHEN_COMPARATOR[m.group(1)] if m else ""

    @property
    def threshold(self) -> float:
        m = _WHEN_RE.match(self.when)
        return float(m.group(2)) if m else 0.0


class EventTrigger(BaseModel):
    """``kind: event`` (ADR-0131 D4) — start this pipeline when ``topic`` is
    published on the bus (a leg's ``emits:``, #2605, or any domain publisher).

    ``input`` maps a pipeline input to ``$event.<path>`` into the event's payload
    (or a literal), exactly like a webhook's. Paused until enabled, like a
    schedule (#1568): an event must not start unattended work on install."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["event"]
    topic: str
    input: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    enabled: bool = False

    @field_validator("topic")
    @classmethod
    def _domain_topic(cls, v: str) -> str:
        v = (v or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", v):
            raise ValueError(f"topic {v!r}: a domain topic is letters, digits and `._:-`")
        if v.startswith("run."):
            raise ValueError(f"topic {v!r}: `run.*` is the engine's own lifecycle firehose")
        return v


def event_spec_yaml(decl: EventTrigger, *, workflow_ref: str) -> str:
    """The trigger-store record for a declared event (ADR-0131 D4): a ``bus``
    ingress on the topic, ``run.template`` pointed at the declaring pipeline.
    Validated by the same strict loader as a webhook's before it is stored."""
    spec: dict[str, Any] = {
        "source": "event",
        "ingress": {"transport": "bus", "topic": decl.topic},
        "normalize": {},
        "run": {"template": workflow_ref, "input": dict(decl.input)},
    }
    if decl.description:
        spec["description"] = decl.description
    return yaml.safe_dump(spec, sort_keys=False)


DeclaredTrigger = Annotated[
    Union[ManualTrigger, ScheduleTrigger, WebhookTrigger, WatchTrigger, EventTrigger],
    Field(discriminator="kind"),
]


# ─── parse + validate ────────────────────────────────────────────────


def parse_declared_triggers(raw: Any, *, where: str = "triggers") -> list[Any]:
    """Validate a front-matter ``triggers:`` value into typed declarations.

    Every failure is a :class:`DeclaredTriggerError` naming the entry and the
    reason. Called from ``TemplateSpec``'s validator, so this runs on the same
    seam ``input_schema`` is checked on — one place, every reader.
    """
    if raw is None or raw == []:
        return []
    if not isinstance(raw, list):
        raise DeclaredTriggerError(
            f"{where}: must be a list of trigger declarations, got "
            f"{type(raw).__name__}"
        )
    out: list[Any] = []
    for i, entry in enumerate(raw):
        site = f"{where}[{i}]"
        if isinstance(entry, (ManualTrigger, ScheduleTrigger, WebhookTrigger, WatchTrigger, EventTrigger)):
            out.append(entry)  # already parsed (a re-validation pass)
            continue
        if not isinstance(entry, dict):
            raise DeclaredTriggerError(
                f"{site}: must be a mapping with a `kind:`, got {type(entry).__name__}"
            )
        kind = str(entry.get("kind") or "").strip()
        if not kind:
            raise DeclaredTriggerError(
                f"{site}: missing `kind:` — expected one of {', '.join(TRIGGER_KINDS)}"
            )
        if kind not in TRIGGER_KINDS:
            raise DeclaredTriggerError(
                f"{site}: unknown trigger kind {kind!r} — expected one of "
                f"{', '.join(TRIGGER_KINDS)}"
            )
        model = {
            "manual": ManualTrigger, "schedule": ScheduleTrigger,
            "webhook": WebhookTrigger, "watch": WatchTrigger, "event": EventTrigger,
        }[kind]
        try:
            decl = model.model_validate(entry)
        except Exception as e:  # noqa: BLE001 — pydantic's detail is the message
            raise DeclaredTriggerError(f"{site} (kind: {kind}): {e}") from None
        _check_semantics(decl, site=site)
        out.append(decl)
    return out


def _check_semantics(decl: Any, *, site: str) -> None:
    """The checks pydantic types can't express: a real cron, a known receiver,
    a parseable threshold. Each one is the same check the surface that fires
    the trigger already makes, so a declaration cannot pass here and fail there."""
    if isinstance(decl, ScheduleTrigger):
        cron = (decl.cron or "").strip()
        if not cron:
            raise DeclaredTriggerError(f"{site}: `cron:` is required for a schedule trigger")
        # apscheduler is a base dep; the schedules router imports it the same
        # lazy way (heavy-ish, and only the cron path needs it).
        from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415

        try:
            CronTrigger.from_crontab(cron)
        except Exception as e:  # noqa: BLE001 — apscheduler's message names the field
            raise DeclaredTriggerError(
                f"{site}: {cron!r} is not a valid 5-field cron — {e}"
            ) from None
    elif isinstance(decl, WebhookTrigger):
        source = (decl.source or "").strip()
        if not source:
            raise DeclaredTriggerError(f"{site}: `source:` is required for a webhook trigger")
        if preset_for(source) is None and not (decl.path or "").strip():
            raise DeclaredTriggerError(
                f"{site}: {source!r} is not a known receiver preset "
                f"({', '.join(PRESET_SOURCES)}) — name one of those, or give an "
                f"explicit `path:` for a custom source"
            )
        if (decl.path or "").strip():
            # #206 — the same rule the spec loader applies, said at the declaration.
            problem = ingress_path_problem(decl.path.strip())
            if problem:
                raise DeclaredTriggerError(f"{site}: `path:` — {problem}")
    elif isinstance(decl, WatchTrigger):
        _check_watch(decl, site=site)


def _check_watch(decl: "WatchTrigger", *, site: str) -> None:
    """A watch declares a METRIC or a SOURCE, never both and never neither.

    The two shapes want different things and arm differently, so the split is
    checked once, here, and named in the message: a declaration that mixes them
    would otherwise materialise as whichever branch happened to read first.
    """
    metric_fields = [f for f in ("signal", "when") if (getattr(decl, f) or "").strip()]
    if decl.source is not None and metric_fields:
        raise DeclaredTriggerError(
            f"{site}: a watch declares `source:` (what is watched) OR "
            f"`signal:`/`when:` (a threshold on a number), not both — drop "
            f"{', '.join('`' + f + ':`' for f in metric_fields)} or drop `source:`"
        )
    if decl.source is None:
        if not metric_fields:
            raise DeclaredTriggerError(
                f"{site}: a watch needs `source:` (a dataset, an s3 prefix or a "
                f"table) or `signal:` + `when:` (a threshold on a number)"
            )
        if not (decl.signal or "").strip():
            raise DeclaredTriggerError(f"{site}: `signal:` is required for a metric watch")
        if not _WHEN_RE.match(decl.when or ""):
            raise DeclaredTriggerError(
                f"{site}: `when:` must be a comparator and a number, e.g. \"> 1000\" "
                f"(got {decl.when!r})"
            )
        # The source-only fields are meaningless without a source, and a
        # declaration that carries them reads as armed while nothing polls it.
        stray = [f for f in ("every", "register_as", "eval_path", "on_change", "inputs",
                             "est_cost_usd", "enabled") if f in decl.model_fields_set]
        if stray:
            raise DeclaredTriggerError(
                f"{site}: {', '.join('`' + f + ':`' for f in stray)} "
                f"{'belongs' if len(stray) == 1 else 'belong'} to a SOURCE watch — a "
                f"metric watch has nothing to poll (its `signal:` names a number, not a "
                f"place to read one from), so it is validated here and armed by nobody"
            )
        return
    _check_watch_source(decl, site=site)


#: Per-kind: the fields a source MUST carry, and the ones it must NOT.
_SOURCE_FIELDS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "dataset": (("ref",), ("uri", "connection", "dsn_env", "table", "cursor")),
    "s3": (("uri",), ("ref", "connection", "dsn_env", "table", "cursor")),
    "table": (("table", "cursor"), ("ref", "uri")),
}


def _check_watch_source(decl: "WatchTrigger", *, site: str) -> None:
    """The source shape: a cadence, the per-kind fields, and somewhere to land.

    Every check here is one the fire path would otherwise make far too late —
    a watch whose cron does not parse or whose s3 uri is a bucket name is a
    trigger that reads as wired and never fires (#1543).
    """
    src = decl.source
    assert src is not None  # _check_watch's branch guarantees it
    required, forbidden = _SOURCE_FIELDS[src.kind]
    missing = [f for f in required if not (getattr(src, f) or "").strip()]
    if missing:
        raise DeclaredTriggerError(
            f"{site}: a `source: {{kind: {src.kind}}}` needs "
            f"{', '.join('`' + f + ':`' for f in missing)}"
        )
    extra = [f for f in forbidden if (getattr(src, f) or "").strip()]
    if extra:
        raise DeclaredTriggerError(
            f"{site}: {', '.join('`' + f + ':`' for f in extra)} "
            f"{'is' if len(extra) == 1 else 'are'} not part of a "
            f"`source: {{kind: {src.kind}}}` — remove "
            f"{'it' if len(extra) == 1 else 'them'} so the record says one thing"
        )
    if src.kind == "table" and not (src.connection or "").strip() and not (src.dsn_env or "").strip():
        raise DeclaredTriggerError(
            f"{site}: a table source needs `connection:` (a Connectors entry) or "
            f"`dsn_env:` (an env var NAME). A literal DSN is deliberately not "
            f"accepted — the flow document is a committed, hashed record"
        )
    if src.kind == "table" and (src.connection or "").strip() and (src.dsn_env or "").strip():
        raise DeclaredTriggerError(
            f"{site}: declare `connection:` OR `dsn_env:`, not both — two credential "
            f"sources means the record does not say which one ran"
        )
    if src.kind == "s3" and not src.uri.strip().startswith("s3://"):
        raise DeclaredTriggerError(
            f"{site}: an s3 source's `uri:` must be an `s3://bucket/prefix/` URI "
            f"(got {src.uri!r})"
        )
    cron = (decl.every or "").strip()
    if not cron:
        raise DeclaredTriggerError(
            f"{site}: a source watch needs `every:` — a 5-field cron saying how often "
            f"to look. There is no poller to inherit a cadence from: the watch "
            f"materialises into the cron store that already fires (ADR-0101 D4)"
        )
    # The SAME check the schedule route and the scheduler make, so a declaration
    # cannot pass here and fail there.
    from apscheduler.triggers.cron import CronTrigger  # noqa: PLC0415

    try:
        CronTrigger.from_crontab(cron)
    except Exception as e:  # noqa: BLE001 — apscheduler's message names the field
        raise DeclaredTriggerError(
            f"{site}: `every: {cron!r}` is not a valid 5-field cron — {e}"
        ) from None
    if src.kind != "dataset" and not (decl.register_as or "").strip():
        raise DeclaredTriggerError(
            f"{site}: `register_as:` is required for a {src.kind} source — new bytes are "
            f"admitted and versioned under a dataset NAME, and the watch will not "
            f"invent one"
        )
    if "propose" in decl.model_fields_set:
        raise DeclaredTriggerError(
            f"{site}: `propose:` is the METRIC watch's field. A source watch says "
            f"`on_change:` — propose (the default: file a human-gated proposal), "
            f"start (the explicit opt-in to unattended spend) or none (import and "
            f"stop). Two fields answering one question is how a `start` becomes a "
            f"`propose` nobody notices"
        )


# ─── materialisation ─────────────────────────────────────────────────


def is_armed(decl: Any) -> bool:
    """Does this declaration materialise into a store that actually fires?

    One question, one answer, one place — the console route, the Triggers tab
    and the tests all read it here rather than each re-deriving "watch, but only
    when it has a source". A declaration that reads as wired while nothing fires
    it is the failure mode #1543 named.
    """
    if isinstance(decl, WatchTrigger):
        return decl.is_source_watch
    return getattr(decl, "kind", "") in MATERIALISED_KINDS


def declared_id(ref: str, kind: str, index: int) -> str:
    """Deterministic store id: same workflow + same kind + same slot ⇒ same row.

    This is what makes a re-enable converge instead of duplicating (the
    ``pipeline_triggers.schedule_id_for`` rule, one grain over).
    """
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", (ref or "").strip().lower()).strip("-")
    return f"declared--{slug}--{kind}--{index}"


def declared_prefix(ref: str) -> str:
    """The id prefix every row this workflow owns starts with."""
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", (ref or "").strip().lower()).strip("-")
    return f"declared--{slug}--"


def webhook_spec_yaml(decl: WebhookTrigger, *, workflow_ref: str) -> str:
    """The trigger-spec YAML for a declared webhook — the record the store keeps.

    Field for field what the console's ``TriggerForm.buildYaml`` writes from the
    same preset (source · description · ingress transport/path/method/signature ·
    normalize · match · run.template + run.input), with ``run.template`` pointed
    at the declaring workflow. Emitted through ``yaml.safe_dump`` rather than the
    form's line builder because a hand-built string cannot safely quote a secret
    reference; the text is validated by ``load_trigger_spec_text`` before it is
    ever stored, exactly like the form's.
    """
    preset = preset_for(decl.source) or preset_for("custom") or {}
    path = (decl.path or "").strip() or str(preset.get("path") or f"/triggers/{decl.source}")
    spec: dict[str, Any] = {
        "source": decl.source,
        "ingress": {
            "transport": "http",
            "path": path,
            "method": str(preset.get("method") or "POST"),
        },
        # `normalize` is REQUIRED by the spec loader; the preset's starter
        # mapping is what the form seeds too.
        "normalize": dict(preset.get("normalize") or {"event_type": "type"}),
    }
    if decl.description:
        spec["description"] = decl.description
    sig = preset.get("signature") or {}
    if sig.get("header") and sig.get("secret"):
        spec["ingress"]["signature"] = {
            "header": sig["header"], "secret": sig["secret"],
            "scheme": sig.get("scheme", "hmac_sha256"),
            "required": bool(sig.get("required", False)),
        }
    if decl.match:
        spec["match"] = {k: list(v) for k, v in decl.match.items()}
    run: dict[str, Any] = {"template": workflow_ref}
    if decl.input:
        run["input"] = dict(decl.input)
    spec["run"] = run
    return yaml.safe_dump(spec, sort_keys=False, default_flow_style=False)


#: The pipeline a source watch fires (ADR-0101 D4). It is a PIPELINE, not a
#: flow, because the governed import it must run — normalize · dedup ·
#: decontaminate · PII redact · register — is the `clean` FLOW, and only a
#: pipeline can compose a flow as a leg. Wiring a second copy of that admission
#: chain into a template is exactly what `pull-and-admit.yaml` refused to do.
#:
#: The pack segment is the pack's ``pack.toml`` name (``dataset-readiness``), the one
#: spelling every install records since #2527; rows written with the directory spelling
#: are respelled by ``cairn pipeline respell-packs`` and still resolve through ``ref_key``.
#: The console's own import launch (``DatasetsView``) spells it the same way.
WATCH_PIPELINE_REF = "pipeline:dataset-readiness/watch-source"

#: ADR-0131 D3 — the reserved input key a CORE dataset watch rides its schedule
#: row under. The fire path (``dispatch/scheduler.py``) strips it before launch.
CORE_WATCH_KEY = "$watch"



def core_watch_unarmed_reason(decl: "WatchTrigger") -> str:
    """Why a PIPELINE's source watch is not armed, or ``""`` when core arms it
    (ADR-0131 D3). Only a ``dataset`` watch with ``on_change: start|none`` is: the
    governed import and the proposal chain behind ``s3``/``table`` and
    ``propose`` (the platform's ``watch-source``) start flows, not pipelines."""
    src = decl.source
    if src is None or src.kind != "dataset":
        return (f"a {getattr(src, 'kind', '?')!r} watch on a pipeline is not armed — "
                f"only a `dataset` watch is (ADR-0131 D3)")
    if decl.on_change == "propose":
        return ("`on_change: propose` on a pipeline is not armed — use `start` (the "
                "pipeline's own gates still apply) or `none` (ADR-0131 D3)")
    return ""


def core_watch_inputs(decl: "WatchTrigger") -> dict[str, Any]:
    """A core ``dataset`` watch's row inputs (ADR-0131 D3): what the declaring
    pipeline starts with, plus the watch itself under :data:`CORE_WATCH_KEY`."""
    src = decl.source
    assert src is not None and src.kind == "dataset"
    register = (decl.register_as or "").strip() or src.ref
    start: dict[str, Any] = {"dataset": f"dataset:{register}@latest"}
    start.update(decl.inputs or {})
    start[CORE_WATCH_KEY] = {"source": "dataset", "dataset": src.ref,
                             "on_change": decl.on_change}
    return start


def watch_name(ref: str, decl: "WatchTrigger") -> str:
    """A row name a human can read in Settings › Triggers without opening it."""
    src = decl.source
    what = {
        "dataset": lambda: f"dataset {src.ref}",
        "s3": lambda: src.uri,
        "table": lambda: f"{src.connection or src.dsn_env}:{src.table}",
    }[src.kind]()
    return f"{ref} ← watch {what}"


def watch_pipeline_inputs(
    decl: "WatchTrigger", *, workflow_ref: str, watch_id: str,
) -> dict[str, Any]:
    """The stored schedule's inputs — everything ``watch-source`` needs to run.

    Flat and fully resolved on purpose. The row is what the scheduler validates
    at fire time and what a reader sees in the console, so "which table, landing
    where, starting what, and may it spend" must be answerable from the row
    without reading the flow's front matter back.

    ``watch_id`` is the row's own id: the check reads and advances its marker on
    that row (``StoredSchedule.watch_state``), which is why the watch needs no
    store of its own and why deleting the watch deletes its memory with it.
    """
    src = decl.source
    assert src is not None
    register = (decl.register_as or "").strip() or (src.ref if src.kind == "dataset" else "")
    # The version this check is about to register. `@latest` rather than a
    # number because the number does not exist until the import lands, and
    # `latest` at dispatch time IS that version (artifacts.parse_dataset_ref).
    start_inputs: dict[str, Any] = {}
    if register:
        start_inputs["dataset"] = f"dataset:{register}@latest"
    start_inputs.update(decl.inputs or {})
    return {
        "watch_id": watch_id,
        "source_kind": src.kind,
        "dataset": src.ref,
        "uri": src.uri,
        "connection": src.connection,
        "dsn_env": src.dsn_env,
        "table": src.table,
        "cursor": src.cursor,
        "register": register,
        "eval_path": decl.eval_path,
        # The declaring workflow is what a change proposes or starts — the
        # trigger belongs to the workflow (D1), so the workflow is the subject.
        "start_workflow": workflow_ref,
        "start_inputs": start_inputs,
        # ── the never-auto-spend switch ─────────────────────────────────────
        # True ONLY for `on_change: start`. The pipeline's paid leg is routed
        # off unless this is true, and the default declaration cannot produce
        # it: `on_change` defaults to `propose`, which files a gated proposal.
        "autostart": decl.on_change == "start",
        "notify": decl.on_change != "none",
        "est_cost_usd": float(decl.est_cost_usd or 0.0),
    }


class MaterialiseResult(BaseModel):
    """What one materialisation pass did — the log line and the test's assertion."""

    created: list[str] = Field(default_factory=list)
    updated: list[str] = Field(default_factory=list)
    unchanged: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    #: Declared kinds this pass could not arm (today: ``watch``).
    unarmed: list[str] = Field(default_factory=list)

    @property
    def touched(self) -> int:
        return len(self.created) + len(self.updated) + len(self.removed)


async def materialise_declared_triggers(
    ref: str,
    triggers: list[Any],
    *,
    workspace_id: str = DEFAULT_WORKSPACE,
    schedule_store: Any | None = None,
    trigger_store: Any | None = None,
    id_for: Optional[Callable[[str, int], str]] = None,
    owned_prefixes: tuple[str, ...] = (),
) -> MaterialiseResult:
    """Write *ref*'s declared triggers into the existing stores for one workspace.

    Idempotent and convergent:

    * a row that does not exist is created (a schedule paused unless the flow
      says ``enabled: true``);
    * a row this workflow already owns is updated when the declaration changed
      and left alone when it did not — ``enabled``, ``last_fired_at`` and
      ``created_at`` are the operator's / the runtime's and survive the update;
    * a row this workflow used to own but no longer declares is deleted, so
      removing a ``triggers:`` entry actually stops it firing;
    * a row with no ``declared_by``, or another workflow's, is NEVER touched —
      what a user made by hand in Settings stays theirs.

    ``id_for`` and ``owned_prefixes`` exist for ONE caller: pipelines (ADR-0101
    S7, #1987). A pipeline's ``on:`` block has been materialising into this same
    schedule store since #1159, under ids of its own
    (``pipeline--<pack>--<name>--<i>``). Re-keying those rows to
    :func:`declared_id` would either duplicate a live cron or reset an
    operator's pause, so the pipeline path keeps its ids
    (``id_for``) and tells this function which extra id prefixes it owns
    (``owned_prefixes``) — for the stale sweep, and to ADOPT a pre-S7 row whose
    ``declared_by`` is empty because it was written before the field existed.
    Both default to the workflow behaviour and change nothing for a flow.
    """
    # lazy: both stores open a DB handle on construction, and this module is on
    # the compiler's import path via TemplateSpec (cycle-free but not free).
    from cairn.core.storage.schedule_store import StoredSchedule  # noqa: PLC0415
    from cairn.core.storage.trigger_store import StoredTrigger  # noqa: PLC0415

    ref = (ref or "").strip()
    res = MaterialiseResult()
    if not ref:
        return res

    sched_plan: dict[str, Any] = {}
    hook_plan: dict[str, Any] = {}
    rid_of = id_for or (lambda kind, i: declared_id(ref, kind, i))
    for i, decl in enumerate(triggers or []):
        if isinstance(decl, ScheduleTrigger):
            rid = rid_of("schedule", i)
            sched_plan[rid] = StoredSchedule(
                id=rid, workspace_id=workspace_id,
                name=decl.description or f"{ref} @ {decl.cron}",
                cron=decl.cron.strip(), workflow=ref, inputs=dict(decl.inputs),
                environment=decl.environment, config_preset=decl.config_preset,
                enabled=decl.enabled, misfire=decl.misfire,
                description=decl.description, declared_by=ref,
            )
        elif isinstance(decl, WebhookTrigger):
            rid = rid_of("webhook", i)
            text = webhook_spec_yaml(decl, workflow_ref=ref)
            # Through the SAME strict loader the console's create route uses:
            # the listing path skips an unparseable stored row silently, so a
            # row that cannot be read back must never be written in the first
            # place.
            try:
                load_trigger_spec_text(text, name=rid)
            except Exception as e:  # noqa: BLE001 — the loader's message is the useful one
                raise DeclaredTriggerError(
                    f"{ref}: the webhook trigger declared for source {decl.source!r} "
                    f"does not build a valid receiver spec — {e}"
                ) from None
            hook_plan[rid] = StoredTrigger(
                id=rid, workspace_id=workspace_id,
                name=f"{ref} ← {decl.source}", source=decl.source,
                yaml=text, enabled=decl.enabled, description=decl.description,
                declared_by=ref,
            )
        elif isinstance(decl, EventTrigger):
            rid = rid_of("event", i)
            text = event_spec_yaml(decl, workflow_ref=ref)
            try:
                load_trigger_spec_text(text, name=rid)
            except Exception as e:  # noqa: BLE001 — the loader's message is the useful one
                raise DeclaredTriggerError(
                    f"{ref}: the event trigger on topic {decl.topic!r} does not build a "
                    f"valid record — {e}") from None
            hook_plan[rid] = StoredTrigger(
                id=rid, workspace_id=workspace_id,
                name=decl.description or f"{ref} ← event {decl.topic}", source="event",
                yaml=text, enabled=decl.enabled, description=decl.description,
                declared_by=ref,
            )
        elif isinstance(decl, WatchTrigger):
            if not decl.is_source_watch:
                res.unarmed.append("watch")
                continue
            rid = rid_of("watch", i)
            if ref.startswith("pipeline:"):
                # ADR-0131 D3 — a PIPELINE's watch is core's: the row starts the
                # declaring pipeline and the fire path probes first. What core
                # cannot arm is named, not armed. A flow's watch keeps ADR-0101 D4.
                why = core_watch_unarmed_reason(decl)
                if why:
                    res.unarmed.append(f"watch ({why})")
                    continue
                sched_plan[rid] = StoredSchedule(
                    id=rid, workspace_id=workspace_id,
                    name=decl.description or watch_name(ref, decl),
                    cron=decl.every.strip(), workflow=ref,
                    inputs=core_watch_inputs(decl),
                    environment="", config_preset="",
                    enabled=decl.enabled, misfire="skip",
                    description=decl.description, declared_by=ref,
                )
                continue
            sched_plan[rid] = StoredSchedule(
                id=rid, workspace_id=workspace_id,
                name=decl.description or watch_name(ref, decl),
                cron=decl.every.strip(), workflow=WATCH_PIPELINE_REF,
                inputs=watch_pipeline_inputs(decl, workflow_ref=ref, watch_id=rid),
                environment="", config_preset="",
                enabled=decl.enabled, misfire="skip",
                description=decl.description, declared_by=ref,
            )

    # Both passes run even when the plan is EMPTY — that is exactly when a row
    # this workflow used to declare has to be removed.
    await _converge(
        plan=sched_plan, ref=ref, workspace_id=workspace_id, res=res,
        store=schedule_store or _default_schedule_store(),
        owns_close=schedule_store is None,
        compare=("cron", "workflow", "inputs", "environment", "config_preset",
                 "misfire", "description", "name", "declared_by"),
        owned_prefixes=owned_prefixes,
    )
    await _converge(
        plan=hook_plan, ref=ref, workspace_id=workspace_id, res=res,
        store=trigger_store or _default_trigger_store(),
        owns_close=trigger_store is None,
        compare=("source", "yaml", "description", "name", "declared_by"),
        owned_prefixes=owned_prefixes,
    )
    if res.touched:
        logger.info(
            "declared triggers for %s in %s: +%d ~%d -%d", ref, workspace_id,
            len(res.created), len(res.updated), len(res.removed),
        )
    return res


def _default_schedule_store() -> Any:
    from cairn.core.storage.schedule_store import default_schedule_store  # noqa: PLC0415

    return default_schedule_store()


def _default_trigger_store() -> Any:
    from cairn.core.storage.trigger_store import default_trigger_store  # noqa: PLC0415

    return default_trigger_store()


def _owns(existing: Any, ref: str, owned_prefixes: tuple[str, ...]) -> bool:
    """Is this stored row *ref*'s to converge?

    Normally: it says so (``declared_by == ref``). The one exception is a row
    an EARLIER cairn wrote under an id only this ref's materialiser can
    generate — a pipeline's ``pipeline--<pack>--<name>--<i>`` schedule from
    #1159, written before ``declared_by`` existed. An empty ``declared_by``
    there means "pre-S7", not "hand-made in Settings", so it is adopted (and
    stamped on the update) instead of being abandoned as an orphan that keeps
    firing next to its replacement. Hand-made rows are unaffected: the console
    slugs a row id from its NAME and never mints one of these prefixes.
    """
    by = getattr(existing, "declared_by", "") or ""
    if by == ref or (by and same_ref(by, ref)):   # a row declared under the pack's former spelling (#2527)
        return True
    return not by and any(str(existing.id).startswith(p) for p in owned_prefixes)


def _http_ingress_path(text: Any) -> str:
    """The HTTP ingress path a stored trigger row's YAML declares, else ``""``
    (a schedule row, an event row, or text that does not parse)."""
    if not isinstance(text, str) or not text:
        return ""
    try:
        spec = load_trigger_spec_text(text)
    except Exception:  # noqa: BLE001 — an unreadable row claims no path
        return ""
    if getattr(spec.ingress, "transport", "http") != "http":
        return ""
    return str(spec.ingress.path or "")


async def _claimed_elsewhere(store: Any, rid: str, row: Any, workspace_id: str) -> str:
    """Why *row* may not be written for *workspace_id*: its id, its name or its HTTP
    ingress path already belongs to ANOTHER workspace (#213). ``""`` when it may.

    Ids and names are global in every store and ``create`` is an upsert, so writing
    the row would replace the other workspace's row and move it here; a shared
    ingress path would put two workspaces behind one receiver."""
    other = await store.get(rid)
    if other is not None and other.workspace_id != workspace_id:
        return f"its id {rid!r} belongs to workspace {other.workspace_id!r}"
    by_name = getattr(store, "get_by_name", None)
    name = getattr(row, "name", "")
    if callable(by_name) and name:
        other = await by_name(name)
        if other is not None and other.workspace_id != workspace_id:
            return f"its name {name!r} belongs to workspace {other.workspace_id!r}"
    path = _http_ingress_path(getattr(row, "yaml", None))
    if path:
        for other in await store.list(None):
            if other.workspace_id != workspace_id and _http_ingress_path(other.yaml) == path:
                return f"its path {path} belongs to workspace {other.workspace_id!r}"
    return ""


async def _converge(
    *, plan: dict[str, Any], ref: str, workspace_id: str, res: MaterialiseResult,
    store: Any, owns_close: bool, compare: tuple[str, ...],
    owned_prefixes: tuple[str, ...] = (),
) -> None:
    """Drop this workflow's stale rows, then upsert *plan* into *store*.

    The sweep comes FIRST: a row minted under the pack's former spelling (#2527) carries
    the same unique ``name`` as its replacement, and Postgres refuses the create while
    the old row exists. Rows in *plan* are never swept.
    """
    try:
        prefixes = (declared_prefix(ref), *owned_prefixes)
        for existing in await store.list(workspace_id):
            rid = str(existing.id)
            if any(rid.startswith(p) for p in prefixes) and rid not in plan and \
                    _owns(existing, ref, owned_prefixes):
                await store.delete(rid, workspace_id)
                res.removed.append(rid)
        for rid, row in plan.items():
            why = await _claimed_elsewhere(store, rid, row, workspace_id)
            if why:
                # #213 — fail closed: the other workspace keeps its row untouched.
                path = _http_ingress_path(getattr(row, "yaml", None))
                logger.error(
                    "not arming %s for %s in workspace %r: %s. A trigger belongs to one "
                    "workspace; give this one its own path (and name) or disable it there",
                    f"webhook receiver {path}" if path else rid, ref, workspace_id, why,
                )
                res.unarmed.append(f"{rid} ({why})")
                continue
            existing = await store.get(rid, workspace_id)
            if existing is None:
                await store.create(row)
                res.created.append(rid)
                continue
            if not _owns(existing, ref, owned_prefixes):
                # a hand-made row (or another workflow's) squatting this id —
                # never overwrite someone else's record.
                logger.warning(
                    "not materialising %s for %s: the row exists and is not "
                    "declared by this workflow", rid, ref,
                )
                continue
            if all(getattr(existing, f, None) == getattr(row, f, None) for f in compare):
                res.unchanged.append(rid)
                continue
            # the declaration wins on the declared fields; the operator keeps
            # the switch and the runtime keeps its watermarks.
            updates = {f: getattr(row, f) for f in compare}
            if hasattr(store, "update_fields"):
                await store.update_fields(rid, updates, workspace_id)
            else:
                await _replace(store, existing, row, workspace_id)
            res.updated.append(rid)
    finally:
        if owns_close:
            close = getattr(store, "close", None)
            if callable(close):
                close()


async def _replace(store: Any, existing: Any, row: Any, workspace_id: str) -> None:
    """Update path for a store with no ``update_fields`` (the trigger store):
    keep the row's history fields, replace the declared ones. ``create`` is an
    upsert on the primary key in every backend, so this is one write."""
    keep = {"created_at": getattr(existing, "created_at", None),
            "enabled": getattr(existing, "enabled", None)}
    merged = row.model_copy(update={k: v for k, v in keep.items() if v is not None})
    await store.create(merged)


# ─── the two entry points ────────────────────────────────────────────


def declared_triggers_for(ref: str) -> list[Any]:
    """Read one workflow's — or one pipeline's — declared triggers.

    Parse-only: no capability registry, no operator resolution, no LLM client —
    so the console read route and the startup sync stay cheap and cannot be
    broken by an unrelated compile failure.

    A ``pipeline:<pack>/<name>`` ref answers from the pipeline's own record
    (ADR-0101 S7, #1987): both its ``triggers:`` block and its legacy ``on:``
    block, in one list of the same typed declarations. That is what lets the
    console's Triggers read (``GET /console/api/workflows/{ref}/triggers``)
    serve a pipeline through the route it already has, rather than growing a
    second one that would answer the same question differently.
    """
    if ref.startswith(PIPELINE_REF_PREFIX):
        # lazy: pipeline_triggers pulls the pipeline model and pack discovery,
        # and this module sits on the compiler's import path via TemplateSpec.
        from cairn.core.engine.pipeline_triggers import (  # noqa: PLC0415
            declarations_for_ref,
        )

        return declarations_for_ref(ref)
    # lazy: frontmatter is a heavy-ish parse and skill_engine pulls in pack
    # discovery; neither belongs on this module's import path.
    import frontmatter  # noqa: PLC0415

    from cairn.core.engine.skill_engine import resolve_template_path  # noqa: PLC0415

    try:
        path = resolve_template_path(ref)
    except Exception:  # noqa: BLE001 — not a template ref (a pipeline, say)
        return []
    meta = frontmatter.load(path).metadata
    return parse_declared_triggers(meta.get("triggers"), where=f"{path}: triggers")


async def sync_declared_triggers(
    *, workspace_id: Optional[str] = None, store: Any | None = None,
) -> int:
    """Materialise the declared triggers of every ACTIVE workflow — the pass the
    server runs once at start so a flow enabled by an earlier process (or by a
    pack update that changed its cron) is armed as declared.

    ``workspace_id=None`` covers every workspace the registry knows, because a
    schedule is workspace-scoped and a restart must not silently disarm the
    workspaces that are not the default one. Best-effort per workflow: one
    unreadable flow must never stop the server from booting.
    """
    from cairn.core.workflow_registry import get_workflow_registry  # noqa: PLC0415 - cycle

    reg = store or get_workflow_registry()
    try:
        records = await reg.list_installed(workspace_id)
    except Exception:  # noqa: BLE001 — no registry yet ⇒ nothing to arm
        return 0
    n = 0
    for rec in records:
        if getattr(rec, "status", "") != "active":
            continue
        ref = getattr(rec, "workflow_ref", "") or ""
        if not ref or ref.startswith(PIPELINE_REF_PREFIX):
            # a pipeline arms its own declarations at INSTALL, through
            # ``pipeline_triggers.materialise_pipeline_triggers`` (#1159, and
            # ADR-0101 S7 for the `triggers:` spelling) — same materialiser,
            # different door, so running it again from here would be a
            # duplicate pass over rows that are already convergent.
            continue
        try:
            decls = declared_triggers_for(ref)
        except Exception as e:  # noqa: BLE001 — a bad declaration is named, not fatal
            logger.warning("workflow %s declares invalid triggers: %s", ref, e)
            continue
        if not decls:
            continue
        try:
            res = await materialise_declared_triggers(
                ref, decls, workspace_id=getattr(rec, "workspace_id", DEFAULT_WORKSPACE),
            )
            n += res.touched
        except Exception:  # noqa: BLE001 — arming is best-effort, never blocks boot
            logger.exception("could not materialise declared triggers for %s", ref)
    return n


__all__ = [
    "MATERIALISED_KINDS",
    "MISFIRE_POLICIES",
    "PIPELINE_REF_PREFIX",
    "TRIGGER_KINDS",
    "WATCH_ON_CHANGE",
    "WATCH_PIPELINE_REF",
    "CORE_WATCH_KEY",
    "EventTrigger",
    "event_spec_yaml",
    "core_watch_inputs",
    "core_watch_unarmed_reason",
    "DeclaredTrigger",
    "DeclaredTriggerError",
    "ManualTrigger",
    "MaterialiseResult",
    "ScheduleTrigger",
    "WatchSource",
    "WatchTrigger",
    "WebhookTrigger",
    "declared_id",
    "declared_prefix",
    "declared_triggers_for",
    "is_armed",
    "materialise_declared_triggers",
    "parse_declared_triggers",
    "sync_declared_triggers",
    "watch_name",
    "watch_pipeline_inputs",
    "webhook_spec_yaml",
]
