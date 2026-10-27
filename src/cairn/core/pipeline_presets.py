"""Launch presets for PIPELINES (#1789, ADR-0071 §6).

ADR-0071 §6 said workflow inputs and pipeline inputs are the same problem one
layer up and should share one mental model. #793 built half of it — the
:class:`~cairn.core.workflow_config_store.InputPreset` table, its CRUD, and the
Run-form picker — but only ever reachable for a workflow TEMPLATE. A pipeline,
which is the noun ADR-0084 D9 made primary and the one whose input sets are
actually large (the retrain-cycle flagship declares 28), had no way to name
one. So it was launched five times on 2026-08-28 by pasting a 19-key JSON body
into curl, and one fat-fingered field cost a paid GPU cycle.

This module finishes that surface. Three rules it is built on:

**One store, not a second one.** ``InputPreset`` is keyed
``(workspace_id, template_ref, name)``, and ``template_ref`` is just "the thing
the preset belongs to". A pipeline addresses the same table under its CANONICAL
``pipeline:<pack>/<name>`` ref — the spelling the engine already uses to
disambiguate a pipeline from a same-named flow (``PIPELINE_PREFIX``) and the
exact string a launch record stamps as its ``bundle_ref``. The namespaces
therefore cannot collide, and no ``kind`` column, second table, or migration is
needed. :func:`canonical_pipeline_ref` is the one place that normalisation
happens, so ``training/retrain-cycle`` and
``pipeline:training/retrain-cycle`` address the same preset.

**One resolution path.** :func:`resolve_preset` is awaited from inside
``launch_pipeline`` — the single door the CLI, the console and the cron trigger
all converge on. No client ever expands a preset into inputs and posts the
result: a preset resolved on the client is a preset that means something
different depending on who launched, which is the opposite of what provenance is
for. The CLI passes a NAME.

**A pack may SHIP one, as data.** ``presets/*.yaml`` in a pack is a new asset
dir (``all_preset_dirs``) holding named input sets for that pack's own
pipelines — YAML only, no code (the packs-are-data rule). A saved preset with
the same name SHADOWS the pack's, so a deployment can retune the seed without
editing an installed wheel, and deleting the saved one falls back to the shipped
default. Pack presets are read-only: they have no ``last_used_at`` because there
is nowhere to write it, which is the honest answer rather than a fabricated one.
"""
from __future__ import annotations

from cairn.core.refs import same_ref

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from cairn.core.engine.pipeline import pipeline_pack_name
from cairn.core.engine.pipeline_resolve import PIPELINE_PREFIX, resolve_pipeline_ref
from cairn.core.pack.registry import all_preset_dirs
from cairn.core.workflow_config_store import InputPreset, default_workflow_config_store

logger = logging.getLogger("cairn.core.pipeline_presets")

#: Where a resolved preset came from. Stamped on the launch record's provenance
#: so "launched from what" also answers "…and who owns that definition".
SOURCE_PACK = "pack"
SOURCE_SAVED = "saved"


class PresetNotFoundError(LookupError):
    """No preset of that name for this pipeline — saved or pack-shipped."""


class UnknownPresetInputsError(ValueError):
    """#1789 criterion 5 — the preset carries a key the pipeline no longer
    declares.

    This is the failure mode the whole feature is otherwise exposed to: a preset
    is written once and read for months, while the pipeline it targets keeps
    evolving. Silently dropping the stale key would launch a *different* run
    than the preset describes and leave nothing in the record to say so — the
    input would simply fall back to its declared default, or go missing, and
    only the numbers at the end would be wrong. So a stale key is a refusal that
    NAMES the key, raised before any record exists.

    The pipeline's own ``$default`` rules are untouched: this checks the
    preset's keys against the pipeline's DECLARED inputs, nothing else.
    """


def canonical_pipeline_ref(ref: str) -> str:
    """The ``pipeline:<pack>/<name>`` spelling a preset is keyed under.

    Resolves through the installed defs so every alias of one pipeline
    (``retrain-cycle``, ``training/retrain-cycle``, the prefixed form)
    lands on the same key. An unresolvable ref is returned prefixed-but-unchanged
    rather than raised on: the CRUD routes resolve their own refs and report
    their own 404s, and a preset for a pipeline that is momentarily uninstalled
    should still be listable rather than vanish.
    """
    raw = (ref or "").strip()
    try:
        d = resolve_pipeline_ref(raw)
    except Exception:  # noqa: BLE001 — an ambiguous/unknown ref is not this function's error
        d = None
    if d is not None:
        return f"{PIPELINE_PREFIX}{d.pack}/{d.name}"
    return raw if raw.startswith(PIPELINE_PREFIX) else f"{PIPELINE_PREFIX}{raw}"


def _parse_preset_file(path: Path) -> list[InputPreset]:
    """One ``presets/*.yaml`` → the presets it declares.

    Shape (deliberately boring — it is data)::

        pipeline: training/retrain-cycle
        presets:
          - name: w2-cycle
            description: ...
            inputs: {k: v, ...}

    A malformed file is skipped with a warning rather than raised, matching
    ``all_pipeline_defs``: one broken neighbour must not make a valid preset
    unresolvable.
    """
    try:
        doc = yaml.safe_load(path.read_text()) or {}
    except Exception:  # noqa: BLE001 — a bad file is a lint problem, not a launch failure
        logger.warning("skipping unparseable preset file %s", path)
        return []
    if not isinstance(doc, dict):
        logger.warning("skipping preset file %s: top level is not a mapping", path)
        return []
    pipeline = str(doc.get("pipeline") or "").strip()
    if not pipeline:
        logger.warning("skipping preset file %s: no `pipeline:` ref", path)
        return []
    ref = canonical_pipeline_ref(pipeline)
    out: list[InputPreset] = []
    for entry in doc.get("presets") or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            logger.warning("skipping a nameless preset in %s", path)
            continue
        out.append(InputPreset(
            name=name, template_ref=ref, workspace_id="default",
            inputs=dict(entry.get("inputs") or {}),
            description=str(entry.get("description") or "").strip(),
            # the pack, not a user — visible in the listing and on the record
            created_by=f"pack:{pipeline_pack_name(path)}",  # #2513: the manifest's name, not the dir
        ))
    return out


def pack_presets(ref: str) -> dict[str, InputPreset]:
    """Pack-shipped seed presets for one pipeline, by name.

    Read fresh from disk on each call: the set is tiny (a handful of files
    across installed packs), and caching it would make a pack install invisible
    until a restart.
    """
    canon = canonical_pipeline_ref(ref)
    found: dict[str, InputPreset] = {}
    for d in all_preset_dirs():
        for yml in sorted(Path(d).glob("*.yaml")):
            for p in _parse_preset_file(yml):
                if same_ref(p.template_ref, canon):   # a seed file may predate #2527's spelling
                    found[p.name] = p
    return found


async def list_presets(
    workspace_id: str, ref: str, *, store: Any = None,
) -> list[tuple[InputPreset, str]]:
    """Every preset addressable for this pipeline, as ``(preset, source)``.

    Saved presets shadow pack-shipped ones of the same name — the same
    precedence :func:`resolve_preset` applies, so what the picker lists is
    exactly what a launch would resolve.
    """
    canon = canonical_pipeline_ref(ref)
    store = store or default_workflow_config_store()
    merged: dict[str, tuple[InputPreset, str]] = {
        name: (p, SOURCE_PACK) for name, p in pack_presets(canon).items()
    }
    for p in await store.list_input_presets(workspace_id, canon):
        merged[p.name] = (p, SOURCE_SAVED)
    return [merged[k] for k in sorted(merged)]


async def resolve_preset(
    workspace_id: str, ref: str, name: str, *, store: Any = None,
) -> tuple[InputPreset, str]:
    """The ONE lookup: a saved preset first, else the pack's seed, else raise.

    Raises :class:`PresetNotFoundError` naming what IS available — a mistyped
    preset name is the likeliest error at this door, and the flagship's whole
    point is that the user should not have to go read a file to find the right
    spelling.
    """
    canon = canonical_pipeline_ref(ref)
    store = store or default_workflow_config_store()
    saved = await store.get_input_preset(workspace_id, canon, name)
    if saved is not None:
        return saved, SOURCE_SAVED
    seeds = pack_presets(canon)
    if name in seeds:
        return seeds[name], SOURCE_PACK
    known = sorted({*seeds, *(p.name for p in await store.list_input_presets(workspace_id, canon))})
    raise PresetNotFoundError(
        f"no preset {name!r} for {canon}. Available: "
        + (", ".join(known) if known else "(none saved, and the pack ships none)")
    )


def assert_preset_keys_declared(preset: InputPreset, declared: dict, source: str) -> None:
    """Criterion 5 — refuse LOUDLY, naming the key(s), when a preset carries an
    input the pipeline no longer declares. Called before the record exists."""
    unknown = sorted(k for k in (preset.inputs or {}) if k not in (declared or {}))
    if not unknown:
        return
    raise UnknownPresetInputsError(
        f"preset {preset.name!r} ({source}) sets input(s) {unknown!r} that "
        f"{preset.template_ref} no longer declares — the pipeline's inputs are "
        f"{sorted(declared or {})!r}. Update or delete the preset; a stale key is "
        f"never silently dropped."
    )


async def expand_preset(
    workspace_id: str, d: Any, name: str, *, store: Any = None,
) -> tuple[InputPreset, str]:
    """The launch door's preset step for an already-resolved definition ``d``: the one
    lookup (:func:`resolve_preset`) and the stale-key refusal
    (:func:`assert_preset_keys_declared`). ``launch_pipeline_def`` and the CLI's
    ``pipeline explain`` / ``show --plan`` (#231) call it, so a dry run refuses what the
    launch refuses; the caller layers its inputs over the preset with
    :func:`merge_over_preset`."""
    rec, source = await resolve_preset(
        workspace_id, f"{PIPELINE_PREFIX}{d.pack}/{d.name}", name, store=store)
    assert_preset_keys_declared(rec, d.inputs or {}, source)
    return rec, source


def merge_over_preset(preset_inputs: dict, supplied: dict) -> tuple[dict, dict]:
    """``(merged, overrides)`` — an explicit ``--input k=v`` wins per key.

    ``overrides`` is the subset of the caller's own inputs that SHADOWED a
    preset key; that is the pair the provenance records, because it is the only
    part a reader cannot reconstruct from the merged ``inputs`` alone. A key the
    caller supplied that the preset never set is not an override, it is just an
    input, and it is already visible verbatim on the record.
    """
    preset_inputs = dict(preset_inputs or {})
    supplied = dict(supplied or {})
    overrides = {k: v for k, v in supplied.items() if k in preset_inputs}
    return {**preset_inputs, **supplied}, overrides


async def touch_preset(
    workspace_id: str, preset: InputPreset, source: str, *, store: Any = None,
) -> None:
    """Stamp ``last_used_at`` on a SAVED preset that just resolved a launch.

    Best-effort and never fatal: a launch must not fail because a bookkeeping
    write did. Pack presets are skipped — they live in an installed wheel and
    there is nothing to write to, so their ``last_used_at`` stays null rather
    than being invented.
    """
    if source != SOURCE_SAVED:
        return
    try:
        store = store or default_workflow_config_store()
        stamp = datetime.now(timezone.utc).isoformat()
        await store.put_input_preset(preset.model_copy(update={"last_used_at": stamp}))
    except Exception:  # noqa: BLE001 — provenance bookkeeping never blocks a launch
        logger.warning("could not stamp last_used_at on preset %s", preset.id, exc_info=True)


def preset_row(preset: InputPreset, source: str) -> dict[str, Any]:
    """The listing shape (criterion 3): name + input count + last-used, plus the
    inputs themselves so a picker can show what it would send."""
    return {
        "name": preset.name,
        "description": preset.description,
        "inputs": dict(preset.inputs or {}),
        "input_count": len(preset.inputs or {}),
        "source": source,
        "last_used_at": preset.last_used_at,
        "created_at": preset.created_at,
        "created_by": preset.created_by,
        "updated_at": preset.updated_at,
    }


__all__ = [
    "SOURCE_PACK", "SOURCE_SAVED",
    "PresetNotFoundError", "UnknownPresetInputsError",
    "assert_preset_keys_declared", "canonical_pipeline_ref", "expand_preset", "list_presets",
    "merge_over_preset", "pack_presets", "preset_row", "resolve_preset",
    "touch_preset",
]
