"""The kernel runner — spec interpreter (ADR-0081 D3, #1048) — core since #2583
(ADR-0129 D1; until then it lived in the builtin dataflow pack's engine package).

Generalizes the proven ``run_fix_pipeline`` skeleton: sequential stages over a
deterministic workdir, ``.done`` resume markers per stage, progressive
``metrics.json`` (what remote/exec's poll and the control loop read), a
``result.json`` terminal artifact, and budget self-enforcement
(``max_seconds`` → the runner aborts itself; the conformance suite's
budget-abort case runs for real here).

Stdlib-only (D5): stage KERNELS own their heavy imports; the interpreter never
does. Executes stages via the shared registry — no stage-specific logic may
live here (the #1048 anti-deviation rule: bespoke runner code is what would
turn every future adapter into a rewrite).

Every engine adapter runs kernels through :func:`run_dataflow` (ADR-0118 D7):
the stage-contract checks (identity, configuration, output, frozen resume
stats — ADR-0118 D5) live HERE, once, so an adapter outside the builtin pack
carries them without importing that pack (ADR-0129 D2).
"""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import time
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from cairn.core.dataflow import ArtifactDescriptor, DataflowResult, DataflowSpec, capture, sysmetrics
from cairn.core.dataflow.conformance_check import StageContractError, problems
from cairn.core.dataflow.manifest import Manifest

from cairn.core.dataflow.stage_loader import load_stages, stage_load_failures, stage_versions

_log = logging.getLogger(__name__)

#: workdir entries that are runner bookkeeping, never part of the working set
_BOOKKEEPING = ("metrics.json", "result.json", "checkpoints")


class BudgetExceeded(RuntimeError):
    pass


#: Stat key a stage reports its own spend under. Summed across stages and
#: mapped items to enforce ``budget.max_cost_usd``.
_COST_STAT = "cost_usd"
#: Stat key a stage reports its working-set size under.
_ROWS_STAT = "rows"


def _sum_stat(stats: dict, key: str) -> float:
    """Total a per-stage scalar across everything reported so far.

    Stage stats are namespaced ``<stage>.<key>`` in the accumulator, so match
    on the suffix. Non-numeric values are ignored rather than fatal: a budget
    check must not be the thing that kills a run over a malformed stat.
    """
    total = 0.0
    for k, v in stats.items():
        if k == key or k.endswith(f".{key}"):
            try:
                total += float(v or 0)
            except (TypeError, ValueError):
                continue
    return total


def _peak_stat(stats: dict, key: str) -> float:
    """The largest value any single stage reported."""
    best = 0.0
    for k, v in stats.items():
        if k == key or k.endswith(f".{key}"):
            try:
                best = max(best, float(v or 0))
            except (TypeError, ValueError):
                continue
    return best


#: #1441 — runner-owned stats a stage gains when it called the governed LLM
#: chokepoint (``llm_complete``) at least once; absent otherwise, so a stage that
#: makes no call keeps exactly the stats it reported. Summed across mapped items.
_LLM_STATS = ("llm_calls", "llm_tokens_in", "llm_tokens_out", "llm_usd", "llm_unpriced_calls")


def _metered(call: Callable[[], Optional[dict]], *,
             on_cost: Optional[Callable[[float], None]] = None) -> tuple[dict, dict]:
    """Run one kernel call inside an ``obs.cost_scope`` and return ``(raw stats,
    metered llm stats)`` — ``{}`` for the latter when no LLM call was made.

    The engine runs kernels in its own process (builtin subprocess, SSH box,
    Metaflow task), where no control-plane scope exists, so every
    ``llm_complete`` → ``obs.record_llm`` used to be a no-op and a paid stage
    reported $0 (#1441 Z2). The scope MEASURES without the session backstop
    (``enforce_cap=False``): a leg's spend is governed by its reservation and
    ``spec.budget``. An in-process caller's own scope still sees the calls.
    """
    from cairn.core import obs  # noqa: PLC0415 — lazy: the metering scope only (#1441)

    outer = obs.get()
    llm: dict = {}
    with obs.cost_scope("dataflow-stage", enforce_cap=False) as ctx:
        try:
            stats = call() or {}
        finally:
            calls = list(ctx.llm_calls)
            if calls:
                if outer is not None:
                    outer.llm_calls.extend(calls)
                    outer.cost_usd = round(outer.cost_usd + ctx.cost_usd, 6)
                llm = {
                    "llm_calls": len(calls),
                    "llm_tokens_in": sum(int(c.get("in") or 0) for c in calls),
                    "llm_tokens_out": sum(int(c.get("out") or 0) for c in calls),
                    "llm_usd": round(sum(float(c.get("usd") or 0.0) for c in calls), 6),
                    "llm_unpriced_calls": sum(1 for c in calls if c.get("priced") is False),
                }
            if on_cost is not None and ctx.cost_usd > 0:
                try:
                    on_cost(round(ctx.cost_usd, 6))
                except Exception:  # noqa: BLE001 — progress must not replace a kernel failure
                    _log.debug("writing partial dataflow cost failed", exc_info=True)
    return stats, llm


def _with_llm(stats: dict, llm: dict) -> dict:
    """Merge the metered keys into a stage's stats. A kernel that reported its own
    ``cost_usd`` keeps it (it knows spend the chokepoint never saw, and counting
    both would double it); otherwise the stage's ``cost_usd`` is its ``llm_usd``,
    which is what ``budget.max_cost_usd`` and ``DataflowResult.cost_usd`` sum."""
    if not llm:
        return stats
    out = {**stats, **llm}
    if _COST_STAT not in stats:
        out[_COST_STAT] = llm["llm_usd"]
    return out


def _carry_metered(parts: list, merged: dict) -> dict:
    """A mapped stage's merged stats keep the items' metered spend whatever the
    merge (a kernel's own merge callable need not know these keys)."""
    metered = [p for p in parts if isinstance(p, dict) and p.get("llm_calls")]
    if not metered:
        return merged
    out = dict(merged)
    for k in _LLM_STATS:
        out[k] = round(sum(float(p.get(k) or 0) for p in metered), 6) if k == "llm_usd" \
            else int(sum(int(p.get(k) or 0) for p in metered))
    if _COST_STAT not in out:
        out[_COST_STAT] = round(sum(_sum_stat(p, _COST_STAT) for p in parts if isinstance(p, dict)), 6)
    return out


def _check_budget(spec: DataflowSpec, started: float, *, where: str,
                  cost_usd: float = 0.0, rows: float = 0.0) -> None:
    """Enforce every ceiling the spec declared — not just the one that used to
    be checked.

    ``Budget`` has carried ``max_cost_usd`` and ``max_rows`` since ADR-0081
    ("Hard ceilings the engine must respect; breach → abort + failed status …
    a conforming engine self-enforces these"), but only ``max_seconds`` was
    ever read. A declared ceiling that nothing enforces is worse than an
    absent one: it reads as a guarantee.

    Semantics, which differ on purpose:
      * **cost is SUMMED** — spend accumulates across stages and mapped items.
      * **rows is a PEAK** — it bounds the size of the working set a stage may
        process, not the total of every stage's view of the same rows.
    """
    b = spec.budget
    if b.max_seconds and time.monotonic() - started > b.max_seconds:
        raise BudgetExceeded(f"budget.max_seconds={b.max_seconds} exceeded {where}")
    if b.max_cost_usd and cost_usd > b.max_cost_usd:
        raise BudgetExceeded(
            f"budget.max_cost_usd={b.max_cost_usd} exceeded {where} "
            f"(spent ${cost_usd:.4f})")
    if b.max_rows and rows > b.max_rows:
        raise BudgetExceeded(
            f"budget.max_rows={b.max_rows} exceeded {where} ({int(rows)} rows)")


def _write_metrics(workdir: Path, payload: dict) -> None:
    # small atomic-ish write: a torn read by a poller just means "not yet"
    (workdir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")


def _include_known_cost(payload: dict, cost_usd: float) -> dict:
    """Add cumulative cost only when a finite positive amount is known."""
    if 0 < cost_usd < float("inf"):
        payload["cost_usd"] = round(cost_usd, 6)
    return payload


def _marker(workdir: Path, i: int, stage: str) -> Path:
    return workdir / f"stage-{i:02d}-{stage.replace('/', '_')}.done"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()[:16]


def _emit_checkpoint(workdir: Path, i: int, ref_stage: str,
                     spec_key: str, stage_stats: dict) -> dict:
    """ADR-0088 D2 (#1248 H6a) — SYNCHRONOUS snapshot of the working set
    after a `checkpoint: true` stage: every top-level workdir FILE (markers
    and bookkeeping excluded) copies into ``checkpoints/stage-NN/`` with a
    manifest the control plane relays (H6b) and the resume path consumes
    (H6c). Local and credential-free by design (D4 — the box never touches
    the artifact store)."""
    dest = workdir / "checkpoints" / f"stage-{i:02d}"
    dest.mkdir(parents=True, exist_ok=True)
    files = []
    for p in sorted(workdir.iterdir()):
        if not p.is_file() or p.name in _BOOKKEEPING or p.name.endswith(".done"):
            continue
        shutil.copy2(p, dest / p.name)
        files.append({"name": p.name, "content_hash": _sha256(p),
                      "size": p.stat().st_size})
    manifest = {"spec_key": spec_key, "stage_index": i, "stage": ref_stage,
                "files": files, "stats": dict(stage_stats or {})}
    (dest / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


def _seed_resume(spec: DataflowSpec, workdir: Path, spec_key: str,
                 stats: dict) -> None:
    """ADR-0088 D3 — materialize ``resume_from`` into a fresh workdir: seed
    the checkpointed files and write SYNTHETIC ``.done`` markers (frozen
    stats) for every stage at or before the checkpoint, so the existing
    skip-on-marker loop resumes without a single new branch in it. A
    ``spec_key`` mismatch invalidates the whole resume (the spec changed —
    the same validity rule the markers follow): restart from stage 0, loudly.
    """
    rf = spec.resume_from
    if rf is None:
        return
    if rf.spec_key != spec_key:
        stats["resume_invalid_spec"] = True
        _log.warning("resume_from ignored: spec_key %s != submitted %s "
                     "(the spec changed; restarting from stage 0)",
                     rf.spec_key, spec_key)
        return
    for art in rf.artifacts:
        uri = str(art.get("uri", ""))
        dest = workdir / str(art.get("dest") or Path(uri).name)
        if uri.startswith("file://"):
            shutil.copy2(uri[len("file://"):], dest)
        elif uri.startswith(("http://", "https://")):
            # control-plane-minted URL (the D4 phase-2 path)
            urllib.request.urlretrieve(uri, dest)  # noqa: S310 — uri comes from the spec the control plane built
        else:
            raise ValueError(
                f"resume_from artifact uri {uri!r}: unsupported scheme — the "
                f"control plane materializes checkpoints as file:// or http(s)."
            )
    frozen = {int(c.get("index", -1)): c for c in rf.completed}
    for i, ref in enumerate(spec.stages[:rf.stage_index + 1]):
        c = frozen.get(i, {})
        _marker(workdir, i, ref.stage).write_text(
            json.dumps(dict(c.get("stats") or {})), encoding="utf-8")
    stats["resumed_from_stage"] = rf.stage_index


# ── stage-level map: fan-out (#1290/#1360, ADR-0090) ────────────────────────


def _item_marker(workdir: Path, i: int, stage: str, j: int) -> Path:
    return workdir / f"stage-{i:02d}-{stage.replace('/', '_')}.item-{j:03d}.done"


def _map_item(task: tuple) -> dict:
    """One mapped item, in a POOL CHILD. Plain data in, plain data out — the
    child re-resolves the kernel from the registry, the same picklability
    pattern as #962's ShardTask (a live registry does not pickle)."""
    stage, item_dir, config, inputs = task
    from cairn.core.dataflow.stage_loader import load_stages  # noqa: PLC0415 — child-process re-resolve
    wd = Path(item_dir)
    wd.mkdir(parents=True, exist_ok=True)
    def _write_cost(cost_usd: float) -> None:
        _write_metrics(wd, _include_known_cost({}, cost_usd))

    stats, llm = _metered(
        lambda: load_stages()[stage](wd, dict(config), list(inputs)),
        on_cost=_write_cost,
    )
    stats = _with_llm(stats, llm)
    try:
        _write_metrics(wd, dict(stats))
    except OSError:  # noqa: BLE001 — item progress is best-effort
        _log.warning("writing mapped item metrics failed for %s", wd, exc_info=True)
    return stats


def _plan_map_items(ref, workdir: Path) -> list:
    m = ref.map
    if m.over == "shards":
        mf = workdir / "manifest.json"
        if not mf.exists():
            raise ValueError(
                f"stage {ref.stage!r}: map.over='shards' but the working set has "
                "no manifest.json — a prior stage must write one (ADR-0081 D2), "
                "or map an explicit list via map.over='$config.<key>'"
            )
        return [s.model_dump() for s in Manifest.load(mf).shards]
    if m.over.startswith("$config."):
        # MapBTNode semantics: missing/empty → zero branches; non-list wraps.
        raw = ref.config.get(m.over[len("$config."):])
        if raw is None:
            return []
        return list(raw) if isinstance(raw, list) else [raw]
    raise ValueError(
        f"stage {ref.stage!r}: map.over must be 'shards' or '$config.<key>', "
        f"got {m.over!r}"
    )


def _merge_map_stats(ref, parts: list, kernel_fn) -> dict:
    """Fan the per-item stats into ONE stage result (ADR-0090 §2)."""
    m = ref.map
    out: dict = {"items_total": len(parts), "items_done": len(parts)}
    if m.merge in ("stats_sum", "concat"):
        for p in parts:
            for k, v in (p or {}).items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    out[k] = out.get(k, 0) + v
        if m.merge == "concat":
            out["items"] = [dict(p or {}) for p in parts]
        return out
    fn = getattr(kernel_fn, m.merge, None)
    if not callable(fn):
        raise ValueError(
            f"stage {ref.stage!r}: map.merge={m.merge!r} is neither a builtin "
            "(stats_sum | concat) nor a callable attribute on the kernel"
        )
    merged = dict(fn(parts) or {})
    merged.setdefault("items_total", len(parts))
    merged.setdefault("items_done", len(parts))
    return merged


def _run_mapped_stage(registry: dict, ref, index: int, workdir: Path,
                      spec: DataflowSpec, started: float,
                      on_progress: Optional[Callable[[dict], None]],
                      prior_cost: float = 0.0) -> dict:
    """Execute one ``map:`` stage: plan items → waves through the #962 pool →
    merge. Per-item ``.done`` markers make the fan-out resume-safe at ITEM
    granularity (an interrupted stage re-runs only its unfinished items);
    each item gets its own subdir so a shared filesystem cannot interleave
    kernel outputs. Waves of ``max_parallel`` give the budget check a
    boundary to land on — the same stage-boundary granularity the runner
    already enforces, one level down."""
    from cairn.core.operator.batch import map_shards_local  # noqa: PLC0415 — pool import only paid when a map: stage exists

    m = ref.map
    items = _plan_map_items(ref, workdir)
    total = len(items)
    if ref.contract is not None:   # ADR-0118 D2 — each item's config, before any item runs
        for item in items:
            _conform(spec, ref, ref.contract.configuration, {**ref.config, m.as_: item}, "configuration")
    item_stats: dict[int, dict] = {}
    pending: list[int] = []
    for j in range(total):
        mk = _item_marker(workdir, index, ref.stage, j)
        if mk.exists():
            item_stats[j] = json.loads(mk.read_text(encoding="utf-8"))
        else:
            pending.append(j)

    def _emit() -> None:
        cost_usd = prior_cost + sum(
            _sum_stat(s, _COST_STAT) for s in item_stats.values()
        )
        for j in range(total):
            if j in item_stats:
                continue
            try:
                item_metrics = json.loads(
                    (workdir / f"map-{index:02d}" / f"item-{j:03d}" / "metrics.json")
                    .read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                continue
            if isinstance(item_metrics, dict):
                cost_usd += _sum_stat(item_metrics, _COST_STAT)
        payload = {"stage": ref.stage, "stages_done": index,
                   "stages_total": len(spec.stages),
                   # additive keys (D8a stays compatible) — the poller sees
                   # item progress without a schema change
                   "items_done": len(item_stats), "items_total": total,
                   "elapsed_s": round(time.monotonic() - started, 2)}
        _include_known_cost(payload, cost_usd)
        _write_metrics(workdir, payload)
        if on_progress:
            on_progress(payload)

    _emit()
    for w in range(0, len(pending), m.max_parallel):
        # Wave boundaries are where a fan-out can still be stopped cheaply: the
        # spend of the waves already run is known, and the next wave has not
        # been bought yet. Items are shards of ONE working set, so their rows
        # sum here (the peak comparison happens one level up).
        done_items = list(item_stats.values())
        _check_budget(
            spec, started,
            where=(f"mid-fan-out at stage {ref.stage!r} "
                   f"({len(item_stats)}/{total} items done)"),
            cost_usd=prior_cost + sum(_sum_stat(s, _COST_STAT) for s in done_items),
            rows=sum(_sum_stat(s, _ROWS_STAT) for s in done_items),
        )
        wave = pending[w:w + m.max_parallel]
        tasks = []
        for j in wave:
            cfg = dict(ref.config)
            cfg[m.as_] = items[j]
            tasks.append((ref.stage,
                          str(workdir / f"map-{index:02d}" / f"item-{j:03d}"),
                          cfg, list(spec.input_artifacts)))
        try:
            results = map_shards_local(_map_item, tasks, requested_workers=m.max_parallel)
        except Exception:
            try:
                _emit()
            except Exception:  # noqa: BLE001 — progress must not replace a kernel failure
                _log.debug("writing partial mapped metrics failed", exc_info=True)
            raise
        for j, res in zip(wave, results):
            _item_marker(workdir, index, ref.stage, j).write_text(
                json.dumps(res or {}), encoding="utf-8")
            item_stats[j] = res or {}
        _emit()
    parts = [item_stats[j] for j in range(total)]
    return _carry_metered(parts, _merge_map_stats(ref, parts, registry[ref.stage]))


# ── ADR-0118 D5: the stage contract, checked in the interpreter ─────────────


def _stage_contract(spec: DataflowSpec, ref, kernel) -> Optional[object]:
    """The admitted contract of one stage, or ``None`` for a legacy stage. An unstamped
    spec (the dev loop, ``cairn flow export``, an unadmitted template) runs legacy (D6);
    inside an admitted spec an opted-in stage with no contract is lost provenance (M2a)."""
    if spec.contracts is None:
        return None
    if ref.contract is None and getattr(kernel, "implements", ()):
        raise StageContractError(
            f"stage {ref.stage!r} implements {list(kernel.implements)} but leg {spec.contracts.leg!r} "
            "was admitted with no contract for it; refusing to run unrecorded")
    return ref.contract


def _conform(spec: DataflowSpec, ref, uri: str, value: dict, part: str) -> None:
    found = problems(spec.contracts.schemas[uri], value, part)
    if found:
        raise StageContractError(f"stage {ref.stage!r} does not conform to {ref.contract.capability_ref}: "
                                 + "; ".join(found))


def _check_identity(ref, kernel) -> None:
    """The loaded kernel must implement the recorded revision at the recorded version."""
    version = str(stage_versions().get(ref.stage) or "")
    if ref.contract.capability_ref not in tuple(getattr(kernel, "implements", ()) or ()) or version != ref.contract.version:
        raise StageContractError(
            f"stage {ref.stage!r} kernel {version or '(unversioned)'} implementing "
            f"{list(getattr(kernel, 'implements', ()) or ())} no longer matches the admitted "
            f"{ref.contract.capability_ref} at {ref.contract.version or '(unversioned)'}; refusing to run a drifted kernel")


def _check_frozen(spec: DataflowSpec, ref, frozen: dict) -> None:
    """Resume: the frozen stats were produced by the admitted kernel version, and conform."""
    if "kernel_version" not in frozen or frozen["kernel_version"] != ref.contract.version:
        raise StageContractError(
            f"stage {ref.stage!r}: resumed stats carry kernel_version {frozen.get('kernel_version')!r}, not the "
            f"admitted {ref.contract.version!r}; refusing to resume")
    _conform(spec, ref, ref.contract.output, frozen, "output")


def _descriptor_uri(out: str) -> str:
    return out if out.startswith(("s3://", "file://", "gs://", "https://", "hf://", "stub://")) else f"file://{out}"


#: The fields a named output may carry (``outputs.<name>.<field>``).
_OUTPUT_FIELDS = ("output", "content_hash", "kind", "rows", "bytes", "register_as", "register_role")


def _named_outputs(stage: str, st: dict, workdir: Path, parents: list[str]) -> list[ArtifactDescriptor]:
    """ADR-0141 D1 — a stage that makes several products names them (a split's train, val and
    test files): ``outputs: [<name>, …]`` and, per name, flat ``outputs.<name>.<field>`` stats
    (stats cross the boundary as scalars, ADR-0081 D3). Each becomes a descriptor in the result's
    ``named_outputs`` with the stage's parents — the products are siblings of the stage's own
    descriptor, so none descends from another and the next stage still descends from the stage —
    and its own stats, which is where the control plane reads ``register_as`` / ``register_role``.
    A named output without ``output`` and ``content_hash`` is a kernel bug and fails the flow.
    An ``outputs`` stat with no ``outputs.<name>.*`` key is some kernel's own stat, left alone."""
    names = st.get("outputs")
    if not names or not any(str(k).startswith("outputs.") for k in st):
        return []
    if (not isinstance(names, list) or not all(isinstance(n, str) and n for n in names)
            or len(set(names)) != len(names)):
        raise ValueError(f"stage {stage!r}: `outputs` must be a list of distinct names, got {names!r}")
    found: list[ArtifactDescriptor] = []
    for name in names:
        fields = {f: st[f"outputs.{name}.{f}"] for f in _OUTPUT_FIELDS if f"outputs.{name}.{f}" in st}
        out, digest = str(fields.get("output") or ""), str(fields.get("content_hash") or "")
        if not out or not digest:
            raise ValueError(f"stage {stage!r}: output {name!r} needs outputs.{name}.output "
                             f"and outputs.{name}.content_hash")
        kind = fields.get("kind") if fields.get("kind") in ("dataset", "model", "report", "index") else "dataset"
        rows, size = fields.get("rows"), fields.get("bytes")
        found.append(ArtifactDescriptor(
            uri=_descriptor_uri(out if ("://" in out or out.startswith("/")) else str(workdir / out)),
            kind=kind,
            content_hash=digest,
            rows=int(rows) if isinstance(rows, int) and rows >= 0 else None,
            size_bytes=int(size) if isinstance(size, int) else None,
            parents=list(parents),
            stats={"output_name": name, "stage": stage, **fields},
        ))
    return found


def run_dataflow(
    spec: DataflowSpec,
    workdir: Path,
    *,
    on_progress: Optional[Callable[[dict], None]] = None,
) -> DataflowResult:
    """Execute the spec's stages sequentially in ``workdir``. Resume-safe: a
    stage whose ``.done`` marker survives is skipped (same-input re-runs are
    deterministic, so skipping is correct — the run_fix_pipeline contract)."""
    workdir.mkdir(parents=True, exist_ok=True)
    registry = load_stages()
    unknown = [s.stage for s in spec.stages if s.stage not in registry]
    if unknown:
        # #2069 — separate "nobody ships this" from "it is installed and its
        # import raised". Both leave the stage out of the registry, and only one
        # of them is fixed by installing a pack. Reporting them the same way
        # cost two runs on a GPU box chasing a missing entry point that was
        # actually a missing dependency (#2086).
        failed = stage_load_failures()
        broken = [s for s in unknown if s in failed]
        missing = [s for s in unknown if s not in failed]
        parts = []
        if broken:
            parts.append("; ".join(
                f"{s!r} is installed but failed to import: {failed[s]}" for s in broken))
        if missing:
            parts.append(f"no kernel is registered for {missing} "
                         f"(registry has {sorted(registry)})")
        raise ValueError("unknown stages " + str(unknown) + " — " + " | ".join(parts))

    started = time.monotonic()
    stats: dict = {"stages_total": len(spec.stages), "stages_done": 0}
    descriptors: list[ArtifactDescriptor] = []
    #: ADR-0141 — the products stages name (:func:`_named_outputs`); never a stage's parent.
    named: list[ArtifactDescriptor] = []
    # Checkpoint identity: the CARRIED control-plane key when present (#1314 —
    # engines may rewrite the spec at submit, e.g. SSH input localization, so
    # hashing what we received can diverge from the control plane's lookup
    # key); else the hash WITHOUT resume_from (a resumed submission must hash
    # to the SAME key as the original, #1248 H6a) and WITHOUT checkpoint_key
    # (stamping must not change the key; unstamped specs keep old hashes).
    spec_key = spec.checkpoint_key or hashlib.blake2b(
        spec.model_dump_json(exclude={"resume_from", "checkpoint_key"}).encode("utf-8"),
        digest_size=8,
    ).hexdigest()
    _seed_resume(spec, workdir, spec_key, stats)

    for i, ref in enumerate(spec.stages):
        _check_budget(spec, started, where=f"at stage {ref.stage!r}",
                      cost_usd=_sum_stat(stats, _COST_STAT),
                      rows=_peak_stat(stats, _ROWS_STAT))
        marker = _marker(workdir, i, ref.stage)
        contract = _stage_contract(spec, ref, registry[ref.stage])
        if marker.exists():
            stage_stats = json.loads(marker.read_text(encoding="utf-8"))
            if contract is not None:
                _check_frozen(spec, ref, stage_stats)
        else:
            if contract is not None:
                _check_identity(ref, registry[ref.stage])
                if getattr(ref, "map", None) is None:
                    _conform(spec, ref, contract.configuration, dict(ref.config), "configuration")
            # #1465 — uniform run capture: diff the workdir around the stage so
            # everything it created/changed is mirrored to the artifact store
            # and described in its stats (which ride into result_payloads).
            # No-op without CAIRN_ARTIFACT_S3_BUCKET; never fails the stage.
            _before = capture.snapshot_workdir(workdir)
            # #1465 — system metrics: sample cpu/ram/gpu while the stage runs
            # (gpu-declaring stages by default, CAIRN_SYSMETRICS=on for all).
            # The series file lands in the workdir AFTER the stage, so the
            # capture diff below mirrors it like any other stage product.
            _sampler = sysmetrics.start(
                workdir, i, ref.stage,
                ref.resources.model_dump() if getattr(ref, "resources", None) else None)
            _llm: dict = {}   # a mapped stage's items carry their own metered keys

            def _write_stage_cost(stage_cost_usd: float) -> None:
                progress = {
                    "stage": ref.stage,
                    "stages_done": i,
                    "stages_total": len(spec.stages),
                    "elapsed_s": round(time.monotonic() - started, 2),
                }
                _include_known_cost(
                    progress, _sum_stat(stats, _COST_STAT) + stage_cost_usd)
                _write_metrics(workdir, progress)
                if on_progress:
                    on_progress(progress)

            try:
                if getattr(ref, "map", None) is not None:
                    # #1290/ADR-0090 — fan this stage out over a runtime list,
                    # through the #962 pool; the stage's own .done marker still
                    # records the MERGED result, so downstream resume is unchanged.
                    stage_stats = _run_mapped_stage(
                        registry, ref, i, workdir, spec, started, on_progress,
                        prior_cost=_sum_stat(stats, _COST_STAT))
                else:
                    stage_stats, _llm = _metered(lambda: registry[ref.stage](
                        workdir, dict(ref.config), list(spec.input_artifacts)),
                        on_cost=_write_stage_cost)
            finally:
                _sys = _sampler.stop() if _sampler is not None else {}
            if contract is not None:
                # the RAW stats (the merged result for a mapped stage), before the runner adds
                # its own keys and before the marker: a refusal leaves nothing to resume past.
                _conform(spec, ref, contract.output, dict(stage_stats or {}), "output")
            # #1441 — the metered LLM spend, AFTER the raw output check (runner-owned keys).
            stage_stats = _with_llm(dict(stage_stats or {}), _llm)
            if _sys:
                stage_stats = {**(stage_stats or {}), **_sys}
            _captured = capture.capture_stage(workdir, _before, i, ref.stage)
            if _captured:
                stage_stats = {**(stage_stats or {}), **_captured}
            # #2188 (T03, C2) — which kernel IMPLEMENTATION produced these rows:
            # the distribution that registered the stage. Written into the
            # stage's own stats BEFORE the marker, so a resumed stage reports
            # the version that ran, never the one installed at resume time.
            version = stage_versions().get(ref.stage)
            if version:
                stage_stats = {**(stage_stats or {}), "kernel_version": version}
            marker.write_text(json.dumps(stage_stats or {}), encoding="utf-8")
            if getattr(ref, "checkpoint", False):
                # D2 — durable when claimed, degrading when not: a failed
                # snapshot records itself and the flow continues (today's
                # restart-from-zero is the fallback, not a crash).
                try:
                    _emit_checkpoint(workdir, i, ref.stage, spec_key, stage_stats or {})
                    stats["checkpoint.last_index"] = i
                except Exception as e:  # noqa: BLE001 — insurance must not destroy the insured
                    stats["checkpoint_failed"] = f"{ref.stage}: {type(e).__name__}"
                    _log.warning("checkpoint after %s failed: %s", ref.stage, e)
        for k, v in (stage_stats or {}).items():
            stats[f"{ref.stage}.{k}"] = v
        stats["stages_done"] = i + 1
        progress = {"stage": ref.stage, "stages_done": i + 1,
                    "stages_total": len(spec.stages), "elapsed_s": round(time.monotonic() - started, 2)}
        _include_known_cost(progress, _sum_stat(stats, _COST_STAT))
        _write_metrics(workdir, progress)
        if on_progress:
            on_progress(progress)
        # #1442 — a REAL descriptor when the stage tells us where its product
        # lives (`output` = a path/URI, e.g. export.jsonl → s3://…) and what it
        # is (`content_hash`); the placeholder form (spec key + index, a
        # `stage-NN` pseudo-URI) stays for stages that only rewrite the working
        # file, so identical re-runs still register identically. Lineage: each
        # stage's parent is the previous stage's descriptor (stage 0: the
        # flow's declared inputs) — the chain #1432 found empty.
        st = dict(stage_stats or {})
        out = str(st.get("output") or "")
        is_uri = "://" in out or out.startswith("/")
        # parents are registry IDS (content hashes) — that is what the
        # store's lineage DAG (`artifact_parents`) walks; the previous stage's
        # URI was persisted but /lineage could not resolve it (found live).
        # Stage 0's parents are the flow's declared inputs (URIs by nature —
        # external data; resolvable once inputs are registered artifacts).
        parents = [descriptors[-1].content_hash] if descriptors else list(spec.input_artifacts)
        if not descriptors:
            # #1432 cross-leg lineage: an ingest stage reports the CONTENT hash
            # of what it read (`source_hash`) — that IS the upstream export's
            # registry id (content-addressed store), so the DAG walks across
            # legs with no lookup. Fallback: the source URI (the control plane
            # tries to resolve it; unknown external inputs stay as recorded).
            sh = str(st.get("source_hash") or "")
            src = str(st.get("source") or "")
            if sh and sh not in parents:
                parents.append(sh)
            elif src and ("://" in src or src.startswith("/")) and src not in parents:
                parents.append(src if "://" in src else f"file://{src}")
        rows = st.get("rows")
        # #2189 (T04) — a stage may say WHAT its product is (`kind: model` for a
        # packaged candidate); anything else stays the dataset default.
        kind = st.get("kind") if st.get("kind") in ("dataset", "model", "report", "index") else "dataset"
        descriptors.append(ArtifactDescriptor(
            uri=_descriptor_uri(out) if is_uri else f"file://{workdir}/stage-{i:02d}",
            kind=kind,
            content_hash=str(st.get("content_hash") or f"{spec_key}-{i:02d}"),
            rows=int(rows) if isinstance(rows, int) and rows >= 0 else None,
            size_bytes=int(st["bytes"]) if isinstance(st.get("bytes"), int) else None,
            parents=parents,
            stats={k: v for k, v in st.items()},
        ))
        named.extend(_named_outputs(ref.stage, st, workdir, parents))

    # The per-stage check runs BEFORE each stage, so it can stop the next one
    # from starting but never sees the last one's spend. Without this final
    # check a dataflow whose closing stage blew the ceiling would report
    # success — the breach has to be visible even when there is nothing left
    # to stop.
    _check_budget(spec, started, where="after the final stage",
                  cost_usd=_sum_stat(stats, _COST_STAT),
                  rows=_peak_stat(stats, _ROWS_STAT))

    # A cost ceiling can only bind against reported spend. If one was declared
    # and no stage ever reported `cost_usd`, say so in the stats rather than
    # letting the run finish looking guarded — an unenforceable ceiling that
    # stays silent is the same lie as an unenforced one (#1664's shape).
    if spec.budget.max_cost_usd and _sum_stat(stats, _COST_STAT) == 0.0:
        stats["cost_budget_unenforced"] = True

    return DataflowResult(
        output_artifacts=descriptors,
        named_outputs=named,
        stats=stats,
        # #1441 Z3 — the field ADR-0081 D2 always had; the same sum the ceiling binds on.
        cost_usd=max(0.0, round(_sum_stat(stats, _COST_STAT), 6)),
        summary=f"dataflow ok: {len(spec.stages)} stage(s)",
    )
