"""``cairn pipeline`` — inspect a pack's runnable surface (#1126, ADR-0084 D9).

A pipeline composes a pack's own flows into one entity; each step is a leg that
becomes its own governed run. ``show`` answers "what will this do, and on which
runtime" — the leg KIND is the load-bearing column, because it decides which
adapter executes that leg.

``explain`` (#1792) answers the harder question the owner actually asks before
spending — "how is each step TRIGGERED" — by dry-running the same resolution the
bridge performs: target file, engine, compute target, every gate on the path with
its authority, each config value's source layer, destinations, timeout and retry
as parsed. It projects :mod:`cairn.core.engine.pipeline_explain`, which calls the
runtime's own functions, so the two cannot drift.

Since #2134 S4 it also closes the OTHER half of "what will this do": a RESOLVED
block that lists every effective value beside the document that owns it —
``pipeline`` · ``profile:<name>`` · ``workspace`` · ``run`` · ``default`` ·
``inherited:<leg>``. ADR-0106 D4 keeps those four owners separate on purpose
(different people, different change cadence); this is the one view that merges
them for reading without merging them for editing.
"""
from __future__ import annotations

import typer
from rich.console import Console

from cairn.core.engine.pipeline import PipelineDef, plan_pipeline
from cairn.core.engine.pipeline_resolve import all_pipeline_defs, resolve_pipeline_ref

app = typer.Typer(
    help="Inspect pack pipelines — the composed, runnable surface (ADR-0084 D4).",
    no_args_is_help=True,
)
console = Console()

#: Which runtime executes each leg kind — the point of the whole design, so the
#: CLI states it rather than leaving the reader to infer it.
_RUNTIME = {
    "flow": "engine adapter (builtin/metaflow)",
    "template": "langgraph adapter",
    "task": "operator, in-process",
    "gate": "control plane (approval queue)",
    "loop": "control plane (bounded unroll)",
}


@app.command("ls")
def ls_cmd() -> None:
    """List every pipeline across installed packs."""
    defs = all_pipeline_defs()
    if not defs:
        console.print("[yellow]no pipelines installed[/] — a domain pack ships "
                      "at least one (ADR-0084 D4)")
        raise typer.Exit(0)
    for d in sorted(defs, key=lambda p: (p.pack, p.name)):
        kinds = ",".join(sorted({s.leg_kind for s in d.steps}))
        console.print(f"  [cyan]{d.pack}/{d.name}[/]  [dim]{len(d.steps)} legs "
                      f"({kinds})  {d.content_hash}[/]")


def render_pipeline(d: PipelineDef) -> str:
    """Human rendering of a pipeline's legs, gates and wires (also used by tests)."""
    lines = [f"{d.pack}/{d.name}  —  {d.description or '(no description)'}",
             f"  hash: {d.content_hash}"]
    if d.inputs:
        lines.append(f"  inputs: {', '.join(sorted(d.inputs))}")
    if d.governance:
        lines.append(f"  governance: {d.governance}")
    lines.append("")
    for s in d.steps:
        kind = s.leg_kind
        if kind == "gate":
            lines.append(f"  ⏸  GATE {s.gate}"
                         + (f"  shows {s.show}" if s.show else ""))
            continue
        if kind == "loop":
            lines.append(f"  ↻  LOOP {s.loop}  max={s.max_iterations} "
                         f"until={s.until!r} on_exhausted={s.on_exhausted}")
            for inner in s.steps:
                lines.append(f"       · {inner.name} [{inner.leg_kind}] {inner.ref}")
            continue
        dep = f"  after {s.depends_on}" if s.depends_on else ""
        # #1251 — a `when:` leg is OPTIONAL, not gated: mark it as such at the
        # bullet, so "this leg may not run" is readable without the ADR.
        bullet = "◦" if s.when else "•"
        lines.append(f"  {bullet}  {s.name} [{kind}] {s.ref}{dep}")
        if s.when:
            lines.append(f"       when: {s.when}   → ROUTE: skipped (pipeline "
                         f"continues) when false")
        opt = sorted(k for k, v in (s.consumes or {}).items()
                     if str(v).strip().endswith("?"))
        if opt:
            lines.append(f"       optional inputs: {', '.join(opt)} "
                         f"(omitted when their leg is routed off)")
        lines.append(f"       runtime: {_RUNTIME.get(kind, '?')}")
    return "\n".join(lines)


@app.command("show")
def show_cmd(
    ref: str = typer.Argument(..., help="<pack>/<name> or pipeline:<pack>/<name>"),
    plan: bool = typer.Option(False, "--plan", help="Show the execution plan instead."),
) -> None:
    """Show a pipeline's legs, gates, wires — and which runtime runs each leg."""
    d = resolve_pipeline_ref(ref)
    if d is None:
        known = [f"{p.pack}/{p.name}" for p in all_pipeline_defs()]
        console.print(f"[red]unknown pipeline {ref!r}[/] — installed: {known or '(none)'}")
        raise typer.Exit(2)
    if plan:
        for s in plan_pipeline(d).steps:
            console.print(f"  {s.id:16s} ref={s.ref:32s} deps={s.depends_on} "
                          f"approval={s.approval}")
        raise typer.Exit(0)
    console.print(render_pipeline(d), markup=False)


@app.command("explain")
def explain_cmd(
    ref: str = typer.Argument(..., help="<pack>/<name> or pipeline:<pack>/<name>"),
    input_: list[str] = typer.Option(
        None, "--input", "-i", help="`key=value` you would launch with (JSON parsed "
        "when possible) — shows which config values it changes; repeatable"),
    environment: str = typer.Option(
        "", "--environment", "-e", help="resolve gates/engine for THIS environment"),
    workspace: str = typer.Option(
        "default", "--workspace", "-w", help="whose variables answer the "
        "`workspace` layer of the resolved view (ADR-0101 D2)"),
    json_out: bool = typer.Option(False, "--json", help="the raw projection"),
) -> None:
    """Dry-run how every step will be TRIGGERED — before anything spends (#1792).

    Per leg: the resolved target file, the engine and compute target that will
    run it, every gate on its path with the authority demanding it, each config
    value with the layer it came from, where its outputs land, and the timeout
    and retry AS PARSED. Every one of those is produced by calling the same
    function the runtime calls, so this cannot describe a resolution that will
    not happen — including the unpleasant truths (a `retry:` that is not a map
    renders as `IGNORED (not a map)`).

    Then the RESOLVED block (#2134 S4): each declared input, the governance
    numbers, the engine/target backends and the gates, each beside the document
    that owns it — so "which file do I edit to change this" is answered without
    opening four of them.

    Offline: installed packs + the environment's selection plane + the
    deployment's policies + this workspace's variables. No run, no engine, no
    reservation.
    """
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D: local compiles get local-dev

    default_dev_profile()
    import asyncio
    import json

    from cairn.core.engine.pipeline_explain import explain_pipeline, render_explain
    from cairn.core.engine.pipeline_resolve import AmbiguousRefError

    try:
        d = resolve_pipeline_ref(ref)
    except AmbiguousRefError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(2) from e
    if d is None:
        known = [f"{p.pack}/{p.name}" for p in all_pipeline_defs()]
        console.print(f"[red]unknown pipeline {ref!r}[/] — installed: {known or '(none)'}")
        raise typer.Exit(2)

    inputs: dict = {}
    for pair in input_ or []:
        key, sep, raw = pair.partition("=")
        if not key or not sep:
            console.print(f"[red]--input must be key=value, got {pair!r}[/red]")
            raise typer.Exit(2)
        try:
            inputs[key] = json.loads(raw)
        except ValueError:
            inputs[key] = raw
    doc = asyncio.run(explain_pipeline(d, environment=environment, inputs=inputs,
                                       workspace_id=workspace))
    if json_out:
        console.print_json(json.dumps(doc, default=str))
        return
    # soft_wrap: the rendering is a table, and rich's default wrap at the
    # terminal width folds every row in half.
    console.print(render_explain(doc), markup=False, highlight=False, soft_wrap=True)
    # next-command discipline (#1792): the verb that acts on what was just read.
    console.print(f"\n  launch: cairn remote launch {d.pack}/{d.name} -i key=value"
                  f"   (add --preset <name> to launch from a saved set)",
                  markup=False, soft_wrap=True)


#: Pipeline states that mean the CLI's job is over (#1702).
_TERMINAL = ("done", "failed", "cancelled")


async def _drive_until_terminal(bridge, pipeline_id: str, *, poll_s: float = 2.0):
    """#1702 — keep the launching bridge ALIVE and report leg transitions until
    the pipeline terminates. Returns the final record.

    Under the in-process runtime the launching process's bridge is the ONLY
    thing driving the pipeline: `launch_pipeline` returns as soon as the first
    leg is submitted, so stopping the bridge there abandons the record at
    `status=running` with no executor behind it — exit 0, looks like success,
    and manufactures exactly the orphan class the reconciler never reaps
    (#1670). Observed live 2026-08-26 (`pl_61ac889bd661477a`, 13 minutes of
    nothing); the identical launch under a kept-alive bridge worked first try.
    """
    import asyncio

    last: dict[str, str] = {}
    while True:
        rec = await bridge.store.get(pipeline_id)
        if rec is None:
            return None
        for sid, st in rec.step_state.items():
            if last.get(sid) != st.status:
                last[sid] = st.status
                console.print(f"  [dim]leg[/dim] {sid:24s} → {st.status}"
                              + (f"  [dim]{st.run_id}[/dim]" if st.run_id else ""))
        if rec.status in _TERMINAL:
            return rec
        await asyncio.sleep(poll_s)


@app.command("run")
def run_pipeline(
    ref: str = typer.Argument(
        ..., help="`<pack>/<name>` or `pipeline:<pack>/<name>` — a pipeline, or a "
                  "FLOW, which runs as its one-leg pipeline (ADR-0105 D1)"),
    input_: list[str] = typer.Option(
        None, "--input", "-i", help="`key=value` (JSON parsed when possible); repeatable"),
    environment: str = typer.Option("", "--environment", "-e", help="environment to run in"),
    detach: bool = typer.Option(
        False, "--detach", help="Launch and exit without waiting. Under the in-process "
        "runtime this leaves the pipeline UNOWNED (nothing drives its legs) — exit 2."),
) -> None:
    """Launch a pipeline and drive it.

    The same door the console uses, so a run started here and one started from
    the UI are the same governed object. ``ref`` is resolved the way ``POST
    /start`` resolves it (:func:`resolve_start_ref`), which since ADR-0106 D1
    (#2134) means: a declared pipeline runs, a bare FLOW ref is refused with
    the one-leg wrapper to add, and a pipeline WINS over a flow at the same
    ref. ``cairn run`` keeps the flow-shaped surface — its inputs and the
    dev-loop flags, which need no pipeline at all (ADR-0106 D6).

    The difference from the console is ownership (#1702): its caller is a
    long-lived server; this process is the pipeline's owner only for as long as
    it lives. So by default the CLI STAYS: bridge up, leg transitions printed,
    exit code from the pipeline's own terminal state. ``--detach`` restores
    fire-and-forget — a fine thing under the Temporal runtime (the workflow
    owns the pipeline), and an explicit, loudly-flagged abandonment under the
    in-process one.
    """
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D: local compiles get local-dev

    default_dev_profile()
    import asyncio
    import json

    from cairn.core import liveness, settings
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge
    from cairn.core.engine.runner import Runner
    from cairn.core.storage import build_storage
    from cairn.core.dispatch.pipeline_launch import (
        MissingInputsError, PipelineNotFoundError, launch_pipeline,
    )
    from cairn.core.dispatch.start import (
        FlowNotRunnableError, UnknownStartRefError, resolve_start_ref,
    )
    from cairn.core.engine.pipeline_resolve import AmbiguousRefError

    inputs: dict = {}
    for pair in input_ or []:
        key, _, raw = pair.partition("=")
        if not key or not _:
            console.print(f"[red]--input must be key=value, got {pair!r}[/red]")
            raise typer.Exit(2)
        try:
            inputs[key] = json.loads(raw)
        except ValueError:
            inputs[key] = raw            # a bare string is the common case

    async def _go():
        # Resolved INSIDE, so an unknown ref or a bare flow ref lands in the
        # same except-block that renders every other launch refusal.
        _kind, bare = resolve_start_ref(ref)
        dispatcher = RunDispatcher(Runner(), backend=build_storage())
        bridge = PipelineBridge(dispatcher)
        # ADR-0125 D2 — this process runs in-process legs, so it holds an owner
        # lock for its life, as the server does: a server next to it can tell
        # its legs are alive, and it can tell the server's are.
        await liveness.start_process_owner(dispatcher.store)
        await bridge.start()
        try:
            rec = await launch_pipeline(
                f"pipeline:{bare}", inputs=inputs, bridge=bridge,
                environment=environment or None, trigger={"source": "cli"},
            )
            console.print(f"[green]launched[/green] {ref}  →  [bold]{rec.pipeline_id}[/bold]")
            console.print(f"  status: {rec.status}   legs: {len(rec.step_state)}")
            if detach or rec.status in _TERMINAL:
                return rec
            # #1702 — the whole fix: the bridge stays up until the pipeline no
            # longer needs an owner, instead of dying in a `finally` millis
            # after the first leg submits.
            return await _drive_until_terminal(bridge, rec.pipeline_id)
        finally:
            bridge.stop()
            await liveness.stop_process_owner()

    from cairn.core.policy.pipeline_gates import PolicyDeniedError  # noqa: PLC0415 - cycle: policy → engine
    from cairn.core.admission_door import AdmissionRefusedError  # noqa: PLC0415 — same

    try:
        rec = asyncio.run(_go())
    except FlowNotRunnableError as e:
        # ADR-0106 D1 — the ref names a component. Exit 2 (a refusal of the
        # request, not a failure of the launch), with the remedy in the message.
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(2) from e
    except (PipelineNotFoundError, AmbiguousRefError, MissingInputsError,
            UnknownStartRefError, NotImplementedError) as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1) from e
    except PolicyDeniedError as e:  # #1252 — the environment refused the launch
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(3) from e
    except AdmissionRefusedError as e:  # #2190 — the admission door refused
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(3) from e
    except KeyboardInterrupt:
        # The bridge died with this process; say so instead of pretending.
        console.print("[yellow]interrupted — the pipeline record survives, but under the "
                      "in-process runtime nothing is driving it now. A server restart "
                      "re-adopts ownership (not execution) of in-flight records.[/yellow]")
        raise typer.Exit(130) from None

    if detach and rec.status not in _TERMINAL and settings.runtime() != "temporal":
        # #1702 fix-direction 3: never exit 0 after abandoning a pipeline.
        console.print(f"[yellow]detached — {rec.pipeline_id} is UNOWNED under the "
                      f"in-process runtime: its legs will not advance. Drive it from a "
                      f"server, or rerun without --detach.[/yellow]")
        raise typer.Exit(2)

    console.print(f"  final: [bold]{rec.status}[/bold]"
                  + (f"   [red]{rec.trigger.get('failure_reason', '')}[/red]"
                     if rec.status == "failed" else ""))
    console.print(f"  inspect: [dim]cairn pipeline show {ref}[/dim]")
    if rec.status == "failed":
        raise typer.Exit(1)
    if rec.status == "cancelled":
        raise typer.Exit(1)


@app.command("respell-packs")
def respell_packs_cmd(
    apply: bool = typer.Option(False, "--apply", help="Write the rewrite. Without it nothing is written."),
    server_stopped: bool = typer.Option(
        False, "--server-stopped",
        help="Required with --apply: the server and workers are stopped, so nothing writes these rows meanwhile."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print sample before/after pairs per table."),
) -> None:
    """Rewrite recorded refs of the three hyphenated packs to their manifest spelling (#2527).

    `dataset_readiness/…`, `security_triage/…` and `security_kb/…` (the directory
    spelling a source checkout recorded) become `dataset-readiness/…`,
    `security-triage/…` and `security-kb/…` (the pack.toml name every install now
    records) in runs, pipeline records, workflow configs and presets, the
    workflow registry, schedules and triggers.

    Only identity moves: a run's template_ref and fingerprint, a pipeline record's
    bundle_ref and step refs, a schedule's workflow, and the ids that embed a ref
    (configs, presets, registry rows, declared schedules and triggers), which are
    re-keyed. Inputs, results and outputs are never read. A re-key whose target id
    already exists is reported as a conflict, not applied.

    Order on an upgrade: stop the server and workers, install the new version, take
    a backup, run this with --apply --server-stopped, then start. A server started
    first re-arms declared schedules under the new spelling and sweeps the old rows
    itself, but every other recorded ref stays mixed until this runs.

    Dry run by default: --apply is the only thing that writes, and it needs
    --server-stopped. Each write is compare-and-set, so a row changed meanwhile is
    skipped and reported. Idempotent: a second --apply finds nothing. Never touches
    the audit log (hash-chained), run event logs or content-addressed registry rows.
    Not exactly reversible, so take a backup first.

    The stores are whatever this environment selects: CAIRN_STORAGE=postgres://…
    for prod, else each store's sqlite file (CAIRN_*_DB / CAIRN_SQLITE_DB).
    """
    from cairn.core.storage.respell_packs import NEVER, respell_packs

    if apply and not server_stopped:
        console.print("[red]refusing to --apply[/] while the server may be writing these rows: stop the "
                      "server and workers, take a backup, then pass --server-stopped")
        raise typer.Exit(2)
    rep = respell_packs(apply=apply)
    head = "[bold]applied[/]" if apply else "[bold]dry run[/] (nothing written)"
    console.print(f"{head} — {rep.summary()}")
    if verbose:
        for table, rows in rep.samples.items():
            console.print(f"  [bold]{table}[/]")
            for key, what in rows:
                console.print(f"    {key}  {what}")
    for line in rep.conflicts:
        console.print(f"  [yellow]conflict[/] {line} [dim]— resolve by hand: keep one row[/]")
    for line in rep.raced:
        console.print(f"  [yellow]changed meanwhile, skipped[/] {line} [dim]— run again once quiet[/]")
    if rep.missing:
        console.print(f"  [dim]not present here: {', '.join(rep.missing)}[/]")
    console.print(f"  [dim]never rewritten: {'; '.join(NEVER)}[/]")
    if not apply and rep.total:
        console.print("  [dim]take a backup, then re-run with --apply[/]")


@app.command("backfill")
def backfill_cmd(
    apply: bool = typer.Option(
        False, "--apply", help="WRITE. Without it this only reports what it would do."),
    revert: bool = typer.Option(
        False, "--revert", help="Undo: remove every backfilled record and clear the "
                                "bundle_pipeline_id it stamped on its run."),
    batch_size: int = typer.Option(
        200, "--batch-size", help="Runs per keyset page — a large table is never "
                                  "loaded whole."),
    workspace: str = typer.Option(
        "", "--workspace", help="Only this workspace (default: every workspace)."),
    limit: int = typer.Option(
        0, "--limit", help="Stop after scanning this many runs (0 = all)."),
    infer_ref: bool = typer.Option(
        False, "--infer-ref", help="For a run with no template_ref, ask the canonical "
        "resolver (results / trigger payload / fingerprint) instead of skipping it. "
        "Second-hand, so opt-in; such records are marked trigger.ref_inferred."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="List every run."),
) -> None:
    """Give every pipeline-less run its one-leg pipeline (#2128 item C).

    ADR-0105 D1: every run is a leg of a pipeline. #2133 made that true at the
    chokepoint, for every run created from now on; this makes it true of the
    HISTORY, so the count of runs with no bundle_pipeline_id reaches 0 — the
    invariant is total, with no permanent "legacy" label.

    Dry run by default. --apply is the only thing that writes, so a prod
    operator sees the number before touching anything.

    Idempotent and resumable: the scan predicate is "the run has no pipeline",
    which a successful wrap clears, so a second pass finds nothing and an
    interrupted pass resumes where it stopped. Paging is keyset by run id,
    never OFFSET, so a large table is never loaded whole.

    Reversible: every record it writes carries trigger.source = backfill.
    --revert is that predicate; the raw SQL for both tiers is in
    cairn.core.storage.backfill_pipelines if you would rather see the rows.

    The store is whatever this environment selects: CAIRN_STORAGE=postgres://…
    for prod, else the sqlite file (CAIRN_SQLITE_DB / ~/.cairn/cairn.db).
    """
    import asyncio

    from cairn.core.storage import default_store
    from cairn.core.storage.backfill_pipelines import (
        backfill_orphan_runs, revert_backfill,
    )
    from cairn.core.storage.pipeline_store import default_pipeline_store

    store, pstore = default_store(), default_pipeline_store()

    if revert:
        rep = asyncio.run(revert_backfill(store=store, pipeline_store=pstore, apply=apply))
        head = "[bold]reverting[/]" if apply else "[bold]dry run[/] (nothing written)"
        console.print(f"{head} — {rep.summary()}")
        if verbose:
            for pid in rep.pipeline_ids:
                console.print(f"  [dim]{pid}[/]")
        if not apply and rep.found:
            console.print("  [dim]re-run with --apply to remove them[/]")
        return

    rep = asyncio.run(backfill_orphan_runs(
        store=store, pipeline_store=pstore, apply=apply,
        batch_size=batch_size, workspace_id=workspace or None, limit=limit,
        infer_ref=infer_ref,
    ))
    head = "[bold]applying[/]" if apply else "[bold]dry run[/] (nothing written)"
    console.print(f"{head} — {rep.summary()}")
    if verbose:
        for run_id, pid in rep.pairs:
            console.print(f"  [green]{run_id}[/]  →  {pid}")
    if rep.skipped:
        # Two reasons, and they need different decisions from the operator: a
        # run with no `template_ref` has no flow address to make a leg of (and
        # inventing one would put a false ref in the audit trail), while a
        # non-terminal run is a zombie the bridge would ACT on if we adopted it.
        # Grouped, so the count against each reason is readable — these are the
        # rows that keep the invariant's count above 0.
        console.print(f"[yellow]{len(rep.skipped)} run(s) cannot be backfilled[/]")
        no_ref = [r for r, why in rep.skipped.items() if "no template_ref" in why]
        zombie = [r for r, why in rep.skipped.items() if "not terminal" in why]
        if no_ref:
            console.print(f"  [yellow]{len(no_ref)}[/] with no template_ref "
                          f"[dim]— no flow address to make a leg of[/]")
            for run_id in no_ref:
                console.print(f"    [dim]{run_id}[/]")
            if not infer_ref:
                console.print("    [dim]--infer-ref asks the canonical resolver "
                              "(results / trigger payload / fingerprint) instead[/]")
        if zombie:
            console.print(f"  [yellow]{len(zombie)}[/] not terminal [dim]— a zombie "
                          f"run; a record for it would be resumed by the bridge, so "
                          f"it is reported rather than adopted[/]")
            for run_id in zombie:
                console.print(f"    [dim]{run_id}[/]")
    if not apply and rep.wrapped:
        console.print("  [dim]re-run with --apply to write them[/]")
