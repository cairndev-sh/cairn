"""The ONE generic Metaflow bridge flow (#1078, ADR-0082 D1/D5/D7).

Never code-generated per pipeline: a single ``FlowSpec`` that receives a
``DataflowSpec`` as a parameter and executes it through the SAME
``run_dataflow()`` runner the builtin engine runs — the ``execute`` step is a
thin adapter, not a second stage interpreter (#1243; the #1048 anti-deviation
rule the runner's own docstring cites: "bespoke runner code is what would
turn every future adapter into a rewrite"). Stage resume markers, budget
self-enforcement, and per-stage ``ArtifactDescriptor`` emission are therefore
the runner's job, not this bridge's.

Run standalone (which is what makes it testable):

    python -m operator_dataflow_metaflow.metaflow_bridge run --spec_json '{"stages":[…]}'

Progress is written to ``workdir/progress.json`` after each stage rather than
read back from Metaflow artifacts, because artifacts only become readable once a
task COMPLETES — a poller needs mid-run stage progress (the D8a contract, #1076).
``run_dataflow``'s ``on_progress`` callback exists precisely so an adapter can
keep writing whatever file shape its own poller reads.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from metaflow import FlowSpec, Parameter, step  # noqa: PLC0415 - optional [metaflow] extra, module only imported by the adapter

from cairn.core.dataflow import DataflowSpec

from cairn.core.dataflow.runner import BudgetExceeded, run_dataflow


def _write(path: Path, payload: dict) -> None:
    """Atomic-ish write so a concurrent poller never reads a half file.

    The parent is created here because each REMOTE task runs in its OWN
    container: `start` may have made the workdir, but the `end` task is a
    different pod on a different filesystem, where that directory does not
    exist. Without this the flow dies at `end` with FileNotFoundError after
    all the real work succeeded (#1358). The file remains the LOCAL progress
    contract; for a remote run the authoritative result is the `result_payload`
    Metaflow artifact, which the engine reads via the client.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, path)


def _known_cost(metrics: dict) -> float | None:
    value = metrics.get("cost_usd")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return round(float(value), 6) if 0 < value < float("inf") else None


def _metrics_cost(workdir: Path) -> float | None:
    try:
        metrics = json.loads((workdir / "metrics.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return _known_cost(metrics) if isinstance(metrics, dict) else None


class CairnFlow(FlowSpec):
    """Straight-line stage interpreter. One step, N kernels, sequential —
    executed via :func:`run_dataflow`, never re-implemented here.

    Deliberately NOT one Metaflow step per stage: the stage list is dynamic and
    Metaflow's graph is static, so per-stage steps would mean code-generating a
    flow per pipeline — the thing D1 forbids. Per-stage @resources arrives with
    the foreach variant when a stage genuinely needs its own machine.
    """

    spec_json = Parameter("spec_json", default="{}", help="Serialized DataflowSpec.")
    workdir = Parameter("workdir", default="", help="Where progress/result land.")

    @step
    def start(self):
        self.spec = json.loads(self.spec_json)
        self._wd = Path(self.workdir or ".")
        self._wd.mkdir(parents=True, exist_ok=True)
        stages_total = len(self.spec.get("stages") or [])
        _write(self._wd / "progress.json", {
            "state": "running", "stage": None,
            "stages_done": 0, "stages_total": stages_total,
        })
        self.next(self.execute)

    @step
    def execute(self):
        """Run the parsed spec through the shared runner — stage resume,
        budget self-enforcement, and artifact descriptors come from
        ``run_dataflow`` itself, exactly like the builtin engine."""
        wd = Path(self.workdir or ".")
        parsed = DataflowSpec.model_validate(self.spec)

        def on_progress(progress: dict) -> None:
            # SAME progress.json shape the poller reads (D8a): state/stage/
            # stages_done/stages_total — on_progress supplies the mid-run
            # signal `run_dataflow` doesn't otherwise expose until a Metaflow
            # artifact is readable (i.e. after the whole step completes).
            payload = {
                "state": "running",
                "stage": progress.get("stage"),
                "stages_done": progress.get("stages_done", 0),
                "stages_total": progress.get("stages_total", len(parsed.stages)),
            }
            cost_usd = _known_cost(progress)
            if cost_usd is not None:
                payload["cost_usd"] = cost_usd
            _write(wd / "progress.json", payload)

        try:
            result = run_dataflow(parsed, wd, on_progress=on_progress)
        except BudgetExceeded as e:
            _write(wd / "result.json", {"error": str(e), "kind": "budget"})
            self.failure = f"BudgetExceeded: {e}"
            self.partial_cost_usd = _metrics_cost(wd)
            self.next(self.failed)
            return
        except Exception as e:  # noqa: BLE001 — a failed dataflow is a REAL failure (#568)
            _write(wd / "result.json", {"error": f"{type(e).__name__}: {e}"})
            self.failure = f"{type(e).__name__}: {e}"
            self.partial_cost_usd = _metrics_cost(wd)
            self.next(self.failed)
            return
        self.result_payload = result.model_dump()
        self.next(self.end)

    @step
    def failed(self):
        raise RuntimeError(self.failure)

    @step
    def end(self):
        wd = Path(self.workdir or ".")
        parsed = DataflowSpec.model_validate(self.spec)
        payload = dict(self.result_payload)
        if parsed.parent_run:
            payload["lineage_parent_run"] = parsed.parent_run
        _write(wd / "result.json", payload)
        stages_total = len(parsed.stages)
        progress = {
            "state": "succeeded", "stage": None,
            "stages_done": stages_total, "stages_total": stages_total,
        }
        cost_usd = _known_cost(payload)
        if cost_usd is not None:
            progress["cost_usd"] = cost_usd
        _write(wd / "progress.json", progress)


if __name__ == "__main__":
    CairnFlow()
