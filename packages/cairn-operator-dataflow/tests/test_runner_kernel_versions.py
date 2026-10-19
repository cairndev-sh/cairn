"""#2188 (T03, C2) — the runner reports which kernel IMPLEMENTATION ran each
stage: ``<stage>.kernel_version`` from the distribution that registered the
entry point. Evidence beside the stage's own stats; the control plane folds it
into the version's check map."""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError, distribution, entry_points
from pathlib import Path

import pytest

from cairn.core.dataflow import DataflowSpec, StageRef
from cairn.core.dataflow.runner import run_dataflow
from cairn.core.dataflow.stage_loader import stage_versions


def _noop_dist_version() -> str:
    """The version of whichever distribution registers `noop`: cairn-operator-dataflow in the
    monorepo, `cairndev` in the public wheel (ADR-0137 D2). Never a hard-coded name (D5)."""
    (ep,) = [e for e in entry_points(group="cairn.dataflow_stages") if e.name == "noop"]
    assert ep.dist.name in {"cairn-operator-dataflow", "cairndev"}, ep.dist.name  # an independent oracle
    return ep.dist.version


def _installed(name: str) -> bool:
    try:
        distribution(name)
    except PackageNotFoundError:
        return False
    return True


def test_every_registered_kernel_has_a_distribution_version():
    versions = stage_versions()
    assert versions["noop"] == _noop_dist_version()


@pytest.mark.skipif(not _installed("cairn-operator-dataset-checks"),
                    reason="`ingest.normalize` is registered by the platform-only "
                           "cairn-operator-dataset-checks, which is not installed")
def test_the_dataset_checks_kernels_register_their_own_distribution():
    assert "ingest.normalize" in stage_versions()


def test_the_runner_stamps_the_kernel_version_on_each_stage(tmp_path: Path):
    res = run_dataflow(DataflowSpec(stages=[StageRef(stage="noop")]), tmp_path)
    assert res.stats["noop.kernel_version"] == _noop_dist_version()


def test_a_resumed_stage_reports_the_version_that_produced_it(tmp_path: Path, monkeypatch):
    """The version rides the stage's own .done marker: a resume after an
    upgrade reports what RAN, not what is installed now."""
    spec = DataflowSpec(stages=[StageRef(stage="noop")])
    run_dataflow(spec, tmp_path)                                   # writes the marker with the version
    import cairn.core.dataflow.runner as runner_mod
    monkeypatch.setattr(runner_mod, "stage_versions", lambda: {"noop": "99.0.0"})   # an "upgrade"
    res = run_dataflow(spec, tmp_path)                             # resumes from the marker
    assert res.stats["noop.kernel_version"] == _noop_dist_version()


def test_kernels_from_the_flow_stages_group_get_a_version_too():
    pytest.importorskip("operator_training", reason="a cairn.flow_stages kernel pack must be installed")
    versions = stage_versions()
    assert any(name.startswith("train.") for name in versions), sorted(versions)[:10]
