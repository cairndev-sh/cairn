"""`examples/holdout`, on a real server: the leaky version is refused at launch, the clean one
starts, and a cutoff before the overlapping row's date admits the leaky one (#333)."""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import Cell, cell_params

pytestmark = [pytest.mark.e2e]

PACK = Path(__file__).resolve().parents[3] / "examples" / "holdout"
TRAIN = "holdout-example/train"


def _register(api, source: str, name: str, role: str) -> None:
    pid = api.start("holdout-example/register", {"source": str(PACK / "data" / source), "name": name, "role": role})
    rec = api.wait_terminal(pid, timeout=120)
    assert rec["status"] == "done", rec.get("trigger", {}).get("failure_reason")


@pytest.mark.parametrize("cell", cell_params())
def test_the_holdout_example_refuses_the_leaky_set(cell: Cell, server_factory) -> None:
    srv = server_factory(cell, "holdout-example", pack_path=str(PACK))
    api = srv.api()
    _register(api, "eval.jsonl", "eval-set", "test")
    _register(api, "train-leaky.jsonl", "train-leaky", "training")
    _register(api, "train-clean.jsonl", "train-clean", "training")
    before = len(api.c.get("/pipelines").json().get("pipelines", []))

    refused = api.c.post("/start", json={"ref": TRAIN, "inputs": {"train_version": "dataset:train-leaky@latest"}})
    assert refused.status_code == 403, refused.text
    assert "shares 1 row(s) with the test set dataset:eval-set@1 (exact): e1" in refused.text
    assert len(api.c.get("/pipelines").json().get("pipelines", [])) == before, "a refused launch has no record"

    bad = api.c.post("/start", json={"ref": TRAIN, "inputs": {"train_version": "dataset:train-clean@latest",
                                                              "eval_cutoff": "last tuesday"}})
    assert bad.status_code == 403 and "eval_cutoff" in bad.text, bad.text

    clean = api.start(TRAIN, {"train_version": "dataset:train-clean@latest"})
    api.wait_step(clean, "approve", "awaiting_approval", timeout=60)
    cut = api.start(TRAIN, {"train_version": "dataset:train-leaky@latest", "eval_cutoff": "2026-06-01"})
    api.wait_step(cut, "approve", "awaiting_approval", timeout=60)
