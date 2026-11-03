"""How far past a cost cap a template's spend gets before it stops: no API key, no spend.

Runs a real engine with a keyless stub model on the in-process and DBOS runtimes, and launches two
pipelines from ``tests/e2e/hello/budget-pack``. In one, a template makes six model calls in a row under
a cap that the third crosses. In the other, a template makes eight calls at once. The leg must fail, the
pipeline must fail, and the leg after it must never start. Calls in a row must stop on the one that
crossed the cap, so what they pass it by is less than one call. If any of this differs, the published
claim would be wrong: the script exits non-zero and writes no result.

The parallel row is what the cap can't hold: calls already sent are paid for. So it shows the spend the
run recorded next to the calls the model received.

Temporal isn't measured, because this needs a Temporal server. The result says so, in its own row.

    .venv/bin/python benchmark/budget_overshoot.py [--out benchmark/results]
"""
from __future__ import annotations

import argparse
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import yaml

from _common import REPO, RESULTS, LocalEngine, wait_for, write_result

sys.path.insert(0, str(REPO))

PACK = REPO / "tests" / "e2e" / "hello" / "budget-pack"
SEQUENTIAL = "hello-budget/capped"
PARALLEL = "hello-budget/capped-fan-out"
ITEMS = list("abcdefgh")
SEQUENTIAL_CALLS = 6
#: every trial of each pipeline, because how many parallel calls get counted depends on timing
TRIALS = 3
#: long enough that the parallel calls are all sent before the first answer comes back
CHAT_DELAY_SECONDS = 0.2
CHAT = "/chat/completions"
COLUMNS = {"runtime": "Runtime", "calls": "Template", "cap": "Cap", "spent": "Spend recorded",
           "past": "Past the cap", "model": "Calls the model received", "ended": "Leg and pipeline"}
NOTES = [
    "A cap can't take back the call that crosses it, so calls in a row pass it by at most one call. "
    "Here that's half a call, and it's 20% only because this test's cap is just 2.5 calls.",
    "Calls sent at once can pass a cap by nearly all of them: all 8 reached the model before any answer came "
    "back, and the second answer is the one that crosses this cap. The percentage depends on how many calls "
    "there are and how small the cap is. A call answered after the run stops could be paid for without being "
    "counted. These runs didn't show that, but it would make the recorded spend too low.",
    "Temporal isn't measured, because this script needs a Temporal server. From the code, the worker only "
    "meters, so the run should fail when it ends, with its calls already made.",
]


@dataclass(frozen=True)
class Trial:
    """One pipeline launched and followed to its end."""
    pipeline_status: str
    leg_status: str
    next_leg_run: Any
    error: str
    spent_usd: float
    model_calls: int


def declared_cap(flow: str) -> float:
    """The ``budget.max_cost_usd`` the template declares, so this script can't drift from it."""
    text = (PACK / "flows" / f"{flow}.md").read_text(encoding="utf-8")
    return float(yaml.safe_load(text.split("---")[1])["budget"]["max_cost_usd"])


def check_stopped(trial: Trial) -> None:
    """What both pipelines must do: the leg fails, the pipeline stops, and nothing runs after it."""
    if trial.leg_status != "failed" or trial.pipeline_status != "failed":
        raise RuntimeError(f"a capped leg must fail its pipeline: {trial}")
    if trial.next_leg_run:
        raise RuntimeError(f"the leg after the cap ran: {trial}")
    if "over budget" not in trial.error:
        raise RuntimeError(f"the leg's error must say the cap stopped it: {trial}")


def check_sequential(trials: list[Trial], cap: float) -> None:
    """Calls in a row stop on the one that crossed the cap, every time."""
    for trial in trials:
        check_stopped(trial)
        if not 0 < trial.model_calls < SEQUENTIAL_CALLS:
            raise RuntimeError(f"{SEQUENTIAL} must stop before its {SEQUENTIAL_CALLS} calls: {trial}")
        one_call = trial.spent_usd / trial.model_calls
        if not cap < trial.spent_usd <= cap + one_call:
            raise RuntimeError(f"{SEQUENTIAL} must pass its ${cap} cap by at most one call: {trial}")
    if len({(t.model_calls, t.spent_usd) for t in trials}) != 1:
        raise RuntimeError(f"{SEQUENTIAL} must stop on the same call every time: {trials}")


def check_parallel(trials: list[Trial], cap: float, one_call_usd: float) -> None:
    """Calls at once stop the run, and the run never records more than the model received."""
    for trial in trials:
        check_stopped(trial)
        if trial.spent_usd <= cap or not 0 < trial.model_calls <= len(ITEMS):
            raise RuntimeError(f"{PARALLEL} must pass its ${cap} cap and make at most {len(ITEMS)} calls: {trial}")
        if trial.spent_usd > trial.model_calls * one_call_usd + 1e-9:
            raise RuntimeError(f"{PARALLEL} recorded more than the model received at ${one_call_usd} a call: {trial}")


def _usd(value: float) -> str:
    return f"${value:.6f}"


def _span(values: list[int]) -> str:
    return str(values[0]) if min(values) == max(values) else f"{min(values)} to {max(values)}"


def row(runtime: str, what: str, trials: list[Trial], cap: float, declared: int) -> dict[str, str]:
    """The worst trial's spend, so the table never reads better than a run did."""
    spent = max(t.spent_usd for t in trials)
    return {
        "runtime": runtime, "calls": what, "cap": _usd(cap), "spent": _usd(spent),
        "past": f"{_usd(spent - cap)} ({(spent - cap) / cap:.0%})",
        "model": f"{_span([t.model_calls for t in trials])} of {declared}",
        "ended": f"{trials[0].leg_status}, next leg never started",
    }


NOT_MEASURED = {"runtime": "Temporal", "calls": "not measured", "cap": "n/a", "spent": "n/a", "past": "n/a",
                "model": "n/a", "ended": "n/a"}


def _one_trial(client: Any, stub: Any, ref: str, inputs: dict) -> Trial:
    before = stub.calls[CHAT]
    pipeline_id = client.launch_pipeline(f"pipeline:{ref}", inputs)["pipeline_id"]
    record = wait_for(lambda: client.pipeline(pipeline_id),
                      lambda r: r.get("status") in ("done", "failed", "cancelled"),
                      what=f"{ref} to end", timeout=180.0)
    time.sleep(CHAT_DELAY_SECONDS * 2)  # a request already sent is counted when it arrives
    steps = record["step_state"]
    run = client.run(steps["calls"]["run_id"])
    return Trial(pipeline_status=record["status"], leg_status=run["status"],
                 next_leg_run=steps["after"].get("run_id"), error=run.get("error") or "",
                 spent_usd=float((run.get("cost") or {}).get("llm_usd") or 0.0),
                 model_calls=stub.calls[CHAT] - before)


def engine_env(model_url: str) -> dict[str, str]:
    """Point every way out at the stub, so a key or gateway in this shell can't be spent on."""
    return {"CAIRN_PACK_PATH": str(PACK), "OPENAI_BASE_URL": model_url, "OPENAI_API_BASE": model_url,
            "OPENAI_API_KEY": "sk-stub", "LLM_GATEWAY": "direct", "MAX_COST_USD": "1"}


def measure(runtime: str, env: dict[str, str], stub: Any, make_client: Callable[[str], Any]) -> list[dict[str, str]]:
    with LocalEngine(env={**engine_env(stub.base_url), **env}) as engine:
        client = make_client(engine.url)
        try:
            stub.chat_delay = 0.0
            sequential = [_one_trial(client, stub, SEQUENTIAL, {"question": "q", "items": ITEMS})
                          for _ in range(TRIALS)]
            stub.chat_delay = CHAT_DELAY_SECONDS
            parallel = [_one_trial(client, stub, PARALLEL, {"question": "q", "items": ITEMS})
                        for _ in range(TRIALS)]
        except BaseException:
            print(engine.log_tail(), file=sys.stderr)
            raise
    sequential_cap, parallel_cap = declared_cap("six-calls"), declared_cap("fan-out")
    check_sequential(sequential, sequential_cap)
    check_parallel(parallel, parallel_cap, sequential[0].spent_usd / sequential[0].model_calls)
    return [row(runtime, f"{SEQUENTIAL_CALLS} calls in a row", sequential, sequential_cap, SEQUENTIAL_CALLS),
            row(runtime, f"{len(ITEMS)} calls at once", parallel, parallel_cap, len(ITEMS))]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=RESULTS)
    args = parser.parse_args()

    from cairn_client import CairnClient  # noqa: PLC0415 — after --help, which needs no install
    from tests.e2e.model_stub import ModelStub  # noqa: PLC0415 — after sys.path has the repo

    def make_client(url: str) -> CairnClient:
        client = CairnClient(url)
        client.login("dev", "dev")
        return client

    stub = ModelStub().start()
    try:
        with tempfile.TemporaryDirectory(prefix="cairn-benchmark-") as scratch:
            dbos = {"CAIRN_RUNTIME": "dbos", "OBS_DBOS_DSN": f"sqlite:///{scratch}/dbos.sqlite",
                    "OBS_CHECKPOINTER": "sqlite", "OBS_CHECKPOINTER_PATH": f"{scratch}/checkpoints.sqlite"}
            results = [*measure("in-process", {}, stub, make_client), *measure("DBOS on SQLite", dbos, stub, make_client),
                       NOT_MEASURED]
    finally:
        stub.stop()

    path = write_result(args.out, "budget_overshoot", title="How far past a cost cap a template's spend gets", inputs={
        "pipelines": [SEQUENTIAL, PARALLEL], "trials": TRIALS, "chat_delay_seconds": CHAT_DELAY_SECONDS,
    }, results=results, extra={"columns": COLUMNS, "notes": NOTES})
    print(path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
