"""The direct-template CLI path isolates model calls in ``--mock`` mode."""
from __future__ import annotations

import json
import os
from pathlib import Path

from langchain_openai import ChatOpenAI
from pydantic import BaseModel
import typer

import cairn.core.operator as operator_api
from cairn.cli.commands import run as run_mod
from cairn.core.engine import runner as engine_runner
from cairn.core.operator import Operation
from cairn.services.llm.redacting import MOCK_LLM_RESPONSE


def _write_inputs(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("CAIRN_RUNS_DIR", str(tmp_path / "runs"))
    input_path = tmp_path / "in.json"
    input_path.write_text(json.dumps({"indicator": "203.0.113.7"}), encoding="utf-8")
    return input_path


def _invoke_mock_run(template_ref: str, input_path: Path) -> int:
    try:
        run_mod.cmd(
            trigger_file=template_ref,
            source=None,
            service=None,
            environment="dev",
            input_file=str(input_path),
            dry_run=False,
            mock=True,
            mode="live",
            interactive=False,
            detach=False,
            from_run=None,
            series=None,
            set_=None,
        )
    except typer.Exit as exc:
        return exc.exit_code
    raise AssertionError("the run command did not exit")


def test_mock_prompt_flow_returns_deterministic_output_without_provider(
    tmp_path, monkeypatch, scratch_pack, dev_packs,
):
    input_path = _write_inputs(tmp_path, monkeypatch)
    pack = scratch_pack("mock_llm", "mock-llm")
    (pack / "flows" / "echo.md").write_text(
        "---\nname: echo\nversion: 0.1.0\n"
        "flow:\n  type: prompt\n  model: gpt-4o-mini\n"
        "---\n\nReturn a concise response.\n",
        encoding="utf-8",
    )
    dev_packs(pack.parent)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-invalid-test-key")

    provider_calls: list[bool] = []

    async def _provider_call(self, messages, stop=None, run_manager=None, **kwargs):
        provider_calls.append(True)
        raise AssertionError("mock mode reached the provider client")

    monkeypatch.setattr(ChatOpenAI, "_agenerate", _provider_call)
    exit_code = _invoke_mock_run("mock-llm/echo", input_path)
    run_id = next(path.name for path in (tmp_path / "runs").iterdir())

    metadata = json.loads((Path(os.environ["CAIRN_RUNS_DIR"]) / run_id / "meta.json").read_text())
    assert exit_code == 0
    assert metadata["status"] == "done"
    assert metadata["results"][0]["output"] == {
        "type": "prompt",
        "output": MOCK_LLM_RESPONSE,
    }
    assert provider_calls == []


def test_mock_refuses_nested_unsupported_operator_before_execution(
    tmp_path, monkeypatch, scratch_pack, dev_packs,
):
    input_path = _write_inputs(tmp_path, monkeypatch)
    pack = scratch_pack("mock_unsafe", "mock-unsafe")
    (pack / "flows" / "echo.md").write_text(
        "---\nname: echo\nversion: 0.1.0\n"
        "flow:\n  type: state_machine\n  root:\n"
        "    type: subgraph_node\n    template: mock-unsafe/unsafe\n  nodes: {}\n"
        "---\n\nParent flow.\n",
        encoding="utf-8",
    )
    (pack / "flows" / "unsafe.md").write_text(
        "---\nname: unsafe\nversion: 0.1.0\n"
        "flow:\n  type: state_machine\n  root:\n"
        "    type: operation\n    operator: test/unsafe\n"
        "    input:\n      value: $input.indicator\n"
        "    output:\n      result: $state.final_output\n  nodes: {}\n"
        "---\n\nNested operation.\n",
        encoding="utf-8",
    )
    dev_packs(pack.parent)

    class _Input(BaseModel):
        value: str

    class _Output(BaseModel):
        result: str

    calls: list[bool] = []

    class UnsafeOperation(Operation[_Input, _Output]):
        name = "test/unsafe"
        version = "0.1.0"
        input_schema = _Input
        output_schema = _Output

        def run(self, inp, ctx):
            calls.append(True)
            return _Output(result=inp.value)

    original_loader = operator_api.load_all_operators

    def _load_with_unsafe(*args, **kwargs):
        original_loader(*args, **kwargs)
        operator_api.default_registry.register(UnsafeOperation)

    monkeypatch.setattr(operator_api, "load_all_operators", _load_with_unsafe)
    executions: list[bool] = []
    real_run = engine_runner.run_within_budget

    async def _capture(compiled, input_data):
        executions.append(True)
        return await real_run(compiled, input_data)

    monkeypatch.setattr(engine_runner, "run_within_budget", _capture)
    exit_code = _invoke_mock_run("mock-unsafe/echo", input_path)
    run_id = next(path.name for path in (tmp_path / "runs").iterdir())

    metadata = json.loads((Path(os.environ["CAIRN_RUNS_DIR"]) / run_id / "meta.json").read_text())
    assert exit_code == 1
    assert metadata["status"] == "failed"
    assert "test/unsafe" in metadata["error"]
    assert "do not declare supports_mock" in metadata["error"]
    assert calls == []
    assert executions == []
