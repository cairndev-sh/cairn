"""A keyless install says "no model configured"; no placeholder key reaches a provider.

The 0.1.2 verification found ``cairn dev``, ``cairn run --mock`` and ``cairn gate
--mock`` sending agentic model calls to the vendor with ``sk-validate-offline``:
``cairn.core.validate`` stamped that key into ``os.environ`` at import, the CLI
imports it on every command, and ``cairn dev`` copied the environment into the
engine. The call came back as a 401 naming a key nobody set.

Pinned here: (1) importing the CLI leaves ``OPENAI_API_KEY`` unset, so ``cairn
dev``'s engine env carries no key; (2) a model built without a key refuses to
call the provider with its compile-only placeholder and names the fix; (3) a
keyless OpenAI-compatible endpoint (``OPENAI_BASE_URL``, the hello README's stub)
still gets the call. No test here touches the network: the provider call is a
stub that records it was reached.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_openai import ChatOpenAI

from cairn.services.llm.factory import build_chat_model
from cairn.services.llm.redacting import ModelNotConfiguredError

_KEY_ENVS = ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_API_BASE", "LLM_GATEWAY")


def test_importing_the_cli_stamps_no_api_key(tmp_path):
    env = {k: v for k, v in os.environ.items() if k not in _KEY_ENVS}
    env["HOME"] = str(tmp_path)  # no ~/.cairn state
    code = (
        "import json, os\n"
        "import cairn.cli.main\n"
        "import cairn.core.validate\n"
        "from cairn.cli.commands.dev import dev_env\n"
        "print(json.dumps([os.environ.get('OPENAI_API_KEY'),\n"
        "                  dev_env(8471).get('OPENAI_API_KEY')]))\n"
    )
    r = subprocess.run([sys.executable, "-P", "-c", code], env=env, cwd=tmp_path,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout.strip().splitlines()[-1]) == [None, None]


@pytest.fixture
def provider(monkeypatch):
    """Stand in for the vendor: record every call that would leave the process."""
    calls: list[str] = []

    async def _agenerate(self, messages, stop=None, run_manager=None, **kw):
        calls.append(self.openai_api_key.get_secret_value() if self.openai_api_key else "")
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="ok"))])

    monkeypatch.setattr(ChatOpenAI, "_agenerate", _agenerate)
    for k in _KEY_ENVS:
        monkeypatch.delenv(k, raising=False)
    return calls


def test_a_keyless_model_refuses_to_call_the_provider(provider):
    llm = build_chat_model("gpt-4o-mini")  # compiles offline: no key needed to construct
    with pytest.raises(ModelNotConfiguredError, match="no model configured.*OPENAI_API_KEY"):
        asyncio.run(llm.ainvoke([HumanMessage(content="hi")]))
    assert provider == []


def test_a_keyless_endpoint_still_gets_the_call(provider, monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "http://127.0.0.1:9/v1")
    llm = build_chat_model("gpt-4o-mini")
    assert asyncio.run(llm.ainvoke([HumanMessage(content="hi")])).content == "ok"
    assert len(provider) == 1


def test_a_real_key_is_used_as_given(provider, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-real-looking")
    llm = build_chat_model("gpt-4o-mini")
    assert asyncio.run(llm.ainvoke([HumanMessage(content="hi")])).content == "ok"
    assert provider == ["sk-real-looking"]
