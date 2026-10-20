"""The redaction chokepoint for LANGCHAIN-driven LLM calls (#1376, ADR-0028).

ADR-0028 puts redaction "before anything leaves to the vendor", and
``cairn.core.llm.client.llm_complete`` implements that for direct completions.
But the product's main LLM surface never calls it: template prompt nodes
(``compiler._build_prompt_node``), the chat router, and ``cairn ask``'s ReAct
loop all construct a LangChain chat model via
:func:`cairn.services.llm.factory.build_chat_model` and invoke it through
LangChain. So with a ``redact.text`` binding ACTIVE, every agentic run was
sending unredacted content to the vendor — the chokepoint existed and the
traffic went around it.

Why a model subclass and not a callback: LangChain callbacks observe, they do
not mutate the outbound prompt. Overriding the generation methods is the only
place that sits between "messages assembled" and "bytes on the wire", and it
survives ``bind_tools`` — tool-calling turns route through the same
``_agenerate``/``_astream`` on the underlying model, which is exactly where
tool RESULTS (run records, audit rows, knowledge chunks) would otherwise
egress unredacted.

Cost of the no-op path is one identity check: with no redactor bound,
``get_redactor()`` returns the shared ``NoOpRedactor`` and we delegate
untouched, so deployments without a binding pay nothing.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Iterator, Optional

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI

logger = logging.getLogger("cairn.services.llm.redacting")

#: The key ``build_chat_model`` injects when none is configured, so a template
#: COMPILES offline (``ChatOpenAI`` checks its key at construction, #412). It is
#: never a credential: a model holding it refuses to call a provider (below).
COMPILE_PLACEHOLDER_KEY = "sk-cairn-compile-placeholder"


class ModelNotConfiguredError(RuntimeError):
    """An LLM call was attempted with no model configured (no key, no endpoint)."""


def _refuse_without_model(model: "RedactingChatOpenAI") -> None:
    """Fail fast, before any network call, when the model holds only the compile
    placeholder and no endpoint is configured. Sending the placeholder to the
    vendor would only come back as a 401 that names a key nobody set. With
    ``OPENAI_BASE_URL``/``base_url`` set (a keyless OpenAI-compatible endpoint,
    such as a local stub), the call goes ahead: such an endpoint needs no key."""
    key = model.openai_api_key.get_secret_value() if model.openai_api_key else ""
    if key != COMPILE_PLACEHOLDER_KEY:
        return
    if model.openai_api_base or os.environ.get("OPENAI_BASE_URL") or os.environ.get("OPENAI_API_BASE"):
        return
    raise ModelNotConfiguredError(
        f"no model configured for {model.model_name!r}: set OPENAI_API_KEY, or "
        "OPENAI_BASE_URL for a keyless OpenAI-compatible endpoint (a local stub or "
        "gateway), or point LLM_GATEWAY at a gateway that holds the key")


async def redact_lc_messages(messages: list[BaseMessage]) -> list[BaseMessage]:
    """Redact the text content of LangChain messages. Identity when unbound.

    Mirrors ``obs.redact_messages`` (which speaks dicts) for the LangChain
    message shape. Only ``str`` content is redacted: multimodal/content-block
    payloads are passed through untouched rather than mangled — a limitation
    worth stating out loud, since an image-carrying message would bypass this.
    """
    from cairn.core.obs.redact import _NOOP, get_redactor  # noqa: PLC0415 — cycle: services → core

    r = get_redactor()
    if r is _NOOP:
        return messages
    out: list[BaseMessage] = []
    for m in messages:
        content = getattr(m, "content", None)
        if isinstance(content, str) and content:
            res = await r.redact_text(content)
            # copy() keeps the concrete message class + tool_call metadata;
            # rebuilding as a generic message would drop tool_call_id and
            # silently break the ReAct loop.
            out.append(m.model_copy(update={"content": res.text})
                       if res.redacted else m)
        else:
            out.append(m)
    return out


class RedactingChatOpenAI(ChatOpenAI):
    """``ChatOpenAI`` that routes outbound messages through the redactor.

    Async only, deliberately: cairn's run paths are async end to end, and a
    sync ``_generate`` cannot await the (async) redactor. A sync call would
    silently skip redaction, so it is refused rather than allowed to look like
    it worked.
    """

    async def _agenerate(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                         run_manager: Any = None, **kwargs: Any) -> Any:
        _refuse_without_model(self)
        return await super()._agenerate(
            await redact_lc_messages(messages), stop=stop, run_manager=run_manager, **kwargs)

    async def _astream(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                       run_manager: Any = None, **kwargs: Any) -> Any:
        _refuse_without_model(self)
        async for chunk in super()._astream(
                await redact_lc_messages(messages), stop=stop,
                run_manager=run_manager, **kwargs):
            yield chunk

    def _generate(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                  run_manager: Any = None, **kwargs: Any) -> Any:
        from cairn.core.obs.redact import _NOOP, get_redactor  # noqa: PLC0415 — cycle

        _refuse_without_model(self)
        if get_redactor() is not _NOOP:
            raise RuntimeError(
                "a redact.text binding is active but this model was invoked "
                "SYNCHRONOUSLY — redaction is async and would be skipped. Use "
                "ainvoke/astream (cairn's run paths are async), or unbind "
                "redact.text if this path is genuinely exempt (#1376)."
            )
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    def _stream(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                run_manager: Any = None, **kwargs: Any) -> Iterator[Any]:
        _refuse_without_model(self)
        return super()._stream(messages, stop=stop, run_manager=run_manager, **kwargs)


__all__ = ["COMPILE_PLACEHOLDER_KEY", "ModelNotConfiguredError", "RedactingChatOpenAI",
           "redact_lc_messages"]
