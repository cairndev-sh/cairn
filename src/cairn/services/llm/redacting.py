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

import json
import logging
import os
from typing import Any, Iterator, Optional

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_openai import ChatOpenAI

logger = logging.getLogger("cairn.services.llm.redacting")
MOCK_LLM_RESPONSE = "Mock LLM response."

#: The key ``build_chat_model`` injects when none is configured, so a template
#: COMPILES offline (``ChatOpenAI`` checks its key at construction, #412). It is
#: never a credential: a model holding it refuses to call a provider (below).
COMPILE_PLACEHOLDER_KEY = "sk-cairn-compile-placeholder"


class ModelNotConfiguredError(RuntimeError):
    """An LLM call was attempted with no model configured (no key, no endpoint)."""


def _mock_mode_active() -> bool:
    from cairn.core.operator.runtime import mock_mode_active  # noqa: PLC0415 — avoid services/core import cycle

    return mock_mode_active()


def _resolve_schema_ref(root_schema: dict[str, Any], reference: str) -> dict[str, Any] | None:
    if not reference.startswith("#/"):
        return None
    value: Any = root_schema
    for part in reference[2:].split("/"):
        key = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value if isinstance(value, dict) else None


def _schema_mock_value(
    schema: dict[str, Any],
    root_schema: dict[str, Any] | None = None,
    resolved_refs: frozenset[str] = frozenset(),
) -> Any:
    """Build a minimal deterministic JSON value for structured-output calls."""
    root_schema = schema if root_schema is None else root_schema
    reference = schema.get("$ref")
    if isinstance(reference, str):
        if reference in resolved_refs:
            return {}
        resolved = _resolve_schema_ref(root_schema, reference)
        if resolved is not None:
            return _schema_mock_value(resolved, root_schema, resolved_refs | {reference})

    if "const" in schema:
        return schema["const"]
    if schema.get("enum"):
        return schema["enum"][0]
    for union_key in ("anyOf", "oneOf"):
        variants = schema.get(union_key)
        if isinstance(variants, list):
            if any(variant.get("type") == "null" for variant in variants):
                return None
            non_null = [variant for variant in variants if variant.get("type") != "null"]
            if not non_null:
                return None
            return _schema_mock_value(non_null[0], root_schema, resolved_refs)

    kind = schema.get("type")
    if isinstance(kind, list):
        kind = next((item for item in kind if item != "null"), "null")
    if kind == "object" or "properties" in schema:
        properties = schema.get("properties") or {}
        required = schema.get("required") or []
        return {
            name: _schema_mock_value(properties[name], root_schema, resolved_refs)
            for name in required
            if name in properties
        }
    if kind == "array":
        return []
    if kind == "boolean":
        return False
    if kind == "integer":
        return int(schema.get("minimum", 0))
    if kind == "number":
        return float(schema.get("minimum", 0.0))
    if kind == "string":
        return "mock"
    if kind == "null":
        return None
    return {}


def _json_schema(schema: Any) -> dict[str, Any] | None:
    if isinstance(schema, dict):
        function = schema.get("function")
        if isinstance(function, dict) and isinstance(function.get("parameters"), dict):
            return function["parameters"]
        nested = schema.get("schema")
        if isinstance(nested, dict):
            return nested
        if "properties" in schema or "type" in schema:
            return schema
    if isinstance(schema, type):
        schema_factory = getattr(schema, "model_json_schema", None)
        if not callable(schema_factory):
            schema_factory = getattr(schema, "schema", None)
        if callable(schema_factory):
            result = schema_factory()
            if isinstance(result, dict):
                return result
    return None


def _tool_choice_name(choice: Any) -> str | None:
    if isinstance(choice, str) and choice not in {"auto", "none", "required"}:
        return choice
    if isinstance(choice, dict):
        function = choice.get("function")
        name = function.get("name") if isinstance(function, dict) else None
        if isinstance(name, str):
            return name
    return None


def _mock_response_parts(
    kwargs: dict[str, Any],
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    structured = kwargs.get("ls_structured_output_format")
    structured = structured if isinstance(structured, dict) else {}
    schema = _json_schema(structured.get("schema"))
    structured_kwargs = structured.get("kwargs")
    method = structured_kwargs.get("method") if isinstance(structured_kwargs, dict) else None
    tool_name = _tool_choice_name(kwargs.get("tool_choice"))

    if schema is None:
        response_format = kwargs.get("response_format")
        if isinstance(response_format, type):
            schema = _json_schema(response_format)
            method = "json_schema"
        elif isinstance(response_format, dict):
            json_schema = response_format.get("json_schema")
            if isinstance(json_schema, dict):
                schema = _json_schema(json_schema.get("schema"))
                method = "json_schema"

    if schema is None and tool_name is not None:
        tools = kwargs.get("tools")
        if isinstance(tools, list):
            for tool in tools:
                if not isinstance(tool, dict):
                    continue
                function = tool.get("function")
                if isinstance(function, dict) and function.get("name") == tool_name:
                    schema = _json_schema(tool)
                    method = "function_calling"
                    break

    if schema is not None:
        value = _schema_mock_value(schema)
        if method == "function_calling" and isinstance(value, dict) and tool_name is not None:
            return "", {}, [{
                    "name": tool_name,
                    "args": value,
                    "id": "mock-structured-output",
                    "type": "tool_call",
            }]

        additional_kwargs = {"parsed": value} if method == "json_schema" else {}
        return json.dumps(value, sort_keys=True), additional_kwargs, []

    response_format = kwargs.get("response_format")
    if isinstance(response_format, dict) and response_format.get("type") == "json_object":
        return "{}", {}, []
    return MOCK_LLM_RESPONSE, {}, []


def _mock_chat_result(kwargs: dict[str, Any]) -> ChatResult:
    content, additional_kwargs, tool_calls = _mock_response_parts(kwargs)
    return ChatResult(
        generations=[ChatGeneration(message=AIMessage(
            content=content,
            additional_kwargs=additional_kwargs,
            tool_calls=tool_calls,
        ))]
    )


def _mock_chat_chunk(kwargs: dict[str, Any]) -> ChatGenerationChunk:
    content, additional_kwargs, tool_calls = _mock_response_parts(kwargs)
    tool_call_chunks = [
        {
            "name": call["name"],
            "args": json.dumps(call["args"], sort_keys=True),
            "id": call["id"],
            "index": index,
        }
        for index, call in enumerate(tool_calls)
    ]
    return ChatGenerationChunk(message=AIMessageChunk(
        content=content,
        additional_kwargs=additional_kwargs,
        tool_call_chunks=tool_call_chunks,
    ))


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
        if _mock_mode_active():
            return _mock_chat_result(kwargs)
        _refuse_without_model(self)
        return await super()._agenerate(
            await redact_lc_messages(messages), stop=stop, run_manager=run_manager, **kwargs)

    async def _astream(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                       run_manager: Any = None, **kwargs: Any) -> Any:
        if _mock_mode_active():
            yield _mock_chat_chunk(kwargs)
            return
        _refuse_without_model(self)
        async for chunk in super()._astream(
                await redact_lc_messages(messages), stop=stop,
                run_manager=run_manager, **kwargs):
            yield chunk

    def _generate(self, messages: list[BaseMessage], stop: Optional[list[str]] = None,
                  run_manager: Any = None, **kwargs: Any) -> Any:
        if _mock_mode_active():
            return _mock_chat_result(kwargs)
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
        if _mock_mode_active():
            return iter((_mock_chat_chunk(kwargs),))
        _refuse_without_model(self)
        return super()._stream(messages, stop=stop, run_manager=run_manager, **kwargs)


__all__ = ["COMPILE_PLACEHOLDER_KEY", "ModelNotConfiguredError", "RedactingChatOpenAI",
           "redact_lc_messages"]
