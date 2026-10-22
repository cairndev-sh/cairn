"""
CompiledTemplate — the result of running a template file through the compiler.

A CompiledTemplate holds:
  - The validated TemplateSpec
  - A callable `runnable` (LangGraph graph or simple LLM wrapper) ready to invoke
  - The original prose body (for skill context injection)
  - Metadata about which flow type produced it
  - A content hash (§24.11.3) — immutable identity for replay + regression eval

Templates are invoked via `await compiled.run(input)`. The input shape depends
on the flow type but always returns a dict.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Optional

from cairn.core.engine.schema import TemplateSpec


RunFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def ast_dict(spec: TemplateSpec) -> dict[str, Any]:
    """Canonical JSON-able projection of the parsed template AST (§24.22).

    This is the **same projection** :func:`compute_content_hash` hashes, so
    the AST snapshot and the content hash always agree — a template whose
    snapshot is unchanged has an unchanged frontmatter half of its hash.
    Consumers: golden AST tests (AST2) and run-record persistence (AST3).

    ``triggers`` is EXCLUDED (ADR-0101 D1, #1973). A template's identity is what
    it executes; how it is started is start-side config that lives in the same
    file for the author's convenience. Including it would mean editing a cron
    re-hashes the template — invalidating every pinned hash, every golden
    fixture and every ``--compare-against`` tag for a change that alters no
    behaviour of the run itself.
    """
    return spec.model_dump(mode="json", exclude_none=False, exclude={"triggers"})


def ast_json(spec: TemplateSpec, *, indent: Optional[int] = 2) -> str:
    """Serialize the AST projection as deterministic JSON.

    ``sort_keys=True`` + fixed separators make output byte-stable across
    runs; ``indent=2`` (default) keeps golden fixtures diff-reviewable.
    Pass ``indent=None`` for the compact form (run records).
    """
    return json.dumps(
        ast_dict(spec),
        sort_keys=True,
        indent=indent,
        separators=(",", ": ") if indent else (",", ":"),
        default=str,
    )


def compute_content_hash(spec: TemplateSpec, body: str) -> str:
    """Compute the content-addressed identity of a template (§24.11.3).

    Hash = sha256(canonical_json(frontmatter) + body), prefixed with the
    algorithm name so future migrations stay unambiguous.

    Canonical JSON (``sort_keys=True``, no extra whitespace) makes the
    hash **insensitive to frontmatter key order** — reordering YAML keys
    in the source doesn't change the identity, but changing a value does.
    Body text is appended verbatim; whitespace in the prose IS part of
    identity (a re-indented prompt is a different prompt to the LLM).

    The hash flows into:
      - ``CompiledTemplate.content_hash`` (returned by compiler)
      - ``runs.template_hash`` column (each run records the exact version
        that ran — §24.11.3 storage extension)
      - skill tag → hash resolution (§24.11.3 mutable tags)
      - ``cairn eval --compare-against <tag-or-hash>`` (§24.11.4)
    """
    # Same canonical projection as :func:`ast_dict` (§24.22 — snapshot and
    # hash must agree), serialized compact for hashing.
    frontmatter_json = json.dumps(
        ast_dict(spec),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    h = hashlib.sha256()
    h.update(frontmatter_json.encode("utf-8"))
    h.update(b"\n---body---\n")  # explicit boundary so a body starting with
                                  # the same bytes as a frontmatter key
                                  # can't collide with a frontmatter-only
                                  # template that has matching prefix.
    h.update(body.encode("utf-8"))
    return f"sha256:{h.hexdigest()}"


@dataclass
class CompiledTemplate:
    """A compiled template ready to execute.

    ``content_hash`` is the §24.11.3 content-addressed identity — same
    template body + frontmatter → same hash, across machines, across
    runs. Mutable tags point at hashes; ``cairn eval --compare-against
    <tag>`` resolves the tag to its current hash and pins the eval against
    that immutable version.
    """
    spec: TemplateSpec
    prose: str
    flow_type: Literal["prompt", "agent_loop", "state_machine"]
    runnable: RunFn
    content_hash: str = ""
    unsupported_mock_operators: tuple[str, ...] = ()

    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """Invoke the compiled template with the given input."""
        return await self.runnable(input_data)

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def version(self) -> str:
        return self.spec.version

    def ast_json(self, *, indent: Optional[int] = 2) -> str:
        """The §24.22 AST snapshot for this template (delegates to
        module-level :func:`ast_json` over ``self.spec``)."""
        return ast_json(self.spec, indent=indent)

    @property
    def short_hash(self) -> str:
        """First 12 hex chars of the content hash — for display in ``ls``,
        ``run`` summary, etc. Matches Docker / git short-hash convention.
        """
        if not self.content_hash:
            return ""
        if self.content_hash.startswith("sha256:"):
            return self.content_hash[7:19]
        return self.content_hash[:12]
