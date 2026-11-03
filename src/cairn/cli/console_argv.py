"""The CLI commands the console's short verbs shell out to (ADR-0165 D10).

Outside ``cli/tui`` because that package needs textual: the test that holds these to the real CLI
has to run where the console's dependencies aren't installed.
"""
from __future__ import annotations

LOCAL_CMDS = {"ls", "doctor", "validate", "run", "replay", "connect", "eval",
              "init", "lock", "visualize"}
_SPELLED = {"ls": ["pack", "ls"], "doctor": ["setup", "--check"], "validate": ["pack", "check"],
            "connect": ["admin", "connect"], "init": ["pack", "new"], "lock": ["pack", "lock"]}


def cli_argv(toks: list[str]) -> list[str]:
    head, rest = toks[0], toks[1:]
    if head == "replay":
        return ["status", *rest, "--replay"]
    if head == "visualize":
        return ["pack", "show", "--graph", *(rest[1:] if rest[:1] == ["template"] else rest)]
    return [*_SPELLED.get(head, [head]), *rest]
