"""Toad-style interactive terminal console for cairn (`cairn admin console`, #56).

A Textual TUI + Rich rendering over the cairn-server HTTP + SSE control plane.
Lives behind the optional ``[tui]`` extra (textual + textual-autocomplete) so the
base CLI stays lean. Import lazily — never at CLI startup.
"""

from cairn.cli.tui.app import CairnConsole

__all__ = ["CairnConsole"]
