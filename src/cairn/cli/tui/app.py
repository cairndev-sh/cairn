"""``cairn remote console`` — a Toad-style Textual control-center over cairn (#56).

A tabbed browser (Runs · Workflows · Connections · Profiles) + a command bar at
the bottom that runs *any* cairn command: server verbs (`submit`/`approve`/`ps`)
go over HTTP+SSE with a live view and clickable HITL; local verbs (`ls`/`doctor`/
`connect`/`replay`/`run`/…) shell out and stream Rich output; and `ask <intent>`
runs the governed NL→plan resolver (②). Textual = shell + widgets; Rich = content.

Ask is the platform's (ADR-0134 D6): its hooks here — the ``ask`` verb, the Workflows
tab and ``run``'s template completion — are offered only when a ``cairn.cli`` provider
mounted ``ask`` (:func:`_ask_provided`), and ``ask`` runs through that mounted command.

Needs ``textual`` and ``textual-autocomplete``, which ``cairndev`` does not install
(there is no ``tui`` extra); ``cairn remote console`` names the command that adds them.
"""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

import httpx
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.command import DiscoveryHit, Hit, Provider
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button, DataTable, Footer, Header, Input, Label, Markdown, RichLog, Static, TabbedContent, TabPane,
)
from textual_autocomplete import AutoComplete, DropdownItem, TargetState

# server verbs (HTTP+SSE) vs local verbs (shell out) vs ask (②) ──────
_SERVER_CMDS = {"submit", "ps", "watch", "status", "logs", "approve", "reject"}
_LOCAL_CMDS = {"ls", "doctor", "validate", "run", "replay", "connect", "eval",
               "init", "lock", "visualize"}
COMMANDS: dict[str, str] = {
    "ask": 'ask "<natural-language intent>" — resolve to a governed plan (②)',
    "submit": "<skill> [--service S] [-i k=v ...]  — fire a skill (auto-watches)",
    "watch": "<run_id>  — stream a run", "status": "<run_id>  — lifecycle",
    "logs": "<run_id>  — event trace", "approve": "<run_id>  — approve the gate",
    "reject": "<run_id>  — reject the gate", "ps": "refresh runs",
    "ls": "list packs / workflows", "doctor": "preflight checks",
    "run": "<pack>/<template> -i k=v  — run a template locally",
    "replay": "<run_id>  — render a past run's trace", "connect": "ls | add | rm  — connections",
    "validate": "<path>  — validate offline", "help": "help (F1)", "quit": "exit (q)",
}
_TAKES_SKILL = {"submit"}
_TAKES_TEMPLATE = {"run"}
_TAKES_RUN = {"watch", "status", "logs", "approve", "reject", "replay"}
_FLAGS = ["--service", "--input", "-i", "--profile", "--json"]
_TERMINAL = {"done", "failed", "cancelled"}
_STYLE = {"queued": "dim", "running": "yellow", "awaiting_approval": "bold yellow",
          "done": "bold green", "failed": "bold red", "cancelled": "red"}


def _cairn_bin() -> list[str]:
    """The cairn CLI invocation (the venv sibling of this python)."""
    cand = Path(sys.executable).parent / "cairn"
    return [str(cand)] if cand.exists() else [sys.executable, "-m", "cairn.cli.main"]


def _skill_names() -> list[str]:
    try:
        from cairn.core.engine.skill_engine import SkillEngine
        return sorted(SkillEngine().list_skills())
    except Exception:
        return []


def _ask_provided() -> bool:
    """Whether a ``cairn.cli`` provider mounted ``ask`` (ADR-0134 D6). The public CLI's
    hidden ``ask`` stub does not count: it mounts only when no provider did."""
    try:
        from cairn.cli.main import PROVIDER_COMMANDS  # noqa: PLC0415 — the CLI this console runs under
    except Exception:  # noqa: BLE001
        return False
    return "ask" in PROVIDER_COMMANDS


def _commands() -> dict[str, str]:
    """:data:`COMMANDS`, without ``ask`` unless a provider supplied it."""
    return {k: v for k, v in COMMANDS.items() if k != "ask" or _ask_provided()}


def _candidate_templates() -> list[dict[str, str]]:
    """The templates the Workflows tab and ``run`` completion list. The read is core's own
    (:mod:`cairn.core.pack.discovery`, what ``GET /packs`` serves), so it needs no Ask and is
    offered on every install (ADR-0134 D6 gates only the Ask hooks)."""
    from cairn.core.pack.discovery import candidate_templates  # noqa: PLC0415

    return candidate_templates()


def _template_refs() -> list[str]:
    try:
        return sorted(c["ref"] for c in _candidate_templates())
    except Exception:
        return []


def _field_label(f: Any) -> str:
    """Human label for a schema-generated form field: title, * if required,
    [choices] for an enum, else a hint from the description."""
    label = f.title + ("*" if f.required else "")
    if f.enum:
        label += "  [" + " | ".join(str(e) for e in f.enum) + "]"
    elif f.description:
        label += f"  — {f.description[:48]}"
    return label


class _CommandComplete(AutoComplete):
    """Token-scoped, context-aware completion across all verbs.

    Command-bar UX: **Enter runs the typed command, Tab completes.** The base
    AutoComplete applies the highlighted completion on Enter (swallowing the
    Input's Submitted), which made commands silently never run — so we override
    the Enter case to just hide the dropdown and let the Input submit (#56).
    """

    def _listen_to_messages(self, event) -> None:  # noqa: ANN001
        if getattr(event, "key", None) == "enter":
            self.action_hide()
            return  # don't complete; let the Input.Submitted fire → run command
        super()._listen_to_messages(event)

    def get_search_string(self, target_state: TargetState) -> str:
        before = target_state.text[: target_state.cursor_position]
        return before.split(" ")[-1] if before else ""

    def get_candidates(self, target_state: TargetState) -> list[DropdownItem]:
        before = target_state.text[: target_state.cursor_position]
        toks = before.split(" ")
        if len(toks) <= 1:
            return [DropdownItem(c, prefix="⌘ ") for c in _commands()]
        cmd, cur = toks[0], toks[-1]
        if cur.startswith("-"):
            return [DropdownItem(f, prefix="⚑ ") for f in _FLAGS]
        if cmd in _TAKES_SKILL and len(toks) == 2:
            return [DropdownItem(s, prefix="▶ ") for s in _skill_names()]
        if cmd in _TAKES_TEMPLATE and len(toks) == 2:
            return [DropdownItem(s, prefix="▶ ") for s in _template_refs()]
        if cmd in _TAKES_RUN and len(toks) == 2:
            return [DropdownItem(r, prefix="● ") for r in getattr(self.app, "run_ids", [])]
        return []


_HELP_MD = """\
# cairn console

A live, governed view of the cairn control plane.

## Tabs — select a row to act on it
**Runs** → Watch / Replay / Cancel · **Workflows** → Plan / Run ·
**Connections** → Test / Remove (＋ Add) · **Profiles** → Doctor

## Command bar — run any cairn command
```
ask "fine-tune mistral under $100"     ← NL → governed plan (②)
submit <skill> --service S -i k=v      ← fire on the server (auto-watch)
run <pack>/<template> -i k=v           ← run a template locally
ls · doctor · connect ls · replay <id> · validate <path>
watch/status/logs/approve/reject <run_id>     ps     quit
```
Autocomplete suggests commands → skills / template-refs / run-ids → flags.

## HITL
At a governed gate the run shows **awaiting approval** and the
**✓ Approve / ✗ Reject** buttons appear — click them or press **a** / **r**.

`↵` run · `a`/`r` decide · `^P` palette · `F1` help · `^R` refresh · `q` quit
"""


class HelpScreen(ModalScreen):
    BINDINGS = [Binding("escape,f1,q", "dismiss", "Close")]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-box"):
            yield Markdown(_HELP_MD)
            yield Button("Close (Esc)", id="help-close", variant="primary")

    @on(Button.Pressed, "#help-close")
    def _close(self) -> None:
        self.dismiss()


class ActionsScreen(ModalScreen[Optional[str]]):
    """A small modal listing the actions for a selected entity. Each button
    dismisses the screen with its action id; Esc / Cancel dismiss ``None`` (#56)."""

    BINDINGS = [Binding("escape", "dismiss", "Close")]

    def __init__(self, title: str, actions: list[tuple[str, str, str]]) -> None:
        super().__init__()
        self._title = title
        self._actions = actions  # (action_id, label, variant)

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-box"):
            yield Static(self._title, id="modal-title")
            with Horizontal(id="modal-actions"):
                for aid, label, variant in self._actions:
                    yield Button(label, id=f"act-{aid}", variant=variant)
                yield Button("Cancel (Esc)", id="modal-close")

    def on_mount(self) -> None:
        if self._actions:
            self.query_one(f"#act-{self._actions[0][0]}", Button).focus()

    @on(Button.Pressed)
    def _pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "modal-close":
            self.dismiss(None)
        elif bid.startswith("act-"):
            self.dismiss(bid[len("act-"):])


class FormScreen(ModalScreen[Optional[dict]]):
    """A modal form — one Input per field. Submit (or Enter) dismisses with
    ``{key: value}``; Esc / Cancel dismiss ``None`` (#56)."""

    BINDINGS = [Binding("escape", "dismiss", "Close")]

    def __init__(self, title: str, fields: list[tuple[str, str, str]],
                 submit_label: str = "Submit") -> None:
        super().__init__()
        self._title = title
        self._fields = fields  # (key, label, default)
        self._submit_label = submit_label

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-box"):
            yield Static(self._title, id="modal-title")
            for key, label, default in self._fields:
                yield Label(label)
                yield Input(value=default, id=f"f-{key}")
            with Horizontal(id="modal-actions"):
                yield Button(self._submit_label, id="form-submit", variant="success")
                yield Button("Cancel (Esc)", id="modal-close")

    def on_mount(self) -> None:
        if self._fields:
            self.query_one(f"#f-{self._fields[0][0]}", Input).focus()

    @on(Input.Submitted)
    def _on_enter(self) -> None:
        self._submit()

    @on(Button.Pressed)
    def _pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "modal-close":
            self.dismiss(None)
        elif bid == "form-submit":
            self._submit()

    def _submit(self) -> None:
        self.dismiss({key: self.query_one(f"#f-{key}", Input).value for key, _, _ in self._fields})


class CairnCommands(Provider):
    """Fuzzy command palette (Ctrl+P) over cairn verbs, skills, templates and
    live runs — selecting a hit prefills the command bar or opens the matching
    actions modal, so the palette and the typed bar share one dispatch (#56)."""

    async def discover(self) -> Hits:  # noqa: F821 — Hits is a textual type alias
        for display, help_, runner in self.app._palette_catalog():
            yield DiscoveryHit(display, runner, help=help_)

    async def search(self, query: str) -> Hits:  # noqa: F821
        matcher = self.matcher(query)
        for display, help_, runner in self.app._palette_catalog():
            score = matcher.match(display)
            if score > 0:
                yield Hit(score, matcher.highlight(display), runner, help=help_)


class CairnConsole(App):
    COMMANDS = App.COMMANDS | {CairnCommands}
    TITLE = "cairn"
    SUB_TITLE = "control plane"
    CSS = """
    Screen { layout: vertical; }
    #body { height: 1fr; }
    #left { width: 44%; }
    #detail { width: 1fr; border: round $primary; padding: 0 1; }
    #status { height: 1; }
    #events { height: 1fr; }
    #gate { display: none; height: auto; padding: 1; border: round $warning; }
    #gate.show { display: block; }
    #gate Button { margin: 0 2 0 0; }
    DataTable { height: 1fr; }
    #cmd { dock: bottom; }
    #conn-add { dock: top; height: 3; margin: 0 0 1 0; }
    #help-box { width: 84%; height: 84%; border: thick $primary; background: $surface; padding: 1 2; }
    ActionsScreen, FormScreen { align: center middle; }
    #modal-box { width: 70%; max-width: 90; height: auto; border: thick $primary; background: $surface; padding: 1 2; }
    #modal-title { text-style: bold; padding-bottom: 1; }
    #modal-actions { height: auto; padding-top: 1; }
    #modal-actions Button { margin: 0 2 0 0; }
    #modal-box Label { color: $text-muted; padding-top: 1; }
    """
    BINDINGS = [
        Binding("f1", "help", "Help"),
        Binding("ctrl+r", "refresh", "Refresh"),
        Binding("a", "approve", "Approve"),
        Binding("r", "reject", "Reject"),
        # ctrl+q is priority so it quits even while the command bar (an Input,
        # which swallows a bare `q`) has focus; `q` stays for table focus.
        Binding("ctrl+q", "quit", "Quit", priority=True),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(self, *, endpoint: str, token: Optional[str] = None) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.token = token
        self.http: Optional[httpx.AsyncClient] = None
        self.run_ids: list[str] = []
        self.current: Optional[str] = None
        self.pending_run: Optional[str] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            with TabbedContent(id="left"):
                with TabPane("Runs", id="tab-runs"):
                    yield DataTable(id="runs")
                with TabPane("Workflows", id="tab-wf"):
                    yield DataTable(id="workflows")
                with TabPane("Connections", id="tab-conn"):
                    yield Button("➕ Add connection", id="conn-add", variant="primary")
                    yield DataTable(id="connections")
                with TabPane("Profiles", id="tab-prof"):
                    yield DataTable(id="profiles")
            with Vertical(id="detail"):
                yield Static("type a command below — try `ls`, `ask \"…\"`, or `submit <skill>`", id="status")
                yield RichLog(id="events", highlight=True, markup=True, wrap=True)
                with Horizontal(id="gate"):
                    yield Button("✓ Approve (a)", id="approve", variant="success")
                    yield Button("✗ Reject (r)", id="reject", variant="error")
        cmd = Input(placeholder='ask "investigate the checkout incident"   ·   submit <skill>   ·   ls', id="cmd")
        yield cmd
        yield _CommandComplete(cmd, id="cmd-ac")
        yield Footer()

    def on_mount(self) -> None:
        # The console is a cairn front-end — load the pack/operator registries so
        # the Workflows tab + autocomplete (skills/templates) resolve. Best-effort.
        try:
            from cairn.core.operator import load_all_operators
            from cairn.core.pack.loader import load_all_packs
            load_all_operators(strict=False)
            load_all_packs(strict=False)
        except Exception:  # noqa: BLE001 — never block the UI on registry load
            pass
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        self.http = httpx.AsyncClient(base_url=self.endpoint, headers=headers, timeout=30.0)
        self.query_one("#runs", DataTable).add_columns("run", "status", "skill", "age")
        self.query_one("#workflows", DataTable).add_columns("template", "category", "description")
        self.query_one("#connections", DataTable).add_columns("name", "kind", "id")
        self.query_one("#profiles", DataTable).add_columns("profile", "extends", "bindings")
        for tid in ("runs", "workflows", "connections", "profiles"):
            self.query_one(f"#{tid}", DataTable).cursor_type = "row"
        self.query_one("#cmd", Input).focus()
        self._load_workflows()
        self._load_profiles()
        self._load_connections()
        self.set_interval(2.0, self.action_refresh)
        self.action_refresh()

    async def on_unmount(self) -> None:
        if self.http is not None:
            await self.http.aclose()

    def check_action(self, action: str, parameters):  # noqa: ANN001
        """Don't let the a/r approve/reject keys fire while the user is typing
        in the command bar — only when the run/gate pane has focus (#56)."""
        if action in ("approve", "reject") and isinstance(self.focused, Input):
            return False
        return True

    def _log(self) -> RichLog:
        return self.query_one("#events", RichLog)

    # ── browse tabs ───────────────────────────────────────────────
    def _load_workflows(self) -> None:
        try:
            cands = _candidate_templates()
        except Exception as e:  # noqa: BLE001
            cands = []; self.notify(f"workflows: {e}", severity="error")
        t = self.query_one("#workflows", DataTable); t.clear()
        for c in cands:
            t.add_row(c["ref"], c.get("category", ""), (c.get("description", "") or "")[:60], key=c["ref"])

    def _load_profiles(self) -> None:
        t = self.query_one("#profiles", DataTable); t.clear()
        prof_dir = Path(__file__).resolve().parents[2] / "profiles"
        for f in sorted(prof_dir.glob("*.toml")) if prof_dir.exists() else []:
            try:
                import tomllib
                data = tomllib.loads(f.read_text())
                prof = data.get("profile", {})
                t.add_row(f.stem, str(prof.get("extends", "")), str(len(prof.get("bindings", {}) or {})), key=str(f))
            except Exception:
                t.add_row(f.stem, "?", "?", key=str(f))

    @work(group="conn", exclusive=True)
    async def _load_connections(self) -> None:
        try:
            from cairn.core.connections import default_connection_store
            conns = await default_connection_store().list()
        except Exception as e:  # noqa: BLE001
            self.notify(f"connections: {e}", severity="warning"); return
        t = self.query_one("#connections", DataTable); t.clear()
        for c in conns:
            name = getattr(c, "name", "")
            t.add_row(name, getattr(c, "kind", ""), getattr(c, "id", "")[:12], key=name)

    @work(group="refresh", exclusive=True)
    async def action_refresh(self) -> None:
        if self.http is None:
            return
        try:
            resp = await self.http.get("/runs", params={"limit": 50})
            invs = resp.json().get("investigations", []) if resp.status_code < 400 else []
        except Exception:
            self.query_one("#status", Static).update(Text(f"⚠ server unreachable at {self.endpoint}", style="red"))
            return
        self.run_ids = [i.get("id", "") for i in invs]
        t = self.query_one("#runs", DataTable); t.clear()
        for inv in invs:
            st = inv.get("status", "")
            results = inv.get("results") or []
            skill = (results[0].get("skill_name") if results else "") or str(inv.get("matched_skills", ""))
            t.add_row(inv.get("id", "")[-12:], Text(st, style=_STYLE.get(st, "")),
                      str(skill)[:20], str(inv.get("created_at", ""))[-8:], key=inv.get("id", ""))

    # ── command bar ───────────────────────────────────────────────
    @on(Input.Submitted, "#cmd")
    def _on_command(self, event: Input.Submitted) -> None:
        self.query_one("#cmd", Input).value = ""
        self._dispatch(event.value.strip())

    def _prefill(self, text: str) -> None:
        """Drop ``text`` into the command bar and focus it — the palette uses
        this for verbs/skills that still need an argument typed."""
        inp = self.query_one("#cmd", Input)
        inp.value = text
        inp.cursor_position = len(text)
        inp.focus()

    def _dispatch(self, line: str) -> None:
        if not line:
            return
        # `ask` takes free text — don't shlex-split the intent.
        if line.split(" ", 1)[0] == "ask":
            if not _ask_provided():
                self.notify("`ask` is part of cairn-platform; this install does not include it",
                            severity="warning")
                return
            intent = line[3:].strip().strip('"').strip("'")
            if intent:
                self._ask(intent)
            return
        try:
            toks = shlex.split(line)
        except ValueError:
            toks = line.split()
        cmd, args = toks[0], toks[1:]
        if cmd in ("quit", "exit", "q"):
            self.exit()
        elif cmd == "help":
            self.action_help()
        elif cmd == "ps":
            self.action_refresh(); self.notify("refreshed")
        elif cmd == "submit" and args:
            self._submit(args)
        elif cmd in _TAKES_RUN and args and cmd != "replay":
            self.current = args[0]; self._open_run(args[0])
        elif cmd in _LOCAL_CMDS:
            self._shell(toks)
        else:
            self.notify(f"unknown or incomplete: {line!r}", severity="warning")

    # ── ask ② ─────────────────────────────────────────────────────
    def _ask(self, intent: str) -> None:
        """NL → governed plan through the ``ask`` command the platform's provider mounted
        (``cairn ask run <intent>``): resolves and plans, never runs (ADR-0134 D6)."""
        self._shell(["ask", "run", intent])

    # ── server submit + live view ─────────────────────────────────
    @work(group="submit")
    async def _submit(self, args: list[str]) -> None:
        skill = args[0]; service: Optional[str] = None; payload: dict[str, str] = {}
        i = 1
        while i < len(args):
            a = args[i]
            if a == "--service" and i + 1 < len(args):
                service = args[i + 1]; i += 2
            elif a in ("-i", "--input") and i + 1 < len(args) and "=" in args[i + 1]:
                k, v = args[i + 1].split("=", 1); payload[k] = v; i += 2
            else:
                i += 1
        body = {"skill_name": skill, "service": service, "payload": payload, "wait": False}
        try:
            resp = await self.http.post("/triggers/manual", json=body)
            run_id = resp.json().get("run_id")
        except Exception as e:
            self.notify(f"submit failed: {e}", severity="error"); return
        if not run_id:
            self.notify("submit returned no run_id", severity="error"); return
        self.notify(f"submitted {run_id}")
        self.current = run_id
        self.action_refresh()
        self._open_run(run_id)

    # ── actionable entities (select a row → an actions modal) ─────
    def _act(self, title: str, actions: list[tuple[str, str, str]],
             handler: Callable[[Optional[str]], None]) -> None:
        self.push_screen(ActionsScreen(title, actions), handler)

    # ── command palette (Ctrl+P) ─────────────────────────────────
    def _palette_catalog(self) -> list[tuple[str, str, Callable[[], None]]]:
        """(display, help, runner) tuples surfaced by the Ctrl+P palette.

        Instant verbs run; verbs/skills that need an argument prefill the
        command bar; templates and live runs open their Slice-B actions modal.
        """
        items: list[tuple[str, str, Callable[[], None]]] = [
            ("refresh", "↻ reload the runs table", self.action_refresh),
            ("ls", "list packs / workflows", lambda: self._dispatch("ls")),
            ("doctor", "preflight checks", lambda: self._dispatch("doctor")),
            ("connect ls", "list connections", lambda: self._dispatch("connect ls")),
            ("help", "open the help screen", self.action_help),
            ("quit", "exit the console", self.exit),
        ]
        for verb, desc in _commands().items():
            items.append((f"{verb} …", desc, lambda v=verb: self._prefill(f"{v} ")))
        for s in _skill_names():
            items.append((f"submit {s}", "fire this skill on the server",
                          lambda s=s: self._prefill(f"submit {s} ")))
        for ref in _template_refs():
            items.append((f"workflow {ref}", "plan / run this template",
                          lambda r=ref: self._wf_menu(r)))
        for rid in self.run_ids:
            items.append((f"run {rid[-12:]}", "watch / replay / cancel",
                          lambda r=rid: self._run_menu(r)))
        return items

    @on(DataTable.RowSelected, "#runs")
    def _on_run_row(self, event: DataTable.RowSelected) -> None:
        rid = event.row_key.value
        if rid:
            self._run_menu(rid)

    def _run_menu(self, rid: str) -> None:
        self.current = rid
        self._act(f"run · {rid[-12:]}",
                  [("watch", "▶ Watch", "primary"), ("replay", "↺ Replay", "default"),
                   ("cancel", "■ Cancel", "error")],
                  lambda a: self._run_action(a, rid))

    def _run_action(self, action: Optional[str], run_id: str) -> None:
        if action == "watch":
            self._open_run(run_id)
        elif action == "replay":
            self._shell(["replay", run_id])
        elif action == "cancel":
            self._cancel(run_id)

    @work(group="cancel")
    async def _cancel(self, run_id: str) -> None:
        if self.http is None:
            return
        try:
            await self.http.post(f"/runs/{run_id}/cancel")
            self.notify(f"cancel requested for {run_id}")
        except Exception as e:  # noqa: BLE001
            self.notify(f"cancel failed: {e}", severity="error")
        self.action_refresh()

    @on(DataTable.RowSelected, "#workflows")
    def _on_wf_row(self, event: DataTable.RowSelected) -> None:
        ref = event.row_key.value
        if ref:
            self._wf_menu(ref)

    def _wf_menu(self, ref: str) -> None:
        # `plan` is `cairn ask plan` — offered only when a provider mounted `ask` (ADR-0134 D6);
        # `run` needs nothing beyond core.
        actions = [("plan", "🧭 Plan", "primary")] if _ask_provided() else []
        self._act(f"workflow · {ref}", [*actions, ("run", "▶ Run", "success")],
                  lambda a: self._wf_action(a, ref))

    def _wf_action(self, action: Optional[str], ref: str) -> None:
        if action == "plan" and _ask_provided():
            self._shell(["ask", "plan", ref])
        elif action == "run":
            self._open_run_form(ref)

    def _open_run_form(self, ref: str) -> None:
        """Generate the run form from the pack's input schema (#109 Phase 1).
        Falls back to an --input path field if the template declares no inputs."""
        # lazy: keep the optional-TUI module import light (registries load on mount)
        from cairn.core.ui_contract import build_ui_contract
        from cairn.core.ui_render import contract_to_form_fields
        try:
            contract = build_ui_contract(ref)
            fields = contract_to_form_fields(contract)
        except Exception as e:  # noqa: BLE001
            self.notify(f"contract failed: {e}", severity="error"); return
        if not fields:
            self.push_screen(
                FormScreen(f"run {ref}", [("input", "path to an --input JSON file", "")],
                           submit_label="Run locally"),
                lambda vals: self._run_template(ref, vals),
            )
            return
        form = [(f.name, _field_label(f), "" if f.default is None else str(f.default)) for f in fields]
        # offer the browser as an alternative — fill it here OR on the server's web UI
        self.notify(f"or fill it in a browser: {self.endpoint}/ui/{contract.slug}", timeout=8)
        self.push_screen(
            FormScreen(f"run {ref}", form, submit_label="Run locally"),
            lambda vals: self._run_with_contract(ref, contract, vals),
        )

    def _run_with_contract(self, ref: str, contract: Any, vals: Optional[dict]) -> None:
        if not vals:
            return
        from cairn.core.ui_render import coerce_and_validate  # lazy (see _open_run_form)
        typed, errors = coerce_and_validate(contract, vals)
        if errors:
            self.notify(" · ".join(errors[:3]), severity="error"); return
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(typed, f)
            path = f.name
        self._shell(["run", ref, "--input", path])

    def _run_template(self, ref: str, vals: Optional[dict]) -> None:
        if not vals:
            return
        path = (vals.get("input") or "").strip()
        if not path:
            self.notify("run needs a path to an --input JSON file", severity="warning")
            return
        self._shell(["run", ref, "--input", path])

    @on(DataTable.RowSelected, "#connections")
    def _on_conn_row(self, event: DataTable.RowSelected) -> None:
        name = event.row_key.value
        if not name:
            return
        self._act(f"connection · {name}",
                  [("test", "✓ Test", "primary"), ("remove", "🗑 Remove", "error")],
                  lambda a: self._conn_action(a, name))

    def _conn_action(self, action: Optional[str], name: str) -> None:
        if action == "test":
            self._shell(["connect", "test", name])
        elif action == "remove":
            self._shell(["connect", "rm", name])

    @on(Button.Pressed, "#conn-add")
    def _conn_add_btn(self) -> None:
        self.push_screen(
            FormScreen("add connection",
                       [("provider", "provider (e.g. openai, anthropic, notion)", ""),
                        ("name", "name (optional — defaults to the provider)", "")],
                       submit_label="Add"),
            self._do_conn_add,
        )

    def _do_conn_add(self, vals: Optional[dict]) -> None:
        if not vals or not (vals.get("provider") or "").strip():
            return
        toks = ["connect", "add", vals["provider"].strip()]
        if (vals.get("name") or "").strip():
            toks += ["--name", vals["name"].strip()]
        self._shell(toks, after=self._load_connections)

    @on(DataTable.RowSelected, "#profiles")
    def _on_prof_row(self, event: DataTable.RowSelected) -> None:
        path = event.row_key.value
        if not path:
            return
        self._act(f"profile · {Path(path).stem}",
                  [("doctor", "🩺 Doctor", "primary")],
                  lambda a: self._prof_action(a, path))

    def _prof_action(self, action: Optional[str], path: str) -> None:
        if action == "doctor":
            # doctor reads the profile from CAIRN_PROFILE (a path) — target the
            # selected one rather than the ambient profile.
            self._shell(["doctor"], env={"CAIRN_PROFILE": path})

    def _open_run(self, run_id: str) -> None:
        log = self._log(); log.clear()
        log.write(Text(f"⇢ attaching to {run_id}", style="dim"))
        self.query_one("#gate").remove_class("show")
        self._stream(run_id)

    @work(group="stream", exclusive=True)
    async def _stream(self, run_id: str) -> None:
        last = 0; log = self._log()
        while True:
            terminal = False; ev = data = seq = None
            try:
                async with self.http.stream("GET", f"/runs/{run_id}/stream",
                                            params={"after_seq": last}, timeout=None) as r:
                    if r.status_code >= 400:
                        log.write(Text(f"stream error {r.status_code}", style="red")); return
                    async for line in r.aiter_lines():
                        if line == "":
                            if ev is not None:
                                self._render_event(run_id, seq, ev, data)
                                if seq is not None:
                                    last = seq
                                if ev in _TERMINAL:
                                    terminal = True; break
                            ev = data = seq = None; continue
                        if line.startswith("id:"):
                            try: seq = int(line[3:].strip())
                            except ValueError: seq = None
                        elif line.startswith("event:"): ev = line[6:].strip()
                        elif line.startswith("data:"): data = line[5:].strip()
            except Exception:
                pass
            if terminal:
                return
            try:
                resp = await self.http.get(f"/runs/{run_id}")
                if (resp.json().get("status") if resp.status_code < 400 else None) in _TERMINAL:
                    return
            except Exception:
                return
            await asyncio.sleep(1.0)

    def _render_event(self, run_id, seq, ev_type, data) -> None:
        if isinstance(data, str):
            try: data = json.loads(data)
            except Exception: pass
        summ = "  ".join(f"{k}={str(v)[:50]}" for k, v in list(data.items())[:4]) if isinstance(data, dict) and data else ""
        style = _STYLE.get(ev_type or "", "cyan")
        self._log().write(Text.assemble((f"{seq if seq is not None else '·':>3} ", "dim"),
                                         (f"{ev_type}", style), ("  " + summ if summ else "", "grey70")))
        self.query_one("#status", Static).update(Text.assemble((f"{run_id}  ", "bold"), (ev_type or "", style)))
        if ev_type == "awaiting_approval":
            self.pending_run = run_id
            self.query_one("#gate").add_class("show")
            # Focus the Approve button so a click, Enter, or a/r all work — and
            # so a/r are no longer captured by the command Input (check_action).
            self.query_one("#approve", Button).focus()
            self.notify("approval required — ✓ Approve / ✗ Reject (click or a/r)", severity="warning")

    # ── local commands (shell out, stream Rich output) ────────────
    @work(group="shell")
    async def _shell(self, toks: list[str], *, env: Optional[dict] = None,
                     after: Optional[Callable[[], None]] = None) -> None:
        log = self._log()
        log.write(Text.assemble(("$ cairn ", "bold green"), (" ".join(toks), "green")))
        try:
            proc = await asyncio.create_subprocess_exec(
                *_cairn_bin(), *toks, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env={**os.environ, **env} if env else None,
            )
        except Exception as e:  # noqa: BLE001
            log.write(Text(f"failed to launch: {e}", style="red")); return
        assert proc.stdout is not None
        async for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip("\n")
            if "composition conflict" in line:
                continue
            log.write(Text.from_ansi(line))
        await proc.wait()
        log.write(Text(f"[exit {proc.returncode}]", style="dim"))
        if after is not None:
            after()

    # ── HITL ──────────────────────────────────────────────────────
    @on(Button.Pressed, "#approve")
    def _btn_approve(self) -> None:
        self.action_approve()

    @on(Button.Pressed, "#reject")
    def _btn_reject(self) -> None:
        self.action_reject()

    def action_approve(self) -> None:
        self._resume("approve")

    def action_reject(self) -> None:
        self._resume("reject")

    @work(group="resume")
    async def _resume(self, decision: str) -> None:
        rid = self.pending_run
        if not rid:
            return
        self.query_one("#gate").remove_class("show")
        try:
            await self.http.post(f"/runs/{rid}/resume", params={"decision": decision, "approver": "console"})
            self.notify(f"{decision}d {rid}")
        except Exception as e:
            self.notify(f"resume failed: {e}", severity="error")
        self.pending_run = None
        self._stream(rid)

    def action_help(self) -> None:
        self.push_screen(HelpScreen())


def run_console(endpoint: str, token: Optional[str] = None) -> None:
    CairnConsole(endpoint=endpoint, token=token).run()
