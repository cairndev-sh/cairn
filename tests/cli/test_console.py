"""#56 — the Textual console (`cairn admin console`).

Headless smoke + autocomplete contracts via Textual's run_test pilot. Skipped
where the optional `tui` extra (textual + textual-autocomplete) isn't installed,
so base-install CI stays green.
"""
from __future__ import annotations

import pytest

pytest.importorskip("textual")
pytest.importorskip("textual_autocomplete")

from textual.widgets import DataTable, Input  # noqa: E402
from textual_autocomplete import TargetState  # noqa: E402

import cairn.cli.tui.app as tui_app  # noqa: E402
from cairn.cli.tui.app import (  # noqa: E402
    ActionsScreen, CairnCommands, CairnConsole, FormScreen,
)

# unreachable endpoint → the refresh worker degrades gracefully (no crash).
_EP = "http://127.0.0.1:1"


@pytest.fixture
def ask_provided(monkeypatch):
    """A `cairn.cli` provider mounted `ask` (the platform installed, ADR-0134 D6)."""
    monkeypatch.setattr(tui_app, "_ask_provided", lambda: True)


@pytest.fixture
def no_ask_provider(monkeypatch):
    """No provider mounted `ask` (a public install): only the hidden stub answers."""
    monkeypatch.setattr(tui_app, "_ask_provided", lambda: False)


async def test_console_composes_all_widgets():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        for sel in ("#runs", "#cmd", "#events", "#gate", "#approve", "#reject", "#cmd-ac"):
            app.query_one(sel)  # raises if a widget is missing
        # the runs table is set up with its columns
        assert len(app.query_one("#runs", DataTable).columns) == 4


async def test_autocomplete_is_context_aware():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        ac = app.query_one("#cmd-ac")
        # 1st token → commands
        cmds = [str(d.main) for d in ac.get_candidates(TargetState(text="su", cursor_position=2))]
        assert "submit" in cmds and "approve" in cmds
        # a dash → flags
        flags = [str(d.main) for d in ac.get_candidates(TargetState(text="submit x --", cursor_position=11))]
        assert "--service" in flags and "-i" in flags
        # `submit <here>` → skill names (a list; resolved from the local registry)
        skills = [str(d.main) for d in ac.get_candidates(TargetState(text="submit ", cursor_position=7))]
        assert isinstance(skills, list)
        # token-scoped search string = the current word, not the whole line
        assert ac.get_search_string(TargetState(text="submit inc", cursor_position=10)) == "inc"


async def test_f1_opens_help_screen():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.press("f1")
        await pilot.pause()
        assert app.screen.__class__.__name__ == "HelpScreen"


async def test_unreachable_server_does_not_crash():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.2)  # refresh worker runs against the dead endpoint
        assert app.run_ids == []  # no runs, but the app is alive


async def test_control_center_tabs_present_and_populate(ask_provided):
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        for sel in ("#workflows", "#connections", "#profiles", "#tab-runs", "#tab-wf"):
            app.query_one(sel)
        await pilot.pause(0.3)  # in-process loaders (registry + profile files) run
        assert app.query_one("#workflows", DataTable).row_count > 0  # registered templates
        assert app.query_one("#profiles", DataTable).row_count >= 1  # profile .toml files


async def test_autocomplete_spans_all_verbs_and_template_refs(ask_provided):
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)
        ac = app.query_one("#cmd-ac")
        cmds = {str(d.main) for d in ac.get_candidates(TargetState(text="", cursor_position=0))}
        assert {"ask", "submit", "run", "ls", "connect", "replay"} <= cmds
        # `run <here>` → template refs (pack/template)
        refs = [str(d.main) for d in ac.get_candidates(TargetState(text="run ", cursor_position=4))]
        assert any("/" in r for r in refs)


async def test_ask_hooks_are_offered_only_with_an_ask_provider(no_ask_provider):
    """ADR-0134 D6: without a provider's `ask`, the console offers no `ask` verb and no Ask
    palette entries, and `ask <intent>` runs nothing — while the Workflows tab and `run`
    completion, which read core's template list and not Ask, still work."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        ac = app.query_one("#cmd-ac")
        cmds = {str(d.main) for d in ac.get_candidates(TargetState(text="", cursor_position=0))}
        assert "ask" not in cmds and "submit" in cmds
        refs = [str(d.main) for d in ac.get_candidates(TargetState(text="run ", cursor_position=4))]
        assert any("/" in r for r in refs)
        assert app.query_one("#workflows", DataTable).row_count > 0
        assert not any(label.startswith("ask") for label, _, _ in app._palette_catalog())
        # a workflow still opens its menu; only the Ask-backed `plan` action is withheld
        assert any(label.startswith("workflow ") for label, _, _ in app._palette_catalog())
        offered: list = []
        app._act = lambda title, actions, cb: offered.append([a[0] for a in actions])
        app._wf_menu("demo/flow")
        assert offered == [["run"]]
        app._wf_action("plan", "demo/flow")
        app._dispatch('ask "fine-tune mistral"')
        assert seen == []


async def test_ask_runs_through_the_mounted_command(ask_provided):
    """With a provider's `ask`, `ask <intent>` shells `cairn ask run <intent>` — the command
    the provider mounted — instead of importing an Ask module (ADR-0134 D6)."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        app._dispatch('ask "fine-tune mistral under $100"')
        assert seen == [["ask", "run", "fine-tune mistral under $100"]]


async def test_approve_reject_keys_scoped_off_the_command_bar():
    """Root-cause of the 'console feels broken' bug: the global a/r bindings
    hijacked the command Input while typing. check_action must veto them while
    the Input is focused, but leave them live once focus is elsewhere (#56)."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        # the command bar is focused on mount → a/r must NOT fire (you're typing)
        assert isinstance(app.focused, Input)
        assert app.check_action("approve", ()) is False
        assert app.check_action("reject", ()) is False
        # unrelated actions are never vetoed
        assert app.check_action("help", ()) is True
        assert app.check_action("refresh", ()) is True
        # move focus off the command bar → a/r decide the gate again
        app.set_focus(app.query_one("#runs", DataTable))
        await pilot.pause()
        assert not isinstance(app.focused, Input)
        assert app.check_action("approve", ()) is True
        assert app.check_action("reject", ()) is True


async def test_autocomplete_dropdown_opens_on_typing():
    """Beyond the candidate logic: typing into the focused command bar must
    actually OPEN the dropdown (the visible affordance the user was missing)."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        assert isinstance(app.focused, Input)  # cmd bar has focus on mount
        await pilot.press("s", "u")            # type "su" → matches submit/status
        await pilot.pause()
        ac = app.query_one("#cmd-ac")
        assert app.query_one("#cmd", Input).value == "su"
        assert ac.display is True              # dropdown is shown, not just computed
        assert ac.option_list.option_count > 0


# ── Slice B — actionable entities (clickable buttons + modal forms) ─────


async def test_actions_modal_returns_action_id_and_cancel_is_none():
    """The actions modal dismisses with the pressed button's action id; the
    Cancel button dismisses None (the caller treats None as 'do nothing')."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        got: list = []
        app.push_screen(ActionsScreen("t", [("run", "Run", "success"), ("plan", "Plan", "primary")]),
                        lambda a: got.append(a))
        await pilot.pause()
        await pilot.click("#act-run")
        await pilot.pause()
        assert got == ["run"]
        # and Cancel → None
        app.push_screen(ActionsScreen("t", [("run", "Run", "success")]), lambda a: got.append(a))
        await pilot.pause()
        await pilot.click("#modal-close")
        await pilot.pause()
        assert got == ["run", None]


async def test_entity_rows_route_to_the_right_command(ask_provided):
    """Row actions dispatch to the existing verbs — replay/cancel/watch for
    runs, ask-plan for workflows, connect test/rm for connections."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        app._open_run = lambda rid: seen.append(["watch", rid])
        app._cancel = lambda rid: seen.append(["cancel", rid])
        app._wf_action("plan", "incident/incident-rca")
        app._run_action("replay", "run-123")
        app._run_action("cancel", "run-123")
        app._run_action("watch", "run-123")
        app._conn_action("test", "openai")
        app._conn_action("remove", "openai")
        assert ["ask", "plan", "incident/incident-rca"] in seen
        assert ["replay", "run-123"] in seen
        assert ["cancel", "run-123"] in seen
        assert ["watch", "run-123"] in seen
        assert ["connect", "test", "openai"] in seen
        assert ["connect", "rm", "openai"] in seen


async def test_profile_doctor_targets_selected_profile_via_env():
    """Profiles→Doctor runs `cairn setup --check` with OBS_PROFILE pointed at the
    selected profile file (doctor reads the profile from that env path)."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        captured: dict = {}
        app._shell = lambda toks, **k: captured.update(toks=toks, **k)
        app._prof_action("doctor", "/x/secure.toml")
        assert captured["toks"] == ["doctor"]  # `setup --check` by test_cli_calls_itself.py
        assert captured["env"] == {"CAIRN_PROFILE": "/x/secure.toml"}


async def test_workflow_run_form_falls_back_to_input_path_without_a_schema(monkeypatch):
    """A template with no declared inputs → the single `--input` path form
    (fallback), which shells `cairn run <ref> --input <path>`. (#109 Phase 1
    schema-generated forms are covered separately below.)"""
    import cairn.core.ui_render as render
    monkeypatch.setattr(render, "contract_to_form_fields", lambda c: [])
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        app._open_run_form("incident/incident-rca")
        await pilot.pause()
        assert isinstance(app.screen, FormScreen)
        app.screen.query_one("#f-input", Input).value = "/tmp/alert.json"
        await pilot.click("#form-submit")
        await pilot.pause()
        assert ["run", "incident/incident-rca", "--input", "/tmp/alert.json"] in seen


async def test_add_connection_form_shells_connect_add():
    """The Add-connection button opens a form, then shells
    `cairn connect add <provider> --name <name>` on submit."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        app._conn_add_btn()
        await pilot.pause()
        assert isinstance(app.screen, FormScreen)
        app.screen.query_one("#f-provider", Input).value = "openai"
        app.screen.query_one("#f-name", Input).value = "my-oai"
        await pilot.click("#form-submit")
        await pilot.pause()
        assert ["connect", "add", "openai", "--name", "my-oai"] in seen


# ── Slice C — command palette (Ctrl+P) ─────────────────────────────────

_PREFILL_RUN = "run …"  # the verb-prefill catalog entry for `run` (U+2026)


def test_command_palette_provider_is_registered():
    """The palette is wired by adding our Provider to App.COMMANDS — the
    palette UI + fuzzy matcher are Textual's, we only supply the catalog."""
    assert CairnCommands in CairnConsole.COMMANDS


async def test_palette_catalog_covers_verbs_skills_templates_runs(ask_provided):
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)  # registry load → skills/templates resolve
        displays = [d for d, _, _ in app._palette_catalog()]
        for instant in ("refresh", "ls", "doctor", "quit"):
            assert instant in displays
        assert _PREFILL_RUN in displays                       # verb prefill
        assert any(d.startswith("workflow ") for d in displays)  # templates


async def test_palette_verb_entry_prefills_and_focuses_command_bar():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        runner = {d: r for d, _, r in app._palette_catalog()}[_PREFILL_RUN]
        runner()
        await pilot.pause()
        inp = app.query_one("#cmd", Input)
        assert inp.value == "run "
        assert app.focused is inp


async def test_palette_instant_verb_dispatches_through_one_path():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test():
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        {d: r for d, _, r in app._palette_catalog()}["doctor"]()
        assert ["doctor"] in seen  # palette + command bar share _dispatch


async def test_palette_template_entry_opens_the_workflow_modal(ask_provided):
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)
        wf = next(r for d, _, r in app._palette_catalog() if d.startswith("workflow "))
        wf()
        await pilot.pause()
        assert isinstance(app.screen, ActionsScreen)


async def test_ctrl_p_opens_the_command_palette():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.press("ctrl+p")
        await pilot.pause()
        assert app.screen.__class__.__name__ == "CommandPalette"


# ── command-bar usability regressions ──────────────────────────────────


async def test_enter_runs_the_typed_command_not_autocomplete():
    """The bug behind 'the command doesn't work': the autocomplete swallowed
    Enter to apply a completion, so the command never ran. Enter must submit."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        ran: list = []
        app._dispatch = lambda line: ran.append(line)
        await pilot.press("l", "s")
        await pilot.pause()
        assert app.query_one("#cmd-ac").display is True  # dropdown is open...
        await pilot.press("enter")
        await pilot.pause()
        assert ran == ["ls"]                              # ...yet Enter runs it
        assert app.query_one("#cmd", Input).value == ""   # bar cleared on submit


async def test_tab_still_completes_in_the_command_bar():
    """Enter runs; Tab is what accepts a completion."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.press("s", "u", "b")
        await pilot.pause()
        await pilot.press("tab")
        await pilot.pause()
        assert app.query_one("#cmd", Input).value == "submit"


async def test_ctrl_q_quits_while_the_command_bar_is_focused():
    """A bare `q` is swallowed by the focused Input; ctrl+q (priority) quits."""
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        assert isinstance(app.focused, Input)
        await pilot.press("ctrl+q")
        await pilot.pause()
        assert app.is_running is False


# ── #109 Phase 1 — the workflow run form is generated from the pack schema ──


async def test_workflow_run_form_is_generated_from_the_input_schema():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)  # registries load → contract resolves
        app._open_run_form("training/finetune")
        await pilot.pause()
        assert isinstance(app.screen, FormScreen)
        # fields came from FinetuneRequest.schema.yaml — one per property,
        # NOT the single "--input path" fallback.
        app.screen.query_one("#f-dataset_path", Input)
        app.screen.query_one("#f-method", Input)


async def test_run_form_validates_then_runs_with_typed_inputs():
    app = CairnConsole(endpoint=_EP, token=None)
    async with app.run_test() as pilot:
        await pilot.pause(0.3)
        seen: list = []
        app._shell = lambda toks, **k: seen.append(toks)
        from cairn.core.ui_contract import build_ui_contract
        contract = build_ui_contract("training/finetune")
        # required dataset_path blank → validation fails → no run
        app._run_with_contract("training/finetune", contract, {"dataset_path": ""})
        assert seen == []
        # valid → shells `run <ref> --input <tmp json>`
        app._run_with_contract("training/finetune", contract, {"dataset_path": "s3://d/t.jsonl"})
        assert seen and seen[0][:2] == ["run", "training/finetune"] and "--input" in seen[0]
