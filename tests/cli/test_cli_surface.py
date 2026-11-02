"""The `cairn` command tree is pinned to ADR-0165 (D1, D8, D10, D11, D12).

The table in D10 is read from the ADR, so there is one copy. A command in the tree with no row
fails here, and so does a row whose new name does not work or whose old name does not warn.
"""
from __future__ import annotations

import importlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

import cairn.cli.main as cli_main
import cairn.cli.providers as cli_providers

ADR = Path(__file__).parents[2] / "docs/decisions/ADR-0165-the-cli-is-five-commands.md"
_ANSI = re.compile(r"\x1b\[[0-9;]*m")  # typer forces colour under GITHUB_ACTIONS


# ── reading the ADR ───────────────────────────────────────────────────────────────────────────

def d1_block() -> dict[str, dict[str, str]]:
    """D1's headings that always show: {heading: {command: its help line}}."""
    block = ADR.read_text().split("**D1 ", 1)[1].split("```text\n", 1)[1].split("```", 1)[0]
    out: dict[str, dict[str, str]] = {}
    heading = ""
    for line in block.splitlines():
        if line and not line.startswith(" "):
            heading = line.split(":", 1)[0]
            out[heading] = {}
        elif heading and line.strip() and heading != "Platform commands":  # names only, and only when installed
            name, text = line.split(maxsplit=1)
            out[heading][name] = text.strip()
    return out


def d10_rows() -> dict[str, str]:
    """The core half of D10: {`cairn <today>`: what its After column says}."""
    table = ADR.read_text().split("### Core", 1)[1].split("### Platform", 1)[0]
    rows = {}
    for today, after in re.findall(r"^\| `(cairn [^`]+)` \| (.+) \|$", table, re.MULTILINE):
        rows[today] = after
    return rows


def spellings(today: str, after: str) -> list[str]:
    """The command lines an After cell names: the row itself when it is `unchanged`."""
    if after.startswith("unchanged"):
        return [today]
    return [span for span in re.findall(r"`([^`]+)`", after) if span.startswith("cairn ")]


ROWS = {today: after for today, after in d10_rows().items() if today != "cairn --version"}
RENAMED = {today: after for today, after in ROWS.items() if not after.startswith(("unchanged", "removed"))}


# ── walking the tree ──────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def public_app():
    """`cairn.cli.main` as a public install sees it: no `cairn.cli` provider."""
    mp = pytest.MonkeyPatch()
    mp.setattr(cli_providers, "entry_points", lambda group: [])
    try:
        yield importlib.reload(cli_main).app
    finally:
        mp.undo()
        importlib.reload(cli_main)


def tree(app: typer.Typer) -> dict[str, list]:
    """Every command path in ``app``, hidden ones too: {"cairn a b": [root, a, b]}."""
    out: dict[str, list] = {}

    def walk(cmd, chain: list) -> None:
        out[" ".join(["cairn", *(c.name for c in chain[1:])])] = chain
        for sub in getattr(cmd, "commands", {}).values():
            walk(sub, [*chain, sub])

    root = typer.main.get_command(app)
    walk(root, [root])
    return out


def unlisted(commands: dict[str, list], rows: dict[str, str]) -> set[str]:
    """Command paths with no row: not a row's today, not named by any row, not a group of those."""
    named = {" ".join(s.split()[:i]) for today, after in rows.items() for s in spellings(today, after)
             for i in range(1, len(_words(s)) + 1)}
    return {path for path in commands if path != "cairn" and path not in rows and path not in named}


def _words(spelling: str) -> list[str]:
    """A spelling's leading command words, up to its first flag or placeholder."""
    words: list[str] = []
    for word in spelling.split():
        if word.startswith(("-", "<")):
            break
        words.append(word)
    return words


def resolve(commands: dict[str, list], spelling: str) -> str:
    """The deepest command a spelling reaches; words past it are arguments (`cairn decide reject`).
    A group takes no arguments, so a word past a group is a missing command: it resolves to none."""
    words = _words(spelling)
    full = list(words)
    while words and " ".join(words) not in commands:
        words.pop()
    reached = " ".join(words)
    if len(words) < len(full) and getattr(commands.get(reached, [None])[-1], "commands", None):
        return ""
    return reached


def help_panels(out: str) -> dict[str, dict[str, str]]:
    """A rich `--help` page's panels, in order: {heading: {command: its help line}}."""
    panels: dict[str, dict[str, str]] = {}
    current: dict[str, str] = {}
    for line in _ANSI.sub("", out).splitlines():
        title = re.match(r"╭─ (.+?) ─+╮", line)
        row = re.match(r"│ ([a-z][a-z-]*)\s+(.*?)\s*│$", line)
        if title:
            current = panels.setdefault(title.group(1), {})
        elif row:
            current[row.group(1)] = row.group(2)
    return panels


def _command_panels(app: typer.Typer) -> dict[str, dict[str, str]]:
    res = CliRunner().invoke(app, ["--help"], env={"COLUMNS": "200", "TERMINAL_WIDTH": "200"})
    assert res.exit_code == 0, res.output
    panels = help_panels(res.output)
    panels.pop("Options", None)
    return panels


# ── D1: what `--help` leads with ──────────────────────────────────────────────────────────────

def _d1_without_providers() -> dict[str, dict[str, str]]:
    return {h: rows for h, rows in d1_block().items() if h != "Platform commands"}


def test_help_is_what_d1_says(public_app) -> None:
    panels = _command_panels(public_app)
    assert list(panels) == list(_d1_without_providers())
    for heading, rows in _d1_without_providers().items():
        assert {n: t.rstrip(".") for n, t in panels[heading].items()} == rows
        assert list(panels[heading]) == list(rows)  # in D1's order, too


def test_the_d1_check_can_fail() -> None:
    extra = typer.Typer()
    extra.command("run", rich_help_panel="Main commands")(lambda: None)
    extra.command("made-up", rich_help_panel="Main commands")(lambda: None)
    assert list(_command_panels(extra)["Main commands"]) != list(d1_block()["Main commands"])


# ── D10, D11: every command has a row, every row works ────────────────────────────────────────

def test_the_table_was_read() -> None:
    assert len(ROWS) > 70 and "cairn remote advance" in ROWS and "cairn pilot" not in ROWS


def test_every_command_has_a_row(public_app) -> None:
    assert unlisted(tree(public_app), ROWS) == set()


def test_a_new_name_missing_under_its_group_is_not_found(public_app) -> None:
    commands = tree(public_app)
    assert resolve(commands, "cairn admin connect rm") == "cairn admin connect rm"
    assert resolve(commands, "cairn admin connect made-up") == ""
    assert resolve(commands, "cairn decide reject") == "cairn decide"  # an argument, not a command


def test_an_unlisted_command_fails_the_tree_test() -> None:
    app = typer.Typer()
    app.command("run")(lambda: None)
    app.command("made-up")(lambda: None)
    assert unlisted(tree(app), {"cairn run": "unchanged"}) == {"cairn made-up"}


@pytest.mark.parametrize("today", sorted(ROWS))
def test_a_row_still_answers_under_its_old_name(public_app, today: str) -> None:
    if ROWS[today].startswith("removed"):
        pytest.skip("a group whose commands have their own rows")
    assert today in tree(public_app)


@pytest.mark.parametrize("today", sorted(RENAMED))
def test_a_row_works_under_its_new_name(public_app, today: str) -> None:
    commands = tree(public_app)
    for spelling in spellings(today, ROWS[today]):
        reached = resolve(commands, spelling)
        assert reached and reached != "cairn", f"{spelling}: no such command"
        chain = commands[reached]
        assert not any(c.hidden for c in chain), f"{spelling} is hidden"
        options = {o for p in chain[-1].params for o in getattr(p, "opts", [])}
        flags = {w for w in spelling.split() if w.startswith("-")}
        assert flags <= options, f"{spelling}: {flags - options} missing"


@pytest.mark.parametrize("today", sorted(RENAMED))
def test_an_old_name_is_hidden_and_warns(public_app, today: str) -> None:
    commands = tree(public_app)
    chain = commands[today]
    assert any(c.hidden for c in chain), f"{today} is listed in --help"
    if chain[-1].callback is None:  # a bare group: its commands have their own rows
        return
    said = getattr(chain[-1].callback, "renamed_to", None)
    assert said, f"{today} does not say its new name"
    allowed = {resolve(commands, s) for s in spellings(today, ROWS[today])}
    assert resolve(commands, said) in allowed, f"{today} says {said}, the table says {ROWS[today]}"


@pytest.mark.parametrize("old, new", [
    (["connect", "providers"], ["admin", "connect", "providers"]),
    (["init", "policy", "--help"], ["pack", "new", "policy", "--help"]),
])
def test_old_and_new_run_the_same_code(public_app, old: list[str], new: list[str]) -> None:
    run = CliRunner().invoke
    before, after = run(public_app, old), run(public_app, new)
    assert before.exit_code == after.exit_code == 0
    if "--help" not in old:
        assert before.stdout == after.stdout
        assert "is now `cairn admin connect providers`" in before.stderr and not after.stderr


def test_a_group_that_is_a_command_warns_when_it_runs(public_app, monkeypatch) -> None:
    import cairn.cli.commands.demo as demo

    monkeypatch.setattr(demo, "seed_demo_workspace", lambda *a, **k: _async({"runs": 0, "pipelines": 0}))
    res = CliRunner().invoke(public_app, ["demo"])
    assert "`cairn demo` is now `cairn admin demo`" in res.stderr
    assert "is now" not in CliRunner().invoke(public_app, ["admin", "demo"]).stderr


async def _async(value):
    return value


# ── D8: providers ─────────────────────────────────────────────────────────────────────────────

class _Ep:
    """The attributes `load_cli_providers` reads from an entry point."""

    def __init__(self, name: str, provider=None, dist: str | None = None) -> None:
        self.name, self.value, self._provider = name, f"fake:{name}", provider
        self.dist = SimpleNamespace(name=dist) if dist else None

    def load(self):
        if self._provider is None:
            raise AssertionError("a refused provider's code is never imported")
        return self._provider


def _group(heading: str | None = None) -> typer.Typer:
    group = typer.Typer(**({"rich_help_panel": heading} if heading else {}))
    group.command("one")(lambda: typer.echo("one ran"))
    group.command("two")(lambda: typer.echo("two ran"))
    return group


def _single(heading: str | None = None) -> typer.Typer:
    single = typer.Typer(**({"rich_help_panel": heading} if heading else {}))
    single.command("only")(lambda: typer.echo("only ran"))
    return single


@pytest.fixture
def with_providers(monkeypatch):
    """`cairn.cli.main` re-imported with exactly the given providers."""
    def load(*eps: _Ep):
        monkeypatch.setattr(cli_providers, "entry_points",
                            lambda group: list(eps) if group == cli_providers.ENTRY_POINT_GROUP else [])
        return importlib.reload(cli_main)

    yield load
    monkeypatch.undo()
    importlib.reload(cli_main)


def test_a_provider_names_its_heading_between_main_and_the_rest(with_providers) -> None:
    main = with_providers(_Ep("platform", lambda: [("grp", _group("Platform commands")),
                                                   ("solo", _single("Platform commands"))]))
    panels = _command_panels(main.app)
    assert list(panels)[-2:] == ["Platform commands", "All other commands"]
    assert "Main commands" in panels and list(panels).index("Main commands") < list(panels).index("Platform commands")
    assert set(panels["Platform commands"]) == {"grp", "solo"}
    assert list(panels["All other commands"]) == ["admin"]


def test_a_provider_that_names_no_heading_gets_its_entry_point_name(with_providers) -> None:
    main = with_providers(_Ep("platform", lambda: [("grp", _group()), ("solo", _single())]))
    assert set(_command_panels(main.app)["Platform commands"]) == {"grp", "solo"}


@pytest.mark.parametrize("taken", [cli_providers.MAIN_COMMANDS, cli_providers.OTHER_COMMANDS])
def test_a_provider_cannot_take_a_public_heading(with_providers, taken: str) -> None:
    main = with_providers(_Ep("platform", lambda: [("grp", _group(taken)), ("solo", _single(taken))]))
    panels = _command_panels(main.app)
    assert set(panels["Platform commands"]) == {"grp", "solo"}
    assert not {"grp", "solo"} & set(panels["Main commands"]) and list(panels["All other commands"]) == ["admin"]


@pytest.mark.parametrize("dist", ["cairn-pack-finetune", "cairn-operator-training", "cairn-contracts-training",
                                  "cairn_operator_training", "Cairn.Pack.Finetune"])
def test_a_pack_operator_or_contract_wheel_adds_no_command(with_providers, dist: str, caplog) -> None:
    main = with_providers(_Ep("sneaky", None, dist=dist),
                          _Ep("platform", lambda: [("grp", _group("Platform commands"))], dist="cairn-platform"))
    assert main.PROVIDER_COMMANDS == ["grp"]
    assert any("sneaky" in r.getMessage() and "adds no command" in r.getMessage() for r in caplog.records)
    res = CliRunner().invoke(main.app, ["grp", "one"])
    assert res.exit_code == 0 and res.output == "one ran\n"
    assert CliRunner().invoke(main.app, ["sneaky"]).exit_code != 0


def test_a_refused_wheel_does_not_shadow_a_provider_of_the_same_name(with_providers) -> None:
    good = _Ep("grp", lambda: [("grp", _group())], dist="cairn-platform")
    main = with_providers(_Ep("grp", None, dist="cairn-operator-evil"), good)
    assert main.PROVIDER_COMMANDS == ["grp"]


def test_the_doctor_shows_a_refused_wheel(with_providers) -> None:
    from cairn.cli.commands.doctor import check_cli_providers

    with_providers(_Ep("sneaky", None, dist="cairn-operator-training"))
    rows = {r.name: r for r in check_cli_providers()}
    assert rows["cli_provider[sneaky]"].status == "warn"
    assert "cairn-operator-training" in rows["cli_provider[sneaky]"].detail


# ── D6: `cairn pack` ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def run(public_app, example_pack):
    return lambda *argv: CliRunner().invoke(public_app, list(argv))


def test_pack_new_scaffolds_a_pack_that_pack_check_passes(run, tmp_path) -> None:
    res = run("pack", "new", "pack", "demo_pack", "--dest", str(tmp_path / "demo_pack"))
    assert res.exit_code == 0, res.output
    assert (tmp_path / "demo_pack" / "pack.toml").is_file()
    assert "cairn pack check" in res.output and "cairn validate" not in res.output
    res = run("pack", "check", str(tmp_path / "demo_pack"))
    assert res.exit_code == 0, res.output
    assert "operator manifest" in res.output or "drift" in res.output  # the second half of the check ran


def test_pack_check_takes_paths_and_its_exit_code_is_the_worst_of_both(run, tmp_path, monkeypatch) -> None:
    bad = tmp_path / "pipelines"
    bad.mkdir()
    (bad / "broken.yaml").write_text("not: [a pipeline\n")
    assert run("pack", "check", str(bad)).exit_code == 1
    empty = tmp_path / "nothing"
    empty.mkdir()
    assert run("pack", "check", str(empty)).exit_code == 1
    nothing = run("pack", "check", str(empty), "--json")
    assert nothing.exit_code == 1 and "no artifacts found" in nothing.stderr

    import cairn.core.pack.drift as drift

    good = run("pack", "new", "pack", "ok_pack", "--dest", str(tmp_path / "ok_pack"))
    assert good.exit_code == 0, good.output
    finding = drift.DriftFinding(kind=sorted(drift.DRIFT_KINDS)[0], operator="x/y", detail="drifted")
    monkeypatch.setattr(drift, "check_installed_operator_drift", lambda: [finding])
    assert run("pack", "check").exit_code == 1
    assert run("pack", "check", str(tmp_path / "ok_pack")).exit_code == 1  # the paths are fine; the drift is not


def test_pack_check_json(run, tmp_path, monkeypatch) -> None:
    import cairn.core.pack.drift as drift

    monkeypatch.setattr(drift, "check_installed_operator_drift", lambda: [])
    assert json.loads(run("pack", "check", "--json").stdout) == []  # unchanged for CI without paths
    assert run("pack", "new", "pack", "ok_pack", "--dest", str(tmp_path / "ok_pack")).exit_code == 0
    res = run("pack", "check", str(tmp_path / "ok_pack"), "--json")
    assert res.exit_code == 0, res.output
    doc = json.loads(res.stdout)
    assert doc["drift"] == [] and doc["artifacts"] and all(a["ok"] for a in doc["artifacts"])
    (tmp_path / "ok_pack" / "pipelines" / "broken.yaml").write_text("not: [a pipeline\n")
    res = run("pack", "check", str(tmp_path / "ok_pack"), "--json")
    assert res.exit_code == 1 and not all(a["ok"] for a in json.loads(res.stdout)["artifacts"])


@pytest.mark.parametrize("argv", [["--strict"], ["--template-path", "x"]])
def test_pack_check_refuses_a_path_option_without_paths(run, argv: list[str]) -> None:
    assert run("pack", "check", *argv).exit_code == 2


def test_pack_ls_lists_pipelines_and_flows(run) -> None:
    pipelines = json.loads(run("pack", "ls", "--pipelines", "--json").stdout)
    assert any(r["ref"] == "hello-cairn/profile-and-publish" for r in pipelines)
    assert "hello-cairn/profile-and-publish" in run("pack", "ls", "--pipelines").stdout
    flows = json.loads(run("pack", "ls", "--flows", "--json").stdout)
    assert any(r["ref"].startswith("hello-cairn/") for r in flows)
    assert "hello-cairn/" in run("pack", "ls", "--flows").stdout
    assert json.loads(run("pack", "ls", "no-such-pack", "--flows", "--json").stdout) == []
    assert json.loads(run("pack", "ls", "hello-cairn", "--pipelines", "--json").stdout) == pipelines


@pytest.mark.parametrize("argv", [["--flows", "--pipelines"], ["--flows", "--unused"], ["operators", "--pipelines"]])
def test_pack_ls_refuses_views_that_do_not_combine(run, argv: list[str]) -> None:
    assert run("pack", "ls", *argv).exit_code == 2


def test_pack_ls_without_a_view_is_the_old_listing(run) -> None:
    res = run("pack", "ls", "hello-cairn")
    assert res.exit_code == 0 and res.stdout.startswith("hello-cairn (")


def test_pack_show_has_one_door_for_each_view(run) -> None:
    ref = "hello-cairn/profile-and-publish"
    assert ref in run("pack", "show", ref).stdout
    assert run("pack", "show", ref, "--plan").exit_code == 0
    assert json.loads(run("pack", "show", ref, "--json").stdout)["name"] == "profile-and-publish"
    assert json.loads(run("pack", "show", ref, "--plan", "--json").stdout)["steps"]
    flow = next(r["ref"] for r in json.loads(run("pack", "ls", "--flows", "--json").stdout) if r["kind"] != "agentic")
    res = run("pack", "show", flow, "--split", "--json")
    assert res.exit_code == 0, res.output
    assert json.loads(res.stdout)[0]["stages"]


@pytest.mark.parametrize("argv", [
    ["--plan", "--split"], ["--split", "--graph"], ["--output", "f"], ["--graph", "--json"],
    ["--split", "-i", "a=b"], ["--graph", "--preset", "p"],
])
def test_pack_show_refuses_what_does_not_combine(run, argv: list[str]) -> None:
    assert run("pack", "show", "hello-cairn/profile-and-publish", *argv).exit_code == 2


@pytest.mark.parametrize("argv", [
    ["--config", "a=b"], ["--to", "pipeline", "--stdout", "--out", "f"],
    ["--to", "metaflow", "--demo", "none"], ["--to", "metaflow", "--profile", "p"],
])
def test_pack_export_refuses_what_does_not_apply_to_its_target(run, argv: list[str]) -> None:
    assert run("pack", "export", "hello-cairn/x", *argv).exit_code == 2


def test_pack_export_to_metaflow_is_the_flow_export(run, tmp_path) -> None:
    res = run("pack", "export", "no-such/flow", "--to", "metaflow", "--stdout")
    assert res.exit_code == 1 and "cannot export" in res.output


# ── D7, D9: `cairn admin` ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", [["trigger", "ls"], ["trigger", "enable"], ["trigger", "disable"],
                                  ["preset", "ls"], ["preset", "rm"], ["console"]])
def test_admin_engine_commands_take_long_options_only(public_app, path: list[str]) -> None:
    chain = tree(public_app)[" ".join(["cairn", "admin", *path])]
    options = {o for p in chain[-1].params for o in getattr(p, "opts", [])}
    assert {"--endpoint", "--token", "--profile"} <= options
    assert "-e" not in options


def test_an_old_remote_alias_keeps_e_as_the_endpoint(public_app) -> None:
    chain = tree(public_app)["cairn remote triggers"]
    assert "-e" in {o for p in chain[-1].params for o in getattr(p, "opts", [])}


# ── D6, D7: every option the ADR lists ────────────────────────────────────────────────────────

def _listed_options() -> dict[str, set[str]]:
    """The command lines of D6 and D7's code blocks: {`cairn pack show`: its --options}."""
    text = ADR.read_text()
    blocks = [text.split(f"**{d} ", 1)[1].split("```text\n", 1)[1].split("```", 1)[0] for d in ("D6", "D7")]
    out: dict[str, set[str]] = {}
    current = ""
    for line in "\n".join(blocks).splitlines():
        if line.startswith("cairn "):
            current = " ".join(_words(re.sub(r" [A-Z\[].*", "", line)))
            current = re.sub(r" (pack\|.*)$", "", current)
            out.setdefault(current, set())
        out[current] |= set(re.findall(r"(?<![\w-])--?[a-zA-Z][\w-]*", line)) if current else set()
    return out


@pytest.mark.parametrize("spelling", sorted(_listed_options()))
def test_an_adr_listed_command_takes_every_option_listed(public_app, spelling: str) -> None:
    commands = tree(public_app)
    assert resolve(commands, spelling), f"{spelling}: no such command"
    chain = commands[resolve(commands, spelling)]
    assert len(chain) > 2, f"{spelling}: no such command"
    cmds = [chain[-1], *getattr(chain[-1], "commands", {}).values()]  # `pack new`'s are its subcommands'
    options = {o for c in cmds for p in c.params for o in getattr(p, "opts", [])}
    assert _listed_options()[spelling] <= options, _listed_options()[spelling] - options
