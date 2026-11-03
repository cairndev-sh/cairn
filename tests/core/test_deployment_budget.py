"""ADR-0155 — the deployment budget: the profile table, the effective cap and the explain row."""
from __future__ import annotations

import math

import pytest

from cairn.core.dispatch.pipeline_launch import effective_budget
from cairn.core.engine.pipeline_resolved import governance_rows
from cairn.core.pack.profile import ProfileError, load_profile


def _profile(tmp_path, body, name="p.toml"):
    p = tmp_path / name
    p.write_text(body)
    return load_profile(p)


# ── D1: the profile table ───────────────────────────────────────────────────

def test_a_profile_parses_the_budget_table(tmp_path):
    prof = _profile(tmp_path, "[profile.budget]\ndefault_usd = 20\nceiling_usd = 100\nlocal_hourly_usd = 0.8\n")
    assert prof.budget == {"default_usd": 20.0, "ceiling_usd": 100.0, "local_hourly_usd": 0.8}
    assert _profile(tmp_path, "[profile.backends]\n", "q.toml").budget == {}


@pytest.mark.parametrize("body", [
    "[profile.budget]\nlimit_usd = 1\n",                             # unknown key
    "[profile.budget]\nceiling_usd = -1\n",                          # negative
    "[profile.budget]\nceiling_usd = nan\n",                         # NaN
    "[profile.budget]\nceiling_usd = inf\n",                         # not finite
    "[profile.budget]\nceiling_usd = \"5\"\n",                       # not a number
    "[profile.budget]\nceiling_usd = true\n",                        # a bool is not a number
    "[profile.budget]\ndefault_usd = 50\nceiling_usd = 10\n",        # default above ceiling
])
def test_a_malformed_budget_table_fails_parsing(tmp_path, body):
    with pytest.raises(ProfileError, match="budget"):
        _profile(tmp_path, body)


def test_a_budget_table_must_be_a_table(tmp_path):
    with pytest.raises(ProfileError, match=r"\[profile.budget\] must be a table"):
        _profile(tmp_path, "[profile]\nbudget = 5\n")


def test_extends_replaces_per_key_and_still_checks_the_order(tmp_path):
    (tmp_path / "base.toml").write_text("[profile.budget]\ndefault_usd = 5\nceiling_usd = 50\nlocal_hourly_usd = 1\n")
    child = _profile(tmp_path, '[profile]\nextends = "./base.toml"\n[profile.budget]\nceiling_usd = 80\n', "c.toml")
    assert child.budget == {"default_usd": 5.0, "ceiling_usd": 80.0, "local_hourly_usd": 1.0}
    with pytest.raises(ProfileError, match="above ceiling_usd"):
        _profile(tmp_path, '[profile]\nextends = "./base.toml"\n[profile.budget]\nceiling_usd = 2\n', "d.toml")


# ── D2: the effective cap, every row of the table ───────────────────────────

def _gov(usd=None):
    return {"budget": {"usd": usd}} if usd is not None else {}


@pytest.mark.parametrize("gov,dep,usd,source", [
    (_gov(8), {}, 8, "pipeline"),
    (_gov(8), {"ceiling_usd": 100}, 8, "pipeline"),
    (_gov(80), {"ceiling_usd": 50}, 50, "deployment_ceiling"),
    (_gov(50), {"ceiling_usd": 50}, 50, "pipeline"),
    (_gov(), {"default_usd": 20}, 20, "deployment_default"),
    (_gov(), {"default_usd": 20, "ceiling_usd": 100}, 20, "deployment_default"),
    (_gov(), {"ceiling_usd": 100}, 100, "deployment_ceiling"),
    (_gov(8), {"default_usd": 20}, 8, "pipeline"),
    (_gov(80), {"default_usd": 20, "ceiling_usd": 50}, 50, "deployment_ceiling"),
])
def test_the_effective_budget_is_the_smaller_cap(gov, dep, usd, source):
    eff = effective_budget(gov, dep)
    assert (eff["usd"], eff["source"]) == (usd, source)
    assert eff["declared_usd"] == (gov.get("budget") or {}).get("usd")
    assert eff["default_usd"] == dep.get("default_usd") and eff["ceiling_usd"] == dep.get("ceiling_usd")


def test_nothing_declared_and_nothing_set_is_unbounded():
    assert effective_budget({}, {}) == {}
    assert effective_budget({"series": "s"}, {"local_hourly_usd": 1.0}) == {}


def test_the_profile_object_is_accepted_as_well_as_its_table(tmp_path):
    prof = _profile(tmp_path, "[profile.budget]\nceiling_usd = 9\n")
    assert effective_budget({}, prof)["usd"] == 9 and effective_budget({}, prof.budget)["usd"] == 9
    assert math.isfinite(effective_budget({}, prof)["usd"])


# ── D7: explain shows the cap the launch door enforces ──────────────────────

def _budget_row(gov, *, dep=None, workspace=None):
    rows = governance_rows(gov, workspace=workspace or {}, profile_name="prod", deployment_budget=dep)
    return next(r for r in rows if r["key"] == "budget_usd")


def test_explain_shows_the_deployment_layer_when_it_decides_the_cap():
    row = _budget_row({}, dep={"default_usd": 20, "ceiling_usd": 100})
    assert (row["value"], row["source"]) == (20, "profile:prod") and "default" in row["detail"]
    row = _budget_row({"budget": {"usd": 500}}, dep={"ceiling_usd": 100})
    assert (row["value"], row["source"]) == (100, "profile:prod")
    assert "above the deployment's ceiling" in row["detail"] and "runs under the ceiling" in row["detail"]


def test_explain_keeps_the_pipeline_figure_when_it_is_the_cap():
    row = _budget_row({"budget": {"usd": 8}}, dep={"ceiling_usd": 100})
    assert (row["value"], row["source"]) == (8, "pipeline")


def test_explain_no_longer_reads_the_workspace_cost_cap_for_a_pipeline():
    row = _budget_row({}, workspace={"cost_cap_usd": 7})
    assert row["value"] is None and row["source"] == "unknown"
    row = _budget_row({"budget": {"usd": 8}}, workspace={"cost_cap_usd": 7})
    assert (row["value"], row["source"]) == (8, "pipeline")
    series = next(r for r in governance_rows({}, workspace={"series": "s-ws"}, profile_name="")
                  if r["key"] == "series")
    assert series["value"] == "s-ws" and series["source"] == "workspace"
