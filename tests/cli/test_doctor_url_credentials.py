"""GHSA-gw2v-3922-g7gm — ``cairn doctor`` never prints a URL's user or password.

Its ``set:`` rows printed ``CAIRN_BUS``, ``CAIRN_TRIGGER_INBOX`` and
``CAIRN_JOB_CHANNEL`` verbatim (they are not marked secret, yet hold DSNs), the masked
``CAIRN_STORAGE`` row kept 8 characters (``user:sec…`` for a DSN missing its scheme),
and the ``config[<key>]`` rows printed the raw value. Doctor output is what people
paste into issues.
"""
from __future__ import annotations

import asyncio

import pytest

from cairn.cli.commands import doctor as doctor_mod

_KNOBS = ("CAIRN_STORAGE", "CAIRN_BUS", "CAIRN_TRIGGER_INBOX", "CAIRN_JOB_CHANNEL")


@pytest.fixture(autouse=True)
def _no_ambient(monkeypatch):
    for var in (*_KNOBS, "CAIRN_PROFILE", "OBS_PROFILE", "OBS_STORAGE", "OBS_BUS"):
        monkeypatch.delenv(var, raising=False)


def _rows(monkeypatch, env, value):
    monkeypatch.setenv(env, value)
    return doctor_mod.check_settings() + doctor_mod._backend_provenance_checks()


@pytest.mark.parametrize("env", _KNOBS)
@pytest.mark.parametrize("value", [
    "postgresql://alice:s3cret@db.invalid:5432/cairn",
    "alice:s3cret@db.invalid:5432/cairn",
    "nats://alice:s3cret@db.invalid:4222",
])
def test_no_row_prints_the_credentials(monkeypatch, env, value):
    for r in _rows(monkeypatch, env, value):
        assert "alice" not in r.detail and "s3cret" not in r.detail, (r.name, r.detail)


def test_a_dsn_row_still_names_where_it_points(monkeypatch):
    rows = {r.name: r.detail for r in _rows(
        monkeypatch, "CAIRN_BUS", "postgresql://alice:s3cret@db.invalid:5432/cairn")}
    assert rows["CAIRN_BUS"] == "set: postgresql://db.invalid:5432/cairn"
    assert rows["config[event_bus]"].startswith("event_bus=postgres ")


def test_a_plain_value_is_unchanged(monkeypatch):
    rows = {r.name: r.detail for r in _rows(monkeypatch, "CAIRN_JOB_CHANNEL", "http")}
    assert rows["CAIRN_JOB_CHANNEL"] == "set: http"


TOKEN = "T0AAA/B0BBB/SEKRETTOKENVALUE"
WEBHOOK = f"https://hooks.slack.com/services/{TOKEN}"


def test_a_webhook_row_keeps_its_host_and_hides_its_token(monkeypatch):
    """A secret URL's path can be the secret: a Slack webhook's token is in it."""
    rows = {r.name: r.detail for r in _rows(monkeypatch, "SLACK_WEBHOOK_URL", WEBHOOK)}
    assert rows["SLACK_WEBHOOK_URL"] == "set: https://hooks.slack.com/***"


def test_the_reachability_row_hides_it_too(monkeypatch):
    async def reachable(url):
        assert url == WEBHOOK, "the probe still goes to the real URL"
        return True, "HTTP 200"

    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK)
    monkeypatch.setattr(doctor_mod, "_probe_url", reachable)
    row = asyncio.run(doctor_mod.check_slack_webhook())
    assert row.detail == "https://hooks.slack.com/*** — HTTP 200"


@pytest.mark.parametrize("value, shown", [
    ("https://hooks.example.com/hook/abc123", "https://hooks.example.com/***"),
    ("https://user:pw@hooks.example.com:8443/hook?key=abc123", "https://hooks.example.com:8443/***"),
    ("https://hooks.example.com", "https://hooks.example.com"),
    ("wss://hooks.example.com/abc123", "wss://hooks.example.com/***"),
])
def test_a_secret_url_shows_only_where_it_points(value, shown):
    assert doctor_mod._mask(value) == shown


@pytest.mark.parametrize("value, shown", [
    ("postgresql://alice:s3cret@db.invalid:5432/cairn", "postgresql://db.invalid:5432/cairn"),
    ("postgresql+asyncpg://alice:s3cret@db.invalid/cairn", "postgresql+asyncpg://db.invalid/cairn"),
    ("sqlite:////var/cairn/dev.db", "sqlite:////var/cairn/dev.db"),
])
def test_a_secret_database_url_still_names_its_database(value, shown):
    assert doctor_mod._mask(value) == shown
