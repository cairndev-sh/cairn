"""GHSA-gw2v-3922-g7gm — ``cairn doctor`` never prints a URL's user or password.

Its ``set:`` rows printed ``CAIRN_BUS``, ``CAIRN_TRIGGER_INBOX`` and
``CAIRN_JOB_CHANNEL`` verbatim (they are not marked secret, yet hold DSNs), the masked
``CAIRN_STORAGE`` row kept 8 characters (``user:sec…`` for a DSN missing its scheme),
and the ``config[<key>]`` rows printed the raw value. Doctor output is what people
paste into issues.
"""
from __future__ import annotations

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
