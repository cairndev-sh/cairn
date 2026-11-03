"""GHSA-gw2v-3922-g7gm — a storage or bus URL's user and password are never printed.

``default_store()``'s refusal echoed the raw ``CAIRN_STORAGE``, and the backend kind
``cairn doctor`` prints fell back to the raw value when it had no scheme (a DSN missing
it, or with a typo). Both now name at most the scheme; ``redacted_url`` is the one
place a diagnostic turns a URL into something printable.
"""
from __future__ import annotations

import pytest

from cairn.core import settings
from cairn.core.profile_backends import effective_backends
from cairn.core.storage import default_store

# user, password and host of every leaking value below
_SECRETS = ("alice", "s3cret", "db.invalid")

_LEAKY = [
    "alice:s3cret@db.invalid:5432/cairn",                           # scheme missing
    "postgress://alice:s3cret@db.invalid:5432/cairn",               # scheme typo
    "POSTGRES://alice:s3cret@db.invalid/cairn",                     # the store is case-sensitive
    "mysql://alice:s3cret@db.invalid/cairn",
    "//alice:s3cret@db.invalid/cairn",
    "foo://alice:s3cret@[::1/cairn",                                # unparseable netloc
]


def _clean(text: str) -> bool:
    return not any(s in text for s in _SECRETS)


@pytest.mark.parametrize("value", _LEAKY)
def test_store_refusal_never_echoes_the_value(monkeypatch, value):
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.setenv("CAIRN_STORAGE", value)
    with pytest.raises(ValueError, match="CAIRN_STORAGE is not a storage URL") as ei:
        default_store()
    assert _clean(str(ei.value)), str(ei.value)


def test_store_refusal_names_the_scheme(monkeypatch):
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.setenv("CAIRN_STORAGE", "mysql://alice:s3cret@db.invalid/cairn")
    with pytest.raises(ValueError, match=r"a mysql:// URL"):
        default_store()


@pytest.mark.parametrize("env", ["CAIRN_STORAGE", "CAIRN_BUS", "CAIRN_TRIGGER_INBOX", "CAIRN_JOB_CHANNEL"])
@pytest.mark.parametrize("value", _LEAKY + ["postgresql://alice:s3cret@db.invalid:5432/cairn"])
def test_backend_kind_never_carries_the_value(env, value):
    kinds = effective_backends({}, env={env: value})
    assert all(_clean(str(v)) for v in kinds.values()), kinds


@pytest.mark.parametrize(("env", "value", "kind"), [
    ("CAIRN_STORAGE", "postgresql://alice:s3cret@db.invalid/cairn", "postgres"),
    ("CAIRN_BUS", "postgresql://alice:s3cret@db.invalid/cairn", "postgres"),
    ("CAIRN_BUS", "inprocess", "inprocess"),
    ("CAIRN_JOB_CHANNEL", "http", "http"),
    ("CAIRN_JOB_CHANNEL", "nats://alice:s3cret@db.invalid:4222", "nats"),
    ("CAIRN_TRIGGER_INBOX", "memory", "memory"),
    ("CAIRN_STORAGE", "alice:s3cret@db.invalid:5432/cairn", "unrecognized"),
])
def test_backend_kind_keeps_the_kind(env, value, kind):
    key = {"CAIRN_STORAGE": "storage", "CAIRN_BUS": "event_bus",
           "CAIRN_TRIGGER_INBOX": "trigger_inbox", "CAIRN_JOB_CHANNEL": "job_channel"}[env]
    assert effective_backends({}, env={env: value})[key] == kind


@pytest.mark.parametrize(("value", "shown"), [
    ("postgresql://alice:s3cret@db.invalid:5433/cairn", "postgresql://db.invalid:5433/cairn"),
    ("postgresql://db.invalid/cairn?password=s3cret", "postgresql://db.invalid/cairn"),
    ("nats://alice:s3cret@db.invalid:4222", "nats://db.invalid:4222"),
    ("sqlite:///abs/path.db", "sqlite:///abs/path.db"),
    ("postgres://alice:p@ss/word@db.invalid/cairn", "postgres://***"),  # unescaped '@' and '/'
    ("foo://alice:s3cret@[::1/cairn", "foo://***"),
    ("alice:s3cret@db.invalid:5432/cairn", "***"),
    ("inprocess", "inprocess"),
    ("", ""),
])
def test_redacted_url(value, shown):
    assert settings.redacted_url(value) == shown


@pytest.mark.parametrize(("value", "scheme"), [
    ("PostgreSQL://x", "postgresql"), ("postgres+asyncpg://x", "postgres+asyncpg"),
    ("alice:s3cret@db.invalid", ""), ("sqlite:/x.db", ""), ("://x", ""), ("", ""),
])
def test_url_scheme(value, scheme):
    assert settings.url_scheme(value) == scheme
