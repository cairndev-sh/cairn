"""``cairn.core.dataflow.fidelity`` — the deterministic, recorded subsample (#2029).

What a pack relies on: the kept set depends on the corpus, the seed and ``n`` only
(never on row order or key order), the digest proves which rows were kept, and an
unset fidelity passes the corpus through untouched.
"""
from __future__ import annotations

import json
import random

from cairn.core.dataflow.fidelity import Fidelity, digest_rows, plan, row_hash

ROWS = [{"input": f"q{i}", "output": f"a{i}"} for i in range(50)]


def test_the_kept_set_does_not_depend_on_row_order():
    shuffled = ROWS[:]
    random.Random(1).shuffle(shuffled)
    a, b = plan(ROWS, Fidelity(rows=5, seed=7)), plan(shuffled, Fidelity(rows=5, seed=7))
    assert a.sampled and a.n == 5 and a.total == 50
    assert a.digest == b.digest
    assert sorted(map(row_hash, a.kept)) == sorted(map(row_hash, b.kept))


def test_a_different_seed_keeps_different_rows():
    assert plan(ROWS, Fidelity(rows=5, seed=7)).digest != plan(ROWS, Fidelity(rows=5, seed=8)).digest


def test_key_order_and_serialisation_do_not_change_a_row():
    row = {"input": "q", "output": "a"}
    assert row_hash(row) == row_hash(json.dumps({"output": "a", "input": "q"})) \
        == row_hash(b' {"input":"q","output":"a"} ')


def test_an_unset_fidelity_passes_through_with_the_whole_corpus_digest():
    out = plan(ROWS, Fidelity())
    assert not out.sampled and out.kept == ROWS and out.n == out.total == 50
    assert out.digest == digest_rows(ROWS)


def test_rows_wins_over_fraction_and_a_fraction_keeps_at_least_one_row():
    assert Fidelity(rows=3, fraction=0.5).n_for(50) == 3
    assert Fidelity(fraction=0.001).n_for(20) == 1
    assert Fidelity.from_config({"fidelity_fraction": "0.1", "fidelity_seed": ""}).seed == 13
