---
title: LLM judges are advisory
description: "What a judge score is worth in cairn: how a judge is measured against human ratings, and why a judge score never promotes a model on its own."
---

**Can an LLM judge promote my model? No.** In cairn a judge score is an
opinion. It can sit beside evidence, and it is never the evidence that
releases a promotion.

Two things stand behind that answer, and one thing this page does not claim.

## What decides a promotion

The operator that moves a governed tag, `artifact/promote`, declares
`requires_approval`. The approval check runs before the operator does, so the
tag does not move until an approval is recorded. A person records it with
`cairn decide <pipeline_id> approve --step <gate>`. The
[gates page](/concepts/gates/) covers the mechanics.

A judge has no step in that path. Nothing in the pipeline machinery reads a
judge score to release a gate. A data gate is decided by the predicate its
author wrote, over the outputs of earlier legs, so a judge's verdict can only
matter to a gate if someone writes it into that predicate. For anything a
number can decide, prefer a deterministic number: a held-out loss, a benchmark
bar, a baseline delta.

## How a judge is measured

A judge is a named, versioned rubric: model, prompt, labels, temperature. A
version is immutable; editing the prompt makes a new version with a new hash, so
an old measurement is never read as evidence about a new rubric.

To find out whether a judge tracks a person, cairn compares its verdicts with
human ratings on a **golden** dataset: a version registered with the role
`golden`, which is refused unless a person has rated every row. Three numbers
come out, always shown together:

| Number | What it tells you |
|---|---|
| `tau` | Kendall's tau-b: does the judge order rows the way a human does. Handles the ties a three-label judge produces. |
| `mae` | Mean absolute error on the same ordinal scale: how far off it is when it is off. |
| `n` | How many rows both sides ruled on. A high tau on ten rows is not evidence. |

Labels map onto an ordinal scale (`correct` 1.0, `partial` 0.5, `incorrect`
0.0, and the synonyms listed in `LABEL_SCALE`). A verdict that is an error, or a
label cairn does not know, is dropped and counted as skipped, never scored as a
disagreement.

## What the measurement buys

A judge is **untrusted** until measured. With no golden evaluation, or with
tau below 0.5 or fewer than 30 rows, its scores are marked `untrusted_metric`
and do not count as evidence. A workspace can change the bar with the
`judge_trust` workspace variable (`tau_min`, `n_min`); a malformed value falls
back to the defaults, never to no bar.

A judge that clears the bar is **trusted**, and that only means its score may
contribute. The rule in `assess_promotion_evidence` has no third case:

- judge evidence alone, trusted or not, is refused;
- judge evidence plus a deterministic metric or a human review is allowed;
- an untrusted score beside a metric is allowed, but only the metric counts;
- a promotion with no judge score in it is unaffected.

The function can refuse a promotion. It cannot release one.

Agreement records go stale when the judge gets a new version or the golden set
gets a new version. A stale record is flagged, not discarded, so a judge is not
demoted before anyone can re-measure it.

## What a judge costs while it runs

Judge definitions are data. A judge-backed eval (an operator that declares the
`eval.judge` capability) is sampled by the automatic eval of finished runs:
20% of runs by default, chosen by a hash of the run id so the choice is
repeatable, and set with `CAIRN_AUTOEVAL_JUDGE_SAMPLE_PCT`. A run that already
carries a failure label is always scored. Deterministic gates always run.
`CAIRN_AUTOEVAL=off` turns the whole step off. A judge outage is logged and
never fails the run.

## What this repository does not do

- It ships the trust rule as a function and tests it
  (`tests/core/test_judge_agreement.py`), but no operator or route in this
  repository calls `assess_promotion_evidence`. Whether a given deployment's
  promotion path calls it depends on the packs installed there. The guarantee
  you can rely on from this repository alone is the approval on
  `artifact/promote`.
- It ships no judge operator and no command to measure a judge. Running a
  judge over a golden set happens in whichever pack provides the `eval.judge`
  operator.
- It does not publish an agreement number. None has been measured here.

## Next

- [Gates and approvals](/concepts/gates/): data gates, person gates and operator approvals.
- [Runs and the record](/concepts/runs/): where every decision is kept.
