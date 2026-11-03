# holdout example

See the holdout door refuse training data that overlaps an eval set. No API key, no spend.
It needs `cairndev` 0.5.1 or later, and a checkout of this repository for its files.

```bash
export CAIRN_PACK_PATH=$PWD/examples/holdout
cairn setup --port 8472

cairn run holdout-example/register --environment sandbox -i source=$PWD/examples/holdout/data/eval.jsonl -i name=eval-set -i role=test
cairn run holdout-example/register --environment sandbox -i source=$PWD/examples/holdout/data/train-leaky.jsonl -i name=train-leaky
cairn run holdout-example/register --environment sandbox -i source=$PWD/examples/holdout/data/train-clean.jsonl -i name=train-clean

cairn run holdout-example/train --environment sandbox -i train_version=dataset:train-leaky@latest
```

The last command is refused before the pipeline has a record, because `train-leaky.jsonl` holds a row of `eval.jsonl`. The refusal names the versions and the row.

With the clean version, the same pipeline starts and waits at its gate:

```bash
cairn run holdout-example/train --environment sandbox -i train_version=dataset:train-clean@latest
```

The overlapping row is dated `2026-06-02`, so a cutoff before that date leaves it out of the comparison, and the leaky version is admitted:

```bash
cairn run holdout-example/train --environment sandbox -i train_version=dataset:train-leaky@latest -i eval_cutoff=2026-06-01
```

A cutoff the door can't read is refused, whatever the data: try `-i eval_cutoff="last tuesday"`.

`cairn setup --stop` stops the engine. [Holdout and eval cutoff](https://docs.cairndev.sh/concepts/holdout/) explains what the door checks.
