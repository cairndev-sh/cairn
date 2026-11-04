# cairn-contracts-train

Shared contract definitions for the `train` capability domain — data only (ADR-0111, ADR-0127 D4).

| Definition | Kind | Owner of the meaning |
|---|---|---|
| `contract://train.finetune/1.0.0` | capability | this distribution |
| `contract://train.finetune.input/1.0.0` | schema | this distribution |
| `contract://train.finetune.configuration/1.0.0` | schema (closed: `base_model`) | this distribution |
| `contract://train.finetune.output/1.0.0` | schema | this distribution |
| `contract://train.eval_loss/1.0.0` | capability | this distribution |
| `contract://train.eval_loss.input/1.0.0` | schema | this distribution |
| `contract://train.eval_loss.configuration/1.0.0` | schema (closed, empty in v1) | this distribution |
| `contract://train.eval_loss.output/1.0.0` | schema | this distribution |
| `contract://train.finetune.report/1.0.0` | report | this distribution |
| `contract://train.eval_loss.report/1.0.0` | report | this distribution |
| `contract://train.finetune/1.1.0` | capability (input 1.1.0; the rest 1.0.0) | this distribution |
| `contract://train.finetune.input/1.1.0` | schema (1.0.0 plus `device`, `precision`) | this distribution |
| `contract://train.eval_loss/1.1.0` | capability (input 1.1.0; the rest 1.0.0) | this distribution |
| `contract://train.eval_loss.input/1.1.0` | schema (1.0.0 plus `device`, `precision`) | this distribution |

Implemented at 1.1.0 by `train/lora` and `train/eval-loss` (`cairn-operator-train-local`, a
builtin; ADR-0167); each
report definition equals its operator's `reports`. The private training
pack may implement the same revision. A new revision is a new file and a new `[[definitions]]`
row; an existing revision's bytes never change (its digest is recorded in every pipeline
admitted against it).
