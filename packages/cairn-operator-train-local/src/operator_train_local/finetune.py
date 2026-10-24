"""The ONE implementation of ``train.finetune`` (ADR-0127 D4), shared by the ``train/lora``
operation, the ``train.lora`` ml stage and ``train/eval-loss``.

A plain LoRA fine-tune on CPU: ``transformers`` loads the base model in float32, ``peft``
wraps it with a LoRA adapter (target modules inferred per architecture), and a torch AdamW
loop runs ``steps`` optimizer steps over batches of the training rows. No ``Trainer``, no
``accelerate``, no ``trl``, no ``bitsandbytes``, no CUDA: the point is a real loss curve and a
real adapter on any laptop, in seconds on the ``sshleifer/tiny-gpt2`` stand-in and minutes on
the ``Qwen/Qwen2.5-0.5B`` default.

The training stack is the pack's ``[train]`` extra. Nothing here imports it at module load, so
the pack imports and registers on a base install; :func:`require_train_stack` is the one place
that turns its absence into the error a caller sees. :func:`mock_result` is the deterministic,
torch-free stand-in the operations' ``mock`` and the stage's ``mock: true`` return.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

_log = logging.getLogger(__name__)

INSTALL_HINT = "install cairndev[train] (torch, transformers, peft, datasets)"
#: Rows per optimizer step. Fixed, not a contract knob: a CPU fine-tune of this size is
#: bounded by ``steps`` and ``max_seq_len``, and a batch larger than the dataset wraps.
BATCH_SIZE = 4
#: One row in eight is held out for ``eval_loss`` once there are at least eight rows.
HOLDOUT_EVERY = 8


class TrainStackMissing(RuntimeError):
    """The ``[train]`` extra is not installed; the message names it."""


class TrainingCancelled(Exception):
    """The fine-tune was stopped before it returned a completed adapter."""


def _check_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise TrainingCancelled("fine-tuning cancelled")


def require_train_stack() -> None:
    """Fail loudly, naming the extra, when torch/transformers/peft are absent."""
    # optional dependency: the [train] extra. Lazy so the pack imports and registers
    # (and the manifest gates run) on a base install without the training stack.
    try:
        import peft  # noqa: F401, PLC0415
        import torch  # noqa: F401, PLC0415
        import transformers  # noqa: F401, PLC0415
    except ImportError as e:
        raise TrainStackMissing(f"train-local needs the training stack, and {e.name!r} is not importable: "
                                f"{INSTALL_HINT}") from e


# ── data ─────────────────────────────────────────────────────────────────────

def load_texts(dataset: str, max_samples: int) -> list[str]:
    """The first ``max_samples`` ``text`` values: from a local JSONL file (a path or a
    ``file://`` URI), or else from a dataset ref ``datasets.load_dataset`` opens (the ``train``
    split's ``text`` column). A path-shaped value that names no file is refused, never sent to
    the hub."""
    path = Path(dataset.removeprefix("file://")).expanduser()
    looks_local = dataset.startswith(("file://", ".", "/", "~")) or path.suffix in (".jsonl", ".json")
    texts: list[str] = []
    if path.is_file():
        with path.open(encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                text = row.get("text") if isinstance(row, dict) else None
                if not isinstance(text, str):
                    raise ValueError(f"{path}:{n}: a row must be an object with a string 'text' field")
                texts.append(text)
                if len(texts) >= max_samples:
                    break
    elif looks_local:
        raise FileNotFoundError(f"dataset {dataset!r}: no such file")
    else:
        require_train_stack()
        import datasets  # noqa: PLC0415 - optional dependency: the [train] extra (see require_train_stack)

        rows = datasets.load_dataset(dataset, split="train")
        texts = [str(t) for t in rows.select(range(min(max_samples, len(rows))))["text"]]
    if not texts:
        raise ValueError(f"dataset {dataset!r} has no rows")
    return texts


def split_holdout(texts: list[str]) -> tuple[list[str], list[str]]:
    """``(train, held_out)``: the last ``len // 8`` rows are held out (none below eight rows)."""
    held = len(texts) // HOLDOUT_EVERY
    return (texts[:-held], texts[-held:]) if held else (list(texts), [])


# ── the deterministic stand-in ───────────────────────────────────────────────

def mock_result(*, run: str, dataset: str, base_model: str, lora_rank: int, steps: int, max_samples: int,
                **_: Any) -> dict[str, Any]:
    """What a fine-tune would report, derived from its inputs alone: no torch, no files. The
    adapter path is always ``mock://train-local/<run>``: never a path that looks like a real
    adapter, whatever ``output_dir`` says, so nothing downstream mistakes the stand-in for weights."""
    key = hashlib.sha256(json.dumps([dataset, base_model, lora_rank, steps, max_samples]).encode()).hexdigest()
    train_loss = round(2.0 + int(key[:4], 16) / 65535, 4)
    return {
        "adapter_path": f"mock://train-local/{run}",
        "train_loss": train_loss,
        "eval_loss": round(train_loss + 0.125, 4) if max_samples >= HOLDOUT_EVERY else None,
        "tokens_seen": steps * min(BATCH_SIZE, max_samples) * 16,
        "steps_run": steps,
        "base_model": base_model,
        "elapsed_s": 0.0,
    }


def mock_eval(*, adapter_path: str, dataset: str, max_samples: int, **_: Any) -> dict[str, Any]:
    key = hashlib.sha256(json.dumps([adapter_path, dataset, max_samples]).encode()).hexdigest()
    return {"eval_loss": round(2.0 + int(key[:4], 16) / 65535, 4), "tokens_seen": max_samples * 16}


# ── the real thing ───────────────────────────────────────────────────────────

def _load(base_model: str):
    import torch  # noqa: PLC0415 - optional dependency: the [train] extra (see require_train_stack)
    from transformers import AutoModelForCausalLM, AutoTokenizer  # noqa: PLC0415 - [train] extra

    tokenizer = AutoTokenizer.from_pretrained(base_model)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(base_model, dtype=torch.float32)
    return tokenizer, model


def _batch(tokenizer, texts: list[str], max_seq_len: int) -> dict:
    enc = tokenizer(texts, truncation=True, max_length=max_seq_len, padding=True, return_tensors="pt")
    labels = enc["input_ids"].clone()
    labels[enc["attention_mask"] == 0] = -100
    return {"input_ids": enc["input_ids"], "attention_mask": enc["attention_mask"], "labels": labels}


def _mean_loss(model: Any, tokenizer: Any, texts: list[str], max_seq_len: int,
               cancel_event: threading.Event | None = None) -> tuple[float, int]:
    """Token-weighted mean causal-LM loss over ``texts`` and the non-padding tokens scored."""
    import torch  # noqa: PLC0415 - optional dependency: the [train] extra

    model.eval()
    total, targets, tokens = 0.0, 0, 0
    with torch.no_grad():
        for i in range(0, len(texts), BATCH_SIZE):
            _check_cancelled(cancel_event)
            enc = _batch(tokenizer, texts[i:i + BATCH_SIZE], max_seq_len)
            _check_cancelled(cancel_event)
            n = int((enc["labels"][:, 1:] != -100).sum())  # the shifted positions the loss averages
            if n == 0:
                continue
            batch_loss = model(**enc).loss
            _check_cancelled(cancel_event)
            total += float(batch_loss) * n
            targets += n
            tokens += int(enc["attention_mask"].sum())
    if targets == 0:
        raise ValueError("no row is longer than one token: nothing to score")
    loss = total / targets
    if not math.isfinite(loss):
        raise RuntimeError(f"eval loss is {loss}")
    return loss, tokens


def adapter_digest(adapter_dir: str, files: list[str]) -> tuple[str, int]:
    """``(sha256, bytes)`` over ``files`` (paths relative to ``adapter_dir``, the ones this run
    wrote), in name order. Anything else already in the directory is neither hashed nor counted."""
    h, size = hashlib.sha256(), 0
    for rel in sorted(files):
        data = (Path(adapter_dir) / rel).read_bytes()
        h.update(rel.encode() + b"\0" + data)
        size += len(data)
    return h.hexdigest(), size


def _snapshot(directory: Path) -> dict[str, tuple[int, int]]:
    return {p.relative_to(directory).as_posix(): (p.stat().st_mtime_ns, p.stat().st_size)
            for p in directory.rglob("*") if p.is_file()}


def finetune(*, dataset: str, base_model: str, lora_rank: int, steps: int, max_samples: int,
             learning_rate: float, max_seq_len: int, output_dir: str,
             on_step: Optional[Callable[[int, float], None]] = None,
             cancel_event: threading.Event | None = None,
             written: Optional[list[str]] = None) -> dict[str, Any]:
    """Run the LoRA fine-tune and save the adapter to ``output_dir``; the contract's output.
    ``written``, when given, receives the paths (relative to ``output_dir``) the save wrote."""
    require_train_stack()
    _check_cancelled(cancel_event)
    import torch  # noqa: PLC0415 - optional dependency: the [train] extra
    from peft import LoraConfig, get_peft_model  # noqa: PLC0415 - [train] extra

    started = time.monotonic()
    torch.manual_seed(0)
    train, held = split_holdout(load_texts(dataset, max_samples))
    _check_cancelled(cancel_event)
    tokenizer, model = _load(base_model)
    _check_cancelled(cancel_event)
    model = get_peft_model(model, LoraConfig(r=lora_rank, lora_alpha=2 * lora_rank, lora_dropout=0.0,
                                             task_type="CAUSAL_LM"))
    optimizer = torch.optim.AdamW([p for p in model.parameters() if p.requires_grad], lr=learning_rate)
    model.train()
    losses: list[float] = []
    tokens = 0
    size = min(BATCH_SIZE, len(train))
    for step in range(steps):
        _check_cancelled(cancel_event)
        enc = _batch(tokenizer, [train[(step * size + i) % len(train)] for i in range(size)], max_seq_len)
        _check_cancelled(cancel_event)
        loss = model(**enc).loss
        _check_cancelled(cancel_event)
        value = float(loss.detach())
        if not math.isfinite(value):
            raise RuntimeError(f"train loss is {value} at step {step}; refusing to save the adapter")
        loss.backward()
        _check_cancelled(cancel_event)
        optimizer.step()
        optimizer.zero_grad()
        _check_cancelled(cancel_event)
        losses.append(value)
        tokens += int(enc["attention_mask"].sum())
        if on_step is not None:
            on_step(step, value)
    _check_cancelled(cancel_event)
    eval_loss = _mean_loss(model, tokenizer, held, max_seq_len, cancel_event)[0] if held else None
    _check_cancelled(cancel_event)
    out = Path(output_dir)
    _check_cancelled(cancel_event)
    out.mkdir(parents=True, exist_ok=True)
    before = _snapshot(out)
    model.save_pretrained(output_dir)
    _check_cancelled(cancel_event)
    if written is not None:
        written.extend(sorted(rel for rel, stat in _snapshot(out).items() if before.get(rel) != stat))
    _log.info("train-local: %d steps on %s, adapter at %s", steps, base_model, output_dir)
    return {
        "adapter_path": str(output_dir),
        "train_loss": sum(losses) / len(losses),
        "eval_loss": eval_loss,
        "tokens_seen": tokens,
        "steps_run": len(losses),
        "base_model": base_model,
        "elapsed_s": round(time.monotonic() - started, 3),
    }


def evaluate(*, adapter_path: str, base_model: str, dataset: str, max_samples: int,
             max_seq_len: int) -> dict[str, Any]:
    """Load ``base_model`` with the adapter at ``adapter_path`` and score the first
    ``max_samples`` rows of ``dataset``."""
    require_train_stack()
    from peft import PeftModel  # noqa: PLC0415 - optional dependency: the [train] extra

    if not Path(adapter_path).is_dir():
        raise FileNotFoundError(f"adapter {adapter_path!r}: no such directory")
    texts = load_texts(dataset, max_samples)
    tokenizer, model = _load(base_model)
    model = PeftModel.from_pretrained(model, adapter_path)
    loss, tokens = _mean_loss(model, tokenizer, texts, max_seq_len)
    return {"eval_loss": loss, "tokens_seen": tokens}
