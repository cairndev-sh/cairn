# ADR-0150 — The public SSH path runs in the box's virtualenv by default, and the public tree names no private registry

- **Status:** ACCEPTED 2026-10-24 by the owner (decision 4a: an explicitly named image).
- **Date:** 2026-10-24
- **Issue:** #202
- **Amends:** ADR-0104 D1 (the SSH path runs the CI-built image instead of a virtualenv) and
  ADR-0133 D3 (image names and the registry keep the private organisation's namespace).
- **Keeps:** ADR-0104 D1's image mode (a box that names an image runs it via `docker run`, and
  the run records the image), `remote_exec_mode` and its `auto` / `image` / `venv` values, and
  a `compute.ssh` connection's own `mode` and `image` overriding the environment per box.
  ADR-0133 D1 and D2 (private issue links become plain text; the leak scan) are unchanged.

## Context

The builtin engine's SSH dispatch fell back to a built-in image,
`operator_dataflow.engines_ssh.DEFAULT_TRAIN_IMAGE`, whenever image mode was selected and
nothing named an image. That image lives under a registry namespace a public user cannot pull,
so on a public install image mode without an explicit image failed on the box at
`docker pull`, after the leg had been admitted, its budget reserved and the job submitted.

The public tree also named that namespace in profile field descriptions and in test fixtures,
which ADR-0133 D3 allowed on the grounds that the images were published there. A public user
reading those names has nothing to pull.

## Decision

**D1 — On the public SSH path venv is the default; image mode needs an image named
explicitly.** There is no built-in default image. Image mode runs only an image named by the
`compute.ssh` connection's `image` or by the environment's `remote_image` backend
(`CAIRN_TRAIN_IMAGE`). When a leg's mode resolves to image and no image is named, the engine
refuses the leg at submit, where the connection and the environment are both known, before
anything reaches the box; the message names the settings to change. `auto`, the default, runs
the box's virtualenv when no image is named. This amends ADR-0104 D1, which made the CI-built
image the SSH path's unit of work in place of a virtualenv.

**D2 — The public tree names no private registry namespace.** Code, docs, Helm, compose and
tests in the public tree name no private registry namespace; test fixtures use
`ghcr.io/example/...`. A public GPU image is a later decision with its own ADR. This amends
ADR-0133 D3, which kept the private organisation's namespace in image names and the
registry. Locked ADRs keep their text as written.

## Consequences

- A deployment that relied on the built-in default must name an image (`remote_image` /
  `CAIRN_TRAIN_IMAGE`, or the connection's `image`) or run in venv mode. Without one, image mode
  is refused at submit with a message saying what to set.
- `operator_dataflow.engines_ssh.DEFAULT_TRAIN_IMAGE` is removed.
- `cairn doctor` reports each compute target's execution unit with the engine's precedence,
  reading the environment's `remote_exec_mode` and `remote_image` as well as the connection:
  venv is OK and suggests no image; a pinned image is OK; a moving or missing tag warns; image
  mode with no image anywhere FAILS, naming the connection's or the environment's remedy. When
  the environment cannot be read, a row whose answer depends on it is a warn that says so. Core
  cannot import the engine, so the doctor's precedence is a twin of the engine's, held to it by
  `tests/core/test_exec_unit_parity.py`.
- The run record names the image the deployment chose, never one the code chose.
