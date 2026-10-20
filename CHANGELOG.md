# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Failed and retried flow-leg attempts now retain known partial spend in run cost accounting and subsequent budget reservations.

## [0.1.3]

### Security
- The server's access log no longer records a bearer token sent in the query string: on the run event stream, which accepts `?access_token=`, the value is logged as `***`.
- Webhook receivers live under `/triggers/`. A receiver whose path collides with an app route is refused, a receiver is public only for its own method and only with a non-empty signing secret, and its runs land in the workspace that declared it (#141).

### Fixed
- With no model configured, `cairn dev`, `cairn run --mock` and `cairn gate --mock` sent agentic model calls to the vendor with a placeholder key and failed with a 401. No placeholder key leaves the process now: the call stops with "no model configured: set OPENAI_API_KEY, or OPENAI_BASE_URL …". A keyless OpenAI-compatible endpoint (`OPENAI_BASE_URL`) still gets the call.
- `cairn remote pipelines` shows each pipeline's name and step progress (it printed `0/0 steps` and no name).
- The engine image starts with a bare `docker run`: without `CAIRN_PROFILE` it runs the packaged `secure` profile. The bind address still follows sign-in (0.0.0.0 with auth on, 127.0.0.1 with auth off), and the Dockerfile header and the install page show runs that answer. CI starts the built image on every pull request.
- The Helm example values pin image tags that exist (the bare version), the released chart's `appVersion` is that tag, and the docs, chart README and install notes no longer say the GHCR packages are private (#140).
- `cairn-admin`, `cairn remote console` and `cairn test` no longer point at extras that do not exist: they name the packages they need and the command that adds them to cairn's own environment.
- `cairn pack sync` and `cairn pack add` render their colours instead of printing Rich markup.
- `cairn compute sweep` without an AWS region says to pass `--region` or set `AWS_DEFAULT_REGION`, instead of a traceback.
- `cairn pipeline show` no longer says flow legs cannot run yet.
- `cairn dev` no longer promises a console at `/console` (the web console is not part of the public core), and `cairn demo`/`cairn dev --seed` say how to read the `demo` workspace.
- `cairn init operator` scaffolds a smoke test per operator, so `cairn test <pack>` has something to run.
- `cairn remote approve`/`reject` (`POST /runs/{id}/resume`) on a pipeline step gate answers 409 with the `cairn remote advance` command, not 502, on the in-process runtime; under DBOS and Temporal the step-gate resume is unchanged.
- When a capability has no binding in the active profile, `cairn validate` names the active profile and the profiles the pack ships (hello-cairn validates under `profiles/hello.toml`).

### Changed
- CONTRIBUTING matches the public tree: the sync line includes `runtime-dbos` (the DBOS runtime tests need it), the console, npm, pre-commit and status-audit steps are gone, `cairn dev --engine-only` with a CLI login is the dev loop, and the CI gates and branch protection are described as they are.
- The quickstart's `pip` alternative installs from the GitHub Release (`--find-links`) until the packages are on PyPI.
- Feature proof: the run hook (`hooks`) is proved on `profile-and-publish`, which every cell runs, and the `public-e2e` job installs the `hello` extra that declares it. `secrets.infisical` and `worker.pull` stay NOT PROVED, each with the reason.

## [0.1.2]

### Security
- A bearer token in the URL (`?access_token=`) is accepted only on the run event stream; anywhere else it is refused (401).
- A per-run worker token reaches only its own run's reporting routes (events, control, heartbeat); anything else answers 403.
- A webhook receiver answers without a cairn token only when its signature is required; an optional-signature receiver stays behind sign-in.
- The OAuth callback is reachable without a token (the provider's redirect carries none) and escapes everything it shows; the saga approval decides on `POST /triggers/approve/{id}` only.
- The workspace check covers the credential the auth guard accepted, so a token for one workspace cannot read another workspace's run stream.

### Fixed
- The quickstart's `cairn remote login` failed on a plain install with "password hashing needs the 'auth' extra": `cairn dev` turns on password sign-in, so `pwdlib[argon2]` is now a base dependency (the `auth` extra still works and installs nothing more).
- `cairn dev` and `cairn remote` start the engine with `python -P`, so a `cairn/` folder in the working directory (a checkout) is never imported in place of the installed engine.

## [0.1.1]

### Added
- 0.1.1, the first complete public release: the engine `cairndev`, `cairn-client`, the optional Metaflow and SkyPilot dataflow engines (`cairn-operator-dataflow-metaflow`, `cairn-operator-dataflow-skypilot`) and the example pack `cairn-pack-hello`.

### Changed
- The builtin operator packs and the core contracts ship inside the `cairndev` wheel (ADR-0137): they are no longer distributions of their own, and their extras are `cairndev` extras (`ssh`, `train`, `infisical`, `dataflow-engine`, `rerank-cross-encoder`, `rerank-cohere`).
