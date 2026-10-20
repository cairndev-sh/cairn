# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
