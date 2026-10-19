# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- 0.1.1, the first complete public release: the engine `cairndev`, `cairn-client`, the optional Metaflow and SkyPilot dataflow engines (`cairn-operator-dataflow-metaflow`, `cairn-operator-dataflow-skypilot`) and the example pack `cairn-pack-hello`.

### Changed
- The builtin operator packs and the core contracts ship inside the `cairndev` wheel (ADR-0137): they are no longer distributions of their own, and their extras are `cairndev` extras (`ssh`, `train`, `infisical`, `dataflow-engine`, `rerank-cross-encoder`, `rerank-cohere`).
