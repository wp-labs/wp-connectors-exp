# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-05-04

### Changed
- Bump `wp-connector-api` from `0.9` to `0.10`.
- Bump `wp-log` from `0.3` to `0.4`.
- Bump `wp-conf-base` from `0.3` to `0.4`.
- Bump `orion-error` from `0.7` to `0.8`.
- Bump `orion_conf` from `0.6` to `0.7`.
- Bump `wp-connector-test-utils` from `0.1.1` to `0.2.0`.
- Adapt DMDB connector error construction to unit `SinkReason`/`SourceReason` variants with details carried by `StructError`.
- Replace DMDB runtime `anyhow::Result`/`Result<T, String>` usage with `DmdbReason`/`DmdbResult`.
- Preserve DMDB structured errors as sources when converting to `SourceError`/`SinkError` at connector API boundaries.

### Removed
- Remove unused direct `wp-error` dependency to avoid retaining the older connector/error dependency stack.

[Unreleased]: https://github.com/wp-labs/wp-connectors-labs/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/wp-labs/wp-connectors-labs/releases/tag/v0.1.3
