# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The three SDKs (Go, Python, TypeScript) are versioned independently; entries
note the affected SDK(s) where relevant.

## [Unreleased]

_Nothing yet._

## [0.1.0] - 2026-06-02

### Added

- Go reference SDK (read + write) for the `.td` container format.
- Python SDK (read + write).
- TypeScript browser SDK (read-only).
- Cross-language fixture and golden-file test harness verifying that files
  written by the Go reference SDK are read byte-compatibly by every SDK.
