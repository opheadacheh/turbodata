# Contributing to turbodata

Thanks for your interest in contributing! turbodata is a cross-language
container format with three SDKs that must stay byte-compatible: **Go** (the
reference implementation), **Python**, and **TypeScript**. This guide explains
how to set each one up, run its tests, and exercise the cross-language
compatibility checks that are the strongest correctness signal in this repo.

## Repository layout

```
go/
  turbodata/    Go SDK — the reference implementation (read + write)
  examples/     runnable Go examples (write, read, sample, multiread, mcap→td)
  benchmark/    performance benchmarks
  cmd/          maintenance tooling (e.g. gen-fixtures)
py/             Python SDK (read + write) + examples + tests
ts/             TypeScript SDK (browser, read-only) + tests
```

The Go workspace (`go/go.work`) unions four modules. Library consumers only
pull in `go/turbodata`; the example/benchmark/tooling modules depend on heavier
packages and live separately.

## Prerequisites

- **Go** 1.26.1+ (matches `go/turbodata/go.mod`)
- **Python** 3.9+
- **Node.js** 20+

## Per-SDK setup and tests

### Go

The real test suite lives in `go/turbodata`:

```bash
cd go/turbodata
go vet ./...
go test ./...
```

The other modules are compile-checked:

```bash
cd go/cmd      && go vet ./... && go build -o /dev/null ./...
cd go/examples && go vet ./... && go build -o /dev/null ./...
```

> Note: in `go/cmd`, use `go build -o /dev/null ./...` rather than
> `go build ./...` — the latter tries to emit a binary named `gen-fixtures`
> into the directory that already holds the `gen-fixtures` package.

### Python

```bash
cd py
python -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
pytest -q
```

### TypeScript

```bash
cd ts
npm ci          # package-lock.json is committed
npm test        # vitest run
npm run build   # tsc -p tsconfig.build.json
npm run typecheck
```

## Cross-language fixture tests

A file written by any SDK must be readable, byte-for-byte, by every other SDK.
Two mechanisms enforce this:

1. **Go → TypeScript fixtures.** `go/cmd/gen-fixtures` writes a set of `.td`
   files plus a `.golden.txt` per fixture into `ts/test/fixtures/`. The golden
   file records a sha256 over the *decoded* message stream. The TS suite
   (`reader.test.ts`, `video.test.ts`, …) reads each committed `.td`, recomputes
   that stream hash, and asserts it matches the golden. The committed fixtures
   are checked in, so `npm test` alone is already a cross-language assertion.

2. **Go → Python.** `py/tests/test_cross_language.py` reads
   `go/examples/demo.td` (produced by the Go `write` example) and every
   `ts/test/fixtures/*.td`. The Go-demo test is `skipif` the file is absent, so
   to actually exercise the Go→Python read path you must generate the demo file
   first.

### Regenerating fixtures

```bash
cd go
go run ./examples/write                       # writes go/examples/demo.td
go run ./cmd/gen-fixtures -out ../ts/test/fixtures
```

Then run the Python and TS suites:

```bash
cd py && pytest -q
cd ts && npm test
```

> **Important — `.td` binaries are not byte-reproducible.** Compressed fixtures
> use zstd, whose output is not guaranteed identical across library versions or
> even runs. Do **not** assert on `git diff` of the `.td` files. The decoded
> stream is deterministic, so the committed `.golden.txt` files *are* stable —
> CI regenerates fixtures and diffs only the `*.golden.txt` files to confirm the
> Go writer still produces the expected decoded output.

After regenerating locally, restore the working tree so you don't accidentally
commit nondeterministic binary churn or the generated demo file:

```bash
git checkout ts/test/fixtures   # discard nondeterministic .td churn
rm -f go/examples/demo.td        # not tracked; do not commit
```

Only commit fixture changes when the *golden* output actually changed (i.e. you
intentionally changed the on-disk format).

## Coding conventions

- **License header.** Every new source file starts with the Apache 2.0 header,
  using the language's comment syntax. Copy an existing file's header verbatim,
  e.g. for Go/TypeScript:

  ```go
  // Copyright 2026 Wanjia He
  //
  // Licensed under the Apache License, Version 2.0 (the "License");
  // you may not use this file except in compliance with the License.
  // You may obtain a copy of the License at
  //
  //     http://www.apache.org/licenses/LICENSE-2.0
  //
  // Unless required by applicable law or agreed to in writing, software
  // distributed under the License is distributed on an "AS IS" BASIS,
  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  // See the License for the specific language governing permissions and
  // limitations under the License.
  ```

  Python uses `#` comments with the same text.

- **Match the surrounding style.** Run `gofmt`/`go vet` for Go, keep Python
  consistent with the existing modules, and let `tsc`/vitest guard the TS code.
- **Keep changes surgical.** Don't reformat or refactor unrelated code.
- **Format parity matters.** Any change to the on-disk encoding must be made in
  all three SDKs and reflected in regenerated fixtures + goldens.

## Commit and PR flow

1. Fork and branch from `main`.
2. Make your change; add or update tests. For format-affecting changes, update
   all three SDKs and regenerate fixtures.
3. Run the relevant test suites (and the cross-language checks if you touched
   encoding/decoding).
4. Update `CHANGELOG.md` under `## [Unreleased]`.
5. Open a PR against `main` and fill out the PR template.

There is **no DCO / sign-off requirement** — a normal `git commit` is fine.

## Releasing

Maintainers: see [`RELEASING.md`](./RELEASING.md) for the per-language tag
scheme and the publish workflows.
