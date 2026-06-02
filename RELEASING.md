# Releasing

turbodata is a monorepo with three independently versioned SDKs. Each is
released by pushing a language-prefixed tag. Bump the version in the SDK's
manifest first, then tag.

| SDK | Version file | Tag pattern | Publishes to |
| --- | --- | --- | --- |
| Go | `go/turbodata/go.mod` (module path) | `go/turbodata/vX.Y.Z` | proxy.golang.org / pkg.go.dev |
| Python | `py/pyproject.toml` (`version`) | `py-vX.Y.Z` | PyPI |
| TypeScript | `ts/package.json` (`version`) | `ts-vX.Y.Z` | npm |

## Go

Go modules in a subdirectory must be tagged with the **full module path as the
prefix**. The module path is `github.com/opheadacheh/turbodata/go/turbodata`, so:

```bash
git tag go/turbodata/v0.1.0
git push origin go/turbodata/v0.1.0
```

No publish step is required — `go get` and pkg.go.dev resolve the tag from the
repository. Verify with:

```bash
go list -m github.com/opheadacheh/turbodata/go/turbodata@v0.1.0
```

## Python (PyPI)

Publishing uses **PyPI Trusted Publishing (OIDC)** — no API token. One-time
setup on PyPI (project → Publishing → add a GitHub publisher):

- Owner: `opheadacheh`, Repository: `turbodata`
- Workflow: `publish-python.yml`, Environment: `pypi`

Then for each release:

```bash
# 1. bump version in py/pyproject.toml
# 2. tag and push
git tag py-v0.1.0
git push origin py-v0.1.0
```

[`.github/workflows/publish-python.yml`](./.github/workflows/publish-python.yml)
builds the sdist+wheel and publishes them.

## TypeScript (npm)

One-time setup: create an npm **automation** access token and add it as the
repository secret `NPM_TOKEN`.

```bash
# 1. bump version in ts/package.json
# 2. tag and push
git tag ts-v0.1.0
git push origin ts-v0.1.0
```

[`.github/workflows/publish-npm.yml`](./.github/workflows/publish-npm.yml) runs
`npm ci`, `npm run build`, and `npm publish --provenance --access public`.

## Checklist

- [ ] Version bumped in the SDK manifest.
- [ ] `CHANGELOG.md` updated.
- [ ] CI green on `main`.
- [ ] Tag pushed using the pattern above.
- [ ] (Python/npm) publish workflow succeeded; package visible on the registry.
