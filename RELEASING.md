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

Publishing uses **npm Trusted Publishing (OIDC)** — no `NPM_TOKEN` secret.
For a new package, publish the first version manually after enabling account
2FA (`npm login`, then build, test, and `npm publish --access public` in `ts`).
Once the package exists, open its Settings → Trusted Publisher on npmjs.com,
select GitHub Actions, and configure:

- Organization or user: `opheadacheh`, Repository: `turbodata`
- Workflow filename: `publish-npm.yml`, Environment name: leave empty
- Allow direct publishing with `npm publish` if the option is shown.

The workflow uses Node.js 24 and npm 11 to support OIDC authentication. See
the [npm Trusted Publishing documentation](https://docs.npmjs.com/trusted-publishers/).
For subsequent releases, choose a version that has not already been published:

```bash
# 1. update ts/package.json and ts/package-lock.json (from ts: npm version patch --no-git-tag-version)
# 2. update CHANGELOG.md and commit the release changes, including this workflow
# 3. tag the release commit and push (example after a manual 0.1.0 release)
git tag ts-v0.1.1
git push origin ts-v0.1.1
```

[`.github/workflows/publish-npm.yml`](./.github/workflows/publish-npm.yml) runs
`npm ci`, `npm run build`, `npm test`, and `npm publish --provenance --access public`.

## Checklist

- [ ] Version bumped in the SDK manifest.
- [ ] `CHANGELOG.md` updated.
- [ ] CI green on `main`.
- [ ] Tag pushed using the pattern above.
- [ ] (Python/npm) publish workflow succeeded; package visible on the registry.
