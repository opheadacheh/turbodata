# Security Policy

## Supported versions

turbodata is pre-1.0. Security fixes are applied to the latest released
version of each SDK only.

| SDK | Version | Supported |
| --- | --- | --- |
| Go (`go/turbodata`) | 0.1.x | yes |
| Python (`turbodata`) | 0.1.x | yes |
| TypeScript (`turbodata`) | 0.1.x | yes |

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for
an undisclosed vulnerability.

Use GitHub's private vulnerability reporting:

1. Go to https://github.com/opheadacheh/turbodata/security/advisories/new
2. Describe the issue, the affected SDK(s) and version(s), and a reproduction
   if you have one.

We aim to acknowledge a report within **7 days** and to provide a remediation
plan or fix timeline within **30 days**. Once a fix is released we will publish
a security advisory crediting the reporter (unless you prefer to remain
anonymous).

## Scope

This project parses and produces binary `.td` container files. Reports about
malformed-input handling (e.g. a crafted file causing a crash, unbounded
memory growth, or out-of-bounds access in any SDK's reader) are in scope and
especially welcome.
