# Contributing

Thanks for improving `plaid-cache`.

## Local Workflow

Run the core gates from the repository root:

```sh
go build ./...
go vet ./...
go test -race ./...
```

There is no Makefile, no vendored dependency tree, and no repository-level `golangci-lint` configuration.

The race detector is part of the normal gate rather than an occasional extra pass. The daemon serves concurrent clients over a single index and a shared blob store, so races are the defect class this project is most exposed to.

The project builds without cgo, and the release pipeline builds every target with `CGO_ENABLED=0`. Verify that a change keeps that true:

```sh
CGO_ENABLED=0 go build ./...
```

Format only files you change:

```sh
gofmt -w <files>
```

## Pull Requests

Keep pull requests scoped to one behavior or cleanup theme. Include tests when changing protocol framing, index or eviction semantics, blob publication, daemon lifecycle, or configuration resolution.

Tests use the standard library `testing` package only, build their fixtures under `t.TempDir()`, and give each test function a comment stating the invariant it pins.

Two properties are worth calling out explicitly in a pull request that touches them:

- **Failure handling.** A cache must never break a build. If your change adds a failure path, say how it degrades to a miss.
- **Shared-cache trust boundary.** Cache entries can be integrity-checked by digest, but shared writable storage does not authenticate who produced an entry. Any change to how remote entries are fetched, trusted, or namespaced should document its effect on that boundary.
