# plaid-cache — conventions for agents

This file is read automatically by Claude Code. Other agent tooling (Codex,
etc.) that looks for `AGENTS.md` should read this file too — `AGENTS.md` in
this repo is a symlink to `CLAUDE.md`.

The repo has no Makefile, no linter config, and no vendor directory. There is
nothing to wrap locally; invoke the Go toolchain directly. GitHub workflows run
the same Go gates for pull requests and releases.

## Commands

Run from the repo root. No env vars, no flags, no build tags needed.

```
go build ./...        # builds everything including cmd/plaid-cache
go vet ./...
go test -race ./...   # the CI-equivalent test gate
```

`-race` is not optional here. The daemon serves concurrent clients over a
single index and a shared blob store; a race is the failure mode this codebase
is most likely to ship. Run the race detector before you push, and prefer it
over plain `go test` even when iterating on one package:

```
go test -race ./internal/<package>/...
```

Tests that exercise the daemon bind a unix socket under `t.TempDir()`. They are
safe to run in parallel with each other, but a test that spawns a real daemon
against a shared cache directory is not — Pebble's exclusive lock will reject
the second opener. Scope every test to its own temp directory.

### Lint

`golangci-lint` runs with defaults; there is no project config and no lint step
in CI. Lint the files you touched rather than the whole tree:

```
golangci-lint run ./internal/<package>/...
```

### Format

Format only the files you edited. Do not run a repo-wide `gofmt -w .` as a
drive-by.

## Repo layout

- `cmd/plaid-cache/` — the CLI. With no arguments it is the `GOCACHEPROG`
  plugin; subcommands are `serve`, `status`, `clean`, `gc`, `version`. The
  package path is load-bearing: the release pipeline builds `./cmd/plaid-cache`
  by repository name.
- `internal/ids/` — `ActionID` and `OutputID`, both `[32]byte` raw sha256.
  Defined once here and imported everywhere; hex encoding happens only at the
  wire and filesystem boundaries.
- `internal/wire/` — `GOCACHEPROG` protocol types and framing. Wire-compatible
  with the Go toolchain, so changes here are protocol changes.
- `internal/blob/` — content-addressed local body store. Atomic publish, real
  disk usage accounting.
- `internal/index/` — Pebble-backed index: action lookups, an LRU secondary
  index, output refcounts, byte counters, and eviction. Holds Pebble's
  exclusive directory lock.
- `internal/remote/` — S3 Express One Zone backend plus a no-op implementation
  so the tool is fully usable with the local cache only.
- `internal/daemon/` — the socket server and the auto-spawning client.
- `internal/config/` — environment-variable resolution.

## Module + build conventions

- Module path: `github.com/conductorone/plaid-cache`.
- Go 1.26. Non-vendored module; `-mod=mod` defaults apply.
- **No cgo.** `CGO_ENABLED=0` must build. The release pipeline builds with cgo
  disabled for every target, so a cgo dependency breaks releases, not just
  purity. This rules out cgo-backed storage engines.
- The only build constraints are `GOOS`-style ones on paired files (a `unix`
  implementation next to a portable fallback). There are no custom build tags
  and nothing to pass on the command line.
- No special `GOFLAGS`.

## Code conventions

- **File header on every `.go` file:**
  ```go
  // Copyright 2026 The plaid-cache authors. All rights reserved.
  // SPDX-License-Identifier: Apache-2.0
  ```
- **Godoc comment on every type, func, and const block**, exported or not.
  Comments explain why, not what. A comment that restates the code is noise; a
  comment that records a constraint the code cannot express is the point.
- **Errors:** `fmt.Errorf("FuncName: %w", err)` — function-name prefix,
  lowercase message, no trailing punctuation. Errors that cross the package
  boundary out to the user are prefixed `plaid-cache: `.
- **A miss is not an error at the index layer:** return `(zero, false, nil)`.
  At the blob layer a miss is an error wrapping `fs.ErrNotExist`. Never
  `(nil, nil)`.
- **No option structs.** Configuration is environment variables with a
  documented precedence chain, resolved by small named functions. Unknown
  values fail loud with a `want one of: ...` message. Escape hatches are
  `PLAID_GOCACHE_DISABLE_*=1` and `PLAID_GOCACHE_FORCE_*=1`.
- **Tests use the standard library only.** No testify. Assert with
  `if got != want { t.Fatalf(...) }`, build fixtures under `t.TempDir()`, and
  give every test function a godoc line stating the invariant it pins.
- **The CLI never calls `os.Exit` outside `main`.** The dispatcher is an `app`
  struct carrying `args`, `stdin`, `stdout`, `stderr`; `run() int` returns the
  exit code so tests can drive it with captured buffers. Exit codes are named
  constants.

## Correctness rules that outrank convenience

- **A cache must never break a build.** Every failure path — unreachable
  daemon, unreadable index, remote timeout, full disk — degrades to a miss and
  lets the build proceed. Log it, count it, move on.
- **The index is a rebuildable accelerator, never the system of record.** A
  lost or corrupt index degrades to a rescan of the blob store, never to data
  loss or a wrong byte budget.
- **Every cache hit must hand back a readable file path.** The Go toolchain
  reads the body off disk itself; a hit that does not point at an existing file
  is a hard error on the caller's side.
- **Uploads are best-effort and off the critical path.** They never block or
  fail a build.

## Workflow

- Branches merge fast-forward into `main` after review. Never push to `main`
  directly; never force-push `main`.
- Keep pull requests scoped to one behavior or cleanup theme.

## What this repo is NOT

This repo does not inherit the conventions of the larger monorepos in the same
organization. In particular:

- No Makefile. There is no `make dev`, `make test`, or `make lint` — those
  targets do not exist. Call `go` directly.
- None of the monorepo's build-flag requirements apply: no forced
  `-mod=vendor`, no `-buildvcs=false`, no project-specific build tags, no
  wrapper script that has to set the environment up first. Default toolchain
  settings work.
- No code generation, no protobufs, no frontend, no container orchestration for
  local development.
- No dependency on any private repository, registry, or credential to build and
  test. The only optional external dependency at runtime is an S3-compatible
  bucket you supply yourself.
