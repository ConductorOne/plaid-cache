# plaid-cache

[![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/plaid-cache.svg)](https://pkg.go.dev/github.com/conductorone/plaid-cache)

`plaid-cache` is a `GOCACHEPROG`-compatible build cache for the Go toolchain, backed by a content-addressed local body store with a Pebble-backed index and an optional S3 Express One Zone remote tier shared across machines.

The index gives the local cache bounded growth — entries are pruned oldest-first by last use until both a TTL and a total-size ceiling hold — so a long-lived workspace cache stops growing without a periodic manual wipe. The remote tier lets a cold machine or a fresh CI runner start warm.

## Install

Install from source:

```sh
go install github.com/conductorone/plaid-cache/cmd/plaid-cache@latest
```

Build locally:

```sh
go build -o ./plaid-cache ./cmd/plaid-cache
```

Run with Docker after an image has been published:

```sh
docker run --rm ghcr.io/conductorone/plaid-cache:latest status
```

## Usage

Point the Go toolchain at the binary and build as usual:

```sh
export GOCACHEPROG=plaid-cache
go build ./...
```

With no arguments `plaid-cache` acts as the cache plugin on stdin/stdout. It is a thin client: it connects to a daemon over a unix socket in the cache directory, spawning one if none is running. Pebble takes an exclusive lock on the index directory, so exactly one process owns the index no matter how many builds run concurrently.

Inspect and manage the cache:

```sh
plaid-cache status
plaid-cache gc
plaid-cache clean
```

Run the daemon in the foreground, for a container or a supervised service:

```sh
plaid-cache serve
```

Subcommands are `serve`, `status`, `clean`, `gc`, and `version`.

## Configuration

Configuration is environment variables only. There are no defaults for the bucket, region, prefix, or endpoint: leaving `PLAID_GOCACHE_S3_BUCKET` empty runs the cache entirely locally.

| Variable | Purpose | Default |
| --- | --- | --- |
| `PLAID_GOCACHE_DIR` | Local cache root. | `$XDG_CACHE_HOME/plaid-cache`, else `os.UserCacheDir()/plaid-cache` |
| `PLAID_GOCACHE_MAX_BYTES` | Local size ceiling. Accepts `50GB`, `1TiB`. | `20GB` |
| `PLAID_GOCACHE_TTL` | Local entry TTL, as a Go duration. | `168h` |
| `PLAID_GOCACHE_S3_BUCKET` | Remote bucket. Empty means local only. | empty |
| `PLAID_GOCACHE_S3_REGION` | Remote region. | from the AWS config chain |
| `PLAID_GOCACHE_S3_PREFIX` | Key prefix within the bucket. | empty |
| `PLAID_GOCACHE_MIN_UPLOAD_SIZE` | Skip uploading bodies smaller than this. Skipping also omits the action record, so the entry becomes a remote miss and the action is re-run rather than re-downloaded. | `0` (upload everything) |
| `PLAID_GOCACHE_UPLOAD_CONCURRENCY` | Remote upload workers. | `NumCPU` |
| `PLAID_GOCACHE_TOUCH_GRANULARITY` | Relatime-style window for last-used updates. | `1h` |
| `PLAID_GOCACHE_IDLE_TIMEOUT` | Daemon exits after this long with no connections. | `30m` |
| `PLAID_GOCACHE_EVICT_INTERVAL` | Eviction ticker period. | `1m` |
| `PLAID_GOCACHE_DISABLE_EVICTION` | `1` disables eviction entirely. | unset |
| `PLAID_GOCACHE_DISABLE_DAEMON` | `1` forces direct in-process mode. | unset |
| `PLAID_GOCACHE_LOG` | Verbosity: `off`, `error`, `info`, `debug`. | `error` |

Unrecognized values fail loudly rather than falling back to a default.

### Remote tier

The remote tier is an S3 Express One Zone directory bucket. Directory bucket names end in `--x-s3` and encode their availability zone, so a bucket name looks like:

```sh
export PLAID_GOCACHE_S3_BUCKET=example-bucket--usw2-az1--x-s3
export PLAID_GOCACHE_S3_REGION=us-west-2
export PLAID_GOCACHE_S3_PREFIX=go-build
```

Credentials come from the standard AWS configuration chain. The caller needs `s3express:CreateSession` on the bucket in addition to the usual object permissions; the SDK negotiates the session automatically.

Uploads are best-effort. They run in a bounded worker pool off the critical path, and a failure is logged and counted rather than propagated — a cache must never fail a build. If the daemon cannot be reached at all, the plugin falls back to direct mode, warns on stderr, and lets the build proceed.

Keys are laid out as `[<prefix>/]action/<xx>/<hex-action-id>` and `[<prefix>/]output/<xx>/<hex-output-id>`, mirroring the de-facto convention so a bucket stays interoperable with other `GOCACHEPROG` implementations.

## Shared Cache Trust Model

A shared build cache is a performance layer, not a security boundary. A cache entry is a promise that some compilation of some inputs produced these bytes, and anything that can write to the shared bucket can make that promise. Consuming an entry means executing or linking bytes you did not build.

`plaid-cache` addresses bodies by content and verifies a fetched body against its digest, which catches corruption and truncation. It does not — and cannot — tell you who produced an entry. A writer that controls both the action record and the body can make them agree.

Treat write access to a shared bucket as equivalent to commit access to everything built against it:

- Use separate buckets, key prefixes, or IAM policies for jobs at different trust levels.
- Do not let untrusted fork builds, lower-trust repositories, or unrelated tenants write to a bucket that protected-branch CI reads.
- Prefer read-only credentials for any job that does not need to populate the cache.

The local tier has the same property in miniature: the cache directory and its unix socket are user-scoped, and anything that can write the cache directory can serve arbitrary bytes to your builds.

## Development

Run the local gates from the repository root:

```sh
go build ./...
go vet ./...
go test -race ./...
```

The race detector is part of the normal gate: the daemon serves concurrent clients over one index, so data races are the failure mode that matters most here.

The repository has no Makefile, no vendored dependencies, and no repository-level linter configuration.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the local development workflow.

## License

Apache License 2.0.
