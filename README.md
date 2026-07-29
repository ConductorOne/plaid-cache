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

`status` summarises what is in the cache and how close it is to being evicted:

```
directory   /home/you/.cache/plaid-cache
config      /home/you/.config/plaid-cache/config
entries     274 actions, 206 objects (1.33x dedup, 173.1 KiB avg)
size        34.8 MiB of 64.0 MiB (54.4%, 29.2 MiB free)
volume      12.1 GiB used of 50.0 GiB (24.2%, 37.9 GiB free)
ttl         168h0m0s
age         oldest 4s, newest 1s
remote      s3://example-bucket--usw2-az1--x-s3/arm64
daemon      pid 195247, up 4s
hit rate    59.6% of 344 lookups
hits        205 local, 0 remote
misses      139
puts        275
uploads     205 ok, 0 failed, 0 dropped, 0 skipped
lifetime    71.2% of 128443 lookups since 2026-07-14 09:00 UTC (every process; see `plaid-cache stats`)
```

The counters above `lifetime` are this daemon's own. The one below is every
process that has ever used this cache, which is usually the number you want:
a daemon exits after its idle timeout and a plugin invocation lasts one build,
so a process counter for a machine that has been quiet for half an hour
describes almost nothing that happened on it.

Two of those lines answer questions the raw counters do not. The dedup ratio is
actions per stored body, which is what refcounting outputs buys: many actions
resolve to one object. The age span is the last-used time of the least and most
recently used entries, so an oldest age well under the TTL means only the size
ceiling is currently evicting anything.

`status` reads from the running daemon when there is one, since it holds the
index and the live counters, and opens the index directly when there is not. It
never starts a daemon just to answer a question, so the metrics lines are absent
when nothing is running.

Run the daemon in the foreground, for a container or a supervised service:

```sh
plaid-cache serve
```

### Activity history

`status` describes the cache now; `stats` describes what it has done:

```
$ plaid-cache stats -since 24h
window      last 24h0m0s, 9 hours with activity
hit rate    73.4% of 51203 lookups
hits        38200 local, 1382 remote
misses      11621
puts        4021
uploads     4021 ok, 0 failed, 0 dropped, 0 skipped
lifetime    71.2% of 128443 lookups since 2026-07-14 09:00 UTC

hour (UTC)          lookups   hit%    local  remote   misses   puts
2026-07-28 09:00      12043  81.2%     9600     180     2263     900
2026-07-28 10:00       8110  64.9%     5100     164     2846     612
```

The counters are persisted in the index, so they survive the daemon's idle exit
and cover every process that has used the cache. Without that, a hit rate is a
statement about whichever process happens to answer — which for a machine that
has been idle is a fresh daemon that has seen one build, or no daemon at all.

The per-hour rows are the reason to keep history rather than one running total:
a total cannot distinguish a cache that is working now from one that worked well
last week, and a hit rate that is falling looks identical to a healthy one when
it is averaged over a fortnight. Hours with no activity are simply absent.

`-json` emits the whole response, including every bucket, for a tool to read:

```sh
plaid-cache stats -since 168h -json
```

Two weeks of hourly buckets are kept, which is a few hundred bytes an hour and
under a megabyte in total; older ones are dropped by the same write that records
new activity. A cache that exists to bound growth should not accumulate its own
telemetry forever.

Counters are written a few seconds after the work they describe, and on a clean
exit — including the idle timeout. A process killed outright, by an OOM or a
container teardown, loses at most those few seconds.

Subcommands are `serve`, `status`, `stats`, `clean`, `gc`, `adopt`, and
`version`.

### Adopting an existing go-cache-plugin stage

`plaid-cache` and [tailscale/go-cache-plugin](https://github.com/tailscale/go-cache-plugin)
address bodies identically, so a local stage written by one is already laid out
the way the other expects. `adopt` imports one:

```sh
plaid-cache adopt /path/to/go-cache-plugin/stage
plaid-cache adopt -dry-run /path/to/go-cache-plugin/stage
```

It reconstructs the action-to-output mapping from that stage's own records — one
file per action, named for the action id and containing the output id and size —
and publishes the bodies by **hardlink**, so nothing is copied:

```
16836 records: 16836 adopted (16836 linked, 0 copied, 14.1 GiB), 0 already
present, 0 missing bodies, 0 size mismatches, 0 malformed, in 2.4s
```

Hardlinking is what makes this safe to run against a stage the other cache is
still using: each side holds its own name for one inode, so neither one's pruning
can pull data out from under the other. It also means the imported bytes count
toward this cache's size ceiling, which is the reason to adopt rather than simply
point both tools at one directory — an index that does not know about the bodies
cannot bound them.

Recency is taken from the action file rather than the body. go-cache-plugin
stamps a body it faults in from the remote tier with the time that content was
originally produced, so an entry in daily use can have a months-old body; its own
prune keys off the action file for the same reason. The body's time becomes the
entry's creation time.

Adoption writes to the index, which exactly one process may hold, so `adopt` asks
a running daemon to do the import and only holds the index itself when no daemon
answers. That is not a detail: on the machine that actually needs a stage
migrated, the daemon is busy serving builds — the switch to `plaid-cache` and the
stage becoming dead weight are the same event — so requiring the daemon to be
stopped meant the migration had to win a race it loses on a busy machine.

It is idempotent: records already indexed are left alone.

Bodies are only linkable within one filesystem. Two mounts of the same ZFS
dataset report the same device and still refuse a link, so a cross-device stage
falls back to copying — the `linked` and `copied` counts say which happened.

### Overriding the limits

The two eviction limits can be set by environment variable, or by flag on `serve`
and `gc`. A flag wins over the environment, which wins over the default:

```sh
plaid-cache serve -max-bytes=50GiB -ttl=72h    # for the daemon's lifetime
plaid-cache gc -max-bytes=1GiB                 # for one pass only
```

`gc` forwards the override to a running daemon rather than applying it locally,
which matters because the daemon reads its configuration once at startup. Before
this it would read its own, generous ceiling and prune nothing, and a request
that appeared to be ignored was easy to misread as eviction being broken. The
response reports the limits the pass actually applied:

```
$ plaid-cache gc -max-bytes=1MiB
pruned 267 actions, 201 objects, freed 34.8 MiB in 12ms
applied      max-bytes 1.0 MiB, ttl 168h0m0s (this pass only)
```

The override lasts for that pass. The daemon's own policy, and what its eviction
ticker does next, are unchanged — a one-off sweep does not silently become the
new configuration. To change the policy itself, restart the daemon with the flag
or the environment variable set.

Zero is a meaningful value for either limit and disables that constraint, so
`-max-bytes=0` prunes on age alone and `-ttl=0` on size alone.

### What `max-bytes` counts

Allocated bytes on disk, not the lengths of the files.

The distinction is not academic on a compressing filesystem. A body's cost is
first recorded the moment it is written, which is the one moment it cannot be
measured — ZFS defers allocation to the next transaction group, so a file written
a second ago reports a single block however large it is — so the figure taken then
is deliberately an overestimate. It is corrected once the allocation is real:
before a size-driven eviction, bodies past the settle window are re-measured and
their recorded costs replaced.

Without that correction the budget stays at the logical lengths forever, and on a
dataset compressing 3x a 40 GiB ceiling starts evicting at about 13 GiB of actual
disk. That was [issue #5](https://github.com/conductorone/plaid-cache/issues/5):
34748 entries pruned while two thirds of the budget sat unused.

The re-measure runs on the automatic path only when the recorded total is within
10% of the ceiling, since below that the difference cannot change any decision,
and it costs one stat per body — tens of milliseconds for tens of thousands of
files. `gc` measures whatever the pressure, because a pass someone asked for by
hand should be decided on current numbers, and it reports what changed:

```
$ plaid-cache gc
pruned 0 actions, 0 objects, freed 0 B in 29ms
measured     780 of 786 objects re-measured, recorded size 320.4 MiB -> 123.3 MiB
``` `status` reports
the volume alongside the budget, so the two can be compared:

```
size        10.5 GiB of 40.0 GiB (26.3%, 29.5 GiB free)
volume      16.0 GiB used of 50.0 GiB (32.0%, 34.0 GiB free)
```

On platforms without `st_blocks` the budget is the logical length, which is all
that is available there.

## Configuration

Every setting has one name and one fallback chain: the environment, then the
configuration file, then the default. There are no defaults for the bucket,
region, prefix, or endpoint — leaving `PLAID_GOCACHE_S3_BUCKET` empty runs the
cache entirely locally.

### The configuration file

Settings that should apply to every build go in a file, so that a shell profile is
not the only place to put them:

```sh
$XDG_CONFIG_HOME/plaid-cache/config     # or ~/.config/plaid-cache/config
```

It is a list of `KEY=value` lines using **the same names as the environment**, so
there is one vocabulary rather than two with a mapping in between — a setting moves
between a shell and a file by copying the line:

```sh
# ~/.config/plaid-cache/config
s3-bucket = my-bucket--usw2-az1--x-s3
s3-region = us-west-2
max-bytes = 50GiB
ttl       = 168h
```

The shared `PLAID_GOCACHE_` prefix may be left off, keys are case-insensitive, and
dashes read as underscores. Blank lines and `#` comments are ignored, a leading
`export ` is tolerated, and a value may be quoted when its spacing matters.

**The environment wins.** A file is a standing preference on one machine; a
variable is a decision about the invocation in front of you, and a wrapper script
or a CI job has no other way to express one.

**A key outside the documented set is an error, not a warning**, as is a duplicate
key or a line that is not `KEY=value`. The whole risk of a configuration file is a
setting that looks applied and is not: a typo in a size ceiling that quietly
reverted to the default would let the cache grow until it filled the disk, which is
the failure this tool exists to prevent. `status` prints the file it read for the
same reason.

Note what that costs. The GOCACHEPROG plugin is one of the commands that reads this
file, so a malformed file fails builds until it is fixed, the same way an
unparseable `PLAID_GOCACHE_MAX_BYTES` already does. That is the deliberate trade:
the error names the file, the line, and the offending key, and it is one edit to
fix — where a silently-ignored file gives you a cache that is not the one you
configured and no way to notice.

An absent file is the normal case and not an error. `PLAID_GOCACHE_CONFIG` names
one explicitly, and a file named that way must exist.

`XDG_CACHE_HOME` is read from the environment only — it is the platform's setting
rather than this tool's, and a file able to move every program's cache root would
be a surprising amount of reach for this one to have.

| Variable | Purpose | Default |
| --- | --- | --- |
| `PLAID_GOCACHE_CONFIG` | Configuration file to read, overriding the XDG lookup. A file named here must exist. | `$XDG_CONFIG_HOME/plaid-cache/config` |
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
| `PLAID_GOCACHE_COMPACT_AFTER` | Pruned entries that must accumulate before the index is compacted. Deletes in an LSM are writes, so pruning grows the index until a compaction reclaims it. | `1000` |
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
