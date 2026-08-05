# plaid-cache

[![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/plaid-cache.svg)](https://pkg.go.dev/github.com/conductorone/plaid-cache)

`plaid-cache` is a `GOCACHEPROG`-compatible build cache for the Go toolchain, backed by a content-addressed local body store with a Pebble-backed index and an optional S3 Express One Zone remote tier shared across machines. It also speaks both of [Bazel's remote-cache protocols](#bazel) — the gRPC Remote Execution API and the HTTP one — so a Bazel build can share the same store and the same shared tier.

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

For a daemon on another machine there is `plaid-cache status -from <address>`,
which reads the same report over an endpoint that daemon has to have been asked
to serve; see [monitoring a shared daemon](#monitoring-a-shared-daemon).

Run the daemon in the foreground, for a container or a supervised service:

```sh
plaid-cache serve
```

### Bazel

The daemon can also serve Bazel's remote cache, so a Bazel build shares the local store, the byte budget, and the shared remote tier with the Go builds beside it. Both of Bazel's cache protocols are available, over one store:

```sh
plaid-cache serve -bazel-grpc-addr localhost:9096
```

```sh
bazel build --remote_cache=grpc://localhost:9096 //...
```

```sh
plaid-cache serve -bazel-addr localhost:9095
```

```sh
bazel build --remote_cache=http://localhost:9095 //...
```

Both listeners are off unless asked for, and both take a full address rather than a port so that the choice between loopback and every interface is explicit. `PLAID_GOCACHE_BAZEL_GRPC_ADDR` and `PLAID_GOCACHE_BAZEL_ADDR` are the same settings from the environment or the configuration file. They may run at once: a build that uploads over one reads its own outputs back over the other.

**Prefer gRPC.** It is the only one of the two whose client can ask the server which blobs it already holds, and that one question is worth more than everything else on this page put together — see [choosing a protocol](#choosing-a-protocol).

The code keeps storage and transport apart on purpose. The storage adapter knows Bazel's two keyspaces and how they map onto this cache; each transport knows only its own wire format. Both protocols address the same keyspaces with the same digests, so the second front end is transport code over the same storage rather than a second storage design.

Bazel's two keyspaces map onto what the index already stores. A CAS blob is addressed by the SHA-256 of its own content, which is exactly an output id, so a blob uploaded by Bazel and the same bytes produced by a Go build occupy one file locally and one object remotely. An action-cache entry maps an action digest to an `ActionResult` message; storing that message *as* a body makes it the same shape as every other entry, so nothing here parses a protocol buffer and no new record type, table, or refcounting rule was needed. The two keyspaces are namespaced apart before they reach the index, because one digest legitimately names an entry in both — Bazel stores an action's `Action` message in the CAS under the digest that keys its `ActionResult` in the action cache.

Bazel traffic shows up in `plaid-cache status` and `plaid-cache stats` beside the toolchain's, since both go through the same tiers.

#### Choosing a protocol

The two protocols reach the same store, so the choice is about what a client can say over each.

Bazel gates output uploads on an existence check: `UploadManifest.upload` asks the cache which of the digests it is about to send are missing, and sends only those. Over gRPC that is a real `FindMissingBlobs` call with a real answer. Over HTTP there is no such call, and Bazel's HTTP client answers the question for itself by declaring every digest missing — so every action that re-runs re-uploads outputs the server already holds.

Measured against Bazel 9.2.0, on a throwaway workspace whose four actions each produce an 8 MiB output (32 MiB total). Bytes are counted on the wire, client to server, by a proxy in front of the listener:

| | HTTP | gRPC |
| --- | ---: | ---: |
| Cold build, nothing cached | 33,561,200 | 33,625,080 |
| Fresh output base, cache hit — nothing re-runs | 1,708 | 3,967 |
| **Fresh output base, actions re-run and produce identical outputs** | **33,561,200** | **8,495** |

The third row is the one that matters, and it is the common case in any workflow where action keys change more often than action outputs do — a compiler flag, an environment variable, a timestamp in an input. HTTP re-sends all 32 MiB. gRPC sends 8 KB, a reduction of about 3,950×, because `FindMissingBlobs` told it not to bother.

That the saving comes from `FindMissingBlobs` and not from somewhere else was checked by breaking it: a build against a server whose `FindMissingBlobs` was stubbed to report everything missing — the answer Bazel's HTTP client assumes — uploaded 1,879,253 bytes for the same third row, 221× more. (It is not the full 32 MiB because the ByteStream write of a blob the server already has is terminated as soon as the server sees which blob it is, which catches what is already in flight. That backstop is real, but it is a backstop; the question is worth asking a step earlier.)

gRPC brings three smaller things with it:

- **Batching.** `BatchReadBlobs` and `BatchUpdateBlobs` carry many small blobs per round trip, which matters for the many-small-outputs shape that HTTP pays a request each for.
- **Resumable uploads.** A ByteStream write that breaks part-way is resumed from where it stopped rather than restarted. Over HTTP a retried `PUT` starts a several-hundred-megabyte body again from byte zero.
- **Compression.** `--remote_cache_compression` is gRPC-only in Bazel, and is supported here.

HTTP remains fully supported and is the simpler thing to operate: one port, one protocol, no protobuf. If your outputs are small, or your actions rarely re-run, it costs you little.

#### The gRPC listener

`-bazel-grpc-addr` serves the cache half of the [Remote Execution API](https://github.com/bazelbuild/remote-apis) v2:

- `Capabilities.GetCapabilities` — advertises SHA-256, an updatable action cache, the batch size limit, and `identity` and `zstd` compressors.
- `ActionCache.GetActionResult` / `UpdateActionResult`.
- `ContentAddressableStorage.FindMissingBlobs` / `BatchUpdateBlobs` / `BatchReadBlobs`.
- `ByteStream.Read` / `Write` / `QueryWriteStatus`.

What is deliberately absent:

- **Remote execution.** `GetCapabilities` returns no execution capabilities at all, so a client pointed here with `--remote_executor` is told at connection time rather than one failed call at a time.
- **`GetTree`.** It walks a `Directory` tree inside blobs this cache treats as opaque, on behalf of a client that has been handed the root and can walk it with the blob reads it is making anyway. A cache client never calls it. It returns `UNIMPLEMENTED` with a message saying so.
- **Digest functions other than SHA-256.** A 32-byte SHA-256 digest is already this cache's identifier for a body, which is what lets a Bazel output and the same bytes from a Go build be one file. A client naming another function is refused with `INVALID_ARGUMENT` unless `PLAID_GOCACHE_DISABLE_BAZEL_VERIFY=1` is set, which is the same escape hatch the HTTP listener offers and means the same thing: the operator has decided to trust its clients' addressing.
- **Named instances.** Only the empty instance name is served. Serving several out of one keyspace would let two logically separate caches share entries without either being told.

**Compression.** `--remote_cache_compression` works. A compressed upload is decoded as it streams in and stored as the plain body, and a compressed download is encoded as it streams out, so one stored copy serves a compressing client, a plain one, the HTTP listener and a Go build that produced the same bytes. Encoding runs at zstd's fastest setting: a cache serves a blob many times and stores it once, so time spent squeezing it is paid on every read. Measured with Bazel 9.2.0 on a single 256 MiB output, the cold upload fell from 268,985,383 bytes to 2,196,171.

**Timeouts.** The server imposes no deadline on a transfer, on purpose — a cache blob can be hundreds of megabytes, and the only honest bound is the client's own patience. What it bounds is idleness: a connection with no active stream is closed after ten minutes, and there is deliberately no `MaxConnectionAge`, which would tear down a connection mid-transfer at an age unrelated to whether the transfer is progressing. Its keepalive enforcement is permissive — a client may ping every ten seconds, with or without an active stream — because a client checking whether the server is still there is exactly what lets it tell a stalled transfer from a slow one.

The matching client-side setting is worth knowing about, because it differs sharply between the two protocols. Bazel's HTTP client applies `--remote_timeout` as an idle timeout: a bound on the longest no-progress gap, so a slow-but-moving transfer of any size completes. Its gRPC client applies the same flag as `withDeadlineAfter` on every stub including ByteStream — a deadline on the *whole* call. **Size `--remote_timeout` for your largest blob divided by your slowest realistic throughput, not for your longest expected stall.** A 700 MB output at 25 MB/s needs more than 28 seconds, and will hard-fail rather than stall if it does not get them.

**Memory.** Nothing buffers a whole blob. A read streams from the open file to the wire and a write streams from the wire to a staging file. Measured: the daemon's peak resident memory while a 256 MiB output was uploaded, downloaded and re-offered was 39.5 MB, and 41.3 MB with compression on.

**Resumable writes.** A `ByteStream.Write` that breaks leaves what it delivered on disk, registered under its resource name. `QueryWriteStatus` reports how far it got, and the next `Write` at that offset continues into the same body. A partial upload nobody resumes is released after ten minutes; at most 256 are held at once, and the longest-idle is dropped to make room rather than refusing a new one. Anything left staged by a process that died is swept at the next start.


#### Build without the bytes

`--remote_download_outputs` works over this cache. It is implemented above Bazel's cache-client abstraction — the decision about which outputs to materialise, and the on-demand fetch when something later needs one, are the same code whichever protocol carries the bytes — so an HTTP cache inherits it in full. Measured against Bazel 9.2.0 with a warm cache: `--remote_download_outputs=all` materialised every output, `=minimal` materialised none of them and still took every cache hit, and asking for one afterwards fetched a 400 MB output on demand without re-running the action. The default, `toplevel`, downloads what you asked to build and nothing underneath it.

The one caveat is `--experimental_remote_cache_lease_extension` (off by default), which renews the leases on remote outputs by asking the server which blobs it still has. There is no way to ask that over HTTP, and Bazel's HTTP client answers the question for itself by reporting every blob as absent, so no lease is ever renewed and remote metadata expires on `--experimental_remote_cache_ttl` regardless. Leaving the flag off loses nothing. Over gRPC the question has a real answer: a probe that finds a blob present also refreshes its position in the eviction order, so an entry a build has just decided it need not re-send is one eviction has been told is in use.

#### Re-uploads over HTTP

The HTTP protocol has no way for Bazel to ask which digests the server already has, and Bazel's HTTP client reports every digest as absent rather than probing: a traced build shows it issue `PUT` for every output of every action it ran, with no `HEAD` or `GET` first. Uploads only follow an action that actually executed — a cache hit uploads nothing — but any action that re-runs re-sends outputs the server already holds, and for a several-hundred-megabyte output that is not free. This is the cost the gRPC listener's `FindMissingBlobs` removes; over HTTP the best available answer is later and smaller.

So the server does the skipping instead. A `PUT /cas/<digest>` for a body already stored is drained and dropped: no staging write, no hash, no index write, no upload to the shared tier, and the entry's last-used time is refreshed because a body being offered again is a body in use. The bytes still cross the connection — nothing in the protocol can stop that once the request is sent — but everything after the socket is saved. The bytes on disk stay the ones that were verified when they were first stored, so a later upload that disagrees with them is discarded rather than believed.

Action-cache entries are not skipped this way. They are named by the action rather than by their content, and a re-run action legitimately produces a different `ActionResult`, so the newer one wins.

Two more behaviours are worth knowing about:

- **Uploaded CAS bodies are verified.** A `PUT /cas/<digest>` whose body does not hash to that digest is rejected with `400` and not stored: it would otherwise publish arbitrary bytes under an address promising different ones, on this machine and — once the shared tier has them — on every other. Only the bare hash appears in the request path, so a server cannot tell which digest function produced it; a client using something other than SHA-256 (`--digest_function=blake3`) needs `PLAID_GOCACHE_DISABLE_BAZEL_VERIFY=1`, which stores bodies under the digest they claim.

- **The daemon does not idle out while the listener is up.** A `GOCACHEPROG` client that finds no daemon spawns one. Bazel cannot, and would get a refused connection where it expected a miss, so a daemon asked to serve Bazel is one that has been asked to stay.

#### Action-cache and CAS entries expire independently

An action-cache entry names CAS blobs that are stored, and evicted, separately from it. Nothing in this protocol ties their lifetimes together, so an entry can outlive the blobs it references:

- **Locally**, when eviction reaches a blob before the entry pointing at it. Eviction is oldest-first by last use, and an ordinary build reads an entry and its blobs within the same moment, so they tend to age together — but nothing enforces it, and under `--remote_download_outputs=minimal` the gap between the two widens to however long it is before something needs the bytes.
- **In the shared tier**, when an object-lifecycle rule expires them. There is no delete operation, so remote retention is whatever the bucket's rules say, and an age-based rule cannot be told to expire an action and its outputs as a unit.

Bazel treats the resulting dangling reference as a failed download rather than as corruption, and `--experimental_remote_cache_eviction_retries` (5 by default) bounds how many times it will reset its state and retry rather than failing the build. Serving a plain miss for the absent blob is the whole of the server's obligation, and it is enough: evicting the entire cache in between a `--remote_download_outputs=minimal` build and the request for its 400 MB output re-ran the action and completed the build successfully. Verifying the reference at lookup time would mean parsing `ActionResult` and probing every blob it names on the hot path, which costs more than the occasional re-run it would save, so it is documented here rather than implemented.

If you set a lifecycle rule on the bucket, expiring action records earlier than output bodies keeps the dangling reference on the harmless side: an action record with no body is a miss, where a body with no action record is merely unreferenced.

### Monitoring a shared daemon

`plaid-cache status` reads the local daemon over a unix socket, which is the right answer for a cache on the machine you are sitting at and no answer at all for one serving a room full of builders. `-bazel-monitoring` adds two read-only routes to the Bazel HTTP address for that case:

```sh
plaid-cache serve -bazel-addr localhost:9095 -bazel-monitoring
```

```sh
plaid-cache status -from localhost:9095
curl -s localhost:9095/metrics
```

`status -from` prints the report a local `status` prints, minus the lines that would describe the machine you ran it on rather than the daemon you asked:

```
endpoint    http://cache-host:9095/status
version     plaid-cache v0.4.0
entries     274 actions, 206 objects (1.33x dedup, 173.1 KiB avg)
size        34.8 MiB of 64.0 MiB (54.4%, 29.2 MiB free)
ttl         168h0m0s
age         oldest 20m0s, newest 4s
remote      enabled
daemon      pid 4210, up 3h20m0s
hit rate    59.9% of 347 lookups
...
```

There is no `directory`, no `config`, and no `volume` line, and `remote` says whether a shared tier is configured rather than naming the bucket. The endpoint reports counters and the limits being enforced; it does not report where the cache lives, what file configured it, or what it uploads to. A flag named `-from` and not `-remote` because in this tool "remote" is the S3 tier that every report has a line about.

`/metrics` is Prometheus text exposition, which an OpenTelemetry Collector scrapes as it stands through its `prometheus` receiver — there is no push exporter here, and none is needed to get these numbers into an OTel pipeline. The gauges describe the cache now, the counters are the persisted lifetime tallies that survive the daemon's idle exit, and every number is rendered from the same report `status` prints, so a scrape and a status report taken together cannot disagree:

| Metric | Type | |
| --- | --- | --- |
| `plaid_cache_actions`, `plaid_cache_objects` | gauge | Entries and distinct bodies in the index. |
| `plaid_cache_disk_bytes` | gauge | Bytes of stored bodies the index accounts for. |
| `plaid_cache_max_bytes`, `plaid_cache_ttl_seconds` | gauge | The limits eviction is enforcing. Zero disables that constraint. |
| `plaid_cache_oldest_entry_age_seconds`, `plaid_cache_newest_entry_age_seconds` | gauge | The age span, absent for a cache with no entries. |
| `plaid_cache_uptime_seconds`, `plaid_cache_build_info` | gauge | This daemon and the build serving it. |
| `plaid_cache_remote_tier_enabled` | gauge | 1 when a shared tier is configured. |
| `plaid_cache_gets_total{result}` | counter | Lookups by outcome: `local_hit`, `remote_hit`, `miss`. |
| `plaid_cache_puts_total`, `plaid_cache_repairs_total`, `plaid_cache_compactions_total` | counter | Stores, index entries dropped for a missing body, and compactions. |
| `plaid_cache_uploads_total{result}` | counter | Uploads by outcome: `ok`, `failed`, `dropped`, `skipped`. |
| `plaid_cache_activity_start_time_seconds` | gauge | When the counters above started counting. |

Two labels, both with a short fixed set of values. Nothing is ever labelled by digest, key, path, or client: a series is created for every distinct label value and never forgotten, so a per-request label is an unbounded leak in whatever scrapes it.

**These routes are off unless asked for, and that is the conservative default on purpose.** They describe the host — pid, uptime, entry counts, byte budgets — where the cache routes beside them describe only blobs somebody already knew the digest of. `PLAID_GOCACHE_BAZEL_ADDR` takes a full address precisely because the choice between loopback and every interface is the difference between a private cache and a public one, and this is a second disclosure on top of that choice, so it is a second decision. One setting governs both routes, because they disclose the same thing and a split would offer a choice with nothing behind it.

To let a scraper in without opening the daemon to the network, keep the listener on an interface only the scraper can reach — loopback with a node-local scraper or an SSH tunnel, a pod address reachable only through a network policy — or put a reverse proxy in front of it. The monitoring routes are two fixed paths that no cache request can ever take, so a proxy can serve `/status` and `/metrics` to your monitoring subnet and `/ac/…` and `/cas/…` to your builders, with different rules on each.

Unlike the cache routes, these two report their failures honestly. A cache route answers a broken store with a miss or with a success, because Bazel reads anything else as a build error and a cache must never break a build; a monitoring route answers with a `5xx`, because a reader asking how the daemon is doing is exactly the reader who must not be told "fine". `status -from` exits non-zero on any of it, so a report that could not be obtained is never mistakable for a cache with nothing in it.

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
| `PLAID_GOCACHE_BAZEL_ADDR` | Address for the Bazel HTTP remote cache, e.g. `localhost:9095`. Empty serves it not at all. | empty |
| `PLAID_GOCACHE_BAZEL_GRPC_ADDR` | Address for the Bazel gRPC remote cache, e.g. `localhost:9096`. Empty serves it not at all. | empty |
| `PLAID_GOCACHE_BAZEL_MONITORING` | `1` also serves `/status` and `/metrics` on the Bazel HTTP address, for `plaid-cache status -from` and for a Prometheus scrape. Off by default: they describe the host rather than the cache's contents. | unset |
| `PLAID_GOCACHE_DISABLE_BAZEL_VERIFY` | `1` stops both Bazel listeners from checking that an uploaded CAS body hashes to the digest naming it, and lets a gRPC client name a digest function this cache cannot compute. For clients whose digest function is not SHA-256. | unset |
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
