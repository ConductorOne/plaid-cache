// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/conductorone/plaid-cache/internal/adopt"
	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// loadConfig resolves configuration, reporting failures to stderr.
func (a *app) loadConfig() (*config.Config, bool) {
	cfg, err := config.Load()
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return nil, false
	}
	return cfg, true
}

// logger returns a log function honoring the configured verbosity.
func (a *app) logger(cfg *config.Config, min config.LogLevel) cache.Logf {
	if cfg.Log < min {
		return func(string, ...any) {}
	}
	return func(format string, args ...any) {
		a.errf("plaid-cache: "+format+"\n", args...)
	}
}

// stores holds the tiers a process opened and must close.
type stores struct {
	idx   *index.Index
	blobs *blob.Store
	rem   remote.Backend
}

// close releases the tiers in reverse order of dependency.
func (s *stores) close() {
	if s.rem != nil {
		_ = s.rem.Close()
	}
	if s.idx != nil {
		_ = s.idx.Close()
	}
}

// openStores opens the index, body store, and remote tier.
//
// Opening the index takes Pebble's exclusive directory lock, so this succeeds
// in exactly one process at a time.
func openStores(ctx context.Context, cfg *config.Config) (*stores, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("openStores: %w", err)
	}
	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		return nil, fmt.Errorf("openStores: index: %w", err)
	}
	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("openStores: blobs: %w", err)
	}
	var rem remote.Backend = remote.Noop{}
	if cfg.RemoteEnabled() {
		s3, err := remote.NewS3(ctx, remote.S3Params{
			Bucket:      cfg.S3Bucket,
			Region:      cfg.S3Region,
			Prefix:      cfg.S3Prefix,
			EndpointURL: cfg.S3EndpointURL,
		})
		if err != nil {
			// A misconfigured bucket must not stop the local cache from
			// working; the shared tier is an optimization.
			return &stores{idx: idx, blobs: blobs, rem: remote.Noop{}}, nil
		}
		rem = s3
	}
	return &stores{idx: idx, blobs: blobs, rem: rem}, nil
}

// runServe runs the daemon in the foreground.
func (a *app) runServe(ctx context.Context) int {
	var limits limitFlags
	var bazelAddr, bazelGRPCAddr string
	var bazelMonitoring bool
	register := func(fs *flag.FlagSet) {
		limits.register(fs)
		fs.StringVar(&bazelAddr, "bazel-addr", "",
			"also serve Bazel's HTTP remote-cache protocol on this address, e.g. localhost:9095 (default: PLAID_GOCACHE_BAZEL_ADDR)")
		fs.StringVar(&bazelGRPCAddr, "bazel-grpc-addr", "",
			"also serve Bazel's gRPC remote-cache protocol on this address, e.g. localhost:9096 (default: PLAID_GOCACHE_BAZEL_GRPC_ADDR)")
		fs.BoolVar(&bazelMonitoring, "bazel-monitoring", false,
			"also serve "+bazel.StatusPath+" and "+bazel.MetricsPath+" on the Bazel HTTP address (default: PLAID_GOCACHE_BAZEL_MONITORING)")
	}
	if _, err := a.parseFlags("serve", register, a.args[1:]); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	if err := limits.applyTo(cfg); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	if bazelAddr != "" {
		cfg.BazelAddr = bazelAddr
	}
	if bazelGRPCAddr != "" {
		cfg.BazelGRPCAddr = bazelGRPCAddr
	}
	// The flag can turn monitoring on and not off, matching the addresses above:
	// a flag given is a decision, a flag omitted is silence, and silence must
	// leave the environment's answer standing.
	if bazelMonitoring {
		cfg.BazelMonitoring = true
	}
	logf := a.logger(cfg, config.LogInfo)

	st, err := openStores(ctx, cfg)
	if err != nil {
		// Losing the race for the index lock is the normal outcome when
		// several clients spawn a daemon at once. Exit quietly; the winner
		// serves everyone.
		if errors.Is(err, index.ErrLocked) {
			logf("another daemon owns the index, exiting")
			return exitOK
		}
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	c := cache.New(cache.Params{
		Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem, Logf: logf,
	})
	defer func() { _ = c.Close() }()

	ln, err := daemon.Listen(cfg)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}

	srv := daemon.NewServer(daemon.ServerParams{
		Config: cfg, Cache: c, Index: st.idx, Blobs: st.blobs, Logf: logf, Version: buildVersion(),
	})
	logf("serving on %s (pid %d)", cfg.SocketPath(), os.Getpid())

	// The Bazel listener runs beside the socket rather than instead of it, and
	// it holds the index the deferred closes above release. Stopping the daemon
	// and waiting for it therefore has to happen before those run, which is
	// what the ordering of these two defers buys.
	var bazelWG sync.WaitGroup
	defer bazelWG.Wait()
	defer srv.Stop()

	if cfg.BazelAddr != "" {
		bazelLn, lerr := net.Listen("tcp", cfg.BazelAddr)
		if lerr != nil {
			// Refusing to start is the point. A daemon that quietly served only
			// half of what it was asked for would leave Bazel with a refused
			// connection on every action, which it reports as an error rather
			// than as a miss.
			a.errf("plaid-cache: bazel listener: %v\n", lerr)
			return exitError
		}
		logf("serving the Bazel HTTP cache on http://%s", bazelLn.Addr())
		if cfg.BazelMonitoring {
			// Worth a line of its own: this is the one setting that makes the
			// listener say something about its host rather than only about the
			// blobs it was asked for.
			logf("serving monitoring on http://%s%s and http://%s%s",
				bazelLn.Addr(), bazel.StatusPath, bazelLn.Addr(), bazel.MetricsPath)
		}
		bazelWG.Add(1)
		go func() {
			defer bazelWG.Done()
			if err := srv.ServeBazel(ctx, bazelLn); err != nil && !errors.Is(err, context.Canceled) {
				logf("bazel listener: %v", err)
			}
		}()
	}

	if cfg.BazelGRPCAddr != "" {
		grpcLn, lerr := net.Listen("tcp", cfg.BazelGRPCAddr)
		if lerr != nil {
			a.errf("plaid-cache: bazel grpc listener: %v\n", lerr)
			return exitError
		}
		logf("serving the Bazel gRPC cache on grpc://%s", grpcLn.Addr())
		bazelWG.Add(1)
		go func() {
			defer bazelWG.Done()
			if err := srv.ServeBazelGRPC(ctx, grpcLn); err != nil && !errors.Is(err, context.Canceled) {
				logf("bazel grpc listener: %v", err)
			}
		}()
	}

	if err := srv.Serve(ctx, ln); err != nil && !errors.Is(err, context.Canceled) {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	logf("stopped")
	return exitOK
}

// runPlugin serves the GOCACHEPROG protocol for one `go` invocation.
//
// The preferred path relays to the shared daemon. Every fallback below exists
// because a build must never fail on account of its cache: a cache that can
// break a build is worse than no cache at all.
func (a *app) runPlugin(ctx context.Context) int {
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	logf := a.logger(cfg, config.LogError)

	if !cfg.DisableDaemon {
		err := daemon.RunPlugin(ctx, cfg, buildVersion(), a.stdin, a.stdout, a.logger(cfg, config.LogInfo))
		if err == nil {
			return exitOK
		}
		logf("daemon unavailable (%v), falling back to direct mode", err)
	}
	return a.runDirect(ctx, cfg, logf)
}

// runDirect serves the protocol in this process, without a daemon.
//
// This forfeits sharing between concurrent `go` invocations — only one of
// them can hold the index — but it keeps the cache working for the one that
// wins, and keeps the build working for the ones that do not.
func (a *app) runDirect(ctx context.Context, cfg *config.Config, logf cache.Logf) int {
	st, err := openStores(ctx, cfg)
	if err != nil {
		logf("cannot open cache (%v), serving misses", err)
		return a.serveMisses()
	}
	defer st.close()

	c := cache.New(cache.Params{
		Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem, Logf: logf,
	})
	defer func() { _ = c.Close() }()

	srv := daemon.NewServer(daemon.ServerParams{
		Config: cfg, Cache: c, Index: st.idx, Logf: logf, Version: buildVersion(),
	})
	srv.RunSession(ctx, a.stdin, a.stdout)

	// Prune before exiting. There is no eviction ticker here — that belongs to
	// the daemon — so without this pass a cache used in direct mode never
	// prunes at all and grows past both its TTL and its byte ceiling.
	//
	// The pass is cheap because eviction is driven by the index rather than by
	// walking the body tree, so paying it once per build is affordable in a way
	// a directory walk would not be.
	a.evictOnExit(ctx, cfg, c, logf)
	return exitOK
}

// evictExitTimeout bounds the closing eviction pass so a cancelled build cannot
// be held open by it.
const evictExitTimeout = 15 * time.Second

// evictOnExit runs one eviction pass as a direct-mode session ends.
func (a *app) evictOnExit(ctx context.Context, cfg *config.Config, c *cache.Cache, logf cache.Logf) {
	if cfg.DisableEviction {
		return
	}
	// Detach from ctx: the toolchain closing stdin, or a signal, cancels it,
	// and skipping the prune in exactly those cases is how a cache never
	// prunes at all. The timeout keeps that from delaying exit.
	ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), evictExitTimeout)
	defer cancel()

	res, err := c.Evict(ectx)
	if err != nil {
		logf("evict: %v", err)
		return
	}
	if res.ActionsPruned > 0 || res.ObjectsPruned > 0 {
		logf("evict: pruned %d actions, %d objects, freed %s in %v",
			res.ActionsPruned, res.ObjectsPruned, config.FormatBytes(res.BytesFreed), res.Elapsed)
	}
}

// serveMisses is the last resort: a protocol-correct cache that stores
// nothing. The toolchain rebuilds everything, which is slow but correct.
func (a *app) serveMisses() int {
	if err := daemon.ServeMisses(a.stdin, a.stdout); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	return exitOK
}

// runStatus reports the cache's contents.
func (a *app) runStatus(ctx context.Context) int {
	var from string
	if _, err := a.parseFlags("status", func(fs *flag.FlagSet) {
		// Not -remote. In this tool "remote" already means the shared S3 tier,
		// which every status report has a line about; a flag by that name would
		// read as asking about the bucket rather than about another daemon. What
		// this names is where the report comes from.
		fs.StringVar(&from, "from", "",
			"read the report from another daemon's monitoring endpoint, e.g. cache-host:9095 (default: this machine's own cache)")
	}, a.args[1:]); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	if from != "" {
		// Deliberately before loadConfig: nothing in this machine's environment
		// describes the daemon being asked, so reading it could only mislead —
		// and a broken local configuration is no reason to be unable to ask a
		// remote host how it is doing.
		return a.runStatusFrom(ctx, from)
	}

	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}

	// Ask a running daemon first: it holds the index lock and has live
	// counters. Only if none is running do we open the index ourselves.
	if conn, err := dialExisting(cfg, buildVersion(), daemon.OpStatus); err == nil {
		defer func() { _ = conn.Close() }()
		var resp daemon.StatusResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
		a.printStatus(cfg, resp.Actions, resp.Objects, resp.DiskBytes, &resp, resp.Lifetime, resp.LifetimeSince)
		return exitOK
	}

	st, err := openStores(ctx, cfg)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	s, err := st.idx.Stats()
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	life, since, err := st.idx.TotalActivity()
	if err != nil {
		// A report is still worth printing without its history.
		a.errf("plaid-cache: lifetime activity: %v\n", err)
	}
	a.printStatus(cfg, s.Actions, s.Objects, s.DiskBytes, nil, life, since)
	return exitOK
}

// statusFetchTimeout bounds a read from another daemon. Assembling the report
// is a handful of index lookups, so a host that has not answered in this long is
// not busy, it is unreachable — and a command a person is waiting on should say
// so rather than hang.
const statusFetchTimeout = 15 * time.Second

// maxStatusBody bounds what is read from an endpoint before giving up on it.
//
// A real report is a couple of kilobytes. The bound is what stops a wrong
// address — a log shipper, a video stream, something that answers every path
// with a redirect loop's worth of HTML — from being decoded into memory in the
// hope that JSON turns up later.
const maxStatusBody = 1 << 20

// runStatusFrom reports on a daemon reached over its monitoring endpoint,
// rather than on the cache belonging to this machine.
//
// Every failure here exits non-zero and says which endpoint failed. This command
// is the one an operator reaches for when they suspect a cache is unwell, so a
// report that could not be obtained must never be mistakable for a cache with
// nothing in it.
func (a *app) runStatusFrom(ctx context.Context, addr string) int {
	endpoint, err := statusEndpoint(addr)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	resp, err := (&http.Client{Timeout: statusFetchTimeout}).Do(req)
	if err != nil {
		a.errf("plaid-cache: %s: %v\n", endpoint, err)
		return exitError
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		a.errf("plaid-cache: %s: %s\n", endpoint, resp.Status)
		if resp.StatusCode == http.StatusNotFound {
			// The likeliest cause by some distance, and the one whose fix is not
			// guessable: a daemon serving Bazel with monitoring left off answers
			// this path exactly as it answers any path it does not serve.
			a.errf("plaid-cache: that daemon may be serving Bazel without -bazel-monitoring\n")
		}
		return exitError
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxStatusBody))
	if err != nil {
		a.errf("plaid-cache: %s: %v\n", endpoint, err)
		return exitError
	}
	var r daemon.StatusResponse
	if err := json.Unmarshal(body, &r); err != nil {
		// Truncated JSON and a proxy's error page arrive here alike, and neither
		// is worth quoting back in full at somebody's terminal.
		a.errf("plaid-cache: %s: not a status report: %v\n", endpoint, err)
		return exitError
	}
	if r.PID == 0 {
		// Well-formed JSON from something that is not a daemon. Every real report
		// names the process that produced it, and printing a report of zeros
		// would describe an empty cache rather than a wrong address.
		a.errf("plaid-cache: %s: not a status report: no daemon in it\n", endpoint)
		return exitError
	}
	if r.Err != "" {
		a.errf("plaid-cache: %s: %s\n", endpoint, r.Err)
		return exitError
	}
	a.printStatusFrom(endpoint, &r)
	return exitOK
}

// statusEndpoint turns what a person typed into the URL of a status route.
//
// A bare host and port is the common case and is what the -bazel-addr flag on
// the other end took, so it is accepted as written and given the http scheme.
// The path is fixed rather than taken from the argument: this route lives at one
// place, and accepting a path would invite the same prefix confusion the cache
// routes refuse — so the one path a caller may write is the one they would get
// anyway.
func statusEndpoint(addr string) (string, error) {
	s := strings.TrimSpace(addr)
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("-from: %q is not an address (want e.g. cache-host:9095)", addr)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("-from: %q: got scheme %q, want one of: http, https", addr, u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("-from: %q names no host (want e.g. cache-host:9095)", addr)
	}
	if p := strings.TrimSuffix(u.Path, "/"); p != "" && p != bazel.StatusPath {
		return "", fmt.Errorf("-from: %q: the endpoint is %s, and nothing is served under a prefix", addr, bazel.StatusPath)
	}
	return (&url.URL{Scheme: u.Scheme, User: u.User, Host: u.Host, Path: bazel.StatusPath}).String(), nil
}

// printStatus renders the status report for the cache this machine is
// configured for.
//
// The eviction limits come from the daemon when one is running, not from this
// process's environment. The daemon read its configuration when it started,
// and that is what actually governs the cache; printing the caller's own
// environment would show limits that nothing is enforcing.
func (a *app) printStatus(cfg *config.Config, actions, objects, diskBytes int64, d *daemon.StatusResponse, life cache.MetricsSnapshot, lifeSince int64) {
	maxBytes, ttl := cfg.MaxBytes, cfg.TTL.String()
	if d != nil {
		maxBytes, ttl = d.MaxBytes, d.TTL
	}
	a.outf("directory   %s\n", cfg.Dir)
	if cfg.ConfigFile != "" {
		a.outf("config      %s\n", cfg.ConfigFile)
	}
	a.printEntries(actions, objects, diskBytes)
	a.printSize(diskBytes, maxBytes)

	// What the budget thinks is not what the disk has. Compression, other users
	// of the volume, and snapshots all move the two apart, and the number an
	// operator needs to judge whether a ceiling is set sensibly is the second
	// one: a cache can report itself full with most of the volume idle. It is
	// this machine's volume, so it belongs to this report and to no other.
	if total, avail, err := blob.VolumeUsage(cfg.Dir); err == nil && total > 0 {
		used := total - avail
		a.outf("volume      %s used of %s (%.1f%%, %s free)\n",
			config.FormatBytes(int64(used)), config.FormatBytes(int64(total)),
			100*float64(used)/float64(total), config.FormatBytes(int64(avail)))
	}

	a.printTTL(ttl)
	if d != nil && d.OldestAge != "" {
		a.printAge(d.OldestAge, d.NewestAge)
	}
	if cfg.RemoteEnabled() {
		a.outf("remote      s3://%s/%s\n", cfg.S3Bucket, cfg.S3Prefix)
	} else {
		a.outf("remote      disabled\n")
	}
	if d == nil {
		// Not the end of the report any more. The counters outlive the process
		// that produced them, so a cache nothing is currently using still has a
		// history worth reading — and this is the state anyone asking about a
		// quiet machine finds it in.
		a.outf("daemon      not running\n")
		a.printLifetime(life, lifeSince)
		return
	}
	a.outf("daemon      pid %d, up %s\n", d.PID, d.Uptime)
	a.printCounters(d, cfg.RemoteEnabled())
	a.printLifetime(life, lifeSince)
}

// printStatusFrom renders a report read from another daemon over its monitoring
// endpoint.
//
// Everything here comes off the wire, and the lines the local report opens with
// are absent rather than filled in from this machine: the directory, the
// configuration file, and the volume describe wherever this command happens to
// be run, which has nothing to do with the host being asked. Attributing them to
// it would be a lie with a plausible shape, which is the worst kind to print. The
// endpoint leads the report so that there is no reading of it that leaves which
// cache it describes in doubt.
func (a *app) printStatusFrom(endpoint string, r *daemon.StatusResponse) {
	a.outf("endpoint    %s\n", endpoint)
	if r.Version != "" {
		a.outf("version     %s\n", r.Version)
	}
	a.printEntries(r.Actions, r.Objects, r.DiskBytes)
	a.printSize(r.DiskBytes, r.MaxBytes)
	a.printTTL(r.TTL)
	if r.OldestAge != "" {
		a.printAge(r.OldestAge, r.NewestAge)
	}
	// The bucket is the operator's business and the endpoint does not disclose
	// it. Whether uploads mean anything is the part a reader needs, and that is
	// what this says.
	if r.RemoteEnabled {
		a.outf("remote      enabled\n")
	} else {
		a.outf("remote      disabled\n")
	}
	a.outf("daemon      pid %d, up %s\n", r.PID, r.Uptime)
	a.printCounters(r, r.RemoteEnabled)
	a.printLifetime(r.Lifetime, r.LifetimeSince)
}

// printEntries reports what the cache holds.
//
// The derived figures are the point, not just the raw counts: actions per object
// is the dedup ratio that refcounting outputs exists to produce.
func (a *app) printEntries(actions, objects, diskBytes int64) {
	if objects > 0 {
		a.outf("entries     %d actions, %d objects (%.2fx dedup, %s avg)\n",
			actions, objects, float64(actions)/float64(objects),
			config.FormatBytes(diskBytes/objects))
		return
	}
	a.outf("entries     %d actions, %d objects\n", actions, objects)
}

// printSize reports the bytes held against the ceiling, since the share of the
// budget in use is what says whether eviction is about to start biting.
func (a *app) printSize(diskBytes, maxBytes int64) {
	if maxBytes <= 0 {
		a.outf("size        %s (no limit)\n", config.FormatBytes(diskBytes))
		return
	}
	headroom := maxBytes - diskBytes
	if headroom < 0 {
		headroom = 0
	}
	a.outf("size        %s of %s (%.1f%%, %s free)\n",
		config.FormatBytes(diskBytes), config.FormatBytes(maxBytes),
		100*float64(diskBytes)/float64(maxBytes), config.FormatBytes(headroom))
}

// printTTL reports the age limit, spelling out a disabled one rather than
// printing a zero duration nobody reads as "off".
func (a *app) printTTL(ttl string) {
	if ttl != "" && ttl != "0s" {
		a.outf("ttl         %s\n", ttl)
		return
	}
	a.outf("ttl         none\n")
}

// printAge reports the age span, which says whether the TTL is doing anything:
// if the oldest entry is younger than the TTL, only the size ceiling is
// evicting.
func (a *app) printAge(oldest, newest string) {
	a.outf("age         oldest %s, newest %s\n", oldest, newest)
}

// printCounters reports one daemon's own tally.
//
// A hit rate is the one number worth reading first, and it is the one the caller
// would otherwise have to work out from three separate counters. Repairs are
// called out because a nonzero count means bodies went missing under the index,
// which is worth noticing rather than burying.
//
// It takes the whole report rather than the counters alone because the upload
// backlog is not one of them: it is a level, and it belongs beside the upload
// counters because it is what those counters cannot say — how close the next
// burst is to being dropped.
func (a *app) printCounters(d *daemon.StatusResponse, remoteEnabled bool) {
	m := d.Metrics
	lookups := m.GetLocalHit + m.GetRemoteHit + m.GetMiss
	if lookups > 0 {
		a.outf("hit rate    %.1f%% of %d lookups\n",
			100*float64(m.GetLocalHit+m.GetRemoteHit)/float64(lookups), lookups)
	}
	a.outf("hits        %d local, %d remote\n", m.GetLocalHit, m.GetRemoteHit)
	a.outf("misses      %d\n", m.GetMiss)
	a.outf("puts        %d\n", m.Put)
	if m.GetRepair > 0 {
		a.outf("repairs     %d (index entries dropped for missing bodies)\n", m.GetRepair)
	}
	if m.Compactions > 0 {
		a.outf("compactions %d (index reclaimed after pruning)\n", m.Compactions)
	}
	if remoteEnabled {
		a.outf("uploads     %d ok, %d failed, %d dropped, %d skipped\n",
			m.UploadOK, m.UploadFail, m.UploadDrop, m.UploadSkip)
		if d.UploadQueueCapacity > 0 {
			a.outf("upload q    %d of %d queued\n", d.UploadQueueDepth, d.UploadQueueCapacity)
		}
		a.printConns(d.Remote)
	}
}

// printConns reports how much of the shared tier's traffic reused a connection.
//
// The percentage is the whole line: a transport keeps a bounded number of idle
// connections per host, so a process working harder than that bound spends its
// time on handshakes it need not repeat, and this is the number that says whether
// it is. What each of those handshakes costs is the duration histogram on the
// metrics endpoint; this is the one figure worth having without a scraper.
func (a *app) printConns(rem *remote.StatsSnapshot) {
	if rem == nil {
		return
	}
	total := rem.ConnsReused + rem.ConnsNew
	if total == 0 {
		return
	}
	a.outf("conns       %d requests, %.1f%% on a reused connection\n",
		total, 100*float64(rem.ConnsReused)/float64(total))
}

// printLifetime reports the persisted counters.
//
// Everything above it describes one process. A daemon exits after its idle
// timeout and a plugin invocation lasts one build, so those numbers answer "what
// has this process seen", which for a cache that has been quiet for half an hour
// is nothing — and reading that as the cache's hit rate is exactly the mistake
// this line exists to prevent.
func (a *app) printLifetime(life cache.MetricsSnapshot, since int64) {
	if life.Lookups() == 0 {
		return
	}
	rate, _ := life.HitRate()
	a.outf("lifetime    %.1f%% of %d lookups", 100*rate, life.Lookups())
	if since > 0 {
		a.outf(" since %s", time.Unix(0, since).UTC().Format("2006-01-02 15:04 UTC"))
	}
	a.outf(" (every process; see `plaid-cache stats`)\n")
}

// runGC forces an eviction pass.
func (a *app) runGC(ctx context.Context) int {
	var limits limitFlags
	if _, err := a.parseFlags("gc", limits.register, a.args[1:]); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	params, err := limits.gcParams()
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}

	// A running daemon reads its configuration once at startup, so an override
	// has to travel with the request; otherwise it would appear to be ignored.
	if conn, err := dialExistingGC(cfg, buildVersion(), params); err == nil {
		defer func() { _ = conn.Close() }()
		var resp daemon.GCResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
		if resp.Err != "" {
			a.errf("plaid-cache: %s\n", resp.Err)
			return exitError
		}
		a.printGC(resp.ActionsPruned, resp.ObjectsPruned, resp.BytesFreed, resp.Elapsed)
		a.printMeasured(resp.Measured, resp.Corrected, resp.RecordedBefore, resp.RecordedAfter)
		if params != nil {
			a.outf("applied      max-bytes %s, ttl %s (this pass only)\n",
				config.FormatBytes(resp.AppliedMaxBytes), resp.AppliedTTL)
		}
		return exitOK
	}

	// No daemon: this process owns the index, so the flags simply override the
	// configuration it is about to use.
	if err := limits.applyTo(cfg); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	st, err := openStores(ctx, cfg)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	c := cache.New(cache.Params{
		Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem,
		Logf: a.logger(cfg, config.LogInfo),
	})
	defer func() { _ = c.Close() }()

	res, rec, err := c.EvictNow(ctx, cfg.MaxBytes, cfg.TTL)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	if err := st.idx.Compact(); err != nil {
		a.errf("plaid-cache: compact: %v\n", err)
	}
	a.printGC(res.ActionsPruned, res.ObjectsPruned, res.BytesFreed, res.Elapsed.String())
	a.printMeasured(rec.Objects, rec.Corrected, rec.Before, rec.After)
	return exitOK
}

// printMeasured reports what re-measuring the bodies changed.
//
// Without it a pass that pruned nothing because the recorded size was wrong looks
// identical to a pass that had nothing to do, and the correction — which is the
// reason it pruned nothing — goes unmentioned.
func (a *app) printMeasured(objects, corrected, before, after int64) {
	if corrected == 0 {
		return
	}
	a.outf("measured     %d of %d objects re-measured, recorded size %s -> %s\n",
		corrected, objects, config.FormatBytes(before), config.FormatBytes(after))
}

// printGC renders an eviction result.
func (a *app) printGC(actions, objects, bytes int64, elapsed string) {
	a.outf("pruned %d actions, %d objects, freed %s in %s\n",
		actions, objects, config.FormatBytes(bytes), elapsed)
}

// runClean removes the entire local cache.
func (a *app) runClean(ctx context.Context) int {
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}

	// Stop the daemon first: it holds the index open, and deleting a Pebble
	// directory out from under a live process leaves it writing into unlinked
	// files rather than failing loudly.
	if conn, err := dialExisting(cfg, buildVersion(), daemon.OpShutdown); err == nil {
		_ = conn.Close()
		if err := waitGone(cfg.SocketPath()); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
	}

	if err := os.RemoveAll(cfg.Dir); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	a.outf("removed %s\n", cfg.Dir)
	return exitOK
}

// dialExistingGC is dialExisting for OpGC, carrying the per-pass overrides in
// the hello so they reach a daemon that already read its own configuration.
func dialExistingGC(cfg *config.Config, version string, params *daemon.GCParams) (*daemon.Conn, error) {
	return dialExistingWith(cfg, daemon.Hello{Version: version, Op: daemon.OpGC, GC: params})
}

// dialExisting connects to a daemon that is already running, without
// spawning one. Status, gc, and clean must not start a daemon as a side
// effect of asking a question.
func dialExisting(cfg *config.Config, version string, op daemon.Op) (*daemon.Conn, error) {
	return dialExistingWith(cfg, daemon.Hello{Version: version, Op: op})
}

// dialExistingWith performs the control exchange for an already-composed hello.
func dialExistingWith(cfg *config.Config, hello daemon.Hello) (*daemon.Conn, error) {
	conn, err := daemon.Dial(cfg)
	if err != nil {
		return nil, fmt.Errorf("dialExisting: %w", err)
	}
	if err := writeLine(conn, hello); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dialExisting: %w", err)
	}
	var resp daemon.HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dialExisting: %w", err)
	}
	if !resp.OK {
		_ = conn.Close()
		return nil, fmt.Errorf("dialExisting: daemon refused: %s", resp.Err)
	}
	return conn, nil
}

// writeLine sends one newline-delimited JSON value.
func writeLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// waitGone blocks until the socket disappears, so that a clean does not race
// the daemon's own removal of it.
func waitGone(path string) error {
	return daemon.WaitSocketGone(context.Background(), path)
}

// runAdopt imports a go-cache-plugin stage into this cache.
//
// It takes the index lock directly rather than going through a daemon, because
// adoption is a migration step that runs before builds start. A daemon holding
// the lock is reported plainly instead of being worked around: silently adopting
// into a second index would leave two disagreeing views of the same bodies.
func (a *app) runAdopt(ctx context.Context) int {
	var dryRun bool
	fs, err := a.parseFlagsAllowingArgs("adopt", func(f *flag.FlagSet) {
		f.BoolVar(&dryRun, "dry-run", false, "report what would be adopted without writing anything")
	}, a.args[1:])
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	if fs.NArg() != 1 {
		a.errf("plaid-cache: adopt needs exactly one directory, the go-cache-plugin stage root\n")
		return exitUsage
	}
	legacyDir := fs.Arg(0)

	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}

	// Ask a running daemon to do the import rather than requiring it to be
	// stopped. Exactly one process may hold the index, and on the machine that
	// actually needs a stage migrated that process is busy serving builds — an
	// env switching to plaid-cache starts using the daemon at the same moment its
	// old stage becomes dead weight. Requiring a stop meant the migration lost a
	// race it could not win on a busy machine.
	params := &daemon.AdoptParams{Dir: legacyDir, DryRun: dryRun}
	if conn, derr := dialExistingAdopt(cfg, buildVersion(), params); derr == nil {
		defer func() { _ = conn.Close() }()
		var resp daemon.AdoptResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
		if resp.Err != "" {
			a.errf("plaid-cache: %s\n", resp.Err)
			return exitError
		}
		a.printAdopt(adoptResultFrom(resp), dryRun)
		return exitOK
	}

	// No daemon: this process can hold the index itself.
	st, err := openStores(ctx, cfg)
	if err != nil {
		if errors.Is(err, index.ErrLocked) {
			// A daemon took the index between the dial above and here, or one is
			// starting up. Retrying is the caller's business; saying which
			// process to blame is ours.
			a.errf("plaid-cache: another process took the index mid-adopt; try again\n")
			return exitError
		}
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	res, err := adopt.Run(ctx, adopt.Params{
		LegacyDir: legacyDir,
		Index:     st.idx,
		Blobs:     st.blobs,
		DryRun:    dryRun,
		Logf:      a.logger(cfg, config.LogInfo),
	})
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitError
	}
	a.printAdopt(res, dryRun)
	return exitOK
}

// printAdopt renders an import, from either path.
func (a *app) printAdopt(res adopt.Result, dryRun bool) {
	prefix := ""
	if dryRun {
		prefix = "would adopt: "
	}
	a.outf("%s%s\n", prefix, res)
}

// adoptResultFrom rebuilds a Result from a daemon's response, so that both paths
// print through one formatter rather than two that can drift.
func adoptResultFrom(r daemon.AdoptResponse) adopt.Result {
	// An unparseable duration is not worth failing an import that succeeded; it
	// costs the elapsed time in the output and nothing else.
	d, _ := time.ParseDuration(r.Elapsed)
	return adopt.Result{
		Records:        r.Records,
		Adopted:        r.Adopted,
		AlreadyPresent: r.AlreadyPresent,
		MissingBody:    r.MissingBody,
		SizeMismatch:   r.SizeMismatch,
		Malformed:      r.Malformed,
		Linked:         r.Linked,
		Copied:         r.Copied,
		Bytes:          r.Bytes,
		Elapsed:        d,
	}
}

func dialExistingAdopt(cfg *config.Config, version string, params *daemon.AdoptParams) (*daemon.Conn, error) {
	return dialExistingWith(cfg, daemon.Hello{Version: version, Op: daemon.OpAdopt, Adopt: params})
}
