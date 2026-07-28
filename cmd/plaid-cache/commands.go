// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

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
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
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
		fmt.Fprintf(a.stderr, "plaid-cache: "+format+"\n", args...)
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
	if _, err := a.parseFlags("serve", limits.register, a.args[1:]); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitUsage
	}
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	if err := limits.applyTo(cfg); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitUsage
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
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	c := cache.New(cache.Params{
		Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem, Logf: logf,
	})
	defer c.Close()

	ln, err := daemon.Listen(cfg)
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}

	srv := daemon.NewServer(daemon.ServerParams{
		Config: cfg, Cache: c, Index: st.idx, Logf: logf, Version: buildVersion(),
	})
	logf("serving on %s (pid %d)", cfg.SocketPath(), os.Getpid())

	if err := srv.Serve(ctx, ln); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
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
	defer c.Close()

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
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	return exitOK
}

// runStatus reports the cache's contents.
func (a *app) runStatus(ctx context.Context) int {
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}

	// Ask a running daemon first: it holds the index lock and has live
	// counters. Only if none is running do we open the index ourselves.
	if conn, err := dialExisting(cfg, buildVersion(), daemon.OpStatus); err == nil {
		defer conn.Close()
		var resp daemon.StatusResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
			return exitError
		}
		a.printStatus(cfg, resp.Actions, resp.Objects, resp.DiskBytes, &resp)
		return exitOK
	}

	st, err := openStores(ctx, cfg)
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	s, err := st.idx.Stats()
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	a.printStatus(cfg, s.Actions, s.Objects, s.DiskBytes, nil)
	return exitOK
}

// printStatus renders the status report.
//
// The eviction limits come from the daemon when one is running, not from this
// process's environment. The daemon read its configuration when it started,
// and that is what actually governs the cache; printing the caller's own
// environment would show limits that nothing is enforcing.
func (a *app) printStatus(cfg *config.Config, actions, objects, diskBytes int64, d *daemon.StatusResponse) {
	maxBytes, ttl := cfg.MaxBytes, cfg.TTL.String()
	if d != nil {
		maxBytes, ttl = d.MaxBytes, d.TTL
	}
	fmt.Fprintf(a.stdout, "directory   %s\n", cfg.Dir)

	// Report the derived figures, not just the raw counts. Actions per object is
	// the dedup ratio the output refcounting exists to produce, and the share of
	// the budget in use is what says whether eviction is about to start biting.
	if objects > 0 {
		fmt.Fprintf(a.stdout, "entries     %d actions, %d objects (%.2fx dedup, %s avg)\n",
			actions, objects, float64(actions)/float64(objects),
			config.FormatBytes(diskBytes/objects))
	} else {
		fmt.Fprintf(a.stdout, "entries     %d actions, %d objects\n", actions, objects)
	}

	if maxBytes > 0 {
		pct := 100 * float64(diskBytes) / float64(maxBytes)
		headroom := maxBytes - diskBytes
		if headroom < 0 {
			headroom = 0
		}
		fmt.Fprintf(a.stdout, "size        %s of %s (%.1f%%, %s free)\n",
			config.FormatBytes(diskBytes), config.FormatBytes(maxBytes), pct,
			config.FormatBytes(headroom))
	} else {
		fmt.Fprintf(a.stdout, "size        %s (no limit)\n", config.FormatBytes(diskBytes))
	}

	if ttl != "" && ttl != "0s" {
		fmt.Fprintf(a.stdout, "ttl         %s\n", ttl)
	} else {
		fmt.Fprintf(a.stdout, "ttl         none\n")
	}

	// The age span says whether the TTL is doing anything: if the oldest entry
	// is younger than the TTL, only the size ceiling is evicting.
	if d != nil && d.OldestAge != "" {
		fmt.Fprintf(a.stdout, "age         oldest %s, newest %s\n", d.OldestAge, d.NewestAge)
	}
	if cfg.RemoteEnabled() {
		fmt.Fprintf(a.stdout, "remote      s3://%s/%s\n", cfg.S3Bucket, cfg.S3Prefix)
	} else {
		fmt.Fprintf(a.stdout, "remote      disabled\n")
	}
	if d == nil {
		fmt.Fprintf(a.stdout, "daemon      not running\n")
		return
	}
	fmt.Fprintf(a.stdout, "daemon      pid %d, up %s\n", d.PID, d.Uptime)
	m := d.Metrics

	// A hit rate is the one number worth reading first, and it is the one the
	// caller would otherwise have to work out from three separate counters.
	// Repairs are called out because a nonzero count means bodies went missing
	// under the index, which is worth noticing rather than burying.
	lookups := m.GetLocalHit + m.GetRemoteHit + m.GetMiss
	if lookups > 0 {
		fmt.Fprintf(a.stdout, "hit rate    %.1f%% of %d lookups\n",
			100*float64(m.GetLocalHit+m.GetRemoteHit)/float64(lookups), lookups)
	}
	fmt.Fprintf(a.stdout, "hits        %d local, %d remote\n", m.GetLocalHit, m.GetRemoteHit)
	fmt.Fprintf(a.stdout, "misses      %d\n", m.GetMiss)
	fmt.Fprintf(a.stdout, "puts        %d\n", m.Put)
	if m.GetRepair > 0 {
		fmt.Fprintf(a.stdout, "repairs     %d (index entries dropped for missing bodies)\n", m.GetRepair)
	}
	if m.Compactions > 0 {
		fmt.Fprintf(a.stdout, "compactions %d (index reclaimed after pruning)\n", m.Compactions)
	}
	if cfg.RemoteEnabled() {
		fmt.Fprintf(a.stdout, "uploads     %d ok, %d failed, %d dropped, %d skipped\n",
			m.UploadOK, m.UploadFail, m.UploadDrop, m.UploadSkip)
	}
}

// runGC forces an eviction pass.
func (a *app) runGC(ctx context.Context) int {
	var limits limitFlags
	if _, err := a.parseFlags("gc", limits.register, a.args[1:]); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitUsage
	}
	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	params, err := limits.gcParams()
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitUsage
	}

	// A running daemon reads its configuration once at startup, so an override
	// has to travel with the request; otherwise it would appear to be ignored.
	if conn, err := dialExistingGC(cfg, buildVersion(), params); err == nil {
		defer conn.Close()
		var resp daemon.GCResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
			return exitError
		}
		if resp.Err != "" {
			fmt.Fprintf(a.stderr, "plaid-cache: %s\n", resp.Err)
			return exitError
		}
		a.printGC(resp.ActionsPruned, resp.ObjectsPruned, resp.BytesFreed, resp.Elapsed)
		if params != nil {
			fmt.Fprintf(a.stdout, "applied      max-bytes %s, ttl %s (this pass only)\n",
				config.FormatBytes(resp.AppliedMaxBytes), resp.AppliedTTL)
		}
		return exitOK
	}

	// No daemon: this process owns the index, so the flags simply override the
	// configuration it is about to use.
	if err := limits.applyTo(cfg); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitUsage
	}
	st, err := openStores(ctx, cfg)
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	defer st.close()

	c := cache.New(cache.Params{
		Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem,
		Logf: a.logger(cfg, config.LogInfo),
	})
	defer c.Close()

	res, err := c.Evict(ctx)
	if err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	if err := st.idx.Compact(); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: compact: %v\n", err)
	}
	a.printGC(res.ActionsPruned, res.ObjectsPruned, res.BytesFreed, res.Elapsed.String())
	return exitOK
}

// printGC renders an eviction result.
func (a *app) printGC(actions, objects, bytes int64, elapsed string) {
	fmt.Fprintf(a.stdout, "pruned %d actions, %d objects, freed %s in %s\n",
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
		conn.Close()
		if err := waitGone(cfg.SocketPath()); err != nil {
			fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
			return exitError
		}
	}

	if err := os.RemoveAll(cfg.Dir); err != nil {
		fmt.Fprintf(a.stderr, "plaid-cache: %v\n", err)
		return exitError
	}
	fmt.Fprintf(a.stdout, "removed %s\n", cfg.Dir)
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
		conn.Close()
		return nil, fmt.Errorf("dialExisting: %w", err)
	}
	var resp daemon.HelloResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		conn.Close()
		return nil, fmt.Errorf("dialExisting: %w", err)
	}
	if !resp.OK {
		conn.Close()
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
