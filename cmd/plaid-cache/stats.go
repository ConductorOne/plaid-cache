// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/index"
)

// runStats reports the persisted activity history.
func (a *app) runStats(ctx context.Context) int {
	var since string
	var asJSON bool
	if _, err := a.parseFlags("stats", func(f *flag.FlagSet) {
		f.StringVar(&since, "since", "24h", "how far back to report, as a Go duration")
		f.BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
	}, a.args[1:]); err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	window, err := time.ParseDuration(since)
	if err != nil {
		a.errf("plaid-cache: -since: %q is not a duration (want e.g. 24h, 7d is 168h)\n", since)
		return exitUsage
	}
	if window < 0 {
		a.errf("plaid-cache: -since: %v is negative\n", window)
		return exitUsage
	}

	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	resp, ok := a.collectStats(ctx, cfg, window)
	if !ok {
		return exitError
	}
	if asJSON {
		enc := json.NewEncoder(a.stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(resp); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
		return exitOK
	}
	a.printStats(cfg, resp, window)
	return exitOK
}

// collectStats reads the history from a running daemon, or from the index when
// none is running.
//
// The daemon has to be asked when it exists, because it holds the index lock —
// and it flushes what it has counted before answering, so a report taken right
// after a build includes that build.
func (a *app) collectStats(ctx context.Context, cfg *config.Config, window time.Duration) (daemon.StatsResponse, bool) {
	params := &daemon.StatsParams{Since: window.String()}
	if conn, err := dialExistingStats(cfg, buildVersion(), params); err == nil {
		defer func() { _ = conn.Close() }()
		var resp daemon.StatsResponse
		if err := conn.ReadJSONLine(&resp); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return resp, false
		}
		if resp.Err != "" {
			a.errf("plaid-cache: %s\n", resp.Err)
			return resp, false
		}
		return resp, true
	}

	st, err := openStores(ctx, cfg)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return daemon.StatsResponse{}, false
	}
	defer st.close()

	total, since, err := st.idx.TotalActivity()
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return daemon.StatsResponse{}, false
	}
	cutoff := time.Now().Add(-window)
	buckets, err := st.idx.ActivitySince(cutoff)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return daemon.StatsResponse{}, false
	}
	var windowed cache.MetricsSnapshot
	for _, b := range buckets {
		windowed = windowed.Add(b.Activity)
	}
	return daemon.StatsResponse{
		Lifetime:      total,
		LifetimeSince: since,
		Window:        windowed,
		WindowSince:   cutoff.UTC().Truncate(time.Hour).Unix(),
		Buckets:       buckets,
	}, true
}

// printStats renders the report.
func (a *app) printStats(cfg *config.Config, r daemon.StatsResponse, window time.Duration) {
	a.outf("window      last %s, %s\n", window, hoursWithActivity(r.Buckets))
	a.printActivity(cfg, r.Window)

	if r.Lifetime.Lookups() > 0 {
		rate, _ := r.Lifetime.HitRate()
		a.outf("lifetime    %.1f%% of %d lookups", 100*rate, r.Lifetime.Lookups())
		if r.LifetimeSince > 0 {
			a.outf(" since %s", time.Unix(0, r.LifetimeSince).UTC().Format("2006-01-02 15:04 UTC"))
		}
		a.outf("\n")
	}
	if len(r.Buckets) == 0 {
		return
	}

	// The per-hour rows are the point of keeping history: one number for a
	// fortnight cannot tell you whether the cache is working now or worked well
	// in the past, and a rate that is falling looks identical to a healthy one
	// in a total.
	a.outf("\n%-17s %9s %6s %8s %7s %8s %6s\n",
		"hour (UTC)", "lookups", "hit%", "local", "remote", "misses", "puts")
	for _, b := range r.Buckets {
		act := b.Activity
		rate, ok := act.HitRate()
		pct := "     -"
		if ok {
			pct = fmt.Sprintf("%5.1f%%", 100*rate)
		}
		a.outf("%-17s %9d %6s %8d %7d %8d %6d\n",
			time.Unix(b.Hour, 0).UTC().Format("2006-01-02 15:04"),
			act.Lookups(), pct, act.GetLocalHit, act.GetRemoteHit, act.GetMiss, act.Put)
	}
}

// printActivity renders one counter set.
func (a *app) printActivity(cfg *config.Config, act cache.MetricsSnapshot) {
	if rate, ok := act.HitRate(); ok {
		a.outf("hit rate    %.1f%% of %d lookups\n", 100*rate, act.Lookups())
	} else {
		a.outf("hit rate    no lookups in this window\n")
	}
	a.outf("hits        %d local, %d remote\n", act.GetLocalHit, act.GetRemoteHit)
	a.outf("misses      %d\n", act.GetMiss)
	a.outf("puts        %d\n", act.Put)
	if act.GetRepair > 0 {
		a.outf("repairs     %d (index entries dropped for missing bodies)\n", act.GetRepair)
	}
	if cfg.RemoteEnabled() {
		a.outf("uploads     %d ok, %d failed, %d dropped, %d skipped\n",
			act.UploadOK, act.UploadFail, act.UploadDrop, act.UploadSkip)
	}
}

// hoursWithActivity describes how much of the window has data, which is what
// says whether a low total means a quiet cache or a short history.
func hoursWithActivity(buckets []index.ActivityBucket) string {
	if len(buckets) == 0 {
		return "no recorded activity"
	}
	if len(buckets) == 1 {
		return "1 hour with activity"
	}
	return fmt.Sprintf("%d hours with activity", len(buckets))
}

func dialExistingStats(cfg *config.Config, version string, params *daemon.StatsParams) (*daemon.Conn, error) {
	return dialExistingWith(cfg, daemon.Hello{Version: version, Op: daemon.OpStats, Stats: params})
}
