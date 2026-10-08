// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/index"
)

// runStats reports the persisted activity history.
func (a *app) runStats(ctx context.Context) int {
	var since, from string
	var asJSON bool
	if _, err := a.parseFlags("stats", func(f *flag.FlagSet) {
		f.StringVar(&since, "since", "24h", "how far back to report, as a Go duration")
		f.BoolVar(&asJSON, "json", false, "emit JSON instead of a table")
		f.StringVar(&from, "from", "", "read history from another daemon's monitoring endpoint, e.g. localhost:9095 (default: this machine's own cache)")
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
	if from != "" {
		// Local configuration cannot describe, or prevent reading, another daemon.
		return a.runStatsFrom(ctx, from, window, asJSON)
	}

	cfg, ok := a.loadConfig()
	if !ok {
		return exitError
	}
	resp, ok := a.collectStats(ctx, cfg, window)
	if !ok {
		return exitError
	}
	return a.renderStats(resp, window, cfg.RemoteEnabled(), asJSON, "")
}

// maxStatsBody leaves ample room for two weeks of hourly counters, including
// int64-sized values, without letting a wrong endpoint stream unbounded data.
const maxStatsBody = 1 << 20

// runStatsFrom refuses failed or incomplete reports instead of inventing a cache
// with no activity. It deliberately never opens this machine's configuration.
func (a *app) runStatsFrom(ctx context.Context, addr string, window time.Duration, asJSON bool) int {
	endpoint, err := monitoringEndpoint(addr, bazel.StatsPath)
	if err != nil {
		a.errf("plaid-cache: %v\n", err)
		return exitUsage
	}
	endpoint += "?" + url.Values{"since": {window.String()}}.Encode()
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
			a.errf("plaid-cache: that daemon may be serving Bazel without -bazel-monitoring\n")
		}
		return exitError
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxStatsBody+1))
	if err != nil {
		a.errf("plaid-cache: %s: %v\n", endpoint, err)
		return exitError
	}
	if len(body) > maxStatsBody {
		a.errf("plaid-cache: %s: stats report exceeds %d bytes\n", endpoint, maxStatsBody)
		return exitError
	}
	// Even an idle cache reports both counter sets and the selected cutoff.
	// Pointer fields distinguish legitimate zeros from unrelated or partial JSON.
	var fields struct {
		Lifetime      *cache.MetricsSnapshot `json:"lifetime"`
		LifetimeSince *int64                 `json:"lifetime_since"`
		Window        *cache.MetricsSnapshot `json:"window"`
		WindowSince   *int64                 `json:"window_since"`
		Buckets       json.RawMessage        `json:"buckets"`
		Err           string                 `json:"err"`
	}
	if err := json.Unmarshal(body, &fields); err != nil {
		a.errf("plaid-cache: %s: not a stats report: %v\n", endpoint, err)
		return exitError
	}
	if fields.Err != "" {
		a.errf("plaid-cache: %s: %s\n", endpoint, fields.Err)
		return exitError
	}
	if fields.Lifetime == nil || fields.LifetimeSince == nil ||
		fields.Window == nil || fields.WindowSince == nil || len(fields.Buckets) == 0 {
		a.errf("plaid-cache: %s: not a stats report: missing history fields\n", endpoint)
		return exitError
	}
	r := daemon.StatsResponse{
		Lifetime: *fields.Lifetime, LifetimeSince: *fields.LifetimeSince,
		Window: *fields.Window, WindowSince: *fields.WindowSince,
	}
	if err := json.Unmarshal(fields.Buckets, &r.Buckets); err != nil {
		a.errf("plaid-cache: %s: not a stats report: %v\n", endpoint, err)
		return exitError
	}
	// Historical uploads remain meaningful even if the daemon's S3 tier is now
	// disabled. Never decide whether to show them using the caller's configuration.
	return a.renderStats(r, window, true, asJSON, endpoint)
}

// renderStats keeps local JSON unchanged; remote output also names its source.
func (a *app) renderStats(r daemon.StatsResponse, window time.Duration, uploads, asJSON bool, endpoint string) int {
	if asJSON {
		var report any = r
		if endpoint != "" {
			report = struct {
				Endpoint string `json:"endpoint"`
				daemon.StatsResponse
			}{Endpoint: endpoint, StatsResponse: r}
		}
		enc := json.NewEncoder(a.stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			a.errf("plaid-cache: %v\n", err)
			return exitError
		}
		return exitOK
	}
	if endpoint != "" {
		a.outf("endpoint    %s\n", endpoint)
	}
	a.printStats(uploads, r, window)
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
func (a *app) printStats(uploads bool, r daemon.StatsResponse, window time.Duration) {
	a.outf("window      last %s, %s\n", window, hoursWithActivity(r.Buckets))
	a.printActivity(uploads, r.Window)

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
func (a *app) printActivity(uploads bool, act cache.MetricsSnapshot) {
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
	if uploads {
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
