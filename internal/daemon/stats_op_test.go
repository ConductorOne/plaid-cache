// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"testing"

	"time"

	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/index"
)

// seedActivity records counters in the index before any daemon exists.
func seedActivity(t *testing.T, cfg *config.Config, a index.Activity) {
	t.Helper()
	seedActivityAt(t, cfg, a, 0)
}

// seedActivityAt records counters as of hoursAgo.
func seedActivityAt(t *testing.T, cfg *config.Config, a index.Activity, hoursAgo int) {
	t.Helper()
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	if err := ix.RecordActivity(a, time.Now().Add(time.Duration(hoursAgo)*time.Hour)); err != nil {
		t.Fatalf("RecordActivity: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("index.Close: %v", err)
	}
}

// requestStats performs the control exchange and returns the response.
func requestStats(t *testing.T, cfg *config.Config, p *StatsParams) StatsResponse {
	t.Helper()
	conn := dialServer(t, cfg)
	if err := writeJSONLine(conn, Hello{Version: testVersion, Op: OpStats, Stats: p}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var hello HelloResponse
	if err := conn.ReadJSONLine(&hello); err != nil {
		t.Fatalf("hello response: %v", err)
	}
	if !hello.OK {
		t.Fatalf("daemon refused stats: %s", hello.Err)
	}
	var resp StatsResponse
	if err := conn.ReadJSONLine(&resp); err != nil {
		t.Fatalf("stats response: %v", err)
	}
	return resp
}

// TestStatsReportsPersistedHistory pins that the daemon serves counters recorded
// before it existed, which is the point of persisting them: the process being
// asked is rarely the process that did the work.
func TestStatsReportsPersistedHistory(t *testing.T) {
	cfg := newTestConfig(t)
	seedActivity(t, cfg, index.Activity{GetLocalHit: 40, GetMiss: 10})

	startServer(t, cfg)
	resp := requestStats(t, cfg, nil)
	if resp.Err != "" {
		t.Fatalf("stats: %s", resp.Err)
	}
	if resp.Lifetime.GetLocalHit != 40 || resp.Lifetime.GetMiss != 10 {
		t.Fatalf("lifetime = %+v, want the counts recorded before this daemon started", resp.Lifetime)
	}
	if rate, ok := resp.Lifetime.HitRate(); !ok || rate != 0.8 {
		t.Fatalf("hit rate = %v, %v; want 0.8, true", rate, ok)
	}
	if len(resp.Buckets) == 0 {
		t.Fatal("no per-hour history, so no rate over a window is answerable")
	}
}

// TestStatsFlushesBeforeAnswering pins that a report taken right after a build
// includes that build. The counters are otherwise written on a timer, and
// "wait a minute and ask again" is not an answer.
func TestStatsFlushesBeforeAnswering(t *testing.T) {
	cfg := newTestConfig(t)
	ts := startServer(t, cfg)

	// Real work, and no flush: the periodic one is a minute away and this test
	// is not going to wait for it.
	putCached(t, ts.Server, testActionID(21), testOutputID(21), []byte("body"))
	if _, err := ts.cache.Get(context.Background(), testActionID(21)); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := ts.cache.Get(context.Background(), testActionID(99)); err != nil {
		t.Fatalf("Get of an absent action: %v", err)
	}

	resp := requestStats(t, cfg, nil)
	if resp.Err != "" {
		t.Fatalf("stats: %s", resp.Err)
	}
	if resp.Lifetime.Lookups() != 2 {
		t.Fatalf("lifetime lookups = %d, want the 2 that were never flushed", resp.Lifetime.Lookups())
	}
	if resp.Lifetime.Put != 1 {
		t.Fatalf("lifetime puts = %d, want 1", resp.Lifetime.Put)
	}
}

// TestStatsWindowIsHonoured pins that a window bounds what is summed, so a rate
// asked for one hour is not computed over the whole history.
func TestStatsWindowIsHonoured(t *testing.T) {
	cfg := newTestConfig(t)
	seedActivityAt(t, cfg, index.Activity{GetLocalHit: 100}, -72)
	seedActivityAt(t, cfg, index.Activity{GetLocalHit: 5}, 0)

	startServer(t, cfg)
	resp := requestStats(t, cfg, &StatsParams{Since: "24h"})
	if resp.Err != "" {
		t.Fatalf("stats: %s", resp.Err)
	}
	if resp.Window.GetLocalHit != 5 {
		t.Fatalf("window = %d hits, want only the recent 5", resp.Window.GetLocalHit)
	}
	if resp.Lifetime.GetLocalHit != 105 {
		t.Fatalf("lifetime = %d hits, want all 105", resp.Lifetime.GetLocalHit)
	}
}

// TestStatsRejectsABadWindow pins that a malformed duration is refused rather
// than silently reported over some default span.
func TestStatsRejectsABadWindow(t *testing.T) {
	cfg := newTestConfig(t)
	startServer(t, cfg)

	for _, bad := range []string{"7d", "-1h", "soon"} {
		if resp := requestStats(t, cfg, &StatsParams{Since: bad}); resp.Err == "" {
			t.Fatalf("accepted %q as a window", bad)
		}
	}
}

// TestStatusReportsLifetimeAlongsideTheSession pins that both numbers are
// available, since reading one for the other is the confusion that motivated
// persisting them at all.
func TestStatusReportsLifetimeAlongsideTheSession(t *testing.T) {
	cfg := newTestConfig(t)
	seedActivity(t, cfg, index.Activity{GetLocalHit: 12, GetMiss: 4})

	ts := startServer(t, cfg)
	if _, err := ts.cache.Get(context.Background(), testActionID(31)); err != nil {
		t.Fatalf("Get of an absent action: %v", err)
	}

	conn := dialServer(t, cfg)
	if err := writeJSONLine(conn, Hello{Version: testVersion, Op: OpStatus}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	var hello HelloResponse
	if err := conn.ReadJSONLine(&hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	var st StatusResponse
	if err := conn.ReadJSONLine(&st); err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Metrics.GetMiss != 1 {
		t.Fatalf("session misses = %d, want this daemon's 1", st.Metrics.GetMiss)
	}
	if st.Lifetime.GetMiss != 5 {
		t.Fatalf("lifetime misses = %d, want 4 from before plus this daemon's 1", st.Lifetime.GetMiss)
	}
	if st.LifetimeSince == 0 {
		t.Fatal("no start time for the lifetime figures")
	}
}
