// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
)

// recordStatsHistory gives the real index distinct recent and older activity,
// including uploads made by an earlier process whose S3 tier is no longer on.
func recordStatsHistory(t *testing.T, ix *index.Index) {
	t.Helper()
	hour := time.Now().UTC().Truncate(time.Hour)
	for _, row := range []struct {
		at  time.Time
		act index.Activity
	}{
		{hour.Add(-72 * time.Hour), index.Activity{GetLocalHit: 100, UploadOK: 20}},
		{hour.Add(-2 * time.Hour), index.Activity{GetLocalHit: 5, GetRemoteHit: 2, GetMiss: 3, Put: 4,
			UploadOK: 4, UploadFail: 1, UploadDrop: 2, UploadSkip: 3}},
	} {
		if err := ix.RecordActivity(row.act, row.at); err != nil {
			t.Fatalf("RecordActivity: %v", err)
		}
	}
}

// statsDaemon serves a real daemon on an ephemeral loopback listener, with a
// pending miss that the HTTP report must flush before answering.
func statsDaemon(t *testing.T, monitoring bool) string {
	t.Helper()
	cfg := &config.Config{Dir: t.TempDir(), MaxBytes: 1 << 30, TTL: time.Hour,
		TouchGranularity: time.Hour, UploadConcurrency: 1, DisableEviction: true,
		BazelMonitoring: monitoring}
	st, err := openStores(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openStores: %v", err)
	}
	t.Cleanup(st.close)
	recordStatsHistory(t, st.idx)
	c := cache.New(cache.Params{Config: cfg, Index: st.idx, Blobs: st.blobs, Remote: st.rem})
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("cache.Close: %v", err)
		}
	})
	if _, err := c.Get(context.Background(), ids.ActionID{1}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	s := daemon.NewServer(daemon.ServerParams{Config: cfg, Cache: c, Index: st.idx, Blobs: st.blobs, Version: buildVersion()})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.ServeBazel(context.Background(), ln) }()
	t.Cleanup(func() {
		s.Stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeBazel: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("ServeBazel did not stop")
		}
	})
	return ln.Addr().String()
}

// TestStatsFromHistory exercises persisted and unflushed counters through the
// real HTTP listener and both CLI renderers, without any valid local settings.
func TestStatsFromHistory(t *testing.T) {
	addr := statsDaemon(t, true)
	a, out, errb := newApp(t, "stats", "-from", addr)
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "broken")
	t.Setenv("PLAID_GOCACHE_CONFIG", "/nonexistent/plaid-cache/config")
	if code := a.run(); code != exitOK {
		t.Fatalf("remote table = %d: %s", code, errb)
	}
	for _, want := range []string{
		"endpoint    http://" + addr + "/stats?since=24h0m0s",
		"window      last 24h0m0s, 2 hours with activity",
		"hit rate    63.6% of 11 lookups", "hits        5 local, 2 remote",
		"misses      4", "puts        4", "uploads     4 ok, 1 failed, 2 dropped, 3 skipped",
		"lifetime    96.4% of 111 lookups", "hour (UTC)",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"directory", "config      ", "volume", "bucket"} {
		if strings.Contains(out.String(), unwanted) {
			t.Fatalf("remote output includes local detail %q:\n%s", unwanted, out)
		}
	}
	var cutoff24 int64
	for _, tc := range []struct {
		since   string
		lookups int64
		buckets int
	}{
		{"24h", 11, 2}, {"168h", 111, 3}, {"0", 1, 1},
	} {
		a, out, errb = newApp(t, "stats", "-from", "http://"+addr+"/stats", "-since", tc.since, "-json")
		t.Setenv("PLAID_GOCACHE_MAX_BYTES", "broken")
		if code := a.run(); code != exitOK {
			t.Fatalf("remote JSON %s = %d: %s", tc.since, code, errb)
		}
		var r struct {
			Endpoint string `json:"endpoint"`
			daemon.StatsResponse
		}
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatalf("JSON: %v", err)
		}
		if r.Lifetime.Lookups() != 111 || r.Lifetime.UploadOK != 24 || r.LifetimeSince == 0 ||
			r.Window.Lookups() != tc.lookups || len(r.Buckets) != tc.buckets {
			t.Fatalf("%s history = %+v", tc.since, r)
		}
		var sum index.Activity
		for _, b := range r.Buckets {
			sum = sum.Add(b.Activity)
		}
		if sum != r.Window || !strings.HasPrefix(r.Endpoint, "http://"+addr+"/stats?since=") {
			t.Fatalf("inconsistent window or wrong source: %+v", r)
		}
		if tc.since == "24h" {
			cutoff24 = r.WindowSince
		} else if tc.since == "168h" && r.WindowSince >= cutoff24 {
			t.Fatalf("cutoffs = %d, %d; want the longer window to start earlier", cutoff24, r.WindowSince)
		}
	}
}

// TestStatsLocalHistory keeps the offline local path and its JSON shape intact;
// local-only tables still omit uploads even when older history contains them.
func TestStatsLocalHistory(t *testing.T) {
	a, out, errb := newApp(t, "stats", "-since", "24h")
	cfg, ok := a.loadConfig()
	if !ok {
		t.Fatalf("loadConfig: %s", errb)
	}
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	recordStatsHistory(t, ix)
	if err := ix.Close(); err != nil {
		t.Fatalf("index.Close: %v", err)
	}
	if code := a.run(); code != exitOK {
		t.Fatalf("local stats = %d: %s", code, errb)
	}
	for _, want := range []string{"70.0% of 10 lookups", "97.3% of 110 lookups", "1 hour with activity", "hour (UTC)"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("local report missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out.String(), "endpoint") || strings.Contains(out.String(), "uploads") {
		t.Fatalf("local table changed:\n%s", out)
	}
	a.args = []string{"stats", "-since", "168h", "-json"}
	out.Reset()
	if code := a.run(); code != exitOK {
		t.Fatalf("local JSON = %d: %s", code, errb)
	}
	var r daemon.StatsResponse
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if r.Window.Lookups() != 110 || r.Lifetime.UploadOK != 24 || len(r.Buckets) != 2 || strings.Contains(out.String(), "endpoint") {
		t.Fatalf("local JSON changed: %s", out)
	}
}

// TestStatsFromDisabledAndInvalidRequests keeps monitoring private by default
// and distinguishes an invalid window from an unavailable report.
func TestStatsFromDisabledAndInvalidRequests(t *testing.T) {
	addr := statsDaemon(t, false)
	a, out, errb := newApp(t, "stats", "-from", addr, "-json")
	if code := a.run(); code != exitError || out.Len() != 0 || !strings.Contains(errb.String(), "-bazel-monitoring") {
		t.Fatalf("disabled = %d, stdout %s, stderr %s", code, out, errb)
	}
	addr = statsDaemon(t, true)
	for _, since := range []string{"7d", "-1h", "soon"} {
		a, out, errb = newApp(t, "stats", "-from", addr, "-since", since)
		if code := a.run(); code != exitUsage || out.Len() != 0 || !strings.Contains(errb.String(), "-since") {
			t.Fatalf("invalid CLI window %q = %d, stdout %s, stderr %s", since, code, out, errb)
		}
		resp, err := http.Get("http://" + addr + "/stats?since=" + since)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid HTTP window %q = %d", since, resp.StatusCode)
		}
	}
}

// TestStatsFromFailures never turns transport errors, non-reports, or daemon
// errors into successful all-zero reports, including oversized valid JSON.
func TestStatsFromFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"html", 200, "<html>bad gateway</html>", "not a stats report"},
		{"truncated", 200, `{"lifetime":`, "not a stats report"},
		{"unrelated", 200, `{"status":"ok"}`, "not a stats report"},
		{"null", 200, `null`, "not a stats report"},
		{"partial", 200, `{"window_since":1}`, "not a stats report"},
		{"null counters", 200, `{"lifetime":null,"lifetime_since":0,"window":{},"window_since":1,"buckets":null}`, "not a stats report"},
		{"daemon error", 200, `{"err":"stats: index unreadable"}`, "index unreadable"},
		{"HTTP failure", 503, "unavailable", "503"},
		{"oversized", 200, `{"padding":"` + strings.Repeat("x", maxStatsBody) + `"}`, "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			a, out, errb := newApp(t, "stats", "-from", addr, "-json")
			if code := a.run(); code != exitError || out.Len() != 0 || !strings.Contains(errb.String(), tc.want) || !strings.Contains(errb.String(), addr) {
				t.Fatalf("failure = %d, stdout %s, stderr %s", code, out, errb)
			}
		})
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	a, out, errb := newApp(t, "stats", "-from", addr)
	if code := a.run(); code != exitError || out.Len() != 0 || !strings.Contains(errb.String(), addr) {
		t.Fatalf("unreachable = %d, stdout %s, stderr %s", code, out, errb)
	}
}

// TestStatsFromRejectsBadAddresses refuses non-HTTP schemes and prefixed routes
// before dialing; remote stats follows status's address semantics.
func TestStatsFromRejectsBadAddresses(t *testing.T) {
	for _, addr := range []string{"ftp://localhost:9095", "http://", "http://localhost:9095/prefix", "http://localhost:9095/status", "%zz"} {
		a, out, errb := newApp(t, "stats", "-from", addr)
		if code := a.run(); code != exitUsage || out.Len() != 0 || !strings.Contains(errb.String(), "-from") {
			t.Fatalf("bad address %q = %d, stdout %s, stderr %s", addr, code, out, errb)
		}
	}
}
