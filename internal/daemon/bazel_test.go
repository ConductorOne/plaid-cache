// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// startBazel runs ServeBazel on a loopback listener and returns its base URL.
// The daemon is stopped and the goroutine joined at test end, which is also
// what pins that a stop actually reaches the HTTP listener.
func startBazel(t *testing.T, s *Server) string {
	t.Helper()
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
		case <-time.After(serveStopTimeout):
			t.Errorf("ServeBazel did not return within %v", serveStopTimeout)
		}
	})
	return "http://" + ln.Addr().String()
}

// TestServeBazelRoundTrip pins that the daemon's HTTP listener stores and
// serves a blob over the same tiers the socket serves.
func TestServeBazelRoundTrip(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	base := startBazel(t, s)

	body := []byte("an output produced by some action")
	sum := sha256.Sum256(body)
	url := base + "/cas/" + hex.EncodeToString(sum[:])

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}

	// The counters are the daemon's own, so Bazel traffic shows up in `status`
	// beside the toolchain's rather than in a second place nobody looks.
	if m := s.cache.Metrics(); m.Put != 1 || m.GetLocalHit != 1 {
		t.Fatalf("metrics = %+v, want one put and one local hit", m)
	}
}

// TestServeBazelSuppressesTheIdleExit pins that the HTTP listener keeps the
// daemon alive. A GOCACHEPROG client that finds no daemon spawns one; Bazel
// cannot, and would see a refused connection rather than a miss.
func TestServeBazelSuppressesTheIdleExit(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.IdleTimeout = 10 * time.Millisecond
	s := newTestServer(t, cfg)
	base := startBazel(t, s)

	// Arming the timer is what Serve does before accepting anything; do it
	// directly so the test does not need a socket as well.
	s.armIdleTimer()
	time.Sleep(20 * cfg.IdleTimeout)

	select {
	case <-s.Stopped():
		t.Fatal("the daemon idled out while it was serving Bazel")
	default:
	}
	resp, err := http.Get(base + "/cas/" + hex.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatalf("GET after the idle timeout: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
	}
}

// getBazel performs one GET against the listener and returns the response and
// its body.
func getBazel(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp, body
}

// TestServeBazelWithoutMonitoring pins the default. A daemon serving Bazel says
// nothing about its host unless it was told to, and says nothing about the fact
// that it could: the monitoring paths get the same answer as any other path this
// listener does not serve.
func TestServeBazelWithoutMonitoring(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	base := startBazel(t, s)

	for _, p := range []string{bazel.StatusPath, bazel.MetricsPath} {
		resp, _ := getBazel(t, base+p)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404 from a daemon that was not asked to monitor", p, resp.StatusCode)
		}
	}
}

// TestServeBazelMonitoring pins the other side of the gate: with monitoring on,
// both routes answer, the status route carries the same report the socket serves,
// and the scrape agrees with it about the same daemon at the same moment.
func TestServeBazelMonitoring(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.BazelMonitoring = true
	s := newTestServer(t, cfg)
	putCached(t, s, testActionID(41), testOutputID(41), []byte("an output this daemon holds"))
	base := startBazel(t, s)

	resp, body := getBazel(t, base+bazel.StatusPath)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", bazel.StatusPath, resp.StatusCode)
	}
	if got, want := resp.Header.Get("Content-Type"), "application/json"; got != want {
		t.Fatalf("status Content-Type = %q, want %q", got, want)
	}
	var status StatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("status body %q: %v", body, err)
	}
	if status.PID != os.Getpid() {
		t.Fatalf("status pid = %d, want this process (%d)", status.PID, os.Getpid())
	}
	if status.Version != testVersion {
		t.Fatalf("status version = %q, want %q", status.Version, testVersion)
	}
	if status.Actions != 1 || status.Objects != 1 {
		t.Fatalf("status = %d actions, %d objects, want the one entry just stored", status.Actions, status.Objects)
	}
	// Nothing about where the cache lives goes over the wire. The report is
	// counters; a directory or a bucket name would be host detail this endpoint
	// has no business handing out.
	if strings.Contains(string(body), cfg.Dir) {
		t.Fatalf("status body names the cache directory:\n%s", body)
	}

	resp, body = getBazel(t, base+bazel.MetricsPath)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", bazel.MetricsPath, resp.StatusCode)
	}
	if got, want := resp.Header.Get("Content-Type"), "text/plain; version=0.0.4; charset=utf-8"; got != want {
		t.Fatalf("metrics Content-Type = %q, want %q", got, want)
	}
	e := parseExposition(t, string(body))
	e.wantSample(t, "plaid_cache_actions", float64(status.Actions))
	e.wantSample(t, "plaid_cache_objects", float64(status.Objects))
	e.wantSample(t, "plaid_cache_disk_bytes", float64(status.DiskBytes))
	e.wantSample(t, "plaid_cache_puts_total", float64(status.Lifetime.Put))
}

// TestServeBazelMonitoringLeavesTheCacheAlone pins that turning monitoring on
// does not change what the cache routes do, since it is the same handler and
// the same store answering both.
func TestServeBazelMonitoringLeavesTheCacheAlone(t *testing.T) {
	cfg := newTestConfig(t)
	cfg.BazelMonitoring = true
	s := newTestServer(t, cfg)
	base := startBazel(t, s)

	body := []byte("an output uploaded while monitoring is on")
	sum := sha256.Sum256(body)
	url := base + "/cas/" + hex.EncodeToString(sum[:])

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp, got := getBazel(t, url)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestServeBazelNeedsABodyStore pins that a daemon assembled without one
// refuses the listener rather than failing on the first upload.
func TestServeBazelNeedsABodyStore(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	s.blobs = nil

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := s.ServeBazel(context.Background(), ln); err == nil {
		t.Fatal("ServeBazel started without a body store")
	}
	if c, derr := net.DialTimeout("tcp", ln.Addr().String(), time.Second); derr == nil {
		_ = c.Close()
		t.Fatal("ServeBazel left its listener open after refusing to serve")
	}
}
