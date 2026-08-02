// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
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
