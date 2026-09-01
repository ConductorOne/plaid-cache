// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startPprof runs ServePprof on a loopback listener and returns its base URL.
// The daemon is stopped and the goroutine joined at test end, which also pins
// that stopping the daemon reaches the profiling listener.
func startPprof(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := ListenPprof("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.ServePprof(context.Background(), ln) }()
	t.Cleanup(func() {
		s.Stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServePprof: %v", err)
			}
		case <-time.After(serveStopTimeout):
			t.Errorf("ServePprof did not return within %v", serveStopTimeout)
		}
	})
	return "http://" + ln.Addr().String()
}

// TestListenPprofRejectsNonLoopbackAddresses pins that unauthenticated
// profiling endpoints cannot be exposed by choosing an all-interface address.
func TestListenPprofRejectsNonLoopbackAddresses(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "[::]:0", "localhost:0"} {
		ln, err := ListenPprof(addr)
		if err == nil {
			_ = ln.Close()
			t.Fatalf("ListenPprof(%q) succeeded, want a loopback refusal", addr)
		}
	}
}

// TestServePprofServesTheRuntimeProfileIndex pins that the separate loopback
// listener exposes the standard runtime profiles without adding them to Bazel.
func TestServePprofServesTheRuntimeProfileIndex(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	base := startPprof(t, s)

	resp, err := http.Get(base + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET pprof index: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET pprof index = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read pprof index: %v", err)
	}
	for _, profile := range []string{"heap", "goroutine", "mutex", "block", "profile", "trace"} {
		if !strings.Contains(string(body), profile) {
			t.Fatalf("pprof index does not list %q: %s", profile, body)
		}
	}
}

// TestServeBazelDoesNotServePprof pins that enabling the profiling listener
// cannot broaden the Bazel HTTP handler with process diagnostics.
func TestServeBazelDoesNotServePprof(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	base := startBazel(t, s)

	resp, err := http.Get(base + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET pprof path on Bazel listener: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET pprof path on Bazel listener = %d, want 404", resp.StatusCode)
	}
}
