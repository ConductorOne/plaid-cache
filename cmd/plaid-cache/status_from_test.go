// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/daemon"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// remoteReport is the report a stand-in daemon serves. It is the daemon's own
// wire type rather than a hand-written document, so a change to what a daemon
// sends is a change to what these tests read.
func remoteReport() daemon.StatusResponse {
	return daemon.StatusResponse{
		Version:       "plaid-cache v9.9.9",
		PID:           4210,
		RemoteEnabled: true,
		Actions:       274,
		Objects:       206,
		DiskBytes:     36473344,
		MaxBytes:      1 << 26,
		TTL:           "168h0m0s",
		TTLSeconds:    604800,
		Uptime:        "3h20m0s",
		UptimeSeconds: 12000,
		HaveAgeSpan:   true,
		OldestAge:     "20m0s",
		NewestAge:     "4s",
		Metrics: cache.MetricsSnapshot{
			GetLocalHit: 205, GetRemoteHit: 3, GetMiss: 139, Put: 275, UploadOK: 205,
		},
		Lifetime:      cache.MetricsSnapshot{GetLocalHit: 90000, GetMiss: 38443},
		LifetimeSince: 1_700_000_000_000_000_000,
		Remote:        &remote.StatsSnapshot{ConnsReused: 190, ConnsNew: 22},
	}
}

// serveReport starts a stand-in daemon answering the status route with what the
// handler writes, and returns its host and port as an operator would type it.
func serveReport(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// TestStatusFromReadsAnotherDaemon pins the ergonomic case: one command, one
// address, the normal report.
func TestStatusFromReadsAnotherDaemon(t *testing.T) {
	addr := serveReport(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != bazel.StatusPath {
			t.Errorf("client asked for %q, want %q", r.URL.Path, bazel.StatusPath)
		}
		_ = json.NewEncoder(w).Encode(remoteReport())
	})

	a, out, errb := newApp(t, "status", "-from", addr)
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status -from) = %d, want %d (stderr: %s)", code, exitOK, errb)
	}
	for _, want := range []string{
		"endpoint    http://" + addr + bazel.StatusPath,
		"version     plaid-cache v9.9.9",
		"entries     274 actions, 206 objects",
		"size        34.8 MiB of 64.0 MiB",
		"ttl         168h0m0s",
		"age         oldest 20m0s, newest 4s",
		"remote      enabled",
		"daemon      pid 4210, up 3h20m0s",
		"hit rate    59.9% of 347 lookups",
		"uploads     205 ok",
		// The reuse ratio travels with the rest of the report rather than being
		// something only a scraper can see, because it is the first thing to look
		// at when the shared tier is slower than the bucket should be.
		"conns       212 requests, 89.6% on a reused connection",
		"lifetime    70.1% of 128443 lookups",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
}

// TestStatusOmitsConnsWhenTheDaemonKeepsNone pins that the reuse line is absent
// rather than zero for a daemon that reports no transport accounting.
//
// A daemon with no shared tier has not failed to reuse a connection, and one
// running an older build does not report the field at all; "0 requests, 0.0% on a
// reused connection" would read as a broken pool in both cases.
func TestStatusOmitsConnsWhenTheDaemonKeepsNone(t *testing.T) {
	addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
		r := remoteReport()
		r.Remote = nil
		_ = json.NewEncoder(w).Encode(r)
	})

	a, out, errb := newApp(t, "status", "-from", addr)
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status -from) = %d (stderr: %s)", code, errb)
	}
	if strings.Contains(out.String(), "conns") {
		t.Fatalf("status reported connection reuse for a daemon that sent none:\n%s", out)
	}
}

// TestStatusFromDescribesOnlyTheDaemonItAsked pins that nothing local leaks into
// a report about somewhere else.
//
// The directory, the configuration file, and the volume are this machine's, and
// the daemon being asked has its own that it does not disclose. Printing the
// local ones under a report headed by a remote endpoint would be a lie with a
// plausible shape.
func TestStatusFromDescribesOnlyTheDaemonItAsked(t *testing.T) {
	addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remoteReport())
	})

	a, out, errb := newApp(t, "status", "-from", addr)
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status -from) = %d (stderr: %s)", code, errb)
	}
	for _, unwanted := range []string{"directory", "config      ", "volume"} {
		if strings.Contains(out.String(), unwanted) {
			t.Fatalf("status output attributes %q to a daemon it only read counters from:\n%s", unwanted, out)
		}
	}
}

// TestStatusFromIgnoresLocalConfiguration pins that reading somewhere else does
// not depend on this machine's cache being configured at all. An operator
// diagnosing a shared daemon is often on a laptop whose own settings are beside
// the point, and may well be broken.
func TestStatusFromIgnoresLocalConfiguration(t *testing.T) {
	addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remoteReport())
	})

	a, out, errb := newApp(t, "status", "-from", addr)
	t.Setenv("PLAID_GOCACHE_MAX_BYTES", "twenty gigs")
	if code := a.run(); code != exitOK {
		t.Fatalf("run(status -from) with a broken local setting = %d, want %d (stderr: %s)", code, exitOK, errb)
	}
	if !strings.Contains(out.String(), "daemon      pid 4210") {
		t.Fatalf("status output missing the remote report:\n%s", out)
	}
}

// TestStatusFromUnreachableFails pins that an address nothing is listening on is
// a failure that says so, rather than an empty report that reads as an idle
// cache.
func TestStatusFromUnreachableFails(t *testing.T) {
	// A port that was bound and released: nothing is there, and nothing else
	// picked it up in between with any likelihood.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	a, out, errb := newApp(t, "status", "-from", addr)
	if code := a.run(); code != exitError {
		t.Fatalf("run(status -from) against nothing = %d, want %d", code, exitError)
	}
	if !strings.Contains(errb.String(), addr) {
		t.Fatalf("stderr does not name the endpoint that failed: %s", errb)
	}
	if out.Len() != 0 {
		t.Fatalf("a failed read printed a report anyway:\n%s", out)
	}
}

// TestStatusFromRejectsWhatIsNotAReport pins the answers that are not a daemon:
// a proxy's error page, a report cut off mid-transfer, and well-formed JSON from
// something else entirely. Each fails with a message about this endpoint rather
// than printing a cache full of zeros.
func TestStatusFromRejectsWhatIsNotAReport(t *testing.T) {
	report, err := json.Marshal(remoteReport())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"html", "<html><body>502 Bad Gateway</body></html>"},
		{"truncated", string(report[:len(report)/2])},
		{"someone else's json", `{"status":"ok","service":"not a cache"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			})
			a, out, errb := newApp(t, "status", "-from", addr)
			if code := a.run(); code != exitError {
				t.Fatalf("run(status -from) = %d, want %d", code, exitError)
			}
			if !strings.Contains(errb.String(), "not a status report") {
				t.Fatalf("stderr does not say what was wrong: %s", errb)
			}
			if out.Len() != 0 {
				t.Fatalf("a bad response printed a report anyway:\n%s", out)
			}
		})
	}
}

// TestStatusFromReportsAnErrorStatus pins that a failure the endpoint reports
// honestly is passed on honestly — and that a 404, which is exactly what a
// daemon serving Bazel without monitoring answers, comes with the reason it is
// most likely to be.
func TestStatusFromReportsAnErrorStatus(t *testing.T) {
	for _, tc := range []struct {
		code int
		want string
	}{
		{http.StatusNotFound, "-bazel-monitoring"},
		{http.StatusInternalServerError, "500"},
	} {
		addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "no", tc.code)
		})
		a, _, errb := newApp(t, "status", "-from", addr)
		if code := a.run(); code != exitError {
			t.Fatalf("run(status -from) against %d = %d, want %d", tc.code, code, exitError)
		}
		if !strings.Contains(errb.String(), tc.want) {
			t.Fatalf("stderr for %d does not mention %q: %s", tc.code, tc.want, errb)
		}
	}
}

// TestStatusFromRejectsBadAddresses pins that what cannot be an endpoint is a
// usage error, said before anything is dialled.
func TestStatusFromRejectsBadAddresses(t *testing.T) {
	for _, addr := range []string{
		"ftp://cache-host:9095",      // not a scheme this speaks
		"http://",                    // no host
		"http://host:9095/prefix",    // nothing is served under a prefix
		"http://host:9095/ac/abcdef", // and a cache route is not a report
		"%zz",                        // not a URL at all
	} {
		a, out, errb := newApp(t, "status", "-from", addr)
		if code := a.run(); code != exitUsage {
			t.Fatalf("run(status -from %q) = %d, want %d", addr, code, exitUsage)
		}
		if !strings.Contains(errb.String(), "-from") {
			t.Fatalf("stderr for %q does not name the flag: %s", addr, errb)
		}
		if out.Len() != 0 {
			t.Fatalf("a rejected address printed a report anyway:\n%s", out)
		}
	}
}

// TestStatusFromAcceptsTheSpellingsPeopleUse pins that a host and port, an http
// URL, and that URL with the endpoint's own path all name the same endpoint.
func TestStatusFromAcceptsTheSpellingsPeopleUse(t *testing.T) {
	addr := serveReport(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remoteReport())
	})
	for _, spelling := range []string{addr, "http://" + addr, "http://" + addr + "/", "http://" + addr + bazel.StatusPath} {
		a, out, errb := newApp(t, "status", "-from", spelling)
		if code := a.run(); code != exitOK {
			t.Fatalf("run(status -from %q) = %d, want %d (stderr: %s)", spelling, code, exitOK, errb)
		}
		if !strings.Contains(out.String(), "daemon      pid 4210") {
			t.Fatalf("status -from %q printed no report:\n%s", spelling, out)
		}
	}
}
