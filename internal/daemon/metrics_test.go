// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/index"
)

// exposition is a parsed scrape: the declared type of every family, and the
// value of every series in it, keyed by the series identifier a query would use.
type exposition struct {
	types   map[string]string
	help    map[string]string
	samples map[string]float64
}

// metricNamePattern is the format's own rule for a family name. A name outside
// it is not a strict-mode quibble; a scraper rejects the whole document.
var metricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// parseExposition reads Prometheus text exposition, failing the test on anything
// a scraper would refuse.
//
// It is written out here rather than imported so that the assertions below are
// about the bytes this daemon actually emits. A parser from the same library
// that produced them would agree with itself.
func parseExposition(t *testing.T, text string) exposition {
	t.Helper()
	e := exposition{
		types:   map[string]string{},
		help:    map[string]string{},
		samples: map[string]float64{},
	}
	for n, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		lineNo := n + 1
		if rest, ok := strings.CutPrefix(line, "# HELP "); ok {
			name, help, found := strings.Cut(rest, " ")
			if !found || help == "" {
				t.Fatalf("line %d: %q has no help text", lineNo, line)
			}
			e.help[name] = help
			continue
		}
		if rest, ok := strings.CutPrefix(line, "# TYPE "); ok {
			name, typ, found := strings.Cut(rest, " ")
			if !found {
				t.Fatalf("line %d: %q declares no type", lineNo, line)
			}
			switch typ {
			case "counter", "gauge", "histogram", "summary", "untyped":
			default:
				t.Fatalf("line %d: type %q is not one this format has", lineNo, typ)
			}
			if !metricNamePattern.MatchString(name) {
				t.Fatalf("line %d: %q is not a metric name", lineNo, name)
			}
			e.types[name] = typ
			continue
		}
		if strings.HasPrefix(line, "#") {
			t.Fatalf("line %d: %q is a comment this format does not define", lineNo, line)
		}

		// The value is the last field: a label value may contain spaces of its
		// own, so cutting at the first one would split a label set in half.
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			t.Fatalf("line %d: %q is not a sample", lineNo, line)
		}
		series, value := line[:sp], line[sp+1:]
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatalf("line %d: value %q: %v", lineNo, value, err)
		}
		family := series
		if i := strings.IndexByte(series, '{'); i >= 0 {
			family = series[:i]
			if !strings.HasSuffix(series, "}") {
				t.Fatalf("line %d: %q has an unclosed label set", lineNo, line)
			}
		}
		if !metricNamePattern.MatchString(family) {
			t.Fatalf("line %d: %q is not a metric name", lineNo, family)
		}
		if _, ok := e.types[family]; !ok {
			t.Fatalf("line %d: %s has no preceding TYPE line", lineNo, family)
		}
		if _, ok := e.help[family]; !ok {
			t.Fatalf("line %d: %s has no preceding HELP line", lineNo, family)
		}
		if _, dup := e.samples[series]; dup {
			t.Fatalf("line %d: %s appears twice, which is one series with two values", lineNo, series)
		}
		e.samples[series] = v
	}
	return e
}

// wantSample fails unless the series is present with the expected value.
func (e exposition) wantSample(t *testing.T, series string, want float64) {
	t.Helper()
	got, ok := e.samples[series]
	if !ok {
		t.Fatalf("%s is absent from the exposition", series)
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", series, got, want)
	}
}

// wantType fails unless the family is declared with the expected type.
func (e exposition) wantType(t *testing.T, family, want string) {
	t.Helper()
	got, ok := e.types[family]
	if !ok {
		t.Fatalf("%s is absent from the exposition", family)
	}
	if got != want {
		t.Fatalf("%s is a %s, want a %s", family, got, want)
	}
}

// TestMetricsExposition pins the families, their types, and their units.
//
// The type is the load-bearing part. A tally rendered as a gauge makes rate()
// over it meaningless, and a level rendered as a counter makes it nonsense, so a
// mistake here is not caught downstream — it is inherited by every dashboard
// built on top.
func TestMetricsExposition(t *testing.T) {
	e := parseExposition(t, string(renderMetrics(StatusResponse{
		Version:          "plaid-cache v9.9.9",
		PID:              4210,
		RemoteEnabled:    true,
		Actions:          274,
		Objects:          206,
		DiskBytes:        36473344,
		MaxBytes:         1 << 30,
		TTL:              "168h0m0s",
		TTLSeconds:       604800,
		Uptime:           "4s",
		UptimeSeconds:    4,
		HaveAgeSpan:      true,
		OldestAge:        "1m0s",
		OldestAgeSeconds: 60,
		NewestAge:        "1s",
		NewestAgeSeconds: 1,
		LifetimeSince:    1_700_000_000_000_000_000,

		UploadQueueDepth:    12,
		UploadQueueCapacity: 512,

		Lifetime: cache.MetricsSnapshot{
			GetLocalHit: 205, GetRemoteHit: 3, GetMiss: 139, GetRepair: 2,
			Put: 275, UploadOK: 205, UploadFail: 1, UploadDrop: 2, UploadSkip: 3,
			Compactions: 7,
		},
	})))

	// What the cache holds and what it was told to hold: levels, so gauges.
	for _, f := range []string{
		"plaid_cache_actions", "plaid_cache_objects", "plaid_cache_disk_bytes",
		"plaid_cache_max_bytes", "plaid_cache_ttl_seconds", "plaid_cache_uptime_seconds",
		"plaid_cache_oldest_entry_age_seconds", "plaid_cache_newest_entry_age_seconds",
		"plaid_cache_remote_tier_enabled", "plaid_cache_activity_start_time_seconds",
		"plaid_cache_build_info",
		// A backlog rises and falls, so a counter here would make every
		// dashboard built on it nonsense — and this is the one family meant to
		// be watched climbing, before the drops it predicts have happened.
		"plaid_cache_upload_queue_depth", "plaid_cache_upload_queue_capacity",
	} {
		e.wantType(t, f, "gauge")
	}
	// What has happened: monotonic, so counters, and named _total.
	for _, f := range []string{
		"plaid_cache_gets_total", "plaid_cache_puts_total", "plaid_cache_repairs_total",
		"plaid_cache_uploads_total", "plaid_cache_compactions_total",
	} {
		e.wantType(t, f, "counter")
	}

	e.wantSample(t, "plaid_cache_actions", 274)
	e.wantSample(t, "plaid_cache_objects", 206)
	e.wantSample(t, "plaid_cache_disk_bytes", 36473344)
	e.wantSample(t, "plaid_cache_max_bytes", 1<<30)
	// Seconds, not the "168h0m0s" the human report prints and not milliseconds.
	e.wantSample(t, "plaid_cache_ttl_seconds", 604800)
	e.wantSample(t, "plaid_cache_uptime_seconds", 4)
	e.wantSample(t, "plaid_cache_oldest_entry_age_seconds", 60)
	e.wantSample(t, "plaid_cache_newest_entry_age_seconds", 1)
	e.wantSample(t, "plaid_cache_remote_tier_enabled", 1)
	e.wantSample(t, "plaid_cache_activity_start_time_seconds", 1_700_000_000)
	e.wantSample(t, `plaid_cache_build_info{version="plaid-cache v9.9.9"}`, 1)

	e.wantSample(t, `plaid_cache_gets_total{result="local_hit"}`, 205)
	e.wantSample(t, `plaid_cache_gets_total{result="remote_hit"}`, 3)
	e.wantSample(t, `plaid_cache_gets_total{result="miss"}`, 139)
	e.wantSample(t, "plaid_cache_puts_total", 275)
	e.wantSample(t, "plaid_cache_repairs_total", 2)
	e.wantSample(t, `plaid_cache_uploads_total{result="ok"}`, 205)
	e.wantSample(t, `plaid_cache_uploads_total{result="failed"}`, 1)
	e.wantSample(t, `plaid_cache_uploads_total{result="dropped"}`, 2)
	e.wantSample(t, `plaid_cache_uploads_total{result="skipped"}`, 3)
	e.wantSample(t, "plaid_cache_compactions_total", 7)
	e.wantSample(t, "plaid_cache_upload_queue_depth", 12)
	e.wantSample(t, "plaid_cache_upload_queue_capacity", 512)

	// Every family this daemon exposes carries the one prefix, and nothing
	// carries a label whose values are not a fixed, short list — a label per
	// digest or per client is how a metrics endpoint becomes a memory leak in
	// whatever scrapes it.
	for family := range e.types {
		if !strings.HasPrefix(family, metricPrefix) {
			t.Fatalf("family %s does not carry the %s prefix", family, metricPrefix)
		}
	}
	for series := range e.samples {
		if i := strings.IndexByte(series, '{'); i >= 0 {
			labels := series[i:]
			switch {
			case strings.HasPrefix(labels, `{result="`), strings.HasPrefix(labels, `{version="`):
			default:
				t.Fatalf("series %s carries an unexpected label; only bounded ones belong here", series)
			}
		}
	}
}

// TestMetricsOmitWhatIsNotKnown pins that an empty cache reports no age span
// rather than a span of zero. Zero seconds is a real value — an entry touched
// this instant — so emitting it for "there are no entries" would be a lie a
// dashboard cannot see through.
func TestMetricsOmitWhatIsNotKnown(t *testing.T) {
	e := parseExposition(t, string(renderMetrics(StatusResponse{Version: testVersion})))
	for _, f := range []string{
		"plaid_cache_oldest_entry_age_seconds",
		"plaid_cache_newest_entry_age_seconds",
		"plaid_cache_activity_start_time_seconds",
	} {
		if _, ok := e.types[f]; ok {
			t.Fatalf("%s was emitted for a cache that has no such measurement", f)
		}
	}
	// The counters are still there, at zero: a cache that has done nothing has
	// done nothing, which is a fact, unlike an age span it does not have.
	e.wantSample(t, `plaid_cache_gets_total{result="miss"}`, 0)
}

// TestMetricsEscapeLabelValues pins that a version string carrying a quote or a
// backslash cannot break out of its label and corrupt the document. The version
// comes from the build's ldflags, so it is not attacker-controlled — but a
// document a scraper rejects wholesale is a bad way to find that out.
func TestMetricsEscapeLabelValues(t *testing.T) {
	text := string(renderMetrics(StatusResponse{Version: `v1 "odd" \ build`}))
	e := parseExposition(t, text)
	e.wantSample(t, `plaid_cache_build_info{version="v1 \"odd\" \\ build"}`, 1)
}

// TestMetricsFormatValuesInFull pins decimal notation. Both spellings parse, but
// a byte count printed as 2.1474836e+09 is unreadable in exactly the situation
// where somebody is reading a raw scrape at all.
func TestMetricsFormatValuesInFull(t *testing.T) {
	text := string(renderMetrics(StatusResponse{DiskBytes: 2147483648, MaxBytes: 1 << 40}))
	for _, want := range []string{"plaid_cache_disk_bytes 2147483648", "plaid_cache_max_bytes 1099511627776"} {
		if !strings.Contains(text, want) {
			t.Fatalf("exposition does not contain %q:\n%s", want, text)
		}
	}
}

// TestMetricsAgreeWithTheStatusReport is the one that matters over time.
//
// The scrape and the status report are two renderings of one measurement, and
// the way that stops being true is quietly: someone adds a second place to count
// something, and the two answers drift apart for a week before anyone notices.
// This pins them against a real daemon with real activity behind it.
func TestMetricsAgreeWithTheStatusReport(t *testing.T) {
	cfg := newTestConfig(t)
	seedActivity(t, cfg, index.Activity{GetLocalHit: 11, GetMiss: 4, Put: 6, Compactions: 1})
	s := newTestServer(t, cfg)

	// Real traffic on top of the seeded history, so the numbers being compared
	// are ones the daemon itself produced rather than ones a test wrote down.
	putCached(t, s, testActionID(31), testOutputID(31), []byte("an output the cache is asked to hold"))

	status := s.status()
	body, err := s.bazelMetrics(context.Background())
	if err != nil {
		t.Fatalf("bazelMetrics: %v", err)
	}
	e := parseExposition(t, string(body))

	e.wantSample(t, "plaid_cache_actions", float64(status.Actions))
	e.wantSample(t, "plaid_cache_objects", float64(status.Objects))
	e.wantSample(t, "plaid_cache_disk_bytes", float64(status.DiskBytes))
	e.wantSample(t, "plaid_cache_max_bytes", float64(status.MaxBytes))
	e.wantSample(t, "plaid_cache_ttl_seconds", status.TTLSeconds)
	e.wantSample(t, `plaid_cache_gets_total{result="local_hit"}`, float64(status.Lifetime.GetLocalHit))
	e.wantSample(t, `plaid_cache_gets_total{result="miss"}`, float64(status.Lifetime.GetMiss))
	e.wantSample(t, "plaid_cache_puts_total", float64(status.Lifetime.Put))
	e.wantSample(t, "plaid_cache_compactions_total", float64(status.Lifetime.Compactions))
	e.wantSample(t, `plaid_cache_build_info{version="`+testVersion+`"}`, 1)

	// The lifetime counters include what this process has counted but not yet
	// persisted, which is what makes them comparable with a report taken at the
	// same moment rather than several seconds behind one.
	if status.Lifetime.Put < 7 {
		t.Fatalf("lifetime puts = %d, want the seeded 6 plus the one just stored", status.Lifetime.Put)
	}
	if status.Actions == 0 || status.DiskBytes == 0 {
		t.Fatalf("status = %+v, want a cache holding the entry just stored", status)
	}
}
