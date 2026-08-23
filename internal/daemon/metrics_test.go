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
	"github.com/conductorone/plaid-cache/internal/reapi"
	"github.com/conductorone/plaid-cache/internal/remote"
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
		name := series
		if i := strings.IndexByte(series, '{'); i >= 0 {
			name = series[:i]
			if !strings.HasSuffix(series, "}") {
				t.Fatalf("line %d: %q has an unclosed label set", lineNo, line)
			}
		}
		if !metricNamePattern.MatchString(name) {
			t.Fatalf("line %d: %q is not a metric name", lineNo, name)
		}
		family := e.familyOf(name)
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

// familyOf maps a series name onto the family that declared its type.
//
// It is the format's one exception to "a sample's name is its family's": a
// histogram declares one TYPE line and then emits three series names derived
// from it, so the bucket, sum, and count lines have no declaration of their own
// and a parser that insisted on one would reject a document every scraper
// accepts.
func (e exposition) familyOf(name string) string {
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		base, ok := strings.CutSuffix(name, suffix)
		if !ok {
			continue
		}
		switch e.types[base] {
		case "histogram", "summary":
			return base
		}
	}
	return name
}

// cumulative builds a bucket list in the shape a snapshot has — one cumulative
// count per remote.DurationBuckets edge — from the counts at a few named edges,
// carrying each forward to the edges above it.
func cumulative(t *testing.T, at map[float64]int64) []int64 {
	t.Helper()
	edges := remote.DurationBuckets()
	out := make([]int64, len(edges))
	var running int64
	for i, edge := range edges {
		if v, ok := at[edge]; ok {
			if v < running {
				t.Fatalf("count %d at le=%v is below the %d beneath it, which no cumulative series can be", v, edge, running)
			}
			running = v
		}
		out[i] = running
	}
	return out
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
	text := string(renderMetrics(StatusResponse{
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

		RRCC: reapi.RRCCMetricsSnapshot{
			Complete: 11, MarkerMissing: 2, TreeMissing: 3, FileMissing: 5, Malformed: 7,
		},

		Remote: &remote.StatsSnapshot{
			ConnsReused: 190,
			ConnsNew:    22,
			Ops: []remote.OpDurations{
				{
					Operation: "get_object", Outcome: "ok", Count: 5, SumSeconds: 0.031,
					Buckets: cumulative(t, map[float64]int64{0.005: 2, 0.01: 4}),
				},
				{
					Operation: "put_object", Outcome: "error", Count: 1, SumSeconds: 60,
					Buckets: cumulative(t, nil),
				},
			},
		},

		Lifetime: cache.MetricsSnapshot{
			GetLocalHit: 205, GetRemoteHit: 3, GetMiss: 139, GetRepair: 2,
			Put: 275, UploadOK: 205, UploadFail: 1, UploadDrop: 2, UploadSkip: 3,
			Compactions: 7,
		},
	}))
	e := parseExposition(t, text)

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
		"plaid_cache_rrcc_local_closure_checks_total",
		// Requests that opened a connection and requests that did not: both only
		// ever rise, so a rate() over either is the question worth asking.
		"plaid_cache_remote_requests_total",
	} {
		e.wantType(t, f, "counter")
	}
	// How long the shared tier took, which is the one measurement here that a
	// single number cannot carry: the read path blocks the build, so the tail is
	// the part that costs anything.
	e.wantType(t, "plaid_cache_remote_operation_duration_seconds", "histogram")

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
	e.wantSample(t, `plaid_cache_rrcc_local_closure_checks_total{result="complete"}`, 11)
	e.wantSample(t, `plaid_cache_rrcc_local_closure_checks_total{result="marker_missing"}`, 2)
	e.wantSample(t, `plaid_cache_rrcc_local_closure_checks_total{result="tree_missing"}`, 3)
	e.wantSample(t, `plaid_cache_rrcc_local_closure_checks_total{result="file_missing"}`, 5)
	e.wantSample(t, `plaid_cache_rrcc_local_closure_checks_total{result="malformed"}`, 7)
	e.wantSample(t, "plaid_cache_upload_queue_depth", 12)
	e.wantSample(t, "plaid_cache_upload_queue_capacity", 512)

	e.wantSample(t, `plaid_cache_remote_requests_total{conn="reused"}`, 190)
	e.wantSample(t, `plaid_cache_remote_requests_total{conn="new"}`, 22)

	// A histogram is three series names per label set, and the two that are not
	// buckets are what a quantile is computed against.
	const dur = "plaid_cache_remote_operation_duration_seconds"
	e.wantSample(t, dur+`_bucket{operation="get_object",outcome="ok",le="0.005"}`, 2)
	e.wantSample(t, dur+`_bucket{operation="get_object",outcome="ok",le="0.01"}`, 4)
	e.wantSample(t, dur+`_sum{operation="get_object",outcome="ok"}`, 0.031)
	e.wantSample(t, dur+`_count{operation="get_object",outcome="ok"}`, 5)
	// The overflow bucket is the count, always: the format requires the two to
	// agree, and the fifth observation above is past the last edge.
	e.wantSample(t, dur+`_bucket{operation="get_object",outcome="ok",le="+Inf"}`, 5)

	// A failure that took a minute is entirely past the last edge, which is the
	// case that proves the overflow bucket is not derived from the buckets.
	e.wantSample(t, dur+`_bucket{operation="put_object",outcome="error",le="10"}`, 0)
	e.wantSample(t, dur+`_bucket{operation="put_object",outcome="error",le="+Inf"}`, 1)
	e.wantSample(t, dur+`_sum{operation="put_object",outcome="error"}`, 60)

	// One HELP and one TYPE line for the whole histogram, however many label sets
	// it carries. A repeated declaration is not a harmless duplicate; a scraper
	// rejects the document.
	if got := strings.Count(text, "# TYPE "+dur+" "); got != 1 {
		t.Fatalf("the histogram declared its type %d times, want once", got)
	}
	if got := strings.Count(text, "# HELP "+dur+" "); got != 1 {
		t.Fatalf("the histogram declared its help %d times, want once", got)
	}

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
			case strings.HasPrefix(labels, `{result="`), strings.HasPrefix(labels, `{version="`),
				strings.HasPrefix(labels, `{conn="`), strings.HasPrefix(labels, `{operation="`):
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

// TestMetricsOmitTheRemoteTierWhenThereIsNone pins that a local-only cache
// publishes no transport families at all.
//
// Zeros would be the wrong answer twice over: a cache with no bucket configured
// has not failed to reuse a connection, and a histogram of zeros is
// indistinguishable from a tier that answered every request instantly. A backend
// that keeps accounting but has not been used yet is the other case below —
// there the request counters are real zeros and the histogram is still absent,
// because an operation that has not happened has no duration.
func TestMetricsOmitTheRemoteTierWhenThereIsNone(t *testing.T) {
	e := parseExposition(t, string(renderMetrics(StatusResponse{Version: testVersion})))
	for _, f := range []string{
		"plaid_cache_remote_requests_total",
		"plaid_cache_remote_operation_duration_seconds",
	} {
		if _, ok := e.types[f]; ok {
			t.Fatalf("%s was emitted for a cache with no shared tier", f)
		}
	}

	idle := parseExposition(t, string(renderMetrics(StatusResponse{
		Version: testVersion,
		Remote:  &remote.StatsSnapshot{},
	})))
	idle.wantSample(t, `plaid_cache_remote_requests_total{conn="new"}`, 0)
	if _, ok := idle.types["plaid_cache_remote_operation_duration_seconds"]; ok {
		t.Fatal("a duration histogram was emitted for a tier no operation has reached")
	}
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
