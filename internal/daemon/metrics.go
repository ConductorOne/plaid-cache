// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"strconv"
	"strings"

	"github.com/conductorone/plaid-cache/internal/remote"
)

// metricPrefix namespaces every family this daemon exposes.
//
// One prefix, matching the binary, so that a scrape of a host running several
// things has an obvious way to select this one and no way to collide with
// another exporter's names.
const metricPrefix = "plaid_cache_"

// renderMetrics renders a status report as Prometheus text exposition, version
// 0.0.4 — the format every Prometheus server reads and an OpenTelemetry
// Collector's prometheus receiver scrapes without translation.
//
// It takes the report rather than reading the cache for itself, which is the
// whole design: the socket's status output, the JSON on the status route, and
// this exposition are three renderings of one measurement. A number cannot
// disagree between them, because there is no second place where one is
// produced.
//
// The format is written by hand rather than through a client library. It is a
// dozen families of plain text with two escaping rules, so the library would buy
// correctness this file can demonstrate in a test, and would cost a direct
// dependency on a module this repository currently only inherits — and with it
// that module's upgrade cadence, for a repository whose direct requirements are
// deliberately few.
//
// What is not here is any push path: no OTLP exporter, no SDK, no collector
// configuration. "Scrape this" is a complete answer for a process that already
// serves HTTP, and a push path would mean a daemon that talks to somewhere on
// its own initiative — a much larger thing to own than a text handler.
func renderMetrics(r StatusResponse) []byte {
	var b strings.Builder

	// Not a counter or a gauge worth reading, but the conventional way to make
	// the answer to "which build produced these numbers" available to a query:
	// a constant 1 carrying the version as a label. The label set is fixed, so
	// it adds one series and can never add another.
	writeFamily(&b, "build_info", "gauge",
		"The build of plaid-cache serving this endpoint, as a constant 1.",
		sample{labels: labels("version", r.Version), value: 1})

	// What the cache holds now. Every one of these can fall as well as rise, as
	// eviction runs, so they are gauges; reading any of them as a counter would
	// make a rate() over it meaningless.
	writeFamily(&b, "actions", "gauge",
		"Action entries in the index.", sample{value: float64(r.Actions)})
	writeFamily(&b, "objects", "gauge",
		"Distinct stored bodies. Several actions may share one.", sample{value: float64(r.Objects)})
	writeFamily(&b, "disk_bytes", "gauge",
		"Bytes of stored bodies the index accounts for.", sample{value: float64(r.DiskBytes)})

	// The limits eviction is enforcing, so that a dashboard can draw the budget
	// beside the usage rather than having them configured in two places. Zero
	// means the constraint is off for both, which is what the cache itself
	// means by zero.
	writeFamily(&b, "max_bytes", "gauge",
		"Configured size ceiling. Zero disables the size constraint.", sample{value: float64(r.MaxBytes)})
	writeFamily(&b, "ttl_seconds", "gauge",
		"Configured age limit for an unused entry. Zero disables the age constraint.",
		sample{value: r.TTLSeconds})

	// The age span says whether the TTL is doing anything: an oldest entry
	// younger than the TTL means only the size ceiling is evicting. An empty
	// cache has no span at all, and a family of zeros would read as one whose
	// entries were all touched this instant.
	if r.HaveAgeSpan {
		writeFamily(&b, "oldest_entry_age_seconds", "gauge",
			"Time since the least recently used entry was last used.",
			sample{value: r.OldestAgeSeconds})
		writeFamily(&b, "newest_entry_age_seconds", "gauge",
			"Time since the most recently used entry was last used.",
			sample{value: r.NewestAgeSeconds})
	}

	// The upload backlog, which is the one number here that leads its failure
	// rather than recording it. A queue at its capacity is dropping uploads, and
	// the shared tier quietly stops receiving what this machine builds; the
	// counter that says so is uploads_total{result="dropped"}, and by the time it
	// moves the entries are gone. These two are a process level rather than a
	// persisted total for the same reason they are worth having: what is queued
	// belongs to the daemon holding it and means nothing after it exits.
	writeFamily(&b, "upload_queue_depth", "gauge",
		"Uploads to the shared tier waiting on this daemon's worker pool.",
		sample{value: float64(r.UploadQueueDepth)})
	writeFamily(&b, "upload_queue_capacity", "gauge",
		"Uploads that may wait before further ones are dropped. Depth at capacity means entries are being lost.",
		sample{value: float64(r.UploadQueueCapacity)})

	writeFamily(&b, "uptime_seconds", "gauge",
		"Time since this daemon started.", sample{value: r.UptimeSeconds})
	writeFamily(&b, "remote_tier_enabled", "gauge",
		"1 when a shared remote tier is configured, 0 when the cache is local only.",
		sample{value: boolValue(r.RemoteEnabled)})

	// The counters below are the persisted lifetime tallies, not this process's
	// own. A daemon exits after its idle timeout and the next one starts from
	// zero, so a series built from the process counters would sawtooth for
	// reasons that have nothing to do with the cache, and would read as a cache
	// that keeps losing its history. The persisted totals span every process
	// that has used this cache and only reset when the cache is deleted, which
	// is a reset that means what a counter reset is supposed to mean.
	life := r.Lifetime
	writeFamily(&b, "gets_total", "counter",
		"Lookups, by how each was answered, across every process that has used this cache.",
		sample{labels: labels("result", "local_hit"), value: float64(life.GetLocalHit)},
		sample{labels: labels("result", "remote_hit"), value: float64(life.GetRemoteHit)},
		sample{labels: labels("result", "miss"), value: float64(life.GetMiss)})
	writeFamily(&b, "puts_total", "counter",
		"Entries stored, across every process that has used this cache.",
		sample{value: float64(life.Put)})
	writeFamily(&b, "repairs_total", "counter",
		"Index entries dropped because their body had gone missing. A rising count means bodies are disappearing under the index.",
		sample{value: float64(life.GetRepair)})
	writeFamily(&b, "uploads_total", "counter",
		"Uploads to the shared tier, by outcome. Uploads are best-effort and off the critical path, so a failure costs a future miss rather than a build.",
		sample{labels: labels("result", "ok"), value: float64(life.UploadOK)},
		sample{labels: labels("result", "failed"), value: float64(life.UploadFail)},
		sample{labels: labels("result", "dropped"), value: float64(life.UploadDrop)},
		sample{labels: labels("result", "skipped"), value: float64(life.UploadSkip)})
	writeFamily(&b, "compactions_total", "counter",
		"Index compactions, which reclaim the space pruning leaves behind.",
		sample{value: float64(life.Compactions)})
	writeFamily(&b, "rrcc_local_closure_checks_total", "counter",
		"Experimental remote repository-contents cache closures observed locally, by completeness.",
		sample{labels: labels("result", "complete"), value: float64(r.RRCC.Complete)},
		sample{labels: labels("result", "marker_missing"), value: float64(r.RRCC.MarkerMissing)},
		sample{labels: labels("result", "tree_missing"), value: float64(r.RRCC.TreeMissing)},
		sample{labels: labels("result", "file_missing"), value: float64(r.RRCC.FileMissing)},
		sample{labels: labels("result", "malformed"), value: float64(r.RRCC.Malformed)})

	// The shared tier's transport, which is the only part of this report about how
	// the cache reaches the network rather than about what it holds. Absent when
	// there is no such tier, rather than rendered as zeros: a cache with no bucket
	// configured has not failed to reuse a connection, it has not tried.
	if rem := r.Remote; rem != nil {
		writeFamily(&b, "remote_requests_total", "counter",
			"HTTP requests to the shared tier, by whether the transport had an idle connection to give them. "+
				"conn=\"new\" is a request that paid for a TCP and TLS handshake before it could be sent.",
			sample{labels: labels("conn", "reused"), value: float64(rem.ConnsReused)},
			sample{labels: labels("conn", "new"), value: float64(rem.ConnsNew)})
		writeHistogram(&b, "remote_operation_duration_seconds",
			"How long an operation against the shared tier took, by operation and outcome. "+
				"A get is measured to the open body rather than through it, so reads are first-byte latency; a put is the whole transfer.",
			rem.Ops)
	}

	// When the counters above started counting, so that a total read off a
	// dashboard can be understood as covering a week or an afternoon. Nanos on
	// the wire, seconds here, because seconds is the unit this format uses for
	// everything and a timestamp is no exception.
	if r.LifetimeSince > 0 {
		writeFamily(&b, "activity_start_time_seconds", "gauge",
			"When the first activity these counters include was recorded, as Unix seconds.",
			sample{value: float64(r.LifetimeSince) / 1e9})
	}

	return []byte(b.String())
}

// sample is one line of a metric family: its labels, already rendered, and its
// value.
type sample struct {
	labels string
	value  float64
}

// writeFamily emits one family: the HELP and TYPE lines the format requires,
// then a line per sample.
func writeFamily(b *strings.Builder, name, typ, help string, samples ...sample) {
	writeHeader(b, name, typ, help)
	for _, s := range samples {
		writeSample(b, name, s.labels, s.value)
	}
}

// writeHeader emits a family's HELP and TYPE lines.
//
// They belong to the family and not to a series, so a family carrying several
// label sets — the histogram below — declares them once. A second HELP line for
// the same name is not a harmless repetition; a scraper rejects the document.
func writeHeader(b *strings.Builder, name, typ, help string) {
	b.WriteString("# HELP " + metricPrefix + name + " " + escapeHelp(help) + "\n")
	b.WriteString("# TYPE " + metricPrefix + name + " " + typ + "\n")
}

// writeSample emits one series line. name carries no prefix and labels are
// already rendered.
func writeSample(b *strings.Builder, name, labelSet string, v float64) {
	b.WriteString(metricPrefix + name + labelSet + " " + formatValue(v) + "\n")
}

// writeHistogram emits a histogram family: the three series names the format
// defines for one, for every distribution handed over.
//
// The buckets arrive cumulative and are labelled with the edges the recording
// package owns, so the two cannot disagree about which bound a count belongs to.
// The overflow bucket is written from the count rather than from a fourteenth
// number, because the format requires le="+Inf" to equal _count — they are one
// value, and storing it twice would let them differ.
//
// Nothing is written for a family with no distributions in it. An operation that
// has not happened has no latency, and a bucket set of zeros would read as one
// that answered instantly.
func writeHistogram(b *strings.Builder, name, help string, dists []remote.OpDurations) {
	if len(dists) == 0 {
		return
	}
	edges := remote.DurationBuckets()
	writeHeader(b, name, "histogram", help)
	for _, d := range dists {
		for i, count := range d.Buckets {
			if i >= len(edges) {
				// A snapshot longer than the edges it was measured against
				// cannot come from this build. Report what can be labelled
				// rather than guessing a bound or panicking on a scrape.
				break
			}
			writeSample(b, name+"_bucket",
				labels("operation", d.Operation, "outcome", d.Outcome, "le", formatValue(edges[i])),
				float64(count))
		}
		labelSet := labels("operation", d.Operation, "outcome", d.Outcome)
		writeSample(b, name+"_bucket",
			labels("operation", d.Operation, "outcome", d.Outcome, "le", "+Inf"), float64(d.Count))
		writeSample(b, name+"_sum", labelSet, d.SumSeconds)
		writeSample(b, name+"_count", labelSet, float64(d.Count))
	}
}

// labels renders a label set from alternating names and values.
//
// Every call site in this file passes a fixed set of names and a fixed or
// bounded set of values. That is the rule this endpoint keeps: nothing that
// varies per request — no digest, no key, no path, no client — ever becomes a
// label, because a series is created for every distinct value and never
// forgotten, so an unbounded label is an unbounded memory leak in whatever
// scrapes this.
func labels(pairs ...string) string {
	if len(pairs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(pairs); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(pairs[i])
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(pairs[i+1]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// formatValue renders a sample value.
//
// Decimal rather than exponential notation, with no trailing zeros: both are
// accepted by a parser, but a byte count printed as 2.1474836e+09 is unreadable
// in exactly the situation — reading a raw scrape to find out what is wrong —
// where a person is looking at it at all.
func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// boolValue renders a flag the way this format states one, as 1 or 0.
func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// escapeHelp escapes help text, where a backslash and a newline are the two
// characters that would otherwise end or continue the line wrongly.
func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

// escapeLabelValue escapes a label value, which is quoted and so has a third
// character to hide.
func escapeLabelValue(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
