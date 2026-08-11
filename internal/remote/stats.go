// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"errors"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// stats records what this package's transport did, for the daemon's status
// report and the metrics rendered from it.
//
// It answers two questions the rest of the code cannot, and both are about cost
// rather than correctness. The first is whether a request was handed a
// connection that was already open or paid to establish one: an HTTP transport
// retains a bounded number of idle connections per host, so a process asking for
// more concurrency than that closes connections it is about to want again, and
// the transport is the only place that is visible. The second is how long an
// operation took, which is what says whether the first question matters — a
// handshake nobody waits on is a curiosity.
//
// Every field is atomic and nothing here takes a lock. Reads block the build
// that asked for them and uploads run hundreds a second, so the accounting has
// to cost less than what it measures: an observation is two atomic adds and a
// walk over a dozen float comparisons, and allocates nothing.
type stats struct {
	connsReused atomic.Int64
	connsNew    atomic.Int64

	// ops is one distribution per operation and outcome. Both are small fixed
	// sets, so the series this can produce are bounded at compile time rather
	// than by traffic — the same rule the metrics endpoint keeps for labels.
	ops [numOperations][numOutcomes]histogram
}

// operation is one of the four calls this package makes against the shared
// tier.
//
// It is an index rather than a string so that recording one costs an array
// offset instead of a map lookup, and so that a typo in a label is a compile
// error rather than a second series nobody is looking at.
type operation uint8

// The operations, and the count of them, which is the array bound above.
const (
	opGetAction operation = iota
	opPutAction
	opGetObject
	opPutObject
	numOperations
)

// String names the operation as it appears in a metric label.
func (o operation) String() string {
	switch o {
	case opGetAction:
		return "get_action"
	case opPutAction:
		return "put_action"
	case opGetObject:
		return "get_object"
	case opPutObject:
		return "put_object"
	}
	return "unknown"
}

// outcome is how an operation ended.
//
// A miss is separated from a hit because it is the cheap path — there is no body
// to transfer — so folding the two together would make a read's latency describe
// a mixture whose composition moves with the hit rate. A failure is separated
// from both because a timeout is not a measurement of the tier working.
type outcome uint8

// The outcomes, and the count of them.
const (
	outcomeOK outcome = iota
	outcomeMiss
	outcomeError
	numOutcomes
)

// String names the outcome as it appears in a metric label.
func (o outcome) String() string {
	switch o {
	case outcomeOK:
		return "ok"
	case outcomeMiss:
		return "miss"
	case outcomeError:
		return "error"
	}
	return "unknown"
}

// durationBuckets are the histogram's upper bounds, in seconds.
//
// The edges are for object-store round trips, and the resolution is deliberately
// front-loaded:
//
//   - Under 10ms is where a bucket in the same zone answers at all. Without
//     several edges below it every fast request lands in one, and a regression
//     from 2ms to 8ms is invisible.
//   - 10ms to 100ms is where a request that had to establish a connection
//     separates from one that reused an idle one. A new connection costs a TCP
//     handshake and then a TLS handshake before the request is sent at all,
//     which is a few round trips: single-digit milliseconds on a short path and
//     tens on a longer one. That band is why this histogram belongs beside the
//     connection counters rather than on its own.
//   - 100ms to 10s is transfer rather than round trip. A put streams the whole
//     body, so once an object is more than a few hundred kilobytes its duration
//     is a question about size and bandwidth.
//   - Past the last edge there is nothing left to resolve. An operation that slow
//     either finishes or hits the caller's timeout, and the overflow bucket and
//     the count say which without another handful of series.
//
// Thirteen edges across twelve operation-and-outcome pairs is the ceiling on what
// this family can emit, and it is a ceiling rather than a function of load.
var durationBuckets = [...]float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// DurationBuckets returns the histogram's upper bounds in seconds, ascending.
//
// The renderer needs them to label each bucket, and it is not the same package,
// so they are handed over rather than restated: two lists of edges would be two
// places to change one of them.
func DurationBuckets() []float64 {
	return append([]float64(nil), durationBuckets[:]...)
}

// histogram is a bucketed distribution of durations.
//
// The counts are per bucket rather than cumulative, so an observation touches
// one of them instead of every bucket above it; the cumulative form the
// exposition wants is built when a snapshot is taken, which happens per scrape
// rather than per request. The sum is nanoseconds as an integer because a float
// sum needs a compare-and-swap loop to be atomic, and because nanoseconds is
// what a duration already is.
type histogram struct {
	// counts is one per bucket, plus a last one for everything above the final
	// edge.
	counts [len(durationBuckets) + 1]atomic.Int64

	sumNanos atomic.Int64
}

// observe records one duration.
func (h *histogram) observe(d time.Duration) {
	// A difference of two monotonic readings cannot be negative, but a caller
	// that timed the wrong thing could hand one over, and a negative observation
	// corrupts the sum permanently.
	if d < 0 {
		d = 0
	}
	s := d.Seconds()
	i := 0
	for i < len(durationBuckets) && s > durationBuckets[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumNanos.Add(int64(d))
}

// snapshot returns the distribution in the cumulative form the exposition wants,
// reporting false when nothing has been observed.
func (h *histogram) snapshot() (buckets []int64, count int64, sumSeconds float64, observed bool) {
	cumulative := make([]int64, len(durationBuckets))
	var running int64
	for i := range durationBuckets {
		running += h.counts[i].Load()
		cumulative[i] = running
	}
	count = running + h.counts[len(durationBuckets)].Load()
	if count == 0 {
		return nil, 0, 0, false
	}
	return cumulative, count, float64(h.sumNanos.Load()) / 1e9, true
}

// observe records one completed operation, classifying it by the error it
// returned.
func (s *stats) observe(op operation, err error, d time.Duration) {
	s.ops[op][outcomeOf(err)].observe(d)
}

// outcomeOf classifies an operation's error.
//
// A miss is not a failure here any more than it is anywhere else in this
// package: the tier answered, and answered quickly, which is a latency worth
// recording under its own name rather than one worth discarding.
func outcomeOf(err error) outcome {
	switch {
	case err == nil:
		return outcomeOK
	case errors.Is(err, ErrMiss):
		return outcomeMiss
	default:
		return outcomeError
	}
}

// recordConn counts one request that was given a connection.
func (s *stats) recordConn(reused bool) {
	if reused {
		s.connsReused.Add(1)
		return
	}
	s.connsNew.Add(1)
}

// Snapshot returns a consistent-enough copy of the counters. A snapshot may mix
// counts read a few instructions apart, which is acceptable for reporting and is
// the same trade the cache's own counters make.
func (s *stats) Snapshot() StatsSnapshot {
	snap := StatsSnapshot{
		ConnsReused: s.connsReused.Load(),
		ConnsNew:    s.connsNew.Load(),
	}
	// Pairs nothing has landed in are left out rather than reported as twelve
	// empty distributions. Most of them never happen — a cache that only reads
	// makes no puts, and a bucket that is up makes no errors — and a series that
	// appears the first time an operation does is the shape a scraper already
	// handles for a target that has just started.
	for op := operation(0); op < numOperations; op++ {
		for oc := outcome(0); oc < numOutcomes; oc++ {
			buckets, count, sum, observed := s.ops[op][oc].snapshot()
			if !observed {
				continue
			}
			snap.Ops = append(snap.Ops, OpDurations{
				Operation:  op.String(),
				Outcome:    oc.String(),
				Count:      count,
				SumSeconds: sum,
				Buckets:    buckets,
			})
		}
	}
	return snap
}

// StatsSnapshot is a value copy of a backend's transport accounting, for
// reporting.
//
// It travels as JSON in the daemon's status report, which is what the metrics
// exposition is rendered from, so this is the shape both readers see.
type StatsSnapshot struct {
	// ConnsReused and ConnsNew count requests, not connections: one per HTTP
	// request that the transport handed a connection, split by whether that
	// connection was already open. Their sum is therefore requests attempted
	// that got as far as a connection, and ConnsNew alone is how many
	// connections this process has had to establish.
	ConnsReused int64 `json:"conns_reused"`
	ConnsNew    int64 `json:"conns_new"`

	// Ops holds one distribution per operation and outcome that has occurred.
	Ops []OpDurations `json:"ops,omitempty"`
}

// OpDurations is how long one operation took, for one outcome.
type OpDurations struct {
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`

	// Count is every observation, including those above the last bucket edge,
	// and SumSeconds their total.
	Count      int64   `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`

	// Buckets is a cumulative count per DurationBuckets edge, in that order:
	// Buckets[i] is the observations at or below edge i. There is no entry for
	// everything above the last edge, because that number is Count.
	Buckets []int64 `json:"buckets"`
}

// Statser is a Backend that keeps transport accounting.
//
// It is a separate interface rather than a Backend method because most Backends
// have nothing to report — Noop makes no requests, and a test's fake makes them
// against nothing — and obliging every one of them to grow a method that returns
// an empty snapshot would be four lines of ceremony per implementation to say
// so. A caller asks with a type assertion and reports nothing when the answer is
// no.
type Statser interface {
	// Stats returns the transport accounting so far.
	Stats() StatsSnapshot
}

// httpDoer is the shape of the AWS SDK's HTTPClient option: the one method a
// client of theirs has to have. Declared here so the wrapper below does not
// depend on which service package's spelling of it is in scope.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// tracingClient counts, for every HTTP request that goes through it, whether the
// transport had a connection to give it.
//
// It wraps an HTTP client rather than replacing one. That distinction is the
// whole point of this type: the connection pool, its idle limits and its
// timeouts stay exactly as the SDK resolved them, so a series recorded before
// anyone touches those settings is a measurement of the current behaviour and
// not of a transport this program configured while claiming to observe one.
//
// It counts requests that reached a connection. One that fails before that —
// name resolution, a refused dial — is invisible here and shows up as a failed
// operation in the duration histogram instead. Every request the client makes is
// counted, including a retried attempt and the session a directory bucket
// negotiates for itself, because those use the same pool and compete for the
// same idle connections as the gets and puts do.
type tracingClient struct {
	base  httpDoer
	trace *httptrace.ClientTrace
}

// newTracingClient wraps base so its requests are counted into st.
func newTracingClient(base httpDoer, st *stats) *tracingClient {
	return &tracingClient{
		base: base,
		// One trace for every request rather than one per request. The callback
		// closes over nothing but the counters, so there is nothing per-request
		// to capture, and building it here keeps an allocation and a closure off
		// a path that runs on every upload. httptrace's hooks may run on any
		// goroutine, which two atomic adds are already safe for.
		trace: &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { st.recordConn(info.Reused) },
		},
	}
}

// Do sends one request, with the trace attached.
func (c *tracingClient) Do(r *http.Request) (*http.Response, error) {
	// A shallow copy: the request is the SDK's, and it hands the same one back
	// for each retry attempt.
	return c.base.Do(r.WithContext(httptrace.WithClientTrace(r.Context(), c.trace)))
}
