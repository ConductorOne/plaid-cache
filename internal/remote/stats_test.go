// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// findOp returns the distribution recorded for one operation and outcome.
func findOp(t *testing.T, snap StatsSnapshot, operation, outcome string) OpDurations {
	t.Helper()
	for _, d := range snap.Ops {
		if d.Operation == operation && d.Outcome == outcome {
			return d
		}
	}
	t.Fatalf("no distribution for %s/%s in %+v", operation, outcome, snap.Ops)
	return OpDurations{}
}

// hasOp reports whether a distribution was recorded for one pair.
func hasOp(snap StatsSnapshot, operation, outcome string) bool {
	for _, d := range snap.Ops {
		if d.Operation == operation && d.Outcome == outcome {
			return true
		}
	}
	return false
}

// TestHistogramBucketsAreCumulative pins the form the exposition requires: each
// bucket counts everything at or below its edge, and the count is every
// observation including those past the last one. A histogram whose buckets are
// not cumulative parses fine and makes every quantile computed from it wrong.
func TestHistogramBucketsAreCumulative(t *testing.T) {
	var h histogram
	// One observation for each of four bands, including one past the last edge.
	for _, d := range []time.Duration{
		500 * time.Microsecond, // at or below the first edge, 1ms
		3 * time.Millisecond,   // above 2.5ms, at or below 5ms
		300 * time.Millisecond, // above 250ms, at or below 500ms
		30 * time.Second,       // past the last edge, 10s
	} {
		h.observe(d)
	}

	buckets, count, sum, observed := h.snapshot()
	if !observed {
		t.Fatal("snapshot reports nothing observed after four observations")
	}
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}
	if len(buckets) != len(durationBuckets) {
		t.Fatalf("got %d buckets, want one per edge (%d)", len(buckets), len(durationBuckets))
	}
	if want := (0.0005 + 0.003 + 0.3 + 30); sum != want {
		t.Fatalf("sum = %v seconds, want %v", sum, want)
	}

	// Every count is the number of observations at or below that edge, and the
	// series never falls as the edges rise.
	for i, edge := range durationBuckets {
		var want int64
		for _, d := range []float64{0.0005, 0.003, 0.3, 30} {
			if d <= edge {
				want++
			}
		}
		if buckets[i] != want {
			t.Fatalf("bucket le=%v = %d, want %d", edge, buckets[i], want)
		}
		if i > 0 && buckets[i] < buckets[i-1] {
			t.Fatalf("bucket le=%v = %d fell below the one beneath it (%d)", edge, buckets[i], buckets[i-1])
		}
	}
	// The last edge is 10s and one observation was past it, so the buckets
	// account for three of the four and the count for all of them. That gap is
	// the +Inf bucket the renderer writes from the count.
	if last := buckets[len(buckets)-1]; last != 3 {
		t.Fatalf("last bucket = %d, want 3 with one observation past the final edge", last)
	}
}

// TestHistogramRejectsNegativeDurations pins that a duration nobody could have
// measured cannot corrupt the sum. A negative observation is permanent: the sum
// only accumulates, so no later measurement can correct it.
func TestHistogramRejectsNegativeDurations(t *testing.T) {
	var h histogram
	h.observe(-time.Hour)
	_, count, sum, observed := h.snapshot()
	if !observed || count != 1 {
		t.Fatalf("count = %d observed = %v, want the observation recorded", count, observed)
	}
	if sum != 0 {
		t.Fatalf("sum = %v seconds, want 0 rather than a negative total", sum)
	}
}

// TestOutcomeOfClassifiesErrors pins that a miss is its own outcome and not a
// failure, however it was wrapped on the way out. A miss folded into errors
// would make the error rate track the hit rate, and a miss folded into successes
// would make read latency describe a mixture that moves with it.
func TestOutcomeOfClassifiesErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want outcome
	}{
		{"nil", nil, outcomeOK},
		{"miss", ErrMiss, outcomeMiss},
		{"wrapped miss", fmt.Errorf("GetObject: %w", ErrMiss), outcomeMiss},
		{"other", errors.New("connection reset"), outcomeError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := outcomeOf(c.err); got != c.want {
				t.Fatalf("outcomeOf(%v) = %s, want %s", c.err, got, c.want)
			}
		})
	}
}

// TestSnapshotOmitsUnobservedPairs pins that only pairs something happened in
// are reported. Twelve empty distributions on a cache that only ever reads would
// be a page of zeros a reader has to rule out by hand.
func TestSnapshotOmitsUnobservedPairs(t *testing.T) {
	var s stats
	s.observe(opGetObject, nil, 2*time.Millisecond)
	s.observe(opGetObject, ErrMiss, time.Millisecond)

	snap := s.Snapshot()
	if len(snap.Ops) != 2 {
		t.Fatalf("got %d distributions, want the 2 observed: %+v", len(snap.Ops), snap.Ops)
	}
	if !hasOp(snap, "get_object", "ok") || !hasOp(snap, "get_object", "miss") {
		t.Fatalf("distributions = %+v, want get_object ok and miss", snap.Ops)
	}
	if hasOp(snap, "put_object", "ok") {
		t.Fatal("a put was reported for a backend that has not made one")
	}
}

// TestSnapshotCountsConnections pins the split, and that its two halves add up
// to the requests that got a connection at all.
func TestSnapshotCountsConnections(t *testing.T) {
	var s stats
	s.recordConn(true)
	s.recordConn(true)
	s.recordConn(false)

	snap := s.Snapshot()
	if snap.ConnsReused != 2 || snap.ConnsNew != 1 {
		t.Fatalf("reused = %d new = %d, want 2 and 1", snap.ConnsReused, snap.ConnsNew)
	}
}

// TestDurationBucketsAreAscending pins the order the exposition depends on. A
// bucket list out of order renders an `le` sequence whose counts fall as the
// bound rises, which every consumer of a histogram reads as corrupt.
func TestDurationBucketsAreAscending(t *testing.T) {
	for i := 1; i < len(durationBuckets); i++ {
		if durationBuckets[i] <= durationBuckets[i-1] {
			t.Fatalf("bucket %d (%v) is not above %v", i, durationBuckets[i], durationBuckets[i-1])
		}
	}
	// The exported copy is a copy: a caller that sorts or truncates what it is
	// handed must not be able to reach the edges every recording is measured
	// against.
	got := DurationBuckets()
	if len(got) != len(durationBuckets) {
		t.Fatalf("DurationBuckets returned %d edges, want %d", len(got), len(durationBuckets))
	}
	got[0] = -1
	if durationBuckets[0] == -1 {
		t.Fatal("DurationBuckets handed out the package's own edges")
	}
}

// TestTracingClientCountsConnectionReuse is the direct measurement, taken
// against a transport whose pooling the test controls: with keep-alives the
// second request onward reuses the connection the first opened, and with them
// off every request opens its own.
func TestTracingClientCountsConnectionReuse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)

	cases := []struct {
		name              string
		keepAlives        bool
		wantNew, wantUsed int64
	}{
		{"pooled", true, 1, 2},
		{"no keep-alives", false, 3, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var st stats
			base := &http.Client{Transport: &http.Transport{DisableKeepAlives: !c.keepAlives}}
			client := newTracingClient(base, &st)

			for range 3 {
				req, err := http.NewRequestWithContext(testCtx(t), http.MethodGet, srv.URL, nil)
				if err != nil {
					t.Fatalf("NewRequest: %v", err)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("Do: %v", err)
				}
				// Drained and closed, because a body left unread is a connection
				// the transport cannot put back — which would make this test pass
				// or fail for a reason that has nothing to do with what it pins.
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					t.Fatalf("read body: %v", err)
				}
				if err := resp.Body.Close(); err != nil {
					t.Fatalf("close body: %v", err)
				}
			}

			snap := st.Snapshot()
			if snap.ConnsNew != c.wantNew || snap.ConnsReused != c.wantUsed {
				t.Fatalf("new = %d reused = %d, want %d and %d",
					snap.ConnsNew, snap.ConnsReused, c.wantNew, c.wantUsed)
			}
		})
	}
}

// TestTracingClientDoesNotMutateTheRequest pins that the trace travels on a copy.
// The SDK hands the same request back for each retry attempt, so a wrapper that
// edited the original would leave a trace on it that outlived the call.
func TestTracingClientDoesNotMutateTheRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)

	var st stats
	client := newTracingClient(srv.Client(), &st)
	req, err := http.NewRequestWithContext(testCtx(t), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	ctx := req.Context()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_ = resp.Body.Close()
	if req.Context() != ctx {
		t.Fatal("Do replaced the context on the caller's request")
	}
}

// TestS3RecordsRequestsAndDurations pins the accounting end to end through the
// backend: every request is counted, the pool the SDK configured is left doing
// what it does — the second request here reuses the first's connection — and each
// operation lands in the distribution named for it and for how it ended.
func TestS3RecordsRequestsAndDurations(t *testing.T) {
	out := mkOutput(0x5a)
	body := "an output body"

	s, f := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = io.WriteString(w, body)
	})

	for range 2 {
		rc, size, err := s.GetObject(testCtx(t), out)
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		if size != int64(len(body)) {
			t.Fatalf("size = %d, want %d", size, len(body))
		}
		// Drained and closed so the connection goes back to the pool; the
		// toolchain reads the body it is handed too.
		if _, err := io.Copy(io.Discard, rc); err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
	}
	if got := len(f.all()); got != 2 {
		t.Fatalf("got %d requests, want 2", got)
	}

	snap := s.Stats()
	if total := snap.ConnsNew + snap.ConnsReused; total != 2 {
		t.Fatalf("counted %d requests, want the 2 the server saw", total)
	}
	if snap.ConnsNew != 1 || snap.ConnsReused != 1 {
		t.Fatalf("new = %d reused = %d, want one of each: the second request should "+
			"have reused the connection the first opened", snap.ConnsNew, snap.ConnsReused)
	}

	d := findOp(t, snap, "get_object", "ok")
	if d.Count != 2 {
		t.Fatalf("get_object/ok count = %d, want 2", d.Count)
	}
	if d.SumSeconds <= 0 {
		t.Fatalf("get_object/ok sum = %v seconds, want a positive duration", d.SumSeconds)
	}
	if hasOp(snap, "get_object", "error") {
		t.Fatalf("an error was recorded for two successful gets: %+v", snap.Ops)
	}
}

// TestS3RecordsMissesAndErrorsApart pins that the outcome label distinguishes
// the two, because they are different measurements: a miss is the tier answering
// quickly, and a failure is a latency that describes a timeout rather than the
// bucket.
func TestS3RecordsMissesAndErrorsApart(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		if r.Method == http.MethodPut {
			writeS3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
	})

	if _, _, err := s.GetAction(testCtx(t), mkAction(0xab)); err == nil {
		t.Fatal("GetAction against an absent key returned no error")
	}
	if err := s.PutAction(testCtx(t), mkAction(0xab), mkOutput(0x5a), time.Unix(0, 1)); err == nil {
		t.Fatal("PutAction against a failing bucket returned no error")
	}

	snap := s.Stats()
	if got := findOp(t, snap, "get_action", "miss").Count; got != 1 {
		t.Fatalf("get_action/miss count = %d, want 1", got)
	}
	if got := findOp(t, snap, "put_action", "error").Count; got != 1 {
		t.Fatalf("put_action/error count = %d, want 1", got)
	}
	if hasOp(snap, "get_action", "error") {
		t.Fatalf("an absent key was recorded as an error: %+v", snap.Ops)
	}
}

// TestS3DurationCoversTheRoundTrip pins that the recorded duration is the
// operation's own and not a constant. A server that takes a known minimum to
// answer must land above the buckets below that minimum — the assertion is a
// lower bound rather than an interval, because a test machine can always be slow
// but cannot make a call return before the handler has returned.
func TestS3DurationCoversTheRoundTrip(t *testing.T) {
	const held = 30 * time.Millisecond
	out := mkOutput(0x5a)

	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		time.Sleep(held)
		w.Header().Set("Content-Length", "0")
	})

	rc, _, err := s.GetObject(testCtx(t), out)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	_ = rc.Close()

	d := findOp(t, s.Stats(), "get_object", "ok")
	if d.SumSeconds < held.Seconds() {
		t.Fatalf("sum = %v seconds for a call the server held for %v", d.SumSeconds, held)
	}
	for i, edge := range DurationBuckets() {
		if edge >= held.Seconds() {
			break
		}
		if d.Buckets[i] != 0 {
			t.Fatalf("bucket le=%v holds %d observations, but the call could not "+
				"have finished that fast", edge, d.Buckets[i])
		}
	}
}

// BenchmarkObserve reports what recording one operation costs, which is the
// bound this accounting has to stay inside: uploads reach hundreds a second per
// process and every one of them records. Run with -benchmem; the number that
// matters is 0 B/op, because an allocation here is one the operation did not have
// to make before.
func BenchmarkObserve(b *testing.B) {
	var s stats
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		// A varying duration, so the walk over the bucket edges is not measured
		// against the one branch a predictor would learn.
		s.observe(opPutObject, nil, time.Duration(i%1000)*time.Millisecond)
	}
}
