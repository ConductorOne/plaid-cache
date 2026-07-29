// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"testing"
)

// TestFlushMetricsPersistsWhatHappened pins that counting and persisting are
// connected at all.
func TestFlushMetricsPersistsWhatHappened(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(1), mkOutput(1)
	tc.put(t, a, o, []byte("body"))
	if _, err := tc.cache.Get(t.Context(), a); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if err := tc.cache.FlushMetrics(); err != nil {
		t.Fatalf("FlushMetrics: %v", err)
	}
	got, _, err := tc.idx.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.Put != 1 || got.GetLocalHit != 1 {
		t.Fatalf("persisted %+v, want one put and one local hit", got)
	}
}

// TestFlushMetricsWritesOnlyTheDelta pins that a second flush does not count the
// same operations again.
//
// The counters are cumulative totals in memory, so persisting them as-is would
// double everything on every flush — and the flush runs once a minute for the
// life of the daemon, so the error would compound rather than merely be wrong.
func TestFlushMetricsWritesOnlyTheDelta(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(2), mkOutput(2)
	tc.put(t, a, o, []byte("body"))

	for i := 0; i < 3; i++ {
		if err := tc.cache.FlushMetrics(); err != nil {
			t.Fatalf("FlushMetrics %d: %v", i, err)
		}
	}
	got, _, err := tc.idx.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.Put != 1 {
		t.Fatalf("Put = %d after three flushes of one put, want 1", got.Put)
	}
}

// TestFlushMetricsCarriesOnAfterAFailedFlush pins that a failure loses nothing.
//
// The delta is measured against what was last persisted rather than against
// zero, so counts that could not be written stay pending and the next flush
// carries them.
func TestFlushMetricsCarriesOnAfterAFailedFlush(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(3), mkOutput(3)
	tc.put(t, a, o, []byte("body"))

	// A closed index fails the write without losing the counters.
	if err := tc.idx.Close(); err != nil {
		t.Fatalf("closing the index: %v", err)
	}
	if err := tc.cache.FlushMetrics(); err == nil {
		t.Fatal("FlushMetrics over a closed index reported success")
	}
	if got := tc.cache.Metrics().Put; got != 1 {
		t.Fatalf("Put = %d after a failed flush, want the count still pending", got)
	}
}

// TestCloseFlushes pins the last chance to persist.
//
// A plugin invocation lasts one build and the daemon exits on an idle timeout, so
// for short-lived processes Close is where nearly all the counting would
// otherwise be lost.
func TestCloseFlushes(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(4), mkOutput(4)
	tc.put(t, a, o, []byte("body"))

	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, _, err := tc.idx.TotalActivity()
	if err != nil {
		t.Fatalf("TotalActivity: %v", err)
	}
	if got.Put != 1 {
		t.Fatalf("Put = %d after Close, want 1", got.Put)
	}
}
