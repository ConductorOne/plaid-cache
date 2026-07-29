// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"os"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// age backdates a body past the settle window.
//
// This rather than a clock seam in the production code: the reconcile pass runs
// from eviction, which has no way to be handed a clock, so a test that injected
// one would exercise a path the daemon never takes. An older mtime is what the
// real code reads, and it is what a body actually has by the time this matters.
func age(t *testing.T, tc *testCache, o ids.OutputID) {
	t.Helper()
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(tc.blobs.Path(o), old, old); err != nil {
		t.Fatalf("backdating the body: %v", err)
	}
}

// inflate plants a wrong recorded cost, which is what a write on a compressing
// filesystem leaves behind: the figure taken at write time is the logical length,
// and the real allocation is a fraction of it.
func inflate(t *testing.T, tc *testCache, o ids.OutputID, to int64) {
	t.Helper()
	if applied, _, err := tc.idx.Remeasure(map[ids.OutputID]int64{o: to}); err != nil {
		t.Fatalf("planting an inflated cost: %v", err)
	} else if applied != 1 {
		t.Fatalf("planting an inflated cost applied %d corrections, want 1", applied)
	}
}

// TestReconcileCorrectsAnInflatedCost is the fix for issue #5.
//
// The recorded figure is taken when the body is written, which on ZFS is before
// the filesystem has allocated anything, so it is the logical length. On a
// compressing dataset it stays the logical length forever, and the budget evicts
// at a fraction of the ceiling it was given.
func TestReconcileCorrectsAnInflatedCost(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(1), mkOutput(1)
	tc.put(t, a, o, []byte("a body whose recorded cost will be wrong"))

	age(t, tc, o)
	truth, ok, err := tc.blobs.Measure(o, time.Now())
	if err != nil || !ok {
		t.Fatalf("Measure: %v (settled=%v)", err, ok)
	}
	inflate(t, tc, o, truth*100)

	before, _ := tc.idx.Stats()
	res, err := tc.cache.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Corrected != 1 {
		t.Fatalf("corrected %d objects, want 1 (pending=%d missing=%d)", res.Corrected, res.Pending, res.Missing)
	}
	after, _ := tc.idx.Stats()
	if after.DiskBytes != truth {
		t.Fatalf("recorded %d bytes after reconciling, want the measured %d", after.DiskBytes, truth)
	}
	if after.DiskBytes >= before.DiskBytes {
		t.Fatalf("recorded size did not fall: %d -> %d", before.DiskBytes, after.DiskBytes)
	}
}

// TestReconcileLeavesFreshWritesAlone pins the protection that makes this safe.
//
// A body inside the settle window reports almost no allocation, and writing that
// figure back would undercount a whole cache by orders of magnitude — the failure
// the maximum at write time exists to prevent. It must be counted as pending and
// left for a later pass.
func TestReconcileLeavesFreshWritesAlone(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(2), mkOutput(2)
	tc.put(t, a, o, []byte("a body written just now"))

	before, _ := tc.idx.Stats()
	res, err := tc.cache.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Pending != 1 {
		t.Fatalf("pending = %d, want 1 (corrected=%d)", res.Pending, res.Corrected)
	}
	if res.Corrected != 0 {
		t.Fatalf("corrected %d objects inside the settle window", res.Corrected)
	}
	after, _ := tc.idx.Stats()
	if after.DiskBytes != before.DiskBytes {
		t.Fatalf("recorded size moved on a fresh write: %d -> %d", before.DiskBytes, after.DiskBytes)
	}
}

// TestReconcileWillNotCorrectFromAnUnsettledMeasurement pins that a wrong record
// on a still-fresh body waits rather than being "corrected" from a figure that
// cannot be trusted.
//
// This is the case that makes the settle guard load-bearing rather than
// decorative: re-measuring a fresh body reproduces the same safe overestimate the
// write recorded, so a body with an accurate record shows no difference either
// way. A body with an inaccurate record does — and correcting it here would mean
// writing an allocation figure the filesystem has not computed yet.
func TestReconcileWillNotCorrectFromAnUnsettledMeasurement(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(7), mkOutput(7)
	tc.put(t, a, o, []byte("a body whose record is wrong and whose allocation is not settled"))

	// Deliberately not aged: the body is fresh.
	planted := int64(999_999)
	inflate(t, tc, o, planted)
	before, _ := tc.idx.Stats()

	res, err := tc.cache.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Pending != 1 {
		t.Fatalf("pending = %d, want 1", res.Pending)
	}
	if res.Corrected != 0 {
		t.Fatalf("corrected %d objects from an unsettled measurement", res.Corrected)
	}
	after, _ := tc.idx.Stats()
	if after.DiskBytes != before.DiskBytes {
		t.Fatalf("recorded size moved on an unsettled body: %d -> %d", before.DiskBytes, after.DiskBytes)
	}
	if after.DiskBytes != planted {
		t.Fatalf("recorded %d, want the planted %d left alone until it can be measured", after.DiskBytes, planted)
	}
}

// TestEvictionReconcilesWhenTheBudgetLooksTight is the reported symptom, end to
// end: a ceiling above the real cost but below the recorded one must prune
// nothing.
func TestEvictionReconcilesWhenTheBudgetLooksTight(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(3), mkOutput(3)
	tc.put(t, a, o, []byte("a body that should survive"))

	age(t, tc, o)
	truth, _, err := tc.blobs.Measure(o, time.Now())
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	inflate(t, tc, o, truth*100)

	// Between the truth and the fiction: the entry fits, and only the inflated
	// accounting says otherwise.
	ceiling := truth * 50
	res, err := tc.cache.EvictWith(t.Context(), ceiling, time.Hour)
	if err != nil {
		t.Fatalf("EvictWith: %v", err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d actions with the real cost far under the ceiling", res.ActionsPruned)
	}
	if _, ok, err := tc.idx.Get(a); err != nil || !ok {
		t.Fatalf("the entry was evicted: ok=%v err=%v", ok, err)
	}
}

// TestEvictionSkipsReconcilingWhenThereIsRoom pins that the measurement is not
// paid for when it cannot matter. Below the threshold a pass over every body
// would be pure overhead on a path that runs beside every build.
func TestEvictionSkipsReconcilingWhenThereIsRoom(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(4), mkOutput(4)
	tc.put(t, a, o, []byte("a body with plenty of room"))

	age(t, tc, o)
	truth, _, err := tc.blobs.Measure(o, time.Now())
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	inflate(t, tc, o, truth*100)
	inflated, _ := tc.idx.Stats()

	// A ceiling far above even the inflated figure: nothing is at stake, so the
	// accounting should be left as it is.
	if _, err := tc.cache.EvictWith(t.Context(), inflated.DiskBytes*100, time.Hour); err != nil {
		t.Fatalf("EvictWith: %v", err)
	}
	after, _ := tc.idx.Stats()
	if after.DiskBytes != inflated.DiskBytes {
		t.Fatalf("re-measured with room to spare: %d -> %d", inflated.DiskBytes, after.DiskBytes)
	}
}

// TestReconcileCountsAMissingBody pins that a record whose body is gone is
// reported rather than corrected. Giving it a cost would make a dangling record
// look healthy; eviction's repair path owns that case.
func TestReconcileCountsAMissingBody(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(5), mkOutput(5)
	tc.put(t, a, o, []byte("a body about to disappear"))
	age(t, tc, o)
	if err := tc.blobs.Remove(o); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	res, err := tc.cache.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Missing != 1 || res.Corrected != 0 {
		t.Fatalf("missing=%d corrected=%d, want 1 and 0", res.Missing, res.Corrected)
	}
	_ = a
}

// TestReconcileIsIdempotent pins that a second pass over correct accounting
// writes nothing, since it runs on a ticker for as long as the budget is tight.
func TestReconcileIsIdempotent(t *testing.T) {
	tc := newTestCache(t)
	tc.put(t, mkAction(6), mkOutput(6), []byte("a body"))
	age(t, tc, mkOutput(6))
	if _, err := tc.cache.Reconcile(t.Context()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	res, err := tc.cache.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res.Corrected != 0 {
		t.Fatalf("second pass corrected %d objects, want 0", res.Corrected)
	}
}

// TestEvictNowMeasuresRegardlessOfPressure pins the manual lever.
//
// The automatic path deliberately skips measuring when there is room, so a cache
// well under its ceiling can carry a wrong recorded size indefinitely — and
// status would keep reporting it. An operator running gc is asking for the sweep
// and for the numbers it is decided on, so this path measures whatever the
// pressure.
func TestEvictNowMeasuresRegardlessOfPressure(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(8), mkOutput(8)
	tc.put(t, a, o, []byte("a body under a ceiling it is nowhere near"))
	age(t, tc, o)

	truth, _, err := tc.blobs.Measure(o, time.Now())
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	inflate(t, tc, o, truth*100)
	inflated, _ := tc.idx.Stats()

	// A ceiling so far above the inflated figure that the automatic path would
	// not look.
	res, rec, err := tc.cache.EvictNow(t.Context(), inflated.DiskBytes*100, time.Hour)
	if err != nil {
		t.Fatalf("EvictNow: %v", err)
	}
	if rec.Corrected != 1 {
		t.Fatalf("corrected %d objects, want 1 — gc did not measure", rec.Corrected)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d actions with room to spare", res.ActionsPruned)
	}
	after, _ := tc.idx.Stats()
	if after.DiskBytes != truth {
		t.Fatalf("recorded %d after gc, want the measured %d", after.DiskBytes, truth)
	}
}
