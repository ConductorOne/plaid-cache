// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// collector records the orphans an eviction pass reports.
type collector struct {
	seen []ids.OutputID
}

// fn returns the onOrphan callback.
func (c *collector) fn(o ids.OutputID) error {
	c.seen = append(c.seen, o)
	return nil
}

// TestEvictByTTLOnly pins the TTL constraint in isolation: entries older than
// the cutoff go, entries newer stay, and a disabled size constraint does not
// prune a single extra byte.
func TestEvictByTTLOnly(t *testing.T) {
	ix := openTemp(t)
	hour := int64(time.Hour)
	put(t, ix, mkAction(1), mkOutput(1), baseTime-10*hour, 100)
	put(t, ix, mkAction(2), mkOutput(2), baseTime-5*hour, 100)
	put(t, ix, mkAction(3), mkOutput(3), baseTime-1*hour, 100)

	var c collector
	res, err := ix.Evict(t.Context(), 0, 6*time.Hour, baseTime, c.fn)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 1 || res.ObjectsPruned != 1 || res.BytesFreed != 100 {
		t.Fatalf("EvictResult = %+v, want 1 action / 1 object / 100 bytes", res)
	}
	if len(c.seen) != 1 || c.seen[0] != mkOutput(1) {
		t.Fatalf("orphans = %v, want exactly the oldest output", c.seen)
	}
	wantStats(t, ix, 2, 2, 200)

	if _, ok, _ := ix.Get(mkAction(1)); ok {
		t.Fatal("the expired action survived")
	}
	if _, ok, _ := ix.Get(mkAction(2)); !ok {
		t.Fatal("an in-TTL action was evicted")
	}
}

// TestEvictBySizeOnly pins the size constraint in isolation: pruning proceeds
// oldest-first and stops the moment the budget is met, never one entry later.
func TestEvictBySizeOnly(t *testing.T) {
	ix := openTemp(t)
	for i := range 4 {
		put(t, ix, mkAction(byte(i)), mkOutput(byte(i)), baseTime+int64(i), 100)
	}
	wantStats(t, ix, 4, 4, 400)

	var c collector
	res, err := ix.Evict(t.Context(), 250, 0, baseTime+100, c.fn)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 2 || res.BytesFreed != 200 {
		t.Fatalf("EvictResult = %+v, want 2 actions / 200 bytes", res)
	}
	wantStats(t, ix, 2, 2, 200)

	// Oldest-first: 0 and 1 go, 2 and 3 stay.
	for _, gone := range []byte{0, 1} {
		if _, ok, _ := ix.Get(mkAction(gone)); ok {
			t.Fatalf("action %d should have been evicted", gone)
		}
	}
	for _, kept := range []byte{2, 3} {
		if _, ok, _ := ix.Get(mkAction(kept)); !ok {
			t.Fatalf("action %d should have survived", kept)
		}
	}
}

// TestEvictBySizeAndTTLTogether pins that one pass satisfies BOTH constraints:
// the TTL cutoff removes the expired entries and the size ceiling keeps going
// past it until the budget is met.
func TestEvictBySizeAndTTLTogether(t *testing.T) {
	ix := openTemp(t)
	hour := int64(time.Hour)
	put(t, ix, mkAction(1), mkOutput(1), baseTime-10*hour, 100)
	put(t, ix, mkAction(2), mkOutput(2), baseTime-9*hour, 100)
	put(t, ix, mkAction(3), mkOutput(3), baseTime-1*hour, 100)
	put(t, ix, mkAction(4), mkOutput(4), baseTime, 100)

	// TTL alone would prune 2 (leaving 200); the 150 ceiling forces a third.
	res, err := ix.Evict(t.Context(), 150, 6*time.Hour, baseTime, nil)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 3 || res.BytesFreed != 300 {
		t.Fatalf("EvictResult = %+v, want 3 actions / 300 bytes", res)
	}
	wantStats(t, ix, 1, 1, 100)
	if _, ok, _ := ix.Get(mkAction(4)); !ok {
		t.Fatal("the newest action should have survived")
	}
}

// TestEvictBothConstraintsDisabled pins that ttl <= 0 and maxBytes <= 0 mean
// "do not prune live entries", however old or large the index is.
func TestEvictBothConstraintsDisabled(t *testing.T) {
	ix := openTemp(t)
	put(t, ix, mkAction(1), mkOutput(1), 1, 1_000_000)

	res, err := ix.Evict(t.Context(), 0, 0, baseTime, nil)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 0 || res.BytesFreed != 0 {
		t.Fatalf("EvictResult = %+v, want an empty pass", res)
	}
	wantStats(t, ix, 1, 1, 1_000_000)
}

// TestEvictNoOpWhenConstraintsAlreadyHold pins that a satisfied cache costs
// nothing: the eviction ticker fires every minute and must not churn.
func TestEvictNoOpWhenConstraintsAlreadyHold(t *testing.T) {
	ix := openTemp(t)
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 100)

	res, err := ix.Evict(t.Context(), 10_000, time.Hour, baseTime+1, nil)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 0 || res.ObjectsPruned != 0 || res.BytesFreed != 0 {
		t.Fatalf("EvictResult = %+v, want an empty pass", res)
	}
	wantStats(t, ix, 1, 1, 100)
}

// TestEvictSharedOutputDecrementsWithinOneBatch pins the subtlest bug in the
// package: two actions sharing an output are evicted in the SAME chunk, so both
// decrements have to be applied to the in-flight refcount. Reading the
// committed value twice would apply both to the same base, lose one, and leak
// the body forever with its bytes stuck in the budget.
func TestEvictSharedOutputDecrementsWithinOneBatch(t *testing.T) {
	ix := openTemp(t)
	shared, solo := mkOutput(1), mkOutput(2)
	put(t, ix, mkAction(1), shared, baseTime+1, 1000)
	put(t, ix, mkAction(2), shared, baseTime+2, 1000)
	put(t, ix, mkAction(3), solo, baseTime+3, 500)
	wantStats(t, ix, 3, 2, 1500)

	var c collector
	res, err := ix.Evict(t.Context(), 1200, 0, baseTime+10, c.fn)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	// Evicting action 1 frees nothing; only action 2 releases the body.
	if res.ActionsPruned != 2 {
		t.Fatalf("ActionsPruned = %d, want 2", res.ActionsPruned)
	}
	if res.ObjectsPruned != 1 || res.BytesFreed != 1000 {
		t.Fatalf("EvictResult = %+v, want 1 object / 1000 bytes", res)
	}
	if len(c.seen) != 1 {
		t.Fatalf("onOrphan called %d times, want exactly 1", len(c.seen))
	}
	if c.seen[0] != shared {
		t.Fatalf("orphan = %x, want the shared output %x", c.seen[0][:4], shared[:4])
	}
	wantStats(t, ix, 1, 1, 500)

	// The surviving action must still resolve.
	if _, ok, _ := ix.Get(mkAction(3)); !ok {
		t.Fatal("the newest action was evicted")
	}
}

// TestEvictLeavesNoStaleLRUKeys pins that eviction removes the secondary-index
// key along with the entry. Orphaned 'l' keys would make later passes revisit
// actions that no longer exist.
func TestEvictLeavesNoStaleLRUKeys(t *testing.T) {
	ix := openTemp(t)
	for i := range 6 {
		put(t, ix, mkAction(byte(i)), mkOutput(byte(i)), baseTime+int64(i), 100)
	}
	if _, err := ix.Evict(t.Context(), 200, 0, baseTime+100, nil); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	entries := countPrefix(t, ix, prefixEntry)
	lru := countPrefix(t, ix, prefixLRU)
	if entries != lru {
		t.Fatalf("%d entries but %d lru keys; the secondary index drifted", entries, lru)
	}
	if entries != 2 {
		t.Fatalf("entries = %d, want 2", entries)
	}
}

// TestEvictChunksLargePass pins that a pass larger than one chunk completes.
// The chunk boundary is where a half-applied refcount or a lost counter delta
// would show up.
func TestEvictChunksLargePass(t *testing.T) {
	ix := openTemp(t)
	const n = evictChunkSize*2 + 137
	for i := range n {
		put(t, ix, mkActionN(i), mkOutputN(i), baseTime+int64(i), 10)
	}
	wantStats(t, ix, n, n, n*10)

	var c collector
	res, err := ix.Evict(t.Context(), 0, time.Nanosecond, baseTime+int64(n)+1, c.fn)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != n || res.ObjectsPruned != n || res.BytesFreed != n*10 {
		t.Fatalf("EvictResult = %+v, want everything pruned (n=%d)", res, n)
	}
	if len(c.seen) != n {
		t.Fatalf("onOrphan called %d times, want %d", len(c.seen), n)
	}
	wantStats(t, ix, 0, 0, 0)
	if got := countPrefix(t, ix, prefixLRU); got != 0 {
		t.Fatalf("%d lru keys survived a full eviction", got)
	}
}

// TestEvictHonorsContextCancellation pins that a cancelled pass stops between
// batches and surfaces the cancellation, so daemon shutdown is not blocked by a
// multi-million-entry sweep.
func TestEvictHonorsContextCancellation(t *testing.T) {
	ix := openTemp(t)
	for i := range 10 {
		put(t, ix, mkAction(byte(i)), mkOutput(byte(i)), baseTime+int64(i), 100)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	res, err := ix.Evict(ctx, 100, 0, baseTime+100, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Evict error = %v, want one wrapping context.Canceled", err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("ActionsPruned = %d, want 0 from a pre-cancelled pass", res.ActionsPruned)
	}
	wantStats(t, ix, 10, 10, 1000)
}

// TestEvictOnOrphanErrorAbortsButKeepsCommittedWork pins the documented
// failure mode: the callback runs after its batch commits, so an error stops
// the pass without rolling back. The alternative — deleting bodies before the
// index — would leave a hit pointing at a missing file and break the build.
func TestEvictOnOrphanErrorAbortsButKeepsCommittedWork(t *testing.T) {
	ix := openTemp(t)
	for i := range 4 {
		put(t, ix, mkAction(byte(i)), mkOutput(byte(i)), baseTime+int64(i), 100)
	}

	boom := errors.New("blob store is unwritable")
	calls := 0
	res, err := ix.Evict(t.Context(), 0, time.Nanosecond, baseTime+1000, func(ids.OutputID) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Evict error = %v, want one wrapping the callback error", err)
	}
	if calls != 1 {
		t.Fatalf("onOrphan called %d times, want 1 before aborting", calls)
	}
	// The whole chunk committed before the callback ran, so the index is empty
	// even though only one orphan was reported.
	if res.ActionsPruned != 4 {
		t.Fatalf("ActionsPruned = %d, want the committed chunk of 4", res.ActionsPruned)
	}
	wantStats(t, ix, 0, 0, 0)
}

// TestEvictReportsElapsed pins that EvictResult carries a duration even on the
// error path, so the daemon can log how long a failed sweep ran.
func TestEvictReportsElapsed(t *testing.T) {
	ix := openTemp(t)
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 100)
	res, err := ix.Evict(t.Context(), 0, time.Nanosecond, baseTime+1000, nil)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.Elapsed <= 0 {
		t.Fatalf("Elapsed = %v, want a positive duration", res.Elapsed)
	}
}

// TestEvictSurvivesReopenWithCorrectCounters pins that the counters after an
// eviction pass are the ones a rescan would produce. If eviction and the
// rebuild ever disagree, the byte budget is wrong in one of the two, and the
// disagreement only shows up after a crash.
func TestEvictSurvivesReopenWithCorrectCounters(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	shared := mkOutput(200)
	put(t, ix, mkAction(1), shared, baseTime+1, 1000)
	put(t, ix, mkAction(2), shared, baseTime+2, 1000)
	for i := range 5 {
		put(t, ix, mkAction(byte(i+10)), mkOutput(byte(i+10)), baseTime+int64(i+3), 100)
	}
	wantStats(t, ix, 7, 6, 1500)

	if _, err := ix.Evict(t.Context(), 700, 0, baseTime+100, nil); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	before, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if err := ix.closeAbruptly(); err != nil {
		t.Fatalf("closeAbruptly: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	after, err := reopened.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if before != after {
		t.Fatalf("counters after rebuild = %+v, want the pre-crash %+v", after, before)
	}
}
