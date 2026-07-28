// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// baseTime is a fixed instant so timestamp arithmetic in tests is exact.
const baseTime = int64(1_700_000_000_000_000_000)

// openTemp opens an index in a fresh directory and closes it at test end.
func openTemp(t *testing.T) *Index {
	t.Helper()
	ix, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix
}

// put is a terse Put for tests.
func put(t *testing.T, ix *Index, a ids.ActionID, o ids.OutputID, lastUsed, diskBytes int64) {
	t.Helper()
	e := Entry{OutputID: o, Size: diskBytes, CreatedAt: baseTime, LastUsedAt: lastUsed}
	if err := ix.Put(a, e, diskBytes); err != nil {
		t.Fatalf("Put(%x): %v", a[:4], err)
	}
}

// wantStats asserts the three public counters at once.
func wantStats(t *testing.T, ix *Index, actions, objects, diskBytes int64) {
	t.Helper()
	s, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Actions != actions || s.Objects != objects || s.DiskBytes != diskBytes {
		t.Fatalf("Stats = %+v, want {Actions:%d Objects:%d DiskBytes:%d}", s, actions, objects, diskBytes)
	}
}

// hasKey reports whether a raw key exists, for asserting on the secondary index.
func hasKey(t *testing.T, ix *Index, key []byte) bool {
	t.Helper()
	_, ok, err := ix.getCopy(key)
	if err != nil {
		t.Fatalf("getCopy: %v", err)
	}
	return ok
}

// TestGetMissIsNotAnError pins the contract that a cache miss is
// (Entry{}, false, nil) — never an error, and never a non-zero entry.
func TestGetMissIsNotAnError(t *testing.T) {
	ix := openTemp(t)
	e, ok, err := ix.Get(mkAction(1))
	if err != nil {
		t.Fatalf("Get on empty index: %v", err)
	}
	if ok {
		t.Fatal("Get reported a hit on an empty index")
	}
	if e != (Entry{}) {
		t.Fatalf("Get miss returned %+v, want zero Entry", e)
	}
}

// TestPutGetRoundTrip pins that a stored entry reads back field-for-field and
// that the counters reflect exactly one action and one object.
func TestPutGetRoundTrip(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	want := Entry{OutputID: o, Size: 4096, CreatedAt: baseTime, LastUsedAt: baseTime + 5}
	if err := ix.Put(a, want, 8192); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := ix.Get(a)
	if err != nil || !ok {
		t.Fatalf("Get = (_, %v, %v), want a hit", ok, err)
	}
	if got != want {
		t.Fatalf("Get = %+v, want %+v", got, want)
	}
	wantStats(t, ix, 1, 1, 8192)
}

// TestPutRejectsNegativeTimestamp pins the guard on the LRU keyspace: a
// negative nano sorts past every real timestamp in a big-endian key, which
// would make the entry permanently un-evictable.
func TestPutRejectsNegativeTimestamp(t *testing.T) {
	ix := openTemp(t)
	bad := Entry{OutputID: mkOutput(1), Size: 1, CreatedAt: baseTime, LastUsedAt: -1}
	if err := ix.Put(mkAction(1), bad, 1); err == nil {
		t.Fatal("Put accepted a negative LastUsedAt")
	}
	if err := ix.Put(mkAction(1), Entry{OutputID: mkOutput(1)}, -5); err == nil {
		t.Fatal("Put accepted negative diskBytes")
	}
	wantStats(t, ix, 0, 0, 0)
}

// TestRefcountSharedOutputSurvivesFirstDelete pins the single most important
// invariant in the package: two actions sharing one output must keep the body
// alive until the second delete, and the orphan must be reported exactly once
// with its bytes leaving the budget exactly once.
func TestRefcountSharedOutputSurvivesFirstDelete(t *testing.T) {
	ix := openTemp(t)
	a1, a2, o := mkAction(1), mkAction(2), mkOutput(9)

	put(t, ix, a1, o, baseTime, 4096)
	put(t, ix, a2, o, baseTime+1, 4096)

	// The body is counted once, not once per action.
	wantStats(t, ix, 2, 1, 4096)

	orphan, isOrphan, err := ix.Delete(a1)
	if err != nil {
		t.Fatalf("Delete(a1): %v", err)
	}
	if isOrphan {
		t.Fatalf("Delete(a1) reported orphan %x while a2 still references it", orphan[:4])
	}
	wantStats(t, ix, 1, 1, 4096)

	// a2 must still resolve to the shared body.
	if _, ok, err := ix.Get(a2); err != nil || !ok {
		t.Fatalf("Get(a2) after deleting a1 = (_, %v, %v), want a hit", ok, err)
	}

	orphan, isOrphan, err = ix.Delete(a2)
	if err != nil {
		t.Fatalf("Delete(a2): %v", err)
	}
	if !isOrphan {
		t.Fatal("Delete(a2) did not report the now-unreferenced body as an orphan")
	}
	if orphan != o {
		t.Fatalf("orphan = %x, want %x", orphan[:4], o[:4])
	}
	wantStats(t, ix, 0, 0, 0)
}

// TestDeleteIsIdempotentAndDoesNotDoubleCount pins that deleting an absent
// action is a no-op rather than a second decrement, which would drive the
// refcount and the byte budget negative.
func TestDeleteIsIdempotentAndDoesNotDoubleCount(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 1024)

	if _, isOrphan, err := ix.Delete(a); err != nil || !isOrphan {
		t.Fatalf("first Delete = (_, %v, %v), want an orphan", isOrphan, err)
	}
	orphan, isOrphan, err := ix.Delete(a)
	if err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if isOrphan {
		t.Fatalf("second Delete reported orphan %x for an already-deleted action", orphan[:4])
	}
	wantStats(t, ix, 0, 0, 0)
}

// TestPutReplaceSameOutputDoesNotChurnRefcount pins that re-putting an action
// with an unchanged output leaves the refcount and the budget untouched. A
// naive decrement-then-increment would pass through zero and momentarily look
// like an orphan.
func TestPutReplaceSameOutputDoesNotChurnRefcount(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 2048)
	put(t, ix, a, o, baseTime+1000, 2048)

	wantStats(t, ix, 1, 1, 2048)
	if ix.zeroRefs.Load() != 0 {
		t.Fatalf("zeroRefs = %d, want 0", ix.zeroRefs.Load())
	}
	r, ok, err := ix.loadObjRef(o)
	if err != nil || !ok {
		t.Fatalf("loadObjRef = (_, %v, %v), want the record", ok, err)
	}
	if r.Refs != 1 {
		t.Fatalf("Refs = %d, want 1", r.Refs)
	}
	// The replacement must have moved the LRU key, not left both behind.
	if hasKey(t, ix, lruKey(baseTime, a)) {
		t.Fatal("stale lru key survived a Put replacement")
	}
	if !hasKey(t, ix, lruKey(baseTime+1000, a)) {
		t.Fatal("Put replacement did not write the new lru key")
	}
}

// TestPutReplaceDifferentOutputRetainsBytesUntilReaped pins the asymmetry
// between Put and Delete: Put cannot hand an orphan back to the caller, so the
// stranded record is kept at Refs == 0 and its bytes stay in the budget. Taking
// them out while the file remained on disk is exactly how a byte budget drifts
// permanently wrong.
func TestPutReplaceDifferentOutputRetainsBytesUntilReaped(t *testing.T) {
	ix := openTemp(t)
	a, oldOut, newOut := mkAction(1), mkOutput(1), mkOutput(2)

	put(t, ix, a, oldOut, baseTime, 1000)
	put(t, ix, a, newOut, baseTime+1, 500)

	// One action, but both bodies are still on disk and both must be counted.
	wantStats(t, ix, 1, 2, 1500)
	if got := ix.zeroRefs.Load(); got != 1 {
		t.Fatalf("zeroRefs = %d, want 1", got)
	}
	r, ok, err := ix.loadObjRef(oldOut)
	if err != nil || !ok {
		t.Fatalf("stranded record missing: ok=%v err=%v", ok, err)
	}
	if r.Refs != 0 || r.DiskBytes != 1000 {
		t.Fatalf("stranded record = %+v, want {Refs:0 DiskBytes:1000}", r)
	}

	// An eviction pass with both constraints disabled still reaps pure garbage.
	var reaped []ids.OutputID
	res, err := ix.Evict(t.Context(), 0, 0, baseTime+2, func(o ids.OutputID) error {
		reaped = append(reaped, o)
		return nil
	})
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if len(reaped) != 1 || reaped[0] != oldOut {
		t.Fatalf("reaped = %v, want exactly [%x]", reaped, oldOut[:4])
	}
	if res.ObjectsPruned != 1 || res.BytesFreed != 1000 || res.ActionsPruned != 0 {
		t.Fatalf("EvictResult = %+v, want 0 actions / 1 object / 1000 bytes", res)
	}
	wantStats(t, ix, 1, 1, 500)
	if got := ix.zeroRefs.Load(); got != 0 {
		t.Fatalf("zeroRefs after reap = %d, want 0", got)
	}
}

// TestPutResurrectsZeroRefRecord pins that pointing an action back at a
// stranded body reclaims it instead of double-counting its bytes or leaving it
// queued for reaping.
func TestPutResurrectsZeroRefRecord(t *testing.T) {
	ix := openTemp(t)
	a1, a2 := mkAction(1), mkAction(2)
	oldOut, newOut := mkOutput(1), mkOutput(2)

	put(t, ix, a1, oldOut, baseTime, 1000)
	put(t, ix, a1, newOut, baseTime+1, 500) // strands oldOut at Refs == 0
	put(t, ix, a2, oldOut, baseTime+2, 1000)

	wantStats(t, ix, 2, 2, 1500)
	if got := ix.zeroRefs.Load(); got != 0 {
		t.Fatalf("zeroRefs = %d, want 0 after resurrection", got)
	}
	r, _, err := ix.loadObjRef(oldOut)
	if err != nil {
		t.Fatalf("loadObjRef: %v", err)
	}
	if r.Refs != 1 || r.DiskBytes != 1000 {
		t.Fatalf("resurrected record = %+v, want {Refs:1 DiskBytes:1000}", r)
	}

	// Nothing is garbage now, so an eviction pass must free nothing.
	res, err := ix.Evict(t.Context(), 0, 0, baseTime+3, nil)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ObjectsPruned != 0 || res.BytesFreed != 0 {
		t.Fatalf("EvictResult = %+v, want an empty pass", res)
	}
	wantStats(t, ix, 2, 2, 1500)
}

// TestTouchSkipsWriteInsideGranularity pins the relatime optimization on the
// hot read path: within the window Touch must not write at all, and must leave
// both the entry and its LRU key exactly as they were.
func TestTouchSkipsWriteInsideGranularity(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 1024)

	now := baseTime + int64(30*time.Minute)
	wrote, err := ix.Touch(a, now, time.Hour)
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if wrote {
		t.Fatal("Touch wrote inside the granularity window")
	}
	e, _, err := ix.Get(a)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.LastUsedAt != baseTime {
		t.Fatalf("LastUsedAt = %d, want it unchanged at %d", e.LastUsedAt, baseTime)
	}
	if !hasKey(t, ix, lruKey(baseTime, a)) {
		t.Fatal("Touch disturbed the lru key despite not writing")
	}
}

// TestTouchRekeysLRUOutsideGranularity pins that when Touch does write, it
// moves the secondary-index key in the same batch. A leftover old key would
// make eviction revisit an action at a stale position in the scan.
func TestTouchRekeysLRUOutsideGranularity(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 1024)

	now := baseTime + int64(2*time.Hour)
	wrote, err := ix.Touch(a, now, time.Hour)
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if !wrote {
		t.Fatal("Touch skipped a write outside the granularity window")
	}
	e, _, err := ix.Get(a)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.LastUsedAt != now {
		t.Fatalf("LastUsedAt = %d, want %d", e.LastUsedAt, now)
	}
	if hasKey(t, ix, lruKey(baseTime, a)) {
		t.Fatal("Touch left the stale lru key behind")
	}
	if !hasKey(t, ix, lruKey(now, a)) {
		t.Fatal("Touch did not insert the new lru key")
	}
	// Touching must never change what the action maps to or what it costs.
	wantStats(t, ix, 1, 1, 1024)
}

// TestTouchZeroGranularityAlwaysWrites pins that a disabled window makes every
// hit a write, which is the behaviour a caller asks for with granularity <= 0.
func TestTouchZeroGranularityAlwaysWrites(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 1024)

	wrote, err := ix.Touch(a, baseTime+1, 0)
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if !wrote {
		t.Fatal("Touch with granularity 0 did not write")
	}
}

// TestTouchNeverMovesTimeBackwards pins that a stale clock cannot rewind
// LastUsedAt. The stored timestamp is the only handle on the existing LRU key,
// so rewinding it would strand that key forever.
func TestTouchNeverMovesTimeBackwards(t *testing.T) {
	ix := openTemp(t)
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 1024)

	wrote, err := ix.Touch(a, baseTime-int64(time.Hour), 0)
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if wrote {
		t.Fatal("Touch moved LastUsedAt backwards")
	}
	if !hasKey(t, ix, lruKey(baseTime, a)) {
		t.Fatal("lru key was disturbed by a backwards Touch")
	}
}

// TestTouchMissIsNotAnError pins that touching an absent action reports "did
// not write" rather than failing the caller's hot path.
func TestTouchMissIsNotAnError(t *testing.T) {
	ix := openTemp(t)
	wrote, err := ix.Touch(mkAction(7), baseTime, time.Hour)
	if err != nil {
		t.Fatalf("Touch on a miss: %v", err)
	}
	if wrote {
		t.Fatal("Touch reported a write for an absent action")
	}
}

// TestOpenSecondProcessGetsErrLocked pins the arbitration the daemon spawn race
// depends on: exactly one holder of the directory, and a loser that can
// recognise why it lost.
func TestOpenSecondProcessGetsErrLocked(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer func() { _ = first.Close() }()

	second, err := Open(dir)
	if err == nil {
		_ = second.Close()
		t.Fatal("second Open succeeded; the directory lock is not exclusive")
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open error = %v, want one wrapping ErrLocked", err)
	}
}

// TestOpenAfterCloseSucceeds pins that Close actually releases the lock, so a
// restarted daemon can take the index over.
func TestOpenAfterCloseSucceeds(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCloseIsIdempotent pins that a double Close — a shutdown path racing a
// signal handler — returns the same result instead of panicking on a closed
// channel or a closed database.
func TestCloseIsIdempotent(t *testing.T) {
	ix, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestOperationsAfterCloseReturnErrClosed pins that a late request gets a clean
// error rather than a panic from Pebble.
func TestOperationsAfterCloseReturnErrClosed(t *testing.T) {
	ix, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	a := mkAction(1)
	if _, _, err := ix.Get(a); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if err := ix.Put(a, Entry{OutputID: mkOutput(1)}, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close = %v, want ErrClosed", err)
	}
	if _, err := ix.Touch(a, baseTime, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Touch after Close = %v, want ErrClosed", err)
	}
	if _, _, err := ix.Delete(a); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close = %v, want ErrClosed", err)
	}
	if _, err := ix.Evict(t.Context(), 0, 0, baseTime, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Evict after Close = %v, want ErrClosed", err)
	}
}

// TestCleanShutdownMarkerLifecycle pins the marker protocol itself: Close
// writes it, Open clears it. Everything about counter trust depends on the
// marker meaning "the previous process finished its work".
func TestCleanShutdownMarkerLifecycle(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 1024)
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !markerPresent(t, dir) {
		t.Fatal("Close did not write the clean-shutdown marker")
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	// Still open, so read through the live handle rather than the file.
	if hasKey(t, reopened, metaCleanShutdown) {
		t.Fatal("Open did not clear the clean-shutdown marker")
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCleanShutdownRestoresCounters pins the fast path: after a clean Close the
// persisted counters are reloaded and match what was stored.
func TestCleanShutdownRestoresCounters(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 1000)
	put(t, ix, mkAction(2), mkOutput(1), baseTime+1, 1000) // shares the output
	put(t, ix, mkAction(3), mkOutput(2), baseTime+2, 250)
	wantStats(t, ix, 3, 2, 1250)
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	wantStats(t, reopened, 3, 2, 1250)
}

// TestUncleanShutdownRebuildsCounters pins the core durability promise: with no
// clean-shutdown marker the persisted counters are ignored entirely and
// recomputed from the records. Deliberately poisoned counters must not survive,
// because a wrong byte budget is unrecoverable without a rescan.
func TestUncleanShutdownRebuildsCounters(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 1000)
	put(t, ix, mkAction(2), mkOutput(1), baseTime+1, 1000) // shares the output
	put(t, ix, mkAction(3), mkOutput(2), baseTime+2, 250)
	put(t, ix, mkAction(4), mkOutput(3), baseTime+3, 7)
	put(t, ix, mkAction(4), mkOutput(4), baseTime+4, 11) // strands output 3
	wantStats(t, ix, 4, 4, 1268)

	// Write counters that are wildly wrong, then die without the marker.
	poison := encodeCounters(countersSnapshot{Actions: 999, Objects: 888, DiskBytes: 777, ZeroRefs: 666})
	if err := ix.db.Set(metaCounters, poison, pebble.Sync); err != nil {
		t.Fatalf("poison counters: %v", err)
	}
	if err := ix.closeAbruptly(); err != nil {
		t.Fatalf("closeAbruptly: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after unclean shutdown: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	wantStats(t, reopened, 4, 4, 1268)
	if got := reopened.zeroRefs.Load(); got != 1 {
		t.Fatalf("rebuilt zeroRefs = %d, want 1", got)
	}
}

// TestUncleanShutdownWithMissingCountersRebuilds pins that a never-persisted
// counter record is handled by the same rescan rather than defaulting to zero
// on a non-empty index.
func TestUncleanShutdownWithMissingCountersRebuilds(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, ix, mkAction(1), mkOutput(1), baseTime, 4096)
	if err := ix.closeAbruptly(); err != nil {
		t.Fatalf("closeAbruptly: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	wantStats(t, reopened, 1, 1, 4096)
}

// TestEntriesSurviveReopen pins that the index is actually durable: entries
// written before a crash are readable afterwards.
func TestEntriesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	a, o := mkAction(1), mkOutput(1)
	put(t, ix, a, o, baseTime, 2048)
	if err := ix.closeAbruptly(); err != nil {
		t.Fatalf("closeAbruptly: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	e, ok, err := reopened.Get(a)
	if err != nil || !ok {
		t.Fatalf("Get after reopen = (_, %v, %v), want a hit", ok, err)
	}
	if e.OutputID != o {
		t.Fatalf("OutputID = %x, want %x", e.OutputID[:4], o[:4])
	}
}

// TestCompactRuns pins that Compact reclaims after a delete-heavy pass without
// disturbing live data — the LSM tombstones it clears would otherwise leave the
// store larger after an eviction than before it.
func TestCompactRuns(t *testing.T) {
	ix := openTemp(t)
	for i := range 50 {
		put(t, ix, mkAction(byte(i)), mkOutput(byte(i)), baseTime+int64(i), 100)
	}
	for i := range 40 {
		if _, _, err := ix.Delete(mkAction(byte(i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := ix.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	wantStats(t, ix, 10, 10, 1000)
	if _, ok, err := ix.Get(mkAction(45)); err != nil || !ok {
		t.Fatalf("survivor lost across Compact: ok=%v err=%v", ok, err)
	}
}

// TestConcurrentMutationsKeepCountersExact pins that the counters stay exact
// under concurrent writers. Every mutation is a read-modify-write over an entry
// and its refcount, so it is only safe if the mutation lock actually covers it;
// this is the test -race has to have.
func TestConcurrentMutationsKeepCountersExact(t *testing.T) {
	ix := openTemp(t)
	const workers, perWorker = 8, 40

	done := make(chan struct{})
	for w := range workers {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := range perWorker {
				a := mkAction(byte(w*perWorker + i))
				// Every action gets its own output so the arithmetic is exact.
				o := mkOutput(byte(w*perWorker + i))
				e := Entry{OutputID: o, Size: 10, CreatedAt: baseTime, LastUsedAt: baseTime + int64(i)}
				if err := ix.Put(a, e, 10); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := ix.Touch(a, baseTime+int64(i)+1, 0); err != nil {
					t.Errorf("Touch: %v", err)
					return
				}
				if _, err := ix.Stats(); err != nil {
					t.Errorf("Stats: %v", err)
					return
				}
			}
		}(w)
	}
	for range workers {
		<-done
	}

	// byte-sized IDs collide past 256, so assert against what actually landed.
	live := countPrefix(t, ix, prefixEntry)
	s, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Actions != live {
		t.Fatalf("Stats.Actions = %d, want %d live entries", s.Actions, live)
	}
	if s.Objects != live || s.DiskBytes != live*10 {
		t.Fatalf("Stats = %+v, want %d objects and %d bytes", s, live, live*10)
	}
}

// countPrefix counts the keys under a prefix, as ground truth against counters.
func countPrefix(t *testing.T, ix *Index, p byte) int64 {
	t.Helper()
	lower, upper := prefixRange(p)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer func() { _ = iter.Close() }()
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

// markerPresent reports whether the clean-shutdown marker is on disk, read
// through an independent Pebble handle so it tests the file and not our cache.
func markerPresent(t *testing.T, dir string) bool {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("raw pebble open of %s: %v", filepath.Base(dir), err)
	}
	defer func() { _ = db.Close() }()
	_, closer, err := db.Get(metaCleanShutdown)
	if errors.Is(err, pebble.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("raw Get: %v", err)
	}
	_ = closer.Close()
	return true
}
