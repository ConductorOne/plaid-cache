// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestGetMissOnEmptyCache pins that an unknown action is a miss and not an
// error: the toolchain rebuilds on a miss, and an error would fail the build.
func TestGetMissOnEmptyCache(t *testing.T) {
	tc := newTestCache(t)

	res, err := tc.cache.Get(t.Context(), mkAction(1))
	if err != nil {
		t.Fatalf("Get on an empty cache = error %v, want a nil error", err)
	}
	if !res.Miss {
		t.Fatalf("Get on an empty cache = %+v, want Miss", res)
	}
	if res.DiskPath != "" || res.Size != 0 || res.OutputID != (ids.OutputID{}) {
		t.Fatalf("Get on an empty cache = %+v, want the other fields unset", res)
	}
	if got := tc.cache.Metrics().GetMiss; got != 1 {
		t.Fatalf("GetMiss = %d, want 1", got)
	}
}

// TestPutThenGetServesTheStoredBytes pins the core round trip: a hit hands back
// a path that exists and whose contents are exactly what was stored, under the
// output id and size that were declared.
func TestPutThenGetServesTheStoredBytes(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(1), mkOutput(1)
	body := []byte("compiled output bytes")

	path := tc.put(t, a, o, body)

	res := tc.get(t, a)
	if res.Miss {
		t.Fatal("Get after Put = a miss, want a hit")
	}
	if res.OutputID != o {
		t.Fatalf("OutputID = %s, want %s", res.OutputID, o)
	}
	if res.Size != int64(len(body)) {
		t.Fatalf("Size = %d, want %d", res.Size, len(body))
	}
	if res.DiskPath != path {
		t.Fatalf("DiskPath = %q, want the path Put returned, %q", res.DiskPath, path)
	}
	// Every hit must hand back a readable file: the toolchain reads the body
	// itself, so a hit pointing at nothing is a hard error on its side.
	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatalf("reading DiskPath: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body on disk = %q, want %q", got, body)
	}
	if res.Time.IsZero() {
		t.Fatal("Time is zero, want the entry's creation instant")
	}
	tc.wantStats(t, 1, 1)
}

// TestGetRepairsDanglingIndexEntry pins the repair path: when the body behind an
// index entry has been deleted out from under it, Get must report a miss rather
// than a hit at a missing path, count the repair, drop the entry, and leave the
// byte accounting consistent — after which the action can be stored again.
func TestGetRepairsDanglingIndexEntry(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(2), mkOutput(2)
	body := []byte("body that a partial clean removes")

	path := tc.put(t, a, o, body)
	tc.wantStats(t, 1, 1)

	// Simulate a partial clean: the body is gone, the index still points at it.
	if err := os.Remove(path); err != nil {
		t.Fatalf("removing the body: %v", err)
	}

	res := tc.get(t, a)
	if !res.Miss {
		t.Fatalf("Get over a deleted body = %+v, want a miss rather than a hit at a missing path", res)
	}
	m := tc.cache.Metrics()
	if m.GetRepair != 1 {
		t.Fatalf("GetRepair = %d, want 1", m.GetRepair)
	}
	if m.GetMiss != 1 {
		t.Fatalf("GetMiss = %d, want 1", m.GetMiss)
	}

	if _, ok, err := tc.idx.Get(a); err != nil {
		t.Fatalf("index Get: %v", err)
	} else if ok {
		t.Fatal("the dangling index entry survived the repair")
	}
	tc.wantStats(t, 0, 0)

	// The repair must leave the cache usable, not poisoned.
	again := []byte("rebuilt body")
	path2 := tc.put(t, a, o, again)
	res2 := tc.get(t, a)
	if res2.Miss {
		t.Fatal("Get after re-Put = a miss, want a hit")
	}
	if res2.DiskPath != path2 {
		t.Fatalf("DiskPath = %q, want %q", res2.DiskPath, path2)
	}
	got, err := os.ReadFile(res2.DiskPath)
	if err != nil {
		t.Fatalf("reading DiskPath: %v", err)
	}
	if !bytes.Equal(got, again) {
		t.Fatalf("body on disk = %q, want %q", got, again)
	}
	tc.wantStats(t, 1, 1)
}

// TestRemoteDisabledMakesNoRemoteCalls pins that an empty S3Bucket means the
// backend is never touched — not even on a miss, which is the one path that
// would otherwise reach for it — while local hits are served as usual.
func TestRemoteDisabledMakesNoRemoteCalls(t *testing.T) {
	tc := newTestCache(t) // no withRemote(): S3Bucket is ""
	if tc.cfg.RemoteEnabled() {
		t.Fatal("RemoteEnabled with an empty bucket, want disabled")
	}
	a, o := mkAction(3), mkOutput(3)
	body := []byte("local only body")

	tc.put(t, a, o, body)
	if res := tc.get(t, a); res.Miss {
		t.Fatal("Get after Put = a miss, want a local hit with the remote off")
	}
	if res := tc.get(t, mkAction(99)); !res.Miss {
		t.Fatalf("Get of an unknown action = %+v, want a miss", res)
	}

	// Uploads are asynchronous, so drain the pool before concluding that
	// nothing was sent.
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if calls := tc.rem.callOrder(); len(calls) != 0 {
		t.Fatalf("remote calls = %v, want none with the remote disabled", calls)
	}
	if got := tc.cache.Metrics().UploadSkip; got != 0 {
		t.Fatalf("UploadSkip = %d, want 0: a disabled remote skips nothing, it uploads nothing", got)
	}
}

// TestEvictRemovesOrphanedBodies pins that an eviction pass reports what the
// index pruned and that the body it released is really gone from disk — a
// pruned entry whose file stayed would leave the byte budget unbounded.
func TestEvictRemovesOrphanedBodies(t *testing.T) {
	// MaxBytes of 1 puts every stored body over the ceiling, so the pass is
	// deterministic without depending on wall-clock TTL arithmetic.
	tc := newTestCache(t, withMaxBytes(1))
	a, o := mkAction(4), mkOutput(4)
	body := []byte("evictable body")

	path := tc.put(t, a, o, body)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the body is not on disk before eviction: %v", err)
	}

	res, err := tc.cache.Evict(t.Context())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 1 || res.ObjectsPruned != 1 {
		t.Fatalf("EvictResult = %+v, want 1 action and 1 object pruned", res)
	}
	if res.BytesFreed <= 0 {
		t.Fatalf("EvictResult.BytesFreed = %d, want the body's disk footprint", res.BytesFreed)
	}

	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat of the evicted body = %v, want fs.ErrNotExist", err)
	}
	if _, ok, err := tc.idx.Get(a); err != nil {
		t.Fatalf("index Get: %v", err)
	} else if ok {
		t.Fatal("the evicted action survived in the index")
	}
	tc.wantStats(t, 0, 0)

	if res := tc.get(t, a); !res.Miss {
		t.Fatalf("Get after eviction = %+v, want a miss", res)
	}
}

// TestMetricsSnapshotReflectsOperations spot-checks that the counters track the
// operations actually performed, since the status command and the daemon log
// report nothing else.
func TestMetricsSnapshotReflectsOperations(t *testing.T) {
	tc := newTestCache(t, withRemote())
	a, o := mkAction(5), mkOutput(5)
	body := []byte("body large enough to upload")

	// One miss (local and remote), one put, one local hit.
	if res := tc.get(t, mkAction(200)); !res.Miss {
		t.Fatalf("Get of an unknown action = %+v, want a miss", res)
	}
	tc.put(t, a, o, body)
	if res := tc.get(t, a); res.Miss {
		t.Fatal("Get after Put = a miss, want a local hit")
	}

	// Uploads are asynchronous: Close drains the pool so UploadOK is settled.
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := tc.cache.Metrics()
	want := MetricsSnapshot{GetLocalHit: 1, GetMiss: 1, Put: 1, UploadOK: 1}
	if got != want {
		t.Fatalf("Metrics = %+v, want %+v", got, want)
	}
}

// TestBrokenIndexDegradesToMiss pins that an unusable index is a miss and not an
// error. The index is a rebuildable accelerator, so losing it must cost a
// rebuild rather than fail the build; closing it out from under the cache is the
// cheapest way to make every index call fail.
func TestBrokenIndexDegradesToMiss(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(7), mkOutput(7)
	tc.put(t, a, o, []byte("body the index will forget"))

	if err := tc.idx.Close(); err != nil {
		t.Fatalf("closing the index: %v", err)
	}

	res, err := tc.cache.Get(t.Context(), a)
	if err != nil {
		t.Fatalf("Get over a broken index = error %v, want a nil error", err)
	}
	if !res.Miss {
		t.Fatalf("Get over a broken index = %+v, want a miss", res)
	}
	if got := tc.cache.Metrics().GetMiss; got != 1 {
		t.Fatalf("GetMiss = %d, want 1", got)
	}
}

// TestCloseIsIdempotent pins that a second Close neither panics on the already
// closed job channel nor double-waits: the daemon and the direct-mode CLI both
// have paths that can close twice.
func TestCloseIsIdempotent(t *testing.T) {
	tc := newTestCache(t, withRemote())
	tc.put(t, mkAction(6), mkOutput(6), []byte("uploaded body"))

	if err := tc.cache.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := tc.cache.Metrics().UploadOK; got != 1 {
		t.Fatalf("UploadOK = %d, want 1: the second Close must not re-drain", got)
	}
}
