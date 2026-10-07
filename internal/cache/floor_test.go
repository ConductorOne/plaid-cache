// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"errors"
	"testing"
)

// volumeReporting makes the cache's volume report avail free bytes, so a test
// can stand the cache next to a disk that is fuller than the test machine's.
func volumeReporting(tc *testCache, avail uint64) {
	tc.cache.volumeUsage = func(string) (uint64, uint64, error) { return 1 << 40, avail, nil }
}

// putFour fills the cache with four bodies written oldest first, and returns
// their outputs in that order.
func putFour(t *testing.T, tc *testCache) [4]byte {
	t.Helper()
	body := bytes.Repeat([]byte("x"), 64<<10)
	for i := range byte(4) {
		tc.put(t, mkAction(i+1), mkOutput(i+1), body)
	}
	return [4]byte{1, 2, 3, 4}
}

// present reports which of the numbered actions the index still holds.
func present(t *testing.T, tc *testCache, ns [4]byte) []bool {
	t.Helper()
	out := make([]bool, len(ns))
	for i, n := range ns {
		_, ok, err := tc.idx.Get(mkAction(n))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		out[i] = ok
	}
	return out
}

// TestFreeFloorEvictsBelowTheCeiling pins the point of the floor: with the volume
// short of it, a pass frees the shortfall even though the recorded total
// is far below MaxBytes, oldest entry first and no more than it needs.
func TestFreeFloorEvictsBelowTheCeiling(t *testing.T) {
	tc := newTestCache(t, withMaxBytes(1<<40), withMinFreeBytes(1<<30))
	ns := putFour(t, tc)
	volumeReporting(tc, 1<<30-1) // one byte short

	res, err := tc.cache.Evict(t.Context())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 1 {
		t.Fatalf("pruned %d actions, want exactly the one that covers a one-byte shortfall", res.ActionsPruned)
	}
	if got := present(t, tc, ns); got[0] || !got[1] || !got[2] || !got[3] {
		t.Fatalf("present = %v, want only the oldest entry gone", got)
	}
}

// TestFreeFloorMetLeavesTheCacheAlone pins that the floor costs nothing while
// the volume has the space: a cache below its ceiling with room to spare is not
// pruned at all.
func TestFreeFloorMetLeavesTheCacheAlone(t *testing.T) {
	tc := newTestCache(t, withMaxBytes(1<<40), withMinFreeBytes(1<<30))
	putFour(t, tc)
	volumeReporting(tc, 1<<30)

	res, err := tc.cache.Evict(t.Context())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d actions with the floor met, want 0", res.ActionsPruned)
	}
}

// TestFreeFloorDisabledByDefault pins that zero means off: a disk with no free
// space at all does not evict when no floor was asked for, which keeps every
// existing configuration behaving as it did.
func TestFreeFloorDisabledByDefault(t *testing.T) {
	tc := newTestCache(t, withMaxBytes(1<<40))
	putFour(t, tc)
	volumeReporting(tc, 0)

	res, err := tc.cache.Evict(t.Context())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d actions with no floor configured, want 0", res.ActionsPruned)
	}
}

// TestFreeFloorLargerThanTheCacheEvictsEverything pins that a shortfall bigger
// than everything recorded empties the cache rather than wrapping to a zero
// ceiling, which the index reads as "no size constraint" and would not evict.
func TestFreeFloorLargerThanTheCacheEvictsEverything(t *testing.T) {
	tc := newTestCache(t, withMinFreeBytes(1<<40))
	ns := putFour(t, tc)
	volumeReporting(tc, 0)

	if _, err := tc.cache.Evict(t.Context()); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	for i, ok := range present(t, tc, ns) {
		if ok {
			t.Fatalf("entry %d survived a floor larger than the whole cache", ns[i])
		}
	}
}

// TestFreeFloorUnreadableVolumeFallsBackToTheCeiling pins that a volume that
// cannot be measured is not treated as a full one: eviction keeps to MaxBytes
// and logs why the floor was skipped.
func TestFreeFloorUnreadableVolumeFallsBackToTheCeiling(t *testing.T) {
	tc := newTestCache(t, withMaxBytes(1<<40), withMinFreeBytes(1<<30))
	putFour(t, tc)
	tc.cache.volumeUsage = func(string) (uint64, uint64, error) { return 0, 0, errors.New("no statfs") }

	res, err := tc.cache.Evict(t.Context())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if res.ActionsPruned != 0 {
		t.Fatalf("pruned %d actions on an unreadable volume, want 0", res.ActionsPruned)
	}
	if len(tc.logs.matching("free-space floor: no statfs")) == 0 {
		t.Fatal("the skipped floor was not logged")
	}
}
