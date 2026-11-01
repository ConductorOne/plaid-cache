// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"os"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestHasAnswersRepeatProbesFromMemory pins that a second probe inside the
// presence window is answered without stat'ing the body: the memo is the
// whole point, and this is the one observable difference it makes.
func TestHasAnswersRepeatProbesFromMemory(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(40), mkOutput(40)
	tc.put(t, a, o, []byte("probed twice"))

	if !tc.cache.Has(t.Context(), a) {
		t.Fatal("first Has = false for a stored action")
	}
	// Removed behind the process's back: only the memo can still say present.
	if err := os.Remove(tc.blobs.Path(o)); err != nil {
		t.Fatalf("removing the body: %v", err)
	}
	if !tc.cache.Has(t.Context(), a) {
		t.Fatal("second Has = false, want the remembered answer")
	}
}

// TestHasForgetsWhatEvictionRemoved pins that eviction clears the memo: an
// answer remembered before a pass must not report an evicted action present,
// or a client would skip the upload of a blob the cache no longer holds.
func TestHasForgetsWhatEvictionRemoved(t *testing.T) {
	tc := newTestCache(t, withMaxBytes(1))
	a, o := mkAction(41), mkOutput(41)
	tc.put(t, a, o, []byte("evicted after a probe"))

	if !tc.cache.Has(t.Context(), a) {
		t.Fatal("Has before eviction = false for a stored action")
	}
	if _, err := tc.cache.Evict(t.Context()); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if tc.cache.Has(t.Context(), a) {
		t.Fatal("Has after eviction = true, want the memo invalidated")
	}
}

// TestHasForgetsWhatARepairRemoved pins that the repair path clears the memo
// too: once a lookup has found the body gone and dropped the entry, a probe
// must report it absent so the caller re-uploads.
func TestHasForgetsWhatARepairRemoved(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(42), mkOutput(42)
	tc.put(t, a, o, []byte("repaired after a probe"))

	if !tc.cache.Has(t.Context(), a) {
		t.Fatal("Has before repair = false for a stored action")
	}
	if err := os.Remove(tc.blobs.Path(o)); err != nil {
		t.Fatalf("removing the body: %v", err)
	}
	if res := tc.get(t, a); !res.Miss {
		t.Fatalf("Get over a deleted body = %+v, want a miss", res)
	}
	if tc.cache.Has(t.Context(), a) {
		t.Fatal("Has after repair = true, want the memo invalidated")
	}
}

// TestPresenceMemoStaysBounded pins the memory bound: remembering far more
// digests than one shard may hold never grows that shard past its cap.
func TestPresenceMemoStaysBounded(t *testing.T) {
	var p presence
	now := time.Now().UnixNano()
	for i := range 3 * presenceShardCap {
		var a ids.ActionID
		a[0] = 7 // every digest in one shard
		a[1], a[2], a[3] = byte(i), byte(i>>8), byte(i>>16)
		p.remember(a, p.generation(), now, now)
	}
	if got := len(p.shard(ids.ActionID{7}).m); got > presenceShardCap {
		t.Fatalf("shard holds %d entries, want at most %d", got, presenceShardCap)
	}
}
