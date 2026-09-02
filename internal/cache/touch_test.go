// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestToucherCoalescesPendingAction pins that repeated reads of one action add
// one deferred index update, while a different action remains independently
// eligible for the same worker.
func TestToucherCoalescesPendingAction(t *testing.T) {
	a, b := mkAction(1), mkAction(2)
	started := make(chan struct{})
	release := make(chan struct{})

	var mu sync.Mutex
	var batches [][]ids.ActionID
	first := true
	toucher := newToucher(func(actions []ids.ActionID, _ int64, _ time.Duration) (int, error) {
		copyOfActions := append([]ids.ActionID(nil), actions...)
		mu.Lock()
		batches = append(batches, copyOfActions)
		block := first
		first = false
		mu.Unlock()
		if block {
			close(started)
			<-release
		}
		return len(actions), nil
	}, 0, func(string, ...any) {})
	t.Cleanup(toucher.close)

	toucher.enqueue(a, 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("touch worker did not start")
	}
	toucher.enqueue(a, 0)
	toucher.enqueue(b, 0)
	close(release)
	toucher.close()

	mu.Lock()
	defer mu.Unlock()
	seen := make(map[ids.ActionID]int)
	for _, batch := range batches {
		for _, action := range batch {
			seen[action]++
		}
	}
	if seen[a] != 1 {
		t.Fatalf("touches for first action = %d, want 1 after coalescing", seen[a])
	}
	if seen[b] != 1 {
		t.Fatalf("touches for second action = %d, want 1", seen[b])
	}
}

// TestToucherSkipsFreshEntry pins the relatime check before queue admission:
// the usual warm-cache hit must not wake the index worker at all.
func TestToucherSkipsFreshEntry(t *testing.T) {
	touched := make(chan struct{})
	toucher := newToucher(func([]ids.ActionID, int64, time.Duration) (int, error) {
		close(touched)
		return 0, nil
	}, time.Hour, func(string, ...any) {})
	t.Cleanup(toucher.close)

	toucher.enqueue(mkAction(1), time.Now().UnixNano())
	select {
	case <-touched:
		t.Fatal("fresh entry was queued for a touch")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestGetReturnsWhileTouchWorkerIsBusy pins the cache contract that LRU
// bookkeeping never holds a readable local hit behind a slow index mutation.
func TestGetReturnsWhileTouchWorkerIsBusy(t *testing.T) {
	tc := newTestCache(t)
	tc.cache.touches.close()

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTouch := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseTouch)

	tc.cache.touches = newToucher(func(_ []ids.ActionID, _ int64, _ time.Duration) (int, error) {
		close(started)
		<-release
		return 0, nil
	}, 0, func(string, ...any) {})

	a, o := mkAction(1), mkOutput(1)
	tc.put(t, a, o, []byte("local cache body"))
	result := make(chan Result, 1)
	go func() {
		got, _ := tc.cache.Get(t.Context(), a)
		result <- got
	}()

	select {
	case got := <-result:
		if got.Miss {
			t.Fatal("Get returned a miss while the local body was readable")
		}
	case <-time.After(time.Second):
		t.Fatal("Get waited for the blocked touch worker")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Get did not schedule an LRU touch")
	}
	releaseTouch()
	tc.cache.touches.close()
}
