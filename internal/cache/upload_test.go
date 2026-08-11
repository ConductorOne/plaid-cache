// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockDeadline bounds how long a submission run may take before the test calls
// it blocked. It is generous because it is only ever waited on when the test is
// already failing.
const blockDeadline = 30 * time.Second

// TestMinUploadSizeSkipsSmallBodies pins the threshold: a body under
// MinUploadSize is counted as skipped and never reaches the bucket, while one
// exactly at the threshold is uploaded as both an object and an action record.
func TestMinUploadSizeSkipsSmallBodies(t *testing.T) {
	const threshold = 16
	tc := newTestCache(t, withRemote(), withMinUploadSize(threshold))

	small := bytes.Repeat([]byte("s"), threshold-1)
	aSmall, oSmall := mkAction(12), mkOutput(12)
	// Exactly at the threshold: the comparison is size < MinUploadSize, so this
	// is the boundary that must still upload.
	atThreshold := bytes.Repeat([]byte("L"), threshold)
	aLarge, oLarge := mkAction(13), mkOutput(13)

	tc.put(t, aSmall, oSmall, small)
	tc.put(t, aLarge, oLarge, atThreshold)

	// Uploads are asynchronous: Close drains the worker pool, so the counters
	// and the bucket contents below are settled rather than racing the workers.
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m := tc.cache.Metrics()
	if m.UploadSkip != 1 {
		t.Fatalf("UploadSkip = %d, want 1", m.UploadSkip)
	}
	if m.UploadOK != 1 {
		t.Fatalf("UploadOK = %d, want 1", m.UploadOK)
	}
	if m.UploadFail != 0 || m.UploadDrop != 0 {
		t.Fatalf("UploadFail = %d, UploadDrop = %d, want 0 and 0", m.UploadFail, m.UploadDrop)
	}

	if _, ok := tc.rem.storedObject(oSmall); ok {
		t.Fatal("the sub-threshold body was uploaded, want it skipped")
	}
	if _, ok := tc.rem.storedAction(aSmall); ok {
		t.Fatal("the sub-threshold action record was uploaded; skipping the body must skip the record too")
	}

	got, ok := tc.rem.storedObject(oLarge)
	if !ok {
		t.Fatal("the at-threshold body was not uploaded, want it uploaded")
	}
	if !bytes.Equal(got, atThreshold) {
		t.Fatalf("uploaded body = %q, want %q", got, atThreshold)
	}
	rec, ok := tc.rem.storedAction(aLarge)
	if !ok {
		t.Fatal("the at-threshold action record was not uploaded")
	}
	if rec.output != oLarge {
		t.Fatalf("uploaded action record names %s, want %s", rec.output, oLarge)
	}
}

// TestUploadWritesObjectBeforeAction pins the upload order. An action record
// published ahead of its body advertises a remote hit that cannot be satisfied,
// costing every reader a wasted round trip; a body with no record is merely
// unreferenced.
func TestUploadWritesObjectBeforeAction(t *testing.T) {
	tc := newTestCache(t, withRemote())
	tc.put(t, mkAction(14), mkOutput(14), []byte("body then record"))

	// Uploads are asynchronous: drain the pool so the recorded call order is
	// complete.
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := tc.rem.callOrder()
	want := []string{"PutObject", "PutAction"}
	if !slices.Equal(got, want) {
		t.Fatalf("remote calls = %v, want %v", got, want)
	}
}

// pinPool holds every upload inside PutObject until the returned function is
// called, so a test can fill the queue without racing the workers.
//
// The release is registered as a cleanup ahead of whatever the caller does with
// it, because newTestCache's own Close cleanup runs later and would otherwise
// wedge on a worker nobody let go.
func pinPool(t *testing.T, tc *testCache) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	tc.rem.blockPutObject = release
	return unblock
}

// TestUploadQueueReportsSaturationBeforeDrops pins the gauge that arrives in
// time to be acted on: with the pool pinned, the backlog is readable while it is
// filling and still whole, not only once the queue has overflowed and entries
// the shared tier will never hold are already gone.
func TestUploadQueueReportsSaturationBeforeDrops(t *testing.T) {
	const depth = 8
	tc := newTestCache(t, withRemote(), withUploadQueueDepth(depth))
	unblock := pinPool(t, tc)

	if got, capacity := tc.cache.UploadQueue(); got != 0 || capacity != depth {
		t.Fatalf("UploadQueue = %d of %d on an idle cache, want 0 of %d", got, capacity, depth)
	}

	// Fewer submissions than the queue holds: one is in the pinned worker's
	// hands and the rest are waiting, so nothing can have been dropped yet.
	body := bytes.Repeat([]byte("q"), 32)
	for i := range depth - 1 {
		tc.put(t, mkActionN(i), mkOutputN(i), body)
	}
	got, capacity := tc.cache.UploadQueue()
	if capacity != depth {
		t.Fatalf("UploadQueue capacity = %d, want the configured %d", capacity, depth)
	}
	if got == 0 || got > capacity {
		t.Fatalf("UploadQueue = %d of %d with %d uploads pinned, want a depth in (0, %d]",
			got, capacity, depth-1, capacity)
	}
	if m := tc.cache.Metrics(); m.UploadDrop != 0 {
		t.Fatalf("UploadDrop = %d below capacity, want 0: the depth must be readable before anything is lost", m.UploadDrop)
	}

	// Past capacity the queue can only report itself full, which is why the
	// depth above is the number worth watching.
	for i := depth; i < 4*depth; i++ {
		tc.put(t, mkActionN(i), mkOutputN(i), body)
	}
	if got, capacity := tc.cache.UploadQueue(); got != capacity {
		t.Fatalf("UploadQueue = %d of %d after oversubscribing, want a full queue", got, capacity)
	}
	if m := tc.cache.Metrics(); m.UploadDrop == 0 {
		t.Fatalf("UploadDrop = 0 after oversubscribing a queue of %d, want drops", depth)
	}
	unblock()
}

// TestUploadDropWarningIsRateLimited pins that a saturation episode says so in
// the log exactly once per interval, however many uploads it loses.
//
// Both halves matter. Silence is what made this failure mode invisible — the
// entries are simply absent, and the reader that misses them is on another
// machine much later. A line per drop would be no better: thousands of them a
// second bury the log they were meant to make legible, so the count since the
// last report travels in the line instead.
func TestUploadDropWarningIsRateLimited(t *testing.T) {
	const depth = 2
	tc := newTestCache(t, withRemote(), withUploadQueueDepth(depth))
	unblock := pinPool(t, tc)

	body := bytes.Repeat([]byte("d"), 32)
	const submitted = 40
	for i := range submitted {
		tc.put(t, mkActionN(i), mkOutputN(i), body)
	}

	m := tc.cache.Metrics()
	if m.UploadDrop < 2 {
		t.Fatalf("UploadDrop = %d, want several: the test cannot pin rate limiting without repeated drops", m.UploadDrop)
	}
	lines := tc.logs.matching("upload queue full")
	if len(lines) != 1 {
		t.Fatalf("logged %d drop warnings for %d drops within one %v interval, want 1: %v",
			len(lines), m.UploadDrop, dropLogInterval, lines)
	}
	// The first drop reports immediately and carries the one it is reporting;
	// a warning that named no count would leave a reader unable to tell a
	// single overflow from a sustained one.
	if !strings.Contains(lines[0], "dropped 1 upload") {
		t.Fatalf("drop warning %q does not report how many were dropped", lines[0])
	}
	unblock()
}

// TestUploadBlockTimeoutWaitsForRoom pins the opt-in reversal of the drop trade:
// with a timeout configured, a put whose queue is full waits for room instead of
// losing the entry.
func TestUploadBlockTimeoutWaitsForRoom(t *testing.T) {
	tc := newTestCache(t, withRemote(), withUploadQueueDepth(1), withUploadBlockTimeout(blockDeadline))
	unblock := pinPool(t, tc)

	// One job pins the worker and one fills the queue, so the third has nowhere
	// to go and would be dropped without a timeout to wait out.
	body := bytes.Repeat([]byte("b"), 32)
	tc.put(t, mkActionN(0), mkOutputN(0), body)
	tc.put(t, mkActionN(1), mkOutputN(1), body)

	done := make(chan error, 1)
	go func() {
		_, err := tc.cache.Put(t.Context(), mkActionN(2), mkOutputN(2), bytes.NewReader(body), int64(len(body)))
		done <- err
	}()

	// Nothing can drain while the pool is pinned, so a Put that returns here
	// returned by dropping — which is what the timeout exists to prevent.
	select {
	case err := <-done:
		unblock()
		t.Fatalf("Put returned (err %v) with the queue full and the pool pinned, want it waiting for room", err)
	case <-time.After(50 * time.Millisecond):
	}

	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
	case <-time.After(blockDeadline):
		t.Fatalf("Put blocked for %v after the pool was released, want it to land", blockDeadline)
	}

	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if m := tc.cache.Metrics(); m.UploadDrop != 0 || m.UploadOK != 3 {
		t.Fatalf("UploadOK = %d, UploadDrop = %d, want 3 and 0: a waited-for slot must not lose the entry",
			m.UploadOK, m.UploadDrop)
	}
}

// TestUploadBlockTimeoutExpiresRatherThanStalling pins the bound on that wait. A
// wait with no end would turn a saturated queue into a stalled build, which is
// the failure the drop was chosen to avoid in the first place.
func TestUploadBlockTimeoutExpiresRatherThanStalling(t *testing.T) {
	tc := newTestCache(t, withRemote(), withUploadQueueDepth(1), withUploadBlockTimeout(20*time.Millisecond))
	unblock := pinPool(t, tc)

	body := bytes.Repeat([]byte("e"), 32)
	done := make(chan error, 1)
	go func() {
		for i := range 4 {
			if _, err := tc.cache.Put(t.Context(), mkActionN(i), mkOutputN(i), bytes.NewReader(body), int64(len(body))); err != nil {
				done <- fmt.Errorf("Put(%d): %w", i, err)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(blockDeadline):
		unblock()
		t.Fatalf("Put blocked for %v against a 20ms upload timeout, want the job dropped", blockDeadline)
	}
	if m := tc.cache.Metrics(); m.UploadDrop == 0 {
		t.Fatal("UploadDrop = 0 with the pool pinned past the block timeout, want the waits to have expired")
	}
	unblock()
}

// TestUploadQueueDropsRatherThanBlocking pins the bounded-queue trade: with the
// pool pinned on a stuck transfer, submissions past the queue depth are dropped
// and counted, and no Put ever blocks on the backlog.
func TestUploadQueueDropsRatherThanBlocking(t *testing.T) {
	tc := newTestCache(t, withRemote())

	// Pin the single worker inside PutObject so the queue can only drain by
	// being filled. No sleeping: the release is explicit.
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	// Registered after newTestCache's Close cleanup, so it runs first (cleanups
	// are LIFO) and Close cannot wedge on the pinned worker.
	t.Cleanup(unblock)
	tc.rem.blockPutObject = release

	// One worker with queueDepthPerWorker queue slots can absorb at most one
	// in-flight job plus a full queue before submit must start dropping.
	const submitted = 2 * queueDepthPerWorker
	const absorbable = queueDepthPerWorker + 1

	body := bytes.Repeat([]byte("q"), 32)
	done := make(chan error, 1)
	go func() {
		for i := range submitted {
			if _, err := tc.cache.Put(t.Context(), mkActionN(i), mkOutputN(i), bytes.NewReader(body), int64(len(body))); err != nil {
				done <- fmt.Errorf("Put(%d): %w", i, err)
				return
			}
		}
		done <- nil
	}()

	// A dropped job is the point: the deadline below only ever fires when
	// submit blocks, so a passing run waits for nothing.
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(blockDeadline):
		unblock()
		t.Fatalf("Put blocked for %v on a full upload queue, want the job dropped", blockDeadline)
	}

	m := tc.cache.Metrics()
	if m.Put != submitted {
		t.Fatalf("Put = %d, want %d: every Put must complete even with the pool stuck", m.Put, submitted)
	}
	if want := int64(submitted - absorbable); m.UploadDrop < want {
		t.Fatalf("UploadDrop = %d, want at least %d", m.UploadDrop, want)
	}
	if m.UploadOK != 0 {
		t.Fatalf("UploadOK = %d, want 0 while the only worker is pinned", m.UploadOK)
	}

	unblock()
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Everything that was not dropped had to land somewhere, and nothing may be
	// counted twice.
	final := tc.cache.Metrics()
	if final.UploadOK+final.UploadFail+final.UploadDrop != submitted {
		t.Fatalf("UploadOK(%d) + UploadFail(%d) + UploadDrop(%d) != %d submitted",
			final.UploadOK, final.UploadFail, final.UploadDrop, submitted)
	}
}
