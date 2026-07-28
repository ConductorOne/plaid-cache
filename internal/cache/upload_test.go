// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"fmt"
	"slices"
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
