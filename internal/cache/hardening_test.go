// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestPutAfterCloseDoesNotPanic pins that a late Put is counted, not fatal.
//
// submit's `select` with a `default` looks like it makes a send safe, but a send
// on a closed channel is always ready, so `default` never runs and the send
// panics. A panic in a GOCACHEPROG plugin aborts the build — the one outcome
// this package promises to avoid — so a Put racing Close must be dropped
// instead.
func TestPutAfterCloseDoesNotPanic(t *testing.T) {
	tc := newTestCache(t, withRemote())

	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	body := []byte("late arrival")
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("late-action")))

	// Must not panic, and must still stage the body locally: the toolchain is
	// handed this path and will read it.
	path, err := tc.cache.Put(context.Background(), a, o, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Put after Close: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading staged body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("staged body = %q, want %q", got, body)
	}
	if drops := tc.cache.Metrics().UploadDrop; drops == 0 {
		t.Fatal("UploadDrop = 0; a post-close submit should be counted as dropped")
	}
}

// TestCloseAfterCloseThenPutStillSafe pins that the guard survives repeated
// Close calls, which are legal because close is sync.Once-guarded.
func TestCloseAfterCloseThenPutStillSafe(t *testing.T) {
	tc := newTestCache(t, withRemote())
	for range 3 {
		if err := tc.cache.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	body := []byte("x")
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("a")))
	if _, err := tc.cache.Put(context.Background(), a, o, bytes.NewReader(body), 1); err != nil {
		t.Fatalf("Put after repeated Close: %v", err)
	}
}

// TestUnreadableBodyIsAMissAndKeepsTheIndexEntry pins that only a genuine
// absence triggers the dangling-entry repair.
//
// Any other error from the body store — a transient I/O failure, a permissions
// change — may well describe a body that is perfectly fine. Deleting the index
// entry on that basis discards a good entry and its refcount permanently, so an
// unreadable body must degrade to a miss without mutating the index.
func TestUnreadableBodyIsAMissAndKeepsTheIndexEntry(t *testing.T) {
	tc := newTestCache(t)

	body := []byte("still here, just unreadable")
	o := ids.OutputID(sha256.Sum256(body))
	a := ids.ActionID(sha256.Sum256([]byte("unreadable-action")))
	if _, err := tc.cache.Put(context.Background(), a, o, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	before, err := tc.idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if before.Actions != 1 {
		t.Fatalf("actions before = %d, want 1", before.Actions)
	}

	// Make the shard directory untraversable so Stat fails with EACCES rather
	// than ENOENT. Running as root defeats this, so skip there.
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny access")
	}
	shard := filepath.Dir(tc.blobs.Path(o))
	if err := os.Chmod(shard, 0o000); err != nil {
		t.Fatalf("chmod shard: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o755) })

	res, err := tc.cache.Get(context.Background(), a)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !res.Miss {
		t.Fatal("Get returned a hit for an unreadable body")
	}
	if got := tc.cache.Metrics().GetRepair; got != 0 {
		t.Fatalf("GetRepair = %d, want 0; an unreadable body is not a dangling entry", got)
	}

	after, err := tc.idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.Actions != before.Actions || after.DiskBytes != before.DiskBytes {
		t.Fatalf("index changed on a transient failure: %+v -> %+v", before, after)
	}
}
