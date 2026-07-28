// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"os"
	"slices"
	"testing"
	"time"
)

// remoteMtime is a fixed instant carried across the shared tier, so a faulted-in
// entry's age can be asserted exactly.
var remoteMtime = time.Unix(1_700_000_000, 0)

// TestGetFaultsInFromRemoteAndThenServesLocally pins the fault-in path: a local
// miss that the shared tier can satisfy stages the body locally and records it
// in the index, so the next Get is served from disk without a second round trip.
func TestGetFaultsInFromRemoteAndThenServesLocally(t *testing.T) {
	tc := newTestCache(t, withRemote())
	a, o := mkAction(7), mkOutput(7)
	body := []byte("body produced on another machine")

	tc.rem.programObject(o, body)
	tc.rem.programAction(a, o, remoteMtime)

	res := tc.get(t, a)
	if res.Miss {
		t.Fatal("Get with a remote hit available = a miss, want a hit")
	}
	if res.OutputID != o {
		t.Fatalf("OutputID = %s, want %s", res.OutputID, o)
	}
	if res.Size != int64(len(body)) {
		t.Fatalf("Size = %d, want %d", res.Size, len(body))
	}
	if !res.Time.Equal(remoteMtime) {
		t.Fatalf("Time = %v, want the mtime the remote reported, %v", res.Time, remoteMtime)
	}
	// The body must be staged locally, not streamed straight through.
	staged, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatalf("reading the staged body: %v", err)
	}
	if !bytes.Equal(staged, body) {
		t.Fatalf("staged body = %q, want %q", staged, body)
	}
	// And the index must record it, or the next build faults it in again.
	e, ok, err := tc.idx.Get(a)
	if err != nil {
		t.Fatalf("index Get: %v", err)
	}
	if !ok {
		t.Fatal("the faulted-in entry was not recorded in the index")
	}
	if e.OutputID != o || e.Size != int64(len(body)) {
		t.Fatalf("index entry = %+v, want output %s and size %d", e, o, len(body))
	}
	tc.wantStats(t, 1, 1)

	if got := tc.cache.Metrics().GetRemoteHit; got != 1 {
		t.Fatalf("GetRemoteHit = %d, want 1", got)
	}
	before := tc.rem.callOrder()
	if want := []string{"GetAction", "GetObject"}; !slices.Equal(before, want) {
		t.Fatalf("remote calls = %v, want %v", before, want)
	}

	// The second Get must be served locally.
	res2 := tc.get(t, a)
	if res2.Miss {
		t.Fatal("second Get = a miss, want a local hit off the staged body")
	}
	m := tc.cache.Metrics()
	if m.GetLocalHit != 1 {
		t.Fatalf("GetLocalHit = %d, want 1", m.GetLocalHit)
	}
	if m.GetRemoteHit != 1 {
		t.Fatalf("GetRemoteHit = %d, want it to stay at 1", m.GetRemoteHit)
	}
	if after := tc.rem.callOrder(); !slices.Equal(after, before) {
		t.Fatalf("remote calls after the second Get = %v, want the remote untouched at %v", after, before)
	}
}

// TestRemoteGetActionErrorDegradesToMiss pins that a failing shared tier cannot
// fail a build: Get reports a miss with a nil error and Put still succeeds.
func TestRemoteGetActionErrorDegradesToMiss(t *testing.T) {
	tc := newTestCache(t, withRemote())
	tc.rem.getActionErr = errBoom

	res, err := tc.cache.Get(t.Context(), mkAction(8))
	if err != nil {
		t.Fatalf("Get with a failing remote = error %v, want a nil error", err)
	}
	if !res.Miss {
		t.Fatalf("Get with a failing remote = %+v, want a miss", res)
	}
	if got := tc.cache.Metrics().GetMiss; got != 1 {
		t.Fatalf("GetMiss = %d, want 1", got)
	}

	// A Put against the same broken remote must still store locally.
	body := []byte("stored despite a broken remote")
	path := tc.put(t, mkAction(8), mkOutput(8), body)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the stored body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body on disk = %q, want %q", got, body)
	}
	if res := tc.get(t, mkAction(8)); res.Miss {
		t.Fatal("Get after Put = a miss, want a local hit despite the broken remote")
	}
}

// TestRemoteGetObjectErrorDegradesToMiss pins the same guarantee one step later:
// the action resolved, so the body fetch is what failed, and that too is a miss
// with a nil error rather than a build failure.
func TestRemoteGetObjectErrorDegradesToMiss(t *testing.T) {
	tc := newTestCache(t, withRemote())
	a, o := mkAction(9), mkOutput(9)
	tc.rem.programAction(a, o, remoteMtime)
	tc.rem.getObjectErr = errBoom

	res, err := tc.cache.Get(t.Context(), a)
	if err != nil {
		t.Fatalf("Get with a failing GetObject = error %v, want a nil error", err)
	}
	if !res.Miss {
		t.Fatalf("Get with a failing GetObject = %+v, want a miss", res)
	}
	if want := []string{"GetAction", "GetObject"}; !slices.Equal(tc.rem.callOrder(), want) {
		t.Fatalf("remote calls = %v, want %v", tc.rem.callOrder(), want)
	}
	// Nothing may be recorded from a failed fault-in.
	tc.wantStats(t, 0, 0)

	if _, err := tc.cache.Put(t.Context(), a, o, bytes.NewReader([]byte("ok")), 2); err != nil {
		t.Fatalf("Put with a failing remote: %v", err)
	}
}

// TestRemoteActionPointsAtAbsentBodyIsAMiss pins the eventual-consistency case
// the upload ordering exists to avoid: an action record whose body is not in the
// bucket resolves to a miss rather than a hit that cannot be satisfied.
func TestRemoteActionPointsAtAbsentBodyIsAMiss(t *testing.T) {
	tc := newTestCache(t, withRemote())
	a, o := mkAction(10), mkOutput(10)
	// The action record exists; the object it names was never uploaded.
	tc.rem.programAction(a, o, remoteMtime)

	res, err := tc.cache.Get(t.Context(), a)
	if err != nil {
		t.Fatalf("Get over a dangling remote action = error %v, want a nil error", err)
	}
	if !res.Miss {
		t.Fatalf("Get over a dangling remote action = %+v, want a miss", res)
	}
	if got := tc.cache.Metrics().GetMiss; got != 1 {
		t.Fatalf("GetMiss = %d, want 1", got)
	}
	tc.wantStats(t, 0, 0)
}

// TestUploadErrorsNeverFailPut pins that a remote that rejects every write still
// leaves Put succeeding, the body on disk, and the entry locally hittable — only
// the failure counter moves.
func TestUploadErrorsNeverFailPut(t *testing.T) {
	tc := newTestCache(t, withRemote())
	tc.rem.putObjectErr = errBoom
	tc.rem.putActionErr = errBoom
	a, o := mkAction(11), mkOutput(11)
	body := []byte("body whose upload fails")

	path := tc.put(t, a, o, body)

	// Uploads are asynchronous, so drain the pool before asserting on the
	// upload counters.
	if err := tc.cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	m := tc.cache.Metrics()
	if m.Put != 1 {
		t.Fatalf("Put = %d, want 1", m.Put)
	}
	if m.UploadFail != 1 {
		t.Fatalf("UploadFail = %d, want 1", m.UploadFail)
	}
	if m.UploadOK != 0 {
		t.Fatalf("UploadOK = %d, want 0", m.UploadOK)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the stored body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body on disk = %q, want %q", got, body)
	}
	if res := tc.get(t, a); res.Miss {
		t.Fatal("Get after a failed upload = a miss, want a local hit")
	}
}
