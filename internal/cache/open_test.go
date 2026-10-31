// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// TestOpenServesTheStoredBytes pins that Open hands back the body itself, with
// the length of the file it opened, and counts a local hit like Get does.
func TestOpenServesTheStoredBytes(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(43), mkOutput(43)
	body := []byte("opened rather than stat'ed")
	tc.put(t, a, o, body)

	f, res, err := tc.cache.Open(t.Context(), a)
	if err != nil || res.Miss {
		t.Fatalf("Open = %+v, %v; want a hit", res, err)
	}
	defer func() { _ = f.Close() }()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("reading the opened body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if res.Size != int64(len(body)) || res.OutputID != o {
		t.Fatalf("Open result = %+v, want size %d and output %s", res, len(body), o)
	}
	if m := tc.cache.Metrics(); m.GetLocalHit != 1 {
		t.Fatalf("GetLocalHit = %d, want 1", m.GetLocalHit)
	}
}

// TestOpenRepairsDanglingIndexEntry pins that Open shares Get's repair: a body
// gone from disk is a miss that drops the entry, never a nil file reported as
// a hit.
func TestOpenRepairsDanglingIndexEntry(t *testing.T) {
	tc := newTestCache(t)
	a, o := mkAction(44), mkOutput(44)
	tc.put(t, a, o, []byte("about to go missing"))
	if err := os.Remove(tc.blobs.Path(o)); err != nil {
		t.Fatalf("removing the body: %v", err)
	}

	f, res, err := tc.cache.Open(t.Context(), a)
	if err != nil || !res.Miss || f != nil {
		t.Fatalf("Open over a deleted body = %v, %+v, %v; want a nil file and a miss", f, res, err)
	}
	if m := tc.cache.Metrics(); m.GetRepair != 1 {
		t.Fatalf("GetRepair = %d, want 1", m.GetRepair)
	}
	if _, ok, err := tc.idx.Get(a); err != nil || ok {
		t.Fatalf("index entry after repair: present=%v err=%v, want it dropped", ok, err)
	}
}
