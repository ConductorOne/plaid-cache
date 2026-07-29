// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestObjectsReportsRecordedCosts pins the enumeration the re-measuring pass
// walks.
func TestObjectsReportsRecordedCosts(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	a, o := mkAction(1), mkOutput(1)
	if err := ix.Put(a, Entry{OutputID: o, Size: 100}, 4096); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := ix.Objects()
	if err != nil {
		t.Fatalf("Objects: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d objects, want 1", len(got))
	}
	if got[0].OutputID != o || got[0].DiskBytes != 4096 {
		t.Fatalf("got %+v, want %s at 4096 bytes", got[0], o)
	}
}

// TestRemeasureMovesTheAggregate pins that a correction reaches the counter the
// byte budget reads. Fixing the per-object records without the total would leave
// eviction deciding on the old number.
func TestRemeasureMovesTheAggregate(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	a, o := mkAction(2), mkOutput(2)
	if err := ix.Put(a, Entry{OutputID: o, Size: 100}, 30000); err != nil {
		t.Fatalf("Put: %v", err)
	}

	applied, delta, err := ix.Remeasure(map[ids.OutputID]int64{o: 10000})
	if err != nil {
		t.Fatalf("Remeasure: %v", err)
	}
	if applied != 1 || delta != -20000 {
		t.Fatalf("applied=%d delta=%d, want 1 and -20000", applied, delta)
	}
	st, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.DiskBytes != 10000 {
		t.Fatalf("aggregate = %d, want 10000", st.DiskBytes)
	}
}

// TestRemeasureIgnoresDroppedObjects pins that a correction for something already
// evicted is skipped rather than written.
//
// A pass measures a list of objects and then applies its findings, and eviction
// can run in between. Writing the record back would resurrect an object whose body
// is deleted and add its bytes to a total that had correctly dropped them.
func TestRemeasureIgnoresDroppedObjects(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	a, o := mkAction(3), mkOutput(3)
	if err := ix.Put(a, Entry{OutputID: o, Size: 100}, 8192); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := ix.Delete(a); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	before, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	gone := mkOutput(9)
	applied, delta, err := ix.Remeasure(map[ids.OutputID]int64{gone: 4096})
	if err != nil {
		t.Fatalf("Remeasure: %v", err)
	}
	if applied != 0 || delta != 0 {
		t.Fatalf("applied=%d delta=%d for an object that is not in the index, want 0 and 0", applied, delta)
	}
	after, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.DiskBytes != before.DiskBytes || after.Objects != before.Objects {
		t.Fatalf("a correction for a missing object changed the index: %+v -> %+v", before, after)
	}
}

// TestRemeasureIsANoOpWhenNothingChanged pins that a pass over correct accounting
// writes nothing, since it runs on a ticker for as long as the budget is tight.
func TestRemeasureIsANoOpWhenNothingChanged(t *testing.T) {
	ix := openIndex(t, t.TempDir())
	a, o := mkAction(4), mkOutput(4)
	if err := ix.Put(a, Entry{OutputID: o, Size: 100}, 4096); err != nil {
		t.Fatalf("Put: %v", err)
	}
	applied, delta, err := ix.Remeasure(map[ids.OutputID]int64{o: 4096})
	if err != nil {
		t.Fatalf("Remeasure: %v", err)
	}
	if applied != 0 || delta != 0 {
		t.Fatalf("applied=%d delta=%d for an unchanged cost, want 0 and 0", applied, delta)
	}
}

// TestRemeasureOnAClosedIndexIsAnError pins the guard: Pebble panics on a closed
// database rather than returning an error, and a re-measuring pass can outlive the
// index it was measuring.
func TestRemeasureOnAClosedIndexIsAnError(t *testing.T) {
	ix, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := ix.Remeasure(map[ids.OutputID]int64{mkOutput(5): 1}); err == nil {
		t.Fatal("Remeasure on a closed index returned no error")
	}
	if _, err := ix.Objects(); err == nil {
		t.Fatal("Objects on a closed index returned no error")
	}
}
