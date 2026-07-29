// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/plaid-cache/internal/ids"
)

// ObjectCost is one object's recorded disk cost.
type ObjectCost struct {
	OutputID  ids.OutputID
	DiskBytes int64
}

// Objects returns every object and the cost currently recorded for it.
//
// It materialises the list rather than calling back under the lock, because the
// caller's next move is to stat files — which is far slower than this scan and
// must not hold the index against every build on the machine while it happens.
func (ix *Index) Objects() ([]ObjectCost, error) {
	if ix.closed.Load() {
		return nil, fmt.Errorf("Objects: %w", ErrClosed)
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	lower, upper := prefixRange(prefixObj)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("Objects: %w", err)
	}
	defer func() {
		if cerr := iter.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("Objects: close iterator: %w", cerr)
		}
	}()

	var out []ObjectCost
	for iter.First(); iter.Valid(); iter.Next() {
		o, ok := outputIDFromObjKey(iter.Key())
		if !ok {
			continue
		}
		r, derr := decodeObjRef(iter.Value())
		if derr != nil {
			return nil, fmt.Errorf("Objects: %w", derr)
		}
		out = append(out, ObjectCost{OutputID: o, DiskBytes: r.DiskBytes})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("Objects: %w", err)
	}
	return out, nil
}

// Remeasure replaces the recorded cost of the given objects and adjusts the
// aggregate the byte budget reads.
//
// Corrections are applied in one batch, since a pass that touches thousands of
// objects one write at a time would be slower than the stats that produced it.
//
// An object that has since been dropped is skipped rather than resurrected: its
// bytes are already out of the aggregate, and writing the record back would
// re-add a body that eviction has deleted.
func (ix *Index) Remeasure(corrections map[ids.OutputID]int64) (applied int, delta int64, err error) {
	if len(corrections) == 0 {
		return 0, 0, nil
	}
	if ix.closed.Load() {
		return 0, 0, fmt.Errorf("Remeasure: %w", ErrClosed)
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	for o, want := range corrections {
		r, ok, gerr := ix.loadObjRef(o)
		if gerr != nil {
			return 0, 0, fmt.Errorf("Remeasure: %w", gerr)
		}
		if !ok || r.DiskBytes == want {
			continue
		}
		delta += want - r.DiskBytes
		r.DiskBytes = want
		if serr := b.Set(objKey(o), encodeObjRef(r), nil); serr != nil {
			return 0, 0, fmt.Errorf("Remeasure: %w", serr)
		}
		applied++
	}
	if applied == 0 {
		return 0, 0, nil
	}
	if cerr := b.Commit(pebble.NoSync); cerr != nil {
		return 0, 0, fmt.Errorf("Remeasure: %w", cerr)
	}
	ix.diskBytes.Add(delta)
	return applied, delta, nil
}
