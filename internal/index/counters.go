// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// snapshot reads the in-memory counters.
func (ix *Index) snapshot() countersSnapshot {
	return countersSnapshot{
		Actions:   ix.actions.Load(),
		Objects:   ix.objects.Load(),
		DiskBytes: ix.diskBytes.Load(),
		ZeroRefs:  ix.zeroRefs.Load(),
	}
}

// restore installs a counter snapshot.
func (ix *Index) restore(c countersSnapshot) {
	ix.actions.Store(c.Actions)
	ix.objects.Store(c.Objects)
	ix.diskBytes.Store(c.DiskBytes)
	ix.zeroRefs.Store(c.ZeroRefs)
}

// loadCounters populates the counters at Open and clears the clean-shutdown
// marker.
//
// The persisted counters are trusted only when the marker proves the previous
// process ran Close. Absent or unreadable, the counters are rebuilt from the
// records themselves. Clearing the marker is committed with Sync before the
// index serves anything, so a crash at any later point is detectable.
func (ix *Index) loadCounters() error {
	marker, clean, err := ix.getCopy(metaCleanShutdown)
	if err != nil {
		return fmt.Errorf("loadCounters: %w", err)
	}
	if clean && (len(marker) != 1 || marker[0] != schemaV1) {
		// A marker we cannot vouch for is the same as no marker.
		clean = false
	}

	loaded := false
	if clean {
		if v, ok, err := ix.getCopy(metaCounters); err != nil {
			return fmt.Errorf("loadCounters: %w", err)
		} else if ok {
			if c, derr := decodeCounters(v); derr == nil {
				ix.restore(c)
				loaded = true
			}
		}
	}
	if !loaded {
		if err := ix.rebuildCounters(); err != nil {
			return fmt.Errorf("loadCounters: %w", err)
		}
	}

	if clean {
		b := ix.db.NewBatch()
		defer b.Close()
		if err := b.Delete(metaCleanShutdown, nil); err != nil {
			return fmt.Errorf("loadCounters: %w", err)
		}
		if err := b.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("loadCounters: %w", err)
		}
	}
	return nil
}

// rebuildCounters recomputes every counter by scanning the 'o' table.
//
// One scan yields all three public counters. Objects is the record count and
// DiskBytes their sum; Actions is the sum of the refcounts, which equals the
// number of 'e' records because every mutation writes an entry and its
// refcount in the same atomic batch. Deriving Actions this way avoids a second
// full scan of a table that can hold a million keys.
func (ix *Index) rebuildCounters() error {
	lower, upper := prefixRange(prefixObj)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("rebuildCounters: %w", err)
	}
	defer iter.Close()

	var c countersSnapshot
	for iter.First(); iter.Valid(); iter.Next() {
		r, err := decodeObjRef(iter.Value())
		if err != nil {
			return fmt.Errorf("rebuildCounters: %w", err)
		}
		c.Objects++
		c.DiskBytes += r.DiskBytes
		c.Actions += r.Refs
		if r.Refs == 0 {
			c.ZeroRefs++
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("rebuildCounters: %w", err)
	}
	ix.restore(c)
	return nil
}

// persistCounters mirrors the in-memory counters into the 'm' table.
func (ix *Index) persistCounters(opts *pebble.WriteOptions) error {
	if err := ix.db.Set(metaCounters, encodeCounters(ix.snapshot()), opts); err != nil {
		return fmt.Errorf("persistCounters: %w", err)
	}
	return nil
}

// persistLoop mirrors the counters on a ticker so a running cache reports
// roughly-current numbers to an operator reading the store directly.
func (ix *Index) persistLoop() {
	defer ix.wg.Done()
	t := time.NewTicker(persistInterval)
	defer t.Stop()
	for {
		select {
		case <-ix.done:
			return
		case <-t.C:
			// A failed mirror is not worth failing a build over: the counters
			// are rebuilt from the records whenever they cannot be trusted.
			_ = ix.persistCounters(pebble.NoSync)
		}
	}
}
