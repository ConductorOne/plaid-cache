// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// evictChunkSize bounds how many actions one eviction batch may remove. A pass
// over a million-entry index must not build a single enormous Pebble batch:
// the batch is buffered in memory before commit, and a multi-hundred-megabyte
// one would spike the daemon's RSS at exactly the moment the machine is already
// short of disk.
const evictChunkSize = 1000

// EvictResult reports what an eviction pass did.
type EvictResult struct {
	ActionsPruned int64
	ObjectsPruned int64
	BytesFreed    int64
	Elapsed       time.Duration
}

// Evict prunes oldest-first by LastUsedAt until BOTH constraints hold: every
// remaining entry is newer than ttl, and total disk bytes <= maxBytes. ttl <= 0
// disables the TTL constraint; maxBytes <= 0 disables the size constraint.
//
// onOrphan is called for each body that became unreferenced; if it returns an
// error the pass aborts and the error is returned, but work already committed
// stays committed.
//
// onOrphan runs after its chunk commits, never before. The reverse order would
// briefly leave an index entry pointing at a deleted body, and a Get hit that
// resolves to a missing file fails the build outright — strictly worse than the
// leaked file a failed onOrphan leaves behind.
func (ix *Index) Evict(
	ctx context.Context,
	maxBytes int64,
	ttl time.Duration,
	now int64,
	onOrphan func(ids.OutputID) error,
) (res EvictResult, err error) {
	if ix.closed.Load() {
		return EvictResult{}, fmt.Errorf("Evict: %w", ErrClosed)
	}
	start := time.Now()
	defer func() { res.Elapsed = time.Since(start) }()

	if onOrphan == nil {
		onOrphan = func(ids.OutputID) error { return nil }
	}

	ttlEnabled := ttl > 0
	var ttlCutoff int64
	if ttlEnabled {
		ttlCutoff = now - ttl.Nanoseconds()
	}

	// Bodies stranded by a Put that repointed an action at a different output
	// are pure garbage: nothing references them, and their bytes sit in the
	// budget until they are reaped. Clearing them first may satisfy the size
	// constraint without evicting a single live entry.
	if ix.zeroRefs.Load() > 0 {
		if err := ix.reapZeroRefs(ctx, &res, onOrphan); err != nil {
			return res, err
		}
	}

	for {
		if cerr := ctx.Err(); cerr != nil {
			return res, fmt.Errorf("Evict: %w", cerr)
		}
		orphans, progressed, cerr := ix.evictChunk(ttlEnabled, ttlCutoff, maxBytes, &res)
		if cerr != nil {
			return res, fmt.Errorf("Evict: %w", cerr)
		}
		for _, o := range orphans {
			if oerr := onOrphan(o); oerr != nil {
				return res, fmt.Errorf("Evict: %w", oerr)
			}
		}
		if !progressed {
			return res, nil
		}
	}
}

// evictChunk removes up to evictChunkSize actions in one atomic batch, stopping
// early as soon as neither constraint is violated. It returns the outputs that
// became unreferenced so the caller can report them outside the lock.
func (ix *Index) evictChunk(
	ttlEnabled bool,
	ttlCutoff int64,
	maxBytes int64,
	res *EvictResult,
) (orphans []ids.OutputID, progressed bool, err error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	sizeEnabled := maxBytes > 0
	disk := ix.diskBytes.Load()

	lower, upper := prefixRange(prefixLRU)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, false, err
	}
	// As in rebuildCounters: what Close reports is the iteration error that
	// iter.Error() below has already read.
	defer func() { _ = iter.Close() }()

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	// Two actions in one chunk can share an output. Decrementing each from the
	// committed value would apply both decrements to the same base and lose
	// one, so in-flight refcounts are tracked here for the life of the batch.
	pending := make(map[ids.OutputID]objRef)
	dropped := make(map[ids.OutputID]struct{})

	var nActions, nObjects, freed, stale int64

	for iter.First(); iter.Valid(); iter.Next() {
		// Stale keys count against the chunk too: a table full of them would
		// otherwise build one unbounded batch.
		if nActions+stale >= evictChunkSize {
			break
		}
		ts, a, perr := parseLRUKey(iter.Key())
		if perr != nil {
			return nil, false, perr
		}

		needTTL := ttlEnabled && ts < ttlCutoff
		needSize := sizeEnabled && disk-freed > maxBytes
		if !needTTL && !needSize {
			break
		}

		e, ok, gerr := ix.loadEntry(a)
		if gerr != nil {
			return nil, false, gerr
		}
		if !ok {
			// An 'l' key with no 'e' record cannot arise from atomic batches;
			// if one exists anyway, drop it rather than looping on it forever.
			if derr := b.Delete(lruKey(ts, a), nil); derr != nil {
				return nil, false, derr
			}
			stale++
			continue
		}

		if derr := b.Delete(entryKey(a), nil); derr != nil {
			return nil, false, derr
		}
		if derr := b.Delete(lruKey(e.LastUsedAt, a), nil); derr != nil {
			return nil, false, derr
		}
		nActions++

		o := e.OutputID
		if _, gone := dropped[o]; gone {
			// Already released earlier in this chunk: the refcount that
			// reached zero was wrong, but double-counting the bytes would
			// make the budget wrong too.
			continue
		}
		r, tracked := pending[o]
		if !tracked {
			var found bool
			r, found, gerr = ix.loadObjRef(o)
			if gerr != nil {
				return nil, false, gerr
			}
			if !found {
				continue
			}
		}
		r.Refs--
		if r.Refs > 0 {
			pending[o] = r
			if serr := b.Set(objKey(o), encodeObjRef(r), nil); serr != nil {
				return nil, false, serr
			}
			continue
		}
		if derr := b.Delete(objKey(o), nil); derr != nil {
			return nil, false, derr
		}
		delete(pending, o)
		dropped[o] = struct{}{}
		freed += r.DiskBytes
		nObjects++
		orphans = append(orphans, o)
	}
	if ierr := iter.Error(); ierr != nil {
		return nil, false, ierr
	}
	if nActions == 0 && stale == 0 {
		return nil, false, nil
	}
	if cerr := b.Commit(pebble.Sync); cerr != nil {
		return nil, false, cerr
	}

	ix.actions.Add(-nActions)
	ix.objects.Add(-nObjects)
	ix.diskBytes.Add(-freed)
	res.ActionsPruned += nActions
	res.ObjectsPruned += nObjects
	res.BytesFreed += freed
	return orphans, true, nil
}

// reapZeroRefs deletes every output record left at Refs == 0 and reports each
// as an orphan.
func (ix *Index) reapZeroRefs(ctx context.Context, res *EvictResult, onOrphan func(ids.OutputID) error) error {
	for {
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("reapZeroRefs: %w", cerr)
		}
		orphans, more, err := ix.reapChunk(res)
		if err != nil {
			return fmt.Errorf("reapZeroRefs: %w", err)
		}
		for _, o := range orphans {
			if oerr := onOrphan(o); oerr != nil {
				return fmt.Errorf("reapZeroRefs: %w", oerr)
			}
		}
		if !more {
			return nil
		}
	}
}

// reapChunk removes up to evictChunkSize zero-ref output records. When it
// reaches the end of the 'o' table it also resets the zeroRefs counter to zero,
// which self-heals any drift and guarantees a later pass will not rescan.
func (ix *Index) reapChunk(res *EvictResult) (orphans []ids.OutputID, more bool, err error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	lower, upper := prefixRange(prefixObj)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, false, err
	}
	// As in rebuildCounters: what Close reports is the iteration error that
	// iter.Error() below has already read.
	defer func() { _ = iter.Close() }()

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	var nObjects, freed int64
	hitLimit := false
	for iter.First(); iter.Valid(); iter.Next() {
		if nObjects >= evictChunkSize {
			hitLimit = true
			break
		}
		r, derr := decodeObjRef(iter.Value())
		if derr != nil {
			return nil, false, derr
		}
		if r.Refs != 0 {
			continue
		}
		var o ids.OutputID
		copy(o[:], iter.Key()[1:])
		if delErr := b.Delete(objKey(o), nil); delErr != nil {
			return nil, false, delErr
		}
		nObjects++
		freed += r.DiskBytes
		orphans = append(orphans, o)
	}
	if ierr := iter.Error(); ierr != nil {
		return nil, false, ierr
	}

	if nObjects > 0 {
		if cerr := b.Commit(pebble.Sync); cerr != nil {
			return nil, false, cerr
		}
		ix.objects.Add(-nObjects)
		ix.diskBytes.Add(-freed)
		res.ObjectsPruned += nObjects
		res.BytesFreed += freed
	}
	if hitLimit {
		ix.zeroRefs.Add(-nObjects)
		return orphans, true, nil
	}
	ix.zeroRefs.Store(0)
	return orphans, false, nil
}
