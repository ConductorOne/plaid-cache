// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package index is the Pebble-backed action index: it maps a build action to
// the output it produced, tracks when each action was last used so eviction can
// run oldest-first, and refcounts output bodies so that a body shared by many
// actions survives until the last of them is gone.
//
// The index holds Pebble's exclusive directory lock, so exactly one process may
// have it open. That is deliberate: it is what arbitrates the daemon spawn race
// — the loser of the race sees ErrLocked and retries connecting to the winner.
//
// The index is an accelerator, never a system of record. The bodies on disk are
// the truth. Every durability decision here follows from that: a lost or
// unreadable index must degrade to a rescan, never to a wrong byte budget or to
// a body deleted while something still points at it.
package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// ErrLocked reports that another process holds the index directory lock.
var ErrLocked = errors.New("index is locked by another process")

// ErrClosed reports use of an index after Close.
var ErrClosed = errors.New("index is closed")

// persistInterval is how often the in-memory counters are mirrored into the 'm'
// table. The mirror is only ever trusted after a clean Close, so this bounds
// nothing correctness-critical; it exists so an operator inspecting a running
// cache sees roughly-current numbers.
const persistInterval = 30 * time.Second

// Index is a single-process, exclusively locked action index.
type Index struct {
	db   *pebble.DB
	lock *pebble.Lock

	// mu serializes mutations. Every mutating path is a read-modify-write over
	// an entry and its output's refcount, so it cannot be left to Pebble's
	// per-batch atomicity alone. Reads (Get, Stats) do not take it.
	mu sync.Mutex

	// Counters are an in-memory accelerator, read without locking. They are
	// exact under mu and eventually consistent to a concurrent Stats caller,
	// which is why Stats is documented as a point-in-time summary.
	actions   atomic.Int64
	objects   atomic.Int64
	diskBytes atomic.Int64
	zeroRefs  atomic.Int64

	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
	done      chan struct{}
	wg        sync.WaitGroup
}

// Open acquires Pebble's exclusive directory lock and opens the index,
// rebuilding the counters if the previous process did not shut down cleanly.
//
// A second process gets an error wrapping ErrLocked. The lock is taken
// explicitly, before Open, rather than inferred from a failed pebble.Open:
// that turns "is this a lock conflict?" from a guess about an errno or an
// error string into a fact about which call failed.
func Open(dir string) (*Index, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}
	lock, err := pebble.LockDirectory(dir, vfs.Default)
	if err != nil {
		return nil, fmt.Errorf("Open: %w: %w", ErrLocked, err)
	}
	db, err := pebble.Open(dir, &pebble.Options{Lock: lock})
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("Open: %w", err)
	}

	ix := &Index{db: db, lock: lock, done: make(chan struct{})}
	if err := ix.loadCounters(); err != nil {
		_ = db.Close()
		_ = lock.Close()
		return nil, fmt.Errorf("Open: %w", err)
	}

	ix.wg.Add(1)
	go ix.persistLoop()
	return ix, nil
}

// Close persists the counters, writes the clean-shutdown marker, and releases
// the directory lock. It is idempotent.
func (ix *Index) Close() error {
	ix.closeOnce.Do(func() {
		ix.closed.Store(true)
		close(ix.done)
		ix.wg.Wait()

		var firstErr error
		// The marker and the counters must land together: a marker without
		// fresh counters would make the next Open trust stale numbers.
		b := ix.db.NewBatch()
		if err := b.Set(metaCounters, encodeCounters(ix.snapshot()), nil); err != nil {
			firstErr = err
		}
		if err := b.Set(metaCleanShutdown, []byte{schemaV1}, nil); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := b.Commit(pebble.Sync); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := b.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := ix.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := ix.lock.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if firstErr != nil {
			ix.closeErr = fmt.Errorf("Close: %w", firstErr)
		}
	})
	return ix.closeErr
}

// closeAbruptly releases OS resources without persisting counters or writing
// the clean-shutdown marker, leaving exactly the on-disk state a killed process
// would leave. It exists so tests can exercise the rebuild path.
func (ix *Index) closeAbruptly() error {
	ix.closeOnce.Do(func() {
		ix.closed.Store(true)
		close(ix.done)
		ix.wg.Wait()
		if err := ix.db.Close(); err != nil {
			ix.closeErr = fmt.Errorf("closeAbruptly: %w", err)
		}
		if err := ix.lock.Close(); err != nil && ix.closeErr == nil {
			ix.closeErr = fmt.Errorf("closeAbruptly: %w", err)
		}
	})
	return ix.closeErr
}

// Get returns the entry for an action. A miss is (Entry{}, false, nil).
func (ix *Index) Get(a ids.ActionID) (Entry, bool, error) {
	if ix.closed.Load() {
		return Entry{}, false, fmt.Errorf("Get: %w", ErrClosed)
	}
	v, ok, err := ix.getCopy(entryKey(a))
	if err != nil {
		return Entry{}, false, fmt.Errorf("Get: %w", err)
	}
	if !ok {
		return Entry{}, false, nil
	}
	e, err := decodeEntry(v)
	if err != nil {
		return Entry{}, false, fmt.Errorf("Get: %w", err)
	}
	return e, true, nil
}

// Put inserts or replaces an action, maintaining the LRU index, the output
// refcount, and the byte counters atomically in one Pebble batch.
//
// diskBytes is the real disk usage of the body and is recorded only when the
// output is new. Outputs are content-addressed, so an existing body has an
// existing, already-counted footprint; overwriting it with a caller-supplied
// value is how a byte budget drifts.
//
// Replacing an action that pointed at a different output decrements the old
// output. If that drops it to zero refs the record is kept with Refs == 0
// rather than deleted, because Put has no way to tell the caller to remove the
// body: dropping the record would take those bytes out of the budget while the
// file stayed on disk. Evict reaps such records and reports them as orphans.
func (ix *Index) Put(a ids.ActionID, e Entry, diskBytes int64) error {
	if ix.closed.Load() {
		return fmt.Errorf("Put: %w", ErrClosed)
	}
	if err := validateEntry(e, diskBytes); err != nil {
		return fmt.Errorf("Put: %w", err)
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	old, hadOld, err := ix.loadEntry(a)
	if err != nil {
		return fmt.Errorf("Put: %w", err)
	}

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	var dActions, dObjects, dDisk, dZero int64

	if hadOld {
		if err := b.Delete(lruKey(old.LastUsedAt, a), nil); err != nil {
			return fmt.Errorf("Put: %w", err)
		}
	} else {
		dActions++
	}

	// A same-output replacement must not decrement then increment: the two
	// operations would race through zero and momentarily look like an orphan.
	sameOutput := hadOld && old.OutputID == e.OutputID
	if hadOld && !sameOutput {
		_, becameZero, err := ix.decRefLocked(b, old.OutputID, false)
		if err != nil {
			return fmt.Errorf("Put: %w", err)
		}
		if becameZero {
			dZero++
		}
	}
	if !sameOutput {
		isNew, resurrected, err := ix.incRefLocked(b, e.OutputID, diskBytes)
		if err != nil {
			return fmt.Errorf("Put: %w", err)
		}
		if isNew {
			dObjects++
			dDisk += diskBytes
		}
		if resurrected {
			dZero--
		}
	}

	if err := b.Set(entryKey(a), encodeEntry(e), nil); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if err := b.Set(lruKey(e.LastUsedAt, a), nil, nil); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Put: %w", err)
	}

	ix.actions.Add(dActions)
	ix.objects.Add(dObjects)
	ix.diskBytes.Add(dDisk)
	ix.zeroRefs.Add(dZero)
	return nil
}

// Touch advances LastUsedAt, but only if it is older than granularity — a
// relatime-style optimization that keeps the hot read path from writing on
// every cache hit. Returns whether it wrote.
func (ix *Index) Touch(a ids.ActionID, now int64, granularity time.Duration) (bool, error) {
	n, err := ix.TouchMany([]ids.ActionID{a}, now, granularity)
	return n != 0, err
}

// TouchMany advances LastUsedAt for distinct actions in one Pebble batch.
//
// LastUsedAt is an eviction hint, not part of a cache hit's correctness. A
// crash can lose recent touches and evict a hot body early, but that degrades
// only to a miss. The batch therefore does not sync: making every read wait
// for a storage flush turns LRU bookkeeping into a throughput limiter.
//
// Repeating an action is harmless; only its first occurrence is considered.
func (ix *Index) TouchMany(actions []ids.ActionID, now int64, granularity time.Duration) (int, error) {
	if ix.closed.Load() {
		return 0, fmt.Errorf("TouchMany: %w", ErrClosed)
	}
	if len(actions) == 0 {
		return 0, nil
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	seen := make(map[ids.ActionID]struct{}, len(actions))
	var touched int
	for _, a := range actions {
		if _, duplicate := seen[a]; duplicate {
			continue
		}
		seen[a] = struct{}{}

		e, ok, err := ix.loadEntry(a)
		if err != nil {
			return 0, fmt.Errorf("TouchMany: %w", err)
		}
		if !ok || now <= e.LastUsedAt {
			continue
		}
		if g := granularity.Nanoseconds(); g > 0 && now-e.LastUsedAt < g {
			continue
		}

		// The old key must leave with the new key: stale LRU rows can make
		// eviction revisit an action after its body was already released.
		if err := b.Delete(lruKey(e.LastUsedAt, a), nil); err != nil {
			return 0, fmt.Errorf("TouchMany: %w", err)
		}
		e.LastUsedAt = now
		if err := b.Set(entryKey(a), encodeEntry(e), nil); err != nil {
			return 0, fmt.Errorf("TouchMany: %w", err)
		}
		if err := b.Set(lruKey(now, a), nil, nil); err != nil {
			return 0, fmt.Errorf("TouchMany: %w", err)
		}
		touched++
	}
	if touched == 0 {
		return 0, nil
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return 0, fmt.Errorf("TouchMany: %w", err)
	}
	return touched, nil
}

// Delete removes an action, decrements its output refcount, and reports whether
// the body became unreferenced and should be removed from the blob store.
func (ix *Index) Delete(a ids.ActionID) (ids.OutputID, bool, error) {
	if ix.closed.Load() {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", ErrClosed)
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	e, ok, err := ix.loadEntry(a)
	if err != nil {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", err)
	}
	if !ok {
		return ids.OutputID{}, false, nil
	}

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Delete(entryKey(a), nil); err != nil {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", err)
	}
	if err := b.Delete(lruKey(e.LastUsedAt, a), nil); err != nil {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", err)
	}
	// Unlike Put, Delete can hand the orphan back to the caller, so the record
	// is removed outright and its bytes leave the budget.
	freed, isOrphan, err := ix.decRefLocked(b, e.OutputID, true)
	if err != nil {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return ids.OutputID{}, false, fmt.Errorf("Delete: %w", err)
	}

	ix.actions.Add(-1)
	if isOrphan {
		ix.objects.Add(-1)
		ix.diskBytes.Add(-freed)
		return e.OutputID, true, nil
	}
	return ids.OutputID{}, false, nil
}

// Stats is a point-in-time summary.
type Stats struct {
	Actions   int64
	Objects   int64
	DiskBytes int64
}

// Stats returns the current counters. It reads the in-memory accelerator, so it
// is cheap enough to call on every request.
func (ix *Index) Stats() (Stats, error) {
	if ix.closed.Load() {
		return Stats{}, fmt.Errorf("Stats: %w", ErrClosed)
	}
	return Stats{
		Actions:   ix.actions.Load(),
		Objects:   ix.objects.Load(),
		DiskBytes: ix.diskBytes.Load(),
	}, nil
}

// Compact reclaims LSM space after a large eviction. Deletes are tombstones, so
// an eviction pass temporarily INFLATES the store until this runs.
func (ix *Index) Compact() error {
	if ix.closed.Load() {
		return fmt.Errorf("Compact: %w", ErrClosed)
	}
	// The bounds span every prefix in use; Pebble requires start < end.
	if err := ix.db.Compact(context.Background(), []byte{0x00}, []byte{0xff}, true); err != nil {
		return fmt.Errorf("Compact: %w", err)
	}
	return nil
}

// validateEntry rejects values that would corrupt the keyspace or the budget.
// A negative timestamp sorts past every positive one in the big-endian LRU key
// and would make the entry permanently un-evictable.
func validateEntry(e Entry, diskBytes int64) error {
	switch {
	case e.CreatedAt < 0:
		return fmt.Errorf("negative CreatedAt %d", e.CreatedAt)
	case e.LastUsedAt < 0:
		return fmt.Errorf("negative LastUsedAt %d", e.LastUsedAt)
	case e.Size < 0:
		return fmt.Errorf("negative Size %d", e.Size)
	case diskBytes < 0:
		return fmt.Errorf("negative diskBytes %d", diskBytes)
	}
	return nil
}

// loadEntry reads and decodes an entry. Callers hold mu when the result feeds a
// read-modify-write.
func (ix *Index) loadEntry(a ids.ActionID) (Entry, bool, error) {
	v, ok, err := ix.getCopy(entryKey(a))
	if err != nil || !ok {
		return Entry{}, false, err
	}
	e, err := decodeEntry(v)
	if err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

// loadObjRef reads and decodes an output refcount record.
func (ix *Index) loadObjRef(o ids.OutputID) (objRef, bool, error) {
	v, ok, err := ix.getCopy(objKey(o))
	if err != nil || !ok {
		return objRef{}, false, err
	}
	r, err := decodeObjRef(v)
	if err != nil {
		return objRef{}, false, err
	}
	return r, true, nil
}

// incRefLocked adds a reference to an output, creating the record if needed.
// isNew reports that the output is new to the index and its bytes must be added
// to the budget. resurrected reports that a record left at Refs == 0 by a Put
// replacement is referenced again and is no longer pending reaping.
func (ix *Index) incRefLocked(b *pebble.Batch, o ids.OutputID, diskBytes int64) (isNew, resurrected bool, err error) {
	r, ok, err := ix.loadObjRef(o)
	if err != nil {
		return false, false, err
	}
	if !ok {
		return true, false, b.Set(objKey(o), encodeObjRef(objRef{Refs: 1, DiskBytes: diskBytes}), nil)
	}
	resurrected = r.Refs == 0
	r.Refs++
	// DiskBytes is deliberately left as recorded; see Put's doc comment.
	return false, resurrected, b.Set(objKey(o), encodeObjRef(r), nil)
}

// decRefLocked removes a reference from an output. When the count reaches zero,
// dropRecord decides between deleting the record (the caller will remove the
// body and so must be told its size) and retaining it at Refs == 0 (the caller
// cannot, so the bytes must stay in the budget until Evict reaps it).
func (ix *Index) decRefLocked(b *pebble.Batch, o ids.OutputID, dropRecord bool) (freed int64, hitZero bool, err error) {
	r, ok, err := ix.loadObjRef(o)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		// Defensive: an entry referencing a missing output means the two tables
		// disagree, which atomic batches should make impossible. Treat it as
		// already released rather than propagating a corrupt refcount.
		return 0, false, nil
	}
	r.Refs--
	if r.Refs > 0 {
		return 0, false, b.Set(objKey(o), encodeObjRef(r), nil)
	}
	if dropRecord {
		return r.DiskBytes, true, b.Delete(objKey(o), nil)
	}
	r.Refs = 0
	return 0, true, b.Set(objKey(o), encodeObjRef(r), nil)
}

// getCopy reads a key and copies the value, because Pebble's returned slice is
// only valid until the closer runs.
func (ix *Index) getCopy(key []byte) ([]byte, bool, error) {
	v, closer, err := ix.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, closer.Close()
}

// AgeSpan reports the last-used timestamps of the least and most recently used
// entries, in Unix nanoseconds. ok is false when the index is empty.
//
// It is two seeks rather than a scan: the LRU keyspace is ordered by timestamp,
// so the first and last keys under that prefix are the extremes by
// construction. That is what makes it cheap enough for a status command to call
// on every invocation.
func (ix *Index) AgeSpan() (oldest, newest int64, ok bool, err error) {
	if ix.closed.Load() {
		return 0, 0, false, fmt.Errorf("AgeSpan: %w", ErrClosed)
	}
	lower, upper := prefixRange(prefixLRU)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, 0, false, fmt.Errorf("AgeSpan: %w", err)
	}
	defer func() {
		if cerr := iter.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("AgeSpan: close iterator: %w", cerr)
		}
	}()

	if !iter.First() {
		return 0, 0, false, nil
	}
	oldest, _, err = parseLRUKey(iter.Key())
	if err != nil {
		return 0, 0, false, fmt.Errorf("AgeSpan: %w", err)
	}
	if !iter.Last() {
		// A valid First with no Last cannot happen, but treat the single-entry
		// case explicitly rather than returning a zero newest.
		return oldest, oldest, true, nil
	}
	newest, _, err = parseLRUKey(iter.Key())
	if err != nil {
		return 0, 0, false, fmt.Errorf("AgeSpan: %w", err)
	}
	return oldest, newest, true, nil
}
