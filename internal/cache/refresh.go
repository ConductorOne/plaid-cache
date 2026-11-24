// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// The shared tier's age problem.
//
// A bucket's lifecycle rule expires an object by how long ago it was written,
// not by when it was last read. Nothing on the read path writes: a hit is
// served from the local tier or faulted in from the bucket, and an upload of a
// body the bucket already holds is skipped by PutObject's precondition. So an
// entry's remote age counts from its first upload however much it is used, and
// the entries in heaviest use — the ones every build hits and nobody re-uploads
// — are exactly the ones that expire on schedule.
//
// For Bazel that is worse than a miss. An action result names CAS blobs, a
// client that gets the result does not download blobs it does not need yet,
// and when it later does need one that has expired in the meantime the build
// fails with "lost inputs" rather than re-running anything. Bazel retries, but
// a retry is served the same action result naming the same absent blob.
//
// The refresher closes that gap from the side that knows an entry is in use:
// every local hit offers its action here, and an action whose remote record or
// body is older than RemoteRefreshAfter is written to the bucket again from the
// local copy. A hit therefore keeps an entry alive in the shared tier the same
// way it keeps it alive in the local one.

// refreshShards spreads the recently-checked set over independent locks, for
// the same reason the presence memo is sharded: every hit offers its action.
const refreshShards = 64

// refreshShardCap bounds each shard, and so the set: 64 shards of 8192 is
// about half a million actions, the same bound as the presence memo. When a
// shard fills it is emptied, which costs a repeat check of whatever it held
// and nothing else.
const refreshShardCap = 1 << 13

// refreshQueueDepth bounds the backlog. A full queue drops the offer; the next
// hit on the same action offers it again once its recheck time passes.
const refreshQueueDepth = 1 << 14

// refreshWorkers is how many checks run at once. Each one is a GET of a short
// action record and a HEAD, and only a stale entry costs an upload.
const refreshWorkers = 4

// refreshTimeout bounds one check-and-rewrite, upload included.
const refreshTimeout = 5 * time.Minute

// RefreshSnapshot reports what the refresher has done since this process
// started.
type RefreshSnapshot struct {
	// Checked is how many actions had their remote age looked up.
	Checked int64 `json:"checked"`
	// Rewritten is how many of those had a record or body written again.
	Rewritten int64 `json:"rewritten"`
	// Failed is how many checks or rewrites failed.
	Failed int64 `json:"failed"`
	// Dropped is how many offers found the queue full.
	Dropped int64 `json:"dropped"`
}

// refresher checks the remote age of actions in local use and rewrites the
// stale ones.
type refresher struct {
	after   time.Duration
	recheck time.Duration
	check   func(context.Context, ids.ActionID)
	logf    Logf

	shards [refreshShards]refreshShard
	jobs   chan ids.ActionID
	wg     sync.WaitGroup

	// mu guards jobs against a send racing close; see uploader.mu.
	mu     sync.RWMutex
	closed bool

	checked   atomic.Int64
	rewritten atomic.Int64
	failed    atomic.Int64
	dropped   atomic.Int64
}

// refreshShard is one independently locked slice of the recently-checked set.
type refreshShard struct {
	mu   sync.Mutex
	next map[ids.ActionID]int64
}

// newRefresher starts the workers. A non-positive after returns a refresher
// that ignores every offer.
func newRefresher(after time.Duration, check func(context.Context, ids.ActionID), logf Logf) *refresher {
	r := &refresher{after: after, check: check, logf: logf}
	if after <= 0 {
		return r
	}
	// Checking more often than the window shrinks the margin a stale entry can
	// eat into before it is noticed; a quarter of it keeps that margin at most
	// a quarter of the window without checking a hot entry on every hit.
	r.recheck = max(after/4, time.Minute)
	r.jobs = make(chan ids.ActionID, refreshQueueDepth)
	r.wg.Add(refreshWorkers)
	for range refreshWorkers {
		go r.worker()
	}
	return r
}

// offer reports that an action was just used locally.
func (r *refresher) offer(a ids.ActionID, now int64) {
	if r.jobs == nil {
		return
	}
	sh := &r.shards[int(a[0])%refreshShards]
	sh.mu.Lock()
	if next, ok := sh.next[a]; ok && now < next {
		sh.mu.Unlock()
		return
	}
	if sh.next == nil || len(sh.next) >= refreshShardCap {
		sh.next = make(map[ids.ActionID]int64)
	}
	sh.next[a] = now + r.recheck.Nanoseconds()
	sh.mu.Unlock()

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}
	select {
	case r.jobs <- a:
	default:
		r.dropped.Add(1)
		// Forget it so the next hit offers it again rather than waiting out
		// a recheck window for a check that never ran.
		sh.mu.Lock()
		delete(sh.next, a)
		sh.mu.Unlock()
	}
}

func (r *refresher) worker() {
	defer r.wg.Done()
	for a := range r.jobs {
		ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
		r.check(ctx, a)
		cancel()
	}
}

// close stops accepting offers and waits for in-flight checks. Queued checks
// are abandoned: each is an optimisation the next run of the daemon redoes.
func (r *refresher) close() {
	if r.jobs == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	close(r.jobs)
	r.mu.Unlock()
	// Drain without running, so exit is not held up by a backlog.
	go func() {
		for range r.jobs {
			r.dropped.Add(1)
		}
	}()
	r.wg.Wait()
}

// Snapshot reports the refresher's counters.
func (r *refresher) Snapshot() RefreshSnapshot {
	return RefreshSnapshot{
		Checked:   r.checked.Load(),
		Rewritten: r.rewritten.Load(),
		Failed:    r.failed.Load(),
		Dropped:   r.dropped.Load(),
	}
}

// refreshRemote makes sure the shared tier's copy of a locally held action is
// no older than RemoteRefreshAfter, writing it again from the local copy where
// it is.
func (c *Cache) refreshRemote(ctx context.Context, a ids.ActionID) {
	r := c.refresh
	e, ok, err := c.idx.Get(a)
	if err != nil || !ok {
		// Evicted since the hit. Nothing local to write from, and nothing
		// local that is still using it.
		return
	}

	r.checked.Add(1)
	f, err := c.rem.Freshness(ctx, a, e.OutputID)
	if err != nil {
		r.failed.Add(1)
		c.logf("refresh %s: %v", a, err)
		return
	}
	cutoff := time.Now().Add(-r.after)
	// A zero time, which is how Freshness says absent, is before any cutoff.
	staleBody := f.ObjectWritten.Before(cutoff)
	staleRecord := f.ActionWritten.Before(cutoff)
	if !staleRecord && f.Output != e.OutputID {
		// A fresh record naming a different output is another machine's newer
		// answer for the same action, not ours to overwrite.
		return
	}
	if !staleBody && !staleRecord {
		return
	}

	if staleBody {
		body, size, err := c.blobs.Open(e.OutputID)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				c.logf("refresh %s: open %s: %v", a, e.OutputID, err)
			}
			r.failed.Add(1)
			return
		}
		err = c.rem.RewriteObject(ctx, e.OutputID, body, size)
		_ = body.Close() // read-only; the rewrite's error is the one that matters
		if err != nil {
			r.failed.Add(1)
			c.logf("refresh %s: rewrite %s: %v", a, e.OutputID, err)
			return
		}
	}
	// The record is written after the body, as on upload, and also whenever
	// the body was: a record older than its body would expire first and turn a
	// refreshed body into an unreferenced one.
	if err := c.rem.PutAction(ctx, a, e.OutputID, time.Unix(0, e.CreatedAt)); err != nil {
		r.failed.Add(1)
		c.logf("refresh %s: put action: %v", a, err)
		return
	}
	r.rewritten.Add(1)
}
