// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
)

// ReconcileResult reports what a re-measuring pass found.
type ReconcileResult struct {
	Objects   int64 // objects examined
	Corrected int64 // records whose cost was wrong
	Pending   int64 // too recently written to measure yet
	Missing   int64 // recorded but no longer on disk
	Before    int64 // aggregate cost before the pass
	After     int64 // aggregate cost after it
	Elapsed   time.Duration
}

// Delta is how much the recorded total moved.
func (r ReconcileResult) Delta() int64 { return r.After - r.Before }

// Reconcile re-measures every body against the filesystem and corrects the
// recorded costs the byte budget adds up.
//
// A body's cost is first recorded the moment it is written, which is the one
// moment it cannot be measured: allocation is deferred to the next transaction
// group, so the figure taken then is deliberately an overestimate. Nothing used
// to revisit it. On a compressing filesystem that overestimate is permanent and
// large — measured at 3x, evicting 34748 entries while two thirds of the budget
// sat unused (issue #5).
//
// So the figure is corrected once it can be trusted. Bodies still inside the
// settle window are counted as pending and left for the next pass, which is what
// keeps the deferred-allocation undercount from being written back as truth.
func (c *Cache) Reconcile(ctx context.Context) (ReconcileResult, error) {
	start := time.Now()
	var r ReconcileResult
	defer func() { r.Elapsed = time.Since(start) }()

	objects, err := c.idx.Objects()
	if err != nil {
		return r, fmt.Errorf("Reconcile: %w", err)
	}
	if st, serr := c.idx.Stats(); serr == nil {
		r.Before = st.DiskBytes
	}

	// One instant for the whole pass, so every body is judged against the same
	// settle boundary rather than drifting across it mid-scan.
	now := time.Now()
	corrections := make(map[ids.OutputID]int64)
	for _, o := range objects {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		r.Objects++
		bytes, settled, merr := c.blobs.Measure(o.OutputID, now)
		if merr != nil {
			if errors.Is(merr, fs.ErrNotExist) {
				// The body is gone under the record. Eviction's own repair path
				// owns that; correcting a cost here would only make a dangling
				// record look healthy.
				r.Missing++
				continue
			}
			c.logf("reconcile measure %s: %v", o.OutputID, merr)
			continue
		}
		if !settled {
			r.Pending++
			continue
		}
		if bytes != o.DiskBytes {
			corrections[o.OutputID] = bytes
		}
	}

	applied, _, err := c.idx.Remeasure(corrections)
	if err != nil {
		return r, fmt.Errorf("Reconcile: %w", err)
	}
	r.Corrected = int64(applied)
	if st, serr := c.idx.Stats(); serr == nil {
		r.After = st.DiskBytes
	}
	return r, nil
}

// EvictNow is EvictWith for a pass someone asked for by hand.
//
// It measures first regardless of how much room appears to be left. The threshold
// below exists to keep the stats off the automatic path, where nothing is at stake
// until the budget is tight; an operator running gc is asking for the sweep and for
// the numbers it is decided on, and telling them "not until you are at 90%" would
// be a strange answer to give.
func (c *Cache) EvictNow(ctx context.Context, maxBytes int64, ttl time.Duration) (index.EvictResult, ReconcileResult, error) {
	rec, err := c.Reconcile(ctx)
	if err != nil {
		// Evict on the figures we have rather than refusing the request.
		c.logf("gc: reconcile: %v", err)
	}
	res, err := c.EvictWith(ctx, maxBytes, ttl)
	return res, rec, err
}

// reconcileAtShare is the fraction of the budget at which eviction re-measures
// before deciding anything.
//
// Below it the accounting cannot be costing anyone anything, so the stats are
// pure overhead. At it, the difference between a recorded figure and the truth is
// the difference between evicting and not, which is worth one pass over the
// bodies — tens of milliseconds for tens of thousands of files, against pruning
// entries that did not need pruning.
const reconcileAtShare = 0.9

// reconcileBeforeEvicting corrects the accounting when the budget looks tight,
// so that a size-driven eviction is decided on measured bytes rather than on an
// estimate taken before the filesystem had allocated anything.
func (c *Cache) reconcileBeforeEvicting(ctx context.Context, maxBytes int64) {
	if maxBytes <= 0 {
		return
	}
	st, err := c.idx.Stats()
	if err != nil {
		return
	}
	if float64(st.DiskBytes) < reconcileAtShare*float64(maxBytes) {
		return
	}
	res, err := c.Reconcile(ctx)
	if err != nil {
		// Eviction proceeds on the figures it has. Refusing to evict because the
		// measurement failed would be the one outcome worse than evicting early.
		c.logf("reconcile: %v", err)
		return
	}
	if res.Corrected > 0 {
		c.logf("reconcile: corrected %d of %d objects, %d pending, recorded size %d -> %d bytes in %v",
			res.Corrected, res.Objects, res.Pending, res.Before, res.After, res.Elapsed.Round(time.Millisecond))
	}
}
