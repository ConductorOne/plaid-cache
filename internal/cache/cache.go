// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package cache joins the three tiers — the Pebble index, the local body
// store, and the shared remote bucket — into the two operations the Go
// toolchain actually asks for.
//
// The ordering rule throughout is that local state is authoritative and
// remote state is an optimization. Any remote error degrades to a miss, and
// uploads never block the caller, because a build must not fail or stall on
// a cache.
package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// Logf writes a diagnostic line. It is a field rather than a package global
// so a test can capture output and the daemon can route it to its log file.
type Logf func(format string, args ...any)

// Cache serves cache operations against the three tiers.
type Cache struct {
	cfg     *config.Config
	idx     *index.Index
	blobs   *blob.Store
	rem     remote.Backend
	logf    Logf
	uploads *uploader
	metrics Metrics
}

// Metrics counts what happened, for the status command and the daemon log.
// Every field is read with atomic loads, so a snapshot may mix counts from
// slightly different instants; that is acceptable for reporting.
type Metrics struct {
	GetLocalHit  atomic.Int64
	GetRemoteHit atomic.Int64
	GetMiss      atomic.Int64
	GetRepair    atomic.Int64
	Put          atomic.Int64
	UploadOK     atomic.Int64
	UploadFail   atomic.Int64
	UploadDrop   atomic.Int64
	UploadSkip   atomic.Int64
}

// MetricsSnapshot is a value copy of Metrics for reporting.
type MetricsSnapshot struct {
	GetLocalHit  int64 `json:"get_local_hit"`
	GetRemoteHit int64 `json:"get_remote_hit"`
	GetMiss      int64 `json:"get_miss"`
	GetRepair    int64 `json:"get_repair"`
	Put          int64 `json:"put"`
	UploadOK     int64 `json:"upload_ok"`
	UploadFail   int64 `json:"upload_fail"`
	UploadDrop   int64 `json:"upload_drop"`
	UploadSkip   int64 `json:"upload_skip"`
}

// Snapshot returns a consistent-enough copy of the counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		GetLocalHit:  m.GetLocalHit.Load(),
		GetRemoteHit: m.GetRemoteHit.Load(),
		GetMiss:      m.GetMiss.Load(),
		GetRepair:    m.GetRepair.Load(),
		Put:          m.Put.Load(),
		UploadOK:     m.UploadOK.Load(),
		UploadFail:   m.UploadFail.Load(),
		UploadDrop:   m.UploadDrop.Load(),
		UploadSkip:   m.UploadSkip.Load(),
	}
}

// Params carries the already-constructed dependencies.
type Params struct {
	Config *config.Config
	Index  *index.Index
	Blobs  *blob.Store
	Remote remote.Backend
	Logf   Logf
}

// New constructs a Cache. It does not own the lifetime of Index, Blobs, or
// Remote; the caller closes those.
func New(p Params) *Cache {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rem := p.Remote
	if rem == nil {
		rem = remote.Noop{}
	}
	c := &Cache{
		cfg:   p.Config,
		idx:   p.Index,
		blobs: p.Blobs,
		rem:   rem,
		logf:  logf,
	}
	c.uploads = newUploader(p.Config.UploadConcurrency, logf, &c.metrics)
	return c
}

// Result is the outcome of a Get.
type Result struct {
	// Miss reports that nothing was found. The other fields are unset.
	Miss bool

	OutputID ids.OutputID
	Size     int64
	DiskPath string
	Time     time.Time
}

// Get resolves an action.
//
// The local index is consulted first; on a miss the shared tier is consulted
// and any hit is faulted into the local tiers so the next build resolves it
// without a network round trip.
func (c *Cache) Get(ctx context.Context, a ids.ActionID) (Result, error) {
	e, ok, err := c.idx.Get(a)
	if err != nil {
		// A broken index must not fail the build. Report a miss and let the
		// toolchain rebuild; the index is a rebuildable accelerator.
		c.logf("index get %s: %v", a, err)
		c.metrics.GetMiss.Add(1)
		return Result{Miss: true}, nil
	}
	if ok {
		path, _, err := c.blobs.Get(e.OutputID)
		if err == nil {
			if _, err := c.idx.Touch(a, time.Now().UnixNano(), c.cfg.TouchGranularity); err != nil {
				c.logf("index touch %s: %v", a, err)
			}
			c.metrics.GetLocalHit.Add(1)
			return Result{
				OutputID: e.OutputID,
				Size:     e.Size,
				DiskPath: path,
				Time:     time.Unix(0, e.CreatedAt),
			}, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			// The body may well be fine and simply unreadable right now — a
			// transient I/O error, or a permissions change. Treat it as a miss
			// so the build proceeds, but leave the index alone: deleting the
			// entry would discard a good body and its refcount on the strength
			// of a temporary failure.
			c.logf("blob get %s: %v", e.OutputID, err)
			c.metrics.GetMiss.Add(1)
			return Result{Miss: true}, nil
		}
		// The body is genuinely gone — someone deleted the blob tree by hand,
		// or a partial clean ran. Drop the dangling entry so the accounting
		// stays honest, then fall through to the remote.
		c.metrics.GetRepair.Add(1)
		if orphan, isOrphan, derr := c.idx.Delete(a); derr != nil {
			c.logf("index repair %s: %v", a, derr)
		} else if isOrphan {
			if rerr := c.blobs.Remove(orphan); rerr != nil {
				c.logf("blob repair %s: %v", orphan, rerr)
			}
		}
	}

	return c.getRemote(ctx, a)
}

// getRemote faults an entry in from the shared tier.
func (c *Cache) getRemote(ctx context.Context, a ids.ActionID) (Result, error) {
	if !c.cfg.RemoteEnabled() {
		c.metrics.GetMiss.Add(1)
		return Result{Miss: true}, nil
	}

	outputID, mtime, err := c.rem.GetAction(ctx, a)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			c.logf("remote get action %s: %v", a, err)
		}
		c.metrics.GetMiss.Add(1)
		return Result{Miss: true}, nil
	}

	body, size, err := c.rem.GetObject(ctx, outputID)
	if err != nil {
		// The action record pointed at a body that is not there. The shared
		// tier is eventually consistent with respect to its own lifecycle
		// rules, so this is a miss, not a fault.
		c.logf("remote get object %s: %v", outputID, err)
		c.metrics.GetMiss.Add(1)
		return Result{Miss: true}, nil
	}
	defer body.Close()

	path, diskBytes, err := c.blobs.Put(outputID, body, size)
	if err != nil {
		c.logf("stage remote object %s: %v", outputID, err)
		c.metrics.GetMiss.Add(1)
		return Result{Miss: true}, nil
	}

	now := time.Now().UnixNano()
	created := mtime.UnixNano()
	if mtime.IsZero() {
		created = now
	}
	if err := c.idx.Put(a, index.Entry{
		OutputID:   outputID,
		Size:       size,
		CreatedAt:  created,
		LastUsedAt: now,
	}, diskBytes); err != nil {
		// The body is staged and usable even if we failed to record it; the
		// next eviction pass will reclaim it as an orphan.
		c.logf("index put %s: %v", a, err)
	}

	c.metrics.GetRemoteHit.Add(1)
	return Result{OutputID: outputID, Size: size, DiskPath: path, Time: time.Unix(0, created)}, nil
}

// Put records an action's output.
//
// The body is written to the local store synchronously, because the toolchain
// is handed a path it will read immediately. The upload to the shared tier is
// queued and may be dropped under load.
func (c *Cache) Put(ctx context.Context, a ids.ActionID, o ids.OutputID, body io.Reader, size int64) (diskPath string, err error) {
	path, diskBytes, err := c.blobs.Put(o, body, size)
	if err != nil {
		return "", fmt.Errorf("Put: %w", err)
	}

	now := time.Now().UnixNano()
	if err := c.idx.Put(a, index.Entry{
		OutputID:   o,
		Size:       size,
		CreatedAt:  now,
		LastUsedAt: now,
	}, diskBytes); err != nil {
		// The body is on disk and the path is valid, so the toolchain can
		// proceed. Only our accounting suffered.
		c.logf("index put %s: %v", a, err)
	}
	c.metrics.Put.Add(1)

	if c.cfg.RemoteEnabled() {
		if size < c.cfg.MinUploadSize {
			c.metrics.UploadSkip.Add(1)
		} else {
			c.uploads.submit(uploadJob{
				action:  a,
				output:  o,
				path:    path,
				size:    size,
				mtime:   time.Unix(0, now),
				backend: c.rem,
			})
		}
	}
	return path, nil
}

// Metrics returns a snapshot of the counters.
func (c *Cache) Metrics() MetricsSnapshot { return c.metrics.Snapshot() }

// Evict runs one eviction pass, removing orphaned bodies as the index
// releases them.
func (c *Cache) Evict(ctx context.Context) (index.EvictResult, error) {
	res, err := c.idx.Evict(ctx, c.cfg.MaxBytes, c.cfg.TTL, time.Now().UnixNano(), func(o ids.OutputID) error {
		if rerr := c.blobs.Remove(o); rerr != nil {
			// A body we cannot delete is a leak, not a correctness problem;
			// keep pruning rather than aborting the whole pass.
			c.logf("evict remove %s: %v", o, rerr)
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("Evict: %w", err)
	}
	return res, nil
}

// Close waits for queued uploads to finish or time out. It does not close the
// index, body store, or remote backend.
func (c *Cache) Close() error {
	c.uploads.close()
	return nil
}

// uploadJob is one queued transfer to the shared tier.
type uploadJob struct {
	action  ids.ActionID
	output  ids.OutputID
	path    string
	size    int64
	mtime   time.Time
	backend remote.Backend
}

// uploader runs uploads on a bounded pool with a bounded queue.
//
// Both bounds matter. An unbounded pool would let a cold build open thousands
// of concurrent connections; an unbounded queue would let it accumulate
// unbounded memory and delay process exit. When the queue is full the job is
// dropped and counted, which is the correct trade for a best-effort tier.
type uploader struct {
	jobs    chan uploadJob
	wg      sync.WaitGroup
	logf    Logf
	metrics *Metrics
	once    sync.Once

	// mu guards jobs against a send racing close. A send on a closed channel
	// panics even inside a select with a default, because the send case is
	// always ready once the channel is closed, so default cannot make submit
	// safe after close. A panic here aborts the build, which is the one thing
	// this package promises not to do.
	mu     sync.RWMutex
	closed bool
}

// queueDepthPerWorker sizes the backlog relative to the pool.
const queueDepthPerWorker = 64

// newUploader starts the worker pool.
func newUploader(workers int, logf Logf, m *Metrics) *uploader {
	if workers < 1 {
		workers = 1
	}
	u := &uploader{
		jobs:    make(chan uploadJob, workers*queueDepthPerWorker),
		logf:    logf,
		metrics: m,
	}
	u.wg.Add(workers)
	for range workers {
		go u.worker()
	}
	return u
}

// submit queues a job, dropping it if the queue is full rather than blocking
// the build that produced it.
func (u *uploader) submit(j uploadJob) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.closed {
		// Nothing will drain it, so count it as dropped rather than panicking.
		u.metrics.UploadDrop.Add(1)
		return
	}
	select {
	case u.jobs <- j:
	default:
		u.metrics.UploadDrop.Add(1)
	}
}

// uploadTimeout bounds a single transfer so one stuck connection cannot hold
// up process exit indefinitely.
const uploadTimeout = time.Minute

// worker drains the queue.
func (u *uploader) worker() {
	defer u.wg.Done()
	for j := range u.jobs {
		u.run(j)
	}
}

// run performs one upload: body first, then the action record.
//
// The order matters. An action record pointing at an absent body produces a
// remote hit that cannot be satisfied, which costs a wasted round trip on
// every reader until the body appears. A body with no action record is merely
// unreferenced.
func (u *uploader) run(j uploadJob) {
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	f, err := os.Open(j.path)
	if err != nil {
		u.metrics.UploadFail.Add(1)
		u.logf("upload open %s: %v", j.output, err)
		return
	}
	defer f.Close()

	if err := j.backend.PutObject(ctx, j.output, f, j.size); err != nil {
		u.metrics.UploadFail.Add(1)
		u.logf("upload object %s: %v", j.output, err)
		return
	}
	if err := j.backend.PutAction(ctx, j.action, j.output, j.mtime); err != nil {
		u.metrics.UploadFail.Add(1)
		u.logf("upload action %s: %v", j.action, err)
		return
	}
	u.metrics.UploadOK.Add(1)
}

// close stops accepting work and waits for the pool to drain.
func (u *uploader) close() {
	u.once.Do(func() {
		u.mu.Lock()
		u.closed = true
		close(u.jobs)
		u.mu.Unlock()
		u.wg.Wait()
	})
}
