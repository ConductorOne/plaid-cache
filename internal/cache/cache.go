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

	// prunedSinceCompact is the tombstone debt built up since the last
	// compaction, counted across passes rather than within one: a thousand
	// small evictions leave as many tombstones as a single large one, and a
	// per-pass threshold would never fire for the eviction ticker.
	prunedSinceCompact atomic.Int64

	// flushMu guards flushed, which records the counters as of the last
	// persistence so that a flush writes only what has happened since.
	flushMu sync.Mutex
	flushed MetricsSnapshot
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
	Compactions  atomic.Int64
}

// MetricsSnapshot is a value copy of Metrics for reporting.
//
// It is the index's Activity type rather than a copy of its fields. The two are
// the same ten counters — one set reported, the same set persisted — and a
// second declaration would be a second place to forget when a counter is added,
// with the failure showing up as a number that is always zero in whichever half
// was missed.
type MetricsSnapshot = index.Activity

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
		Compactions:  m.Compactions.Load(),
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
	c.uploads = newUploader(p.Config.UploadConcurrency, p.Config.UploadQueueDepth, p.Config.UploadBlockTimeout, logf, &c.metrics)
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
	defer func() { _ = body.Close() }()

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

// Has reports whether an action already resolves to a readable body,
// refreshing its last-used time if it does.
//
// It is a presence probe rather than a lookup, and is counted as neither a hit
// nor a miss: the caller is deciding whether to write, not trying to read, and
// folding write-side probes into the hit rate would make that number describe
// something else. The refresh is the point of doing it here rather than in the
// caller — a body being offered again is a body in active use, and eviction
// should hear about that.
//
// A missing or unreadable body is reported as absent, so a caller that reacts
// by storing it repairs the dangling entry as a side effect.
func (c *Cache) Has(ctx context.Context, a ids.ActionID) bool {
	e, ok, err := c.idx.Get(a)
	if err != nil || !ok {
		return false
	}
	if _, _, err := c.blobs.Get(e.OutputID); err != nil {
		return false
	}
	if _, err := c.idx.Touch(a, time.Now().UnixNano(), c.cfg.TouchGranularity); err != nil {
		c.logf("index touch %s: %v", a, err)
	}
	return true
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
	c.record(a, o, path, size, diskBytes)
	return path, nil
}

// PutStaged records an action whose body is already written to a staging file
// in the body store.
//
// It serves callers that cannot name a body until they have read it: an entry
// addressed by something other than its own content — a Bazel action-cache
// record is addressed by the action it describes — has no content address to
// pass to Put beforehand. Such a caller streams into blob.Store.Stage, hashes
// as it goes, and hands the file here, where it is published by hardlink rather
// than copied.
//
// The staging file remains the caller's to remove; after a successful publish
// it is a second name for the same inode.
func (c *Cache) PutStaged(ctx context.Context, a ids.ActionID, o ids.OutputID, stagedPath string, size int64) (diskPath string, err error) {
	path, diskBytes, _, err := c.blobs.Adopt(o, stagedPath, size)
	if err != nil {
		return "", fmt.Errorf("PutStaged: %w", err)
	}
	c.record(a, o, path, size, diskBytes)
	return path, nil
}

// record indexes a body that is already published and queues its upload.
//
// It is shared by the two put paths so that they cannot drift on the parts that
// are not about how the bytes reached the disk: what the index records, what
// the counters say, and which uploads the shared tier is offered.
func (c *Cache) record(a ids.ActionID, o ids.OutputID, path string, size, diskBytes int64) {
	now := time.Now().UnixNano()
	if err := c.idx.Put(a, index.Entry{
		OutputID:   o,
		Size:       size,
		CreatedAt:  now,
		LastUsedAt: now,
	}, diskBytes); err != nil {
		// The body is on disk and the path is valid, so the caller can
		// proceed. Only our accounting suffered.
		c.logf("index put %s: %v", a, err)
	}
	c.metrics.Put.Add(1)

	if !c.cfg.RemoteEnabled() {
		return
	}
	if size < c.cfg.MinUploadSize {
		c.metrics.UploadSkip.Add(1)
		return
	}
	c.uploads.submit(uploadJob{
		action:  a,
		output:  o,
		path:    path,
		size:    size,
		mtime:   time.Unix(0, now),
		backend: c.rem,
	})
}

// Metrics returns a snapshot of the counters.
func (c *Cache) Metrics() MetricsSnapshot { return c.metrics.Snapshot() }

// UploadQueue reports how many uploads are waiting and how many may wait.
//
// It is not part of Metrics because it is not the same kind of number. Those
// are counters that only rise and are persisted across processes; this is a
// level that rises and falls and means nothing once the process holding the
// queue is gone. Its use is to be watched while it is still climbing — the drop
// counter can only be read after the entries are already lost.
func (c *Cache) UploadQueue() (depth, capacity int) { return c.uploads.queue() }

// Evict runs one eviction pass, removing orphaned bodies as the index
// releases them.
func (c *Cache) Evict(ctx context.Context) (index.EvictResult, error) {
	return c.EvictWith(ctx, c.cfg.MaxBytes, c.cfg.TTL)
}

// EvictWith runs one eviction pass against explicit limits rather than the
// configured ones.
//
// It exists so a caller can prune harder than the daemon's own settings for a
// single pass. The daemon reads its configuration once at startup, so without
// this the only way to apply a tighter ceiling was to restart it — and a
// request that appeared to be ignored was easy to misread as eviction being
// broken. Zero still means "no constraint" for either limit, so a caller can
// deliberately drop one.
func (c *Cache) EvictWith(ctx context.Context, maxBytes int64, ttl time.Duration) (index.EvictResult, error) {
	// Decide on measured bytes, not on the estimate taken before the filesystem
	// had allocated anything.
	c.reconcileBeforeEvicting(ctx, maxBytes)

	res, err := c.idx.Evict(ctx, maxBytes, ttl, time.Now().UnixNano(), func(o ids.OutputID) error {
		if rerr := c.blobs.Remove(o); rerr != nil {
			// A body we cannot delete is a leak, not a correctness problem;
			// keep pruning rather than aborting the whole pass.
			c.logf("evict remove %s: %v", o, rerr)
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("EvictWith: %w", err)
	}
	c.maybeCompact(res.ActionsPruned)
	return res, nil
}

// compactAfterPruned is the default tombstone debt that triggers a compaction.
//
// Deletes in an LSM are writes: a pass that prunes the index makes it larger,
// and Pebble's own background compactions are driven by level fill, which a
// small index never reaches. Measured on a 3076-entry index, evicting 99.6% of
// it grew the index from 0.63 MiB to 0.82 MiB and 30 seconds of idle time
// reclaimed nothing, while an explicit compaction took it to 0.01 MiB.
const compactAfterPruned = 1000

// maybeCompact compacts the index once enough entries have been pruned.
//
// The threshold exists because compaction is synchronous and would otherwise
// run on every tick of a cache sitting at its ceiling, where each pass prunes a
// handful of entries. Only the caller that observes the debt crossing the
// threshold and successfully resets it compacts, so concurrent passes cannot
// both trigger one.
func (c *Cache) maybeCompact(pruned int64) {
	if pruned <= 0 {
		return
	}
	threshold := c.cfg.CompactAfterPruned
	if threshold <= 0 {
		threshold = compactAfterPruned
	}
	n := c.prunedSinceCompact.Add(pruned)
	if n < threshold || !c.prunedSinceCompact.CompareAndSwap(n, 0) {
		return
	}
	start := time.Now()
	if err := c.idx.Compact(); err != nil {
		c.logf("compact: %v", err)
		return
	}
	c.metrics.Compactions.Add(1)
	c.logf("compacted the index after %d pruned entries in %v", n, time.Since(start).Round(time.Millisecond))
}

// Close waits for queued uploads to finish or time out. It does not close the
// index, body store, or remote backend.
func (c *Cache) Close() error {
	c.uploads.close()
	// Persist before going away. The daemon exits on an idle timeout and a
	// plugin invocation lasts one build, so without this the last stretch of
	// activity — for a short build, all of it — is simply lost.
	if err := c.FlushMetrics(); err != nil {
		c.logf("flush metrics: %v", err)
	}
	return nil
}

// FlushMetrics folds everything counted since the previous flush into the
// index's persistent totals.
//
// Only the delta is written, so repeated flushes accumulate rather than
// overwrite, and several processes counting at once — a daemon and a direct-mode
// plugin that could not reach it — add up instead of clobbering each other.
//
// A failure is returned rather than retried. The counters stay unflushed and the
// next flush carries them, because the delta is measured against what was last
// persisted rather than against zero.
func (c *Cache) FlushMetrics() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	now := c.metrics.Snapshot()
	delta := now.Sub(c.flushed)
	if delta.IsZero() {
		return nil
	}
	if err := c.idx.RecordActivity(delta, time.Now()); err != nil {
		return fmt.Errorf("FlushMetrics: %w", err)
	}
	c.flushed = now
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
//
// What that trade costs is paid somewhere else and much later: the entry is
// simply not in the shared tier, so a reader on another machine takes a clean
// miss and redoes the work, with nothing at the moment of the loss to connect
// the two. Hence queue and depth below, which say how close to full the queue is
// before any of it is lost, and the drop log, which says that it happened.
type uploader struct {
	jobs    chan uploadJob
	wg      sync.WaitGroup
	logf    Logf
	metrics *Metrics
	once    sync.Once

	// blockFor is how long submit waits for room before dropping. Zero, the
	// default, never waits.
	blockFor time.Duration

	// quit is closed at the start of close, before the lock a waiting submit
	// holds is taken, so a bounded wait cannot hold up exit for its whole
	// timeout. It is separate from jobs because closing jobs is what a waiting
	// send must not observe.
	quit chan struct{}

	// droppedSinceLog counts drops not yet reported, and loggedAt is when the
	// last report went out, as Unix nanoseconds. Together they rate-limit the
	// warning: a saturation episode is thousands of drops a second, and a line
	// each would bury the log it is trying to make legible.
	droppedSinceLog atomic.Int64
	loggedAt        atomic.Int64

	// mu guards jobs against a send racing close. A send on a closed channel
	// panics even inside a select with a default, because the send case is
	// always ready once the channel is closed, so default cannot make submit
	// safe after close. A panic here aborts the build, which is the one thing
	// this package promises not to do.
	mu     sync.RWMutex
	closed bool
}

// queueDepthPerWorker sizes the backlog relative to the pool when nothing else
// says otherwise. Configured by PLAID_GOCACHE_UPLOAD_QUEUE_DEPTH.
const queueDepthPerWorker = 64

// dropLogInterval is the shortest gap between two drop warnings. Each one
// carries the count since the last, so a longer gap loses no information about
// how much was lost, only about exactly when.
const dropLogInterval = 30 * time.Second

// newUploader starts the worker pool.
//
// depth is per worker and blockFor may be zero, which is the default and means
// a full queue drops immediately rather than waiting.
func newUploader(workers, depth int, blockFor time.Duration, logf Logf, m *Metrics) *uploader {
	if workers < 1 {
		workers = 1
	}
	if depth < 1 {
		depth = queueDepthPerWorker
	}
	u := &uploader{
		jobs:     make(chan uploadJob, workers*depth),
		logf:     logf,
		metrics:  m,
		blockFor: blockFor,
		quit:     make(chan struct{}),
	}
	u.wg.Add(workers)
	for range workers {
		go u.worker()
	}
	return u
}

// queue reports how much of the backlog is in use, which is the number that
// moves before anything is lost rather than after.
//
// Both halves are needed to read it: a depth of 400 is idle on one pool and
// about to start dropping on another, and the capacity is derived from the
// worker count, so a reader has no way to work it out for themselves.
func (u *uploader) queue() (depth, capacity int) {
	return len(u.jobs), cap(u.jobs)
}

// submit queues a job, dropping it if the queue is full rather than blocking
// the build that produced it.
//
// A configured blockFor buys a bounded wait for room first. It is off by
// default: a put that waits is a build that waits, and the whole promise of this
// tier is that it cannot do that.
func (u *uploader) submit(j uploadJob) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.closed {
		// Nothing will drain it, so count it as dropped rather than panicking.
		// Counted but not logged: a submission arriving after shutdown is not
		// the queue overflowing, and saying so would send a reader looking for
		// load that was never there.
		u.metrics.UploadDrop.Add(1)
		return
	}
	select {
	case u.jobs <- j:
		return
	default:
	}
	if u.blockFor <= 0 {
		u.dropped()
		return
	}
	t := time.NewTimer(u.blockFor)
	defer t.Stop()
	select {
	case u.jobs <- j:
	case <-t.C:
		u.dropped()
	case <-u.quit:
		// close is waiting on the read lock this send holds. What is already
		// queued will still be drained; one more job is not worth delaying the
		// process's exit by the rest of a timeout an operator chose for load,
		// not for shutdown.
		u.dropped()
	}
}

// dropped counts a lost upload and says so in the log, at most once per
// dropLogInterval.
//
// The count since the previous report is the point of the line. A rate is what
// distinguishes a queue that overflowed on one burst from one that has been
// shedding the shared tier's contents for an hour, and the counter alone cannot
// tell those apart without someone already watching it.
func (u *uploader) dropped() {
	u.metrics.UploadDrop.Add(1)
	n := u.droppedSinceLog.Add(1)

	now := time.Now().UnixNano()
	last := u.loggedAt.Load()
	// The first drop reports immediately: last is zero, which is further in the
	// past than any interval. Losing the CAS means another goroutine is
	// reporting this instant, and its line will carry this drop too.
	if now-last < int64(dropLogInterval) || !u.loggedAt.CompareAndSwap(last, now) {
		return
	}
	u.droppedSinceLog.Add(-n)
	_, capacity := u.queue()
	u.logf("upload queue full: dropped %d uploads since the last report; those entries are missing from the "+
		"shared tier (the queue holds %d — raise PLAID_GOCACHE_UPLOAD_QUEUE_DEPTH or PLAID_GOCACHE_UPLOAD_CONCURRENCY)",
		n, capacity)
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
	// Read-only, so there is no buffered write for Close to fail on; the upload
	// itself is what can fail, and it is checked.
	defer func() { _ = f.Close() }()

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
		// Released before the lock is taken, since a submit part-way through a
		// bounded wait holds that lock and would otherwise keep exit waiting for
		// the remainder of its timeout.
		close(u.quit)
		u.mu.Lock()
		u.closed = true
		close(u.jobs)
		u.mu.Unlock()
		u.wg.Wait()
	})
}
