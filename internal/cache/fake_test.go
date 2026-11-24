// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// errBoom stands in for any transport failure the shared tier can produce. The
// package's contract is that the specific error never matters — every one of
// them degrades to a miss — so one sentinel covers every failure test.
var errBoom = errors.New("remote is on fire")

// testBucket is a non-empty S3Bucket, which is the only thing that switches the
// remote tier on. No bucket of this name is ever contacted.
const testBucket = "plaid-cache-test-bucket"

// fakeActionRecord is one programmed remote action record.
type fakeActionRecord struct {
	output ids.OutputID
	mtime  time.Time
}

// fakeRemote is a programmable remote.Backend that records the order of the
// calls made against it.
//
// Every field is guarded by mu because uploads run on the uploader's worker
// pool, so assertions in the test goroutine race the writes otherwise.
type fakeRemote struct {
	mu      sync.Mutex
	calls   []string
	actions map[ids.ActionID]fakeActionRecord
	objects map[ids.OutputID][]byte

	// actionWritten and objectWritten are when each key was last written, as
	// the bucket's last-modified time would say. A conditional PutObject that
	// finds the key present leaves it alone, as S3 does.
	actionWritten map[ids.ActionID]time.Time
	objectWritten map[ids.OutputID]time.Time

	// actionETag is each record's current ETag, which changes on every write.
	actionETag map[ids.ActionID]string
	etagSeq    int

	freshnessErr error

	// objectETag is each body's current ETag, which changes on every write.
	objectETag map[ids.OutputID]string

	// copyErr, when set, is what CopyObjectInPlace returns.
	copyErr error

	// onBodyWrite, when set, runs inside CopyObjectInPlace and RewriteObject
	// before they return, outside mu, so a test can land another writer's
	// record mid-refresh.
	onBodyWrite func()

	getActionErr error
	getObjectErr error
	putObjectErr error
	putActionErr error

	// blockPutObject, when non-nil, holds every PutObject until it is closed.
	// It exists so a test can pin the worker pool and fill the queue.
	blockPutObject chan struct{}
}

// newFakeRemote returns a backend that misses on every read and accepts every
// write.
func newFakeRemote() *fakeRemote {
	return &fakeRemote{
		actions:       make(map[ids.ActionID]fakeActionRecord),
		objects:       make(map[ids.OutputID][]byte),
		actionWritten: make(map[ids.ActionID]time.Time),
		objectWritten: make(map[ids.OutputID]time.Time),
		actionETag:    make(map[ids.ActionID]string),
		objectETag:    make(map[ids.OutputID]string),
	}
}

// GetAction resolves a programmed action record.
func (f *fakeRemote) GetAction(_ context.Context, a ids.ActionID) (ids.OutputID, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "GetAction")
	if f.getActionErr != nil {
		return ids.OutputID{}, time.Time{}, f.getActionErr
	}
	rec, ok := f.actions[a]
	if !ok {
		return ids.OutputID{}, time.Time{}, remote.ErrMiss
	}
	return rec.output, rec.mtime, nil
}

// PutAction records that an action produced an output.
func (f *fakeRemote) PutAction(_ context.Context, a ids.ActionID, o ids.OutputID, mtime time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "PutAction")
	if f.putActionErr != nil {
		return f.putActionErr
	}
	f.writeActionLocked(a, o, mtime)
	return nil
}

// writeActionLocked stores a record and gives it a new ETag. mu must be held.
func (f *fakeRemote) writeActionLocked(a ids.ActionID, o ids.OutputID, mtime time.Time) {
	f.actions[a] = fakeActionRecord{output: o, mtime: mtime}
	f.actionWritten[a] = time.Now()
	f.etagSeq++
	f.actionETag[a] = fmt.Sprintf("\"etag-%d\"", f.etagSeq)
}

// PutActionIf stores a record only if its ETag is still etag, or it is still
// absent when etag is empty.
func (f *fakeRemote) PutActionIf(_ context.Context, a ids.ActionID, o ids.OutputID, mtime time.Time, etag string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "PutActionIf")
	if f.putActionErr != nil {
		return false, f.putActionErr
	}
	if f.actionETag[a] != etag {
		return false, nil
	}
	f.writeActionLocked(a, o, mtime)
	return true, nil
}

// Freshness reports when a's record and o's body were last written.
func (f *fakeRemote) Freshness(_ context.Context, a ids.ActionID, o ids.OutputID) (remote.Freshness, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "Freshness")
	if f.freshnessErr != nil {
		return remote.Freshness{}, f.freshnessErr
	}
	var fr remote.Freshness
	if rec, ok := f.actions[a]; ok {
		fr.Output = rec.output
		fr.ActionWritten = f.actionWritten[a]
		fr.ActionETag = f.actionETag[a]
	}
	if body, ok := f.objects[o]; ok {
		fr.ObjectWritten = f.objectWritten[o]
		fr.ObjectETag = f.objectETag[o]
		fr.ObjectSize = int64(len(body))
	}
	return fr, nil
}

// writeObjectLocked stores a body and gives it a new ETag. mu must be held.
func (f *fakeRemote) writeObjectLocked(o ids.OutputID, body []byte) {
	f.objects[o] = body
	f.objectWritten[o] = time.Now()
	f.etagSeq++
	f.objectETag[o] = fmt.Sprintf("\"obj-%d\"", f.etagSeq)
}

// CopyObjectInPlace gives a body a new last-modified time, as S3's self-copy
// does, if it is still the object fr read.
func (f *fakeRemote) CopyObjectInPlace(_ context.Context, o ids.OutputID, fr remote.Freshness) error {
	f.mu.Lock()
	f.calls = append(f.calls, "CopyObjectInPlace")
	err := f.copyErr
	hook := f.onBodyWrite
	if err == nil {
		body, ok := f.objects[o]
		switch {
		case !ok:
			err = remote.ErrMiss
		case f.objectETag[o] == fr.ObjectETag:
			f.writeObjectLocked(o, body)
		}
	}
	f.mu.Unlock()
	if err == nil && hook != nil {
		hook()
	}
	return err
}

// RewriteObject stores a body whether or not one is already there.
func (f *fakeRemote) RewriteObject(_ context.Context, o ids.OutputID, r io.ReadSeeker, size int64) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if int64(len(body)) != size {
		return fmt.Errorf("RewriteObject: read %d bytes, declared %d", len(body), size)
	}
	f.mu.Lock()
	f.calls = append(f.calls, "RewriteObject")
	f.writeObjectLocked(o, body)
	hook := f.onBodyWrite
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

// age backdates the last-written times of a's record and o's body.
func (f *fakeRemote) age(a ids.ActionID, o ids.OutputID, written time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actionWritten[a] = written
	f.objectWritten[o] = written
}

// written returns the last-written times of a's record and o's body.
func (f *fakeRemote) written(a ids.ActionID, o ids.OutputID) (action, object time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actionWritten[a], f.objectWritten[o]
}

// GetObject opens a programmed body.
func (f *fakeRemote) GetObject(_ context.Context, o ids.OutputID) (io.ReadCloser, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "GetObject")
	if f.getObjectErr != nil {
		return nil, 0, f.getObjectErr
	}
	body, ok := f.objects[o]
	if !ok {
		return nil, 0, remote.ErrMiss
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

// PutObject stores a body, verifying that the declared size matches the bytes
// the uploader actually handed over.
func (f *fakeRemote) PutObject(ctx context.Context, o ids.OutputID, r io.ReadSeeker, size int64) error {
	f.mu.Lock()
	f.calls = append(f.calls, "PutObject")
	err, block := f.putObjectErr, f.blockPutObject
	f.mu.Unlock()

	// Blocking is deliberately done outside mu so a pinned worker does not also
	// wedge the test goroutine's assertions. The ctx arm is a safety valve: a
	// test that forgets to release must fail on its own assertions rather than
	// hang the package.
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	body, rerr := io.ReadAll(r)
	if rerr != nil {
		return rerr
	}
	if int64(len(body)) != size {
		return fmt.Errorf("PutObject: read %d bytes, declared %d", len(body), size)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[o]; !ok {
		f.writeObjectLocked(o, body)
	}
	return nil
}

// Close is a no-op; the cache never closes the backend it was handed.
func (f *fakeRemote) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "Close")
	return nil
}

// programAction makes the backend resolve a to o.
func (f *fakeRemote) programAction(a ids.ActionID, o ids.OutputID, mtime time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeActionLocked(a, o, mtime)
}

// programObject makes the backend serve body for o.
func (f *fakeRemote) programObject(o ids.OutputID, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeObjectLocked(o, append([]byte(nil), body...))
}

// callOrder returns a copy of the recorded call sequence.
func (f *fakeRemote) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// storedObject returns the body stored for o, and whether one exists.
func (f *fakeRemote) storedObject(o ids.OutputID) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[o]
	return body, ok
}

// storedAction returns the record stored for a, and whether one exists.
func (f *fakeRemote) storedAction(a ids.ActionID) (fakeActionRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.actions[a]
	return rec, ok
}

// testCache is a Cache wired to real index and blob tiers under t.TempDir() and
// a programmable remote.
type testCache struct {
	cfg   *config.Config
	idx   *index.Index
	blobs *blob.Store
	rem   *fakeRemote
	cache *Cache
	logs  *logRecorder
}

// logRecorder captures what the cache logged, for the diagnostics that exist to
// be read rather than counted. Lines still reach t.Logf, which is the other
// reason they are there.
//
// mu guards lines because the uploader logs from its worker pool and from
// whichever goroutine submitted a job it had to drop.
type logRecorder struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

// logf records one line and forwards it to the test log.
func (r *logRecorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	r.mu.Lock()
	r.lines = append(r.lines, line)
	r.mu.Unlock()
	r.t.Log(line)
}

// matching returns the recorded lines containing sub.
func (r *logRecorder) matching(sub string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, l := range r.lines {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return out
}

// option mutates the config before the tiers are opened.
type option func(*config.Config)

// withRemote enables the remote tier, which is off by default.
func withRemote() option {
	return func(c *config.Config) { c.S3Bucket = testBucket }
}

// withMinUploadSize sets the upload-skip threshold.
func withMinUploadSize(n int64) option {
	return func(c *config.Config) { c.MinUploadSize = n }
}

// withUploadQueueDepth sets the per-worker upload backlog.
func withUploadQueueDepth(n int) option {
	return func(c *config.Config) { c.UploadQueueDepth = n }
}

// withUploadBlockTimeout makes a full upload queue wait for room rather than
// drop immediately.
func withUploadBlockTimeout(d time.Duration) option {
	return func(c *config.Config) { c.UploadBlockTimeout = d }
}

// withCompactAfter sets the pruned-entry debt that triggers a compaction.
func withCompactAfter(n int64) option {
	return func(c *config.Config) { c.CompactAfterPruned = n }
}

// withMaxBytes sets the eviction size ceiling.
func withMaxBytes(n int64) option {
	return func(c *config.Config) { c.MaxBytes = n }
}

// newTestCache builds a Cache over a real Pebble index and a real body store in
// a fresh temp directory, with a fake remote.
//
// UploadConcurrency defaults to 1 so the uploader's queue depth is exactly
// queueDepthPerWorker on every machine; a NumCPU-derived value would make the
// queue-drop bound machine-dependent.
func newTestCache(t *testing.T, opts ...option) *testCache {
	t.Helper()

	cfg := &config.Config{
		Dir:               t.TempDir(),
		UploadConcurrency: 1,
	}
	for _, o := range opts {
		o(cfg)
	}

	idx, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}

	rem := newFakeRemote()
	logs := &logRecorder{t: t}
	c := New(Params{Config: cfg, Index: idx, Blobs: blobs, Remote: rem, Logf: logs.logf})
	// Registered after the index cleanup so it runs first: the uploader must
	// drain while the index is still open.
	t.Cleanup(func() { _ = c.Close() })

	return &testCache{cfg: cfg, idx: idx, blobs: blobs, rem: rem, cache: c, logs: logs}
}

// put is a terse Cache.Put for tests, returning the on-disk path.
func (tc *testCache) put(t *testing.T, a ids.ActionID, o ids.OutputID, body []byte) string {
	t.Helper()
	path, err := tc.cache.Put(t.Context(), a, o, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Put(%x): %v", a[:4], err)
	}
	return path
}

// get is a terse Cache.Get for tests. A non-nil error always fails the test:
// Get is documented never to return one.
func (tc *testCache) get(t *testing.T, a ids.ActionID) Result {
	t.Helper()
	res, err := tc.cache.Get(t.Context(), a)
	if err != nil {
		t.Fatalf("Get(%x) = error %v, want a miss or a hit and a nil error", a[:4], err)
	}
	return res
}

// wantStats asserts the index's three public counters at once.
func (tc *testCache) wantStats(t *testing.T, actions, objects int64) {
	t.Helper()
	s, err := tc.idx.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Actions != actions || s.Objects != objects {
		t.Fatalf("Stats = %+v, want {Actions:%d Objects:%d}", s, actions, objects)
	}
	if actions == 0 && objects == 0 && s.DiskBytes != 0 {
		t.Fatalf("Stats.DiskBytes = %d, want 0 once nothing is referenced", s.DiskBytes)
	}
}

// mkAction builds a deterministic ActionID filled with n.
func mkAction(n byte) ids.ActionID {
	var a ids.ActionID
	for i := range a {
		a[i] = n
	}
	return a
}

// mkOutput builds a deterministic OutputID filled with n.
func mkOutput(n byte) ids.OutputID {
	var o ids.OutputID
	for i := range o {
		o[i] = n
	}
	return o
}

// mkActionN builds a distinct ActionID for any int, for tests that need more
// ids than a single byte provides.
func mkActionN(n int) ids.ActionID {
	var a ids.ActionID
	binary.BigEndian.PutUint64(a[:8], uint64(n))
	return a
}

// mkOutputN builds a distinct OutputID for any int. The 0xFF marker keeps these
// from colliding with mkOutput's fill pattern.
func mkOutputN(n int) ids.OutputID {
	var o ids.OutputID
	o[0] = 0xFF
	binary.BigEndian.PutUint64(o[8:16], uint64(n))
	return o
}
