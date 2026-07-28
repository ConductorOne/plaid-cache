// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package adopt

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
)

// stage builds a synthetic go-cache-plugin stage under dir.
type stage struct{ dir string }

// newStage creates an empty stage.
func newStage(t *testing.T, dir string) *stage {
	t.Helper()
	for _, sub := range []string{actionSubdir, outputSubdir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatalf("creating stage: %v", err)
		}
	}
	return &stage{dir: dir}
}

// put writes one action record and its body, exactly as go-cache-plugin does:
// the action file is named for the action id and holds the output id and size.
func (s *stage) put(t *testing.T, actionSeed string, body []byte) (ids.ActionID, ids.OutputID) {
	t.Helper()
	a := ids.ActionID(sha256.Sum256([]byte(actionSeed)))
	o := ids.OutputID(sha256.Sum256(body))

	bodyPath := filepath.Join(s.dir, outputSubdir, o.String()[:2], o.String())
	if err := os.MkdirAll(filepath.Dir(bodyPath), 0o755); err != nil {
		t.Fatalf("mkdir body: %v", err)
	}
	if err := os.WriteFile(bodyPath, body, 0o644); err != nil {
		t.Fatalf("write body: %v", err)
	}
	s.putRecordRaw(t, a.String(), fmt.Sprintf("%s %d\n", o, len(body)))
	return a, o
}

// putRecordRaw writes an action file verbatim, for malformed cases.
func (s *stage) putRecordRaw(t *testing.T, name, contents string) {
	t.Helper()
	p := filepath.Join(s.dir, actionSubdir, name[:2], name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir action: %v", err)
	}
	if err := os.WriteFile(p, []byte(contents), 0o644); err != nil {
		t.Fatalf("write action: %v", err)
	}
}

// tiers opens a real index and body store in the same directory tree as dir, so
// hardlinks are possible.
func tiers(t *testing.T, dir string) (*index.Index, *blob.Store) {
	t.Helper()
	ix, err := index.Open(filepath.Join(dir, "index"))
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	bs, err := blob.Open(filepath.Join(dir, "blob"))
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}
	return ix, bs
}

// TestRunReconstructsTheMapping pins that adoption recovers the full
// action-to-output mapping, not merely the bodies.
//
// Bodies alone would be useless: the cache is queried by action id, so without
// the mapping every adopted body would be unreachable and the import would buy
// nothing.
func TestRunReconstructsTheMapping(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	body1 := []byte("first body")
	body2 := []byte("second body")
	a1, o1 := st.put(t, "action-one", body1)
	a2, o2 := st.put(t, "action-two", body2)
	// Two actions sharing one output, which is the common case.
	a3, o3 := st.put(t, "action-three", body1)

	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Records != 3 || res.Adopted != 3 {
		t.Fatalf("records=%d adopted=%d, want 3 and 3 (%s)", res.Records, res.Adopted, res)
	}
	if res.Malformed != 0 || res.MissingBody != 0 || res.SizeMismatch != 0 {
		t.Fatalf("unexpected rejections: %s", res)
	}

	for _, c := range []struct {
		a ids.ActionID
		o ids.OutputID
		n int
	}{{a1, o1, len(body1)}, {a2, o2, len(body2)}, {a3, o3, len(body1)}} {
		e, ok, err := ix.Get(c.a)
		if err != nil {
			t.Fatalf("index.Get: %v", err)
		}
		if !ok {
			t.Fatalf("action %s not adopted", c.a)
		}
		if e.OutputID != c.o {
			t.Fatalf("action %s -> %s, want %s", c.a, e.OutputID, c.o)
		}
		if e.Size != int64(c.n) {
			t.Fatalf("action %s size %d, want %d", c.a, e.Size, c.n)
		}
	}

	// Shared output stored once.
	s, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Actions != 3 || s.Objects != 2 {
		t.Fatalf("stats actions=%d objects=%d, want 3 and 2", s.Actions, s.Objects)
	}
}

// TestRunHardlinksRatherThanCopying pins the property that makes adopting a
// multi-gigabyte stage viable.
//
// Note same-st_dev is not sufficient for a link to succeed: two mounts of one
// ZFS dataset report the same device and still return EXDEV. This keeps source
// and destination in one directory tree, which is how the real migration is laid
// out.
func TestRunHardlinksRatherThanCopying(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	body := []byte("shared inode please")
	_, o := st.put(t, "act", body)

	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Linked != 1 || res.Copied != 0 {
		t.Fatalf("linked=%d copied=%d, want 1 and 0 within one filesystem (%s)", res.Linked, res.Copied, res)
	}

	var a, b syscall.Stat_t
	src := filepath.Join(st.dir, outputSubdir, o.String()[:2], o.String())
	if err := syscall.Stat(src, &a); err != nil {
		t.Fatalf("stat legacy body: %v", err)
	}
	if err := syscall.Stat(bs.Path(o), &b); err != nil {
		t.Fatalf("stat adopted body: %v", err)
	}
	if a.Ino != b.Ino {
		t.Fatalf("inodes differ (%d vs %d); the body was copied, not linked", a.Ino, b.Ino)
	}
}

// TestRunIsIdempotent pins that a second pass changes nothing, so adoption can
// be wired somewhere that may run more than once.
func TestRunIsIdempotent(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	st.put(t, "a", []byte("one"))
	st.put(t, "b", []byte("two"))

	ix, bs := tiers(t, root)
	p := Params{LegacyDir: st.dir, Index: ix, Blobs: bs}
	if _, err := Run(context.Background(), p); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	before, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	res, err := Run(context.Background(), p)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if res.Adopted != 0 {
		t.Fatalf("second pass adopted %d, want 0", res.Adopted)
	}
	if res.AlreadyPresent != 2 {
		t.Fatalf("second pass already-present %d, want 2 (%s)", res.AlreadyPresent, res)
	}
	after, err := ix.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after != before {
		t.Fatalf("second pass changed the index: %+v -> %+v", before, after)
	}
}

// TestRunSkipsRecordsWithoutBodies pins that a stage pruned under its own
// records is imported as far as it can be, rather than failing.
//
// go-cache-plugin's prune removes bodies and orphans actions, so a real stage
// routinely contains records pointing at nothing.
func TestRunSkipsRecordsWithoutBodies(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	good, _ := st.put(t, "good", []byte("kept"))
	_, gone := st.put(t, "orphan", []byte("pruned away"))
	if err := os.Remove(filepath.Join(st.dir, outputSubdir, gone.String()[:2], gone.String())); err != nil {
		t.Fatalf("removing body: %v", err)
	}

	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Adopted != 1 || res.MissingBody != 1 {
		t.Fatalf("adopted=%d missingBody=%d, want 1 and 1 (%s)", res.Adopted, res.MissingBody, res)
	}
	if _, ok, _ := ix.Get(good); !ok {
		t.Fatal("the intact record was not adopted")
	}
}

// TestRunRejectsMismatchedAndMalformed pins that questionable records are
// counted and skipped rather than trusted.
//
// A body that disagrees with its record cannot be published under that content
// address without risking serving the wrong bytes for an action.
func TestRunRejectsMismatchedAndMalformed(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))

	// Body present but the record claims a different size.
	body := []byte("ten bytes!")
	o := ids.OutputID(sha256.Sum256(body))
	bp := filepath.Join(st.dir, outputSubdir, o.String()[:2], o.String())
	if err := os.MkdirAll(filepath.Dir(bp), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(bp, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	mismatch := ids.ActionID(sha256.Sum256([]byte("mismatch")))
	st.putRecordRaw(t, mismatch.String(), fmt.Sprintf("%s 999\n", o))

	// Malformed contents and a filename that is not an action id.
	bad := ids.ActionID(sha256.Sum256([]byte("bad-contents")))
	st.putRecordRaw(t, bad.String(), "not-an-output-id\n")
	st.putRecordRaw(t, "0011notanactionid", "whatever\n")

	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Adopted != 0 {
		t.Fatalf("adopted %d questionable records, want 0 (%s)", res.Adopted, res)
	}
	if res.SizeMismatch != 1 {
		t.Fatalf("sizeMismatch=%d, want 1 (%s)", res.SizeMismatch, res)
	}
	if res.Malformed != 2 {
		t.Fatalf("malformed=%d, want 2 (%s)", res.Malformed, res)
	}
}

// TestRunDryRunWritesNothing pins that a dry run reports without mutating, so
// the scale of an import can be checked before committing to it.
func TestRunDryRunWritesNothing(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	a, o := st.put(t, "a", []byte("body"))

	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs, DryRun: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Adopted != 1 {
		t.Fatalf("dry run reported %d adoptable, want 1", res.Adopted)
	}
	if _, ok, _ := ix.Get(a); ok {
		t.Fatal("dry run wrote an index entry")
	}
	if _, _, err := bs.Get(o); err == nil {
		t.Fatal("dry run published a body")
	}
}

// TestRunCarriesTheBodyMtimeAsCreated pins that the body's time becomes
// CreatedAt, which is the honest answer for when that content was produced.
//
// Recency is deliberately taken from elsewhere; see
// TestRunTakesRecencyFromTheActionFile. An earlier version of this test asserted
// the body's time became LastUsedAt too, which encoded a bug: it made entries
// the other cache considered current look long expired.
func TestRunCarriesTheBodyMtimeAsCreated(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	a, o := st.put(t, "aged", []byte("old body"))

	old := time.Now().Add(-72 * time.Hour).Truncate(time.Second)
	bp := filepath.Join(st.dir, outputSubdir, o.String()[:2], o.String())
	if err := os.Chtimes(bp, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	ix, bs := tiers(t, root)
	if _, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	e, ok, err := ix.Get(a)
	if err != nil || !ok {
		t.Fatalf("adopted entry missing: ok=%v err=%v", ok, err)
	}
	if got := time.Unix(0, e.CreatedAt); got.Sub(old).Abs() > time.Minute {
		t.Fatalf("CreatedAt = %v, want ~%v (the body's time)", got, old)
	}
	// The action file was written just now, so recency must be recent.
	if got := time.Unix(0, e.LastUsedAt); time.Since(got) > time.Hour {
		t.Fatalf("LastUsedAt = %v, want recent; an old body must not age out a fresh entry", got)
	}
}

// TestRunOnAMissingStageIsNotAnError pins that an env that never ran the other
// cache adopts nothing quietly, since the invocation is unconditional.
func TestRunOnAMissingStageIsNotAnError(t *testing.T) {
	root := t.TempDir()
	ix, bs := tiers(t, root)
	res, err := Run(context.Background(), Params{
		LegacyDir: filepath.Join(root, "never-existed"), Index: ix, Blobs: bs,
	})
	if err != nil {
		t.Fatalf("Run on a missing stage: %v", err)
	}
	if res.Records != 0 || res.Adopted != 0 {
		t.Fatalf("adopted something from a missing stage: %s", res)
	}
}

// TestRunHonoursContextCancellation pins that a large import can be interrupted.
func TestRunHonoursContextCancellation(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	for i := range 50 {
		st.put(t, fmt.Sprintf("act-%d", i), []byte(fmt.Sprintf("body-%d", i)))
	}
	ix, bs := tiers(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, Params{LegacyDir: st.dir, Index: ix, Blobs: bs}); err == nil {
		t.Fatal("Run ignored a cancelled context")
	}
}

// TestRunRequiresTiers pins that a misconfigured call fails loudly rather than
// silently adopting nothing.
func TestRunRequiresTiers(t *testing.T) {
	if _, err := Run(context.Background(), Params{LegacyDir: t.TempDir()}); err == nil {
		t.Fatal("Run accepted a nil index and blob store")
	}
}

// TestRunTakesRecencyFromTheActionFile pins which mtime becomes LastUsedAt.
//
// go-cache-plugin stamps a body faulted in from S3 with the time that content
// was originally produced, so an entry in daily use can have a months-old body.
// Its own prune keys off the action file instead. Taking the body's time here
// made a third of a real stage look instantly expired — 4531 of 13969 bodies
// predated a 168h TTL while none of the 16836 action files did — so adoption
// would have thrown away entries the other cache considered current.
func TestRunTakesRecencyFromTheActionFile(t *testing.T) {
	root := t.TempDir()
	st := newStage(t, filepath.Join(root, "legacy"))
	a, o := st.put(t, "in-daily-use", []byte("old content, fresh entry"))

	// The body predates any sane TTL; the action file is current.
	ancient := time.Now().Add(-90 * 24 * time.Hour).Truncate(time.Second)
	bodyPath := filepath.Join(st.dir, outputSubdir, o.String()[:2], o.String())
	if err := os.Chtimes(bodyPath, ancient, ancient); err != nil {
		t.Fatalf("chtimes body: %v", err)
	}
	fresh := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	actionPath := filepath.Join(st.dir, actionSubdir, a.String()[:2], a.String())
	if err := os.Chtimes(actionPath, fresh, fresh); err != nil {
		t.Fatalf("chtimes action: %v", err)
	}

	ix, bs := tiers(t, root)
	if _, err := Run(context.Background(), Params{LegacyDir: st.dir, Index: ix, Blobs: bs}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	e, ok, err := ix.Get(a)
	if err != nil || !ok {
		t.Fatalf("entry missing: ok=%v err=%v", ok, err)
	}

	if got := time.Unix(0, e.LastUsedAt); got.Sub(fresh).Abs() > time.Minute {
		t.Fatalf("LastUsedAt = %v, want ~%v (the action file's time); an entry in use must not read as expired",
			got, fresh)
	}
	if got := time.Unix(0, e.CreatedAt); got.Sub(ancient).Abs() > time.Minute {
		t.Fatalf("CreatedAt = %v, want ~%v (the body's time)", got, ancient)
	}
}
