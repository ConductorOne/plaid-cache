// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// newStore opens a store on a fresh temp dir and registers the invariant that
// no temp file may survive the test. Every Put allocates a "<path>.tmp.<rand>"
// name, so a leak here means some exit path skipped its unlink and the cache
// would accumulate unreachable garbage that eviction never accounts for.
func newStore(t *testing.T) *Store {
	t.Helper()
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { assertNoTempFiles(t, root) })
	return s
}

// assertNoTempFiles fails if any ".tmp." name remains under root.
func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.Contains(d.Name(), ".tmp.") {
			t.Errorf("leftover temp file: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

// outputID returns the content address of body, matching how callers derive
// an OutputID in production.
func outputID(body []byte) ids.OutputID {
	return ids.OutputID(sha256.Sum256(body))
}

// mustPut puts body under its own content address and returns the path.
func mustPut(t *testing.T, s *Store, body []byte) string {
	t.Helper()
	path, disk, err := s.Put(outputID(body), bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if disk <= 0 && len(body) > 0 {
		t.Fatalf("Put diskBytes = %d, want > 0", disk)
	}
	return path
}

// TestOpenCreatesRoot pins that Open materializes a missing root rather than
// deferring the failure to the first Put.
func TestOpenCreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "cache")
	if _, err := Open(root); err != nil {
		t.Fatalf("Open: %v", err)
	}
	fi, err := os.Stat(filepath.Join(root, "output"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got, want := fi.IsDir(), true; got != want {
		t.Fatalf("output IsDir() = %v, want %v", got, want)
	}
}

// TestPathIsShardedByFirstByte pins the <root>/output/<xx>/<hex> layout, which
// the daemon reports verbatim to the go tool as DiskPath.
func TestPathIsShardedByFirstByte(t *testing.T) {
	s := newStore(t)
	var id ids.OutputID
	id[0] = 0xab
	want := filepath.Join(s.Root(), "output", "ab", id.String())
	if got := s.Path(id); got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

// TestPutGetRoundTrip pins that a published body is readable at the path both
// Put and Get report, with the exact bytes that were written.
func TestPutGetRoundTrip(t *testing.T) {
	s := newStore(t)
	body := []byte("the quick brown fox jumps over the lazy dog")
	id := outputID(body)

	putPath, putDisk, err := s.Put(id, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	getPath, getDisk, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if getPath != putPath {
		t.Fatalf("Get path = %q, want %q", getPath, putPath)
	}
	if getDisk != putDisk {
		t.Fatalf("Get diskBytes = %d, want %d", getDisk, putDisk)
	}
	got, err := os.ReadFile(getPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestPutEmptyBody pins that a zero-length body is a legitimate cache entry:
// the go tool stores empty outputs and must get a DiskPath back for them.
func TestPutEmptyBody(t *testing.T) {
	s := newStore(t)
	id := outputID(nil)
	path, _, err := s.Put(id, bytes.NewReader(nil), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got, want := fi.Size(), int64(0); got != want {
		t.Fatalf("Size = %d, want %d", got, want)
	}
}

// TestGetMissWrapsErrNotExist pins that a miss is an error wrapping
// fs.ErrNotExist, so callers can distinguish it from an I/O failure and never
// see a nil error with an empty path.
func TestGetMissWrapsErrNotExist(t *testing.T) {
	s := newStore(t)
	path, disk, err := s.Get(outputID([]byte("never stored")))
	if err == nil {
		t.Fatalf("Get error = nil, want fs.ErrNotExist")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("errors.Is(err, fs.ErrNotExist) = false, want true (err = %v)", err)
	}
	if path != "" {
		t.Fatalf("miss path = %q, want %q", path, "")
	}
	if disk != 0 {
		t.Fatalf("miss diskBytes = %d, want 0", disk)
	}
}

// TestPutConcurrentSameID pins first-writer-wins: many goroutines publishing
// one content address all succeed, agree on the path, and leave exactly one
// correct file. Run under -race.
func TestPutConcurrentSameID(t *testing.T) {
	s := newStore(t)
	body := bytes.Repeat([]byte("concurrent"), 4096)
	id := outputID(body)

	const writers = 64
	paths := make([]string, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			paths[i], _, errs[i] = s.Put(id, bytes.NewReader(body), int64(len(body)))
		}()
	}
	close(start)
	wg.Wait()

	for i := range writers {
		if errs[i] != nil {
			t.Fatalf("writer %d: Put: %v", i, errs[i])
		}
		if paths[i] != s.Path(id) {
			t.Fatalf("writer %d: path = %q, want %q", i, paths[i], s.Path(id))
		}
	}
	got, err := os.ReadFile(s.Path(id))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("published body differs from input (len %d, want %d)", len(got), len(body))
	}
	entries, err := os.ReadDir(filepath.Dir(s.Path(id)))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("shard dir has %d entries, want %d", got, want)
	}
}

// TestPutConcurrentDifferentIDs pins that unrelated writers, including ones
// landing in the same shard directory, do not interfere. Run under -race.
func TestPutConcurrentDifferentIDs(t *testing.T) {
	s := newStore(t)

	const writers = 64
	bodies := make([][]byte, writers)
	for i := range writers {
		bodies[i] = []byte(fmt.Sprintf("body-%d-%s", i, strings.Repeat("x", i*13)))
	}

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, errs[i] = s.Put(outputID(bodies[i]), bytes.NewReader(bodies[i]), int64(len(bodies[i])))
		}()
	}
	wg.Wait()

	for i := range writers {
		if errs[i] != nil {
			t.Fatalf("writer %d: Put: %v", i, errs[i])
		}
		path, _, err := s.Get(outputID(bodies[i]))
		if err != nil {
			t.Fatalf("writer %d: Get: %v", i, err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("writer %d: ReadFile: %v", i, err)
		}
		if !bytes.Equal(got, bodies[i]) {
			t.Fatalf("writer %d: body = %q, want %q", i, got, bodies[i])
		}
	}
}

// TestPutSizeMismatchDoesNotPublish pins that a body disagreeing with its
// declared size is rejected and leaves nothing behind — no published file
// under the content address, and no temp file. A short read must never be
// cached as the full output.
func TestPutSizeMismatchDoesNotPublish(t *testing.T) {
	tests := []struct {
		name     string
		body     []byte
		declared int64
	}{
		{"short body", []byte("only ten!!"), 100},
		{"long body", []byte("more bytes than promised"), 4},
		{"empty body nonzero size", nil, 7},
	}
	for _, tt := range tests {
		s := newStore(t)
		id := outputID([]byte(tt.name))
		_, _, err := s.Put(id, bytes.NewReader(tt.body), tt.declared)
		if err == nil {
			t.Fatalf("%s: Put error = nil, want size mismatch", tt.name)
		}
		if _, statErr := os.Stat(s.Path(id)); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("%s: body was published despite size mismatch (stat err = %v)", tt.name, statErr)
		}
		assertNoTempFiles(t, s.Root())
	}
}

// TestPutRejectsNegativeSize pins that a nonsensical declared size fails fast
// rather than being coerced into a mismatch error after writing the body.
func TestPutRejectsNegativeSize(t *testing.T) {
	s := newStore(t)
	if _, _, err := s.Put(outputID(nil), bytes.NewReader(nil), -1); err == nil {
		t.Fatalf("Put error = nil, want negative size rejection")
	}
}

// TestRemoveAbsentIsNotAnError pins that removing a body that is already gone
// succeeds: eviction races with concurrent writers and orphan cleanup, and
// "already absent" is the state the caller asked for.
func TestRemoveAbsentIsNotAnError(t *testing.T) {
	s := newStore(t)
	if err := s.Remove(outputID([]byte("never stored"))); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

// TestRemoveDeletesBody pins that Remove makes a published body a Get miss.
func TestRemoveDeletesBody(t *testing.T) {
	s := newStore(t)
	body := []byte("removable")
	id := outputID(body)
	mustPut(t, s, body)

	if err := s.Remove(id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, _, err := s.Get(id); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Get after Remove err = %v, want fs.ErrNotExist", err)
	}
	if err := s.Remove(id); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
}

// TestDiskBytesIsRealDiskUsage pins that the reported cost is a plausible
// allocation for a known-size body. It deliberately does not assert
// diskBytes >= size: on a compressing filesystem such as ZFS the allocated
// blocks are legitimately fewer than the logical bytes, and that smaller
// number is the true cost the byte budget should be spending.
func TestDiskBytesIsRealDiskUsage(t *testing.T) {
	s := newStore(t)
	const size = 64 << 10
	body := make([]byte, size)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	_, disk, err := s.Put(outputID(body), bytes.NewReader(body), size)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if disk <= 0 {
		t.Fatalf("diskBytes = %d, want > 0", disk)
	}
	// Generous ceiling: block rounding and per-file overhead cannot plausibly
	// multiply a 64 KiB incompressible body by four.
	if max := int64(4 * size); disk > max {
		t.Fatalf("diskBytes = %d, want <= %d", disk, max)
	}
	if disk%512 != 0 && disk != size {
		t.Fatalf("diskBytes = %d, want a 512-byte multiple or the logical size %d", disk, size)
	}
}

// TestDiskBytesCountsAllocationNotLength pins that many small bodies are not
// accounted at their logical length on a filesystem that rounds allocations up
// to a block. Summing fi.Size() would understate real consumption and let the
// cache overrun its ceiling; the test tolerates filesystems that pack or
// compress, where the sums may legitimately match or invert.
func TestDiskBytesCountsAllocationNotLength(t *testing.T) {
	s := newStore(t)
	const bodies = 32
	var logical, allocated int64
	for i := range bodies {
		body := []byte(fmt.Sprintf("tiny-%d", i))
		_, disk, err := s.Put(outputID(body), bytes.NewReader(body), int64(len(body)))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		logical += int64(len(body))
		allocated += disk
	}
	if allocated <= 0 {
		t.Fatalf("allocated = %d, want > 0", allocated)
	}
	t.Logf("logical=%d allocated=%d over %d small bodies", logical, allocated, bodies)
}

// TestDiskBytesNeverUndercountsLogicalSize pins the floor that makes the byte
// budget meaningful.
//
// st_blocks is not reliable at the moment we stat a freshly written file. ZFS
// defers allocation to the next transaction group, so a multi-megabyte object
// reports a single 512-byte block. Trusting that figure made a 33 MB cache
// report 96 KB, so the size ceiling bounded nothing at all. diskBytes must
// therefore never come in under the logical length.
func TestDiskBytesNeverUndercountsLogicalSize(t *testing.T) {
	s := newStore(t)

	// Large enough that a bogus one-block reading is unmistakable.
	const size = 4 << 20
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i * 7)
	}
	id := sha256.Sum256(body)

	_, got, err := s.Put(id, bytes.NewReader(body), size)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got < size {
		t.Fatalf("Put diskBytes = %d, want >= %d (logical size); st_blocks was trusted blindly", got, size)
	}

	_, got, err = s.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got < size {
		t.Fatalf("Get diskBytes = %d, want >= %d (logical size)", got, size)
	}
}

// TestAdoptHardlinksWithoutCopying pins that adopting an existing body costs no
// additional disk.
//
// This is what makes taking over another cache's stage viable: the bodies are
// already content-addressed identically, so a link publishes them under this
// store's name for free. A copy would double 15 GB of stage on a volume that
// also carries /tmp.
func TestAdoptHardlinksWithoutCopying(t *testing.T) {
	s := newStore(t)

	src := filepath.Join(t.TempDir(), "legacy-body")
	body := bytes.Repeat([]byte("adopt me"), 1024)
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	id := sha256.Sum256(body)

	path, db, linked, err := s.Adopt(id, src, int64(len(body)))
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if !linked {
		t.Fatal("Adopt copied instead of linking on one filesystem")
	}
	if db <= 0 {
		t.Fatalf("diskBytes = %d, want > 0", db)
	}

	// Same inode, so the bytes exist once.
	var a, b syscall.Stat_t
	if err := syscall.Stat(src, &a); err != nil {
		t.Fatalf("stat src: %v", err)
	}
	if err := syscall.Stat(path, &b); err != nil {
		t.Fatalf("stat adopted: %v", err)
	}
	if a.Ino != b.Ino {
		t.Fatalf("inode %d != %d; Adopt did not hardlink", a.Ino, b.Ino)
	}
	if b.Nlink < 2 {
		t.Fatalf("nlink = %d, want >= 2", b.Nlink)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading adopted body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("adopted body does not match the source")
	}
}

// TestAdoptSurvivesSourceRemoval pins that adoption is not a borrow: unlinking
// the original leaves this store's copy intact.
//
// That is the property that makes it safe to adopt a stage the other cache is
// still pruning — each side holds its own name, and the data lives until the
// last one goes.
func TestAdoptSurvivesSourceRemoval(t *testing.T) {
	s := newStore(t)
	src := filepath.Join(t.TempDir(), "doomed")
	body := []byte("outlives its origin")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	id := sha256.Sum256(body)
	if _, _, _, err := s.Adopt(id, src, int64(len(body))); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if err := os.Remove(src); err != nil {
		t.Fatalf("removing source: %v", err)
	}
	p, _, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get after the source was removed: %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("adopted body lost when the source went away: %v", err)
	}
}

// TestAdoptRejectsASizeMismatch pins that a record disagreeing with the file it
// points at is refused, rather than publishing bytes under an address that may
// not describe them.
func TestAdoptRejectsASizeMismatch(t *testing.T) {
	s := newStore(t)
	src := filepath.Join(t.TempDir(), "body")
	body := []byte("twelve bytes")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	id := sha256.Sum256(body)
	if _, _, _, err := s.Adopt(id, src, int64(len(body))+1); err == nil {
		t.Fatal("Adopt accepted a size that disagrees with the source")
	}
	if _, _, err := s.Get(id); err == nil {
		t.Fatal("a rejected adoption still published a body")
	}
}

// TestAdoptIsIdempotent pins that adopting the same body twice, as happens when
// several actions share one output, succeeds both times.
func TestAdoptIsIdempotent(t *testing.T) {
	s := newStore(t)
	src := filepath.Join(t.TempDir(), "shared")
	body := []byte("shared by many actions")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	id := sha256.Sum256(body)
	for i := range 3 {
		if _, _, _, err := s.Adopt(id, src, int64(len(body))); err != nil {
			t.Fatalf("Adopt #%d: %v", i+1, err)
		}
	}
}
