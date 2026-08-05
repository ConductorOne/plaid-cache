// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package blob is the local content-addressed body store.
//
// Bodies live at <root>/output/<xx>/<hex-output-id>, where xx is the first
// byte of the output ID in hex. The fan-out keeps any single directory from
// growing to the hundreds of thousands of entries that make readdir and
// filesystem lookups slow.
//
// Publishing is first-writer-wins via link(2) rather than rename(2). A rename
// replaces an existing name, so a concurrent reader that already resolved the
// path can observe its file swapped out underneath it. link(2) refuses to
// clobber, which under content addressing is exactly the desired outcome: the
// loser of the race wanted to write the same bytes anyway.
package blob

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// dirPerm and filePerm are the modes for created directories and bodies. The
// store is per-user under a cache directory, so group and world get read-only
// access at most.
const (
	dirPerm  fs.FileMode = 0o755
	filePerm fs.FileMode = 0o644
)

// outputDir is the subdirectory of the store root holding bodies. Keeping
// bodies under a named subdirectory leaves room for the index and socket to
// share the cache root without a name collision.
const outputDir = "output"

// stagingDir holds bodies being written by a caller that does not yet know
// their content address. It is a sibling of outputDir rather than a directory
// under it so that a staged file can never be mistaken for a published body by
// anything walking the store.
const stagingDir = "staging"

// Store is a directory of content-addressed bodies. It holds no in-memory
// state and no locks: every operation is a filesystem syscall, so multiple
// Stores over one root, in one process or many, are safe concurrently.
type Store struct {
	root string
}

// Open prepares root as a body store, creating it if absent.
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("Open: empty root")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(abs, outputDir), dirPerm); err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}
	return &Store{root: abs}, nil
}

// Root returns the directory the store was opened on.
func (s *Store) Root() string { return s.root }

// Path returns the on-disk path for an output, whether or not it exists.
func (s *Store) Path(id ids.OutputID) string {
	h := id.String()
	return filepath.Join(s.root, outputDir, h[:2], h)
}

// Put writes body to the store. It is first-writer-wins: concurrent writers of
// the same id publish equivalent bytes by construction (content addressing),
// so an existing file is treated as success. Returns the on-disk path and the
// real disk consumption of the published body.
//
// A body whose length disagrees with size is never published: a truncated
// reader would otherwise poison the cache with a short body under a name that
// promises the full content.
func (s *Store) Put(id ids.OutputID, body io.Reader, size int64) (string, int64, error) {
	if size < 0 {
		return "", 0, fmt.Errorf("Put: negative size %d", size)
	}
	path := s.Path(id)
	if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return "", 0, fmt.Errorf("Put: %w", err)
	}

	tmp, err := createTemp(path)
	if err != nil {
		return "", 0, fmt.Errorf("Put: %w", err)
	}
	// The tmp name must not survive this call on any path, including the
	// error paths and the lost-race path. After a successful link it is a
	// second name for a now-published inode; after a failure it is garbage
	// that no later Put would ever find and reuse.
	defer func() { _ = os.Remove(tmp.Name()) }()

	written, copyErr := io.Copy(tmp, body)
	closeErr := tmp.Close()
	switch {
	case copyErr != nil:
		return "", 0, fmt.Errorf("Put: %w", copyErr)
	case closeErr != nil:
		return "", 0, fmt.Errorf("Put: %w", closeErr)
	case written != size:
		return "", 0, fmt.Errorf("Put: wrote %d bytes, declared %d", written, size)
	}

	// EEXIST means another writer published this content address first. Its
	// bytes are equivalent to ours, so the link failure is a success.
	if err := os.Link(tmp.Name(), path); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", 0, fmt.Errorf("Put: %w", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		return "", 0, fmt.Errorf("Put: %w", err)
	}
	return path, diskBytes(fi), nil
}

// Get returns the path and real disk consumption of a body. A miss is an
// error wrapping fs.ErrNotExist, never a nil error with an empty path, so a
// caller that forgets to check err cannot mistake a miss for a hit.
func (s *Store) Get(id ids.OutputID) (string, int64, error) {
	path := s.Path(id)
	fi, err := os.Stat(path)
	if err != nil {
		return "", 0, fmt.Errorf("Get: %w", err)
	}
	return path, diskBytes(fi), nil
}

// Remove deletes the body. Removing an absent body is not an error: eviction
// and orphan cleanup both race with other writers, and an already-gone body
// is the state they were asking for.
func (s *Store) Remove(id ids.OutputID) error {
	if err := os.Remove(s.Path(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Remove: %w", err)
	}
	return nil
}

// Stage opens an empty file for a body whose content address is not yet known.
//
// Put addresses a body before reading it, which a caller that has to hash the
// bytes first cannot do: an entry keyed by something other than its own content
// — a Bazel action-cache record is keyed by the action it describes — only
// becomes addressable once it has been read. Such a caller streams into this
// file, hashes as it goes, and publishes with Adopt, which hardlinks. The bytes
// therefore cross the disk once rather than twice, which is what makes it
// affordable for a body of several hundred megabytes.
//
// The file lives in the store's own directory so the later link(2) stays
// intra-filesystem; link across mount points fails with EXDEV.
//
// The caller owns the file and must remove it on every path, including after a
// successful Adopt, where it is a second name for a now-published inode. A
// process killed mid-write leaves one behind, which is what CleanTemp is for.
func (s *Store) Stage() (*os.File, error) {
	dir := filepath.Join(s.root, stagingDir)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("Stage: %w", err)
	}
	f, err := createTemp(filepath.Join(dir, "body"))
	if err != nil {
		return nil, fmt.Errorf("Stage: %w", err)
	}
	return f, nil
}

// CleanTemp removes partial bodies left behind by a process that died before it
// could publish or remove them.
//
// Two kinds accumulate, and this sweeps both:
//
//   - Staged bodies, from a caller that had to read a body before it could
//     address it (see Stage). They have a directory to themselves.
//   - Put's own temporary, which shares a directory with the published body so
//     that the link(2) that publishes it stays intra-filesystem. It therefore
//     sits inside the output tree, among the bodies, which is where a sweep
//     scoped to the staging directory never looked.
//
// Both are invisible to the byte budget — nothing in the index references a body
// that was never published — so without this a repeatedly killed daemon leaks
// disk that no eviction pass will ever reclaim, and leaks it in proportion to
// the size of whatever it was writing when it died.
//
// It is safe only because exactly one process holds the index lock, and
// therefore the store, at a time, and only before that process has begun
// serving: a temporary belonging to a write that is still running is
// indistinguishable from an abandoned one.
func (s *Store) CleanTemp() (removed int, err error) {
	n, serr := sweepDir(filepath.Join(s.root, stagingDir), nil)
	removed += n
	if serr != nil {
		err = fmt.Errorf("CleanTemp: %w", serr)
	}

	// The output tree is sharded by the first byte of the id, so a temporary
	// lives one level down rather than at the root.
	outRoot := filepath.Join(s.root, outputDir)
	shards, rerr := os.ReadDir(outRoot)
	if rerr != nil {
		if errors.Is(rerr, fs.ErrNotExist) {
			return removed, err
		}
		return removed, fmt.Errorf("CleanTemp: %w", rerr)
	}
	for _, sh := range shards {
		if !sh.IsDir() {
			continue
		}
		n, serr := sweepDir(filepath.Join(outRoot, sh.Name()), isTempName)
		removed += n
		if serr != nil {
			// One undeletable file is a leak, not a reason to leave the rest.
			err = fmt.Errorf("CleanTemp: %w", serr)
		}
	}
	return removed, err
}

// sweepDir removes the files in dir that match, or every file when match is
// nil. An absent directory is nothing to clean rather than a failure, and one
// file that will not delete does not abandon the others.
func sweepDir(dir string, match func(string) bool) (removed int, err error) {
	entries, rerr := os.ReadDir(dir)
	if rerr != nil {
		if errors.Is(rerr, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, rerr
	}
	for _, e := range entries {
		if e.IsDir() || (match != nil && !match(e.Name())) {
			continue
		}
		if derr := os.Remove(filepath.Join(dir, e.Name())); derr != nil && !errors.Is(derr, fs.ErrNotExist) {
			err = derr
			continue
		}
		removed++
	}
	return removed, err
}

// tempMarker is the infix createTemp puts between a final name and its random
// suffix. A published body is a bare hex id, so the marker cannot occur in one
// and a sweep can recognise a temporary by name alone.
const tempMarker = ".tmp."

// isTempName reports whether name is one of createTemp's.
func isTempName(name string) bool { return strings.Contains(name, tempMarker) }

// tempAttempts bounds the retry loop for an O_EXCL tmp name. A collision
// needs both a random-suffix collision and a live concurrent writer, so more
// than a couple of attempts indicates a broken entropy source rather than
// contention.
const tempAttempts = 8

// createTemp opens "<path>.tmp.<8-hex-rand>" with O_EXCL. The name shares
// path's directory so the subsequent link(2) is intra-filesystem; link across
// mount points fails with EXDEV.
func createTemp(path string) (*os.File, error) {
	for range tempAttempts {
		var buf [4]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return nil, err
		}
		name := path + tempMarker + hex.EncodeToString(buf[:])
		f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("no free temp name for %s after %d attempts", path, tempAttempts)
}

// Adopt takes ownership of a body that already exists elsewhere on disk,
// publishing it under this store's content address without copying the bytes.
//
// It hardlinks, so the adopted body occupies no additional space and neither
// location can corrupt the other: each holds its own name for one inode, and
// the data survives until the last name is unlinked. That is what makes it safe
// to adopt another cache's stage while that cache is still using it.
//
// A cross-device source cannot be linked, so those fall back to a copy. The
// caller learns which happened from linked, because the distinction decides
// whether the operation was free.
//
// Like Put, this is first-writer-wins: an existing body at the same address is
// equivalent by construction, so finding one is success.
func (s *Store) Adopt(id ids.OutputID, srcPath string, size int64) (path string, nbytes int64, linked bool, err error) {
	src, err := os.Stat(srcPath)
	if err != nil {
		return "", 0, false, fmt.Errorf("Adopt: %w", err)
	}
	if src.Size() != size {
		// The source disagrees with the record that pointed at it, so one of
		// them is stale. Refuse rather than publish bytes under an address that
		// may not describe them.
		return "", 0, false, fmt.Errorf("Adopt: %s is %d bytes, want %d", srcPath, src.Size(), size)
	}

	path = s.Path(id)
	if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return "", 0, false, fmt.Errorf("Adopt: %w", err)
	}

	// EEXIST means this address is already published, by us or by an earlier
	// adoption. Content addressing makes those bytes equivalent, so it is
	// success either way.
	lerr := os.Link(srcPath, path)
	switch {
	case lerr == nil, errors.Is(lerr, fs.ErrExist):
		fi, serr := os.Stat(path)
		if serr != nil {
			return "", 0, false, fmt.Errorf("Adopt: %w", serr)
		}
		return path, diskBytes(fi), true, nil
	case !errors.Is(lerr, syscall.EXDEV):
		return "", 0, false, fmt.Errorf("Adopt: link: %w", lerr)
	}

	// Different filesystem: the bytes have to move. Reuse Put so the copy gets
	// the same atomic publish and size verification.
	f, err := os.Open(srcPath)
	if err != nil {
		return "", 0, false, fmt.Errorf("Adopt: %w", err)
	}
	defer f.Close()
	p, db, err := s.Put(id, f, size)
	if err != nil {
		return "", 0, false, fmt.Errorf("Adopt: %w", err)
	}
	return p, db, false, nil
}
