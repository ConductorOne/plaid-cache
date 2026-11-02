// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"os"
)

// ErrUploadFinished reports a write against an upload that has already been
// committed or abandoned.
var ErrUploadFinished = errors.New("upload is already finished")

// Upload is one body being received, in as many pieces as the caller has.
//
// It exists because the two transports differ in exactly one way that reaches
// the storage layer: an HTTP PUT delivers a body as a single reader, and a
// ByteStream Write delivers it as a stream that may break and be resumed on a
// later call. Splitting "receive" from "publish" lets the resumable case hold
// an Upload open across calls without a second copy of the hashing, the digest
// check and the publish-by-hardlink that [Store.Put] would otherwise own alone.
//
// An Upload is not safe for concurrent use. It is one client's half-written
// body; the caller that holds it is the only one entitled to write to it.
type Upload struct {
	store  *Store
	kind   Kind
	digest Digest
	file   *os.File
	sum    hash.Hash
	off    int64
	done   bool

	// zfile, when set, receives the compressed bytes a client sent, so the
	// body can be published in the encoding it arrived in rather than encoded
	// again on every compressed read. It is only usable while zvalid: a
	// stream that broke part-way leaves a truncated frame in it, and the
	// resumed stream starts a new one, so the two no longer decode to the body.
	zfile  *os.File
	zoff   int64
	zvalid bool
}

// zstdMinSaving is the least a compressed encoding must save to be kept
// instead of the plain body: a whole filesystem block and a tenth of the body.
// Below that, the plain body costs the same disk and no decode for a plain
// reader.
const (
	zstdBlock         = 4096
	zstdMinSavingFrac = 10
)

// worthCompressed reports whether a compressed encoding of zlen bytes is worth
// keeping in place of a plain body of plen bytes.
func worthCompressed(zlen, plen int64) bool {
	blocks := func(n int64) int64 { return (n + zstdBlock - 1) / zstdBlock }
	return blocks(zlen) < blocks(plen) && zlen*zstdMinSavingFrac < plen*(zstdMinSavingFrac-1)
}

// TeeZstd starts keeping the compressed bytes of this upload alongside the
// plain ones. It is only meaningful for a CAS upload into a cache that can
// hold compressed bodies, and is a no-op otherwise.
func (u *Upload) TeeZstd() error {
	if u.kind != KindCAS || u.zfile != nil {
		return nil
	}
	if _, can := u.store.cache.(zstdStore); !can {
		return nil
	}
	f, err := u.store.blobs.Stage()
	if err != nil {
		return fmt.Errorf("TeeZstd: %w", err)
	}
	u.zfile, u.zvalid = f, true
	return nil
}

// WriteZstd records compressed bytes the client sent. A failure only makes the
// compressed encoding unusable; the plain body is unaffected.
func (u *Upload) WriteZstd(p []byte) {
	if !u.zvalid {
		return
	}
	n, err := u.zfile.Write(p)
	u.zoff += int64(n)
	if err != nil {
		u.zvalid = false
	}
}

// BreakZstd marks the compressed encoding unusable, because the stream it came
// from broke part-way.
func (u *Upload) BreakZstd() { u.zvalid = false }

// Begin opens an upload of one digest in one keyspace.
//
// The body lands in the blob store's staging area, which the daemon sweeps at
// startup, so an upload the process dies in the middle of costs disk until the
// next start and nothing after it.
func (s *Store) Begin(k Kind, d Digest) (*Upload, error) {
	f, err := s.blobs.Stage()
	if err != nil {
		return nil, fmt.Errorf("Begin: %w", err)
	}
	return &Upload{store: s, kind: k, digest: d, file: f, sum: sha256.New()}, nil
}

// Offset reports how many bytes have been received so far, which is what a
// resuming client must be told to continue from.
func (u *Upload) Offset() int64 { return u.off }

// Write appends to the body, hashing as it goes.
func (u *Upload) Write(p []byte) (int, error) {
	if u.done {
		return 0, ErrUploadFinished
	}
	n, err := u.file.Write(p)
	if n > 0 {
		// The hash must see exactly the bytes that reached the file, so a short
		// write leaves the two consistent and the upload unusable rather than
		// silently hashing more than it stored.
		_, _ = u.sum.Write(p[:n])
		u.off += int64(n)
	}
	if err != nil {
		return n, fmt.Errorf("Write: %w", err)
	}
	return n, nil
}

// Commit publishes the received body and indexes it under the digest.
//
// It returns [ErrDigestMismatch] when a verified CAS body does not hash to the
// digest naming it. Nothing is stored in that case, and the upload is finished
// either way: a caller that wants to try again begins a new one.
func (u *Upload) Commit(ctx context.Context) error {
	if u.done {
		return ErrUploadFinished
	}
	u.done = true

	if err := u.file.Close(); err != nil {
		return fmt.Errorf("Commit: %w", err)
	}

	var computed Digest
	copy(computed[:], u.sum.Sum(nil))

	// A CAS blob is named by its own content, so a mismatch is a caller sending
	// bytes that are not what it says they are. An action-cache entry is named
	// by the action rather than by its body, so its body is stored under the
	// hash just computed and there is nothing to disagree with.
	output := computed.outputID()
	if u.kind == KindCAS {
		if u.store.verify && computed != u.digest {
			u.store.logf("bazel put cas/%s: body hashes to %s", u.digest, computed)
			return fmt.Errorf("Commit: %w", ErrDigestMismatch)
		}
		output = u.digest.outputID()
	}

	if u.zvalid && worthCompressed(u.zoff, u.off) {
		zerr := u.zfile.Close()
		if zerr == nil {
			// TeeZstd only sets zfile when the cache implements zstdStore.
			zs := u.store.cache.(zstdStore)
			_, zerr = zs.PutStagedZstd(ctx, u.kind.actionID(u.digest), output, u.zfile.Name(), u.off)
		}
		if zerr == nil {
			return nil
		}
		// Fall back to the plain body, which is always valid.
		u.store.logf("bazel put cas/%s: keep compressed: %v", u.digest, zerr)
	}

	if _, err := u.store.cache.PutStaged(ctx, u.kind.actionID(u.digest), output, u.file.Name(), u.off); err != nil {
		return fmt.Errorf("Commit: %w", err)
	}
	return nil
}

// Discard releases the upload. It is safe to call after Commit, and safe to
// call twice, so a caller can defer it and forget about it.
//
// The staging name must not survive on any path. After a successful publish it
// is a second name for the published inode; after a failure it is bytes nothing
// will ever look for.
func (u *Upload) Discard() {
	u.done = true
	for _, f := range []*os.File{u.file, u.zfile} {
		if f == nil {
			continue
		}
		_ = f.Close()
		if err := os.Remove(f.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			u.store.logf("bazel unstage %s: %v", f.Name(), err)
		}
	}
}
