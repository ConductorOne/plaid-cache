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
}

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
	_ = u.file.Close()
	if err := os.Remove(u.file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
		u.store.logf("bazel unstage %s: %v", u.file.Name(), err)
	}
}
