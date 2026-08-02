// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package bazel stores what a Bazel build caches, over the same three tiers the
// GOCACHEPROG plugin uses, and serves it over Bazel's HTTP/1.1 remote-cache
// protocol.
//
// The package is deliberately in two halves. [Store] is the storage adapter: it
// knows Bazel's two keyspaces and how they map onto this cache, and nothing
// about how a request arrived. [Handler] is the transport: it knows request
// paths, status codes and streaming, and nothing about how anything is stored.
// Bazel's other cache protocol — the gRPC Remote Execution API — addresses the
// same two keyspaces with the same digests, so a second transport would be a
// new front end over this same [Store] rather than a second storage design.
// HTTP first is therefore a reversible bet.
//
// # Mapping onto the existing index
//
// The protocols already agree with this cache on the shape of the data:
//
//   - A CAS blob is addressed by the SHA-256 of its own content, which is
//     exactly an [ids.OutputID]. The blob is stored under that address, so a
//     body uploaded by Bazel and the same body produced by a Go build occupy
//     one file locally and one object remotely.
//
//   - An action-cache entry maps an action digest to an ActionResult message.
//     The index maps an [ids.ActionID] to one body. Storing the ActionResult
//     message *as* the body makes the two the same shape, so the entry needs no
//     new record type, no new table, and no change to refcounting or eviction:
//     the ActionResult is a body like any other, addressed by its own hash, and
//     the action entry points at it. Nothing here parses a protocol buffer.
//
// Both keyspaces are reached through [cache.Cache], so a Bazel build gets the
// shared remote tier, the byte budget, the LRU and the counters without any of
// them knowing Bazel exists.
//
// # Independent expiry of action and CAS entries
//
// An action-cache entry names CAS blobs that are stored, and evicted,
// separately from it. Nothing in either protocol ties their lifetimes together,
// so an entry can outlive the blobs it references — locally when eviction
// reaches them first, and in the shared tier when an object-lifecycle rule
// expires them, since that tier has no delete and cannot be told to expire an
// action and its outputs as a unit.
//
// Bazel treats the resulting dangling reference as a failed download rather
// than as corruption: it re-runs the action, and retries the build outright if
// it has already committed to the entry, which is what
// --experimental_remote_cache_eviction_retries bounds. Serving a miss for an
// absent blob is therefore the whole of the server's obligation. Verifying the
// reference at lookup time would mean parsing ActionResult and probing every
// blob it names, on the hot path, which costs more than the occasional re-run
// it would save.
package bazel

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/ids"
)

// ErrDigestMismatch reports a CAS upload whose body does not hash to the digest
// naming it. It is the one failure here that is the caller's fault.
var ErrDigestMismatch = errors.New("body does not match its digest")

// store is the part of [cache.Cache] the adapter uses.
//
// It is an interface so a test can drive the failure paths a working cache
// almost never takes — a full disk, an unreadable index — which are exactly the
// paths this package promises something about.
type store interface {
	Get(ctx context.Context, a ids.ActionID) (cache.Result, error)
	Has(ctx context.Context, a ids.ActionID) bool
	PutStaged(ctx context.Context, a ids.ActionID, o ids.OutputID, stagedPath string, size int64) (string, error)
}

// stager opens the staging files uploads are streamed into. It is the part of
// blob.Store the adapter uses.
type stager interface {
	Stage() (*os.File, error)
}

// Store holds what a Bazel build caches. It has no mutable state of its own:
// every call is a call into the cache, which is already safe for concurrent
// use.
type Store struct {
	cache  store
	blobs  stager
	logf   cache.Logf
	verify bool
}

// StoreParams carries the adapter's dependencies.
type StoreParams struct {
	Cache store
	Blobs stager
	Logf  cache.Logf

	// Verify makes a CAS upload whose body does not hash to the digest naming
	// it an error rather than a stored entry.
	//
	// It is worth leaving on: without it one buggy or malicious writer can
	// publish arbitrary bytes under an address that promises different ones,
	// and every later reader — including one on another machine, once the
	// shared tier has them — links them into a binary. Turning it off is for a
	// client whose digests are not SHA-256, since only the bare hash travels
	// and a server cannot tell which function produced it.
	Verify bool
}

// NewStore constructs a Store.
func NewStore(p StoreParams) *Store {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Store{cache: p.Cache, blobs: p.Blobs, logf: logf, verify: p.Verify}
}

// Open returns the stored body for a digest, along with its length.
//
// The length comes from the open file rather than from the index, so a caller
// that declares it cannot declare one the body disagrees with.
//
// Every failure is reported as a miss rather than as an error: a caller has
// nothing useful to do with the difference between "not cached" and "the index
// is unreadable", and a cache must never break a build.
func (s *Store) Open(ctx context.Context, k Kind, d Digest) (*os.File, int64, bool) {
	res, err := s.cache.Get(ctx, k.actionID(d))
	if err != nil {
		s.logf("bazel get %s/%s: %v", k, d, err)
		return nil, 0, false
	}
	if res.Miss {
		return nil, 0, false
	}
	f, err := os.Open(res.DiskPath)
	if err != nil {
		// The index says the body is here and it is not, or is unreadable. The
		// cache repairs its own accounting on the next lookup; this is a miss.
		s.logf("bazel open %s: %v", res.DiskPath, err)
		return nil, 0, false
	}
	fi, err := f.Stat()
	if err != nil {
		s.logf("bazel stat %s: %v", res.DiskPath, err)
		_ = f.Close()
		return nil, 0, false
	}
	return f, fi.Size(), true
}

// Put stores a body under a digest, streaming it and hashing it on the way
// past. It never holds the body in memory, and publishes by hardlink rather
// than by copy, so the bytes cross the disk once however large they are.
//
// A CAS body that is already stored is not stored again: the reader is drained
// and the existing entry's last use refreshed. Bazel cannot ask an HTTP cache
// which blobs it already holds — its findMissingDigests over this transport
// reports every digest as absent — so it re-uploads outputs it has uploaded
// before whenever an action re-runs, and for a body of several hundred
// megabytes writing and hashing it a second time is pure waste. Under the gRPC
// protocol the equivalent saving comes from FindMissingBlobs, which stops the
// upload a step earlier; this is the same saving one step later.
//
// The returned error is [ErrDigestMismatch] when the caller's bytes disagree
// with its digest, and otherwise a storage failure the caller is free to treat
// as harmless: nothing was stored, which costs a future miss.
func (s *Store) Put(ctx context.Context, k Kind, d Digest, body io.Reader) error {
	if k == KindCAS && s.cache.Has(ctx, k.actionID(d)) {
		if _, err := io.Copy(io.Discard, body); err != nil {
			return fmt.Errorf("Put: drain: %w", err)
		}
		return nil
	}

	staged, err := s.blobs.Stage()
	if err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	// The staging name must not survive this call on any path. After a
	// successful publish it is a second name for the published inode; after a
	// failure it is bytes nothing will ever look for.
	defer func() {
		_ = staged.Close()
		if rerr := os.Remove(staged.Name()); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			s.logf("bazel unstage %s: %v", staged.Name(), rerr)
		}
	}()

	sum := sha256.New()
	size, err := io.Copy(io.MultiWriter(staged, sum), body)
	if err != nil {
		return fmt.Errorf("Put: receive: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("Put: %w", err)
	}

	var computed Digest
	copy(computed[:], sum.Sum(nil))

	// A CAS blob is named by its own content, so a mismatch is a caller sending
	// bytes that are not what it says they are. An action-cache entry is named
	// by the action rather than by its body, so its body is stored under the
	// hash just computed and there is nothing to disagree with.
	output := computed.outputID()
	if k == KindCAS {
		if s.verify && computed != d {
			s.logf("bazel put cas/%s: body hashes to %s", d, computed)
			return fmt.Errorf("Put: %w", ErrDigestMismatch)
		}
		output = d.outputID()
	}

	if _, err := s.cache.PutStaged(ctx, k.actionID(d), output, staged.Name(), size); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	return nil
}
