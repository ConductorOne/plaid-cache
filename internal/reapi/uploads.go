// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"sync"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/cache"
)

// Limits on partially received uploads.
const (
	// uploadIdleTimeout is how long a broken write is held open for its client
	// to resume into. It is generous because the client that would resume it is
	// recovering from whatever broke the stream, and stingy enough that a
	// client which never returns does not hold a file descriptor for the life
	// of the daemon.
	uploadIdleTimeout = 10 * time.Minute

	// maxPendingUploads bounds how many broken writes are held at once. Each
	// costs an open file and a staged body, so the bound is what stops a client
	// that opens streams and abandons them from consuming the daemon rather
	// than the cache.
	maxPendingUploads = 256
)

// pending is one write that has been started and not finished.
//
// It exists so a broken stream can be resumed rather than restarted. A build
// uploading a several-hundred-megabyte output over a link that drops should
// continue from where it stopped, which is the whole point of the protocol
// carrying an offset at all, and the server cannot offer that without keeping
// the partial body somewhere.
type pending struct {
	res resource
	up  *bazel.Upload

	// sink decodes a compressed stream into up. It belongs to one stream: a
	// resumed write starts a new one, because a client that resumes a
	// compressed upload sends a new frame rather than the rest of the old one.
	sink *decodeSink

	// streamStart is the offset the current stream declared, and streamBytes
	// counts the data received on it. Their sum is the offset the next request
	// on this stream must carry — the protocol's arithmetic, which for a
	// compressed stream mixes an uncompressed start with compressed lengths.
	streamStart int64
	streamBytes int64

	// claimed marks a stream that is actively writing. Two concurrent writes to
	// one resource name would interleave into one body, so the second is
	// refused rather than allowed to corrupt the first.
	claimed bool

	idleSince time.Time
}

// committed reports how many bytes a resuming client should skip.
//
// It is the length of what has actually been decoded and written, which for an
// identity stream is everything received and for a compressed one is everything
// the decoder emitted before the break. Reporting the smaller, certain number
// costs a client a few bytes resent and never leaves a hole.
func (p *pending) committed() int64 { return p.up.Offset() }

// uploads holds the partially received writes, keyed by resource name.
//
// The key is the resource name rather than the digest because that is what the
// client resumes with, and because the name carries an upload id the client
// chose precisely so that two concurrent uploads of the same blob do not
// collide.
type uploads struct {
	mu   sync.Mutex
	byID map[string]*pending
	logf cache.Logf
}

// newUploads constructs an empty registry.
func newUploads(logf cache.Logf) *uploads {
	return &uploads{byID: make(map[string]*pending), logf: logf}
}

// claim takes exclusive use of the pending write for a resource name, if there
// is one and nothing else is writing to it.
//
// The second return value distinguishes "no such upload" from "someone else has
// it", which are different answers to the client.
func (u *uploads) claim(name string) (p *pending, busy bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	p, ok := u.byID[name]
	if !ok {
		return nil, false
	}
	if p.claimed {
		return nil, true
	}
	p.claimed = true
	return p, false
}

// release returns a claimed write to the registry, or drops it.
func (u *uploads) release(p *pending, keep bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	p.claimed = false
	p.idleSince = time.Now()
	if !keep {
		delete(u.byID, p.res.name)
	}
}

// begin registers a freshly started write, replacing and discarding whatever
// was registered under the same name, and reports whether it took the claim.
//
// Replacing is right: a client that starts a stream at offset zero for a name
// it has used before has abandoned the earlier attempt, whatever the server
// still holds for it. A name somebody is actively writing to is the exception —
// two streams into one body would interleave — so the newcomer is refused and
// its staging file released rather than the live writer's being pulled away.
func (u *uploads) begin(p *pending) bool {
	u.mu.Lock()
	old, existed := u.byID[p.res.name]
	if existed && old.claimed {
		u.mu.Unlock()
		p.discard()
		return false
	}
	p.claimed = true
	p.idleSince = time.Now()
	u.byID[p.res.name] = p
	evicted := u.makeRoomLocked()
	u.mu.Unlock()

	if existed {
		old.discard()
	}
	for _, e := range evicted {
		u.logf("bazel grpc: dropped an abandoned upload of %s to stay under %d", e.res.digest, maxPendingUploads)
		e.discard()
	}
	return true
}

// makeRoomLocked drops the longest-idle unclaimed writes until the registry is
// within its bound, and returns them for the caller to discard outside the lock.
//
// Dropping one costs its client a re-upload, which is a cache doing less work
// than it might. Refusing the new one instead would cost the build in front of
// us, which a cache must never do.
func (u *uploads) makeRoomLocked() []*pending {
	var evicted []*pending
	for len(u.byID) > maxPendingUploads {
		var oldest *pending
		for _, p := range u.byID {
			if p.claimed {
				continue
			}
			if oldest == nil || p.idleSince.Before(oldest.idleSince) {
				oldest = p
			}
		}
		if oldest == nil {
			// Everything registered is being written to right now. There is
			// nothing to reclaim that would not corrupt a live stream.
			break
		}
		delete(u.byID, oldest.res.name)
		evicted = append(evicted, oldest)
	}
	return evicted
}

// status reports what a QueryWriteStatus should say about a resource name, if
// there is a pending write for it.
func (u *uploads) status(name string) (committed int64, ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	p, ok := u.byID[name]
	if !ok {
		return 0, false
	}
	if p.claimed {
		// A live stream's offset is moving as it is read, so quoting it would
		// hand back a number that is already stale. The client asking is
		// recovering from a break, so this is a race it does not run in
		// practice; saying nothing is the honest answer to it.
		return 0, false
	}
	return p.committed(), true
}

// sweep discards writes idle past the timeout, and reports how many.
func (u *uploads) sweep(now time.Time) int {
	u.mu.Lock()
	var stale []*pending
	for name, p := range u.byID {
		if p.claimed || now.Sub(p.idleSince) < uploadIdleTimeout {
			continue
		}
		delete(u.byID, name)
		stale = append(stale, p)
	}
	u.mu.Unlock()

	for _, p := range stale {
		u.logf("bazel grpc: abandoned upload of %s timed out", p.res.digest)
		p.discard()
	}
	return len(stale)
}

// discardAll empties the registry, and reports how many writes it released.
//
// A claimed write is released too. This runs after the gRPC server has stopped
// serving, so a claim can only belong to a handler that will never return.
func (u *uploads) discardAll() int {
	u.mu.Lock()
	stale := make([]*pending, 0, len(u.byID))
	for name, p := range u.byID {
		delete(u.byID, name)
		stale = append(stale, p)
	}
	u.mu.Unlock()

	for _, p := range stale {
		p.discard()
	}
	return len(stale)
}

// discard releases everything the pending write holds.
func (p *pending) discard() {
	p.abortStream()
	p.up.Discard()
}

// abortStream tears down the current stream's decoder, if it has one, leaving
// what was already decoded in the upload.
func (p *pending) abortStream() {
	if p.sink != nil {
		p.sink.Abort()
		p.sink = nil
	}
}

// compressed reports whether the client is sending compressed bytes.
func (p *pending) compressed() bool { return p.res.compressor != repb.Compressor_IDENTITY }
