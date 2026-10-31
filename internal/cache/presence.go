// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// presenceTTL is how long a verified presence answer is reused.
//
// Has is asked about the same digests over and over — every remote action
// probes the toolchain and headers it shares with every other action — and
// each answer costs an index lookup and a stat of the body. On ZFS that stat
// alone is several microseconds, and FindMissingBlobs asks thousands at a time.
// Within this window a repeat answer comes from memory.
//
// The window is short because the memo cannot see a body deleted from outside
// the process, which is the case the stat exists to catch. Everything the
// process deletes itself clears the memo outright (see presence.invalidate).
// The same outside deletion could already land between a stat and the client's
// later read, so the window adds a bounded delay to an existing race rather
// than a new one.
const presenceTTL = time.Minute

// presenceShards spreads the memo over independent locks, so concurrent
// probes from many streams do not serialise on one mutex.
const presenceShards = 64

// presenceShardCap bounds each shard, and so the memo: 64 shards of 8192
// entries is about half a million digests, some 35 MB at most. A shard that
// fills first drops what has expired and, if that frees nothing, empties
// outright rather than evicting entry by entry: the memo only saves work, so
// losing it costs a few re-verifications, and the bound is what keeps a sweep
// over millions of digests from turning it into a second index held in memory.
const presenceShardCap = 1 << 13

// presence memoises recent positive Has answers.
//
// A generation counter makes invalidation O(1): an entry remembers the
// generation it was verified under, and any removal advances the counter, so
// every older entry stops matching at once without being visited.
type presence struct {
	gen    atomic.Uint64
	shards [presenceShards]presenceShard
}

// presenceShard is one independently locked slice of the memo.
type presenceShard struct {
	mu sync.Mutex
	m  map[ids.ActionID]presenceEntry
}

// presenceEntry is one remembered answer.
//
// lastUsed is the last-used time the toucher should compare against, so a hit
// from the memo makes the same touch decision a full lookup would have.
type presenceEntry struct {
	gen      uint64
	expires  int64
	lastUsed int64
}

// shard picks the shard for an action. Action IDs are hashes, so any byte of
// them is uniformly distributed.
func (p *presence) shard(a ids.ActionID) *presenceShard {
	return &p.shards[int(a[0])%presenceShards]
}

// generation returns the current generation. A caller about to verify
// presence reads it first, so a removal that races the verification leaves
// the answer it remembers already stale.
func (p *presence) generation() uint64 { return p.gen.Load() }

// lookup returns the remembered last-used time for an action verified
// present under the current generation and not yet expired.
func (p *presence) lookup(a ids.ActionID, now int64) (int64, bool) {
	gen := p.gen.Load()
	s := p.shard(a)
	s.mu.Lock()
	e, ok := s.m[a]
	s.mu.Unlock()
	if !ok || e.gen != gen || now >= e.expires {
		return 0, false
	}
	return e.lastUsed, true
}

// remember records that an action was verified present under gen.
func (p *presence) remember(a ids.ActionID, gen uint64, now, lastUsed int64) {
	s := p.shard(a)
	s.mu.Lock()
	if len(s.m) >= presenceShardCap {
		for k, e := range s.m {
			if now >= e.expires || e.gen != gen {
				delete(s.m, k)
			}
		}
	}
	if s.m == nil || len(s.m) >= presenceShardCap {
		s.m = make(map[ids.ActionID]presenceEntry)
	}
	s.m[a] = presenceEntry{gen: gen, expires: now + int64(presenceTTL), lastUsed: lastUsed}
	s.mu.Unlock()
}

// invalidate forgets every remembered answer. It is called whenever the
// process removes an index entry or a body.
func (p *presence) invalidate() { p.gen.Add(1) }
