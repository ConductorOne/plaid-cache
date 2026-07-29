// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"encoding/binary"
	"fmt"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// Key prefixes partition the single Pebble keyspace into four logical tables.
// They are single bytes rather than strings so that every key has a fixed,
// known width, which lets the LRU scan slice an action ID out of a key without
// parsing.
const (
	prefixEntry byte = 'e' // 'e' + actionID -> Entry
	prefixLRU   byte = 'l' // 'l' + lastUsedUnixNano(8 BE) + actionID -> empty
	prefixObj   byte = 'o' // 'o' + outputID -> objRef
	prefixMeta  byte = 'm' // 'm' + name -> counters / markers
)

// Key widths, precomputed because they appear in every hot-path allocation.
const (
	entryKeyLen = 1 + ids.Size
	lruKeyLen   = 1 + 8 + ids.Size
	objKeyLen   = 1 + ids.Size
)

// Metadata key names. metaCleanShutdown is a presence marker: Close writes it
// and Open deletes it, so finding it absent at Open means the previous process
// died without running Close and the counters cannot be trusted.
var (
	metaCounters      = metaKey("counters")
	metaCleanShutdown = metaKey("clean-shutdown")

	// metaActivity holds the lifetime activity counters, and
	// activityBucketPrefix the per-hour history. The trailing separator matters:
	// it keeps the bucket keys out of a lookup of the total, which would
	// otherwise be a prefix of every one of them.
	metaActivity         = metaKey("activity")
	activityBucketPrefix = metaKey("activity/")
)

// metaKey builds a key in the 'm' table.
func metaKey(name string) []byte {
	k := make([]byte, 0, 1+len(name))
	k = append(k, prefixMeta)
	return append(k, name...)
}

// entryKey builds the 'e' key for an action.
func entryKey(a ids.ActionID) []byte {
	k := make([]byte, entryKeyLen)
	k[0] = prefixEntry
	copy(k[1:], a[:])
	return k
}

// objKey builds the 'o' key for an output.
func objKey(o ids.OutputID) []byte {
	k := make([]byte, objKeyLen)
	k[0] = prefixObj
	copy(k[1:], o[:])
	return k
}

// lruKey builds the secondary-index key that makes eviction an ordered scan.
//
// The timestamp is big-endian so that lexicographic key order — the only order
// Pebble provides — is chronological order. This holds for non-negative nanos
// only; Put rejects negative timestamps so a pre-epoch entry can never sort to
// the wrong end of the scan and become un-evictable.
func lruKey(lastUsedAt int64, a ids.ActionID) []byte {
	k := make([]byte, lruKeyLen)
	k[0] = prefixLRU
	binary.BigEndian.PutUint64(k[1:9], uint64(lastUsedAt))
	copy(k[9:], a[:])
	return k
}

// parseLRUKey recovers the timestamp and action ID encoded in an 'l' key.
func parseLRUKey(k []byte) (lastUsedAt int64, a ids.ActionID, err error) {
	if len(k) != lruKeyLen || k[0] != prefixLRU {
		return 0, ids.ActionID{}, fmt.Errorf("parseLRUKey: malformed lru key of length %d", len(k))
	}
	lastUsedAt = int64(binary.BigEndian.Uint64(k[1:9]))
	copy(a[:], k[9:])
	return lastUsedAt, a, nil
}

// outputIDFromObjKey recovers the output id an 'o' key names.
func outputIDFromObjKey(k []byte) (ids.OutputID, bool) {
	if len(k) != objKeyLen || k[0] != prefixObj {
		return ids.OutputID{}, false
	}
	var o ids.OutputID
	copy(o[:], k[1:])
	return o, true
}

// prefixRange returns the [lower, upper) bounds covering every key under p,
// for use as Pebble iterator bounds.
func prefixRange(p byte) (lower, upper []byte) {
	return []byte{p}, []byte{p + 1}
}
