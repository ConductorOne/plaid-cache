// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"encoding/binary"
	"fmt"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// schemaV1 is the current value-encoding version. Every stored value carries it
// as a leading byte so a future layout change can be detected rather than
// misparsed: a decoder that reads a v2 record as v1 would return plausible
// garbage, and garbage in a refcount silently corrupts the byte budget.
const schemaV1 byte = 1

// Encoded value widths. The layouts are fixed, so decoding is a length check
// plus a few loads with no allocation.
const (
	entryValLen = 1 + ids.Size + 8 + 8 + 8 // version, output, size, created, lastUsed
	objValLen   = 1 + 8 + 8                // version, refs, diskBytes
	countersLen = 1 + 8 + 8 + 8 + 8        // version, actions, objects, diskBytes, zeroRefs
)

// Entry is what an action maps to.
type Entry struct {
	OutputID   ids.OutputID
	Size       int64 // logical body size, reported to the go tool
	CreatedAt  int64 // unix nanos
	LastUsedAt int64 // unix nanos
}

// objRef refcounts a body, because many actions can share one output.
//
// DiskBytes is the real disk usage of the body, which is the quantity the size
// budget is denominated in. It lives here rather than on Entry so that N
// actions sharing one output contribute those bytes exactly once.
type objRef struct {
	Refs      int64
	DiskBytes int64
}

// encodeEntry serializes e.
func encodeEntry(e Entry) []byte {
	b := make([]byte, entryValLen)
	b[0] = schemaV1
	copy(b[1:1+ids.Size], e.OutputID[:])
	binary.BigEndian.PutUint64(b[33:41], uint64(e.Size))
	binary.BigEndian.PutUint64(b[41:49], uint64(e.CreatedAt))
	binary.BigEndian.PutUint64(b[49:57], uint64(e.LastUsedAt))
	return b
}

// decodeEntry parses an 'e' value.
func decodeEntry(b []byte) (Entry, error) {
	if len(b) != entryValLen {
		return Entry{}, fmt.Errorf("decodeEntry: got %d bytes, want %d", len(b), entryValLen)
	}
	if b[0] != schemaV1 {
		return Entry{}, fmt.Errorf("decodeEntry: unknown schema version %d", b[0])
	}
	var e Entry
	copy(e.OutputID[:], b[1:1+ids.Size])
	e.Size = int64(binary.BigEndian.Uint64(b[33:41]))
	e.CreatedAt = int64(binary.BigEndian.Uint64(b[41:49]))
	e.LastUsedAt = int64(binary.BigEndian.Uint64(b[49:57]))
	return e, nil
}

// encodeObjRef serializes r.
func encodeObjRef(r objRef) []byte {
	b := make([]byte, objValLen)
	b[0] = schemaV1
	binary.BigEndian.PutUint64(b[1:9], uint64(r.Refs))
	binary.BigEndian.PutUint64(b[9:17], uint64(r.DiskBytes))
	return b
}

// decodeObjRef parses an 'o' value.
func decodeObjRef(b []byte) (objRef, error) {
	if len(b) != objValLen {
		return objRef{}, fmt.Errorf("decodeObjRef: got %d bytes, want %d", len(b), objValLen)
	}
	if b[0] != schemaV1 {
		return objRef{}, fmt.Errorf("decodeObjRef: unknown schema version %d", b[0])
	}
	return objRef{
		Refs:      int64(binary.BigEndian.Uint64(b[1:9])),
		DiskBytes: int64(binary.BigEndian.Uint64(b[9:17])),
	}, nil
}

// countersSnapshot is the persisted form of the in-memory accelerator counters.
//
// zeroRefs is not part of the public Stats: it exists so Evict can skip the
// 'o'-prefix garbage sweep in O(1) when there is nothing to sweep.
type countersSnapshot struct {
	Actions   int64
	Objects   int64
	DiskBytes int64
	ZeroRefs  int64
}

// encodeCounters serializes c.
func encodeCounters(c countersSnapshot) []byte {
	b := make([]byte, countersLen)
	b[0] = schemaV1
	binary.BigEndian.PutUint64(b[1:9], uint64(c.Actions))
	binary.BigEndian.PutUint64(b[9:17], uint64(c.Objects))
	binary.BigEndian.PutUint64(b[17:25], uint64(c.DiskBytes))
	binary.BigEndian.PutUint64(b[25:33], uint64(c.ZeroRefs))
	return b
}

// decodeCounters parses an 'm/counters' value.
func decodeCounters(b []byte) (countersSnapshot, error) {
	if len(b) != countersLen {
		return countersSnapshot{}, fmt.Errorf("decodeCounters: got %d bytes, want %d", len(b), countersLen)
	}
	if b[0] != schemaV1 {
		return countersSnapshot{}, fmt.Errorf("decodeCounters: unknown schema version %d", b[0])
	}
	return countersSnapshot{
		Actions:   int64(binary.BigEndian.Uint64(b[1:9])),
		Objects:   int64(binary.BigEndian.Uint64(b[9:17])),
		DiskBytes: int64(binary.BigEndian.Uint64(b[17:25])),
		ZeroRefs:  int64(binary.BigEndian.Uint64(b[25:33])),
	}, nil
}
