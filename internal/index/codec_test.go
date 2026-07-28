// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"encoding/binary"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// TestEntryCodecRoundTrip pins that an Entry survives encode/decode unchanged,
// including the full-width 64-bit fields a narrower layout would truncate.
func TestEntryCodecRoundTrip(t *testing.T) {
	want := Entry{
		OutputID:   mkOutput(0xAB),
		Size:       1<<62 - 1,
		CreatedAt:  1_700_000_000_000_000_000,
		LastUsedAt: 1_700_000_000_000_000_001,
	}
	b := encodeEntry(want)
	if len(b) != entryValLen {
		t.Fatalf("encodeEntry length = %d, want %d", len(b), entryValLen)
	}
	got, err := decodeEntry(b)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestObjRefCodecRoundTrip pins that a refcount record survives encode/decode.
func TestObjRefCodecRoundTrip(t *testing.T) {
	want := objRef{Refs: 12345, DiskBytes: 987654321}
	got, err := decodeObjRef(encodeObjRef(want))
	if err != nil {
		t.Fatalf("decodeObjRef: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestCountersCodecRoundTrip pins that the persisted counter snapshot,
// including the unexported zeroRefs field, survives encode/decode.
func TestCountersCodecRoundTrip(t *testing.T) {
	want := countersSnapshot{Actions: 7, Objects: 5, DiskBytes: 4096, ZeroRefs: 2}
	got, err := decodeCounters(encodeCounters(want))
	if err != nil {
		t.Fatalf("decodeCounters: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestDecodeRejectsUnknownSchemaVersion pins that a record written by a future
// layout is refused rather than misparsed. Reading a v2 refcount as v1 would
// yield a plausible number, and a wrong refcount corrupts the byte budget
// permanently and silently.
func TestDecodeRejectsUnknownSchemaVersion(t *testing.T) {
	entry := encodeEntry(Entry{})
	entry[0] = schemaV1 + 1
	if _, err := decodeEntry(entry); err == nil {
		t.Fatal("decodeEntry accepted an unknown schema version")
	}

	ref := encodeObjRef(objRef{Refs: 1})
	ref[0] = 0
	if _, err := decodeObjRef(ref); err == nil {
		t.Fatal("decodeObjRef accepted an unknown schema version")
	}

	ctr := encodeCounters(countersSnapshot{})
	ctr[0] = 99
	if _, err := decodeCounters(ctr); err == nil {
		t.Fatal("decodeCounters accepted an unknown schema version")
	}
}

// TestDecodeRejectsWrongLength pins that a truncated value is an error rather
// than a panic on a short slice index.
func TestDecodeRejectsWrongLength(t *testing.T) {
	if _, err := decodeEntry(encodeEntry(Entry{})[:10]); err == nil {
		t.Fatal("decodeEntry accepted a truncated value")
	}
	if _, err := decodeObjRef([]byte{schemaV1}); err == nil {
		t.Fatal("decodeObjRef accepted a truncated value")
	}
	if _, err := decodeCounters(nil); err == nil {
		t.Fatal("decodeCounters accepted a nil value")
	}
}

// TestLRUKeyOrdersChronologically pins the property the whole eviction design
// rests on: byte order over 'l' keys is time order, so an ordered Pebble scan
// is an oldest-first scan.
func TestLRUKeyOrdersChronologically(t *testing.T) {
	a := mkAction(1)
	times := []int64{0, 1, 1_000_000, 1_700_000_000_000_000_000, 1<<62 - 1}
	for i := 1; i < len(times); i++ {
		lo := lruKey(times[i-1], a)
		hi := lruKey(times[i], a)
		if string(lo) >= string(hi) {
			t.Fatalf("lruKey(%d) >= lruKey(%d); byte order is not time order", times[i-1], times[i])
		}
	}
}

// TestParseLRUKeyRoundTrip pins that the eviction scan can recover the exact
// timestamp and action ID it needs from a key alone, with no extra read.
func TestParseLRUKeyRoundTrip(t *testing.T) {
	wantTS := int64(1_700_000_000_123_456_789)
	wantAction := mkAction(0x5C)
	gotTS, gotAction, err := parseLRUKey(lruKey(wantTS, wantAction))
	if err != nil {
		t.Fatalf("parseLRUKey: %v", err)
	}
	if gotTS != wantTS {
		t.Fatalf("timestamp = %d, want %d", gotTS, wantTS)
	}
	if gotAction != wantAction {
		t.Fatalf("action = %x, want %x", gotAction, wantAction)
	}
}

// TestParseLRUKeyRejectsMalformed pins that a key of the wrong width or prefix
// is an error, so a corrupt scan fails loudly instead of yielding a garbage
// action ID that would then be deleted.
func TestParseLRUKeyRejectsMalformed(t *testing.T) {
	if _, _, err := parseLRUKey([]byte{prefixLRU, 1, 2}); err == nil {
		t.Fatal("parseLRUKey accepted a short key")
	}
	bad := lruKey(1, mkAction(1))
	bad[0] = prefixEntry
	if _, _, err := parseLRUKey(bad); err == nil {
		t.Fatal("parseLRUKey accepted a key with the wrong prefix")
	}
}

// mkAction builds a deterministic ActionID filled with n.
func mkAction(n byte) ids.ActionID {
	var a ids.ActionID
	for i := range a {
		a[i] = n
	}
	return a
}

// mkOutput builds a deterministic OutputID filled with n.
func mkOutput(n byte) ids.OutputID {
	var o ids.OutputID
	for i := range o {
		o[i] = n
	}
	return o
}

// mkActionN builds a distinct ActionID for any int, for tests that need more
// than the 256 ids mkAction can produce.
func mkActionN(n int) ids.ActionID {
	var a ids.ActionID
	binary.BigEndian.PutUint64(a[:8], uint64(n))
	return a
}

// mkOutputN builds a distinct OutputID for any int. The 0xFF marker keeps these
// from colliding with mkOutput's fill pattern.
func mkOutputN(n int) ids.OutputID {
	var o ids.OutputID
	o[0] = 0xFF
	binary.BigEndian.PutUint64(o[8:16], uint64(n))
	return o
}
