// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ids

import (
	"strings"
	"testing"
)

// TestStringIsLowercaseHex pins the encoding used in S3 keys and filesystem
// paths, which are the two places these strings become durable.
func TestStringIsLowercaseHex(t *testing.T) {
	var a ActionID
	for i := range a {
		a[i] = byte(i)
	}
	got := a.String()
	if len(got) != HexSize {
		t.Fatalf("len(String()) = %d, want %d", len(got), HexSize)
	}
	if got != strings.ToLower(got) {
		t.Fatalf("String() = %q, want lowercase", got)
	}
	if want := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}

	var o OutputID
	copy(o[:], a[:])
	if o.String() != got {
		t.Fatalf("OutputID.String() = %q, ActionID.String() = %q, want identical encoding", o.String(), got)
	}
}

// TestParseRoundTrip pins that parsing inverts String for both id types.
func TestParseRoundTrip(t *testing.T) {
	var a ActionID
	for i := range a {
		a[i] = byte(255 - i)
	}
	back, err := ParseActionID(a.String())
	if err != nil {
		t.Fatalf("ParseActionID: %v", err)
	}
	if back != a {
		t.Fatalf("round trip = %x, want %x", back, a)
	}

	var o OutputID
	copy(o[:], a[:])
	obak, err := ParseOutputID(o.String())
	if err != nil {
		t.Fatalf("ParseOutputID: %v", err)
	}
	if obak != o {
		t.Fatalf("round trip = %x, want %x", obak, o)
	}
}

// TestParseRejectsShortInput pins the reason parseHex checks the length itself
// rather than leaning on hex.Decode.
//
// hex.Decode fills only as many bytes as it is given and leaves the tail zeroed,
// so a truncated id would silently become a different, valid-looking content
// address — one that names the wrong cache entry rather than failing.
func TestParseRejectsShortInput(t *testing.T) {
	full := strings.Repeat("ab", Size)
	for _, s := range []string{
		"",
		full[:len(full)-2],
		full[:len(full)-1],
		full + "ab",
		full + "a",
	} {
		if got, err := ParseActionID(s); err == nil {
			t.Fatalf("ParseActionID(%d chars) = %x, want error", len(s), got)
		}
		if got, err := ParseOutputID(s); err == nil {
			t.Fatalf("ParseOutputID(%d chars) = %x, want error", len(s), got)
		}
	}
}

// TestParseRejectsNonHex pins that non-hex characters are an error rather than
// being decoded to something arbitrary.
func TestParseRejectsNonHex(t *testing.T) {
	for _, s := range []string{
		strings.Repeat("zz", Size),
		strings.Repeat("ab", Size-1) + "g!",
		strings.Repeat("ab", Size-1) + "  ",
	} {
		if got, err := ParseActionID(s); err == nil {
			t.Fatalf("ParseActionID(%q) = %x, want error", s, got)
		}
	}
}

// TestParseRejectsUppercase documents current behavior rather than asserting a
// requirement: hex.Decode accepts uppercase, so parsing is lenient even though
// String only ever emits lowercase. A caller must not rely on the string form
// being canonical after a round trip through user input.
func TestParseRejectsUppercase(t *testing.T) {
	upper := strings.ToUpper(strings.Repeat("ab", Size))
	got, err := ParseActionID(upper)
	if err != nil {
		t.Fatalf("ParseActionID(uppercase): %v", err)
	}
	if got.String() != strings.ToLower(upper) {
		t.Fatalf("String() = %q, want the lowercase form %q", got.String(), strings.ToLower(upper))
	}
}

// TestFromBytesRequiresExactLength pins the wire boundary: the GOCACHEPROG
// protocol delivers raw byte slices, and a wrong-length one must be refused
// rather than zero-padded into a different content address.
func TestFromBytesRequiresExactLength(t *testing.T) {
	good := make([]byte, Size)
	for i := range good {
		good[i] = byte(i)
	}
	a, err := ActionIDFromBytes(good)
	if err != nil {
		t.Fatalf("ActionIDFromBytes: %v", err)
	}
	if a[0] != 0 || a[Size-1] != byte(Size-1) {
		t.Fatalf("ActionIDFromBytes lost data: %x", a)
	}
	o, err := OutputIDFromBytes(good)
	if err != nil {
		t.Fatalf("OutputIDFromBytes: %v", err)
	}
	if o[Size-1] != byte(Size-1) {
		t.Fatalf("OutputIDFromBytes lost data: %x", o)
	}

	for _, n := range []int{0, 1, Size - 1, Size + 1, 2 * Size} {
		b := make([]byte, n)
		if _, err := ActionIDFromBytes(b); err == nil {
			t.Fatalf("ActionIDFromBytes(%d bytes) succeeded, want error", n)
		}
		if _, err := OutputIDFromBytes(b); err == nil {
			t.Fatalf("OutputIDFromBytes(%d bytes) succeeded, want error", n)
		}
	}
	if _, err := ActionIDFromBytes(nil); err == nil {
		t.Fatal("ActionIDFromBytes(nil) succeeded, want error")
	}
}

// TestFromBytesCopies pins that the id does not alias the caller's slice. The
// wire decoder reuses buffers, so aliasing would let a later request mutate an
// id already handed to the index.
func TestFromBytesCopies(t *testing.T) {
	b := make([]byte, Size)
	for i := range b {
		b[i] = 0xAA
	}
	a, err := ActionIDFromBytes(b)
	if err != nil {
		t.Fatalf("ActionIDFromBytes: %v", err)
	}
	before := a.String()
	for i := range b {
		b[i] = 0xBB
	}
	if a.String() != before {
		t.Fatalf("id changed when the source slice was mutated: %q -> %q", before, a.String())
	}
}

// TestSizeConstants pins that the constants agree with the array types, since
// callers size buffers from them.
func TestSizeConstants(t *testing.T) {
	if Size != 32 {
		t.Fatalf("Size = %d, want 32 (raw sha256)", Size)
	}
	if HexSize != 2*Size {
		t.Fatalf("HexSize = %d, want %d", HexSize, 2*Size)
	}
	var a ActionID
	var o OutputID
	if len(a) != Size || len(o) != Size {
		t.Fatalf("array lengths %d/%d disagree with Size %d", len(a), len(o), Size)
	}
}
