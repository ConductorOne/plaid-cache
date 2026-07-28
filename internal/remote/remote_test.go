// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// mkAction returns an ActionID whose every byte is b, so its hex form and its
// shard directory are both predictable from a single value.
func mkAction(b byte) ids.ActionID {
	var a ids.ActionID
	for i := range a {
		a[i] = b
	}
	return a
}

// mkOutput returns an OutputID whose every byte is b.
func mkOutput(b byte) ids.OutputID {
	var o ids.OutputID
	for i := range o {
		o[i] = b
	}
	return o
}

// hexOf repeats the two-hex-digit form of b to a full id.
func hexOf(b string) string { return strings.Repeat(b, ids.Size) }

// TestKeyspaceLayout pins the exact remote key layout, byte for byte. The
// layout is a cross-tool interoperability contract with go-cache-plugin, so a
// change here silently orphans every entry already in a shared bucket rather
// than failing loudly.
func TestKeyspaceLayout(t *testing.T) {
	a := mkAction(0xab)
	o := mkOutput(0x0f)

	cases := []struct {
		name    string
		prefix  string
		wantAct string
		wantObj string
	}{
		{
			name:    "empty prefix is not rooted",
			prefix:  "",
			wantAct: "action/ab/" + hexOf("ab"),
			wantObj: "output/0f/" + hexOf("0f"),
		},
		{
			name:    "prefix is prepended",
			prefix:  "cache",
			wantAct: "cache/action/ab/" + hexOf("ab"),
			wantObj: "cache/output/0f/" + hexOf("0f"),
		},
		{
			name:    "trailing slash is normalized away",
			prefix:  "cache/",
			wantAct: "cache/action/ab/" + hexOf("ab"),
			wantObj: "cache/output/0f/" + hexOf("0f"),
		},
		{
			name:    "multi segment prefix is preserved",
			prefix:  "team/go/v1",
			wantAct: "team/go/v1/action/ab/" + hexOf("ab"),
			wantObj: "team/go/v1/output/0f/" + hexOf("0f"),
		},
		{
			// path.Join keeps a leading slash, which in S3 makes a key whose
			// first path element is empty. Pinned as observed behavior, not as
			// an endorsement: a rooted prefix is a misconfiguration.
			name:    "leading slash is retained",
			prefix:  "/cache",
			wantAct: "/cache/action/ab/" + hexOf("ab"),
			wantObj: "/cache/output/0f/" + hexOf("0f"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			k := keyspace{prefix: c.prefix}
			if got := k.actionKey(a); got != c.wantAct {
				t.Fatalf("actionKey = %q, want %q", got, c.wantAct)
			}
			if got := k.objectKey(o); got != c.wantObj {
				t.Fatalf("objectKey = %q, want %q", got, c.wantObj)
			}
		})
	}
}

// TestKeyspaceShardIsFirstByte pins that the shard directory is the first byte
// of the id in hex, which is what keeps any one listing prefix small in a
// bucket holding millions of objects.
func TestKeyspaceShardIsFirstByte(t *testing.T) {
	var a ids.ActionID
	a[0] = 0x07
	a[1] = 0xff
	k := keyspace{}
	want := "action/07/07ff" + strings.Repeat("00", ids.Size-2)
	if got := k.actionKey(a); got != want {
		t.Fatalf("actionKey = %q, want %q", got, want)
	}

	var o ids.OutputID
	o[0] = 0x07
	o[1] = 0xff
	want = "output/07/07ff" + strings.Repeat("00", ids.Size-2)
	if got := k.objectKey(o); got != want {
		t.Fatalf("objectKey = %q, want %q", got, want)
	}
}

// TestActionRecordRoundTrip pins that an action record survives format/parse
// unchanged at nanosecond resolution, including timestamps before the epoch.
// The mtime crosses machines, so losing sign or precision would make a faulted
// in object look newer or older than the machine that produced it saw it.
func TestActionRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		out   ids.OutputID
		mtime time.Time
	}{
		{"typical", mkOutput(0x5a), time.Unix(0, 1_700_000_000_123_456_789)},
		{"epoch", mkOutput(0x00), time.Unix(0, 0)},
		{"sub second", mkOutput(0x01), time.Unix(0, 1)},
		{"negative sub second", mkOutput(0x02), time.Unix(0, -1)},
		{"negative", mkOutput(0xff), time.Unix(0, -1_500_000_000_123_456_789)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := formatActionRecord(c.out, c.mtime)
			gotOut, gotTime, err := parseActionRecord([]byte(s))
			if err != nil {
				t.Fatalf("parseActionRecord(%q): %v", s, err)
			}
			if gotOut != c.out {
				t.Fatalf("output id = %s, want %s", gotOut, c.out)
			}
			if gotTime.UnixNano() != c.mtime.UnixNano() {
				t.Fatalf("mtime = %d ns, want %d ns", gotTime.UnixNano(), c.mtime.UnixNano())
			}
		})
	}
}

// TestFormatActionRecordShape pins the on-the-wire shape of a record: hex id,
// one space, decimal Unix nanoseconds. go-cache-plugin parses the same bytes.
func TestFormatActionRecordShape(t *testing.T) {
	got := formatActionRecord(mkOutput(0xab), time.Unix(0, 1_700_000_000_000_000_001))
	want := hexOf("ab") + " 1700000000000000001"
	if got != want {
		t.Fatalf("formatActionRecord = %q, want %q", got, want)
	}
}

// TestParseActionRecordRejectsMalformed pins that a malformed record is an
// error rather than a zero OutputID. A zero id is a syntactically valid
// content address, so silently returning it would point the build at the wrong
// body instead of at nothing.
func TestParseActionRecordRejectsMalformed(t *testing.T) {
	valid := hexOf("ab")
	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"whitespace only", "   \n\t "},
		{"one field", valid},
		{"three fields", valid + " 1 2"},
		{"missing timestamp separator", valid + "1700000000"},
		{"non hex output id", strings.Repeat("zz", ids.Size) + " 1700000000"},
		{"short output id", strings.Repeat("ab", ids.Size-1) + " 1700000000"},
		{"long output id", strings.Repeat("ab", ids.Size+1) + " 1700000000"},
		{"odd length output id", valid[:len(valid)-1] + " 1700000000"},
		{"unparseable timestamp", valid + " not-a-number"},
		{"float timestamp", valid + " 1700000000.5"},
		{"timestamp overflows int64", valid + " 99999999999999999999"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, mtime, err := parseActionRecord([]byte(c.in))
			if err == nil {
				t.Fatalf("parseActionRecord(%q) = (%s, %v, nil), want error", c.in, o, mtime)
			}
			if o != (ids.OutputID{}) {
				t.Fatalf("parseActionRecord(%q) returned output id %s alongside an error", c.in, o)
			}
		})
	}
}

// TestParseActionRecordAcceptsExtraWhitespace pins that surrounding
// whitespace, which a trailing newline in a stored object produces, is
// tolerated. Rejecting it would turn every hand-written or line-terminated
// entry into a permanent miss.
func TestParseActionRecordAcceptsExtraWhitespace(t *testing.T) {
	in := "  " + hexOf("ab") + "\t1700000000000000001\n"
	o, mtime, err := parseActionRecord([]byte(in))
	if err != nil {
		t.Fatalf("parseActionRecord(%q): %v", in, err)
	}
	if o != mkOutput(0xab) {
		t.Fatalf("output id = %s, want %s", o, mkOutput(0xab))
	}
	if got := mtime.UnixNano(); got != 1_700_000_000_000_000_001 {
		t.Fatalf("mtime = %d ns, want 1700000000000000001 ns", got)
	}
}

// TestParseActionRecordNeverYieldsZeroTime pins that a parsed mtime is never
// the Go zero time: no int64 nanosecond count maps to January 1 of year 1. A
// reader therefore cannot use IsZero as an "mtime unknown" sentinel on anything
// that came out of the shared tier, and a producer must not pass a zero
// time.Time to PutAction, whose UnixNano is undefined.
func TestParseActionRecordNeverYieldsZeroTime(t *testing.T) {
	for _, ns := range []int64{0, 1, -1, 1 << 62, -(1 << 62)} {
		rec := formatActionRecord(mkOutput(0xab), time.Unix(0, ns))
		_, mtime, err := parseActionRecord([]byte(rec))
		if err != nil {
			t.Fatalf("parseActionRecord(%q): %v", rec, err)
		}
		if mtime.IsZero() {
			t.Fatalf("parseActionRecord(%q) mtime is the zero time", rec)
		}
	}
}

// TestErrMissIsNotExist pins that ErrMiss is discoverable through the standard
// fs.ErrNotExist sentinel. Callers treat absence as a miss and any other error
// as a fault, so the wrapping is the contract, not an implementation detail.
func TestErrMissIsNotExist(t *testing.T) {
	if !errors.Is(ErrMiss, fs.ErrNotExist) {
		t.Fatal("ErrMiss does not satisfy errors.Is(err, fs.ErrNotExist)")
	}
	if !errors.Is(ErrMiss, ErrMiss) {
		t.Fatal("ErrMiss does not satisfy errors.Is against itself")
	}
}

// TestNoopMissesEveryRead pins that the default, no-bucket backend reports a
// miss as an fs.ErrNotExist error rather than a nil value, so local-only mode
// cannot be mistaken for a bucket holding empty objects.
func TestNoopMissesEveryRead(t *testing.T) {
	var b Backend = Noop{}
	ctx := context.Background()

	o, mtime, err := b.GetAction(ctx, mkAction(0x01))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Noop.GetAction error = %v, want fs.ErrNotExist", err)
	}
	if o != (ids.OutputID{}) || !mtime.IsZero() {
		t.Fatalf("Noop.GetAction = (%s, %v), want zero values", o, mtime)
	}

	rc, size, err := b.GetObject(ctx, mkOutput(0x01))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Noop.GetObject error = %v, want fs.ErrNotExist", err)
	}
	if rc != nil || size != 0 {
		t.Fatalf("Noop.GetObject = (%v, %d), want (nil, 0)", rc, size)
	}
}

// TestNoopWritesSucceed pins that discarding a write is reported as success.
// An error would surface as a build-visible fault in the default
// configuration, where there is no bucket to write to at all.
func TestNoopWritesSucceed(t *testing.T) {
	var b Backend = Noop{}
	ctx := context.Background()

	if err := b.PutAction(ctx, mkAction(0x01), mkOutput(0x02), time.Unix(0, 1)); err != nil {
		t.Fatalf("Noop.PutAction: %v", err)
	}
	if err := b.PutObject(ctx, mkOutput(0x02), strings.NewReader("body"), 4); err != nil {
		t.Fatalf("Noop.PutObject: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Noop.Close: %v", err)
	}
}
