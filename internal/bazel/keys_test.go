// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// hex64 is a well-formed digest in a request path.
const hex64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// TestParsePathAcceptsTheTwoRoutes pins that the only paths this server answers
// are the two Bazel builds, and that the digest is decoded rather than kept as
// text.
func TestParsePathAcceptsTheTwoRoutes(t *testing.T) {
	for _, tc := range []struct {
		path string
		want Kind
	}{
		{"/ac/" + hex64, KindAC},
		{"/cas/" + hex64, KindCAS},
	} {
		k, d, ok := parsePath(tc.path)
		if !ok {
			t.Fatalf("parsePath(%q) rejected a valid path", tc.path)
		}
		if k != tc.want {
			t.Fatalf("parsePath(%q) kind = %v, want %v", tc.path, k, tc.want)
		}
		if d.String() != hex64 {
			t.Fatalf("parsePath(%q) digest = %s, want %s", tc.path, d, hex64)
		}
	}
}

// TestParsePathRejectsEverythingElse pins that a path prefix is not accepted.
// Bazel appends ac/ or cas/ to the path of the --remote_cache URI, so two
// differently-prefixed clients would otherwise share one keyspace silently.
func TestParsePathRejectsEverythingElse(t *testing.T) {
	for _, p := range []string{
		"",
		"/",
		"/ac",
		"/ac/",
		"/cas/" + hex64[:63],        // one hex character short
		"/cas/" + hex64 + "0",       // one too long
		"/cas/" + hex64[:62] + "zz", // right length, not hex
		"/prefix/cas/" + hex64,      // a URI path prefix
		"/cas/" + hex64 + "/extra",  // a trailing segment
		"/status",                   // some other endpoint
		"cas/" + hex64,              // no leading slash
		"/blobs/" + hex64,           // the REAPI spelling
		"/cas/",                     // empty digest
	} {
		if _, _, ok := parsePath(p); ok {
			t.Fatalf("parsePath(%q) accepted an invalid path", p)
		}
	}
}

// TestKeyspacesDoNotShareAnActionID pins the namespacing that keeps Bazel's two
// keyspaces apart.
//
// One digest names an entry in both at once — Bazel stores an action's Action
// message in the CAS under the digest that keys its ActionResult in the action
// cache — so deriving the index key from the raw digest would let one overwrite
// the other.
func TestKeyspacesDoNotShareAnActionID(t *testing.T) {
	var d Digest
	copy(d[:], "a digest whose bytes do not matter")

	ac, cas := KindAC.actionID(d), KindCAS.actionID(d)
	if ac == cas {
		t.Fatalf("ac and cas derived the same action id %s", ac)
	}
	if ac == ids.ActionID(d) || cas == ids.ActionID(d) {
		t.Fatalf("a derived action id is the raw digest, so it can collide with a Go build's")
	}
}

// TestActionIDIsStable pins that the derivation is a pure function of the
// keyspace and the digest. It is a cache key: a change to it silently discards
// every entry stored by an older build, here and in the shared tier.
func TestActionIDIsStable(t *testing.T) {
	var d Digest
	copy(d[:], "stability")
	if got := KindCAS.actionID(d); got != KindCAS.actionID(d) {
		t.Fatalf("actionID is not deterministic")
	}
	var other Digest
	copy(other[:], "stabilitz")
	if KindCAS.actionID(d) == KindCAS.actionID(other) {
		t.Fatalf("two digests derived the same action id")
	}
}

// TestCASBlobIsStoredUnderItsOwnDigest pins that a CAS body's output id is the
// digest itself, which is what lets one stored body serve both a Bazel request
// and a Go build that produced the same bytes.
func TestCASBlobIsStoredUnderItsOwnDigest(t *testing.T) {
	var d Digest
	copy(d[:], "content address")
	if got := d.outputID(); got != ids.OutputID(d) {
		t.Fatalf("outputID = %s, want the digest %s", got, d)
	}
}
