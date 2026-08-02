// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// Kind is one of the two keyspaces a Bazel cache serves.
//
// The pair is a property of Bazel's cache model rather than of any one
// transport: the HTTP protocol names them in the request path, and the gRPC
// one in the service a call is made against. They are declared here, beside the
// storage adapter, so that a second transport would reuse this vocabulary
// instead of inventing its own.
type Kind int

// The keyspaces, named for the path segment the HTTP protocol selects them
// with.
const (
	// KindAC is the action cache: an action digest to the ActionResult
	// describing what running it produced.
	KindAC Kind = iota

	// KindCAS is content-addressed storage: a digest to the bytes that hash to
	// it.
	KindCAS
)

// String returns the keyspace's name, which is also its HTTP path segment.
func (k Kind) String() string {
	if k == KindCAS {
		return "cas"
	}
	return "ac"
}

// Namespace tags separate the two keyspaces from each other, and both from the
// Go toolchain's action ids.
//
// One digest names an entry in both keyspaces at once, and routinely does:
// Bazel stores an action's Action message in the CAS under the very digest that
// keys its ActionResult in the action cache, so a build that runs two actions
// issues two pairs of same-digest writes. Indexing both under the raw digest
// would let each overwrite the other and hand a reader a body that does not
// answer its question. Hashing a per-keyspace tag together with the digest
// keeps them apart, and keeps both apart from the Go action ids in a cache
// directory that serves those too.
const (
	namespaceAC  = "plaid-cache/bazel/ac\x00"
	namespaceCAS = "plaid-cache/bazel/cas\x00"
)

// Digest is a Bazel content digest: 32 raw bytes, the same width as every
// identifier in this cache.
//
// Only the hash travels in a request, never the size that accompanies it in the
// protocol's Digest message, so the stored length is whatever was uploaded.
type Digest [ids.Size]byte

// ParseDigest decodes the hex form a request carries.
//
// The length is checked before decoding because hex.Decode accepts a short
// input and leaves the tail zeroed, which for a content address means two
// different digests mapping to one key.
func ParseDigest(s string) (Digest, error) {
	var d Digest
	if len(s) != ids.HexSize {
		return Digest{}, fmt.Errorf("ParseDigest: got %d hex chars, want %d", len(s), ids.HexSize)
	}
	if _, err := hex.Decode(d[:], []byte(s)); err != nil {
		return Digest{}, fmt.Errorf("ParseDigest: %w", err)
	}
	return d, nil
}

// String returns the lowercase hex encoding, as it appears in a request.
func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// outputID reads a digest as a content address. A CAS blob is stored under its
// own digest, which is what lets one stored body serve both a Bazel request and
// a Go build that produced the same bytes.
func (d Digest) outputID() ids.OutputID { return ids.OutputID(d) }

// actionID derives the index key for one digest in one keyspace.
func (k Kind) actionID(d Digest) ids.ActionID {
	h := sha256.New()
	if k == KindCAS {
		h.Write([]byte(namespaceCAS))
	} else {
		h.Write([]byte(namespaceAC))
	}
	h.Write(d[:])
	return ids.ActionID(h.Sum(nil))
}
