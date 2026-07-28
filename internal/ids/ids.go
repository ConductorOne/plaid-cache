// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package ids defines the two content-addressed identifiers used throughout
// plaid-cache.
//
// Both are raw 32-byte SHA-256 digests rather than hex strings. Hex encoding
// is a boundary concern: it appears on the GOCACHEPROG wire, in S3 keys, and
// in filesystem paths, but never in the index keyspace, where fixed-width
// binary keys halve the key size and sort identically.
package ids

import (
	"encoding/hex"
	"fmt"
)

// Size is the length in bytes of an ActionID or OutputID.
const Size = 32

// HexSize is the length of the hex encoding of an ActionID or OutputID.
const HexSize = 2 * Size

// ActionID identifies a build action: the inputs to a single unit of work.
type ActionID [Size]byte

// OutputID identifies the content of an action's output. Distinct actions
// frequently share one output, which is why the index refcounts outputs
// rather than deleting a body along with the first action that referenced it.
type OutputID [Size]byte

// String returns the lowercase hex encoding of a.
func (a ActionID) String() string { return hex.EncodeToString(a[:]) }

// String returns the lowercase hex encoding of o.
func (o OutputID) String() string { return hex.EncodeToString(o[:]) }

// ParseActionID decodes a lowercase hex action ID.
func ParseActionID(s string) (ActionID, error) {
	var a ActionID
	if err := parseHex(s, a[:]); err != nil {
		return ActionID{}, fmt.Errorf("ParseActionID: %w", err)
	}
	return a, nil
}

// ParseOutputID decodes a lowercase hex output ID.
func ParseOutputID(s string) (OutputID, error) {
	var o OutputID
	if err := parseHex(s, o[:]); err != nil {
		return OutputID{}, fmt.Errorf("ParseOutputID: %w", err)
	}
	return o, nil
}

// ActionIDFromBytes converts a raw byte slice, as delivered on the
// GOCACHEPROG wire, into an ActionID.
func ActionIDFromBytes(b []byte) (ActionID, error) {
	var a ActionID
	if len(b) != Size {
		return ActionID{}, fmt.Errorf("ActionIDFromBytes: got %d bytes, want %d", len(b), Size)
	}
	copy(a[:], b)
	return a, nil
}

// OutputIDFromBytes converts a raw byte slice, as delivered on the
// GOCACHEPROG wire, into an OutputID.
func OutputIDFromBytes(b []byte) (OutputID, error) {
	var o OutputID
	if len(b) != Size {
		return OutputID{}, fmt.Errorf("OutputIDFromBytes: got %d bytes, want %d", len(b), Size)
	}
	copy(o[:], b)
	return o, nil
}

// parseHex decodes s into dst, requiring an exact length match. Go's
// hex.Decode would accept a short input and silently leave the tail zeroed,
// which for a content address means two different inputs mapping to one key.
func parseHex(s string, dst []byte) error {
	if len(s) != 2*len(dst) {
		return fmt.Errorf("got %d hex chars, want %d", len(s), 2*len(dst))
	}
	if _, err := hex.Decode(dst, []byte(s)); err != nil {
		return err
	}
	return nil
}
