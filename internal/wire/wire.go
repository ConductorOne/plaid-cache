// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package wire implements the GOCACHEPROG protocol framing, mirroring
// cmd/go/internal/cacheprog.
//
// The protocol is one JSON object per line in each direction. Requests flow
// from the go toolchain to the cache program; responses flow back. A put
// request whose BodySize is greater than zero is followed by a second line
// holding the body as a base64-encoded JSON string.
//
// Requests are pipelined: the go tool does not wait for a response before
// sending the next request, and responses are correlated by ID rather than by
// order. A server may therefore answer out of order from a worker pool, which
// is why Encoder serializes writes internally.
//
// The package deliberately carries raw []byte identifiers rather than
// ids.ActionID/ids.OutputID, because that is the shape the toolchain's JSON
// uses. Request.Action and Request.Output convert at the boundary.
package wire

import (
	"fmt"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// Cmd is a protocol verb. The set is open-ended by design: the server
// advertises what it implements in the handshake, and the go tool only sends
// verbs it saw advertised.
type Cmd string

// The verbs plaid-cache implements. "get" and "put" are the cache operations;
// "close" asks the program to flush and exit.
const (
	CmdGet   Cmd = "get"
	CmdPut   Cmd = "put"
	CmdClose Cmd = "close"
)

// Request is one line of JSON from the go tool. For CmdPut with BodySize > 0,
// the body follows as a separate line containing a base64 JSON string.
type Request struct {
	ID       int64  `json:"ID"`
	Command  Cmd    `json:"Command"`
	ActionID []byte `json:"ActionID,omitempty"`
	OutputID []byte `json:"OutputID,omitempty"`
	BodySize int64  `json:"BodySize,omitempty"`
}

// Response is one line of JSON back to the go tool.
//
// Every get hit must set DiskPath to an existing readable file: the go tool
// reads the body from disk itself and treats a hit without DiskPath as an
// error. Get bodies are never streamed over the wire.
type Response struct {
	ID            int64      `json:"ID,omitempty"`
	Err           string     `json:"Err,omitempty"`
	KnownCommands []Cmd      `json:"KnownCommands,omitempty"`
	Miss          bool       `json:"Miss,omitempty"`
	OutputID      []byte     `json:"OutputID,omitempty"`
	Size          int64      `json:"Size,omitempty"`
	Time          *time.Time `json:"Time,omitempty"`
	DiskPath      string     `json:"DiskPath,omitempty"`
}

// KnownCommands returns the verb set advertised in the handshake. It returns a
// fresh slice per call so a caller that appends to it cannot corrupt every
// subsequent handshake.
func KnownCommands() []Cmd {
	return []Cmd{CmdGet, CmdPut, CmdClose}
}

// Action converts the raw wire ActionID into the typed identifier used by the
// index and blob layers.
func (r *Request) Action() (ids.ActionID, error) {
	a, err := ids.ActionIDFromBytes(r.ActionID)
	if err != nil {
		return ids.ActionID{}, fmt.Errorf("Request.Action: %w", err)
	}
	return a, nil
}

// Output converts the raw wire OutputID into the typed identifier used by the
// index and blob layers.
func (r *Request) Output() (ids.OutputID, error) {
	o, err := ids.OutputIDFromBytes(r.OutputID)
	if err != nil {
		return ids.OutputID{}, fmt.Errorf("Request.Output: %w", err)
	}
	return o, nil
}

// SetOutputID stores o in the response as the raw bytes the go tool expects.
// It copies rather than slicing o so a later mutation of the caller's value
// cannot reach into an already-queued response.
func (r *Response) SetOutputID(o ids.OutputID) {
	b := make([]byte, ids.Size)
	copy(b, o[:])
	r.OutputID = b
}
