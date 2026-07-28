// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// Encoder writes responses back to the go tool.
//
// It is safe for concurrent use. Requests are pipelined and a server answers
// them from a worker pool, so without the mutex two goroutines could interleave
// their JSON objects on one line and poison the stream for both.
type Encoder struct {
	mu sync.Mutex
	bw *bufio.Writer
	je *json.Encoder
}

// NewEncoder returns an Encoder writing the response stream to w.
func NewEncoder(w io.Writer) *Encoder {
	bw := bufio.NewWriter(w)
	return &Encoder{bw: bw, je: json.NewEncoder(bw)}
}

// Encode writes one response as a single line and flushes it. Flushing per
// response is required, not merely polite: the go tool blocks waiting for the
// reply to a request it has already sent, so a buffered response deadlocks the
// build.
func (e *Encoder) Encode(resp *Response) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.je.Encode(resp); err != nil {
		return fmt.Errorf("Encoder.Encode: %w", err)
	}
	if err := e.bw.Flush(); err != nil {
		return fmt.Errorf("Encoder.Encode: %w", err)
	}
	return nil
}

// WriteHandshake emits the unsolicited capability response that opens the
// conversation. The go tool sends nothing until it has seen this line, so it
// must go out before the first read.
func (e *Encoder) WriteHandshake() error {
	if err := e.Encode(&Response{ID: 0, KnownCommands: KnownCommands()}); err != nil {
		return fmt.Errorf("Encoder.WriteHandshake: %w", err)
	}
	return nil
}
