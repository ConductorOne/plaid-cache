// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// RequestEncoder writes requests, and their optional bodies, to a cache
// program. It is the mirror of Decoder and the half plaid-cache uses when it
// is the client: the connect path proxies the go tool's requests to the daemon
// over the unix socket.
//
// It is safe for concurrent use, because a proxy multiplexes many toolchain
// requests onto one socket.
type RequestEncoder struct {
	mu sync.Mutex
	bw *bufio.Writer
	je *json.Encoder
}

// NewRequestEncoder returns a RequestEncoder writing the request stream to w.
func NewRequestEncoder(w io.Writer) *RequestEncoder {
	bw := bufio.NewWriter(w)
	return &RequestEncoder{bw: bw, je: json.NewEncoder(bw)}
}

// Encode writes one request, followed by the base64 body line when
// req.BodySize is greater than zero. body must yield exactly req.BodySize
// bytes; it is ignored otherwise and may be nil.
//
// A body that does not match the declared size is an error, and the stream is
// unusable afterwards: part of the line has already been written and there is
// no way to retract it.
func (e *RequestEncoder) Encode(req *Request, body io.Reader) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if req.BodySize < 0 {
		return fmt.Errorf("RequestEncoder.Encode: negative BodySize %d", req.BodySize)
	}
	if err := e.je.Encode(req); err != nil {
		return fmt.Errorf("RequestEncoder.Encode: %w", err)
	}
	if req.BodySize > 0 {
		if body == nil {
			return fmt.Errorf("RequestEncoder.Encode: BodySize %d with no body", req.BodySize)
		}
		if err := e.writeBody(req.BodySize, body); err != nil {
			return err
		}
	}
	if err := e.bw.Flush(); err != nil {
		return fmt.Errorf("RequestEncoder.Encode: %w", err)
	}
	return nil
}

// writeBody emits the quoted base64 line for exactly size bytes of body.
func (e *RequestEncoder) writeBody(size int64, body io.Reader) error {
	if err := e.bw.WriteByte('"'); err != nil {
		return fmt.Errorf("RequestEncoder.Encode: %w", err)
	}
	enc := base64.NewEncoder(base64.StdEncoding, e.bw)
	n, err := io.CopyN(enc, body, size)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("RequestEncoder.Encode: body length mismatch: got %d bytes, want %d", n, size)
	}
	if err != nil {
		return fmt.Errorf("RequestEncoder.Encode: copy body: %w", err)
	}
	var scratch [1]byte
	switch extra, err := body.Read(scratch[:]); {
	case extra > 0:
		return fmt.Errorf("RequestEncoder.Encode: body length mismatch: got more than the declared %d bytes", size)
	case err != nil && !errors.Is(err, io.EOF):
		return fmt.Errorf("RequestEncoder.Encode: read body: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("RequestEncoder.Encode: %w", err)
	}
	if _, err := e.bw.WriteString("\"\n"); err != nil {
		return fmt.Errorf("RequestEncoder.Encode: %w", err)
	}
	return nil
}

// ResponseDecoder reads responses from a cache program. It is the mirror of
// Encoder, and like Decoder it is single-reader: one goroutine pulls responses
// and dispatches them to waiters keyed by Response.ID.
type ResponseDecoder struct {
	br *bufio.Reader
}

// NewResponseDecoder returns a ResponseDecoder reading the response stream
// from r.
func NewResponseDecoder(r io.Reader) *ResponseDecoder {
	return &ResponseDecoder{br: bufio.NewReader(r)}
}

// Next returns the next response. The handshake arrives as an ordinary
// response with ID 0 and a populated KnownCommands, so it needs no special
// call. A clean end of stream is reported as io.EOF.
func (d *ResponseDecoder) Next() (*Response, error) {
	line, err := readLine(d.br)
	if err != nil {
		return nil, err
	}
	resp := new(Response)
	if err := json.Unmarshal(line, resp); err != nil {
		return nil, fmt.Errorf("ResponseDecoder.Next: decode response: %w", err)
	}
	return resp, nil
}
