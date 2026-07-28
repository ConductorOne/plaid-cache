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
	"strconv"
)

// Decoder reads requests, and their optional bodies, from the go tool.
//
// A Decoder is not safe for concurrent use: the protocol is a byte stream, so
// exactly one goroutine may pull from it. That goroutine is free to hand each
// decoded request to a worker pool, because responses are correlated by ID.
type Decoder struct {
	br *bufio.Reader

	// body is the reader handed out by the most recent Next. It stays set
	// until the next call so that an undrained or failed body is reported
	// instead of silently desynchronizing the stream.
	body *bodyReader

	// emptyBodyLinePending records that the previous request was a put with
	// BodySize 0. The go tool omits the body line entirely in that case, but
	// other clients emit an empty one; both are accepted, and the difference
	// cannot be probed eagerly without blocking a pipelined stream.
	emptyBodyLinePending bool
}

// NewDecoder returns a Decoder reading the request stream from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{br: bufio.NewReader(r)}
}

// Next returns the next request and, for a put carrying a body, a reader over
// the decoded body bytes. The body reader is nil for every other request.
//
// The body reader MUST be read to io.EOF before Next is called again. It
// shares the underlying stream, so abandoning it half-read would leave the
// remainder of the base64 line where the next request line is expected; Next
// reports that misuse as an error rather than decoding garbage.
//
// A clean end of stream is reported as io.EOF.
func (d *Decoder) Next() (*Request, io.Reader, error) {
	if d.body != nil {
		switch {
		case d.body.err != nil:
			// Leave d.body set: the stream position is unknown after a body
			// error, so every later call must fail the same way.
			return nil, nil, fmt.Errorf("Decoder.Next: previous body failed: %w", d.body.err)
		case !d.body.done:
			return nil, nil, errors.New("Decoder.Next: previous request body was not fully consumed")
		}
		d.body = nil
	}

	line, err := readFrame(d.br)
	if err != nil {
		return nil, nil, err
	}
	if d.emptyBodyLinePending {
		d.emptyBodyLinePending = false
		if len(line) > 0 && line[0] == '"' {
			if err := checkEmptyBodyLine(line); err != nil {
				return nil, nil, err
			}
			line, err = readFrame(d.br)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	req := new(Request)
	if err := json.Unmarshal(line, req); err != nil {
		// Include the offending bytes: a desynchronized stream is otherwise
		// almost impossible to diagnose from the error alone.
		return nil, nil, fmt.Errorf("Decoder.Next: decode request %s: %w", truncate(line, 120), err)
	}
	switch {
	case req.BodySize < 0:
		return nil, nil, fmt.Errorf("Decoder.Next: negative BodySize %d", req.BodySize)
	case req.BodySize > 0 && req.Command != CmdPut:
		return nil, nil, fmt.Errorf("Decoder.Next: command %q carries a body of %d bytes", req.Command, req.BodySize)
	case req.BodySize > 0:
		d.body = newBodyReader(d.br, req.BodySize)
		return req, d.body, nil
	case req.Command == CmdPut:
		d.emptyBodyLinePending = true
	}
	return req, nil, nil
}

// checkEmptyBodyLine validates the optional body line that follows a put with
// BodySize 0. Anything other than an empty payload means the sender and the
// declared size disagree, which would corrupt the cached object.
func checkEmptyBodyLine(line []byte) error {
	var s string
	if err := json.Unmarshal(line, &s); err != nil {
		return fmt.Errorf("Decoder.Next: decode empty body line: %w", err)
	}
	if s != "" {
		return fmt.Errorf("Decoder.Next: body length mismatch: got %d encoded bytes, want 0", len(s))
	}
	return nil
}

// readFrame returns the next non-empty line.
//
// Blank lines are skipped because the Go toolchain emits one after every
// request: cmd/go encodes the request with a json.Encoder, which already
// appends a newline, and then writes a second newline of its own
// (cmd/go/internal/cache/prog.go). A decoder that treats the resulting blank
// line as a frame fails on the very first request of every real build.
func readFrame(br *bufio.Reader) ([]byte, error) {
	for {
		line, err := readLine(br)
		if err != nil {
			return nil, err
		}
		if len(line) > 0 {
			return line, nil
		}
	}
}

// readLine returns one line with its terminator stripped.
//
// bufio.Scanner is deliberately not used: its default 64KiB token limit would
// silently fail on the multi-megabyte base64 body lines this protocol carries.
// ReadSlice plus an accumulator has no ceiling beyond available memory.
func readLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if err == nil {
			chunk = chunk[:len(chunk)-1]
		}
		if len(chunk) > 0 {
			line = append(line, chunk...)
		}
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(line) == 0 {
				return nil, io.EOF
			}
			// A final line without its terminator is accepted; a peer that
			// died mid-line is caught by the JSON decode instead.
			return line, nil
		default:
			return nil, fmt.Errorf("readLine: %w", err)
		}
	}
}

// bodyReader decodes one base64 body line into the caller's buffer, enforcing
// that the decoded length is exactly the declared BodySize.
type bodyReader struct {
	line *quotedReader
	b64  io.Reader
	size int64
	n    int64
	done bool
	err  error
}

// newBodyReader wraps the body line that immediately follows a put request.
func newBodyReader(br *bufio.Reader, size int64) *bodyReader {
	line := &quotedReader{br: br}
	return &bodyReader{
		line: line,
		b64:  base64.NewDecoder(base64.StdEncoding, line),
		size: size,
	}
}

// Read implements io.Reader over the decoded body bytes.
func (b *bodyReader) Read(p []byte) (int, error) {
	switch {
	case b.err != nil:
		return 0, b.err
	case b.done:
		return 0, io.EOF
	case len(p) == 0:
		return 0, nil
	}
	// Never hand back more than the declared size: an over-long body must
	// surface as a mismatch, not as extra bytes in the caller's object.
	if rem := b.size - b.n; int64(len(p)) > rem {
		p = p[:rem]
	}
	if len(p) == 0 {
		return 0, b.finish()
	}

	n, err := b.b64.Read(p)
	b.n += int64(n)
	switch {
	case err == nil:
		if b.n < b.size {
			return n, nil
		}
		// The declared size is satisfied. Validate the tail now rather than
		// on a later Read, so a caller that reads exactly BodySize bytes and
		// stops still leaves the decoder ready for the next request.
		if ferr := b.finish(); !errors.Is(ferr, io.EOF) {
			return n, ferr
		}
		return n, nil
	case !errors.Is(err, io.EOF):
		return n, b.fail(fmt.Errorf("bodyReader.Read: decode base64: %w", err))
	case b.n < b.size:
		return n, b.fail(fmt.Errorf("bodyReader.Read: body length mismatch: decoded %d bytes, want %d", b.n, b.size))
	}
	if err := b.finish(); !errors.Is(err, io.EOF) {
		return n, err
	}
	return n, io.EOF
}

// finish confirms the encoded body ends exactly at the declared size and
// consumes the rest of the line. It returns io.EOF on success so Read can
// forward it directly.
func (b *bodyReader) finish() error {
	var scratch [1]byte
	n, err := b.b64.Read(scratch[:])
	if n > 0 {
		return b.fail(fmt.Errorf("bodyReader.Read: body length mismatch: decoded more than the declared %d bytes", b.size))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return b.fail(fmt.Errorf("bodyReader.Read: decode base64: %w", err))
	}
	if err := b.line.drain(); err != nil {
		return b.fail(err)
	}
	b.done = true
	return io.EOF
}

// fail records err so every later Read, and the next Decoder.Next, report the
// original cause instead of a confusing downstream symptom.
func (b *bodyReader) fail(err error) error {
	if b.err == nil {
		b.err = err
	}
	return b.err
}

// quotedReader yields the raw bytes between the quotes of one JSON-string body
// line. The base64 alphabet contains no character that JSON would escape, so
// scanning for the closing quote is sufficient and no unescaping is needed.
type quotedReader struct {
	br *bufio.Reader
	// buf aliases the bufio buffer and is invalidated by the next read from
	// br, so it is always drained into the caller's slice before refilling.
	buf     []byte
	opened  bool
	closed  bool
	drained bool
}

// Read implements io.Reader over the encoded payload, reporting io.EOF at the
// closing quote.
func (q *quotedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !q.opened {
		// Skip the line terminators separating the request from its body.
		// The Go toolchain writes an extra newline after every request, so a
		// body line is normally preceded by a blank line.
		var c byte
		for {
			b, err := q.br.ReadByte()
			if errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			if err != nil {
				return 0, fmt.Errorf("quotedReader.Read: %w", err)
			}
			if b != '\n' && b != '\r' {
				c = b
				break
			}
		}
		if c != '"' {
			return 0, fmt.Errorf("quotedReader.Read: want quoted body line, got %q", string(c))
		}
		q.opened = true
	}
	for len(q.buf) == 0 {
		if q.closed {
			return 0, io.EOF
		}
		if err := q.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, q.buf)
	q.buf = q.buf[n:]
	return n, nil
}

// fill loads the next chunk of payload. ErrBufferFull is the normal path for a
// body line larger than the bufio buffer: it yields what has been read so far
// and leaves the closing quote for a later call.
func (q *quotedReader) fill() error {
	chunk, err := q.br.ReadSlice('"')
	switch {
	case err == nil:
		q.buf = chunk[:len(chunk)-1]
		q.closed = true
		return nil
	case errors.Is(err, bufio.ErrBufferFull):
		q.buf = chunk
		return nil
	case errors.Is(err, io.EOF):
		return io.ErrUnexpectedEOF
	default:
		return fmt.Errorf("quotedReader.fill: %w", err)
	}
}

// drain consumes the closing quote and the line terminator.
//
// It is required rather than optional: a padded base64 body lets the decoder
// stop before it has read the closing quote, and leaving those bytes in the
// stream would put the next request line one quote out of alignment.
func (q *quotedReader) drain() error {
	if q.drained {
		return nil
	}
	var scratch [64]byte
	for {
		n, err := q.Read(scratch[:])
		if n > 0 {
			return fmt.Errorf("quotedReader.drain: unexpected trailing bytes in body line")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	c, err := q.br.ReadByte()
	switch {
	case errors.Is(err, io.EOF):
		// The peer ended the stream right after the body; nothing follows
		// that could be misread.
	case err != nil:
		return fmt.Errorf("quotedReader.drain: %w", err)
	case c != '\n':
		return fmt.Errorf("quotedReader.drain: want newline after body line, got %q", string(c))
	}
	q.drained = true
	return nil
}

// truncate renders b for an error message, capping the length so a
// multi-megabyte body line cannot flood the log.
func truncate(b []byte, max int) string {
	if len(b) == 0 {
		return "(empty line)"
	}
	if len(b) <= max {
		return strconv.Quote(string(b))
	}
	return strconv.Quote(string(b[:max])) + "..."
}
