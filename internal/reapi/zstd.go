// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compression is a property of the wire, never of storage.
//
// A blob arrives compressed or plain and leaves compressed or plain according
// to what each client asked for, but it is stored once, uncompressed, under the
// digest of its uncompressed content. That is what lets one stored body answer
// a compressed reader, a plain reader, the HTTP transport and a Go build that
// produced the same bytes, instead of holding a copy per encoding.
//
// The cost is a decode on a compressed upload and an encode on a compressed
// download, both streaming. Neither is paid unless a client asks for it.

// errUploadAborted unblocks a decoder whose stream ended early.
var errUploadAborted = errors.New("upload aborted")

// decodeSink accepts compressed bytes and writes the plain bytes they decode to
// through to dst.
//
// A decoder is a pull-shaped thing — it reads from a reader — and a gRPC stream
// is a push-shaped one, so the two are joined by a pipe. The goroutine belongs
// to the sink and ends when the sink does; every exit from a write closes the
// pipe, which is what stops it outliving the upload it feeds.
type decodeSink struct {
	pw   *io.PipeWriter
	done chan error
}

// newDecodeSink starts a decoder feeding dst.
func newDecodeSink(dst io.Writer) *decodeSink {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		// One decoder goroutine per upload. The default fans out across CPUs,
		// which is the wrong trade for a server that may be decoding many
		// uploads at once and cares about the total rather than about any one
		// of them finishing first.
		dec, err := zstd.NewReader(pr, zstd.WithDecoderConcurrency(1))
		if err != nil {
			_ = pr.CloseWithError(err)
			done <- err
			return
		}
		defer dec.Close()
		_, err = dec.WriteTo(dst)
		// Closing the read half is what unblocks a client still pushing bytes
		// into a decode that has already failed.
		_ = pr.CloseWithError(err)
		done <- err
	}()
	return &decodeSink{pw: pw, done: done}
}

// Write pushes compressed bytes at the decoder.
func (d *decodeSink) Write(p []byte) (int, error) {
	n, err := d.pw.Write(p)
	if err != nil {
		return n, fmt.Errorf("Write: %w", err)
	}
	return n, nil
}

// Finish ends the stream and reports whether it decoded cleanly.
func (d *decodeSink) Finish() error {
	_ = d.pw.Close()
	if err := <-d.done; err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("Finish: %w", err)
	}
	return nil
}

// Abort tears the decoder down mid-stream, leaving whatever it had already
// decoded in place.
//
// A resumed upload starts a fresh decoder rather than continuing this one: a
// client resuming a compressed write compresses again from the offset it was
// told, so what it sends next is the start of a new frame and not the middle of
// this one. Reporting only what was decoded before the break — which is what
// the offset a resuming client is given comes from — keeps the two agreeing.
func (d *decodeSink) Abort() {
	_ = d.pw.CloseWithError(errUploadAborted)
	<-d.done
}

// encodeReader compresses everything read from src.
//
// The encoder runs at its fastest setting. A cache serves the same blob many
// times and stores it once, so time spent squeezing it is paid on every read
// and saved on none of them; the point of compressing at all is the link, and
// the fast setting already wins most of that.
func encodeReader(dst io.Writer) (*zstd.Encoder, error) {
	enc, err := zstd.NewWriter(dst,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		return nil, fmt.Errorf("encodeReader: %w", err)
	}
	return enc, nil
}

// decodeAll expands one whole compressed buffer, for the batch RPCs, which
// carry entire blobs in one message and have nothing to stream.
func decodeAll(p []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("decodeAll: %w", err)
	}
	defer dec.Close()
	out, err := dec.DecodeAll(p, nil)
	if err != nil {
		return nil, fmt.Errorf("decodeAll: %w", err)
	}
	return out, nil
}

// encodeAll compresses one whole buffer, for the batch RPCs.
func encodeAll(p []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, fmt.Errorf("encodeAll: %w", err)
	}
	defer enc.Close()
	return enc.EncodeAll(p, nil), nil
}
