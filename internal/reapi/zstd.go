// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

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
//
// What is not paid per transfer is building the coders. An encoder carries a
// history buffer twice its window and a set of block buffers, and a decoder its
// own window; allocating those for every read made constructing them cost as
// much as the encoding itself on small blobs, and made them most of what the
// process allocated. So streaming coders are pooled, and the whole-buffer
// coders the batch RPCs use are shared, which their EncodeAll and DecodeAll are
// built for.

// errUploadAborted unblocks a decoder whose stream ended early.
var errUploadAborted = errors.New("upload aborted")

// encodeWindow is the window the encoders compress over.
//
// The library's default is 8 MiB, and its history buffer is twice that, so a
// pool of encoders held one per concurrent compressed read is mostly history.
// At the fastest setting the encoder's match finder rarely reaches that far
// back, so a smaller window costs next to nothing in ratio; the benchmark that
// chose this measured no difference at 4 MiB and under a percent at 1 MiB on
// build outputs. It is also a window every decoder accepts without asking for
// more memory.
const encodeWindow = 1 << 20

// encoderOptions configure every encoder this package builds.
//
// One goroutine per stream: the default fans out across CPUs, which is the
// wrong trade for a server that may be encoding many reads at once and cares
// about the total rather than about any one of them finishing first. The
// fastest setting because a cache serves the same blob many times and stores it
// once, so time spent squeezing it is paid on every read and saved on none; the
// point of compressing at all is the link, and the fast setting already wins
// most of that.
func encoderOptions(concurrency int) []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithWindowSize(encodeWindow),
	}
}

// encoders holds streaming encoders between reads.
//
// Only an encoder whose stream closed cleanly goes back. One that failed part
// way is dropped instead: Reset does clear its state, but a pool is the wrong
// place to find out it missed something, and with one goroutine per encoder
// there is nothing left running to leak.
var encoders sync.Pool

// getEncoder returns an encoder ready to write a new frame to dst.
func getEncoder(dst io.Writer) (*zstd.Encoder, error) {
	if enc, ok := encoders.Get().(*zstd.Encoder); ok {
		enc.Reset(dst)
		return enc, nil
	}
	enc, err := zstd.NewWriter(dst, encoderOptions(1)...)
	if err != nil {
		return nil, fmt.Errorf("getEncoder: %w", err)
	}
	return enc, nil
}

// putEncoder returns an encoder whose Close succeeded. It is detached from its
// writer first so the pool does not keep a finished stream reachable.
func putEncoder(enc *zstd.Encoder) {
	enc.Reset(nil)
	encoders.Put(enc)
}

// decoders holds streaming decoders between uploads, on the same terms as
// encoders: only one that reached the end of a clean stream goes back.
var decoders sync.Pool

// getDecoder returns a decoder reading from src.
func getDecoder(src io.Reader) (*zstd.Decoder, error) {
	if dec, ok := decoders.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(src); err != nil {
			dec.Close()
			return nil, fmt.Errorf("getDecoder: %w", err)
		}
		return dec, nil
	}
	// One goroutine per upload, for the reason encoderOptions gives.
	dec, err := zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("getDecoder: %w", err)
	}
	return dec, nil
}

// putDecoder returns a decoder that decoded its stream to the end.
func putDecoder(dec *zstd.Decoder) {
	// Reset to no input fails by design, and is only here to drop the
	// decoder's reference to the stream it finished.
	_ = dec.Reset(nil)
	decoders.Put(dec)
}

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
		dec, err := getDecoder(pr)
		if err != nil {
			_ = pr.CloseWithError(err)
			done <- err
			return
		}
		_, err = dec.WriteTo(dst)
		if err == nil || errors.Is(err, io.EOF) {
			putDecoder(dec)
		} else {
			dec.Close()
		}
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

// batchCoders are the whole-buffer encoder and decoder the batch RPCs share.
//
// EncodeAll and DecodeAll are safe to call concurrently and hand each call one
// of a fixed set of internal coders, so one of each sized to the CPUs is the
// whole pool: a batch beyond that many waits for a coder rather than building
// one, which on a CPU-bound server is the same wait by another name.
var batchCoders = sync.OnceValues(func() (*zstd.Encoder, *zstd.Decoder) {
	n := runtime.GOMAXPROCS(0)
	// Neither constructor fails for options this fixed; a nil from either is
	// reported by the call that needed it.
	enc, _ := zstd.NewWriter(nil, encoderOptions(n)...)
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(n))
	return enc, dec
})

// decodeAll expands one whole compressed buffer, for the batch RPCs, which
// carry entire blobs in one message and have nothing to stream.
func decodeAll(p []byte) ([]byte, error) {
	_, dec := batchCoders()
	if dec == nil {
		return nil, errors.New("decodeAll: no decoder")
	}
	out, err := dec.DecodeAll(p, nil)
	if err != nil {
		return nil, fmt.Errorf("decodeAll: %w", err)
	}
	return out, nil
}

// encodeAll compresses one whole buffer, for the batch RPCs.
func encodeAll(p []byte) ([]byte, error) {
	enc, _ := batchCoders()
	if enc == nil {
		return nil, errors.New("encodeAll: no encoder")
	}
	return enc.EncodeAll(p, nil), nil
}
