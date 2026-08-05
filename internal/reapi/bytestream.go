// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"errors"
	"io"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// byteStreamService moves blobs too large for a batch.
//
// Nothing here holds a whole blob. A read streams from the open file to the
// wire and a write streams from the wire to a staging file, so the memory a
// transfer costs is a chunk, not an output — which is the difference between
// serving a three-hundred-megabyte binary and falling over on one.
type byteStreamService struct {
	bytestream.UnimplementedByteStreamServer
	srv *Server
}

// Read streams a blob to the client.
func (b *byteStreamService) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	res, err := parseReadResource(req.GetResourceName())
	if err != nil {
		return err
	}
	if err := b.srv.checkResource(res); err != nil {
		return err
	}
	offset, limit := req.GetReadOffset(), req.GetReadLimit()
	if offset < 0 || limit < 0 {
		return status.Errorf(codes.InvalidArgument, "plaid-cache: read offset %d and limit %d must not be negative", offset, limit)
	}
	if res.compressor != repb.Compressor_IDENTITY && limit != 0 {
		// Required by the API: a limit counts uncompressed bytes, and the
		// server has no way to stop a compressed stream at one of them.
		return status.Error(codes.InvalidArgument, "plaid-cache: a compressed read cannot carry a read limit")
	}

	if res.digest == emptyDigest {
		if offset > 0 {
			return status.Errorf(codes.OutOfRange, "plaid-cache: read offset %d is past the end of the empty blob", offset)
		}
		return nil
	}

	ctx := stream.Context()
	f, size, ok := b.srv.store.Open(ctx, bazel.KindCAS, res.digest)
	if !ok {
		return status.Errorf(codes.NotFound, "plaid-cache: no blob %s", res.digest)
	}
	defer func() { _ = f.Close() }()

	if offset > size {
		return status.Errorf(codes.OutOfRange, "plaid-cache: read offset %d is past the end of a %d-byte blob", offset, size)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		b.srv.logf("bazel grpc: seek %s: %v", res.digest, err)
		return status.Errorf(codes.Internal, "plaid-cache: cannot read blob %s", res.digest)
	}

	remaining := size - offset
	if limit > 0 && limit < remaining {
		remaining = limit
	}
	src := io.LimitReader(f, remaining)
	out := &chunkWriter{stream: stream, buf: make([]byte, 0, readChunkSize)}

	if res.compressor == repb.Compressor_ZSTD {
		enc, err := encodeReader(out)
		if err != nil {
			b.srv.logf("bazel grpc: compress %s: %v", res.digest, err)
			return status.Errorf(codes.Internal, "plaid-cache: cannot compress blob %s", res.digest)
		}
		if _, err := io.CopyBuffer(enc, src, make([]byte, readChunkSize)); err != nil {
			_ = enc.Close()
			return err
		}
		if err := enc.Close(); err != nil {
			return err
		}
	} else if _, err := io.CopyBuffer(out, src, make([]byte, readChunkSize)); err != nil {
		return err
	}
	return out.Flush()
}

// chunkWriter cuts a stream of writes into response messages of a bounded size.
//
// It buffers rather than sending each write straight through because the
// compressed path writes whatever the encoder happens to emit, which is often
// far smaller than a message worth sending on its own.
type chunkWriter struct {
	stream bytestream.ByteStream_ReadServer
	buf    []byte
}

// Write appends to the current chunk, sending whenever one is full.
func (c *chunkWriter) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		n := min(readChunkSize-len(c.buf), len(p))
		c.buf = append(c.buf, p[:n]...)
		p = p[n:]
		if len(c.buf) == readChunkSize {
			if err := c.Flush(); err != nil {
				return written - len(p), err
			}
		}
	}
	return written, nil
}

// Flush sends whatever is buffered.
func (c *chunkWriter) Flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	// The buffer is reused once Send returns, which is safe because gRPC
	// serialises the message before it does.
	if err := c.stream.Send(&bytestream.ReadResponse{Data: c.buf}); err != nil {
		return err
	}
	c.buf = c.buf[:0]
	return nil
}

// Write receives a blob from the client.
//
// A stream that breaks part-way leaves what it delivered on disk, registered
// under its resource name, so the client can ask QueryWriteStatus where it got
// to and continue from there rather than resending the whole blob.
func (b *byteStreamService) Write(stream bytestream.ByteStream_WriteServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "plaid-cache: write stream carried no request")
		}
		return err
	}
	res, err := parseWriteResource(first.GetResourceName())
	if err != nil {
		return err
	}
	if err := b.srv.checkResource(res); err != nil {
		return err
	}
	if first.GetWriteOffset() < 0 {
		return status.Errorf(codes.InvalidArgument, "plaid-cache: write offset %d must not be negative", first.GetWriteOffset())
	}

	if res.digest == emptyDigest {
		// The empty blob is always present, so there is nothing to store. The
		// stream still has to be drained for the client to see a clean end.
		if err := drain(stream, first); err != nil {
			return err
		}
		return stream.SendAndClose(&bytestream.WriteResponse{CommittedSize: 0})
	}

	// A blob another client finished uploading in the meantime ends this one
	// immediately. The API defines the answer: the full uncompressed size for a
	// plain upload, and -1 for a compressed one, whose committed size counts
	// compressed bytes this stream never sent.
	if b.srv.store.Has(ctx, bazel.KindCAS, res.digest) {
		committed := res.size
		if res.compressor != repb.Compressor_IDENTITY {
			committed = -1
		}
		return stream.SendAndClose(&bytestream.WriteResponse{CommittedSize: committed})
	}

	p, err := b.startStream(res, first.GetWriteOffset())
	if err != nil {
		return err
	}

	committed, err := b.receive(stream, p, first)
	if err != nil {
		// Keep what arrived. The client is entitled to ask where it got to and
		// carry on, which is the whole reason this survives a broken stream.
		p.abortStream()
		b.srv.uploads.release(p, true)
		return err
	}

	b.srv.uploads.release(p, false)
	p.discard()
	return stream.SendAndClose(&bytestream.WriteResponse{CommittedSize: committed})
}

// startStream finds or creates the upload a write belongs to.
//
// An offset of zero starts afresh, replacing anything held under the same
// resource name: a client that rewinds to the beginning has abandoned whatever
// it sent before. A non-zero offset must match an upload this server is holding
// and the point it reached, because the alternative is silently writing a body
// with a hole in it.
func (b *byteStreamService) startStream(res resource, offset int64) (*pending, error) {
	if offset == 0 {
		up, err := b.srv.store.Begin(bazel.KindCAS, res.digest)
		if err != nil {
			b.srv.logf("bazel grpc: stage %s: %v", res.digest, err)
			return nil, status.Errorf(codes.Internal, "plaid-cache: cannot accept blob %s", res.digest)
		}
		p := &pending{res: res, up: up}
		if !b.srv.uploads.begin(p) {
			return nil, status.Errorf(codes.Aborted, "plaid-cache: another write to %q is in progress", res.name)
		}
		p.streamStart = 0
		p.streamBytes = 0
		if p.compressed() {
			p.sink = newDecodeSink(p.up)
		}
		return p, nil
	}

	p, busy := b.srv.uploads.claim(res.name)
	if busy {
		return nil, status.Errorf(codes.Aborted, "plaid-cache: another write to %q is in progress", res.name)
	}
	if p == nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"plaid-cache: no upload to resume at offset %d for %q", offset, res.name)
	}
	if got := p.committed(); got != offset {
		b.srv.uploads.release(p, true)
		return nil, status.Errorf(codes.InvalidArgument,
			"plaid-cache: write offset %d does not match the %d bytes already received", offset, got)
	}
	p.streamStart = offset
	p.streamBytes = 0
	if p.compressed() {
		// A resuming client compresses again from the offset it was given, so
		// what arrives next is a new frame rather than the rest of the old one.
		p.sink = newDecodeSink(p.up)
	}
	return p, nil
}

// receive consumes the rest of a write stream and reports the committed size to
// answer with.
func (b *byteStreamService) receive(stream bytestream.ByteStream_WriteServer, p *pending, req *bytestream.WriteRequest) (int64, error) {
	for {
		if name := req.GetResourceName(); name != "" && name != p.res.name {
			return 0, status.Errorf(codes.InvalidArgument,
				"plaid-cache: request names %q on a stream writing %q", name, p.res.name)
		}
		if want := p.streamStart + p.streamBytes; req.GetWriteOffset() != want {
			return 0, status.Errorf(codes.InvalidArgument,
				"plaid-cache: write offset %d is not the expected %d", req.GetWriteOffset(), want)
		}

		if data := req.GetData(); len(data) > 0 {
			dst := io.Writer(p.up)
			if p.sink != nil {
				dst = p.sink
			}
			if _, err := dst.Write(data); err != nil {
				b.srv.logf("bazel grpc: receive %s: %v", p.res.digest, err)
				return 0, status.Errorf(codes.Internal, "plaid-cache: cannot store blob %s", p.res.digest)
			}
			p.streamBytes += int64(len(data))
		}

		if req.GetFinishWrite() {
			return b.finish(stream.Context(), p)
		}

		next, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// The client closed without saying it was finished, which is
				// what a broken upload looks like from here.
				return 0, status.Error(codes.InvalidArgument, "plaid-cache: write stream ended before finish_write")
			}
			return 0, err
		}
		req = next
	}
}

// finish completes a write and publishes the body.
func (b *byteStreamService) finish(ctx context.Context, p *pending) (int64, error) {
	if p.sink != nil {
		err := p.sink.Finish()
		p.sink = nil
		if err != nil {
			return 0, status.Errorf(codes.InvalidArgument, "plaid-cache: blob %s does not decompress: %v", p.res.digest, err)
		}
	}
	if got := p.up.Offset(); got != p.res.size {
		return 0, status.Errorf(codes.InvalidArgument,
			"plaid-cache: blob %s is %d bytes, not the %d it declares", p.res.digest, got, p.res.size)
	}
	if err := p.up.Commit(ctx); err != nil {
		if errors.Is(err, bazel.ErrDigestMismatch) {
			return 0, status.Errorf(codes.InvalidArgument, "plaid-cache: blob does not hash to %s", p.res.digest)
		}
		// A store that did not happen costs a future miss. The client is told
		// it succeeded rather than made to resend a body it no longer holds.
		b.srv.logf("bazel grpc: commit %s: %v", p.res.digest, err)
	}
	// A plain upload's committed size is the blob's. A compressed one's is what
	// the client counted as it sent: the offset it started at plus the
	// compressed bytes it pushed, which is the arithmetic the API defines for
	// its own offsets and the only number the two ends agree on.
	if p.compressed() {
		return p.streamStart + p.streamBytes, nil
	}
	return p.res.size, nil
}

// drain reads a stream to its end, discarding it.
func drain(stream bytestream.ByteStream_WriteServer, req *bytestream.WriteRequest) error {
	for !req.GetFinishWrite() {
		next, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		req = next
	}
	return nil
}

// QueryWriteStatus reports how far a write got, so a client can resume it.
func (b *byteStreamService) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	res, err := parseWriteResource(req.GetResourceName())
	if err != nil {
		return nil, err
	}
	if err := b.srv.checkResource(res); err != nil {
		return nil, err
	}

	if committed, ok := b.srv.uploads.status(req.GetResourceName()); ok {
		return &bytestream.QueryWriteStatusResponse{CommittedSize: committed}, nil
	}
	if res.digest == emptyDigest || b.srv.store.Has(ctx, bazel.KindCAS, res.digest) {
		committed := res.size
		if res.compressor != repb.Compressor_IDENTITY {
			committed = -1
		}
		return &bytestream.QueryWriteStatusResponse{CommittedSize: committed, Complete: true}, nil
	}
	return nil, status.Errorf(codes.NotFound, "plaid-cache: nothing is being written to %q", req.GetResourceName())
}
