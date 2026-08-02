// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// writeName is the resource name a client uploads a blob under.
func writeName(uuid string, d *repb.Digest) string {
	return fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid, d.GetHash(), d.GetSizeBytes())
}

// compressedWriteName is the same, for a zstd upload.
func compressedWriteName(uuid string, d *repb.Digest) string {
	return fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", uuid, d.GetHash(), d.GetSizeBytes())
}

// readName is the resource name a client downloads a blob under.
func readName(d *repb.Digest) string {
	return fmt.Sprintf("blobs/%s/%d", d.GetHash(), d.GetSizeBytes())
}

// bigBody is a body no batch would carry, built to be incompressible so that a
// compressed transfer is exercised rather than trivially shrunk away.
func bigBody(t *testing.T, n int) []byte {
	t.Helper()
	r := rand.NewChaCha8([32]byte{7})
	b := make([]byte, n)
	if _, err := r.Read(b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	return b
}

// upload streams a body to the server in chunks, and returns the committed size
// the server reported.
func upload(t *testing.T, h *harness, name string, body []byte, chunk int) int64 {
	t.Helper()
	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	for off := 0; off < len(body) || off == 0; off += chunk {
		end := min(off+chunk, len(body))
		if err := stream.Send(&bytestream.WriteRequest{
			ResourceName: name,
			WriteOffset:  int64(off),
			Data:         body[off:end],
			FinishWrite:  end == len(body),
		}); err != nil {
			t.Fatalf("Send at %d: %v", off, err)
		}
		if end == len(body) {
			break
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	return resp.GetCommittedSize()
}

// download reads a blob back in full.
func download(t *testing.T, h *harness, name string) ([]byte, error) {
	t.Helper()
	stream, err := h.bs.Read(ctx(t), &bytestream.ReadRequest{ResourceName: name})
	if err != nil {
		return nil, err
	}
	var got []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return got, nil
		}
		if err != nil {
			return nil, err
		}
		got = append(got, msg.GetData()...)
	}
}

// TestByteStreamRoundTripsALargeBlob pins the path a real build spends most of
// its bytes on: an output far larger than any batch, streamed both ways.
func TestByteStreamRoundTripsALargeBlob(t *testing.T) {
	h := newHarness(t)
	body := bigBody(t, 5<<20)
	d := digestOf(body)

	if got := upload(t, h, writeName("upload-1", d), body, 256<<10); got != int64(len(body)) {
		t.Fatalf("committed size = %d, want %d", got, len(body))
	}

	back, err := download(t, h, readName(d))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("read back %d bytes that differ from the %d written", len(back), len(body))
	}
}

// TestByteStreamResumesABrokenWrite pins the whole point of the protocol
// carrying an offset.
//
// A stream that dies part-way through a several-hundred-megabyte output must
// not cost the client the whole upload. It asks QueryWriteStatus where the
// server got to and continues from there, and the body that results must be the
// one it was uploading.
func TestByteStreamResumesABrokenWrite(t *testing.T) {
	h := newHarness(t)
	body := bigBody(t, 1<<20)
	d := digestOf(body)
	name := writeName("resumable", d)
	half := len(body) / 2

	// First attempt: send half, then abandon the stream without finishing.
	first, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := first.Send(&bytestream.WriteRequest{ResourceName: name, WriteOffset: 0, Data: body[:half]}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := first.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("abandoned stream = %v, want InvalidArgument for the missing finish_write", err)
	}

	// The server must say how far it got.
	st, err := h.bs.QueryWriteStatus(ctx(t), &bytestream.QueryWriteStatusRequest{ResourceName: name})
	if err != nil {
		t.Fatalf("QueryWriteStatus: %v", err)
	}
	if st.GetComplete() {
		t.Fatalf("an unfinished upload reported complete")
	}
	if st.GetCommittedSize() != int64(half) {
		t.Fatalf("committed size = %d, want the %d already sent", st.GetCommittedSize(), half)
	}

	// Second attempt: continue from there.
	second, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := second.Send(&bytestream.WriteRequest{
		ResourceName: name,
		WriteOffset:  st.GetCommittedSize(),
		Data:         body[half:],
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := second.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.GetCommittedSize() != int64(len(body)) {
		t.Fatalf("committed size = %d, want %d", resp.GetCommittedSize(), len(body))
	}

	back, err := download(t, h, readName(d))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("the resumed upload did not reassemble the original body")
	}
}

// TestQueryWriteStatusReportsACompletedBlob pins that a client asking about a
// blob somebody else finished is told it is done rather than asked to send it.
func TestQueryWriteStatusReportsACompletedBlob(t *testing.T) {
	h := newHarness(t)
	body := []byte("already here")
	d := digestOf(body)
	putBlob(t, h, body)

	st, err := h.bs.QueryWriteStatus(ctx(t), &bytestream.QueryWriteStatusRequest{
		ResourceName: writeName("whoever", d),
	})
	if err != nil {
		t.Fatalf("QueryWriteStatus: %v", err)
	}
	if !st.GetComplete() || st.GetCommittedSize() != int64(len(body)) {
		t.Fatalf("status = (%d, complete=%v), want (%d, true)", st.GetCommittedSize(), st.GetComplete(), len(body))
	}
}

// TestQueryWriteStatusOfNothingIsNotFound pins the answer for a resource the
// server has never heard of, which is what tells a client to start at zero.
func TestQueryWriteStatusOfNothingIsNotFound(t *testing.T) {
	h := newHarness(t)
	_, err := h.bs.QueryWriteStatus(ctx(t), &bytestream.QueryWriteStatusRequest{
		ResourceName: writeName("nobody", digestOf([]byte("never sent"))),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown resource = %v, want NotFound", err)
	}
}

// TestWriteOfAStoredBlobEndsImmediately pins the short circuit the API defines
// for a blob another client finished first: the upload stops without error,
// reporting the blob's full size.
func TestWriteOfAStoredBlobEndsImmediately(t *testing.T) {
	h := newHarness(t)
	body := []byte("someone else got there first")
	d := digestOf(body)
	putBlob(t, h, body)

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: writeName("late", d), WriteOffset: 0, Data: body, FinishWrite: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.GetCommittedSize() != int64(len(body)) {
		t.Fatalf("committed size = %d, want %d", resp.GetCommittedSize(), len(body))
	}
}

// TestWriteRejectsABodyThatDoesNotHashToItsDigest pins content addressing over
// this transport: a stream whose bytes disagree with the name they arrived
// under is refused, and nothing is published.
func TestWriteRejectsABodyThatDoesNotHashToItsDigest(t *testing.T) {
	h := newHarness(t)
	body := []byte("the bytes actually sent")
	lie := digestOf([]byte("a different body entirely"))
	lie.SizeBytes = int64(len(body))

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: writeName("liar", lie), WriteOffset: 0, Data: body, FinishWrite: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("mismatched body = %v, want InvalidArgument", err)
	}
	if f, _, ok := h.store.Open(ctx(t), bazel.KindCAS, mustDigest(t, lie.GetHash())); ok {
		_ = f.Close()
		t.Fatalf("a body that does not hash to its digest was published under it")
	}
}

// TestWriteRejectsAShortBody pins that a stream which says it is finished
// before delivering the length it declared is refused.
func TestWriteRejectsAShortBody(t *testing.T) {
	h := newHarness(t)
	body := []byte("a body that will be truncated")
	d := digestOf(body)

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: writeName("short", d), WriteOffset: 0, Data: body[:5], FinishWrite: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("short body = %v, want InvalidArgument", err)
	}
}

// TestWriteRejectsAnOutOfOrderOffset pins that a client which skips ahead is
// told, rather than having a hole written into a body that will then hash to
// nothing anybody asked for.
func TestWriteRejectsAnOutOfOrderOffset(t *testing.T) {
	h := newHarness(t)
	body := []byte("a body sent out of order")
	d := digestOf(body)

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: writeName("skipper", d), WriteOffset: 0, Data: body[:4],
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{WriteOffset: 99, Data: body[4:], FinishWrite: true}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("out-of-order offset = %v, want InvalidArgument", err)
	}
}

// TestReadHonoursOffsetAndLimit pins partial reads, which is how a client
// resumes a download it lost part-way through.
func TestReadHonoursOffsetAndLimit(t *testing.T) {
	h := newHarness(t)
	body := bigBody(t, 64<<10)
	d := digestOf(body)
	upload(t, h, writeName("partial", d), body, 16<<10)

	stream, err := h.bs.Read(ctx(t), &bytestream.ReadRequest{
		ResourceName: readName(d), ReadOffset: 1000, ReadLimit: 500,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var got []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, msg.GetData()...)
	}
	if !bytes.Equal(got, body[1000:1500]) {
		t.Fatalf("read %d bytes from offset 1000, want the 500 at that offset", len(got))
	}
}

// TestReadPastTheEndIsOutOfRange pins that an offset beyond the blob is a
// distinct answer from a miss, so a client can tell a bad request from an
// evicted blob.
func TestReadPastTheEndIsOutOfRange(t *testing.T) {
	h := newHarness(t)
	body := []byte("a short body")
	d := digestOf(body)
	upload(t, h, writeName("short-read", d), body, 1<<10)

	stream, err := h.bs.Read(ctx(t), &bytestream.ReadRequest{ResourceName: readName(d), ReadOffset: 9999})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.OutOfRange {
		t.Fatalf("read past the end = %v, want OutOfRange", err)
	}
}

// TestReadOfAnAbsentBlobIsNotFound pins the answer that tells a client an
// output its action result named has been evicted.
func TestReadOfAnAbsentBlobIsNotFound(t *testing.T) {
	h := newHarness(t)
	stream, err := h.bs.Read(ctx(t), &bytestream.ReadRequest{ResourceName: readName(digestOf([]byte("gone")))})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("absent blob = %v, want NotFound", err)
	}
}

// TestReadOfTheEmptyBlobSucceeds pins that the blob nobody uploads can still be
// read, which is what makes skipping its upload safe.
func TestReadOfTheEmptyBlobSucceeds(t *testing.T) {
	h := newHarness(t)
	got, err := download(t, h, readName(digestOf(nil)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("the empty blob read back %d bytes", len(got))
	}
}

// TestByteStreamRoundTripsACompressedBlob pins that a client using
// --remote_cache_compression gets its bytes back, and that the entry it leaves
// behind is the plain body — readable by a client that does not compress, and
// by the HTTP transport.
func TestByteStreamRoundTripsACompressedBlob(t *testing.T) {
	h := newHarness(t)
	body := bytes.Repeat([]byte("a compressible output "), 40000)
	d := digestOf(body)

	packed, err := encodeAll(body)
	if err != nil {
		t.Fatalf("encodeAll: %v", err)
	}
	name := compressedWriteName("zstd-upload", d)

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	const chunk = 32 << 10
	for off := 0; off < len(packed); off += chunk {
		end := min(off+chunk, len(packed))
		if err := stream.Send(&bytestream.WriteRequest{
			ResourceName: name,
			WriteOffset:  int64(off),
			Data:         packed[off:end],
			FinishWrite:  end == len(packed),
		}); err != nil {
			t.Fatalf("Send at %d: %v", off, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	// Plain read: what was stored is the uncompressed body.
	plain, err := download(t, h, readName(d))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(plain, body) {
		t.Fatalf("a compressed upload did not store the uncompressed body")
	}

	// Compressed read: decodes to the same thing.
	got, err := download(t, h, fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.GetHash(), d.GetSizeBytes()))
	if err != nil {
		t.Fatalf("compressed Read: %v", err)
	}
	back, err := decodeAll(got)
	if err != nil {
		t.Fatalf("decodeAll: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("the compressed read did not decode to the original body")
	}
}

// TestCompressedReadRejectsALimit pins the one thing the API requires a server
// to refuse here: a limit counts uncompressed bytes, which a compressed stream
// cannot be stopped at.
func TestCompressedReadRejectsALimit(t *testing.T) {
	h := newHarness(t)
	body := []byte("something")
	d := digestOf(body)
	putBlob(t, h, body)

	stream, err := h.bs.Read(ctx(t), &bytestream.ReadRequest{
		ResourceName: fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.GetHash(), d.GetSizeBytes()),
		ReadLimit:    10,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("compressed read with a limit = %v, want InvalidArgument", err)
	}
}

// TestWriteToANamedInstanceIsRefused pins that the instance-name rule holds on
// resource names as well as on RPC fields.
func TestWriteToANamedInstanceIsRefused(t *testing.T) {
	h := newHarness(t)
	d := digestOf([]byte("x"))
	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: "some-instance/" + writeName("u", d), FinishWrite: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("named instance = %v, want InvalidArgument", err)
	}
}

// TestAbandonedUploadsAreSwept pins that a client which never resumes does not
// hold a staged body for the life of the daemon.
func TestAbandonedUploadsAreSwept(t *testing.T) {
	h := newHarness(t)
	body := bigBody(t, 8<<10)
	d := digestOf(body)
	name := writeName("abandoned", d)

	stream, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Send(&bytestream.WriteRequest{ResourceName: name, Data: body[:100]}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatalf("abandoned stream reported success")
	}

	if n := h.srv.uploads.sweep(time.Now().Add(-time.Hour)); n != 0 {
		t.Fatalf("swept %d uploads that are not yet idle", n)
	}
	if n := h.srv.uploads.sweep(time.Now().Add(2 * uploadIdleTimeout)); n != 1 {
		t.Fatalf("swept %d idle uploads, want 1", n)
	}
	if _, err := h.bs.QueryWriteStatus(ctx(t), &bytestream.QueryWriteStatusRequest{ResourceName: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("a swept upload = %v, want NotFound", err)
	}
}

// marshalResult encodes an ActionResult the way both transports store one.
func marshalResult(t *testing.T, r *repb.ActionResult) []byte {
	t.Helper()
	b, err := proto.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return b
}

// TestConcurrentWritesToOneResourceAreSerialised pins that two streams into one
// resource name do not interleave into one body.
//
// The resource name carries an upload id the client chose so that concurrent
// uploads of the same blob do not collide; two streams that reuse it are a
// client bug, and the answer is to refuse the second rather than to write a
// body that is neither client's.
func TestConcurrentWritesToOneResourceAreSerialised(t *testing.T) {
	h := newHarness(t)
	body := bigBody(t, 1<<20)
	d := digestOf(body)
	name := writeName("contended", d)

	first, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := first.Send(&bytestream.WriteRequest{ResourceName: name, Data: body[:1024]}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait for the server to have taken the claim, so that "second" is
	// genuinely the second and the test is not racing its own setup.
	waitClaimed(t, h, name)

	second, err := h.bs.Write(ctx(t))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := second.Send(&bytestream.WriteRequest{ResourceName: name, Data: body[:1024]}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := second.CloseAndRecv(); status.Code(err) != codes.Aborted {
		t.Fatalf("second concurrent write = %v, want Aborted", err)
	}

	// The first stream is untouched and still finishes its own body.
	if err := first.Send(&bytestream.WriteRequest{WriteOffset: 1024, Data: body[1024:], FinishWrite: true}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := first.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	back, err := download(t, h, readName(d))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(back, body) {
		t.Fatalf("the contended upload did not store the first stream's body")
	}
}

// waitClaimed blocks until the server has an in-flight write for a resource
// name, so a test that depends on ordering does not race the server's own
// bookkeeping.
func waitClaimed(t *testing.T, h *harness, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.srv.uploads.mu.Lock()
		p, ok := h.srv.uploads.byID[name]
		claimed := ok && p.claimed
		h.srv.uploads.mu.Unlock()
		if claimed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the server never took a claim on %q", name)
}
