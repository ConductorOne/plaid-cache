// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// requestLine marshals req the way the go tool does: one JSON object, one
// newline.
func requestLine(t *testing.T, req *Request) string {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return string(b) + "\n"
}

// bodyLine renders body as the base64 JSON string line that follows a put.
func bodyLine(body []byte) string {
	return `"` + base64.StdEncoding.EncodeToString(body) + "\"\n"
}

// fill returns n deterministic bytes, so a failure reports a reproducible
// mismatch rather than a random one.
func fill(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// testActionID returns a distinguishable 32-byte action ID.
func testActionID(seed byte) ids.ActionID {
	var a ids.ActionID
	for i := range a {
		a[i] = seed + byte(i)
	}
	return a
}

// testOutputID returns a distinguishable 32-byte output ID.
func testOutputID(seed byte) ids.OutputID {
	var o ids.OutputID
	for i := range o {
		o[i] = seed*2 + byte(i)
	}
	return o
}

// TestWriteHandshakeEmitsUnsolicitedCapabilityLine pins that the server opens
// the conversation with exactly one Response{ID:0, KnownCommands:[get,put,close]}
// line, with ID omitted by omitempty, before reading anything.
func TestWriteHandshakeEmitsUnsolicitedCapabilityLine(t *testing.T) {
	var buf bytes.Buffer
	if err := NewEncoder(&buf).WriteHandshake(); err != nil {
		t.Fatalf("WriteHandshake: %v", err)
	}
	got := buf.String()
	want := "{\"KnownCommands\":[\"get\",\"put\",\"close\"]}\n"
	if got != want {
		t.Fatalf("handshake line = %q, want %q", got, want)
	}

	resp, err := NewResponseDecoder(strings.NewReader(got)).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if resp.ID != 0 {
		t.Fatalf("ID = %d, want 0", resp.ID)
	}
	if len(resp.KnownCommands) != 3 {
		t.Fatalf("len(KnownCommands) = %d, want 3", len(resp.KnownCommands))
	}
	for i, want := range []Cmd{CmdGet, CmdPut, CmdClose} {
		if got := resp.KnownCommands[i]; got != want {
			t.Fatalf("KnownCommands[%d] = %q, want %q", i, got, want)
		}
	}
}

// TestKnownCommandsReturnsFreshSlice pins that a caller mutating the returned
// slice cannot corrupt later handshakes.
func TestKnownCommandsReturnsFreshSlice(t *testing.T) {
	first := KnownCommands()
	first[0] = "clobbered"
	if got := KnownCommands()[0]; got != CmdGet {
		t.Fatalf("KnownCommands()[0] = %q, want %q", got, CmdGet)
	}
}

// TestDecoderDecodesGetRequest pins the get round trip: fields survive the
// wire and no body reader is handed back.
func TestDecoderDecodesGetRequest(t *testing.T) {
	action := testActionID(1)
	in := requestLine(t, &Request{ID: 7, Command: CmdGet, ActionID: action[:]})

	req, body, err := NewDecoder(strings.NewReader(in)).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if body != nil {
		t.Fatalf("body reader = %v, want nil for get", body)
	}
	if req.ID != 7 {
		t.Fatalf("ID = %d, want 7", req.ID)
	}
	if req.Command != CmdGet {
		t.Fatalf("Command = %q, want %q", req.Command, CmdGet)
	}
	gotAction, err := req.Action()
	if err != nil {
		t.Fatalf("Action: %v", err)
	}
	if gotAction != action {
		t.Fatalf("ActionID = %s, want %s", gotAction, action)
	}
}

// TestDecoderPutBodyRoundTrip pins that a put body survives base64 framing
// byte for byte at every base64 padding remainder, and that the request that
// follows it decodes from the correct stream offset.
func TestDecoderPutBodyRoundTrip(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 5, 63, 64, 65, 4096, 8192} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			want := fill(size)
			action, output := testActionID(2), testOutputID(3)
			in := requestLine(t, &Request{
				ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: int64(size),
			}) + bodyLine(want) +
				requestLine(t, &Request{ID: 2, Command: CmdClose})

			d := NewDecoder(strings.NewReader(in))
			req, body, err := d.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if body == nil {
				t.Fatal("body reader = nil, want non-nil for put with BodySize > 0")
			}
			if req.BodySize != int64(size) {
				t.Fatalf("BodySize = %d, want %d", req.BodySize, size)
			}
			gotOutput, err := req.Output()
			if err != nil {
				t.Fatalf("Output: %v", err)
			}
			if gotOutput != output {
				t.Fatalf("OutputID = %s, want %s", gotOutput, output)
			}
			got, err := io.ReadAll(body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("body = %d bytes, want %d identical bytes", len(got), len(want))
			}

			next, nextBody, err := d.Next()
			if err != nil {
				t.Fatalf("Next after body: %v", err)
			}
			if nextBody != nil {
				t.Fatalf("body reader = %v, want nil for close", nextBody)
			}
			if next.Command != CmdClose {
				t.Fatalf("Command = %q, want %q", next.Command, CmdClose)
			}
		})
	}
}

// TestDecoderPutBodyExceedingScannerTokenLimit pins that a body far larger
// than bufio.Scanner's 64KiB default token cap decodes intact — the framing
// has no line-length ceiling.
func TestDecoderPutBodyExceedingScannerTokenLimit(t *testing.T) {
	want := fill(1<<20 + 7)
	action, output := testActionID(4), testOutputID(5)
	in := requestLine(t, &Request{
		ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: int64(len(want)),
	}) + bodyLine(want) +
		requestLine(t, &Request{ID: 2, Command: CmdGet, ActionID: action[:]})

	d := NewDecoder(strings.NewReader(in))
	_, body, err := d.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("body length = %d, want %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body bytes differ from the %d bytes written", len(want))
	}
	// The stream must still be aligned after a multi-megabyte line.
	next, _, err := d.Next()
	if err != nil {
		t.Fatalf("Next after large body: %v", err)
	}
	if next.ID != 2 {
		t.Fatalf("ID = %d, want 2", next.ID)
	}
}

// TestDecoderReadFullBodyWithoutTrailingEOFRead pins that a caller which reads
// exactly BodySize bytes, and so never sees io.EOF from the reader, still
// leaves the decoder able to proceed.
func TestDecoderReadFullBodyWithoutTrailingEOFRead(t *testing.T) {
	want := fill(1000)
	action, output := testActionID(6), testOutputID(7)
	in := requestLine(t, &Request{
		ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: int64(len(want)),
	}) + bodyLine(want) +
		requestLine(t, &Request{ID: 2, Command: CmdClose})

	d := NewDecoder(strings.NewReader(in))
	_, body, err := d.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(body, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("body bytes differ")
	}
	next, _, err := d.Next()
	if err != nil {
		t.Fatalf("Next after ReadFull: %v", err)
	}
	if next.ID != 2 {
		t.Fatalf("ID = %d, want 2", next.ID)
	}
}

// TestDecoderPutWithZeroBodySize pins that a zero-length put is accepted both
// with and without the optional empty body line, since the go tool omits it
// and other clients do not.
func TestDecoderPutWithZeroBodySize(t *testing.T) {
	action, output := testActionID(8), testOutputID(9)
	put := requestLine(t, &Request{ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:]})
	closeReq := requestLine(t, &Request{ID: 2, Command: CmdClose})

	for name, in := range map[string]string{
		"omitted": put + closeReq,
		"empty":   put + bodyLine(nil) + closeReq,
	} {
		t.Run(name, func(t *testing.T) {
			d := NewDecoder(strings.NewReader(in))
			req, body, err := d.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if body != nil {
				t.Fatalf("body reader = %v, want nil for BodySize 0", body)
			}
			if req.BodySize != 0 {
				t.Fatalf("BodySize = %d, want 0", req.BodySize)
			}
			next, _, err := d.Next()
			if err != nil {
				t.Fatalf("Next after zero-size put: %v", err)
			}
			if next.Command != CmdClose {
				t.Fatalf("Command = %q, want %q", next.Command, CmdClose)
			}
		})
	}
}

// TestDecoderRejectsNonEmptyBodyForZeroBodySize pins that a body line carrying
// bytes a zero BodySize did not declare is refused rather than silently
// dropped.
func TestDecoderRejectsNonEmptyBodyForZeroBodySize(t *testing.T) {
	action, output := testActionID(10), testOutputID(11)
	in := requestLine(t, &Request{ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:]}) +
		bodyLine([]byte("surprise"))

	d := NewDecoder(strings.NewReader(in))
	if _, _, err := d.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	_, _, err := d.Next()
	if err == nil {
		t.Fatal("Next = nil error, want a body length mismatch")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("error = %q, want it to mention a mismatch", err)
	}
}

// TestDecoderRejectsMalformedJSON pins that a corrupt request line fails loudly
// instead of yielding a zero-valued request.
func TestDecoderRejectsMalformedJSON(t *testing.T) {
	for name, in := range map[string]string{
		"truncated":  "{\"ID\":1,\"Command\":\n",
		"not json":   "this is not json\n",
		"wrong type": "{\"ID\":\"one\",\"Command\":\"get\"}\n",
	} {
		t.Run(name, func(t *testing.T) {
			req, body, err := NewDecoder(strings.NewReader(in)).Next()
			if err == nil {
				t.Fatalf("Next = (%+v, %v, nil), want an error", req, body)
			}
			if !strings.Contains(err.Error(), "Decoder.Next") {
				t.Fatalf("error = %q, want a Decoder.Next prefix", err)
			}
		})
	}
}

// TestDecoderRejectsBodyLengthMismatch pins that a decoded body which is
// shorter or longer than the declared BodySize is an error, never a silently
// truncated or over-long cache object.
func TestDecoderRejectsBodyLengthMismatch(t *testing.T) {
	action, output := testActionID(12), testOutputID(13)
	cases := map[string]struct {
		declared int64
		actual   []byte
	}{
		"short":       {declared: 64, actual: fill(10)},
		"long":        {declared: 10, actual: fill(64)},
		"long by one": {declared: 3, actual: fill(4)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			in := requestLine(t, &Request{
				ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: tc.declared,
			}) + bodyLine(tc.actual)

			d := NewDecoder(strings.NewReader(in))
			_, body, err := d.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if _, err := io.ReadAll(body); err == nil {
				t.Fatal("read body = nil error, want a length mismatch")
			} else if !strings.Contains(err.Error(), "body length mismatch") {
				t.Fatalf("error = %q, want it to mention a body length mismatch", err)
			}
			// The failure must stay sticky: the stream position is unknown.
			if _, _, err := d.Next(); err == nil {
				t.Fatal("Next after a failed body = nil error, want the failure to persist")
			}
		})
	}
}

// TestDecoderRejectsUndrainedBody pins the documented misuse: calling Next
// before the previous body reader hit io.EOF reports an error instead of
// decoding the tail of the base64 line as a request.
func TestDecoderRejectsUndrainedBody(t *testing.T) {
	action, output := testActionID(14), testOutputID(15)
	in := requestLine(t, &Request{
		ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: 4096,
	}) + bodyLine(fill(4096)) +
		requestLine(t, &Request{ID: 2, Command: CmdClose})

	d := NewDecoder(strings.NewReader(in))
	_, body, err := d.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if _, err := io.ReadFull(body, make([]byte, 16)); err != nil {
		t.Fatalf("partial read: %v", err)
	}
	_, _, err = d.Next()
	if err == nil {
		t.Fatal("Next = nil error, want an undrained-body error")
	}
	if !strings.Contains(err.Error(), "not fully consumed") {
		t.Fatalf("error = %q, want it to mention an unconsumed body", err)
	}
}

// TestDecoderReportsEOFAtEndOfStream pins that a clean end of stream surfaces
// as a bare io.EOF, so a server can distinguish it from a protocol failure.
func TestDecoderReportsEOFAtEndOfStream(t *testing.T) {
	in := requestLine(t, &Request{ID: 1, Command: CmdClose})
	d := NewDecoder(strings.NewReader(in))
	if _, _, err := d.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if _, _, err := d.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
}

// TestDecoderRejectsTruncatedBodyLine pins that a peer dying mid-body is an
// error rather than a short object published to the cache.
func TestDecoderRejectsTruncatedBodyLine(t *testing.T) {
	action, output := testActionID(16), testOutputID(17)
	in := requestLine(t, &Request{
		ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: 4096,
	}) + `"` + base64.StdEncoding.EncodeToString(fill(4096))[:100]

	d := NewDecoder(strings.NewReader(in))
	_, body, err := d.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if _, err := io.ReadAll(body); err == nil {
		t.Fatal("read body = nil error, want an unexpected EOF")
	}
}

// TestEncoderConcurrentWritesProduceWholeLines pins that concurrent Encode
// calls never interleave: every line parses as one response and every ID
// appears exactly once. Run under -race.
func TestEncoderConcurrentWritesProduceWholeLines(t *testing.T) {
	const writers, perWriter = 16, 32

	var buf bytes.Buffer
	e := NewEncoder(&buf)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				id := int64(w*perWriter + i + 1)
				output := testOutputID(byte(id))
				resp := &Response{
					ID:       id,
					Size:     id * 1024,
					DiskPath: strings.Repeat("d", int(id)),
				}
				resp.SetOutputID(output)
				if err := e.Encode(resp); err != nil {
					t.Errorf("Encode(%d): %v", id, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := make(map[int64]bool, writers*perWriter)
	d := NewResponseDecoder(bytes.NewReader(buf.Bytes()))
	for {
		resp, err := d.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if seen[resp.ID] {
			t.Fatalf("ID %d seen twice", resp.ID)
		}
		seen[resp.ID] = true
		if got, want := len(resp.DiskPath), int(resp.ID); got != want {
			t.Fatalf("len(DiskPath) = %d for ID %d, want %d", got, resp.ID, want)
		}
		if got, want := resp.Size, resp.ID*1024; got != want {
			t.Fatalf("Size = %d for ID %d, want %d", got, resp.ID, want)
		}
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("decoded %d responses, want %d", len(seen), writers*perWriter)
	}
}

// TestRequestEncoderRoundTripsThroughDecoder pins that the client half and the
// server half agree on framing, including the body line, so the daemon proxy
// can speak to its own server.
func TestRequestEncoderRoundTripsThroughDecoder(t *testing.T) {
	action, output := testActionID(18), testOutputID(19)
	want := fill(300000)

	var buf bytes.Buffer
	enc := NewRequestEncoder(&buf)
	if err := enc.Encode(&Request{ID: 1, Command: CmdGet, ActionID: action[:]}, nil); err != nil {
		t.Fatalf("Encode get: %v", err)
	}
	put := &Request{ID: 2, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: int64(len(want))}
	if err := enc.Encode(put, bytes.NewReader(want)); err != nil {
		t.Fatalf("Encode put: %v", err)
	}
	if err := enc.Encode(&Request{ID: 3, Command: CmdClose}, nil); err != nil {
		t.Fatalf("Encode close: %v", err)
	}

	d := NewDecoder(bytes.NewReader(buf.Bytes()))
	req, body, err := d.Next()
	if err != nil {
		t.Fatalf("Next get: %v", err)
	}
	if req.Command != CmdGet || body != nil {
		t.Fatalf("first request = (%q, body %v), want (get, nil)", req.Command, body)
	}

	req, body, err = d.Next()
	if err != nil {
		t.Fatalf("Next put: %v", err)
	}
	if req.BodySize != int64(len(want)) {
		t.Fatalf("BodySize = %d, want %d", req.BodySize, len(want))
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %d bytes, want %d identical bytes", len(got), len(want))
	}

	req, _, err = d.Next()
	if err != nil {
		t.Fatalf("Next close: %v", err)
	}
	if req.Command != CmdClose {
		t.Fatalf("Command = %q, want %q", req.Command, CmdClose)
	}
}

// TestRequestEncoderRejectsBodySizeMismatch pins that the client half refuses
// to declare a size its body cannot supply.
func TestRequestEncoderRejectsBodySizeMismatch(t *testing.T) {
	action, output := testActionID(20), testOutputID(21)
	cases := map[string]struct {
		declared int64
		body     []byte
	}{
		"short": {declared: 100, body: fill(10)},
		"long":  {declared: 10, body: fill(100)},
		"nil":   {declared: 10},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var body io.Reader
			if tc.body != nil {
				body = bytes.NewReader(tc.body)
			}
			req := &Request{ID: 1, Command: CmdPut, ActionID: action[:], OutputID: output[:], BodySize: tc.declared}
			err := NewRequestEncoder(&bytes.Buffer{}).Encode(req, body)
			if err == nil {
				t.Fatal("Encode = nil error, want a body size error")
			}
			if !strings.Contains(err.Error(), "RequestEncoder.Encode") {
				t.Fatalf("error = %q, want a RequestEncoder.Encode prefix", err)
			}
		})
	}
}

// TestEncoderRoundTripsThroughResponseDecoder pins that every response field
// the go tool relies on, DiskPath above all, survives the round trip.
func TestEncoderRoundTripsThroughResponseDecoder(t *testing.T) {
	output := testOutputID(22)
	var buf bytes.Buffer
	e := NewEncoder(&buf)
	hit := &Response{ID: 5, Size: 4096, DiskPath: "/tmp/plaid-cache/output/aa/aabb"}
	hit.SetOutputID(output)
	if err := e.Encode(hit); err != nil {
		t.Fatalf("Encode hit: %v", err)
	}
	if err := e.Encode(&Response{ID: 6, Miss: true}); err != nil {
		t.Fatalf("Encode miss: %v", err)
	}

	d := NewResponseDecoder(bytes.NewReader(buf.Bytes()))
	got, err := d.Next()
	if err != nil {
		t.Fatalf("Next hit: %v", err)
	}
	if got.DiskPath != hit.DiskPath {
		t.Fatalf("DiskPath = %q, want %q", got.DiskPath, hit.DiskPath)
	}
	if got.Size != hit.Size {
		t.Fatalf("Size = %d, want %d", got.Size, hit.Size)
	}
	if !bytes.Equal(got.OutputID, output[:]) {
		t.Fatalf("OutputID = %x, want %x", got.OutputID, output[:])
	}
	if got.Miss {
		t.Fatal("Miss = true, want false on a hit")
	}

	got, err = d.Next()
	if err != nil {
		t.Fatalf("Next miss: %v", err)
	}
	if !got.Miss {
		t.Fatal("Miss = false, want true")
	}
	if got.DiskPath != "" {
		t.Fatalf("DiskPath = %q, want empty on a miss", got.DiskPath)
	}
}

// TestRequestTypedIDConversionRejectsWrongLength pins that a truncated wire ID
// is refused rather than zero-padded into a different content address.
func TestRequestTypedIDConversionRejectsWrongLength(t *testing.T) {
	req := &Request{ID: 1, Command: CmdGet, ActionID: []byte{1, 2, 3}}
	if _, err := req.Action(); err == nil {
		t.Fatal("Action = nil error, want a length error")
	}
	if _, err := req.Output(); err == nil {
		t.Fatal("Output = nil error, want a length error")
	}
}

// TestDecoderRejectsBodyOnNonPutCommand pins that only put may carry a body:
// any other command declaring one means the peer and the server disagree about
// where the next line starts.
func TestDecoderRejectsBodyOnNonPutCommand(t *testing.T) {
	action := testActionID(23)
	in := requestLine(t, &Request{ID: 1, Command: CmdGet, ActionID: action[:], BodySize: 8}) + bodyLine(fill(8))
	if _, _, err := NewDecoder(strings.NewReader(in)).Next(); err == nil {
		t.Fatal("Next = nil error, want a rejected body on get")
	}
}
