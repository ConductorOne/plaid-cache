// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

// startBazelGRPC runs ServeBazelGRPC on a loopback listener and returns a
// connection to it. The daemon is stopped and the goroutine joined at test end,
// which is also what pins that a stop actually reaches the gRPC listener.
func startBazelGRPC(t *testing.T, s *Server) *grpc.ClientConn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.ServeBazelGRPC(context.Background(), ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		s.Stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeBazelGRPC: %v", err)
			}
		case <-time.After(serveStopTimeout):
			t.Errorf("ServeBazelGRPC did not return within %v", serveStopTimeout)
		}
	})
	return conn
}

// TestServeBazelGRPCRoundTrip pins that the daemon's gRPC listener stores and
// serves over the same tiers the socket serves.
func TestServeBazelGRPCRoundTrip(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	conn := startBazelGRPC(t, s)
	cas := repb.NewContentAddressableStorageClient(conn)

	body := []byte("an output produced by some action")
	d := grpcDigest(body)

	up, err := cas.BatchUpdateBlobs(context.Background(), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: d, Data: body}},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := codes.Code(up.GetResponses()[0].GetStatus().GetCode()); c != codes.OK {
		t.Fatalf("store returned %v", c)
	}

	down, err := cas.BatchReadBlobs(context.Background(), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if !bytes.Equal(down.GetResponses()[0].GetData(), body) {
		t.Fatalf("read back %q, want %q", down.GetResponses()[0].GetData(), body)
	}
}

// TestBothBazelListenersShareOneCache pins that the two transports are two
// front ends over one store rather than two caches.
//
// It also pins the thing that would break if each listener swept the staging
// directory for itself: the sweep has to happen once, before either serves, and
// never again while an upload is in flight.
func TestBothBazelListenersShareOneCache(t *testing.T) {
	cfg := newTestConfig(t)
	s := newTestServer(t, cfg)
	base := startBazel(t, s)
	conn := startBazelGRPC(t, s)
	cas := repb.NewContentAddressableStorageClient(conn)

	body := []byte("uploaded over http, read over grpc")
	d := grpcDigest(body)

	req, err := http.NewRequest(http.MethodPut, base+"/cas/"+d.GetHash(), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	// The blob is present as far as gRPC is concerned, which is the answer the
	// HTTP transport has no way to give.
	missing, err := cas.FindMissingBlobs(context.Background(), &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{d},
	})
	if err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}
	if len(missing.GetMissingBlobDigests()) != 0 {
		t.Fatalf("a blob uploaded over HTTP was reported missing over gRPC")
	}

	down, err := cas.BatchReadBlobs(context.Background(), &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if !bytes.Equal(down.GetResponses()[0].GetData(), body) {
		t.Fatalf("gRPC read back %q, want %q", down.GetResponses()[0].GetData(), body)
	}

	// And back the other way.
	grpcOnly := []byte("uploaded over grpc, read over http")
	gd := grpcDigest(grpcOnly)
	if _, err := cas.BatchUpdateBlobs(context.Background(), &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: gd, Data: grpcOnly}},
	}); err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	got, err := http.Get(base + "/cas/" + gd.GetHash())
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", got.StatusCode)
	}
	read, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(read, grpcOnly) {
		t.Fatalf("HTTP read back %q, want %q", read, grpcOnly)
	}
}

// grpcDigest is the protocol digest for a body.
func grpcDigest(b []byte) *repb.Digest {
	sum := sha256.Sum256(b)
	return &repb.Digest{Hash: hex.EncodeToString(sum[:]), SizeBytes: int64(len(b))}
}
