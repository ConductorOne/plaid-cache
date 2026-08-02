// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// harness is one server over one real cache, plus clients for every service.
//
// The cache underneath is the real index and body store rather than a fake, so
// what these tests pin is what a build actually gets: the same eviction
// accounting, the same content addressing, the same two namespaced keyspaces.
type harness struct {
	srv   *Server
	store *bazel.Store
	cache *cache.Cache

	caps repb.CapabilitiesClient
	ac   repb.ActionCacheClient
	cas  repb.ContentAddressableStorageClient
	bs   bytestream.ByteStreamClient
}

// newHarness starts a server on a loopback listener and dials it.
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithVerify(t, true)
}

// newHarnessWithVerify starts a server with digest verification on or off.
func newHarnessWithVerify(t *testing.T, verify bool) *harness {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		Dir:               dir,
		MaxBytes:          1 << 30,
		TTL:               time.Hour,
		TouchGranularity:  time.Hour,
		UploadConcurrency: 1,
		DisableEviction:   true,
	}
	ix, err := index.Open(cfg.IndexDir())
	if err != nil {
		t.Fatalf("index.Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	blobs, err := blob.Open(cfg.BlobDir())
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}
	c := cache.New(cache.Params{Config: cfg, Index: ix, Blobs: blobs, Remote: remote.Noop{}})
	t.Cleanup(func() { _ = c.Close() })

	store := bazel.NewStore(bazel.StoreParams{Cache: c, Blobs: blobs, Verify: verify, Logf: t.Logf})
	srv := New(Params{Store: store, Logf: t.Logf, Verify: verify})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	g := grpc.NewServer(ServerOptions()...)
	srv.Register(g)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = g.Serve(ln)
	}()

	conn, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		g.Stop()
		<-served
		srv.DiscardUploads()
	})

	return &harness{
		srv:   srv,
		store: store,
		cache: c,
		caps:  repb.NewCapabilitiesClient(conn),
		ac:    repb.NewActionCacheClient(conn),
		cas:   repb.NewContentAddressableStorageClient(conn),
		bs:    bytestream.NewByteStreamClient(conn),
	}
}

// digestOf is the protocol digest for a body.
func digestOf(b []byte) *repb.Digest {
	sum := sha256.Sum256(b)
	return &repb.Digest{Hash: bazel.Digest(sum).String(), SizeBytes: int64(len(b))}
}

// ctx is a per-test context.
func ctx(t *testing.T) context.Context { return t.Context() }
