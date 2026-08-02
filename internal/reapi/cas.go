// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"context"
	"errors"
	"io"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// casService serves content-addressed blobs.
type casService struct {
	repb.UnimplementedContentAddressableStorageServer
	srv *Server
}

// checkInstance rejects a named instance, for the same reason a resource name
// carrying one is rejected: one keyspace serving several instance names would
// let two logically separate caches share entries without saying so.
func checkInstance(name string) error {
	if name != "" {
		return status.Errorf(codes.InvalidArgument,
			"plaid-cache: instance %q is not served; this cache serves a single unnamed instance", name)
	}
	return nil
}

// FindMissingBlobs reports which of a client's digests this cache does not
// hold.
//
// This is the reason the gRPC transport exists. Bazel's HTTP cache client has
// no way to ask the question, so it answers it itself by declaring every digest
// missing and uploading accordingly: every action that re-runs re-sends outputs
// the server already has, and a single output is routinely hundreds of
// megabytes. An honest answer here turns all of that into one round trip.
//
// It is honest and it is cheap, which have to be true together. Each digest
// costs an index lookup and a stat, never a read of the body, so answering for
// every output of a build at once stays proportional to the number of outputs
// rather than to their size.
//
// A digest reported present is also touched, so the eviction pass treats it as
// in use. Without that, the answer would be a promise the cache was free to
// break between here and the client's later read.
func (c *casService) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	if err := checkInstance(req.GetInstanceName()); err != nil {
		return nil, err
	}
	if err := c.srv.checkDigestFunction(req.GetDigestFunction()); err != nil {
		return nil, err
	}

	missing := make([]*repb.Digest, 0, len(req.GetBlobDigests()))
	for _, bd := range req.GetBlobDigests() {
		d, err := digest(bd)
		if err != nil {
			return nil, err
		}
		if d == emptyDigest {
			continue
		}
		if !c.srv.store.Has(ctx, bazel.KindCAS, d) {
			missing = append(missing, bd)
		}
		if err := ctx.Err(); err != nil {
			// A client that has given up on a batch of thousands should not
			// keep the index busy answering the rest of it.
			return nil, status.FromContextError(err).Err()
		}
	}
	return &repb.FindMissingBlobsResponse{MissingBlobDigests: missing}, nil
}

// BatchUpdateBlobs stores several small blobs in one call.
//
// A per-blob failure is a per-blob status rather than a failed call: the other
// blobs in the batch stored fine, and a client told the whole batch failed
// would resend all of them.
func (c *casService) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	if err := checkInstance(req.GetInstanceName()); err != nil {
		return nil, err
	}
	if err := c.srv.checkDigestFunction(req.GetDigestFunction()); err != nil {
		return nil, err
	}

	resp := &repb.BatchUpdateBlobsResponse{
		Responses: make([]*repb.BatchUpdateBlobsResponse_Response, 0, len(req.GetRequests())),
	}
	for _, r := range req.GetRequests() {
		resp.Responses = append(resp.Responses, &repb.BatchUpdateBlobsResponse_Response{
			Digest: r.GetDigest(),
			Status: c.updateOne(ctx, r),
		})
	}
	return resp, nil
}

// updateOne stores one blob of a batch and reports how it went.
func (c *casService) updateOne(ctx context.Context, r *repb.BatchUpdateBlobsRequest_Request) *rpcstatus.Status {
	d, err := digest(r.GetDigest())
	if err != nil {
		return status.Convert(err).Proto()
	}

	data := r.GetData()
	switch r.GetCompressor() {
	case repb.Compressor_IDENTITY:
	case repb.Compressor_ZSTD:
		data, err = decodeAll(data)
		if err != nil {
			return status.Newf(codes.InvalidArgument, "plaid-cache: blob %s does not decompress: %v", d, err).Proto()
		}
	default:
		return status.Newf(codes.InvalidArgument,
			"plaid-cache: compressor %s is not supported for batch updates", r.GetCompressor()).Proto()
	}

	// The declared size is checked before the content hash so that a client
	// that got the length wrong is told which of the two it got wrong.
	if want := r.GetDigest().GetSizeBytes(); int64(len(data)) != want {
		return status.Newf(codes.InvalidArgument,
			"plaid-cache: blob %s is %d bytes, not the %d it declares", d, len(data), want).Proto()
	}

	if err := c.srv.store.Put(ctx, bazel.KindCAS, d, bytes.NewReader(data)); err != nil {
		if errors.Is(err, bazel.ErrDigestMismatch) {
			return status.Newf(codes.InvalidArgument, "plaid-cache: blob does not hash to %s", d).Proto()
		}
		// A store that did not happen costs a future miss and nothing else, so
		// the client is told it succeeded rather than made to resend.
		c.srv.logf("bazel grpc: batch put %s: %v", d, err)
	}
	return nil
}

// BatchReadBlobs returns several small blobs in one call.
//
// The batch is refused outright when the client asks for more than the total it
// was told a batch may carry, rather than truncated: a client that sized its
// batch against the advertised limit never sees this, and one that ignored the
// limit is better told so than handed a short answer it has to notice.
func (c *casService) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	if err := checkInstance(req.GetInstanceName()); err != nil {
		return nil, err
	}
	if err := c.srv.checkDigestFunction(req.GetDigestFunction()); err != nil {
		return nil, err
	}

	var total int64
	for _, bd := range req.GetDigests() {
		total += bd.GetSizeBytes()
	}
	if total > maxBatchTotalSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument,
			"plaid-cache: batch of %d bytes exceeds the %d this cache serves in one call",
			total, int64(maxBatchTotalSizeBytes))
	}

	compressor := repb.Compressor_IDENTITY
	for _, a := range req.GetAcceptableCompressors() {
		if a == repb.Compressor_ZSTD {
			compressor = repb.Compressor_ZSTD
			break
		}
	}

	resp := &repb.BatchReadBlobsResponse{
		Responses: make([]*repb.BatchReadBlobsResponse_Response, 0, len(req.GetDigests())),
	}
	for _, bd := range req.GetDigests() {
		resp.Responses = append(resp.Responses, c.readOne(ctx, bd, compressor))
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
	return resp, nil
}

// readOne returns one blob of a batch.
func (c *casService) readOne(ctx context.Context, bd *repb.Digest, compressor repb.Compressor_Value) *repb.BatchReadBlobsResponse_Response {
	out := &repb.BatchReadBlobsResponse_Response{Digest: bd}

	d, err := digest(bd)
	if err != nil {
		out.Status = status.Convert(err).Proto()
		return out
	}
	if d == emptyDigest {
		out.Data = nil
		return out
	}

	f, size, ok := c.srv.store.Open(ctx, bazel.KindCAS, d)
	if !ok {
		// A miss is per-blob and expected: it is how a client learns that
		// something an action result referenced has since been evicted, which
		// it recovers from by re-running the action.
		out.Status = status.Newf(codes.NotFound, "plaid-cache: no blob %s", d).Proto()
		return out
	}
	defer f.Close()

	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		c.srv.logf("bazel grpc: batch read %s: %v", d, err)
		out.Status = status.Newf(codes.NotFound, "plaid-cache: no blob %s", d).Proto()
		return out
	}

	if compressor == repb.Compressor_ZSTD {
		encoded, err := encodeAll(data)
		if err != nil {
			// Serving it plain is always allowed; the compressor is a hint.
			c.srv.logf("bazel grpc: compress %s: %v", d, err)
		} else {
			out.Compressor = repb.Compressor_ZSTD
			data = encoded
		}
	}
	out.Data = data
	return out
}

// GetTree walks a Directory tree already stored in the CAS.
//
// It is not served. Expanding a tree means parsing the Directory messages
// inside blobs this cache treats as opaque, on behalf of a client that has
// already been handed the root and can walk it with the blob reads it is
// making anyway. A cache client never calls it; an execution client does, and
// this is not an execution service.
func (c *casService) GetTree(_ *repb.GetTreeRequest, _ repb.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented,
		"plaid-cache: GetTree is not served; this is a cache, not an execution service")
}
