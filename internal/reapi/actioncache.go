// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"bytes"
	"context"
	"io"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// actionCacheService maps action digests to the results of running them.
//
// The stored body is the serialised ActionResult and nothing else, which is
// what makes an entry written here readable over the HTTP transport and an
// entry written there readable here. The two transports are two ways to reach
// one cache, not two caches.
type actionCacheService struct {
	repb.UnimplementedActionCacheServer
	srv *Server
}

// GetActionResult returns what an action produced last time it ran.
//
// The inlining fields are not honoured. A client that asked for stdout inline
// gets an ActionResult that names it in the CAS instead, which is a response
// the API allows and every client already handles, and which keeps this from
// reading blobs nobody may want.
func (a *actionCacheService) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	if err := a.srv.checkDigestFunction(req.GetDigestFunction()); err != nil {
		return nil, err
	}
	d, err := digest(req.GetActionDigest())
	if err != nil {
		return nil, err
	}

	f, size, ok := a.srv.store.Open(ctx, bazel.KindAC, d)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plaid-cache: no action result for %s", d)
	}
	defer func() { _ = f.Close() }()

	if size > maxActionResultBytes {
		// The same keyspace is writable over HTTP, where a body is opaque bytes
		// of any length. Parsing is the one place a whole body is held in
		// memory, so it is the one place that declines.
		a.srv.logf("bazel grpc: action result %s is %d bytes, refusing to parse", d, size)
		return nil, status.Errorf(codes.NotFound, "plaid-cache: no action result for %s", d)
	}

	body, err := io.ReadAll(io.LimitReader(f, maxActionResultBytes))
	if err != nil {
		a.srv.logf("bazel grpc: read action result %s: %v", d, err)
		return nil, status.Errorf(codes.NotFound, "plaid-cache: no action result for %s", d)
	}

	var res repb.ActionResult
	if err := proto.Unmarshal(body, &res); err != nil {
		// Something is stored under this action that is not an ActionResult. A
		// miss lets the client re-run the action and overwrite it, which is the
		// only repair available and happens to be the right one.
		a.srv.logf("bazel grpc: action result %s does not parse: %v", d, err)
		return nil, status.Errorf(codes.NotFound, "plaid-cache: no action result for %s", d)
	}
	return &res, nil
}

// UpdateActionResult records what an action produced.
//
// The message is re-serialised rather than stored as it arrived, because gRPC
// hands over a parsed message and not the bytes behind it. Both transports
// therefore store a valid encoding of the same message, which is all either
// needs of the other.
func (a *actionCacheService) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	if err := a.srv.checkDigestFunction(req.GetDigestFunction()); err != nil {
		return nil, err
	}
	d, err := digest(req.GetActionDigest())
	if err != nil {
		return nil, err
	}
	if req.GetActionResult() == nil {
		return nil, status.Error(codes.InvalidArgument, "plaid-cache: missing action result")
	}

	body, err := proto.Marshal(req.GetActionResult())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plaid-cache: action result does not encode: %v", err)
	}

	if err := a.srv.store.Put(ctx, bazel.KindAC, d, bytes.NewReader(body)); err != nil {
		// A store that did not happen costs a future miss. Reporting it would
		// cost the build, which a cache must never do.
		a.srv.logf("bazel grpc: put action result %s: %v", d, err)
	}
	return req.GetActionResult(), nil
}
