// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rpcMethods are the calls this server answers, in the order they are
// reported. Fixing the set up front keeps the label cardinality bounded by
// construction and lets the hot path index a table instead of locking a map.
var rpcMethods = [...]string{
	"/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
	"/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult",
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree",
	"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
	"/google.bytestream.ByteStream/Read",
	"/google.bytestream.ByteStream/Write",
	"/google.bytestream.ByteStream/QueryWriteStatus",
}

// rpcBuckets are the upper bounds, in seconds, of the call-duration
// histogram. They start well below a millisecond because a presence probe or
// a small batch answered from memory finishes there, and the shape of that
// fast end is what says whether the daemon is keeping up; they run to half a
// minute because a ByteStream transfer of a large output legitimately does.
var rpcBuckets = [...]float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

// RPCBuckets returns the histogram's bucket upper bounds, in seconds.
func RPCBuckets() []float64 { return rpcBuckets[:] }

// rpcOutcomes are how a call can end, as reported. not_found is separate from
// error because it is the normal answer to an action-cache or blob lookup that
// misses, and folding it into error would make that series useless.
var rpcOutcomes = [...]string{"ok", "not_found", "error"}

// rpcHistogram is one method's duration distribution for one outcome.
type rpcHistogram struct {
	buckets [len(rpcBuckets)]atomic.Int64
	count   atomic.Int64
	sumNs   atomic.Int64
}

// rpcMetrics is what the gRPC surface has been asked to do, process-local.
//
// It exists to answer the questions the persisted counters cannot: which
// calls the load is made of, how long each takes, how large the presence
// checks are, and how many bytes cross the listener in each direction.
type rpcMetrics struct {
	calls [len(rpcMethods)][len(rpcOutcomes)]rpcHistogram

	digestsPresent atomic.Int64
	digestsMissing atomic.Int64
	bytesRead      atomic.Int64
	bytesWritten   atomic.Int64
}

// methodIndex returns a method's row, or -1 for one this server does not
// count.
func methodIndex(fullMethod string) int {
	for i, m := range rpcMethods {
		if m == fullMethod {
			return i
		}
	}
	return -1
}

// outcomeIndex classifies a handler's error.
func outcomeIndex(err error) int {
	switch status.Code(err) {
	case codes.OK:
		return 0
	case codes.NotFound:
		return 1
	default:
		return 2
	}
}

// observe records one finished call.
func (m *rpcMetrics) observe(fullMethod string, d time.Duration, err error) {
	i := methodIndex(fullMethod)
	if i < 0 {
		return
	}
	h := &m.calls[i][outcomeIndex(err)]
	s := d.Seconds()
	for b, le := range rpcBuckets {
		if s <= le {
			h.buckets[b].Add(1)
			break
		}
	}
	h.count.Add(1)
	h.sumNs.Add(int64(d))
}

// RPCMethodSnapshot is one method's distribution for one outcome. Buckets are
// per-bucket counts, not cumulative; Count includes calls past the last bound.
type RPCMethodSnapshot struct {
	Method     string  `json:"method"`
	Outcome    string  `json:"outcome"`
	Buckets    []int64 `json:"buckets"`
	Count      int64   `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`
}

// RPCMetricsSnapshot is a copy of rpcMetrics for reporting.
type RPCMetricsSnapshot struct {
	Calls []RPCMethodSnapshot `json:"calls,omitempty"`

	// DigestsPresent and DigestsMissing are FindMissingBlobs answers per
	// digest. Their sum over the FindMissingBlobs call count is the average
	// batch size, which nothing else records.
	DigestsPresent int64 `json:"digests_present"`
	DigestsMissing int64 `json:"digests_missing"`

	// BytesRead and BytesWritten are blob bytes served to and received from
	// clients, uncompressed, across batch and streaming calls.
	BytesRead    int64 `json:"bytes_read"`
	BytesWritten int64 `json:"bytes_written"`
}

// snapshot copies the counters. Methods never called are left out.
func (m *rpcMetrics) snapshot() RPCMetricsSnapshot {
	out := RPCMetricsSnapshot{
		DigestsPresent: m.digestsPresent.Load(),
		DigestsMissing: m.digestsMissing.Load(),
		BytesRead:      m.bytesRead.Load(),
		BytesWritten:   m.bytesWritten.Load(),
	}
	for i := range m.calls {
		for o := range m.calls[i] {
			h := &m.calls[i][o]
			n := h.count.Load()
			if n == 0 {
				continue
			}
			s := RPCMethodSnapshot{
				Method:     shortMethod(rpcMethods[i]),
				Outcome:    rpcOutcomes[o],
				Buckets:    make([]int64, len(rpcBuckets)),
				Count:      n,
				SumSeconds: time.Duration(h.sumNs.Load()).Seconds(),
			}
			for b := range h.buckets {
				s.Buckets[b] = h.buckets[b].Load()
			}
			out.Calls = append(out.Calls, s)
		}
	}
	return out
}

// shortMethod trims a full method name to its final element: the services
// do not share any method name, so the last element alone is unambiguous and
// is what someone reading a dashboard expects.
func shortMethod(full string) string {
	for i := len(full) - 1; i >= 0; i-- {
		if full[i] == '/' {
			return full[i+1:]
		}
	}
	return full
}

// RPCMetrics returns what the gRPC surface has done since this server started.
func (s *Server) RPCMetrics() RPCMetricsSnapshot { return s.rpc.snapshot() }

// Interceptors returns the server options that time every call into the
// RPC metrics. They are separate from ServerOptions because they belong to
// one Server's counters rather than to the transport's configuration.
func (s *Server) Interceptors() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			start := time.Now()
			resp, err := handler(ctx, req)
			s.rpc.observe(info.FullMethod, time.Since(start), err)
			return resp, err
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			start := time.Now()
			err := handler(srv, ss)
			s.rpc.observe(info.FullMethod, time.Since(start), err)
			return err
		}),
	}
}
