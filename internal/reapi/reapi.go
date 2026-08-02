// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package reapi serves the cache half of Bazel's Remote Execution API over
// gRPC, on top of the same storage adapter the HTTP transport uses.
//
// It is a transport and nothing else. Every byte it stores or serves goes
// through [bazel.Store], so a build that talks gRPC lands in the same index,
// under the same two namespaced keyspaces, sharing the same bodies, byte budget
// and eviction as a build that talks HTTP or a Go toolchain talking GOCACHEPROG.
// Nothing here decides where anything lives.
//
// # Why a second transport at all
//
// The two protocols are not equivalent, and the difference is not stylistic.
//
// An HTTP remote cache has no way to ask what the server already holds. Bazel's
// client for it answers its own findMissingDigests call by reporting every
// digest as absent, because the protocol gives it nothing better to say, and
// then uploads accordingly. Every action that re-runs re-uploads outputs the
// server already has, and build outputs are routinely hundreds of megabytes.
// FindMissingBlobs is a real question with a real answer here, and answering it
// honestly is the whole reason this package exists. Batched reads and writes,
// resumable uploads and per-blob status codes come along with it.
//
// # What is not served
//
// This is a cache, not an execution service. Capabilities advertise no
// execution capabilities at all, so a client pointed here with --remote_executor
// is told plainly rather than discovering it one failed call at a time.
//
// GetTree is likewise unimplemented. It walks a Directory tree already stored
// in the CAS, which is an execution-side convenience: a client caching against
// this server fetches the Tree blob it was handed and walks it itself.
//
// # Instance names
//
// A resource name may carry an instance name, and this server accepts only the
// empty one. Serving several instance names out of one keyspace would let two
// logically separate caches silently share entries, which is the same
// cache-poisoning shape the HTTP transport rejects a path prefix for.
package reapi

import (
	"crypto/sha256"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/bazel"
	"github.com/conductorone/plaid-cache/internal/cache"
)

// Protocol limits.
//
// maxBatchTotalSizeBytes is advertised in Capabilities, and is what a client
// sizes its batches against. maxMessageBytes is the gRPC frame limit, and is
// larger on purpose: a batch that exactly fills the advertised budget still
// carries digests, per-blob status messages and protobuf framing, and a client
// obeying the limit we published must not then be rejected by a limit we did
// not.
const (
	maxBatchTotalSizeBytes = 4 << 20
	maxMessageBytes        = 16 << 20

	// readChunkSize is how much of a blob one Read response carries. It is far
	// below the frame limit because the frame is a buffer on both sides, and a
	// build streaming several large outputs at once pays for it per stream.
	readChunkSize = 1 << 20

	// maxActionResultBytes bounds what will be parsed as an ActionResult. The
	// bodies in that keyspace are written by cache clients and are kilobytes,
	// but the same keyspace is reachable over HTTP, where a body is opaque
	// bytes of any length. Parsing is the one place this package holds a whole
	// body in memory, so it is the one place that needs a ceiling.
	maxActionResultBytes = 16 << 20
)

// emptyDigest is the SHA-256 of no bytes.
//
// The API requires servers to behave as though the empty blob is always
// present: clients are entitled to skip uploading it, and several do, so a
// server that reported it missing would be asking for an upload that never
// comes and then failing the read that follows.
var emptyDigest = bazel.Digest(sha256.Sum256(nil))

// Server carries what every service handler needs. The handlers themselves are
// separate types because the generated code embeds a distinct forward-compatible
// base in each, and one struct cannot embed four of them.
type Server struct {
	store   *bazel.Store
	uploads *uploads
	logf    cache.Logf

	// verify mirrors the store's digest checking. It is read here to decide
	// whether a client may name a digest function this server cannot check,
	// which is the one protocol-level consequence of turning verification off.
	verify bool
}

// Params carries the server's dependencies.
type Params struct {
	Store *bazel.Store
	Logf  cache.Logf

	// Verify reports whether the store checks that an uploaded CAS body hashes
	// to the digest naming it. It must match what the store was built with.
	Verify bool
}

// New constructs a Server.
func New(p Params) *Server {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Server{store: p.Store, uploads: newUploads(logf), logf: logf, verify: p.Verify}
}

// Register attaches every service this package implements to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	repb.RegisterCapabilitiesServer(g, &capabilitiesService{srv: s})
	repb.RegisterActionCacheServer(g, &actionCacheService{srv: s})
	repb.RegisterContentAddressableStorageServer(g, &casService{srv: s})
	bytestream.RegisterByteStreamServer(g, &byteStreamService{srv: s})
}

// SweepUploads discards partially received uploads that have been idle too
// long, and reports how many it discarded.
//
// A broken Write leaves a staging file open so the client can resume into it,
// and a client that never comes back would otherwise leave it open for the life
// of the daemon. Calling this periodically is what bounds that.
func (s *Server) SweepUploads() int { return s.uploads.sweep(time.Now()) }

// DiscardUploads releases every partially received upload, and reports how many
// there were. It is what a stopping server owes the staging directory.
func (s *Server) DiscardUploads() int { return s.uploads.discardAll() }

// UploadSweepInterval is how often [Server.SweepUploads] should be called. It
// is a fraction of the idle timeout so an abandoned upload is released within
// roughly that timeout rather than an interval later.
const UploadSweepInterval = time.Minute

// Keepalive and message-size settings for a gRPC server hosting this package.
//
// The transfers here are large and slow by design, so the settings that matter
// are the ones that do *not* impose a deadline. There is deliberately no
// MaxConnectionAge: it would tear down a connection mid-transfer at an age that
// has nothing to do with whether the transfer is making progress, and a blob of
// several hundred megabytes over a slow link is exactly the case worth caching.
//
// What is bounded is idleness. MaxConnectionIdle closes a connection with no
// active stream, which reclaims a client that has gone away without touching
// one that is merely slow.
//
// The enforcement policy is deliberately permissive. A gRPC server's default
// rejects client pings more often than every five minutes, and closes the
// connection of a client that pings with no active stream — both of which a
// client configured for keepalive on a long transfer will do. Punishing a
// client for checking whether the server is still there is the opposite of what
// this server wants: it is the client's own liveness signal that lets it tell a
// stalled transfer from a slow one without guessing at a timeout.
//
// None of which the client is obliged to take advantage of. Bazel applies
// --remote_timeout to its gRPC stubs, ByteStream included, as a deadline on the
// whole call rather than on a gap within it, so a large blob fails outright
// unless that flag exceeds the time the transfer honestly needs. Nothing here
// can fix that; what it can do is not add a second deadline of its own, which is
// why there is none.
const (
	serverKeepaliveIdle    = 10 * time.Minute
	serverKeepalivePing    = 2 * time.Hour
	serverKeepaliveTimeout = 20 * time.Second
	clientKeepaliveMinTime = 10 * time.Second
)

// ServerOptions returns the gRPC server options this package expects.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: serverKeepaliveIdle,
			Time:              serverKeepalivePing,
			Timeout:           serverKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             clientKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
	}
}

// digest converts a protocol Digest into the fixed-width identifier the store
// is keyed by.
func digest(d *repb.Digest) (bazel.Digest, error) {
	if d == nil {
		return bazel.Digest{}, status.Error(codes.InvalidArgument, "plaid-cache: missing digest")
	}
	parsed, err := bazel.ParseDigest(d.GetHash())
	if err != nil {
		return bazel.Digest{}, status.Errorf(codes.InvalidArgument, "plaid-cache: %v", err)
	}
	if d.GetSizeBytes() < 0 {
		return bazel.Digest{}, status.Errorf(codes.InvalidArgument, "plaid-cache: digest size %d is negative", d.GetSizeBytes())
	}
	return parsed, nil
}

// checkDigestFunction reports whether a client's named digest function is one
// this server can serve.
//
// Only SHA-256 is advertised, and only SHA-256 produces a body this server can
// check against the address it was published under. A client naming another
// function is refused unless verification is off, which is the setting that
// says the operator has decided to trust its clients' addressing — the same
// decision, and the same escape hatch, as on the HTTP transport.
func (s *Server) checkDigestFunction(v repb.DigestFunction_Value) error {
	switch v {
	case repb.DigestFunction_UNKNOWN, repb.DigestFunction_SHA256:
		return nil
	}
	if !s.verify {
		return nil
	}
	return status.Errorf(codes.InvalidArgument,
		"plaid-cache: digest function %s is not served; this cache serves sha256", v)
}
