// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reapi

import (
	"strconv"
	"strings"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/plaid-cache/internal/bazel"
)

// resource is a decoded ByteStream resource name.
//
// The two directions share a grammar with an optional prefix and two optional
// segments, and the pieces that vary — whether the bytes on the wire are
// compressed, and which digest function named them — change what the handler
// must do rather than only what it must parse. Decoding once, into a value that
// says what those pieces were, keeps that decision in one place.
type resource struct {
	// name is the resource name exactly as the client sent it. A resumed write
	// is matched by this string, so it must survive parsing unchanged.
	name string

	digest bazel.Digest

	// size is the length of the blob once uncompressed, as the client declared
	// it. The store does not record a declared length, so this is what a read
	// is checked against and what a completed write must have received.
	size int64

	// compressor is how the bytes travel, not how they are stored. Everything
	// is stored uncompressed, so one body serves a compressed reader, an
	// uncompressed reader and a Go build alike.
	compressor repb.Compressor_Value

	// digestFunc is the digest-function segment, empty when absent. The API
	// requires it to be omitted for SHA-256, so a present one always names
	// something this server does not compute.
	digestFunc string
}

// resourceKeywords are the segments a resource name reserves. A client's
// instance name may not equal any of them, which is what makes the boundary
// between an instance name of unknown length and the rest of the name findable.
var resourceKeywords = map[string]bool{
	"blobs":            true,
	"uploads":          true,
	"actions":          true,
	"actionResults":    true,
	"operations":       true,
	"capabilities":     true,
	"compressed-blobs": true,
}

// parseReadResource decodes `{instance}/blobs/{df/}{hash}/{size}` and its
// compressed form.
func parseReadResource(name string) (resource, error) {
	segs, err := trimInstance(name)
	if err != nil {
		return resource{}, err
	}
	res, rest, err := parseBlobRef(name, segs)
	if err != nil {
		return resource{}, err
	}
	// A read name ends at the size. Only an upload carries trailing metadata,
	// and accepting it here would make two different names read one blob.
	if len(rest) != 0 {
		return resource{}, status.Errorf(codes.InvalidArgument,
			"plaid-cache: read resource %q has trailing segments", name)
	}
	return res, nil
}

// parseWriteResource decodes
// `{instance}/uploads/{uuid}/blobs/{df/}{hash}/{size}{/metadata}` and its
// compressed form.
func parseWriteResource(name string) (resource, error) {
	segs, err := trimInstance(name)
	if err != nil {
		return resource{}, err
	}
	if len(segs) < 2 || segs[0] != "uploads" {
		return resource{}, status.Errorf(codes.InvalidArgument,
			"plaid-cache: write resource %q is not uploads/{uuid}/blobs/{hash}/{size}", name)
	}
	if segs[1] == "" {
		return resource{}, status.Errorf(codes.InvalidArgument,
			"plaid-cache: write resource %q has an empty upload id", name)
	}
	// Trailing metadata after the size is the client's to choose and the
	// server's to ignore, so it is not inspected here. It stays part of
	// resource.name, which is how a resumed write finds its way back.
	res, _, err := parseBlobRef(name, segs[2:])
	return res, err
}

// trimInstance strips the instance-name prefix, which this server serves only
// as the empty one, and returns the remaining segments.
func trimInstance(name string) ([]string, error) {
	segs := strings.Split(name, "/")
	for i, s := range segs {
		if resourceKeywords[s] {
			if i != 0 {
				return nil, status.Errorf(codes.InvalidArgument,
					"plaid-cache: resource %q names instance %q; this cache serves a single unnamed instance",
					name, strings.Join(segs[:i], "/"))
			}
			return segs, nil
		}
	}
	return nil, status.Errorf(codes.InvalidArgument,
		"plaid-cache: resource %q names neither blobs nor an upload", name)
}

// parseBlobRef decodes the `blobs/{df/}{hash}/{size}` tail shared by both
// directions, returning whatever segments follow it.
func parseBlobRef(name string, segs []string) (resource, []string, error) {
	res := resource{name: name, compressor: repb.Compressor_IDENTITY}

	switch {
	case len(segs) == 0:
		return resource{}, nil, malformed(name)
	case segs[0] == "blobs":
		segs = segs[1:]
	case segs[0] == "compressed-blobs":
		if len(segs) < 2 {
			return resource{}, nil, malformed(name)
		}
		c, ok := compressorByName[segs[1]]
		if !ok {
			return resource{}, nil, status.Errorf(codes.InvalidArgument,
				"plaid-cache: compressor %q is not supported; this cache supports identity and zstd", segs[1])
		}
		res.compressor = c
		segs = segs[2:]
	default:
		return resource{}, nil, malformed(name)
	}

	if len(segs) < 2 {
		return resource{}, nil, malformed(name)
	}
	// The digest-function segment is optional and unlabelled, so it is
	// recognised by what it is not: anything that does not parse as a digest
	// sits where a digest function would.
	if d, err := bazel.ParseDigest(segs[0]); err == nil {
		res.digest = d
	} else {
		if len(segs) < 3 {
			return resource{}, nil, malformed(name)
		}
		res.digestFunc = segs[0]
		segs = segs[1:]
		d, err := bazel.ParseDigest(segs[0])
		if err != nil {
			return resource{}, nil, malformed(name)
		}
		res.digest = d
	}

	size, err := strconv.ParseInt(segs[1], 10, 64)
	if err != nil || size < 0 {
		return resource{}, nil, status.Errorf(codes.InvalidArgument,
			"plaid-cache: resource %q has no valid size", name)
	}
	res.size = size
	return res, segs[2:], nil
}

// malformed reports a resource name that does not fit the grammar at all.
func malformed(name string) error {
	return status.Errorf(codes.InvalidArgument,
		"plaid-cache: resource %q is not blobs/{hash}/{size} or compressed-blobs/{compressor}/{hash}/{size}", name)
}

// compressorByName maps the lowercase enum spelling a resource name uses onto
// the enum. Identity is absent deliberately: `compressed-blobs/identity` names
// compressed bytes that are not compressed, which is a client bug worth
// reporting rather than accommodating.
var compressorByName = map[string]repb.Compressor_Value{
	"zstd": repb.Compressor_ZSTD,
}

// checkResource rejects a resource this server cannot serve, for the reasons it
// cannot serve it.
func (s *Server) checkResource(res resource) error {
	// The API requires the digest-function segment to be omitted for SHA-256,
	// so a present one always names something else. It is tolerated only under
	// the same setting that tolerates it on an RPC field: verification off,
	// meaning the operator has decided to trust its clients' addressing.
	if res.digestFunc != "" && s.verify {
		return status.Errorf(codes.InvalidArgument,
			"plaid-cache: digest function %q is not served; this cache serves sha256", res.digestFunc)
	}
	return nil
}
