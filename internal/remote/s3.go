// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// S3 is a Backend stored in an S3 bucket.
//
// It works with both general-purpose buckets and S3 Express One Zone
// directory buckets, whose names end in "--x-s3". For a directory bucket the
// SDK negotiates and refreshes a session automatically, which is what makes
// the single-digit-millisecond latency available; the calling IAM principal
// needs s3express:CreateSession in addition to the usual object permissions.
type S3 struct {
	client *s3.Client
	bucket string
	keys   keyspace

	// stats is this backend's transport accounting: how often a request was
	// given a connection that was already open, and how long each operation
	// took. It is per backend rather than per package, because the pool it
	// describes belongs to the client constructed below.
	stats *stats
}

// S3Params configures an S3 backend. Bucket is required; every other field
// falls back to the AWS SDK's own resolution chain, and none of them has a
// value baked into this program.
type S3Params struct {
	Bucket      string
	Region      string
	Prefix      string
	EndpointURL string
}

// NewS3 constructs an S3 backend.
func NewS3(ctx context.Context, p S3Params) (*S3, error) {
	if p.Bucket == "" {
		return nil, errors.New("NewS3: bucket is required")
	}

	var opts []func(*awsconfig.LoadOptions) error
	if p.Region != "" {
		opts = append(opts, awsconfig.WithRegion(p.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("NewS3: load aws config: %w", err)
	}

	var s3opts []func(*s3.Options)
	if p.EndpointURL != "" {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(p.EndpointURL)
			// A custom endpoint is nearly always a local S3 implementation,
			// which addresses buckets by path rather than by virtual host.
			o.UsePathStyle = true
		})
	}

	st := &stats{}
	s3opts = append(s3opts, func(o *s3.Options) {
		// Wrap the client the SDK already resolved rather than installing one of
		// our own. By the time an option function runs, o.HTTPClient is the
		// client this call would otherwise have used, pool settings and all, so
		// wrapping it cannot change any of them — which is the point. Nothing
		// here tunes the pool; this exists to find out whether it needs tuning.
		o.HTTPClient = newTracingClient(o.HTTPClient, st)
	})

	return &S3{
		client: s3.NewFromConfig(cfg, s3opts...),
		bucket: p.Bucket,
		keys:   keyspace{prefix: p.Prefix},
		stats:  st,
	}, nil
}

// Stats reports what this backend's transport has done. It satisfies Statser.
func (s *S3) Stats() StatsSnapshot { return s.stats.Snapshot() }

// GetAction resolves an action to the output it produced.
func (s *S3) GetAction(ctx context.Context, a ids.ActionID) (ids.OutputID, time.Time, error) {
	start := time.Now()
	o, mtime, err := s.getAction(ctx, a)
	s.stats.observe(opGetAction, err, time.Since(start))
	return o, mtime, err
}

// getAction is GetAction without the timing, so that there is one place the
// duration is recorded rather than one per return path.
func (s *S3) getAction(ctx context.Context, a ids.ActionID) (ids.OutputID, time.Time, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.keys.actionKey(a)),
	})
	if err != nil {
		if isNotFound(err) {
			return ids.OutputID{}, time.Time{}, ErrMiss
		}
		return ids.OutputID{}, time.Time{}, fmt.Errorf("GetAction: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	// An action record is two short fields; cap the read so a corrupt or
	// hostile object cannot be streamed into memory without bound.
	body, err := io.ReadAll(io.LimitReader(out.Body, 256))
	if err != nil {
		return ids.OutputID{}, time.Time{}, fmt.Errorf("GetAction: read: %w", err)
	}
	o, mtime, err := parseActionRecord(body)
	if err != nil {
		// A malformed record is a corrupt cache entry, not a transport
		// failure. Reporting a miss lets the build recompute and overwrite it.
		return ids.OutputID{}, time.Time{}, ErrMiss
	}
	return o, mtime, nil
}

// PutAction records that an action produced an output.
func (s *S3) PutAction(ctx context.Context, a ids.ActionID, o ids.OutputID, mtime time.Time) error {
	start := time.Now()
	err := s.putAction(ctx, a, o, mtime)
	s.stats.observe(opPutAction, err, time.Since(start))
	return err
}

// putAction is PutAction without the timing.
func (s *S3) putAction(ctx context.Context, a ids.ActionID, o ids.OutputID, mtime time.Time) error {
	body := formatActionRecord(o, mtime)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.keys.actionKey(a)),
		Body:          strings.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return fmt.Errorf("PutAction: %w", err)
	}
	return nil
}

// GetObject opens a body from the bucket.
func (s *S3) GetObject(ctx context.Context, o ids.OutputID) (io.ReadCloser, int64, error) {
	start := time.Now()
	body, size, err := s.getObject(ctx, o)
	// The measured span ends when the body is open, not when it has been read:
	// the reader is handed to the caller, and the time it spends draining it is
	// the caller's transfer rather than this tier's round trip. So a get here is
	// first-byte latency, which is the number the connection counters beside it
	// are about; a put below is the whole transfer, because this package does
	// stream that one itself.
	s.stats.observe(opGetObject, err, time.Since(start))
	return body, size, err
}

// getObject is GetObject without the timing.
func (s *S3) getObject(ctx context.Context, o ids.OutputID) (io.ReadCloser, int64, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.keys.objectKey(o)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, ErrMiss
		}
		return nil, 0, fmt.Errorf("GetObject: %w", err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, size, nil
}

// PutObject stores a body, skipping the upload if the key already holds one.
//
// Bodies are content-addressed, so an existing key by definition holds
// equivalent bytes. The conditional write collapses the usual
// head-then-put into a single round trip, and a precondition failure is the
// success case rather than an error.
func (s *S3) PutObject(ctx context.Context, o ids.OutputID, r io.ReadSeeker, size int64) error {
	start := time.Now()
	err := s.putObject(ctx, o, r, size)
	// A conditional write that lost is recorded as a success, because that is
	// what it is: the bytes are in the bucket. It is a fast success, though — no
	// body was transferred — so it lands low in the distribution and drags the
	// put percentiles down on a fleet that keeps offering the same objects.
	s.stats.observe(opPutObject, err, time.Since(start))
	return err
}

// putObject is PutObject without the timing.
func (s *S3) putObject(ctx context.Context, o ids.OutputID, r io.ReadSeeker, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.keys.objectKey(o)),
		Body:          r,
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return nil
		}
		return fmt.Errorf("PutObject: %w", err)
	}
	return nil
}

// Close releases the backend. The SDK client holds no resources needing an
// explicit shutdown, so this exists only to satisfy Backend.
func (s *S3) Close() error { return nil }

// isNotFound reports whether err means the key is absent.
//
// The shape of a missing-key error varies by operation and by bucket type:
// GetObject returns a typed NoSuchKey, HeadObject returns an untyped 404, and
// directory buckets have been known to use other codes, so this checks the
// typed error, the error code, and the HTTP status.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return httpStatus(err) == http.StatusNotFound
}

// isPreconditionFailed reports whether err means the conditional write lost,
// i.e. the object already exists.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "PreconditionFailed" {
		return true
	}
	return httpStatus(err) == http.StatusPreconditionFailed
}

// httpStatus extracts the HTTP status code from a smithy transport error, or
// zero if err did not come from an HTTP response.
func httpStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() != 0 {
		return re.HTTPStatusCode()
	}
	return 0
}
