// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/ids"
)

// testBucket is the bucket every fake-server test addresses. With a custom
// endpoint the SDK uses path-style addressing, so it appears as the first path
// element of every request.
const testBucket = "b"

// recordedReq is one request as the fake S3 server saw it.
type recordedReq struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
}

// fakeS3 records the requests a test's handler received.
type fakeS3 struct {
	mu   sync.Mutex
	reqs []recordedReq
}

func (f *fakeS3) record(r *http.Request) recordedReq {
	body, _ := io.ReadAll(r.Body)
	got := recordedReq{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Body: body}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, got)
	return got
}

func (f *fakeS3) all() []recordedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedReq(nil), f.reqs...)
}

// newTestS3 starts a fake S3 server and returns a backend pointed at it, plus
// the request recorder. Credentials and the SDK's own configuration sources are
// pinned to hermetic values so signing succeeds without touching the host's AWS
// setup, and retries are disabled so one call is one request.
func newTestS3(t *testing.T, prefix string, h func(*fakeS3, http.ResponseWriter, *http.Request)) (*S3, *fakeS3) {
	t.Helper()

	f := &fakeS3{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h(f, w, r)
	}))
	t.Cleanup(srv.Close)

	missing := filepath.Join(t.TempDir(), "no-such-aws-file")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFAKEFAKEFAKEFAKE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fakefakefakefakefakefakefakefakefakefake")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")

	s, err := NewS3(context.Background(), S3Params{
		EndpointURL: srv.URL,
		Bucket:      testBucket,
		Region:      "us-west-2",
		Prefix:      prefix,
	})
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("S3.Close: %v", err)
		}
	})
	return s, f
}

// testCtx returns a context that fails the test rather than blocking forever if
// a call does not return.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// writeS3Error writes an S3-shaped XML error, the form the SDK's deserializers
// expect when mapping a status onto a typed error.
func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+
		code+`</Code><Message>`+code+`</Message></Error>`)
}

// TestNewS3RequiresBucket pins that a missing bucket is a construction error
// rather than a backend that signs requests against an empty bucket name.
func TestNewS3RequiresBucket(t *testing.T) {
	if s, err := NewS3(context.Background(), S3Params{Region: "us-west-2"}); err == nil {
		t.Fatalf("NewS3 with no bucket = %v, want error", s)
	}
}

// TestS3GetActionHappyPath pins that a stored action record resolves to its
// output id and original mtime, and that the request goes to the path-style
// prefixed key the keyspace computes.
func TestS3GetActionHappyPath(t *testing.T) {
	const prefix = "team/go"
	action := mkAction(0xab)
	wantOut := mkOutput(0x5a)
	wantNanos := int64(1_700_000_000_123_456_789)
	wantPath := "/" + testBucket + "/" + keyspace{prefix: prefix}.actionKey(action)

	s, f := newTestS3(t, prefix, func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		body := wantOut.String() + " " + strconv.FormatInt(wantNanos, 10)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = io.WriteString(w, body)
	})

	gotOut, gotTime, err := s.GetAction(testCtx(t), action)
	if err != nil {
		t.Fatalf("GetAction: %v", err)
	}
	if gotOut != wantOut {
		t.Fatalf("output id = %s, want %s", gotOut, wantOut)
	}
	if got := gotTime.UnixNano(); got != wantNanos {
		t.Fatalf("mtime = %d ns, want %d ns", got, wantNanos)
	}

	reqs := f.all()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].Method != http.MethodGet {
		t.Fatalf("method = %s, want GET", reqs[0].Method)
	}
	if reqs[0].Path != wantPath {
		t.Fatalf("path = %q, want %q", reqs[0].Path, wantPath)
	}
}

// TestS3GetActionNotFoundIsMiss pins that an absent key is reported as a miss
// rather than a generic error, for both the typed NoSuchKey body and a bare 404
// from an S3 implementation that sends no error document.
func TestS3GetActionNotFoundIsMiss(t *testing.T) {
	cases := []struct {
		name string
		code string
	}{
		{"typed NoSuchKey", "NoSuchKey"},
		{"typed NotFound", "NotFound"},
		{"bare 404", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
				f.record(r)
				if c.code == "" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				writeS3Error(w, http.StatusNotFound, c.code)
			})

			_, _, err := s.GetAction(testCtx(t), mkAction(0xab))
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("GetAction error = %v, want fs.ErrNotExist", err)
			}
		})
	}
}

// TestS3GetActionMalformedBodyIsMiss pins that a corrupt action record is a
// miss, not a fault. The build then recomputes and overwrites the entry, which
// is the only way a poisoned key ever heals; returning an error would instead
// surface a bucket problem for something the cache can fix itself.
func TestS3GetActionMalformedBodyIsMiss(t *testing.T) {
	for _, body := range []string{"", "garbage", "deadbeef 12", mkOutput(0x01).String() + " later"} {
		t.Run(strconv.Quote(body), func(t *testing.T) {
			s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
				f.record(r)
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				_, _ = io.WriteString(w, body)
			})

			out, _, err := s.GetAction(testCtx(t), mkAction(0xab))
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("GetAction error = %v, want fs.ErrNotExist", err)
			}
			if out != (ids.OutputID{}) {
				t.Fatalf("GetAction returned output id %s alongside a miss", out)
			}
		})
	}
}

// TestS3GetActionCapsBodyRead pins the 256-byte read cap on an action record.
// The served object holds a valid record inside the first 256 bytes followed by
// a megabyte of junk: parsing succeeds only if the tail is never read, so an
// unbounded read would both stall the build and report a spurious miss.
func TestS3GetActionCapsBodyRead(t *testing.T) {
	wantOut := mkOutput(0x5a)
	wantNanos := int64(1_700_000_000_000_000_001)

	head := wantOut.String() + " " + strconv.FormatInt(wantNanos, 10)
	if len(head) > 256 {
		t.Fatalf("record is %d bytes, does not fit the read cap", len(head))
	}
	// Pad with spaces to exactly the cap so the truncated prefix still parses,
	// then append junk that would add a third field if it were read.
	body := head + strings.Repeat(" ", 256-len(head)) + strings.Repeat("x", 1<<20)

	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		// The client hangs up after the cap, so a short write is expected.
		_, _ = io.WriteString(w, body)
	})

	gotOut, gotTime, err := s.GetAction(testCtx(t), mkAction(0xab))
	if err != nil {
		t.Fatalf("GetAction: %v", err)
	}
	if gotOut != wantOut {
		t.Fatalf("output id = %s, want %s", gotOut, wantOut)
	}
	if got := gotTime.UnixNano(); got != wantNanos {
		t.Fatalf("mtime = %d ns, want %d ns", got, wantNanos)
	}
}

// TestS3GetActionServerErrorIsNotMiss pins that a transport or service failure
// stays distinguishable from absence. Both are treated as a miss by callers,
// but only the latter may be cached as "known absent".
func TestS3GetActionServerErrorIsNotMiss(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusForbidden} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
				f.record(r)
				writeS3Error(w, status, "InternalError")
			})

			_, _, err := s.GetAction(testCtx(t), mkAction(0xab))
			if err == nil {
				t.Fatalf("GetAction on HTTP %d = nil error, want error", status)
			}
			if errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("GetAction on HTTP %d reported a miss: %v", status, err)
			}
		})
	}
}

// TestS3GetObjectHappyPath pins that a body is returned verbatim, that its size
// comes from Content-Length rather than from reading it, and that the key is the
// output keyspace and not the action one.
func TestS3GetObjectHappyPath(t *testing.T) {
	const prefix = "cache/"
	out := mkOutput(0x0f)
	want := []byte("this is an object body")
	wantPath := "/" + testBucket + "/" + keyspace{prefix: prefix}.objectKey(out)

	s, f := newTestS3(t, prefix, func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.Header().Set("Content-Length", strconv.Itoa(len(want)))
		_, _ = w.Write(want)
	})

	rc, size, err := s.GetObject(testCtx(t), out)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	if size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", size, len(want))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}

	reqs := f.all()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].Path != wantPath {
		t.Fatalf("path = %q, want %q", reqs[0].Path, wantPath)
	}
}

// TestS3GetObjectNotFoundIsMiss pins that an absent body is a miss and that no
// reader is handed back for the caller to close.
func TestS3GetObjectNotFoundIsMiss(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
	})

	rc, size, err := s.GetObject(testCtx(t), mkOutput(0x0f))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetObject error = %v, want fs.ErrNotExist", err)
	}
	if rc != nil || size != 0 {
		t.Fatalf("GetObject = (%v, %d), want (nil, 0)", rc, size)
	}
}

// TestS3GetObjectServerErrorIsNotMiss pins that a service failure on a body
// read is a real error, keeping a bucket outage distinguishable from an
// evicted body.
func TestS3GetObjectServerErrorIsNotMiss(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		writeS3Error(w, http.StatusInternalServerError, "InternalError")
	})

	_, _, err := s.GetObject(testCtx(t), mkOutput(0x0f))
	if err == nil {
		t.Fatal("GetObject on HTTP 500 = nil error, want error")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetObject on HTTP 500 reported a miss: %v", err)
	}
}

// TestS3PutObjectIsConditional pins that a body upload carries If-None-Match:
// *. Bodies are content-addressed and immutable, so the conditional write is
// what collapses head-then-put into one round trip.
func TestS3PutObjectIsConditional(t *testing.T) {
	out := mkOutput(0x0f)
	body := "object body"
	wantPath := "/" + testBucket + "/" + keyspace{}.objectKey(out)

	s, f := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.WriteHeader(http.StatusOK)
	})

	if err := s.PutObject(testCtx(t), out, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	reqs := f.all()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].Method != http.MethodPut {
		t.Fatalf("method = %s, want PUT", reqs[0].Method)
	}
	if reqs[0].Path != wantPath {
		t.Fatalf("path = %q, want %q", reqs[0].Path, wantPath)
	}
	if got := reqs[0].Header.Get("If-None-Match"); got != "*" {
		t.Fatalf("If-None-Match = %q, want %q", got, "*")
	}
	if !bytes.Contains(reqs[0].Body, []byte(body)) {
		t.Fatalf("uploaded body = %q, want it to carry %q", reqs[0].Body, body)
	}
}

// TestS3PutObjectPreconditionFailedIsSuccess pins that losing the conditional
// write is the dedup path, not a failure: the key already holds bytes that, by
// content addressing, are equivalent.
func TestS3PutObjectPreconditionFailedIsSuccess(t *testing.T) {
	cases := []struct {
		name string
		code string
	}{
		{"coded PreconditionFailed", "PreconditionFailed"},
		{"bare 412", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
				f.record(r)
				if c.code == "" {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
				writeS3Error(w, http.StatusPreconditionFailed, c.code)
			})

			body := "object body"
			err := s.PutObject(testCtx(t), mkOutput(0x0f), strings.NewReader(body), int64(len(body)))
			if err != nil {
				t.Fatalf("PutObject on HTTP 412 = %v, want nil", err)
			}
		})
	}
}

// TestS3PutObjectServerErrorFails pins that a genuine upload failure is
// reported. Uploads run in the background, so this error is what the caller
// logs or counts; swallowing it would make a write-only bucket look healthy.
func TestS3PutObjectServerErrorFails(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		writeS3Error(w, http.StatusInternalServerError, "InternalError")
	})

	body := "object body"
	if err := s.PutObject(testCtx(t), mkOutput(0x0f), strings.NewReader(body), int64(len(body))); err == nil {
		t.Fatal("PutObject on HTTP 500 = nil error, want error")
	}
}

// TestS3PutActionOverwrites pins that an action record is written
// unconditionally, with no If-None-Match. An action legitimately maps to a new
// output when the toolchain changes, so a conditional write would pin the
// mapping to the first result forever.
func TestS3PutActionOverwrites(t *testing.T) {
	action := mkAction(0xab)
	out := mkOutput(0x5a)
	mtime := time.Unix(0, 1_700_000_000_123_456_789)
	wantPath := "/" + testBucket + "/" + keyspace{}.actionKey(action)

	s, f := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.WriteHeader(http.StatusOK)
	})

	if err := s.PutAction(testCtx(t), action, out, mtime); err != nil {
		t.Fatalf("PutAction: %v", err)
	}

	reqs := f.all()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].Method != http.MethodPut {
		t.Fatalf("method = %s, want PUT", reqs[0].Method)
	}
	if reqs[0].Path != wantPath {
		t.Fatalf("path = %q, want %q", reqs[0].Path, wantPath)
	}
	if got, ok := reqs[0].Header["If-None-Match"]; ok {
		t.Fatalf("PutAction sent If-None-Match: %q, want the header absent", got)
	}
	want := formatActionRecord(out, mtime)
	if !bytes.Contains(reqs[0].Body, []byte(want)) {
		t.Fatalf("uploaded record = %q, want it to carry %q", reqs[0].Body, want)
	}
}

// TestS3PutActionServerErrorFails pins that a failed record write is reported
// rather than silently dropped, since a missing record makes every future
// lookup of that action a miss.
func TestS3PutActionServerErrorFails(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		writeS3Error(w, http.StatusInternalServerError, "InternalError")
	})

	err := s.PutAction(testCtx(t), mkAction(0xab), mkOutput(0x5a), time.Unix(0, 1))
	if err == nil {
		t.Fatal("PutAction on HTTP 500 = nil error, want error")
	}
}

// TestS3CancelledContextIsNotMiss pins that a cancelled or timed-out call is a
// real error. A stalled network must not be recorded as absence, which would
// make the entry look permanently missing.
func TestS3CancelledContextIsNotMiss(t *testing.T) {
	s, _ := newTestS3(t, "", func(f *fakeS3, w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.GetAction(ctx, mkAction(0xab))
	if err == nil {
		t.Fatal("GetAction with a cancelled context = nil error, want error")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetAction with a cancelled context reported a miss: %v", err)
	}
}
