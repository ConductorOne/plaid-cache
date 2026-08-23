// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/cache"
	"github.com/conductorone/plaid-cache/internal/config"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
)

// newHandler builds a handler over a real index and body store with no remote
// tier, so the tests exercise the code that actually runs rather than a fake of
// it. Verification is on, matching the default, and the monitoring routes are
// off, also matching the default.
func newHandler(t testing.TB) (*Handler, *blob.Store) {
	st, _, blobs := newStore(t)
	return NewHandler(HandlerParams{Store: st, Logf: t.Logf}), blobs
}

// newMonitoredHandler builds a handler whose monitoring routes are served, from
// providers a test controls.
func newMonitoredHandler(t *testing.T, status StatusFunc, metrics MetricsFunc) *Handler {
	st, _, _ := newStore(t)
	return NewHandler(HandlerParams{Store: st, Logf: t.Logf, Status: status, Metrics: metrics})
}

// newStore builds the storage adapter the handler sits on, and hands back the
// cache underneath it so a test can assert on what was actually stored.
func newStore(t testing.TB) (*Store, *cache.Cache, *blob.Store) {
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

	return NewStore(StoreParams{Cache: c, Blobs: blobs, Verify: true, Logf: t.Logf}), c, blobs
}

// sha256Hex is the digest a CAS path carries for a body.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// do runs one request against the handler and returns the recorded response.
func do(t *testing.T, h *Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// wantStatus fails unless the response carries the expected status.
func wantStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("status = %d, want %d (body %q)", w.Code, want, w.Body.String())
	}
}

// TestCASRoundTrip pins the store-then-fetch path for a content-addressed blob,
// including the Content-Length Bazel refuses a response without.
func TestCASRoundTrip(t *testing.T) {
	h, _ := newHandler(t)
	body := []byte("a compiled object, as far as the cache is concerned")
	path := "/cas/" + sha256Hex(body)

	wantStatus(t, do(t, h, http.MethodPut, path, body), http.StatusOK)

	w := do(t, h, http.MethodGet, path, nil)
	wantStatus(t, w, http.StatusOK)
	if got := w.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if got, want := w.Header().Get("Content-Length"), strconv.Itoa(len(body)); got != want {
		t.Fatalf("Content-Length = %q, want %q", got, want)
	}
}

// TestACRoundTrip pins that an action-cache entry is stored under the action
// digest naming it rather than under a digest of its body, which is what makes
// an opaque ActionResult message storable without parsing it.
func TestACRoundTrip(t *testing.T) {
	h, _ := newHandler(t)
	// Deliberately not the digest of the body: an action-cache key names the
	// action, and the server never sees the inputs that produced it.
	path := "/ac/" + hex64
	body := []byte("\x08\x00 an ActionResult, byte for byte")

	wantStatus(t, do(t, h, http.MethodPut, path, body), http.StatusOK)

	w := do(t, h, http.MethodGet, path, nil)
	wantStatus(t, w, http.StatusOK)
	if got := w.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestGetMissIs404 pins the status Bazel reads as a miss. Anything else is an
// error to it, and an error fails the build the cache exists to speed up.
func TestGetMissIs404(t *testing.T) {
	h, _ := newHandler(t)
	for _, p := range []string{"/ac/" + hex64, "/cas/" + hex64} {
		wantStatus(t, do(t, h, http.MethodGet, p, nil), http.StatusNotFound)
	}
}

// TestKeyspacesStayApart is the namespacing test: one digest names an entry in
// both keyspaces at once, and each must keep its own body.
func TestKeyspacesStayApart(t *testing.T) {
	h, _ := newHandler(t)
	actionResult := []byte("the ActionResult for this action")
	blobBody := []byte("the Action message for this action")
	// The CAS path has to carry the blob's own digest to pass verification,
	// and the action-cache path is then given the same digest deliberately.
	d := sha256Hex(blobBody)

	wantStatus(t, do(t, h, http.MethodPut, "/cas/"+d, blobBody), http.StatusOK)
	wantStatus(t, do(t, h, http.MethodPut, "/ac/"+d, actionResult), http.StatusOK)

	if got := do(t, h, http.MethodGet, "/cas/"+d, nil).Body.Bytes(); !bytes.Equal(got, blobBody) {
		t.Fatalf("cas body = %q, want %q", got, blobBody)
	}
	if got := do(t, h, http.MethodGet, "/ac/"+d, nil).Body.Bytes(); !bytes.Equal(got, actionResult) {
		t.Fatalf("ac body = %q, want %q", got, actionResult)
	}
}

// TestPutRejectsMismatchedCASBody pins that a body which does not hash to the
// digest naming it is refused and not stored. Accepting it would publish
// arbitrary bytes under an address promising different ones — and, once the
// shared tier has it, on every other machine too.
func TestPutRejectsMismatchedCASBody(t *testing.T) {
	h, _ := newHandler(t)
	path := "/cas/" + hex64

	wantStatus(t, do(t, h, http.MethodPut, path, []byte("not what the digest says")), http.StatusBadRequest)
	wantStatus(t, do(t, h, http.MethodGet, path, nil), http.StatusNotFound)
}

// TestPutStoresClaimedDigestWhenVerificationIsOff pins the escape hatch for a
// client whose digest function is not SHA-256: the body is stored under the
// digest it was given, unverified.
func TestPutStoresClaimedDigestWhenVerificationIsOff(t *testing.T) {
	h, _ := newHandler(t)
	h.store.verify = false
	path := "/cas/" + hex64
	body := []byte("bytes under a digest this server cannot check")

	wantStatus(t, do(t, h, http.MethodPut, path, body), http.StatusOK)
	w := do(t, h, http.MethodGet, path, nil)
	wantStatus(t, w, http.StatusOK)
	if got := w.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestUnroutablePathIs404 pins that a path this server does not serve is a
// miss rather than an error, on every method.
func TestUnroutablePathIs404(t *testing.T) {
	h, _ := newHandler(t)
	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete} {
		wantStatus(t, do(t, h, m, "/cas/not-a-digest", nil), http.StatusNotFound)
	}
}

// TestMethodNotAllowed pins that a routable path with an unsupported method
// says so, and says what it would accept.
func TestMethodNotAllowed(t *testing.T) {
	h, _ := newHandler(t)
	w := do(t, h, http.MethodDelete, "/cas/"+hex64, nil)
	wantStatus(t, w, http.StatusMethodNotAllowed)
	if got := w.Header().Get("Allow"); got != "GET, HEAD, PUT" {
		t.Fatalf("Allow = %q, want %q", got, "GET, HEAD, PUT")
	}
}

// TestHeadReportsLengthWithoutBody pins that a HEAD costs the headers of a hit
// and none of its bytes.
func TestHeadReportsLengthWithoutBody(t *testing.T) {
	h, _ := newHandler(t)
	body := bytes.Repeat([]byte("x"), 4096)
	path := "/cas/" + sha256Hex(body)
	wantStatus(t, do(t, h, http.MethodPut, path, body), http.StatusOK)

	w := do(t, h, http.MethodHead, path, nil)
	wantStatus(t, w, http.StatusOK)
	if got, want := w.Header().Get("Content-Length"), strconv.Itoa(len(body)); got != want {
		t.Fatalf("Content-Length = %q, want %q", got, want)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("HEAD returned %d body bytes, want 0", w.Body.Len())
	}
}

// TestEmptyBodyRoundTrips pins that a zero-length blob is a hit rather than a
// miss. Bazel reads 204 as a miss, so a hit must be 200 even with nothing to
// send.
func TestEmptyBodyRoundTrips(t *testing.T) {
	h, _ := newHandler(t)
	path := "/cas/" + sha256Hex(nil)

	wantStatus(t, do(t, h, http.MethodPut, path, []byte{}), http.StatusOK)
	w := do(t, h, http.MethodGet, path, nil)
	wantStatus(t, w, http.StatusOK)
	if got := w.Header().Get("Content-Length"); got != "0" {
		t.Fatalf("Content-Length = %q, want %q", got, "0")
	}
}

// TestPutLeavesNoStagedFile pins that the staging area is empty once a request
// has been answered, whether it stored anything or not. A staged body is
// invisible to the byte budget, so one left behind is disk nothing reclaims.
func TestPutLeavesNoStagedFile(t *testing.T) {
	h, blobs := newHandler(t)
	body := []byte("stored")
	wantStatus(t, do(t, h, http.MethodPut, "/cas/"+sha256Hex(body), body), http.StatusOK)
	wantStatus(t, do(t, h, http.MethodPut, "/cas/"+hex64, []byte("refused")), http.StatusBadRequest)

	entries, err := os.ReadDir(filepath.Join(blobs.Root(), "staging"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging holds %d files, want 0", len(entries))
	}
}

// TestMonitoringRoutesServeWhatTheyAreGiven pins the success shape of both
// monitoring routes: the provider's bytes, unaltered, under the media type that
// declares what they are.
func TestMonitoringRoutesServeWhatTheyAreGiven(t *testing.T) {
	h := newMonitoredHandler(t,
		func(context.Context) (any, error) { return map[string]any{"pid": 4210}, nil },
		func(context.Context) ([]byte, error) { return []byte("# TYPE plaid_cache_actions gauge\n"), nil })

	w := do(t, h, http.MethodGet, StatusPath, nil)
	wantStatus(t, w, http.StatusOK)
	if got, want := w.Header().Get("Content-Type"), "application/json"; got != want {
		t.Fatalf("status Content-Type = %q, want %q", got, want)
	}
	var report struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatalf("status body %q: %v", w.Body, err)
	}
	if report.PID != 4210 {
		t.Fatalf("status pid = %d, want 4210", report.PID)
	}

	w = do(t, h, http.MethodGet, MetricsPath, nil)
	wantStatus(t, w, http.StatusOK)
	if got, want := w.Header().Get("Content-Type"), metricsContentType; got != want {
		t.Fatalf("metrics Content-Type = %q, want %q", got, want)
	}
	if got := w.Body.String(); got != "# TYPE plaid_cache_actions gauge\n" {
		t.Fatalf("metrics body = %q, want the provider's bytes unaltered", got)
	}
}

// TestMonitoringRoutesAreAbsentUnlessAsked pins the conservative default. A
// handler given no providers answers these paths exactly as it answers any other
// path it does not serve, so a scan learns nothing about whether this daemon
// could have been made to talk.
func TestMonitoringRoutesAreAbsentUnlessAsked(t *testing.T) {
	h, _ := newHandler(t)
	for _, p := range []string{StatusPath, MetricsPath} {
		w := do(t, h, http.MethodGet, p, nil)
		wantStatus(t, w, http.StatusNotFound)
		if got := w.Body.String(); !strings.Contains(got, "not a cache path") {
			t.Fatalf("GET %s said %q, want the same answer any unserved path gets", p, got)
		}
	}
}

// TestMonitoringLeavesTheCacheRoutesAlone pins that adding these routes changed
// nothing about the two that carry builds: the keyspaces still round-trip, and
// a path that is neither is still a miss.
func TestMonitoringLeavesTheCacheRoutesAlone(t *testing.T) {
	h := newMonitoredHandler(t,
		func(context.Context) (any, error) { return struct{}{}, nil },
		func(context.Context) ([]byte, error) { return nil, nil })

	body := []byte("an output stored while monitoring is on")
	for _, p := range []string{"/cas/" + sha256Hex(body), "/ac/" + hex64} {
		wantStatus(t, do(t, h, http.MethodPut, p, body), http.StatusOK)
		w := do(t, h, http.MethodGet, p, nil)
		wantStatus(t, w, http.StatusOK)
		if got := w.Body.Bytes(); !bytes.Equal(got, body) {
			t.Fatalf("GET %s = %q, want %q", p, got, body)
		}
	}
	// Neither a cache route nor a monitoring one, including the shapes that
	// would exist if the new routes had been given a keyspace's shape.
	for _, p := range []string{"/status/" + hex64, "/metrics/" + hex64, "/status/cas/" + hex64, "/cas/not-a-digest"} {
		wantStatus(t, do(t, h, http.MethodGet, p, nil), http.StatusNotFound)
	}
}

// TestMonitoringRoutesRefuseWrites pins that these two are read-only, and say
// what they would accept. A cache route takes a PUT; nothing here does.
func TestMonitoringRoutesRefuseWrites(t *testing.T) {
	h := newMonitoredHandler(t,
		func(context.Context) (any, error) { return struct{}{}, nil },
		func(context.Context) ([]byte, error) { return nil, nil })

	for _, p := range []string{StatusPath, MetricsPath} {
		for _, m := range []string{http.MethodPut, http.MethodPost, http.MethodDelete} {
			w := do(t, h, m, p, []byte("x"))
			wantStatus(t, w, http.StatusMethodNotAllowed)
			if got := w.Header().Get("Allow"); got != "GET, HEAD" {
				t.Fatalf("%s %s: Allow = %q, want %q", m, p, got, "GET, HEAD")
			}
		}
		// HEAD costs the headers and none of the body, as it does on a hit.
		w := do(t, h, http.MethodHead, p, nil)
		wantStatus(t, w, http.StatusOK)
		if w.Body.Len() != 0 {
			t.Fatalf("HEAD %s returned %d body bytes, want 0", p, w.Body.Len())
		}
	}
}

// TestMonitoringRoutesReportTheirFailuresHonestly pins the discipline these
// routes keep and the cache routes deliberately do not.
//
// A cache route reports a broken store as a miss or as a success, because Bazel
// reads anything else as a build error. Nothing reads these two, and answering
// "fine" with an empty report would hide exactly the failure the reader is
// asking about — so a provider that cannot answer produces a 5xx.
func TestMonitoringRoutesReportTheirFailuresHonestly(t *testing.T) {
	broken := errors.New("index is unreadable")
	h := newMonitoredHandler(t,
		func(context.Context) (any, error) { return nil, broken },
		func(context.Context) ([]byte, error) { return nil, broken })

	for _, p := range []string{StatusPath, MetricsPath} {
		w := do(t, h, http.MethodGet, p, nil)
		wantStatus(t, w, http.StatusInternalServerError)
	}

	// The same handler, in the same broken state, still answers a cache lookup
	// as a miss rather than as an error. The two disciplines coexist because
	// their readers differ.
	wantStatus(t, do(t, h, http.MethodGet, "/cas/"+hex64, nil), http.StatusNotFound)
}

// TestStatusRouteRefusesAnUnencodableReport pins that a report which cannot be
// marshalled is a failure with a status code rather than a truncated body under
// a 200, which is what writing the encoder straight to the response would give.
func TestStatusRouteRefusesAnUnencodableReport(t *testing.T) {
	h := newMonitoredHandler(t,
		func(context.Context) (any, error) { return func() {}, nil }, nil)
	wantStatus(t, do(t, h, http.MethodGet, StatusPath, nil), http.StatusInternalServerError)
	// Metrics was given no provider, so it is unrouted even though status is.
	wantStatus(t, do(t, h, http.MethodGet, MetricsPath, nil), http.StatusNotFound)
}

// totalAlloc reports the bytes this process has allocated in total, which is
// cumulative and therefore unaffected by whether a collection has run.
func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// failingStore is a store whose operations all fail, standing in for a full
// disk or an unreadable index.
type failingStore struct{}

// Get always fails.
func (failingStore) Get(context.Context, ids.ActionID) (cache.Result, error) {
	return cache.Result{}, errors.New("index is unreadable")
}

// Has always reports absent, so an upload is always attempted.
func (failingStore) Has(context.Context, ids.ActionID) bool { return false }

// HasRemote always reports absent because the test store has no shared tier.
func (failingStore) HasRemote(context.Context, ids.ActionID) bool { return false }

// PutStaged always fails.
func (failingStore) PutStaged(context.Context, ids.ActionID, ids.OutputID, string, int64) (string, error) {
	return "", errors.New("no space left on device")
}

// TestBrokenCacheStillAnswersCleanly pins the promise this package makes about
// its own failures: a lookup against a broken cache is a miss, and a store that
// could not happen is still reported as success. Bazel treats a non-200 read as
// an error rather than a miss, and does not re-read what it uploaded, so both
// answers cost a rebuild where the honest status would cost the build.
func TestBrokenCacheStillAnswersCleanly(t *testing.T) {
	_, blobs := newHandler(t)
	h := NewHandler(HandlerParams{
		Store: NewStore(StoreParams{Cache: failingStore{}, Blobs: blobs, Verify: true, Logf: t.Logf}),
		Logf:  t.Logf,
	})

	wantStatus(t, do(t, h, http.MethodGet, "/cas/"+hex64, nil), http.StatusNotFound)
	body := []byte("a body the store will refuse")
	wantStatus(t, do(t, h, http.MethodPut, "/cas/"+sha256Hex(body), body), http.StatusOK)
}

// TestLargeBodyIsStreamed pins that a large upload and its download both move
// through fixed-size buffers.
//
// The size is the point: an implementation that read a request body into memory
// would pass every other test here and then hold hundreds of megabytes per
// concurrent action on a real build. The allowance is far above io.Copy's
// buffer and far below the body, so it catches whole-body buffering without
// being sensitive to anything else the runtime does.
func TestLargeBodyIsStreamed(t *testing.T) {
	h, _ := newHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	const size = 64 << 20
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i)
	}
	sum := sha256.Sum256(body)
	url := srv.URL + "/cas/" + hex.EncodeToString(sum[:])

	before := totalAlloc()

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.ContentLength = size
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != size {
		t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, size)
	}
	got := sha256.New()
	n, err := io.Copy(got, resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if n != size {
		t.Fatalf("read %d bytes, want %d", n, size)
	}
	if !bytes.Equal(got.Sum(nil), sum[:]) {
		t.Fatalf("downloaded body has a different digest than the one uploaded")
	}

	// The client is measured along with the server, so the allowance covers
	// both sides of a 64 MiB round trip.
	if used := totalAlloc() - before; used > size/4 {
		t.Fatalf("a %d-byte round trip allocated %d bytes; the body is being buffered", size, used)
	}
}
