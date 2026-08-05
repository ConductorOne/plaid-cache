// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/conductorone/plaid-cache/internal/cache"
)

// The monitoring routes: the two paths on this listener that are not part of
// Bazel's protocol at all, but a report on the daemon serving it, for an
// operator with no shell on the host.
//
// Each is a single segment, matched in full, which is what keeps them clear of
// the discipline [parsePath] documents. Every cache route is a keyspace and a
// digest — two segments — so a bare word can never be read as one: these cannot
// become a third keyspace by accident, cannot be reached under a prefix, and
// leave what /ac and /cas mean untouched. Being distinct fixed paths also means
// a reverse proxy in front of this listener can route them to a different
// audience than the cache itself.
const (
	StatusPath  = "/status"
	MetricsPath = "/metrics"
)

// StatusFunc produces the report [StatusPath] serves, and MetricsFunc the
// exposition [MetricsPath] serves. A nil one leaves that route unserved.
//
// Both hand back what the daemon has already decided to say rather than letting
// this package assemble it, and for the same reason: the numbers belong to the
// package that owns the counters, and a second assembly here would be a second
// accounting path free to disagree with the first. StatusFunc returns the value
// to encode rather than encoded bytes because the daemon's response type cannot
// be named from here — the daemon package imports this one, so referring to it
// would be an import cycle, and a second copy of it here is exactly the drift
// that having one type avoids.
//
// An error from either means the daemon could not answer, and is reported to the
// client as one. See [Handler.serveMonitoring] for why that differs from the
// cache routes.
type (
	StatusFunc  func(ctx context.Context) (any, error)
	MetricsFunc func(ctx context.Context) ([]byte, error)
)

// metricsContentType is the Prometheus text exposition format's own media type.
// An OpenTelemetry Collector's prometheus receiver reads the same bytes.
const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// Handler serves Bazel's HTTP/1.1 remote-cache protocol over a [Store].
//
// The protocol is four routes — GET and PUT on /ac/<hex-digest> and
// /cas/<hex-digest> — with every body opaque bytes. This is the whole of the
// transport: what the routes mean is [Store]'s business.
type Handler struct {
	store   *Store
	logf    cache.Logf
	status  StatusFunc
	metrics MetricsFunc
}

// HandlerParams carries what a Handler serves.
type HandlerParams struct {
	Store *Store
	Logf  cache.Logf

	// Status and Metrics serve the monitoring routes. Nil, the zero value,
	// leaves that route unrouted: the caller decides whether this listener
	// discloses anything about its host, and a listener that has not been told
	// to is indistinguishable from one built before the routes existed.
	Status  StatusFunc
	Metrics MetricsFunc
}

// NewHandler constructs a Handler over an existing Store.
func NewHandler(p HandlerParams) *Handler {
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Handler{store: p.Store, logf: logf, status: p.Status, metrics: p.Metrics}
}

// ServeHTTP routes one request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The monitoring routes come first because they are not cache routes, and
	// each only when something is on offer: with nothing, the path falls through
	// to the same 404 any other unserved path gets, which says nothing about
	// whether this build of the daemon has the route at all.
	switch r.URL.Path {
	case StatusPath:
		if h.status != nil {
			h.serveMonitoring(w, r, "status", "application/json", func(ctx context.Context) ([]byte, error) {
				v, err := h.status(ctx)
				if err != nil {
					return nil, err
				}
				b, err := json.Marshal(v)
				if err != nil {
					return nil, err
				}
				// A trailing newline so the document reads sanely from a
				// terminal, which is where a good deal of it will be read.
				return append(b, '\n'), nil
			})
			return
		}
	case MetricsPath:
		if h.metrics != nil {
			h.serveMonitoring(w, r, "metrics", metricsContentType, h.metrics)
			return
		}
	}

	k, d, ok := parsePath(r.URL.Path)
	if !ok {
		// Not a route this cache serves. A GET of one is also the answer Bazel
		// reads as a miss, which is the right outcome for a client asking about
		// something that cannot be here.
		http.Error(w, "plaid-cache: not a cache path", http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.get(w, r, k, d)
	case http.MethodPut:
		h.put(w, r, k, d)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT")
		http.Error(w, "plaid-cache: method not allowed", http.StatusMethodNotAllowed)
	}
}

// get answers one lookup.
//
// A miss is 404 and there is no other failure status, deliberately: Bazel reads
// 404 and 204 as a miss and any other non-200 as an error, so a server that
// reported its own trouble honestly would turn a broken cache into a broken
// build.
func (h *Handler) get(w http.ResponseWriter, r *http.Request, k Kind, d Digest) {
	f, size, ok := h.store.Open(r.Context(), k, d)
	if !ok {
		http.Error(w, "plaid-cache: cache miss", http.StatusNotFound)
		return
	}
	defer func() { _ = f.Close() }()

	// Bazel rejects a response with no Content-Length outright and truncates
	// one whose body falls short of it, so the declared length and the bytes
	// sent must come from the same open file.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, f); err != nil {
		// The status line is already sent, so there is nothing left to tell the
		// client. Go closes the connection when a body falls short of its
		// declared length, which is how it finds out.
		h.logf("bazel send %s/%s: %v", k, d, err)
	}
}

// put stores one upload.
//
// A storage failure is reported as success. Bazel does not re-read what it
// uploaded, so a store that did not happen costs a future miss and nothing
// else, where an error status risks the build over a full disk.
func (h *Handler) put(w http.ResponseWriter, r *http.Request, k Kind, d Digest) {
	err := h.store.Put(r.Context(), k, d, r.Body)
	switch {
	case err == nil:
	case errors.Is(err, ErrDigestMismatch):
		http.Error(w, "plaid-cache: body does not match its digest", http.StatusBadRequest)
		return
	default:
		h.logf("bazel put %s/%s: %v", k, d, err)
	}
	// Bazel accepts 200, 201, 202 and 204 for an upload.
	w.WriteHeader(http.StatusOK)
}

// serveMonitoring answers one request on a monitoring route.
//
// These report their failures honestly, with real status codes, and that is a
// deliberate break from the two routes above. A cache route lies about its
// troubles because Bazel reads any non-200 other than a miss as a build error,
// so an honest 500 there turns a broken cache into a broken build. Nothing reads
// these two: a scraper or an operator asking how the daemon is doing wants the
// truth, and answering "fine" with an empty report would hide exactly the
// failure they are asking about. The two disciplines are opposite because their
// readers are, and neither should be extended to the other's routes.
func (h *Handler) serveMonitoring(w http.ResponseWriter, r *http.Request, what, contentType string, body func(context.Context) ([]byte, error)) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
	default:
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "plaid-cache: method not allowed", http.StatusMethodNotAllowed)
		return
	}

	b, err := body(r.Context())
	if err != nil {
		h.logf("bazel %s: %v", what, err)
		http.Error(w, "plaid-cache: "+what+" is unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	// A report is true for the instant it was taken and no longer, so an
	// intermediary must not be allowed to serve a stale one as current.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := w.Write(b); err != nil {
		h.logf("bazel send %s: %v", what, err)
	}
}

// parsePath splits a request path into the keyspace and digest it names.
//
// Bazel builds the path by appending "ac/<hash>" or "cas/<hash>" to the path
// component of the --remote_cache URI, so a URI carrying a path prefix would
// send that prefix along. Only the two bare routes are served: accepting an
// arbitrary prefix would let two prefixes share one keyspace without saying so,
// which is a cache-poisoning shape rather than a convenience.
func parsePath(p string) (Kind, Digest, bool) {
	rest, ok := strings.CutPrefix(p, "/")
	if !ok {
		return 0, Digest{}, false
	}
	name, hexDigest, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, Digest{}, false
	}
	var k Kind
	switch name {
	case "ac":
		k = KindAC
	case "cas":
		k = KindCAS
	default:
		return 0, Digest{}, false
	}
	// A trailing path segment is rejected by the length check inside
	// ParseDigest, since the cut above leaves any remaining slashes here.
	d, err := ParseDigest(hexDigest)
	if err != nil {
		return 0, Digest{}, false
	}
	return k, d, true
}
