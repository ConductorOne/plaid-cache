// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package bazel

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/conductorone/plaid-cache/internal/cache"
)

// Handler serves Bazel's HTTP/1.1 remote-cache protocol over a [Store].
//
// The protocol is four routes — GET and PUT on /ac/<hex-digest> and
// /cas/<hex-digest> — with every body opaque bytes. This is the whole of the
// transport: what the routes mean is [Store]'s business.
type Handler struct {
	store *Store
	logf  cache.Logf
}

// NewHandler constructs a Handler over an existing Store.
func NewHandler(s *Store, logf cache.Logf) *Handler {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Handler{store: s, logf: logf}
}

// ServeHTTP routes one request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	defer f.Close()

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
