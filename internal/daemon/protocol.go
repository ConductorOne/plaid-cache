// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package daemon runs the single process that owns the cache index, and the
// thin client that the Go toolchain actually executes.
//
// Pebble takes an exclusive lock on its directory, so exactly one process can
// hold the index. The Go toolchain, meanwhile, starts a fresh GOCACHEPROG
// plugin for every invocation, and several invocations routinely run at once.
// The daemon reconciles the two: the plugin process is a byte relay onto a
// unix socket, and the long-lived daemon behind it owns the index, runs
// eviction on a timer, and exits when it has been idle.
package daemon

import (
	"encoding/json"
	"time"

	"github.com/conductorone/plaid-cache/internal/cache"
)

// Op selects what a connection is for. It is exchanged once, before any
// GOCACHEPROG bytes, so that control operations never enter the toolchain's
// protocol namespace.
type Op string

// The operations a client may request.
const (
	// OpSession hands the rest of the connection to the GOCACHEPROG protocol.
	OpSession Op = "session"

	// OpShutdown asks the daemon to exit, used when a client discovers a
	// version mismatch and must replace it.
	OpShutdown Op = "shutdown"

	// OpStatus reports counters without disturbing the cache.
	OpStatus Op = "status"

	// OpGC forces an eviction pass.
	OpGC Op = "gc"

	// OpAdopt imports a go-cache-plugin stage.
	//
	// Adoption writes to the index, which exactly one process may hold, so
	// asking the holder to do it is the only way to migrate a stage on a machine
	// that is already building. That is the normal case rather than the awkward
	// one: an env switching to plaid-cache starts serving builds through the
	// daemon at the same moment its old stage needs importing.
	OpAdopt Op = "adopt"
)

// Hello is the first line a client sends.
type Hello struct {
	// Version is the client's build version. A daemon running different code
	// than its clients is a correctness hazard, not a compatibility question,
	// because both sides interpret the same on-disk index.
	Version string `json:"version"`

	Op Op `json:"op"`

	// GC carries per-pass eviction overrides for OpGC. Nil means use the
	// daemon's own configuration.
	GC *GCParams `json:"gc,omitempty"`

	// Adopt names the stage to import, for OpAdopt.
	Adopt *AdoptParams `json:"adopt,omitempty"`
}

// AdoptParams names the go-cache-plugin stage a daemon should import.
type AdoptParams struct {
	// Dir is the stage root, holding that cache's action/ and output/ trees.
	Dir string `json:"dir"`

	DryRun bool `json:"dry_run,omitempty"`
}

// GCParams overrides the daemon's eviction limits for a single pass.
//
// The fields are pointers because zero is a meaningful value for both — it
// disables that constraint — so "unset" has to be distinguishable from "set to
// zero". TTL is a string so it travels in the same Go duration syntax the
// configuration uses, rather than as a bare count of nanoseconds.
type GCParams struct {
	MaxBytes *int64  `json:"max_bytes,omitempty"`
	TTL      *string `json:"ttl,omitempty"`
}

// HelloResponse is the daemon's reply to Hello.
type HelloResponse struct {
	Version string `json:"version"`
	OK      bool   `json:"ok"`
	Err     string `json:"err,omitempty"`
}

// StatusResponse reports the daemon's view of the cache.
type StatusResponse struct {
	PID       int                   `json:"pid"`
	Actions   int64                 `json:"actions"`
	Objects   int64                 `json:"objects"`
	DiskBytes int64                 `json:"disk_bytes"`
	MaxBytes  int64                 `json:"max_bytes"`
	TTL       string                `json:"ttl"`
	Uptime    string                `json:"uptime"`
	OldestAge string                `json:"oldest_age,omitempty"`
	NewestAge string                `json:"newest_age,omitempty"`
	Metrics   cache.MetricsSnapshot `json:"metrics"`
	Err       string                `json:"err,omitempty"`
}

// GCResponse reports the outcome of a forced eviction pass.
type GCResponse struct {
	ActionsPruned int64  `json:"actions_pruned"`
	ObjectsPruned int64  `json:"objects_pruned"`
	BytesFreed    int64  `json:"bytes_freed"`
	Elapsed       string `json:"elapsed"`

	// The limits the pass actually applied, so a caller can see that an
	// override took effect rather than having to infer it from the outcome.
	AppliedMaxBytes int64  `json:"applied_max_bytes"`
	AppliedTTL      string `json:"applied_ttl"`

	Err string `json:"err,omitempty"`
}

// AdoptResponse reports the outcome of an import.
//
// The fields mirror adopt.Result rather than embedding it, so that the wire
// shape is declared here instead of following a type free to change for reasons
// of its own.
type AdoptResponse struct {
	Records        int64 `json:"records"`
	Adopted        int64 `json:"adopted"`
	AlreadyPresent int64 `json:"already_present"`
	MissingBody    int64 `json:"missing_body"`
	SizeMismatch   int64 `json:"size_mismatch"`
	Malformed      int64 `json:"malformed"`
	Linked         int64 `json:"linked"`
	Copied         int64 `json:"copied"`
	Bytes          int64 `json:"bytes"`

	Elapsed string `json:"elapsed"`

	Err string `json:"err,omitempty"`
}

// writeJSONLine emits one newline-delimited JSON value.
func writeJSONLine(w interface{ Write([]byte) (int, error) }, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// handshakeTimeout bounds the control exchange so a client that connects and
// then stalls cannot hold a daemon slot open.
const handshakeTimeout = 10 * time.Second
