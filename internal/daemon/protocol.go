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
	"github.com/conductorone/plaid-cache/internal/index"
	"github.com/conductorone/plaid-cache/internal/remote"
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

	// OpStats reports the persisted activity history.
	OpStats Op = "stats"

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

	// Stats bounds the history requested, for OpStats.
	Stats *StatsParams `json:"stats,omitempty"`
}

// StatsParams selects how much history to report.
type StatsParams struct {
	// Since is a Go duration; the report covers the hours at or after it.
	Since string `json:"since"`
}

// StatsResponse reports the persisted counters.
//
// Lifetime is every hour ever recorded, including those already pruned from the
// per-hour history, so a total does not shrink when the window rolls over.
type StatsResponse struct {
	Lifetime      cache.MetricsSnapshot  `json:"lifetime"`
	LifetimeSince int64                  `json:"lifetime_since"`
	Window        cache.MetricsSnapshot  `json:"window"`
	WindowSince   int64                  `json:"window_since"`
	Buckets       []index.ActivityBucket `json:"buckets"`

	Err string `json:"err,omitempty"`
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
//
// One type serves three readers: the local socket's status report, the same
// report read from another host over the monitoring route, and the metrics
// exposition rendered from it. That is deliberate. A second shape for any of
// them would be a second place to assemble the same numbers, free to disagree
// with the first about what the cache holds.
type StatusResponse struct {
	// Lifetime is the persisted activity across every process that has used this
	// cache, and LifetimeSince when the first of it was recorded. Metrics below
	// is this daemon's own tally, which describes only however much of the day
	// this process happened to be up for.
	Lifetime      cache.MetricsSnapshot `json:"lifetime"`
	LifetimeSince int64                 `json:"lifetime_since"`

	// Version is the build answering. The socket learns it in the handshake and
	// refuses a mismatch; a reader that arrived over HTTP has no such exchange,
	// and a report whose provenance is unstated is a report about nothing in
	// particular.
	Version string `json:"version"`

	// RemoteEnabled says whether a shared tier is configured, without naming it.
	// Whether uploads are meaningful is what a reader needs; which bucket they
	// go to is the operator's business and not something this report discloses.
	RemoteEnabled bool `json:"remote_enabled"`

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

	// UploadQueueDepth is how many uploads to the shared tier are waiting on
	// this daemon's pool right now, and UploadQueueCapacity how many may.
	//
	// They are here rather than in Metrics because they are levels rather than
	// counters, and because they are what says saturation is coming: the drop
	// counter beside them only moves once entries have already been lost, and
	// says nothing about a queue that is one burst away from losing them.
	UploadQueueDepth    int `json:"upload_queue_depth"`
	UploadQueueCapacity int `json:"upload_queue_capacity"`

	// Remote is the shared tier's transport accounting: how often a request was
	// given a connection that was already open, and how long each operation
	// took. It is nil when the backend keeps none, which is every local-only
	// cache, so a reader can tell "nothing to report" from "nothing happened".
	//
	// It is this process's own, for the same reason the queue above is: it
	// describes a connection pool that lives and dies with this daemon.
	Remote *remote.StatsSnapshot `json:"remote,omitempty"`

	// The durations above again, in seconds.
	//
	// Each pair is two encodings of one measurement taken at one moment, not two
	// measurements: the strings are Go durations for a person reading a report,
	// and these are numbers for a scrape, which has no business parsing "168h0m0s"
	// and no use for milliseconds. HaveAgeSpan distinguishes an empty cache, which
	// has no age span at all, from one whose entries were all touched this second.
	TTLSeconds       float64 `json:"ttl_seconds"`
	UptimeSeconds    float64 `json:"uptime_seconds"`
	HaveAgeSpan      bool    `json:"have_age_span"`
	OldestAgeSeconds float64 `json:"oldest_age_seconds"`
	NewestAgeSeconds float64 `json:"newest_age_seconds"`
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

	// What re-measuring the bodies changed, so a pass that pruned nothing
	// because the recorded size was wrong says so rather than looking inert.
	Measured       int64 `json:"measured"`
	Corrected      int64 `json:"corrected"`
	RecordedBefore int64 `json:"recorded_before"`
	RecordedAfter  int64 `json:"recorded_after"`

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
