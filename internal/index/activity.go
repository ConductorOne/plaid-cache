// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// Activity counts what the cache did.
//
// The counters live in the index rather than only in memory because a process
// counter answers the wrong question. The daemon exits after its idle timeout
// and a plugin invocation lasts one build, so an in-memory hit rate describes
// however much of the day happened to be covered by the process being asked —
// which for a cache that has been quiet for half an hour is nothing at all.
// Anyone assessing whether the cache is working wants the machine's history, not
// the current process's.
type Activity struct {
	GetLocalHit  int64 `json:"get_local_hit"`
	GetRemoteHit int64 `json:"get_remote_hit"`
	GetMiss      int64 `json:"get_miss"`
	GetRepair    int64 `json:"get_repair"`
	Put          int64 `json:"put"`
	UploadOK     int64 `json:"upload_ok"`
	UploadFail   int64 `json:"upload_fail"`
	UploadDrop   int64 `json:"upload_drop"`
	UploadSkip   int64 `json:"upload_skip"`
	Compactions  int64 `json:"compactions"`
}

// Add returns the sum of two counter sets.
func (a Activity) Add(b Activity) Activity {
	return Activity{
		GetLocalHit:  a.GetLocalHit + b.GetLocalHit,
		GetRemoteHit: a.GetRemoteHit + b.GetRemoteHit,
		GetMiss:      a.GetMiss + b.GetMiss,
		GetRepair:    a.GetRepair + b.GetRepair,
		Put:          a.Put + b.Put,
		UploadOK:     a.UploadOK + b.UploadOK,
		UploadFail:   a.UploadFail + b.UploadFail,
		UploadDrop:   a.UploadDrop + b.UploadDrop,
		UploadSkip:   a.UploadSkip + b.UploadSkip,
		Compactions:  a.Compactions + b.Compactions,
	}
}

// Sub returns what happened between an earlier snapshot and this one.
func (a Activity) Sub(b Activity) Activity {
	return Activity{
		GetLocalHit:  a.GetLocalHit - b.GetLocalHit,
		GetRemoteHit: a.GetRemoteHit - b.GetRemoteHit,
		GetMiss:      a.GetMiss - b.GetMiss,
		GetRepair:    a.GetRepair - b.GetRepair,
		Put:          a.Put - b.Put,
		UploadOK:     a.UploadOK - b.UploadOK,
		UploadFail:   a.UploadFail - b.UploadFail,
		UploadDrop:   a.UploadDrop - b.UploadDrop,
		UploadSkip:   a.UploadSkip - b.UploadSkip,
		Compactions:  a.Compactions - b.Compactions,
	}
}

// IsZero reports whether nothing was counted.
func (a Activity) IsZero() bool { return a == Activity{} }

// Lookups is how many times the cache was asked for something.
func (a Activity) Lookups() int64 { return a.GetLocalHit + a.GetRemoteHit + a.GetMiss }

// Hits is how many of those were answered from either tier.
func (a Activity) Hits() int64 { return a.GetLocalHit + a.GetRemoteHit }

// HitRate is the fraction of lookups answered, and false when nothing was
// looked up — a rate over no lookups is not zero, it is unknown, and reporting
// "0.0%" for an idle cache reads as a broken one.
func (a Activity) HitRate() (float64, bool) {
	n := a.Lookups()
	if n == 0 {
		return 0, false
	}
	return float64(a.Hits()) / float64(n), true
}

// ActivityBucket is one hour of counters.
type ActivityBucket struct {
	// Hour is the start of the hour, as Unix seconds in UTC.
	Hour int64 `json:"hour"`

	Activity Activity `json:"activity"`
}

// activityRecord is the persisted lifetime total.
type activityRecord struct {
	Version int      `json:"version"`
	Since   int64    `json:"since"` // Unix nanos of the first recorded activity
	Total   Activity `json:"total"`
}

// activityVersion is the record version. A record from a future version is
// reported rather than guessed at.
const activityVersion = 1

// ActivityRetention is how much per-hour history is kept.
//
// The buckets are the reason this is bounded: one JSON record per hour is a few
// hundred bytes, so two weeks costs well under a megabyte, and pruning is one
// range delete per flush. A cache whose whole purpose is bounded growth should
// not accumulate its own telemetry forever.
const ActivityRetention = 14 * 24 * time.Hour

// RecordActivity folds a delta into the lifetime total and into the bucket for
// the hour containing at, and drops buckets past the retention window.
//
// It refuses a closed index rather than reaching into Pebble, which panics on a
// closed database instead of returning an error. This is reachable in ordinary
// operation: the cache flushes its counters when it closes, and an index that
// was closed first — by a shutdown ordering, or because it broke and was closed
// underneath — would otherwise take the process down over telemetry.
//
// It is a no-op for an empty delta, so an idle daemon's periodic flush writes
// nothing at all rather than rewriting the same record every minute.
func (ix *Index) RecordActivity(delta Activity, at time.Time) error {
	if delta.IsZero() {
		return nil
	}
	if ix.closed.Load() {
		return fmt.Errorf("RecordActivity: %w", ErrClosed)
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	total, since, err := ix.loadActivity()
	if err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	if since == 0 {
		since = at.UnixNano()
	}
	rec, err := json.Marshal(activityRecord{
		Version: activityVersion,
		Since:   since,
		Total:   total.Add(delta),
	})
	if err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}

	hour := at.UTC().Truncate(time.Hour)
	bucket, err := ix.loadActivityBucket(hour)
	if err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	bucketVal, err := json.Marshal(bucket.Add(delta))
	if err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}

	b := ix.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(metaActivity, rec, nil); err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	if err := b.Set(activityBucketKey(hour), bucketVal, nil); err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	// One range delete keeps the history bounded without a scan.
	lower, _ := activityBucketRange()
	if err := b.DeleteRange(lower, activityBucketKey(hour.Add(-ActivityRetention)), nil); err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	// Not synced. Losing the last flush to a power cut costs a minute of
	// counters, and paying a disk sync for telemetry on a path that runs beside
	// every build is not a trade worth making.
	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("RecordActivity: %w", err)
	}
	return nil
}

// TotalActivity returns the lifetime counters and the Unix-nanos instant the
// first of them was recorded, which is zero when nothing has been.
func (ix *Index) TotalActivity() (Activity, int64, error) {
	if ix.closed.Load() {
		return Activity{}, 0, fmt.Errorf("TotalActivity: %w", ErrClosed)
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.loadActivity()
}

// ActivitySince returns the hourly buckets at or after cutoff, oldest first.
func (ix *Index) ActivitySince(cutoff time.Time) ([]ActivityBucket, error) {
	if ix.closed.Load() {
		return nil, fmt.Errorf("ActivitySince: %w", ErrClosed)
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	_, upper := activityBucketRange()
	iter, err := ix.db.NewIter(&pebble.IterOptions{
		LowerBound: activityBucketKey(cutoff.UTC().Truncate(time.Hour)),
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("ActivitySince: %w", err)
	}
	defer func() {
		if cerr := iter.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("ActivitySince: close iterator: %w", cerr)
		}
	}()

	var out []ActivityBucket
	for iter.First(); iter.Valid(); iter.Next() {
		hour, ok := hourFromBucketKey(iter.Key())
		if !ok {
			continue
		}
		var a Activity
		if err := json.Unmarshal(iter.Value(), &a); err != nil {
			// One unreadable hour should not deny a report of the rest.
			continue
		}
		out = append(out, ActivityBucket{Hour: hour, Activity: a})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("ActivitySince: %w", err)
	}
	return out, nil
}

// loadActivity reads the lifetime record. The caller holds ix.mu.
func (ix *Index) loadActivity() (Activity, int64, error) {
	v, ok, err := ix.getCopy(metaActivity)
	if err != nil {
		return Activity{}, 0, err
	}
	if !ok {
		return Activity{}, 0, nil
	}
	var rec activityRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return Activity{}, 0, fmt.Errorf("activity record: %w", err)
	}
	if rec.Version > activityVersion {
		return Activity{}, 0, fmt.Errorf("activity record: version %d is newer than %d", rec.Version, activityVersion)
	}
	return rec.Total, rec.Since, nil
}

// loadActivityBucket reads one hour's counters. The caller holds ix.mu.
func (ix *Index) loadActivityBucket(hour time.Time) (Activity, error) {
	v, ok, err := ix.getCopy(activityBucketKey(hour))
	if err != nil || !ok {
		return Activity{}, err
	}
	var a Activity
	if err := json.Unmarshal(v, &a); err != nil {
		// A damaged bucket restarts from zero rather than refusing to count.
		return Activity{}, nil
	}
	return a, nil
}

// activityBucketKey builds the key for one hour. The timestamp is big-endian so
// that byte order is time order.
func activityBucketKey(hour time.Time) []byte {
	k := make([]byte, 0, len(activityBucketPrefix)+8)
	k = append(k, activityBucketPrefix...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(hour.UTC().Truncate(time.Hour).Unix()))
	return append(k, ts[:]...)
}

// hourFromBucketKey recovers the hour a bucket key names.
func hourFromBucketKey(k []byte) (int64, bool) {
	if len(k) != len(activityBucketPrefix)+8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(k[len(activityBucketPrefix):])), true
}

// activityBucketRange returns the bounds covering every bucket.
func activityBucketRange() (lower, upper []byte) {
	lower = append([]byte{}, activityBucketPrefix...)
	upper = append([]byte{}, activityBucketPrefix...)
	upper[len(upper)-1]++
	return lower, upper
}
