// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package adopt imports an existing go-cache-plugin local stage into
// plaid-cache's index.
//
// The two tools address bodies identically — sha256 of the content, sharded by
// its first byte — so a stage written by one is already laid out the way the
// other expects. What go-cache-plugin has and plaid-cache lacks is the mapping
// from action to output, and that is recoverable: go-cache-plugin records it as
// one small file per action, named for the action id and containing the output
// id and size.
//
// Adoption therefore reconstructs the whole cache rather than guessing at it,
// and it moves bodies by hardlink, so a stage of any size is imported without
// copying a byte. The point is not only to avoid the copy: an index that knows
// about the bodies is an index whose size ceiling accounts for them, which a
// shared directory would not be.
package adopt

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/conductorone/plaid-cache/internal/blob"
	"github.com/conductorone/plaid-cache/internal/ids"
	"github.com/conductorone/plaid-cache/internal/index"
)

// Params configures one adoption pass.
type Params struct {
	// LegacyDir is the go-cache-plugin stage root: the directory holding its
	// action/ and output/ trees.
	LegacyDir string

	Index *index.Index
	Blobs *blob.Store

	// DryRun reports what would be adopted without writing anything.
	DryRun bool

	Logf func(format string, args ...any)
}

// Result reports what a pass did.
//
// The categories are separated because they mean different things: a missing
// body is a stage that was pruned under its own records and is expected, while
// a malformed record suggests something else wrote there.
type Result struct {
	Records        int64 // action files examined
	Adopted        int64 // entries added to the index
	AlreadyPresent int64 // already indexed, left alone
	MissingBody    int64 // record pointed at a body that is gone
	SizeMismatch   int64 // body present but disagrees with the record
	Malformed      int64 // unparseable record or filename
	Linked         int64 // bodies published by hardlink, costing no space
	Copied         int64 // bodies copied, because the stage is on another device
	Bytes          int64 // disk bytes now accounted for in the index
	Elapsed        time.Duration
}

// String renders a Result for a log line or a command's output.
func (r Result) String() string {
	return fmt.Sprintf("%d records: %d adopted (%d linked, %d copied, %s), %d already present, "+
		"%d missing bodies, %d size mismatches, %d malformed, in %v",
		r.Records, r.Adopted, r.Linked, r.Copied, humanBytes(r.Bytes), r.AlreadyPresent,
		r.MissingBody, r.SizeMismatch, r.Malformed, r.Elapsed.Round(time.Millisecond))
}

// humanBytes renders a byte count for display.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// actionSubdir and outputSubdir are go-cache-plugin's stage layout.
const (
	actionSubdir = "action"
	outputSubdir = "output"
)

// Run imports LegacyDir into the index.
//
// It is safe to run against a stage the other cache is still using: bodies are
// hardlinked rather than moved, so both sides keep a working name for the same
// inode, and neither one's pruning can pull data out from under the other.
//
// It is also safe to run twice. An action already in the index is left alone
// rather than re-adopted, so a second pass costs a lookup per record and changes
// nothing.
func Run(ctx context.Context, p Params) (Result, error) {
	start := time.Now()
	var r Result
	defer func() { r.Elapsed = time.Since(start) }()

	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if p.Index == nil || p.Blobs == nil {
		return r, errors.New("Run: index and blobs are required")
	}

	actionRoot := filepath.Join(p.LegacyDir, actionSubdir)
	if _, err := os.Stat(actionRoot); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Nothing to adopt is a normal outcome, not a failure: an env that
			// never ran the other cache has no stage.
			return r, nil
		}
		return r, fmt.Errorf("Run: %w", err)
	}

	walkErr := filepath.WalkDir(actionRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// A single unreadable shard should not abandon the rest of a stage
			// with thousands of usable records.
			logf("adopt: %s: %v (skipped)", path, err)
			return nil
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r.Records++

		rec, perr := readRecord(path, d.Name())
		if perr != nil {
			r.Malformed++
			logf("adopt: %s: %v", path, perr)
			return nil
		}

		if _, ok, gerr := p.Index.Get(rec.action); gerr != nil {
			logf("adopt: index lookup %s: %v", rec.action, gerr)
			return nil
		} else if ok {
			r.AlreadyPresent++
			return nil
		}

		bodyPath := filepath.Join(p.LegacyDir, outputSubdir, rec.output.String()[:2], rec.output.String())
		fi, serr := os.Stat(bodyPath)
		if serr != nil {
			// The stage pruned this body but kept the record, or never had it.
			r.MissingBody++
			return nil
		}
		if fi.Size() != rec.size {
			r.SizeMismatch++
			logf("adopt: %s is %d bytes, record says %d", bodyPath, fi.Size(), rec.size)
			return nil
		}
		if p.DryRun {
			r.Adopted++
			r.Bytes += rec.size
			return nil
		}

		_, nbytes, linked, aerr := p.Blobs.Adopt(rec.output, bodyPath, rec.size)
		if aerr != nil {
			logf("adopt: %s: %v", bodyPath, aerr)
			return nil
		}
		if linked {
			r.Linked++
		} else {
			r.Copied++
		}

		// Recency comes from the action file, not the body.
		//
		// go-cache-plugin stamps a body it faults in from S3 with the time that
		// content was originally produced, so a body can be months old while the
		// entry is in daily use. Its own prune keys off the action file's
		// modification time for exactly that reason. Using the body's time here
		// made a third of a healthy stage look instantly expired: measured on a
		// real stage, 4531 of 13969 bodies predated a 168h TTL while none of the
		// 16836 action files did.
		//
		// The body's time is still the honest answer for when the content was
		// created, so the two timestamps come from different places on purpose.
		created := fi.ModTime().UnixNano()
		lastUsed := created
		if ai, aerr := d.Info(); aerr == nil {
			if n := ai.ModTime().UnixNano(); n > 0 {
				lastUsed = n
			}
		}
		if created <= 0 {
			created = lastUsed
		}
		if created <= 0 || lastUsed <= 0 {
			now := time.Now().UnixNano()
			created, lastUsed = now, now
		}
		if perr := p.Index.Put(rec.action, index.Entry{
			OutputID:   rec.output,
			Size:       rec.size,
			CreatedAt:  created,
			LastUsedAt: lastUsed,
		}, nbytes); perr != nil {
			logf("adopt: index put %s: %v", rec.action, perr)
			return nil
		}
		r.Adopted++
		r.Bytes += nbytes
		return nil
	})
	if walkErr != nil {
		return r, fmt.Errorf("Run: %w", walkErr)
	}
	return r, nil
}

// record is one go-cache-plugin action file.
type record struct {
	action ids.ActionID
	output ids.OutputID
	size   int64
}

// readRecord parses an action file: the name is the action id, and the contents
// are the output id and the body size separated by a space.
func readRecord(path, name string) (record, error) {
	action, err := ids.ParseActionID(name)
	if err != nil {
		return record{}, fmt.Errorf("readRecord: name: %w", err)
	}
	// Records are two short fields; cap the read so a stray large file in the
	// tree cannot be pulled into memory.
	f, err := os.Open(path)
	if err != nil {
		return record{}, fmt.Errorf("readRecord: %w", err)
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 256)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return record{}, fmt.Errorf("readRecord: %w", err)
	}

	fields := strings.Fields(string(buf[:n]))
	if len(fields) != 2 {
		return record{}, fmt.Errorf("readRecord: got %d fields, want 2", len(fields))
	}
	output, err := ids.ParseOutputID(fields[0])
	if err != nil {
		return record{}, fmt.Errorf("readRecord: output id: %w", err)
	}
	size, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return record{}, fmt.Errorf("readRecord: size: %w", err)
	}
	if size < 0 {
		return record{}, fmt.Errorf("readRecord: negative size %d", size)
	}
	return record{action: action, output: output, size: size}, nil
}
