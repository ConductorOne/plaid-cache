// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package config resolves plaid-cache's runtime configuration from the
// environment.
//
// Every setting has exactly one environment variable and a documented
// fallback chain. There are deliberately no option structs and no
// configuration file: the tool is executed by the Go toolchain as a
// GOCACHEPROG plugin, which offers no way to pass flags, so the environment
// is the only channel that reaches every invocation.
//
// Unparseable values are a hard error rather than a silent fallback to the
// default. A typo in PLAID_GOCACHE_MAX_BYTES that quietly disabled the size
// ceiling would let the cache grow until it filled the disk, which is the
// exact failure this tool exists to prevent.
package config

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// LogLevel controls how much plaid-cache writes to its log.
type LogLevel int

// Log levels, in increasing order of verbosity.
const (
	LogOff LogLevel = iota
	LogError
	LogInfo
	LogDebug
)

// String returns the canonical spelling of the level, matching the values
// accepted in PLAID_GOCACHE_LOG.
func (l LogLevel) String() string {
	switch l {
	case LogOff:
		return "off"
	case LogError:
		return "error"
	case LogInfo:
		return "info"
	case LogDebug:
		return "debug"
	default:
		return "unknown"
	}
}

// Config is the fully resolved configuration for one plaid-cache process.
type Config struct {
	// Dir is the local cache root. It holds the body store, the index, the
	// daemon socket, and the daemon log.
	Dir string

	// MaxBytes is the ceiling on local body-store disk usage. Zero disables
	// the size constraint, leaving only the TTL.
	MaxBytes int64

	// TTL is how long an unused entry survives. Zero disables the TTL
	// constraint, leaving only the size ceiling.
	TTL time.Duration

	// TouchGranularity is the relatime window: a cache hit only rewrites the
	// entry's last-used timestamp if the recorded one is older than this.
	// Without it, every hit on a warm cache would issue an index write.
	TouchGranularity time.Duration

	// S3Bucket is the remote tier's bucket. Empty means local-only operation,
	// which is the default: there is no sensible bucket to guess.
	S3Bucket string

	// S3Region is the remote tier's region. Empty defers to the AWS SDK's own
	// resolution chain.
	S3Region string

	// S3Prefix is prepended to every remote key, allowing one bucket to serve
	// several architectures or toolchain versions without collision.
	S3Prefix string

	// S3EndpointURL overrides the AWS endpoint, for tests against a local S3
	// implementation.
	S3EndpointURL string

	// MinUploadSize is the body size below which upload to the remote tier is
	// skipped. Zero, the default, uploads everything.
	//
	// Raising it trades remote hit rate for round trips, and the trade is
	// worse than it looks: skipping the upload also skips the action record,
	// so the entry becomes a remote miss and the toolchain re-runs the action
	// rather than merely re-downloading its output. Body size is a poor proxy
	// for how expensive an action was, so a threshold penalizes cheap-to-store
	// but expensive-to-compute work first.
	MinUploadSize int64

	// UploadConcurrency bounds in-flight remote uploads.
	UploadConcurrency int

	// IdleTimeout is how long the daemon runs with no connected clients
	// before exiting.
	IdleTimeout time.Duration

	// EvictInterval is how often the daemon runs an eviction pass.
	EvictInterval time.Duration

	// DisableEviction turns off the eviction ticker entirely. Intended for
	// debugging; it reintroduces unbounded growth.
	DisableEviction bool

	// DisableDaemon forces direct in-process operation, skipping the shared
	// index. Concurrent invocations then cannot share bounded growth.
	DisableDaemon bool

	// Log is the verbosity level.
	Log LogLevel
}

// Default values for settings that have one. Bucket, region, prefix, and
// endpoint deliberately have no defaults.
const (
	defaultMaxBytes         = 20 << 30 // 20GB
	defaultTTL              = 168 * time.Hour
	defaultTouchGranularity = time.Hour
	defaultMinUploadSize    = 0
	defaultIdleTimeout      = 30 * time.Minute
	defaultEvictInterval    = time.Minute
)

// Load resolves configuration from the process environment.
func Load() (*Config, error) {
	dir, err := resolveDir()
	if err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}

	c := &Config{
		Dir:               dir,
		S3Bucket:          os.Getenv("PLAID_GOCACHE_S3_BUCKET"),
		S3Region:          os.Getenv("PLAID_GOCACHE_S3_REGION"),
		S3Prefix:          os.Getenv("PLAID_GOCACHE_S3_PREFIX"),
		S3EndpointURL:     os.Getenv("PLAID_GOCACHE_S3_ENDPOINT_URL"),
		UploadConcurrency: runtime.NumCPU(),
	}

	if c.MaxBytes, err = envBytes("PLAID_GOCACHE_MAX_BYTES", defaultMaxBytes); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.TTL, err = envDuration("PLAID_GOCACHE_TTL", defaultTTL); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.TouchGranularity, err = envDuration("PLAID_GOCACHE_TOUCH_GRANULARITY", defaultTouchGranularity); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.MinUploadSize, err = envBytes("PLAID_GOCACHE_MIN_UPLOAD_SIZE", defaultMinUploadSize); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.IdleTimeout, err = envDuration("PLAID_GOCACHE_IDLE_TIMEOUT", defaultIdleTimeout); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.EvictInterval, err = envDuration("PLAID_GOCACHE_EVICT_INTERVAL", defaultEvictInterval); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.UploadConcurrency, err = envInt("PLAID_GOCACHE_UPLOAD_CONCURRENCY", runtime.NumCPU()); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.UploadConcurrency < 1 {
		return nil, fmt.Errorf("Load: PLAID_GOCACHE_UPLOAD_CONCURRENCY: got %d, want >= 1", c.UploadConcurrency)
	}
	if c.DisableEviction, err = envBool("PLAID_GOCACHE_DISABLE_EVICTION"); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.DisableDaemon, err = envBool("PLAID_GOCACHE_DISABLE_DAEMON"); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.Log, err = envLogLevel("PLAID_GOCACHE_LOG", LogError); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	return c, nil
}

// BlobDir is the root of the content-addressed body store.
func (c *Config) BlobDir() string { return filepath.Join(c.Dir, "blob") }

// IndexDir is the Pebble directory. Exactly one process may hold it.
func (c *Config) IndexDir() string { return filepath.Join(c.Dir, "index") }

// maxSocketPath is the longest unix socket path we will use.
//
// The kernel's sockaddr_un.sun_path is 108 bytes on Linux and 104 on macOS,
// including the terminator, and bind fails with EINVAL past it. The margin
// below the smaller limit leaves room for the filename itself.
const maxSocketPath = 100

// SocketDir is the directory holding the daemon's unix socket.
//
// It normally is the cache directory itself, which keeps everything the tool
// creates under one root. A deep cache directory can exceed the kernel's
// socket path limit, though, so an over-long path falls back to a directory
// under the system temporary directory, named from a digest of the cache
// directory. The derivation is deterministic, so every client of a given cache
// agrees on where to look.
//
// The fallback is a directory rather than a bare filename because the socket's
// permissions cannot be set atomically: bind creates it with umask-derived
// permissions and only a following chmod narrows it. Holding the socket inside
// a directory the owner alone may traverse closes that window, which matters
// most for the fallback, since the system temporary directory is world
// writable.
func (c *Config) SocketDir() string {
	if len(filepath.Join(c.Dir, socketName)) <= maxSocketPath {
		return c.Dir
	}
	sum := sha256.Sum256([]byte(c.Dir))
	return filepath.Join(os.TempDir(), fmt.Sprintf("plaid-cache-%x", sum[:8]))
}

// socketName is the socket's filename within SocketDir.
const socketName = "plaid-cache.sock"

// SocketPath is the daemon's unix socket.
func (c *Config) SocketPath() string { return filepath.Join(c.SocketDir(), socketName) }

// LogPath is where a daemon spawned in the background sends its output. A
// detached daemon has no terminal to inherit, so without this its diagnostics
// would be lost.
func (c *Config) LogPath() string { return filepath.Join(c.Dir, "plaid-cache.log") }

// RemoteEnabled reports whether a remote tier is configured.
func (c *Config) RemoteEnabled() bool { return c.S3Bucket != "" }

// resolveDir returns the local cache root, honoring the documented chain:
// PLAID_GOCACHE_DIR, then XDG_CACHE_HOME, then os.UserCacheDir.
func resolveDir() (string, error) {
	if d := os.Getenv("PLAID_GOCACHE_DIR"); d != "" {
		abs, err := filepath.Abs(d)
		if err != nil {
			return "", fmt.Errorf("resolveDir: PLAID_GOCACHE_DIR: %w", err)
		}
		return abs, nil
	}
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "plaid-cache"), nil
	}
	d, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolveDir: %w", err)
	}
	return filepath.Join(d, "plaid-cache"), nil
}

// envDuration reads a Go duration, e.g. "168h" or "90m".
func envDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("envDuration: %s: %q is not a duration (want e.g. 168h, 90m)", name, v)
	}
	if d < 0 {
		return 0, fmt.Errorf("envDuration: %s: got %v, want >= 0", name, d)
	}
	return d, nil
}

// envInt reads a plain integer.
func envInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("envInt: %s: %q is not an integer", name, v)
	}
	return n, nil
}

// envBool treats only "1" as true and only "" or "0" as false, rejecting
// anything else. Accepting "false" but silently ignoring "no" is the kind of
// near-miss that makes a disable flag appear not to work.
func envBool(name string) (bool, error) {
	switch v := os.Getenv(name); v {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("envBool: %s: got %q, want one of: 0, 1", name, v)
	}
}

// envLogLevel reads a log level name.
func envLogLevel(name string, def LogLevel) (LogLevel, error) {
	switch v := os.Getenv(name); v {
	case "":
		return def, nil
	case "off":
		return LogOff, nil
	case "error":
		return LogError, nil
	case "info":
		return LogInfo, nil
	case "debug":
		return LogDebug, nil
	default:
		return 0, fmt.Errorf("envLogLevel: %s: got %q, want one of: off, error, info, debug", name, v)
	}
}

// byteUnits maps a suffix to its multiplier. Both SI and IEC spellings are
// accepted because operators reach for both, and quietly interpreting "GB" as
// 2^30 or "GiB" as 10^9 would misreport a disk budget by 7%.
var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
	{"B", 1},
}

// envBytes reads a byte quantity, accepting a bare integer or a suffixed one.
func envBytes(name string, def int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def, nil
	}
	n, err := ParseBytes(v)
	if err != nil {
		return 0, fmt.Errorf("envBytes: %s: %w", name, err)
	}
	return n, nil
}

// ParseBytes converts a human-written byte quantity such as "20GB", "512MiB",
// or "1048576" into a count of bytes.
func ParseBytes(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	if t == "" {
		return 0, fmt.Errorf("ParseBytes: empty value")
	}
	for _, u := range byteUnits {
		num, ok := strings.CutSuffix(t, u.suffix)
		if !ok {
			continue
		}
		num = strings.TrimSpace(num)
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("ParseBytes: %q is not a byte size (want e.g. 20GB, 512MiB, 1048576)", s)
		}
		if f < 0 {
			return 0, fmt.Errorf("ParseBytes: %q is negative", s)
		}
		return int64(f * float64(u.mult)), nil
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ParseBytes: %q is not a byte size (want e.g. 20GB, 512MiB, 1048576)", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("ParseBytes: %q is negative", s)
	}
	return n, nil
}

// FormatBytes renders a byte count in IEC units for human display.
func FormatBytes(n int64) string {
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
