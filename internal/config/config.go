// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package config resolves plaid-cache's runtime configuration.
//
// Every setting has exactly one name and a documented fallback chain: the
// process environment, then the configuration file, then the default. The
// environment comes first because the tool is executed by the Go toolchain as a
// GOCACHEPROG plugin, which offers no way to pass flags — the environment is the
// only channel that reaches every invocation, so it has to be able to override a
// file a user wrote months ago and forgot.
//
// The file exists because that same constraint makes settings awkward to apply
// otherwise: a user who wants a bucket for every build has nowhere to put it but
// a shell profile. It uses the same names as the environment, so there is one
// vocabulary to learn rather than two with a mapping between them.
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
	// ConfigFile is the file settings were read from, empty when there was
	// none. It is reported by status: a setting coming from a file nobody
	// remembers writing is otherwise hard to account for.
	ConfigFile string

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

	// CompactAfterPruned is how many pruned entries must accumulate before the
	// index is compacted. Deletes in an LSM are writes, so pruning grows the
	// index until a compaction reclaims it, and Pebble's own background
	// compactions are driven by level fill, which a small index never reaches.
	CompactAfterPruned int64

	// BazelAddr is the TCP address the daemon serves Bazel's HTTP remote-cache
	// protocol on, e.g. "localhost:9095". Empty, the default, serves it not at
	// all: a second listener is something to ask for rather than something to
	// find running.
	//
	// It is a full address rather than a port so that "which interface" is an
	// explicit decision. A cache accepts bodies that later become the binaries
	// a machine runs, so the difference between loopback and every interface is
	// the difference between a private cache and a public one.
	BazelAddr string

	// DisableBazelVerify stops the Bazel listener from checking that an
	// uploaded CAS body hashes to the digest naming it. It exists for a client
	// whose digest function is not SHA-256, since only the bare hash appears in
	// the request path and a server cannot tell which function produced it.
	DisableBazelVerify bool

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
	defaultCompactAfter     = 1000
)

// Load resolves configuration from the environment and the configuration file.
func Load() (*Config, error) {
	file, path, err := loadFile()
	if err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	// The environment wins. A file is a set of defaults for a user's machine; a
	// variable is a decision about the invocation in front of you, and one that
	// a wrapper script or a CI job has no other way to express.
	src := func(name string) string {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			return v
		}
		return file[name]
	}

	dir, err := resolveDir(src)
	if err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}

	c := &Config{
		ConfigFile:        path,
		Dir:               dir,
		S3Bucket:          src("PLAID_GOCACHE_S3_BUCKET"),
		S3Region:          src("PLAID_GOCACHE_S3_REGION"),
		S3Prefix:          src("PLAID_GOCACHE_S3_PREFIX"),
		S3EndpointURL:     src("PLAID_GOCACHE_S3_ENDPOINT_URL"),
		BazelAddr:         src("PLAID_GOCACHE_BAZEL_ADDR"),
		UploadConcurrency: runtime.NumCPU(),
	}

	if c.MaxBytes, err = envBytes(src, "PLAID_GOCACHE_MAX_BYTES", defaultMaxBytes); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.TTL, err = envDuration(src, "PLAID_GOCACHE_TTL", defaultTTL); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.TouchGranularity, err = envDuration(src, "PLAID_GOCACHE_TOUCH_GRANULARITY", defaultTouchGranularity); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.MinUploadSize, err = envBytes(src, "PLAID_GOCACHE_MIN_UPLOAD_SIZE", defaultMinUploadSize); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.IdleTimeout, err = envDuration(src, "PLAID_GOCACHE_IDLE_TIMEOUT", defaultIdleTimeout); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.EvictInterval, err = envDuration(src, "PLAID_GOCACHE_EVICT_INTERVAL", defaultEvictInterval); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.CompactAfterPruned, err = envBytes(src, "PLAID_GOCACHE_COMPACT_AFTER", defaultCompactAfter); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.UploadConcurrency, err = envInt(src, "PLAID_GOCACHE_UPLOAD_CONCURRENCY", runtime.NumCPU()); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.UploadConcurrency < 1 {
		return nil, fmt.Errorf("Load: PLAID_GOCACHE_UPLOAD_CONCURRENCY: got %d, want >= 1", c.UploadConcurrency)
	}
	if c.DisableBazelVerify, err = envBool(src, "PLAID_GOCACHE_DISABLE_BAZEL_VERIFY"); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.DisableEviction, err = envBool(src, "PLAID_GOCACHE_DISABLE_EVICTION"); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.DisableDaemon, err = envBool(src, "PLAID_GOCACHE_DISABLE_DAEMON"); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	if c.Log, err = envLogLevel(src, "PLAID_GOCACHE_LOG", LogError); err != nil {
		return nil, fmt.Errorf("Load: %w", err)
	}
	return c, nil
}

// source resolves one setting by name, returning "" when nothing sets it.
type source func(name string) string

// configFileEnvVar names a configuration file explicitly, overriding the
// XDG lookup. A file named this way must exist: a caller who points at a path
// has stated it matters, and silently ignoring a typo would apply a
// configuration they did not ask for.
const configFileEnvVar = "PLAID_GOCACHE_CONFIG"

// configFileName is the file's name inside its directory.
const configFileName = "config"

// settingNames are the keys a configuration file may set.
//
// A key outside this set is an error rather than a warning. The whole risk of a
// configuration file is a setting that looks applied and is not, and a typo in a
// size ceiling that silently reverts to the default would let the cache grow
// until it filled the disk — the failure this tool exists to prevent.
var settingNames = map[string]bool{
	"PLAID_GOCACHE_DIR":                  true,
	"PLAID_GOCACHE_MAX_BYTES":            true,
	"PLAID_GOCACHE_TTL":                  true,
	"PLAID_GOCACHE_S3_BUCKET":            true,
	"PLAID_GOCACHE_S3_REGION":            true,
	"PLAID_GOCACHE_S3_PREFIX":            true,
	"PLAID_GOCACHE_S3_ENDPOINT_URL":      true,
	"PLAID_GOCACHE_MIN_UPLOAD_SIZE":      true,
	"PLAID_GOCACHE_UPLOAD_CONCURRENCY":   true,
	"PLAID_GOCACHE_TOUCH_GRANULARITY":    true,
	"PLAID_GOCACHE_IDLE_TIMEOUT":         true,
	"PLAID_GOCACHE_EVICT_INTERVAL":       true,
	"PLAID_GOCACHE_COMPACT_AFTER":        true,
	"PLAID_GOCACHE_BAZEL_ADDR":           true,
	"PLAID_GOCACHE_DISABLE_BAZEL_VERIFY": true,
	"PLAID_GOCACHE_DISABLE_EVICTION":     true,
	"PLAID_GOCACHE_DISABLE_DAEMON":       true,
	"PLAID_GOCACHE_LOG":                  true,
}

// ConfigFilePath returns where the configuration file is looked for, whether or
// not one is there.
func ConfigFilePath() (string, error) {
	if p := os.Getenv(configFileEnvVar); p != "" {
		return filepath.Abs(p)
	}
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "plaid-cache", configFileName), nil
	}
	d, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("ConfigFilePath: %w", err)
	}
	return filepath.Join(d, "plaid-cache", configFileName), nil
}

// loadFile reads the configuration file and returns its settings and the path
// they came from.
//
// An absent file is the normal case and not an error, unless the caller named
// one explicitly.
func loadFile() (map[string]string, string, error) {
	path, err := ConfigFilePath()
	if err != nil {
		return nil, "", err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && os.Getenv(configFileEnvVar) == "" {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("loadFile: %s: %w", path, err)
	}
	vals, err := parseFile(string(b))
	if err != nil {
		return nil, "", fmt.Errorf("loadFile: %s: %w", path, err)
	}
	return vals, path, nil
}

// parseFile reads KEY=value lines, the same names the environment uses.
//
// Using one vocabulary rather than a file schema and a mapping between them is
// the point: what a user reads in the documentation is what they write in the
// file, and a setting can be moved between a shell and a file by copying the
// line. The PLAID_GOCACHE_ prefix may be left off, since repeating it on every
// line of a file that is already about plaid-cache is only noise.
func parseFile(text string) (map[string]string, error) {
	vals := map[string]string{}
	for n, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("line %d: %q is not KEY=value", n+1, raw)
		}
		key := normalizeKey(k)
		if !settingNames[key] {
			return nil, fmt.Errorf("line %d: unknown setting %q", n+1, strings.TrimSpace(k))
		}
		if _, dup := vals[key]; dup {
			// Last-wins would leave one of two contradictory lines silently
			// dead, and a reader no way to tell which.
			return nil, fmt.Errorf("line %d: %s is set twice", n+1, key)
		}
		vals[key] = unquote(strings.TrimSpace(v))
	}
	return vals, nil
}

// normalizeKey accepts the spellings a person actually writes: any case, dashes
// for underscores, and the shared prefix left off.
func normalizeKey(k string) string {
	key := strings.ToUpper(strings.TrimSpace(k))
	key = strings.ReplaceAll(key, "-", "_")
	if !strings.HasPrefix(key, "PLAID_GOCACHE_") {
		key = "PLAID_GOCACHE_" + key
	}
	return key
}

// unquote strips one layer of matching quotes, so a value with trailing spaces
// can be written deliberately.
func unquote(v string) string {
	if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
		return v[1 : len(v)-1]
	}
	return v
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
//
// XDG_CACHE_HOME is read from the environment only. It is the platform's
// setting, not this tool's, and a file that could move every program's cache
// root would be a surprising thing for this file to be able to do.
func resolveDir(src source) (string, error) {
	if d := src("PLAID_GOCACHE_DIR"); d != "" {
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
func envDuration(src source, name string, def time.Duration) (time.Duration, error) {
	v := src(name)
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
func envInt(src source, name string, def int) (int, error) {
	v := src(name)
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
func envBool(src source, name string) (bool, error) {
	switch v := src(name); v {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("envBool: %s: got %q, want one of: 0, 1", name, v)
	}
}

// envLogLevel reads a log level name.
func envLogLevel(src source, name string, def LogLevel) (LogLevel, error) {
	switch v := src(name); v {
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
func envBytes(src source, name string, def int64) (int64, error) {
	v := strings.TrimSpace(src(name))
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
