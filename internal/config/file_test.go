// Copyright 2026 The plaid-cache authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfigFile puts a configuration file where the XDG lookup will find it.
func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	home := t.TempDir()
	dir := filepath.Join(home, "plaid-cache")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, configFileName)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("XDG_CONFIG_HOME", home)
	return path
}

// TestFileSuppliesDefaults pins the reason the file exists: settings that should
// apply to every build, without a shell profile being the only place to put them.
func TestFileSuppliesDefaults(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	path := writeConfigFile(t, `
# a comment, and a blank line above

PLAID_GOCACHE_S3_BUCKET = my-bucket
PLAID_GOCACHE_MAX_BYTES = 5GiB
PLAID_GOCACHE_TTL       = 24h
PLAID_GOCACHE_DIR       = `+dir+`
`)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3Bucket != "my-bucket" {
		t.Errorf("bucket = %q, want my-bucket", cfg.S3Bucket)
	}
	if cfg.MaxBytes != 5<<30 {
		t.Errorf("max bytes = %d, want %d", cfg.MaxBytes, int64(5)<<30)
	}
	if cfg.TTL != 24*time.Hour {
		t.Errorf("ttl = %v, want 24h", cfg.TTL)
	}
	if cfg.Dir != dir {
		t.Errorf("dir = %q, want %q", cfg.Dir, dir)
	}
	if cfg.ConfigFile != path {
		t.Errorf("ConfigFile = %q, want %q", cfg.ConfigFile, path)
	}
}

// TestEnvironmentBeatsTheFile pins the precedence.
//
// A file is a standing preference on one machine; a variable is a decision about
// the invocation in front of you, and a wrapper script or a CI job has no other
// way to express one. A file that could override those would make the tool
// unpredictable in exactly the places it is invoked automatically.
func TestEnvironmentBeatsTheFile(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "S3_BUCKET=from-file\nMAX_BYTES=1GiB\n")
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())
	t.Setenv("PLAID_GOCACHE_S3_BUCKET", "from-env")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3Bucket != "from-env" {
		t.Errorf("bucket = %q, want the environment to win", cfg.S3Bucket)
	}
	// The setting the environment says nothing about still comes from the file.
	if cfg.MaxBytes != 1<<30 {
		t.Errorf("max bytes = %d, want the file's 1GiB", cfg.MaxBytes)
	}
}

// TestFileKeysMayOmitThePrefix pins the spellings a person actually writes. The
// prefix is noise in a file that is already about this tool.
func TestFileKeysMayOmitThePrefix(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "s3-bucket = short\nDir = "+t.TempDir()+"\n")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3Bucket != "short" {
		t.Errorf("bucket = %q, want short", cfg.S3Bucket)
	}
}

// TestUnknownKeyIsAnError pins that a typo is refused rather than ignored.
//
// A setting that looks applied and is not is the whole risk of having a file at
// all: a mistyped size ceiling would silently revert to the default and let the
// cache grow until it filled the disk, which is the failure this tool exists to
// prevent.
func TestUnknownKeyIsAnError(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "PLAID_GOCACHE_MAX_BYTE=5GiB\n")

	_, err := Load()
	if err == nil {
		t.Fatal("a mistyped setting was accepted")
	}
	if !strings.Contains(err.Error(), "MAX_BYTE") {
		t.Fatalf("the error does not name the offending key: %v", err)
	}
}

// TestDuplicateKeyIsAnError pins that two contradictory lines are refused rather
// than one silently winning, which would leave a reader no way to tell which.
func TestDuplicateKeyIsAnError(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "MAX_BYTES=1GiB\nMAX_BYTES=2GiB\n")

	if _, err := Load(); err == nil {
		t.Fatal("a setting written twice was accepted")
	}
}

// TestMalformedLineIsAnError pins that a line that is not KEY=value is refused.
func TestMalformedLineIsAnError(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "this is not a setting\n")

	if _, err := Load(); err == nil {
		t.Fatal("a malformed line was accepted")
	}
}

// TestBadValueInAFileFailsLikeABadVariable pins that the file gets no leniency
// the environment would not get.
func TestBadValueInAFileFailsLikeABadVariable(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "MAX_BYTES=not-a-size\n")

	if _, err := Load(); err == nil {
		t.Fatal("an unparseable value in a file was accepted")
	}
}

// TestNoFileIsNormal pins that a machine without a configuration file works, which
// is how most of them are.
func TestNoFileIsNormal(t *testing.T) {
	clearEnv(t)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("PLAID_GOCACHE_DIR", t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with no configuration file: %v", err)
	}
	if cfg.ConfigFile != "" {
		t.Errorf("ConfigFile = %q with no file present", cfg.ConfigFile)
	}
}

// TestNamedFileMustExist pins that a caller who points at a path gets an error
// when it is not there. Ignoring the typo would silently apply a configuration
// they did not ask for.
func TestNamedFileMustExist(t *testing.T) {
	clearEnv(t)
	t.Setenv(configFileEnvVar, filepath.Join(t.TempDir(), "nope"))

	if _, err := Load(); err == nil {
		t.Fatal("a configuration file named explicitly and absent was ignored")
	}
}

// TestNamedFileWins pins the explicit override, which is what a test harness or a
// service manager uses to pin a configuration.
func TestNamedFileWins(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "S3_BUCKET=from-xdg\n")
	named := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(named, []byte("S3_BUCKET=from-named\nDIR="+t.TempDir()+"\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv(configFileEnvVar, named)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.S3Bucket != "from-named" {
		t.Errorf("bucket = %q, want the named file to win", cfg.S3Bucket)
	}
}

// TestFileCannotMoveEveryProgramsCacheRoot pins that XDG_CACHE_HOME is read from
// the environment only. It is the platform's setting rather than this tool's, and
// a file able to change it would be a surprising amount of reach.
func TestFileCannotMoveEveryProgramsCacheRoot(t *testing.T) {
	clearEnv(t)
	writeConfigFile(t, "PLAID_GOCACHE_MAX_BYTES=1GiB\n")
	if parsed, err := parseFile("XDG_CACHE_HOME=/somewhere\n"); err == nil {
		t.Fatalf("a file was allowed to set XDG_CACHE_HOME: %v", parsed)
	}
}

// TestValuesKeepDeliberateSpacing pins that quoting works, so a value whose
// spacing matters can be written.
func TestValuesKeepDeliberateSpacing(t *testing.T) {
	got, err := parseFile(`S3_PREFIX = " padded "` + "\n")
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	if got["PLAID_GOCACHE_S3_PREFIX"] != " padded " {
		t.Fatalf("prefix = %q, want the quoted spacing kept", got["PLAID_GOCACHE_S3_PREFIX"])
	}
}
