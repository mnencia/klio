/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package ingest

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// These tests drive the real ingestion logic against a real (filesystem-backed)
// Kopia repository, with no PostgreSQL involved. They build the tar streams that
// pg_basebackup would send, ingest them, then restore the snapshot with Kopia's
// own restore code and check that the result is byte-faithful.

const testPassword = "test-password-1234567890"

// newRepo creates and connects a filesystem-backed Kopia repository, returning
// the path to its config file.
func newRepo(ctx context.Context, t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	storageDir := filepath.Join(dir, "storage")
	if err := os.MkdirAll(storageDir, 0o700); err != nil {
		t.Fatalf("mkdir storage: %v", err)
	}

	st, err := filesystem.New(ctx, &filesystem.Options{Path: storageDir}, true)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	if err := repo.Initialize(ctx, st, nil, testPassword); err != nil {
		t.Fatalf("initialize repo: %v", err)
	}

	configFile := filepath.Join(dir, "repo.config")
	if err := repo.Connect(ctx, configFile, st, testPassword, nil); err != nil {
		t.Fatalf("connect repo: %v", err)
	}

	return configFile
}

// tarEntry describes one member to write into a synthetic tar.
type tarEntry struct {
	name     string
	typeflag byte
	mode     int64
	data     []byte
	linkname string
	modTime  time.Time
}

func buildTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     e.mode,
			ModTime:  e.modTime,
			Linkname: e.linkname,
		}
		if e.typeflag == tar.TypeReg {
			hdr.Size = int64(len(e.data))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write tar header %q: %v", e.name, err)
		}
		if e.typeflag == tar.TypeReg {
			if _, err := tw.Write(e.data); err != nil {
				t.Fatalf("write tar body %q: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	return buf.Bytes()
}

// ingestTar opens the repository, runs the ingestion inside a write session, and
// returns the produced snapshot. configurePrior, when set, can attach a prior
// image to the options for an incremental ingest.
func ingestTar(
	ctx context.Context,
	t *testing.T,
	configFile string,
	tarBytes []byte,
	opts Options,
	configurePrior func(w repo.RepositoryWriter, opts *Options),
) *Result {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	var result *Result
	err = repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			if configurePrior != nil {
				configurePrior(w, &opts)
			}
			var runErr error
			result, runErr = Run(ctx, w, bytes.NewReader(tarBytes), opts)

			return runErr
		})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	return result
}

// restoreSnapshot restores the snapshot with the given manifest ID to targetDir.
func restoreSnapshot(ctx context.Context, t *testing.T, configFile, manifestID, targetDir string) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo for restore: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	man, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(manifestID))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	root, err := snapshotfs.SnapshotRoot(rep, man)
	if err != nil {
		t.Fatalf("snapshot root: %v", err)
	}

	output := &restore.FilesystemOutput{
		TargetPath:           targetDir,
		OverwriteDirectories: true,
		OverwriteFiles:       true,
		OverwriteSymlinks:    true,
		// The test runs as a non-root user, so skip restoring owner/group
		// (the synthetic tar headers carry uid/gid 0).
		SkipOwners: true,
	}
	if err := output.Init(ctx); err != nil {
		t.Fatalf("init restore output: %v", err)
	}

	_, err = restore.Entry(ctx, rep, output, root, restore.Options{
		// Restore everything in full; do not write shallow .kopia-entry
		// placeholders (the zero value would shallow-restore at depth 0).
		RestoreDirEntryAtDepth: math.MaxInt32,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
}

func defaultSource() snapshot.SourceInfo {
	return snapshot.SourceInfo{Host: "cluster-a", UserName: "streaming", Path: "/pgdata"}
}

// TestIngestRestoreIsByteFaithful ingests a tar that looks like a pg_basebackup
// main data directory (nested directories, files, a symlink) and checks that a
// restore reproduces the same tree and file contents.
func TestIngestRestoreIsByteFaithful(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	modTime := time.Unix(1_700_000_000, 0)
	pgVersion := []byte("16\n")
	label := []byte("START WAL LOCATION: 0/2000028\n")
	relation := bytes.Repeat([]byte{0xAB}, 8192)

	tarBytes := buildTar(t, []tarEntry{
		{name: "PG_VERSION", typeflag: tar.TypeReg, mode: 0o600, data: pgVersion, modTime: modTime},
		{name: "backup_label", typeflag: tar.TypeReg, mode: 0o600, data: label, modTime: modTime},
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: relation, modTime: modTime},
		{name: "pg_wal", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "current_logfiles", typeflag: tar.TypeSymlink, mode: 0o777, linkname: "log/postgresql.log", modTime: modTime},
	})

	result := ingestTar(ctx, t, configFile, tarBytes, Options{Source: defaultSource()}, nil)
	if result.ManifestID == "" || result.RootObjectID == "" {
		t.Fatalf("empty result: %+v", result)
	}
	if result.Stats.FileCount != 3 {
		t.Fatalf("file count = %d, want 3", result.Stats.FileCount)
	}

	out := filepath.Join(t.TempDir(), "restored")
	restoreSnapshot(ctx, t, configFile, result.ManifestID, out)

	assertFileEqual(t, filepath.Join(out, "PG_VERSION"), pgVersion)
	assertFileEqual(t, filepath.Join(out, "backup_label"), label)
	assertFileEqual(t, filepath.Join(out, "base/1/1259"), relation)

	if fi, err := os.Stat(filepath.Join(out, "base/1")); err != nil || !fi.IsDir() {
		t.Fatalf("base/1 not restored as a directory: %v", err)
	}
	if target, err := os.Readlink(filepath.Join(out, "current_logfiles")); err != nil || target != "log/postgresql.log" {
		t.Fatalf("symlink not restored: target=%q err=%v", target, err)
	}
}

// TestIncrementalCombineOnIngest takes a full backup of a relation segment, then
// ingests an incremental that changes some of its blocks, and checks that the
// restored file is the correct full image (changed blocks from the incremental,
// unchanged blocks from the prior backup).
func TestIncrementalCombineOnIngest(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	modTime := time.Unix(1_700_000_000, 0)

	// Full backup: a 4-block relation segment, each block distinct.
	fullRelation := bytes.Join([][]byte{
		priorBlock(0), priorBlock(1), priorBlock(2), priorBlock(3),
	}, nil)
	fullTar := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: fullRelation, modTime: modTime},
	})

	full := ingestTar(ctx, t, configFile, fullTar,
		Options{Source: defaultSource(), BlockSize: testBlockSize}, nil)

	// Incremental backup: blocks 1 and 3 changed, blocks 0 and 2 come from prior.
	incrementalMember := buildIncrementalFile(t, 4, []uint32{1, 3})
	incTar := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/INCREMENTAL.1259", typeflag: tar.TypeReg, mode: 0o600, data: incrementalMember, modTime: modTime},
	})

	inc := ingestTar(ctx, t, configFile, incTar,
		Options{Source: defaultSource(), Incremental: true, BlockSize: testBlockSize},
		func(w repo.RepositoryWriter, opts *Options) {
			priorMan, err := snapshot.LoadSnapshot(ctx, w, manifest.ID(full.ManifestID))
			if err != nil {
				t.Fatalf("load prior snapshot: %v", err)
			}
			opts.Prior = NewKopiaPriorImage(ctx, w, priorMan.RootEntry.ObjectID)
		})

	out := filepath.Join(t.TempDir(), "restored-incremental")
	restoreSnapshot(ctx, t, configFile, inc.ManifestID, out)

	// The INCREMENTAL. prefix must be gone, and the file must be the combined
	// full image.
	want := bytes.Join([][]byte{
		priorBlock(0), presentBlock(1), priorBlock(2), presentBlock(3),
	}, nil)
	assertFileEqual(t, filepath.Join(out, "base/1/1259"), want)

	if _, err := os.Stat(filepath.Join(out, "base/1/INCREMENTAL.1259")); !os.IsNotExist(err) {
		t.Fatalf("INCREMENTAL.1259 should not exist after combine, err=%v", err)
	}
}

// TestIngestHandlesOutOfOrderMembers feeds the tar members in a deliberately
// scrambled order: a deep file before any of its parent directory members, an
// explicit directory member after its own child, and global/pg_control last (as
// pg_basebackup sends it). A correct assembler must reproduce the tree
// regardless of arrival order. This would fail against an implementation that
// assumed depth-first, contiguous input.
func TestIngestHandlesOutOfOrderMembers(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	modTime := time.Unix(1_700_000_000, 0)
	dirModTime := time.Unix(1_700_000_500, 0)
	relation := bytes.Repeat([]byte{0xCD}, 8192)
	control := []byte("synthetic-pg_control")
	pgVersion := []byte("16\n")

	tarBytes := buildTar(t, []tarEntry{
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: relation, modTime: modTime},
		{name: "PG_VERSION", typeflag: tar.TypeReg, mode: 0o600, data: pgVersion, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o750, modTime: dirModTime},
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "global", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "global/pg_control", typeflag: tar.TypeReg, mode: 0o600, data: control, modTime: modTime},
	})

	result := ingestTar(ctx, t, configFile, tarBytes, Options{Source: defaultSource()}, nil)
	if result.Stats.FileCount != 3 {
		t.Fatalf("file count = %d, want 3", result.Stats.FileCount)
	}

	out := filepath.Join(t.TempDir(), "restored-unordered")
	restoreSnapshot(ctx, t, configFile, result.ManifestID, out)

	assertFileEqual(t, filepath.Join(out, "base/1/1259"), relation)
	assertFileEqual(t, filepath.Join(out, "PG_VERSION"), pgVersion)
	assertFileEqual(t, filepath.Join(out, "global/pg_control"), control)

	// base/1 was first auto-created (with default mode) when its child arrived;
	// the explicit directory member, sent later, must override that default.
	fi, err := os.Stat(filepath.Join(out, "base/1"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("base/1 not restored as a directory: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o750 {
		t.Fatalf("base/1 mode = %o, want 750 (explicit dir member must win)", perm)
	}
}

// TestIngestPreservesFileMetadata checks that file mode and modification time
// from the tar header survive a round trip through the snapshot. (uid/gid need
// root to restore, so the integration test skips owners; mode and mtime are
// asserted here.)
func TestIngestPreservesFileMetadata(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	modTime := time.Unix(1_700_010_000, 0)
	data := []byte("payload\n")

	tarBytes := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/file_640", typeflag: tar.TypeReg, mode: 0o640, data: data, modTime: modTime},
		{name: "base/file_600", typeflag: tar.TypeReg, mode: 0o600, data: data, modTime: modTime},
	})

	result := ingestTar(ctx, t, configFile, tarBytes, Options{Source: defaultSource()}, nil)

	out := filepath.Join(t.TempDir(), "restored-metadata")
	restoreSnapshot(ctx, t, configFile, result.ManifestID, out)

	wantModes := map[string]os.FileMode{
		"base/file_640": 0o640,
		"base/file_600": 0o600,
	}
	for name, wantMode := range wantModes {
		fi, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("stat %q: %v", name, err)
		}
		if perm := fi.Mode().Perm(); perm != wantMode {
			t.Fatalf("%q mode = %o, want %o", name, perm, wantMode)
		}
		if got := fi.ModTime().Unix(); got != modTime.Unix() {
			t.Fatalf("%q mtime = %d, want %d", name, got, modTime.Unix())
		}
	}
}

// TestPriorImageRejectsNonFileEntry checks that resolving a prior-image relation
// path fails loudly when a path component is not the expected type (a symlink or
// a directory where a regular file is required), rather than reading the wrong
// object's bytes as relation block data.
func TestPriorImageRejectsNonFileEntry(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)
	modTime := time.Unix(1_700_000_000, 0)

	relation := bytes.Repeat([]byte{0xAB}, testBlockSize)
	tarBytes := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1259", typeflag: tar.TypeReg, mode: 0o600, data: relation, modTime: modTime},
		{name: "current_logfiles", typeflag: tar.TypeSymlink, mode: 0o777, linkname: "log/x.log", modTime: modTime},
	})
	full := ingestTar(ctx, t, configFile, tarBytes,
		Options{Source: defaultSource(), BlockSize: testBlockSize}, nil)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	man, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(full.ManifestID))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	prior := NewKopiaPriorImage(ctx, rep, man.RootEntry.ObjectID)

	if _, err := prior.BlockReader("current_logfiles", testBlockSize); !errors.Is(err, ErrPriorEntryWrongType) {
		t.Fatalf("symlink as file: got %v, want ErrPriorEntryWrongType", err)
	}
	if _, err := prior.BlockReader("base", testBlockSize); !errors.Is(err, ErrPriorEntryWrongType) {
		t.Fatalf("directory as file: got %v, want ErrPriorEntryWrongType", err)
	}

	// A genuine regular file still resolves.
	r, err := prior.BlockReader("base/1259", testBlockSize)
	if err != nil {
		t.Fatalf("regular file: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func assertFileEqual(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path) //nolint:gosec // test reads a path it just restored
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file %q: got %d bytes, want %d bytes (content differs)", path, len(got), len(want))
	}
}
