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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// setGlobalCompression sets the repository's global compression policy, the same
// knob `kopia policy set --compression` writes.
func setGlobalCompression(ctx context.Context, t *testing.T, configFile string, name compression.Name) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	if err := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "policy"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			return policy.SetPolicy(ctx, w, policy.GlobalPolicySourceInfo, &policy.Policy{
				CompressionPolicy: policy.CompressionPolicy{CompressorName: name},
			})
		}); err != nil {
		t.Fatalf("set compression policy: %v", err)
	}
}

// repoStorageSize sums the on-disk bytes of a filesystem-backed repository.
func repoStorageSize(t *testing.T, configFile string) int64 {
	t.Helper()

	storageDir := filepath.Join(filepath.Dir(configFile), "storage")
	var total int64
	if err := filepath.WalkDir(storageDir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, e := d.Info(); e == nil {
				total += info.Size()
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("walk storage: %v", err)
	}

	return total
}

// TestIngestCompressionFollowsPolicy proves the helper compresses segment data
// according to the repository's effective compression policy: with a global
// zstd policy set, resolveCompressor picks it up, highly compressible data
// stores far smaller than in an unpoliced repo, and the result still restores
// byte-exact.
func TestIngestCompressionFollowsPolicy(t *testing.T) {
	ctx := context.Background()
	modTime := time.Unix(1_700_000_000, 0)

	// Highly compressible relation file spanning several 128 KB segments.
	data := bytes.Repeat([]byte("PostgreSQL page payload padding 0123456789ABCDEF\n"), 12000)
	tarBytes := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: data, modTime: modTime},
	})

	src := snapshot.SourceInfo{Host: "h", UserName: "u", Path: "/pgdata"}
	opts := Options{Source: src, Tags: map[string]string{"klio.io/tag": "t", "klio.io/content": "pgdata"}}

	plain := newRepo(ctx, t)
	plainRes := ingestTar(ctx, t, plain, tarBytes, opts, nil)
	plainSize := repoStorageSize(t, plain)

	comp := newRepo(ctx, t)
	setGlobalCompression(ctx, t, comp, "zstd-fastest")

	rep, err := repo.Open(ctx, comp, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	if got := resolveCompressor(ctx, rep, src); got != "zstd-fastest" {
		_ = rep.Close(ctx)
		t.Fatalf("resolveCompressor = %q, want zstd-fastest", got)
	}
	_ = rep.Close(ctx)

	compRes := ingestTar(ctx, t, comp, tarBytes, opts, nil)
	compSize := repoStorageSize(t, comp)

	if compSize >= plainSize/2 {
		t.Fatalf("expected compressed repo to be far smaller: compressed=%d plain=%d", compSize, plainSize)
	}

	for _, tc := range []struct{ name, cfg, man string }{
		{"plain", plain, plainRes.ManifestID},
		{"compressed", comp, compRes.ManifestID},
	} {
		out := filepath.Join(t.TempDir(), tc.name)
		restoreSnapshot(ctx, t, tc.cfg, tc.man, out)
		assertFileEqual(t, filepath.Join(out, "base/1/1259"), data)
	}
}

// TestIncrementalRebuildCompressesSegments proves a segment rebuilt from an
// INCREMENTAL.* member is compressed by the same policy as a freshly written
// one. The rebuild path assembles the segment in a pool buffer and hands it to
// the shared writer, so it could silently lose the compressor that the whole-file
// path applies; this checks the stored content of a rebuilt segment directly.
func TestIncrementalRebuildCompressesSegments(t *testing.T) {
	ctx := context.Background()
	modTime := time.Unix(1_700_000_000, 0)

	const (
		blockSize = 1024
		segBlocks = 4
		blocks    = 8
	)

	// Highly compressible blocks, distinct from each other.
	blockOf := func(b byte) []byte { return bytes.Repeat([]byte{b}, blockSize) }
	full := make([]byte, 0, blocks*blockSize)
	for i := range blocks {
		full = append(full, blockOf(byte('a'+i))...)
	}

	configFile := newRepo(ctx, t)
	setGlobalCompression(ctx, t, configFile, "zstd-fastest")

	opts := Options{
		Source:       defaultSource(),
		BlockSize:    blockSize,
		SegmentBytes: segBlocks * blockSize,
	}
	fullRes := ingestTar(ctx, t, configFile, buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: full, modTime: modTime},
	}), opts, nil)

	// Change block 5, which puts segment 1 (blocks 4..7) on the rebuild path
	// while segment 0 is taken by reference.
	changed := bytes.Repeat([]byte{'Z'}, blockSize)
	incOpts := opts
	incOpts.Incremental = true
	incRes := ingestTar(ctx, t, configFile,
		buildTar(t, []tarEntry{
			{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
			{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
			{
				name: "base/1/INCREMENTAL.1259", typeflag: tar.TypeReg, mode: 0o600,
				data:    buildIncrementalMemberSized(blocks, []uint32{5}, [][]byte{changed}, blockSize),
				modTime: modTime,
			},
		}), incOpts,
		func(w repo.RepositoryWriter, o *Options) {
			priorMan, err := snapshot.LoadSnapshot(ctx, w, manifest.ID(fullRes.ManifestID))
			if err != nil {
				t.Fatalf("load prior snapshot: %v", err)
			}
			o.Prior = NewKopiaPriorImage(ctx, w, priorMan.RootEntry.ObjectID)
		})

	// No prior segment map is installed in this fixture, so nothing can be taken
	// by reference and both segments go through the rebuild path: one assembled
	// from the changed block plus prior blocks, one entirely from prior blocks.
	if incRes.Stats.RebuiltSegments != 2 || incRes.Stats.ReferencedSegments != 0 {
		t.Fatalf("rebuilt=%d referenced=%d, want 2 and 0",
			incRes.Stats.RebuiltSegments, incRes.Stats.ReferencedSegments)
	}

	ids := incRes.SegmentMap.Files["base/1/1259"]
	if len(ids) != 2 {
		t.Fatalf("segment map has %d segments, want 2: %v", len(ids), ids)
	}
	for i, id := range ids {
		assertContentCompressed(ctx, t, configFile, id, i)
	}
}

// assertContentCompressed requires the single-content object to be stored
// smaller than its logical size, which only happens if a compressor was applied.
func assertContentCompressed(ctx context.Context, t *testing.T, configFile, oidStr string, index int) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	oid, err := object.ParseID(oidStr)
	if err != nil {
		t.Fatalf("parse object id %q: %v", oidStr, err)
	}
	// The object ID's own compression marker is only set when the object writer
	// compresses locally; when the repository supports content compression (as
	// here) the content manager does it, so the stored lengths are the evidence.
	cid, _, ok := oid.ContentID()
	if !ok {
		t.Fatalf("object %s is not a single content", oidStr)
	}

	info, err := rep.ContentInfo(ctx, cid)
	if err != nil {
		t.Fatalf("content info for %v: %v", cid, err)
	}
	if info.PackedLength >= info.OriginalLength {
		t.Fatalf("segment %d content %v not compressed: packed=%d original=%d",
			index, cid, info.PackedLength, info.OriginalLength)
	}
}
