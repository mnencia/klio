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
