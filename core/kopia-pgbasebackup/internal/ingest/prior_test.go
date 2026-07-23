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
	"io"
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
)

// TestKopiaPriorImageCachesDirManifests is the regression test for the
// directory-manifest cache in KopiaPriorImage: readDirManifest must return
// the exact same *snapshot.DirManifest for a repeated lookup of the same
// object ID, proving the second call was served from the cache rather than
// re-fetching and re-decoding the object. Without the cache, resolving many
// files that share a parent directory (as happens once per changed relation
// file in an incremental backup) would redundantly re-fetch and re-JSON-decode
// that directory's manifest every single time.
func TestKopiaPriorImageCachesDirManifests(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	tarBytes := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: []byte("relation-data")},
	})

	result := ingestTar(ctx, t, configFile, tarBytes, Options{Source: defaultSource()}, nil)
	if result.ManifestID == "" {
		t.Fatalf("empty manifest ID")
	}

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	man, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(result.ManifestID))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if man.RootEntry == nil {
		t.Fatalf("snapshot has no root entry")
	}

	p := NewKopiaPriorImage(ctx, rep, man.RootEntry.ObjectID)

	first, err := p.readDirManifest(man.RootEntry.ObjectID)
	if err != nil {
		t.Fatalf("first readDirManifest: %v", err)
	}

	second, err := p.readDirManifest(man.RootEntry.ObjectID)
	if err != nil {
		t.Fatalf("second readDirManifest: %v", err)
	}

	if first != second {
		t.Fatalf("readDirManifest returned different pointers for the same object ID: "+
			"got %p and %p, want a cache hit on the second call", first, second)
	}
}

// TestObjectBlockReaderZeroFillsBeyondLength checks the block boundary that
// drives the zero-fill fix: a block fully within the prior object reads
// normally, while a block that is not fully present (past the end, or a
// non-block-aligned partial tail) returns io.EOF and increments the
// zero-filled-block counter. This mirrors pg_combinebackup's floor(len/BLCKSZ)
// handling.
func TestObjectBlockReaderZeroFillsBeyondLength(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	// Two full blocks plus a partial tail, so the object length is not a whole
	// multiple of the block size.
	data := append(bytes.Repeat([]byte{1}, 2*testBlockSize), []byte("tail")...)
	tarBytes := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: data},
	})
	result := ingestTar(ctx, t, configFile, tarBytes,
		Options{Source: defaultSource(), BlockSize: testBlockSize}, nil)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	man, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(result.ManifestID))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	p := NewKopiaPriorImage(ctx, rep, man.RootEntry.ObjectID)
	br, err := p.BlockReader("base/1/1259", testBlockSize)
	if err != nil {
		t.Fatalf("block reader: %v", err)
	}
	defer br.Close() //nolint:errcheck

	// Blocks 0 and 1 are fully present.
	for _, n := range []uint32{0, 1} {
		if _, err := br.ReadBlock(n); err != nil {
			t.Fatalf("ReadBlock(%d): unexpected error %v", n, err)
		}
	}
	// Block 2 is only a partial tail, and block 5 is well past the end: both are
	// "not fully present" and must report io.EOF so the combine zero-fills them.
	for _, n := range []uint32{2, 5} {
		if _, err := br.ReadBlock(n); !errors.Is(err, io.EOF) {
			t.Fatalf("ReadBlock(%d): got %v, want io.EOF", n, err)
		}
	}

	if got := p.ZeroFilledBlocks(); got != 2 {
		t.Fatalf("ZeroFilledBlocks() = %d, want 2", got)
	}
}
