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
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// gcTestBlockSize is PostgreSQL's real BLCKSZ; the small testBlockSize used
// elsewhere is too tiny to make a segment split into multiple Kopia contents.
const gcTestBlockSize = 8192

// TestReferencedSegmentSurvivesGC is the regression test for the GC-reachability
// bug: a segment map persisted only as object-ID strings did not keep the
// segment objects reachable, so garbage collection deleted the seek-table
// content of a multi-content (indirect) segment, and the next incremental that
// referenced it failed with "content not found". SaveSegmentMap now references
// each segment object from the map's snapshot so GC keeps them.
//
// The repo uses a FIXED-128K splitter and 256 KiB segments, so each segment is
// a two-content indirect object (the vulnerable shape). The chain is: full ->
// persist segment map -> FULL GC -> incremental that changes one block (leaving
// the other segment clean and referenced). Before the fix this panics in the
// combine; after it, the incremental restores byte-for-byte.
func TestReferencedSegmentSurvivesGC(t *testing.T) {
	ctx := context.Background()
	cfg := newRepoWithSplitter(ctx, t, "FIXED-128K")
	const (
		nBlocks  = 64        // 512 KiB relation
		segBytes = 256 << 10 // 256 KiB segments -> 2 segments, each 2 contents (indirect)
	)
	modTime := time.Unix(1_700_000_000, 0)

	fullBlocks := make([][]byte, nBlocks)
	for i := range fullBlocks {
		fullBlocks[i] = gcBlock(i, 0)
	}
	fullTar := gcTar(t, "base/1/1259", bytes.Join(fullBlocks, nil), modTime)
	fullMan, segMan := gcIngest(ctx, t, cfg,
		Options{Source: defaultSource(), BlockSize: gcTestBlockSize, SegmentBytes: segBytes}, fullTar, "", "")

	// Full GC deletes any content unreferenced by a retained snapshot. Without
	// the fix this removes the clean segment's seek-table content.
	gcFull(ctx, t, cfg)

	// Incremental changing block 0: segment 0 is rebuilt, segment 1 stays clean
	// and is referenced from the prior — its seek-table must have survived GC.
	incTar := gcTar(t, "base/1/"+IncrementalFilePrefix+"1259", gcIncrMember(nBlocks, []uint32{0}), modTime)
	incMan, _ := gcIngest(ctx, t, cfg,
		Options{Source: defaultSource(), Incremental: true, BlockSize: gcTestBlockSize, SegmentBytes: segBytes},
		incTar, fullMan, segMan)

	out := filepath.Join(t.TempDir(), "restored")
	restoreSnapshot(ctx, t, cfg, incMan, out)

	want := make([][]byte, nBlocks)
	want[0] = gcBlock(0, 1)
	for i := 1; i < nBlocks; i++ {
		want[i] = gcBlock(i, 0)
	}
	assertFileEqual(t, filepath.Join(out, "base/1/1259"), bytes.Join(want, nil))
}

// gcIngest runs an ingest and persists its segment map in the same write
// session, returning the data and segment-map snapshot manifest IDs. When
// priorData is set it combines against that prior, resolving the prior segments
// from priorSeg via LoadSegmentMap (exercising the persisted, GC-safe path).
func gcIngest(
	ctx context.Context, t *testing.T, cfg string, opts Options, tarBytes []byte, priorData, priorSeg string,
) (string, string) {
	t.Helper()
	rep, err := repo.Open(ctx, cfg, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	var dataMan, segMan string
	werr := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "gc-test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			if priorData != "" {
				dman, err := snapshot.LoadSnapshot(ctx, w, manifest.ID(priorData))
				if err != nil {
					return err
				}
				pi := NewKopiaPriorImage(ctx, w, dman.RootEntry.ObjectID)
				sman, err := snapshot.LoadSnapshot(ctx, w, manifest.ID(priorSeg))
				if err != nil {
					return err
				}
				_, segs, err := LoadSegmentMap(ctx, w, sman.RootEntry.ObjectID)
				if err != nil {
					return err
				}
				pi.SetSegments(segs)
				opts.Prior = pi
			}
			res, runErr := Run(ctx, w, bytes.NewReader(tarBytes), opts)
			if runErr != nil {
				return runErr
			}
			dataMan = res.ManifestID
			id, serr := SaveSegmentMap(ctx, w, defaultSource(),
				map[string]string{"klio.io/tag": "b", "klio.io/content": "pgdata-segmap"},
				"segment map", res.SegmentMap)
			segMan = id

			return serr
		})
	if werr != nil {
		t.Fatalf("ingest: %v", werr)
	}

	return dataMan, segMan
}

// gcFull runs a full maintenance pass with SafetyNone, deleting all content not
// reachable from a retained snapshot.
func gcFull(ctx context.Context, t *testing.T, cfg string) {
	t.Helper()
	rep, err := repo.Open(ctx, cfg, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	dr, ok := rep.(repo.DirectRepository)
	if !ok {
		t.Fatal("not a direct repository")
	}
	if err := repo.DirectWriteSession(ctx, dr, repo.WriteSessionOptions{Purpose: "gc-test"},
		func(ctx context.Context, dw repo.DirectRepositoryWriter) error {
			return snapshotmaintenance.Run(ctx, dw, maintenance.ModeFull, true, maintenance.SafetyNone)
		}); err != nil {
		t.Fatalf("maintenance: %v", err)
	}
}

func newRepoWithSplitter(ctx context.Context, t *testing.T, splitter string) string {
	t.Helper()
	dir := t.TempDir()
	storage := filepath.Join(dir, "storage")
	st, err := filesystem.New(ctx, &filesystem.Options{Path: storage}, true)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	if err := repo.Initialize(ctx, st,
		&repo.NewRepositoryOptions{ObjectFormat: format.ObjectFormat{Splitter: splitter}}, testPassword); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	cfg := filepath.Join(dir, "repo.config")
	if err := repo.Connect(ctx, cfg, st, testPassword, nil); err != nil {
		t.Fatalf("connect: %v", err)
	}

	return cfg
}

// gcBlock returns a distinct pseudo-random BLCKSZ block for (index, version).
func gcBlock(index, version int) []byte {
	b := make([]byte, gcTestBlockSize)
	rand.New(rand.NewSource(int64(index)*1_000_003 + int64(version) + 1)).Read(b) //nolint:gosec

	return b
}

func gcTar(t *testing.T, name string, data []byte, modTime time.Time) []byte {
	t.Helper()

	return buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: name, typeflag: tar.TypeReg, mode: 0o600, data: data, modTime: modTime},
	})
}

// gcIncrMember builds an INCREMENTAL.* member: header + changed block data
// (changed blocks get version-1 content).
func gcIncrMember(nBlocks int, changed []uint32) []byte {
	var buf bytes.Buffer
	w32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	w32(incrementalMagic)
	w32(uint32(len(changed))) //nolint:gosec
	w32(uint32(nBlocks))      //nolint:gosec
	for _, n := range changed {
		w32(n)
	}
	for buf.Len()%gcTestBlockSize != 0 {
		buf.WriteByte(0)
	}
	for _, n := range changed {
		buf.Write(gcBlock(int(n), 1))
	}

	return buf.Bytes()
}
