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
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// This file adversarially tests the segment-reference feature across long
// backup chains. Each subtest drives the real production path — Run (combine),
// SaveSegmentMap (compressed per-source persistence), LoadSegmentMap, and the
// installPriorSegments size gate — over many generations of random churn
// (change, grow, shrink, uninitialized-hole zero-fill, and mid-chain segment
// size changes), asserting a byte-exact restore against an independent model at
// every generation. Seeds are fixed so failures reproduce.

// chainFile is one relation tracked across a chain: its logical path in the
// snapshot tree and its current block contents (the model of truth).
type chainFile struct {
	path   string
	blocks [][]byte
}

// genState holds the state carried from one generation to the next.
type genState struct {
	priorRoot     object.ID
	priorSegs     map[string][]object.ID
	priorSegBlock int
}

func TestSegmentChainByteFaithful(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Parallel()
			runSegmentChain(t, seed)
		})
	}
}

//nolint:cyclop,funlen // one self-contained chain simulator; splitting hurts clarity
func runSegmentChain(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test fixture, not crypto

	configFile := newRepo(ctx, t)

	// Two relations in the same directory exercise per-file segment-map keying.
	files := []*chainFile{
		{path: "base/1/1259", blocks: randomBlocks(rng, 1+rng.Intn(9))},
		{path: "base/1/2619", blocks: randomBlocks(rng, 1+rng.Intn(9))},
	}

	const (
		generations = 10
		blockSize   = testBlockSize
	)

	// segBlocks is the current segment size in blocks; it occasionally changes
	// mid-chain to exercise the size-mismatch rebuild-all path and re-enable.
	segBlocks := 2 + rng.Intn(2) // 2 or 3 blocks per segment
	var st genState

	// Coverage counters: the chain is only adversarial if it actually exercises
	// referencing, rebuild-all, and zero-fill. Asserted non-vacuous at the end.
	var totalRef, totalRebuilt, rebuildAllGens, sizeChanges int
	lastSeg := segBlocks

	for gen := range generations {
		incremental := gen > 0

		// Change the segment size mid-chain (never on the full): forced once at a
		// fixed generation so every seed exercises the size-mismatch rebuild-all
		// and whole-file-carry path deterministically, plus random changes.
		if incremental && (gen == generations/2 || rng.Intn(4) == 0) {
			for {
				n := 1 + rng.Intn(3)
				if n != segBlocks {
					segBlocks = n

					break
				}
			}
		}
		segBytes := segBlocks * blockSize

		// Mutate every file's model and build this generation's tar members.
		entries := make([]tarEntry, 0, 2+len(files))
		entries = append(entries,
			tarEntry{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: time.Unix(1_700_000_000, 0)},
			tarEntry{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: time.Unix(1_700_000_000, 0)},
		)
		for _, f := range files {
			prior := f.blocks
			desired := mutateBlocks(rng, prior)
			f.blocks = desired

			name, data := memberFor(t, f.path, prior, desired, incremental)
			entries = append(entries, tarEntry{
				name: name, typeflag: tar.TypeReg, mode: 0o600, data: data,
				modTime: time.Unix(1_700_000_000, 0),
			})
		}
		incTar := buildTar(t, entries)

		// Decide, exactly as installPriorSegments does, whether to reference.
		wantReference := incremental && st.priorSegs != nil && st.priorSegBlock == segBlocks

		res := ingestChainGen(ctx, t, configFile, incTar, segBytes, incremental,
			st.priorRoot, wantReference, st.priorSegs)

		if !wantReference && res.Stats.ReferencedSegments != 0 {
			t.Fatalf("gen %d: referenced %d segments with no matching prior map",
				gen, res.Stats.ReferencedSegments)
		}

		totalRef += int(res.Stats.ReferencedSegments)
		totalRebuilt += int(res.Stats.RebuiltSegments)
		if incremental && !wantReference {
			rebuildAllGens++
		}
		if segBlocks != lastSeg {
			sizeChanges++
			lastSeg = segBlocks
		}

		// Restore and verify every file byte-for-byte against the model.
		outDir := t.TempDir()
		restoreSnapshot(ctx, t, configFile, res.ManifestID, outDir)
		for _, f := range files {
			assertFileEqual(t, filepath.Join(outDir, f.path), bytes.Join(f.blocks, nil))
		}

		// Persist this generation's segment map (compressed snapshot) and load it
		// back for the next generation, mirroring production.
		priorRoot, segs, segBlk := roundTripSegmap(ctx, t, configFile, res)
		st = genState{priorRoot: priorRoot, priorSegs: segs, priorSegBlock: segBlk}
	}

	t.Logf("seed %d coverage: referenced=%d rebuilt=%d rebuildAllGens=%d sizeChanges=%d",
		seed, totalRef, totalRebuilt, rebuildAllGens, sizeChanges)
	if totalRef == 0 {
		t.Fatalf("seed %d never referenced a prior segment; chain is not exercising the feature", seed)
	}
	if rebuildAllGens == 0 {
		t.Fatalf("seed %d never hit a rebuild-all (size-mismatch) generation; the carry/degrade path is untested", seed)
	}
	if sizeChanges == 0 {
		t.Fatalf("seed %d never changed segment size; the size-gate path is untested", seed)
	}
}

// ingestChainGen ingests one generation. For an incremental it builds a prior
// image rooted at priorRoot and, when wantReference is true, installs the prior
// segment map so unchanged segments are referenced.
func ingestChainGen(
	ctx context.Context, t *testing.T, configFile string, tarBytes []byte,
	segBytes int, incremental bool, priorRoot object.ID,
	wantReference bool, priorSegs map[string][]object.ID,
) *Result {
	t.Helper()

	return ingestTar(ctx, t, configFile, tarBytes,
		Options{
			Source: defaultSource(), Incremental: incremental,
			BlockSize: testBlockSize, SegmentBytes: segBytes,
		},
		func(w repo.RepositoryWriter, opts *Options) {
			if !incremental {
				return
			}
			prior := NewKopiaPriorImage(ctx, w, priorRoot)
			if wantReference {
				prior.SetSegments(priorSegs)
			}
			opts.Prior = prior
		})
}

// roundTripSegmap persists res.SegmentMap as a compressed snapshot and loads it
// back, returning the data snapshot root, the parsed prior segments, and the
// recorded segment size — exactly what the next generation resolves in
// production.
func roundTripSegmap(
	ctx context.Context, t *testing.T, configFile string, res *Result,
) (object.ID, map[string][]object.ID, int) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	dataMan, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(res.ManifestID))
	if err != nil {
		t.Fatalf("load data snapshot: %v", err)
	}

	var segManID string
	if werr := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test-segmap"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			id, serr := SaveSegmentMap(ctx, w, defaultSource(),
				map[string]string{"klio.io/tag": "gen", "klio.io/content": "pgdata-segmap"},
				"segment map", res.SegmentMap)
			segManID = id

			return serr
		}); werr != nil {
		t.Fatalf("save segment map: %v", werr)
	}

	segMan, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(segManID))
	if err != nil {
		t.Fatalf("load segmap snapshot: %v", err)
	}
	segBlocks, segs, err := LoadSegmentMap(ctx, rep, segMan.RootEntry.ObjectID)
	if err != nil {
		t.Fatalf("load segment map: %v", err)
	}

	return dataMan.RootEntry.ObjectID, segs, segBlocks
}

// randomBlocks returns n distinct, non-zero blocks.
func randomBlocks(rng *rand.Rand, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = freshBlock(rng)
	}

	return out
}

// mutateBlocks returns a new desired state derived from prior: a random new
// length (grow/shrink/same) with a random subset of blocks changed, some grown
// blocks left as uninitialized zero holes.
func mutateBlocks(rng *rand.Rand, prior [][]byte) [][]byte {
	newLen := max(len(prior)+rng.Intn(5)-2, 1) // -2..+2, floored at 1
	out := make([][]byte, newLen)
	for i := range out {
		switch {
		case i < len(prior):
			if rng.Intn(3) == 0 {
				out[i] = freshBlock(rng) // changed
			} else {
				out[i] = prior[i] // unchanged, inherited
			}
		case rng.Intn(3) == 0:
			out[i] = make([]byte, testBlockSize) // uninitialized hole, zero-filled
		default:
			out[i] = freshBlock(rng) // new content
		}
	}

	return out
}

// memberFor builds the tar member for a file: the full file bytes for a full
// backup, or an INCREMENTAL.* member (relative to prior) for an incremental.
func memberFor(t *testing.T, path string, prior, desired [][]byte, incremental bool) (string, []byte) {
	t.Helper()
	if !incremental {
		return path, bytes.Join(desired, nil)
	}

	dir, base := filepath.Split(path)
	truncation := uint32(len(desired)) //nolint:gosec // small test length

	var (
		nums []uint32
		data [][]byte
	)
	for i, blk := range desired {
		inheritPrior := i < len(prior) && bytes.Equal(blk, prior[i])
		zeroHole := i >= len(prior) && isZeroBlock(blk)
		if inheritPrior || zeroHole {
			continue // absent: taken from prior or zero-filled by the combine
		}
		nums = append(nums, uint32(i))
		data = append(data, blk)
	}

	return dir + IncrementalFilePrefix + base, buildIncrementalMember(truncation, nums, data)
}

// buildIncrementalMember builds an INCREMENTAL.* member with arbitrary present
// block data (buildIncrementalFile only emits its fixed presentBlock pattern),
// at the default test block size.
func buildIncrementalMember(truncation uint32, nums []uint32, data [][]byte) []byte {
	return buildIncrementalMemberSized(truncation, nums, data, testBlockSize)
}

// buildIncrementalMemberSized is buildIncrementalMember for a caller using a
// block size other than testBlockSize; the header padding depends on it.
func buildIncrementalMemberSized(truncation uint32, nums []uint32, data [][]byte, blockSize int) []byte {
	var buf bytes.Buffer
	w32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	w32(incrementalMagic)
	w32(uint32(len(nums))) //nolint:gosec // small test length
	w32(truncation)
	for _, n := range nums {
		w32(n)
	}
	if len(nums) > 0 {
		for buf.Len()%blockSize != 0 {
			buf.WriteByte(0)
		}
		for _, d := range data {
			buf.Write(d)
		}
	}

	return buf.Bytes()
}

// freshBlock returns a distinct non-zero block. A 4-byte random nonce plus a
// fill byte make accidental equality with another block or with a zero block
// vanishingly unlikely.
func freshBlock(rng *rand.Rand) []byte {
	b := make([]byte, testBlockSize)
	binary.LittleEndian.PutUint32(b, rng.Uint32())
	fill := byte(rng.Intn(255) + 1) //nolint:gosec // 1..255 fits a byte
	for i := 4; i < len(b); i++ {
		b[i] = fill
	}

	return b
}

func isZeroBlock(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}

	return true
}
