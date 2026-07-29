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
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
)

// TestConcurrentSegmentWritesMatchSequential ingests the same multi-segment tar
// twice into one repository, once with a single segment writer and once with
// many, and requires the two snapshots to be identical: same root object, same
// segment map, and a byte-faithful restore. Segments complete out of order under
// concurrency, so this is what guards the file's segment order (and therefore
// the reconstructed relation) against the writers' finishing order.
func TestConcurrentSegmentWritesMatchSequential(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	const (
		blockSize   = 8192
		segBytes    = 2 * blockSize
		segments    = 40
		shortTail   = 3000 // final segment shorter than segBytes
		manySegFile = "base/1/1249"
	)

	modTime := time.Unix(1_700_000_000, 0)

	// Every block distinct, so a misordered segment cannot restore correctly by
	// accident, and the tail is deliberately not segment-aligned.
	relation := make([]byte, 0, segments*segBytes+shortTail)
	for i := range segments*(segBytes/blockSize) + 1 {
		relation = append(relation, bytes.Repeat([]byte{byte(i), byte(i >> 8)}, blockSize/2)...)
	}
	relation = relation[:segments*segBytes+shortTail]

	tarBytes := buildTar(t, []tarEntry{
		{name: "PG_VERSION", typeflag: tar.TypeReg, mode: 0o600, data: []byte("17\n"), modTime: modTime},
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: manySegFile, typeflag: tar.TypeReg, mode: 0o600, data: relation, modTime: modTime},
		// A file shorter than one segment, and an empty one, on the same run.
		{name: "base/1/PG_VERSION", typeflag: tar.TypeReg, mode: 0o600, data: []byte("17\n"), modTime: modTime},
		{name: "base/1/pg_internal.init", typeflag: tar.TypeReg, mode: 0o600, data: nil, modTime: modTime},
	})

	opts := func(concurrency int) Options {
		return Options{
			Source:           defaultSource(),
			BlockSize:        blockSize,
			SegmentBytes:     segBytes,
			WriteConcurrency: concurrency,
		}
	}

	sequential := ingestTar(ctx, t, configFile, tarBytes, opts(1), nil)
	concurrent := ingestTar(ctx, t, configFile, tarBytes, opts(16), nil)

	if sequential.RootObjectID != concurrent.RootObjectID {
		t.Fatalf("root object differs: sequential=%s concurrent=%s",
			sequential.RootObjectID, concurrent.RootObjectID)
	}
	if !reflect.DeepEqual(sequential.SegmentMap, concurrent.SegmentMap) {
		t.Fatalf("segment map differs:\nsequential=%+v\nconcurrent=%+v",
			sequential.SegmentMap, concurrent.SegmentMap)
	}
	if got := len(concurrent.SegmentMap.Files[manySegFile]); got != segments+1 {
		t.Fatalf("%s has %d segments, want %d", manySegFile, got, segments+1)
	}

	out := filepath.Join(t.TempDir(), "restored")
	restoreSnapshot(ctx, t, configFile, concurrent.ManifestID, out)
	assertFileEqual(t, filepath.Join(out, manySegFile), relation)
}

// TestPipelinedFilesResolveIntoTheRightDirectories ingests a tar of many small
// files spread over several directories, which is the case the cross-file
// pipeline changes: each file is queued and resolved after the reader has moved
// past it, possibly out of order. The resulting tree must be identical to the one
// a single writer produces (nothing resolves late enough to miss its directory,
// and no entry lands in the wrong one), the counters must still add up, and every
// file must restore byte for byte.
func TestPipelinedFilesResolveIntoTheRightDirectories(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	const (
		blockSize = 8192
		segBytes  = 2 * blockSize
		dirs      = 6
		perDir    = 40
	)

	modTime := time.Unix(1_700_000_000, 0)
	entries := make([]tarEntry, 0, 2+(1+perDir)*dirs)
	entries = append(entries,
		tarEntry{name: "PG_VERSION", typeflag: tar.TypeReg, mode: 0o600, data: []byte("17\n"), modTime: modTime},
		tarEntry{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
	)

	// Sizes cycle through empty, sub-segment, exactly one segment and several
	// segments, so short and long files resolve interleaved.
	want := map[string][]byte{}
	for d := range dirs {
		dir := fmt.Sprintf("base/%d", 16384+d)
		entries = append(entries, tarEntry{name: dir, typeflag: tar.TypeDir, mode: 0o700, modTime: modTime})

		for f := range perDir {
			sizes := []int{0, 100, blockSize, segBytes, 3*segBytes + 17}
			size := sizes[(d+f)%len(sizes)]
			data := bytes.Repeat([]byte{byte(d), byte(f)}, size/2)
			name := fmt.Sprintf("%s/%d", dir, 1000+f)
			entries = append(entries, tarEntry{
				name: name, typeflag: tar.TypeReg, mode: 0o600, data: data, modTime: modTime,
			})
			want[name] = data
		}
	}
	tarBytes := buildTar(t, entries)

	opts := func(concurrency int) Options {
		return Options{
			Source:           defaultSource(),
			BlockSize:        blockSize,
			SegmentBytes:     segBytes,
			WriteConcurrency: concurrency,
		}
	}

	sequential := ingestTar(ctx, t, configFile, tarBytes, opts(1), nil)
	pipelined := ingestTar(ctx, t, configFile, tarBytes, opts(16), nil)

	if sequential.RootObjectID != pipelined.RootObjectID {
		t.Fatalf("root object differs: sequential=%s pipelined=%s",
			sequential.RootObjectID, pipelined.RootObjectID)
	}
	// Compare the counters but not Phases: those are timings, which differ
	// between any two runs. Zeroing the field rather than listing the counters
	// keeps a newly added counter covered by this check automatically.
	seqCounters, pipeCounters := sequential.Stats, pipelined.Stats
	seqCounters.Phases, pipeCounters.Phases = Phases{}, Phases{}
	if !reflect.DeepEqual(seqCounters, pipeCounters) {
		t.Fatalf("stats differ:\nsequential=%+v\npipelined=%+v", seqCounters, pipeCounters)
	}
	if !reflect.DeepEqual(sequential.SegmentMap, pipelined.SegmentMap) {
		t.Fatal("segment maps differ")
	}

	wantFiles := int64(len(want)) + 1 // the files above, plus PG_VERSION
	if pipelined.Stats.FileCount != wantFiles {
		t.Fatalf("file count = %d, want %d", pipelined.Stats.FileCount, wantFiles)
	}
	var wantSize int64 = 3 // PG_VERSION
	for _, data := range want {
		wantSize += int64(len(data))
	}
	if pipelined.Stats.TotalSize != wantSize {
		t.Fatalf("total size = %d, want %d", pipelined.Stats.TotalSize, wantSize)
	}

	out := filepath.Join(t.TempDir(), "restored")
	restoreSnapshot(ctx, t, configFile, pipelined.ManifestID, out)
	for name, data := range want {
		assertFileEqual(t, filepath.Join(out, name), data)
	}
}

// TestIncrementalTarMixesFullAndIncrementalMembers covers what the cross-file
// pipeline newly allows: an incremental tar carries whole members (backup_label,
// pg_control, configuration, any new relation) alongside INCREMENTAL.* ones, and
// a whole member's segments are now still being written while a later
// INCREMENTAL.* member is combined against the prior image. That combine reads
// the repository, so this asserts the two directions do not interfere: every
// file restores correctly, whichever order they resolve in.
func TestIncrementalTarMixesFullAndIncrementalMembers(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)
	modTime := time.Unix(1_700_000_000, 0)

	// A whole member big enough to still be in flight when the members after it
	// are read: 300 segments at two blocks each.
	const bigBlocks = 600

	bigData := make([]byte, 0, bigBlocks*testBlockSize)
	for i := range bigBlocks {
		bigData = append(bigData, bytes.Repeat([]byte{byte(i), byte(i >> 8)}, testBlockSize/2)...)
	}
	relation := bytes.Join([][]byte{
		priorBlock(0), priorBlock(1), priorBlock(2), priorBlock(3),
	}, nil)

	opts := func(incremental bool) Options {
		return Options{
			Source:           defaultSource(),
			Incremental:      incremental,
			BlockSize:        testBlockSize,
			SegmentBytes:     2 * testBlockSize,
			WriteConcurrency: 16,
		}
	}

	fullTar := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/1259", typeflag: tar.TypeReg, mode: 0o600, data: relation, modTime: modTime},
	})
	full := ingestTar(ctx, t, configFile, fullTar, opts(false), nil)

	// The incremental: a big whole member, then a whole member, then the
	// INCREMENTAL.* one, then more whole members after it.
	incMember := buildIncrementalFile(t, 4, []uint32{1, 3})
	label := []byte("START WAL LOCATION: 0/2000028\nINCREMENTAL FROM LSN: 0/1000000\n")
	control := bytes.Repeat([]byte{0xC0}, 3*testBlockSize)
	incTar := buildTar(t, []tarEntry{
		{name: "base", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "base/1/16400", typeflag: tar.TypeReg, mode: 0o600, data: bigData, modTime: modTime},
		{name: "backup_label", typeflag: tar.TypeReg, mode: 0o600, data: label, modTime: modTime},
		{name: "base/1/INCREMENTAL.1259", typeflag: tar.TypeReg, mode: 0o600, data: incMember, modTime: modTime},
		{name: "global", typeflag: tar.TypeDir, mode: 0o700, modTime: modTime},
		{name: "global/pg_control", typeflag: tar.TypeReg, mode: 0o600, data: control, modTime: modTime},
	})

	inc := ingestTar(ctx, t, configFile, incTar, opts(true),
		func(w repo.RepositoryWriter, o *Options) {
			priorMan, err := snapshot.LoadSnapshot(ctx, w, manifest.ID(full.ManifestID))
			if err != nil {
				t.Fatalf("load prior snapshot: %v", err)
			}
			o.Prior = NewKopiaPriorImage(ctx, w, priorMan.RootEntry.ObjectID)
		})

	out := filepath.Join(t.TempDir(), "restored-mixed")
	restoreSnapshot(ctx, t, configFile, inc.ManifestID, out)

	assertFileEqual(t, filepath.Join(out, "base/1/16400"), bigData)
	assertFileEqual(t, filepath.Join(out, "global/pg_control"), control)
	// The combined relation: changed blocks from the incremental, the rest from
	// the prior image.
	assertFileEqual(t, filepath.Join(out, "base/1/1259"), bytes.Join([][]byte{
		priorBlock(0), presentBlock(1), priorBlock(2), presentBlock(3),
	}, nil))
	// backup_label loses its INCREMENTAL lines, so the tree is a full backup.
	assertFileEqual(t, filepath.Join(out, "backup_label"), []byte("START WAL LOCATION: 0/2000028\n"))
}

// TestTruncatedMemberFailsIngest feeds a tar whose last member declares more
// bytes than it carries. The read error must fail the ingest rather than store a
// short file, and it must surface even though the segments before it were handed
// off to the writer pool.
func TestTruncatedMemberFailsIngest(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	const blockSize = 8192

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: "base/1/1249", Typeflag: tar.TypeReg, Mode: 0o600, Size: 8 * blockSize,
	}); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := tw.Write(bytes.Repeat([]byte{0xCD}, 3*blockSize)); err != nil {
		t.Fatalf("write body: %v", err)
	}
	// Deliberately no Close(): the archive ends mid-member.
	truncated := buf.Bytes()

	err := ingestTarErr(ctx, t, configFile, truncated, Options{
		Source:           defaultSource(),
		BlockSize:        blockSize,
		SegmentBytes:     2 * blockSize,
		WriteConcurrency: 16,
	}, nil)
	if err == nil {
		t.Fatal("ingest of a truncated member succeeded, want failure")
	}
}
