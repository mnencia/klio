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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// segmentBlocksFile is the root-level file holding the segment size (in blocks),
// keeping the persisted map self-describing.
const segmentBlocksFile = "segmentBlocks"

// segmentMapCompressor compresses the segment-map directory manifests. They are
// highly redundant (repeated structure, object IDs, path prefixes) so this
// compresses very well; kopia decompresses transparently on read.
const segmentMapCompressor = "zstd-fastest"

// SegmentMap is the in-memory, self-describing form of a backup's per-file
// segment object IDs. SegmentBlocks records the segment size in blocks so an
// incremental references the prior segments only when the configured size still
// matches.
type SegmentMap struct {
	SegmentBlocks int
	Files         map[string][]string
}

// SaveSegmentMap persists m as a directory-tree snapshot for the given source,
// tagged so it is resolved by tags and reaped with the backup
// like the manifest. It must run inside the same write session as the data
// snapshot so the two commit together. Returns the snapshot manifest ID.
//
// The map is stored as a tree, not as a JSON blob: the root holds a small
// segmentBlocks file plus one directory per file, whose entries are that file's
// segments in order (named by zero-padded index, object ID = segment OID). The
// tree is the mapping the next incremental loads to reference unchanged prior
// segments, and it records the segment size so that referencing is only enabled
// on a size match. It is far smaller than a JSON blob (~8x in practice), since
// the manifests compress well and each object ID appears once. The tree entries
// also keep the segment objects GC-reachable, but that is now redundant: the
// file object built by buildFileObject references each segment object directly
// (it does not flatten their indexes), so the data snapshot already keeps every
// referenced segment, including a multi-content segment's seek-table, reachable.
func SaveSegmentMap(
	ctx context.Context,
	w repo.RepositoryWriter,
	source snapshot.SourceInfo,
	tags map[string]string,
	description string,
	m SegmentMap,
) (string, error) {
	now := fs.UTCTimestampFromTime(time.Now())
	root := &snapshotfs.DirManifestBuilder{}

	sbOID, err := writeSmallObject(ctx, w, strconv.Itoa(m.SegmentBlocks))
	if err != nil {
		return "", fmt.Errorf("while writing segment size: %w", err)
	}
	root.AddEntry(&snapshot.DirEntry{Name: segmentBlocksFile, Type: snapshot.EntryTypeFile, ModTime: now, ObjectID: sbOID})

	for path, ids := range m.Files {
		leafOID, summary, err := writeSegmentDir(ctx, w, path, ids, now)
		if err != nil {
			return "", err
		}
		root.AddEntry(&snapshot.DirEntry{
			Name:       encodePath(path),
			Type:       snapshot.EntryTypeDirectory,
			ModTime:    now,
			ObjectID:   leafOID,
			DirSummary: summary,
		})
	}

	rootManifest := root.Build(now, "")
	rootOID, err := snapshotfs.WriteDirManifest(ctx, w, ".", rootManifest, segmentMapCompressor)
	if err != nil {
		return "", fmt.Errorf("while writing segment map root: %w", err)
	}

	man := &snapshot.Manifest{
		Source:      source,
		Description: description,
		StartTime:   now,
		EndTime:     fs.UTCTimestampFromTime(time.Now()),
		RootEntry: &snapshot.DirEntry{
			Name:       ".",
			Type:       snapshot.EntryTypeDirectory,
			ObjectID:   rootOID,
			DirSummary: rootManifest.Summary,
		},
		Tags: userTags(tags),
	}
	manID, err := snapshot.SaveSnapshot(ctx, w, man)
	if err != nil {
		return "", fmt.Errorf("while saving segment map snapshot: %w", err)
	}

	return string(manID), nil
}

// writeSegmentDir writes one file's segments as a directory whose entries are
// named by zero-padded index (so they sort into segment order) and reference the
// segment objects. Returns the directory object ID and its summary.
func writeSegmentDir(
	ctx context.Context, w repo.RepositoryWriter, path string, ids []string, now fs.UTCTimestamp,
) (object.ID, *fs.DirectorySummary, error) {
	leaf := &snapshotfs.DirManifestBuilder{}
	for i, s := range ids {
		segOID, err := object.ParseID(s)
		if err != nil {
			return object.EmptyID, nil, fmt.Errorf("while parsing segment object id %q: %w", s, err)
		}
		leaf.AddEntry(&snapshot.DirEntry{
			Name:     fmt.Sprintf("%08d", i),
			Type:     snapshot.EntryTypeFile,
			ModTime:  now,
			ObjectID: segOID,
		})
	}
	manifest := leaf.Build(now, "")
	oid, err := snapshotfs.WriteDirManifest(ctx, w, path, manifest, segmentMapCompressor)
	if err != nil {
		return object.EmptyID, nil, fmt.Errorf("while writing segment directory for %q: %w", path, err)
	}

	return oid, manifest.Summary, nil
}

// LoadSegmentMap reads a segment map tree rooted at rootOID, returning the
// recorded segment size (in blocks) and the per-file segment object IDs.
func LoadSegmentMap(
	ctx context.Context, rep repo.Repository, rootOID object.ID,
) (int, map[string][]object.ID, error) {
	rootManifest, err := readDirManifest(ctx, rep, rootOID)
	if err != nil {
		return 0, nil, fmt.Errorf("while reading segment map root: %w", err)
	}

	segmentBlocks := 0
	files := make(map[string][]object.ID)
	for _, e := range rootManifest.Entries {
		if e.Name == segmentBlocksFile {
			segmentBlocks, err = readIntObject(ctx, rep, e.ObjectID)
			if err != nil {
				return 0, nil, fmt.Errorf("while reading segment size: %w", err)
			}

			continue
		}

		path, err := decodePath(e.Name)
		if err != nil {
			return 0, nil, fmt.Errorf("while decoding segment map path %q: %w", e.Name, err)
		}
		segs, err := readSegmentDir(ctx, rep, e.ObjectID)
		if err != nil {
			return 0, nil, err
		}
		files[path] = segs
	}

	return segmentBlocks, files, nil
}

// readSegmentDir returns a file's segment object IDs in index order.
func readSegmentDir(ctx context.Context, rep repo.Repository, dirOID object.ID) ([]object.ID, error) {
	manifest, err := readDirManifest(ctx, rep, dirOID)
	if err != nil {
		return nil, fmt.Errorf("while reading segment directory: %w", err)
	}
	entries := manifest.Entries
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	segs := make([]object.ID, len(entries))
	for i, e := range entries {
		segs[i] = e.ObjectID
	}

	return segs, nil
}

func readDirManifest(ctx context.Context, rep repo.Repository, oid object.ID) (*snapshot.DirManifest, error) {
	reader, err := rep.OpenObject(ctx, oid)
	if err != nil {
		return nil, fmt.Errorf("while opening directory %v: %w", oid, err)
	}
	defer reader.Close() //nolint:errcheck

	var manifest snapshot.DirManifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("while decoding directory %v: %w", oid, err)
	}

	return &manifest, nil
}

func writeSmallObject(ctx context.Context, w repo.RepositoryWriter, s string) (object.ID, error) {
	writer := w.NewObjectWriter(ctx, object.WriterOptions{Description: "SEGMAP-META"})
	defer writer.Close() //nolint:errcheck
	if _, err := writer.Write([]byte(s)); err != nil {
		return object.EmptyID, err
	}

	return writer.Result()
}

func readIntObject(ctx context.Context, rep repo.Repository, oid object.ID) (int, error) {
	reader, err := rep.OpenObject(ctx, oid)
	if err != nil {
		return 0, err
	}
	defer reader.Close() //nolint:errcheck

	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(data))
}

// encodePath / decodePath map a file's logical path to and from a single
// directory-entry name (kopia entry names cannot contain "/").
func encodePath(path string) string { return base64.RawURLEncoding.EncodeToString([]byte(path)) }

func decodePath(name string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(name)
	if err != nil {
		return "", err
	}

	return string(raw), nil
}
