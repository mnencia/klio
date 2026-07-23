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

// Package ingest assembles a PostgreSQL BASE_BACKUP tar stream into a Kopia
// snapshot bottom-up from the Kopia library's primitives: it streams each file
// through a repository object writer, collects the directory entries as it
// reads, finalizes the directories deepest-first at the end, and saves a single
// self-contained snapshot. It tolerates any member order and never stages the
// backup to local disk.
package ingest

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// Options configures a single ingest.
type Options struct {
	// Source is the snapshot SourceInfo (host, username, logical path).
	Source snapshot.SourceInfo

	// Description is the snapshot description.
	Description string

	// Tags are the snapshot tags.
	Tags map[string]string

	// Pins are the snapshot retention pins.
	Pins []string

	// Incremental indicates the tar contains INCREMENTAL.* members that must be
	// combined against the prior image.
	Incremental bool

	// Prior resolves prior-image block readers for incremental reconstruction.
	// Required when Incremental is true.
	Prior PriorImage

	// BlockSize is the PostgreSQL BLCKSZ used for incremental reconstruction.
	// Defaults to DefaultBlockSize.
	BlockSize int

	// SegmentBytes is the fixed, block-aligned segment size for segment-
	// reference storage. Each file is split into segments of this many bytes,
	// each written as its own object and concatenated into the file object;
	// an incremental references an unchanged prior segment instead of reading
	// its blocks back. Defaults to DefaultSegmentBytes. Rounded down to a whole
	// number of blocks.
	SegmentBytes int
}

// Stats holds the counters for an ingest.
type Stats struct {
	TotalSize int64
	FileCount int64

	// ReusedFiles is the number of unchanged files that referenced the prior
	// object directly instead of reading its blocks back to rebuild it.
	ReusedFiles int64

	// ReferencedSegments is the number of clean segments an incremental took by
	// reference from the prior backup (no readback), and RebuiltSegments the
	// number it rewrote (readback confined to those segments' unchanged blocks).
	ReferencedSegments int64
	RebuiltSegments    int64
}

// Result is the outcome of an ingest, serialized to stdout by the CLI.
type Result struct {
	ManifestID   string
	RootObjectID string
	Stats        Stats

	// SegmentMap records, per logical file path, the ordered segment object IDs
	// that make up the file in this backup, plus the segment size. The next
	// incremental references these to avoid reading unchanged segments back. It
	// is persisted as a compressed directory-tree snapshot (see SaveSegmentMap).
	SegmentMap SegmentMap
}

// PriorImage provides access to a prior backup's files for incremental
// reconstruction.
type PriorImage interface {
	// BlockReader returns a reader for the prior image of the relation segment
	// at relPath (the INCREMENTAL. prefix already stripped).
	BlockReader(relPath string, blockSize int) (PriorBlockReader, error)

	// Entry returns the prior snapshot's file entry for relPath (its ObjectID
	// and size), so an unchanged file can reference the prior object directly
	// instead of reading and rehashing every block to rebuild an identical one.
	Entry(relPath string) (*snapshot.DirEntry, error)

	// Segments returns the prior backup's ordered segment object IDs for
	// relPath, so an incremental can reference an unchanged segment by object ID
	// instead of reading its blocks back. ok is false when the prior backup has
	// no recorded segment map for the file (e.g. taken before segmentation), in
	// which case the caller rebuilds every segment as today.
	//
	// Invariant: the returned segments MUST have been produced at the same
	// SegmentBytes as this ingest. combineSegments derives each referenced
	// segment's byte length from the current segment size, so a list built at a
	// different size (same count, different boundaries) can claim a wrong length
	// and silently corrupt the restore. Enforcing the match is the caller's
	// responsibility: installPriorSegments in cmd/ingest.go installs the prior
	// map only when priorBlocks equals the configured segmentBlocks.
	// combineSegments' structural count check alone is not sufficient.
	Segments(relPath string) (segments []object.ID, ok bool)
}

// dirNode is one directory in the tree being assembled. Directories are kept in
// a map and finalized once at end of stream (children before parents), so the
// assembler does not assume any particular member ordering. pg_basebackup, for
// example, sends global/pg_control last, well after the global subtree.
type dirNode struct {
	path    string
	builder *snapshotfs.DirManifestBuilder
	modTime time.Time
	mode    os.FileMode
	uid     uint32
	gid     uint32
}

// DefaultSegmentBytes is the default segment size for segment-reference storage
// (128 KiB). It balances the two costs a segment size trades off: larger
// segments drag more unchanged data into each rebuilt (dirty) segment, so a
// scattered change amplifies repo growth; smaller segments multiply the segment
// objects, the per-backup segment map, and the content index. 128 KiB keeps
// nearly all of the locality readback win while cutting scattered amplification
// by more than an order of magnitude versus a multi-megabyte segment, and its
// index/maintenance cost is already near the floor. It is a whole number of
// blocks for any BLCKSZ up to 128 KiB.
const DefaultSegmentBytes = 128 << 10

// assembler builds a snapshot from a tar stream.
type assembler struct {
	ctx   context.Context //nolint:containedctx // bound to a single ingest call
	rep   repo.RepositoryWriter
	opts  Options
	start time.Time
	dirs  map[string]*dirNode
	stats Stats

	// segmentBlocks is the segment size in blocks (SegmentBytes / BlockSize).
	segmentBlocks int
	// segMap records this backup's per-file ordered segment object IDs, carried
	// forward so the next incremental can reference unchanged segments.
	segMap map[string][]object.ID
}

// segRef is a segment object together with the logical byte length it
// contributes to its file. The length is known at write time (freshly written or
// referenced from the prior), so the file's index can be built without reading
// any segment back — see finishFile / buildFileObject.
type segRef struct {
	oid    object.ID
	length int64
}

// Kopia encodes a multi-segment ("indirect") object as a small JSON seek-table
// content, prefixed like the index content below, whose object ID is the index
// content's ID string with one leading "I" per indirection level. These mirror
// the (unexported) kopia writer so the helper can build a file's index from
// segment IDs and their known lengths, instead of calling ConcatenateObjects
// (which opens every segment just to rediscover those lengths — on the server
// path that re-reads the whole backup).
const indirectContentPrefix content.IDPrefix = "x"

const (
	indirectObjectPrefix = "I"
	indirectStreamID     = "kopia:indirect"
)

// indirectIndex is the on-object JSON layout kopia reads for a multi-segment
// object (see object.LoadIndexObject); it must match kopia's format exactly.
type indirectIndex struct {
	StreamID string                       `json:"stream"`
	Entries  []object.IndirectObjectEntry `json:"entries"`
}

// Run ingests the tar stream and saves a snapshot, returning its identifiers.
func Run(ctx context.Context, rep repo.RepositoryWriter, tarStream io.Reader, opts Options) (*Result, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	segmentBlocks := SegmentBlocksFor(opts.SegmentBytes, opts.BlockSize)

	start := time.Now()

	a := &assembler{
		ctx:           ctx,
		rep:           rep,
		opts:          opts,
		start:         start,
		dirs:          map[string]*dirNode{},
		segmentBlocks: segmentBlocks,
		segMap:        map[string][]object.ID{},
	}
	// The data directory root is not a tar member, so default it to the mode
	// PostgreSQL uses for PGDATA.
	a.ensureDir(".").mode = 0o700

	if err := a.consume(tarStream); err != nil {
		return nil, err
	}

	rootEntry, err := a.finalizeAll()
	if err != nil {
		return nil, err
	}

	man := &snapshot.Manifest{
		Source:      opts.Source,
		Description: opts.Description,
		StartTime:   fs.UTCTimestampFromTime(start),
		EndTime:     fs.UTCTimestampFromTime(time.Now()),
		RootEntry:   rootEntry,
		Tags:        userTags(opts.Tags),
		Pins:        opts.Pins,
		Stats: snapshot.Stats{
			TotalFileSize:  a.stats.TotalSize,
			TotalFileCount: int32(a.stats.FileCount), //nolint:gosec // file counts fit int32 in practice
		},
	}

	manID, err := snapshot.SaveSnapshot(ctx, rep, man)
	if err != nil {
		return nil, fmt.Errorf("while saving snapshot: %w", err)
	}

	return &Result{
		ManifestID:   string(manID),
		RootObjectID: rootEntry.ObjectID.String(),
		Stats:        a.stats,
		SegmentMap:   a.serializeSegmentMap(),
	}, nil
}

// SegmentBlocksFor returns the segment size in blocks for the given byte and
// block sizes, applying the same defaults Run uses. It is the single source of
// truth so the caller can gate prior-segment referencing on the same value.
func SegmentBlocksFor(segmentBytes, blockSize int) int {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	if segmentBytes <= 0 {
		segmentBytes = DefaultSegmentBytes
	}

	return max(segmentBytes/blockSize, 1)
}

// serializeSegmentMap renders this backup's per-file segment object IDs as the
// self-describing SegmentMap, recording the segment size so an incremental can
// tell whether the size still matches before referencing.
func (a *assembler) serializeSegmentMap() SegmentMap {
	files := make(map[string][]string, len(a.segMap))
	for p, ids := range a.segMap {
		strs := make([]string, len(ids))
		for i, id := range ids {
			strs[i] = id.String()
		}
		files[p] = strs
	}

	return SegmentMap{SegmentBlocks: a.segmentBlocks, Files: files}
}

// backupLabelFile is the name of the PostgreSQL backup label file.
const backupLabelFile = "backup_label"

// incrementalLabelPrefix marks the lines an incremental backup_label carries
// that a full backup does not. They must be removed when combining so the tree
// is recognized as a full backup.
const incrementalLabelPrefix = "INCREMENTAL FROM "

// stripIncrementalLabelLines removes the INCREMENTAL FROM lines from a
// backup_label, turning an incremental label into a full-backup label.
func stripIncrementalLabelLines(label []byte) []byte {
	lines := bytes.Split(label, []byte("\n"))
	kept := lines[:0]
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte(incrementalLabelPrefix)) {
			continue
		}
		kept = append(kept, line)
	}

	return bytes.Join(kept, []byte("\n"))
}

// userTagPrefix is the label-key prefix Kopia uses for user-defined snapshot
// tags. The kopia CLI adds it to every --tags key, and `snapshot list --tags`
// filters on the prefixed key, so the helper must add it too for snapshots to
// be found by the same tag queries klio uses on the CLI path.
const userTagPrefix = "tag:"

// UserTagLabels converts ingest tags into the manifest labels a saved
// snapshot actually carries (see userTagPrefix): SaveSnapshot stores each
// Options.Tags entry as a label under this prefix, the same way the kopia
// CLI does for --tags. A caller resolving a snapshot by these same tags
// directly through the repository (bypassing the CLI, which applies the
// prefix itself) must filter on the prefixed keys too.
func UserTagLabels(tags map[string]string) map[string]string {
	return userTags(tags)
}

// userTags returns the tags with each key prefixed for Kopia, matching the
// kopia CLI convention.
func userTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[userTagPrefix+k] = v
	}

	return out
}

// consume reads the whole tar stream, member by member.
func (a *assembler) consume(tarStream io.Reader) error {
	tr := tar.NewReader(tarStream)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("while reading tar member: %w", err)
		}

		name := path.Clean(strings.TrimPrefix(hdr.Name, "./"))
		if name == "." || name == "/" || name == "" {
			continue
		}

		if err := a.handleMember(tr, hdr, name); err != nil {
			return err
		}
	}
}

// handleMember dispatches a single tar member.
func (a *assembler) handleMember(tr *tar.Reader, hdr *tar.Header, name string) error {
	if hdr.Typeflag == tar.TypeDir {
		node := a.ensureDir(name)
		node.modTime = hdr.ModTime
		node.mode = hdr.FileInfo().Mode()
		node.uid = uint32(hdr.Uid) //nolint:gosec // unix uid fits uint32
		node.gid = uint32(hdr.Gid) //nolint:gosec // unix gid fits uint32

		return nil
	}

	var (
		de      *snapshot.DirEntry
		logical string
		err     error
	)
	switch hdr.Typeflag {
	case tar.TypeReg:
		de, logical, err = a.addFile(tr, hdr, name)
	case tar.TypeSymlink:
		de, logical, err = a.addSymlink(hdr, name)
	default:
		// Block/char devices, fifos, etc. do not occur in a PGDATA backup.
		return nil
	}
	if err != nil {
		return err
	}

	a.ensureDir(path.Dir(logical)).builder.AddEntry(de)

	return nil
}

// ensureDir returns the directory node for p, creating it (and its ancestors)
// with sensible defaults if it has not been seen yet.
func (a *assembler) ensureDir(p string) *dirNode {
	if node, ok := a.dirs[p]; ok {
		return node
	}

	node := &dirNode{
		path:    p,
		builder: &snapshotfs.DirManifestBuilder{},
		modTime: a.start,
		mode:    0o700,
	}
	a.dirs[p] = node

	if p != "." {
		a.ensureDir(path.Dir(p))
	}

	return node
}

// finalizeAll writes every directory manifest, children before parents, and
// returns the DirEntry for the root.
func (a *assembler) finalizeAll() (*snapshot.DirEntry, error) {
	paths := make([]string, 0, len(a.dirs))
	for p := range a.dirs {
		if p != "." {
			paths = append(paths, p)
		}
	}
	// Deeper paths (more separators) finalize first, so a child's DirEntry is
	// added to its parent before the parent itself is written.
	sort.Slice(paths, func(i, j int) bool {
		return strings.Count(paths[i], "/") > strings.Count(paths[j], "/")
	})

	for _, p := range paths {
		de, err := a.writeDir(a.dirs[p])
		if err != nil {
			return nil, err
		}
		a.dirs[path.Dir(p)].builder.AddEntry(de)
	}

	return a.writeDir(a.dirs["."])
}

// writeDir builds and writes a directory manifest, returning its DirEntry.
func (a *assembler) writeDir(node *dirNode) (*snapshot.DirEntry, error) {
	manifest := node.builder.Build(fs.UTCTimestampFromTime(node.modTime), "")

	oid, err := snapshotfs.WriteDirManifest(a.ctx, a.rep, node.path, manifest, "")
	if err != nil {
		return nil, fmt.Errorf("while writing directory manifest for %q: %w", node.path, err)
	}

	name := path.Base(node.path)
	if node.path == "." {
		name = "."
	}

	return &snapshot.DirEntry{
		Name:        name,
		Type:        snapshot.EntryTypeDirectory,
		Permissions: snapshot.Permissions(node.mode & fs.ModBits),
		ModTime:     fs.UTCTimestampFromTime(node.modTime),
		UserID:      node.uid,
		GroupID:     node.gid,
		ObjectID:    oid,
		DirSummary:  manifest.Summary,
	}, nil
}

// fileContent resolves the content reader and logical path for a plain tar file
// member. On an incremental backup it strips the INCREMENTAL FROM lines from
// backup_label so the combined tree is a self-contained full backup.
func (a *assembler) fileContent(
	tr *tar.Reader, name string,
) (io.Reader, string, func(error) error, error) {
	noop := func(copyErr error) error { return copyErr }

	if a.opts.Incremental && path.Base(name) == backupLabelFile {
		raw, rErr := io.ReadAll(tr)
		if rErr != nil {
			return nil, "", nil, fmt.Errorf("while reading %q: %w", name, rErr)
		}

		return bytes.NewReader(stripIncrementalLabelLines(raw)), name, noop, nil
	}

	return tr, name, noop, nil
}

// addFile streams a regular file into the repository and returns its DirEntry
// and the logical path it belongs at. INCREMENTAL.* members are combined
// against the prior image when running an incremental backup; the returned path
// has the INCREMENTAL. prefix stripped.
func (a *assembler) addFile(tr *tar.Reader, hdr *tar.Header, name string) (*snapshot.DirEntry, string, error) {
	if a.opts.Incremental && strings.HasPrefix(path.Base(name), IncrementalFilePrefix) {
		return a.addIncrementalFile(tr, hdr, name)
	}

	content, logical, finish, err := a.fileContent(tr, name)
	if err != nil {
		return nil, "", err
	}

	return a.writeSegmentedFile(hdr, logical, content, finish)
}

// writeSegmentedFile splits content into fixed-size segment objects, records
// their IDs for the segment map, and returns a DirEntry whose object is their
// concatenation. finish reconciles any producer error (a no-op for plain
// members).
func (a *assembler) writeSegmentedFile(
	hdr *tar.Header, logical string, content io.Reader, finish func(error) error,
) (*snapshot.DirEntry, string, error) {
	segBytes := int64(a.segmentBlocks * a.opts.BlockSize)

	var (
		segs    []segRef
		written int64
	)
	for {
		oid, n, err := a.writeSegment(content, segBytes, logical)
		if err != nil {
			_ = finish(err)

			return nil, "", fmt.Errorf("while writing file %q: %w", logical, err)
		}
		if n == 0 {
			break // end of content
		}
		segs = append(segs, segRef{oid: oid, length: n})
		written += n
		if n < segBytes {
			break // last, short segment
		}
	}
	if err := finish(nil); err != nil {
		return nil, "", fmt.Errorf("while writing file %q: %w", logical, err)
	}

	oid, err := a.finishFile(logical, segs)
	if err != nil {
		return nil, "", err
	}

	a.stats.FileCount++
	a.stats.TotalSize += written

	return a.fileEntry(hdr, logical, oid, written), logical, nil
}

// writeSegment writes up to limit bytes from content into a new object,
// returning its ID and the number of bytes written (0 at end of content).
func (a *assembler) writeSegment(content io.Reader, limit int64, logical string) (object.ID, int64, error) {
	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "SEG:" + logical})
	defer writer.Close() //nolint:errcheck

	n, err := io.CopyN(writer, content, limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return object.EmptyID, 0, err
	}
	if n == 0 {
		return object.EmptyID, 0, nil
	}

	oid, err := writer.Result()

	return oid, n, err
}

// finishFile records a file's segment list for the segment map and resolves its
// object ID. A single-segment (or empty) file is its own object. A multi-segment
// file's object is an indirect (seek-table) object built from the segments'
// known lengths — see buildFileObject. This never reads the segments back, so it
// is safe to run inline (no flush) and avoids the whole-backup re-read that
// object.ConcatenateObjects incurs on the server path.
func (a *assembler) finishFile(logical string, segs []segRef) (object.ID, error) {
	if len(segs) == 0 {
		writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "SEG:" + logical})
		defer writer.Close() //nolint:errcheck
		oid, err := writer.Result()
		if err != nil {
			return object.EmptyID, fmt.Errorf("while writing empty file %q: %w", logical, err)
		}
		segs = []segRef{{oid: oid, length: 0}}
	}

	ids := make([]object.ID, len(segs))
	for i, s := range segs {
		ids[i] = s.oid
	}
	a.segMap[logical] = ids

	if len(segs) == 1 {
		return segs[0].oid, nil
	}

	return a.buildFileObject(logical, segs)
}

// buildFileObject writes a file's indirect (seek-table) object from its segments
// and their known lengths, returning its object ID. It reimplements
// object.ConcatenateObjects for single-content segments without opening any of
// them: ConcatenateObjects opens every input to rediscover its length, which on
// the server path re-reads the entire backup's data. The written index and the
// derived object ID match kopia's own format (see the indirect* constants), so
// the result is read back like any concatenated object.
func (a *assembler) buildFileObject(logical string, segs []segRef) (object.ID, error) {
	entries := make([]object.IndirectObjectEntry, len(segs))
	var start int64
	for i, s := range segs {
		entries[i] = object.IndirectObjectEntry{Start: start, Length: s.length, Object: s.oid}
		start += s.length
	}

	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{
		Description: "SEGIDX:" + logical,
		Prefix:      indirectContentPrefix,
	})
	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(indirectIndex{StreamID: indirectStreamID, Entries: entries}); err != nil {
		return object.EmptyID, fmt.Errorf("while encoding segment index of %q: %w", logical, err)
	}

	indexOID, err := writer.Result()
	if err != nil {
		return object.EmptyID, fmt.Errorf("while writing segment index of %q: %w", logical, err)
	}

	oid, err := object.ParseID(indirectObjectPrefix + indexOID.String())
	if err != nil {
		return object.EmptyID, fmt.Errorf("while forming file object of %q: %w", logical, err)
	}

	return oid, nil
}

// fileEntry builds a file DirEntry from the tar header metadata plus a resolved
// object ID and size (freshly written, or reused from the prior image).
func (a *assembler) fileEntry(hdr *tar.Header, logical string, oid object.ID, size int64) *snapshot.DirEntry {
	return &snapshot.DirEntry{
		Name:        path.Base(logical),
		Type:        snapshot.EntryTypeFile,
		Permissions: snapshot.Permissions(hdr.FileInfo().Mode() & fs.ModBits),
		FileSize:    size,
		ModTime:     fs.UTCTimestampFromTime(hdr.ModTime),
		UserID:      uint32(hdr.Uid), //nolint:gosec // unix uid fits uint32
		GroupID:     uint32(hdr.Gid), //nolint:gosec // unix gid fits uint32
		ObjectID:    oid,
	}
}

// addIncrementalFile combines an INCREMENTAL.* member against the prior image
// and returns the resulting file DirEntry and its logical path (INCREMENTAL.
// prefix stripped). An unchanged file references the prior object directly; a
// changed file is combined segment by segment, referencing unchanged segments
// from the prior and rebuilding only those that contain a changed block.
func (a *assembler) addIncrementalFile(
	tr *tar.Reader, hdr *tar.Header, name string,
) (*snapshot.DirEntry, string, error) {
	relPath := path.Join(path.Dir(name), strings.TrimPrefix(path.Base(name), IncrementalFilePrefix))

	header, err := ParseIncrementalHeader(tr, a.opts.BlockSize)
	if err != nil {
		return nil, "", fmt.Errorf("while parsing incremental header for %q: %w", name, err)
	}
	if a.opts.Prior == nil {
		return nil, "", fmt.Errorf("incremental member %q but no prior image configured", name)
	}

	// Metadata-only fast path: with no changed blocks and an unchanged length,
	// the whole file is byte-identical to the prior, so reference the prior
	// object directly and carry its segment list forward unchanged.
	if len(header.BlockNumbers) == 0 {
		if err := assertDrained(tr, make([]byte, 1)); err != nil {
			return nil, "", fmt.Errorf("while combining %q: %w", name, err)
		}

		prior, err := a.opts.Prior.Entry(relPath)
		if err != nil {
			return nil, "", fmt.Errorf("while resolving prior entry for %q: %w", relPath, err)
		}

		if prior.FileSize == int64(header.TruncationBlockLength)*int64(a.opts.BlockSize) {
			a.stats.FileCount++
			a.stats.ReusedFiles++
			a.stats.TotalSize += prior.FileSize
			a.carryPriorSegments(relPath, prior.ObjectID)

			return a.fileEntry(hdr, relPath, prior.ObjectID, prior.FileSize), relPath, nil
		}
	}

	return a.combineSegments(tr, hdr, header, relPath)
}

// carryPriorSegments records, for a file taken by the whole-file reuse path,
// the prior backup's segment list so the next incremental can still reference
// this file's segments. When the prior has no recorded segments (a
// pre-segmentation backup), the whole object is recorded as a single segment.
func (a *assembler) carryPriorSegments(relPath string, priorOID object.ID) {
	if segs, ok := a.opts.Prior.Segments(relPath); ok {
		a.segMap[relPath] = segs

		return
	}
	// The prior recorded no segment list (a pre-segmentation backup, or a chain
	// whose size changed and left the map uninstalled). Recording the whole-file
	// object as one "segment" is correct only when the file fits in a single
	// segment; for a multi-segment file the next incremental would splice the
	// whole file into the segment-0 slot and corrupt the reconstruction. So
	// record it only when it is genuinely one segment, else omit the file and let
	// the next incremental rebuild every segment.
	if a.priorBlockCount(relPath) <= a.segmentBlocks {
		a.segMap[relPath] = []object.ID{priorOID}
	}
}

// combineSegments rebuilds a changed file segment by segment. A segment with no
// changed block is referenced from the prior (no readback) when its byte range
// matches the prior's same-index segment; otherwise it is rebuilt from the
// incremental blocks and the prior's unchanged blocks. The per-segment objects
// are concatenated into the file object.
//
// integrity checks read clearer as one sequential function than split apart.
//
//nolint:cyclop // the per-segment reference/rebuild walk plus its end-of-stream
func (a *assembler) combineSegments(
	blockData io.Reader, hdr *tar.Header, h *IncrementalHeader, relPath string,
) (*snapshot.DirEntry, string, error) {
	sb := a.segmentBlocks
	total := int(h.TruncationBlockLength)
	priorBlocks := a.priorBlockCount(relPath)
	priorSegs, _ := a.opts.Prior.Segments(relPath)

	// Only reference prior segments when the prior segment list is structurally
	// consistent with the prior file length (exactly one object per sb-block
	// segment). An inconsistent list — e.g. a whole-file object carried as a
	// single segment for a multi-segment file — must never be spliced; rebuild
	// every segment instead. This both rejects such a map and heals a chain
	// whose earlier backup persisted one. This count check assumes the prior map
	// was produced at the current segment size; that size match is the caller's
	// responsibility (see PriorImage.Segments), not verified here.
	canReference := len(priorSegs) == (priorBlocks+sb-1)/sb

	var reader PriorBlockReader
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	ensureReader := func() (PriorBlockReader, error) {
		if reader == nil {
			r, err := a.opts.Prior.BlockReader(relPath, a.opts.BlockSize)
			if err != nil {
				return nil, fmt.Errorf("while opening prior image of %q: %w", relPath, err)
			}
			reader = r
		}

		return reader, nil
	}

	buf := make([]byte, a.opts.BlockSize)
	zeroBlock := make([]byte, a.opts.BlockSize)
	cursor := 0 // index into h.BlockNumbers
	var segs []segRef

	for start := 0; start < total; start += sb {
		end := min(start+sb, total)
		dirty := cursor < len(h.BlockNumbers) && int(h.BlockNumbers[cursor]) < end
		j := start / sb
		priorEnd := min(start+sb, priorBlocks)
		segLen := int64(end-start) * int64(a.opts.BlockSize)

		if !dirty && canReference && j < len(priorSegs) && end == priorEnd {
			segs = append(segs, segRef{oid: priorSegs[j], length: segLen})
			a.stats.ReferencedSegments++

			continue
		}

		oid, err := a.rebuildSegment(blockData, h, &cursor, start, end, buf, zeroBlock, ensureReader, relPath)
		if err != nil {
			return nil, "", err
		}
		segs = append(segs, segRef{oid: oid, length: segLen})
		a.stats.RebuiltSegments++
	}

	if cursor != len(h.BlockNumbers) {
		return nil, "", fmt.Errorf("%w: consumed %d of %d present blocks",
			ErrReconstructIncomplete, cursor, len(h.BlockNumbers))
	}
	if err := assertDrained(blockData, buf[:1]); err != nil {
		return nil, "", fmt.Errorf("while combining %q: %w", relPath, err)
	}

	oid, err := a.finishFile(relPath, segs)
	if err != nil {
		return nil, "", err
	}

	size := int64(total) * int64(a.opts.BlockSize)
	a.stats.FileCount++
	a.stats.TotalSize += size

	return a.fileEntry(hdr, relPath, oid, size), relPath, nil
}

// rebuildSegment writes one segment covering output blocks [start, end): a
// present block comes from blockData (advancing cursor), an absent one from the
// prior image.
func (a *assembler) rebuildSegment(
	blockData io.Reader, h *IncrementalHeader, cursor *int, start, end int, buf, zeroBlock []byte,
	ensureReader func() (PriorBlockReader, error), relPath string,
) (object.ID, error) {
	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "SEG:" + relPath})
	defer writer.Close() //nolint:errcheck

	for i := start; i < end; i++ {
		if *cursor < len(h.BlockNumbers) && int(h.BlockNumbers[*cursor]) == i {
			if _, err := io.ReadFull(blockData, buf); err != nil {
				return object.EmptyID, fmt.Errorf("while reading incremental block %d: %w", i, err)
			}
			*cursor++
			if _, err := writer.Write(buf); err != nil {
				return object.EmptyID, fmt.Errorf("while writing block %d: %w", i, err)
			}

			continue
		}

		reader, err := ensureReader()
		if err != nil {
			return object.EmptyID, err
		}
		// A block beyond the prior image is an uninitialized extension block:
		// zero-fill it, matching pg_combinebackup (see writePriorOrZeroBlock).
		blockNum := uint32(i) //nolint:gosec // block index fits uint32
		if werr := writePriorOrZeroBlock(reader, blockNum, zeroBlock, writer); werr != nil {
			return object.EmptyID, werr
		}
	}

	oid, err := writer.Result()
	if err != nil {
		return object.EmptyID, fmt.Errorf("while finalizing segment of %q: %w", relPath, err)
	}

	return oid, nil
}

// priorBlockCount returns the prior file's length in blocks, or 0 when the file
// is absent from the prior (a new file, which has no segments to reference).
func (a *assembler) priorBlockCount(relPath string) int {
	entry, err := a.opts.Prior.Entry(relPath)
	if err != nil {
		return 0
	}

	return int(entry.FileSize) / a.opts.BlockSize
}

// addSymlink stores a symbolic link, writing its target as the object content
// (mirroring the Kopia uploader convention), and returns its DirEntry and path.
func (a *assembler) addSymlink(hdr *tar.Header, name string) (*snapshot.DirEntry, string, error) {
	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "SYMLINK:" + name})
	defer writer.Close() //nolint:errcheck

	written, err := io.Copy(writer, strings.NewReader(hdr.Linkname))
	if err != nil {
		return nil, "", fmt.Errorf("while writing symlink %q: %w", name, err)
	}

	oid, err := writer.Result()
	if err != nil {
		return nil, "", fmt.Errorf("while finalizing symlink %q: %w", name, err)
	}

	return &snapshot.DirEntry{
		Name:        path.Base(name),
		Type:        snapshot.EntryTypeSymlink,
		Permissions: snapshot.Permissions(hdr.FileInfo().Mode() & fs.ModBits),
		FileSize:    written,
		ModTime:     fs.UTCTimestampFromTime(hdr.ModTime),
		UserID:      uint32(hdr.Uid), //nolint:gosec // unix uid fits uint32
		GroupID:     uint32(hdr.Gid), //nolint:gosec // unix gid fits uint32
		ObjectID:    oid,
	}, name, nil
}
