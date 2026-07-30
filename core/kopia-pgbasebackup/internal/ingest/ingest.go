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
// snapshot bottom-up from the Kopia library's primitives: it cuts each file into
// fixed-size segments written concurrently as their own objects, indexes them
// into the file's object, files each directory entry once that file's segments
// are stored, finalizes the directories deepest-first at the end, and saves a
// single self-contained snapshot. It tolerates any member order and never stages
// the backup to local disk.
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
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
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

	// WriteConcurrency is how many segments are written to the repository at
	// once. Defaults to DefaultWriteConcurrency, whose comment explains the
	// choice.
	WriteConcurrency int
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

	// Phases is where the ingest spent its time, by kind of work.
	Phases Phases
}

// Phases accumulates where an ingest spent its wall time. The costs of this path
// are dominated by round trips to the repository, and which round trips dominate
// differs between a full backup (segment writes) and an incremental (prior-image
// reads), so attributing them is the only way to know which one to attack. All
// durations are in milliseconds; the concurrent ones sum across goroutines and so
// can exceed the wall clock.
type Phases struct {
	// TarReadMS is time spent pulling bytes out of the tar stream, which is the
	// floor set by PostgreSQL and the network.
	TarReadMS int64

	// PriorReadMS is time spent obtaining unchanged blocks from the prior backup
	// to rebuild a dirty segment, and PriorBlockReads the number of blocks so
	// obtained. Only a fraction of those reach the repository: Kopia's object
	// reader holds the chunk it is positioned in, so the first block of a
	// segment pays for fetching the whole segment and the rest are memory
	// copies. PriorReadMS is therefore dominated by roughly one fetch per
	// rebuilt segment, not by PriorBlockReads.
	PriorReadMS     int64
	PriorBlockReads int64

	// SegmentWriteMS is time spent writing segment objects (hash plus the
	// server's existence check plus the streamed write), summed across writers.
	SegmentWriteMS int64

	// IndexWriteMS is time spent writing the per-file seek tables, and
	// FinalizeMS the directory manifests plus SaveSnapshot at end of stream.
	IndexWriteMS int64
	FinalizeMS   int64
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

	// BlockReaderForObject returns a reader for an already-resolved prior
	// object, whose block numbers are relative to that object. Resolving a path
	// is not safe to do concurrently, so a rebuild resolves on the reading
	// goroutine and reads through this from a writer.
	BlockReaderForObject(oid object.ID, blockSize int) (PriorBlockReader, error)

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
// by more than an order of magnitude versus a multi-megabyte segment. It is a
// whole number of blocks for any BLCKSZ up to 128 KiB.
//
// The object count this implies is not free, and costs more than the index size
// this comment once claimed was near the floor. A 566 GB backup is ~4.6 M
// segments, and the post-backup `kopia snapshot verify` walks every object: it
// took 21m54s remotely, against seconds for the ~140 K objects the filesystem
// path produces for the same database at Kopia's ~4 MB dynamic chunks. Any
// revisit of this size should weigh verification and maintenance per object, not
// just amplification and index size.
const DefaultSegmentBytes = 128 << 10

// assembler builds a snapshot from a tar stream.
type assembler struct {
	ctx   context.Context //nolint:containedctx // bound to a single ingest call
	rep   repo.RepositoryWriter
	opts  Options
	start time.Time
	dirs  map[string]*dirNode
	stats Stats

	// segmentBlocks is the segment size in blocks (SegmentBytes / BlockSize) and
	// segBytes the same size in bytes. segBytes is also the pool's buffer size,
	// so recomputing it elsewhere risks a window that outgrows the buffer.
	segmentBlocks int
	segBytes      int
	// segMap records this backup's per-file ordered segment object IDs, carried
	// forward so the next incremental can reference unchanged segments.
	segMap map[string][]object.ID
	// compressor is the compression applied to segment data, resolved once from
	// the repository's effective policy for the source (see resolveCompressor).
	// Empty means no compression.
	compressor compression.Name
	// segWriter writes segment objects concurrently.
	segWriter *segmentWriter
	// phases accumulates the per-kind timings reported in Stats. Segment writes
	// are timed on the writer goroutines, so these are atomic.
	phases phaseCounters
	// pending holds the members already handed to the segment writers whose
	// object is not resolved yet, in stream order. See resolveSettled.
	pending []*pendingFile
}

// pendingFile is a member whose bytes have all been handed to the segment
// writers but whose object ID, and therefore its directory entry, is not known
// yet. entry is complete except for that ID.
type pendingFile struct {
	logical string
	entry   *snapshot.DirEntry
	batch   *segmentBatch
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
	if opts.WriteConcurrency <= 0 {
		opts.WriteConcurrency = DefaultWriteConcurrency
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
		segBytes:      segmentBlocks * opts.BlockSize,
		segMap:        map[string][]object.ID{},
		compressor:    resolveCompressor(ctx, rep, opts.Source),
	}
	a.segWriter = newSegmentWriter(a, opts.WriteConcurrency, a.segBytes)
	defer a.segWriter.close()

	// The data directory root is not a tar member, so default it to the mode
	// PostgreSQL uses for PGDATA.
	a.ensureDir(".").mode = 0o700

	if err := a.consume(tarStream); err != nil {
		return nil, err
	}

	// Every entry must be filed before the directories are built.
	if err := a.resolveAll(); err != nil {
		return nil, err
	}

	finalizeStart := time.Now()

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
		Stats: snapshot.Stats{
			TotalFileSize:  a.stats.TotalSize,
			TotalFileCount: int32(a.stats.FileCount), //nolint:gosec // file counts fit int32 in practice
		},
	}

	manID, err := snapshot.SaveSnapshot(ctx, rep, man)
	if err != nil {
		return nil, fmt.Errorf("while saving snapshot: %w", err)
	}
	a.phases.addFinalize(finalizeStart)
	a.stats.Phases = a.phases.snapshot()

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
		if err := a.resolveSettled(); err != nil {
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

	switch hdr.Typeflag {
	case tar.TypeReg:
		return a.addFile(tr, hdr, name)
	case tar.TypeSymlink:
		de, logical, err := a.addSymlink(hdr, name)
		if err != nil {
			return err
		}
		a.addEntry(logical, de)

		return nil
	default:
		// Block/char devices, fifos, etc. do not occur in a PGDATA backup.
		return nil
	}
}

// addEntry files a resolved entry under its parent directory. Directories are
// only built at end of stream, and Kopia sorts a directory's entries when it
// builds the manifest, so entries may be filed in any order: a file resolved
// after the reader moved past it lands in the same manifest it would have.
func (a *assembler) addEntry(logical string, de *snapshot.DirEntry) {
	a.ensureDir(path.Dir(logical)).builder.AddEntry(de)
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
func (a *assembler) fileContent(tr *tar.Reader, name string) (io.Reader, string, error) {
	if a.opts.Incremental && path.Base(name) == backupLabelFile {
		raw, rErr := io.ReadAll(tr)
		if rErr != nil {
			return nil, "", fmt.Errorf("while reading %q: %w", name, rErr)
		}

		return bytes.NewReader(stripIncrementalLabelLines(raw)), name, nil
	}

	return tr, name, nil
}

// addFile streams a regular file into the repository and files its directory
// entry. INCREMENTAL.* members are combined against the prior image when
// running an incremental backup, which resolves them inline; a member that
// arrives in full is handed to the segment writers and resolved later.
func (a *assembler) addFile(tr *tar.Reader, hdr *tar.Header, name string) error {
	if a.opts.Incremental && strings.HasPrefix(path.Base(name), IncrementalFilePrefix) {
		return a.addIncrementalFile(tr, hdr, name)
	}

	content, logical, err := a.fileContent(tr, name)
	if err != nil {
		return err
	}

	return a.writeSegmentedFile(hdr, logical, content)
}

// writeSegmentedFile splits content into fixed-size segment objects and queues
// the file for resolution once they are written. It does not wait for them: this
// goroutine only reads the stream and hands off buffers, so the writers stay
// busy across file boundaries instead of draining at every one of them.
func (a *assembler) writeSegmentedFile(
	hdr *tar.Header, logical string, content io.Reader,
) error {
	var (
		written int64
		readErr error
	)
	batch := newSegmentBatch()
	for index := 0; !batch.failed(); index++ {
		buf := a.segWriter.acquire()
		readStart := time.Now()
		n, err := io.ReadFull(content, buf[:a.segBytes])
		a.phases.addTarRead(readStart)
		if n > 0 {
			a.segWriter.submit(batch, logical, index, buf, n)
			written += int64(n)
		} else {
			a.segWriter.release(buf)
		}
		if err != nil {
			// A short read (ErrUnexpectedEOF) is the file's last segment and a
			// zero-length one (EOF) its end; anything else is a real failure.
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				readErr = fmt.Errorf("while reading file %q: %w", logical, err)
			}

			break
		}
	}

	batch.seal()

	if readErr != nil {
		// Drain before dropping this batch, so its buffers are back in the pool.
		_, _ = batch.wait()

		return readErr
	}

	// The object ID is the one thing still missing from the entry; resolveHead
	// fills it in once the segments are written.
	a.pending = append(a.pending, &pendingFile{
		logical: logical,
		entry:   a.fileEntry(hdr, logical, object.EmptyID, written),
		batch:   batch,
	})

	return a.capPending()
}

// maxPendingPerWriter caps the queue of unresolved files at this many per
// segment writer. Files normally resolve within a few members of being read, so
// the cap only bites when one file's segments lag far behind the reader, and it
// keeps the queue and the deferred index writes from growing without bound.
const maxPendingPerWriter = 4

// capPending keeps the queue within its cap, waiting for the writers when it is
// over. The queue is otherwise drained opportunistically, between members.
func (a *assembler) capPending() error {
	for len(a.pending) > maxPendingPerWriter*a.opts.WriteConcurrency {
		if err := a.resolveHead(); err != nil {
			return err
		}
	}

	return nil
}

// resolveSettled resolves the unresolved files whose segments have all been
// written, oldest first, and stops at the first one still in flight. It runs
// between tar members, so a file's index object is written while the writers are
// busy with the next file rather than in a burst at end of stream.
func (a *assembler) resolveSettled() error {
	for len(a.pending) > 0 && a.pending[0].batch.done() {
		if err := a.resolveHead(); err != nil {
			return err
		}
	}

	return nil
}

// resolveAll drains the queue, waiting for every outstanding segment. It must
// run before the directories are finalized, so that every entry is filed.
func (a *assembler) resolveAll() error {
	for len(a.pending) > 0 {
		if err := a.resolveHead(); err != nil {
			return err
		}
	}

	return nil
}

// resolveHead completes the oldest unresolved file, waiting for its segments.
func (a *assembler) resolveHead() error {
	p := a.pending[0]
	a.pending = a.pending[1:]

	segs, err := p.batch.wait()
	if err != nil {
		return fmt.Errorf("while writing file %q: %w", p.logical, err)
	}

	oid, err := a.finishFile(p.logical, segs)
	if err != nil {
		return err
	}
	p.entry.ObjectID = oid

	a.stats.FileCount++
	a.stats.TotalSize += p.entry.FileSize
	a.addEntry(p.logical, p.entry)

	return nil
}

// resolveCompressor returns the compressor the repository's effective policy
// selects for source, so segment data is compressed exactly as Kopia's own CLI
// snapshotter would be by `kopia policy set --compression`. One global policy
// therefore governs both the filesystem path and this helper. An empty name (no
// policy, or none resolvable) means no compression, matching the prior default.
func resolveCompressor(ctx context.Context, rep repo.Repository, source snapshot.SourceInfo) compression.Name {
	pol, _, _, err := policy.GetEffectivePolicy(ctx, rep, source)
	if err != nil {
		return ""
	}

	return pol.CompressionPolicy.CompressorName
}

// writeSegmentBytes writes one segment's bytes as a new object and returns its
// ID. It is called from the segment-writer pool, so it must touch nothing on the
// assembler beyond the repository writer and the resolved compressor.
func (a *assembler) writeSegmentBytes(data []byte, logical string) (object.ID, error) {
	defer a.phases.addSegmentWrite(time.Now())

	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "SEG:" + logical, Compressor: a.compressor})
	defer writer.Close() //nolint:errcheck

	if _, err := writer.Write(data); err != nil {
		return object.EmptyID, err
	}

	return writer.Result()
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

	defer a.phases.addIndexWrite(time.Now())

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
func (a *assembler) addIncrementalFile(tr *tar.Reader, hdr *tar.Header, name string) error {
	relPath := path.Join(path.Dir(name), strings.TrimPrefix(path.Base(name), IncrementalFilePrefix))

	header, err := ParseIncrementalHeader(tr, a.opts.BlockSize)
	if err != nil {
		return fmt.Errorf("while parsing incremental header for %q: %w", name, err)
	}
	if a.opts.Prior == nil {
		return fmt.Errorf("incremental member %q but no prior image configured", name)
	}

	// Metadata-only fast path: with no changed blocks and an unchanged length,
	// the whole file is byte-identical to the prior, so reference the prior
	// object directly and carry its segment list forward unchanged.
	if len(header.BlockNumbers) == 0 {
		if err := assertDrained(tr, make([]byte, 1)); err != nil {
			return fmt.Errorf("while combining %q: %w", name, err)
		}

		prior, err := a.opts.Prior.Entry(relPath)
		if err != nil {
			return fmt.Errorf("while resolving prior entry for %q: %w", relPath, err)
		}

		if prior.FileSize == int64(header.TruncationBlockLength)*int64(a.opts.BlockSize) {
			a.stats.FileCount++
			a.stats.ReusedFiles++
			a.stats.TotalSize += prior.FileSize
			a.carryPriorSegments(relPath, prior.ObjectID)
			a.addEntry(relPath, a.fileEntry(hdr, relPath, prior.ObjectID, prior.FileSize))

			return nil
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
) error {
	sb := a.segmentBlocks
	total := int(h.TruncationBlockLength)
	priorSegs, _ := a.opts.Prior.Segments(relPath)

	// Resolve the prior file once per member: resolution walks the prior tree
	// through a cache that must not be touched concurrently, so it cannot happen
	// on a worker, and its length is what decides whether referencing is even
	// structurally possible. A member with no prior entry is only an error if a
	// segment actually needs prior blocks, which sourceFor decides.
	prior := priorLocator{relPath: relPath, segments: priorSegs}
	prior.entry, prior.entryErr = a.opts.Prior.Entry(relPath)

	priorBlocks := 0
	if prior.entryErr == nil {
		priorBlocks = int(prior.entry.FileSize) / a.opts.BlockSize
	}

	// Only reference prior segments when the prior segment list is structurally
	// consistent with the prior file length (exactly one object per sb-block
	// segment). An inconsistent list — e.g. a whole-file object carried as a
	// single segment for a multi-segment file — must never be spliced; rebuild
	// every segment instead. This both rejects such a map and heals a chain
	// whose earlier backup persisted one. This count check assumes the prior map
	// was produced at the current segment size; that size match is the caller's
	// responsibility (see PriorImage.Segments), not verified here.
	prior.canReference = len(priorSegs) == (priorBlocks+sb-1)/sb

	cursor := 0 // index into h.BlockNumbers
	batch := newSegmentBatch()
	fillErr := error(nil)

	for start := 0; start < total; start += sb {
		end := min(start+sb, total)
		dirty := cursor < len(h.BlockNumbers) && int(h.BlockNumbers[cursor]) < end
		j := start / sb
		priorEnd := min(start+sb, priorBlocks)
		segLen := int64(end-start) * int64(a.opts.BlockSize)

		if !dirty && prior.canReference && j < len(priorSegs) && end == priorEnd {
			batch.set(j, segRef{oid: priorSegs[j], length: segLen})
			a.stats.ReferencedSegments++

			continue
		}

		// Read the blocks that arrived in the tar stream here, on this goroutine,
		// because the stream has to be consumed in order. Everything else about
		// this segment — fetching its unchanged blocks from the prior image and
		// writing it — is handed to a worker, so those round trips overlap
		// instead of running one at a time.
		buf := a.segWriter.acquire()
		holes, err := a.readPresentBlocks(blockData, h, &cursor, start, end, buf[:segLen])
		if err != nil {
			a.segWriter.release(buf)
			fillErr = fmt.Errorf("while combining %q: %w", relPath, err)

			break
		}

		fill, err := prior.sourceFor(holes, start, j)
		if err != nil {
			a.segWriter.release(buf)
			fillErr = err

			break
		}

		a.segWriter.submitRebuilt(batch, relPath, j, buf, int(segLen), fill)
		a.stats.RebuiltSegments++
	}

	// The stream checks have to run before this member is left behind.
	if fillErr == nil {
		fillErr = a.checkCombineConsumedStream(blockData, h, cursor, relPath)
	}
	batch.seal()
	if fillErr != nil {
		_, _ = batch.wait()

		return fillErr
	}

	size := int64(total) * int64(a.opts.BlockSize)
	a.pending = append(a.pending, &pendingFile{
		logical: relPath,
		entry:   a.fileEntry(hdr, relPath, object.EmptyID, size),
		batch:   batch,
	})

	return a.capPending()
}

// checkCombineConsumedStream verifies the combine consumed exactly the blocks
// the header promised, and that nothing follows them. Either mismatch means the
// header and the data section disagree, which would otherwise be stored as a
// silently corrupt segment.
func (a *assembler) checkCombineConsumedStream(
	blockData io.Reader, h *IncrementalHeader, cursor int, relPath string,
) error {
	if cursor != len(h.BlockNumbers) {
		return fmt.Errorf("%w: consumed %d of %d present blocks",
			ErrReconstructIncomplete, cursor, len(h.BlockNumbers))
	}
	if err := assertDrained(blockData, make([]byte, 1)); err != nil {
		return fmt.Errorf("while combining %q: %w", relPath, err)
	}

	return nil
}

// readPresentBlocks places the blocks of segment [start, end) that arrived in
// the tar stream into dst, advancing cursor, and returns the indices *within the
// segment* that it did not fill. Those holes are the segment's unchanged blocks,
// which a worker reads from the prior image; keeping only the stream read here is
// what allows that.
func (a *assembler) readPresentBlocks(
	blockData io.Reader, h *IncrementalHeader, cursor *int, start, end int, dst []byte,
) ([]int, error) {
	bs := a.opts.BlockSize

	var holes []int
	for i := start; i < end; i++ {
		if *cursor < len(h.BlockNumbers) && int(h.BlockNumbers[*cursor]) == i {
			block := dst[(i-start)*bs : (i-start+1)*bs]

			readStart := time.Now()
			_, err := io.ReadFull(blockData, block)
			a.phases.addTarRead(readStart)
			if err != nil {
				return nil, fmt.Errorf("while reading incremental block %d: %w", i, err)
			}
			*cursor++

			continue
		}

		holes = append(holes, i-start)
	}

	return holes, nil
}

// priorLocator is a member's prior-image state, resolved once on the goroutine
// reading the tar stream and consulted per segment.
type priorLocator struct {
	relPath  string
	segments []object.ID
	entry    *snapshot.DirEntry
	entryErr error

	// canReference says the segment list is structurally consistent with the
	// prior file's length, which is what makes segment k of it correspond to
	// this backup's segment k.
	canReference bool
}

// sourceFor decides where a rebuilt segment's unchanged blocks come from, or
// returns nil when it has none to fetch.
//
// The prior backup's segment at this index is preferred: it starts at the same
// block, so a hole at segment index k is block k of it, and being a single
// content means opening it costs one fetch and no seek table. It may be *shorter*
// than this segment when the file has grown, and that needs no special case: a
// block past its end reads as EOF, which the combine materializes as a
// zero-filled uninitialized extension block, exactly as PostgreSQL's
// pg_combinebackup does.
//
// Without a usable segment list the whole prior file stands in, at the cost of an
// extra fetch for its seek table. Either way the prior file has to exist, so a
// resolution failure surfaces here rather than being mistaken for an empty prior.
func (p *priorLocator) sourceFor(holes []int, start, j int) (*priorFill, error) {
	if len(holes) == 0 {
		return nil, nil //nolint:nilnil // no holes means no prior work, not an error
	}

	if p.canReference && j < len(p.segments) {
		return &priorFill{source: p.segments[j], srcFirst: 0, holes: holes}, nil
	}

	if p.entryErr != nil {
		return nil, fmt.Errorf("while resolving prior image of %q: %w", p.relPath, p.entryErr)
	}

	return &priorFill{source: p.entry.ObjectID, srcFirst: start, holes: holes}, nil
}

// fillFromPrior obtains a rebuilt segment's unchanged blocks. It runs on a
// segment writer, so it opens its own reader and shares nothing but the
// repository and the atomic counters.
func (a *assembler) fillFromPrior(fill *priorFill, dst []byte) error {
	bs := a.opts.BlockSize

	// Time from before the open: opening a prior object is what fetches its
	// content, so timing only the block copies would attribute nearly none of
	// the cost to the prior read.
	fetchStart := time.Now()
	defer a.phases.addPriorRead(fetchStart)

	reader, err := a.opts.Prior.BlockReaderForObject(fill.source, bs)
	if err != nil {
		return err
	}
	defer reader.Close() //nolint:errcheck

	for _, k := range fill.holes {
		block := dst[k*bs : (k+1)*bs]

		blockNum := uint32(fill.srcFirst + k) //nolint:gosec // block index fits uint32
		if rerr := copyPriorOrZeroBlock(reader, blockNum, block); rerr != nil {
			return rerr
		}
		a.phases.addPriorBlockRead()
	}

	return nil
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
