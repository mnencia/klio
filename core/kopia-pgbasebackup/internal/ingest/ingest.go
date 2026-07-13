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

	// Incremental indicates the tar contains INCREMENTAL.* members that must be
	// combined against the prior image.
	Incremental bool

	// Prior resolves prior-image block readers for incremental reconstruction.
	// Required when Incremental is true.
	Prior PriorImage

	// BlockSize is the PostgreSQL BLCKSZ used for incremental reconstruction.
	// Defaults to DefaultBlockSize.
	BlockSize int
}

// Stats holds the counters for an ingest.
type Stats struct {
	TotalSize int64
	FileCount int64

	// ReusedFiles is the number of unchanged files that referenced the prior
	// object directly instead of reading its blocks back to rebuild it.
	ReusedFiles int64
}

// Result is the outcome of an ingest, serialized to stdout by the CLI.
type Result struct {
	ManifestID   string
	RootObjectID string
	Stats        Stats
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

// assembler builds a snapshot from a tar stream.
type assembler struct {
	ctx   context.Context //nolint:containedctx // bound to a single ingest call
	rep   repo.RepositoryWriter
	opts  Options
	start time.Time
	dirs  map[string]*dirNode
	stats Stats
}

// Run ingests the tar stream and saves a snapshot, returning its identifiers.
func Run(ctx context.Context, rep repo.RepositoryWriter, tarStream io.Reader, opts Options) (*Result, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}

	start := time.Now()

	a := &assembler{
		ctx:   ctx,
		rep:   rep,
		opts:  opts,
		start: start,
		dirs:  map[string]*dirNode{},
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
	}, nil
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
// backup_label so the combined tree is a self-contained full backup. The
// returned finish is a no-op here; INCREMENTAL.* members are handled separately
// by addIncrementalFile. The finish signature is kept so writeFile can serve
// both paths.
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

	return a.writeFile(hdr, logical, content, finish)
}

// writeFile streams content into a new repository object and returns the file's
// DirEntry. finish reconciles the io.Copy error with any background producer
// (incremental reconstruction); for plain members it is a no-op.
func (a *assembler) writeFile(
	hdr *tar.Header, logical string, content io.Reader, finish func(error) error,
) (*snapshot.DirEntry, string, error) {
	writer := a.rep.NewObjectWriter(a.ctx, object.WriterOptions{Description: "FILE:" + logical})
	defer writer.Close() //nolint:errcheck

	written, copyErr := io.Copy(writer, content)
	if err := finish(copyErr); err != nil {
		return nil, "", fmt.Errorf("while writing file %q: %w", logical, err)
	}

	oid, err := writer.Result()
	if err != nil {
		return nil, "", fmt.Errorf("while finalizing file %q: %w", logical, err)
	}

	a.stats.FileCount++
	a.stats.TotalSize += written

	return a.fileEntry(hdr, logical, oid, written), logical, nil
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

// reconstructReader streams the reconstructed full image produced by a
// background Reconstruct goroutine. The consumer must call finish to release
// the goroutine (and the prior image it holds) and to learn the real error.
type reconstructReader struct {
	pr   *io.PipeReader
	done <-chan error
}

func (r *reconstructReader) Read(p []byte) (int, error) { return r.pr.Read(p) }

// finish reconciles the consumer's io.Copy error with the reconstruction
// result. It must be called exactly once after the io.Copy that drained Read.
//
// Closing the pipe reader unblocks the producer if io.Copy stopped early
// (e.g. a repository write error), preventing a goroutine leak and ensuring the
// prior image is closed. When io.Copy failed, that error is the root cause
// (it is either the reconstruction error propagated through the pipe, or the
// downstream write error that aborted the copy), so it is preferred.
func (r *reconstructReader) finish(copyErr error) error {
	r.pr.CloseWithError(copyErr) //nolint:gosec // always returns nil
	recErr := <-r.done
	if copyErr != nil {
		return copyErr
	}

	return recErr
}

// addIncrementalFile combines an INCREMENTAL.* member against the prior image
// and returns the resulting file DirEntry and its logical path (INCREMENTAL.
// prefix stripped). A file with no changed blocks whose length still matches
// the prior reuses the prior object directly (metadata only, no block reads);
// otherwise the full image is reconstructed block by block and written.
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

	// Metadata-only fast path: with no changed blocks the rebuilt file would be
	// byte-identical to the prior object, so reference that object directly
	// instead of reading every block back to rebuild and re-hash an identical
	// file. Block-level reconstruction plus Kopia dedup converges on the same
	// object; this skips the read-back and rehash. A file that shrank
	// (truncation shorter than the prior) falls through to reconstruct its
	// prefix, which is a genuinely different object.
	if len(header.BlockNumbers) == 0 {
		// A zero-block incremental is header-only. Reject trailing data before
		// trusting it, the same integrity check reconstruction applies via
		// assertDrained (the fast path never reaches Reconstruct). A well-formed
		// member is already at EOF here, so this consumes nothing.
		if err := assertDrained(tr, make([]byte, 1)); err != nil {
			return nil, "", fmt.Errorf("while combining %q: %w", name, err)
		}

		prior, err := a.opts.Prior.Entry(relPath)
		if err != nil {
			return nil, "", fmt.Errorf("while resolving prior entry for %q: %w", relPath, err)
		}

		// Reuse the prior object only when the file is unchanged in full (its
		// length still matches). A shrunk file (shorter truncation) is a genuine
		// prefix of the prior, so it falls through to reconstruct that prefix.
		if prior.FileSize == int64(header.TruncationBlockLength)*int64(a.opts.BlockSize) {
			a.stats.FileCount++
			a.stats.ReusedFiles++
			a.stats.TotalSize += prior.FileSize

			return a.fileEntry(hdr, relPath, prior.ObjectID, prior.FileSize), relPath, nil
		}
	}

	prior, err := a.opts.Prior.BlockReader(relPath, a.opts.BlockSize)
	if err != nil {
		return nil, "", fmt.Errorf("while opening prior image of %q: %w", relPath, err)
	}

	reader := startReconstruct(header, tr, prior)

	return a.writeFile(hdr, relPath, reader, reader.finish)
}

// startReconstruct spawns the reconstruction of an incremental member against
// its prior image and returns a reader streaming the rebuilt full image. The
// caller must call the reader's finish exactly once to release the goroutine
// (and the prior image) and to learn the real error.
func startReconstruct(header *IncrementalHeader, blockData io.Reader, prior PriorBlockReader) *reconstructReader {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		defer prior.Close() //nolint:errcheck
		err := Reconstruct(header, blockData, prior, pw)
		pw.CloseWithError(err)
		done <- err
	}()

	return &reconstructReader{pr: pr, done: done}
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
