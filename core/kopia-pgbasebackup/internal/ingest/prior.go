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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

var (
	// ErrPriorEntryNotFound is returned when a path cannot be resolved in the
	// prior snapshot tree.
	ErrPriorEntryNotFound = errors.New("entry not found in prior snapshot")

	// ErrPriorEntryWrongType is returned when a path component in the prior
	// snapshot tree has an unexpected type (e.g. a symlink or directory where a
	// regular file is required). Reading such an object as block data would
	// silently corrupt the reconstructed relation.
	ErrPriorEntryWrongType = errors.New("prior snapshot entry has unexpected type")
)

// KopiaPriorImage resolves prior-image files from a prior snapshot's directory
// tree by decoding the directory-manifest objects (which Kopia stores as JSON).
type KopiaPriorImage struct {
	ctx     context.Context //nolint:containedctx // bound to a single ingest call
	rep     repo.Repository
	rootOID object.ID
}

// NewKopiaPriorImage builds a prior image rooted at the given directory object.
func NewKopiaPriorImage(ctx context.Context, rep repo.Repository, rootOID object.ID) *KopiaPriorImage {
	return &KopiaPriorImage{ctx: ctx, rep: rep, rootOID: rootOID}
}

// BlockReader implements PriorImage.
//
//nolint:ireturn // returns the PriorBlockReader abstraction by design
func (p *KopiaPriorImage) BlockReader(relPath string, blockSize int) (PriorBlockReader, error) {
	oid, err := p.resolve(relPath)
	if err != nil {
		return nil, err
	}

	reader, err := p.rep.OpenObject(p.ctx, oid)
	if err != nil {
		return nil, fmt.Errorf("while opening prior object for %q: %w", relPath, err)
	}

	return &objectBlockReader{reader: reader, blockSize: blockSize}, nil
}

// resolve walks the directory tree to find the object ID for relPath. Every
// intermediate component must be a directory and the final component a regular
// file; otherwise resolution fails rather than reading a non-file object (a
// symlink target or a directory manifest) as relation block data.
func (p *KopiaPriorImage) resolve(relPath string) (object.ID, error) {
	current := p.rootOID
	clean := path.Clean(strings.TrimPrefix(relPath, "./"))

	parts := make([]string, 0)
	for part := range strings.SplitSeq(clean, "/") {
		if part == "" || part == "." {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return object.EmptyID, fmt.Errorf("%w: %q resolves to the tree root", ErrPriorEntryNotFound, relPath)
	}

	for i, part := range parts {
		manifest, err := p.readDirManifest(current)
		if err != nil {
			return object.EmptyID, err
		}

		entry := findEntry(manifest, part)
		if entry == nil {
			return object.EmptyID, fmt.Errorf("%w: %q (at %q)", ErrPriorEntryNotFound, relPath, part)
		}

		want := snapshot.EntryTypeDirectory
		if i == len(parts)-1 {
			want = snapshot.EntryTypeFile
		}
		if entry.Type != want {
			return object.EmptyID, fmt.Errorf("%w: %q (at %q) is %q, want %q",
				ErrPriorEntryWrongType, relPath, part, entry.Type, want)
		}

		current = entry.ObjectID
	}

	return current, nil
}

// readDirManifest decodes a directory object into its DirManifest.
func (p *KopiaPriorImage) readDirManifest(oid object.ID) (*snapshot.DirManifest, error) {
	reader, err := p.rep.OpenObject(p.ctx, oid)
	if err != nil {
		return nil, fmt.Errorf("while opening directory object %v: %w", oid, err)
	}
	defer reader.Close() //nolint:errcheck

	var manifest snapshot.DirManifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("while decoding directory manifest %v: %w", oid, err)
	}

	return &manifest, nil
}

func findEntry(manifest *snapshot.DirManifest, name string) *snapshot.DirEntry {
	for _, e := range manifest.Entries {
		if e.Name == name {
			return e
		}
	}

	return nil
}

// objectBlockReader reads fixed-size blocks from a Kopia object at increasing
// offsets.
type objectBlockReader struct {
	reader    object.Reader
	blockSize int
}

// Close releases the underlying Kopia object reader.
func (o *objectBlockReader) Close() error {
	return o.reader.Close()
}

func (o *objectBlockReader) ReadBlock(n uint32) ([]byte, error) {
	offset := int64(n) * int64(o.blockSize)
	if _, err := o.reader.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("while seeking prior block %d: %w", n, err)
	}

	buf := make([]byte, o.blockSize)
	if _, err := io.ReadFull(o.reader, buf); err != nil {
		return nil, fmt.Errorf("while reading prior block %d: %w", n, err)
	}

	return buf, nil
}
