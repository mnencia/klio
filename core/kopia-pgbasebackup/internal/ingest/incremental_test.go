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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// testBlockSize is a small BLCKSZ to keep fixtures readable.
const testBlockSize = 16

// buildIncrementalFile builds an in-stream INCREMENTAL.* file: header (magic,
// nblocks, truncation, ascending block numbers), BLCKSZ padding, then one
// block of data per present block (filled with the block number).
func buildIncrementalFile(t *testing.T, truncation uint32, present []uint32) []byte {
	t.Helper()
	var buf bytes.Buffer
	w32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	w32(incrementalMagic)
	w32(uint32(len(present))) //nolint:gosec // test fixture, small slice
	w32(truncation)
	for _, p := range present {
		w32(p)
	}
	// A zero-block incremental is header-only (no padding, no data), matching
	// PostgreSQL. Only pad and append data when there are present blocks.
	if len(present) > 0 {
		for buf.Len()%testBlockSize != 0 {
			buf.WriteByte(0)
		}
		for _, p := range present {
			buf.Write(presentBlock(p))
		}
	}

	return buf.Bytes()
}

// presentBlock is the data block for a changed block p (distinct from prior).
func presentBlock(p uint32) []byte {
	return bytes.Repeat([]byte{byte(p) | 0x80}, testBlockSize) //nolint:gosec // test fixture, values < 128
}

// priorBlock is the data block for an unchanged block n in the prior image.
func priorBlock(n uint32) []byte {
	return bytes.Repeat([]byte{byte(n)}, testBlockSize) //nolint:gosec // test fixture, values < 128
}

// mapPrior serves prior blocks from a map; missing blocks error.
type mapPrior struct {
	blocks map[uint32][]byte
}

func (m mapPrior) ReadBlock(n uint32) ([]byte, error) {
	b, ok := m.blocks[n]
	if !ok {
		return nil, io.EOF
	}

	return b, nil
}

func (m mapPrior) Close() error { return nil }

func TestParseIncrementalHeader(t *testing.T) {
	data := buildIncrementalFile(t, 5, []uint32{1, 3})
	r := bytes.NewReader(data)

	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if h.TruncationBlockLength != 5 {
		t.Fatalf("truncation = %d, want 5", h.TruncationBlockLength)
	}
	if len(h.BlockNumbers) != 2 || h.BlockNumbers[0] != 1 || h.BlockNumbers[1] != 3 {
		t.Fatalf("block numbers = %v", h.BlockNumbers)
	}
	// Reader must be positioned at the first data block.
	rest, _ := io.ReadAll(r)
	if len(rest) != 2*testBlockSize {
		t.Fatalf("remaining data = %d bytes, want %d", len(rest), 2*testBlockSize)
	}
}

func TestParseIncrementalHeaderRejectsBadMagic(t *testing.T) {
	bad := make([]byte, 12)
	binary.LittleEndian.PutUint32(bad[0:4], 0xdeadbeef)
	_, err := ParseIncrementalHeader(bytes.NewReader(bad), testBlockSize)
	if err == nil {
		t.Fatal("expected magic error")
	}
}

func TestParseIncrementalHeaderRejectsNonAscending(t *testing.T) {
	var buf bytes.Buffer
	w32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	w32(incrementalMagic)
	w32(2)
	w32(5)
	w32(3)
	w32(3) // not strictly ascending
	_, err := ParseIncrementalHeader(&buf, testBlockSize)
	if err == nil {
		t.Fatal("expected ascending error")
	}
}

func TestParseIncrementalHeaderRejectsBlockBeyondTruncation(t *testing.T) {
	// Present block 5 with truncation 5: block 5 is out of [0, 5) range.
	data := buildIncrementalFile(t, 5, []uint32{1, 5})
	_, err := ParseIncrementalHeader(bytes.NewReader(data), testBlockSize)
	if !errors.Is(err, ErrBlockBeyondTruncation) {
		t.Fatalf("expected ErrBlockBeyondTruncation, got %v", err)
	}
}

func TestCopyPriorOrZeroBlockZeroFillsBeyondPrior(t *testing.T) {
	// A block the prior image does not hold (io.EOF) is an uninitialized
	// extension block below truncation: copyPriorOrZeroBlock must zero-fill it,
	// matching pg_combinebackup, rather than failing. This is the shared helper
	// the live fillSegment path uses; full-file reconstruction correctness is
	// covered by TestIncrementalSegmentReference and the chain test.
	prior := mapPrior{blocks: map[uint32][]byte{1: priorBlock(1)}} // block 2 absent -> EOF
	// Pre-dirtied, because the helper writes into a reused pool buffer and so
	// must clear what was there rather than leave a stale block behind.
	dst := bytes.Repeat([]byte{0xFF}, testBlockSize)
	if err := copyPriorOrZeroBlock(prior, 2, dst); err != nil {
		t.Fatalf("copyPriorOrZeroBlock: %v", err)
	}
	if !bytes.Equal(dst, make([]byte, testBlockSize)) {
		t.Fatal("absent prior block should be zero-filled")
	}
}

// shortPrior returns a block shorter than one block, modeling a broken
// PriorBlockReader.
type shortPrior struct{}

func (shortPrior) ReadBlock(uint32) ([]byte, error) { return make([]byte, testBlockSize-1), nil }
func (shortPrior) Close() error                     { return nil }

func TestCopyPriorOrZeroBlockRejectsShortBlock(t *testing.T) {
	// The destination is a window into a reused segment buffer, so accepting a
	// short block would leave the previous segment's bytes in its tail. Fail
	// instead of storing a segment that is part stale data.
	dst := bytes.Repeat([]byte{0xFF}, testBlockSize)
	err := copyPriorOrZeroBlock(shortPrior{}, 7, dst)
	if !errors.Is(err, ErrBlockNotAvailable) {
		t.Fatalf("expected ErrBlockNotAvailable, got %v", err)
	}
}

// errPrior returns a non-EOF error for every block, modeling a genuine read
// failure (I/O or decode error) as opposed to a block merely past the end.
type errPrior struct{ err error }

func (e errPrior) ReadBlock(uint32) ([]byte, error) { return nil, e.err }
func (errPrior) Close() error                       { return nil }

func TestCopyPriorOrZeroBlockReadErrorFails(t *testing.T) {
	// A genuine (non-EOF) failure reading a needed prior block must fail loudly
	// rather than silently zero-filling.
	boom := errors.New("disk on fire")
	err := copyPriorOrZeroBlock(errPrior{err: boom}, 0, make([]byte, testBlockSize))
	if !errors.Is(err, ErrBlockNotAvailable) {
		t.Fatalf("expected ErrBlockNotAvailable, got %v", err)
	}
}

func TestUserTags(t *testing.T) {
	if userTags(nil) != nil {
		t.Fatal("nil tags should return nil")
	}
	out := userTags(map[string]string{"klio.io/content": "pgdata", "klio.io/tag": "b1"})
	if out["tag:klio.io/content"] != "pgdata" || out["tag:klio.io/tag"] != "b1" {
		t.Fatalf("unexpected prefixed tags: %v", out)
	}
	if _, ok := out["klio.io/content"]; ok {
		t.Fatal("unprefixed key must not be present")
	}
}

func TestStripIncrementalLabelLines(t *testing.T) {
	in := []byte("START WAL LOCATION: 0/A000028\nLABEL: x\nSTART TIMELINE: 1\n" +
		"INCREMENTAL FROM LSN: 0/8000028\nINCREMENTAL FROM TLI: 1\n")
	out := string(stripIncrementalLabelLines(in))
	if strings.Contains(out, "INCREMENTAL FROM") {
		t.Fatalf("INCREMENTAL FROM lines not stripped: %q", out)
	}
	for _, want := range []string{"START WAL LOCATION: 0/A000028", "LABEL: x", "START TIMELINE: 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q to remain, got %q", want, out)
		}
	}
}
