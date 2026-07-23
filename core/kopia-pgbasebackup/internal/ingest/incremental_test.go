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
	"time"
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

func TestReconstructRejectsTrailingData(t *testing.T) {
	// Hand-build a file whose header claims one block but whose data section
	// carries two: reconstruction must not silently discard the extra block.
	var buf bytes.Buffer
	w32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	w32(incrementalMagic)
	w32(1) // nblocks claimed
	w32(3) // truncation
	w32(0) // present block 0
	for buf.Len()%testBlockSize != 0 {
		buf.WriteByte(0)
	}
	buf.Write(presentBlock(0))
	buf.Write(presentBlock(1)) // undeclared trailing block

	r := bytes.NewReader(buf.Bytes())
	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	prior := mapPrior{blocks: map[uint32][]byte{1: priorBlock(1), 2: priorBlock(2)}}
	err = Reconstruct(h, r, prior, &bytes.Buffer{})
	if !errors.Is(err, ErrIncrementalTrailingData) {
		t.Fatalf("expected ErrIncrementalTrailingData, got %v", err)
	}
}

func TestReconstructMixesIncrementalAndPrior(t *testing.T) {
	// truncation 5, blocks 1 and 3 changed; 0,2,4 come from prior.
	data := buildIncrementalFile(t, 5, []uint32{1, 3})
	r := bytes.NewReader(data)
	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	prior := mapPrior{blocks: map[uint32][]byte{
		0: priorBlock(0),
		2: priorBlock(2),
		4: priorBlock(4),
	}}

	var out bytes.Buffer
	if err := Reconstruct(h, r, prior, &out); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}

	got := out.Bytes()
	if len(got) != 5*testBlockSize {
		t.Fatalf("output length = %d, want %d", len(got), 5*testBlockSize)
	}
	// Verify each block came from the right source.
	want := bytes.Join([][]byte{
		priorBlock(0),
		presentBlock(1),
		priorBlock(2),
		presentBlock(3),
		priorBlock(4),
	}, nil)
	if !bytes.Equal(got, want) {
		t.Fatal("reconstructed output mismatch")
	}
}

func TestReconstructZeroBlockIsFullPrior(t *testing.T) {
	// Unchanged file: zero present blocks, reconstructed entirely from prior.
	data := buildIncrementalFile(t, 3, nil)
	r := bytes.NewReader(data)
	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	prior := mapPrior{blocks: map[uint32][]byte{
		0: priorBlock(0),
		1: priorBlock(1),
		2: priorBlock(2),
	}}

	var out bytes.Buffer
	if err := Reconstruct(h, r, prior, &out); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if out.Len() != 3*testBlockSize {
		t.Fatalf("output length = %d, want %d", out.Len(), 3*testBlockSize)
	}
}

func TestReconstructMissingPriorBlockZeroFills(t *testing.T) {
	// Block 2 is below truncation but present in neither the incremental nor the
	// prior image (the prior is shorter): it is an uninitialized extension block
	// and must be reconstructed as zero-filled, matching pg_combinebackup, not
	// treated as an error.
	data := buildIncrementalFile(t, 3, []uint32{0})
	r := bytes.NewReader(data)
	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// mapPrior returns io.EOF for any block it does not hold, modeling a prior
	// image shorter than the truncation length.
	prior := mapPrior{blocks: map[uint32][]byte{
		1: priorBlock(1),
	}}

	var out bytes.Buffer
	if err := Reconstruct(h, r, prior, &out); err != nil {
		t.Fatalf("reconstruct: %v", err)
	}

	want := bytes.Join([][]byte{
		presentBlock(0),
		priorBlock(1),
		make([]byte, testBlockSize), // block 2: zero-filled
	}, nil)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatal("reconstructed output mismatch; block 2 should be zero-filled")
	}
}

// errPrior returns a non-EOF error for every block, modeling a genuine read
// failure (I/O or decode error) as opposed to a block merely past the end.
type errPrior struct{ err error }

func (e errPrior) ReadBlock(uint32) ([]byte, error) { return nil, e.err }
func (errPrior) Close() error                       { return nil }

func TestReconstructPriorReadErrorFails(t *testing.T) {
	// A genuine (non-EOF) failure reading a needed prior block must still fail
	// loudly rather than silently zero-filling.
	data := buildIncrementalFile(t, 2, []uint32{0})
	r := bytes.NewReader(data)
	h, err := ParseIncrementalHeader(r, testBlockSize)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	boom := errors.New("disk on fire")
	if err := Reconstruct(h, r, errPrior{err: boom}, &bytes.Buffer{}); !errors.Is(err, ErrBlockNotAvailable) {
		t.Fatalf("expected ErrBlockNotAvailable, got %v", err)
	}
}

func TestReconstructReaderFinishUnblocksProducer(t *testing.T) {
	// A consumer that errors before draining must not leak the producer
	// goroutine: finish closes the pipe, unblocking the stuck writer.
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		_, err := pw.Write(make([]byte, 1024)) // blocks until pipe is closed
		pw.CloseWithError(err)
		done <- err
		close(exited)
	}()

	rr := &reconstructReader{pr: pr, done: done}
	boom := errors.New("downstream write failed")
	if got := rr.finish(boom); !errors.Is(got, boom) {
		t.Fatalf("finish returned %v, want %v", got, boom)
	}

	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine leaked after finish")
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
