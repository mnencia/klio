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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// incrementalMagic is the magic number prefixing a server-emitted INCREMENTAL.*
// file (PostgreSQL INCREMENTAL_MAGIC, see basebackup_incremental.c).
const incrementalMagic uint32 = 0xd3ae1f0d

// IncrementalFilePrefix is the file-name prefix the server gives to incremental
// relation files inside an incremental BASE_BACKUP tar.
const IncrementalFilePrefix = "INCREMENTAL."

// DefaultBlockSize is the PostgreSQL default block size (BLCKSZ).
const DefaultBlockSize = 8192

var (
	// ErrBadIncrementalMagic is returned when the INCREMENTAL.* header does not
	// start with the expected magic number.
	ErrBadIncrementalMagic = errors.New("invalid incremental file magic")

	// ErrBlockNotAvailable is returned during reconstruction when a block that
	// is not in the incremental cannot be read from the prior image for a
	// genuine reason (an I/O or decode error, not merely being past the prior's
	// end). A block simply beyond the prior image is treated as a zero-filled
	// uninitialized extension block, matching PostgreSQL's pg_combinebackup.
	ErrBlockNotAvailable = errors.New("prior image block could not be read")

	// ErrBlocksNotAscending is returned when the present-block array is not
	// sorted ascending, which the single-pass reconstruction relies on.
	ErrBlocksNotAscending = errors.New("incremental block numbers are not ascending")

	// ErrBlockBeyondTruncation is returned when a present block number is at or
	// beyond the truncation block length. PostgreSQL guarantees present blocks
	// are strictly below truncation; a violation would leave that block's data
	// unread, silently corrupting the reconstructed segment.
	ErrBlockBeyondTruncation = errors.New("incremental block number at or beyond truncation block length")

	// ErrReconstructIncomplete is returned when reconstruction finishes without
	// consuming every present block (and its data). It guards against a silent
	// miscombine that would store a corrupt segment as if it were valid.
	ErrReconstructIncomplete = errors.New("incremental reconstruction did not consume all present blocks")

	// ErrIncrementalTrailingData is returned when the incremental block stream
	// still has bytes after all present blocks were consumed, indicating the
	// header and the data section disagree on the block count.
	ErrIncrementalTrailingData = errors.New("incremental file has trailing data after the last block")
)

// IncrementalHeader is the parsed header of an INCREMENTAL.* file.
type IncrementalHeader struct {
	// BlockNumbers are the relative block numbers present in the incremental
	// file, in ascending order.
	BlockNumbers []uint32

	// TruncationBlockLength is the length, in blocks, that the reconstructed
	// relation segment must have.
	TruncationBlockLength uint32
}

// ParseIncrementalHeader reads and validates the header of an INCREMENTAL.*
// file from r, consuming exactly the BLCKSZ-aligned header (including padding)
// so that r is positioned at the first block of data on return.
func ParseIncrementalHeader(r io.Reader, blockSize int) (*IncrementalHeader, error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	var fixed [12]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, fmt.Errorf("while reading incremental header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(fixed[0:4])
	if magic != incrementalMagic {
		return nil, fmt.Errorf("%w: got %#x", ErrBadIncrementalMagic, magic)
	}

	nblocks := binary.LittleEndian.Uint32(fixed[4:8])
	truncation := binary.LittleEndian.Uint32(fixed[8:12])

	blockNumbers, err := readAscendingBlockNumbers(r, nblocks)
	if err != nil {
		return nil, err
	}

	// Present blocks must lie strictly below the truncation length. The combine
	// only walks output blocks in [0, truncation), so a block at or beyond it
	// would never be read from the data section, silently corrupting the result.
	if n := len(blockNumbers); n > 0 && blockNumbers[n-1] >= truncation {
		return nil, fmt.Errorf("%w: block %d, truncation %d",
			ErrBlockBeyondTruncation, blockNumbers[n-1], truncation)
	}

	// When the file carries block data, it is BLCKSZ-aligned, so discard the
	// padding after the variable-length header. A zero-block incremental (an
	// unchanged file) is header-only: no padding and no data follow, so there
	// is nothing to skip.
	if nblocks > 0 {
		rawHeaderLen := 12 + 4*int(nblocks)
		padded := roundUp(rawHeaderLen, blockSize)
		if pad := padded - rawHeaderLen; pad > 0 {
			if _, err := io.CopyN(io.Discard, r, int64(pad)); err != nil {
				return nil, fmt.Errorf("while skipping incremental header padding: %w", err)
			}
		}
	}

	return &IncrementalHeader{
		BlockNumbers:          blockNumbers,
		TruncationBlockLength: truncation,
	}, nil
}

// readAscendingBlockNumbers reads n little-endian block numbers and verifies
// they are strictly ascending, which the single-pass reconstruction relies on.
func readAscendingBlockNumbers(r io.Reader, n uint32) ([]uint32, error) {
	if n == 0 {
		return nil, nil
	}

	raw := make([]byte, 4*int(n))
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("while reading incremental block numbers: %w", err)
	}

	blockNumbers := make([]uint32, n)
	var prev uint32
	for i := range blockNumbers {
		blockNumbers[i] = binary.LittleEndian.Uint32(raw[4*i : 4*i+4])
		if i > 0 && blockNumbers[i] <= prev {
			return nil, ErrBlocksNotAscending
		}
		prev = blockNumbers[i]
	}

	return blockNumbers, nil
}

// PriorBlockReader reads block number n (0-based) from the prior full image of
// the relation segment being reconstructed. Close releases the underlying
// prior-image object once reconstruction is done.
type PriorBlockReader interface {
	ReadBlock(n uint32) ([]byte, error)
	io.Closer
}

// copyPriorOrZeroBlock copies output block i from the prior image into dst. A
// block at or beyond the prior image (io.EOF) is one the server extended the
// file to but never WAL-logged (an uninitialized page); PostgreSQL's
// pg_combinebackup materializes it as zero-filled, so match that rather than
// failing. Any other read failure is a genuine error.
func copyPriorOrZeroBlock(prior PriorBlockReader, i uint32, dst []byte) error {
	block, err := prior.ReadBlock(i)
	if err != nil {
		if errors.Is(err, io.EOF) {
			clear(dst)

			return nil
		}

		return fmt.Errorf("%w: block %d: %w", ErrBlockNotAvailable, i, err)
	}
	// dst is a window into a reused segment buffer, so a short block would leave
	// the previous segment's bytes in its tail instead of failing. Reject it:
	// a PriorBlockReader owes a whole block or an error.
	if len(block) != len(dst) {
		return fmt.Errorf("%w: block %d: got %d bytes, want %d",
			ErrBlockNotAvailable, i, len(block), len(dst))
	}
	copy(dst, block)

	return nil
}

// assertDrained verifies r is at EOF, using scratch as a one-byte probe buffer.
func assertDrained(r io.Reader, scratch []byte) error {
	_, err := io.ReadFull(r, scratch[:1])
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return ErrIncrementalTrailingData
	default:
		return fmt.Errorf("while checking for trailing incremental data: %w", err)
	}
}

func roundUp(n, multiple int) int {
	if multiple <= 0 {
		return n
	}
	r := n % multiple
	if r == 0 {
		return n
	}

	return n + (multiple - r)
}
