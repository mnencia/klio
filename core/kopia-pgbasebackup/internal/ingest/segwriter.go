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
	"sync"

	"github.com/kopia/kopia/repo/object"
)

// DefaultWriteConcurrency is the number of segments written to the repository
// at once.
//
// Writing a segment is latency-bound, not CPU-bound: on the Kopia server path
// the client hashes the segment locally, then blocks on a synchronous
// GetContentInfo round trip (the server's dedup existence check, performed for
// every write of 50 KB or more) before streaming the write asynchronously. At
// 128 KiB segments that round trip is the dominant per-segment cost, and on a
// full backup it seldom hits (the data was just hashed for the first time), so
// a sequential writer spends most of a backup waiting on it. Overlapping the
// writes hides it: in a synthetic write loop against a real Kopia server,
// 128 KiB segments went from 192 MiB/s at concurrency 1 to 422 MiB/s at 16,
// which is what the same repository sustains with multi-megabyte objects.
//
// The useful concurrency is therefore set by the round-trip time, and 16 is
// where the curve flattens. The cost is bounded: at most twice this many segment
// buffers are live at once (a few MiB).
//
// Deliberately not derived from runtime.NumCPU, the way Kopia sizes its own
// uploader (see Uploader.effectiveParallelFileReads): that uploader hashes,
// compresses and encrypts locally, so its work is CPU-bound, while here only the
// hash is local (the server compresses and encrypts) and the rest is waiting. A
// CPU rule would cut concurrency on exactly the deployments that need it, such
// as a CPU-limited sidecar talking to a server whose round trip is unchanged.
// Concurrency that does track cores is already applied on the side that knows
// them: the repository server dispatches at most 2*NumCPU requests at once.
const DefaultWriteConcurrency = 16

// segmentWriter writes segment objects on a pool of goroutines shared by the
// whole ingest, so one segment's server round trip overlaps with the next.
//
// Driving one repository writer from several goroutines is how Kopia's own
// uploader works (it uploads many files at once through one writer), and the
// gRPC client multiplexes requests by ID, so nothing here needs serializing.
type segmentWriter struct {
	a    *assembler
	jobs chan segmentJob

	// free returns spare buffers to the producer. A buffer is owned by exactly
	// one party at a time — the producer while it fills it, then a worker until
	// the segment is written — so its bytes are never read while being
	// overwritten. Taking a buffer from here is also what bounds how far ahead
	// of the writers the producer may run.
	free chan []byte

	wg sync.WaitGroup
}

// segmentJob is one segment to write. data is the filled prefix of buf, and buf
// is what goes back to the pool once the segment is stored. fill, when set,
// names prior-image blocks the worker must obtain before the segment is
// complete: an incremental hands off a segment holding only the blocks that
// arrived in the tar stream.
type segmentJob struct {
	data    []byte
	buf     []byte
	logical string
	index   int
	batch   *segmentBatch
	fill    *priorFill
}

// priorFill is the prior-image work a worker does before writing a rebuilt
// segment: read each block in holes out of source and place it in the segment.
//
// Doing this on the worker is the point. A prior read costs a round trip to the
// repository (8.4 ms against a cross-zone server, where moving the same 128 KB
// costs about 1 ms), and there is one per rebuilt segment, so performing them one
// at a time on the goroutine reading the tar stream made them nearly the whole
// cost of an incremental backup.
//
// source is usually the prior backup's *segment* at this index rather than the
// whole prior file: it covers exactly the blocks this segment needs, it is a
// single content so opening it is one fetch and no seek table, and it needs no
// path resolution. Falling back to the whole prior file costs an extra fetch for
// its seek table.
//
// holes are indices within the segment, so hole k lives at k*BLCKSZ in the
// segment and is read from block srcFirst+k of source. srcFirst is 0 when source
// is this segment's prior counterpart and the segment's first block number when
// it is the whole prior file, which is the only difference between the two cases.
type priorFill struct {
	source   object.ID
	srcFirst int
	holes    []int
}

// segmentBatch collects one file's segment writes. Results are recorded by
// index, so the file's segment order does not depend on the order the workers
// happen to finish in.
//
// A batch is pollable as well as waitable (see settled), so the reader can move
// on to the next tar member and come back to a file once its segments have
// landed, instead of standing still at every file boundary.
type segmentBatch struct {
	mu   sync.Mutex
	refs []segRef
	err  error

	// remaining counts segments submitted but not yet recorded, and sealed says
	// the producer has submitted the last one. settled is closed when both are
	// true, which is the only "this file is complete" signal; remaining hits
	// zero repeatedly while a file is still being read.
	remaining int
	sealed    bool
	settled   chan struct{}
}

func newSegmentBatch() *segmentBatch {
	return &segmentBatch{settled: make(chan struct{})}
}

// newSegmentWriter starts the worker pool, preallocating the segment buffers.
// close stops it.
func newSegmentWriter(a *assembler, concurrency, segBytes int) *segmentWriter {
	sw := &segmentWriter{
		a:    a,
		jobs: make(chan segmentJob, concurrency),
		free: make(chan []byte, 2*concurrency),
	}

	// Two buffers per worker: one being written, one being filled.
	for range 2 * concurrency {
		sw.free <- make([]byte, segBytes)
	}

	sw.wg.Add(concurrency)
	for range concurrency {
		go sw.work()
	}

	return sw
}

func (sw *segmentWriter) work() {
	defer sw.wg.Done()

	for job := range sw.jobs {
		oid, err := sw.fillAndWrite(job)
		sw.free <- job.buf
		// record last: it can settle the batch, after which the reader may
		// resolve the file, and by then the buffer must be back in the pool.
		job.batch.record(job.index, segRef{oid: oid, length: int64(len(job.data))}, err)
	}
}

// fillAndWrite completes a segment and stores it: for a rebuilt segment that
// means obtaining its unchanged blocks from the prior image first.
func (sw *segmentWriter) fillAndWrite(job segmentJob) (object.ID, error) {
	if job.fill != nil {
		if err := sw.a.fillFromPrior(job.fill, job.data); err != nil {
			return object.EmptyID, err
		}
	}

	return sw.a.writeSegmentBytes(job.data, job.logical)
}

// close stops the workers. Every batch must have been resolved first.
func (sw *segmentWriter) close() {
	close(sw.jobs)
	sw.wg.Wait()
}

// acquire takes a buffer to fill, blocking until a worker returns one.
func (sw *segmentWriter) acquire() []byte {
	return <-sw.free
}

// release returns an unused buffer (the producer read no bytes into it).
func (sw *segmentWriter) release(buf []byte) {
	sw.free <- buf
}

// submit queues the first n bytes of buf as the segment at the given index. It
// must not be called after the batch is sealed.
func (sw *segmentWriter) submit(batch *segmentBatch, logical string, index int, buf []byte, n int) {
	batch.add()
	sw.jobs <- segmentJob{data: buf[:n], buf: buf, logical: logical, index: index, batch: batch}
}

// submitRebuilt queues a segment that still needs prior-image blocks, which the
// worker obtains before writing it.
func (sw *segmentWriter) submitRebuilt(
	batch *segmentBatch, logical string, index int, buf []byte, n int, fill *priorFill,
) {
	batch.add()
	sw.jobs <- segmentJob{
		data: buf[:n], buf: buf, logical: logical, index: index, batch: batch, fill: fill,
	}
}

// add accounts for a segment before it is queued. Counting it afterwards would
// let a worker record that segment first, driving remaining negative and
// settling the batch while the file is still being read.
func (b *segmentBatch) add() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.remaining++
}

// seal records that the file's last segment has been submitted. A batch with no
// segments at all (an empty file) settles right here.
func (b *segmentBatch) seal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.sealed = true
	b.settleLocked()
}

// settleLocked closes settled once the producer is done submitting and every
// submitted segment has been recorded. Exactly one caller can observe that
// transition: while the batch is unsealed no record can see it, and once it is
// sealed no further segment is added, so only the last record (or seal itself,
// when nothing is outstanding) closes the channel.
func (b *segmentBatch) settleLocked() {
	if b.sealed && b.remaining == 0 {
		close(b.settled)
	}
}

// record stores a written segment at its index. The first error wins and is
// reported by wait, which is why a failed write may leave a gap behind.
func (b *segmentBatch) record(index int, ref segRef, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.remaining--
	defer b.settleLocked()

	if err != nil {
		if b.err == nil {
			b.err = err
		}

		return
	}

	b.storeLocked(index, ref)
}

// set records a segment that needs no write because it is taken by reference
// from the prior backup, so its object ID is already known. Mixing these with
// submitted segments is how an incremental file's segment list is assembled.
func (b *segmentBatch) set(index int, ref segRef) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.storeLocked(index, ref)
}

// storeLocked places a segment at its index, growing the slice as needed:
// segments are recorded out of order, both because writes finish out of order
// and because a referenced segment is recorded immediately.
func (b *segmentBatch) storeLocked(index int, ref segRef) {
	for len(b.refs) <= index {
		b.refs = append(b.refs, segRef{})
	}
	b.refs[index] = ref
}

// done reports whether the file's segments have all been written, without
// blocking, so the reader can resolve what is ready between tar members.
func (b *segmentBatch) done() bool {
	select {
	case <-b.settled:
		return true
	default:
		return false
	}
}

// failed reports whether a segment of this batch has already failed, so the
// producer can stop feeding a file whose write is doomed instead of streaming
// the rest of it into the pool.
func (b *segmentBatch) failed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.err != nil
}

// wait blocks until the file's segments have all been written, then returns
// them in order. The batch must be sealed, otherwise there is nothing to wait
// for and this blocks forever.
func (b *segmentBatch) wait() ([]segRef, error) {
	<-b.settled

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.err != nil {
		return nil, b.err
	}

	return b.refs, nil
}
