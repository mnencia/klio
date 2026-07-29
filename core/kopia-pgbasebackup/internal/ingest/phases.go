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
	"sync/atomic"
	"time"
)

// phaseCounters accumulates the phase timings an ingest reports. Segment writes
// happen on the writer goroutines, so every counter is atomic and the whole
// struct is safe to update from any of them.
type phaseCounters struct {
	tarRead         atomic.Int64
	priorRead       atomic.Int64
	priorBlockReads atomic.Int64
	segmentWrite    atomic.Int64
	indexWrite      atomic.Int64
	finalize        atomic.Int64
}

// One method per phase rather than a single since(&counter, start): naming the
// phase at the call site is what stops a region being billed to the wrong one,
// and passing a pointer to a sibling field invites exactly that.

func (c *phaseCounters) addTarRead(start time.Time) { c.tarRead.Add(int64(time.Since(start))) }

func (c *phaseCounters) addPriorRead(start time.Time) { c.priorRead.Add(int64(time.Since(start))) }

func (c *phaseCounters) addPriorBlockRead() { c.priorBlockReads.Add(1) }

func (c *phaseCounters) addSegmentWrite(start time.Time) {
	c.segmentWrite.Add(int64(time.Since(start)))
}

func (c *phaseCounters) addIndexWrite(start time.Time) {
	c.indexWrite.Add(int64(time.Since(start)))
}

func (c *phaseCounters) addFinalize(start time.Time) { c.finalize.Add(int64(time.Since(start))) }

// snapshot renders the counters as the reported Phases, converting to
// milliseconds: nanoseconds are noise at the scale these phases run at, and
// milliseconds keep the JSON readable.
func (c *phaseCounters) snapshot() Phases {
	ms := func(v *atomic.Int64) int64 { return v.Load() / int64(time.Millisecond) }

	return Phases{
		TarReadMS:       ms(&c.tarRead),
		PriorReadMS:     ms(&c.priorRead),
		PriorBlockReads: c.priorBlockReads.Load(),
		SegmentWriteMS:  ms(&c.segmentWrite),
		IndexWriteMS:    ms(&c.indexWrite),
		FinalizeMS:      ms(&c.finalize),
	}
}
