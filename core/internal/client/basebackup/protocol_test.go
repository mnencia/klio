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

package basebackup

import (
	"context"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedReceiver replays a fixed list of backend messages.
type scriptedReceiver struct {
	msgs []pgproto3.BackendMessage
	pos  int
}

//nolint:ireturn // implements the messageReceiver interface, which returns pgproto3.BackendMessage
func (s *scriptedReceiver) ReceiveMessage(_ context.Context) (pgproto3.BackendMessage, error) {
	if s.pos >= len(s.msgs) {
		return nil, io.ErrUnexpectedEOF
	}
	msg := s.msgs[s.pos]
	s.pos++

	return msg, nil
}

// recordedArchive collects the data routed to one archive.
type recordedArchive struct {
	name string
	path string
	data []byte
}

// recordingSink captures what the dispatcher routes to it.
type recordingSink struct {
	resultSets []resultSet
	archives   []*recordedArchive
	manifest   []byte

	cur        *recordedArchive
	inManifest bool
}

func (r *recordingSink) handleResultSet(rs resultSet) error {
	r.resultSets = append(r.resultSets, rs)

	return nil
}

func (r *recordingSink) beginArchive(name, path string) error {
	a := &recordedArchive{name: name, path: path}
	r.archives = append(r.archives, a)
	r.cur = a
	r.inManifest = false

	return nil
}

func (r *recordingSink) beginManifest() error {
	r.cur = nil
	r.inManifest = true

	return nil
}

func (r *recordingSink) writeData(data []byte) error {
	if r.inManifest {
		r.manifest = append(r.manifest, data...)
	} else if r.cur != nil {
		r.cur.data = append(r.cur.data, data...)
	}

	return nil
}

func (r *recordingSink) endCopyStream() error { return nil }

func rowDesc(names ...string) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(names))
	for i, n := range names {
		fields[i] = pgproto3.FieldDescription{Name: []byte(n)}
	}

	return &pgproto3.RowDescription{Fields: fields}
}

func dataRow(values ...[]byte) *pgproto3.DataRow {
	return &pgproto3.DataRow{Values: values}
}

// newArchiveChunk builds a 'n' chunk: name and tablespace path, each
// NUL-terminated.
func newArchiveChunk(name, path string) *pgproto3.CopyData {
	b := make([]byte, 0, len(name)+len(path)+3)
	b = append(b, chunkNewArchive)
	b = append(b, name...)
	b = append(b, 0)
	b = append(b, path...)
	b = append(b, 0)

	return &pgproto3.CopyData{Data: b}
}

func dataChunk(data string) *pgproto3.CopyData {
	return &pgproto3.CopyData{Data: append([]byte{chunkData}, data...)}
}

func manifestChunk() *pgproto3.CopyData {
	return &pgproto3.CopyData{Data: []byte{chunkManifest}}
}

func progressChunk() *pgproto3.CopyData {
	return &pgproto3.CopyData{Data: []byte{chunkProgress, 0, 0, 0, 0, 0, 0, 0, 0}}
}

func TestDispatchResultSetsAndChunks(t *testing.T) {
	ctx := context.Background()

	recv := &scriptedReceiver{msgs: []pgproto3.BackendMessage{
		// Start position result set.
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		// Tablespace list (main data dir only).
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")),
		&pgproto3.CommandComplete{},
		// Single COPY stream of typed chunks.
		&pgproto3.CopyOutResponse{},
		newArchiveChunk("base.tar", ""),
		dataChunk("tarpart1"),
		dataChunk("tarpart2"),
		progressChunk(),
		manifestChunk(),
		dataChunk("MANIFEST"),
		&pgproto3.CopyDone{},
		// End position result set.
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/3000000"), []byte("1")),
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	}}

	sink := &recordingSink{}
	require.NoError(t, dispatch(ctx, recv, sink))

	require.Len(t, sink.resultSets, 3)
	assert.Equal(t, []string{"recptr", "tli"}, sink.resultSets[0].columns)
	assert.Equal(t, []string{"spcoid", "spclocation", "size"}, sink.resultSets[1].columns)
	require.Len(t, sink.archives, 1)
	assert.Equal(t, "base.tar", sink.archives[0].name)
	assert.Equal(t, []byte("tarpart1tarpart2"), sink.archives[0].data)
	assert.Equal(t, []byte("MANIFEST"), sink.manifest)
}

func TestParseNewArchive(t *testing.T) {
	t.Run("main data directory", func(t *testing.T) {
		name, path := parseNewArchive([]byte("base.tar\x00\x00"))
		assert.Equal(t, "base.tar", name)
		assert.Empty(t, path)
	})

	t.Run("tablespace with path", func(t *testing.T) {
		name, path := parseNewArchive([]byte("16385.tar\x00/srv/ts1\x00"))
		assert.Equal(t, "16385.tar", name)
		assert.Equal(t, "/srv/ts1", path)
	})
}

func TestDispatchPropagatesErrorResponse(t *testing.T) {
	ctx := context.Background()
	recv := &scriptedReceiver{msgs: []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{Message: "boom", Code: "XX000"},
	}}

	err := dispatch(ctx, recv, &recordingSink{})
	require.Error(t, err)

	var serverErr *ServerError
	require.ErrorAs(t, err, &serverErr)
	assert.Equal(t, "boom", serverErr.Message)
	assert.Equal(t, "XX000", serverErr.Code)
}
