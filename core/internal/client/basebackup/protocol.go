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
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// errUnexpectedMessage is returned when the backend sends a message that is not
// valid for the current point in the BASE_BACKUP protocol.
var errUnexpectedMessage = errors.New("unexpected message in BASE_BACKUP stream")

// Chunk type bytes used by the PG15+ BASE_BACKUP COPY stream. The whole backup
// (every archive plus the manifest) travels in a single COPY-out stream, and
// each CopyData message starts with one of these type bytes. See pg_basebackup
// and the replication protocol documentation.
const (
	chunkNewArchive   = 'n' // a new archive (tablespace) begins
	chunkData         = 'd' // archive or manifest payload
	chunkManifest     = 'm' // the manifest begins; following data chunks are manifest bytes
	chunkProgress     = 'p' // progress report (int64 bytes processed); ignored
	archiveNameSuffix = ".tar"
)

// messageReceiver abstracts the single backend method the dispatcher needs.
// *pgconn.PgConn satisfies it; tests use a synthetic implementation.
type messageReceiver interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
}

// resultSet is a parsed simple-query result set. A nil entry in a row means a
// SQL NULL value.
type resultSet struct {
	columns []string
	rows    [][][]byte
}

// streamSink consumes the structured output of a BASE_BACKUP command: the
// ordinary result sets and the typed chunks of the single COPY-out stream.
type streamSink interface {
	// handleResultSet is called once per ordinary result set.
	handleResultSet(rs resultSet) error

	// beginArchive starts a new archive (one per tablespace, plus the main data
	// directory). tablespacePath is the server-side location ("" for the main
	// data directory).
	beginArchive(name, tablespacePath string) error

	// beginManifest signals that following data belongs to the backup_manifest.
	beginManifest() error

	// writeData appends payload to the current archive or to the manifest.
	writeData(data []byte) error

	// endCopyStream is called at CopyDone, after the last chunk.
	endCopyStream() error
}

// dispatch reads the backend message stream produced by a BASE_BACKUP command,
// up to and including ReadyForQuery, routing result sets and the COPY-out chunk
// stream to the sink.
//
//nolint:cyclop // a flat switch over the protocol message types reads clearer than a split
func dispatch(ctx context.Context, recv messageReceiver, sink streamSink) error {
	for {
		msg, err := recv.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("while receiving BASE_BACKUP message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.RowDescription:
			rs, rsErr := readResultSet(ctx, recv, m)
			if rsErr != nil {
				return rsErr
			}
			if sinkErr := sink.handleResultSet(rs); sinkErr != nil {
				return sinkErr
			}

		case *pgproto3.CopyOutResponse:
			if copyErr := readCopyStream(ctx, recv, sink); copyErr != nil {
				return copyErr
			}

		case *pgproto3.CommandComplete:
			// Emitted after the COPY stream and each result set; nothing to do.

		case *pgproto3.ReadyForQuery:
			return nil

		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
			// Asynchronous/diagnostic messages: ignore.

		case *pgproto3.ErrorResponse:
			return errorResponseToError(m)

		default:
			return fmt.Errorf("%w: %T", errUnexpectedMessage, msg)
		}
	}
}

// readCopyStream consumes the single COPY-out stream of typed chunks until
// CopyDone, routing each chunk to the sink.
func readCopyStream(ctx context.Context, recv messageReceiver, sink streamSink) error {
	for {
		msg, err := recv.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("while receiving COPY chunk: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if dispatchErr := dispatchChunk(m.Data, sink); dispatchErr != nil {
				return dispatchErr
			}

		case *pgproto3.CopyDone:
			return sink.endCopyStream()

		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
			// ignore

		case *pgproto3.ErrorResponse:
			return errorResponseToError(m)

		default:
			return fmt.Errorf("%w: %T (in COPY stream)", errUnexpectedMessage, msg)
		}
	}
}

// dispatchChunk routes one CopyData chunk by its type byte.
func dispatchChunk(data []byte, sink streamSink) error {
	if len(data) == 0 {
		return nil
	}

	payload := data[1:]
	switch data[0] {
	case chunkNewArchive:
		name, tablespacePath := parseNewArchive(payload)

		return sink.beginArchive(name, tablespacePath)
	case chunkData:
		return sink.writeData(payload)
	case chunkManifest:
		return sink.beginManifest()
	case chunkProgress:
		// Progress report: not needed.
		return nil
	default:
		return fmt.Errorf("%w: unknown COPY chunk type %q", errUnexpectedMessage, rune(data[0]))
	}
}

// parseNewArchive parses a 'n' chunk payload: the archive name and the
// tablespace path, each a NUL-terminated string.
func parseNewArchive(payload []byte) (string, string) {
	parts := bytes.SplitN(payload, []byte{0}, 3)
	name := string(parts[0])
	var tablespacePath string
	if len(parts) > 1 {
		tablespacePath = string(parts[1])
	}

	return name, tablespacePath
}

// readResultSet consumes the DataRow messages following a RowDescription, up to
// and including the trailing CommandComplete.
//
//nolint:cyclop // a flat switch over the protocol message types reads clearer than a split
func readResultSet(
	ctx context.Context,
	recv messageReceiver,
	rd *pgproto3.RowDescription,
) (resultSet, error) {
	rs := resultSet{columns: make([]string, len(rd.Fields))}
	for i, f := range rd.Fields {
		rs.columns[i] = string(f.Name)
	}

	for {
		msg, err := recv.ReceiveMessage(ctx)
		if err != nil {
			return rs, fmt.Errorf("while receiving result-set message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.DataRow:
			row := make([][]byte, len(m.Values))
			for i, v := range m.Values {
				if v != nil {
					// Copy: the backend reuses its read buffer across messages.
					row[i] = append([]byte(nil), v...)
				}
			}
			rs.rows = append(rs.rows, row)

		case *pgproto3.CommandComplete:
			return rs, nil

		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
			// ignore

		case *pgproto3.ErrorResponse:
			return rs, errorResponseToError(m)

		default:
			return rs, fmt.Errorf("%w: %T (in result set)", errUnexpectedMessage, msg)
		}
	}
}

// ServerError is a PostgreSQL ErrorResponse surfaced as a Go error. It keeps the
// message and SQLSTATE code so callers can inspect them with errors.As.
type ServerError struct {
	Message string
	Code    string
}

// Error implements the error interface.
func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s (SQLSTATE %s)", e.Message, e.Code)
}

// errorResponseToError converts a backend ErrorResponse into a *ServerError.
func errorResponseToError(e *pgproto3.ErrorResponse) error {
	return &ServerError{Message: e.Message, Code: e.Code}
}
