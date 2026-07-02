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
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	// ErrMissingStartPosition is returned when the BASE_BACKUP stream did not
	// include the starting WAL position result set.
	ErrMissingStartPosition = errors.New("BASE_BACKUP did not report a start position")

	// ErrMissingEndPosition is returned when the BASE_BACKUP stream did not
	// include the ending WAL position result set.
	ErrMissingEndPosition = errors.New("BASE_BACKUP did not report an end position")

	// ErrManifestNotReceived is returned when MANIFEST was requested but the
	// backend did not send a manifest stream.
	ErrManifestNotReceived = errors.New("BASE_BACKUP manifest was requested but not received")

	// ErrMainArchiveNotReceived is returned when the stream completed without the
	// main data directory archive (base.tar). It guards against recording a
	// truncated stream that carried no data as a successful backup.
	ErrMainArchiveNotReceived = errors.New("BASE_BACKUP did not send the main data directory archive")

	// ErrDuplicateTablespaceSet is returned when more than one tablespace-shaped
	// result set arrives. Result sets are classified by shape, so a second
	// 3-column set would otherwise silently overwrite the real tablespace list.
	ErrDuplicateTablespaceSet = errors.New("BASE_BACKUP sent more than one tablespace result set")

	// ErrUnexpectedArchive is returned when an archive name is malformed or names
	// a tablespace OID absent from the tablespace list.
	ErrUnexpectedArchive = errors.New("BASE_BACKUP sent an unexpected archive")

	// ErrMalformedPositionRow is returned when a BASE_BACKUP position result set
	// does not carry exactly one row of a valid LSN and timeline.
	ErrMalformedPositionRow = errors.New("BASE_BACKUP position result set is malformed")

	// errAborted unblocks the handler goroutine when a dispatch error tears down
	// an in-flight archive.
	errAborted = errors.New("BASE_BACKUP aborted")
)

// CheckpointMode selects how the server triggers the checkpoint that starts the
// backup.
type CheckpointMode string

const (
	// CheckpointFast requests an immediate checkpoint, matching the current
	// pg_backup_start(fast := true) behavior.
	CheckpointFast CheckpointMode = "fast"

	// CheckpointSpread spreads the checkpoint over time to reduce I/O impact.
	CheckpointSpread CheckpointMode = "spread"
)

// Options configures a BASE_BACKUP request.
type Options struct {
	// Label is the human-readable backup label (BASE_BACKUP LABEL).
	Label string

	// Checkpoint selects the checkpoint mode. Defaults to CheckpointFast.
	Checkpoint CheckpointMode

	// Manifest requests a backup_manifest (MANIFEST 'yes'), needed to drive a
	// future incremental backup.
	Manifest bool

	// Incremental requests an incremental backup (BASE_BACKUP INCREMENTAL).
	// Requires a prior UploadManifest call on the same connection. PostgreSQL
	// 17+. The orchestration layer drives this via basebackupexec.Options.
	Incremental bool
}

// DataSource identifies a single tar stream produced by BASE_BACKUP.
type DataSource struct {
	// IsTablespace is true for a non-default tablespace; false for the main
	// data directory.
	IsTablespace bool

	// TablespaceOID is the tablespace OID (empty for the main data directory).
	TablespaceOID string

	// Location is the server-side filesystem location of a tablespace (empty
	// for the main data directory).
	Location string
}

// DataSourceHandler consumes one data tar stream (the main data directory or a
// tablespace). The reader is valid only for the duration of the call.
type DataSourceHandler func(ctx context.Context, src DataSource, tar io.Reader) error

// Tablespace is one entry of the BASE_BACKUP tablespace list. The main data
// directory appears as an entry with an empty OID and Location.
type Tablespace struct {
	// OID is the tablespace OID (empty for the main data directory).
	OID string
	// Location is the server-side filesystem path of the tablespace (empty for
	// the main data directory).
	Location string
	// Size is the tablespace size in bytes as reported by the server.
	Size int64
}

// Result holds the metadata parsed from a completed BASE_BACKUP.
type Result struct {
	// StartLSN is the WAL position at which the backup starts.
	StartLSN uint64
	// StartTLI is the timeline the backup runs on. A base backup stays on one
	// timeline, so it is also the end timeline.
	StartTLI int32
	// EndLSN is the WAL position at which the backup ends.
	EndLSN uint64
	// Tablespaces is the tablespace list, including the main data directory entry
	// (which has an empty OID).
	Tablespaces []Tablespace
	// Manifest is the captured backup_manifest, populated when Options.Manifest
	// is set.
	Manifest []byte
}

// buildQuery renders the BASE_BACKUP replication command for the given options.
func buildQuery(opts Options) string {
	checkpoint := opts.Checkpoint
	if checkpoint == "" {
		checkpoint = CheckpointFast
	}

	parts := []string{
		"LABEL " + quoteLiteral(opts.Label),
		"CHECKPOINT " + quoteLiteral(string(checkpoint)),
		// WAL is streamed separately by the WAL receiver, so disable it here.
		"WAL false",
		"TABLESPACE_MAP",
	}

	if opts.Manifest {
		parts = append(parts, "MANIFEST 'yes'")
	} else {
		parts = append(parts, "MANIFEST 'no'")
	}

	if opts.Incremental {
		parts = append(parts, "INCREMENTAL")
	}

	return fmt.Sprintf("BASE_BACKUP (%s)", strings.Join(parts, ", "))
}

// quoteLiteral renders a string as a single-quoted SQL literal, doubling any
// embedded single quotes.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Run issues a BASE_BACKUP on conn, dispatching each data tar stream to handler
// and capturing the manifest (when requested). conn must be a replication
// connection.
func Run(
	ctx context.Context,
	conn *pgconn.PgConn,
	opts Options,
	handler DataSourceHandler,
) (*Result, error) {
	sql := buildQuery(opts)

	conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	if err := conn.Frontend().Flush(); err != nil {
		return nil, fmt.Errorf("while sending BASE_BACKUP: %w", err)
	}

	state := &runState{ctx: ctx, opts: opts, handler: handler}
	if err := dispatch(ctx, conn, state); err != nil {
		state.abort()

		return nil, err
	}

	return state.finish()
}

// UploadManifest sends a prior backup_manifest to the server via the
// UPLOAD_MANIFEST replication command. It must be called before Run with
// Options.Incremental set. PostgreSQL 17+.
func UploadManifest(ctx context.Context, conn *pgconn.PgConn, manifest []byte) error {
	conn.Frontend().SendQuery(&pgproto3.Query{String: "UPLOAD_MANIFEST"})
	if err := conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("while sending UPLOAD_MANIFEST: %w", err)
	}

	// The server replies with CopyInResponse, after which we stream the
	// manifest as COPY data.
	if err := awaitCopyInResponse(ctx, conn); err != nil {
		return err
	}

	conn.Frontend().Send(&pgproto3.CopyData{Data: manifest})
	conn.Frontend().Send(&pgproto3.CopyDone{})
	if err := conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("while sending manifest data: %w", err)
	}

	return awaitReadyForQuery(ctx, conn)
}

func awaitCopyInResponse(ctx context.Context, recv messageReceiver) error {
	for {
		msg, err := recv.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("while waiting for CopyInResponse: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyInResponse:
			return nil
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
			continue
		case *pgproto3.ErrorResponse:
			return errorResponseToError(m)
		default:
			return fmt.Errorf("%w: %T (awaiting CopyInResponse)", errUnexpectedMessage, msg)
		}
	}
}

func awaitReadyForQuery(ctx context.Context, recv messageReceiver) error {
	for {
		msg, err := recv.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("while waiting for ReadyForQuery: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		case *pgproto3.CommandComplete, *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
			continue
		case *pgproto3.ErrorResponse:
			return errorResponseToError(m)
		default:
			return fmt.Errorf("%w: %T (awaiting ReadyForQuery)", errUnexpectedMessage, msg)
		}
	}
}

// copyMode tracks what the current COPY data chunks belong to.
type copyMode int

const (
	modeNone copyMode = iota
	modeArchive
	modeManifest
)

// runState accumulates the parsed result sets and drives the per-archive
// handler while a BASE_BACKUP is dispatched.
type runState struct {
	ctx     context.Context //nolint:containedctx // bound to a single Run call
	opts    Options
	handler DataSourceHandler

	startSeen       bool
	endSeen         bool
	mainArchiveSeen bool
	tablespacesSeen bool
	result          Result
	manifestBuf     bytes.Buffer
	manifestSeen    bool

	// Streaming state for the current archive. The handler runs in a goroutine
	// reading from pipe; data chunks are written to pipe as they arrive.
	mode       copyMode
	pipe       *io.PipeWriter
	handlerErr chan error
}

const (
	startEndColumnCount    = 2
	tablespaceColumnCount  = 3
	startPositionColumnTag = "recptr"
)

func (s *runState) handleResultSet(rs resultSet) error {
	switch {
	case isStartEndResultSet(rs):
		return s.handleStartEndResultSet(rs)
	case isTablespaceResultSet(rs):
		return s.handleTablespaceResultSet(rs)
	default:
		// Unknown result sets (e.g. PROGRESS) are tolerated and ignored.
		return nil
	}
}

func isStartEndResultSet(rs resultSet) bool {
	if len(rs.columns) != startEndColumnCount {
		return false
	}

	return strings.EqualFold(rs.columns[0], startPositionColumnTag)
}

func isTablespaceResultSet(rs resultSet) bool {
	return len(rs.columns) == tablespaceColumnCount
}

func (s *runState) handleStartEndResultSet(rs resultSet) error {
	if len(rs.rows) != 1 {
		return fmt.Errorf("%w: expected 1 row, got %d", ErrMalformedPositionRow, len(rs.rows))
	}

	lsn, tli, err := parsePositionRow(rs.rows[0])
	if err != nil {
		return err
	}

	if !s.startSeen {
		s.result.StartLSN = lsn
		s.result.StartTLI = tli
		s.startSeen = true
	} else {
		// A base backup runs on a single timeline, so the end timeline always
		// equals StartTLI; only the end LSN is recorded.
		s.result.EndLSN = lsn
		s.endSeen = true
	}

	return nil
}

func parsePositionRow(row [][]byte) (uint64, int32, error) {
	if len(row) < startEndColumnCount || row[0] == nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrMalformedPositionRow, row)
	}

	lsn, err := pglogrepl.ParseLSN(string(row[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("%w: invalid LSN %q: %w", ErrMalformedPositionRow, string(row[0]), err)
	}

	var tli int32
	if row[1] != nil {
		parsed, parseErr := strconv.ParseInt(string(row[1]), 10, 32)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("%w: invalid timeline %q: %w", ErrMalformedPositionRow, string(row[1]), parseErr)
		}
		tli = int32(parsed)
	}

	return uint64(lsn), tli, nil
}

func (s *runState) handleTablespaceResultSet(rs resultSet) error {
	if s.tablespacesSeen {
		return ErrDuplicateTablespaceSet
	}
	s.tablespacesSeen = true

	s.result.Tablespaces = make([]Tablespace, 0, len(rs.rows))
	for _, row := range rs.rows {
		tbl := Tablespace{}
		if len(row) > 0 && row[0] != nil {
			tbl.OID = string(row[0])
		}
		if len(row) > 1 && row[1] != nil {
			tbl.Location = string(row[1])
		}
		if len(row) > 2 && row[2] != nil {
			if size, err := strconv.ParseInt(string(row[2]), 10, 64); err == nil {
				tbl.Size = size
			}
		}
		s.result.Tablespaces = append(s.result.Tablespaces, tbl)
	}

	return nil
}

// beginArchive finishes the current archive (if any) and starts streaming a new
// one to the handler. name is the archive file name (e.g. "base.tar" or
// "<oid>.tar"); tablespacePath is the server-side tablespace location.
func (s *runState) beginArchive(name, tablespacePath string) error {
	if err := s.finishArchive(); err != nil {
		return err
	}

	if !strings.HasSuffix(name, archiveNameSuffix) {
		return fmt.Errorf("%w: name %q has no %q suffix", ErrUnexpectedArchive, name, archiveNameSuffix)
	}

	src := dataSourceFromArchive(name, tablespacePath)
	if src.IsTablespace && !s.knownTablespaceOID(src.TablespaceOID) {
		return fmt.Errorf("%w: tablespace OID %q is not in the tablespace list",
			ErrUnexpectedArchive, src.TablespaceOID)
	}
	if !src.IsTablespace {
		s.mainArchiveSeen = true
	}

	pr, pw := io.Pipe()
	s.pipe = pw
	s.handlerErr = make(chan error, 1)
	s.mode = modeArchive

	go func() {
		err := s.handler(s.ctx, src, pr)
		// Unblock any pending write and surface the handler's outcome.
		_ = pr.CloseWithError(err)
		s.handlerErr <- err
	}()

	return nil
}

// beginManifest finishes the current archive and switches to capturing the
// backup_manifest.
func (s *runState) beginManifest() error {
	if err := s.finishArchive(); err != nil {
		return err
	}
	s.mode = modeManifest
	s.manifestSeen = true

	return nil
}

// writeData routes a data chunk to the current archive or to the manifest.
func (s *runState) writeData(data []byte) error {
	switch s.mode {
	case modeArchive:
		if _, err := s.pipe.Write(data); err != nil {
			// The handler closed the pipe (it failed); surface its error.
			return s.finishArchive()
		}

		return nil
	case modeManifest:
		s.manifestBuf.Write(data)

		return nil
	case modeNone:
		// Data before any archive header: the server does not do this.
		return fmt.Errorf("%w: data chunk before any archive", errUnexpectedMessage)
	default:
		return fmt.Errorf("%w: data chunk in unknown mode", errUnexpectedMessage)
	}
}

// endCopyStream finishes the last archive at CopyDone.
func (s *runState) endCopyStream() error {
	return s.finishArchive()
}

// finishArchive closes the current archive pipe and waits for the handler.
func (s *runState) finishArchive() error {
	if s.mode != modeArchive || s.pipe == nil {
		return nil
	}

	_ = s.pipe.Close()
	err := <-s.handlerErr
	s.pipe = nil
	s.handlerErr = nil
	s.mode = modeNone

	return err
}

// abort tears down any in-flight handler goroutine after a dispatch error, so
// it does not leak.
func (s *runState) abort() {
	if s.mode != modeArchive || s.pipe == nil {
		return
	}

	_ = s.pipe.CloseWithError(errAborted)
	<-s.handlerErr
	s.pipe = nil
	s.handlerErr = nil
	s.mode = modeNone
}

// knownTablespaceOID reports whether oid appears in the tablespace list parsed
// from the BASE_BACKUP result sets.
func (s *runState) knownTablespaceOID(oid string) bool {
	for _, tbl := range s.result.Tablespaces {
		if tbl.OID != "" && tbl.OID == oid {
			return true
		}
	}

	return false
}

// dataSourceFromArchive maps a BASE_BACKUP archive name to a DataSource. The
// main data directory is sent as "base.tar"; tablespaces as "<oid>.tar".
func dataSourceFromArchive(name, tablespacePath string) DataSource {
	base := strings.TrimSuffix(name, archiveNameSuffix)
	if base == "base" {
		return DataSource{}
	}

	return DataSource{
		IsTablespace:  true,
		TablespaceOID: base,
		Location:      tablespacePath,
	}
}

// finish validates and returns the assembled Result.
func (s *runState) finish() (*Result, error) {
	if !s.startSeen {
		return nil, ErrMissingStartPosition
	}
	if !s.endSeen {
		return nil, ErrMissingEndPosition
	}
	if !s.mainArchiveSeen {
		return nil, ErrMainArchiveNotReceived
	}
	if s.opts.Manifest && !s.manifestSeen {
		return nil, ErrManifestNotReceived
	}

	if s.manifestSeen {
		s.result.Manifest = s.manifestBuf.Bytes()
	}

	return &s.result, nil
}
