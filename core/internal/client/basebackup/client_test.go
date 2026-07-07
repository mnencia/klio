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

func TestBuildQuery(t *testing.T) {
	t.Run("full backup defaults to fast checkpoint and disabled WAL", func(t *testing.T) {
		sql := buildQuery(Options{Label: "backup-1", Manifest: true})
		assert.Equal(t,
			"BASE_BACKUP (LABEL 'backup-1', CHECKPOINT 'fast', WAL false, TABLESPACE_MAP, MANIFEST 'yes')",
			sql)
	})

	t.Run("incremental backup adds INCREMENTAL", func(t *testing.T) {
		sql := buildQuery(Options{Label: "b", Manifest: true, Incremental: true})
		assert.Contains(t, sql, "INCREMENTAL")
		assert.Contains(t, sql, "MANIFEST 'yes'")
	})

	t.Run("spread checkpoint is honored", func(t *testing.T) {
		sql := buildQuery(Options{Label: "b", Checkpoint: CheckpointSpread})
		assert.Contains(t, sql, "CHECKPOINT 'spread'")
		assert.Contains(t, sql, "MANIFEST 'no'")
	})

	t.Run("label single quotes are escaped", func(t *testing.T) {
		sql := buildQuery(Options{Label: "o'brien"})
		assert.Contains(t, sql, "LABEL 'o''brien'")
	})
}

// runFullBackup wires a scripted message stream through Run-equivalent logic by
// driving the runState through dispatch directly (Run itself needs a live
// *pgconn.PgConn to send the query).
func runFullBackup(
	t *testing.T,
	opts Options,
	msgs []pgproto3.BackendMessage,
	handler DataSourceHandler,
) (*Result, error) {
	t.Helper()
	state := &runState{ctx: context.Background(), opts: opts, handler: handler}
	if err := dispatch(context.Background(), &scriptedReceiver{msgs: msgs}, state); err != nil {
		state.abort()

		return nil, err
	}

	return state.finish()
}

func TestRunStateFullBackupWithTablespaceAndManifest(t *testing.T) {
	captured := map[string][]byte{}
	handler := func(_ context.Context, src DataSource, tar io.Reader) error {
		data, err := io.ReadAll(tar)
		require.NoError(t, err)
		key := "pgdata"
		if src.IsTablespace {
			key = "tbl:" + src.TablespaceOID
		}
		captured[key] = data

		return nil
	}

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("3")),
		&pgproto3.CommandComplete{},
		// Tablespace list: main data dir (NULL/NULL) + one real tablespace.
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("2048")),
		dataRow([]byte("16385"), []byte("/srv/ts1"), []byte("4096")),
		&pgproto3.CommandComplete{},
		// Single COPY stream: main archive, tablespace archive, then manifest.
		&pgproto3.CopyOutResponse{},
		newArchiveChunk("base.tar", ""),
		dataChunk("PGDATA-TAR"),
		newArchiveChunk("16385.tar", "/srv/ts1"),
		dataChunk("TS1-TAR"),
		manifestChunk(),
		dataChunk("MANIFEST-BYTES"),
		&pgproto3.CopyDone{},
		// End position.
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/4000000"), []byte("3")),
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	}

	result, err := runFullBackup(t, Options{Label: "b", Manifest: true}, msgs, handler)
	require.NoError(t, err)

	assert.Equal(t, int32(3), result.StartTLI)
	assert.NotZero(t, result.StartLSN)
	assert.NotZero(t, result.EndLSN)
	assert.Greater(t, result.EndLSN, result.StartLSN)
	require.Len(t, result.Tablespaces, 2)
	assert.Equal(t, "16385", result.Tablespaces[1].OID)
	assert.Equal(t, []byte("MANIFEST-BYTES"), result.Manifest)

	assert.Equal(t, []byte("PGDATA-TAR"), captured["pgdata"])
	assert.Equal(t, []byte("TS1-TAR"), captured["tbl:16385"])
}

func TestRunStateMissingManifestIsError(t *testing.T) {
	handler := func(_ context.Context, _ DataSource, tar io.Reader) error {
		_, err := io.Copy(io.Discard, tar)

		return err
	}

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")),
		&pgproto3.CommandComplete{},
		&pgproto3.CopyOutResponse{},
		newArchiveChunk("base.tar", ""),
		dataChunk("PGDATA"),
		&pgproto3.CopyDone{},
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/3000000"), []byte("1")),
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	}

	// Manifest requested but never sent.
	_, err := runFullBackup(t, Options{Label: "b", Manifest: true}, msgs, handler)
	require.ErrorIs(t, err, ErrManifestNotReceived)
}

func TestRunStateMissingStartPositionIsError(t *testing.T) {
	state := &runState{ctx: context.Background(), opts: Options{}}
	_, err := state.finish()
	require.ErrorIs(t, err, ErrMissingStartPosition)
}

func TestRunStateConnectionDropMidStreamIsError(t *testing.T) {
	// The stream is cut after the first data chunk: no CopyDone, no end
	// position, no ReadyForQuery. This must fail loudly, never report success.
	handler := func(_ context.Context, _ DataSource, tar io.Reader) error {
		_, err := io.Copy(io.Discard, tar)

		return err
	}

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")),
		&pgproto3.CommandComplete{},
		&pgproto3.CopyOutResponse{},
		newArchiveChunk("base.tar", ""),
		dataChunk("PGDATA"),
		// Connection drops here: scriptedReceiver returns io.ErrUnexpectedEOF.
	}

	_, err := runFullBackup(t, Options{Label: "b"}, msgs, handler)
	require.Error(t, err)
}

func TestRunStateNoCopyStreamIsError(t *testing.T) {
	// ReadyForQuery arrives but the server never sent a COPY stream, so no
	// archive was ingested. Even without a manifest requested this must fail.
	handler := func(_ context.Context, _ DataSource, _ io.Reader) error { return nil }

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")),
		&pgproto3.CommandComplete{},
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/3000000"), []byte("1")),
		&pgproto3.CommandComplete{},
		&pgproto3.ReadyForQuery{},
	}

	_, err := runFullBackup(t, Options{Label: "b"}, msgs, handler)
	require.ErrorIs(t, err, ErrMainArchiveNotReceived)
}

func TestRunStateUnknownTablespaceArchiveIsError(t *testing.T) {
	handler := func(_ context.Context, _ DataSource, tar io.Reader) error {
		_, err := io.Copy(io.Discard, tar)

		return err
	}

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")), // only the main data dir, no tablespaces
		&pgproto3.CommandComplete{},
		&pgproto3.CopyOutResponse{},
		newArchiveChunk("base.tar", ""),
		dataChunk("PGDATA"),
		// A tablespace archive whose OID was never advertised.
		newArchiveChunk("99999.tar", "/srv/ghost"),
		dataChunk("GHOST"),
		&pgproto3.CopyDone{},
	}

	_, err := runFullBackup(t, Options{Label: "b"}, msgs, handler)
	require.ErrorIs(t, err, ErrUnexpectedArchive)
}

func TestRunStateDuplicateTablespaceSetIsError(t *testing.T) {
	handler := func(_ context.Context, _ DataSource, _ io.Reader) error { return nil }

	msgs := []pgproto3.BackendMessage{
		rowDesc("recptr", "tli"),
		dataRow([]byte("0/2000028"), []byte("1")),
		&pgproto3.CommandComplete{},
		rowDesc("spcoid", "spclocation", "size"),
		dataRow(nil, nil, []byte("1024")),
		&pgproto3.CommandComplete{},
		// A second 3-column result set must be rejected, not silently override.
		rowDesc("spcoid", "spclocation", "size"),
		dataRow([]byte("16385"), []byte("/srv/ts1"), []byte("4096")),
		&pgproto3.CommandComplete{},
	}

	_, err := runFullBackup(t, Options{Label: "b"}, msgs, handler)
	require.ErrorIs(t, err, ErrDuplicateTablespaceSet)
}

func TestDataSourceFromArchive(t *testing.T) {
	main := dataSourceFromArchive("base.tar", "")
	assert.False(t, main.IsTablespace)
	assert.Empty(t, main.TablespaceOID)

	ts := dataSourceFromArchive("16385.tar", "/srv/ts1")
	assert.True(t, ts.IsTablespace)
	assert.Equal(t, "16385", ts.TablespaceOID)
	assert.Equal(t, "/srv/ts1", ts.Location)
}

func TestParsePositionRow(t *testing.T) {
	lsn, tli, err := parsePositionRow([][]byte{[]byte("0/2000028"), []byte("7")})
	require.NoError(t, err)
	assert.Equal(t, int32(7), tli)
	assert.NotZero(t, lsn)

	_, _, err = parsePositionRow([][]byte{nil, []byte("1")})
	require.ErrorIs(t, err, ErrMalformedPositionRow)

	_, _, err = parsePositionRow([][]byte{[]byte("not-an-lsn"), []byte("1")})
	require.ErrorIs(t, err, ErrMalformedPositionRow)
}

// TestResultSetClassification is the regression test for classifying
// BASE_BACKUP result sets by their first column's name rather than by column
// count: an unrelated result set that merely happens to have the same shape
// (2 or 3 columns) as a position or tablespace result set must not be
// misclassified.
func TestResultSetClassification(t *testing.T) {
	t.Run("recptr/tli is a start/end result set", func(t *testing.T) {
		rs := resultSet{columns: []string{"recptr", "tli"}}
		assert.True(t, isStartEndResultSet(rs))
		assert.False(t, isTablespaceResultSet(rs))
	})

	t.Run("spcoid/spclocation/size is a tablespace result set", func(t *testing.T) {
		rs := resultSet{columns: []string{"spcoid", "spclocation", "size"}}
		assert.True(t, isTablespaceResultSet(rs))
		assert.False(t, isStartEndResultSet(rs))
	})

	t.Run("an unrelated 3-column result set is not a tablespace result set", func(t *testing.T) {
		rs := resultSet{columns: []string{"foo", "bar", "baz"}}
		assert.False(t, isTablespaceResultSet(rs))
	})

	t.Run("an unrelated 2-column result set is not a start/end result set", func(t *testing.T) {
		rs := resultSet{columns: []string{"foo", "bar"}}
		assert.False(t, isStartEndResultSet(rs))
	})

	t.Run("an empty result set matches neither", func(t *testing.T) {
		rs := resultSet{}
		assert.False(t, isStartEndResultSet(rs))
		assert.False(t, isTablespaceResultSet(rs))
	})
}
