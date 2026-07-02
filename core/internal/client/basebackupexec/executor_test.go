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

package basebackupexec

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/basebackup"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	kopiaconn "github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
	"github.com/cloudnative-pg/klio/core/internal/kopia"
)

// Compile-time assertion that the Kopia connection satisfies the Uploader
// contract used by the executor.
var _ Uploader = (*kopiaconn.Connection)(nil)

// fakeUploader records the ingest options it is called with, so tests can
// assert the per-source decisions the executor makes.
type fakeUploader struct {
	hostname     string
	ingestByPath map[string]kopia.IngestOptions
}

func newFakeUploader() *fakeUploader {
	return &fakeUploader{hostname: "cluster-a", ingestByPath: map[string]kopia.IngestOptions{}}
}

func (f *fakeUploader) GetHostname() string { return f.hostname }

func (f *fakeUploader) IngestBaseBackup(
	_ context.Context, _ io.Reader, opts kopia.IngestOptions,
) (*kopia.IngestResult, error) {
	f.ingestByPath[opts.SourcePath] = opts

	return &kopia.IngestResult{ManifestID: "manifest-for-" + opts.SourcePath}, nil
}

func (f *fakeUploader) UploadBackupManifest(_ context.Context, _, _ string, _ []byte) error {
	return nil
}

func (f *fakeUploader) UploadBackupMetadata(
	_ context.Context, _ string, _ *klioclient.BackupMetadata,
) error {
	return nil
}

// TestMakeHandlerFallsBackToFullForSourceWithoutPrior is the regression test for
// a tablespace created after the previous backup. On an incremental backup such
// a source has no combine base, so it must be ingested as a full source rather
// than driving the helper with --incremental and no --previous-manifest (which
// would fail the whole backup).
func TestMakeHandlerFallsBackToFullForSourceWithoutPrior(t *testing.T) {
	up := newFakeUploader()
	e := &Executor{uploader: up, clusterName: up.hostname}
	env := &environment{
		pgData:          "/pgdata",
		tablespaceNames: map[string]string{"16385": "ts_old", "9999": "ts_new"},
	}
	opts := Options{
		Incremental: true,
		// The main data directory has a prior; the new tablespace does not.
		PreviousKopiaManifests: map[string][]string{mainDataSourceKey: {"m-pgdata"}},
	}

	handler := e.makeHandler("backup-2", env, opts, map[string]string{})

	// Main data directory: has a prior, so it is ingested incrementally.
	require.NoError(t, handler(context.Background(), basebackup.DataSource{}, strings.NewReader("")))
	main := up.ingestByPath["/pgdata"]
	assert.True(t, main.Incremental, "main data dir must be incremental")
	assert.Equal(t, []string{"m-pgdata"}, main.PreviousManifests)

	// A tablespace with no prior manifest: full ingest, no previous manifests.
	newTS := basebackup.DataSource{IsTablespace: true, TablespaceOID: "9999"}
	require.NoError(t, handler(context.Background(), newTS, strings.NewReader("")))
	ts := up.ingestByPath["/tablespace/9999"]
	assert.False(t, ts.Incremental, "a source without a prior must be ingested as full")
	assert.Empty(t, ts.PreviousManifests)
}

// TestMakeHandlerPerSourceIncrementalDecision exercises a single incremental
// backup that spans several sources at once: the main data directory and two
// tablespaces, only one of which existed in the prior backup. Each source's
// incremental decision (and the manifests handed to the helper) must be resolved
// independently from that source's own combine base.
func TestMakeHandlerPerSourceIncrementalDecision(t *testing.T) {
	up := newFakeUploader()
	e := &Executor{uploader: up, clusterName: up.hostname}
	env := &environment{
		pgData:          "/pgdata",
		tablespaceNames: map[string]string{"16385": "ts_old", "9999": "ts_new"},
	}
	opts := Options{
		Incremental: true,
		PreviousKopiaManifests: map[string][]string{
			mainDataSourceKey:   {"m-pgdata"},
			"/tablespace/16385": {"m-ts-old"},
			// "/tablespace/9999" intentionally absent: created after the prior backup.
		},
	}

	handler := e.makeHandler("backup-2", env, opts, map[string]string{})

	sources := []basebackup.DataSource{
		{}, // main data directory
		{IsTablespace: true, TablespaceOID: "16385"},
		{IsTablespace: true, TablespaceOID: "9999"},
	}
	for _, src := range sources {
		require.NoError(t, handler(context.Background(), src, strings.NewReader("")))
	}

	cases := []struct {
		path            string
		wantIncremental bool
		wantManifests   []string
	}{
		{"/pgdata", true, []string{"m-pgdata"}},
		{"/tablespace/16385", true, []string{"m-ts-old"}},
		{"/tablespace/9999", false, nil},
	}
	for _, tc := range cases {
		got := up.ingestByPath[tc.path]
		assert.Equal(t, tc.wantIncremental, got.Incremental, "Incremental for %s", tc.path)
		assert.Equal(t, tc.wantManifests, got.PreviousManifests, "PreviousManifests for %s", tc.path)
	}
}

func TestValidateIncrementalPreconditions(t *testing.T) {
	t.Run("server older than PostgreSQL 17 is rejected", func(t *testing.T) {
		env := &environment{serverVersionNum: 160004}
		err := validateIncrementalPreconditions(env, Options{PreviousManifest: []byte("m")})
		require.ErrorIs(t, err, ErrIncrementalRequiresPG17)
	})

	t.Run("missing previous manifest is rejected", func(t *testing.T) {
		env := &environment{serverVersionNum: 170004}
		err := validateIncrementalPreconditions(env, Options{})
		require.ErrorIs(t, err, ErrIncrementalRequiresManifest)
	})

	t.Run("PostgreSQL 17 with a manifest is accepted", func(t *testing.T) {
		env := &environment{serverVersionNum: 170000}
		require.NoError(t, validateIncrementalPreconditions(env, Options{PreviousManifest: []byte("m")}))
	})
}

func TestSourceDescriptor(t *testing.T) {
	env := &environment{
		pgData:          "/var/lib/postgresql/data/pgdata",
		tablespaceNames: map[string]string{"16385": "ts_fast"},
	}

	t.Run("main data directory", func(t *testing.T) {
		key, path, tags := sourceDescriptor(basebackup.DataSource{}, "backup-1", env)
		assert.Equal(t, mainDataSourceKey, key)
		assert.Equal(t, "/var/lib/postgresql/data/pgdata", path)
		assert.Equal(t, "pgdata", tags[klioclient.BackupContentTagName])
		assert.Equal(t, "backup-1", tags[klioclient.BackupNameTagName])
	})

	t.Run("main data directory key is stable across data_directory changes", func(t *testing.T) {
		moved := &environment{pgData: "/srv/restored/pgdata"}
		keyA, _, _ := sourceDescriptor(basebackup.DataSource{}, "backup-1", env)
		keyB, pathB, _ := sourceDescriptor(basebackup.DataSource{}, "backup-2", moved)
		// The combine-base key must not move with the physical path, while the
		// snapshot path tracks the physical data_directory.
		assert.Equal(t, keyA, keyB)
		assert.Equal(t, "/srv/restored/pgdata", pathB)
	})

	t.Run("tablespace", func(t *testing.T) {
		src := basebackup.DataSource{IsTablespace: true, TablespaceOID: "16385", Location: "/srv/ts"}
		key, path, tags := sourceDescriptor(src, "backup-1", env)
		assert.Equal(t, "/tablespace/16385", key)
		assert.Equal(t, "/tablespace/16385", path)
		assert.Equal(t, "tablespace", tags[klioclient.BackupContentTagName])
		assert.Equal(t, "ts_fast", tags[klioclient.TablespaceNameTagName])
	})
}

func TestBuildMetadata(t *testing.T) {
	e := &Executor{clusterName: "cluster-a"}
	env := &environment{
		pgData:          "/pgdata",
		segmentSize:     16 * 1024 * 1024,
		systemID:        7123456789012345678,
		tablespaceNames: map[string]string{"16385": "ts_fast"},
	}
	started := time.Unix(1000, 0)

	result := &basebackup.Result{
		StartLSN: 0x2000028,
		StartTLI: 1,
		EndLSN:   0x3000000,
		Tablespaces: []basebackup.Tablespace{
			{OID: "", Location: ""},             // main data dir
			{OID: "16385", Location: "/srv/ts"}, // real tablespace
		},
		Manifest: []byte("m"),
	}

	meta := e.buildMetadata("backup-1", started, env, result, false)

	assert.Equal(t, "backup-1", meta.Name)
	assert.Equal(t, "cluster-a", meta.ClusterName)
	assert.Equal(t, int64(1000), meta.StartedAt)
	assert.Equal(t, uint64(0x2000028), meta.StartLSN)
	assert.Equal(t, uint64(0x3000000), meta.EndLSN)
	assert.Equal(t, 1, meta.Timeline)
	assert.Equal(t, int64(7123456789012345678), meta.SystemID)
	assert.Equal(t, "/pgdata", meta.PgData)

	// Only the real tablespace is recorded; the main data dir entry is skipped.
	require.Len(t, meta.Tablespaces, 1)
	assert.Equal(t, "16385", meta.Tablespaces[0].Oid)
	assert.Equal(t, "ts_fast", meta.Tablespaces[0].Name)
	assert.Equal(t, "/srv/ts", meta.Tablespaces[0].Path)

	// WAL file names are derived from LSN + TLI + segment size.
	assert.NotEmpty(t, meta.StartWAL)
	assert.NotEmpty(t, meta.EndWAL)

	// A full backup records the full method annotation.
	assert.Equal(t, BackupMethodFull, meta.Annotations[BackupMethodAnnotationName])
}

func TestBuildMetadataRecordsIncrementalMethod(t *testing.T) {
	e := &Executor{clusterName: "cluster-a"}
	env := &environment{pgData: "/pgdata", segmentSize: 16 * 1024 * 1024}
	result := &basebackup.Result{StartLSN: 0x2000028, StartTLI: 1, EndLSN: 0x3000000}

	full := e.buildMetadata("backup-full", time.Unix(1000, 0), env, result, false)
	assert.Equal(t, BackupMethodFull, full.Annotations[BackupMethodAnnotationName])

	incremental := e.buildMetadata("backup-incr", time.Unix(1000, 0), env, result, true)
	assert.Equal(t, BackupMethodIncremental, incremental.Annotations[BackupMethodAnnotationName])
}
