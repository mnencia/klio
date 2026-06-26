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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/basebackup"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	kopiaconn "github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
)

// Compile-time assertion that the Kopia connection satisfies the Uploader
// contract used by the executor.
var _ Uploader = (*kopiaconn.Connection)(nil)

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

	meta := e.buildMetadata("backup-1", started, env, result)

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
}
