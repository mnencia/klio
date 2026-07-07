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

package cmd

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/kopia-pgbasebackup/internal/ingest"
)

const testPassword = "test-password-1234567890"

// blockSize is a small stand-in for PostgreSQL's real BLCKSZ, sized to match
// the exact length of the test relation data below.
const blockSize = 16

// newRepo creates and connects a filesystem-backed Kopia repository, returning
// the path to its config file.
func newRepo(ctx context.Context, t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	storageDir := filepath.Join(dir, "storage")
	require.NoError(t, os.MkdirAll(storageDir, 0o700))

	st, err := filesystem.New(ctx, &filesystem.Options{Path: storageDir}, true)
	require.NoError(t, err)
	require.NoError(t, repo.Initialize(ctx, st, nil, testPassword))

	configFile := filepath.Join(dir, "repo.config")
	require.NoError(t, repo.Connect(ctx, configFile, st, testPassword, nil))

	return configFile
}

// ingestFull opens the repository and ingests a single-file tar as a full
// (non-incremental) source, mirroring one BASE_BACKUP tar stream.
func ingestFull(
	ctx context.Context, t *testing.T, configFile string, source snapshot.SourceInfo, tags map[string]string,
) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "base", Typeflag: tar.TypeDir, Mode: 0o700}))
	data := []byte("0123456789ABCDEF") // exactly one blockSize-sized block
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "base/1259", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(data)),
	}))
	_, err = tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	err = repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			_, runErr := ingest.Run(ctx, w, &buf, ingest.Options{Source: source, Tags: tags, BlockSize: blockSize})

			return runErr
		})
	require.NoError(t, err)
}

// TestBuildPriorImageResolvesByTagNotPath is the regression test for the
// actual bug this mechanism exists to fix: a prior's combine base must be
// found by tag (content, backup name), never by the current backup's
// physical source path, since that path is not stable across backups (see
// executor.go's mainDataSourceKey doc comment in the core module). It also
// confirms no manifest ID or object ID ever needs to be supplied by the
// caller — only the prior backup's name.
func TestBuildPriorImageResolvesByTagNotPath(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	fullSource := snapshot.SourceInfo{Host: "cluster-a", UserName: "streaming", Path: "/pgdata"}
	fullTags := map[string]string{contentTag: "pgdata", backupNameTag: "backup-1"}
	ingestFull(ctx, t, configFile, fullSource, fullTags)

	// The next backup's physical pgData path differs from the prior's (e.g.
	// after a restore-in-place) — buildPriorImage must still resolve it.
	movedSource := snapshot.SourceInfo{Host: "cluster-a", UserName: "streaming", Path: "/srv/restored/pgdata"}

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	prior, err := buildPriorImage(ctx, rep, movedSource, "backup-1", fullTags)
	require.NoError(t, err)
	require.NotNil(t, prior)

	reader, err := prior.BlockReader("base/1259", blockSize)
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck
	block, err := reader.ReadBlock(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("0123456789ABCDEF"), block)

	t.Run("unknown backup name is an error", func(t *testing.T) {
		_, err := buildPriorImage(ctx, rep, movedSource, "no-such-backup", fullTags)
		require.ErrorIs(t, err, errPriorSnapshotNotFound)
	})

	t.Run("empty backup name is an error", func(t *testing.T) {
		_, err := buildPriorImage(ctx, rep, movedSource, "", fullTags)
		require.ErrorIs(t, err, errIncrementalRequiresPrevious)
	})
}

// TestBuildPriorImageResolvesRenamedTablespace is the regression test for a
// tablespace renamed between backups (ALTER TABLESPACE ... RENAME changes
// the name but not the OID or the synthetic /tablespace/<oid> source path).
// The filter must key on that stable path, never on the mutable
// tablespace-name tag, or a rename would make the prior unresolvable and
// fail the whole backup mid-ingest.
func TestBuildPriorImageResolvesRenamedTablespace(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	tsSource := snapshot.SourceInfo{Host: "cluster-a", UserName: "streaming", Path: "/tablespace/16385"}
	oldNameTags := map[string]string{
		contentTag:               "tablespace",
		backupNameTag:            "backup-1",
		"klio.io/tablespaceName": "ts_old",
	}
	ingestFull(ctx, t, configFile, tsSource, oldNameTags)

	// Same OID/path, but the current ingest's own tags reflect the
	// tablespace's new name — buildPriorImage must still resolve the prior,
	// which was tagged under the old name.
	currentTags := map[string]string{
		contentTag:               "tablespace",
		backupNameTag:            "backup-2",
		"klio.io/tablespaceName": "ts_new",
	}

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	prior, err := buildPriorImage(ctx, rep, tsSource, "backup-1", currentTags)
	require.NoError(t, err)
	require.NotNil(t, prior)

	reader, err := prior.BlockReader("base/1259", blockSize)
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck
	block, err := reader.ReadBlock(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("0123456789ABCDEF"), block)
}

// TestBuildPriorImageAmbiguousIsError confirms buildPriorImage fails loudly
// rather than silently picking one snapshot when the filter tags match more
// than one — for example a reused backup name — since combining against an
// arbitrary combine base would corrupt the incremental.
func TestBuildPriorImageAmbiguousIsError(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	source := snapshot.SourceInfo{Host: "cluster-a", UserName: "streaming", Path: "/pgdata"}
	tags := map[string]string{contentTag: "pgdata", backupNameTag: "backup-1"}
	// Two snapshots under the same source and tags stand in for a reused --name.
	ingestFull(ctx, t, configFile, source, tags)
	ingestFull(ctx, t, configFile, source, tags)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	_, err = buildPriorImage(ctx, rep, source, "backup-1", tags)
	require.ErrorIs(t, err, errAmbiguousPriorSnapshot)
}
