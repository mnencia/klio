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
	"strings"
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
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

// ingestSourceSegmap ingests one distinct block as a full backup for source and
// returns its resulting segment map (with real object IDs). fileData must be
// exactly one blockSize block.
func ingestSourceSegmap(
	ctx context.Context, t *testing.T, configFile string,
	source snapshot.SourceInfo, tags map[string]string, fileData []byte, segBytes int,
) ingest.SegmentMap {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "base", Typeflag: tar.TypeDir, Mode: 0o700}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "base/1259", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(fileData)),
	}))
	_, err = tw.Write(fileData)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	var segMap ingest.SegmentMap
	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			res, runErr := ingest.Run(ctx, w, &buf,
				ingest.Options{Source: source, Tags: tags, BlockSize: blockSize, SegmentBytes: segBytes})
			if runErr != nil {
				return runErr
			}
			segMap = res.SegmentMap

			id, serr := ingest.SaveSegmentMap(ctx, w, segmapSource(source),
				map[string]string{backupNameTag: tags[backupNameTag], contentTag: segmapContentValue(tags[contentTag])},
				nil, "segment map", res.SegmentMap)
			_ = id

			return serr
		}))

	return segMap
}

// TestBuildPriorImageMultiSourceSegmapResolvesPerSource is the adversarial test
// for the per-source segment-map tag scheme. With pgdata and two tablespaces
// all present under one backup name — each with its own data and segment-map
// snapshot — every source must resolve its OWN prior segment map, never a
// sibling's, with no ambiguity, and the data snapshot's own resolution must not
// be shadowed by the coexisting segment-map snapshot. A broken scheme (a shared
// segmap content value, or substring tag matching) would surface here as an
// ambiguity error or a mismatched installed map.
func TestBuildPriorImageMultiSourceSegmapResolvesPerSource(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)
	const segBytes = 2 * blockSize
	segBlocks := ingest.SegmentBlocksFor(segBytes, blockSize)

	sources := []struct {
		name    string
		source  snapshot.SourceInfo
		content string
		data    []byte
		segMap  ingest.SegmentMap
	}{
		{
			name:    "pgdata",
			source:  snapshot.SourceInfo{Host: "c", UserName: "s", Path: "/pgdata"},
			content: "pgdata",
			data:    bytes.Repeat([]byte{0x11}, blockSize),
		},
		{
			name:    "tablespace-16385",
			source:  snapshot.SourceInfo{Host: "c", UserName: "s", Path: "/tablespace/16385"},
			content: "tablespace",
			data:    bytes.Repeat([]byte{0x22}, blockSize),
		},
		{
			name:    "tablespace-16386",
			source:  snapshot.SourceInfo{Host: "c", UserName: "s", Path: "/tablespace/16386"},
			content: "tablespace",
			data:    bytes.Repeat([]byte{0x33}, blockSize),
		},
	}

	for i := range sources {
		tags := map[string]string{contentTag: sources[i].content, backupNameTag: "backup-1"}
		sources[i].segMap = ingestSourceSegmap(ctx, t, configFile, sources[i].source, tags, sources[i].data, segBytes)
	}

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	for _, s := range sources {
		t.Run(s.name, func(t *testing.T) {
			currentTags := map[string]string{contentTag: s.content, backupNameTag: "backup-2"}
			prior, err := buildPriorImage(ctx, rep, s.source, "backup-1", currentTags, segBlocks)
			require.NoError(t, err)
			require.NotNil(t, prior)

			got, ok := prior.Segments("base/1259")
			require.True(t, ok, "expected this source's segment map to be installed")

			want := s.segMap.Files["base/1259"]
			require.Len(t, got, len(want))
			for i := range want {
				assert.Equal(t, want[i], got[i].String(), "installed segment map is not this source's own")
			}
		})
	}
}

// TestBuildPriorImageSizeMismatchSkipsSegments confirms the size gate: an
// incremental references the prior segments only when the configured segment
// size matches the size recorded in the prior map. A mismatch must leave the
// segments uninstalled so the incremental rebuilds every segment rather than
// splicing a differently-sized prior segment and corrupting the file.
func TestBuildPriorImageSizeMismatchSkipsSegments(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	source := snapshot.SourceInfo{Host: "c", UserName: "s", Path: "/pgdata"}
	tags := map[string]string{contentTag: "pgdata", backupNameTag: "backup-1"}
	segMap := ingestSourceSegmap(ctx, t, configFile, source, tags, bytes.Repeat([]byte{0x11}, blockSize), 2*blockSize)
	require.Equal(t, 2, segMap.SegmentBlocks)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	current := map[string]string{contentTag: "pgdata", backupNameTag: "backup-2"}

	matching, err := buildPriorImage(ctx, rep, source, "backup-1", current, 2)
	require.NoError(t, err)
	_, ok := matching.Segments("base/1259")
	assert.True(t, ok, "matching segment size should install the prior segments")

	mismatched, err := buildPriorImage(ctx, rep, source, "backup-1", current, 3)
	require.NoError(t, err)
	_, ok = mismatched.Segments("base/1259")
	assert.False(t, ok, "mismatched segment size must not install the prior segments")
}

// TestBuildPriorImageAmbiguousSegmapIsError confirms a duplicated segment-map
// snapshot (e.g. an interrupted retry) fails loudly rather than picking one,
// mirroring the data-snapshot ambiguity guard.
func TestBuildPriorImageAmbiguousSegmapIsError(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	source := snapshot.SourceInfo{Host: "c", UserName: "s", Path: "/pgdata"}
	tags := map[string]string{contentTag: "pgdata", backupNameTag: "backup-1"}
	segMap := ingestSourceSegmap(ctx, t, configFile, source, tags, bytes.Repeat([]byte{0x11}, blockSize), 2*blockSize)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	// A second segment-map snapshot under the same source and tags.
	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			_, serr := ingest.SaveSegmentMap(ctx, w, source,
				map[string]string{backupNameTag: "backup-1", contentTag: segmapContentValue("pgdata")},
				nil, "duplicate", segMap)

			return serr
		}))

	current := map[string]string{contentTag: "pgdata", backupNameTag: "backup-2"}
	_, err = buildPriorImage(ctx, rep, source, "backup-1", current, 2)
	require.ErrorIs(t, err, errAmbiguousPriorSnapshot)
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

	prior, err := buildPriorImage(ctx, rep, movedSource, "backup-1", fullTags, 0)
	require.NoError(t, err)
	require.NotNil(t, prior)

	reader, err := prior.BlockReader("base/1259", blockSize)
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck
	block, err := reader.ReadBlock(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("0123456789ABCDEF"), block)

	t.Run("unknown backup name is an error", func(t *testing.T) {
		_, err := buildPriorImage(ctx, rep, movedSource, "no-such-backup", fullTags, 0)
		require.ErrorIs(t, err, errPriorSnapshotNotFound)
	})

	t.Run("empty backup name is an error", func(t *testing.T) {
		_, err := buildPriorImage(ctx, rep, movedSource, "", fullTags, 0)
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

	prior, err := buildPriorImage(ctx, rep, tsSource, "backup-1", currentTags, 0)
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

	_, err = buildPriorImage(ctx, rep, source, "backup-1", tags, 0)
	require.ErrorIs(t, err, errAmbiguousPriorSnapshot)
}

// takeProtocolBackup performs one backup of source the way the protocol path
// does: the data snapshot, then this backup's segment map in the same write
// session through the production saveSegmentMap, so the test exercises the real
// source derivation rather than a copy of it.
func takeProtocolBackup(
	ctx context.Context, t *testing.T, configFile string,
	source snapshot.SourceInfo, backupName, content string, fileData []byte, segBytes int,
) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "base", Typeflag: tar.TypeDir, Mode: 0o700}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "base/1259", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(fileData)),
	}))
	_, err = tw.Write(fileData)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	opts := ingest.Options{
		Source:       source,
		Tags:         map[string]string{contentTag: content, backupNameTag: backupName},
		BlockSize:    blockSize,
		SegmentBytes: segBytes,
	}

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "test"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			res, runErr := ingest.Run(ctx, w, &buf, opts)
			if runErr != nil {
				return runErr
			}

			return saveSegmentMap(ctx, w, source, opts, res.SegmentMap)
		}))
}

// setKeepLatestPolicy pins keepLatest on target and zeroes every other retention
// rule. The zeroing matters: unset rules inherit kopia's global defaults
// (keepHourly 48, keepDaily 7, ...), any of which would retain a snapshot that
// keepLatest drops and make the assertions vacuous.
func setKeepLatestPolicy(
	ctx context.Context, t *testing.T, configFile string, target snapshot.SourceInfo, keepLatest int,
) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	zero := policy.OptionalInt(0)
	keep := policy.OptionalInt(keepLatest)

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "policy"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			return policy.SetPolicy(ctx, w, target, &policy.Policy{
				RetentionPolicy: policy.RetentionPolicy{
					KeepLatest:  &keep,
					KeepHourly:  &zero,
					KeepDaily:   &zero,
					KeepWeekly:  &zero,
					KeepMonthly: &zero,
					KeepAnnual:  &zero,
				},
			})
		}))
}

// expireAllSources applies retention to every source in the repository, which is
// what klio's "kopia snapshot expire --delete klio@host" does: expiry is a
// per-source operation, and that is the whole reason a shared source was unsafe.
func expireAllSources(ctx context.Context, t *testing.T, configFile string) {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{Purpose: "expire"},
		func(ctx context.Context, w repo.RepositoryWriter) error {
			sources, err := snapshot.ListSources(ctx, w)
			if err != nil {
				return err
			}
			for _, si := range sources {
				if _, err := policy.ApplyRetentionPolicy(ctx, w, si, true); err != nil {
					return err
				}
			}

			return nil
		}))
}

// countSnapshotsByTag resolves snapshots the way a recovery does — by backup name
// and content tag across the whole user@host, path-independent.
func countSnapshotsByTag(
	ctx context.Context, t *testing.T, configFile string, scope snapshot.SourceInfo, backupName, content string,
) int {
	t.Helper()

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	ids, err := snapshot.ListSnapshotManifests(ctx, rep, &scope, ingest.UserTagLabels(map[string]string{
		backupNameTag: backupName,
		contentTag:    content,
	}))
	require.NoError(t, err)

	return len(ids)
}

// TestRetentionKeepsEveryRetainedBackupsData is the regression test for the
// segment map sharing a Kopia source with the data snapshot.
//
// Kopia enforces retention per source, so a shared source spent two of every
// keepLatest budget on a single backup, retaining only about N/2 backups. Worse,
// the segment map is always written after its data snapshot, so under any count-
// or time-bucket rule it outranked the data snapshot of the same backup:
// retention deleted data and preserved segment maps. Reaping was not atomic, so
// the backup kept advertising itself as complete and the loss surfaced only when
// a recovery was attempted.
//
// With keepLatest=2 and three backups, both retained backups must still resolve
// their own data snapshot. Before the fix the pgdata source held six snapshots
// (three data, three maps) and only backup-3's data survived.
func TestRetentionKeepsEveryRetainedBackupsData(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)
	const segBytes = 2 * blockSize

	source := snapshot.SourceInfo{Host: "c", UserName: "klio", Path: "/pgdata"}
	scope := snapshot.SourceInfo{Host: source.Host, UserName: source.UserName}

	setKeepLatestPolicy(ctx, t, configFile, scope, 2)

	names := []string{"backup-1", "backup-2", "backup-3"}
	for i, name := range names {
		// Distinct data per backup, so the snapshots are not identical trees and
		// cannot be collapsed by identical-snapshot handling.
		takeProtocolBackup(ctx, t, configFile, source, name, "pgdata",
			bytes.Repeat([]byte{byte(0x10 + i)}, blockSize), segBytes)
	}

	expireAllSources(ctx, t, configFile)

	// The two newest backups keep their data: this is the property that broke.
	for _, name := range []string{"backup-2", "backup-3"} {
		assert.Equal(t, 1, countSnapshotsByTag(ctx, t, configFile, scope, name, "pgdata"),
			"%s lost its data snapshot to retention while its metadata survived", name)
	}

	// And retention really ran, so the assertions above are not vacuous.
	assert.Equal(t, 0, countSnapshotsByTag(ctx, t, configFile, scope, "backup-1", "pgdata"),
		"retention did not expire the oldest backup, so this test proves nothing")

	// Each source carries one snapshot per backup, so the segment maps are reaped
	// on the same boundary as the data rather than outliving it.
	assert.Equal(t, 0, countSnapshotsByTag(ctx, t, configFile, scope, "backup-1", "pgdata-segmap"))
	for _, name := range []string{"backup-2", "backup-3"} {
		assert.Equal(t, 1, countSnapshotsByTag(ctx, t, configFile, scope, name, "pgdata-segmap"))
	}
}

// TestSegmentMapUsesItsOwnSource states the structural invariant behind the
// retention fix directly, independent of any policy: the segment map must not
// land on the data snapshot's source.
func TestSegmentMapUsesItsOwnSource(t *testing.T) {
	ctx := context.Background()
	configFile := newRepo(ctx, t)

	source := snapshot.SourceInfo{Host: "c", UserName: "klio", Path: "/pgdata"}
	takeProtocolBackup(ctx, t, configFile, source, "backup-1", "pgdata",
		bytes.Repeat([]byte{0x11}, blockSize), 2*blockSize)

	rep, err := repo.Open(ctx, configFile, testPassword, &repo.Options{})
	require.NoError(t, err)
	defer rep.Close(ctx) //nolint:errcheck

	sources, err := snapshot.ListSources(ctx, rep)
	require.NoError(t, err)

	paths := make([]string, 0, len(sources))
	for _, si := range sources {
		paths = append(paths, si.Path)
	}
	assert.ElementsMatch(t, []string{"/pgdata", "/pgdata_segmap"}, paths,
		"the segment map must occupy its own source, since kopia expires per source")
}

// TestSegmapSourceDerivation pins where the _segmap suffix lands. It must attach
// to the component naming the kind of source, never to the one identifying it, so
// that a tablespace keeps its OID as the leaf and no derived path answers "yes" to
// tablespacePathPrefix — the prefix buildPriorImage uses to decide whether a
// source is a tablespace at all.
func TestSegmapSourceDerivation(t *testing.T) {
	cases := []struct {
		name string
		path string
		want string
	}{
		{name: "pgdata", path: "/var/lib/postgresql/data/pgdata", want: "/var/lib/postgresql/data/pgdata_segmap"},
		{name: "pgdata with trailing slash", path: "/pgdata/", want: "/pgdata_segmap"},
		{name: "tablespace keeps its oid as the leaf", path: "/tablespace/16385", want: "/tablespace_segmap/16385"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := segmapSource(snapshot.SourceInfo{Host: "c", UserName: "klio", Path: tc.path})

			assert.Equal(t, tc.want, got.Path)
			// Host and user name identify the cluster and must be carried over
			// unchanged, or the map lands under a different identity than its data.
			assert.Equal(t, "c", got.Host)
			assert.Equal(t, "klio", got.UserName)
			assert.False(t, strings.HasPrefix(got.Path, tablespacePathPrefix),
				"a segmap source must not look like a tablespace source")
		})
	}
}
