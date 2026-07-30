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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cloudnative-pg/klio/core/kopia-pgbasebackup/internal/ingest"
)

// backupNameTag and contentTag must match klioclient.BackupNameTagName and
// klioclient.BackupContentTagName in the core module. The two modules are
// not code-shared (kopia-pgbasebackup is a separate Go module), but already
// agree on every tag key passed via --tag today.
const (
	backupNameTag = "klio.io/tag"
	contentTag    = "klio.io/content"
)

// segmentMapContentSuffix is appended to a source's data content-tag value to
// form the content tag of its segment-map snapshot (e.g. "pgdata-segmap"). A
// distinct value keeps the segment map resolvable per source and prevents it
// from making the data snapshot's resolution ambiguous.
const segmentMapContentSuffix = "-segmap"

// segmentMapPathSuffix forms the segment map's own Kopia source from the data
// source's path (e.g. "/pgdata" -> "/pgdata_segmap"), mirroring the
// pgdata_manifest and pgdata_meta convention in the core module. See segmapSource
// for where the suffix lands on a tablespace.
//
// The segment map MUST live on its own source, because Kopia's retention is
// enforced per source: `kopia snapshot expire` applies keepLatest (and every
// other rule) to each source's snapshots independently. Sharing a source with
// the data snapshot put two snapshots per backup into one budget, so keepLatest
// N retained only about N/2 backups. Worse, the segment map is written after its
// data snapshot, so under any count- or time-bucket rule it outranked the data
// snapshot of the same backup and retention deleted data while keeping segment
// maps — and because reaping was not atomic, the backup still advertised itself
// as complete until a recovery failed.
const segmentMapPathSuffix = "_segmap"

// tablespacePathPrefix is the synthetic SourceInfo.Path prefix core's
// executor.go gives every tablespace source ("/tablespace/<oid>"), used
// here to recognize a tablespace source without needing its own tag.
const tablespacePathPrefix = "/tablespace/"

var (
	errInvalidTag                  = errors.New("invalid --tag, expected key=value")
	errIncrementalRequiresPrevious = errors.New("--incremental requires --previous-backup-name")
	errEmptyRequiredFlag           = errors.New("required flag must not be empty")
	errPriorSnapshotNotFound       = errors.New("prior snapshot not found")
	errAmbiguousPriorSnapshot      = errors.New("prior snapshot is ambiguous")
)

// ingestCmd implements the "ingest" subcommand.
//
//nolint:gochecknoglobals
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Ingest a BASE_BACKUP tar stream (on stdin) into the Kopia repository",
	Example: "kopia-pgbasebackup ingest --config-file=... --host=... " +
		"--username=... --source-path=... < tar-stream",
	RunE: runIngest,
}

//nolint:gochecknoinits
func init() {
	ingestCmd.Flags().String("config-file", "", "path to the Kopia repository config file")
	ingestCmd.Flags().String("host", "", "snapshot SourceInfo.Host (cluster name)")
	ingestCmd.Flags().String("username", "", "snapshot SourceInfo.UserName")
	ingestCmd.Flags().String("source-path", "", "snapshot SourceInfo.Path")
	ingestCmd.Flags().Bool("incremental", false, "input tar is an incremental backup")
	ingestCmd.Flags().Int("block-size", ingest.DefaultBlockSize, "PostgreSQL BLCKSZ for incremental reconstruction")
	ingestCmd.Flags().StringArray("tag", nil, "snapshot tag key=value (repeatable)")
	ingestCmd.Flags().StringArray("pin", nil, "snapshot retention pin (repeatable)")
	ingestCmd.Flags().String("previous-backup-name", "",
		"prior backup name for incremental combine")

	for _, name := range []string{"config-file", "host", "username", "source-path"} {
		_ = ingestCmd.MarkFlagRequired(name)
	}
}

// ingestOutput is the JSON contract printed on stdout.
type ingestOutput struct {
	ManifestID   string      `json:"manifestId"`
	RootObjectID string      `json:"rootObjectId"`
	Stats        outputStats `json:"stats"`
}

type outputStats struct {
	TotalSize   int64 `json:"totalSize"`
	HashedBytes int64 `json:"hashedBytes"`
	CachedBytes int64 `json:"cachedBytes"`
	FileCount   int64 `json:"fileCount"`

	// ReusedFiles is the number of unchanged files that referenced the prior
	// object directly, and PriorReadBytes the block bytes read back from the
	// prior image to reconstruct the rest. Together they show how much of an
	// incremental's read-back the reuse fast path avoided.
	ReusedFiles    int64 `json:"reusedFiles"`
	PriorReadBytes int64 `json:"priorReadBytes"`

	// ZeroFilledBlocks is the number of below-truncation blocks that were
	// present in neither the incremental nor the prior image and so were
	// materialized as zero-filled uninitialized extension blocks. Normally
	// small; a spike may indicate a WAL-summary anomaly worth investigating.
	ZeroFilledBlocks int64 `json:"zeroFilledBlocks"`

	// ReferencedSegments is the number of clean segments taken by reference from
	// the prior backup (no readback), and RebuiltSegments the number rewritten.
	ReferencedSegments int64 `json:"referencedSegments"`
	RebuiltSegments    int64 `json:"rebuiltSegments"`

	// Phases attributes the ingest's time to the kind of work that consumed it,
	// which is what says whether a slow backup was waiting on PostgreSQL, on
	// segment writes, or on reading the prior image back.
	Phases phases `json:"phases"`
}

// phases mirrors ingest.Phases in the JSON contract.
type phases struct {
	TarReadMS       int64 `json:"tarReadMs"`
	PriorReadMS     int64 `json:"priorReadMs"`
	PriorBlockReads int64 `json:"priorBlockReads"`
	SegmentWriteMS  int64 `json:"segmentWriteMs"`
	IndexWriteMS    int64 `json:"indexWriteMs"`
	FinalizeMS      int64 `json:"finalizeMs"`
}

func runIngest(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	configFile, _ := cmd.Flags().GetString("config-file")
	host, _ := cmd.Flags().GetString("host")
	username, _ := cmd.Flags().GetString("username")
	sourcePath, _ := cmd.Flags().GetString("source-path")

	// MarkFlagRequired only checks that the flag was passed, not that it carries
	// a value. Reject an empty value so a blank --host cannot produce a snapshot
	// under an empty SourceInfo identity.
	for _, f := range []struct{ name, value string }{
		{"config-file", configFile},
		{"host", host},
		{"username", username},
		{"source-path", sourcePath},
	} {
		if f.value == "" {
			return fmt.Errorf("%w: --%s", errEmptyRequiredFlag, f.name)
		}
	}

	incremental, _ := cmd.Flags().GetBool("incremental")
	blockSize, _ := cmd.Flags().GetInt("block-size")
	tags, _ := cmd.Flags().GetStringArray("tag")
	pins, _ := cmd.Flags().GetStringArray("pin")
	previousBackupName, _ := cmd.Flags().GetString("previous-backup-name")

	tagMap, err := parseTags(tags)
	if err != nil {
		return err
	}

	source := snapshot.SourceInfo{Host: host, UserName: username, Path: sourcePath}
	opts := ingest.Options{
		Source:       source,
		Description:  "pg_basebackup ingest of " + sourcePath,
		Tags:         tagMap,
		Pins:         pins,
		Incremental:  incremental,
		BlockSize:    blockSize,
		SegmentBytes: ingest.DefaultSegmentBytes,
	}

	rep, err := repo.Open(ctx, configFile, viper.GetString("kopia-password"), &repo.Options{})
	if err != nil {
		return fmt.Errorf("while opening Kopia repository: %w", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	segmentBlocks := ingest.SegmentBlocksFor(ingest.DefaultSegmentBytes, blockSize)
	result, uploaded, priorReadBytes, zeroFilledBlocks, err := ingestIntoRepo(
		ctx, rep, opts, source, incremental, previousBackupName, tagMap, segmentBlocks)
	if err != nil {
		return err
	}

	return emitResult(result, uploaded, priorReadBytes, zeroFilledBlocks)
}

// ingestIntoRepo runs the ingest inside a write session, returning the result,
// the number of bytes actually uploaded (for the dedup statistics), the bytes
// read back from the prior image during incremental reconstruction (zero for a
// full backup or for unchanged files taken via the reuse fast path), and the
// count of below-truncation blocks zero-filled as uninitialized extensions.
func ingestIntoRepo(
	ctx context.Context,
	rep repo.Repository,
	opts ingest.Options,
	source snapshot.SourceInfo,
	incremental bool,
	previousBackupName string,
	tagMap map[string]string,
	segmentBlocks int,
) (*ingest.Result, int64, int64, int64, error) {
	var (
		uploaded         atomic.Int64
		result           *ingest.Result
		priorReadBytes   int64
		zeroFilledBlocks int64
	)

	writeErr := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose:  "kopia-pgbasebackup",
		OnUpload: func(n int64) { uploaded.Add(n) },
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		var prior *ingest.KopiaPriorImage
		if incremental {
			var priorErr error
			prior, priorErr = buildPriorImage(ctx, w, source, previousBackupName, tagMap, segmentBlocks)
			if priorErr != nil {
				return priorErr
			}
			opts.Prior = prior
		}

		var runErr error
		result, runErr = ingest.Run(ctx, w, os.Stdin, opts)
		if prior != nil {
			priorReadBytes = prior.BytesRead()
			zeroFilledBlocks = prior.ZeroFilledBlocks()
		}
		if runErr != nil {
			return runErr
		}

		return saveSegmentMap(ctx, w, source, opts, result.SegmentMap)
	})
	if writeErr != nil {
		return nil, 0, 0, 0, fmt.Errorf("while ingesting BASE_BACKUP: %w", writeErr)
	}

	return result, uploaded.Load(), priorReadBytes, zeroFilledBlocks, nil
}

// saveSegmentMap persists this backup's segment map as its own compressed,
// tagged directory-tree snapshot, in the same write session as the data snapshot
// so the two commit together. It is written to the source's derived segmap source
// (see segmentMapPathSuffix) and tagged with the backup name plus a segmap
// content type, so it is resolved by tags and keeps its own retention budget.
// Every backup that stores segments (full, referencing incremental, and
// rebuild-all incremental alike) writes one; a source with no files writes
// nothing.
//
// Reaping a segment map independently of its data snapshot is safe: the data
// snapshot references every segment object directly, so segments stay
// GC-reachable without the map (see ingest.SaveSegmentMap). A map that outlives
// its data snapshot only wastes space, and one that is reaped early costs the
// next incremental its referencing, never correctness.
func saveSegmentMap(
	ctx context.Context, w repo.RepositoryWriter, source snapshot.SourceInfo,
	opts ingest.Options, segMap ingest.SegmentMap,
) error {
	if len(segMap.Files) == 0 {
		return nil
	}

	tags := make(map[string]string, len(opts.Tags))
	maps.Copy(tags, opts.Tags)
	tags[contentTag] = segmapContentValue(opts.Tags[contentTag])

	if _, err := ingest.SaveSegmentMap(
		ctx, w, segmapSource(source), tags, opts.Pins, "segment map for "+source.Path, segMap,
	); err != nil {
		return fmt.Errorf("while saving segment map: %w", err)
	}

	return nil
}

// segmapContentValue derives a source's segment-map content-tag value from its
// data content value (e.g. "pgdata" -> "pgdata-segmap"). See
// segmentMapContentSuffix for why the value is distinct.
func segmapContentValue(dataContent string) string {
	return dataContent + segmentMapContentSuffix
}

// segmapSource derives the segment map's own Kopia source from the data source.
// See segmentMapPathSuffix for why the segment map must not share a source.
//
// The suffix always lands on the component that names the *kind* of source, never
// on the one that identifies it. For pgdata that is the whole path
// ("/pgdata" -> "/pgdata_segmap"). For a tablespace, whose synthetic path is
// "/tablespace/<oid>", it is the prefix ("/tablespace/16385" ->
// "/tablespace_segmap/16385"), so the OID stays the leaf. Suffixing the OID
// instead would both invent a tablespace named "16385_segmap" and leave the
// derived path matching tablespacePathPrefix, i.e. still answering "yes" to "is
// this a tablespace source?".
func segmapSource(source snapshot.SourceInfo) snapshot.SourceInfo {
	segmap := source

	if oid, isTablespace := strings.CutPrefix(source.Path, tablespacePathPrefix); isTablespace {
		segmap.Path = strings.TrimSuffix(tablespacePathPrefix, "/") + segmentMapPathSuffix + "/" + oid

		return segmap
	}

	segmap.Path = strings.TrimSuffix(source.Path, "/") + segmentMapPathSuffix

	return segmap
}

// buildPriorImage resolves the prior backup's snapshot for this same source
// and exposes its tree for incremental reconstruction, then installs the prior
// segment map so unchanged segments can be referenced. Resolution and use
// happen here, in the same call, immediately before the combine runs, so there
// is no manifest ID handed across a process boundary for a later pin/unpin to
// invalidate.
func buildPriorImage(
	ctx context.Context, rep repo.Repository, source snapshot.SourceInfo,
	previousBackupName string, tagMap map[string]string, segmentBlocks int,
) (*ingest.KopiaPriorImage, error) {
	if previousBackupName == "" {
		return nil, errIncrementalRequiresPrevious
	}

	// A tablespace is pinned to its path, so its segment map must be looked up on
	// the derived segmap path rather than the data path; pgdata resolves purely by
	// tag and needs no path at all.
	scope := snapshot.SourceInfo{Host: source.Host, UserName: source.UserName}
	segmapScope := scope
	if strings.HasPrefix(source.Path, tablespacePathPrefix) {
		scope.Path = source.Path
		segmapScope.Path = segmapSource(source).Path
	}

	man, err := resolvePriorSnapshot(ctx, rep, scope, previousBackupName, tagMap[contentTag])
	if err != nil {
		return nil, err
	}
	if man.RootEntry == nil {
		return nil, fmt.Errorf("prior snapshot %q has no root entry", previousBackupName)
	}

	prior := ingest.NewKopiaPriorImage(ctx, rep, man.RootEntry.ObjectID)

	if err := installPriorSegments(
		ctx, rep, prior, segmapScope, previousBackupName, tagMap[contentTag], segmentBlocks,
	); err != nil {
		return nil, err
	}

	return prior, nil
}

// resolvePriorSnapshot finds the single prior snapshot for this source and
// content type. The filter keys only on discriminators stable across backups:
// content type, backup name, and — for a tablespace — its OID carried in the
// (stable) source path, never the mutable tablespace-name tag. pgdata's own
// physical path is not stable across backups (e.g. after a restore-in-place),
// so it is deliberately left out of scope.
func resolvePriorSnapshot(
	ctx context.Context, rep repo.Repository, scope snapshot.SourceInfo,
	previousBackupName, content string,
) (*snapshot.Manifest, error) {
	filterTags := map[string]string{backupNameTag: previousBackupName}
	if content != "" {
		filterTags[contentTag] = content
	}

	// ListSnapshotManifests filters on raw manifest labels, not Options.Tags
	// values directly — SaveSnapshot stores each tag under a "tag:"-prefixed
	// label (see ingest.UserTagLabels), so the filter must use the same
	// prefixed keys to match.
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, &scope, ingest.UserTagLabels(filterTags))
	if err != nil {
		return nil, fmt.Errorf("while finding prior snapshot %q: %w", previousBackupName, err)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: %q", errPriorSnapshotNotFound, previousBackupName)
	}
	// A single klio backup writes exactly one snapshot per source, so more than
	// one match means the tags no longer identify a unique combine base (for
	// example a reused --name, or an incomplete snapshot left by an interrupted
	// earlier attempt). Combining against an arbitrary one would silently
	// corrupt the incremental, so fail loudly instead of guessing.
	if len(ids) > 1 {
		return nil, fmt.Errorf("%w: %q matches %d snapshots", errAmbiguousPriorSnapshot, previousBackupName, len(ids))
	}

	man, err := snapshot.LoadSnapshot(ctx, rep, ids[0])
	if err != nil {
		return nil, fmt.Errorf("while loading prior snapshot %q: %w", previousBackupName, err)
	}

	return man, nil
}

// installPriorSegments resolves the prior backup's segment-map snapshot for this
// source and installs its per-file segment IDs on prior, enabling segment
// referencing. scope must be the segmap scope, not the data scope. It is a no-op
// — leaving the incremental to rebuild every segment — when the prior recorded no
// segment map (a backup taken before this feature, or one whose map has since
// been reaped) or when the prior's segment size differs from the configured one,
// since referencing a differently-sized segment would corrupt the file.
func installPriorSegments(
	ctx context.Context, rep repo.Repository, prior *ingest.KopiaPriorImage,
	scope snapshot.SourceInfo, previousBackupName, content string, segmentBlocks int,
) error {
	filterTags := map[string]string{
		backupNameTag: previousBackupName,
		contentTag:    segmapContentValue(content),
	}

	ids, err := snapshot.ListSnapshotManifests(ctx, rep, &scope, ingest.UserTagLabels(filterTags))
	if err != nil {
		return fmt.Errorf("while finding prior segment map %q: %w", previousBackupName, err)
	}
	if len(ids) == 0 {
		return nil
	}
	if len(ids) > 1 {
		return fmt.Errorf("%w: %q segment map matches %d snapshots",
			errAmbiguousPriorSnapshot, previousBackupName, len(ids))
	}

	man, err := snapshot.LoadSnapshot(ctx, rep, ids[0])
	if err != nil {
		return fmt.Errorf("while loading prior segment map %q: %w", previousBackupName, err)
	}
	if man.RootEntry == nil {
		return nil
	}

	priorBlocks, segs, err := ingest.LoadSegmentMap(ctx, rep, man.RootEntry.ObjectID)
	if err != nil {
		return err
	}
	if priorBlocks == segmentBlocks {
		prior.SetSegments(segs)
	}

	return nil
}

func emitResult(result *ingest.Result, uploaded, priorReadBytes, zeroFilledBlocks int64) error {
	// These byte counts are an approximation, not an exact accounting. uploaded
	// is the number of bytes written to the blob store (post-compression,
	// post-dedup), reported as hashedBytes; TotalSize is the logical size of the
	// ingested files. cachedBytes is their difference, clamped at zero. Because
	// the two are measured on different sides of compression they are not
	// strictly comparable; the ratio is only meant as a signal of how effective
	// deduplication was for this ingest, not an exact cache-hit byte count.
	cached := max(result.Stats.TotalSize-uploaded, 0)

	out := ingestOutput{
		ManifestID:   result.ManifestID,
		RootObjectID: result.RootObjectID,
		Stats: outputStats{
			TotalSize:          result.Stats.TotalSize,
			HashedBytes:        uploaded,
			CachedBytes:        cached,
			FileCount:          result.Stats.FileCount,
			ReusedFiles:        result.Stats.ReusedFiles,
			PriorReadBytes:     priorReadBytes,
			ZeroFilledBlocks:   zeroFilledBlocks,
			ReferencedSegments: result.Stats.ReferencedSegments,
			RebuiltSegments:    result.Stats.RebuiltSegments,
			Phases: phases{
				TarReadMS:       result.Stats.Phases.TarReadMS,
				PriorReadMS:     result.Stats.Phases.PriorReadMS,
				PriorBlockReads: result.Stats.Phases.PriorBlockReads,
				SegmentWriteMS:  result.Stats.Phases.SegmentWriteMS,
				IndexWriteMS:    result.Stats.Phases.IndexWriteMS,
				FinalizeMS:      result.Stats.Phases.FinalizeMS,
			},
		},
	}

	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		return fmt.Errorf("while writing result: %w", err)
	}

	return nil
}

func parseTags(tags []string) (map[string]string, error) {
	result := make(map[string]string, len(tags))
	for _, t := range tags {
		key, value, ok := strings.Cut(t, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%w: %q", errInvalidTag, t)
		}
		result[key] = value
	}

	return result, nil
}
