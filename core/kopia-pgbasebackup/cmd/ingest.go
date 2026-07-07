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
	previousBackupName, _ := cmd.Flags().GetString("previous-backup-name")

	tagMap, err := parseTags(tags)
	if err != nil {
		return err
	}

	source := snapshot.SourceInfo{Host: host, UserName: username, Path: sourcePath}
	opts := ingest.Options{
		Source:      source,
		Description: "pg_basebackup ingest of " + sourcePath,
		Tags:        tagMap,
		Incremental: incremental,
		BlockSize:   blockSize,
	}

	rep, err := repo.Open(ctx, configFile, viper.GetString("kopia-password"), &repo.Options{})
	if err != nil {
		return fmt.Errorf("while opening Kopia repository: %w", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	result, uploaded, err := ingestIntoRepo(ctx, rep, opts, source, incremental, previousBackupName, tagMap)
	if err != nil {
		return err
	}

	return emitResult(result, uploaded)
}

// ingestIntoRepo runs the ingest inside a write session, returning the result
// and the number of bytes actually uploaded (for the dedup statistics).
func ingestIntoRepo(
	ctx context.Context,
	rep repo.Repository,
	opts ingest.Options,
	source snapshot.SourceInfo,
	incremental bool,
	previousBackupName string,
	tagMap map[string]string,
) (*ingest.Result, int64, error) {
	var (
		uploaded atomic.Int64
		result   *ingest.Result
	)

	writeErr := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose:  "kopia-pgbasebackup",
		OnUpload: func(n int64) { uploaded.Add(n) },
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		if incremental {
			prior, priorErr := buildPriorImage(ctx, w, source, previousBackupName, tagMap)
			if priorErr != nil {
				return priorErr
			}
			opts.Prior = prior
		}

		var runErr error
		result, runErr = ingest.Run(ctx, w, os.Stdin, opts)

		return runErr
	})
	if writeErr != nil {
		return nil, 0, fmt.Errorf("while ingesting BASE_BACKUP: %w", writeErr)
	}

	return result, uploaded.Load(), nil
}

// buildPriorImage resolves the prior backup's snapshot for this same source
// and exposes its tree for incremental reconstruction. Resolution and use
// happen here, in the same call, immediately before the combine runs, so
// there is no manifest ID handed across a process boundary for a later
// pin/unpin to invalidate.
//
// The filter keys only on discriminators that are stable across backups:
// content type, backup name, and — for a tablespace — its OID, carried in
// the (stable) source path, never the mutable tablespace-name tag (ALTER
// TABLESPACE ... RENAME changes the name but not the OID or path). pgdata's
// own physical path is *not* stable across backups (for example after a
// restore-in-place), so it is deliberately left out of scope instead.
//
//nolint:ireturn // returns the PriorImage abstraction by design
func buildPriorImage(
	ctx context.Context, rep repo.Repository, source snapshot.SourceInfo,
	previousBackupName string, tagMap map[string]string,
) (ingest.PriorImage, error) {
	if previousBackupName == "" {
		return nil, errIncrementalRequiresPrevious
	}

	filterTags := map[string]string{backupNameTag: previousBackupName}
	if content, ok := tagMap[contentTag]; ok {
		filterTags[contentTag] = content
	}

	scope := snapshot.SourceInfo{Host: source.Host, UserName: source.UserName}
	if strings.HasPrefix(source.Path, tablespacePathPrefix) {
		scope.Path = source.Path
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
	if man.RootEntry == nil {
		return nil, fmt.Errorf("prior snapshot %q has no root entry", previousBackupName)
	}

	return ingest.NewKopiaPriorImage(ctx, rep, man.RootEntry.ObjectID), nil
}

func emitResult(result *ingest.Result, uploaded int64) error {
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
			TotalSize:   result.Stats.TotalSize,
			HashedBytes: uploaded,
			CachedBytes: cached,
			FileCount:   result.Stats.FileCount,
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
