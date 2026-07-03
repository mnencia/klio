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
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cloudnative-pg/klio/core/kopia-pgbasebackup/internal/ingest"
)

var (
	errInvalidTag                  = errors.New("invalid --tag, expected key=value")
	errIncrementalRequiresPrevious = errors.New("--incremental requires --previous-manifest")
	errEmptyRequiredFlag           = errors.New("required flag must not be empty")
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
	ingestCmd.Flags().String("previous-manifest", "",
		"prior snapshot manifest ID for incremental combine")

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
	pins, _ := cmd.Flags().GetStringArray("pin")
	prevManifest, _ := cmd.Flags().GetString("previous-manifest")

	tagMap, err := parseTags(tags)
	if err != nil {
		return err
	}

	opts := ingest.Options{
		Source:      snapshot.SourceInfo{Host: host, UserName: username, Path: sourcePath},
		Description: "pg_basebackup ingest of " + sourcePath,
		Tags:        tagMap,
		Pins:        pins,
		Incremental: incremental,
		BlockSize:   blockSize,
	}

	rep, err := repo.Open(ctx, configFile, viper.GetString("kopia-password"), &repo.Options{})
	if err != nil {
		return fmt.Errorf("while opening Kopia repository: %w", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	result, uploaded, err := ingestIntoRepo(ctx, rep, opts, incremental, prevManifest)
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
	incremental bool,
	prevManifest string,
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
			prior, priorErr := buildPriorImage(ctx, w, prevManifest)
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

// buildPriorImage loads the prior snapshot referenced by previous-manifest
// and exposes its tree for incremental reconstruction. A single source is
// always combined against exactly one prior snapshot (the depth-1
// invariant), so the flag carries a single manifest ID.
//
//nolint:ireturn // returns the PriorImage abstraction by design
func buildPriorImage(ctx context.Context, rep repo.Repository, prevManifest string) (ingest.PriorImage, error) {
	if prevManifest == "" {
		return nil, errIncrementalRequiresPrevious
	}

	prior, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(prevManifest))
	if err != nil {
		return nil, fmt.Errorf("while loading prior snapshot %q: %w", prevManifest, err)
	}
	if prior.RootEntry == nil {
		return nil, fmt.Errorf("prior snapshot %q has no root entry", prevManifest)
	}

	return ingest.NewKopiaPriorImage(ctx, rep, prior.RootEntry.ObjectID), nil
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
