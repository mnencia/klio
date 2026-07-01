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

// Command kopia-pgbasebackup ingests a PostgreSQL BASE_BACKUP tar stream
// (delivered on stdin) into a Kopia repository, building a self-contained
// snapshot bottom-up. It exists as a separate binary so the Kopia Go library
// never enters the main klio binary; klio invokes it via exec, exactly as it
// invokes the kopia CLI.
//
// The helper code lives in this repository. It consumes the Kopia library under
// its canonical import path; a replace directive in go.mod currently sources it
// from the Klio fork until the patches are merged upstream.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"

	"github.com/cloudnative-pg/klio/core/kopia-pgbasebackup/internal/ingest"
)

var (
	errMissingRequiredFlags        = errors.New("--config-file, --host, --username and --source-path are required")
	errInvalidTag                  = errors.New("invalid --tag, expected key=value")
	errIncrementalRequiresPrevious = errors.New("--incremental requires at least one --previous-manifest")
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kopia-pgbasebackup ingest [flags] < tar-stream")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "ingest":
		if err := runIngest(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "kopia-pgbasebackup: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// stringList is a repeatable string flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)

	return nil
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

// ingestFlags holds the parsed ingest command flags.
type ingestFlags struct {
	configFile  string
	incremental bool
	prevManis   stringList
	opts        ingest.Options
}

func parseIngestFlags(args []string) (*ingestFlags, error) {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)

	var (
		configFile  = fs.String("config-file", "", "path to the Kopia repository config file")
		host        = fs.String("host", "", "snapshot SourceInfo.Host (cluster name)")
		username    = fs.String("username", "", "snapshot SourceInfo.UserName")
		sourcePath  = fs.String("source-path", "", "snapshot SourceInfo.Path")
		incremental = fs.Bool("incremental", false, "input tar is an incremental backup")
		blockSize   = fs.Int("block-size", ingest.DefaultBlockSize, "PostgreSQL BLCKSZ for incremental reconstruction")
		tags        stringList
		pins        stringList
		prevManis   stringList
	)
	fs.Var(&tags, "tag", "snapshot tag key=value (repeatable)")
	fs.Var(&pins, "pin", "snapshot retention pin (repeatable)")
	fs.Var(&prevManis, "previous-manifest",
		"prior snapshot manifest ID for incremental combine (repeatable; only the first is used)")

	if err := fs.Parse(args); err != nil {
		return nil, err //nolint:wrapcheck
	}

	if *configFile == "" || *host == "" || *username == "" || *sourcePath == "" {
		return nil, errMissingRequiredFlags
	}

	tagMap, err := parseTags(tags)
	if err != nil {
		return nil, err
	}

	return &ingestFlags{
		configFile:  *configFile,
		incremental: *incremental,
		prevManis:   prevManis,
		opts: ingest.Options{
			Source:      snapshot.SourceInfo{Host: *host, UserName: *username, Path: *sourcePath},
			Description: "pg_basebackup ingest of " + *sourcePath,
			Tags:        tagMap,
			Pins:        pins,
			Incremental: *incremental,
			BlockSize:   *blockSize,
		},
	}, nil
}

func runIngest(ctx context.Context, args []string) error {
	f, err := parseIngestFlags(args)
	if err != nil {
		return err
	}

	rep, err := repo.Open(ctx, f.configFile, os.Getenv("KOPIA_PASSWORD"), &repo.Options{})
	if err != nil {
		return fmt.Errorf("while opening Kopia repository: %w", err)
	}
	defer rep.Close(ctx) //nolint:errcheck

	result, uploaded, err := ingestIntoRepo(ctx, rep, f)
	if err != nil {
		return err
	}

	return emitResult(result, uploaded)
}

// ingestIntoRepo runs the ingest inside a write session, returning the result
// and the number of bytes actually uploaded (for the dedup statistics).
func ingestIntoRepo(ctx context.Context, rep repo.Repository, f *ingestFlags) (*ingest.Result, int64, error) {
	var (
		uploaded atomic.Int64
		result   *ingest.Result
	)

	opts := f.opts

	writeErr := repo.WriteSession(ctx, rep, repo.WriteSessionOptions{
		Purpose:  "kopia-pgbasebackup",
		OnUpload: func(n int64) { uploaded.Add(n) },
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		if f.incremental {
			prior, priorErr := buildPriorImage(ctx, w, f.prevManis)
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

// buildPriorImage loads the prior snapshot referenced by the first
// previous-manifest and exposes its tree for incremental reconstruction.
//
// The flag is repeatable for forward compatibility, but a single source is
// combined against exactly one prior snapshot (the depth-1 invariant), so only
// the first manifest is used; any others are ignored.
//
//nolint:ireturn // returns the PriorImage abstraction by design
func buildPriorImage(ctx context.Context, rep repo.Repository, prevManis stringList) (ingest.PriorImage, error) {
	if len(prevManis) == 0 {
		return nil, errIncrementalRequiresPrevious
	}

	prior, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(prevManis[0]))
	if err != nil {
		return nil, fmt.Errorf("while loading prior snapshot %q: %w", prevManis[0], err)
	}
	if prior.RootEntry == nil {
		return nil, fmt.Errorf("prior snapshot %q has no root entry", prevManis[0])
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

func parseTags(tags stringList) (map[string]string, error) {
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
