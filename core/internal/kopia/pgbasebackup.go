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

package kopia

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"slices"

	"github.com/cloudnative-pg/machinery/pkg/log"
)

// errEmptyManifestID is returned when the helper reports a successful ingest
// without a manifest ID, which would leave the backup unreferenceable.
var errEmptyManifestID = errors.New("kopia-pgbasebackup returned an empty manifest ID")

// PgBaseBackupBinary is the name of the kopia-pgbasebackup helper executable.
//
// The helper is built from the Kopia fork (see core/Dockerfile) and ingests a
// PostgreSQL BASE_BACKUP tar stream into the Kopia repository, keeping the Kopia
// Go library out of the main klio binary. klio invokes it via exec, exactly as
// it invokes the kopia CLI today.
const PgBaseBackupBinary = "kopia-pgbasebackup"

// LookupPgBaseBackupBinary finds the kopia-pgbasebackup helper in the system
// PATH and returns its absolute path. It mirrors LookupBinary for the kopia CLI.
func LookupPgBaseBackupBinary() (string, error) {
	return lookupBinary(PgBaseBackupBinary)
}

// PgBaseBackupClient wraps the kopia-pgbasebackup helper CLI.
type PgBaseBackupClient struct {
	// Binary is the path to the kopia-pgbasebackup executable.
	Binary string

	// ConfigFile is the path to the Kopia configuration file. It is the SAME
	// repository config klio uses for the kopia CLI, so authentication and
	// repository selection are identical.
	ConfigFile string

	// Password is the repository encryption password (e.g. "mtls").
	Password string
}

// IngestOptions are the parameters for a single ingest invocation. One
// invocation ingests one source (the main data directory tar, or one
// non-default tablespace tar). See the helper CLI contract in the design.
type IngestOptions struct {
	// Host is the snapshot SourceInfo.Host. It must match the certificate CN
	// hostname (the cluster name).
	Host string

	// Username is the snapshot SourceInfo.UserName.
	Username string

	// SourcePath is the snapshot SourceInfo.Path, e.g. the PGDATA path or
	// /tablespace/<oid>.
	SourcePath string

	// Tags are the key-value pairs to associate with the snapshot, e.g.
	// content=pgdata|tablespace, backupName=…
	Tags map[string]string

	// Pins are the retention pins to apply (mirrors the current upload path,
	// e.g. the tier2 pin).
	Pins []string

	// PreviousBackupName is the prior backup's name, used to combine an
	// incremental backup into a full image. No Kopia-internal identifier is
	// passed here: the helper resolves its own combine-base snapshot by tag,
	// atomically, immediately before use, so there is nothing for a later
	// pin/unpin (which reassigns manifest IDs) to invalidate.
	PreviousBackupName string

	// Incremental marks the input tar as an incremental backup.
	Incremental bool
}

// IngestStats are the per-source ingestion statistics reported by the helper.
// hashedBytes versus cachedBytes is the signal that deduplication is effective.
//
// The JSON names must match outputStats in
// core/kopia-pgbasebackup/cmd/ingest.go. The helper is a separate Go module, so
// nothing but that contract connects the two: a field missing here is silently
// dropped rather than failing to compile.
type IngestStats struct {
	TotalSize   int64 `json:"totalSize"`
	HashedBytes int64 `json:"hashedBytes"`
	CachedBytes int64 `json:"cachedBytes"`
	FileCount   int64 `json:"fileCount"`

	// ReusedFiles is the number of unchanged files an incremental referenced
	// from the prior object directly, and PriorReadBytes the block bytes read
	// back from the prior image to reconstruct the rest. Together they show how
	// much read-back the incremental reuse fast path avoided.
	ReusedFiles    int64 `json:"reusedFiles"`
	PriorReadBytes int64 `json:"priorReadBytes"`

	// ZeroFilledBlocks is the number of below-truncation blocks present in
	// neither the incremental nor the prior image, materialized as zero-filled
	// uninitialized extension blocks (matching pg_combinebackup). Normally
	// small; a spike may indicate a WAL-summary anomaly worth investigating.
	ZeroFilledBlocks int64 `json:"zeroFilledBlocks"`

	// ReferencedSegments is the number of storage segments an incremental took
	// from the prior backup by object ID, and RebuiltSegments the number it
	// wrote instead. A segment is rebuilt when it holds a changed block, but
	// also when it cannot be referenced at all: the prior backup recorded no
	// segment map, or recorded one at a different segment size, or the file grew
	// past the prior image. So a high RebuiltSegments with little real churn
	// means referencing was disabled, not that the database was busy.
	//
	// RebuiltSegments times the segment size bounds what the backup paid in
	// stored bytes and in read-back; both come in lower, since stored segments
	// are compressed and deduplicated, and only a rebuilt segment's unchanged
	// blocks are read back.
	ReferencedSegments int64 `json:"referencedSegments"`
	RebuiltSegments    int64 `json:"rebuiltSegments"`
}

// IngestResult is the single JSON object the helper prints on stdout when an
// ingest completes successfully.
type IngestResult struct {
	ManifestID   string      `json:"manifestId"`
	RootObjectID string      `json:"rootObjectId"`
	Stats        IngestStats `json:"stats"`
}

// Ingest pipes a single tar stream (one source) to the helper on stdin and
// parses the JSON result printed on stdout. stderr is captured line-by-line via
// the shared RunWithLogCapture pattern (including disk-full detection).
func (c *PgBaseBackupClient) Ingest(
	ctx context.Context,
	tarStream io.Reader,
	opts IngestOptions,
) (*IngestResult, error) {
	contextLogger := log.FromContext(ctx)

	args := []string{
		"ingest",
		"--config-file=" + c.ConfigFile,
		"--host=" + opts.Host,
		"--username=" + opts.Username,
		"--source-path=" + opts.SourcePath,
	}

	// Sort the tag keys so the argument order is deterministic across runs.
	for _, k := range slices.Sorted(maps.Keys(opts.Tags)) {
		args = append(args, fmt.Sprintf("--tag=%s=%s", k, opts.Tags[k]))
	}

	for _, pin := range opts.Pins {
		args = append(args, "--pin="+pin)
	}

	if opts.PreviousBackupName != "" {
		args = append(args, "--previous-backup-name="+opts.PreviousBackupName)
	}

	if opts.Incremental {
		args = append(args, "--incremental")
	}

	var stdout bytes.Buffer

	cmd := exec.CommandContext(ctx, c.Binary, args...) //nolint:gosec
	cmd.Env = c.environmentVariables()
	cmd.Stdin = tarStream

	// Log only non-sensitive, structured fields. The full argv is deliberately
	// not logged: a future credential-bearing flag would otherwise leak into the
	// logs. Tag values (which may carry user-controlled data) are omitted; only
	// counts are reported.
	contextLogger.Info("Ingesting BASE_BACKUP source into Kopia",
		"sourcePath", opts.SourcePath,
		"incremental", opts.Incremental,
		"tagCount", len(opts.Tags),
		"pinCount", len(opts.Pins),
		"previousBackupName", opts.PreviousBackupName)

	if err := RunWithLogCapture(ctx, cmd, &stdout); err != nil {
		return nil, fmt.Errorf("while ingesting BASE_BACKUP source %q: %w", opts.SourcePath, err)
	}

	result, err := parseIngestResult(stdout.Bytes())
	if err != nil {
		return nil, fmt.Errorf("while parsing kopia-pgbasebackup output for source %q: %w", opts.SourcePath, err)
	}

	contextLogger.Info("Ingested BASE_BACKUP source into Kopia",
		"sourcePath", opts.SourcePath,
		"incremental", opts.Incremental,
		"totalSize", result.Stats.TotalSize,
		"hashedBytes", result.Stats.HashedBytes,
		"cachedBytes", result.Stats.CachedBytes,
		"fileCount", result.Stats.FileCount,
		"reusedFiles", result.Stats.ReusedFiles,
		"priorReadBytes", result.Stats.PriorReadBytes,
		"zeroFilledBlocks", result.Stats.ZeroFilledBlocks,
		"referencedSegments", result.Stats.ReferencedSegments,
		"rebuiltSegments", result.Stats.RebuiltSegments)

	return result, nil
}

func (c *PgBaseBackupClient) environmentVariables() []string {
	return kopiaEnvironment(c.Password)
}

// parseIngestResult decodes the single JSON object printed by the helper.
func parseIngestResult(stdout []byte) (*IngestResult, error) {
	var result IngestResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout), &result); err != nil {
		return nil, fmt.Errorf("invalid ingest result JSON: %w", err)
	}

	if result.ManifestID == "" {
		return nil, errEmptyManifestID
	}

	return &result, nil
}
