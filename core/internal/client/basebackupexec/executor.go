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

// Package basebackupexec orchestrates a base backup sourced from the
// PostgreSQL BASE_BACKUP streaming-replication protocol. It drives the
// basebackup protocol client, pipes each tar stream into the kopia-pgbasebackup
// ingestion helper, assembles the backup metadata, and persists the
// backup_manifest for future incremental backups.
package basebackupexec

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/cloudnative-pg/machinery/pkg/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/cloudnative-pg/klio/core/internal/client/basebackup"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	"github.com/cloudnative-pg/klio/core/internal/kopia"
)

// Uploader is the subset of the Kopia connection the executor needs. It is
// implemented by *kopia.Connection.
type Uploader interface {
	// GetHostname returns the cluster name from the client certificate.
	GetHostname() string

	// IngestBaseBackup pipes one tar stream into the repository via the helper.
	IngestBaseBackup(ctx context.Context, tar io.Reader, opts kopia.IngestOptions) (*kopia.IngestResult, error)

	// UploadBackupManifest persists the PostgreSQL backup_manifest.
	UploadBackupManifest(ctx context.Context, backupName, pgData string, manifest []byte, pinned bool) error

	// UploadBackupMetadata persists the backup metadata and marks the backup done.
	UploadBackupMetadata(
		ctx context.Context,
		backupName string,
		metadata *klioclient.BackupMetadata,
		pinned bool,
	) error
}

// Executor runs a BASE_BACKUP-sourced backup.
type Executor struct {
	// repConn is a replication connection used for the BASE_BACKUP and
	// UPLOAD_MANIFEST commands.
	repConn *pgconn.PgConn

	// sqlConn is a standard connection used to read metadata that the protocol
	// does not provide (data directory, WAL segment size, tablespace names).
	sqlConn *pgx.Conn

	uploader    Uploader
	clusterName string
}

// New creates a BASE_BACKUP executor.
func New(repConn *pgconn.PgConn, sqlConn *pgx.Conn, uploader Uploader, clusterName string) *Executor {
	return &Executor{
		repConn:     repConn,
		sqlConn:     sqlConn,
		uploader:    uploader,
		clusterName: clusterName,
	}
}

// Options configures a single backup run.
type Options struct {
	// Name is the backup name. A timestamp-based name is generated when empty.
	Name string

	// Pinned marks the produced snapshots with the tier2 pin.
	Pinned bool

	// Incremental requests an incremental backup. Requires PreviousManifest to
	// be set. PostgreSQL 17+. This drives basebackup.Options.Incremental on the
	// protocol layer.
	Incremental bool

	// PreviousManifest is the prior backup's backup_manifest, sent via
	// UPLOAD_MANIFEST before an incremental BASE_BACKUP.
	PreviousManifest []byte

	// PreviousKopiaManifests maps a source path to the prior snapshot's Kopia
	// manifest IDs, passed to the helper so it can combine the incremental
	// against the prior full image. Resolving these from the repository is the
	// caller's responsibility.
	PreviousKopiaManifests map[string][]string
}

// Run executes the backup and returns its assembled metadata.
func (e *Executor) Run(ctx context.Context, opts Options) (*klioclient.BackupMetadata, error) {
	contextLogger := log.FromContext(ctx)

	name := opts.Name
	if name == "" {
		name = time.Now().Format("20060102150405")
	}
	startedAt := time.Now()

	env, err := e.readEnvironment(ctx)
	if err != nil {
		return nil, err
	}

	if opts.Incremental {
		if env.serverVersionNum < pg17VersionNum {
			return nil, fmt.Errorf("%w: server reports version %d",
				ErrIncrementalRequiresPG17, env.serverVersionNum)
		}
		if len(opts.PreviousManifest) == 0 {
			return nil, ErrIncrementalRequiresManifest
		}
		contextLogger.Info("Uploading prior manifest for incremental backup")
		if uErr := basebackup.UploadManifest(ctx, e.repConn, opts.PreviousManifest); uErr != nil {
			return nil, fmt.Errorf("while uploading prior manifest: %w", uErr)
		}
	}

	// kopiaManifests records the Kopia snapshot manifest ID produced for each
	// source. It is persisted in the metadata so a later incremental backup can
	// use it as the combine base.
	kopiaManifests := map[string]string{}
	handler := e.makeHandler(name, env, opts, kopiaManifests)

	contextLogger.Info("Starting BASE_BACKUP", "name", name, "incremental", opts.Incremental)
	result, err := basebackup.Run(ctx, e.repConn, basebackup.Options{
		Label:       name,
		Checkpoint:  basebackup.CheckpointFast,
		Manifest:    true,
		Incremental: opts.Incremental,
	}, handler)
	if err != nil {
		return nil, fmt.Errorf("while running BASE_BACKUP: %w", err)
	}

	metadata := e.buildMetadata(name, startedAt, env, result)
	metadata.KopiaManifests = kopiaManifests
	// BASE_BACKUP ships backup_label, tablespace_map and global/pg_control
	// inside the data tar, so the restored tree is self-contained.
	metadata.SelfContainedPgData = true

	contextLogger.Info("Persisting backup manifest")
	if err := e.uploader.UploadBackupManifest(ctx, name, env.pgData, result.Manifest, opts.Pinned); err != nil {
		return nil, fmt.Errorf("while persisting backup manifest: %w", err)
	}

	contextLogger.Info("Uploading backup metadata")
	if err := e.uploader.UploadBackupMetadata(ctx, name, metadata, opts.Pinned); err != nil {
		return nil, fmt.Errorf("while uploading backup metadata: %w", err)
	}

	return metadata, nil
}

// makeHandler returns the per-source ingest handler. It records each source's
// resulting Kopia manifest ID into kopiaManifests.
func (e *Executor) makeHandler(
	name string,
	env *environment,
	opts Options,
	kopiaManifests map[string]string,
) basebackup.DataSourceHandler {
	var pins []string
	if opts.Pinned {
		pins = []string{klioclient.Tier2Pin}
	}

	return func(ctx context.Context, src basebackup.DataSource, tar io.Reader) error {
		key, sourcePath, tags := sourceDescriptor(src, name, env)

		ingestOpts := kopia.IngestOptions{
			SourcePath:        sourcePath,
			Tags:              tags,
			Pins:              pins,
			Incremental:       opts.Incremental,
			PreviousManifests: opts.PreviousKopiaManifests[key],
		}

		res, err := e.uploader.IngestBaseBackup(ctx, tar, ingestOpts)
		if err != nil {
			return fmt.Errorf("while ingesting source %q: %w", sourcePath, err)
		}

		// basebackup.Run invokes the handler sequentially, one COPY stream at a
		// time, so this map write needs no synchronization.
		kopiaManifests[key] = res.ManifestID

		log.FromContext(ctx).Info("Ingested BASE_BACKUP source",
			"sourceKey", key,
			"sourcePath", sourcePath,
			"manifestId", res.ManifestID,
			"hashedBytes", res.Stats.HashedBytes,
			"cachedBytes", res.Stats.CachedBytes,
		)

		return nil
	}
}

// mainDataSourceKey is the stable combine-base key for the main data directory.
// Unlike the physical data_directory path, it does not change between backups
// (e.g. after a restore-in-place), so a later incremental can always match its
// prior full image.
const mainDataSourceKey = "pgdata"

// sourceDescriptor maps a protocol data source to a stable combine-base key, the
// Kopia snapshot SourceInfo.Path, and the Kopia tags (in that return order). The
// key is what matches a source against the prior backup's combine base and must
// not depend on the physical data_directory path; the snapshot path keeps the
// physical path for the main directory and the synthetic /tablespace/<oid> path
// for tablespaces.
func sourceDescriptor(
	src basebackup.DataSource, name string, env *environment,
) (string, string, map[string]string) {
	if src.IsTablespace {
		key := "/tablespace/" + src.TablespaceOID

		return key, key, map[string]string{
			klioclient.BackupContentTagName:  "tablespace",
			klioclient.TablespaceNameTagName: env.tablespaceNames[src.TablespaceOID],
			klioclient.BackupNameTagName:     name,
		}
	}

	return mainDataSourceKey, env.pgData, map[string]string{
		klioclient.BackupContentTagName: "pgdata",
		klioclient.BackupNameTagName:    name,
	}
}

// buildMetadata assembles BackupMetadata from the protocol result.
//
// backup_label and tablespace_map are not populated here: on the BASE_BACKUP
// path they arrive inside the main data tar and are ingested as ordinary files,
// from which restore reads them (see the design document). The LSN metadata
// comes from the protocol result sets, not from parsing the label.
func (e *Executor) buildMetadata(
	name string,
	startedAt time.Time,
	env *environment,
	result *basebackup.Result,
) *klioclient.BackupMetadata {
	tablespaces := make([]klioclient.TablespaceLayout, 0, len(result.Tablespaces))
	for _, tbl := range result.Tablespaces {
		if tbl.OID == "" {
			// The main data directory entry; not a real tablespace.
			continue
		}
		tablespaces = append(tablespaces, klioclient.TablespaceLayout{
			Oid:  tbl.OID,
			Name: env.tablespaceNames[tbl.OID],
			Path: tbl.Location,
		})
	}

	metadata := &klioclient.BackupMetadata{
		Name:        name,
		ClusterName: e.clusterName,
		StartedAt:   startedAt.Unix(),
		StoppedAt:   time.Now().Unix(),
		StartLSN:    result.StartLSN,
		EndLSN:      result.EndLSN,
		Timeline:    int(result.StartTLI),
		SystemID:    env.systemID,
		SegmentSize: env.segmentSize,
		PgData:      env.pgData,
		Tablespaces: tablespaces,
	}

	// A base backup runs on a single timeline (PostgreSQL aborts it on a
	// timeline switch), so StartTLI is the backup's timeline for both WALs.
	if env.segmentSize > 0 && result.StartTLI > 0 {
		tli := int(result.StartTLI)
		if startWAL, err := types.Int64ToLSN(result.StartLSN).WALFileName(tli, env.segmentSize); err == nil {
			metadata.StartWAL = startWAL
		}
		if endWAL, err := types.Int64ToLSN(result.EndLSN).WALFileName(tli, env.segmentSize); err == nil {
			metadata.EndWAL = endWAL
		}
	}

	return metadata
}
