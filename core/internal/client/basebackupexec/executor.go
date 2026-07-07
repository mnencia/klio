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

// BackupMethodAnnotationName is the metadata annotation recording whether the
// backup was assembled as an incremental (BASE_BACKUP INCREMENTAL) or as a full
// backup. It is surfaced on the Backup resource as .status.pluginMetadata, so
// callers can distinguish an incremental backup from a full one (including one
// that fell back from an incremental request) without reading logs.
const BackupMethodAnnotationName = "klio.io/backup-method"

const (
	// BackupMethodIncremental is the BackupMethodAnnotationName value for an
	// incremental backup.
	BackupMethodIncremental = "incremental"

	// BackupMethodFull is the BackupMethodAnnotationName value for a full backup.
	BackupMethodFull = "full"
)

// Uploader is the subset of the Kopia connection the executor needs. It is
// implemented by *kopia.Connection.
type Uploader interface {
	// GetHostname returns the cluster name from the client certificate.
	GetHostname() string

	// IngestBaseBackup pipes one tar stream into the repository via the helper.
	IngestBaseBackup(ctx context.Context, tar io.Reader, opts kopia.IngestOptions) (*kopia.IngestResult, error)

	// UploadBackupManifest persists the PostgreSQL backup_manifest.
	UploadBackupManifest(ctx context.Context, backupName, pgData string, manifest []byte) error

	// UploadBackupMetadata persists the backup metadata and marks the backup done.
	UploadBackupMetadata(
		ctx context.Context,
		backupName string,
		metadata *klioclient.BackupMetadata,
	) error
}

// ProtocolUploader is the subset of the Kopia connection the BASE_BACKUP
// protocol CLI orchestration needs: everything Uploader needs, plus checking
// helper availability and reading a prior backup's manifest. It is
// implemented by *kopia.Connection.
type ProtocolUploader interface {
	Uploader

	// SupportsBaseBackupIngestion reports whether the kopia-pgbasebackup
	// helper is available on this connection.
	SupportsBaseBackupIngestion() bool

	// GetBackupManifest retrieves a prior backup's persisted PostgreSQL
	// backup_manifest, used as the UPLOAD_MANIFEST base for an incremental.
	GetBackupManifest(ctx context.Context, backupName string) ([]byte, error)

	// ResolveManifestID confirms a snapshot matching tags currently exists,
	// returning its Kopia manifest ID. Used only as a go/no-go existence
	// check (see priorbackup.go's verifyPriorPgdataExists) — the returned ID
	// is never stored or used as a combine base, since it is not stable
	// across pin/unpin.
	ResolveManifestID(ctx context.Context, tags map[string]string) (string, error)
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

	// Incremental requests an incremental backup. Requires PreviousManifest to
	// be set. PostgreSQL 17+. This drives basebackup.Options.Incremental on the
	// protocol layer.
	Incremental bool

	// PreviousManifest is the prior backup's backup_manifest, sent via
	// UPLOAD_MANIFEST before an incremental BASE_BACKUP.
	PreviousManifest []byte

	// PreviousBackup is the prior backup being combined against. Each
	// source's combine base is identified to the kopia-pgbasebackup helper
	// by this backup's name alone (see makeHandler) — no Kopia-internal
	// identifier is resolved or stored here; the helper resolves its own
	// combine-base snapshot by tag, atomically, immediately before use.
	PreviousBackup *klioclient.BackupMetadata
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
		if err := validateIncrementalPreconditions(env, opts); err != nil {
			return nil, err
		}
		contextLogger.Info("Uploading prior manifest for incremental backup")
		if uErr := basebackup.UploadManifest(ctx, e.repConn, opts.PreviousManifest); uErr != nil {
			return nil, fmt.Errorf("while uploading prior manifest: %w", uErr)
		}
	}

	handler := e.makeHandler(name, env, opts)

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

	metadata, err := e.buildMetadata(name, startedAt, env, result, opts.Incremental)
	if err != nil {
		return nil, err
	}
	// BASE_BACKUP ships backup_label, tablespace_map and global/pg_control
	// inside the data tar, so the restored tree is self-contained.
	metadata.SelfContainedPgData = true

	contextLogger.Info("Persisting backup manifest")
	if err := e.uploader.UploadBackupManifest(ctx, name, env.pgData, result.Manifest); err != nil {
		return nil, fmt.Errorf("while persisting backup manifest: %w", err)
	}

	contextLogger.Info("Uploading backup metadata")
	if err := e.uploader.UploadBackupMetadata(ctx, name, metadata); err != nil {
		return nil, fmt.Errorf("while uploading backup metadata: %w", err)
	}

	return metadata, nil
}

// makeHandler returns the per-source ingest handler.
func (e *Executor) makeHandler(
	name string,
	env *environment,
	opts Options,
) basebackup.DataSourceHandler {
	return func(ctx context.Context, src basebackup.DataSource, tar io.Reader) error {
		key, sourcePath, tags, err := sourceDescriptor(src, name, env)
		if err != nil {
			return err
		}

		// Incremental is decided per source, not per backup. A source that has no
		// prior combine base (for example a tablespace created after the previous
		// backup) is ingested as a full source even during an incremental backup:
		// PostgreSQL streams its files in full, and passing --incremental with no
		// --previous-backup-name would fail the helper.
		previousBackupName := ""
		if opts.Incremental && opts.PreviousBackup != nil && sourceExistedInPrior(key, opts.PreviousBackup) {
			previousBackupName = opts.PreviousBackup.Name
		}
		srcIncremental := opts.Incremental && previousBackupName != ""

		ingestOpts := kopia.IngestOptions{
			SourcePath:         sourcePath,
			Tags:               tags,
			Incremental:        srcIncremental,
			PreviousBackupName: previousBackupName,
		}

		res, err := e.uploader.IngestBaseBackup(ctx, tar, ingestOpts)
		if err != nil {
			return fmt.Errorf("while ingesting source %q: %w", sourcePath, err)
		}

		log.FromContext(ctx).Info("Ingested BASE_BACKUP source",
			"sourceKey", key,
			"sourcePath", sourcePath,
			"hashedBytes", res.Stats.HashedBytes,
			"cachedBytes", res.Stats.CachedBytes,
		)

		return nil
	}
}

// sourceExistedInPrior reports whether prior had a source matching key:
// pgdata always did, for any completed backup; a tablespace only if its OID
// appears in prior.Tablespaces. Used to decide whether a source has a
// combine base at all, without needing any Kopia-internal identifier. A
// tablespace created after prior was taken correctly reports false, so the
// source is ingested as full.
func sourceExistedInPrior(key string, prior *klioclient.BackupMetadata) bool {
	if key == mainDataSourceKey {
		return true
	}
	for _, tbl := range prior.Tablespaces {
		if tablespaceSourceKey(tbl.Oid) == key {
			return true
		}
	}

	return false
}

// mainDataSourceKey is the stable combine-base key for the main data directory.
// Unlike the physical data_directory path, it does not change between backups
// (e.g. after a restore-in-place), so a later incremental can always match its
// prior full image.
const mainDataSourceKey = "pgdata"

// tablespaceSourceKey is the stable combine-base key (and Kopia SourceInfo.Path)
// for a tablespace. Keyed on the OID, not the name or physical mount, so a
// later incremental still matches its prior full image across a rename or a
// relocation.
func tablespaceSourceKey(oid string) string {
	return "/tablespace/" + oid
}

// sourceDescriptor maps a protocol data source to a stable combine-base key, the
// Kopia snapshot SourceInfo.Path, and the Kopia tags (in that return order). The
// key is what matches a source against the prior backup's combine base and must
// not depend on the physical data_directory path; the snapshot path keeps the
// physical path for the main directory and the synthetic /tablespace/<oid> path
// for tablespaces.
func sourceDescriptor(
	src basebackup.DataSource, name string, env *environment,
) (string, string, map[string]string, error) {
	if src.IsTablespace {
		tsName, err := env.tablespaceName(src.TablespaceOID)
		if err != nil {
			return "", "", nil, err
		}

		key := tablespaceSourceKey(src.TablespaceOID)

		return key, key, map[string]string{
			klioclient.BackupContentTagName:  "tablespace",
			klioclient.TablespaceNameTagName: tsName,
			klioclient.BackupNameTagName:     name,
		}, nil
	}

	return mainDataSourceKey, env.pgData, map[string]string{
		klioclient.BackupContentTagName: "pgdata",
		klioclient.BackupNameTagName:    name,
	}, nil
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
	incremental bool,
) (*klioclient.BackupMetadata, error) {
	tablespaces := make([]klioclient.TablespaceLayout, 0, len(result.Tablespaces))
	for _, tbl := range result.Tablespaces {
		if tbl.OID == "" {
			// The main data directory entry; not a real tablespace.
			continue
		}
		tsName, err := env.tablespaceName(tbl.OID)
		if err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, klioclient.TablespaceLayout{
			Oid:  tbl.OID,
			Name: tsName,
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

	// Record whether this backup was assembled incrementally, so the Backup
	// resource reflects it. An incremental request that fell back to a full
	// backup is recorded as full, since incremental is cleared upstream.
	method := BackupMethodFull
	if incremental {
		method = BackupMethodIncremental
	}
	metadata.SetAnnotation(BackupMethodAnnotationName, method)

	return metadata, nil
}
