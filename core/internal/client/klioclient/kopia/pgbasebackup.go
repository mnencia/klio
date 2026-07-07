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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cloudnative-pg/machinery/pkg/log"

	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	"github.com/cloudnative-pg/klio/core/internal/kopia"
)

// ErrPgBaseBackupNotConfigured is returned when an ingest is attempted but the
// kopia-pgbasebackup helper is not available.
var ErrPgBaseBackupNotConfigured = errors.New("kopia-pgbasebackup helper is not configured")

// ErrMissingSnapshotIdentity is returned when the connection has no host or user
// identity. Ingesting under an empty identity would store the snapshot under the
// wrong SourceInfo and make it unfindable on restore.
var ErrMissingSnapshotIdentity = errors.New("kopia connection has no host/username identity for the snapshot")

// SupportsBaseBackupIngestion reports whether the BASE_BACKUP ingestion helper
// is available on this connection.
func (s *Connection) SupportsBaseBackupIngestion() bool {
	return s.pgBaseBackup != nil
}

// IngestBaseBackup pipes a single BASE_BACKUP tar stream into the Kopia
// repository through the kopia-pgbasebackup helper. Host and Username are
// filled from the connection's certificate identity so they always match the
// repository ACLs.
func (s *Connection) IngestBaseBackup(
	ctx context.Context,
	tar io.Reader,
	opts kopia.IngestOptions,
) (*kopia.IngestResult, error) {
	if s.pgBaseBackup == nil {
		return nil, ErrPgBaseBackupNotConfigured
	}

	if s.hostName == "" || s.userName == "" {
		return nil, fmt.Errorf("%w: host=%q username=%q", ErrMissingSnapshotIdentity, s.hostName, s.userName)
	}

	opts.Host = s.hostName
	opts.Username = s.userName

	result, err := s.pgBaseBackup.Ingest(ctx, tar, opts)
	if err != nil {
		return nil, fmt.Errorf("while ingesting BASE_BACKUP source: %w", err)
	}

	return result, nil
}

// GetBackupManifest retrieves the persisted PostgreSQL backup_manifest for the
// named backup from the repository. Like the RestorePgData family, it is scoped
// to the connection's own hostname (see getSnapshotID).
func (s *Connection) GetBackupManifest(
	ctx context.Context,
	backupName string,
) ([]byte, error) {
	entryID, err := s.getSnapshotID(ctx, map[string]string{
		klioclient.BackupNameTagName:    backupName,
		klioclient.BackupContentTagName: "manifest",
	})
	if err != nil {
		if _, ok := errors.AsType[NoSnapshotFoundError](err); ok {
			return nil, newNoBackupFoundError(s.GetHostname(), backupName)
		}

		return nil, err
	}

	content, err := s.kopia.RestoreSingleFile(ctx, entryID, "backup_manifest", log.FromContext(ctx).Debug)
	if err != nil {
		return nil, fmt.Errorf("while restoring backup manifest: %w", err)
	}

	return content, nil
}

// UploadBackupManifest persists the PostgreSQL backup_manifest alongside the
// backup so a future incremental backup can be requested. The manifest is
// stored as a single-file snapshot, mirroring how backup metadata is stored.
func (s *Connection) UploadBackupManifest(
	ctx context.Context,
	backupName, pgData string,
	manifest []byte,
	pinned bool,
) error {
	tags := map[string]string{
		klioclient.BackupNameTagName:    backupName,
		klioclient.BackupContentTagName: "manifest",
	}

	var pins []string
	if pinned {
		pins = []string{klioclient.Tier2Pin}
	}

	opts := kopia.SnapshotFileContentOptions{
		Content:       manifest,
		FileName:      "backup_manifest",
		DirectoryName: strings.TrimSuffix(pgData, "/") + "_manifest",
		Description:   "backup manifest for " + backupName,
		Tags:          tags,
		Pins:          pins,
	}

	if err := s.kopia.SnapshotFileContent(ctx, opts); err != nil {
		return fmt.Errorf("while snapshotting backup manifest: %w", err)
	}

	return nil
}
