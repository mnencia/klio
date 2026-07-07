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

package basebackupexec

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/cloudnative-pg/machinery/pkg/types"
	"github.com/jackc/pgx/v5"

	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	kopiaconn "github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
)

var (
	// ErrPreviousBackupNotFound is returned when a previous backup selected by
	// name does not exist for the cluster.
	ErrPreviousBackupNotFound = errors.New("the requested previous backup does not exist")

	// ErrPreviousBackupUnusable is returned when an explicitly requested previous
	// backup cannot serve as an incremental base.
	ErrPreviousBackupUnusable = errors.New("the requested previous backup cannot serve as an incremental base")

	// ErrSummarizeWALDisabled is returned when an incremental backup is
	// requested but summarize_wal is off on the source cluster. Incremental is
	// structurally impossible here regardless of which prior backup would
	// otherwise be chosen, so this fails unconditionally instead of falling
	// back to a full backup.
	ErrSummarizeWALDisabled = errors.New("summarize_wal is disabled on the source cluster; incremental backups require it")
)

// SelectPriorBackup chooses the backup to use as the incremental base: the one
// named by previousBackupName (an ErrPreviousBackupNotFound error when it does
// not exist), or the most recent backup otherwise. A nil result with a nil error
// means auto-detection found no backup, so the caller should fall back to a full
// backup.
func SelectPriorBackup(
	backups klioclient.BackupList, previousBackupName string,
) (*klioclient.BackupMetadata, error) {
	if previousBackupName == "" {
		return backups.GetLatestBackup(), nil
	}

	prior := findBackupByName(backups, previousBackupName)
	if prior == nil {
		return nil, fmt.Errorf("%w: %q", ErrPreviousBackupNotFound, previousBackupName)
	}

	return prior, nil
}

// ValidatePriorForIncremental reports whether prior can seed an incremental
// backup of a cluster with the given system identifier. A prior from a different
// cluster lineage cannot serve as a combine base. Whether the prior is a valid
// ancestor on the current timeline history is left to PostgreSQL, which
// validates it when the incremental BASE_BACKUP runs. The returned reason is
// suitable for a log message.
func ValidatePriorForIncremental(prior *klioclient.BackupMetadata, systemID int64) (bool, string) {
	if systemID != 0 && prior.SystemID != systemID {
		return false, fmt.Sprintf(
			"previous backup belongs to a different cluster (system identifier %d, current %d)",
			prior.SystemID, systemID)
	}

	return true, ""
}

// findBackupByName returns the backup with the given name, or nil when none
// matches.
func findBackupByName(backups klioclient.BackupList, name string) *klioclient.BackupMetadata {
	for i := range backups {
		if backups[i].Name == name {
			return &backups[i]
		}
	}

	return nil
}

// BackupLister is the subset of the Kopia connection needed to list a
// cluster's backups, used to auto-detect or validate the incremental base.
// It is implemented by *kopia.MultiConnection.
type BackupLister interface {
	ListBackups(ctx context.Context, hostname string) (klioclient.BackupList, error)
}

// ResolvePriorForIncremental tries to set up an incremental backup. It
// selects a single prior backup, reads its stored PostgreSQL backup_manifest
// (for UPLOAD_MANIFEST), and confirms its pgdata Kopia snapshot still exists,
// setting the prior on executorOpts so the executor can identify each
// source's combine base to kopia-pgbasebackup by backup name alone (see
// executor.go's makeHandler — no Kopia-internal identifier is resolved or
// stored here). It returns true when incremental is possible.
//
// The prior is chosen by previousBackupName when set, otherwise as the most
// recent backup for the cluster. The distinction also governs what happens
// when the chosen backup turns out to be unusable: an explicitly named
// backup is a hard error (the user's request cannot be honored), while the
// auto-detected one falls back to a full backup with a warning.
func ResolvePriorForIncremental(
	ctx context.Context,
	lister BackupLister,
	tier1 ProtocolUploader,
	sqlConn *pgx.Conn,
	hostname string,
	previousBackupName string,
	executorOpts *Options,
) (bool, error) {
	contextLogger := log.FromContext(ctx)

	if err := requireSummarizeWALEnabled(ctx, sqlConn); err != nil {
		return false, err
	}

	backups, err := lister.ListBackups(ctx, hostname)
	if err != nil {
		return false, fmt.Errorf("while listing backups: %w", err)
	}

	explicit := previousBackupName != ""
	prior, err := SelectPriorBackup(backups, previousBackupName)
	if err != nil {
		return false, err
	}
	if prior == nil {
		contextLogger.Warning(
			"Incremental backup requested but no previous backup exists; falling back to a full backup")

		return false, nil
	}

	// unusable turns a reason into the right outcome: a hard error when the
	// backup was named explicitly, or a warning plus a full-backup fallback when
	// it was auto-detected.
	unusable := func(reason string) (bool, error) {
		return priorUnusable(explicit, prior, reason, contextLogger)
	}

	if reason := checkPriorLineage(ctx, sqlConn, prior, contextLogger); reason != "" {
		return unusable(reason)
	}

	if reason := checkPriorWALSummaries(ctx, sqlConn, prior, contextLogger); reason != "" {
		return unusable(reason)
	}

	manifest, reason, err := resolveCombineBase(ctx, tier1, prior)
	if err != nil {
		return false, err
	}
	if reason != "" {
		return unusable(reason)
	}

	executorOpts.PreviousManifest = manifest
	executorOpts.PreviousBackup = prior

	return true, nil
}

// resolveCombineBase reads prior's stored PostgreSQL backup_manifest and
// confirms its pgdata Kopia snapshot still exists. loadPriorManifest also
// serves as the "was this produced by the BASE_BACKUP protocol path" gate:
// only that path ever calls UploadBackupManifest, so a prior from the
// filesystem-snapshot path has no stored backup_manifest and is rejected
// here. A non-empty reason means prior is unusable; a nil error with an
// empty reason means manifest is ready to use.
func resolveCombineBase(
	ctx context.Context, tier1 ProtocolUploader, prior *klioclient.BackupMetadata,
) ([]byte, string, error) {
	manifest, reason, err := loadPriorManifest(ctx, tier1, prior)
	if err != nil || reason != "" {
		return nil, reason, err
	}

	reason, err = verifyPriorPgdataExists(ctx, tier1, prior)
	if err != nil || reason != "" {
		return nil, reason, err
	}

	return manifest, "", nil
}

// priorUnusable maps an "unusable prior" reason to the right outcome: a hard
// error when the prior was named explicitly (the user's request cannot be
// honored), or a warning plus a full-backup fallback when it was
// auto-detected.
func priorUnusable(
	explicit bool, prior *klioclient.BackupMetadata, reason string, logger log.Logger,
) (bool, error) {
	if explicit {
		return false, fmt.Errorf("%w: %q: %s", ErrPreviousBackupUnusable, prior.Name, reason)
	}
	logger.Warning(
		"Most recent backup cannot serve as an incremental base; falling back to a full backup",
		"reason", reason, "previousBackup", prior.Name)

	return false, nil
}

// requireSummarizeWALEnabled hard-fails when summarize_wal is off: the source
// server can never produce the WAL summaries an incremental BASE_BACKUP needs,
// regardless of which prior backup would otherwise be chosen. A failure to
// read the setting is best effort: it logs and returns nil so the backup
// falls through to the existing summary-availability check and ultimately to
// PostgreSQL's own gate.
func requireSummarizeWALEnabled(ctx context.Context, sqlConn *pgx.Conn) error {
	var enabled bool
	if err := sqlConn.QueryRow(
		ctx, "SELECT current_setting('summarize_wal')::boolean",
	).Scan(&enabled); err != nil {
		log.FromContext(ctx).Warning(
			"Could not verify summarize_wal on the source cluster; proceeding", "err", err)

		return nil
	}
	if !enabled {
		return ErrSummarizeWALDisabled
	}

	return nil
}

// checkPriorLineage returns a non-empty "unusable" reason when prior belongs to
// a different cluster lineage than the source server: the repository can still
// hold backups from a destroyed-and-recreated cluster that reused this hostname,
// and combining against one would be meaningless (PostgreSQL's UPLOAD_MANIFEST
// would reject it too). Ancestry on the current timeline history is left to
// PostgreSQL's WAL summarizer during the incremental BASE_BACKUP. A failure to
// read the identifier is best effort: it logs and returns "" so the backup falls
// through to PostgreSQL's own checks.
func checkPriorLineage(
	ctx context.Context, sqlConn *pgx.Conn, prior *klioclient.BackupMetadata, logger log.Logger,
) string {
	systemID, err := readSystemIdentifier(ctx, sqlConn)
	if err != nil {
		logger.Warning(
			"Could not read the cluster system identifier to validate the incremental base; proceeding",
			"err", err, "previousBackup", prior.Name)

		return ""
	}

	_, reason := ValidatePriorForIncremental(prior, systemID)

	return reason
}

// checkPriorWALSummaries returns a non-empty "unusable" reason when the source
// server no longer retains the WAL summaries needed to compute an incremental
// against prior. PostgreSQL prunes them after wal_summary_keep_time, after which
// it would fail the BASE_BACKUP INCREMENTAL; detecting it here lets an
// auto-detected prior degrade to a full backup with a clear reason. The query is
// best effort: a failure (for example on PostgreSQL < 17, where the function does
// not exist) logs and returns "", leaving the definitive gate to the executor.
func checkPriorWALSummaries(
	ctx context.Context, sqlConn *pgx.Conn, prior *klioclient.BackupMetadata, logger log.Logger,
) string {
	retained, err := priorWALSummariesRetained(ctx, sqlConn, prior)
	if err != nil {
		logger.Warning(
			"Could not verify WAL summary availability for the incremental base; proceeding",
			"err", err, "previousBackup", prior.Name)

		return ""
	}
	if !retained {
		return "WAL summaries covering the previous backup are not available " +
			"(check summarize_wal and wal_summary_keep_time on the source cluster)"
	}

	return ""
}

// verifyPriorPgdataExists confirms prior's pgdata Kopia snapshot still
// exists, as a go/no-go signal for attempting incremental at all: if
// pgdata's own combine base is already gone, nothing meaningful would be
// incremental anyway. This is a live, tag-based existence check (see
// kopia.Connection.getSnapshotID) — the manifest ID it resolves is
// discarded, never used as a combine base, since it is not stable across
// pin/unpin (the actual combine base is resolved by kopia-pgbasebackup
// itself, atomically, by backup name, immediately before use — see
// executor.go's makeHandler). A non-empty reason means prior is unusable; a
// nil error with an empty reason means pgdata's snapshot is confirmed
// present.
func verifyPriorPgdataExists(
	ctx context.Context, tier1 ProtocolUploader, prior *klioclient.BackupMetadata,
) (string, error) {
	_, err := tier1.ResolveManifestID(ctx, map[string]string{
		klioclient.BackupContentTagName: "pgdata",
		klioclient.BackupNameTagName:    prior.Name,
	})
	if err != nil {
		if _, ok := errors.AsType[kopiaconn.NoSnapshotFoundError](err); ok {
			return "prior Kopia snapshot for pgdata no longer exists in the repository", nil
		}

		return "", fmt.Errorf("while verifying prior pgdata snapshot: %w", err)
	}

	return "", nil
}

// loadPriorManifest reads the stored backup_manifest for prior. A missing
// manifest yields a non-empty "unusable" reason; a genuine repository failure is
// returned as the error.
func loadPriorManifest(
	ctx context.Context, tier1 ProtocolUploader, prior *klioclient.BackupMetadata,
) ([]byte, string, error) {
	manifest, err := tier1.GetBackupManifest(ctx, prior.Name)
	if err != nil {
		if _, ok := errors.AsType[kopiaconn.NoBackupFoundError](err); ok {
			return nil, "no stored backup_manifest", nil
		}

		return nil, "", fmt.Errorf("while reading the previous backup manifest from the repository: %w", err)
	}

	return manifest, "", nil
}

// priorWALSummariesRetained reports whether the source server still has a WAL
// summary covering the prior backup's start LSN on its timeline. PostgreSQL
// needs an unbroken summary chain from that LSN to compute an incremental, and
// prunes the oldest summaries after wal_summary_keep_time. Checking the old
// boundary is race-free against summarizer lag, which only affects the newest
// end of the range.
func priorWALSummariesRetained(ctx context.Context, sqlConn *pgx.Conn, prior *klioclient.BackupMetadata) (bool, error) {
	startLSN := string(types.Int64ToLSN(prior.StartLSN))

	var retained bool
	if err := sqlConn.QueryRow(
		ctx,
		`SELECT count(*) > 0 FROM pg_available_wal_summaries() `+
			`WHERE tli = $1 AND start_lsn <= $2::pg_lsn AND end_lsn >= $2::pg_lsn`,
		prior.Timeline, startLSN,
	).Scan(&retained); err != nil {
		return false, fmt.Errorf("while checking WAL summary availability: %w", err)
	}

	return retained, nil
}

// readSystemIdentifier reads the current cluster's system identifier, used to
// reject an incremental base from a different cluster lineage.
func readSystemIdentifier(ctx context.Context, sqlConn *pgx.Conn) (int64, error) {
	var systemID int64
	if err := sqlConn.QueryRow(
		ctx, "SELECT system_identifier FROM pg_control_system()",
	).Scan(&systemID); err != nil {
		return 0, fmt.Errorf("while reading system identifier: %w", err)
	}

	return systemID, nil
}
