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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cloudnative-pg/klio/core/internal/backupfailure"
	"github.com/cloudnative-pg/klio/core/internal/cli"
	"github.com/cloudnative-pg/klio/core/internal/client/basebackupexec"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient/grpcclient"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
	"github.com/cloudnative-pg/klio/core/internal/grpc"
	kopiaWrapper "github.com/cloudnative-pg/klio/core/internal/kopia"
	"github.com/cloudnative-pg/klio/core/pkg/config"
)

var (
	// ErrProtocolRequiresTier1 is returned when the BASE_BACKUP protocol path is
	// requested but no writable tier1 connection is available.
	ErrProtocolRequiresTier1 = errors.New("the BASE_BACKUP protocol path requires a tier1 connection")

	// ErrHelperNotAvailable is returned when the kopia-pgbasebackup helper is not
	// installed but the BASE_BACKUP protocol path was requested.
	ErrHelperNotAvailable = errors.New("the kopia-pgbasebackup helper is not available")

	// ErrIncrementalRequiresProtocol is returned when an incremental backup (or an
	// explicit previous backup) is requested without the BASE_BACKUP protocol
	// path, which is the only path able to produce an incremental backup.
	ErrIncrementalRequiresProtocol = errors.New(
		"incremental backup requires the BASE_BACKUP protocol path (--use-base-backup-protocol)")
)

// runCmd represents the `backup run` command
//
//nolint:gochecknoglobals
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Backup the PostgreSQL cluster to the opened Klio server",
	RunE:  cli.RunEWithExitCode(runBackup),
}

//nolint:cyclop
func runBackup(cmd *cobra.Command, _ []string) error {
	var configuration config.Data

	contextLogger := log.FromContext(cmd.Context())

	// IMPORTANT: this requires this program to be built with "-tags viper_bind_struct"
	// when using environment variables
	if err := viper.Unmarshal(&configuration); err != nil {
		return fmt.Errorf("could not unmarshal configuration: %w", err)
	}

	// Sets the default values, to be overridden by the user configuration
	configuration.SetDefaults()

	if configuration.Client == (config.ClientConfig{}) {
		return cli.ErrClientSectionIsRequired
	}
	if configuration.Client.Base == (config.BaseRepositoryClientConfig{}) {
		return cli.ErrKopiaClientSectionIsRequired
	}
	if configuration.Client.Wal == (config.WalRepositoryClientConfig{}) {
		return cli.ErrKlioClientSectionIsRequired
	}
	if configuration.Source == (config.SourceConfig{}) {
		return cli.ErrSourceSectionIsRequired
	}

	waitWALs, _ := cmd.Flags().GetBool("wait-for-wals")
	tier2, _ := cmd.Flags().GetBool("enable-tier2-backup")
	useProtocol, _ := cmd.Flags().GetBool("use-base-backup-protocol")
	incremental, _ := cmd.Flags().GetBool("incremental")
	previousBackupName, _ := cmd.Flags().GetString("previous-backup-name")
	backupName, _ := cmd.Flags().GetString("name")

	// When the CLI flags are not explicitly set, fall back to the values
	// configured by the operator in the Klio configuration. When neither is
	// set, klio keeps using the filesystem-snapshot path (the historical
	// default), so existing deployments are unaffected.
	useProtocol = resolveBackupOption(
		cmd.Flags().Changed("use-base-backup-protocol"), useProtocol, configuration.Backup.UseBaseBackupProtocol)
	incremental = resolveBackupOption(
		cmd.Flags().Changed("incremental"), incremental, configuration.Backup.Incremental)

	// Incremental backups (and an explicitly named prior backup) only exist on
	// the BASE_BACKUP protocol path. Reject the combination loudly instead of
	// silently taking a full filesystem-snapshot backup.
	if err := validateIncrementalRequiresProtocol(incremental, previousBackupName, useProtocol); err != nil {
		return err
	}

	if err := configuration.Validate(); err != nil {
		return fmt.Errorf("configuration validation error: %w", err)
	}

	kopiaClient, err := kopia.MultiConnect(
		cmd.Context(),
		&configuration.Client,
	)
	if err != nil {
		return cli.NewCodedError(
			fmt.Errorf("while connecting to the Klio server: %w %q", err, configuration.Client.Base.URL),
			backupfailure.RepositoryError.ExitCode)
	}
	defer kopiaClient.Close(cmd.Context())

	// Overrides the repository's global policy for this cluster's source.
	if err := setTier1CompressionPolicy(cmd.Context(), kopiaClient, &configuration); err != nil {
		return cli.NewCodedError(
			fmt.Errorf("while setting the tier1 compression policy: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	var metadata *klioclient.BackupMetadata
	if useProtocol {
		metadata, err = runViaBaseBackupProtocol(cmd.Context(), &configuration, kopiaClient, baseBackupProtocolOptions{
			name:               backupName,
			pinned:             tier2,
			incremental:        incremental,
			previousBackupName: previousBackupName,
		})
	} else {
		metadata, err = runViaFilesystem(cmd.Context(), &configuration, kopiaClient, backupName, tier2)
	}
	if err != nil {
		return err
	}

	grpcClient, err := grpcclient.Connect(&configuration.Client, configuration.Client.Wal.Address)
	if err != nil {
		return cli.NewCodedError(
			fmt.Errorf("while connecting to the Klio server: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	for {
		//nolint:gosec // postgres timeline is uint32 in practice, fits int32
		timeline := int32(metadata.Timeline)

		result, err := grpcClient.CloseBackup(cmd.Context(), &grpc.CloseBackupRequest{
			ClusterName:            kopiaClient.GetHostname(),
			BackupName:             metadata.Name,
			Timeline:               timeline,
			StartWal:               metadata.StartWAL,
			EndWal:                 metadata.EndWAL,
			SegmentSize:            metadata.SegmentSize,
			SendToTier2:            tier2,
			Tier2RetentionPolicy:   marshalTier2RetentionPolicy(cmd.Context(), &configuration),
			Tier2CompressionPolicy: marshalTier2CompressionPolicy(cmd.Context(), &configuration),
		})
		if err != nil {
			return cli.NewCodedError(
				fmt.Errorf("while closing the backup: %w", err),
				backupfailure.RepositoryError.ExitCode)
		}

		if waitWALs && len(result.GetMissingWalFiles()) > 0 {
			contextLogger.Info(
				"Detected missing WAL files, waiting for 5 seconds",
				"missingWALFiles", result.GetMissingWalFiles(),
			)
			time.Sleep(5 * time.Second)

			continue
		}

		if result.GetTier2Schedule() {
			contextLogger.Info("Backup completed, triggered synchronization to tier2 if available")
		}

		break
	}

	// Marshal metadata to JSON
	if err := json.NewEncoder(os.Stdout).Encode(metadata); err != nil {
		return fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	return nil
}

// setTier1CompressionPolicy applies the per-cluster tier1 compression policy
// to the cluster's source in the tier1 repository. It is always applied, even
// when unconfigured, so that removing the compression section resets the
// source back to inheriting the repository's global policy instead of
// leaving a stale, previously-set override in place.
func setTier1CompressionPolicy(
	ctx context.Context,
	client *kopia.MultiConnection,
	configuration *config.Data,
) error {
	policy := toKopiaCompressionPolicy(configuration.Tier1CompressionPolicy)

	target := kopiaWrapper.Target{
		Username: client.GetUsername(),
		Hostname: client.GetHostname(),
	}

	return client.SetCompressionPolicy(ctx, target, policy)
}

// toKopiaCompressionPolicy converts a config compression policy into the Kopia
// wrapper representation. A nil input yields the zero policy.
func toKopiaCompressionPolicy(p *config.CompressionPolicy) kopiaWrapper.CompressionPolicy {
	if p == nil {
		return kopiaWrapper.CompressionPolicy{}
	}

	return kopiaWrapper.CompressionPolicy{
		Algorithm: p.Algorithm,
		MinSize:   p.MinSize,
		MaxSize:   p.MaxSize,
	}
}

// marshalTier2RetentionPolicy serializes the tier2 retention policy to the
// JSON representation expected by the WAL server. It returns an empty string
// when no policy is configured or serialization fails.
func marshalTier2RetentionPolicy(ctx context.Context, configuration *config.Data) string {
	if configuration.Tier2RetentionPolicy == nil {
		return ""
	}

	policy := kopiaWrapper.RetentionPolicy{
		KeepLatest:  configuration.Tier2RetentionPolicy.KeepLatest,
		KeepHourly:  configuration.Tier2RetentionPolicy.KeepHourly,
		KeepDaily:   configuration.Tier2RetentionPolicy.KeepDaily,
		KeepWeekly:  configuration.Tier2RetentionPolicy.KeepWeekly,
		KeepMonthly: configuration.Tier2RetentionPolicy.KeepMonthly,
		KeepAnnual:  configuration.Tier2RetentionPolicy.KeepAnnual,
	}

	content, err := json.Marshal(policy)
	if err != nil {
		log.FromContext(ctx).Error(err, "Error while serializing the tier2 retention policy, skipping")

		return ""
	}

	return string(content)
}

// marshalTier2CompressionPolicy serializes the tier2 compression policy to the
// JSON representation expected by the WAL server. It is always serialized,
// even when unconfigured, so that removing the compression section resets the
// cluster's tier2 source back to inheriting the repository's global policy
// instead of leaving a stale, previously-set override in place. It returns an
// empty string only when serialization fails.
func marshalTier2CompressionPolicy(ctx context.Context, configuration *config.Data) string {
	policy := toKopiaCompressionPolicy(configuration.Tier2CompressionPolicy)

	content, err := json.Marshal(policy)
	if err != nil {
		log.FromContext(ctx).Error(err, "Error while serializing the tier2 compression policy, skipping")

		return ""
	}

	return string(content)
}

// resolveBackupOption decides the effective value of a backup option. An
// explicitly-set CLI flag overrides the operator-supplied configuration
// default; otherwise the configuration value is used. With neither set the
// configuration default (false) wins, preserving the historical behavior.
func resolveBackupOption(flagChanged, flagValue, configValue bool) bool {
	if flagChanged {
		return flagValue
	}

	return configValue
}

// validateIncrementalRequiresProtocol rejects requesting an incremental backup
// (or an explicit previous backup) without the BASE_BACKUP protocol path, which
// is the only path able to produce one. Without this guard the request would be
// silently downgraded to a full filesystem-snapshot backup.
func validateIncrementalRequiresProtocol(incremental bool, previousBackupName string, useProtocol bool) error {
	if (incremental || previousBackupName != "") && !useProtocol {
		return ErrIncrementalRequiresProtocol
	}

	return nil
}

// runViaFilesystem performs the local filesystem-snapshot backup (the existing
// path) using a directly-mounted PGDATA.
func runViaFilesystem(
	ctx context.Context,
	configuration *config.Data,
	kopiaClient *kopia.MultiConnection,
	backupName string,
	tier2 bool,
) (*klioclient.BackupMetadata, error) {
	conn, err := pgx.Connect(ctx, configuration.Source.StandardDSN)
	if err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while connecting to PostgreSQL: %w", err),
			backupfailure.SourceError.ExitCode)
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	backupExecutor := klioclient.NewBackupExecutor(conn, kopiaClient, kopiaClient.GetHostname())

	if err := backupExecutor.Start(ctx, klioclient.BackupOptions{Name: backupName}); err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while starting the backup: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	if err := backupExecutor.Upload(ctx, tier2); err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while uploading data: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	metadata, err := backupExecutor.Close(ctx, tier2)
	if err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while closing the backup: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	return metadata, nil
}

// baseBackupProtocolOptions configure the BASE_BACKUP protocol path.
type baseBackupProtocolOptions struct {
	name               string
	pinned             bool
	incremental        bool
	previousBackupName string
}

// runViaBaseBackupProtocol performs a backup sourced from the PostgreSQL
// BASE_BACKUP streaming-replication protocol, ingesting each tar stream into
// Kopia through the kopia-pgbasebackup helper. It does not require PGDATA to be
// mounted locally.
func runViaBaseBackupProtocol(
	ctx context.Context,
	configuration *config.Data,
	kopiaClient *kopia.MultiConnection,
	opts baseBackupProtocolOptions,
) (*klioclient.BackupMetadata, error) {
	tier1, ok := kopiaClient.Tier1.(*kopia.Connection)
	if !ok || tier1 == nil {
		return nil, cli.NewCodedError(
			ErrProtocolRequiresTier1,
			backupfailure.RepositoryError.ExitCode)
	}
	if !tier1.SupportsBaseBackupIngestion() {
		return nil, cli.NewCodedError(
			ErrHelperNotAvailable,
			backupfailure.RepositoryError.ExitCode)
	}

	repConn, err := pgconn.Connect(ctx, configuration.Source.DSN)
	if err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while opening the replication connection: %w", err),
			backupfailure.SourceError.ExitCode)
	}
	defer func() {
		_ = repConn.Close(ctx)
	}()

	sqlConn, err := pgx.Connect(ctx, configuration.Source.StandardDSN)
	if err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while connecting to PostgreSQL: %w", err),
			backupfailure.SourceError.ExitCode)
	}
	defer func() {
		_ = sqlConn.Close(ctx)
	}()

	hostname := kopiaClient.GetHostname()

	executorOpts := basebackupexec.Options{
		Name:        opts.name,
		Pinned:      opts.pinned,
		Incremental: opts.incremental,
	}

	if opts.incremental {
		possible, err := resolvePreviousBackup(ctx, kopiaClient, tier1, sqlConn, hostname, opts, &executorOpts)
		if err != nil {
			return nil, err
		}
		// Incremental is a best-effort optimization: when no usable prior backup
		// manifest is available, fall back to a full backup (with a warning
		// already logged), rather than failing.
		executorOpts.Incremental = possible
	}

	executor := basebackupexec.New(repConn, sqlConn, tier1, hostname)

	metadata, err := executor.Run(ctx, executorOpts)
	if err != nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("while running the BASE_BACKUP protocol backup: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	return metadata, nil
}

// resolvePreviousBackup tries to set up an incremental backup. It selects a
// single prior backup and reads both the PostgreSQL backup_manifest (for
// UPLOAD_MANIFEST) and that backup's per-source Kopia manifest IDs (the combine
// base) from it, so the two halves always describe the same backup. It returns
// true when incremental is possible.
//
// The prior is chosen by --previous-backup-name when set, otherwise as the most
// recent backup for the cluster. The distinction also governs what happens when
// the chosen backup turns out to be unusable: an explicitly named backup is a
// hard error (the user's request cannot be honored), while the auto-detected one
// falls back to a full backup with a warning.
func resolvePreviousBackup(
	ctx context.Context,
	kopiaClient *kopia.MultiConnection,
	tier1 *kopia.Connection,
	sqlConn *pgx.Conn,
	hostname string,
	opts baseBackupProtocolOptions,
	executorOpts *basebackupexec.Options,
) (bool, error) {
	contextLogger := log.FromContext(ctx)

	backups, err := kopiaClient.ListBackups(ctx, hostname)
	if err != nil {
		return false, cli.NewCodedError(
			fmt.Errorf("while listing backups: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	explicit := opts.previousBackupName != ""
	prior, err := basebackupexec.SelectPriorBackup(backups, opts.previousBackupName)
	if err != nil {
		return false, cli.NewCodedError(err, backupfailure.RepositoryError.ExitCode)
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

	// The combine base requires the prior backup's per-source Kopia snapshots,
	// which only the BASE_BACKUP protocol path records. A prior produced by the
	// filesystem-snapshot path cannot be used as an incremental base.
	if len(prior.KopiaManifests) == 0 {
		return unusable("no Kopia manifest references (not produced by the BASE_BACKUP protocol path)")
	}

	if reason := checkPriorLineage(ctx, sqlConn, prior, contextLogger); reason != "" {
		return unusable(reason)
	}

	if reason := checkPriorWALSummaries(ctx, sqlConn, prior, contextLogger); reason != "" {
		return unusable(reason)
	}

	manifest, reason, err := loadPriorManifest(ctx, tier1, hostname, prior)
	if err != nil {
		return false, err
	}
	if reason != "" {
		return unusable(reason)
	}

	executorOpts.PreviousManifest = manifest
	executorOpts.PreviousKopiaManifests = basebackupexec.ToMultiManifestMap(prior.KopiaManifests)

	return true, nil
}

// priorUnusable maps an "unusable prior" reason to the right outcome: a hard
// coded error when the prior was named explicitly (the user's request cannot be
// honored), or a warning plus a full-backup fallback when it was auto-detected.
func priorUnusable(
	explicit bool, prior *klioclient.BackupMetadata, reason string, logger log.Logger,
) (bool, error) {
	if explicit {
		return false, cli.NewCodedError(
			fmt.Errorf("%w: %q: %s", basebackupexec.ErrPreviousBackupUnusable, prior.Name, reason),
			backupfailure.RepositoryError.ExitCode)
	}
	logger.Warning(
		"Most recent backup cannot serve as an incremental base; falling back to a full backup",
		"reason", reason, "previousBackup", prior.Name)

	return false, nil
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

	_, reason := basebackupexec.ValidatePriorForIncremental(prior, systemID)

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

// loadPriorManifest reads the stored backup_manifest for prior. A missing
// manifest yields a non-empty "unusable" reason; a genuine repository failure is
// returned as the error.
func loadPriorManifest(
	ctx context.Context, tier1 *kopia.Connection, hostname string, prior *klioclient.BackupMetadata,
) ([]byte, string, error) {
	manifest, err := tier1.GetBackupManifest(ctx, hostname, prior.Name)
	if err != nil {
		if _, ok := errors.AsType[kopia.NoBackupFoundError](err); ok {
			return nil, "no stored backup_manifest", nil
		}

		return nil, "", cli.NewCodedError(
			fmt.Errorf("while reading the previous backup manifest from the repository: %w", err),
			backupfailure.RepositoryError.ExitCode)
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
	// pg_lsn text format is the two 32-bit halves in hex, e.g. "1/B374D848".
	startLSN := fmt.Sprintf("%X/%X", uint32(prior.StartLSN>>32), uint32(prior.StartLSN)) //nolint:gosec // LSN halves

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

//nolint:gochecknoinits
func init() {
	// Here you will define your flags and configuration settings.
	runCmd.Flags().StringP("name", "n", "", "The backup name")

	runCmd.Flags().Bool(
		"wait-for-wals",
		true,
		"When enabled, wait until all the required WAL files have "+
			"been archived in tier1 before declaring the backup completed.")

	runCmd.Flags().Bool(
		"enable-tier2-backup",
		false,
		"When enabled, require the backup to be sent to tier2")

	runCmd.Flags().Bool(
		"use-base-backup-protocol",
		false,
		"Opt in to sourcing the base backup from the PostgreSQL BASE_BACKUP "+
			"streaming-replication protocol and ingesting it through the "+
			"kopia-pgbasebackup helper. Disabled by default: klio snapshots a "+
			"locally-mounted PGDATA as before. May also be enabled via the Klio "+
			"configuration (backup.use_base_backup_protocol).")

	runCmd.Flags().Bool(
		"incremental",
		false,
		"Request a PostgreSQL incremental backup (BASE_BACKUP INCREMENTAL). "+
			"Requires --use-base-backup-protocol and PostgreSQL 17+. The prior "+
			"backup is detected automatically; when none is usable, klio logs a "+
			"warning and falls back to a full backup.")

	runCmd.Flags().String(
		"previous-backup-name",
		"",
		"Optional name of the backup to use as the incremental base. Its "+
			"backup_manifest and its Kopia snapshots are both used, so they always "+
			"match. When unset, klio uses the most recent backup. A named backup "+
			"that does not exist or cannot serve as a base is a hard error (unlike "+
			"the auto-detected case, which falls back to a full backup).")

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	BackupCmd.AddCommand(runCmd)
}
