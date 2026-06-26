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

	// ErrPreviousBackupNotFound is returned when --previous-backup-name names a
	// backup that does not exist for the cluster.
	ErrPreviousBackupNotFound = errors.New("the requested previous backup does not exist")

	// ErrPreviousBackupUnusable is returned when an explicitly requested previous
	// backup cannot serve as an incremental base.
	ErrPreviousBackupUnusable = errors.New("the requested previous backup cannot serve as an incremental base")

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
	prior, err := selectPriorBackup(backups, opts)
	if err != nil {
		return false, err
	}
	if prior == nil {
		contextLogger.Warning(
			"Incremental backup requested but no previous backup exists; falling back to a full backup")

		return false, nil
	}

	// unusable reports that the chosen prior cannot serve as a base. It is a hard
	// error when the backup was named explicitly, or a warning plus a full-backup
	// fallback when it was auto-detected.
	unusable := func(reason string) (bool, error) {
		if explicit {
			return false, cli.NewCodedError(
				fmt.Errorf("%w: %q: %s", ErrPreviousBackupUnusable, prior.Name, reason),
				backupfailure.RepositoryError.ExitCode)
		}
		contextLogger.Warning(
			"Most recent backup cannot serve as an incremental base; falling back to a full backup",
			"reason", reason, "previousBackup", prior.Name)

		return false, nil
	}

	// The combine base requires the prior backup's per-source Kopia snapshots,
	// which only the BASE_BACKUP protocol path records. A prior produced by the
	// filesystem-snapshot path cannot be used as an incremental base.
	if len(prior.KopiaManifests) == 0 {
		return unusable("no Kopia manifest references (not produced by the BASE_BACKUP protocol path)")
	}

	// Reject a prior that belongs to a different cluster lineage: the repository
	// can still hold backups from a destroyed-and-recreated cluster that reused
	// this hostname, and combining against one would be meaningless. PostgreSQL's
	// UPLOAD_MANIFEST would reject it too. Ancestry (is the prior a valid base on
	// the current timeline history?) is left to PostgreSQL's WAL summarizer,
	// which is the authority and validates it during the incremental BASE_BACKUP.
	systemID, err := readSystemIdentifier(ctx, sqlConn)
	if err != nil {
		contextLogger.Warning(
			"Could not read the cluster system identifier to validate the incremental base; "+
				"falling back to a full backup",
			"err", err, "previousBackup", prior.Name)

		return false, nil
	}
	if ok, reason := validatePriorForIncremental(prior, systemID); !ok {
		return unusable(reason)
	}

	// Both halves are sourced from the same prior backup: the backup_manifest
	// uploaded to PostgreSQL and the per-source Kopia snapshots combined against
	// must describe the same point in time, or the incremental would be
	// unrestorable.
	manifest, err := tier1.GetBackupManifest(ctx, hostname, prior.Name)
	if err != nil {
		if _, ok := errors.AsType[kopia.NoBackupFoundError](err); ok {
			return unusable("no stored backup_manifest")
		}

		return false, cli.NewCodedError(
			fmt.Errorf("while reading the previous backup manifest from the repository: %w", err),
			backupfailure.RepositoryError.ExitCode)
	}

	executorOpts.PreviousManifest = manifest
	executorOpts.PreviousKopiaManifests = toMultiManifestMap(prior.KopiaManifests)

	return true, nil
}

// selectPriorBackup chooses the backup to use as the incremental base: the one
// named by --previous-backup-name (a hard error when it does not exist), or the
// most recent backup otherwise. A nil result with a nil error means
// auto-detection found no backup, so the caller should fall back to a full one.
func selectPriorBackup(
	backups klioclient.BackupList, opts baseBackupProtocolOptions,
) (*klioclient.BackupMetadata, error) {
	if opts.previousBackupName == "" {
		return pickLatestBackup(backups), nil
	}

	prior := findBackupByName(backups, opts.previousBackupName)
	if prior == nil {
		return nil, cli.NewCodedError(
			fmt.Errorf("%w: %q", ErrPreviousBackupNotFound, opts.previousBackupName),
			backupfailure.RepositoryError.ExitCode)
	}

	return prior, nil
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

// validatePriorForIncremental reports whether prior can seed an incremental
// backup of a cluster with the given system identifier. A prior from a different
// cluster lineage cannot serve as a combine base. Whether the prior is a valid
// ancestor on the current timeline history is left to PostgreSQL, which
// validates it when the incremental BASE_BACKUP runs. The returned reason is
// suitable for a log message.
func validatePriorForIncremental(prior *klioclient.BackupMetadata, systemID int64) (bool, string) {
	if systemID != 0 && prior.SystemID != systemID {
		return false, fmt.Sprintf(
			"previous backup belongs to a different cluster (system identifier %d, current %d)",
			prior.SystemID, systemID)
	}

	return true, ""
}

// pickLatestBackup returns the backup with the greatest StartedAt, or nil when
// the list is empty.
func pickLatestBackup(backups klioclient.BackupList) *klioclient.BackupMetadata {
	var latest *klioclient.BackupMetadata
	for i := range backups {
		if latest == nil || backups[i].StartedAt > latest.StartedAt {
			latest = &backups[i]
		}
	}

	return latest
}

// toMultiManifestMap adapts the stored source→manifest-ID map to the
// source→[]manifest-ID shape the helper expects.
func toMultiManifestMap(in map[string]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string][]string, len(in))
	for source, id := range in {
		out[source] = []string{id}
	}

	return out
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
