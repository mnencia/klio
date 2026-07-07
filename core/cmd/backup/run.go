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

	// ErrIncrementalRequiresProtocol is returned when an incremental backup is
	// requested without the BASE_BACKUP protocol path, which is the only path
	// able to produce an incremental backup.
	ErrIncrementalRequiresProtocol = errors.New(
		"incremental backup requires the BASE_BACKUP protocol path (--use-base-backup-protocol)")

	// ErrPreviousBackupNameRequiresIncremental is returned when
	// --previous-backup-name is set without --incremental: naming a specific
	// backup as the incremental base has no effect unless an incremental backup
	// is actually requested.
	ErrPreviousBackupNameRequiresIncremental = errors.New(
		"--previous-backup-name requires --incremental")
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
	previousBackupName, _ := cmd.Flags().GetString("previous-backup-name")
	backupName, _ := cmd.Flags().GetString("name")

	// use-base-backup-protocol and incremental are bound to the Klio
	// configuration via viper.BindPFlag (see init()): an explicitly-set CLI
	// flag overrides the configuration value, otherwise the configuration
	// value is used, defaulting to false (the historical filesystem-snapshot
	// path) when neither is set.
	useProtocol := configuration.Backup.UseBaseBackupProtocol
	incremental := configuration.Backup.Incremental

	// Naming a previous backup only makes sense when requesting an incremental
	// backup. Reject the combination loudly instead of silently dropping the
	// name and taking a full backup.
	if err := validatePreviousBackupNameRequiresIncremental(incremental, previousBackupName); err != nil {
		return err
	}

	// Incremental backups only exist on the BASE_BACKUP protocol path. Reject
	// the combination loudly instead of silently taking a full
	// filesystem-snapshot backup.
	if err := validateIncrementalRequiresProtocol(incremental, useProtocol); err != nil {
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

// validateIncrementalRequiresProtocol rejects requesting an incremental backup
// without the BASE_BACKUP protocol path, which is the only path able to
// produce one. Without this guard the request would be silently downgraded to
// a full filesystem-snapshot backup.
func validateIncrementalRequiresProtocol(incremental, useProtocol bool) error {
	if incremental && !useProtocol {
		return ErrIncrementalRequiresProtocol
	}

	return nil
}

// validatePreviousBackupNameRequiresIncremental rejects specifying a previous
// backup name without also requesting an incremental backup. Without this
// guard the name would be silently dropped and a full backup would run
// instead of using (or hard-erroring on) the named one.
func validatePreviousBackupNameRequiresIncremental(incremental bool, previousBackupName string) error {
	if previousBackupName != "" && !incremental {
		return ErrPreviousBackupNameRequiresIncremental
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
	tier1, ok := kopiaClient.Tier1.(basebackupexec.ProtocolUploader)
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
		possible, err := basebackupexec.ResolvePriorForIncremental(
			ctx, kopiaClient, tier1, sqlConn, hostname, opts.previousBackupName, &executorOpts)
		if err != nil {
			// A disabled summarize_wal is a source-cluster misconfiguration, not a
			// repository failure, so it carries the source exit code for accurate
			// failure-category reporting.
			if errors.Is(err, basebackupexec.ErrSummarizeWALDisabled) {
				return nil, cli.NewCodedError(err, backupfailure.SourceError.ExitCode)
			}

			return nil, cli.NewCodedError(err, backupfailure.RepositoryError.ExitCode)
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
		"Take the base backup over the PostgreSQL BASE_BACKUP "+
			"streaming-replication protocol and ingest it into the Kopia "+
			"repository through the kopia-pgbasebackup helper.")
	_ = viper.BindPFlag("backup.use_base_backup_protocol", runCmd.Flags().Lookup("use-base-backup-protocol"))

	runCmd.Flags().Bool(
		"incremental",
		false,
		"Request a PostgreSQL incremental backup. Requires "+
			"--use-base-backup-protocol and PostgreSQL 17+. The prior "+
			"backup is detected automatically; when none is usable, klio logs a "+
			"warning and falls back to a full backup.")
	_ = viper.BindPFlag("backup.incremental", runCmd.Flags().Lookup("incremental"))

	runCmd.Flags().String(
		"previous-backup-name",
		"",
		"Optional name of the backup to use as the incremental base. Requires "+
			"--incremental. When unset, klio uses the most recent backup.")

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	BackupCmd.AddCommand(runCmd)
}
