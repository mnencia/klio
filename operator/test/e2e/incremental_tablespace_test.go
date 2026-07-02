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

package e2e

import (
	"context"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/types"

	"github.com/cloudnative-pg/klio/operator/internal/klioconfig"
	machineryConditions "github.com/cloudnative-pg/klio/operator/test/machinery/pkg/conditions"
	machineryFeatures "github.com/cloudnative-pg/klio/operator/test/machinery/pkg/features"
	"github.com/cloudnative-pg/klio/operator/test/machinery/pkg/postgres"
	"github.com/cloudnative-pg/klio/operator/test/utils/mutators"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/certificates"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/cnpg"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/klio"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/secrets"
)

// incrementalNewTablespaceFeature exercises the per-source incremental decision:
// a tablespace is added to the source cluster only *after* the full backup, so
// it has no counterpart in the prior backup. On the incremental backup klio must
// ingest the main data directory incrementally (it has a prior source) while
// ingesting the new tablespace as a full source (it has none) — combining both
// on ingest into a self-contained snapshot. The recovered cluster must contain
// both the main-directory data and the tablespace data, which proves the mixed
// full+incremental per-source path works end to end.
type incrementalNewTablespaceFeature struct {
	name              string
	scenario          *commonBackupRestoreScenario
	fullBackup        *cnpgv1.Backup
	incrementalBackup *cnpgv1.Backup
	recoveryCluster   *cnpgv1.Cluster
	tablespaces       []cnpgv1.TablespaceConfiguration
	mutate            []machineryFeatures.RecoveryClusterMutateFunc
}

func (f *incrementalNewTablespaceFeature) Name() string             { return f.name }
func (f *incrementalNewTablespaceFeature) Setup() types.StepFunc    { return f.scenario.Setup }
func (f *incrementalNewTablespaceFeature) Teardown() types.StepFunc { return f.scenario.Teardown }

func (f *incrementalNewTablespaceFeature) Run() types.StepFunc {
	return func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		t.Helper()
		t.Log("Running incremental backup with a newly-added tablespace feature test")

		r, err := resources.New(cfg.Client().RESTConfig())
		require.NoError(t, err, "failed to create resources client")
		src := &f.scenario.sourcePrimaryPod

		backupTimeout := wait.WithTimeout(3 * time.Minute)
		backupInterval := wait.WithInterval(10 * time.Second)

		// First data set in the main data directory, then a full backup. No prior
		// exists yet, so the incremental request falls back to a full backup.
		_, err = postgres.ExecPostgresQuery(ctx, r, src, "postgres",
			"CREATE TABLE t1 AS SELECT generate_series(1, 1000) AS x;")
		require.NoError(t, err, "failed to create table t1")
		require.NoError(t, postgres.CheckpointAndSwitchWal(ctx, r, src), "failed to checkpoint/switch WAL (1)")

		require.NoError(t, r.Create(ctx, f.fullBackup), "failed to create full backup")
		require.NoError(t,
			wait.For(machineryConditions.BackupIsCompleted(r, f.fullBackup), backupTimeout, backupInterval),
			"full backup not completed")

		// No prior exists yet, so the incremental request must have fallen back to
		// a full backup, recorded on the Backup resource's plugin metadata.
		assertBackupMethod(ctx, t, r, f.fullBackup, backupMethodFull)

		// Add a tablespace to the running source cluster. It exists only from the
		// incremental backup onward, so it has no source in the prior full backup.
		f.addTablespace(ctx, t, r)

		// Data in the newly-added tablespace. On the incremental backup this source
		// has no prior, so it must be ingested as a full source.
		_, err = postgres.ExecPostgresQuery(ctx, r, src, "postgres",
			"CREATE TABLE t2 TABLESPACE tbs1 AS SELECT generate_series(1, 500) AS y;")
		require.NoError(t, err, "failed to create table t2 in tablespace tbs1")
		require.NoError(t, postgres.CheckpointAndSwitchWal(ctx, r, src), "failed to checkpoint/switch WAL (2)")

		require.NoError(t, r.Create(ctx, f.incrementalBackup), "failed to create incremental backup")
		require.NoError(t,
			wait.For(machineryConditions.BackupIsCompleted(r, f.incrementalBackup), backupTimeout, backupInterval),
			"incremental backup not completed")

		// Confirm the backup really took the incremental path (rather than silently
		// falling back to a full backup). The new tablespace source going full is
		// proven by the restore below: a wrong attempt to combine it against a
		// missing prior would fail the backup.
		assertBackupMethod(ctx, t, r, f.incrementalBackup, backupMethodIncremental)

		// Recover from the incremental backup.
		for _, mutate := range f.mutate {
			require.NoError(t, mutate(ctx, f.recoveryCluster, r), "failed to mutate recovery cluster")
		}
		require.NoError(t, r.Create(ctx, f.recoveryCluster), "failed to create recovery cluster")
		require.NoError(t,
			wait.For(machineryConditions.ClusterIsReady(r, f.recoveryCluster),
				wait.WithTimeout(5*time.Minute), wait.WithInterval(10*time.Second)),
			"recovery cluster not ready")

		// The recovered cluster must contain both the main-directory data (ingested
		// incrementally) and the tablespace data (ingested as a full source),
		// proving the mixed per-source snapshot was combined into a full image.
		var recoveryPrimary corev1.Pod
		require.NoError(t,
			r.Get(ctx, f.recoveryCluster.Status.CurrentPrimary, f.recoveryCluster.Namespace, &recoveryPrimary),
			"failed to get recovery cluster primary pod")
		assertCount(ctx, t, r, &recoveryPrimary, "t1", 1000)
		assertCount(ctx, t, r, &recoveryPrimary, "t2", 500)

		return ctx
	}
}

// addTablespace adds the feature's tablespace to the running source cluster and
// waits until PostgreSQL actually reports it. Waiting on the tablespace existing
// (rather than on the Cluster being ready) is race-free: the Cluster can still
// read as ready in the instant between the spec update and the reconcile, while
// pg_tablespace only lists tbs1 once CNPG has created it.
func (f *incrementalNewTablespaceFeature) addTablespace(ctx context.Context, t *testing.T, r *resources.Resources) {
	t.Helper()
	src := &f.scenario.sourcePrimaryPod

	var cluster cnpgv1.Cluster
	require.NoError(t,
		r.Get(ctx, f.scenario.cnpgCluster.Name, f.scenario.cnpgCluster.Namespace, &cluster),
		"failed to get source cluster")
	cluster.Spec.Tablespaces = f.tablespaces
	require.NoError(t, r.Update(ctx, &cluster), "failed to add tablespace to source cluster")

	require.NoError(t, wait.For(func(ctx context.Context) (bool, error) {
		out, err := postgres.ExecPostgresQuery(ctx, r, src, "postgres",
			"SELECT count(*) FROM pg_tablespace WHERE spcname = 'tbs1';")
		if err != nil {
			// The instance may be briefly unavailable while CNPG reconciles the
			// tablespace; keep polling instead of failing.
			return false, nil //nolint:nilerr // transient exec errors are retried
		}

		return out == "1", nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(10*time.Second)),
		"tablespace tbs1 was not created on the source cluster")
}

// RecoverClusterViaIncrementalWithNewTablespace is the e2e feature covering an
// incremental backup whose source set gained a tablespace after the prior full
// backup (the per-source full+incremental decision).
func RecoverClusterViaIncrementalWithNewTablespace(namespace string) *incrementalNewTablespaceFeature {
	const (
		externalClusterName     = "source-cluster"
		cnpgSourceClusterName   = "test-cluster-source"
		cnpgRestoredClusterName = "test-cluster-restore"
	)

	namespaceObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}

	issuer := certificates.GetSelfSignedIssuerObject("selfsigned-issuer", namespace)
	certificate := certificates.GetCertificateObject("test", namespace, []string{klioServerName}, issuer)
	caCertificate := certificates.GetCACertificateObject("test-ca", namespace, issuer)
	caIssuer := certificates.GetCAIssuerObject("test-ca-issuer", namespace, caCertificate.Spec.SecretName)

	// The source cluster starts without a tablespace; it is added at runtime,
	// after the full backup, so the tablespace is new to the incremental backup.
	cnpgCluster := cnpg.GetCnpgClusterObject(cnpgSourceClusterName, namespace, 1, "klio-plugin-configuration",
		cnpg.ClusterTemplateOptions{
			StorageClass: testCfg.StorageClass,
			// Incremental backups require the WAL summarizer on the cluster.
			Parameters: map[string]string{"summarize_wal": "on"},
		})

	tablespaces := []cnpgv1.TablespaceConfiguration{{
		Name:    "tbs1",
		Owner:   cnpgv1.DatabaseRoleRef{Name: "postgres"},
		Storage: cnpgv1.StorageConfiguration{Size: "1Gi"},
	}}
	if testCfg.StorageClass != "" {
		tablespaces[0].Storage.StorageClass = new(testCfg.StorageClass)
	}

	userCertificate := certificates.GetUserCertificateObject("klio-user", namespace,
		"klio-user@"+cnpgSourceClusterName, caIssuer)
	klioPluginConfigurationSource := klio.GetPluginConfigurationObject(
		"klio-plugin-configuration",
		namespace,
		klio.PluginConfigurationTemplateOptions{
			ServerCertificate:     certificate,
			ClientCertificate:     userCertificate,
			ClusterName:           cnpgSourceClusterName,
			UseBaseBackupProtocol: true,
			Incremental:           true,
		},
	)
	ageSecrets := secrets.GetKlioAgeEncryptionSecrets("encryption", namespace, "testencryptionpassword123")
	klioServer := klio.GetServerObject(
		klioServerName,
		namespace,
		klio.ServerTemplateOptions{
			Image:              testCfg.ServerImage,
			StorageClass:       testCfg.StorageClass,
			TLSSecretName:      certificate.Spec.SecretName,
			ClientCASecretName: caCertificate.Spec.SecretName,
			Encryption: klio.EncryptionOptions{
				EncryptionKeySecretName: ageSecrets.EncryptionKeySecret.Name,
				EncryptionKeyFileName:   "encryption-key.age",
				IdentitySecretName:      ageSecrets.IdentitySecret.Name,
				IdentityFileName:        "identity.txt",
			},
		},
	)

	fullBackup := cnpg.GetCnpgBackupObject("test-backup-full", namespace, cnpgv1.DefaultBackupTarget, cnpgCluster)
	incrementalBackup := cnpg.GetCnpgBackupObject("test-backup-incr", namespace, cnpgv1.DefaultBackupTarget, cnpgCluster)

	// The recovery cluster must declare the tablespace up front so CNPG
	// provisions its PVC when restoring the snapshot that contains it.
	recoveryCluster := cnpgCluster.DeepCopy()
	recoveryCluster.Name = cnpgRestoredClusterName
	recoveryCluster.Spec.Tablespaces = tablespaces
	recoveryCluster.Spec.ExternalClusters = []cnpgv1.ExternalCluster{{
		Name: externalClusterName,
		PluginConfiguration: &cnpgv1.PluginConfiguration{
			Name:    "klio.cnpg.io",
			Enabled: new(true),
			Parameters: map[string]string{
				klioconfig.PluginConfigurationRefParam: "klio-plugin-configuration-recovery",
			},
		},
	}}
	recoveryCluster.Spec.Bootstrap = &cnpgv1.BootstrapConfiguration{
		Recovery: &cnpgv1.BootstrapRecovery{Source: externalClusterName},
	}
	recoveryCluster.Spec.Plugins = []cnpgv1.PluginConfiguration{}

	klioPluginConfigurationRecovery := klioPluginConfigurationSource.DeepCopy()
	klioPluginConfigurationRecovery.Name = "klio-plugin-configuration-recovery"
	klioPluginConfigurationRecovery.Spec.ClusterName = cnpgCluster.Name

	scenario := &commonBackupRestoreScenario{
		namespace:                       namespaceObj,
		cnpgCluster:                     cnpgCluster,
		userCertificate:                 userCertificate,
		encryptionSecret:                ageSecrets.EncryptionKeySecret,
		identitySecret:                  ageSecrets.IdentitySecret,
		issuer:                          issuer,
		caIssuer:                        caIssuer,
		caCertificate:                   caCertificate,
		certificate:                     certificate,
		klioServer:                      klioServer,
		klioPluginConfigurationSource:   klioPluginConfigurationSource,
		klioPluginConfigurationRecovery: klioPluginConfigurationRecovery,
		name:                            "RecoverClusterViaIncrementalWithNewTablespace",
	}

	return &incrementalNewTablespaceFeature{
		name:              "RecoverClusterViaIncrementalWithNewTablespace",
		scenario:          scenario,
		fullBackup:        fullBackup,
		incrementalBackup: incrementalBackup,
		recoveryCluster:   recoveryCluster,
		tablespaces:       tablespaces,
		mutate:            []machineryFeatures.RecoveryClusterMutateFunc{mutators.CreateBackupIDMutator(incrementalBackup)},
	}
}
