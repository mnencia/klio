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
	"strconv"
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

	"github.com/cloudnative-pg/klio/operator/internal/cnpgi"
	"github.com/cloudnative-pg/klio/operator/internal/klioconfig"
	machineryConditions "github.com/cloudnative-pg/klio/operator/test/machinery/pkg/conditions"
	machineryFeatures "github.com/cloudnative-pg/klio/operator/test/machinery/pkg/features"
	"github.com/cloudnative-pg/klio/operator/test/machinery/pkg/pods"
	"github.com/cloudnative-pg/klio/operator/test/machinery/pkg/postgres"
	"github.com/cloudnative-pg/klio/operator/test/utils/mutators"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/certificates"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/cnpg"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/klio"
	"github.com/cloudnative-pg/klio/operator/test/utils/templates/secrets"
)

// incrementalRecoveryFeature takes a full base backup over the BASE_BACKUP
// protocol, then an incremental backup after more data is written, then
// recovers a cluster from the incremental backup. It exercises the
// combine-on-ingest path: the incremental snapshot must restore as a full image
// containing both data sets.
type incrementalRecoveryFeature struct {
	name              string
	scenario          *commonBackupRestoreScenario
	fullBackup        *cnpgv1.Backup
	incrementalBackup *cnpgv1.Backup
	recoveryCluster   *cnpgv1.Cluster
	mutate            []machineryFeatures.RecoveryClusterMutateFunc
}

func (f *incrementalRecoveryFeature) Name() string             { return f.name }
func (f *incrementalRecoveryFeature) Setup() types.StepFunc    { return f.scenario.Setup }
func (f *incrementalRecoveryFeature) Teardown() types.StepFunc { return f.scenario.Teardown }

func (f *incrementalRecoveryFeature) Run() types.StepFunc {
	return func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		t.Helper()
		t.Log("Running incremental backup feature test")

		r, err := resources.New(cfg.Client().RESTConfig())
		require.NoError(t, err, "failed to create resources client")
		src := &f.scenario.sourcePrimaryPod

		backupTimeout := wait.WithTimeout(3 * time.Minute)
		backupInterval := wait.WithInterval(10 * time.Second)

		// First data set, then a full backup (no prior exists yet, so the
		// incremental request falls back to a full backup).
		_, err = postgres.ExecPostgresQuery(ctx, r, src, "postgres",
			"CREATE TABLE t1 AS SELECT generate_series(1, 1000) AS x;")
		require.NoError(t, err, "failed to create table t1")
		require.NoError(t, postgres.CheckpointAndSwitchWal(ctx, r, src), "failed to checkpoint/switch WAL (1)")

		require.NoError(t, r.Create(ctx, f.fullBackup), "failed to create full backup")
		require.NoError(t,
			wait.For(machineryConditions.BackupIsCompleted(r, f.fullBackup), backupTimeout, backupInterval),
			"full backup not completed")

		// Second data set, then an incremental backup against the prior one.
		_, err = postgres.ExecPostgresQuery(ctx, r, src, "postgres",
			"CREATE TABLE t2 AS SELECT generate_series(1, 500) AS y;")
		require.NoError(t, err, "failed to create table t2")
		require.NoError(t, postgres.CheckpointAndSwitchWal(ctx, r, src), "failed to checkpoint/switch WAL (2)")

		require.NoError(t, r.Create(ctx, f.incrementalBackup), "failed to create incremental backup")
		require.NoError(t,
			wait.For(machineryConditions.BackupIsCompleted(r, f.incrementalBackup), backupTimeout, backupInterval),
			"incremental backup not completed")

		// Confirm the second backup really took the incremental path (and did
		// not silently fall back to a full backup).
		logs, err := pods.GetLogs(ctx, r, src, cnpgi.KlioPluginContainerName)
		require.NoError(t, err, "failed to get klio-plugin logs")
		require.Contains(t, logs, "Uploading prior manifest for incremental backup",
			"second backup did not take the incremental path")

		// Recover from the incremental backup.
		for _, mutate := range f.mutate {
			require.NoError(t, mutate(ctx, f.recoveryCluster, r), "failed to mutate recovery cluster")
		}
		require.NoError(t, r.Create(ctx, f.recoveryCluster), "failed to create recovery cluster")
		require.NoError(t,
			wait.For(machineryConditions.ClusterIsReady(r, f.recoveryCluster),
				wait.WithTimeout(3*time.Minute), wait.WithInterval(10*time.Second)),
			"recovery cluster not ready")

		// The recovered cluster must contain both data sets, proving the
		// incremental snapshot was combined into a full image on ingest.
		var recoveryPrimary corev1.Pod
		require.NoError(t,
			r.Get(ctx, f.recoveryCluster.Status.CurrentPrimary, f.recoveryCluster.Namespace, &recoveryPrimary),
			"failed to get recovery cluster primary pod")
		assertCount(ctx, t, r, &recoveryPrimary, "t1", 1000)
		assertCount(ctx, t, r, &recoveryPrimary, "t2", 500)

		return ctx
	}
}

func assertCount(ctx context.Context, t *testing.T, r *resources.Resources, pod *corev1.Pod, table string, want int) {
	t.Helper()
	out, err := postgres.ExecPostgresQuery(ctx, r, pod, "postgres", "SELECT COUNT(*) FROM "+table+";")
	require.NoError(t, err, "failed to count rows in %s", table)
	got, err := strconv.Atoi(out)
	require.NoError(t, err, "unexpected count output for %s: %q", table, out)
	require.Equal(t, want, got, "row count mismatch for %s", table)
}

// RecoverClusterViaIncrementalBackup is the incremental backup e2e feature.
func RecoverClusterViaIncrementalBackup(namespace string) *incrementalRecoveryFeature {
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

	cnpgCluster := cnpg.GetCnpgClusterObject(cnpgSourceClusterName, namespace, 1, "klio-plugin-configuration",
		cnpg.ClusterTemplateOptions{
			StorageClass: testCfg.StorageClass,
			// Incremental backups require the WAL summarizer on the cluster.
			Parameters: map[string]string{"summarize_wal": "on"},
		})

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

	recoveryCluster := cnpgCluster.DeepCopy()
	recoveryCluster.Name = cnpgRestoredClusterName
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
		name:                            "RecoverClusterViaIncrementalBackup",
	}

	return &incrementalRecoveryFeature{
		name:              "RecoverClusterViaIncrementalBackup",
		scenario:          scenario,
		fullBackup:        fullBackup,
		incrementalBackup: incrementalBackup,
		recoveryCluster:   recoveryCluster,
		mutate:            []machineryFeatures.RecoveryClusterMutateFunc{mutators.CreateBackupIDMutator(incrementalBackup)},
	}
}
