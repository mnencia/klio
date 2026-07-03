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
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/basebackupexec"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
	kopiaWrapper "github.com/cloudnative-pg/klio/core/internal/kopia"
	"github.com/cloudnative-pg/klio/core/pkg/config"
)

// fakeProtocolUploader implements basebackupexec.ProtocolUploader. Only
// GetBackupManifest is configurable; the rest are trivial stubs since
// loadPriorManifest never calls them.
type fakeProtocolUploader struct {
	manifest    []byte
	manifestErr error
}

func (f *fakeProtocolUploader) GetHostname() string { return "cluster-a" }

func (f *fakeProtocolUploader) IngestBaseBackup(
	context.Context, io.Reader, kopiaWrapper.IngestOptions,
) (*kopiaWrapper.IngestResult, error) {
	return nil, nil
}

func (f *fakeProtocolUploader) UploadBackupManifest(context.Context, string, string, []byte, bool) error {
	return nil
}

func (f *fakeProtocolUploader) UploadBackupMetadata(
	context.Context, string, *klioclient.BackupMetadata, bool,
) error {
	return nil
}

func (f *fakeProtocolUploader) SupportsBaseBackupIngestion() bool { return true }

func (f *fakeProtocolUploader) GetBackupManifest(context.Context, string, string) ([]byte, error) {
	return f.manifest, f.manifestErr
}

var _ basebackupexec.ProtocolUploader = (*fakeProtocolUploader)(nil)

func TestValidateIncrementalRequiresProtocol(t *testing.T) {
	t.Run("incremental without protocol is rejected", func(t *testing.T) {
		err := validateIncrementalRequiresProtocol(true, false)
		require.ErrorIs(t, err, ErrIncrementalRequiresProtocol)
	})

	t.Run("incremental with protocol is allowed", func(t *testing.T) {
		require.NoError(t, validateIncrementalRequiresProtocol(true, true))
	})

	t.Run("neither requested is allowed", func(t *testing.T) {
		require.NoError(t, validateIncrementalRequiresProtocol(false, false))
	})
}

func TestValidatePreviousBackupNameRequiresIncremental(t *testing.T) {
	t.Run("previous backup name without incremental is rejected", func(t *testing.T) {
		err := validatePreviousBackupNameRequiresIncremental(false, "base-1")
		require.ErrorIs(t, err, ErrPreviousBackupNameRequiresIncremental)
	})

	t.Run("previous backup name with incremental is allowed", func(t *testing.T) {
		require.NoError(t, validatePreviousBackupNameRequiresIncremental(true, "base-1"))
	})

	t.Run("no previous backup name without incremental is allowed", func(t *testing.T) {
		require.NoError(t, validatePreviousBackupNameRequiresIncremental(false, ""))
	})
}

// TestBackupOptionsPreferExplicitFlagOverConfig exercises the exact
// viper.BindPFlag wiring init() sets up for use-base-backup-protocol: an
// explicitly-set CLI flag must win over the configuration value in both
// directions (forcing on and forcing off), and the configuration value must
// be used when the flag is left at its default. Uses an isolated viper
// instance so it can't leak state into other tests or the package's real
// runCmd.
func TestBackupOptionsPreferExplicitFlagOverConfig(t *testing.T) {
	tests := []struct {
		name       string
		configYAML string
		flagArgs   []string
		want       bool
	}{
		{
			name: "neither flag nor config set keeps the historical default off",
			want: false,
		},
		{
			name:       "configuration value is used when the flag is not set",
			configYAML: "backup:\n  use_base_backup_protocol: true\n",
			want:       true,
		},
		{
			name:       "explicit flag forces it on over a false configuration",
			configYAML: "backup:\n  use_base_backup_protocol: false\n",
			flagArgs:   []string{"--use-base-backup-protocol=true"},
			want:       true,
		},
		{
			name:       "explicit flag forces it off over a true configuration",
			configYAML: "backup:\n  use_base_backup_protocol: true\n",
			flagArgs:   []string{"--use-base-backup-protocol=false"},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			if tt.configYAML != "" {
				require.NoError(t, v.ReadConfig(strings.NewReader(tt.configYAML)))
			}

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			fs.Bool("use-base-backup-protocol", false, "")
			require.NoError(t, v.BindPFlag("backup.use_base_backup_protocol", fs.Lookup("use-base-backup-protocol")))
			require.NoError(t, fs.Parse(tt.flagArgs))

			var configuration config.Data
			require.NoError(t, v.Unmarshal(&configuration))
			assert.Equal(t, tt.want, configuration.Backup.UseBaseBackupProtocol)
		})
	}
}

func TestLoadPriorManifest(t *testing.T) {
	prior := &klioclient.BackupMetadata{Name: "backup-1"}

	t.Run("manifest found", func(t *testing.T) {
		tier1 := &fakeProtocolUploader{manifest: []byte("manifest-bytes")}

		manifest, reason, err := loadPriorManifest(context.Background(), tier1, "cluster-a", prior)
		require.NoError(t, err)
		assert.Empty(t, reason)
		assert.Equal(t, []byte("manifest-bytes"), manifest)
	})

	t.Run("no stored manifest is a reason, not an error", func(t *testing.T) {
		tier1 := &fakeProtocolUploader{manifestErr: kopia.NoBackupFoundError{}}

		manifest, reason, err := loadPriorManifest(context.Background(), tier1, "cluster-a", prior)
		require.NoError(t, err)
		assert.Nil(t, manifest)
		assert.Equal(t, "no stored backup_manifest", reason)
	})

	t.Run("a repository failure is returned as an error", func(t *testing.T) {
		tier1 := &fakeProtocolUploader{manifestErr: errors.New("connection reset")}

		manifest, reason, err := loadPriorManifest(context.Background(), tier1, "cluster-a", prior)
		require.Error(t, err)
		assert.Empty(t, reason)
		assert.Nil(t, manifest)
	})
}

func TestPriorUnusable(t *testing.T) {
	prior := &klioclient.BackupMetadata{Name: "backup-1"}
	logger := log.FromContext(context.Background())

	t.Run("an explicitly named backup is a hard error", func(t *testing.T) {
		possible, err := priorUnusable(true, prior, "wrong cluster lineage", logger)
		assert.False(t, possible)
		require.ErrorIs(t, err, basebackupexec.ErrPreviousBackupUnusable)
		require.ErrorContains(t, err, "backup-1")
		require.ErrorContains(t, err, "wrong cluster lineage")
	})

	t.Run("an auto-detected backup falls back to a full backup with a warning", func(t *testing.T) {
		possible, err := priorUnusable(false, prior, "wrong cluster lineage", logger)
		assert.False(t, possible)
		require.NoError(t, err)
	})
}
