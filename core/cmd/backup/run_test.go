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
	"testing"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/basebackupexec"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	"github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
	kopiaWrapper "github.com/cloudnative-pg/klio/core/internal/kopia"
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

func TestResolveBackupOption(t *testing.T) {
	t.Run("default (no flag, no config) keeps the historical path", func(t *testing.T) {
		// flag not changed, config false -> false (filesystem-snapshot path).
		assert.False(t, resolveBackupOption(false, false, false))
	})

	t.Run("operator configuration opts in", func(t *testing.T) {
		// flag not changed, config true -> true.
		assert.True(t, resolveBackupOption(false, false, true))
	})

	t.Run("explicit flag overrides configuration (force on)", func(t *testing.T) {
		assert.True(t, resolveBackupOption(true, true, false))
	})

	t.Run("explicit flag overrides configuration (force off)", func(t *testing.T) {
		assert.False(t, resolveBackupOption(true, false, true))
	})
}

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
