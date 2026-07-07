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
	"io"
	"testing"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
	kopiaconn "github.com/cloudnative-pg/klio/core/internal/client/klioclient/kopia"
	kopiaWrapper "github.com/cloudnative-pg/klio/core/internal/kopia"
)

var _ BackupLister = (*kopiaconn.MultiConnection)(nil)

// fakeProtocolUploader implements ProtocolUploader. Only GetBackupManifest and
// ResolveManifestID are configurable; the rest are trivial stubs since the
// tests here never call them.
type fakeProtocolUploader struct {
	manifest    []byte
	manifestErr error

	// resolveManifestID, when set, computes ResolveManifestID's result per
	// call; otherwise it returns a fixed "resolved-id" for every call.
	resolveManifestID func(tags map[string]string) (string, error)
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

func (f *fakeProtocolUploader) ResolveManifestID(_ context.Context, tags map[string]string) (string, error) {
	if f.resolveManifestID == nil {
		return "resolved-id", nil
	}

	return f.resolveManifestID(tags)
}

var _ ProtocolUploader = (*fakeProtocolUploader)(nil)

func TestSelectPriorBackup(t *testing.T) {
	backups := klioclient.BackupList{
		{Name: "old", StartedAt: 100},
		{Name: "newest", StartedAt: 300},
		{Name: "middle", StartedAt: 200},
	}

	t.Run("auto-detects the most recent backup", func(t *testing.T) {
		prior, err := SelectPriorBackup(backups, "")
		require.NoError(t, err)
		require.NotNil(t, prior)
		assert.Equal(t, "newest", prior.Name)
	})

	t.Run("auto-detection on an empty list returns nil, nil (fall back to full)", func(t *testing.T) {
		prior, err := SelectPriorBackup(nil, "")
		require.NoError(t, err)
		assert.Nil(t, prior)
	})

	t.Run("selects the named backup", func(t *testing.T) {
		prior, err := SelectPriorBackup(backups, "middle")
		require.NoError(t, err)
		require.NotNil(t, prior)
		assert.Equal(t, "middle", prior.Name)
	})

	t.Run("a named backup that does not exist is a hard error", func(t *testing.T) {
		prior, err := SelectPriorBackup(backups, "missing")
		require.ErrorIs(t, err, ErrPreviousBackupNotFound)
		assert.Nil(t, prior)
	})
}

func TestValidatePriorForIncremental(t *testing.T) {
	const systemID int64 = 7123456789012345678

	t.Run("same cluster lineage is usable regardless of timeline", func(t *testing.T) {
		// A prior from an earlier timeline (the normal post-failover case) is a
		// valid base; ancestry is validated by PostgreSQL, not here.
		prior := &klioclient.BackupMetadata{SystemID: systemID, Timeline: 1}
		ok, reason := ValidatePriorForIncremental(prior, systemID)
		assert.True(t, ok)
		assert.Empty(t, reason)
	})

	t.Run("different system identifier is rejected", func(t *testing.T) {
		prior := &klioclient.BackupMetadata{SystemID: systemID + 1}
		ok, reason := ValidatePriorForIncremental(prior, systemID)
		assert.False(t, ok)
		assert.Contains(t, reason, "different cluster")
	})

	t.Run("missing system identifier on the prior is rejected", func(t *testing.T) {
		prior := &klioclient.BackupMetadata{SystemID: 0}
		ok, reason := ValidatePriorForIncremental(prior, systemID)
		assert.False(t, ok)
		assert.Contains(t, reason, "different cluster")
	})
}

func TestFindBackupByName(t *testing.T) {
	backups := klioclient.BackupList{
		{Name: "first", StartedAt: 100},
		{Name: "second", StartedAt: 200},
	}

	t.Run("returns the matching backup", func(t *testing.T) {
		got := findBackupByName(backups, "second")
		require.NotNil(t, got)
		assert.Equal(t, "second", got.Name)
	})

	t.Run("nil when no backup matches", func(t *testing.T) {
		assert.Nil(t, findBackupByName(backups, "missing"))
	})

	t.Run("nil for an empty list", func(t *testing.T) {
		assert.Nil(t, findBackupByName(nil, "first"))
	})
}

func TestVerifyPriorPgdataExists(t *testing.T) {
	t.Run("pgdata exists", func(t *testing.T) {
		prior := &klioclient.BackupMetadata{Name: "backup-1"}
		tier1 := &fakeProtocolUploader{}

		reason, err := verifyPriorPgdataExists(context.Background(), tier1, prior)
		require.NoError(t, err)
		assert.Empty(t, reason)
	})

	t.Run("a missing snapshot is a reason, not an error", func(t *testing.T) {
		prior := &klioclient.BackupMetadata{Name: "backup-1"}
		tier1 := &fakeProtocolUploader{
			resolveManifestID: func(map[string]string) (string, error) {
				return "", kopiaconn.NoSnapshotFoundError{}
			},
		}

		reason, err := verifyPriorPgdataExists(context.Background(), tier1, prior)
		require.NoError(t, err)
		assert.NotEmpty(t, reason)
	})

	t.Run("a repository failure is returned as an error", func(t *testing.T) {
		prior := &klioclient.BackupMetadata{Name: "backup-1"}
		tier1 := &fakeProtocolUploader{
			resolveManifestID: func(map[string]string) (string, error) {
				return "", errors.New("connection reset")
			},
		}

		reason, err := verifyPriorPgdataExists(context.Background(), tier1, prior)
		require.Error(t, err)
		assert.Empty(t, reason)
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
		tier1 := &fakeProtocolUploader{manifestErr: kopiaconn.NoBackupFoundError{}}

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
		require.ErrorIs(t, err, ErrPreviousBackupUnusable)
		require.ErrorContains(t, err, "backup-1")
		require.ErrorContains(t, err, "wrong cluster lineage")
	})

	t.Run("an auto-detected backup falls back to a full backup with a warning", func(t *testing.T) {
		possible, err := priorUnusable(false, prior, "wrong cluster lineage", logger)
		assert.False(t, possible)
		require.NoError(t, err)
	})
}
