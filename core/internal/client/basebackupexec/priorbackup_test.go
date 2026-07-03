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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
)

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

func TestPickLatestBackup(t *testing.T) {
	t.Run("empty list returns nil", func(t *testing.T) {
		assert.Nil(t, pickLatestBackup(nil))
	})

	t.Run("picks the most recent by StartedAt", func(t *testing.T) {
		backups := klioclient.BackupList{
			{Name: "old", StartedAt: 100},
			{Name: "newest", StartedAt: 300},
			{Name: "middle", StartedAt: 200},
		}
		latest := pickLatestBackup(backups)
		assert.NotNil(t, latest)
		assert.Equal(t, "newest", latest.Name)
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
