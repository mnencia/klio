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

package kopia

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseIngestResult(t *testing.T) {
	t.Run("valid result", func(t *testing.T) {
		input := []byte(`{
			"manifestId": "m-123",
			"rootObjectId": "o-456",
			"stats": {"totalSize": 100, "hashedBytes": 80, "cachedBytes": 20, "fileCount": 7}
		}`)

		result, err := parseIngestResult(input)
		require.NoError(t, err)
		assert.Equal(t, "m-123", result.ManifestID)
		assert.Equal(t, "o-456", result.RootObjectID)
		assert.Equal(t, int64(100), result.Stats.TotalSize)
		assert.Equal(t, int64(80), result.Stats.HashedBytes)
		assert.Equal(t, int64(20), result.Stats.CachedBytes)
		assert.Equal(t, int64(7), result.Stats.FileCount)
	})

	t.Run("surrounding whitespace is tolerated", func(t *testing.T) {
		input := []byte("\n  {\"manifestId\": \"m-1\"}\n")

		result, err := parseIngestResult(input)
		require.NoError(t, err)
		assert.Equal(t, "m-1", result.ManifestID)
	})

	t.Run("empty manifest ID is rejected", func(t *testing.T) {
		input := []byte(`{"rootObjectId": "o-1"}`)

		_, err := parseIngestResult(input)
		require.ErrorIs(t, err, errEmptyManifestID)
	})

	t.Run("invalid JSON is rejected", func(t *testing.T) {
		_, err := parseIngestResult([]byte("not json"))
		require.Error(t, err)
	})
}

func TestPgBaseBackupClientEnvironmentVariables(t *testing.T) {
	t.Run("without password", func(t *testing.T) {
		c := &PgBaseBackupClient{}
		env := c.environmentVariables()
		assert.Contains(t, env, "KOPIA_CHECK_FOR_UPDATES=false")
		assert.NotContains(t, env, "KOPIA_PASSWORD=")
	})

	t.Run("with password", func(t *testing.T) {
		c := &PgBaseBackupClient{Password: "mtls"}
		env := c.environmentVariables()
		assert.Contains(t, env, "KOPIA_CHECK_FOR_UPDATES=false")
		assert.Contains(t, env, "KOPIA_PASSWORD=mtls")
	})
}
