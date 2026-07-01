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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		err := validateIncrementalRequiresProtocol(true, "", false)
		require.ErrorIs(t, err, ErrIncrementalRequiresProtocol)
	})

	t.Run("previous backup name without protocol is rejected", func(t *testing.T) {
		err := validateIncrementalRequiresProtocol(false, "base-1", false)
		require.ErrorIs(t, err, ErrIncrementalRequiresProtocol)
	})

	t.Run("incremental with protocol is allowed", func(t *testing.T) {
		require.NoError(t, validateIncrementalRequiresProtocol(true, "base-1", true))
	})

	t.Run("neither requested is allowed", func(t *testing.T) {
		require.NoError(t, validateIncrementalRequiresProtocol(false, "", false))
	})
}
