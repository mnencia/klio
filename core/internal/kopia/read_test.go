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
)

func TestBuildRestoreSnapshotArgs(t *testing.T) {
	t.Run("parallelism is requested explicitly", func(t *testing.T) {
		// Kopia defaults to --parallel 8, which leaves a segment-ingested restore
		// waiting on the repository server's per-object round trips.
		args := buildRestoreSnapshotArgs("/etc/kopia/config", "snap-1", "/dest")

		assert.Contains(t, args, "--parallel=32")
		assert.Contains(t, args, "--config-file=/etc/kopia/config")
		assert.Equal(t, "restore", args[0])
	})

	t.Run("snapshot ID and destination come last", func(t *testing.T) {
		// They are positional, so a flag appended after them would be misread.
		args := buildRestoreSnapshotArgs("/c", "snap-1", "/dest")

		assert.Equal(t, []string{"snap-1", "/dest"}, args[len(args)-2:])
	})
}
