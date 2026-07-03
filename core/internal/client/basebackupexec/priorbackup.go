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
	"errors"
	"fmt"

	"github.com/cloudnative-pg/klio/core/internal/client/klioclient"
)

var (
	// ErrPreviousBackupNotFound is returned when a previous backup selected by
	// name does not exist for the cluster.
	ErrPreviousBackupNotFound = errors.New("the requested previous backup does not exist")

	// ErrPreviousBackupUnusable is returned when an explicitly requested previous
	// backup cannot serve as an incremental base.
	ErrPreviousBackupUnusable = errors.New("the requested previous backup cannot serve as an incremental base")
)

// SelectPriorBackup chooses the backup to use as the incremental base: the one
// named by previousBackupName (an ErrPreviousBackupNotFound error when it does
// not exist), or the most recent backup otherwise. A nil result with a nil error
// means auto-detection found no backup, so the caller should fall back to a full
// backup.
func SelectPriorBackup(
	backups klioclient.BackupList, previousBackupName string,
) (*klioclient.BackupMetadata, error) {
	if previousBackupName == "" {
		return backups.GetLatestBackup(), nil
	}

	prior := findBackupByName(backups, previousBackupName)
	if prior == nil {
		return nil, fmt.Errorf("%w: %q", ErrPreviousBackupNotFound, previousBackupName)
	}

	return prior, nil
}

// ValidatePriorForIncremental reports whether prior can seed an incremental
// backup of a cluster with the given system identifier. A prior from a different
// cluster lineage cannot serve as a combine base. Whether the prior is a valid
// ancestor on the current timeline history is left to PostgreSQL, which
// validates it when the incremental BASE_BACKUP runs. The returned reason is
// suitable for a log message.
func ValidatePriorForIncremental(prior *klioclient.BackupMetadata, systemID int64) (bool, string) {
	if systemID != 0 && prior.SystemID != systemID {
		return false, fmt.Sprintf(
			"previous backup belongs to a different cluster (system identifier %d, current %d)",
			prior.SystemID, systemID)
	}

	return true, ""
}

// findBackupByName returns the backup with the given name, or nil when none
// matches.
func findBackupByName(backups klioclient.BackupList, name string) *klioclient.BackupMetadata {
	for i := range backups {
		if backups[i].Name == name {
			return &backups[i]
		}
	}

	return nil
}
