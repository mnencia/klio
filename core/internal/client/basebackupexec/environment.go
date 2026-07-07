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
	"fmt"
)

// ErrIncrementalRequiresManifest is returned when an incremental backup is
// requested without a prior manifest to upload.
var ErrIncrementalRequiresManifest = errors.New("incremental backup requires a previous manifest")

// ErrIncrementalRequiresPG17 is returned when an incremental backup is requested
// against a server older than PostgreSQL 17, which does not support
// UPLOAD_MANIFEST or the INCREMENTAL BASE_BACKUP option.
var ErrIncrementalRequiresPG17 = errors.New("incremental backup requires PostgreSQL 17 or newer")

// ErrUnknownTablespace is returned when a tablespace archive's OID has no
// corresponding name in the environment's tablespace list. The name comes
// from a pg_tablespace query taken before the BASE_BACKUP protocol call,
// which independently reports its own OID/path list later in the same run;
// an unknown OID means a tablespace was created or renamed in between. That
// is not safe to ignore: an empty name would tag the resulting snapshot
// indistinguishably from any other tablespace hitting the same race, and
// restore's tag-based snapshot lookup could then pick the wrong one. Failing
// here instead makes that collision structurally impossible.
var ErrUnknownTablespace = errors.New("tablespace has no known name (created or renamed during the backup)")

// pg17VersionNum is the server_version_num for the first PostgreSQL release that
// supports incremental BASE_BACKUP.
const pg17VersionNum = 170000

// validateIncrementalPreconditions checks that an incremental backup can run
// against the given environment: the server must be PostgreSQL 17 or newer and a
// prior backup_manifest must be available to upload.
func validateIncrementalPreconditions(env *environment, opts Options) error {
	if env.serverVersionNum < pg17VersionNum {
		return fmt.Errorf("%w: server reports version %d",
			ErrIncrementalRequiresPG17, env.serverVersionNum)
	}
	if len(opts.PreviousManifest) == 0 {
		return ErrIncrementalRequiresManifest
	}

	return nil
}

// environment holds metadata that the BASE_BACKUP protocol does not provide and
// that must be read from a standard SQL connection.
type environment struct {
	// pgData is the data directory path on the source server.
	pgData string

	// segmentSize is the WAL segment size in bytes.
	segmentSize uint64

	// systemID is the cluster system identifier, used to reject an incremental
	// base that belongs to a different cluster lineage.
	systemID int64

	// serverVersionNum is the server_version_num (e.g. 170004 for PostgreSQL
	// 17.4), used to gate features such as incremental backup.
	serverVersionNum int

	// tablespaceNames maps a tablespace OID to its name.
	tablespaceNames map[string]string
}

// tablespaceName returns the name recorded for oid when the environment was
// read. See ErrUnknownTablespace for why an unknown OID is an error rather
// than an empty name.
func (env *environment) tablespaceName(oid string) (string, error) {
	name, ok := env.tablespaceNames[oid]
	if !ok {
		return "", fmt.Errorf("%w: OID %q", ErrUnknownTablespace, oid)
	}

	return name, nil
}

// readEnvironment collects the metadata needed to assemble the backup result.
func (e *Executor) readEnvironment(ctx context.Context) (*environment, error) {
	env := &environment{tablespaceNames: map[string]string{}}

	if err := e.sqlConn.QueryRow(ctx, "SHOW data_directory").Scan(&env.pgData); err != nil {
		return nil, fmt.Errorf("while reading data_directory: %w", err)
	}

	if err := e.sqlConn.QueryRow(
		ctx, "SELECT bytes_per_wal_segment FROM pg_control_init()",
	).Scan(&env.segmentSize); err != nil {
		return nil, fmt.Errorf("while reading WAL segment size: %w", err)
	}

	if err := e.sqlConn.QueryRow(
		ctx, "SELECT system_identifier FROM pg_control_system()",
	).Scan(&env.systemID); err != nil {
		return nil, fmt.Errorf("while reading system identifier: %w", err)
	}

	if err := e.sqlConn.QueryRow(
		ctx, "SELECT current_setting('server_version_num')::int",
	).Scan(&env.serverVersionNum); err != nil {
		return nil, fmt.Errorf("while reading server version: %w", err)
	}

	rows, err := e.sqlConn.Query(
		ctx,
		`SELECT oid::text, spcname::text FROM pg_tablespace WHERE pg_tablespace_location(oid) <> ''`,
	)
	if err != nil {
		return nil, fmt.Errorf("while reading tablespaces: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid, name string
		if err := rows.Scan(&oid, &name); err != nil {
			return nil, fmt.Errorf("while scanning tablespace row: %w", err)
		}
		env.tablespaceNames[oid] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("while iterating tablespaces: %w", err)
	}

	return env, nil
}
