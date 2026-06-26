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

// Package basebackup implements a PostgreSQL streaming-replication client for
// the BASE_BACKUP command. It reads cluster data over the replication protocol
// (instead of snapshotting a locally-mounted PGDATA) and exposes each
// server-produced tar stream to a caller-supplied handler, which pipes it into
// the kopia-pgbasebackup ingestion helper.
//
// The client keeps all PostgreSQL-protocol work in the main klio binary: it
// parses the BASE_BACKUP result sets (start/end LSN and timeline, the
// tablespace list), demultiplexes one tar COPY stream per source, and captures
// the backup_manifest. The Kopia-side ingestion lives in the kopia-pgbasebackup
// helper (a separate binary built from core/kopia-pgbasebackup).
//
// Protocol note: the result-set shape and ordering target the PG15+ "new"
// BASE_BACKUP protocol with client streaming. Result sets are classified by
// shape rather than position, so the dispatcher tolerates ordering differences.
// The exact option set and manifest framing can still vary across PostgreSQL
// major versions (see the design document).
package basebackup
