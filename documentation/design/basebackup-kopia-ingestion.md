# `BASE_BACKUP` protocol ingestion into Kopia

This document explains how Klio backs up a PostgreSQL cluster by reading it over
the PostgreSQL streaming replication protocol (`BASE_BACKUP`) and storing the
result in Kopia. It also explains the PostgreSQL 17+ incremental variant. The
final section maps each part to the code.

Kopia is the content-addressed, deduplicating backup library that Klio stores
backups in.

## Overview

Klio's original base backup reads a locally mounted `PGDATA` as a set of files
and snapshots it with the Kopia command-line tool. The protocol path instead
asks PostgreSQL for the data over a replication connection and streams it
straight into Kopia, with no local staging disk and with Kopia block-level
deduplication preserved.

Both paths coexist. The protocol path is opt-in, and the filesystem path stays
the default, so existing deployments are not affected.

## When to use it

Enable the protocol path when one of its advantages matters. Otherwise the
default filesystem-snapshot path is fine, and for a plain full backup it is the
more efficient choice.

Advantages over the filesystem-snapshot path:

- The main one is PostgreSQL native incremental backup (PostgreSQL 17+), which
  is only available over the protocol. The server uses its WAL summaries to read
  and ship only the blocks changed since the prior backup, so it does not read
  blocks that have not changed. The filesystem path works at file granularity:
  Kopia skips files that are unchanged (by size and modification time), but it
  re-reads any changed file in full to re-hash it, so a large relation segment
  that changed by even one block is read again in full. Each Kopia snapshot
  still stays a self-contained full image, so a restore never replays a chain
  with `pg_combinebackup`.
- It does not need a locally mounted `PGDATA`, so it can back up instances whose
  data volume cannot be mounted: remote, not co-located, or non-CloudNativePG
  PostgreSQL.

Costs:

- The first backup reads the whole database either way, so an initial full
  backup over the stream costs about the same as a first filesystem backup with
  Kopia, though Kopia reads from disk more efficiently. On every full backup the
  stream also gives up Kopia's skip of unchanged files (a CPU cost from
  re-hashing, not a transfer or storage cost), so the protocol path's gain comes
  from incremental backups, not from full ones.
- Switching from the filesystem path to the protocol path pays that full-read
  cost again: the first protocol backup is a full one, because an incremental
  needs a prior backup taken over the protocol (a filesystem-path backup cannot
  serve as the combine base).
- Incremental needs PostgreSQL 17+ with `summarize_wal = on`. See
  [Limitations](#limitations) for the full list.

## Enabling it

The path is chosen for each backup:

- CLI: `klio backup run --use-base-backup-protocol`, with optional
  `--incremental` (PostgreSQL 17+) and `--previous-backup-name <name>`.
- Operator: the `PluginConfiguration` backup options
  (`backup.useBaseBackupProtocol`, `backup.incremental`), which Klio turns into
  the client configuration (`backup.use_base_backup_protocol`,
  `backup.incremental`).

An explicit CLI flag overrides the value set by the operator. Three requests
are rejected with a clear error instead of doing something surprising: asking
for `--incremental` without the protocol path, asking for
`--previous-backup-name` without `--incremental`, and asking for an
incremental against a server older than PostgreSQL 17.

## Architecture

The Kopia Go library is kept out of the main `klio` binary. The PostgreSQL
protocol work stays in `klio`, which already has the pgx and pglogrepl client
libraries. The Kopia work runs in a separate program, `kopia-pgbasebackup`. Klio
runs it with `exec`, the same way it runs the `kopia` command-line tool today,
and sends one tar stream per source on the program's standard input.

A source is the main data directory or one non-default tablespace. Klio runs the
helper once per source: the main data directory first, then one run per
tablespace. This matches how the filesystem path uploads each source.

```
                  PostgreSQL (primary or standby, replication conn)
                  │  BASE_BACKUP [INCREMENTAL]
                  ▼
  ┌─── klio (main binary) ────────────────────────────────────────────────┐
  │  BASE_BACKUP client (pgconn/pglogrepl)                                │
  │   • parses result sets: start/end LSN, TLI, tablespace list           │
  │   • captures the backup_manifest stream (MANIFEST 'yes')              │
  │   • splits the COPY stream into one tar per source                    │
  │   • assembles BackupMetadata (LSN→WAL range)                          │
  └───────────────┬───────────────────────────────────────────────────────┘
                  │ per source: tar stream on stdin + flags
                  ▼
  ┌─── kopia-pgbasebackup (in-repo module, Kopia library) ────────────────┐
  │  reads tar stream → builds a Kopia tree bottom-up → writes objects    │
  │  → SaveSnapshot → prints { manifestId, rootObjectId, stats } as JSON  │
  │  (incremental: combines INCREMENTAL.* against the prior snapshot)     │
  └───────────────┬───────────────────────────────────────────────────────┘
                  ▼
                  Kopia repository (tier1; tier2 relay as today)
```

Klio stores backups in two tiers. Tier1 is the primary Kopia repository that the
helper writes to. Tier2 is a second repository that backups are relayed to
afterwards, for longer retention. The protocol path writes to tier1 and
schedules the tier2 relay the same way the filesystem path does.

### Division of responsibility

| Concern | Owner |
| --- | --- |
| `BASE_BACKUP` / `UPLOAD_MANIFEST` protocol, result-set parsing | `klio` |
| start and end position (LSN, timeline), WAL range metadata | `klio` |
| `backup_manifest` capture and persistence | `klio` |
| splitting the per-tablespace tar streams | `klio` |
| tar to Kopia tree, object and directory writes, `SaveSnapshot` | helper |
| combine-on-ingest for incremental (see below) | helper |
| reading prior snapshot content for incremental | helper |

`backup_label` and `tablespace_map` travel inside the main data tar and are
stored as ordinary files; a restore reads them back from the tree. The position
metadata that Klio records comes from the protocol result sets, not from parsing
the label.

### Helper interface

Kopia identifies each snapshot by a source: a host, a user name, and a path
(together, a `SourceInfo`). The config file passed to the helper is the same one
`klio` uses for the `kopia` tool. It holds the Kopia server address and the
mutual TLS (mTLS) client credentials. The host must match the client
certificate's Common Name (CN). Klio runs the helper as:

```
kopia-pgbasebackup ingest \
  --config-file=<path>          # same Kopia repo config klio already uses
  --host=<clusterName>          # snapshot SourceInfo.Host (must match cert CN)
  --username=<user>             # snapshot SourceInfo.UserName
  --source-path=<logical path>  # snapshot SourceInfo.Path (PGDATA path, or
                                #   /tablespace/<oid>)
  --tag key=value               # repeatable (content=pgdata|tablespace,
                                #   backupName=...)
  [--pin <name>]                # tier2 retention pin (mirrors the current path)
  [--previous-backup-name=<name>]  # incremental only: the combine base's
                                #   backup name; the helper resolves the
                                #   actual snapshot itself, by tag, right
                                #   before combining (see "Selecting the
                                #   prior backup")
  [--incremental]               # incremental only: stdin is an incremental tar
  < <one tar stream on stdin>
```

On success the helper prints a single JSON object on standard output and exits
with status zero:

```json
{
  "manifestId": "...",
  "rootObjectId": "...",
  "stats": { "totalSize": 0, "hashedBytes": 0, "cachedBytes": 0,
             "fileCount": 0, "reusedFiles": 0, "priorReadBytes": 0 }
}
```

`hashedBytes` versus `cachedBytes` shows whether upload deduplication is
effective. `reusedFiles` and `priorReadBytes` cover the read side of an
incremental: how many unchanged files were referenced from the prior object
directly (the fast path above), and how many block bytes were read back from the
prior image to rebuild the rest. Klio logs both when it runs the helper.
Progress and logs go to standard error, read line by line (this includes the
disk-full detection). On failure the exit status is non-zero, and Klio attaches
the last lines of standard error to the error it reports, so the real cause is
visible.

The flags and the JSON object are not the whole contract. The helper writes
snapshots that the `kopia` command-line tool later reads back (restore, list,
metadata), so the two programs also depend on the same on-disk Kopia repository
format and the same tag convention. The helper writes the snapshot tags with the
same `tag:` key prefix the CLI uses, so Klio's tag queries find the backups on
restore. If either side diverged (for example, a Kopia upgrade that changed the
tag format), the helper would still write snapshots, but the CLI would no longer
find them, and a restore would fail with a "no backup found" error. The helper
and the CLI must therefore be built from the same Kopia version (see Build and
packaging).

## Full backup

### Protocol client

Klio issues `BASE_BACKUP` with the write-ahead log (WAL) disabled because Klio
streams WAL on its own (see below), with `MANIFEST 'yes'` so a manifest is kept
for a later incremental, and with the tablespace map. It reads the result sets
for the start position (the timeline ID, or TLI, and the start log sequence
number, or LSN), the tablespace list, and the end position (the end LSN), and it
captures the manifest stream. `BASE_BACKUP` plays the same role as
`pg_backup_start` and `pg_backup_stop`, so those SQL calls are not used on this
path.

The whole backup arrives as one COPY-out stream of typed chunks (new archive,
data, manifest, progress). The client sends each archive to its own helper
process and records the Kopia manifest each source produced. If the stream ends
without the closing position result set, or without the main data directory
archive, the client fails the backup with an error. A truncated stream is never
recorded as a successful backup.

From the start and end LSNs and the timeline, Klio computes the range of WAL
files the backup needs. It then runs the `CloseBackup` gRPC call, as the
filesystem path does, which checks that this WAL range is present and schedules
the tier2 relay.

### Helper ingestion

The helper reads the tar stream once, from start to end, and builds a Kopia
snapshot from the Kopia library's lower-level building blocks instead of its
high-level uploader. The uploader needs a filesystem it can read in parallel and
seek within, so it cannot read from a one-way pipe.

1. Each file member is streamed through a content-addressed object writer (which
   deduplicates blocks on its own). This yields an object ID and a directory
   entry that carries the metadata from the tar header (name, type, size, mode,
   modification time, and ownership).
1. Each entry is added to a per-directory builder, held in a map keyed by
   directory path. Parent directories are created on demand, so a member can
   arrive before its own directory member.
1. At the end of the stream the directories are finalized deepest first. Each
   directory listing is written to an object and added as an entry in its
   parent, so every child is written before its parent.
1. The root is saved with `SaveSnapshot`, and the result JSON is printed.

Because every directory builder is kept until the end, the helper accepts
members in any order, including `pg_basebackup` sending `global/pg_control`
last. An explicit directory member overrides the defaults of a directory that
was created on demand.

Files are ingested one at a time, but each large file is still uploaded in
parallel chunks, so most of the work on big relation segments stays parallel.

### Byte-faithful reconstruction

A restore must produce a cluster that PostgreSQL can start, so the stored tree
reproduces:

- file modes and modification times, taken from the tar headers;
- `backup_label` and `tablespace_map`, as files in the main data tree;
- tablespaces: each tablespace's contents arrive as a separate tar and are
  ingested as their own source (`content=tablespace`, with `SourceInfo.Path` set
  to `/tablespace/<oid>`), which matches the existing `TablespaceLayout` model.
  A restore recreates the symlinks from `tablespace_map`, as before;
- `pg_control`, which arrives inside the main tar.

Because the main tar already carries `backup_label`, `tablespace_map`, and
`pg_control`, the restored tree is self-contained. On this path Klio does not
fetch a separate control-data snapshot and does not rewrite the label. The
backup metadata records this with `BackupMetadata.SelfContainedPgData`, and a
restore uses that flag to pick the right path.

## Incremental backup (PostgreSQL 17+)

### Prerequisites

- `summarize_wal = on` on the source server. Klio checks this directly (`SELECT
  current_setting('summarize_wal')`) before selecting a prior backup at all: if
  it is off, the request fails immediately with a clear error, regardless of
  whether a usable prior exists or the prior was auto-detected or named
  explicitly. Incremental is structurally impossible without it, so there is no
  fallback for this case.
- WAL summaries that still cover the range since the prior backup. PostgreSQL
  prunes summaries after [`wal_summary_keep_time`][pg-wal-summary-keep-time] (10
  days by default). With `summarize_wal` on, Klio checks that the summary
  covering the prior backup's start LSN is still present (via
  `pg_available_wal_summaries()`); if it has been pruned, an auto-detected prior
  falls back to a full backup and an explicitly named prior fails with a clear
  error. This mirrors the server-side requirement described in [Making an
  Incremental Backup][pg-incremental]: "if the necessary files have already
  been removed [...] the incremental backup will fail".
- A stored `backup_manifest` from the prior backup. Every backup captures one
  via `MANIFEST 'yes'`.

`summarize_wal` is a cluster-side setting. The operator exposes it through the
`PluginConfiguration` and `Server` configuration, but Klio does not set it for
the user.

### Protocol flow

1. `UPLOAD_MANIFEST`: send the prior backup's `backup_manifest`.
1. `BASE_BACKUP INCREMENTAL`: the same options as a full backup, plus the
   incremental request, on the same replication connection.
1. Store the new `backup_manifest` for the next incremental.

### Incremental file format

A server-emitted `INCREMENTAL.*` file starts with a header: a magic number, the
block count, the truncation block length, and an ascending array of present
block numbers. The header is padded up to a multiple of `BLCKSZ` (PostgreSQL's
block size, normally 8 KB). The data of the present blocks follows, in array
order. Two cases matter:

- an unchanged file is sent as a zero-block incremental (header only); the
  helper reuses the prior backup's object for it directly, without reading its
  blocks back (see combine-on-ingest), or rebuilds the surviving prefix of a
  truncated file from the prior;
- some files arrive in full even in an incremental backup: a file whose size is
  not a multiple of `BLCKSZ`, a free space map (FSM) fork, a new file, or a file
  where more than 90% of the blocks changed. The helper tells incremental and
  full members apart by the `INCREMENTAL.` filename prefix, and ingests full
  members exactly as in a full backup.

### Combine-on-ingest

Combine-on-ingest means rebuilding the full file from an incremental at the
moment it is ingested. For each `INCREMENTAL.*` member the helper rebuilds the
full image in a single forward pass and feeds it straight into the object
writer. Each output block comes either from the incremental stream (for a
present block) or from the prior full image:

```
open prior file P = prior snapshot's <path> (INCREMENTAL. prefix stripped)
cursor = 0                               # into the ascending present[] array
for i in 0 .. truncation_block_length-1:
    if cursor < nblocks and present[cursor] == i:
        block = next BLCKSZ from the incremental stream   # forward
        cursor++
    else:
        block = P.read_block(i)                           # prior full image
    object_writer.write(block)
```

The incremental data arrives in ascending block order, which matches the forward
walk of `i`, and the prior reads are at increasing offsets. Identical rebuilt
blocks deduplicate against the prior on their own. So the new repository storage
is roughly the changed blocks, while PostgreSQL only read and shipped the
changed blocks.

A file with no present blocks whose length still matches the prior takes a
metadata-only fast path: rebuilding it would read every block back only to
produce an object byte-identical to the prior, so the helper references the
prior object directly instead. No blocks are read, and nothing is hashed again.
Block-level reconstruction plus deduplication would converge on that same
object, so the reference graph (and the depth-1 invariant below) is unchanged;
the fast path only avoids the read-back. In a typical incremental the unchanged
files are the bulk of the data, so this avoids most of the repository reads a
rebuild would otherwise incur. A file that shrank (a shorter truncation) is a
genuine prefix of the prior and still goes through the rebuild above. The helper
reports how many files it reused and how many prior bytes it read back (see the
`reusedFiles` and `priorReadBytes` stats).

The rebuild is strict. It checks that every present block is consumed and that
the incremental stream ends exactly where expected. It also resolves the prior
tree with type checks: each intermediate path component must be a directory, and
the leaf must be a regular file. A malformed input therefore fails with an error
instead of producing a corrupt segment.

### One full image per backup (chain depth 1)

The single-pass rebuild works only because combine-on-ingest produces a full
image on every backup. The prior is therefore always a single complete file, and
the chain depth at ingest is always exactly one. Klio never stores a raw
incremental chain in Kopia: each incremental is combined at ingest against the
immediately-prior full image, read from the repository that the helper already
has open. A restore is the same as for a full backup: one full snapshot, with no
`pg_combinebackup`. (`pg_combinebackup` is multi-pass only to support deeper
chains, which this design never creates.)

This depth-1 property is a hard invariant, not just a convenience. Three parts
of the system rely on it: reconstruction (the single forward pass assumes the
prior is one complete file), restore (it treats every snapshot as a full backup
and never runs `pg_combinebackup`), and retention (any backup can be deleted
because each snapshot stands alone). Do not introduce stored incremental chains:
that would break all three at once.

### Edge cases

- Relation extension: new blocks are always present in the incremental, by
  construction. A block that is neither present nor available from the prior is
  a defensive error.
- Relation truncation: the rebuild stops at the truncation block length and does
  not copy prior blocks beyond it.
- Segmented relations (larger than 1 GB, split into `.1`, `.2`, and so on): each
  segment is an independent tar member with its own header, handled by the same
  loop with no state shared across segments.

### Selecting the prior backup

An incremental uses a single prior backup. Klio reads the prior's stored
`backup_manifest` (sent with `UPLOAD_MANIFEST`) and confirms every Kopia
snapshot the prior contributes as a combine base (its pgdata and each of its
tablespaces) still exists, then identifies the prior to the helper **by backup
name alone** — Klio's `klio backup run --previous-backup-name` flag and the
helper's own `kopia-pgbasebackup ingest --previous-backup-name` flag (see
"Helper interface" above) are distinct flags at different layers that happen
to share a name, both ultimately carrying the same backup name. No Kopia
manifest ID or object ID is ever resolved by Klio or stored across backups
for this purpose: the helper resolves its own combine-base snapshot itself,
by tag (content type, tablespace OID, backup name — never by the current
backup's physical source path, which is not stable across backups), in the
same call that uses it, immediately before the combine runs. Those tags
identify exactly one snapshot for a well-formed backup, so the helper treats
a match of more than one (for example from a reused backup name) as an error
rather than combining against an arbitrary one. This mirrors how restore
already finds a backup's snapshot (by tag, live), and avoids a class of bug
where a stored Kopia-internal identifier goes stale: Kopia reassigns a
snapshot's manifest ID whenever its retention pins change (for example the
tier2-relay unpin), so anything that resolved and cached such an ID earlier
than the moment of use could resolve to a since-superseded, no-longer-existing
ID.

By default the prior is the most recent backup; `klio backup run
--previous-backup-name` selects a specific one.

For an automatically chosen prior, incremental is best-effort: Klio falls back
to a full backup when it cannot use the prior. This happens when there is no
previous backup, when the prior came from the filesystem path and has no
stored `backup_manifest` (only the BASE_BACKUP protocol path produces one),
when the prior belongs to a different cluster lineage, when any of its Kopia
snapshots (pgdata or a tablespace) no longer exists, or when the WAL summaries
covering it have been pruned past `wal_summary_keep_time`. Every combine base
is checked up front because the prior `backup_manifest` is uploaded as a whole:
PostgreSQL then streams every file it lists in incremental form with no
per-source fallback once streaming starts, so a combine base that went missing
would otherwise fail the whole backup mid-stream instead of degrading to a full
one here. When `--previous-backup-name` names a backup that does not exist or
cannot serve as a base, that is an error: an explicit request cannot be honored
silently. Repository errors are always errors.

A disabled `summarize_wal` is not part of this best-effort list: it fails the
request unconditionally, before prior selection even runs, since no choice of
prior backup can make incremental possible on a server that never produces WAL
summaries at all (see [Prerequisites](#prerequisites)).

## WAL handling

WAL handling does not change. WAL is streamed by the existing receiver, and
`BASE_BACKUP` is issued with WAL disabled so the backup does not duplicate it.
As on the filesystem path, the `CloseBackup` check confirms that the WAL range
covering the backup is present.

## Storage layout

- The `backup_manifest` is stored as a single-file snapshot (`content=manifest`,
  tagged with the backup name), next to the metadata snapshot and pinned with
  it. Retention treats it like the metadata.
- No per-source Kopia identifier is recorded in the backup metadata. The
  incremental combine base is identified by backup name only and resolved
  live by the helper at combine time (see "Selecting the prior backup").

The repository layout and the restore format do not change relative to the
filesystem path.

## Build and packaging

The helper is its own Go module at `core/kopia-pgbasebackup`. It is nested under
`core/` so it is inside the container build context, but it stays a separate
module so its dependencies do not leak into `core` or `operator`. It imports the
Kopia library under the canonical path `github.com/kopia/kopia`, with a
`replace` directive that points at the Klio fork (`github.com/leonardoce/kopia`)
until the patches merge upstream. The helper therefore tracks the same patched
library as the Kopia CLI. Both the CLI and the helper are built in the same
`core/Dockerfile` builder stage, and `core/internal/kopia` finds the helper at
runtime (`LookupPgBaseBackupBinary`).

Go does not descend into nested modules, so the helper is built, tested, and
linted on its own. See AGENTS.md for the commands and the dedicated CI tasks.

## Limitations

- Incremental requires PostgreSQL 17 or newer and `summarize_wal = on`.
- Server-side compression (the `BASE_BACKUP COMPRESSION` option) is not
  supported. The helper expects an uncompressed tar stream.
- A full backup over the stream gives up Kopia's skip of unchanged files. That
  is a CPU cost only, not a transfer or storage cost, and incremental backup
  avoids it.

## Code map

- Protocol client and COPY-stream parsing: `core/internal/client/basebackup/`
  (`client.go`, `protocol.go`).
- Backup orchestration (drives the client, the helper, and the metadata):
  `core/internal/client/basebackupexec/` (`executor.go`, `environment.go`).
- CLI wiring and prior-backup selection: `core/cmd/backup/run.go`.
- Helper invocation from `klio`: `core/internal/kopia/pgbasebackup.go` and
  `core/internal/client/klioclient/kopia/pgbasebackup.go`.
- Ingestion helper (separate module): `core/kopia-pgbasebackup/` (`main.go`,
  `internal/ingest/{ingest,incremental,prior}.go`).
- Restore (self-contained-tree handling):
  `core/internal/client/klioclient/restore.go`.
- Operator configuration surface:
  `operator/api/v1alpha1/plugin_configuration_types.go`,
  `operator/internal/klioconfig/config.go`.
- WAL receiver (the model for the protocol client):
  `core/internal/client/sendwal/receiver.go`.
- Build: `core/Dockerfile`.
- PostgreSQL source references: `src/backend/backup/basebackup_incremental.c`
  (incremental writer), `src/bin/pg_combinebackup/reconstruct.c` (reader), and
  `pg_basebackup` (`receivelog.c`, `bbstreamer_*`).
- PostgreSQL documentation: [Making an Incremental Backup][pg-incremental],
  [`pg_basebackup --incremental`][pg-basebackup],
  [`pg_combinebackup`][pg-combinebackup] and its
  [limitations][pg-combinebackup-limits], the [`summarize_wal`][pg-summarize-wal]
  and [`wal_summary_keep_time`][pg-wal-summary-keep-time] settings, and the
  [WAL summarization functions][pg-wal-summary-funcs]
  (`pg_available_wal_summaries()`).

[pg-incremental]: https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-INCREMENTAL-BACKUP
[pg-basebackup]: https://www.postgresql.org/docs/current/app-pgbasebackup.html
[pg-combinebackup]: https://www.postgresql.org/docs/current/app-pgcombinebackup.html
[pg-combinebackup-limits]: https://www.postgresql.org/docs/current/app-pgcombinebackup.html#APP-PGCOMBINEBACKUP-LIMITATIONS
[pg-summarize-wal]: https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-SUMMARIZE-WAL
[pg-wal-summary-keep-time]: https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-SUMMARY-KEEP-TIME
[pg-wal-summary-funcs]: https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-BACKUP
