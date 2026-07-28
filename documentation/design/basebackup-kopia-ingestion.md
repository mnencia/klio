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
             "fileCount": 0, "reusedFiles": 0, "priorReadBytes": 0,
             "referencedSegments": 0, "rebuiltSegments": 0 }
}
```

`hashedBytes` versus `cachedBytes` shows whether upload deduplication is
effective. `reusedFiles` and `priorReadBytes` cover the read side of an
incremental: how many unchanged files were referenced from the prior object
directly (the fast path above), and how many block bytes were read back from the
prior image to rebuild the rest. `referencedSegments` and `rebuiltSegments` show
the same split inside changed files: storage segments taken from the prior by
reference versus rebuilt (see combine-on-ingest). Klio logs them when it runs
the helper.
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

1. Each file member is cut into fixed-size storage segments, each streamed
   through its own content-addressed object writer (which deduplicates blocks on
   its own), and the file's object is a seek table over those segments (see
   [Combine-on-ingest](#combine-on-ingest), which is why files are stored this
   way). This yields an object ID and a directory entry carrying the metadata
   from the tar header (name, type, size, mode, modification time, and
   ownership).
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

The tar stream is read by a single goroutine, in order, but the storage segments
it cuts the stream into are written to the repository by a pool of writers (16
by default). That is not a CPU optimization.
Writing a segment through the Kopia repository server is latency-bound: the
client hashes the segment locally, then blocks on a synchronous existence check
(the server's deduplication pre-check, which the Kopia gRPC client performs for
every write of 50 KB or more) before streaming the write itself asynchronously.
At 128 KB segments that round trip dominates the per-segment cost, and on a full
backup it seldom hits, since the data was just hashed for the first time. So a
sequential writer spends most of a backup waiting, and overlapping the writes
hides it: measured against a real Kopia server, a 2 GiB stream went from 14.3 s
with one writer to 5.3 s with 16 (147 to 383 MiB/s), the latter being what that
repository sustains with multi-megabyte objects. The useful concurrency is
therefore set by the round-trip time rather than by the core count.

A segment rebuilt from an `INCREMENTAL.*` member goes through the same pool. It
is assembled in a pool buffer on the reader goroutine, because its two sources
have to be read in order (the changed blocks from the tar stream, the unchanged
ones from the prior image through a single seeking reader), and only the write
is handed off. That still removes one of the two round trips a rebuilt segment
pays, overlapping its write with the next segment's reads: 2000 rebuilt segments
went from 4.9 s to 3.4 s, a saving of 0.76 ms each. Overlapping the prior-image
reads too would need a reader per worker, which is not done (see
[Limitations](#limitations)).

The reader also does not stop at a file boundary. Waiting there would drain the
pipeline once per file and cap the useful concurrency at the number of segments
one file happens to have: a 256 KB relation is two segments, so waiting for it
means two writes in flight instead of sixteen. Instead a file whose bytes have
all been handed off is queued, and the reader resolves that queue between
members, taking the ones whose segments have landed. Resolving a file is what
writes its seek table, counts it, and files its directory entry.

This is safe because a directory is only built at end of stream, and Kopia sorts
a directory's entries when it builds the manifest, so a file resolved after the
reader has moved past it lands in exactly the manifest it would have. Two rules
keep the rest honest: a segment's buffer is owned by one party at a time (the
reader while it fills it, then one writer until the segment is stored), and a
file's segments are recorded by index, so neither a file's segment order nor the
tree depends on the order the writers finish in. The queue is drained before
the directories are finalized, and capped so that a single lagging file cannot
let it grow without bound.

Removing that per-file wait is worth about one round trip per file, which is the
term that grows when the repository server is not next door: measured on that
same server (round trip ~0.6 ms), 2000 files of 256 KB went from 2.9 s to 1.7 s
once the wait was gone, a saving of 0.61 ms per file. A single large file sees
no change, as it never waited for anything but its own tail.

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

Combine-on-ingest means producing the full file from an incremental at the
moment it is ingested, so every backup is a complete image (see chain depth
below). It works at *storage-segment* granularity rather than rebuilding the
whole file every time.

Every file, full or incremental, is split into a sequence of fixed-size,
block-aligned **storage segments** (128 KB each), each written as its own
object. The file's own object is an *indirect* (seek-table) object over those
segments, which the helper builds directly from the segment object IDs and their
known lengths (`buildFileObject`). That is what makes referencing cheap: an
incremental takes an unchanged segment from the prior backup by object ID alone,
and the file object just lists it. (To avoid clashing with PostgreSQL's own
1 GB relation *file* segments (`.1`, `.2`, and so on, which this document calls
relation segment files), a storage segment is Klio's internal 128 KB unit.)

The helper builds the seek-table itself rather than calling Kopia's
`ConcatenateObjects`, because `ConcatenateObjects` opens every input segment to
rediscover its length (compression and padding hide it), which on the Kopia
server path re-reads the whole backup's data. The helper already knows each
segment's length at write time, so it writes the seek-table with no read-back.

For each `INCREMENTAL.*` member the helper classifies each storage segment by
whether it holds a present (changed) block, then references the clean ones and
rebuilds the dirty ones:

```
S = storage-segment size in blocks (128 KB / BLCKSZ)
for each segment covering blocks [k·S, (k+1)·S):
    if the segment has no present block:
        take the prior backup's segment k by reference     # no readback
    else:
        rebuild it block by block, as a full backup would:
            present block -> next BLCKSZ from the incremental stream (forward)
            absent  block -> prior image block (read back)
concatenate the segments (referenced and rebuilt) into the file object
```

So the read-back shrinks from *every* unchanged block of a changed file to only
the unchanged blocks inside its dirty segments; a change confined to part of a
large relation touches only the segments around it. Identical rebuilt blocks
still deduplicate against the prior on their own. A file with no present blocks
whose length matches the prior skips this entirely and takes the whole-file
fast path: the helper references the prior object directly, reading and hashing
nothing. A file whose every segment is dirty degrades to the old full-file
rebuild, never worse. A file that shrank (a shorter truncation) is a genuine
prefix of the prior and is rebuilt. The helper reports the split as the
`reusedFiles`, `priorReadBytes`, `referencedSegments`, and `rebuiltSegments`
stats.

The rebuild is strict. It checks that every present block is consumed and that
the incremental stream ends exactly where expected. It also resolves the prior
tree with type checks: each intermediate path component must be a directory, and
the leaf must be a regular file. A malformed input therefore fails with an error
instead of producing a corrupt object.

### The segment map

An incremental needs the prior backup's per-file segment object IDs and the
segment size they were written at. It resolves these from a **segment map**: a
compressed directory-tree snapshot each backup writes, scoped and tagged like
the source's data snapshot (`content=<source>-segmap`, plus the backup name), so
it is resolved by tag and reaped with the backup like the `backup_manifest`. The
root holds the segment size plus one subdirectory per file, whose entries are
that file's storage segments in order.

The map is stored as a tree, not a flat list of IDs, so it stays compact (the
manifests compress well and each object ID appears once) and self-describing.
The tree entries also keep the segment objects garbage-collection-reachable, but
that is redundant: `buildFileObject` references each segment object directly (it
does not flatten their indexes), so the data snapshot's file object already
keeps every referenced segment reachable, including a multi-content segment's
seek-table. The map's job is the mapping, not GC-rescue.

The map is **self-describing**: it records the segment size, so an incremental
references prior segments only when that size matches the one configured now.
Otherwise it references nothing and rebuilds every segment at the current size,
a byte-correct full rebuild rather than a spliced mismatch, and references
re-enable on the next backup. The size is fixed at 128 KB and not
user-configurable: it balances scattered-change repository growth (smaller is
better) against object and index count (larger is better).

### One full image per backup (chain depth 1)

Combine-on-ingest produces a full image on every backup, whether it referenced
prior segments or rebuilt them. The prior is therefore always a single complete
file, and the chain depth at ingest is exactly one. Klio never stores a raw
incremental chain in Kopia: each incremental is combined at ingest against the
immediately-prior full image, read from the repository that the helper already
has open. A restore is the same as for a full backup: one full snapshot, with no
`pg_combinebackup`. (`pg_combinebackup` is multi-pass only to support deeper
chains, which this design never creates.)

This depth-1 property is a hard invariant, not just a convenience. Three parts
of the system rely on it: reconstruction (rebuilding a dirty segment assumes the
prior is one complete file), restore (it treats every snapshot as a full backup
and never runs `pg_combinebackup`), and retention (any backup can be deleted
because each snapshot stands alone). Do not introduce stored incremental chains:
that would break all three at once.

### Edge cases

- Relation extension: a block below the truncation length that is present in
  neither the incremental nor the prior image is one PostgreSQL extended the
  file to but never WAL-logged (an uninitialized page). It is reconstructed as
  a zero-filled block, matching PostgreSQL's `pg_combinebackup`, and counted in
  the ingest stats (`zeroFilledBlocks`). See Limitations. A block that cannot be
  read from the prior for a genuine reason (an I/O error) is still a hard error.
- Relation truncation: the rebuild stops at the truncation block length and does
  not copy prior blocks beyond it.
- Relation segment files (a relation larger than 1 GB, split into `.1`, `.2`,
  and so on): each is an independent tar member with its own header, split into
  storage segments and combined on its own, with no state shared across files.

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
- Each source's segment map is stored as a compressed directory-tree snapshot
  (`content=<source>-segmap`, tagged with the backup name) and reaped with the
  backup like the manifest. It records that source's per-file storage-segment
  object IDs and keeps them GC-reachable (see "The segment map").
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
- Files are stored as fixed 128 KB storage segments, and the size is not
  configurable. An incremental rebuilds every segment that contains a changed
  block, so a change scattered uniformly across a relation grows the repository
  by roughly the touched segments rather than the logical change size; a
  localized change is far cheaper.
- A rebuilt segment's *write* is overlapped like any other, but its reads of the
  prior image are not: they go through one seeking reader per file, advanced in
  order, so each rebuilt segment still pays a serialized round trip to fetch the
  prior segment it is rebuilt from. Overlapping those too means giving each
  writer its own prior reader, which is worth doing only if the read-back itself
  stays (see the storage-amplification limitation above).
- Incremental reconstruction, like PostgreSQL's `pg_combinebackup`, cannot
  distinguish a genuinely uninitialized (all-zero) extension page from one whose
  modification the WAL summaries failed to record. A below-truncation block
  present in neither the incremental nor the prior image is assumed
  uninitialized and materialized as zeros; WAL replay reinitializes such pages
  on recovery. The `zeroFilledBlocks` ingest stat surfaces how often this
  happens so an anomaly is visible rather than silent.

## Code map

- Protocol client and COPY-stream parsing: `core/internal/client/basebackup/`
  (`client.go`, `protocol.go`).
- Backup orchestration (drives the client, the helper, and the metadata):
  `core/internal/client/basebackupexec/` (`executor.go`, `environment.go`).
- CLI wiring and prior-backup selection: `core/cmd/backup/run.go`.
- Helper invocation from `klio`: `core/internal/kopia/pgbasebackup.go` and
  `core/internal/client/klioclient/kopia/pgbasebackup.go`.
- Ingestion helper (separate module): `core/kopia-pgbasebackup/` (`main.go`,
  `internal/ingest/{ingest,incremental,prior,segmap,segwriter}.go`).
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
