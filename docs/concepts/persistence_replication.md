# Persistence & Replication Concepts

Redis-Implementation provides snapshot-based durability today and scaffolding for replication tomorrow. This guide details the design decisions, data flow, and APIs that govern these subsystems.

---

## Persistence Overview

* **Goal:** Produce Redis-compatible RDB snapshots on demand (`SAVE`) or in the background (`BGSAVE`).
* **Components:** `src/persistence.c`, `src/rdb.c`, `include/persistence.h`, `include/rdb.h`.
* **Concurrency:** A single background save can run at a time, coordinated by a global mutex (`save_lock`).

---

## SAVE vs. BGSAVE

| Command | Mode | Blocking? | Notes |
|---------|------|-----------|-------|
| `SAVE` | Synchronous | Yes | Called via `persistence_execute_sync`. Clients wait until the snapshot completes or errors out. |
| `BGSAVE` | Asynchronous | No | `pthread_create` launches a worker thread (`background_save`). Client gets immediate status. |

Both paths share `perform_save`, which:

1. Captures a snapshot via `datastore_snapshot`.
2. Writes the RDB file using `rdb_save` (respects `--dir` and `--dbfilename` from config).
3. Frees snapshot entries with `datastore_snapshot_free`.

`begin_save` and `finish_save` wrap the critical section to prevent overlapping saves. If a save is in progress, the functions return `EBUSY` so callers can react appropriately.

---

## RDB Format (Simplified)

Our RDB writer currently emits:

1. Header with Redis-style magic string and version.
2. For each key:
   * Optional expiration timestamp.
   * Type byte (string).
   * Key length and contents.
   * Value length and contents.
3. Footer checksum placeholder (future enhancement).

While minimal, this layout keeps us compatible with basic Redis tooling and forms the foundation for more complex types later.

---

## Error Handling

* `persistence_save` returns `-1` on I/O errors and propagates `errno` (e.g., `EACCES`, `ENOENT`).
* Command handlers translate these into RESP strings:
  * `ERR Background save already in progress`
  * `ERR save failed`
  * `Background saving started`
* Background saves print status messages to stdout. Hook into a logging framework when available.

---

## Scheduling Saves

Future work could introduce:

* **Timed saves:** Triggered by cron-like intervals or write counts.
* **Replication-driven saves:** Master produces snapshots for replicas automatically when needed.
* **AOF (Append Only File):** Complementary persistence mode, not yet implemented.

Document scheduling changes here when implemented.

---

## Replication Architecture (Current State)

Replication is under construction. Existing components include:

* **Backlog:** Linked list of serialized command payloads (`replication_backlog_entry_t`) appended via `backlog_append`. This will feed replicas once streaming is enabled.
* **Handshake handlers:** `handle_replconf_command` and `handle_psync_command` respond to replica negotiation, returning Redis-compatible replies (`+OK`, `FULLRESYNC`, etc.).
* **Background thread stub:** `replication_start` is responsible for launching replication-specific workers (future feature).

---

## Planned Replication Flow

1. **Replica connects** and sends `PING`/`HELLO`/`AUTH` as any client.
2. **Capability exchange** via `REPLCONF` to advertise `psync2` support.
3. **`PSYNC` request** with master replication ID and offset.
4. **Master reply:**
   * Full resync (current behavior) → send `FULLRESYNC` + snapshot.
   * Partial resync (future) → stream backlog entries from the requested offset.
5. **Continuous streaming** of write commands to replica sockets, ensuring ACK handling and backpressure awareness.

---

## Replication Backlog

* Located in `src/replication.c`; protected by `backlog_lock`.
* Entries store RESP-formatted command strings ready to be sent verbatim to replicas.
* `replication_backlog_stream(int fd)` (planned implementation) writes the backlog to a replica socket atomically using `send_all`.
* Configurable retention policies (size limit, trimming) will be needed before partial resync can ship.

---

## Consistency Guarantees

* **Durability:** With only RDB snapshots, writes acknowledged after a `SAVE` are durable; writes after that point may be lost if the process crashes before the next snapshot. Introduce AOF or higher-frequency saves for stronger guarantees.
* **Replica lag:** Until live streaming is implemented, replicas rely on full resync and may be stale between snapshots.

---

## Extending Persistence & Replication

| Goal | Considerations |
|------|----------------|
| Append-Only File (AOF) | Requires write command logging, fsync policies, rewrite pipeline. |
| Multi-threaded saves | Offload serialization to workers, ensure datastore snapshot remains consistent. |
| Partial resync | Track offsets, flush backlog when replicas disconnect and reconnect quickly. |
| TLS replication | Wrap sockets with TLS context; coordinate certificates and ACL. |

Ensure documentation is updated and tests are added when pursuing these enhancements.

---

## Troubleshooting

| Problem | Clue | Fix |
|---------|------|-----|
| `ERR Background save already in progress` | Concurrent `SAVE`/`BGSAVE` requests. | Wait for background save to finish or add queuing logic. |
| Snapshot file missing | Wrong `--dir` or `--dbfilename`. | Verify config, ensure directory exists and is writable. |
| Replicas stuck waiting | Streaming not implemented yet. | Track progress in roadmap; consider manual `SAVE` + copy as workaround. |
| High pause times during SAVE | Large dataset or slow disk. | Schedule saves during low-traffic periods; explore incremental snapshotting. |

---

## Related Reading

* [`data_storage.md`](data_storage.md) — Core datastore implementation used by persistence.
* [`docs/redis_parity_design.md`](../redis_parity_design.md) — Roadmap items for durability and replication parity.
* Official Redis documents on persistence: <https://redis.io/docs/management/persistence/>
