# Data Storage Concepts

Redis-Implementation maintains a minimalist in-memory datastore with TTL support and snapshotting. This document explains the structures, algorithms, and design trade-offs behind that layer.

---

## Overview

* **Key-value engine:** Hash table with chaining (`redis_store_t` in `src/datastore.c`).
* **Values:** Simple strings backed by SDS (Simple Dynamic Strings) for Redis compatibility.
* **TTL:** Absolute millisecond timestamps; both passive (on access) and active (background) expiration.
* **Snapshots:** RDB-style serialization for persistence and replication handoffs.

---

## Hash Table Architecture

* `redis_store_t` owns an array of bucket pointers (`redis_entry_t **buckets`).
* Bucket index: `hash_key(key) % bucket_count`, where `hash_key` uses 64-bit FNV-1a for good distribution.
* Collision resolution: singly linked list per bucket (`entry->next`).
* `resize_if_needed` doubles the bucket count when the load factor exceeds 0.75.

**Key invariants**

1. `store->lock` must be held when mutating buckets or entries.
2. Strings stored in the datastore are SDS copies; callers may free their buffers immediately after `SET`.
3. Expired keys are removed lazily on lookup to keep closures simple.

---

## SDS (Simple Dynamic Strings)

* `sds` is a heap-allocated string with length metadata and room for growth.
* Advantages: O(1) length (`sdslen`), natural fit for Redis wire protocol.
* Conversions: Helper `sds_to_cstring` copies SDS contents into a null-terminated C string when needed (e.g., for RESP replies).
* For now, SDS lives in `src/sds.c` (lightweight implementation). Future optimizations could plug in the full Redis SDS library.

---

## TTL Management

* Each entry has `expiry_ms == 0` when no TTL is set; otherwise it holds an absolute timestamp (`uint64_t`).
* `datastore_expire_in` and `datastore_expire_at` update this field under lock.
* Passive expiration: `datastore_get` invokes `remove_if_expired` before returning a value.
* Active expiration: `src/expiry.c` periodically scans buckets, pruning up to `limit` entries per tick (default 128).
* RESP-level behavior mirrors Redis:
  * `TTL` returns `-2` (missing), `-1` (no TTL), or remaining seconds.
  * `PTTL` returns the same values but in milliseconds.
  * `EXPIRE`, `PEXPIRE`, and `PERSIST` now respond with booleans when RESP3 is enabled.

---

## Snapshotting

* `datastore_snapshot` allocates an array of `redis_snapshot_entry_t`, each containing a key string, value string, and TTL metadata.
* Callers (persistence, replication) must free the snapshot with `datastore_snapshot_free` to avoid leaks.
* Snapshots represent a consistent view at the moment they were captured thanks to datastore locking.
* Only string keys/values are supported today; expand the structure when new data types land (lists, sets, etc.).

---

## Persistence Hooks

* `datastore_set`, `datastore_delete`, and TTL operations return status codes consumed by command handlers.
* `persistence_save` consumes snapshot data to produce an RDB file using `rdb_save`.
* Snapshot format includes:
  * Database number (currently always 0).
  * Key type (string).
  * Expiration timestamp if present.
  * Raw string payload.

---

## Memory Considerations

* Hash table and SDS allocations use the global allocator (libc `malloc` or jemalloc).
* `datastore_prune_expired` helps cap memory usage by removing dead keys proactively.
* When storing large values, consider implementing copy-on-write or segmented storage to avoid long pause times during snapshotting (future work).

---

## Extending the Datastore

1. **New data type:** Define a new enum/type marker, extend `redis_entry_t`, update serialization/deserialization paths, and add command handlers.
2. **Sharding:** Abstract the hash table behind an interface that can target multiple partitions. Ensure TTL and snapshot logic can operate per shard.
3. **Threaded writes:** Introduce finer-grained locking or lock-free structures if the reactor becomes multi-threaded.

_When adding features, always update this document and `docs/redis_parity_design.md` to keep the roadmap aligned._

---

## Troubleshooting

| Issue | Symptoms | Fix |
|-------|----------|-----|
| Memory leak after repeated `SET`/`DEL` | Valgrind shows lost blocks | Ensure new code frees temporary SDS or snapshot buffers. |
| TTL not expiring keys | Keys persist past expected time | Verify system clock, ensure expiration thread is running (`expiry_start`). |
| Snapshot missing key | RDB lacks recently written key | Confirm `datastore_snapshot` saw latest state (writes must complete before SAVE). |
| Crash in `datastore_get` | Null-pointer dereference | Check for `NULL` store pointer or uninitialized `redis_store_t`. |

---

## Related Reading

* [`runtime_architecture.md`](runtime_architecture.md) — how the datastore fits into the broader pipeline.
* Redis design docs on dictionaries and SDS: <https://redis.io/docs/interact/data-types/>.
* `src/datastore.c`, `include/datastore.h` — authoritative source for behavior.
