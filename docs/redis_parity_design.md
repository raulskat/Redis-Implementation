# Redis Parity Design Blueprint

> _Objective_: evolve this Redis-Implementation into an industrial-grade cache
> that can stand shoulder-to-shoulder with upstream Redis, and leave behind a
> reusable playbook for projects that depend on Redis semantics or embed it as a
> service component.

---

## 1. Product Goals & Non-Functional Targets

- **Protocol compatibility**: Support RESP2 and RESP3 negotiation, command set,
  replication handshakes, and clustering messages so existing clients work
  unmodified.
- **Correctness & durability**: Single primary crash must not lose acknowledged
  writes beyond configured durability (RDB/AOF/Append-Fsync policies).
- **Throughput & latency**: Comparable to open-source Redis on commodity
  hardware; event-loop-driven networking, zero-copy command dispatch, and
  O(1) data structure operations are baseline expectations.
- **Horizontal scale-out**: Support read replicas, sharding (cluster mode), and
  Sentinel-style failover orchestration.
- **Operational excellence**: Deterministic configuration, observability (metrics,
  tracing hooks), security (TLS, ACLs), and hot upgrade paths.
- **Extensibility**: A documented module/plugin interface and a clear Strategy for
  swapping persistence, eviction policies, or transport adapters.

---

## 2. Production Redis Feature Map

| Domain            | Upstream Capabilities                                            | Priority    |
|-------------------|------------------------------------------------------------------|-------------|
| Network / I/O     | Single-threaded event loop (epoll/kqueue), TLS, RESP2/3          | Immediate   |
| Data Structures   | Strings, hashes, lists, sets, sorted sets, streams, bitmaps      | Staged      |
| Persistence       | RDB snapshots, AOF, AOF rewrite, multi-threaded I/O              | Immediate   |
| Replication       | PSYNC2, replica backlog, partial resync, async propagation       | Immediate   |
| Clustering        | Hash slots (16384), cluster bus, gossip, failover, resharding    | Phase 3     |
| High Availability | Sentinel, quorum voting, auto failover                           | Phase 3     |
| Modules           | ABI for modules, data types, timer APIs                          | Phase 4     |
| Observability     | INFO introspection, slowlog, monitoring hooks, command stats     | Immediate   |
| Security          | ACLs, TLS, command renaming, protected mode                      | Phase 2     |

---

## 3. High-Level Architecture (HLD)

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         Redis Process                       │
│                                                             │
│  ┌──────────┐    ┌───────────┐     ┌──────────────┐         │
│  │ Reactor  │───►│ Command   │────►│  Execution   │──┐      │
│  │ (I/O)    │    │ Pipeline  │     │  Engine      │  │      │
│  └──────────┘    │  (parse,  │     └──────────────┘  │      │
│        ▲         │  validate,│            │          │      │
│        │         │  dispatch)│            ▼          │      │
│        │         └───────────┘     ┌──────────────┐ │      │
│        │                            │ Data Store   │◄┘      │
│        └─────── Client sockets ─────┤ (memtables,  │         │
│                                     │  dicts, TTL) │         │
│                                     └──────────────┘         │
│          ┌──────────────┐                   │               │
│          │ Persistence  │◄──────────────────┘               │
│          │  (RDB/AOF)   │                   │               │
│          └──────────────┘                   │               │
│                       │                     ▼               │
│                  ┌────────┐   ┌───────────────────────────┐ │
│                  │Backlog │◄──│ Replication & Propagation │ │
│                  └────────┘   └───────────────────────────┘ │
│                               │           │                 │
│                 ┌─────────────┴─┐   ┌─────┴────────┐        │
│                 │ Cluster Bus / │   │ Sentinel /   │        │
│                 │ Slot Manager  │   │ Orchestrator │        │
│                 └───────────────┘   └──────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Execution Flow (Happy Path)
1. **Networking** – Reactor waits on sockets via `epoll`/`kqueue`, accepts new
   clients, registers interest for read/write events.
2. **Command Pipeline** – RESP parser builds argv, validator chain enforces arity
   and syntax, dispatcher routes to family-specific handlers.
3. **Execution Engine** – Commands run on thread-safe data structures; side
   effects emit domain events (Strategy + Observer patterns).
4. **Persistence** – Durable writes flow into backlog, RDB/AOF modules capture
   state based on configured policies (`appendfsync`, `save`, `dir`).
5. **Replication** – Primary writes replicate via backlog to replicas using PSYNC2
   semantics; replicas acknowledge offsets.
6. **Maintenance** – Background threads handle expiration sampling, AOF rewrite,
   cluster gossip, failover, metrics flush.

---

## 4. Low-Level Design (LLD)

### 4.1 Networking Layer
- **Reactor pattern** with pluggable backends (epoll, kqueue, IOCP on Windows).
- Non-blocking sockets, TCP keepalive, TLS handshake optional.
- Connection state machine: `Greeting → Auth → Command Loop → (Pub/Sub|Closing)`.
- Pub/Sub fanout uses shared buffers, `writev` batching, and high/low watermarks
  for back pressure.

### 4.2 Command Pipeline
- **Parser**: Streaming RESP2/3 parser, supports inline protocol fallback.
- **Validator Chain**: Chain-of-Responsibility enforcing ACLs, syntax, conditional
  modifiers (e.g., NX/XX, GT/LT for ZADD), multi/exec preconditions.
- **Dispatcher**: Hash map keyed by uppercase command name, value contains handler,
  metadata (flags, micro-op selection), instrumentation.
- **Execution Context**: Holds config, data store handles, event bus, persistence,
  replication channels, ACL user context, latency monitors.
- **Observer Hooks**: Write/delete events propagate to replication backlog,
  keyspace notifications, slowlog, StatsD/Prometheus counters.

### 4.3 Data Store
- **Primary dict** – 16k hash table segments (similar to `dict.c`), progressive rehash.
- **Value encodings** – SDS strings, `ziplist`/`quicklist` for lists, `intset` for small sets,
  skiplist + dict for sorted sets, radix tree for streams.
- **Memory allocator** – Jemalloc (default) with idle purge, memory arenas per thread.
- **Expiration** – Mixed passive (on access) + active sampling (10 keys per run).
  Maintain `expires` dictionary keyed by pointer to underlying key to dedupe lookups.
- **Eviction policies** – Strategy pattern: `noeviction`, `volatile-lru`, `allkeys-lfu`, etc.

### 4.4 Persistence Engine
- **RDB Snapshots** – Fork child (posix) or `copy-on-write` thread clamp; incremental
  files using `rdb.c` pipeline. Respect `save` config thresholds.
- **AOF** – Append log, sync policy (`everysec`, `always`, `no`). Background rewrite
  merges base RDB + diff log.
- **Dual Transport** – Option to stream to object storage (S3/GCS) or disk via Strategy
  plug to unify cloud/edge deployments.
- **Consistency Model** – At-least-once by default; optional WAIT command to enforce
  synchronous replication (majority ack).

### 4.5 Replication & Backlog
- **PSYNC2** handshake, `runid` + `repl_offset`.
- **Backlog ring buffer** sized by `repl-backlog-size`.
- **Replica state machine** – `CONNECT` → `SYNC` → `ONLINE`; supports partial resync,
  offsets acked via `REPLCONF ACK`.
- **Async propagation** – Writes passed through command observers, encoded to raw RESP,
  fanout to replicas via backlog + writer threads.
- **Failover** – Local priority & election, once sentinel/cluster integrated.

### 4.6 Clustering & HA (Roadmap)
- **Hash Slot** partition (0..16383), CRC16-based mapping.
- **Cluster Bus** – Gossip channels on port `+10000`, nodes exchange PING/PONG,
  fail reports, slot migrations.
- **Consistent metadata** – Config epoch, failover state, epoch increment on masters.
- **Sentinel** – Active monitor, quorum-based failover, publish `+switch-master`.

### 4.7 Observability & Safety
- **Metrics** – Prometheus and/or StatsD exporters, command latency histograms,
  keyspace hit/miss, memory stats.
- **Logging** – Structured (JSON) logs for ingestion, slowlog ring buffer for long ops.
- **Security** – ACL list, `AUTH`, command renaming, network binding, TLS support.
- **Testing** – Unit (data structures), integration (command semantics), Jepsen-like
  consistency tests, fuzzing (RESP parser).

---

## 5. Design Pattern Inventory

| Pattern                | Current Usage                               | Planned Enhancements                                     |
|-----------------------|----------------------------------------------|----------------------------------------------------------|
| Reactor               | Non-blocking I/O via custom poll loop        | Swap-in libevent/epoll backend abstraction               |
| Command               | Dispatcher table + validators per command    | Dynamic command registration, plugin commands            |
| Strategy              | Persistence (sync/async)                     | Eviction policies, module-defined data encodings         |
| Observer              | Command events for replication, metrics      | Keyspace notifications, pub/sub bridging                 |
| Chain of Responsibility| Validation stack (arity, syntax)            | ACL enforcement, conditional option parsing              |
| Singleton             | Config, event dispatcher, metrics registry   | Global config manager with hot-reload channels           |
| Builder               | Cluster topology, RDB rewrite metadata       | Persisted configuration snapshots                        |
| State                 | Replica connection lifecycle                 | Client connection states (multi/exec, pub/sub, script)   |
| Factory               | Connection state creation                    | Module factories for new data types                      |

---

## 6. Implementation Roadmap

### Phase 0 – Foundation Hardening
- [x] Runtime subsystem registry and reactor-driven server loop
- [x] Command dispatcher + validator metadata scaffolding
- [x] Hash-table datastore with TTL and snapshot support
- [x] RESP handshake scaffolding (`HELLO` 2/3) and replication backlog recorder
- [ ] Backend-agnostic poller abstraction (`epoll`/`kqueue`) with transport adapters
- [ ] Upgrade string/allocator layer (SDS, jemalloc integration)
- [ ] Extend validator metadata and ACL scaffolding

### Phase 1 – Persistence & Replication Parity
- Introduce AOF writer, rewrite pipeline, append-only configuration.
- Implement PSYNC2 with backlog, partial resync, WAIT command semantics.
- RDB child-save using real `fork`/`copy-on-write` (or platform abstraction).
- Observability: INFO sections, slowlog, latency monitor.

### Phase 2 – Data Structures & Scripting
- Port list/set/zset modules, shared integer encodings, streams minimal MVP.
- Lua scripting (embedded LuaJIT/luau) and deterministic EVAL path.
- Module API skeleton ( registering data types, commands, event hooks).

### Phase 3 – Clustering & Sentinel
- Cluster bus, slot map, resharding commands (`CLUSTER MEET`, `ADDSLOTS`).
- Consistency of config epoch, failover elections.
- Sentinel monitors with quorum, `MONITOR`, `NOTIFY`, promotion pipeline.

### Phase 4 – Enterprise Features
- Multi-tenancy (logical DBs with ACL segmentation).
- TLS support, mutual auth, dynamic ACL editing.
- Observability enhancements: OTEL traces, audit logs.
- Pluggable storage tier (semi-persistent caches or disk/index integrations).

---

## 7. Reuse Guidance for Other Projects

1. **Modular Interfaces** – Keep command dispatcher, validators, event bus, and
   datastore interfaces stable; projects embedding Redis behavior can swap in
   custom handlers or storage engines without touching protocol code.
2. **Strategy Registries** – Expose registration for eviction, persistence, and
   replication strategies enabling targeted overrides (e.g., custom LRU scoring).
3. **Event Hooks** – Provide structured events (JSON or protobuf) for cross-process
   consumers to monitor keyspace changes, replication states, or metrics.
4. **Extensible CLI** – Develop a CLI harness (`redis-cli` compatible) plus
   gRPC/HTTP proxies to reuse caching services in polyglot stacks.
5. **Testing Harness** – Build a Jepsen-style suite and fuzzers; reuse across
   embedded deployments to ensure deterministic behavior.

---

## 8. Next Actions

- [ ] Align existing code with this blueprint: adopt production-grade dict/sds,
      finalize reactor backend abstraction, and finish validator pipeline.
- [ ] Craft Epic issues per roadmap phase, each with acceptance criteria and
      load/regression test definition.
- [ ] Establish benchmarking harness (memtier_benchmark, YCSB) to measure progress.
- [ ] Document contribution guidelines and architectural decision records (ADRs).

---

## 9. References (for deeper study)

- _Redis in Action_ by Josiah Carlson.
- Redis source: `src/server.c`, `src/networking.c`, `src/replication.c`.
- Redis docs: persistence (RDB/AOF), replication, cluster, modules API.
- Jepsen analyses of Redis for consistency discussions.
- Patterns of High-Performance Web Services (event loop, batching, lock-free queues).

This blueprint should serve as both a roadmap and a reusable design brief for
future projects that need Redis-like capabilities or embed the server as an
internal cache. By driving toward the milestones above, we can iteratively reach
feature and performance parity with upstream Redis while keeping the codebase
modular, testable, and extensible.
