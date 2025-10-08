# Redis-Implementation Project Guide

> **Mission**  
> Build a production-grade Redis-compatible server that teams can study, extend,
> and embed. This guide chronicles the “why, what, and how” of the project so
> contributors and users can understand the system at a glance and trace its
> evolution over time.

---

## 1. Executive Summary

- **What**: A modular C implementation of Redis, designed to reach functional
  parity with upstream Redis while remaining approachable for learners and
  customizable for internal platforms.
- **Why**:
  - Demystify the Redis internals (networking, persistence, replication).
  - Provide a sandbox for design pattern exploration (Reactor, Command,
    Strategy, Observer, etc.).
  - Supply an embeddable cache that teams can tweak without vendor lock-in.
- **Current Status**:  
  Phase 0 hardening in progress. Reactor-based networking, command dispatcher,
  persistence strategies, and replication observers are in place. The detailed
  parity blueprint (`docs/redis_parity_design.md`) defines the long-term roadmap.

---

## 2. High-Level Architecture

```
Clients ──► Reactor (I/O) ──► Command Pipeline ──► Execution Engine
                                     │                     │
                                     ▼                     ▼
                           Persistence & Replication  In-Memory Store
```

- **Runtime Orchestrator**: Encapsulates configuration and coordinates the bootstrap/shutdown of all subsystems before requests flow through a pluggable subsystem registry.
- **Reactor Layer**: Backend-pluggable event loop (default `poll`) managing non-blocking sockets.
- **Command Pipeline**: RESP parser, validator chain, dispatcher table, event bus.
- **Execution Engine**: Command handlers keyed by feature families (core,
  key-value, expiration, admin, replication).
- **Data Store**: Mutex-protected dictionary of SDS-backed key/value entries with TTL support.
- **Session Layer**: Tracks per-connection RESP protocol version, ACL state (AUTH/HELLO), and optional connection names (persisted when `--persist-connection-names` is enabled).
- **Access Control**: Lightweight ACL manager that gates commands based on capability flags and runtime authentication requirements. Configure credentials via `--requireuser`/`--requirepass`, attach default roles with `--userrole`, and add extra accounts through `--acluser user=pass:roles`. Connections authenticate with `AUTH` or `HELLO AUTH`.
- **Background Services**: Expiration thread, persistence strategies (sync/async),
  replication handshake stub, RDB loader/saver.

### Patterns in Use
| Pattern | Location | Role |
|---------|----------|------|
| Reactor | `src/reactor.c`, `src/server.c` | Non-blocking network processing |
| Command | `src/command.c` | Dispatches RESP commands to handlers |
| Strategy | `src/persistence.c` | Switchable save modes (sync/async) |
| Observer | `src/command_events.c`, `src/replication.c` | Notify replication & metrics |
| Chain of Responsibility | Validator array in `src/command.c` | Arity & syntax enforcement |

---

## 3. Detailed Module Tour

| Module | Files | Highlights |
|--------|-------|------------|
| **Runtime / Main** | `src/runtime.c`, `src/main.c` | Lifecycle orchestration with subsystem registry, configuration, bootstrap/shutdown |
| **Config** | `src/config.c` | CLI arg parsing, default paths, replication flags |
| **Command dispatcher** | `src/command.c`, `include/command_dispatcher.h` | Table-driven handlers, validator chains, event emission |
| **Handlers** | `src/handlers_*.c` | Families: basic, kv, admin, expiration, replication |
| **RESP protocol** | `src/resp.c` | Parser and serializer for RESP messages |
| **Networking** | `src/server.c`, `src/connection.c`, `src/reactor.c`, `src/reactor_poll.c`, `src/reactor_epoll.c`, `src/reactor_kqueue.c` | Reactor loop with pluggable backends, connection state machines |
| **Datastore** | `src/datastore.c`, `include/datastore.h` | Hash-table key/value storage with TTL, snapshotting |
| **SDS Strings** | `src/sds.c`, `include/sds.h` | Simple dynamic strings with optional jemalloc backing |
| **ACL / Auth** | `src/acl.c`, `include/acl.h`, `src/handlers_admin.c` | Requirepass configuration, AUTH handler, command capability flags |
| **Persistence** | `src/persistence.c`, `src/rdb.c` | Snapshot and future AOF hooks |
| **Replication** | `src/replication.c` | Listener registration, backlog stream, master handshake |
| **Expiry** | `src/expiry.c` | Background expiration scheduler |
| **Documentation** | `docs/*.md` | Architecture plan, parity blueprint, concept dictionary, this guide |

Each module is registered via the command context, making it easier to swap components.

---

## 4. Development Roadmap

- **Subsystem Registry**: `runtime_register_subsystem` lets core or optional components (expiry, replication, future modules) hook into startup/shutdown sequencing with clear error handling and optional fallbacks. Optional subsystems can fail without crashing the server; the runtime logs warnings and continues boot, while mandatory subsystems bubble errors before clients connect.

**Phase 0 – Foundation Hardening (current)**  
- [x] Reactor-based I/O layer  
- [x] Command validation chain  
- [x] Persistence Strategy pattern (sync/async)  
- [x] Runtime orchestration layer  
- [x] Hash-table datastore with SDS-backed keys/values  
- [x] Pluggable reactor backends (poll/epoll/kqueue)  
- [x] RESP handshake scaffolding (`HELLO 2/3`) with session protocols  
- [x] SDS string layer with optional jemalloc allocator  
- [x] Command capability flags + per-connection ACL sessions (AUTH/HELLO AUTH)  
- [x] Replication backlog recorder and streaming pipeline  
- [ ] Enable jemalloc by default (post-benchmark)

**Phase 1 – Persistence & Replication Parity**  
- Implement AOF writer + rewrite  
- PSYNC2 replication with backlog  
- WAIT command semantics  
- Observability: INFO sections, slowlog, latency tracker

**Phase 2 – Data Structures & Scripting**  
- Lists, sets, sorted sets, streams  
- Lua scripting support  
- Module API skeleton

**Phase 3 – Clustering & Sentinel**  
- Cluster slot map, gossip bus  
- Sentinel quorum monitoring & failover  
- Config epoch management

**Phase 4 – Enterprise Features**  
- ACLs, TLS  
- Advanced eviction policies  
- Extensible metrics exporters  
- Pluggable storage tier (e.g., tiered caching)

Refer to `docs/redis_parity_design.md` for the in-depth blueprint.

---

## 5. Version History & Milestones

| Version | Date | Highlights |
|---------|------|------------|
| v0.1 (Baseline) | 2025-09 | Modularized legacy `server.c`; RESP parser; basic commands; RDB load |
| v0.2 | 2025-10 | Added persistence strategies, replication listener skeleton, reactor networking, command validator chain |
| v0.3 | 2025-10 | Hash-table datastore, pluggable reactor backends, SDS string layer, ACL/`AUTH` scaffolding, replication backlog |
| v0.4 (Planned) | Q1 2026 | Production-grade dict/jemalloc defaults, AOF writer, PSYNC2 partial resync |
| v1.0 (Target) | TBD | Redis-parity release: cluster, replication, persistence, modules |

Change logs will be maintained in `CHANGELOG.md` once releases are tagged.

---

## 6. Advantages & Strengths

- **Educational**: Clear modular boundaries make it ideal for learning Redis internals.
- **Extensible**: Strategy and observer patterns allow swapping components without invasive changes.
- **Cross-project reuse**: Command dispatcher, persistence layer, and event bus can be embedded in other services.
- **Documentation-first**: Architecture and roadmap documented from the outset for new contributors.

---

## 7. Challenges & Risks

- **Parity scope**: Matching Redis feature-for-feature is a multi-year effort; requires disciplined phased execution.
- **Performance tuning**: Achieving Redis-level throughput demands advanced memory management (jemalloc), CPU pinning, and assembly optimizations.
- **Consistency testing**: Must invest in rigorous integration and Jepsen-style tests to guarantee correctness.
- **Operational tooling**: Sentinel/cluster orchestration adds significant complexity around state transitions and failure handling.

Mitigation: maintain milestone-specific acceptance tests, CI pipelines, benchmarking harnesses, and staged rollouts.

---

## 8. Contributor Workflow

1. **Environment Setup**
   - Linux/WSL dev environment with GCC/Clang, Make.
   - `make` to build, `./redis-server` to run locally.
   - Recommended: `redis-cli` or `nc` for manual testing.
2. **Documentation**  
   - Read `docs/architecture.md` and `docs/redis_parity_design.md`.
   - Use the concept dictionary in `docs/concepts/` when you need background on C patterns, modules, or tooling.
   - Update relevant docs for new features/changes.
3. **Testing**  
   - Unit tests (to be added) for modules.
   - Integration tests with RESP command scripts.
   - Benchmark harness (future: `memtier_benchmark` scripts).
4. **Coding Standards**  
   - C11, `-Wall -Wextra -pedantic` clean builds.
   - Avoid non-portable APIs without abstraction layers.
   - Prefer Strategy/Observer for configurable behavior.
5. **Review & Merge**  
   - Submit PRs referencing roadmap milestones.
   - Provide docs/tests for new features.

---

## 9. Usage Guide

```bash
# Build and run (WSL/Linux)
make                 # default build
make JEMALLOC=1      # optional: link with jemalloc
./redis-server --dir /tmp/rdbfile --dbfilename dump.rdb --port 6380 --requirepass secret

Benchmarking is in flight to determine whether jemalloc should ship as the default allocator; keep building with `JEMALLOC=1` when collecting performance data so we can flip the default with confidence.

# Example commands
printf '*1\r\n$4\r\nPING\r\n' | nc -q1 127.0.0.1 6380
printf '*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n' | nc -q1 127.0.0.1 6380
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc -q1 127.0.0.1 6380
printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc -q1 127.0.0.1 6380
```

Server logs (stdout) display replication observer messages, persistence outcomes, and errors.

---

## 10. Future Documentation Additions

- ADRs (Architecture Decision Records) per major change.
- Detailed design docs for each major subsystem (e.g., persistence, replication, clustering).
- Contributor quick-start and checklist.
- Benchmarking results and regression dashboards.
- Migration guide for users upgrading across versions.

---

## 11. Appendices

### A. Glossary
- **RESP**: Redis Serialization Protocol.
- **RDB**: Redis database snapshot format.
- **AOF**: Append-Only File persistence method.
- **PSYNC2**: Redis replication protocol allowing partial resync.
- **Sentinel**: Redis’s system for monitoring and failover.
- **Cluster**: Horizontal sharding via hash slots.

### B. References
- Official Redis documentation (https://redis.io/docs).
- Redis source tree (https://github.com/redis/redis).
- Redis design blog posts and community talks.
- `docs/redis_parity_design.md` within this repo.

---

This guide should evolve alongside the code. Every milestone should update the
roadmap, strengths/challenges, and version history so future readers can trace
the journey from educational prototype to production-ready Redis alternative.






