# Modular Redis Implementation Plan

## Current Goals
1. Make the legacy `server.c` codebase modular.
2. Establish a clean separation between networking, command handling, persistence, replication, and the in-memory store.
3. Define a roadmap of phases to reach a feature-complete Redis clone.

## Target Source Layout
```
+-- include/
|   +-- command.h
|   +-- command_dispatcher.h
|   +-- command_events.h
|   +-- command_handlers.h
|   +-- command_utils.h
|   +-- config.h
|   +-- connection.h
|   +-- datastore.h
|   +-- expiry.h
|   +-- persistence.h
|   +-- replication.h
|   +-- resp.h
|   +-- rdb.h
|   +-- reactor.h
|   +-- runtime.h
|   +-- acl.h
|   +-- sds.h
|   +-- server.h
+-- src/
|   +-- command.c
|   +-- command_dispatcher.c
|   +-- command_events.c
|   +-- command_utils.c
|   +-- config.c
|   +-- connection.c
|   +-- datastore.c
|   +-- handlers_admin.c
|   +-- handlers_basic.c
|   +-- handlers_kv.c
|   +-- handlers_replication.c
|   +-- handlers_expiration.c
|   +-- main.c
|   +-- persistence.c
|   +-- replication.c
|   +-- resp.c
|   +-- rdb.c
|   +-- expiry.c
|   +-- reactor.c
|   +-- reactor_epoll.c
|   +-- reactor_kqueue.c
|   +-- reactor_poll.c
|   +-- runtime.c
|   +-- acl.c
|   +-- sds.c
```

## Module Responsibilities
- **config**: Parse CLI arguments, hold runtime configuration, expose helpers.
- **datastore**: Manage hash-table key/value storage, SDS-backed keys/values, TTL bookkeeping, concurrency control.
- **resp**: Read/write RESP frames, convert socket buffers into argument vectors.
- **command**: Central dispatch table that routes parsed commands to specialised handlers.
- **command handlers**: Implement individual command families (core ops, key/value, expiration, persistence, replication).
- **rdb**: Load the initial dataset from disk and provide helpers for writing RDB snapshots.
- **server**: Register listening sockets with the reactor and glue network activity into the command layer.
- **reactor**: Provide a backend-agnostic event loop abstraction (`poll`, `epoll`, `kqueue`) to multiplex sockets.
- **connection**: Own the per-connection RESP decode/send loop so transport concerns stay isolated.
- **replication**: Handle master/slave negotiation, keep sockets to master alive, stream updates.
- **persistence**: Coordinate synchronous/background saves, manage SAVE/BGSAVE state, and call into the RDB writer.
- **expiry**: Run the active expiration scheduler that periodically prunes stale keys.
- **runtime**: Central application coordinator with a subsystem registry that orchestrates configuration, datastore lifecycle, optional/mandatory subsystem startup, and graceful shutdown.
- **command dispatcher/events**: Maintain the command registry, validation chains, and observer hooks for replication/metrics.
- **acl**: Manage global authentication/authorization state (requirepass, AUTH command) and enforce command capability flags.
- **sds**: Provide a Redis-style dynamic string abstraction with optional jemalloc backing.

## Phase Roadmap
- **Phase 1** - Foundations (complete): Modular code layout, fix parsing bugs, basic RESP command set (PING/ECHO/SET/GET/KEYS/CONFIG/INFO), RDB load, single-threaded correctness with mutex-protected store.
- **Phase 2** - Persistence polish (complete): Implement synchronous/background saves (`SAVE`/`BGSAVE`), expose `FLUSHALL`, and add checksum verification for produced snapshots.
- **Phase 3** - Expiration fidelity (current): Active + passive expiration, support `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` commands.
- **Phase 4** - Replication maturity: Complete partial resync, backlog handling, propagate write commands, support `PSYNC`/`REPLCONF` negotiations.
- **Phase 5** - Advanced commands & modules: Sorted sets, lists, pub/sub, transactions, scripting stub.



Each phase will keep the modules isolated so we can iterate without large-scale rewrites.


