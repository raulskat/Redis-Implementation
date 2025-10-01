# Modular Redis Implementation Plan

## Current Goals
1. Make the legacy `server.c` codebase modular.
2. Establish a clean separation between networking, command handling, persistence, replication, and the in-memory store.
3. Define a roadmap of phases to reach a feature-complete Redis clone.

## Target Source Layout
```
+-- include/
¦   +-- command.h
¦   +-- config.h
¦   +-- datastore.h
¦   +-- replication.h
¦   +-- resp.h
¦   +-- rdb.h
¦   +-- server.h
+-- src/
¦   +-- command.c
¦   +-- config.c
¦   +-- datastore.c
¦   +-- main.c
¦   +-- replication.c
¦   +-- resp.c
¦   +-- rdb.c
```

## Module Responsibilities
- **config**: Parse CLI arguments, hold runtime configuration, expose helpers.
- **datastore**: Manage key/value storage, TTL bookkeeping, concurrency control.
- **resp**: Read/write RESP frames, convert socket buffers into argument vectors.
- **command**: Dispatch parsed commands, call into datastore/persistence/replication layers, format responses.
- **rdb**: Load the initial dataset from disk (and later persist snapshots).
- **server**: Accept clients, spin up handler threads, bridge sockets with the command layer.
- **replication**: Handle master/slave negotiation, keep sockets to master alive, stream updates.

## Phase Roadmap
- **Phase 1** – Foundations (current task): Modular code layout, fix parsing bugs, basic RESP command set (PING/ECHO/SET/GET/KEYS/CONFIG/INFO), RDB load, single-threaded correctness with mutex-protected store.
- **Phase 2** – Persistence polish: Implement periodic RDB save, handle `SAVE`, `BGSAVE`, and `FLUSHALL`, add checksum verification.
- **Phase 3** – Expiration fidelity: Active + passive expiration, support `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` commands.
- **Phase 4** – Replication maturity: Complete partial resync, backlog handling, propagate write commands, support `PSYNC`/`REPLCONF` negotiations.
- **Phase 5** – Advanced commands & modules: Sorted sets, lists, pub/sub, transactions, scripting stub.

Each phase will keep the modules isolated so we can iterate without large-scale rewrites.
