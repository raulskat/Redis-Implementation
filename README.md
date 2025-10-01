# Redis-like Server Implementation

This project implements a lightweight Redis-compatible server written in C. The legacy single-file prototype has been split into focused modules for networking, command parsing, persistence, replication, and the in-memory datastore. The goal is to iterate towards a feature-complete Redis clone following the phase roadmap captured in docs/architecture.md.

## Current Capabilities
- RESP command parsing with support for PING, ECHO, SET, GET, KEYS, CONFIG GET, INFO replication, REPLCONF, and PSYNC.
- Thread-safe in-memory key/value store with optional TTL (millisecond resolution).
- RDB bootstrap loader that understands string-encoded keys/values and absolute expirations.
- Multithreaded TCP server accepting concurrent clients.
- Replica handshake stub that connects to the configured master and negotiates basic replication state.

## Building
Requirements:
- GCC/Clang with pthread support (POSIX platforms or Mingw-w64 with the POSIX threading model).
- Make.

`
make          # builds the redis-server binary
`

The resulting executable is ./redis-server. To clean artifacts run make clean.

> **Note**: If you are on Windows, use the MSYS2/Mingw-w64 toolchain configured with POSIX threads, or build inside WSL. The legacy MinGW (win32) environment does not ship <pthread.h>.

## Running
`
./redis-server --dir /tmp/rdbfile --dbfilename dump.rdb --port 6379
`

Optional flags:
- --dir <path>: directory containing the RDB file.
- --dbfilename <name>: RDB file name.
- --port <n>: TCP port to listen on.
- --replicaof <host> <port>: start as a replica and connect to the given master.

## Repository Layout
`
+-- include/          # Public headers for each module
+-- src/              # Module implementations
+-- docs/architecture.md
+-- Makefile
+-- README.md
`

## Roadmap (high level)
Phase 1 (current): modularisation, RESP parser, safer datastore, RDB load fixes, replication handshake stub.
Phase 2: scheduled/common persistence commands (SAVE, BGSAVE), checksum verification, FLUSHALL.
Phase 3: full expiration machinery and commands (EXPIRE, TTL, passive/active eviction).
Phase 4: replication backlog, streaming updates, partial resync, write propagation.
Phase 5: broader command surface (lists, sets, pub/sub, transactions) and memory optimisations.

See docs/architecture.md for detailed module responsibilities and the multi-phase execution plan.
