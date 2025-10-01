# RedisFoundry Command Manual

RedisFoundry aims to behave like a lightweight Redis-compatible server while exposing a deliberately small, well-documented command surface. This document summarises every command the current build understands, how the request should be structured, and what to expect in response.

> **Notation**
> - Square brackets [] denote optional tokens.
> - Uppercase identifiers (KEY, VALUE) indicate user-supplied values (case-sensitive unless stated otherwise).
> - RESP examples use the standard 
redis-cli form (COMMAND arg �).

---

## 1. Server Lifecycle

### 1.1 Startup Flags

| Flag | Description | Example |
|------|-------------|---------|
| --dir <path> | Directory that holds the RDB file (created if missing). | --dir /var/lib/redisfoundry |
| --dbfilename <file> | Snapshot filename written inside --dir. | --dbfilename dump.rdb |
| --port <int> | TCP port to listen on (default 6379). | --port 6380 |
| --replicaof <host> <port> | Start as a replica of a master at <host>:<port>. | --replicaof 127.0.0.1 6379 |

### 1.2 Shutdown

Use 
redis-cli -p <port> shutdown for a graceful stop. The server flushes in-flight writes and the expiration thread is halted automatically.

---

## 2. Core Data Commands

### 2.1 PING
- **Syntax**: PING [MESSAGE]
- **Response**: PONG or the supplied message (echoed back).
- **Notes**: Useful health probe; safe across both master/replica roles.

### 2.2 ECHO
- **Syntax**: ECHO MESSAGE
- **Response**: MESSAGE (verbatim).

### 2.3 SET
- **Syntax**: SET KEY VALUE [EX seconds] [PX milliseconds]
- **Response**: OK on success.
- **Notes**:
  - If both EX and PX are absent, the key persists indefinitely.
  - Passing a non-positive TTL expires the key immediately.
  - Duplicate SET replaces existing value and resets TTL.

### 2.4 GET
- **Syntax**: GET KEY
- **Response**: bulk string value or 
il if missing/expired.

### 2.5 KEYS
- **Syntax**: KEYS *
- **Response**: Array of all currently live keys.
- **Notes**: No pattern matching yet; used mostly for debugging. TTL pruning is triggered before the list is produced.

### 2.6 FLUSHALL / FLUSHDB
- **Syntax**: FLUSHALL
- **Response**: OK
- **Notes**: Clears the entire in-memory dataset (same as FLUSHDB, which is accepted for client compatibility). TTL timers are reset because keys are removed.

---

## 3. Expiration Commands

All expiration times are stored as absolute millisecond timestamps. Expired keys disappear passively (whenever accessed) and actively (background pruning thread).

### 3.1 EXPIRE
- **Syntax**: EXPIRE KEY seconds
- **Response**: Integer 1 if TTL set, Nil if key absent or already gone.
- **Notes**: Zero/negative seconds expires immediately.

### 3.2 PEXPIRE
- **Syntax**: PEXPIRE KEY milliseconds
- **Response**: Integer 1 on success, Nil otherwise.

### 3.3 TTL
- **Syntax**: TTL KEY
- **Response**:
  - -2: key does not exist (or expired since last access).
  - -1: key exists but has no TTL.
  - >=0: remaining lifetime in seconds (rounded down).

### 3.4 PTTL
- **Syntax**: PTTL KEY
- **Response**:
  - -2: key absent.
  - -1: key present without TTL.
  - >=0: remaining lifetime in milliseconds.

### 3.5 PERSIST
- **Syntax**: PERSIST KEY
- **Response**: Integer 1 if key�s TTL cleared; Nil if key missing or already persistent.

### Background Expiration
- A dedicated thread wakes every 100 ms (configurable in source) and prunes up to 128 stale keys per cycle. This keeps the dataset lean even without client access.

---

## 4. Persistence Commands

### 4.1 SAVE
- **Syntax**: SAVE
- **Response**: OK on success; ERR Background save already in progress if a snapshot is running.
- **Behaviour**: Synchronous snapshot to --dir/--dbfilename. Client writes are processed only after the save finishes.

### 4.2 BGSAVE
- **Syntax**: BGSAVE
- **Response**: Background saving started or ERR Background save already in progress.
- **Behaviour**: Spawns a background worker thread that runs SAVE logic without blocking clients. Status lines print to stdout upon completion.

### 4.3 FLUSHALL (revisited)
- Clearing the store and then SAVE produces an empty RDB. Replicas re-sync with the truncated dataset at next handshake.

---

## 5. Information & Configuration

### 5.1 CONFIG GET <param>
- **Supported params**: dir, dbfilename
- **Response**: Two-element array [param, value] or empty array if unknown.

### 5.2 INFO [section]
- **Supported sections**: (default) overall, 
eplication
- INFO replication returns:
  - 
ole:master or 
ole:slave
  - master_host, master_port (for replicas)
  - Static master_replid, master_repl_offset

### 5.3 PING, ECHO (see �2) double as diagnostics.

---

## 6. Replication Commands

### 6.1 REPLCONF
- Used internally by replicas to acknowledge capabilities; returns OK.

### 6.2 PSYNC
- Syntax: PSYNC ? -1
- Response: FULLRESYNC <replid> <offset> followed by an empty RDB payload (current implementation). Incremental propagation is not yet implemented; replicas rely on snapshot transfer.

**Replica Caveats**
- Behaviour: replicas connect to master, perform REPLCONF negotiation, and import the master�s RDB.
- Limitations: live command propagation is not implemented yet (writes on master after the initial sync aren�t streamed). This is the primary milestone for Phase 4.

---

## 7. Administration Script Snippets

### 7.1 Starting a Master and Replica (WSL)
`ash
# Master on 6379
./redis-server --dir /tmp/rdb-master --dbfilename dump.rdb --port 6379

# Replica on 6380
./redis-server --dir /tmp/rdb-replica --dbfilename replica.rdb --port 6380 --replicaof 127.0.0.1 6379
`

### 7.2 Smoke Test
```bash
redis-cli -p 6379 ping
redis-cli -p 6379 set demo value
redis-cli -p 6379 expire demo 5
redis-cli -p 6379 ttl demo
sleep 6
redis-cli -p 6379 get demo   # -> (nil)
```

### 7.3 Snapshot Workflow
```bash
redis-cli -p 6379 save       # synchronous
# or
edis-cli -p 6379 bgsave   # asynchronous
ls -l /tmp/rdb-master/dump.rdb
```

---

## 8. Behaviour Guarantees & Limitations
- **Consistency**: SET/GET/EXPIRE operations are atomic per key; in-memory store is mutex-protected.
- **Snapshots**: SAVE/BGSAVE produce Redis-compatible RDB files for the data types currently supported (simple strings).
- **Expiration**: TTL resolution is millisecond; background pruning strives for eventual removal but does not guarantee exact timing.
- **Replication**: Initial sync works; continuous command propagation not yet available.
- **Protocol**: RESP2 subset; pipelining and basic multibulk commands supported.

---

## 9. Future Roadmap (Context)
- Phase 4 will stream mutating commands to replicas and maintain a backlog for partial resyncs.
- Phase 5 introduces complex data types (lists, sets), transactions, pub/sub.

---

## 10. Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| ERR Background save already in progress | Parallel SAVE/BGSAVE issued. | Wait for completion or check stdout for status. |
| Keys linger after TTL | Process idle? | Background thread may take up to 100?ms; manual access (GET) also prunes. |
| Replica not updating | Phase 4 feature missing. | Trigger manual SAVE; restart replica to resync. |
| Could not connect | Wrong port or server not running. | Verify 
edis-cli -p <port> ping. |
| ERR unknown command | Command not yet implemented. | Reference table above before assuming parity with upstream Redis. |

---

Happy hacking with RedisFoundry! For deeper architectural notes, see docs/architecture.md and follow the phase roadmap as new features land.
