# Runtime Architecture Guide

Redis-Implementation follows a modular design so each subsystem can evolve independently. This document explains how requests flow through the system, which files implement each stage, and the invariants you should preserve when extending the server.

---

## High-Level Pipeline

```
Client Socket
   │
   ▼
Reactor (poll/epoll/kqueue)  ──► Connection State ──► RESP Parser ──► Command Dispatcher
                                                   │                  │
                                                   ▼                  ▼
                                          Session (ACL, RESP mode)   Command Handlers
                                                   │                  │
                                                   └────► Datastore / Persistence / Replication
```

Each block corresponds to concrete code modules:

| Stage | Responsibilities | Key Files |
|-------|------------------|-----------|
| Reactor | Multiplex sockets, deliver readable/writable events. | `src/reactor.c`, `src/reactor_poll.c`, `src/reactor_epoll.c` (platform-specific). |
| Connection State | Buffer incoming bytes, call `resp_parse`, maintain per-client session. | `src/connection.c`, `include/connection.h`. |
| RESP Parser | Turn raw bytes into argument vectors (`resp_command_t`). | `src/resp.c`, `include/resp.h`. |
| Command Dispatcher | Validate arity/flags, route to handler functions, emit command events. | `src/command.c`, `src/command_dispatcher.c`, `include/command_dispatcher.h`. |
| Handlers | Implement command families (basic, key-value, admin, expiration, replication). | `src/handlers_*.c`, `include/command_handlers.h`. |
| Datastore | Manage in-memory data, expiration, snapshots. | `src/datastore.c`, `include/datastore.h`. |
| Persistence | Execute SAVE/BGSAVE, coordinate background threads, write RDB files. | `src/persistence.c`, `include/persistence.h`, `src/rdb.c`. |
| Replication | Prepare backlog, manage replica negotiation (future streaming). | `src/replication.c`, `include/replication.h`. |
| ACL/Sessions | Enforce authentication, role checks, RESP mode negotiation. | `src/acl.c`, `include/acl.h`, `include/command.h`. |

---

## Reactor and Event Loop

* The reactor abstracts platform-specific polling. On Linux, `poll(2)` is used by default (`src/reactor_poll.c`). Future backends (`epoll`, `kqueue`) plug into the same interface.
* Events trigger `connection_handle_event`, which receives the socket FD and `POLLIN`/`POLLOUT` bitmask. Errors such as `POLLERR` tear down the connection gracefully.
* Keep the reactor single-threaded for predictable state transitions. Expensive tasks (persistence saves) run on background threads to avoid blocking the reactor.

---

## Connection Lifecycle

* **Creation:** `connection_state_create` initializes buffers, associates the connection with the global `command_context_t`, and seeds a fresh `command_session_t`.
* **Processing:** Incoming bytes accumulate in `state->buffer`. Once a full RESP frame is available, `command_handle` is invoked with the parsed command.
* **Reset:** After network disconnects, `command_session_reset` returns the session to defaults (RESP2 mode, ACL reset, optional connection name restoration when persistence is enabled).

_Invariant:_ Never hold onto pointers into `state->buffer` after processing—handlers receive copies of each bulk string.

---

## Command Dispatch and Validation

`command_handle` performs several checks before invoking a handler:

1. **Lookup:** `command_dispatcher_find` performs case-insensitive matching.
2. **Validation:** `command_spec_validate` enforces arity, authorization flags, and optional validator callbacks (e.g., `CONFIG GET` subcommand checks).
3. **Handler Call:** The handler returns a RESP-formatted reply. Non-zero returns are logged but still forwarded to the event observers.
4. **Events:** `command_event_dispatcher_dispatch` notifies listeners (replication, metrics hooks) with a `command_event_t` struct.

Keep validation stateless and side-effect-free; all state changes happen in handlers.

---

## Session State & ACL

* Each connection has an embedded `acl_state_t`. It mirrors the global configuration but tracks per-connection authentication.
* `HELLO` and `AUTH` use `acl_authenticate` to validate credentials and update roles.
* RESP version (`session->resp_version`) propagates to reply helpers—commands query it to select RESP2 vs. RESP3 encodings.
* Connection names set through `HELLO SETNAME` are persisted across resets only when `redis_config_t.persist_connection_names` is true.

_Invariant:_ Commands that depend on ACL must only check access through `acl_has_role` to honor future policy changes.

---

## Datastore and TTL Management

* The datastore uses a chaining hash table (`redis_store_t`). All modifications run under `pthread_mutex_lock(&store->lock)` to guarantee thread safety.
* TTLs are stored as absolute millisecond timestamps in each entry (`entry->expiry_ms`). Active pruning is handled by a background expiration thread (see `src/expiry.c`), while passive pruning occurs on lookup/set operations.
* Snapshotting functions (`datastore_snapshot`, `datastore_snapshot_free`) supply persistence layers with a stable view of the keyspace.

---

## Persistence Workflow

* Synchronous `SAVE` runs in the foreground under `persistence_execute_sync`, blocking new commands until completion.
* `BGSAVE` spawns a background thread that performs the same work asynchronously (`persistence_execute_async`). A mutex ensures only one save runs at a time.
* RDB serialization lives in `src/rdb.c`. For now only simple string types are emitted.
* Errors propagate to clients via RESP strings (`ERR Background save already in progress`), while logs print success/failure messages.

---

## Replication Roadmap

* Current implementation populates a replication backlog and responds to `PSYNC` requests with placeholder data.
* `replication_backlog_stream` (planned) will stream buffered writes to replicas once continuous propagation is implemented.
* `REPLCONF` and `PSYNC` commands live in `src/handlers_replication.c`, stubbing out the negotiation sequence for future work.

---

## Event Observers and Extensibility

* Command events allow subsystems to subscribe to write/delete/expiry notifications without tight coupling.
* The design anticipates modules or plugins registering additional commands; keep dispatcher logic generic and avoid hard-coded assumptions about the number of handlers.
* Configuration changes flow through `command_context_init`—new subsystems should register with the runtime there to ensure deterministic startup.

---

## Error Propagation and Robustness

* Each layer must fail fast and leave the system in a consistent state. For instance, `persistence_save` releases `save_lock` on all exit paths.
* When a subsystem fails during startup, the runtime should abort initialization cleanly (future improvement: return structured error codes).
* Use guard clauses liberally to avoid deep nesting and reduce the risk of forgotten cleanup steps.

---

## Reference Flow: `SET`

1. **Client** sends `*3\r\n$3\r\nSET\r\n...`.
2. **Connection** buffers data, `resp_parse` returns `resp_command_t`.
3. **Dispatcher** finds `handle_set_command`, validates ACL (`CMD_FLAG_WRITE`).
4. **Handler** writes to datastore, optionally sets expiry, returns `+OK\r\n`.
5. **Event bus** emits a `COMMAND_EVENT_WRITE` for replication/metrics.
6. **Network** replies to client via `send` in the connection loop.

---

## Reference Flow: `HELLO 3 AUTH user pass SETNAME client-01`

1. **Command parsing** handles optional arguments in `handle_hello_command`.
2. **ACL** verifies credentials (`acl_authenticate`).
3. **Session** updates RESP mode, connection name, and authentication flags.
4. **Response** uses `send_resp3_hello` to deliver a RESP3 map with metadata (role, mode, username, connection name).

---

## Future Enhancements

* Replace ad-hoc logging with structured logging macros.
* Expand the event system for metrics and tracing sinks.
* Introduce job queues for long-running tasks (AOF rewrite, multi-stage replication) to avoid overloading the reactor thread.

Keep this document current by adding new flow diagrams or tables when you introduce subsystems or change behavior.
